// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// TODO
// - support expressions in fields and condition lists

package pack

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"blockwatch.cc/knoxdb/vec"
)

var QueryLogMinDuration time.Duration = 500 * time.Millisecond

type Query struct {
	Name       string        // optional, used for query stats
	Fields     FieldList     // SELECT ...
	Conditions ConditionList // WHERE ... AND (TODO: OR)
	Order      OrderType     // ASC|DESC
	Limit      int           // LIMIT ...
	NoCache    bool          // explicitly disable pack caching for this query
	NoIndex    bool          // explicitly disable index query (use for many known duplicates)

	// GroupBy   FieldList   // GROUP BY ... - COLLATE/COLLAPSE
	// OrderBy   FieldList    // ORDER BY ...
	// Aggregate AggregateList // sum, mean, ...

	// internal
	table     *Table    // cached table pointer
	pkids     []uint64  // primary key list from index lookup, return to pool on close
	reqfields FieldList // all fields required by this query

	// metrics
	start time.Time
	lap   time.Time
	stats QueryStats
}

type QueryStats struct {
	CompileTime    time.Duration `json:"compile_time"`
	JournalTime    time.Duration `json:"journal_time"`
	IndexTime      time.Duration `json:"index_time"`
	ScanTime       time.Duration `json:"scan_time"`
	TotalTime      time.Duration `json:"total_time"`
	PacksScheduled int           `json:"packs_scheduled"`
	PacksScanned   int           `json:"packs_scanned"`
	RowsMatched    int           `json:"rows_matched"`
	IndexLookups   int           `json:"index_lookups"`
}

// type AggregateList []Aggregate
// type Aggregate struct {
// 	Field Field
// 	Func  AggFunction
// }

func (q Query) IsEmptyMatch() bool {
	return q.pkids != nil && len(q.pkids) == 0
}

func NewQuery(name string, table *Table) Query {
	now := time.Now()
	f := table.Fields()
	return Query{
		Name:      name,
		table:     table,
		start:     now,
		lap:       now,
		Fields:    f,
		Order:     OrderAsc,
		reqfields: f,
	}
}

func (q *Query) Close() {
	if q.pkids != nil {
		q.pkids = q.pkids[:0]
		q.table.pkPool.Put(q.pkids)
		q.pkids = nil
	}
	if q.table != nil {
		q.stats.TotalTime = time.Since(q.start)
		if q.stats.TotalTime > QueryLogMinDuration {
			log.Warnf("%s", newLogClosure(func() string {
				return q.PrintTiming()
			}))
		}
		q.table = nil
	}
	q.Fields = nil
	q.reqfields = nil
}

func (q *Query) Table() *Table {
	return q.table
}

func (q *Query) Runtime() time.Duration {
	return time.Since(q.start)
}

func (q *Query) PrintTiming() string {
	return fmt.Sprintf("query: %s compile=%s journal=%s index=%s scan=%s total=%s matched=%d rows, scheduled=%d packs, scanned=%d packs, searched=%d index rows",
		q.Name,
		q.stats.CompileTime,
		q.stats.JournalTime,
		q.stats.IndexTime,
		q.stats.ScanTime,
		q.stats.TotalTime,
		q.stats.RowsMatched,
		q.stats.PacksScheduled,
		q.stats.PacksScanned,
		q.stats.IndexLookups,
	)
}

func (q *Query) Compile(t *Table) error {
	if t == nil {
		return ErrNoTable
	}
	if !strings.HasPrefix(q.Name, t.Name()) {
		q.Name = t.Name() + "." + q.Name
	}
	q.table = t
	q.start = time.Now()
	q.lap = q.start

	// process conditions first
	if err := q.Conditions.Compile(q.table); err != nil {
		return fmt.Errorf("pack: %s %v", q.Name, err)
	}

	// determine required table fields for this query
	if len(q.Fields) == 0 {
		q.Fields = t.Fields()
	} else {
		q.Fields = q.Fields.MergeUnique(t.Fields().Pk())
	}
	q.reqfields = q.Fields.MergeUnique(q.Conditions.Fields()...)

	// check query can be processed
	if err := q.Check(); err != nil {
		q.stats.TotalTime = time.Since(q.lap)
		return err
	}
	q.stats.CompileTime = time.Since(q.lap)
	return nil
}

func (q Query) Check() error {
	tfields := q.table.Fields()
	for _, v := range q.reqfields {
		// field must exist
		if !tfields.Contains(v.Name) {
			return fmt.Errorf("undefined field '%s.%s' in query %s", q.table.name, v.Name, q.Name)
		}
		// field type must match
		if tfields.Find(v.Name).Type != v.Type {
			return fmt.Errorf("mismatched type %s for field '%s.%s' in query %s", v.Type, q.table.name, v.Name, q.Name)
		}
		// field index must be valid
		if v.Index < 0 || v.Index >= len(tfields) {
			return fmt.Errorf("illegal index %d for field '%s.%s' in query %s", v.Index, q.table.name, v.Name, q.Name)
		}
	}
	return nil
}

// INDEX QUERY: use index lookup for indexed fields
// - fetch pk lists for every indexed field
// - intersect (logical AND) or merge (logical OR, not yet implemented) pk lists
// - when resolved, replace cond with new FilterModeIn cond
// - return pkid slice for later pool recycling
func (q *Query) QueryIndexes(ctx context.Context, tx *Tx) error {
	q.lap = time.Now()
	if q.NoIndex {
		q.stats.IndexTime = time.Since(q.lap)
		return nil
	}
	idxFields := q.table.fields.Indexed()
	for i, cond := range q.Conditions {
		if !idxFields.Contains(cond.Field.Name) {
			// log.Tracef("query: %s table non-indexed field '%s' for cond %d, fallback to table scan",
			// 	q.table.name, cond.Field.Name, i)
			continue
		}
		idx := q.table.indexes.FindField(cond.Field.Name)
		if idx == nil {
			// log.Tracef("query: %s table missing index on field %s for cond %d, fallback to table scan",
			// 	q.table.name, cond.Field.Name, i)
			continue
		}
		if !idx.CanMatch(cond) {
			// log.Tracef("query: index %s cannot match cond %d, fallback to table scan", idx.Name, i)
			continue
		}
		// lookup matching primary keys from index (result is sorted)
		pkmatch, err := idx.LookupTx(ctx, tx, cond)
		if err != nil {
			q.Close()
			return err
		}

		// intersect with primary keys from a previous index scan, if any
		// (i.e. logical AND)
		if q.pkids == nil {
			q.pkids = pkmatch
		} else {
			q.pkids = vec.Uint64.Intersect(q.pkids, pkmatch, q.table.pkPool.Get().([]uint64))
			pkmatch = pkmatch[:0]
			q.table.pkPool.Put(pkmatch)
		}

		// mark condition as processed (exclude hash indexes because they may
		// have collisions; to protect against this, we continue matching this
		// condition against the full result set, which should be much smaller
		// now)
		if !idx.Type.MayHaveCollisions() {
			q.Conditions[i].processed = true
		}
	}
	q.stats.IndexLookups = len(q.pkids)

	// add new condition (pk match) and remove processed conditions
	if len(q.pkids) > 0 {
		conds := ConditionList{
			Condition{
				Field:    q.table.Fields().Pk(), // primary key
				Mode:     FilterModeIn,          // must be in
				Value:    q.pkids,               // list
				IsSorted: true,                  // already sorted by index lookup
				Raw:      "pkid's from index lookup",
			},
		}
		for _, v := range q.Conditions {
			if !v.processed {
				conds = append(conds, v)
			}
		}
		// append and compile the pk lookup condition in-place
		q.Conditions = conds
		q.Conditions[0].Compile()
	}
	q.stats.IndexTime = time.Since(q.lap)
	return nil
}

// TODO: support more complex cond matches, right now this is a simple AND
func (q *Query) MakePackSchedule(reverse bool) []int {
	schedule := make([]int, 0, q.table.packidx.Len())
	// walk list in pk order (pairs are always sorted by min pk)
	for _, p := range q.table.packidx.pairs {
		if q.Conditions.MaybeMatchPack(q.table.packidx.packs[p.pos]) {
			schedule = append(schedule, p.pos)
		}
	}
	q.stats.PacksScheduled = len(schedule)
	// reverse for descending walk
	if reverse {
		for l, r := 0, len(schedule)-1; l < r; l, r = l+1, r-1 {
			schedule[l], schedule[r] = schedule[r], schedule[l]
		}
	}
	return schedule
}

// ordered list of packs that may contain matching ids (list can be reversed)
func (q *Query) MakePackLookupSchedule(ids []uint64, reverse bool) []int {
	schedule := make([]int, 0, q.table.packidx.Len())

	// extract min/max values from pack header's pk column
	mins, maxs := q.table.packidx.MinMaxSlices()

	// create schedule, note that this schedule may contain too many packs
	// because we only test the global max/min of requested lookup id's
	for i := range mins {
		// skip packs that don't contain pks in range
		if !vec.Uint64.ContainsRange(ids, mins[i], maxs[i]) {
			continue
		}
		schedule = append(schedule, i)
	}

	// sort schedule by min pk
	sort.Slice(schedule, func(i, j int) bool { return mins[schedule[i]] < mins[schedule[j]] })

	if reverse {
		for l, r := 0, len(schedule)-1; l < r; l, r = l+1, r-1 {
			schedule[l], schedule[r] = schedule[r], schedule[l]
		}
	}
	q.stats.PacksScheduled = len(schedule)
	return schedule
}

func (q Query) WithFields(names ...string) Query {
	q.Fields = q.table.Fields().Select(names...)
	return q
}

func (q Query) WithOrder(o OrderType) Query {
	q.Order = o
	return q
}

func (q Query) WithDesc() Query {
	q.Order = OrderDesc
	return q
}

func (q Query) WithAsc() Query {
	q.Order = OrderAsc
	return q
}

func (q Query) WithLimit(l int) Query {
	q.Limit = l
	return q
}

func (q Query) WithoutIndex() Query {
	q.NoIndex = true
	return q
}

func (q Query) WithoutCache() Query {
	q.NoCache = true
	return q
}

func (q Query) AndCondition(field string, mode FilterMode, value interface{}) Query {
	q.Conditions = append(q.Conditions, Condition{
		Field: q.table.Fields().Find(field),
		Mode:  mode,
		Value: value,
	})
	return q
}

func (q Query) AndEqual(field string, value interface{}) Query {
	return q.AndCondition(field, FilterModeEqual, value)
}

func (q Query) AndNotEqual(field string, value interface{}) Query {
	return q.AndCondition(field, FilterModeNotEqual, value)
}

func (q Query) AndIn(field string, value interface{}) Query {
	return q.AndCondition(field, FilterModeIn, value)
}

func (q Query) AndNotIn(field string, value interface{}) Query {
	return q.AndCondition(field, FilterModeNotIn, value)
}

func (q Query) AndLt(field string, value interface{}) Query {
	return q.AndCondition(field, FilterModeLt, value)
}

func (q Query) AndLte(field string, value interface{}) Query {
	return q.AndCondition(field, FilterModeLte, value)
}

func (q Query) AndGt(field string, value interface{}) Query {
	return q.AndCondition(field, FilterModeGt, value)
}

func (q Query) AndGte(field string, value interface{}) Query {
	return q.AndCondition(field, FilterModeGte, value)
}

func (q Query) AndRegexp(field string, value interface{}) Query {
	return q.AndCondition(field, FilterModeRegexp, value)
}

func (q Query) AndRange(field string, from, to interface{}) Query {
	q.Conditions = append(q.Conditions, Condition{
		Field: q.table.Fields().Find(field),
		Mode:  FilterModeRange,
		From:  from,
		To:    to,
	})
	return q
}

func (q Query) Execute(ctx context.Context, val interface{}) error {
	v := reflect.ValueOf(val)
	if v.Kind() != reflect.Ptr {
		return fmt.Errorf("pack: non-pointer passed to Execute")
	}
	v = reflect.Indirect(v)
	switch v.Kind() {
	case reflect.Slice:
		// get slice element type
		elem := v.Type().Elem()
		return q.table.Stream(ctx, q, func(r Row) error {
			// create new slice element (may be a pointer to struct)
			e := reflect.New(elem)
			ev := e

			// if element is ptr to struct, allocate the underlying struct
			if e.Elem().Kind() == reflect.Ptr {
				ev.Elem().Set(reflect.New(e.Elem().Type().Elem()))
				ev = reflect.Indirect(e)
			}

			// decode the struct element (re-use our interface-based methods)
			if err := r.Decode(ev.Interface()); err != nil {
				return err
			}

			// append slice element
			v.Set(reflect.Append(v, e.Elem()))
			return nil
		})
	case reflect.Struct:
		return q.table.Stream(ctx, q.WithLimit(1), func(r Row) error {
			return r.Decode(val)
		})
	default:
		return fmt.Errorf("pack: non-slice/struct passed to Execute")
	}
}

func (q Query) Stream(ctx context.Context, fn func(r Row) error) error {
	return q.table.Stream(ctx, q, fn)
}

func (q Query) Delete(ctx context.Context) (int64, error) {
	return q.table.Delete(ctx, q)
}

func (q Query) Count(ctx context.Context) (int64, error) {
	return q.table.Count(ctx, q)
}
