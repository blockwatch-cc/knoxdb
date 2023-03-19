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
	logpkg "github.com/echa/log"
)

var QueryLogMinDuration time.Duration = 500 * time.Millisecond

type Query struct {
	Name       string           // optional, used for query stats
	Fields     []string         // SELECT ...
	Conditions UnboundCondition // WHERE ... AND / OR tree
	Order      OrderType        // ASC|DESC
	Limit      int              // LIMIT ...
	Offset     int              // OFFSET ...
	NoCache    bool             // explicitly disable pack caching for this query
	NoIndex    bool             // explicitly disable index query (use for many known duplicates)
	Debugf     logpkg.LogfFn

	// GroupBy   FieldList   // GROUP BY ... - COLLATE/COLLAPSE
	// OrderBy   FieldList    // ORDER BY ...
	// Aggregate AggregateList // sum, mean, ...

	// internal
	table *Table            // cached table pointer
	conds ConditionTreeNode // compiled conditions
	fout  FieldList         // output fields
	freq  FieldList         // required fields (for out and query matching)
	fidx  FieldList         // existing index fields

	// metrics
	logAfter time.Duration
	start    time.Time
	lap      time.Time
	stats    QueryStats
	debug    bool
}

type QueryStats struct {
	CompileTime    time.Duration `json:"compile_time"`
	AnalyzeTime    time.Duration `json:"analyze_time"`
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
	return q.conds.NoMatch()
}

func NewQuery(name string) Query {
	return Query{
		Name:     name,
		Order:    OrderAsc,
		Debugf:   logpkg.Noop,
		logAfter: QueryLogMinDuration,
	}
}

func (t *Table) NewQuery(name string) Query {
	return NewQuery(name).WithTable(t)
}

func (q *Query) Close() {
	if q.table != nil {
		q.stats.TotalTime = time.Since(q.start)
		if q.stats.TotalTime > q.logAfter {
			log.Warnf("%s", newLogClosure(func() string {
				return q.PrintTiming()
			}))
		}
		q.table = nil
	}
	q.Fields = nil
	q.fout = nil
	q.freq = nil
	q.fidx = nil
}

func (q Query) Table() *Table {
	return q.table
}

func (q Query) Runtime() time.Duration {
	return time.Since(q.start)
}

func (q Query) IsBound() bool {
	return q.table != nil && !q.conds.Empty()
}

func (q Query) PrintTiming() string {
	return fmt.Sprintf("query: %s compile=%s analyze=%s journal=%s index=%s scan=%s total=%s matched=%d rows, scheduled=%d packs, scanned=%d packs, searched=%d index rows",
		q.Name,
		q.stats.CompileTime,
		q.stats.AnalyzeTime,
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

func (q *Query) Tick() time.Duration {
	dur, _ := q.TickNow()
	return dur
}

func (q *Query) TickNow() (time.Duration, time.Time) {
	now := time.Now()
	diff := now.Sub(q.lap)
	q.lap = now
	return diff, now
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

	// ensure all queried fields exist
	tableFields := q.table.fields
	for _, f := range q.conds.Fields() {
		if !tableFields.Contains(f.Name) {
			return fmt.Errorf("pack: missing table field %s in table %s for query %s", f.Name, t.Name(), q.Name)
		}
	}

	// process conditions
	if q.conds.Empty() {
		q.conds = q.Conditions.Bind(q.table)
		if err := q.conds.Compile(); err != nil {
			return fmt.Errorf("pack: %s %v", q.Name, err)
		}
	}

	// identify output fields
	if len(q.fout) == 0 {
		if len(q.Fields) == 0 {
			q.fout = tableFields
		} else {
			q.fout = tableFields.Select(q.Fields...)
			q.fout = q.fout.MergeUnique(tableFields.Pk()).Sort()
		}
	}

	// identify required fields (output + used in conditions)
	if len(q.freq) == 0 {
		q.freq = q.fout.MergeUnique(q.conds.Fields()...).Sort()
		q.freq = q.freq.MergeUnique(tableFields.Pk()).Sort()
	}

	// identify index fields
	if len(q.fidx) == 0 {
		q.fidx = t.fields.Indexed()
	}

	// check query can be processed
	q.stats.CompileTime = q.Tick()
	if err := q.check(); err != nil {
		q.stats.TotalTime = q.stats.CompileTime
		return err
	}

	if q.debug {
		q.Debugf("%s", newLogClosure(func() string {
			return q.Dump()
		}))
	} else {
		// set a sane default in case query struct was created with q := pack.Query{}
		q.Debugf = logpkg.Noop
	}
	return nil
}

func (q Query) check() error {
	tfields := q.table.fields
	for _, v := range q.freq {
		tfield := tfields.Find(v.Name)
		// field must exist
		if !tfield.IsValid() {
			return fmt.Errorf("undefined field '%s/%s' in query %s", q.table.name, v.Name, q.Name)
		}
		// field type must match
		if tfield.Type != v.Type {
			return fmt.Errorf("mismatched type %s for field '%s/%s' in query %s", v.Type, q.table.name, v.Name, q.Name)
		}
		// field index must be valid
		if v.Index < 0 || v.Index >= len(tfields) {
			return fmt.Errorf("illegal index %d for field '%s/%s' in query %s", v.Index, q.table.name, v.Name, q.Name)
		}
	}
	// root condition may be empty but must not be a leaf for index queries to work
	if q.conds.Leaf() {
		return fmt.Errorf("unexpected simple condition tree in query %s", q.Name)
	}
	if q.Limit < 0 {
		return fmt.Errorf("invalid limit %d", q.Limit)
	}
	if q.Offset < 0 {
		return fmt.Errorf("invalid offset %d", q.Offset)
	}
	return nil
}

func (q *Query) queryIndexNode(ctx context.Context, tx *Tx, node *ConditionTreeNode) error {
	// - visit all leafs, run index scan when field is indexed and condition allowed
	// - if collission-free, mark condition as processed (don't execute again)
	// - add IN cond to front of current tree branch level
	//   -> leaf-roots do not exist (invariant)
	ins := make([]ConditionTreeNode, 0)
	for i, v := range node.Children {
		if v.Leaf() {
			if !q.fidx.Contains(v.Cond.Field.Name) {
				q.Debugf("query: %s table non-indexed field %s for cond %s, fallback to table scan",
					q.Name, v.Cond.Field.Name, v.Cond.String())
				continue
			}
			idx := q.table.indexes.FindField(v.Cond.Field.Name)
			if idx == nil {
				q.Debugf("query: %s table missing index on field %s for cond %d, fallback to table scan",
					q.Name, v.Cond.Field.Name, v.Cond.String())
				continue
			}
			if !idx.CanMatch(*v.Cond) {
				q.Debugf("query: %s index %s cannot match cond %s, fallback to table scan",
					q.Name, idx.Name, v.Cond.String())
				continue
			}

			q.Debugf("query: %s index scan for %s", idx.name, v.Cond.String())

			// lookup matching primary keys from index (result is sorted)
			pkmatch, err := idx.LookupTx(ctx, tx, *v.Cond)
			if err != nil {
				log.Errorf("%s index scan: %v", q.Name, err)
				return err
			}
			q.stats.IndexLookups += len(pkmatch)

			// mark condition as processed (exclude hash indexes because they may
			// have collisions; to protect against this, we continue matching this
			// condition against the full result set, which should be much smaller
			// now)
			if !idx.Type.MayHaveCollisions() {
				v.Cond.processed = true
			}
			q.Debugf("query: %s index scan found %d matches", q.Name, len(pkmatch))

			if len(pkmatch) == 0 {
				v.Cond.nomatch = true
				continue
			}

			// create new leaf node
			c := &Condition{
				Field:    q.table.fields.Pk(), // primary key
				Mode:     FilterModeIn,        // IN
				Value:    pkmatch,             // list
				IsSorted: true,                // already sorted by index lookup
				Raw:      v.Cond.Raw + "/index_lookup",
			}

			// compile to build internal maps
			if err := c.Compile(); err != nil {
				return fmt.Errorf("pack: %s %v", q.Name, err)
			}

			// keep for later append
			ins = append(ins, ConditionTreeNode{Cond: c})
		} else {
			// recurse into child (use ptr to slice element)
			if err := q.queryIndexNode(ctx, tx, &node.Children[i]); err != nil {
				return err
			}
		}
	}

	// add new leafs to front of child list; this assumes the new indexed
	// condition (a list of primary keys) has lower execution cost than
	// other conditions in the same sub-tree
	//
	// FIXME: ideally we would keep processed conditions around and just skip
	// them in MaybeMatchPack() and MatchPack(); then we could just prepend
	// node.Children = append(ins, node.Children...)
	if len(ins) > 0 {
		for _, v := range node.Children {
			// skip processed source conditions unless they led to an empty result
			// because we need them to check for nomatch later
			if v.Leaf() && v.Cond.processed && !v.Cond.nomatch {
				q.Debugf("query: %s replacing condition %s", q.Name, v.Cond.String())
				continue
			}
			ins = append(ins, v)
		}
		node.Children = ins
		q.Debugf("Updated query: %v", logpkg.NewClosure(func() string {
			return q.Dump()
		}))
	}

	return nil
}

// INDEX QUERY: use index lookup for indexed fields
// - fetch pk lists for every indexed field
// - when resolved, replace source condition with new FilterModeIn condition
func (q *Query) QueryIndexes(ctx context.Context, tx *Tx) error {
	if q.NoIndex || q.conds.Empty() {
		return nil
	}
	if err := q.queryIndexNode(ctx, tx, &q.conds); err != nil {
		return err
	}
	q.stats.IndexTime = q.Tick()
	return nil
}

// collect list of packs to visit in pk order
func (q *Query) MakePackSchedule(reverse bool) []int {
	schedule := make([]int, 0, q.table.packidx.Len())
	// walk list in pk order (pairs are always sorted by min pk)
	for _, p := range q.table.packidx.pos {
		if q.conds.MaybeMatchPack(q.table.packidx.packs[p]) {
			schedule = append(schedule, int(p))
		}
	}
	// reverse for descending walk
	if reverse {
		for l, r := 0, len(schedule)-1; l < r; l, r = l+1, r-1 {
			schedule[l], schedule[r] = schedule[r], schedule[l]
		}
	}
	q.stats.PacksScheduled = len(schedule)
	q.stats.AnalyzeTime = q.Tick()
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
	q.stats.AnalyzeTime = q.Tick()
	return schedule
}

func (q Query) WithTable(table *Table) Query {
	q.table = table
	return q
}

func (q Query) WithFields(names ...string) Query {
	q.Fields = append(q.Fields, names...)
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

func (q Query) WithOffset(o int) Query {
	q.Offset = o
	return q
}

func (q Query) WithIndex(enable bool) Query {
	q.NoIndex = !enable
	return q
}

func (q Query) WithoutIndex() Query {
	q.NoIndex = true
	return q
}

func (q Query) WithCache(enable bool) Query {
	q.NoCache = !enable
	return q
}

func (q Query) WithoutCache() Query {
	q.NoCache = true
	return q
}

func (q Query) WithStats() Query {
	q.logAfter = 0
	return q
}

func (q Query) WithoutStats() Query {
	q.logAfter = time.Hour
	return q
}

func (q Query) WithStatsAfter(d time.Duration) Query {
	q.logAfter = d
	return q
}

func (q Query) WithDebug() Query {
	q.debug = true
	q.Debugf = log.Debugf
	return q
}

func (q Query) AndCondition(conds ...UnboundCondition) Query {
	if len(conds) == 0 {
		return q
	}
	q.Conditions.Add(And(conds...))
	return q
}

func (q Query) OrCondition(conds ...UnboundCondition) Query {
	if len(conds) == 0 {
		return q
	}
	q.Conditions.Add(Or(conds...))
	return q
}

func (q Query) And(field string, mode FilterMode, value interface{}) Query {
	q.Conditions.And(field, mode, value)
	return q
}

func (q Query) Or(field string, mode FilterMode, value interface{}) Query {
	q.Conditions.Or(field, mode, value)
	return q
}

func (q Query) AndEqual(field string, value interface{}) Query {
	return q.And(field, FilterModeEqual, value)
}

func (q Query) AndNotEqual(field string, value interface{}) Query {
	return q.And(field, FilterModeNotEqual, value)
}

func (q Query) AndIn(field string, value interface{}) Query {
	return q.And(field, FilterModeIn, value)
}

func (q Query) AndNotIn(field string, value interface{}) Query {
	return q.And(field, FilterModeNotIn, value)
}

func (q Query) AndLt(field string, value interface{}) Query {
	return q.And(field, FilterModeLt, value)
}

func (q Query) AndLte(field string, value interface{}) Query {
	return q.And(field, FilterModeLte, value)
}

func (q Query) AndGt(field string, value interface{}) Query {
	return q.And(field, FilterModeGt, value)
}

func (q Query) AndGte(field string, value interface{}) Query {
	return q.And(field, FilterModeGte, value)
}

func (q Query) AndRegexp(field string, value interface{}) Query {
	return q.And(field, FilterModeRegexp, value)
}

func (q Query) AndRange(field string, from, to interface{}) Query {
	q.Conditions.AndRange(field, from, to)
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

func (q Query) Run(ctx context.Context) (*Result, error) {
	return q.table.Query(ctx, q)
}
