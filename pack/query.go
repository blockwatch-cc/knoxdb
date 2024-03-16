// Copyright (c) 2018-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// TODO
// - support expressions in fields and condition lists

package pack

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

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
	table Table             // table interface
	conds ConditionTreeNode // compiled conditions
	fout  FieldList         // output fields
	freq  FieldList         // required fields (output + query matching)

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
	RowsScanned    int           `json:"rows_scanned"`
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

// func (t *Table) NewQuery(name string) Query {
// 	return NewQuery(name).WithTable(t)
// }

// func (s *Store) NewQuery(name string) Query {
// 	return NewQuery(name).WithStore(s)
// }

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
}

func (q Query) Table() Table {
	return q.table
}

func (q Query) Runtime() time.Duration {
	return time.Since(q.start)
}

func (q Query) IsBound() bool {
	return q.table != nil && !q.conds.Empty()
}

func (q Query) PrintTiming() string {
	return fmt.Sprintf("query: %s compile=%s analyze=%s journal=%s index=%s scan=%s total=%s matched=%d rows, scanned=%d rows, scheduled=%d packs, scanned=%d packs, searched=%d index rows",
		q.Name,
		q.stats.CompileTime,
		q.stats.AnalyzeTime,
		q.stats.JournalTime,
		q.stats.IndexTime,
		q.stats.ScanTime,
		q.stats.TotalTime,
		q.stats.RowsMatched,
		q.stats.RowsScanned,
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

func (q *Query) Compile() error {
	if q.table == nil {
		return ErrNoTable
	}
	fields := q.table.Fields()
	if !strings.HasPrefix(q.Name, q.table.Name()) {
		q.Name = q.table.Name() + "." + q.Name
	}
	q.start = time.Now()
	q.lap = q.start

	// ensure all queried fields exist
	for _, f := range q.conds.Fields() {
		if !fields.Contains(f.Name) {
			return fmt.Errorf("pack: missing table field %s in table %s for query %s", f.Name, q.table.Name(), q.Name)
		}
	}

	// process conditions
	if q.conds.Empty() {
		q.conds = q.Conditions.Bind(fields)
		if err := q.conds.Compile(); err != nil {
			return fmt.Errorf("pack: %s %v", q.Name, err)
		}
	}

	// identify output fields
	if len(q.fout) == 0 {
		if len(q.Fields) == 0 {
			q.fout = fields
		} else {
			q.fout = fields.Select(q.Fields...)
			q.fout = q.fout.MergeUnique(fields.Pk()).Sort()
		}
	}

	// identify required fields (output + used in conditions)
	if len(q.freq) == 0 {
		q.freq = q.fout.MergeUnique(q.conds.Fields()...).Sort()
		q.freq = q.freq.MergeUnique(fields.Pk()).Sort()
	}

	// check query can be processed
	q.stats.CompileTime = q.Tick()
	if err := q.check(fields); err != nil {
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

func (q Query) check(fields FieldList) error {
	for _, v := range q.freq {
		field := fields.Find(v.Name)
		// field must exist
		if !field.IsValid() {
			return fmt.Errorf("undefined field '%s' in query %s", v.Name, q.Name)
		}
		// field type must match
		if field.Type != v.Type {
			return fmt.Errorf("mismatched type %s for field '%s' in query %s", v.Type, v.Name, q.Name)
		}
		// field index must be valid
		if v.Index < 0 || v.Index >= len(fields) {
			return fmt.Errorf("illegal index %d for field '%s' in query %s", v.Index, v.Name, q.Name)
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

// INDEX QUERY: use index lookup for indexed fields
// - fetch pk lists for every indexed field
// - when resolved, replace source condition with new FilterModeIn condition
func (q *Query) QueryIndexes(ctx context.Context, tx *Tx) error {
	if q.NoIndex || q.conds.Empty() {
		return nil
	}
	hits, err := q.table.QueryIndexesTx(ctx, tx, &q.conds)
	if err != nil {
		return fmt.Errorf("query %s: %v", q.Name, err)
	}
	q.stats.IndexLookups += hits
	q.stats.IndexTime = q.Tick()
	return nil
}

func (q Query) WithTable(table Table) Query {
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

func (q Query) And(field string, mode FilterMode, value any) Query {
	q.Conditions.And(field, mode, value)
	return q
}

func (q Query) Or(field string, mode FilterMode, value any) Query {
	q.Conditions.Or(field, mode, value)
	return q
}

func (q Query) AndEqual(field string, value any) Query {
	return q.And(field, FilterModeEqual, value)
}

func (q Query) AndNotEqual(field string, value any) Query {
	return q.And(field, FilterModeNotEqual, value)
}

func (q Query) AndIn(field string, value any) Query {
	return q.And(field, FilterModeIn, value)
}

func (q Query) AndNotIn(field string, value any) Query {
	return q.And(field, FilterModeNotIn, value)
}

func (q Query) AndLt(field string, value any) Query {
	return q.And(field, FilterModeLt, value)
}

func (q Query) AndLte(field string, value any) Query {
	return q.And(field, FilterModeLte, value)
}

func (q Query) AndGt(field string, value any) Query {
	return q.And(field, FilterModeGt, value)
}

func (q Query) AndGte(field string, value any) Query {
	return q.And(field, FilterModeGte, value)
}

func (q Query) AndRegexp(field string, value any) Query {
	return q.And(field, FilterModeRegexp, value)
}

func (q Query) AndRange(field string, from, to any) Query {
	q.Conditions.AndRange(field, from, to)
	return q
}

func (q Query) Execute(ctx context.Context, val any) error {
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
