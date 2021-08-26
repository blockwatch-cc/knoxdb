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
	Name       string            // optional, used for query stats
	Fields     FieldList         // SELECT ...
	Conditions ConditionTreeNode // WHERE ... AND / OR tree
	Order      OrderType         // ASC|DESC
	Limit      int               // LIMIT ...
	Offset     int               // OFFSET ...
	NoCache    bool              // explicitly disable pack caching for this query
	NoIndex    bool              // explicitly disable index query (use for many known duplicates)

	// GroupBy   FieldList   // GROUP BY ... - COLLATE/COLLAPSE
	// OrderBy   FieldList    // ORDER BY ...
	// Aggregate AggregateList // sum, mean, ...

	// internal
	table     *Table    // cached table pointer
	reqfields FieldList // all fields required by this query
	idxFields FieldList

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
	return q.Conditions.NoMatch()
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
		idxFields: table.fields.Indexed(),
	}
}

func (q *Query) Close() {
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
	if err := q.Conditions.Compile(); err != nil {
		return fmt.Errorf("pack: %s %v", q.Name, err)
	}

	// determine required table fields for this query
	if len(q.Fields) == 0 {
		q.Fields = t.Fields()
	} else {
		q.Fields = q.Fields.MergeUnique(t.Fields().Pk())
	}
	q.reqfields = q.Fields.MergeUnique(q.Conditions.Fields()...)
	if len(q.idxFields) == 0 {
		q.idxFields = t.fields.Indexed()
	}

	// check query can be processed
	if err := q.Check(); err != nil {
		q.stats.TotalTime = time.Since(q.lap)
		return err
	}
	q.stats.CompileTime = time.Since(q.lap)

	log.Debug(newLogClosure(func() string {
		return q.Dump()
	}))

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
	// root condition may be empty but must not be a leaf
	if q.Conditions.Leaf() {
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
			if !q.idxFields.Contains(v.Cond.Field.Name) {
				log.Debugf("query: %s table non-indexed field %s for cond %s, fallback to table scan",
					q.Name, v.Cond.Field.Name, v.Cond.String())
				continue
			}
			idx := q.table.indexes.FindField(v.Cond.Field.Name)
			if idx == nil {
				log.Debugf("query: %s table missing index on field %s for cond %d, fallback to table scan",
					q.Name, v.Cond.Field.Name, v.Cond.String())
				continue
			}
			if !idx.CanMatch(*v.Cond) {
				log.Debugf("query: %s index %s cannot match cond %s, fallback to table scan",
					q.Name, idx.Name, v.Cond.String())
				continue
			}

			log.Debugf("query: %s index scan for %s", q.Name, v.Cond.String())

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
			log.Debugf("query: %s index scan found %d matches", q.Name, len(pkmatch))

			if len(pkmatch) == 0 {
				v.Cond.nomatch = true
				continue
			}

			// create new leaf node
			c := &Condition{
				Field:    q.table.Fields().Pk(), // primary key
				Mode:     FilterModeIn,          // IN
				Value:    pkmatch,               // list
				IsSorted: true,                  // already sorted by index lookup
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
				log.Debugf("query: %s replacing condition %s", q.Name, v.Cond.String())
				continue
			}
			ins = append(ins, v)
		}
		node.Children = ins
		log.Debug(newLogClosure(func() string {
			return "Updated query:\n" + q.Dump()
		}))
	}

	return nil
}

// INDEX QUERY: use index lookup for indexed fields
// - fetch pk lists for every indexed field
// - when resolved, replace source condition with new FilterModeIn condition
func (q *Query) QueryIndexes(ctx context.Context, tx *Tx) error {
	q.lap = time.Now()
	if q.NoIndex || q.Conditions.Empty() {
		q.stats.IndexTime = time.Since(q.lap)
		return nil
	}
	if err := q.queryIndexNode(ctx, tx, &q.Conditions); err != nil {
		return err
	}
	q.stats.IndexTime = time.Since(q.lap)
	return nil
}

// collect list of packs to visit in pk order
func (q *Query) MakePackSchedule(reverse bool) []int {
	schedule := make([]int, 0, q.table.packidx.Len())
	// walk list in pk order (pairs are always sorted by min pk)
	for _, p := range q.table.packidx.pos {
		if q.Conditions.MaybeMatchPack(q.table.packidx.packs[p]) {
			schedule = append(schedule, int(p))
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

func (q Query) And(conds ...UnboundCondition) Query {
	if len(conds) == 0 {
		return q
	}

	// create a new AND node to bind children
	node := ConditionTreeNode{
		OrKind:   COND_AND,
		Children: make([]ConditionTreeNode, 0),
	}

	// bind each unbound condition and add the new node element
	for _, v := range conds {
		node.AddNode(v.Bind(q.table))
	}

	// append to tree
	if q.Conditions.Empty() {
		q.Conditions.ReplaceNode(node)
	} else {
		q.Conditions.AddNode(node)
	}

	return q
}

func (q Query) Or(conds ...UnboundCondition) Query {
	if len(conds) == 0 {
		return q
	}

	// create a new OR node to bind children
	node := ConditionTreeNode{
		OrKind:   COND_OR,
		Children: make([]ConditionTreeNode, 0),
	}

	// bind each unbound condition and add to the new node element
	for _, v := range conds {
		node.AddNode(v.Bind(q.table))
	}

	// append to tree
	if q.Conditions.Empty() {
		q.Conditions.ReplaceNode(node)
	} else {
		q.Conditions.AddNode(node)
	}

	return q
}

func (q Query) AndCondition(field string, mode FilterMode, value interface{}) Query {
	q.Conditions.AddAndCondition(&Condition{
		Field: q.table.Fields().Find(field),
		Mode:  mode,
		Value: value,
	})
	return q
}

func (q Query) OrCondition(field string, mode FilterMode, value interface{}) Query {
	q.Conditions.AddOrCondition(&Condition{
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
	if q.Conditions.OrKind {
		q.Conditions = ConditionTreeNode{
			OrKind:   false,
			Children: []ConditionTreeNode{q.Conditions},
		}
	}
	q.Conditions.Children = append(q.Conditions.Children,
		ConditionTreeNode{
			Cond: &Condition{
				Field: q.table.Fields().Find(field),
				Mode:  FilterModeRange,
				From:  from,
				To:    to,
			},
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
