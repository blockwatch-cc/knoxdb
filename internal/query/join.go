// Copyright (c) 2018-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// TODO
// - complex predicates "JOIN ON a.f = b.f AND a.id = b.id"

package query

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"blockwatch.cc/knoxdb/internal/cmp"
	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/schema"
	"github.com/echa/log"
)

type JoinType = types.JoinType

type JoinOrder byte

const (
	JoinOrderNone JoinOrder = iota
	JoinOrderLeftRight
	JoinOrderRightleft
)

type JoinPlan struct {
	Tag   string
	Type  JoinType
	Mode  FilterMode
	Order JoinOrder
	Left  JoinTable
	Right JoinTable
	Limit uint32
	Log   log.Logger
	Flags QueryFlags
	Stats QueryStats
	Where *FilterTreeNode // optional post-processing filter on result

	schema *schema.Schema // result schema (mixed between tables, renamed fields)
	buf    *bytes.Buffer  // staging buffer for result merge
}

func NewJoinPlan() *JoinPlan {
	return &JoinPlan{
		Log:   log.Disabled,
		Stats: NewQueryStats(),
	}
}

func (p *JoinPlan) Close() {
	p.Finalize()
	if p.Flags.IsStats() || p.Runtime() > QueryLogMinDuration {
		p.Log.Infof("J> %s: %s", p.Tag, p.Stats)
	}
	p.Tag = ""
	p.Where = nil
	p.Log = nil
	p.schema = nil
	p.buf = nil
}

func (p *JoinPlan) Runtime() time.Duration {
	_, ok := p.Stats.runtime[TOTAL_TIME_KEY]
	if !ok {
		p.Finalize()
	}
	return p.Stats.runtime[TOTAL_TIME_KEY]
}

func (p *JoinPlan) Finalize() {
	// merge table query statistics
	p.Stats.Merge(&p.Left.Plan.Stats)
	p.Stats.Merge(&p.Right.Plan.Stats)
	p.Stats.Finalize()
}

func (p *JoinPlan) WithTag(tag string) *JoinPlan {
	p.Tag = tag
	return p
}

func (p *JoinPlan) WithFlags(f QueryFlags) *JoinPlan {
	p.Flags = f
	return p
}

func (p *JoinPlan) WithLimit(n uint32) *JoinPlan {
	p.Limit = n
	return p
}

func (p *JoinPlan) WithLogger(l log.Logger) *JoinPlan {
	p.Log = l.Clone()
	return p
}

func (p *JoinPlan) WithType(typ JoinType) *JoinPlan {
	p.Type = typ
	return p
}

func (p *JoinPlan) WithOrder(o JoinOrder) *JoinPlan {
	p.Order = o
	return p
}

func (p *JoinPlan) WithTables(l, r engine.TableEngine) *JoinPlan {
	p.Left.Table = l
	p.Right.Table = r
	return p
}

func (p *JoinPlan) WithFilters(l, r *FilterTreeNode) *JoinPlan {
	p.Left.Where = l
	p.Right.Where = r
	return p
}

func (p *JoinPlan) WithSelects(l, r *schema.Schema) *JoinPlan {
	p.Left.Select = l
	p.Right.Select = r
	return p
}

func (p *JoinPlan) WithAliases(l, r []string) *JoinPlan {
	p.Left.As = l
	p.Right.As = r
	return p
}

func (p *JoinPlan) WithLimits(l, r uint32) *JoinPlan {
	p.Left.Limit = l
	p.Right.Limit = r
	return p
}

func (p *JoinPlan) WithOn(f1, f2 *schema.Field, mode FilterMode) *JoinPlan {
	p.Left.On = f1
	p.Right.On = f2
	p.Mode = mode
	return p
}

type JoinTable struct {
	Table  engine.TableEngine
	Where  *FilterTreeNode // optional filter conditions for each table
	Select *schema.Schema  // target output schema (fields from each table)
	On     *schema.Field   // predicate
	Typ    types.BlockType // predicate plock type for cmp
	As     []string        // alias names of output fields, in order
	Limit  uint32          // individual table scan limit
	Plan   *QueryPlan      // executable query plan, used/updated stepwise
	Filter *Filter         // updatable query filter for each step
	PkId   uint16          // id of primary key field (0 == not exist)
}

func (j JoinTable) Validate(kind string) error {
	if j.Table == nil {
		return fmt.Errorf("nil %s table", kind)
	}
	if j.On == nil {
		return fmt.Errorf("missing %s field", kind)
	}
	if !j.On.IsValid() {
		return fmt.Errorf("invalid %s field '%s'", kind, j.On.Name())
	}

	// out schema is selectable
	s := j.Table.Schema()
	if j.Select == nil {
		return fmt.Errorf("missing select schema for table %s", s.Name())
	}
	if err := s.CanSelect(j.Select); err != nil {
		return fmt.Errorf("invalid select term for table %s: %v", s.Name(), err)
	}

	// join predicate fields are selected
	if f, ok := j.Select.FieldByName(j.On.Name()); !ok || f.Id() != j.On.Id() {
		return fmt.Errorf("predicate field %s.%s not selected", s.Name(), j.On.Name())
	}

	// table fields and alias list has same length
	if x, y := j.Select.NumFields(), len(j.As); x != y && y != 0 {
		return fmt.Errorf("mismatched aliases for table %s: %d fields, %d aliases", s.Name(), x, y)
	}

	// filter trees must be valid
	if err := j.Where.Validate(""); err != nil {
		return err
	}

	return nil
}

func (p *JoinPlan) Name() string {
	return strings.Join([]string{
		p.Left.Table.Schema().Name(),
		p.Type.String(),
		p.Right.Table.Schema().Name(),
		"on",
		p.Left.On.Name(),
		p.Mode.String(),
		p.Right.On.Name(),
	}, "_")
}

func (p *JoinPlan) Schema() *schema.Schema {
	return p.schema
}

func (p *JoinPlan) IsEquiJoin() bool {
	return p.Mode == FilterModeEqual
}

func (p *JoinPlan) Validate() error {
	// join type is valid
	if !p.Type.IsValid() {
		return fmt.Errorf("invalid join type %d", p.Type)
	}

	// join condition fields exist
	switch p.Mode {
	case FilterModeEqual, FilterModeNotEqual,
		FilterModeGt, FilterModeGe,
		FilterModeLt, FilterModeLe:
	default:
		return fmt.Errorf("unsupported join predicate mode '%s'", p.Mode)
	}

	// table setup is valid
	if err := p.Left.Validate("left"); err != nil {
		return err
	}
	if err := p.Right.Validate("right"); err != nil {
		return err
	}

	// join condition type matches
	if lt, rt := p.Left.On.Type(), p.Right.On.Type(); lt != rt {
		return fmt.Errorf("field type mismatch '%s'/'%s'", rt, lt)
	}

	return nil
}

// compile output field list and where conditions
func (p *JoinPlan) Compile(ctx context.Context) error {
	// run only once
	if p.schema != nil {
		return nil
	}

	// check consistency
	if err := p.Validate(); err != nil {
		return fmt.Errorf("join %s: %v", p.Tag, err)
	}

	// construct result schema
	ltab := p.Left.Table.Schema()
	rtab := p.Right.Table.Schema()
	p.schema = schema.NewSchema().WithName(p.Name())

	// set pk field (optimization) and remember block type
	p.Left.PkId = ltab.Pk().Id()
	p.Right.PkId = rtab.Pk().Id()
	p.Left.Typ = BlockTypes[p.Left.On.Type()]
	p.Right.Typ = BlockTypes[p.Right.On.Type()]

	// default names {table_name}.{field_name}
	for i, field := range p.Left.Select.Fields() {
		alias := p.Left.As[i]
		if alias == "" {
			alias = ltab.Name() + "." + field.Name()
		}
		p.schema.WithField(
			schema.NewField(field.Type()).
				WithName(alias).
				WithFixed(field.Fixed()).
				WithScale(field.Scale()).
				WithFlags(field.Flags()),
		)
	}

	for i, field := range p.Right.Select.Fields() {
		alias := p.Right.As[i]
		if alias == "" {
			alias = rtab.Name() + "." + field.Name()
		}
		p.schema.WithField(
			schema.NewField(field.Type()).
				WithName(alias).
				WithFixed(field.Fixed()).
				WithScale(field.Scale()).
				WithFlags(field.Flags()),
		)
	}

	// finalize result schema
	p.schema.Complete()

	// alloc staging buffer
	p.buf = bytes.NewBuffer(make([]byte, 0, p.schema.WireSize()))

	// construct and compile query plans, run index scans
	p.Left.Plan = NewQueryPlan().
		WithTag(p.schema.Name()).
		WithTable(p.Left.Table).
		WithSchema(p.Left.Select).
		WithFilters(p.Left.Where).
		WithLogger(p.Log)

	if err := p.Left.Plan.Compile(ctx); err != nil {
		return err
	}
	if err := p.Left.Plan.QueryIndexes(ctx); err != nil {
		return err
	}

	p.Right.Plan = NewQueryPlan().
		WithTag(p.schema.Name()).
		WithTable(p.Right.Table).
		WithSchema(p.Right.Select).
		WithFilters(p.Right.Where).
		WithLogger(p.Log)

	if err := p.Right.Plan.Compile(ctx); err != nil {
		return err
	}
	if err := p.Right.Plan.QueryIndexes(ctx); err != nil {
		return err
	}

	// identify join order when user-defined order is not set
	if p.Order == JoinOrderNone {
		// determine query order (L-R or R-L) based on
		// - join type
		//   - RIGHT: R first, then add IN cond to L
		//   - FULL: needs different algo design !!!
		// - table/filter cardinality and limits
		switch {
		case p.Type == types.RightJoin || p.Type == types.FullJoin:
			p.Order = JoinOrderLeftRight
		default:
			// estimate result set cardinality to define join order
			cardinalityLeft := p.Left.Plan.EstimateCardinality(ctx)
			cardinalityRight := p.Right.Plan.EstimateCardinality(ctx)
			p.Order = JoinOrderLeftRight
			if cardinalityLeft > cardinalityRight {
				p.Order = JoinOrderRightleft
			}
		}
	}

	// add an updateable primary key condition
	// - smaller side: IN (TODO: consider hash table to avoid repeat queries)
	// - larger side: GT on PK (i.e. cursor)
	var x, y *JoinTable
	switch p.Order {
	case JoinOrderLeftRight:
		x, y = &p.Left, &p.Right
	case JoinOrderRightleft:
		x, y = &p.Right, &p.Left
	}

	// pk cursor on large side
	pkField := x.Table.Schema().Pk()
	pkBlockTyp := BlockTypes[pkField.Type()]
	matcher := newFactory(pkBlockTyp).New(FilterModeGt)
	x.Filter = &Filter{
		Name:    pkField.Name(),
		Type:    pkBlockTyp,
		Mode:    FilterModeGt,
		Index:   pkField.Id() - 1,
		Matcher: matcher, // zero
		Value:   matcher.Value(),
	}
	x.Where.AddNode(&FilterTreeNode{Filter: x.Filter})

	// add limit to large side
	x.Plan.WithLimit(p.Limit)

	// IN condition for join predicate column on small side ONLY for equi-joins
	if p.IsEquiJoin() {
		joinField := y.On
		joinBlockType := BlockTypes[joinField.Type()]
		matcher = newFactory(joinBlockType).New(FilterModeIn)
		y.Filter = &Filter{
			Name:    joinField.Name(),
			Type:    joinBlockType,
			Mode:    FilterModeIn,
			Index:   joinField.Id() - 1,
			Matcher: matcher, // updated during processing
			Value:   nil,     // updated during processing
		}
		y.Where.AddNode(&FilterTreeNode{Filter: y.Filter})
	}

	return nil
}

func (p *JoinPlan) Stream(ctx context.Context, fn func(r engine.QueryRow) error) error {
	if err := p.Compile(ctx); err != nil {
		return err
	}

	res := NewStreamResult(p.schema, fn)

	err := p.doJoin(ctx, res)
	if err != nil && err != engine.EndStream {
		return err
	}

	return nil
}

func (p *JoinPlan) Query(ctx context.Context) (engine.QueryResult, error) {
	if err := p.Compile(ctx); err != nil {
		return nil, err
	}

	res := NewResult(p.schema, int(p.Limit))
	if err := p.doJoin(ctx, res); err != nil {
		if err != engine.EndStream {
			res.Close()
			return nil, err
		}
	}

	return res, nil
}

func (p *JoinPlan) doJoin(ctx context.Context, out QueryResultConsumer) error {
	// ------------------------------------------------------------
	// PREPARE
	// ------------------------------------------------------------

	// out is the final result to be returned, agg is an intermediate result
	// to collect potential candidate rows for post filtering
	var (
		agg  *Result
		wrap QueryResultConsumer
	)
	if p.Where != nil {
		agg = NewResult(p.schema, int(p.Limit))
		defer agg.Close()
		wrap = agg
	} else {
		// without post filter we can directly collect result rows into out
		wrap = out
	}

	// ------------------------------------------------------------
	// PROCESS
	// ------------------------------------------------------------
	// Algorithm description
	//
	// Fetches join candidates from both tables, joins and post-processes them.
	// To handle very large tables this algo iterates in blocks, fetching one pack
	// of candidate rows at a time and stops when the requested output row limit
	// is reached or a join operation did not produce any results. This also means
	// that underlying tables are potentially queried multiple times for one join
	// query to complete.

	var (
		lRes, rRes engine.QueryResult
		err        error
	)
	defer func() {
		lRes.Close()
		rRes.Close()
	}()

	// use row_id as an extra cursor to fetch a new block of matching rows
	for {
		// ------------------------------------------------------------
		// QUERY
		// ------------------------------------------------------------
		if p.Order == JoinOrderLeftRight {
			lRes, rRes, err = p.doQuery(ctx, p.Left, p.Right)
		} else {
			lRes, rRes, err = p.doQuery(ctx, p.Right, p.Left)
		}
		if err != nil {
			break
		}

		// exit when no more rows are found
		if lRes.Len() == 0 || rRes.Len() == 0 {
			p.Log.Debugf("J> %s: FINAL result with %d rows", p.Tag, out.Len())
			break
		}

		// ------------------------------------------------------------
		// JOIN
		// ------------------------------------------------------------
		// merge result sets
		switch p.Type {
		case types.InnerJoin:
			if p.IsEquiJoin() {
				err = mergeJoinInner(p, lRes, rRes, wrap)
			} else {
				err = loopJoinInner(p, lRes, rRes, wrap)
			}
		case types.LeftJoin:
			if p.IsEquiJoin() {
				err = mergeJoinLeft(p, lRes, rRes, wrap)
			} else {
				err = loopJoinLeft(p, lRes, rRes, wrap)
			}
		case types.RightJoin:
			if p.IsEquiJoin() {
				err = mergeJoinRight(p, lRes, rRes, wrap)
			} else {
				err = loopJoinRight(p, lRes, rRes, wrap)
			}
		case types.CrossJoin:
			err = loopJoinCross(p, lRes, rRes, wrap)
		// case types.FullJoin:
		//  // does not work with the loop algorithm above
		//  if p.IsEquiJoin() {
		//      n, err = mergeJoinFull(p, lRes, rRes, wrap)
		//  } else {
		//      n, err = loopJoinFull(p, lRes, rRes, wrap)
		//  }
		// case types.SelfJoin:
		// case types.AsOfJoin:
		// case types.WindowJoin:
		default:
			err = fmt.Errorf("join: type %s is not implemented", p.Type)
		}
		if err != nil {
			break
		}

		// close intermediate per-table results after join
		lRes.Close()
		rRes.Close()

		// ------------------------------------------------------------
		// POST-PROCESS
		// ------------------------------------------------------------
		if p.Where != nil {
			p.Log.Debugf("J> %s: post-filter %d result rows with: %s", p.Tag, agg.Len(), p.Where)

			// walk result and append
			var n uint32
			view := schema.NewView(p.schema)
			err = agg.ForEach(func(r engine.QueryRow) error {
				buf := r.Bytes()

				// result record must match conditions
				if !MatchTree(p.Where, view.Reset(buf)) {
					return nil
				}

				// forward to out (can zero-copy because row records are independnt allocs)
				if err := out.Append(buf, true); err != nil {
					return err
				}

				if p.Limit > 0 && n >= p.Limit {
					return engine.EndStream
				}
				return nil
			})
			if err != nil {
				break
			}
			agg.Reset()
		} else {
			if p.Limit > 0 && out.Len() >= int(p.Limit) {
				p.Log.Debugf("J> %s: FINAL result with limit %d", p.Tag, out.Len())
				break
			}
		}
	}

	return err
}

func (p *JoinPlan) doQuery(ctx context.Context, x, y JoinTable) (xRes engine.QueryResult, yRes engine.QueryResult, err error) {
	// fetch names once for debugging
	xname, yname := x.Table.Schema().Name(), y.Table.Schema().Name()

	// 1  query first side of the join
	if p.Flags.IsDebug() {
		p.Log.Debugf("J> %s: %s %s", p.Tag, xname, x.Plan.Dump())
	}

	// run query
	xRes, err = x.Table.Query(ctx, x.Plan)
	if err != nil {
		return
	}

	p.Log.Debugf("J> %s: %s result %d rows", p.Tag, xname, xRes.Len())

	// update pk cursor on first side
	var pk any
	pk, err = xRes.Row(xRes.Len() - 1).Index(int(x.PkId) - 1)
	if err != nil {
		err = fmt.Errorf("%s: missing pk column in %s query result: %v", p.Tag, xname, err)
		return
	}

	// update plan
	x.Filter.Matcher.WithValue(pk)
	// x.Filter.Value = pk

	// 2  query second side

	// equi-joins: override IN condition with matches from x
	if y.Filter != nil {
		predicateColumn, err2 := xRes.Column(x.On.Name())
		if err2 != nil {
			err = fmt.Errorf("%s: missing predicate column in %s query result: %v", p.Tag, xname, err)
			return
		}
		y.Filter.Matcher.WithSlice(predicateColumn)
		// y.Filter.Value = predicateColumn
	}

	if p.Flags.IsDebug() {
		p.Log.Debugf("J> %s: %s %s", p.Tag, yname, y.Plan.Dump())
	}

	// run query
	yRes, err = y.Table.Query(ctx, y.Plan)
	if err != nil {
		return
	}

	p.Log.Debugf("J> %s: %s result %d rows", p.Tag, yname, yRes.Len())
	return
}

func (p *JoinPlan) matchAt(a engine.QueryResult, ra int, b engine.QueryResult, rb int) bool {
	v1, _ := a.Row(ra).Index(int(p.Left.On.Id()) - 1)
	v2, _ := b.Row(rb).Index(int(p.Right.On.Id()) - 1)
	return cmp.Match(p.Mode, p.Left.Typ, v1, v2)
}

func (p *JoinPlan) compareAt(a engine.QueryResult, ra int, b engine.QueryResult, rb int) int {
	v1, _ := a.Row(ra).Index(int(p.Left.On.Id()) - 1)
	v2, _ := b.Row(rb).Index(int(p.Right.On.Id()) - 1)
	return cmp.Cmp(p.Left.Typ, v1, v2)
}

func (p *JoinPlan) appendResult(out QueryResultConsumer, left engine.QueryResult, l int, right engine.QueryResult, r int) error {
	// merge/append and forward into result type, when row number is negative
	// fill with zero value data
	p.buf.Reset()
	if l >= 0 {
		p.buf.Write(left.Record(l))
	} else {
		p.buf.Write(bytes.Repeat([]byte{0}, left.Schema().WireSize()))
	}
	if r >= 0 {
		p.buf.Write(right.Record(r))
	} else {
		p.buf.Write(bytes.Repeat([]byte{0}, right.Schema().WireSize()))
	}
	return out.Append(p.buf.Bytes(), false)
}

// non-equi joins
func loopJoinInner(p *JoinPlan, left, right engine.QueryResult, out QueryResultConsumer) error {
	// build cartesian product (O(n^2)) with
	p.Log.Debugf("J> %s: inner loop join on %d/%d rows", p.Tag, left.Len(), right.Len())
	for i, il := 0, left.Len(); i < il; i++ {
		for j, jl := 0, right.Len(); j < jl; j++ {
			if p.matchAt(left, i, right, j) {
				// merge result and append to out package
				if err := p.appendResult(out, left, i, right, j); err != nil {
					return err
				}
				// stop on limit
				if p.Limit > 0 && out.Len() == int(p.Limit) {
					return engine.EndStream
				}
			}
		}
	}
	return nil
}

// equi-joins only, |l| ~ |r| (close set sizes)
// TODO: never match NULL values (i.e. pkg.IsZeroAt(index,pos) == true)
func mergeJoinInner(p *JoinPlan, left, right engine.QueryResult, out QueryResultConsumer) error {
	// The algorithm works as follows
	//
	// for every left-side row find all matching right-side rows
	// and output a merged and transformed row for each such pair.
	//
	// each side may contain duplicate values for the join predicate,
	// that is, there may be multiple matching rows both in the left
	// and the right set and we're supposed to output the cartesian product.
	//
	// To efficiently support this case we keep the start and end of equal
	// right-side matches and roll back when we advance the left side.
	//
	// To further safe comparisons when we know we're inside a block match
	// we only match the left side against the start of a block.
	var (
		currBlockStart, currBlockEnd int
		haveBlockMatch               bool
		forceMatch                   bool
	)

	p.Log.Debugf("J> %s: inner merge join on %d/%d rows", p.Tag, left.Len(), right.Len())

	// sorted input is required
	//
	// sort left result by predicate column unless it's the primary key
	if !p.Left.On.Is(types.FieldFlagPrimary) {
		left.SortBy(p.Left.On.Name(), types.OrderAsc)
	}

	// sort right result by predicate column unless it's the primary key
	if !p.Right.On.Is(types.FieldFlagPrimary) {
		right.SortBy(p.Right.On.Name(), types.OrderAsc)
	}

	// loop until one result set is exhausted
	i, j, il, jl := 0, 0, left.Len(), right.Len()
	for i < il && j < jl {
		// OPTIMIZATION
		// once we have found a right-side block match of size > 1, we
		// only have to compare the next left-side value once and not
		// again for this block.
		var c int
		if !haveBlockMatch || forceMatch || j > currBlockEnd {
			c = p.compareAt(left, i, right, j)
			forceMatch = false
		}
		switch c {
		case -1:
			// l[i] < r[j]: no or no more matches for the left row
			// ->> advance left side (i)
			i++
			// reset right index to start of the right block in case the
			// next left row has the same join field value
			j = currBlockStart
			haveBlockMatch = currBlockEnd-currBlockStart > 1
			forceMatch = true
		case 1:
			// l[i] > r[j]: no or no more matches for the right value
			// ->> advance right side (j) behind end of block, update block start
			j = currBlockEnd + 1
			currBlockStart = j
			currBlockEnd = j
			haveBlockMatch = false
		case 0:
			// match, append merged result to out
			if err := p.appendResult(out, left, i, right, j); err != nil {
				return err
			}

			// stop on limit
			if p.Limit > 0 && out.Len() == int(p.Limit) {
				return engine.EndStream
			}

			// update indices
			if !haveBlockMatch {
				currBlockEnd = j
			}
			if j+1 < jl {
				// stay at current left pos and advance right pos if
				// we're not at the end of the right result set yet
				j++
			} else {
				// if we're at the end, try matching the next left side
				// result againts the current right-side block
				i++
				j = currBlockStart
				haveBlockMatch = currBlockEnd-currBlockStart > 1
				forceMatch = true
			}
		}
	}
	return nil
}

// equi-joins only, |l| << >> |r| (widely different set sizes)
func hashJoinInner(p *JoinPlan, left, right engine.QueryResult, out QueryResultConsumer) error {
	p.Log.Debugf("J> %s: inner hash join on %d/%d rows", p.Tag, left.Len(), right.Len())
	return engine.ErrNotImplemented
}

// TODO: never match NULL values (i.e. pkg.IsZeroAt(index,pos) == true)
func loopJoinLeft(p *JoinPlan, left, right engine.QueryResult, out QueryResultConsumer) error {
	p.Log.Debugf("J> %s: left loop join on %d/%d rows", p.Tag, left.Len(), right.Len())
	return engine.ErrNotImplemented
}

// TODO: never match NULL values (i.e. pkg.IsZeroAt(index,pos) == true)
func mergeJoinLeft(p *JoinPlan, left, right engine.QueryResult, out QueryResultConsumer) error {
	// The algorithm works as follows
	//
	// for every left-side row find all matching right-side rows
	// and output a merged and transformed row for each such pair.
	//
	// if no right side row matches (search stops when left[i] < right[j]),
	// output the left side with no matching right side (null or default values)
	//
	// each side may contain duplicate values for the join predicate,
	// that is, there may be multiple matching rows both in the left
	// and the right set and we're supposed to output the cartesian product.
	//
	// To efficiently support this case we keep the start and end of equal
	// right-side matches and roll back when we advance the left side.
	//
	// To further safe comparisons when we know we're inside a block match
	// we only match the left side against the start of a block.
	var (
		currBlockStart, currBlockEnd int
		wasMatch                     bool
	)

	p.Log.Debugf("J> %s: left merge join on %d/%d rows", p.Tag, left.Len(), right.Len())

	// sorted input is required
	//
	// sort left result by predicate column unless it's the primary key
	if !p.Left.On.Is(types.FieldFlagPrimary) {
		left.SortBy(p.Left.On.Name(), types.OrderAsc)
	}

	// sort right result by predicate column unless it's the primary key
	if !p.Right.On.Is(types.FieldFlagPrimary) {
		right.SortBy(p.Right.On.Name(), types.OrderAsc)
	}

	// loop until one result set is exhausted
	i, j, il, jl := 0, 0, left.Len(), right.Len()
	for i < il {
		switch p.compareAt(left, i, right, j) {
		case -1:
			// l[i] < r[j]: no or no more matches for the left row
			// ->> output left and advance left side (i)
			if !wasMatch {
				if err := p.appendResult(out, left, i, right, -1); err != nil {
					return err
				}
				if p.Limit > 0 && out.Len() == int(p.Limit) {
					return engine.EndStream
				}
			}
			i++
			j = currBlockStart
			wasMatch = false
		case 1:
			// l[i] > r[j]: no or no more matches for the right value
			// ->> advance right side (j) behind end of block, update block start
			if j+1 < jl {
				j = currBlockEnd + 1
				currBlockStart = j
				currBlockEnd = j
			} else {
				// without a match still output the left side
				if !wasMatch {
					if err := p.appendResult(out, left, i, right, -1); err != nil {
						return err
					}
					if p.Limit > 0 && out.Len() == int(p.Limit) {
						return engine.EndStream
					}
				}
				i++
			}
			wasMatch = false
		case 0:
			// match, append merged result to out
			if err := p.appendResult(out, left, i, right, j); err != nil {
				return err
			}
			if p.Limit > 0 && out.Len() == int(p.Limit) {
				return engine.EndStream
			}
			if j+1 < jl {
				// stay at current left pos and advance right pos if
				// we're not at the end of the right result set yet
				j++
				wasMatch = true
			} else {
				i++
				j = currBlockStart
				wasMatch = false
			}
		}
	}
	return nil
}

// TODO: need hash table to remember whether a row was joined already
// process inner join first, then add missing left, then missing right rows
func loopJoinRight(p *JoinPlan, left, right engine.QueryResult, out QueryResultConsumer) error {
	p.Log.Debugf("J> %s: right loop join on %d/%d rows", p.Tag, left.Len(), right.Len())
	return engine.ErrNotImplemented
}

func mergeJoinRight(p *JoinPlan, left, right engine.QueryResult, out QueryResultConsumer) error {
	p.Log.Debugf("J> %s: right merge join on %d/%d rows", p.Tag, left.Len(), right.Len())
	return engine.ErrNotImplemented
}

func loopJoinFull(p *JoinPlan, left, right engine.QueryResult, out QueryResultConsumer) error {
	p.Log.Debugf("J> %s: full loop join on %d/%d rows", p.Tag, left.Len(), right.Len())
	return engine.ErrNotImplemented
}

func mergeJoinFull(p *JoinPlan, left, right engine.QueryResult, out QueryResultConsumer) error {
	p.Log.Debugf("J> %s: full join on %d/%d rows using merge", p.Tag, left.Len(), right.Len())
	return engine.ErrNotImplemented
}

func loopJoinCross(p *JoinPlan, left, right engine.QueryResult, out QueryResultConsumer) error {
	p.Log.Debugf("J> %s: cross loop join on %d/%d rows", p.Tag, left.Len(), right.Len())
	// build cartesian product (O(n^2))
	for i, il := 0, left.Len(); i < il; i++ {
		for j, jl := 0, right.Len(); j < jl; j++ {
			// merge result and append to out package
			if err := p.appendResult(out, left, i, right, j); err != nil {
				return err
			}
			if p.Limit > 0 && out.Len() == int(p.Limit) {
				return engine.EndStream
			}
		}
	}
	return nil
}
