// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package query

import (
	"context"
	"fmt"
	"time"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/bitmap"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/slicex"
	"github.com/echa/log"
)

type OrderType = types.OrderType

type QueryFlags byte

const (
	QueryFlagNoCache QueryFlags = 1 << iota
	QueryFlagNoIndex
	QueryFlagDebug
	QueryFlagStats
)

func (f QueryFlags) IsNoCache() bool { return f&QueryFlagNoCache > 0 }
func (f QueryFlags) IsNoIndex() bool { return f&QueryFlagNoIndex > 0 }
func (f QueryFlags) IsDebug() bool   { return f&QueryFlagDebug > 0 }
func (f QueryFlags) IsStats() bool   { return f&QueryFlagStats > 0 }

var QueryLogMinDuration time.Duration = 500 * time.Millisecond

// TODO: decide if we want to use bitmaps or replace filters in the tree
//
// how to use the Bits field going forward?
// some AND nodes may not be covered by index scans but when others are
// we have Bits set on the collection node - how to use index scan
// results in table scans (update pack table scan algo, review lsm).

// TODO
// - use nested Operators (Filter, Transform, Aggregate) to express a query
// - define execution Pipeline along operators
// - use query planner to tansform the operator tree to minimize cost
// - support aggregators
// - support groupby

type QueryPlan struct {
	Tag     string
	Filters FilterTreeNode
	Limit   uint32
	Offset  uint32 // discouraged
	Order   OrderType
	Flags   QueryFlags

	// table and index refererences
	Table         engine.TableEngine   // table to query
	Indexes       []engine.IndexEngine // indexes to query
	RequestSchema *schema.Schema       // request schema (filter fields)
	ResultSchema  *schema.Schema       // result schema (output fields)

	// metrics and logging
	Log   log.Logger
	Stats QueryStats
}

func NewQueryPlan() *QueryPlan {
	return &QueryPlan{
		Log:   log.Disabled,
		Stats: NewQueryStats(),
	}
}

func (p *QueryPlan) Close() {
	p.Stats.Finalize()
	if p.Flags.IsStats() || p.Runtime() > QueryLogMinDuration {
		p.Log.Infof("Q> %s: %s", p.Tag, p.Stats)
	}
	p.Tag = ""
	p.Filters = FilterTreeNode{}
	p.Table = nil
	p.Indexes = nil
	p.ResultSchema = nil
	p.ResultSchema = nil
}

func (p *QueryPlan) WithTable(t engine.TableEngine) *QueryPlan {
	p.Table = t
	return p
}

func (p *QueryPlan) WithIndex(i engine.IndexEngine) *QueryPlan {
	p.Indexes = append(p.Indexes, i)
	return p
}

func (p *QueryPlan) WithTag(tag string) *QueryPlan {
	p.Tag = tag
	return p
}

func (p *QueryPlan) WithFlags(f QueryFlags) *QueryPlan {
	p.Flags = f
	return p
}

func (p *QueryPlan) WithFilters(node FilterTreeNode) *QueryPlan {
	p.Filters = node
	return p
}

func (p *QueryPlan) WithOrder(o OrderType) *QueryPlan {
	p.Order = o
	return p
}

func (p *QueryPlan) WithLimit(n uint32) *QueryPlan {
	p.Limit = n
	return p
}

func (p *QueryPlan) WithOffset(n uint32) *QueryPlan {
	p.Offset = n
	return p
}

func (p *QueryPlan) WithSchema(s *schema.Schema) *QueryPlan {
	p.ResultSchema = s
	return p
}

func (p *QueryPlan) WithLogger(l log.Logger) *QueryPlan {
	p.Log = l.Clone()
	return p
}

func (p *QueryPlan) IsEmptyMatch() bool {
	return p.Filters.IsEmptyMatch()
}

func (p *QueryPlan) Runtime() time.Duration {
	_, ok := p.Stats.runtime[TOTAL_TIME_KEY]
	if !ok {
		p.Stats.Finalize()
	}
	return p.Stats.runtime[TOTAL_TIME_KEY]
}

func (p *QueryPlan) Compile(ctx context.Context) error {
	// ensure table is defined
	if p.Table == nil {
		return fmt.Errorf("query %s: result schema: %v", p.Tag, engine.ErrNoTable)
	}

	// log
	if p.Flags.IsDebug() {
		p.Log.SetLevel(log.LevelDebug)
		p.Log.Debug(p.Dump())
	}

	filterFields := slicex.NewOrderedStrings(p.Filters.Fields())

	// ensure request schema is set
	if p.RequestSchema == nil {
		if filterFields.Len() > 0 {
			s, err := p.Table.Schema().SelectNames("", true, filterFields.Values...)
			if err != nil {
				return fmt.Errorf("query %s: make request schema: %v", p.Tag, err)
			}
			p.RequestSchema = s
		} else {
			p.RequestSchema, _ = p.Table.Schema().SelectIds("pk", false, p.Table.Schema().PkId())
		}
	}
	p.Log.Debugf("Q> %s: request %s", p.Tag, p.RequestSchema)

	// identify indexes based on request schema fields
	for _, idx := range p.Table.Indexes() {
		// its sufficient to check the first indexed field only
		// this will select all single-field indexes and all
		// composite indexes where the first index field is used as
		// query condition (they may use prefix key matches)
		idxFields := idx.Schema().FieldNames()
		if !filterFields.Contains(idxFields[0]) {
			continue
		}
		p.Indexes = append(p.Indexes, idx)
	}

	// validate plan
	//
	// result schema must contain pk (required for cursors, pack.LookupIterator)
	if p.ResultSchema.PkIndex() < 0 {
		return fmt.Errorf("query %s: result schema: %v", p.Tag, engine.ErrNoPk)
	}
	p.Log.Debugf("Q> %s: result %s", p.Tag, p.ResultSchema)

	// schemas must match table
	if err := p.Table.Schema().CanSelect(p.RequestSchema); err != nil {
		return fmt.Errorf("query %s: request schema: %v", p.Tag, err)
	}
	if err := p.Table.Schema().CanSelect(p.ResultSchema); err != nil {
		return fmt.Errorf("query %s: result schema: %v", p.Tag, err)
	}
	// filter tree must be valid
	if err := p.Filters.Validate(""); err != nil {
		return fmt.Errorf("query %s: %v", p.Tag, err)
	}

	// optimize plan
	// - [x] reorder filters
	// - [ ] combine filters
	// - [ ] remove ineffective filters
	p.Filters.Optimize()

	p.Stats.Tick("compile_time")

	// wrap expensive call
	if p.Flags.IsDebug() {
		p.Log.Debug(p.Dump())
	}

	return nil
}

func (p *QueryPlan) Execute(ctx context.Context) (engine.QueryResult, error) {
	// TODO: ideally this becomes a push-based execution pipeline
	// at some point where index lookups are one step which forwards
	// bitmap as result

	// query indexes first
	planChanged, err := p.QueryIndexes(ctx)
	if err != nil {
		return nil, err
	}

	// add or replace primary key conditions ?

	// optimize plan again
	if planChanged {
		p.Filters.Optimize()
	}

	// query table next
	return p.Table.Query(ctx, p)
}

// INDEX QUERY: use index lookup for indexed fields
//   - attaches pk bitmaps for every indexed field to relevant filter tree nodes
//   - pack/old: replaces matching condition with new FilterModeIn condition
//     or adds IN condition at front if index may have collisions
func (p *QueryPlan) QueryIndexes(ctx context.Context) (bool, error) {
	if p.Flags.IsNoIndex() || p.Filters.IsEmpty() {
		return false, nil
	}

	// query indexes and aggregate bitmap results
	n, err := p.queryIndexNode(ctx, &p.Filters)
	if err != nil {
		return false, err
	}

	p.Stats.Tick("index_time")
	return n > 0, nil
}

func (p *QueryPlan) queryIndexNode(ctx context.Context, node *FilterTreeNode) (int, error) {
	if node.OrKind {
		return p.queryIndexOr(ctx, node)
	} else {
		return p.queryIndexAnd(ctx, node)
	}
}

func (p *QueryPlan) queryIndexOr(ctx context.Context, node *FilterTreeNode) (int, error) {
	// nested nodes
	if !node.IsLeaf() {
		// 1/  recurse into children one by one
		for i := range node.Children {
			_, err := p.queryIndexNode(ctx, &node.Children[i])
			if err != nil {
				return 0, err
			}
		}

		// 2/  collect nested child bitmap results
		var (
			agg     bitmap.Bitmap
			canSkip bool = true
		)
		for _, child := range node.Children {
			if !child.Bits.IsValid() {
				canSkip = false
				continue
			}
			if agg.IsValid() {
				agg.Or(child.Bits)
			} else {
				agg = child.Bits.Clone()
			}
			canSkip = canSkip && child.Skip
		}

		// 3/ store result on node
		if agg.IsValid() {
			node.Bits = agg
			node.Skip = canSkip
		}
		return node.Bits.Count(), nil
	}

	// leaf nodes

	// convert EQ/IN primary key queries to bitset
	if p.Table.Schema().PkIndex() == int(node.Filter.Index) {
		switch node.Filter.Mode {
		case FilterModeEqual:
			node.Bits = bitmap.NewFromArray([]uint64{node.Filter.Value.(uint64)})
			return node.Bits.Count(), nil
		case FilterModeIn:
			node.Bits = bitmap.NewFromArray(node.Filter.Value.([]uint64))
			return node.Bits.Count(), nil
		}
	}

	// run index scan

	// find index that matches the filter condition
	idx, ok := p.findIndex(node)
	if !ok {
		return 0, nil
	}

	// query the index
	bits, canCollide, err := idx.Query(ctx, node)
	if err != nil {
		return 0, err
	}

	// update the filter condition with a valid bitset
	if bits != nil {
		node.Bits = *bits
		node.Skip = !canCollide
	}

	return node.Bits.Count(), nil
}

func (p *QueryPlan) queryIndexAnd(ctx context.Context, node *FilterTreeNode) (int, error) {
	// pre-process child nodes
	var nHits int
	for i := range node.Children {
		child := &node.Children[i]

		// AND nodes may contain nested OR nodes which we need to visit first
		if child.OrKind {
			n, err := p.queryIndexNode(ctx, child)
			if err != nil {
				return 0, err
			}
			nHits += n
		}

		// convert EQ/IN primary key queries to bitset
		if child.IsLeaf() {
			f := child.Filter
			if p.Table.Schema().PkIndex() == int(f.Index) {
				switch f.Mode {
				case FilterModeEqual:
					child.Bits = bitmap.NewFromArray([]uint64{f.Value.(uint64)})
					child.Skip = true
					nHits++
				case FilterModeIn:
					pks := f.Value.([]uint64)
					child.Bits = bitmap.NewFromArray(pks)
					child.Skip = true
					nHits += len(pks)
				}
			}
		}
	}

	// collect and aggregate nested OR child bitmap results
	if nHits > 0 {
		var (
			agg     bitmap.Bitmap
			canSkip bool = true
		)
		for i := range node.Children {
			child := &node.Children[i]
			if !child.Bits.IsValid() {
				canSkip = false
				continue
			}
			if agg.IsValid() {
				agg.And(child.Bits)
			} else {
				agg = child.Bits.Clone()
			}
			canSkip = canSkip && child.Skip
		}
		if agg.IsValid() {
			node.Bits = agg
			node.Skip = canSkip
			node.Empty = agg.Count() == 0

			// stop early when there were no results on at least one nested index scan
			// this means that later on we will continue to have no results in AND ops
			if agg.Count() == 0 {
				return 0, nil
			}
		}
	}

	// Check whether any index can match some/all of the child nodes
	// as composite key.
	// Note: The index will set child.Skip = true before actually
	// running the scan, in case of failure we cannot backtrack.
	// If we wanted to set Skip here we would have to know which
	// nodes/filters the index has actually touched.
	for _, idx := range p.Indexes {
		if !idx.IsComposite() {
			continue
		}

		if !idx.CanMatch(node) {
			continue
		}

		// try query index, we expect the index sets Skip on all used child nodes
		bits, canCollide, err := idx.QueryComposite(ctx, node)
		if err != nil {
			return 0, err
		}

		// indexes may return nil without error when they cannot match the query
		if bits == nil {
			continue
		}

		// stop on first hit
		node.Bits = *bits
		node.Skip = !canCollide
	}

	// for all unprocessed child nodes, find a matching index and query independently
	for i := range node.Children {
		child := &node.Children[i]

		// skip when index already processed this node
		if child.Skip || child.Bits.IsValid() || !child.IsLeaf() {
			continue
		}

		// find an index that matches the filter condition
		idx, ok := p.findIndex(child)
		if !ok {
			continue
		}

		// query the index
		bits, canCollide, err := idx.Query(ctx, child)
		if err != nil {
			return 0, err
		}

		if bits == nil {
			continue
		}

		child.Bits = *bits
		child.Skip = !canCollide
	}

	// Aggregate AND results
	// we can only skip if all AND conditions are marked
	var agg bitmap.Bitmap
	canSkip := true
	for _, child := range node.Children {
		if child.Bits.IsValid() {
			// aggregate results
			if agg.IsValid() {
				agg.And(child.Bits)
			} else {
				agg = child.Bits
			}
		}
		canSkip = canSkip && child.Skip
	}

	// store aggregate bitmap in node
	if agg.IsValid() {
		node.Bits = agg
		node.Skip = canSkip
		node.Empty = agg.Count() == 0
	}
	return node.Bits.Count(), nil
}

// Find an index compatible with a given filter node. This includes composite indexes.
// - index supports filter mode (EQ is ok, some cannot do LT/GT or IN style filters)
// - single field and composite key indexes must start with the filter field
func (p *QueryPlan) findIndex(node *FilterTreeNode) (engine.IndexEngine, bool) {
	for _, v := range p.Indexes {
		if !v.CanMatch(node) {
			continue
		}
		return v, true
	}
	return nil, false
}
