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

// TODO
// - EstimateCardinality for join planning
// - operator nesting/pipelining (filter, transform, aggregate)
// - schedule execution pipeline along operators (push model)
// - support aggregators
// - support group_by

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

type QueryPlan struct {
	Tag     string
	Filters *FilterTreeNode
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
		Log:     log.Disabled,
		Filters: &FilterTreeNode{},
		Stats:   NewQueryStats(),
	}
}

func (p *QueryPlan) Close() {
	p.Stats.Finalize()
	if p.Flags.IsStats() || p.Runtime() > QueryLogMinDuration {
		p.Log.Infof("Q> %s: %s", p.Tag, p.Stats)
	}
	p.Tag = ""
	p.Filters = nil
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

func (p *QueryPlan) WithFilters(node *FilterTreeNode) *QueryPlan {
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

func (p *QueryPlan) Schema() *schema.Schema {
	return p.ResultSchema
}

func (p *QueryPlan) Runtime() time.Duration {
	_, ok := p.Stats.runtime[TOTAL_TIME_KEY]
	if !ok {
		p.Stats.Finalize()
	}
	return p.Stats.runtime[TOTAL_TIME_KEY]
}

func (p *QueryPlan) Validate() error {
	// ensure table and filters are defined
	if p.Table == nil {
		return fmt.Errorf("query %s: %v", p.Tag, engine.ErrNoTable)
	}
	if p.Filters == nil {
		return fmt.Errorf("query %s: missing filters", p.Tag)
	}

	// filter tree must be valid
	if err := p.Filters.Validate(""); err != nil {
		return fmt.Errorf("query %s: %v", p.Tag, err)
	}

	// check user-provided request schema
	if p.RequestSchema != nil {
		// schemas must match table
		if err := p.Table.Schema().CanSelect(p.RequestSchema); err != nil {
			return fmt.Errorf("query %s: request schema: %v", p.Tag, err)
		}
	}

	// check user-provided result schema
	if p.ResultSchema != nil {
		// result schema must contain pk (required for cursors, pack.LookupIterator)
		if p.ResultSchema.PkIndex() < 0 {
			return fmt.Errorf("query %s: result schema: %v", p.Tag, engine.ErrNoPk)
		}
		if err := p.Table.Schema().CanSelect(p.ResultSchema); err != nil {
			return fmt.Errorf("query %s: result schema: %v", p.Tag, err)
		}
	}

	return nil
}

func (p *QueryPlan) Compile(ctx context.Context) error {
	// validate user data (some empty entries may be filled below)
	err := p.Validate()
	if err != nil {
		return err
	}

	// log incoming plan before compile
	if p.Flags.IsDebug() {
		p.Log.SetLevel(log.LevelDebug)
		p.Log.Debug(p.Dump())
	}

	filterFields := slicex.NewOrderedStrings(p.Filters.Fields())

	// ensure result schema is set
	if p.ResultSchema == nil {
		p.ResultSchema = p.Table.Schema()
	}

	// ensure request schema is set
	if p.RequestSchema == nil {
		if filterFields.Len() > 0 {
			s, err := p.Table.Schema().SelectFields(filterFields.Values...)
			if err != nil {
				return fmt.Errorf("query %s: make request schema: %v", p.Tag, err)
			}
			p.RequestSchema = s.Sort()
		} else {
			s, err := p.Table.Schema().SelectFieldIds(p.Table.Schema().PkId())
			if err != nil {
				return fmt.Errorf("query %s: make request schema: %v", p.Tag, err)
			}
			p.RequestSchema = s.WithName("pk")
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

	p.Log.Debugf("Q> %s: result %s", p.Tag, p.ResultSchema)

	// optimize plan
	// - [x] reorder filters
	// - [x] combine filters
	// - [ ] remove ineffective filters
	p.Filters.Optimize()

	p.Stats.Tick("compile_time")

	// wrap expensive call
	if p.Flags.IsDebug() {
		p.Log.Debug(p.Dump())
	}

	return nil
}

func (p *QueryPlan) EstimateCardinality(ctx context.Context) int64 {
	// TODO: ask tables
	return 0
}

func (p *QueryPlan) Stream(ctx context.Context, fn func(r engine.QueryRow) error) error {
	// query indexes first
	err := p.QueryIndexes(ctx)
	if err != nil {
		return err
	}

	// query table next
	return p.Table.Stream(ctx, p, fn)
}

func (p *QueryPlan) Query(ctx context.Context) (engine.QueryResult, error) {
	// TODO: ideally this becomes a push-based execution pipeline
	// at some point where index lookups are one step which forwards
	// bitmap as result

	// query indexes first
	err := p.QueryIndexes(ctx)
	if err != nil {
		return nil, err
	}

	// query table next
	return p.Table.Query(ctx, p)
}

// INDEX QUERY: use index lookup for indexed fields
//   - attaches pk bitmaps for every indexed field to relevant filter tree nodes
//   - pack/old: replaces matching condition with new FilterModeIn condition
//     or adds IN condition at front if index may have collisions
func (p *QueryPlan) QueryIndexes(ctx context.Context) error {
	if p.Flags.IsNoIndex() || p.Filters.IsEmpty() || p.Filters.IsProcessed() {
		return nil
	}

	// Step 1: query indexes, attach bitmap results
	n, err := p.queryIndexNode(ctx, p.Filters)
	if err != nil {
		return err
	}

	if n > 0 {
		// Step 2: add IN conditions from aggregate bits at each tree level
		p.decorateIndexNodes(p.Filters, true)

		// Step 3: optimize the tree by removing skip nodes and
		// merging / simplifying child nodes
		p.Filters.Optimize()

		// Step 4: adjust request schema (we may have to check less fields now)
		filterFields := slicex.NewOrderedStrings(p.Filters.Fields())
		requestFields := slicex.NewOrderedStrings(p.RequestSchema.FieldNames())
		if !filterFields.Equal(requestFields) && filterFields.Len() > 0 {
			s, err := p.Table.Schema().SelectFields(filterFields.Values...)
			if err != nil {
				return fmt.Errorf("query %s: remake request schema: %v", p.Tag, err)
			}
			p.RequestSchema = s.Sort()
		}
	}

	p.Stats.Tick("index_time")
	return nil
}

func (p *QueryPlan) decorateIndexNodes(node *FilterTreeNode, isRoot bool) {
	// we only handle container nodes here because decoration adds
	// new conditions into the child list

	// special case: all conditions are processed during index scan
	// and all indexes are collision free -> aggregate bitsets into root node
	if isRoot && node.IsProcessed() {
		node.Bits = bitmap.New()
		if node.OrKind {
			for _, child := range node.Children {
				if child.Bits.IsValid() {
					node.Bits.Or(child.Bits)
				}
			}
		} else {
			for _, child := range node.Children {
				if child.Bits.IsValid() {
					node.Bits.And(child.Bits)
				}
			}
		}
		return
	}

	// common case, add PK IN condition to the current tree level
	for _, child := range node.Children {
		// single condition children
		if child.IsLeaf() {
			if child.Bits.IsValid() {
				// add a new primary key IN condition
				matcher := NewFactory(types.FieldTypeUint64).New(FilterModeIn)
				matcher.WithSet(child.Bits.Bitmap)
				node.Children = append(node.Children, &FilterTreeNode{
					Filter: &Filter{
						Name:    child.Filter.Name,
						Type:    child.Filter.Type,
						Mode:    FilterModeIn,
						Index:   child.Filter.Index,
						Matcher: matcher,
						Value:   child.Bits,
					},
					Empty: child.Bits.Count() == 0,
				})
			}
			continue
		}

		// composite child conditions attach bits to the common anchestor
		if child.Bits.IsValid() {
			// add a new primary key IN condition
			matcher := NewFactory(types.FieldTypeUint64).New(FilterModeIn)
			matcher.WithSet(child.Bits.Bitmap)
			node.Children = append(node.Children, &FilterTreeNode{
				Filter: &Filter{
					Name:    p.RequestSchema.Pk().Name(),
					Type:    child.Filter.Type,
					Mode:    FilterModeIn,
					Index:   p.RequestSchema.PkId(),
					Matcher: matcher,
					Value:   child.Bits,
				},
				Empty: child.Bits.Count() == 0,
			})
			// continue below, we may need to visit unprocessed grandchildren
		}

		// recurse decorate child containers
		// (we do this even if we found a composite key index result above
		// because unrelated condition filters may still be unprocessed
		// inside the child tree)
		p.decorateIndexNodes(child, false)
	}
}

func (p *QueryPlan) queryIndexNode(ctx context.Context, node *FilterTreeNode) (int, error) {
	if node.OrKind {
		return p.queryIndexOr(ctx, node)
	} else {
		return p.queryIndexAnd(ctx, node)
	}
}

func (p *QueryPlan) queryIndexOr(ctx context.Context, node *FilterTreeNode) (int, error) {
	// 1  recurse into children one by one
	if !node.IsLeaf() {
		var nHits int
		for _, child := range node.Children {
			m, err := p.queryIndexNode(ctx, child)
			if err != nil {
				return 0, err
			}
			nHits += m
		}
		return nHits, nil
	}

	// 2  run index scan on leaf nodes

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
	// AND nodes may contain nested OR nodes which we need to visit first
	var nHits int
	for _, child := range node.Children {
		if child.OrKind {
			n, err := p.queryIndexNode(ctx, child)
			if err != nil {
				return 0, err
			}
			nHits += n
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
		nHits += bits.Count()
		break
	}

	// for all unprocessed child nodes, find a matching index and query independently
	for _, child := range node.Children {
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
		nHits += bits.Count()
	}

	return nHits, nil
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
