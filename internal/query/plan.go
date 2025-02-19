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

// Query Pipeline
// - simple filter & projection pipeline
// - uses index queries
// - optimizes filter conditions
// - no sort, join, aggregation handling
//
// TODO
// - optimize very large index matches (make optimizer use bitmap instead of []uint64)
// - ideally this becomes a push-based pipeline
// - optimize operator execution plan
// - EstimateCardinality for join planning
// - sort operators
// - join operators
// - aggregation operators
// - group_by operators

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
	Table         engine.QueryableTable   // table to query
	Indexes       []engine.QueryableIndex // indexes to query
	RequestSchema *schema.Schema          // request schema (filter fields)
	ResultSchema  *schema.Schema          // result schema (output fields)
	Snap          *types.Snapshot         // mvcc snapshot

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
		p.Log.Info(p.Stats)
	}
	p.Tag = ""
	p.Filters = nil
	p.Table = nil
	p.Indexes = nil
	p.RequestSchema = nil
	p.ResultSchema = nil
	p.Snap = nil
}

func (p *QueryPlan) WithTable(t engine.QueryableTable) *QueryPlan {
	p.Table = t
	return p
}

func (p *QueryPlan) WithIndex(i engine.QueryableIndex) *QueryPlan {
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
	p.Log = l.Clone().WithTag(p.Tag + " Q>")
	return p
}

func (p *QueryPlan) IsNoMatch() bool {
	return p.Filters.IsNoMatch()
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

func (p *QueryPlan) Errorf(s string, vals ...any) error {
	return fmt.Errorf("query "+p.Tag+": ", vals...)
}

func (p *QueryPlan) Error(err any) error {
	switch val := err.(type) {
	case string:
		return p.Errorf(val)
	default:
		return p.Errorf("%v", err)
	}
}

func (p *QueryPlan) Validate() error {
	// ensure table and filters are defined
	if p.Table == nil {
		return p.Error(engine.ErrNoTable)
	}
	if p.Filters == nil {
		return p.Error("missing filters")
	}

	// filter tree must be valid
	if err := p.Filters.Validate(""); err != nil {
		return p.Error(err)
	}

	// check user-provided request schema
	if p.RequestSchema != nil {
		// schemas must match table
		if err := p.Table.Schema().CanSelect(p.RequestSchema); err != nil {
			return p.Errorf("request schema: %v", err)
		}
	}

	// check user-provided result schema
	if p.ResultSchema != nil {
		// result schema must contain pk (required for cursors, pack.LookupIterator)
		if p.ResultSchema.PkIndex() < 0 {
			return p.Errorf("result schema: %v", engine.ErrNoPk)
		}
		if err := p.Table.Schema().CanSelect(p.ResultSchema); err != nil {
			return p.Errorf("result schema: %v", err)
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
		p.Log.Debug(p)
	}

	// use tx snapshot if exists
	p.Snap = engine.GetSnapshot(ctx)

	// extend filter from snapshot if table supports metadata
	// allow user override by setting an explicit request schema
	if p.RequestSchema == nil && p.Snap != nil && p.Table.Schema().HasMeta() {
		mc, err := And(
			// NEW records are visible when their writer committed before this tx started
			// Note when we allow concurrent tx we must also check each record
			// for snapshot visibility in case snap.Xmin <= $xmin < snap.Xmax
			//
			// $xmin < snap.xmax
			Lt("$xmin", p.Snap.Xmax),

			// DELETED records are still visible until their tombstones commit,
			// i.e. the tombstone was not created/merged before tx start
			// note when we allow concurrent tx (xact != 0) we must also check
			// each record for snapshot visibility when snap.Xmin <= $xmax < snap.Xmax
			//
			// $xmax == 0 || $xmax >= snap.xmax
			Or(Equal("$xmax", 0), Ge("$xmax", p.Snap.Xmax)),
		).Compile(p.Table.Schema())
		if err != nil {
			return p.Errorf("extend request filter: %v", err)
		}
		p.Filters.Children = append(p.Filters.Children, mc.Children...)
	}

	// request at least the primary key field
	filterFields := slicex.NewOrderedStrings(p.Filters.Fields())
	if filterFields.Len() == 0 {
		filterFields.Insert(p.Table.Schema().Pk().Name())
	}

	// construct request schema
	if p.RequestSchema == nil {
		s, err := p.Table.Schema().SelectFields(filterFields.Values...)
		if err != nil {
			return p.Errorf("make request schema: %v", err)
		}
		p.RequestSchema = s.Sort()
	}

	p.Log.Tracef("request %s", p.RequestSchema)

	// identify relevant indexes based on request schema fields
	for _, idx := range p.Table.Indexes() {
		// its sufficient to check the first indexed field only
		// this will select all single-field indexes and all
		// composite indexes where the first index field is used as
		// query condition (they may use prefix key matches)
		idxFirstField := idx.Schema().Fields()[0]
		if !filterFields.Contains(idxFirstField.Name()) {
			continue
		}
		p.Indexes = append(p.Indexes, idx)
	}

	// ensure result schema exists
	if p.ResultSchema == nil {
		p.ResultSchema = p.Table.Schema()
	}
	p.Log.Tracef("result %s", p.ResultSchema)

	// optimize plan
	// - reorder filters
	// - combine filters
	// - remove ineffective filters
	p.Filters.Optimize()

	p.Stats.Tick("compile_time")

	// log optimized plan
	if p.Flags.IsDebug() {
		p.Log.Debug(p)
	}

	return nil
}

// INDEX QUERY: use index lookup for indexed fields and attach pk bitmaps
func (p *QueryPlan) QueryIndexes(ctx context.Context) error {
	if p.Flags.IsNoIndex() || p.Filters.IsProcessed() {
		return nil
	}

	// Step 1: query indexes, attach bitmap results
	n, err := p.queryIndexNode(ctx, p.Filters)
	if err != nil {
		return err
	}
	p.Log.Debugf("%d index results", n)

	// prepare pk field filter template
	ts := p.Table.Schema()
	tmpl := &Filter{
		Name:  ts.Pk().Name(),
		Type:  BlockTypes[ts.Pk().Type()],
		Index: uint16(ts.PkIndex()),
	}

	// Step 2: add IN conditions from aggregate bits at each tree level
	// without index match this adds an always false condition for the pk field
	p.decorateIndexNodes(p.Filters, tmpl, true)
	p.Log.Debugf("Decorated %s", p.Filters)

	// Step 3: optimize by removing skip nodes and merge / simplify others
	p.Filters.Optimize()
	p.Log.Debugf("Optimized %s", p.Filters)

	// Step 4: adjust request schema (we may have to check less fields now)
	filterFields := slicex.NewOrderedStrings(p.Filters.Fields())
	requestFields := slicex.NewOrderedStrings(p.RequestSchema.ActiveFieldNames())
	if !filterFields.Equal(requestFields) && filterFields.Len() > 0 {
		s, err := p.Table.Schema().SelectFields(filterFields.Values...)
		if err != nil {
			return p.Errorf("update request schema: %v", err)
		}
		p.RequestSchema = s.Sort()
	}

	p.Stats.Tick("index_time")
	return nil
}

func (p *QueryPlan) decorateIndexNodes(node *FilterTreeNode, tmpl *Filter, isRoot bool) {
	// we only handle container nodes here because decoration adds
	// new conditions into the child list

	// special case: all conditions are processed during index scan
	// and all indexes are collision free
	if isRoot && node.IsProcessed() {
		// aggregate bitsets
		var bits bitmap.Bitmap
		if node.OrKind {
			for _, child := range node.Children {
				if !child.Bits.IsValid() {
					continue
				}
				if !bits.IsValid() {
					bits = child.Bits
				} else {
					bits.Or(child.Bits)
				}
			}
		} else {
			for _, child := range node.Children {
				if !child.Bits.IsValid() {
					continue
				}
				if !bits.IsValid() {
					bits = child.Bits
				} else {
					bits.And(child.Bits)
				}
			}
		}

		// add a new primary key IN condition to root
		if bits.Count() == 0 {
			node.Children = append(node.Children, &FilterTreeNode{
				Filter: makeFalseFilterFrom(tmpl),
			})
		} else {
			node.Children = append(node.Children, &FilterTreeNode{
				Filter: makeFilterFromSet(tmpl, bits.Bitmap),
			})
		}

		// keep bits in root (for LSM tree query/scan logic)
		node.Bits = bits
		return
	}

	// common case, add PK IN condition to the current tree level
	for _, child := range node.Children {
		// single condition children
		if child.IsLeaf() {
			if child.Bits.IsValid() {
				if child.Bits.Count() == 0 {
					node.Children = append(node.Children, &FilterTreeNode{
						Filter: makeFalseFilterFrom(tmpl),
					})
				} else {
					node.Children = append(node.Children, &FilterTreeNode{
						Filter: makeFilterFromSet(tmpl, child.Bits.Bitmap),
					})
				}
			}
			continue
		}

		// composite child conditions attach bits to the common anchestor
		if child.Bits.IsValid() {
			if child.Bits.Count() == 0 {
				node.Children = append(node.Children, &FilterTreeNode{
					Filter: makeFalseFilterFrom(tmpl),
				})
			} else {
				node.Children = append(node.Children, &FilterTreeNode{
					Filter: makeFilterFromSet(tmpl, child.Bits.Bitmap),
				})
			}
			// continue below, we may need to visit unprocessed grandchildren
		}

		// recurse decorate child containers
		// (we do this even if we found a composite key index result above
		// because unrelated condition filters may still be unprocessed
		// inside the child tree)
		p.decorateIndexNodes(child, tmpl, false)
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

		// try query index, we expect the index will sets node.Skip on visited nodes
		bits, canCollide, err := idx.QueryComposite(ctx, node)
		if err != nil {
			return 0, err
		}

		// indexes may return nil without error when they cannot match the query
		// but will return a non-nil (empty) bitset when no match was found
		if bits == nil {
			continue
		}

		// stop on first hit
		node.Bits = *bits
		node.Skip = !canCollide
		nHits += bits.Count()
		break
	}

	// TODO: push down extra pk conditions to index query
	// identify extra pk conditions for push down
	// var pkNodes []*FilterTreeNode
	// pki := p.Table.Schema().PkIndex()
	// for _, child := range node.Children {
	// 	if child.Skip || child.Bits.IsValid() || !child.IsLeaf() {
	// 		continue
	// 	}
	// 	if child.Filter.Index == uint16(pki) {
	// 		pkNodes = append(pkNodes, child)
	// 	}
	// }

	// for all unprocessed child nodes, find a matching index and query independently
	// push down extra pk field conditions
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
func (p *QueryPlan) findIndex(node *FilterTreeNode) (engine.QueryableIndex, bool) {
	for _, v := range p.Indexes {
		if !v.CanMatch(node) {
			continue
		}
		return v, true
	}
	return nil, false
}

func (p *QueryPlan) EstimateCardinality(ctx context.Context) int64 {
	// TODO: ask tables
	return 0
}
