// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package query

import (
	"context"
	"fmt"
	"time"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/operator/filter"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/xroar"
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
	Filters *filter.Node
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
		Filters: &filter.Node{},
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

func (p *QueryPlan) WithFilters(node *filter.Node) *QueryPlan {
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
	if p.Flags.IsDebug() {
		p.Log = l.Clone("Q:" + p.Tag)
	}
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
	return fmt.Errorf("[Q:"+p.Tag+"] "+s, vals...)
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
		if !p.Table.Schema().ContainsSchema(p.RequestSchema) {
			return p.Errorf("request schema: %v", schema.ErrSchemaMismatch)
		}
	}

	// check user-provided result schema
	if p.ResultSchema != nil {
		// result schema must contain pk (required for cursors, pack.LookupIterator)
		if p.ResultSchema.PkIndex() < 0 {
			return p.Errorf("result schema: %v", engine.ErrNoPk)
		}
		if !p.Table.Schema().ContainsSchema(p.ResultSchema) {
			return p.Errorf("result schema: %v", schema.ErrSchemaMismatch)
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
		p.Log.Debugf("original: %s", p)
	}

	// use tx snapshot if exists
	p.Snap = engine.GetSnapshot(ctx)
	hasMeta := p.Table.Schema().HasMeta()

	// extend filter from snapshot if table supports metadata
	// allow user override by setting an explicit request schema
	if p.RequestSchema == nil && p.Snap != nil && hasMeta {
		mc, err := And(
			// NEW records are visible when their xid committed before the
			// snapshot, i.e. either `xid < snap.xmin` or `xid !E snap.xact`.
			// Note when we allow concurrent tx we must also check each record's
			// snapshot visibility if `snap.Xmin <= $xmin < snap.Xmax`.
			//
			// $xmin < snap.xmax
			Lt("$xmin", p.Snap.Xmax),

			// DELETED records are still visible until their tombstones commit,
			// i.e. the tombstone was not created before the snapshot.
			// Note when we allow concurrent tx (xact != 0) we must also check
			// each record's snapshot visibility if `snap.Xmin <= $xmax < snap.Xmax`.
			//
			// $xmax == 0 || $xmax >= snap.xmax
			Or(Equal("$xmax", 0), Ge("$xmax", p.Snap.Xmax)),
		).Compile(p.Table.Schema())
		if err != nil {
			return p.Errorf("extend request filter: %v", err)
		}
		p.Filters.And(mc)
		if p.Flags.IsDebug() {
			p.Log.Debugf("extended: %s", p)
		}
	}

	// request at least the row_id field
	filterFieldIds := p.Filters.FieldIds()
	if hasMeta {
		filterFieldIds = append(filterFieldIds, schema.MetaRid)
	}
	filterFieldIds = slicex.Unique(filterFieldIds)

	// construct request schema
	if p.RequestSchema == nil {
		s, err := p.Table.Schema().SelectIds(filterFieldIds...)
		if err != nil {
			return p.Errorf("make request schema: %v", err)
		}
		p.RequestSchema = s.Sort().WithName(p.Tag)
	}

	// p.Log.Debugf("request schema %s", p.RequestSchema)

	// identify relevant indexes based on request schema fields
	for _, idx := range p.Table.Indexes() {
		// its sufficient to check the first indexed field only
		// this will select all single-field indexes and all
		// composite indexes where the first index field is used as
		// query condition (they may use prefix key matches)
		if !slicex.Contains(filterFieldIds, idx.IndexSchema().Fields[0].Id) {
			continue
		}
		p.Indexes = append(p.Indexes, idx)
	}

	// ensure result schema exists
	if p.ResultSchema == nil {
		p.ResultSchema = p.Table.Schema()
	}
	// p.Log.Debugf("result schema %s", p.ResultSchema)

	// optimize plan
	// - reorder filters
	// - combine filters
	// - remove ineffective filters
	p.Filters.Optimize()

	p.Stats.Tick("compile_time")

	// log optimized plan
	if p.Flags.IsDebug() {
		p.Log.Debugf("optimized: %s", p)
	}

	return nil
}

// INDEX QUERY: use index lookup for indexed fields and attach pk bitmaps
func (p *QueryPlan) QueryIndexes(ctx context.Context) error {
	if p.Flags.IsNoIndex() || p.Filters.IsProcessed() || len(p.Indexes) == 0 {
		return nil
	}
	origFieldIds := p.Filters.FieldIds()

	// Step 1: query indexes, attach bitmap results
	n, err := p.queryIndexNode(ctx, p.Filters)
	if err != nil {
		return err
	}
	p.Log.Debugf("%d index results", n)

	// prepare rowid field filter template
	ts := p.Table.Schema()
	tmpl := &filter.Filter{
		Name:  "$rid",
		Type:  types.BlockUint64,
		Index: ts.RowIdIndex(),
		Id:    schema.MetaRid,
	}

	// Step 2: add IN conditions from aggregate bits at each tree level
	// without index match this adds an always false condition for the rid field
	p.decorateIndexNodes(p.Filters, tmpl, true)
	p.Log.Debugf("Decorated %s", p.Filters)

	// Step 3: optimize by removing skip nodes and merge / simplify others
	p.Filters.Optimize()
	p.Log.Debugf("Optimized %s", p.Filters)

	// Step 4: adjust request schema (we may have to check less fields now,
	// but keep all meta fields), collect list of fields to drop (lists are sorted)
	drop := slicex.RemoveSorted(
		origFieldIds,
		slicex.Unique(append(p.Filters.FieldIds(), schema.MetaFieldIds...)),
	)
	if len(drop) > 0 {
		keep := slicex.Remove(p.RequestSchema.Ids(), drop)
		s, err := p.Table.Schema().SelectIds(keep...)
		if err != nil {
			return p.Errorf("update request schema: %v", err)
		}
		p.RequestSchema = s.Sort()
	}

	p.Stats.Tick("index_time")
	return nil
}

func (p *QueryPlan) decorateIndexNodes(node *filter.Node, tmpl *filter.Filter, isRoot bool) {
	// we only handle container nodes here because decoration adds
	// new conditions into the child list

	// special case: all conditions are processed during index scan
	// and all indexes are collision free
	if isRoot && node.IsProcessed() {
		// aggregate bitsets
		var bits *xroar.Bitmap
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

		// add a new rowid IN condition to root
		if bits.Count() == 0 {
			node.Children = append(node.Children, &filter.Node{
				Filter: tmpl.AsFalse(),
			})
		} else {
			node.Children = append(node.Children, &filter.Node{
				Filter: tmpl.AsSet(bits),
			})
		}

		// keep bits in root (for LSM tree query/scan logic)
		node.Bits = bits
		return
	}

	// common case, add RID IN condition to the current tree level
	for _, child := range node.Children {
		// single condition children
		if child.IsLeaf() {
			if child.Bits.IsValid() {
				if child.Bits.Count() == 0 {
					node.Children = append(node.Children, &filter.Node{
						Filter: tmpl.AsFalse(),
					})
				} else {
					node.Children = append(node.Children, &filter.Node{
						Filter: tmpl.AsSet(child.Bits),
					})
				}
			}
			continue
		}

		// composite child conditions attach bits to the common anchestor
		if child.Bits.IsValid() {
			if child.Bits.Count() == 0 {
				node.Children = append(node.Children, &filter.Node{
					Filter: tmpl.AsFalse(),
				})
			} else {
				node.Children = append(node.Children, &filter.Node{
					Filter: tmpl.AsSet(child.Bits),
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

func (p *QueryPlan) queryIndexNode(ctx context.Context, node *filter.Node) (int, error) {
	if node.OrKind {
		return p.queryIndexOr(ctx, node)
	} else {
		return p.queryIndexAnd(ctx, node)
	}
}

func (p *QueryPlan) queryIndexOr(ctx context.Context, node *filter.Node) (int, error) {
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
		node.Bits = bits
		node.Skip = !canCollide
	}

	return node.Bits.Count(), nil
}

func (p *QueryPlan) queryIndexAnd(ctx context.Context, node *filter.Node) (int, error) {
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
		node.Bits = bits
		node.Skip = !canCollide
		nHits += bits.Count()
		break
	}

	// TODO: push down extra pk conditions to index query
	// identify extra pk conditions for push down
	// var pkNodes []*filter.Node
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

		child.Bits = bits
		child.Skip = !canCollide
		nHits += bits.Count()
	}

	return nHits, nil
}

// Find an index compatible with a given filter node. This includes composite indexes.
// - index supports filter mode (EQ is ok, some cannot do LT/GT or IN style filters)
// - single field and composite key indexes must start with the filter field
func (p *QueryPlan) findIndex(node *filter.Node) (engine.QueryableIndex, bool) {
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
