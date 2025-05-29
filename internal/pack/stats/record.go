// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package stats

import (
	"encoding/binary"

	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/pkg/schema"
)

const (
	STATS_ROW_KEY = iota
	STATS_ROW_SCHEMA
	STATS_ROW_NVALS
	STATS_ROW_SIZE
)

// field usage in meta wire encoding
// Key: represents the min key across this subtree
// SchemaId: reused to count total number of data packs (i.e. stats rows)
// NValues: sum nvalues across all data packs, i.e. total table size
// DiskSize: sum disk sizes of all data packs, i.e. total table storage size
// data columns: min & max represent min/max over the subtree (inode) or stats pack (dnode)
type Record struct {
	Key      uint32 `knox:"key"`       // data pack key
	SchemaId uint64 `knox:"schema_id"` // data pack schema identifier
	NValues  int64  `knox:"n_values"`  // rows in data pack
	DiskSize int64  `knox:"disk_size"` // total data pack size on disk
	NColumns int64  `knox:"-"`         // data schema columns

	view *schema.View
}

var _ Reader = (*Record)(nil)

func NewRecordFromWire(s *schema.Schema, buf []byte) *Record {
	r := &Record{
		NColumns: int64(s.NumFields()-STATS_DATA_COL_OFFSET) / 2,
		view:     schema.NewView(s).Reset(buf),
	}
	if val, ok := r.view.Get(STATS_ROW_KEY); ok {
		r.Key = val.(uint32)
	}
	if val, ok := r.view.Get(STATS_ROW_SCHEMA); ok {
		r.SchemaId = val.(uint64)
	}
	if val, ok := r.view.Get(STATS_ROW_NVALS); ok {
		r.NValues = val.(int64)
	}
	if val, ok := r.view.Get(STATS_ROW_SIZE); ok {
		r.DiskSize = val.(int64)
	}
	return r
}

func (r *Record) MinMax(col int) (any, any) {
	minx, maxx := minColIndex(col), maxColIndex(col)
	minv, _ := r.view.Get(minx)
	maxv, _ := r.view.Get(maxx)
	return minv, maxv
}

func (r *Record) Min(col int) any {
	minv, _ := r.view.Get(minColIndex(col))
	return minv
}

func (r *Record) Max(col int) any {
	maxv, _ := r.view.Get(maxColIndex(col))
	return maxv
}

func (r Record) View() *schema.View {
	return r.view
}

func NewRecordFromPack(pkg *pack.Package, n int) *Record {
	s := MakeSchema(pkg.Schema())
	rec := &Record{
		Key:      pkg.Key(),
		SchemaId: pkg.Schema().Hash(),
		NValues:  int64(pkg.Len()),
		NColumns: int64(pkg.Schema().NumFields()),
		DiskSize: int64(n),
		view:     schema.NewView(s),
	}
	pstats := pkg.Stats()
	build := schema.NewBuilder(s, binary.LittleEndian)
	build.Write(STATS_ROW_KEY, pkg.Key())
	build.Write(STATS_ROW_SCHEMA, pkg.Schema().Hash())
	build.Write(STATS_ROW_NVALS, int64(pkg.Len()))
	build.Write(STATS_ROW_SIZE, pstats.SizeDiff())

	for i, b := range pkg.Blocks() {
		var minv, maxv any
		if b == nil {
			// use zero values for invalid blocks (deleted from schema)
			minv = b.Type().Zero()
			maxv = minv
		} else {
			// use min/max statistics
			minv = pstats.MinMax[i][0]
			maxv = pstats.MinMax[i][1]
		}

		// calculate data column positions inside statistics schema
		minx, maxx := minColIndex(i), maxColIndex(i)

		// append statistics values
		build.Write(minx, minv)
		build.Write(maxx, maxv)
	}
	rec.view.Reset(build.Bytes())
	build.Reset()
	return rec
}

func (r *Record) Update(pkg *pack.Package) {
	pstats := pkg.Stats()
	build := schema.NewBuilder(r.view.Schema(), binary.LittleEndian)
	build.Write(STATS_ROW_KEY, pkg.Key())
	build.Write(STATS_ROW_SCHEMA, pkg.Schema().Hash())
	build.Write(STATS_ROW_NVALS, int64(pkg.Len()))
	build.Write(STATS_ROW_SIZE, r.DiskSize+pstats.SizeDiff())

	for i, b := range pkg.Blocks() {
		// calculate data column positions inside statistics schema
		minx, maxx := minColIndex(i), maxColIndex(i)

		var minv, maxv any
		switch {
		case b == nil:
			// use zero values for invalid blocks (deleted from schema)
			minv = b.Type().Zero()
			maxv = minv
		case b.IsDirty():
			// use min/max statistics
			minv = pstats.MinMax[i][0]
			maxv = pstats.MinMax[i][1]
		default:
			// reuse existing values when block is not dirty
			minv, _ = r.view.Get(minx)
			maxv, _ = r.view.Get(maxx)
		}

		// append statistics values
		build.Write(minx, minv)
		build.Write(maxx, maxv)
	}
	r.view.Reset(build.Bytes())
	build.Reset()
}
