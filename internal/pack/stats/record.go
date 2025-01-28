// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package stats

import (
	"blockwatch.cc/knoxdb/internal/cmp"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/types"
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

	view *schema.View
}

func NewRecordFromWire(s *schema.Schema, buf []byte) *Record {
	r := &Record{
		view: schema.NewView(s).Reset(buf),
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

func (r Record) MinMax(col int) (any, any) {
	minx, maxx := minColIndex(col), maxColIndex(col)
	minv, _ := r.view.Get(minx)
	maxv, _ := r.view.Get(maxx)
	return minv, maxv
}

func NewRecordFromPack(s *schema.Schema, pkg *pack.Package) *Record {
	rec := &Record{
		Key:      pkg.Key(),
		SchemaId: pkg.Schema().Hash(),
		NValues:  int64(pkg.Len()),
		DiskSize: int64(0),
		view:     schema.NewView(s),
	}
	build := schema.NewBuilder(s)
	build.Write(STATS_ROW_KEY, pkg.Key())
	build.Write(STATS_ROW_SCHEMA, pkg.Schema().Hash())
	build.Write(STATS_ROW_NVALS, int64(pkg.Len()))
	build.Write(STATS_ROW_SIZE, int64(0)) // TODO: set disk size

	fields := pkg.Schema().Exported()
	for i, b := range pkg.Blocks() {
		var minv, maxv any
		if b == nil {
			// use zero values for invalid blocks (deleted from schema)
			minv = cmp.Zero(types.BlockTypes[fields[i].Type])
			maxv = minv
		} else {
			// calculate min/max statistics
			minv, maxv = b.MinMax()
		}

		// calculate data column positions inside statistics schema
		minx, maxx := minColIndex(i), maxColIndex(i)

		// append statistics values
		build.Write(minx, minv)
		build.Write(maxx, maxv)
	}
	rec.view.Reset(build.Bytes())

	return rec
}
