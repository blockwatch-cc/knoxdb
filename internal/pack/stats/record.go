// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package stats

import (
	"encoding/binary"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/operator/filter"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/schema/reflect"
)

const (
	STATS_ROW_KEY     = iota // 0
	STATS_ROW_VERSION        // 1
	STATS_ROW_SCHEMA         // 2
	STATS_ROW_NVALS          // 3
	STATS_ROW_SIZE           // 4
)

// field usage in meta wire encoding
// Key: represents the min key across this subtree
// SchemaId: reused to count total number of data packs (i.e. stats rows)
// NValues: sum nvalues across all data packs, i.e. total table size
// DiskSize: sum disk sizes of all data packs, i.e. total table storage size
// data columns: min & max represent min/max over the subtree (inode) or stats pack (dnode)
type Record struct {
	Key      uint32 `knox:"key"`       // data pack key
	Version  uint32 `knox:"version"`   // data pack version
	SchemaId uint64 `knox:"schema_id"` // data pack schema identifier
	NValues  uint64 `knox:"n_values"`  // rows in data pack
	DiskSize int64  `knox:"disk_size"` // total data pack size on disk

	view *schema.View
}

var (
	// will use record schema for derived metadata schemas
	RecordSchema = reflect.MustSchemaFor[Record]()

	// buffers are written in little endian
	LE = binary.LittleEndian

	// ensure record implements stats reader interface
	_ engine.StatsReader = (*Record)(nil)
)

func NewRecordFromWire(s *schema.Schema, buf []byte) *Record {
	r := &Record{
		view: schema.NewView(s).Reset(buf),
	}
	if val := r.view.Get(STATS_ROW_KEY); val != nil {
		r.Key = val.(uint32)
	}
	if val := r.view.Get(STATS_ROW_VERSION); val != nil {
		r.Version = val.(uint32)
	}
	if val := r.view.Get(STATS_ROW_SCHEMA); val != nil {
		r.SchemaId = val.(uint64)
	}
	if val := r.view.Get(STATS_ROW_NVALS); val != nil {
		r.NValues = val.(uint64)
	}
	if val := r.view.Get(STATS_ROW_SIZE); val != nil {
		r.DiskSize = val.(int64)
	}
	return r
}

func (r *Record) MinMax(col int) (any, any) {
	return r.view.Get(minColIndex(col)), r.view.Get(maxColIndex(col))
}

func (r *Record) Min(col int) any {
	return r.view.Get(minColIndex(col))
}

func (r *Record) Max(col int) any {
	return r.view.Get(maxColIndex(col))
}

func (r Record) View() *schema.View {
	return r.view
}

func NewRecordFromPack(pkg *pack.Package, n int) *Record {
	s := MakeSchema(pkg.Schema())
	rec := &Record{
		Key:      pkg.Key(),
		Version:  pkg.Version(),
		SchemaId: pkg.Schema().Hash,
		NValues:  uint64(pkg.Len()),
		DiskSize: int64(n),
		view:     schema.NewView(s),
	}
	pstats := pkg.Stats()
	wr := s.NewBuffer(1)
	s.Fields[STATS_ROW_KEY].WriteValue(wr, rec.Key, LE)
	s.Fields[STATS_ROW_VERSION].WriteValue(wr, rec.Version, LE)
	s.Fields[STATS_ROW_SCHEMA].WriteValue(wr, rec.SchemaId, LE)
	s.Fields[STATS_ROW_NVALS].WriteValue(wr, rec.NValues, LE)
	s.Fields[STATS_ROW_SIZE].WriteValue(wr, rec.DiskSize+pstats.SizeDiff(), LE)

	for i, b := range pkg.Blocks() {
		var minv, maxv any
		if b == nil {
			// use zero values for invalid blocks (deleted from schema)
			minv = filter.ValueType(b.Type()).Zero()
			maxv = minv
		} else {
			// use min/max statistics
			minv = pstats.MinMax[i][0]
			maxv = pstats.MinMax[i][1]
		}

		// calculate data column positions inside statistics schema
		minx, maxx := minColIndex(i), maxColIndex(i)

		// append statistics values
		s.Fields[minx].WriteValue(wr, minv, LE)
		s.Fields[maxx].WriteValue(wr, maxv, LE)
	}
	rec.view.Reset(wr.Bytes())
	return rec
}
