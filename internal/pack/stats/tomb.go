// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package stats

import (
	"fmt"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/store"
)

const (
	TOMB_KIND_TABLE_PACK byte = iota // table package tombstone
	TOMB_KIND_STATS_PACK             // snode package tombstone
	TOMB_KIND_STATS_NODE             // inode/snode tombstone
)

type Tomb struct {
	db             store.DB // storage reference
	epoch          uint32   // previous epoch
	tkey           []byte   // tomb bucket key
	ekey           []byte   // epoch key (for opening buckets)
	nSpackFields   int      // count of blocks in spacks (from meta schema, sequential ids)
	activeFields   []uint16 // list of all data block ids (from table schema)
	filteredFields []uint16 // list of data block ids with filters (from table schema)
	rangeFields    []uint16 // list of data block ids with range indices (from table schema)
}

func NewTomb() *Tomb {
	return &Tomb{}
}

func (t *Tomb) WithSchema(tableSchema, metaSchema *schema.Schema, use Features) *Tomb {
	t.nSpackFields = metaSchema.NumFields()
	t.activeFields = tableSchema.ActiveFieldIds()
	for _, f := range tableSchema.Fields {
		switch f.Filter {
		case types.FilterTypeBloom2b, types.FilterTypeBloom3b,
			types.FilterTypeBloom4b, types.FilterTypeBloom5b:
			if use.Is(FeatBloomFilter) {
				t.filteredFields = append(t.filteredFields, f.Id)
			}
		case types.FilterTypeBfuse8, types.FilterTypeBfuse16:
			if use.Is(FeatFuseFilter) {
				t.filteredFields = append(t.filteredFields, f.Id)
			}
		case types.FilterTypeBits:
			if use.Is(FeatBitsFilter) {
				t.filteredFields = append(t.filteredFields, f.Id)
			}
		}
		if use.Is(FeatRangeFilter) && f.Type.BlockType().IsInt() {
			t.rangeFields = append(t.rangeFields, f.Id)
		}
	}
	// fmt.Printf("Tomb %s active fields %v\n", t.tkey, t.activeFields)
	// fmt.Printf("Tomb %s filter fields %v\n", t.tkey, t.filteredFields)
	// fmt.Printf("Tomb %s range fields %v\n", t.tkey, t.rangeFields)
	return t
}

func (t *Tomb) WithBucketKey(key []byte) *Tomb {
	t.tkey = key
	return t
}

func (t *Tomb) WithDB(db store.DB) *Tomb {
	t.db = db
	return t
}

func (t *Tomb) WithEpoch(v uint32) *Tomb {
	t.epoch = v
	t.ekey = num.EncodeUvarint(uint64(v))
	return t
}

func (t *Tomb) Close() {
	t.db = nil
	*t = Tomb{}
}

type TombWriter struct {
	t  *Tomb
	sb store.Bucket // spack tombstones
	nb store.Bucket // node tombstones
}

func (t *Tomb) NewWriter(tx store.Tx) *TombWriter {
	return &TombWriter{t: t}
}

func (w *TombWriter) makeBuckets(tx store.Tx) error {
	if w.sb != nil {
		return nil
	}
	var err error
	w.sb, err = w.t.bucket(tx, TOMB_KIND_STATS_PACK)
	if err != nil {
		return err
	}
	w.nb, err = w.t.bucket(tx, TOMB_KIND_STATS_NODE)
	return err
}

func (w *TombWriter) Close() {
	w.t = nil
	w.sb = nil
	w.nb = nil
}

func (w *TombWriter) AddSPack(tx store.Tx, key, ver uint32) error {
	if err := w.makeBuckets(tx); err != nil {
		return err
	}
	// fmt.Printf("Add tomb spack %d[v%d] to epoch %d\n", key, ver, w.t.epoch)
	var b [2 * num.MaxVarintLen32]byte
	buf := num.AppendUvarint(b[:0], uint64(key))
	buf = num.AppendUvarint(buf, uint64(ver))
	return w.sb.Put(buf, nil)
}

func (w *TombWriter) AddNode(tx store.Tx, key []byte) error {
	if err := w.makeBuckets(tx); err != nil {
		return err
	}
	// fmt.Printf("Add tomb node %x to epoch %d\n", key, w.t.epoch)
	return w.nb.Put(key, nil)
}

func (t *Tomb) AddDataPack(tx store.Tx, key, ver uint32) error {
	// fmt.Printf("Add tomb data pack %d[v%d] to epoch %d\n", key, ver, t.epoch)
	var tmp [2 * num.MaxVarintLen32]byte
	buf := num.AppendUvarint(tmp[:0], uint64(key))
	buf = num.AppendUvarint(buf, uint64(ver))
	b, err := t.bucket(tx, TOMB_KIND_TABLE_PACK)
	if err != nil {
		return err
	}
	return b.Put(buf, nil)
}

func (t *Tomb) bucket(tx store.Tx, kind byte) (store.Bucket, error) {
	tb := tx.Bucket(t.tkey)
	if tb == nil {
		return nil, fmt.Errorf("tomb: %v", store.ErrBucketNotFound)
	}
	eb := tb.Bucket(t.ekey)
	var err error
	if eb == nil {
		// fmt.Printf("Create tomb bucket %x for epoch %d\n", t.ekey, t.epoch)
		eb, err = tb.CreateBucket(t.ekey)
		if err != nil {
			return nil, fmt.Errorf("create epoch bucket: %v", err)
		}
	}
	kb := eb.Bucket([]byte{kind})
	if kb == nil {
		// fmt.Printf("Create kind bucket %x for epoch %d\n", kind, t.epoch)
		kb, err = eb.CreateBucket([]byte{kind})
		if err != nil {
			return nil, fmt.Errorf("create kind bucket: %v", err)
		}
	}
	kb.FillPercent(1.0)
	return kb, nil
}
