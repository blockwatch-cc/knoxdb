// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package stats

import (
	"fmt"

	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
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
	for _, f := range tableSchema.Exported() {
		switch f.Index {
		case types.IndexTypeBloom:
			if use.Is(FeatBloomFilter) {
				t.filteredFields = append(t.filteredFields, f.Id)
			}
		case types.IndexTypeBfuse:
			if use.Is(FeatFuseFilter) {
				t.filteredFields = append(t.filteredFields, f.Id)
			}
		case types.IndexTypeBits:
			if use.Is(FeatBitsFilter) {
				t.filteredFields = append(t.filteredFields, f.Id)
			}
		}
		if use.Is(FeatRangeFilter) && f.Type.BlockType().IsInt() {
			t.rangeFields = append(t.rangeFields, f.Id)
		}
	}
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
	t   *Tomb
	sb  store.Bucket // spack tombstones
	nb  store.Bucket // node tombstones
	err error        // deferred error
}

func (t *Tomb) NewWriter(tx store.Tx) *TombWriter {
	w := &TombWriter{
		t: t,
	}
	w.sb, w.err = t.bucket(tx, TOMB_KIND_STATS_PACK)
	if w.err == nil {
		w.nb, w.err = t.bucket(tx, TOMB_KIND_STATS_NODE)
	}
	return w
}

func (w *TombWriter) Close() {
	w.t = nil
	w.sb = nil
	w.nb = nil
}

func (w *TombWriter) AddSPack(tx store.Tx, key, ver uint32) error {
	if w.err != nil {
		return w.err
	}
	var b [2 * num.MaxVarintLen32]byte
	buf := num.AppendUvarint(b[:0], uint64(key))
	buf = num.AppendUvarint(buf, uint64(ver))
	return w.sb.Put(buf, nil)
}

func (w *TombWriter) AddNode(tx store.Tx, key []byte) error {
	if w.err != nil {
		return w.err
	}
	return w.nb.Put(key, nil)
}

func (t *Tomb) AddDataPack(tx store.Tx, key, ver uint32) error {
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
		return nil, fmt.Errorf("tomb: %v", store.ErrNoBucket)
	}
	eb := tb.Bucket(t.ekey)
	var err error
	if eb == nil {
		eb, err = tb.CreateBucketIfNotExists(t.ekey)
		if err != nil {
			return nil, fmt.Errorf("create epoch bucket: %v", err)
		}
	}
	kb := eb.Bucket([]byte{kind})
	if kb == nil {
		kb, err = tb.CreateBucketIfNotExists([]byte{kind})
		if err != nil {
			return nil, fmt.Errorf("create kind bucket: %v", err)
		}
	}
	kb.FillPercent(1.0)
	return kb, nil
}
