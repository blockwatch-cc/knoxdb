// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package stats

import (
	"fmt"

	"blockwatch.cc/knoxdb/internal/operator/filter"
	"blockwatch.cc/knoxdb/internal/types"
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

func (t *Tomb) Clone() *Tomb {
	return &Tomb{
		db:             t.db,
		epoch:          t.epoch,
		tkey:           t.tkey,
		ekey:           t.ekey,
		nSpackFields:   t.nSpackFields,
		activeFields:   t.activeFields,
		filteredFields: t.filteredFields,
		rangeFields:    t.rangeFields,
	}
}

func (t *Tomb) WithSchema(tableSchema, metaSchema *schema.Schema, use Features) *Tomb {
	t.nSpackFields = metaSchema.NumFields()
	t.activeFields = tableSchema.ActiveIds()
	for _, f := range tableSchema.Fields {
		switch f.Filter {
		case types.FL_BLOOM2B, types.FL_BLOOM3B,
			types.FL_BLOOM4B, types.FL_BLOOM5B:
			if use.Is(FeatBloomFilter) {
				t.filteredFields = append(t.filteredFields, f.Id)
			}
		case types.FL_BFUSE8, types.FL_BFUSE16:
			if use.Is(FeatFuseFilter) {
				t.filteredFields = append(t.filteredFields, f.Id)
			}
		case types.FL_BITS:
			if use.Is(FeatBitsFilter) {
				t.filteredFields = append(t.filteredFields, f.Id)
			}
		}
		if use.Is(FeatRangeFilter) && filter.ToValueType(f.Type).IsInt() {
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
	t.ekey = store.EncodeUvarint(uint64(v))
	return t
}

func (t *Tomb) Close() {
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
	var b [2 * store.MaxVarintLen32]byte
	buf := store.AppendUvarint(b[:0], uint64(key))
	buf = store.AppendUvarint(buf, uint64(ver))
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
	var tmp [2 * store.MaxVarintLen32]byte
	buf := store.AppendUvarint(tmp[:0], uint64(key))
	buf = store.AppendUvarint(buf, uint64(ver))
	b, err := t.bucket(tx, TOMB_KIND_TABLE_PACK)
	if err != nil {
		return err
	}
	return b.Put(buf, nil)
}

func (t *Tomb) bucket(tx store.Tx, kind byte) (store.Bucket, error) {
	tb, err := tx.Bucket(t.tkey)
	if err != nil {
		return nil, fmt.Errorf("tomb: %w", err)
	}
	eb, err := tb.Bucket(t.ekey)
	if err != nil {
		// fmt.Printf("Create tomb bucket %x for epoch %d\n", t.ekey, t.epoch)
		eb, err = tb.CreateBucket(t.ekey)
		if err != nil {
			return nil, fmt.Errorf("create epoch bucket: %v", err)
		}
	}
	var kindKey [1]byte
	kindKey[0] = kind
	kb, err := eb.Bucket(kindKey[:])
	if err != nil {
		// fmt.Printf("Create kind bucket %x for epoch %d\n", kind, t.epoch)
		kb, err = eb.CreateBucket(kindKey[:])
		if err != nil {
			return nil, fmt.Errorf("create kind bucket: %v", err)
		}
	}
	return kb, nil
}
