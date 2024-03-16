// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"

	"blockwatch.cc/knoxdb/encoding/bitmap"
	"blockwatch.cc/knoxdb/store"
)

type KeyValueIndex struct {
	name   string    // printable index name
	key    []byte    // name of index data bucket
	fields FieldList // list of field names and types
	idxs   []int     // list of field index positions in original data
	table  Table     // related table
}

func CreateKeyValueIndex(t Table, kind IndexKind, fields FieldList, _ Options) (*KeyValueIndex, error) {
	log.Debugf("Creating %s kv index %s_%s", kind, t.Name(), fields.String())

	// generate unique index name
	idx := &KeyValueIndex{
		name:   fields.String(),
		key:    []byte(fmt.Sprintf("%s_index_%016x", t.Name(), fields.Hash())),
		fields: fields,
		idxs:   make([]int, len(fields)),
		table:  t,
	}
	for i, f := range fields {
		orig := t.Fields().Find(f.Name)
		if orig == nil {
			return nil, fmt.Errorf("pack: %s missing field %q referenced by index %s", t.Name(), f.Name, fields)
		}
		idx.idxs[i] = orig.Index
	}

	err := t.DB().Update(func(tx *Tx) error {
		// check if index exists
		data := tx.Bucket(idx.key)
		if data != nil {
			return ErrIndexExists
		}

		// create index bucket
		_, err := tx.Root().CreateBucketIfNotExists(idx.key)
		return err
	})
	if err != nil {
		return nil, err
	}

	return idx, nil
}

func OpenKeyValueIndex(t Table, kind IndexKind, fields FieldList, _ Options) (*KeyValueIndex, error) {
	log.Debugf("Opening %s kv index %s_%s", kind, t.Name(), fields.String())

	// generate unique index name
	idx := &KeyValueIndex{
		name:   fields.String(),
		key:    []byte(fmt.Sprintf("%s_index_%016x", t.Name(), fields.Hash())),
		fields: fields,
		idxs:   make([]int, len(fields)),
		table:  t,
	}
	for i, f := range fields {
		orig := t.Fields().Find(f.Name)
		if orig == nil {
			return nil, fmt.Errorf("pack: %s missing field %q referenced by index %s", t.Name(), f.Name, fields)
		}
		idx.idxs[i] = orig.Index
	}

	err := t.DB().View(func(tx *Tx) error {
		b := tx.Bucket(idx.key)
		if b == nil {
			return ErrNoIndex
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	log.Debugf("Opened %s kv index %s_%s", idx.Kind(), t.Name(), idx.name)
	return idx, nil
}

func (idx *KeyValueIndex) Drop() error {
	return idx.table.DB().Update(func(tx *Tx) error {
		return tx.Root().DeleteBucket(idx.key)
	})
}

func (idx KeyValueIndex) Name() string {
	return idx.name
}

func (idx KeyValueIndex) Kind() IndexKind {
	return IndexKindComposite
}

func (idx KeyValueIndex) MarshalJSON() ([]byte, error) {
	return json.Marshal(idx.fields)
}

func (idx *KeyValueIndex) UnmarshalJSON(buf []byte) error {
	return json.Unmarshal(buf, &idx.fields)
}

func (idx *KeyValueIndex) Key() []byte {
	return idx.key
}

func (idx *KeyValueIndex) Fields() FieldList {
	return idx.fields
}

func (idx *KeyValueIndex) AddTx(tx *Tx, prev, val []byte) error {
	pkey := idx.table.Fields().CopyData(idx.idxs, prev)
	vkey := idx.table.Fields().CopyData(idx.idxs, val)
	sameKey := bytes.Equal(pkey, vkey)
	if pkey != nil && !sameKey {
		// log.Infof("Idx %s DEL %s", idx.name, hex.EncodeToString(pkey))
		_ = idx.delTx(tx, pkey)
	}
	if vkey != nil && !sameKey {
		// log.Infof("Idx %s ADD %s", idx.name, hex.EncodeToString(vkey))
		return idx.putTx(tx, vkey)
	}
	return nil
}

func (idx *KeyValueIndex) DelTx(tx *Tx, prev []byte) error {
	pkey := idx.table.Fields().CopyData(idx.idxs, prev)
	if pkey != nil {
		// log.Infof("Idx %s DEL %s", idx.name, hex.EncodeToString(pkey))
		return idx.delTx(tx, pkey)
	}
	return nil
}

func (idx *KeyValueIndex) putTx(tx *Tx, key []byte) error {
	data := tx.Bucket(idx.key)
	return data.Put(key, nil)
}

func (idx *KeyValueIndex) delTx(tx *Tx, key []byte) error {
	data := tx.Bucket(idx.key)
	return data.Delete(key)
}

func (idx *KeyValueIndex) Scan(prefix []byte) (bitmap.Bitmap, error) {
	bits := bitmap.New()
	err := idx.table.DB().View(func(tx *Tx) error {
		b := tx.Bucket(idx.key)
		if b == nil {
			return ErrBucketNotFound
		}
		// log.Infof("Idx %s SCAN %s", idx.name, hex.EncodeToString(prefix))
		c := b.Range(prefix, store.IndexCursor)
		defer c.Close()
		for ok := c.First(); ok; ok = c.Next() {
			// assumes pk is last 8 bytes of key
			key := c.Key()
			u64 := binary.BigEndian.Uint64(key[len(key)-8:])
			bits.Set(u64)
			// log.Infof("> found PK %016x", u64)
		}
		return nil
	})
	return bits, err
}

func (idx *KeyValueIndex) ScanTx(tx *Tx, prefix []byte) (bitmap.Bitmap, error) {
	bits := bitmap.New()
	b := tx.Bucket(idx.key)
	if b == nil {
		return bits, ErrBucketNotFound
	}
	// log.Infof("Idx %s SCAN %s", idx.name, hex.EncodeToString(prefix))
	c := b.Range(prefix, store.IndexCursor)
	defer c.Close()
	for ok := c.First(); ok; ok = c.Next() {
		// assumes pk is last 8 bytes of key
		key := c.Key()
		u64 := binary.BigEndian.Uint64(key[len(key)-8:])
		bits.Set(u64)
		// log.Infof("> found PK %016x", u64)
	}
	return bits, nil
}

func (idx *KeyValueIndex) RangeTx(tx *Tx, from, to []byte) (bitmap.Bitmap, error) {
	bits := bitmap.New()
	b := tx.Bucket(idx.key)
	if b == nil {
		return bits, ErrBucketNotFound
	}
	c := b.Cursor(store.IndexCursor)
	defer c.Close()
	for ok := c.Seek(from); ok && bytes.Compare(c.Key(), to) < 0; ok = c.Next() {
		// assumes pk is last 8 bytes of key
		key := c.Key()
		u64 := binary.BigEndian.Uint64(key[len(key)-8:])
		bits.Set(u64)
	}
	return bits, nil
}
