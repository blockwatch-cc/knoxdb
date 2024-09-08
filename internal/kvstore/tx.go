// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//go:build ignore
// +build ignore

package kvstore

import (
	"blockwatch.cc/knoxdb/internal/store"
)

func (kv *KVStore) view(fn func(*Tx) error) error {
	tx, err := kv.tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	return fn(tx)
}

func (kv *KVStore) update(fn func(*Tx) error) error {
	tx, err := kv.tx(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (kv *KVStore) tx(writeable bool) (*Tx, error) {
	tx, err := kv.db.Begin(writeable)
	if err != nil {
		return nil, err
	}
	x := &Tx{
		tx:       tx,
		db:       kv.db,
		pageFill: kv.opts.PageFill,
		maxSize:  kv.opts.TxMaxSize,
	}
	return x, nil
}

type Tx struct {
	tx       store.Tx
	db       store.DB
	pending  int
	size     int
	pageFill float64
	maxSize  int
}

func (t *Tx) Pending() int {
	return t.pending
}

func (t *Tx) Commit() error {
	return t.commit(true)
}

func (t *Tx) CommitAndContinue() error {
	return t.commit(false)
}

func (t *Tx) commit(stop bool) error {
	err := t.tx.Commit()
	if err != nil {
		return err
	}
	t.pending = 0
	t.size = 0
	t.tx = nil
	if !stop {
		t.tx, err = t.db.Begin(true)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *Tx) Rollback() error {
	if t.tx != nil {
		t.tx.Rollback()
	}
	t.pending = 0
	t.size = 0
	return nil
}

func (t *Tx) Bucket(key []byte) store.Bucket {
	return t.tx.Bucket(key)
}

func (t *Tx) Root() store.Bucket {
	return t.tx.Root()
}

func (t *Tx) CreateBucket(key []byte, ignoreExist bool) (store.Bucket, error) {
	b, err := t.Root().CreateBucket(key)
	if err == nil {
		return b, nil
	}
	if store.IsError(err, store.ErrBucketExists) && ignoreExist {
		return t.Bucket(key), nil
	}
	return nil, err
}
