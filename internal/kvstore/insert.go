// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package kvstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"sync/atomic"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/wal"
	"blockwatch.cc/knoxdb/pkg/store"
)

var BE = binary.BigEndian

func (kv *KVStore) Get(ctx context.Context, key []byte) ([]byte, error) {
	// check cache if key size is 8 (uint64)
	if len(key) == 8 {
		buf, ok := kv.engine.BufferCache(kv.storeId).Get(BE.Uint64(key))
		if ok {
			atomic.AddInt64(&kv.metrics.CacheHits, 1)
			return buf.Bytes(), nil
		}
		atomic.AddInt64(&kv.metrics.CacheMisses, 1)
	}

	tx, err := kv.db.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	bucket := tx.Bucket(kv.key)
	if bucket == nil {
		return nil, store.ErrBucketNotFound
	}
	buf := bucket.Get(key)
	if buf == nil {
		return nil, engine.ErrNoKey
	}

	// copy result
	if kv.isZeroCopy {
		buf = bytes.Clone(buf)
	}

	if len(key) == 8 {
		kv.engine.BufferCache(kv.storeId).Add(BE.Uint64(key), engine.NewBuffer(buf))
	}
	atomic.AddInt64(&kv.metrics.QueriedKeys, 1)
	atomic.AddInt64(&kv.metrics.BytesRead, int64(len(buf)))

	return buf, nil
}

func (kv *KVStore) Put(ctx context.Context, key, val []byte) error {
	prevSize := -1
	tx, err := kv.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	bucket := tx.Bucket(kv.key)
	if bucket == nil {
		return store.ErrBucketNotFound
	}
	bucket.FillPercent(kv.opts.PageFill)
	buf := bucket.Get(key)
	if buf != nil {
		prevSize = len(buf)
		// } else {
		// s.meta.Rows++
	}
	err = bucket.Put(key, val)
	if err != nil {
		return err
	}

	// use cache if key size is uint64
	if len(key) == 8 {
		kv.engine.BufferCache(kv.storeId).Add(BE.Uint64(key), engine.NewBuffer(bytes.Clone(val)))
		atomic.AddInt64(&kv.metrics.CacheInserts, 1)
	}

	sz := int64(len(key) + len(val))
	if prevSize >= 0 {
		// update
		atomic.AddInt64(&kv.metrics.UpdatedKeys, 1)
		atomic.AddInt64(&kv.metrics.TotalSize, sz-int64(prevSize))
	} else {
		// insert
		atomic.AddInt64(&kv.metrics.InsertedKeys, 1)
		atomic.AddInt64(&kv.metrics.NumKeys, 1)
		atomic.AddInt64(&kv.metrics.TotalSize, sz)
	}
	atomic.AddInt64(&kv.metrics.BytesWritten, sz)
	return tx.Commit()
}

func (kv *KVStore) Del(ctx context.Context, key []byte) error {
	prevSize := -1
	tx, err := kv.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	bucket := tx.Bucket(kv.key)
	if bucket == nil {
		return store.ErrBucketNotFound
	}
	buf := bucket.Get(key)
	if buf != nil {
		prevSize = len(buf)
		// s.meta.Rows--
	}
	err = bucket.Delete(key)
	if err != nil {
		return err
	}

	if len(key) == 8 {
		kv.engine.BufferCache(kv.storeId).Remove(BE.Uint64(key))
	}

	if prevSize >= 0 {
		atomic.AddInt64(&kv.metrics.NumKeys, -1)
		atomic.AddInt64(&kv.metrics.DeletedKeys, 1)
		atomic.AddInt64(&kv.metrics.TotalSize, -int64(prevSize))
	}

	return tx.Commit()
}

func (kv *KVStore) ApplyWalRecord(ctx context.Context, rec *wal.Record) error {
	return engine.ErrNotImplemented
}
