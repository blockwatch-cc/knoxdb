// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package kvstore

import (
	"bytes"
	"context"
	"sync/atomic"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/wal"
)

func (kv *KVStore) Get(ctx context.Context, key []byte) ([]byte, error) {
	// check cache if key size is 8 (uint64)
	if len(key) == 8 {
		ckey := engine.NewCacheKey(kv.storeId, engine.Key64(key))
		buf, ok := kv.engine.BufferCache().Get(ckey)
		if ok {
			atomic.AddInt64(&kv.stats.CacheHits, 1)
			return buf.Bytes(), nil
		}
		atomic.AddInt64(&kv.stats.CacheMisses, 1)
	}

	tx, err := engine.GetTransaction(ctx).StoreTx(kv.db, false)
	if err != nil {
		return nil, err
	}
	bucket := tx.Bucket(kv.key)
	if bucket == nil {
		return nil, engine.ErrNoBucket
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
		ckey := engine.NewCacheKey(kv.storeId, engine.Key64(key))
		kv.engine.BufferCache().Add(ckey, engine.NewBuffer(buf))
	}
	atomic.AddInt64(&kv.stats.QueriedKeys, 1)
	atomic.AddInt64(&kv.stats.BytesRead, int64(len(buf)))

	return buf, nil
}

func (kv *KVStore) Put(ctx context.Context, key, val []byte) error {
	prevSize := -1
	tx, err := engine.GetTransaction(ctx).StoreTx(kv.db, true)
	if err != nil {
		return err
	}
	bucket := tx.Bucket(kv.key)
	if bucket == nil {
		return engine.ErrNoBucket
	}
	bucket.FillPercent(kv.opts.PageFill)
	buf := bucket.Get(key)
	if buf != nil {
		prevSize = len(buf)
	} else {
		// s.meta.Rows++
	}
	err = bucket.Put(key, val)
	if err != nil {
		return err
	}

	// use cache if key size is uint64
	if len(key) == 8 {
		ckey := engine.NewCacheKey(kv.storeId, engine.Key64(key))
		kv.engine.BufferCache().Add(ckey, engine.NewBuffer(bytes.Clone(val)))
		atomic.AddInt64(&kv.stats.CacheInserts, 1)
	}

	sz := int64(len(key) + len(val))
	if prevSize >= 0 {
		// update
		atomic.AddInt64(&kv.stats.UpdatedKeys, 1)
		atomic.AddInt64(&kv.stats.TotalSize, sz-int64(prevSize))
	} else {
		// insert
		atomic.AddInt64(&kv.stats.InsertedKeys, 1)
		atomic.AddInt64(&kv.stats.NumKeys, 1)
		atomic.AddInt64(&kv.stats.TotalSize, sz)
	}
	atomic.AddInt64(&kv.stats.BytesWritten, sz)
	return nil
}

func (kv *KVStore) Del(ctx context.Context, key []byte) error {
	prevSize := -1
	tx, err := engine.GetTransaction(ctx).StoreTx(kv.db, true)
	if err != nil {
		return err
	}
	bucket := tx.Bucket(kv.key)
	if bucket == nil {
		return engine.ErrNoBucket
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
		ckey := engine.NewCacheKey(kv.storeId, engine.Key64(key))
		kv.engine.BufferCache().Remove(ckey)
	}

	if prevSize >= 0 {
		atomic.AddInt64(&kv.stats.NumKeys, -1)
		atomic.AddInt64(&kv.stats.DeletedKeys, 1)
		atomic.AddInt64(&kv.stats.TotalSize, -int64(prevSize))
	}

	return nil
}

func (kv *KVStore) ApplyWalRecord(ctx context.Context, rec *wal.Record) error {
	return engine.ErrNotImplemented
}
