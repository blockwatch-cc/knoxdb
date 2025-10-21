// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package kvstore

import (
	"bytes"
	"context"
	"sync/atomic"

	"blockwatch.cc/knoxdb/pkg/store"
	"blockwatch.cc/knoxdb/internal/types"
)

func (kv *KVStore) Range(ctx context.Context, prefix []byte, fn func(ctx context.Context, k, v []byte) error) error {
	tx, err := kv.db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	bucket := tx.Bucket(kv.key)
	if bucket == nil {
		return store.ErrBucketNotFound
	}
	c := bucket.Range(prefix)
	defer c.Close()
	for ok := c.First(); ok; ok = c.Next() {
		key, val := c.Key(), c.Value()
		atomic.AddInt64(&kv.metrics.QueriedKeys, 1)
		atomic.AddInt64(&kv.metrics.BytesRead, int64(len(val)))
		err = fn(ctx, key, val)
		if err != nil {
			break
		}
	}
	if err == nil || err == types.EndStream {
		return nil
	}
	return err
}

func (kv *KVStore) Scan(ctx context.Context, from, to []byte, fn func(ctx context.Context, k, v []byte) error) error {
	tx, err := kv.db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	bucket := tx.Bucket(kv.key)
	if bucket == nil {
		return store.ErrBucketNotFound
	}
	c := bucket.Cursor()
	defer c.Close()
	for ok := c.Seek(from); ok && bytes.Compare(c.Key(), to) <= 0; ok = c.Next() {
		key, val := c.Key(), c.Value()
		atomic.AddInt64(&kv.metrics.QueriedKeys, 1)
		atomic.AddInt64(&kv.metrics.BytesRead, int64(len(val)))
		err = fn(ctx, key, val)
		if err != nil {
			break
		}
	}
	if err == nil || err == types.EndStream {
		return nil
	}
	return err
}
