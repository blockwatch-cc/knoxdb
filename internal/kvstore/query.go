// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package kvstore

import (
	"bytes"
	"context"
	"sync/atomic"

	"blockwatch.cc/knoxdb/internal/engine"
)

func (kv *KVStore) Range(ctx context.Context, prefix []byte, fn func(ctx context.Context, k, v []byte) error) error {
	tx, err := engine.GetTransaction(ctx).StoreTx(kv.db, false)
	if err != nil {
		return err
	}
	bucket := tx.Bucket(kv.key)
	if bucket == nil {
		return engine.ErrNoBucket
	}
	c := bucket.Range(prefix)
	defer c.Close()
	for ok := c.First(); ok; ok = c.Next() {
		key, val := c.Key(), c.Value()
		atomic.AddInt64(&kv.stats.QueriedKeys, 1)
		atomic.AddInt64(&kv.stats.BytesRead, int64(len(val)))
		err = fn(ctx, key, val)
		if err != nil {
			break
		}
	}
	if err == nil || err == engine.EndStream {
		return nil
	}
	return err
}

func (kv *KVStore) Scan(ctx context.Context, from, to []byte, fn func(ctx context.Context, k, v []byte) error) error {
	tx, err := engine.GetTransaction(ctx).StoreTx(kv.db, false)
	if err != nil {
		return err
	}
	bucket := tx.Bucket(kv.key)
	if bucket == nil {
		return engine.ErrNoBucket
	}
	c := bucket.Cursor()
	defer c.Close()
	for ok := c.Seek(from); ok && bytes.Compare(c.Key(), to) <= 0; ok = c.Next() {
		key, val := c.Key(), c.Value()
		atomic.AddInt64(&kv.stats.QueriedKeys, 1)
		atomic.AddInt64(&kv.stats.BytesRead, int64(len(val)))
		err = fn(ctx, key, val)
		if err != nil {
			break
		}
	}
	if err == nil || err == engine.EndStream {
		return nil
	}
	return err
}
