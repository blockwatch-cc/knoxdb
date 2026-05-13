// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package store

import (
	"bytes"
	"iter"
	"unsafe"
)

// CommitAndContinue commits the current transaction and
// opens a new transaction of the same type. This is useful
// to batch commit large quantities of data in a loop.
func CommitAndContinue(tx Tx) (Tx, error) {
	db := tx.DB()
	iswrite := tx.IsWriteable()
	err := tx.Commit()
	if err != nil {
		return nil, err
	}
	if iswrite {
		return db.Begin(WithTxWrite())
	}
	return db.Begin()
}

// GetKey traverses a path of nested buckets and returns
// the value for the last key in the list. When a bucket
// is missing along the path or the key does not exist in
// the last bucket, an error is returned.
func GetKey(tx Tx, keys ...[]byte) ([]byte, error) {
	switch l := len(keys); l {
	case 0:
		return nil, ErrKeyRequired
	case 1:
		return nil, ErrIncompatibleValue
	default:
		b, err := GetBucket(tx, keys[:l-1]...)
		if err != nil {
			return nil, err
		}
		return b.Get(keys[l-1])
	}
}

// GetBucket traverses a path of nested buckets and returns
// the last bucket in the list. When a bucket is missig along
// the path an error is returned.
func GetBucket(tx Tx, keys ...[]byte) (Bucket, error) {
	if len(keys) == 0 {
		return nil, ErrKeyRequired
	}
	b, err := tx.Bucket(keys[0])
	if err != nil {
		return nil, err
	}
	for _, k := range keys[1:] {
		b, err = b.Bucket(k)
		if err != nil {
			return nil, err
		}
	}
	return b, nil
}

// PrefixRange returns key range that satisfies the given
// key prefix. The end key is exclusive and is calculated by
// adding 1 to the first non 0xFF byte at the end of prefix.
// If prefix is all 0xFF, the end key will be nil which
// indicates an unbounded range.
func PrefixRange(prefix []byte) ([]byte, []byte) {
	if prefix == nil {
		return nil, nil
	}
	var limit []byte
	for i := len(prefix) - 1; i >= 0; i-- {
		if c := prefix[i]; c < 0xff {
			limit = make([]byte, i+1)
			copy(limit, prefix)
			limit[i] = c + 1
			break
		}
	}
	return prefix, limit
}

// NextKey adds 1 to the key's binary representation which
// and returns a new key strictly higher than key.
func NextKey(key []byte) []byte {
	if len(key) == 0 {
		return []byte{0}
	}
	next := bytes.Clone(key)
	var i int
	for i = len(next) - 1; i >= 0; i-- {
		next[i]++
		if next[i] != 0 {
			break
		}
	}
	if i < 0 {
		// overflow, add extra byte in front
		next = append([]byte{1}, next...)
	}
	return next
}

// PrevKey subtracts 1 from the key's binary representation
// and returns a new key strictly lower than key.
func PrevKey(key []byte) []byte {
	if len(key) == 0 {
		return nil
	}
	prev := bytes.Clone(key)
	var i int
	for i = len(prev) - 1; i >= 0; i-- {
		prev[i]--
		if prev[i] != 0xff {
			break
		}
	}
	if i < 0 {
		// underflow, return nil
		return nil
	}
	return prev
}

// TrimKeyPrefix is an iterator wrapper that trims the first n
// bytes from a pair's key. It is use by iterator scans to reverse
// the effect of having an added bucket prefix on keys.
func TrimKeyPrefix(n int, seq iter.Seq2[[]byte, []byte]) iter.Seq2[[]byte, []byte] {
	return func(yield func([]byte, []byte) bool) {
		for k, v := range seq {
			if !yield(k[n:], v) {
				return
			}
		}
	}
}

func UnsafeBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

func UnsafeString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}
