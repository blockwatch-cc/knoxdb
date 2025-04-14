// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package hashprobe

import (
	"sync"
)

type Integer interface {
	int8 | int16 | int32 | int64 | uint8 | uint16 | uint32 | uint64
}

const (
	MAX_DICT_SIZE  = 1 << 16 // 64k, 16-bit hashes
	MAX_DICT_LIMIT = 1 << 15 // 32k, max unique keys to build dict efficiently

	HASH_CONST = 0x9e3779b97f4a7c15
	HASH_MASK  = 0xFFFF
)

// Optimized hash table for integer dictionary construction.
// limited to up to 16k unique dictionary keys and source
// vectors of 64k elements.
type hashTable[T Integer] struct {
	keys  [MAX_DICT_SIZE]T      // up to 64k dictionary keys
	codes [MAX_DICT_SIZE]uint16 // up to 64k-1 dictionary code words, 0xFFFF as empty marker
}

func (t *hashTable[T]) Init() *hashTable[T] {
	// mark all codes unused
	t.codes[0] = HASH_MASK
	n := 1
	for n < MAX_DICT_SIZE {
		copy(t.codes[n:], t.codes[:n])
		n <<= 1
	}
	return t
}

// Prevent underallocation due to cardinality estimation errors (<25%).
// Arena allocation already rounds to next pow2.
func safeDictLen(n int) int {
	// increase size by error boundary (1/16th)
	n += n >> 4

	// round to multiples of 256
	return (n + 255) &^ 255
}

// Fast hash function (multiply prime) that extracts
// only the last 16 bits for use as hash table address.
func (t *hashTable[T]) hash16(key uint64) uint16 {
	return uint16((key * HASH_CONST) & HASH_MASK) // Prime constant, mask = size-1
}

type HTFactory struct {
	u64Pool sync.Pool
	u32Pool sync.Pool
	u16Pool sync.Pool // unused, tests only
	u8Pool  sync.Pool // unused, tests only
	i64Pool sync.Pool
	i32Pool sync.Pool
	i16Pool sync.Pool // unused, tests only
	i8Pool  sync.Pool // unused, tests only
}

var htFactory = HTFactory{
	u64Pool: sync.Pool{
		New: func() any { return &hashTable[uint64]{} },
	},
	u32Pool: sync.Pool{
		New: func() any { return &hashTable[uint32]{} },
	},
	u16Pool: sync.Pool{
		New: func() any { return &hashTable[uint16]{} },
	},
	u8Pool: sync.Pool{
		New: func() any { return &hashTable[uint8]{} },
	},
	i64Pool: sync.Pool{
		New: func() any { return &hashTable[int64]{} },
	},
	i32Pool: sync.Pool{
		New: func() any { return &hashTable[int32]{} },
	},
	i16Pool: sync.Pool{
		New: func() any { return &hashTable[int16]{} },
	},
	i8Pool: sync.Pool{
		New: func() any { return &hashTable[int8]{} },
	},
}

func allocHashTable[T Integer]() *hashTable[T] {
	switch any(T(0)).(type) {
	case uint64:
		return htFactory.u64Pool.Get().(*hashTable[T])
	case uint32:
		return htFactory.u32Pool.Get().(*hashTable[T])
	case uint16:
		return htFactory.u16Pool.Get().(*hashTable[T])
	case uint8:
		return htFactory.u8Pool.Get().(*hashTable[T])
	case int64:
		return htFactory.i64Pool.Get().(*hashTable[T])
	case int32:
		return htFactory.i32Pool.Get().(*hashTable[T])
	case int16:
		return htFactory.i16Pool.Get().(*hashTable[T])
	case int8:
		return htFactory.i8Pool.Get().(*hashTable[T])
	default:
		return nil
	}
}

func freeHashTable[T Integer](ht *hashTable[T]) {
	switch any(T(0)).(type) {
	case uint64:
		htFactory.u64Pool.Put(ht)
	case uint32:
		htFactory.u32Pool.Put(ht)
	case uint16:
		htFactory.u16Pool.Put(ht)
	case uint8:
		htFactory.u8Pool.Put(ht)
	case int64:
		htFactory.i64Pool.Put(ht)
	case int32:
		htFactory.i32Pool.Put(ht)
	case int16:
		htFactory.i16Pool.Put(ht)
	case int8:
		htFactory.i8Pool.Put(ht)
	}
}
