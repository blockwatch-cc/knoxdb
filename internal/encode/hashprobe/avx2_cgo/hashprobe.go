package main

import (
	"sync"
)

const (
	MAX_DICT_SIZE = 1 << 16 // 64k, 16-bit hashes
	MAX_DICT_KEYS = 1 << 14 // 16k, max unique keys to build dict efficiently
)

// Optimized hash table for integer dictionary construction.
// limited to up to 16k unique dictionary keys and source
// vectors of 64k elements.
type hashTable[T Integer] struct {
	keys  [MAX_DICT_SIZE]T      // up to 64k cardinality unique dictionary keys
	codes [MAX_DICT_SIZE]uint16 // up to 64k assigned dictionary code words, 0xFFFF as empty marker
}

type HTFactory struct {
	u64Pool sync.Pool
	u32Pool sync.Pool
	u16Pool sync.Pool
	u8Pool  sync.Pool
	i64Pool sync.Pool
	i32Pool sync.Pool
	i16Pool sync.Pool
	i8Pool  sync.Pool
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

// Fast hash function (multiply prime) that extracts
// only the last 16 bits for use as hash table address.
func (t *hashTable[T]) hash16(key uint64) uint16 {
	return uint16((key * 0x9e3779b97f4a7c15) & 0xFFFF) // Prime constant, mask = size-1
}

// func (table *hashTable[T]) initHashTable[T Integer]()*hashTable[T] {
// 	// alloc or reuse alocation
// 	if table.keys == nil {
// 		table.keys = AllocT[T](1 << 16)[:1<<16]
// 		table.codes = AllocT[uint16](1 << 16)[:1<<16]
// 		// table.hashes = AllocT[uint16](1 << 16)[:1<<16]
// 	}

// 	// if !util.UseAVX2 {
// 	// 	// Fill values
// 	// 	table.codes[0] = 0xFFFF
// 	// 	n := 1
// 	// 	for n < len(table.codes) {
// 	// 		copy(table.codes[n:], table.codes[:n])
// 	// 		n <<= 1
// 	// 	}
// 	// }
// }
