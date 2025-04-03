// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"math/bits"
	"sync"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/types"
)

var (
	BitLen64 = bits.Len64
)

func SizeOf[T types.Number]() int {
	return int(unsafe.Sizeof(T(0)))
}

func BlockType[T types.Number]() types.BlockType {
	switch any(T(0)).(type) {
	case uint64:
		return types.BlockUint64
	case int64:
		return types.BlockInt64
	case uint32:
		return types.BlockUint32
	case int32:
		return types.BlockInt32
	case uint16:
		return types.BlockUint16
	case int16:
		return types.BlockInt16
	case uint8:
		return types.BlockUint8
	case int8:
		return types.BlockInt8
	case float64:
		return types.BlockFloat64
	case float32:
		return types.BlockFloat32
	default:
		return types.BlockUint64
	}
}

type entry[T types.Integer] struct {
	key   T
	value uint16
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
		New: func() any { return make([]entry[uint64], 1<<16) },
	},
	u32Pool: sync.Pool{
		New: func() any { return make([]entry[uint32], 1<<16) },
	},
	u16Pool: sync.Pool{
		New: func() any { return make([]entry[uint16], 1<<16) },
	},
	u8Pool: sync.Pool{
		New: func() any { return make([]entry[uint8], 1<<16) },
	},
	i64Pool: sync.Pool{
		New: func() any { return make([]entry[int64], 1<<16) },
	},
	i32Pool: sync.Pool{
		New: func() any { return make([]entry[int32], 1<<16) },
	},
	i16Pool: sync.Pool{
		New: func() any { return make([]entry[int16], 1<<16) },
	},
	i8Pool: sync.Pool{
		New: func() any { return make([]entry[int8], 1<<16) },
	},
}

func allocHashTable[T types.Integer]() []entry[T] {
	switch any(T(0)).(type) {
	case uint64:
		return htFactory.u64Pool.Get().([]entry[T])[:1<<16]
	case uint32:
		return htFactory.u32Pool.Get().([]entry[T])[:1<<16]
	case uint16:
		return htFactory.u16Pool.Get().([]entry[T])[:1<<16]
	case uint8:
		return htFactory.u8Pool.Get().([]entry[T])[:1<<16]
	case int64:
		return htFactory.i64Pool.Get().([]entry[T])[:1<<16]
	case int32:
		return htFactory.i32Pool.Get().([]entry[T])[:1<<16]
	case int16:
		return htFactory.i16Pool.Get().([]entry[T])[:1<<16]
	case int8:
		return htFactory.i8Pool.Get().([]entry[T])[:1<<16]
	default:
		return nil
	}
}

func freeHashTable[T types.Integer](ht []entry[T]) {
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

func initHashTable[T types.Integer](table []entry[T]) {
	// Step 1: Fill a small chunk (e.g., 64 entries)
	chunkSize := 64
	for i := 0; i < chunkSize; i++ {
		table[i].value = 0xFFFF
	}

	// Step 2: Double the filled portion until full
	n := chunkSize
	for n < len(table) {
		copy(table[n:], table[:n])
		n <<= 1
	}
}
