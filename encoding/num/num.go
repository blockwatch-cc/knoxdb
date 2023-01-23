// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package num

import (
	"encoding/binary"
	"math"
	"reflect"
	"unsafe"

	"blockwatch.cc/knoxdb/hash/xxhash"
	"blockwatch.cc/knoxdb/vec"
)

var bigEndian = binary.BigEndian

type BlockType byte

const (
	BlockTypeTime    = BlockType(0)
	BlockTypeInt64   = BlockType(1)
	BlockTypeUint64  = BlockType(2)
	BlockTypeFloat64 = BlockType(3)
	BlockTypeBool    = BlockType(4)
	BlockTypeString  = BlockType(5)
	BlockTypeBytes   = BlockType(6)
	BlockTypeInt32   = BlockType(7)
	BlockTypeInt16   = BlockType(8)
	BlockTypeInt8    = BlockType(9)
	BlockTypeUint32  = BlockType(10)
	BlockTypeUint16  = BlockType(11)
	BlockTypeUint8   = BlockType(12)
	BlockTypeFloat32 = BlockType(13)
	BlockTypeInt128  = BlockType(14)
	BlockTypeInt256  = BlockType(15)
	BlockTypeInvalid = BlockType(16)
)

func (b *NumArray[N]) Type() BlockType {
	switch reflect.ValueOf(*new(N)).Kind() {
	case reflect.Int64:
		return BlockTypeInt64
	case reflect.Int32:
		return BlockTypeInt32
	case reflect.Int16:
		return BlockTypeInt16
	case reflect.Int8:
		return BlockTypeInt8
	case reflect.Uint64:
		return BlockTypeUint64
	case reflect.Uint32:
		return BlockTypeUint32
	case reflect.Uint16:
		return BlockTypeUint16
	case reflect.Uint8:
		return BlockTypeUint8
	case reflect.Float64:
		return BlockTypeFloat64
	case reflect.Float32:
		return BlockTypeFloat32
	}
	return BlockTypeInvalid
}

type Number interface {
	int8 | int16 | int32 | int64 | uint8 | uint16 | uint32 | uint64 | float32 | float64
}

type NumArray[T Number] struct {
	slice []T
}

func NewNumArray[T Number](sz int) *NumArray[T] {
	n := new(NumArray[T])
	n.slice = make([]T, sz)
	return n
}

func NewNumArrayFromSlice[T Number](slice []T) *NumArray[T] {
	n := new(NumArray[T])
	n.slice = slice
	return n
}

func (n *NumArray[T]) SetSlice(slice []T) *NumArray[T] {
	n.slice = slice
	return n
}

func (n *NumArray[T]) Len() int {
	return len(n.slice)
}

func (n *NumArray[T]) Cap() int {
	return cap(n.slice)
}

func (n *NumArray[T]) HeapSize() int {
	var sz = 24 // size of slice header
	sz += len(n.slice) * int(unsafe.Sizeof(new(T)))
	return sz
}

func (n *NumArray[T]) Clear() {
	n.slice = n.slice[:0]
}

func (n *NumArray[T]) Release() {
	n.slice = nil
}

func (n *NumArray[T]) Slice() []T {
	return n.slice
}

func (n *NumArray[T]) RangeSlice(start, end int) []T {
	return n.slice[start:end]
}

func (n *NumArray[T]) Elem(idx int) T {
	return n.slice[idx]
}

func (n *NumArray[T]) Set(i int, val T) {
	n.slice[i] = val
}

func (n *NumArray[T]) Swap(i, j int) {
	n.slice[i], n.slice[j] = n.slice[j], n.slice[i]
}

func (n *NumArray[T]) Grow(len int) {
	n.slice = append(n.slice, make([]T, len)...)
}

func (n *NumArray[T]) Append(val T) {
	n.slice = append(n.slice, val)
}

func (n *NumArray[T]) Delete(pos, len int) {
	n.slice = append(n.slice[:pos], n.slice[pos+len:]...)
}

func (n *NumArray[T]) Copy(src []T) {
	n.slice = n.slice[:len(src)]
	copy(n.slice, src)
}

func (n *NumArray[T]) AppendFrom(src []T, pos, len int) {
	n.slice = append(n.slice, src[pos:pos+len]...)
}

func (n *NumArray[T]) ReplaceFrom(src []T, spos, dpos, len int) {
	copy(n.slice[dpos:], src[spos:spos+len])
}

func (n *NumArray[T]) InsertFrom(src interface{}, spos, dpos, len int) {
	slice := src.([]T)
	switch n.Type() {
	case BlockTypeFloat64:
		n.slice = interface{}(vec.Float64.Insert(interface{}(n.slice).([]float64), dpos, interface{}(slice).([]float64)[spos:spos+len]...)).([]T)
	case BlockTypeFloat32:
		n.slice = interface{}(vec.Float32.Insert(interface{}(n.slice).([]float32), dpos, interface{}(slice).([]float32)[spos:spos+len]...)).([]T)
	case BlockTypeInt64:
		n.slice = interface{}(vec.Int64.Insert(interface{}(n.slice).([]int64), dpos, interface{}(slice).([]int64)[spos:spos+len]...)).([]T)
	case BlockTypeInt32:
		n.slice = interface{}(vec.Int32.Insert(interface{}(n.slice).([]int32), dpos, interface{}(slice).([]int32)[spos:spos+len]...)).([]T)
	case BlockTypeInt16:
		n.slice = interface{}(vec.Int16.Insert(interface{}(n.slice).([]int16), dpos, interface{}(slice).([]int16)[spos:spos+len]...)).([]T)
	case BlockTypeInt8:
		n.slice = interface{}(vec.Int8.Insert(interface{}(n.slice).([]int8), dpos, interface{}(slice).([]int8)[spos:spos+len]...)).([]T)
	case BlockTypeUint64:
		n.slice = interface{}(vec.Uint64.Insert(interface{}(n.slice).([]uint64), dpos, interface{}(slice).([]uint64)[spos:spos+len]...)).([]T)
	case BlockTypeUint32:
		n.slice = interface{}(vec.Uint32.Insert(interface{}(n.slice).([]uint32), dpos, interface{}(slice).([]uint32)[spos:spos+len]...)).([]T)
	case BlockTypeUint16:
		n.slice = interface{}(vec.Uint16.Insert(interface{}(n.slice).([]uint16), dpos, interface{}(slice).([]uint16)[spos:spos+len]...)).([]T)
	case BlockTypeUint8:
		n.slice = interface{}(vec.Uint8.Insert(interface{}(n.slice).([]uint8), dpos, interface{}(slice).([]uint8)[spos:spos+len]...)).([]T)
	}
}

func (n *NumArray[T]) MinMax() (interface{}, interface{}) {
	switch n.Type() {
	case BlockTypeFloat64:
		return vec.Float64.MinMax(interface{}(n.slice).([]float64))
	case BlockTypeFloat32:
		return vec.Float32.MinMax(interface{}(n.slice).([]float32))
	case BlockTypeInt64:
		return vec.Int64.MinMax(interface{}(n.slice).([]int64))
	case BlockTypeInt32:
		return vec.Int32.MinMax(interface{}(n.slice).([]int32))
	case BlockTypeInt16:
		return vec.Int16.MinMax(interface{}(n.slice).([]int16))
	case BlockTypeInt8:
		return vec.Int8.MinMax(interface{}(n.slice).([]int8))
	case BlockTypeUint64:
		return vec.Uint64.MinMax(interface{}(n.slice).([]uint64))
	case BlockTypeUint32:
		return vec.Uint32.MinMax(interface{}(n.slice).([]uint32))
	case BlockTypeUint16:
		return vec.Uint16.MinMax(interface{}(n.slice).([]uint16))
	case BlockTypeUint8:
		return vec.Uint8.MinMax(interface{}(n.slice).([]uint8))
	}
	return nil, nil
}

func (n *NumArray[T]) Less(i, j int) bool {
	return n.slice[i] < n.slice[j]
}

func (n *NumArray[T]) MatchEqual(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	switch n.Type() {
	case BlockTypeFloat64:
		return vec.MatchFloat64Equal(interface{}(n.slice).([]float64), val.(float64), bits, mask)
	case BlockTypeFloat32:
		return vec.MatchFloat32Equal(interface{}(n.slice).([]float32), val.(float32), bits, mask)
	case BlockTypeInt64:
		return vec.MatchInt64Equal(interface{}(n.slice).([]int64), val.(int64), bits, mask)
	case BlockTypeInt32:
		return vec.MatchInt32Equal(interface{}(n.slice).([]int32), val.(int32), bits, mask)
	case BlockTypeInt16:
		return vec.MatchInt16Equal(interface{}(n.slice).([]int16), val.(int16), bits, mask)
	case BlockTypeInt8:
		return vec.MatchInt8Equal(interface{}(n.slice).([]int8), val.(int8), bits, mask)
	case BlockTypeUint64:
		return vec.MatchUint64Equal(interface{}(n.slice).([]uint64), val.(uint64), bits, mask)
	case BlockTypeUint32:
		return vec.MatchUint32Equal(interface{}(n.slice).([]uint32), val.(uint32), bits, mask)
	case BlockTypeUint16:
		return vec.MatchUint16Equal(interface{}(n.slice).([]uint16), val.(uint16), bits, mask)
	case BlockTypeUint8:
		return vec.MatchUint8Equal(interface{}(n.slice).([]uint8), val.(uint8), bits, mask)
	}
	return nil
}

func (n *NumArray[T]) MatchNotEqual(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	switch n.Type() {
	case BlockTypeFloat64:
		return vec.MatchFloat64NotEqual(interface{}(n.slice).([]float64), val.(float64), bits, mask)
	case BlockTypeFloat32:
		return vec.MatchFloat32NotEqual(interface{}(n.slice).([]float32), val.(float32), bits, mask)
	case BlockTypeInt64:
		return vec.MatchInt64NotEqual(interface{}(n.slice).([]int64), val.(int64), bits, mask)
	case BlockTypeInt32:
		return vec.MatchInt32NotEqual(interface{}(n.slice).([]int32), val.(int32), bits, mask)
	case BlockTypeInt16:
		return vec.MatchInt16NotEqual(interface{}(n.slice).([]int16), val.(int16), bits, mask)
	case BlockTypeInt8:
		return vec.MatchInt8NotEqual(interface{}(n.slice).([]int8), val.(int8), bits, mask)
	case BlockTypeUint64:
		return vec.MatchUint64NotEqual(interface{}(n.slice).([]uint64), val.(uint64), bits, mask)
	case BlockTypeUint32:
		return vec.MatchUint32NotEqual(interface{}(n.slice).([]uint32), val.(uint32), bits, mask)
	case BlockTypeUint16:
		return vec.MatchUint16NotEqual(interface{}(n.slice).([]uint16), val.(uint16), bits, mask)
	case BlockTypeUint8:
		return vec.MatchUint8NotEqual(interface{}(n.slice).([]uint8), val.(uint8), bits, mask)
	}
	return nil
}

func (n *NumArray[T]) MatchGreaterThan(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	switch n.Type() {
	case BlockTypeFloat64:
		return vec.MatchFloat64GreaterThan(interface{}(n.slice).([]float64), val.(float64), bits, mask)
	case BlockTypeFloat32:
		return vec.MatchFloat32GreaterThan(interface{}(n.slice).([]float32), val.(float32), bits, mask)
	case BlockTypeInt64:
		return vec.MatchInt64GreaterThan(interface{}(n.slice).([]int64), val.(int64), bits, mask)
	case BlockTypeInt32:
		return vec.MatchInt32GreaterThan(interface{}(n.slice).([]int32), val.(int32), bits, mask)
	case BlockTypeInt16:
		return vec.MatchInt16GreaterThan(interface{}(n.slice).([]int16), val.(int16), bits, mask)
	case BlockTypeInt8:
		return vec.MatchInt8GreaterThan(interface{}(n.slice).([]int8), val.(int8), bits, mask)
	case BlockTypeUint64:
		return vec.MatchUint64GreaterThan(interface{}(n.slice).([]uint64), val.(uint64), bits, mask)
	case BlockTypeUint32:
		return vec.MatchUint32GreaterThan(interface{}(n.slice).([]uint32), val.(uint32), bits, mask)
	case BlockTypeUint16:
		return vec.MatchUint16GreaterThan(interface{}(n.slice).([]uint16), val.(uint16), bits, mask)
	case BlockTypeUint8:
		return vec.MatchUint8GreaterThan(interface{}(n.slice).([]uint8), val.(uint8), bits, mask)
	}
	return nil
}

func (n *NumArray[T]) MatchGreaterThanEqual(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	switch n.Type() {
	case BlockTypeFloat64:
		return vec.MatchFloat64GreaterThanEqual(interface{}(n.slice).([]float64), val.(float64), bits, mask)
	case BlockTypeFloat32:
		return vec.MatchFloat32GreaterThanEqual(interface{}(n.slice).([]float32), val.(float32), bits, mask)
	case BlockTypeInt64:
		return vec.MatchInt64GreaterThanEqual(interface{}(n.slice).([]int64), val.(int64), bits, mask)
	case BlockTypeInt32:
		return vec.MatchInt32GreaterThanEqual(interface{}(n.slice).([]int32), val.(int32), bits, mask)
	case BlockTypeInt16:
		return vec.MatchInt16GreaterThanEqual(interface{}(n.slice).([]int16), val.(int16), bits, mask)
	case BlockTypeInt8:
		return vec.MatchInt8GreaterThanEqual(interface{}(n.slice).([]int8), val.(int8), bits, mask)
	case BlockTypeUint64:
		return vec.MatchUint64GreaterThanEqual(interface{}(n.slice).([]uint64), val.(uint64), bits, mask)
	case BlockTypeUint32:
		return vec.MatchUint32GreaterThanEqual(interface{}(n.slice).([]uint32), val.(uint32), bits, mask)
	case BlockTypeUint16:
		return vec.MatchUint16GreaterThanEqual(interface{}(n.slice).([]uint16), val.(uint16), bits, mask)
	case BlockTypeUint8:
		return vec.MatchUint8GreaterThanEqual(interface{}(n.slice).([]uint8), val.(uint8), bits, mask)
	}
	return nil
}

func (n *NumArray[T]) MatchLessThan(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	switch n.Type() {
	case BlockTypeFloat64:
		return vec.MatchFloat64LessThan(interface{}(n.slice).([]float64), val.(float64), bits, mask)
	case BlockTypeFloat32:
		return vec.MatchFloat32LessThan(interface{}(n.slice).([]float32), val.(float32), bits, mask)
	case BlockTypeInt64:
		return vec.MatchInt64LessThan(interface{}(n.slice).([]int64), val.(int64), bits, mask)
	case BlockTypeInt32:
		return vec.MatchInt32LessThan(interface{}(n.slice).([]int32), val.(int32), bits, mask)
	case BlockTypeInt16:
		return vec.MatchInt16LessThan(interface{}(n.slice).([]int16), val.(int16), bits, mask)
	case BlockTypeInt8:
		return vec.MatchInt8LessThan(interface{}(n.slice).([]int8), val.(int8), bits, mask)
	case BlockTypeUint64:
		return vec.MatchUint64LessThan(interface{}(n.slice).([]uint64), val.(uint64), bits, mask)
	case BlockTypeUint32:
		return vec.MatchUint32LessThan(interface{}(n.slice).([]uint32), val.(uint32), bits, mask)
	case BlockTypeUint16:
		return vec.MatchUint16LessThan(interface{}(n.slice).([]uint16), val.(uint16), bits, mask)
	case BlockTypeUint8:
		return vec.MatchUint8LessThan(interface{}(n.slice).([]uint8), val.(uint8), bits, mask)
	}
	return nil
}

func (n *NumArray[T]) MatchLessThanEqual(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	switch n.Type() {
	case BlockTypeFloat64:
		return vec.MatchFloat64LessThanEqual(interface{}(n.slice).([]float64), val.(float64), bits, mask)
	case BlockTypeFloat32:
		return vec.MatchFloat32LessThanEqual(interface{}(n.slice).([]float32), val.(float32), bits, mask)
	case BlockTypeInt64:
		return vec.MatchInt64LessThanEqual(interface{}(n.slice).([]int64), val.(int64), bits, mask)
	case BlockTypeInt32:
		return vec.MatchInt32LessThanEqual(interface{}(n.slice).([]int32), val.(int32), bits, mask)
	case BlockTypeInt16:
		return vec.MatchInt16LessThanEqual(interface{}(n.slice).([]int16), val.(int16), bits, mask)
	case BlockTypeInt8:
		return vec.MatchInt8LessThanEqual(interface{}(n.slice).([]int8), val.(int8), bits, mask)
	case BlockTypeUint64:
		return vec.MatchUint64LessThanEqual(interface{}(n.slice).([]uint64), val.(uint64), bits, mask)
	case BlockTypeUint32:
		return vec.MatchUint32LessThanEqual(interface{}(n.slice).([]uint32), val.(uint32), bits, mask)
	case BlockTypeUint16:
		return vec.MatchUint16LessThanEqual(interface{}(n.slice).([]uint16), val.(uint16), bits, mask)
	case BlockTypeUint8:
		return vec.MatchUint8LessThanEqual(interface{}(n.slice).([]uint8), val.(uint8), bits, mask)
	}
	return nil
}

func (n *NumArray[T]) MatchBetween(from, to interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	switch n.Type() {
	case BlockTypeFloat64:
		return vec.MatchFloat64Between(interface{}(n.slice).([]float64), from.(float64), to.(float64), bits, mask)
	case BlockTypeFloat32:
		return vec.MatchFloat32Between(interface{}(n.slice).([]float32), from.(float32), to.(float32), bits, mask)
	case BlockTypeInt64:
		return vec.MatchInt64Between(interface{}(n.slice).([]int64), from.(int64), to.(int64), bits, mask)
	case BlockTypeInt32:
		return vec.MatchInt32Between(interface{}(n.slice).([]int32), from.(int32), to.(int32), bits, mask)
	case BlockTypeInt16:
		return vec.MatchInt16Between(interface{}(n.slice).([]int16), from.(int16), to.(int16), bits, mask)
	case BlockTypeInt8:
		return vec.MatchInt8Between(interface{}(n.slice).([]int8), from.(int8), to.(int8), bits, mask)
	case BlockTypeUint64:
		return vec.MatchUint64Between(interface{}(n.slice).([]uint64), from.(uint64), to.(uint64), bits, mask)
	case BlockTypeUint32:
		return vec.MatchUint32Between(interface{}(n.slice).([]uint32), from.(uint32), to.(uint32), bits, mask)
	case BlockTypeUint16:
		return vec.MatchUint16Between(interface{}(n.slice).([]uint16), from.(uint16), to.(uint16), bits, mask)
	case BlockTypeUint8:
		return vec.MatchUint8Between(interface{}(n.slice).([]uint8), from.(uint8), to.(uint8), bits, mask)
	}
	return nil
}

func (n *NumArray[T]) Hashes(res []uint64) []uint64 {
	var buf [8]byte
	switch n.Type() {
	case BlockTypeFloat64:
		for i, v := range n.slice {
			bigEndian.PutUint64(buf[:], math.Float64bits(float64(v)))
			res[i] = xxhash.Sum64(buf[:])
		}
	case BlockTypeFloat32:
		for i, v := range n.slice {
			bigEndian.PutUint32(buf[:], math.Float32bits(float32(v)))
			res[i] = xxhash.Sum64(buf[:4])
		}
	case BlockTypeInt64:
		for i, v := range n.slice {
			bigEndian.PutUint64(buf[:], uint64(v))
			res[i] = xxhash.Sum64(buf[:])
		}
	case BlockTypeInt32:
		for i, v := range n.slice {
			bigEndian.PutUint32(buf[:], uint32(v))
			res[i] = xxhash.Sum64(buf[:4])
		}
	case BlockTypeInt16:
		for i, v := range n.slice {
			bigEndian.PutUint16(buf[:], uint16(v))
			res[i] = xxhash.Sum64(buf[:2])
		}
	case BlockTypeInt8:
		for i, v := range n.slice {
			res[i] = xxhash.Sum64([]byte{uint8(v)})
		}
	case BlockTypeUint64:
		for i, v := range n.slice {
			bigEndian.PutUint64(buf[:], uint64(v))
			res[i] = xxhash.Sum64(buf[:])
		}
	case BlockTypeUint32:
		for i, v := range n.slice {
			bigEndian.PutUint32(buf[:], uint32(v))
			res[i] = xxhash.Sum64(buf[:4])
		}
	case BlockTypeUint16:
		for i, v := range n.slice {
			bigEndian.PutUint16(buf[:], uint16(v))
			res[i] = xxhash.Sum64(buf[:2])
		}
	case BlockTypeUint8:
		for i, v := range n.slice {
			res[i] = xxhash.Sum64([]byte{byte(v)})
		}
	}
	return res
}
