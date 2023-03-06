// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package num

import (
	"encoding/binary"
	"math"
	"reflect"
	"sort"
	"unsafe"

	"blockwatch.cc/knoxdb/hash/xxhash"
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

func Insert[T Number](s []T, k int, vs ...T) []T {
	if n := len(s) + len(vs); n <= cap(s) {
		s2 := s[:n]
		copy(s2[k+len(vs):], s[k:])
		copy(s2[k:], vs)
		return s2
	}
	s2 := make([]T, len(s)+len(vs))
	copy(s2, s[:k])
	copy(s2[k:], vs)
	copy(s2[k+len(vs):], s[k:])
	return s2
}

func (n *NumArray[T]) InsertFrom(slice []T, spos, dpos, len int) {
	n.slice = Insert(n.slice, dpos, slice[spos:spos+len]...)
}

func (n *NumArray[T]) MinMax() (T, T) {
	return MinMax(n.slice)
}

func MinMax[T Number](s []T) (T, T) {
	var min, max T
	switch l := len(s); l {
	case 0:
		// nothing
	case 1:
		min, max = s[0], s[0]
	default:
		// If there is more than one element, then initialize min and max
		if s[0] > s[1] {
			max = s[0]
			min = s[1]
		} else {
			max = s[1]
			min = s[0]
		}

		for i := 2; i < l; i++ {
			if s[i] > max {
				max = s[i]
			} else if s[i] < min {
				min = s[i]
			}
		}
	}

	return min, max
}

func RemoveZeros[T Number](s []T) ([]T, int) {
	var n int
	for i, v := range s {
		if v == 0 {
			continue
		}
		s[n] = s[i]
		n++
	}
	s = s[:n]
	return s, n
}

func index[T Number](s []T, val T, last int) int {
	if len(s) <= last {
		return -1
	}

	// search for value in slice starting at last index
	slice := s[last:]
	l := len(slice)
	min, max := slice[0], slice[l-1]
	if val < min || val > max {
		return -1
	}

	// for dense slices (values are continuous) compute offset directly
	if l == int(max-min)+1 {
		return int(val-min) + last
	}

	// for sparse slices, use binary search (slice is sorted)
	idx := sort.Search(l, func(i int) bool { return slice[i] >= val })
	if idx < l && slice[idx] == val {
		return idx + last
	}
	return -1
}

func Remove[T Number](s []T, val T) ([]T, bool) {
	idx := index(s, val, 0)
	if idx < 0 {
		return s, false
	}
	s = append(s[:idx], s[idx+1:]...)
	return s, true
}

func Contains[T Number](s []T, val T) bool {
	// empty s cannot contain values
	if len(s) == 0 {
		return false
	}

	// s is sorted, check against first (min) and last (max) entries
	if s[0] > val || s[len(s)-1] < val {
		return false
	}

	// for dense slices (continuous, no dups) compute offset directly
	if ofs := int(val - s[0]); ofs >= 0 && ofs < len(s) && s[ofs] == val {
		return true
	}

	// use binary search to find value in sorted s
	i := sort.Search(len(s), func(i int) bool { return s[i] >= val })
	if i < len(s) && s[i] == val {
		return true
	}

	return false
}

// ContainsRange returns true when slice s contains any values between
// from and to. Note that from/to do not necessarily have to be members
// themselves, but some intermediate values are. Slice s is expected
// to be sorted and from must be less than or equal to to.
func ContainsRange[T Number](s []T, from, to T) bool {
	n := len(s)
	if n == 0 {
		return false
	}
	// Case A
	if to < s[0] {
		return false
	}
	// shortcut for B.1
	if to == s[0] {
		return true
	}
	// Case E
	if from > s[n-1] {
		return false
	}
	// shortcut for D.3
	if from == s[n-1] {
		return true
	}
	// Case B-D
	// search if lower interval bound is within slice
	min := sort.Search(n, func(i int) bool {
		return s[i] >= from
	})
	// exit when from was found (no need to check if min < n)
	if s[min] == from {
		return true
	}
	// continue search for upper interval bound in the remainder of the slice
	max := sort.Search(n-min, func(i int) bool {
		return s[i+min] >= to
	})
	max = max + min

	// exit when to was found (also solves case C1a)
	if max < n && s[max] == to {
		return true
	}

	// range is contained iff min < max; note that from/to do not necessarily
	// have to be members, but some intermediate values are
	return min < max
}

func (n *NumArray[T]) Less(i, j int) bool {
	return n.slice[i] < n.slice[j]
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
