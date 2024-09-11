// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package query

import (
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/cmp"
	"blockwatch.cc/knoxdb/internal/filter/bloom"
	"blockwatch.cc/knoxdb/internal/xroar"
	"blockwatch.cc/knoxdb/pkg/slicex"
	"golang.org/x/exp/constraints"

	"unsafe"
)

type Number interface {
	constraints.Integer | constraints.Float
}

type numMatchFunc[T Number] func(slice []T, val T, bits, mask *bitset.Bitset) *bitset.Bitset

var (
	// use as placeholder for comparisons that don't exist
	nullPtr = unsafe.Pointer(nil)

	// pull in comparison kernels as variables so we can take their address below
	u64_eq = cmp.MatchUint64Equal
	u32_eq = cmp.MatchUint32Equal
	u16_eq = cmp.MatchUint16Equal
	u8_eq  = cmp.MatchUint8Equal
	i64_eq = cmp.MatchInt64Equal
	i32_eq = cmp.MatchInt32Equal
	i16_eq = cmp.MatchInt16Equal
	i8_eq  = cmp.MatchInt8Equal
	f64_eq = cmp.MatchFloat64Equal
	f32_eq = cmp.MatchFloat32Equal

	u64_ne = cmp.MatchUint64NotEqual
	u32_ne = cmp.MatchUint32NotEqual
	u16_ne = cmp.MatchUint16NotEqual
	u8_ne  = cmp.MatchUint8NotEqual
	i64_ne = cmp.MatchInt64NotEqual
	i32_ne = cmp.MatchInt32NotEqual
	i16_ne = cmp.MatchInt16NotEqual
	i8_ne  = cmp.MatchInt8NotEqual
	f64_ne = cmp.MatchFloat64NotEqual
	f32_ne = cmp.MatchFloat32NotEqual

	u64_gt = cmp.MatchUint64Greater
	u32_gt = cmp.MatchUint32Greater
	u16_gt = cmp.MatchUint16Greater
	u8_gt  = cmp.MatchUint8Greater
	i64_gt = cmp.MatchInt64Greater
	i32_gt = cmp.MatchInt32Greater
	i16_gt = cmp.MatchInt16Greater
	i8_gt  = cmp.MatchInt8Greater
	f64_gt = cmp.MatchFloat64Greater
	f32_gt = cmp.MatchFloat32Greater

	u64_ge = cmp.MatchUint64GreaterEqual
	u32_ge = cmp.MatchUint32GreaterEqual
	u16_ge = cmp.MatchUint16GreaterEqual
	u8_ge  = cmp.MatchUint8GreaterEqual
	i64_ge = cmp.MatchInt64GreaterEqual
	i32_ge = cmp.MatchInt32GreaterEqual
	i16_ge = cmp.MatchInt16GreaterEqual
	i8_ge  = cmp.MatchInt8GreaterEqual
	f64_ge = cmp.MatchFloat64GreaterEqual
	f32_ge = cmp.MatchFloat32GreaterEqual

	u64_lt = cmp.MatchUint64Less
	u32_lt = cmp.MatchUint32Less
	u16_lt = cmp.MatchUint16Less
	u8_lt  = cmp.MatchUint8Less
	i64_lt = cmp.MatchInt64Less
	i32_lt = cmp.MatchInt32Less
	i16_lt = cmp.MatchInt16Less
	i8_lt  = cmp.MatchInt8Less
	f64_lt = cmp.MatchFloat64Less
	f32_lt = cmp.MatchFloat32Less

	u64_le = cmp.MatchUint64LessEqual
	u32_le = cmp.MatchUint32LessEqual
	u16_le = cmp.MatchUint16LessEqual
	u8_le  = cmp.MatchUint8LessEqual
	i64_le = cmp.MatchInt64LessEqual
	i32_le = cmp.MatchInt32LessEqual
	i16_le = cmp.MatchInt16LessEqual
	i8_le  = cmp.MatchInt8LessEqual
	f64_le = cmp.MatchFloat64LessEqual
	f32_le = cmp.MatchFloat32LessEqual

	u64_rg = cmp.MatchUint64Between
	u32_rg = cmp.MatchUint32Between
	u16_rg = cmp.MatchUint16Between
	u8_rg  = cmp.MatchUint8Between
	i64_rg = cmp.MatchInt64Between
	i32_rg = cmp.MatchInt32Between
	i16_rg = cmp.MatchInt16Between
	i8_rg  = cmp.MatchInt8Between
	f64_rg = cmp.MatchFloat64Between
	f32_rg = cmp.MatchFloat32Between

	// Virtual function pointer table for compare kernels. The purpose of this
	// table is to have fast lookup access to kernel functions without long
	// switch statements.
	//
	// 11 filter modes (0 == invalid)
	// 16 block types
	blockMatchFn = [11][16]unsafe.Pointer{
		// FilterModeInvalid
		{},
		// FilterModeEqual
		{
			unsafe.Pointer(&i64_eq), // 0 BlockTime
			unsafe.Pointer(&i64_eq), // 1 BlockInt64
			unsafe.Pointer(&i32_eq), // 2 BlockInt32
			unsafe.Pointer(&i16_eq), // 3 BlockInt16
			unsafe.Pointer(&i8_eq),  // 4 BlockInt8
			unsafe.Pointer(&u64_eq), // 5 BlockUint64
			unsafe.Pointer(&u32_eq), // 6 BlockUint32
			unsafe.Pointer(&u16_eq), // 7 BlockUint16
			unsafe.Pointer(&u8_eq),  // 8 BlockUint8
			unsafe.Pointer(&f64_eq), // 9 BlockFloat64
			unsafe.Pointer(&f32_eq), // 10 BlockFloat32
			nullPtr,                 // 11 BlockBool
			nullPtr,                 // 12 BlockString
			nullPtr,                 // 13 BlockBytes
			nullPtr,                 // 14 BlockInt128
			nullPtr,                 // 15 BlockInt256
		},
		// FilterModeNotEqual
		{
			unsafe.Pointer(&i64_ne), // 0 BlockTime
			unsafe.Pointer(&i64_ne), // 1 BlockInt64
			unsafe.Pointer(&i32_ne), // 2 BlockInt32
			unsafe.Pointer(&i16_ne), // 3 BlockInt16
			unsafe.Pointer(&i8_ne),  // 4 BlockInt8
			unsafe.Pointer(&u64_ne), // 5 BlockUint64
			unsafe.Pointer(&u32_ne), // 6 BlockUint32
			unsafe.Pointer(&u16_ne), // 7 BlockUint16
			unsafe.Pointer(&u8_ne),  // 8 BlockUint8
			unsafe.Pointer(&f64_ne), // 9 BlockFloat64
			unsafe.Pointer(&f32_ne), // 10 BlockFloat32
			nullPtr,                 // 11 BlockBool
			nullPtr,                 // 12 BlockString
			nullPtr,                 // 13 BlockBytes
			nullPtr,                 // 14 BlockInt128
			nullPtr,                 // 15 BlockInt256
		},
		// FilterModeGt
		{
			unsafe.Pointer(&i64_gt), // 0 BlockTime
			unsafe.Pointer(&i64_gt), // 1 BlockInt64
			unsafe.Pointer(&i32_gt), // 2 BlockInt32
			unsafe.Pointer(&i16_gt), // 3 BlockInt16
			unsafe.Pointer(&i8_gt),  // 4 BlockInt8
			unsafe.Pointer(&u64_gt), // 5 BlockUint64
			unsafe.Pointer(&u32_gt), // 6 BlockUint32
			unsafe.Pointer(&u16_gt), // 7 BlockUint16
			unsafe.Pointer(&u8_gt),  // 8 BlockUint8
			unsafe.Pointer(&f64_gt), // 9 BlockFloat64
			unsafe.Pointer(&f32_gt), // 10 BlockFloat32
			nullPtr,                 // 11 BlockBool
			nullPtr,                 // 12 BlockString
			nullPtr,                 // 13 BlockBytes
			nullPtr,                 // 14 BlockInt128
			nullPtr,                 // 15 BlockInt256
		},
		// FilterModeGe
		{
			unsafe.Pointer(&i64_ge), // 0 BlockTime
			unsafe.Pointer(&i64_ge), // 1 BlockInt64
			unsafe.Pointer(&i32_ge), // 2 BlockInt32
			unsafe.Pointer(&i16_ge), // 3 BlockInt16
			unsafe.Pointer(&i8_ge),  // 4 BlockInt8
			unsafe.Pointer(&u64_ge), // 5 BlockUint64
			unsafe.Pointer(&u32_ge), // 6 BlockUint32
			unsafe.Pointer(&u16_ge), // 7 BlockUint16
			unsafe.Pointer(&u8_ge),  // 8 BlockUint8
			unsafe.Pointer(&f64_ge), // 9 BlockFloat64
			unsafe.Pointer(&f32_ge), // 10 BlockFloat32
			nullPtr,                 // 11 BlockBool
			nullPtr,                 // 12 BlockString
			nullPtr,                 // 13 BlockBytes
			nullPtr,                 // 14 BlockInt128
			nullPtr,                 // 15 BlockInt256
		},
		// FilterModeLt
		{
			unsafe.Pointer(&i64_lt), // 0 BlockTime
			unsafe.Pointer(&i64_lt), // 1 BlockInt64
			unsafe.Pointer(&i32_lt), // 2 BlockInt32
			unsafe.Pointer(&i16_lt), // 3 BlockInt16
			unsafe.Pointer(&i8_lt),  // 4 BlockInt8
			unsafe.Pointer(&u64_lt), // 5 BlockUint64
			unsafe.Pointer(&u32_lt), // 6 BlockUint32
			unsafe.Pointer(&u16_lt), // 7 BlockUint16
			unsafe.Pointer(&u8_lt),  // 8 BlockUint8
			unsafe.Pointer(&f64_lt), // 9 BlockFloat64
			unsafe.Pointer(&f32_lt), // 10 BlockFloat32
			nullPtr,                 // 11 BlockBool
			nullPtr,                 // 12 BlockString
			nullPtr,                 // 13 BlockBytes
			nullPtr,                 // 14 BlockInt128
			nullPtr,                 // 15 BlockInt256
		},
		// FilterModeLe
		{
			unsafe.Pointer(&i64_le), // 0 BlockTime
			unsafe.Pointer(&i64_le), // 1 BlockInt64
			unsafe.Pointer(&i32_le), // 2 BlockInt32
			unsafe.Pointer(&i16_le), // 3 BlockInt16
			unsafe.Pointer(&i8_le),  // 4 BlockInt8
			unsafe.Pointer(&u64_le), // 5 BlockUint64
			unsafe.Pointer(&u32_le), // 6 BlockUint32
			unsafe.Pointer(&u16_le), // 7 BlockUint16
			unsafe.Pointer(&u8_le),  // 8 BlockUint8
			unsafe.Pointer(&f64_le), // 9 BlockFloat64
			unsafe.Pointer(&f32_le), // 10 BlockFloat32
			nullPtr,                 // 11 BlockBool
			nullPtr,                 // 12 BlockString
			nullPtr,                 // 13 BlockBytes
			nullPtr,                 // 14 BlockInt128
			nullPtr,                 // 15 BlockInt256
		},
		// FilterModeIn
		{},
		// FilterModeNotIn
		{},
		// FilterModeRange
		{
			unsafe.Pointer(&i64_rg), // 0 BlockTime
			unsafe.Pointer(&i64_rg), // 1 BlockInt64
			unsafe.Pointer(&i32_rg), // 2 BlockInt32
			unsafe.Pointer(&i16_rg), // 3 BlockInt16
			unsafe.Pointer(&i8_rg),  // 4 BlockInt8
			unsafe.Pointer(&u64_rg), // 5 BlockUint64
			unsafe.Pointer(&u32_rg), // 6 BlockUint32
			unsafe.Pointer(&u16_rg), // 7 BlockUint16
			unsafe.Pointer(&u8_rg),  // 8 BlockUint8
			unsafe.Pointer(&f64_rg), // 9 BlockFloat64
			unsafe.Pointer(&f32_rg), // 10 BlockFloat32
			nullPtr,                 // 11 BlockBool
			nullPtr,                 // 12 BlockString
			nullPtr,                 // 13 BlockBytes
			nullPtr,                 // 14 BlockInt128
			nullPtr,                 // 15 BlockInt256
		},
		// FilterModeRegexp
		{},
	}
)

type NumMatcherFactory[T Number] struct {
	typ BlockType
}

func (f NumMatcherFactory[T]) New(m FilterMode) Matcher {
	fn := *(*numMatchFunc[T])(blockMatchFn[m][f.typ])
	switch m {
	case FilterModeEqual:
		return &numEqualMatcher[T]{numMatcher[T]{match: fn}}
	case FilterModeNotEqual:
		return &numNotEqualMatcher[T]{numMatcher[T]{match: fn}}
	case FilterModeGt:
		return &numGtMatcher[T]{numMatcher[T]{match: fn}}
	case FilterModeGe:
		return &numGeMatcher[T]{numMatcher[T]{match: fn}}
	case FilterModeLt:
		return &numLtMatcher[T]{numMatcher[T]{match: fn}}
	case FilterModeLe:
		return &numLeMatcher[T]{numMatcher[T]{match: fn}}
	case FilterModeRange:
		return &numRangeMatcher[T]{numMatcher: numMatcher[T]{match: fn}}
	case FilterModeIn:
		return &numInSetMatcher[T]{}
	case FilterModeNotIn:
		return &numNotInSetMatcher[T]{}
	default:
		// unsupported
		// FilterModeRegexp
		return &noopMatcher{}
	}
}

// numMatcher is a generic value matcher that we use to avoid reimplementing
// similar member functions for specialized matchers below. I.e. it implements
// the WithValue() and MatchBlock() parts of the Matcher interface.
type numMatcher[T Number] struct {
	noopMatcher
	match numMatchFunc[T]
	val   T
	hash  [2]uint32
}

func (m *numMatcher[T]) WithValue(v any) {
	m.val = v.(T)
	m.hash = bloom.HashAny(v)
}

func (m numMatcher[T]) MatchBlock(b *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	return m.match(unsafe.Slice((*T)(b.Ptr()), b.Len()), m.val, bits, mask)
}

// EQUAL ---

type numEqualMatcher[T Number] struct {
	numMatcher[T]
}

func (m numEqualMatcher[T]) MatchValue(v any) bool {
	return m.val == v.(T)
}

func (m numEqualMatcher[T]) MatchRange(from, to any) bool {
	return !(m.val < from.(T) || m.val > to.(T))
}

func (m numEqualMatcher[T]) MatchBloom(flt *bloom.Filter) bool {
	return flt.ContainsHash(m.hash)
}

func (m numEqualMatcher[T]) MatchBitmap(flt *xroar.Bitmap) bool {
	return flt.Contains(uint64(m.val))
}

// NOT EQUAL ---

type numNotEqualMatcher[T Number] struct {
	numMatcher[T]
}

func (m numNotEqualMatcher[T]) MatchValue(v any) bool {
	return m.val != v.(T)
}

func (m numNotEqualMatcher[T]) MatchRange(from, to any) bool {
	return m.val < from.(T) || m.val > to.(T)
}

// GT ---

type numGtMatcher[T Number] struct {
	numMatcher[T]
}

func (m numGtMatcher[T]) MatchValue(v any) bool {
	return m.val < v.(T)
}

func (m numGtMatcher[T]) MatchRange(_, to any) bool {
	// return m.val < from.(T) || m.val < to.(T)
	return m.val < to.(T)
}

// GE ---

type numGeMatcher[T Number] struct {
	numMatcher[T]
}

func (m numGeMatcher[T]) MatchValue(v any) bool {
	return m.val <= v.(T)
}

func (m numGeMatcher[T]) MatchRange(from, to any) bool {
	// return m.val <= from.(T) || m.val <= to.(T)
	return m.val <= to.(T)
}

// LT ---

type numLtMatcher[T Number] struct {
	numMatcher[T]
}

func (m numLtMatcher[T]) MatchValue(v any) bool {
	return m.val > v.(T)
}

func (m numLtMatcher[T]) MatchRange(from, to any) bool {
	// return m.val > from.(T) || m.val > to.(T)
	return m.val > from.(T)
}

// LE ---

type numLeMatcher[T Number] struct {
	numMatcher[T]
}

func (m numLeMatcher[T]) MatchValue(v any) bool {
	return m.val >= v.(T)
}

func (m numLeMatcher[T]) MatchRange(from, to any) bool {
	// return m.val >= from.(T) || m.val >= to.(T)
	return m.val >= from.(T)
}

// RANGE ---

// InBetween, ContainsRange
type numRangeMatcher[T Number] struct {
	numMatcher[T]
	from T
	to   T
}

func (m *numRangeMatcher[T]) Weight() int { return 2 }

func (m *numRangeMatcher[T]) WithRange(from, to any) {
	m.from = from.(T)
	m.to = to.(T)
}

func (m numRangeMatcher[T]) MatchValue(v any) bool {
	return m.from <= v.(T) && m.to >= v.(T)
}

func (m numRangeMatcher[T]) MatchRange(from, to any) bool {
	return !(from.(T) > m.to || to.(T) < m.from)
}

// IN ---

// In, Contains
type numInSetMatcher[T Number] struct {
	noopMatcher
	set    map[T]struct{} // maybe use slicex.OrderedNumbers or xroar bitset instead
	slice  *slicex.OrderedNumbers[T]
	hashes [][2]uint32
}

func (m *numInSetMatcher[T]) Weight() int { return m.slice.Len() }

func (m *numInSetMatcher[T]) WithSet(set any) {
	// TODO: accept Bitmap (xroar) and []T
	// convert and use both
	// xroar FromSortedList

	m.slice = slicex.NewOrderedNumbers(set.([]T)).SetUnique()
	m.set = make(map[T]struct{}, len(m.slice.Values))
	m.hashes = make([][2]uint32, len(m.slice.Values))
	for i, v := range m.slice.Values {
		m.set[v] = struct{}{}
		m.hashes[i] = bloom.HashAny(v)
	}
}

func (m numInSetMatcher[T]) MatchValue(v any) bool {
	return m.slice.Contains(v.(T))
}

func (m numInSetMatcher[T]) MatchRange(from, to any) bool {
	return m.slice.ContainsRange(from.(T), to.(T))
}

func (m numInSetMatcher[T]) MatchBloom(flt *bloom.Filter) bool {
	return flt.ContainsAnyHash(m.hashes)
}

func (m numInSetMatcher[T]) MatchBitmap(flt *xroar.Bitmap) bool {
	for n := range m.set {
		if flt.Contains(uint64(n)) {
			return true
		}
	}
	return false
}

func (m numInSetMatcher[T]) MatchBlock(b *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	if mask != nil {
		// skip masked values
		for i, v := range *(*[]T)(b.Ptr()) {
			if !mask.IsSet(i) {
				continue
			}
			if _, ok := m.set[v]; ok {
				bits.Set(i)
			}
		}
	} else {
		for i, v := range *(*[]T)(b.Ptr()) {
			if _, ok := m.set[v]; ok {
				bits.Set(i)
			}
		}
	}
	return bits
}

// NOT IN ---

type numNotInSetMatcher[T Number] struct {
	noopMatcher
	set   map[T]struct{} // maybe use slicex.OrderedNumbers or xroar bitset instead
	slice *slicex.OrderedNumbers[T]
}

func (m *numNotInSetMatcher[T]) Weight() int { return m.slice.Len() }

func (m *numNotInSetMatcher[T]) WithSet(set any) {
	m.slice = slicex.NewOrderedNumbers(set.([]T)).SetUnique()
	m.set = make(map[T]struct{}, len(m.slice.Values))
	for _, v := range m.slice.Values {
		m.set[v] = struct{}{}
	}
}

func (m numNotInSetMatcher[T]) MatchValue(v any) bool {
	return !m.slice.Contains(v.(T))
}

func (m numNotInSetMatcher[T]) MatchRange(from, to any) bool {
	return !m.slice.ContainsRange(from.(T), to.(T))
}

func (m numNotInSetMatcher[T]) MatchBloom(flt *bloom.Filter) bool {
	// we don't know generally, so full scan is always required
	return true
}

func (m numNotInSetMatcher[T]) MatchBitmap(flt *xroar.Bitmap) bool {
	// we don't know generally, so full scan is always required
	return true
}

func (m numNotInSetMatcher[T]) MatchBlock(b *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	if mask != nil {
		// skip masked values
		for i, v := range *(*[]T)(b.Ptr()) {
			if !mask.IsSet(i) {
				continue
			}
			if _, ok := m.set[v]; !ok {
				bits.Set(i)
			}
		}
	} else {
		for i, v := range *(*[]T)(b.Ptr()) {
			if _, ok := m.set[v]; !ok {
				bits.Set(i)
			}
		}
	}
	return bits
}