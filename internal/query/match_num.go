// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package query

import (
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/cmp"
	"blockwatch.cc/knoxdb/internal/filter"
	"blockwatch.cc/knoxdb/internal/filter/bloom"
	"blockwatch.cc/knoxdb/internal/hash"
	"blockwatch.cc/knoxdb/internal/xroar"

	"unsafe"
)

type Number interface {
	int64 | int32 | int16 | int8 | uint64 | uint32 | uint16 | uint8 | float64 | float32
}

type numMatchFunc[T Number] func(slice []T, val T, bits, mask *bitset.Bitset) *bitset.Bitset

type numRangeMatchFunc[T Number] func(slice []T, from, to T, bits, mask *bitset.Bitset) *bitset.Bitset

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
	blockMatchFn = [11][15]unsafe.Pointer{
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
			nullPtr,                 // 12 BlockBytes
			nullPtr,                 // 13 BlockInt128
			nullPtr,                 // 14 BlockInt256
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
			nullPtr,                 // 12 BlockBytes
			nullPtr,                 // 13 BlockInt128
			nullPtr,                 // 14 BlockInt256
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
			nullPtr,                 // 12 BlockBytes
			nullPtr,                 // 13 BlockInt128
			nullPtr,                 // 14 BlockInt256
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
			nullPtr,                 // 12 BlockBytes
			nullPtr,                 // 13 BlockInt128
			nullPtr,                 // 14 BlockInt256
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
			nullPtr,                 // 12 BlockBytes
			nullPtr,                 // 13 BlockInt128
			nullPtr,                 // 14 BlockInt256
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
			nullPtr,                 // 12 BlockBytes
			nullPtr,                 // 13 BlockInt128
			nullPtr,                 // 14 BlockInt256
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
			nullPtr,                 // 12 BlockBytes
			nullPtr,                 // 13 BlockInt128
			nullPtr,                 // 14 BlockInt256
		},
		// FilterModeRegexp
		{},
	}
)

type NumMatcherFactory[T Number] struct {
	typ BlockType
}

func (f NumMatcherFactory[T]) New(m FilterMode) Matcher {
	switch m {
	case FilterModeEqual:
		fn := *(*numMatchFunc[T])(blockMatchFn[m][f.typ])
		return &numEqualMatcher[T]{numMatcher[T]{match: fn}}
	case FilterModeNotEqual:
		fn := *(*numMatchFunc[T])(blockMatchFn[m][f.typ])
		return &numNotEqualMatcher[T]{numMatcher[T]{match: fn}}
	case FilterModeGt:
		fn := *(*numMatchFunc[T])(blockMatchFn[m][f.typ])
		return &numGtMatcher[T]{numMatcher[T]{match: fn}}
	case FilterModeGe:
		fn := *(*numMatchFunc[T])(blockMatchFn[m][f.typ])
		return &numGeMatcher[T]{numMatcher[T]{match: fn}}
	case FilterModeLt:
		fn := *(*numMatchFunc[T])(blockMatchFn[m][f.typ])
		return &numLtMatcher[T]{numMatcher[T]{match: fn}}
	case FilterModeLe:
		fn := *(*numMatchFunc[T])(blockMatchFn[m][f.typ])
		return &numLeMatcher[T]{numMatcher[T]{match: fn}}
	case FilterModeRange:
		fn := *(*numRangeMatchFunc[T])(blockMatchFn[m][f.typ])
		return &numRangeMatcher[T]{match: fn}
	case FilterModeIn:
		switch f.typ {
		case BlockFloat32, BlockFloat64:
			return &floatInSetMatcher[T]{}
		default:
			return &numInSetMatcher[T]{}
		}

	case FilterModeNotIn:
		switch f.typ {
		case BlockFloat32, BlockFloat64:
			return &floatNotInSetMatcher[T]{}
		default:
			return &numNotInSetMatcher[T]{}
		}
	default:
		// unsupported
		// FilterModeRegexp
		return &noopMatcher{}
	}
}

// numMatcher is a generic value matcher that we use to avoid reimplementing
// similar member functions for specialized matchers below. I.e. it implements
// the WithValue() and MatchVector() parts of the Matcher interface.
type numMatcher[T Number] struct {
	noopMatcher
	match numMatchFunc[T]
	val   T
	hash  hash.HashValue
}

func (m *numMatcher[T]) WithValue(v any) {
	m.val = v.(T)
	m.hash = hash.HashAny(v)
}

func (m *numMatcher[T]) Value() any {
	return m.val
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

func (m numEqualMatcher[T]) MatchFilter(flt filter.Filter) bool {
	if x, ok := flt.(*bloom.Filter); ok {
		return x.ContainsHash(m.hash)
	}
	return flt.Contains(uint64(m.val))
}

func (m numEqualMatcher[T]) MatchVector(b *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	acc := block.NewBlockAccessor[T](b)
	if bm := acc.Matcher(); bm != nil {
		return bm.MatchEqual(m.val, bits, mask)
	}
	return m.match(acc.Slice(), m.val, bits, mask)
}

func (m numEqualMatcher[T]) MatchRangeVectors(mins, maxs *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	// min <= v && max >= v, mask is optional
	f := newFactory(mins.Type())
	le, ge := f.New(FilterModeLe), f.New(FilterModeGe)
	le.WithValue(m.val)
	ge.WithValue(m.val)
	minBits := le.MatchVector(mins, nil, mask)
	if mask != nil {
		minBits.And(mask)
	}
	bits = ge.MatchVector(maxs, bits, minBits)
	bits.And(minBits)
	minBits.Close()
	return bits
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

func (m numNotEqualMatcher[T]) MatchVector(b *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	acc := block.NewBlockAccessor[T](b)
	if bm := acc.Matcher(); bm != nil {
		return bm.MatchNotEqual(m.val, bits, mask)
	}
	return m.match(acc.Slice(), m.val, bits, mask)
}

func (m numNotEqualMatcher[T]) MatchRangeVectors(_, _ *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	// undecided, always true
	if mask != nil {
		bits.Copy(mask)
	} else {
		bits.One()
	}
	return bits
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

func (m numGtMatcher[T]) MatchVector(b *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	acc := block.NewBlockAccessor[T](b)
	if bm := acc.Matcher(); bm != nil {
		return bm.MatchGreater(m.val, bits, mask)
	}
	return m.match(acc.Slice(), m.val, bits, mask)
}

func (m numGtMatcher[T]) MatchRangeVectors(_, maxs *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	// max > v
	gt := newFactory(maxs.Type()).New(FilterModeGt)
	gt.WithValue(m.val)
	return gt.MatchVector(maxs, bits, mask)
}

// GE ---

type numGeMatcher[T Number] struct {
	numMatcher[T]
}

func (m numGeMatcher[T]) MatchValue(v any) bool {
	return m.val <= v.(T)
}

func (m numGeMatcher[T]) MatchRange(_, to any) bool {
	// return m.val <= from.(T) || m.val <= to.(T)
	return m.val <= to.(T)
}

func (m numGeMatcher[T]) MatchVector(b *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	acc := block.NewBlockAccessor[T](b)
	if bm := acc.Matcher(); bm != nil {
		return bm.MatchGreaterEqual(m.val, bits, mask)
	}
	return m.match(acc.Slice(), m.val, bits, mask)
}

func (m numGeMatcher[T]) MatchRangeVectors(_, maxs *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	// max >= v
	ge := newFactory(maxs.Type()).New(FilterModeGe)
	ge.WithValue(m.val)
	return ge.MatchVector(maxs, bits, mask)
}

// LT ---

type numLtMatcher[T Number] struct {
	numMatcher[T]
}

func (m numLtMatcher[T]) MatchValue(v any) bool {
	return m.val > v.(T)
}

func (m numLtMatcher[T]) MatchRange(from, _ any) bool {
	// return m.val > from.(T) || m.val > to.(T)
	return m.val > from.(T)
}

func (m numLtMatcher[T]) MatchVector(b *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	acc := block.NewBlockAccessor[T](b)
	if bm := acc.Matcher(); bm != nil {
		return bm.MatchLess(m.val, bits, mask)
	}
	return m.match(acc.Slice(), m.val, bits, mask)
}

func (m numLtMatcher[T]) MatchRangeVectors(mins, _ *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	// min < v
	lt := newFactory(mins.Type()).New(FilterModeLt)
	lt.WithValue(m.val)
	return lt.MatchVector(mins, bits, mask)
}

// LE ---

type numLeMatcher[T Number] struct {
	numMatcher[T]
}

func (m numLeMatcher[T]) MatchValue(v any) bool {
	return m.val >= v.(T)
}

func (m numLeMatcher[T]) MatchRange(from, _ any) bool {
	// return m.val >= from.(T) || m.val >= to.(T)
	return m.val >= from.(T)
}

func (m numLeMatcher[T]) MatchVector(b *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	acc := block.NewBlockAccessor[T](b)
	if bm := acc.Matcher(); bm != nil {
		return bm.MatchLessEqual(m.val, bits, mask)
	}
	return m.match(acc.Slice(), m.val, bits, mask)
}

func (m numLeMatcher[T]) MatchRangeVectors(mins, _ *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	// min <= v
	le := newFactory(mins.Type()).New(FilterModeLe)
	le.WithValue(m.val)
	return le.MatchVector(mins, bits, mask)
}

// RANGE ---

// InBetween, ContainsRange
type numRangeMatcher[T Number] struct {
	noopMatcher
	match numRangeMatchFunc[T]
	from  T
	to    T
}

func (m *numRangeMatcher[T]) Value() any { return RangeValue{m.from, m.to} }

func (m *numRangeMatcher[T]) Weight() int { return 2 }

func (m *numRangeMatcher[T]) Len() int { return 2 }

func (m *numRangeMatcher[T]) WithValue(v any) {
	val := v.(RangeValue)
	m.from = val[0].(T)
	m.to = val[1].(T)
}

func (m numRangeMatcher[T]) MatchValue(v any) bool {
	return m.from <= v.(T) && m.to >= v.(T)
}

func (m numRangeMatcher[T]) MatchRange(from, to any) bool {
	return !(from.(T) > m.to || to.(T) < m.from)
}

func (m numRangeMatcher[T]) MatchVector(b *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	acc := block.NewBlockAccessor[T](b)
	if bm := acc.Matcher(); bm != nil {
		return bm.MatchBetween(m.from, m.to, bits, mask)
	}
	return m.match(acc.Slice(), m.from, m.to, bits, mask)
}

func (m numRangeMatcher[T]) MatchFilter(flt filter.Filter) bool {
	// we don't know generally, so full scan is required
	return true
}

func (m numRangeMatcher[T]) MatchRangeVectors(mins, maxs *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	// min <= to && max >= from
	f := newFactory(mins.Type())
	le, ge := f.New(FilterModeLe), f.New(FilterModeGe)
	le.WithValue(m.to)
	ge.WithValue(m.from)
	minBits := le.MatchVector(mins, nil, mask)
	if mask != nil {
		minBits.And(mask)
	}
	bits = ge.MatchVector(maxs, bits, minBits)
	bits.And(minBits)
	minBits.Close()
	return bits
}

// IN ---

// In, Contains
type numInSetMatcher[T Number] struct {
	set    *xroar.Bitmap
	hashes []hash.HashValue
}

func (m *numInSetMatcher[T]) Weight() int { return 1 }

func (m *numInSetMatcher[T]) Len() int { return m.set.GetCardinality() }

func (m *numInSetMatcher[T]) Value() any {
	// FIXME: support bitmap in optimizer
	card := m.set.GetCardinality()
	it := m.set.NewIterator()
	vals := make([]T, card)
	for i := 0; i < card; i++ {
		vals[i] = T(it.Next())
	}
	return vals
}

func (m *numInSetMatcher[T]) WithValue(val any) {
	m.WithSlice(val)
}

func (m *numInSetMatcher[T]) WithSlice(slice any) {
	m.set = xroar.NewBitmap()
	for _, v := range slice.([]T) {
		m.set.Set(uint64(v))
	}
	m.hashes = hash.HashAnySlice(slice.([]T))
}

func (m *numInSetMatcher[T]) WithSet(set *xroar.Bitmap) {
	m.set = set
	card := set.GetCardinality()
	it := m.set.NewIterator()
	m.hashes = make([]hash.HashValue, card)
	for i := 0; i < card; i++ {
		m.hashes[i] = hash.HashUint64(it.Next())
	}
}

func (m numInSetMatcher[T]) MatchValue(v any) bool {
	return m.set.Contains(uint64(v.(T)))
}

func (m numInSetMatcher[T]) MatchRange(from, to any) bool {
	return m.set.ContainsRange(uint64(from.(T)), uint64(to.(T)))
}

func (m numInSetMatcher[T]) MatchFilter(flt filter.Filter) bool {
	if x, ok := flt.(*xroar.Bitmap); ok {
		return !xroar.And(m.set, x).IsEmpty()
	}
	for _, h := range m.hashes {
		if flt.Contains(h.Uint64()) {
			return true
		}
	}
	return false
}

func (m numInSetMatcher[T]) MatchVector(b *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	acc := block.NewBlockAccessor[T](b)
	if bm := acc.Matcher(); bm != nil {
		return bm.MatchSet(m.set, bits, mask)
	}
	if mask != nil {
		// skip masked values
		for i, v := range acc.Slice() {
			if !mask.IsSet(i) {
				continue
			}
			if m.set.Contains(uint64(v)) {
				bits.Set(i)
			}
		}
	} else {
		for i, v := range acc.Slice() {
			if m.set.Contains(uint64(v)) {
				bits.Set(i)
			}
		}
	}
	return bits
}

func (m numInSetMatcher[T]) MatchRangeVectors(mins, maxs *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	minx := block.NewBlockAccessor[T](mins).Slice()
	maxx := block.NewBlockAccessor[T](maxs).Slice()
	if mask != nil {
		for i, l := 0, len(minx); i < l; i++ {
			if !mask.IsSet(i) {
				continue
			}
			minU64, maxU64 := uint64(minx[i]), uint64(maxx[i])
			// source could contain negative integers
			if minU64 > maxU64 {
				minU64, maxU64 = maxU64, minU64
			}
			if m.set.ContainsRange(minU64, maxU64) {
				bits.Set(i)
			}
		}
	} else {
		for i, l := 0, len(minx); i < l; i++ {
			minU64, maxU64 := uint64(minx[i]), uint64(maxx[i])
			// source could contain negative integers
			if minU64 > maxU64 {
				minU64, maxU64 = maxU64, minU64
			}
			if m.set.ContainsRange(minU64, maxU64) {
				bits.Set(i)
			}
		}
	}
	return bits

	// setMin, setMax := m.set.Minimum(), m.set.Maximum()
	// rg := newFactory(mins.Type()).New(FilterModeRange)
	// rg.WithValue(RangeValue{T(setMin), T(setMax)})
	// return rg.MatchRangeVectors(mins, maxs, bits, mask)
}

// NOT IN ---

type numNotInSetMatcher[T Number] struct {
	set *xroar.Bitmap
}

func (m *numNotInSetMatcher[T]) Weight() int { return 1 }

func (m *numNotInSetMatcher[T]) Len() int { return m.set.GetCardinality() }

func (m *numNotInSetMatcher[T]) Value() any {
	// FIXME: support bitmap in optimizer
	card := m.set.GetCardinality()
	it := m.set.NewIterator()
	vals := make([]T, card)
	for i := 0; i < card; i++ {
		vals[i] = T(it.Next())
	}
	return vals
}

func (m *numNotInSetMatcher[T]) WithValue(val any) {
	m.WithSlice(val)
}

func (m *numNotInSetMatcher[T]) WithSlice(slice any) {
	m.set = xroar.NewBitmap()
	for _, v := range slice.([]T) {
		m.set.Set(uint64(v))
	}
}

func (m *numNotInSetMatcher[T]) WithSet(set *xroar.Bitmap) {
	m.set = set
}

func (m numNotInSetMatcher[T]) MatchValue(v any) bool {
	return !m.set.Contains(uint64(v.(T)))
}

func (m numNotInSetMatcher[T]) MatchRange(from, to any) bool {
	return !m.set.ContainsRange(uint64(from.(T)), uint64(to.(T)))
}

func (m numNotInSetMatcher[T]) MatchFilter(flt filter.Filter) bool {
	if x, ok := flt.(*xroar.Bitmap); ok {
		return !xroar.AndNot(m.set, x).IsEmpty()
	}

	// we don't know generally, so full scan is required
	return true
}

func (m numNotInSetMatcher[T]) MatchVector(b *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	acc := block.NewBlockAccessor[T](b)
	if bm := acc.Matcher(); bm != nil {
		return bm.MatchNotSet(m.set, bits, mask)
	}
	if mask != nil {
		// skip masked values
		for i, v := range acc.Slice() {
			if !mask.IsSet(i) {
				continue
			}
			if !m.set.Contains(uint64(v)) {
				bits.Set(i)
			}
		}
	} else {
		for i, v := range acc.Slice() {
			if !m.set.Contains(uint64(v)) {
				bits.Set(i)
			}
		}
	}
	return bits
}

func (m numNotInSetMatcher[T]) MatchRangeVectors(_, _ *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	// undecided, always true
	if mask != nil {
		bits.Copy(mask)
	} else {
		bits.One()
	}
	return bits
}
