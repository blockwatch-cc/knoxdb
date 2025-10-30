// Copyright (c) 2023-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package filter

import (
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/cmp"
	"blockwatch.cc/knoxdb/internal/filter"
	"blockwatch.cc/knoxdb/internal/filter/bloom"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/xroar"

	"unsafe"
)

type Number = types.Number

type numMatchFunc[T Number] func(slice []T, val T, bits []byte) int64

type numRangeMatchFunc[T Number] func(slice []T, from, to T, bits []byte) int64

var (
	// use as placeholder for comparisons that don't exist
	nullPtr = unsafe.Pointer(nil)

	// pull in comparison kernels as variables so we can take their address below
	u64_eq = cmp.Uint64Equal
	u32_eq = cmp.Uint32Equal
	u16_eq = cmp.Uint16Equal
	u8_eq  = cmp.Uint8Equal
	i64_eq = cmp.Int64Equal
	i32_eq = cmp.Int32Equal
	i16_eq = cmp.Int16Equal
	i8_eq  = cmp.Int8Equal
	f64_eq = cmp.Float64Equal
	f32_eq = cmp.Float32Equal

	u64_ne = cmp.Uint64NotEqual
	u32_ne = cmp.Uint32NotEqual
	u16_ne = cmp.Uint16NotEqual
	u8_ne  = cmp.Uint8NotEqual
	i64_ne = cmp.Int64NotEqual
	i32_ne = cmp.Int32NotEqual
	i16_ne = cmp.Int16NotEqual
	i8_ne  = cmp.Int8NotEqual
	f64_ne = cmp.Float64NotEqual
	f32_ne = cmp.Float32NotEqual

	u64_gt = cmp.Uint64Greater
	u32_gt = cmp.Uint32Greater
	u16_gt = cmp.Uint16Greater
	u8_gt  = cmp.Uint8Greater
	i64_gt = cmp.Int64Greater
	i32_gt = cmp.Int32Greater
	i16_gt = cmp.Int16Greater
	i8_gt  = cmp.Int8Greater
	f64_gt = cmp.Float64Greater
	f32_gt = cmp.Float32Greater

	u64_ge = cmp.Uint64GreaterEqual
	u32_ge = cmp.Uint32GreaterEqual
	u16_ge = cmp.Uint16GreaterEqual
	u8_ge  = cmp.Uint8GreaterEqual
	i64_ge = cmp.Int64GreaterEqual
	i32_ge = cmp.Int32GreaterEqual
	i16_ge = cmp.Int16GreaterEqual
	i8_ge  = cmp.Int8GreaterEqual
	f64_ge = cmp.Float64GreaterEqual
	f32_ge = cmp.Float32GreaterEqual

	u64_lt = cmp.Uint64Less
	u32_lt = cmp.Uint32Less
	u16_lt = cmp.Uint16Less
	u8_lt  = cmp.Uint8Less
	i64_lt = cmp.Int64Less
	i32_lt = cmp.Int32Less
	i16_lt = cmp.Int16Less
	i8_lt  = cmp.Int8Less
	f64_lt = cmp.Float64Less
	f32_lt = cmp.Float32Less

	u64_le = cmp.Uint64LessEqual
	u32_le = cmp.Uint32LessEqual
	u16_le = cmp.Uint16LessEqual
	u8_le  = cmp.Uint8LessEqual
	i64_le = cmp.Int64LessEqual
	i32_le = cmp.Int32LessEqual
	i16_le = cmp.Int16LessEqual
	i8_le  = cmp.Int8LessEqual
	f64_le = cmp.Float64LessEqual
	f32_le = cmp.Float32LessEqual

	u64_rg = cmp.Uint64Between
	u32_rg = cmp.Uint32Between
	u16_rg = cmp.Uint16Between
	u8_rg  = cmp.Uint8Between
	i64_rg = cmp.Int64Between
	i32_rg = cmp.Int32Between
	i16_rg = cmp.Int16Between
	i8_rg  = cmp.Int8Between
	f64_rg = cmp.Float64Between
	f32_rg = cmp.Float32Between

	// Virtual function pointer table for compare kernels. The purpose of this
	// table is to have fast lookup access to kernel functions without long
	// switch statements.
	//
	// 11 filter modes (0 == invalid)
	// 14 block types
	blockMatchFn = [11][15]unsafe.Pointer{
		// FilterModeInvalid
		{},
		// FilterModeEqual
		{
			nullPtr,                 // 0 BlockInvalid
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
			nullPtr,                 // 0 BlockInvalid
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
			nullPtr,                 // 0 BlockInvalid
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
			nullPtr,                 // 0 BlockInvalid
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
			nullPtr,                 // 0 BlockInvalid
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
			nullPtr,                 // 0 BlockInvalid
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
			nullPtr,                 // 0 BlockInvalid
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
		case BlockFloat32:
			return &floatInSetMatcher[float32]{}
		case BlockFloat64:
			return &floatInSetMatcher[float64]{}
		default:
			return &numInSetMatcher[T]{}
		}

	case FilterModeNotIn:
		switch f.typ {
		case BlockFloat32:
			return &floatNotInSetMatcher[float32]{}
		case BlockFloat64:
			return &floatNotInSetMatcher[float64]{}
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
// the WithValue() and Value() parts of the Matcher interface.
type numMatcher[T Number] struct {
	noopMatcher
	match numMatchFunc[T]
	val   T
	hash  filter.HashValue
}

func (m *numMatcher[T]) WithValue(v any) {
	m.val = v.(T)
	m.hash = filter.HashT(m.val)
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
		// only bloom uses hashes for all data types
		return x.ContainsHash(m.hash)
	}
	// other filters contain numeric values for integers
	return flt.Contains(uint64(m.val))
}

func (m numEqualMatcher[T]) MatchVector(b *block.Block, bits, mask *bitset.Bitset) {
	if b.IsMaterialized() {
		n := m.match(block.NewAccessor[T](b).Slice(), m.val, bits.Bytes())
		bits.ResetCount(int(n))
	} else {
		block.GetMatcher[T](b).MatchEqual(m.val, bits, mask)
	}
}

func (m numEqualMatcher[T]) MatchRangeVectors(mins, maxs *block.Block, bits, mask *bitset.Bitset) {
	// min <= v <= max, mask is optional
	f := newFactory(mins.Type())
	le, ge := f.New(FilterModeLe), f.New(FilterModeGe)
	le.WithValue(m.val)
	ge.WithValue(m.val)
	minBits := bitset.New(mins.Len())
	le.MatchVector(mins, minBits, mask)
	if mask != nil {
		minBits.And(mask)
	}
	ge.MatchVector(maxs, bits, minBits)
	bits.And(minBits)
	minBits.Close()
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

func (m numNotEqualMatcher[T]) MatchVector(b *block.Block, bits, mask *bitset.Bitset) {
	if b.IsMaterialized() {
		n := m.match(block.NewAccessor[T](b).Slice(), m.val, bits.Bytes())
		bits.ResetCount(int(n))
	} else {
		block.GetMatcher[T](b).MatchNotEqual(m.val, bits, mask)
	}
}

func (m numNotEqualMatcher[T]) MatchRangeVectors(_, _ *block.Block, bits, mask *bitset.Bitset) {
	// undecided, always true
	if mask != nil {
		bits.Copy(mask)
	} else {
		bits.One()
	}
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

func (m numGtMatcher[T]) MatchVector(b *block.Block, bits, mask *bitset.Bitset) {
	if b.IsMaterialized() {
		n := m.match(block.NewAccessor[T](b).Slice(), m.val, bits.Bytes())
		bits.ResetCount(int(n))
	} else {
		block.GetMatcher[T](b).MatchGreater(m.val, bits, mask)
	}
}

func (m numGtMatcher[T]) MatchRangeVectors(_, maxs *block.Block, bits, mask *bitset.Bitset) {
	// max > v
	gt := newFactory(maxs.Type()).New(FilterModeGt)
	gt.WithValue(m.val)
	gt.MatchVector(maxs, bits, mask)
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

func (m numGeMatcher[T]) MatchVector(b *block.Block, bits, mask *bitset.Bitset) {
	if b.IsMaterialized() {
		n := m.match(block.NewAccessor[T](b).Slice(), m.val, bits.Bytes())
		bits.ResetCount(int(n))
	} else {
		block.GetMatcher[T](b).MatchGreaterEqual(m.val, bits, mask)
	}
}

func (m numGeMatcher[T]) MatchRangeVectors(_, maxs *block.Block, bits, mask *bitset.Bitset) {
	// max >= v
	ge := newFactory(maxs.Type()).New(FilterModeGe)
	ge.WithValue(m.val)
	ge.MatchVector(maxs, bits, mask)
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

func (m numLtMatcher[T]) MatchVector(b *block.Block, bits, mask *bitset.Bitset) {
	if b.IsMaterialized() {
		n := m.match(block.NewAccessor[T](b).Slice(), m.val, bits.Bytes())
		bits.ResetCount(int(n))
	} else {
		block.GetMatcher[T](b).MatchLess(m.val, bits, mask)
	}
}

func (m numLtMatcher[T]) MatchRangeVectors(mins, _ *block.Block, bits, mask *bitset.Bitset) {
	// min < v
	lt := newFactory(mins.Type()).New(FilterModeLt)
	lt.WithValue(m.val)
	lt.MatchVector(mins, bits, mask)
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

func (m numLeMatcher[T]) MatchVector(b *block.Block, bits, mask *bitset.Bitset) {
	if b.IsMaterialized() {
		n := m.match(block.NewAccessor[T](b).Slice(), m.val, bits.Bytes())
		bits.ResetCount(int(n))
	} else {
		block.GetMatcher[T](b).MatchLessEqual(m.val, bits, mask)
	}
}

func (m numLeMatcher[T]) MatchRangeVectors(mins, _ *block.Block, bits, mask *bitset.Bitset) {
	// min <= v
	le := newFactory(mins.Type()).New(FilterModeLe)
	le.WithValue(m.val)
	le.MatchVector(mins, bits, mask)
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

func (m numRangeMatcher[T]) MatchVector(b *block.Block, bits, mask *bitset.Bitset) {
	if b.IsMaterialized() {
		n := m.match(block.NewAccessor[T](b).Slice(), m.from, m.to, bits.Bytes())
		bits.ResetCount(int(n))
	} else {
		block.GetMatcher[T](b).MatchBetween(m.from, m.to, bits, mask)
	}
}

func (m numRangeMatcher[T]) MatchFilter(flt filter.Filter) bool {
	// we don't know generally, so full scan is required
	return true
}

func (m numRangeMatcher[T]) MatchRangeVectors(mins, maxs *block.Block, bits, mask *bitset.Bitset) {
	// works with materialized and compressed blocks
	// min <= to && max >= from
	f := newFactory(mins.Type())
	le, ge := f.New(FilterModeLe), f.New(FilterModeGe)
	le.WithValue(m.to)
	ge.WithValue(m.from)
	minBits := bitset.New(mins.Len())
	le.MatchVector(mins, minBits, mask)
	if mask != nil {
		minBits.And(mask)
	}
	ge.MatchVector(maxs, bits, minBits)
	bits.And(minBits)
	minBits.Close()
}

// IN ---

// In, Contains
type numInSetMatcher[T Number] struct {
	set    *xroar.Bitmap
	hashes []filter.HashValue
}

func (m *numInSetMatcher[T]) Weight() int { return 1 }

func (m *numInSetMatcher[T]) Len() int { return m.set.Count() }

func (m *numInSetMatcher[T]) Value() any {
	// FIXME: support bitmap in optimizer
	card := m.set.Count()
	it := m.set.NewIterator()
	vals := make([]T, card)
	for i := 0; i < card; i++ {
		v, ok := it.Next()
		if !ok {
			break
		}
		vals[i] = T(v)
	}
	return vals
}

func (m *numInSetMatcher[T]) WithValue(val any) {
	m.WithSlice(val)
}

func (m *numInSetMatcher[T]) WithSlice(slice any) {
	m.set = xroar.New()
	for _, v := range slice.([]T) {
		m.set.Set(uint64(v))
	}
	m.hashes = filter.HashMulti(slice.([]T))
}

func (m *numInSetMatcher[T]) WithSet(set *xroar.Bitmap) {
	m.set = set
	card := set.Count()
	it := m.set.NewIterator()
	m.hashes = make([]filter.HashValue, card)
	for i := range card {
		v, ok := it.Next()
		if !ok {
			break
		}
		m.hashes[i] = filter.HashT(v)
	}
}

func (m numInSetMatcher[T]) MatchValue(v any) bool {
	return m.set.Contains(uint64(v.(T)))
}

func (m numInSetMatcher[T]) MatchRange(from, to any) bool {
	return m.set.ContainsRange(uint64(from.(T)), uint64(to.(T)))
}

func (m numInSetMatcher[T]) MatchFilter(flt filter.Filter) bool {
	switch x := flt.(type) {
	case *xroar.Bitmap:
		return xroar.And(m.set, x).Any()
	case *bloom.Filter:
		for _, h := range m.hashes {
			if flt.Contains(h.Uint64()) {
				return true
			}
		}
	default:
		it := m.set.NewIterator()
		for {
			v, ok := it.Next()
			if !ok {
				break
			}
			if flt.Contains(v) {
				return true
			}
		}
	}
	return false
}

func (m numInSetMatcher[T]) MatchVector(b *block.Block, bits, mask *bitset.Bitset) {
	if !b.IsMaterialized() {
		block.GetMatcher[T](b).MatchInSet(m.set, bits, mask)
		return
	}
	acc := block.NewAccessor[T](b)
	if mask != nil {
		for i := range mask.Iterator() {
			if m.set.Contains(uint64(acc.Get(i))) {
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
}

func (m numInSetMatcher[T]) MatchRangeVectors(mins, maxs *block.Block, bits, mask *bitset.Bitset) {
	// handle compressed blocks
	if !mins.IsMaterialized() || !maxs.IsMaterialized() {
		setMin, setMax := m.set.Min(), m.set.Max()
		rg := newFactory(mins.Type()).New(FilterModeRange)
		rg.WithValue(RangeValue{T(setMin), T(setMax)})
		rg.MatchRangeVectors(mins, maxs, bits, mask)
		return
	}

	// handle fully materialized blocks with raw number vectors
	minx := block.NewAccessor[T](mins).Slice()
	maxx := block.NewAccessor[T](maxs).Slice()
	if mask != nil {
		for i := range mask.Iterator() {
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
		for i := range len(minx) {
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
}

// NOT IN ---

type numNotInSetMatcher[T Number] struct {
	set *xroar.Bitmap
}

func (m *numNotInSetMatcher[T]) Weight() int { return 1 }

func (m *numNotInSetMatcher[T]) Len() int { return m.set.Count() }

func (m *numNotInSetMatcher[T]) Value() any {
	// FIXME: support bitmap in optimizer
	card := m.set.Count()
	it := m.set.NewIterator()
	vals := make([]T, card)
	for i := 0; i < card; i++ {
		v, ok := it.Next()
		if !ok {
			break
		}
		vals[i] = T(v)
	}
	return vals
}

func (m *numNotInSetMatcher[T]) WithValue(val any) {
	m.WithSlice(val)
}

func (m *numNotInSetMatcher[T]) WithSlice(slice any) {
	m.set = xroar.New()
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
	switch x := flt.(type) {
	case *xroar.Bitmap:
		return xroar.AndNot(m.set, x).Any()
	default:
		// we don't know due to false positive probability, so full scan is required
		return true
	}
}

func (m numNotInSetMatcher[T]) MatchVector(b *block.Block, bits, mask *bitset.Bitset) {
	if !b.IsMaterialized() {
		block.GetMatcher[T](b).MatchNotInSet(m.set, bits, mask)
		return
	}
	acc := block.NewAccessor[T](b)
	if mask != nil {
		for i := range mask.Iterator() {
			if !m.set.Contains(uint64(acc.Get(i))) {
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
}

func (m numNotInSetMatcher[T]) MatchRangeVectors(_, _ *block.Block, bits, mask *bitset.Bitset) {
	// undecided, always true
	if mask != nil {
		bits.Copy(mask)
	} else {
		bits.One()
	}
}
