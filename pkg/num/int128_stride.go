// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package num

import (
	"iter"
	"slices"

	"blockwatch.cc/knoxdb/internal/arena"
)

var _ BigIntAccessor[Int128, Int128Stride] = (*Int128Stride)(nil)

type Int128Accessor = BigIntAccessor[Int128, Int128Stride]

// represents a Int128 slice in two strides for higher and lower qword
// used for vector match algorithms
type Int128Stride struct {
	X0 []int64
	X1 []uint64
}

func NewInt128Stride(sz int) *Int128Stride {
	return &Int128Stride{
		arena.AllocInt64(sz),
		arena.AllocUint64(sz),
	}
}

func (s *Int128Stride) Close() {
	arena.Free(s.X0[:0])
	arena.Free(s.X1[:0])
	s.X0 = nil
	s.X1 = nil
}

func (s *Int128Stride) IsNil() bool {
	return s.X0 == nil || s.X1 == nil
}

func (s *Int128Stride) Get(i int) Int128 {
	return Int128{uint64(s.X0[i]), s.X1[i]}
}

func (s *Int128Stride) Set(i int, val Int128) {
	s.X0[i], s.X1[i] = int64(val[0]), val[1]
}

func (s *Int128Stride) Cmp(i, j int) int {
	switch {
	case s.X0[i] < s.X0[j]:
		return -1
	case s.X0[i] > s.X0[j]:
		return 1
	case s.X1[i] < s.X1[j]:
		return -1
	case s.X1[i] > s.X1[j]:
		return 1
	default:
		return 0
	}
}

func (s *Int128Stride) Append(val Int128) int {
	s.X0 = append(s.X0, int64(val[0]))
	s.X1 = append(s.X1, val[1])
	return len(s.X0) - 1
}

func (src *Int128Stride) AppendTo(v BigIntWriter[Int128], sel []uint32) {
	dst := v.(*Int128Stride)
	if sel == nil {
		dst.X0 = append(dst.X0, src.X0...)
		dst.X1 = append(dst.X1, src.X1...)
	} else {
		for v := range sel {
			dst.X0 = append(dst.X0, src.X0[v])
			dst.X1 = append(dst.X1, src.X1[v])
		}
	}
}

func (dst *Int128Stride) Delete(i, j int) {
	dst.X0 = slices.Delete(dst.X0, i, j)
	dst.X1 = slices.Delete(dst.X1, i, j)
}

func (dst *Int128Stride) Clear() {
	clear(dst.X0)
	clear(dst.X1)
	dst.X0 = dst.X0[:0]
	dst.X1 = dst.X1[:0]
}

func (s *Int128Stride) Swap(i, j int) {
	s.X0[i], s.X0[j] = s.X0[j], s.X0[i]
	s.X1[i], s.X1[j] = s.X1[j], s.X1[i]
}

func (s *Int128Stride) Len() int {
	return len(s.X0)
}

func (s *Int128Stride) Cap() int {
	return cap(s.X0)
}

func (s *Int128Stride) Size() int {
	return cap(s.X0) * 16 * 48
}

func (s *Int128Stride) Min() Int128 {
	switch l := s.Len(); l {
	case 0:
		return ZeroInt128
	case 1:
		return s.Get(0)
	default:
		s0 := s.Get(0)
		for i := 2; i < l; i++ {
			si := s.Get(i)
			if si.Lt(s0) {
				s0 = si
			}
		}
		return s0
	}
}

func (s *Int128Stride) Max() Int128 {
	switch l := s.Len(); l {
	case 0:
		return ZeroInt128
	case 1:
		return s.Get(0)
	default:
		s0 := s.Get(0)
		for i := 2; i < l; i++ {
			si := s.Get(i)
			if si.Gt(s0) {
				s0 = si
			}
		}
		return s0
	}
}

func (s *Int128Stride) MinMax() (Int128, Int128) {
	var min, max Int128

	switch l := s.Len(); l {
	case 0:
		// nothing
	case 1:
		min, max = s.Get(0), s.Get(0)
	default:
		// If there is more than one element, then initialize min and max
		s0 := s.Get(0)
		s1 := s.Get(1)
		if s0.Lt(s1) {
			max = s1
			min = s0
		} else {
			max = s0
			min = s1
		}

		for i := 2; i < l; i++ {
			si := s.Get(i)
			if si.Gt(max) {
				max = si
			} else if si.Lt(min) {
				min = si
			}
		}
	}

	return min, max
}

func Int128Optimize(s []Int128) *Int128Stride {
	res := NewInt128Stride(len(s))
	res.X0 = res.X0[:len(s)]
	res.X1 = res.X1[:len(s)]
	for i, v := range s {
		res.X0[i] = int64(v[0])
		res.X1[i] = v[1]
	}
	return res
}

func (s *Int128Stride) Materialize() []Int128 {
	res := make([]Int128, s.Len())
	for i, v := range res {
		v[0] = uint64(s.X0[i])
		v[1] = s.X1[i]
	}
	return res
}

func (s *Int128Stride) Range(i, j int) *Int128Stride {
	return &Int128Stride{s.X0[i:j], s.X1[i:j]}
}

func (dst *Int128Stride) Copy(src *Int128Stride, dstPos, srcPos, n int) {
	copy(dst.X0[dstPos:], src.X0[srcPos:srcPos+n])
	copy(dst.X1[dstPos:], src.X1[srcPos:srcPos+n])
}

func (s *Int128Stride) Iterator() iter.Seq2[int, Int128] {
	return func(fn func(int, Int128) bool) {
		for i := 0; i < len(s.X0); i++ {
			if !fn(i, s.Get(i)) {
				return
			}
		}
	}
}

func (s *Int128Stride) Chunks() BigIntIterator[Int128, Int128Stride] {
	return NewInt128Iterator(s)
}

func (s *Int128Stride) Matcher() BigIntMatcher[Int128] {
	// unused due to circular dependency, see query/match_i128.go
	return nil
}

func (s *Int128Stride) Slice() *Int128Stride {
	return s
}
