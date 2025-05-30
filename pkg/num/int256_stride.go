// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// https://github.com/chfast/intx
// https://github.com/holiman/uint256

package num

import (
	"iter"
	"slices"

	"blockwatch.cc/knoxdb/internal/arena"
)

var _ BigIntAccessor[Int256, Int256Stride] = (*Int256Stride)(nil)

type Int256Accessor = BigIntAccessor[Int256, Int256Stride]

// represents a Int256 slice in four strides fom highest to lowest qword
// used for vector match algorithms
type Int256Stride struct {
	X0 []int64
	X1 []uint64
	X2 []uint64
	X3 []uint64
}

func NewInt256Stride(sz int) *Int256Stride {
	return &Int256Stride{
		arena.AllocInt64(sz),
		arena.AllocUint64(sz),
		arena.AllocUint64(sz),
		arena.AllocUint64(sz),
	}
}

func (s *Int256Stride) Close() {
	arena.Free(s.X0[:0])
	arena.Free(s.X1[:0])
	arena.Free(s.X2[:0])
	arena.Free(s.X3[:0])
	s.X0 = nil
	s.X1 = nil
	s.X2 = nil
	s.X3 = nil
}

func (s *Int256Stride) IsNil() bool {
	return s.X0 == nil || s.X1 == nil || s.X2 == nil || s.X3 == nil
}

func (s *Int256Stride) Get(i int) Int256 {
	return Int256{uint64(s.X0[i]), s.X1[i], s.X2[i], s.X3[i]}
}

func (s *Int256Stride) Set(i int, val Int256) {
	s.X0[i], s.X1[i], s.X2[i], s.X3[i] = int64(val[0]), val[1], val[2], val[3]
}

func (s *Int256Stride) Cmp(i, j int) int {
	switch {
	case s.X0[i] < s.X0[j]:
		return -1
	case s.X0[i] > s.X0[j]:
		return 1
	case s.X1[i] < s.X1[j]:
		return -1
	case s.X1[i] > s.X1[j]:
		return 1
	case s.X2[i] < s.X2[j]:
		return -1
	case s.X2[i] > s.X2[j]:
		return 1
	case s.X3[i] < s.X3[j]:
		return -1
	case s.X3[i] > s.X3[j]:
		return 1
	default:
		return 0
	}
}

func (s *Int256Stride) Append(val Int256) {
	s.X0 = append(s.X0, int64(val[0]))
	s.X1 = append(s.X1, val[1])
	s.X2 = append(s.X2, val[2])
	s.X3 = append(s.X3, val[3])
}

func (src *Int256Stride) AppendTo(v BigIntWriter[Int256], sel []uint32) {
	dst := v.(*Int256Stride)
	if sel == nil {
		dst.X0 = append(dst.X0, src.X0...)
		dst.X1 = append(dst.X1, src.X1...)
		dst.X2 = append(dst.X2, src.X2...)
		dst.X3 = append(dst.X3, src.X3...)
	} else {
		for v := range sel {
			dst.X0 = append(dst.X0, src.X0[int(v)])
			dst.X1 = append(dst.X1, src.X1[int(v)])
			dst.X2 = append(dst.X2, src.X2[int(v)])
			dst.X3 = append(dst.X3, src.X3[int(v)])
		}
	}
}

func (dst *Int256Stride) Delete(i, j int) {
	dst.X0 = slices.Delete(dst.X0, i, j)
	dst.X1 = slices.Delete(dst.X1, i, j)
	dst.X2 = slices.Delete(dst.X2, i, j)
	dst.X3 = slices.Delete(dst.X3, i, j)
}

func (dst *Int256Stride) Clear() {
	clear(dst.X0)
	clear(dst.X1)
	clear(dst.X2)
	clear(dst.X3)
	dst.X0 = dst.X0[:0]
	dst.X1 = dst.X1[:0]
	dst.X2 = dst.X2[:0]
	dst.X3 = dst.X3[:0]
}

func (s *Int256Stride) Swap(i, j int) {
	s.X0[i], s.X0[j] = s.X0[j], s.X0[i]
	s.X1[i], s.X1[j] = s.X1[j], s.X1[i]
	s.X2[i], s.X2[j] = s.X2[j], s.X2[i]
	s.X3[i], s.X3[j] = s.X3[j], s.X3[i]
}

func (s *Int256Stride) Len() int {
	return len(s.X0)
}

func (s *Int256Stride) Cap() int {
	return cap(s.X0)
}

func (s *Int256Stride) Size() int {
	return cap(s.X0)*32 + 96
}

func (s *Int256Stride) Min() Int256 {
	switch l := s.Len(); l {
	case 0:
		return ZeroInt256
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

func (s *Int256Stride) Max() Int256 {
	switch l := s.Len(); l {
	case 0:
		return ZeroInt256
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

func (s *Int256Stride) MinMax() (Int256, Int256) {
	var min, max Int256

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

func Int256Optimize(s []Int256) *Int256Stride {
	res := NewInt256Stride(len(s))
	res.X0 = res.X0[:len(s)]
	res.X1 = res.X1[:len(s)]
	res.X2 = res.X2[:len(s)]
	res.X3 = res.X3[:len(s)]
	for i, v := range s {
		res.X0[i] = int64(v[0])
		res.X1[i] = v[1]
		res.X2[i] = v[2]
		res.X3[i] = v[3]
	}
	return res
}

func (s *Int256Stride) Materialize() []Int256 {
	res := make([]Int256, s.Len())
	for i, v := range res {
		v[0] = uint64(s.X0[i])
		v[1] = s.X1[i]
		v[2] = s.X2[i]
		v[3] = s.X3[i]
	}
	return res
}

func (s *Int256Stride) Range(i, j int) *Int256Stride {
	return &Int256Stride{s.X0[i:j], s.X1[i:j], s.X2[i:j], s.X3[i:j]}
}

func (dst *Int256Stride) Copy(src *Int256Stride, dstPos, srcPos, n int) {
	copy(dst.X0[dstPos:], src.X0[srcPos:srcPos+n])
	copy(dst.X1[dstPos:], src.X1[srcPos:srcPos+n])
	copy(dst.X2[dstPos:], src.X2[srcPos:srcPos+n])
	copy(dst.X3[dstPos:], src.X3[srcPos:srcPos+n])
}

func (s *Int256Stride) Iterator() iter.Seq2[int, Int256] {
	return func(fn func(int, Int256) bool) {
		for i := 0; i < len(s.X0); i++ {
			if !fn(i, Int256{uint64(s.X0[i]), s.X1[i], s.X2[i], s.X3[i]}) {
				return
			}
		}
	}
}

func (s *Int256Stride) Chunks() BigIntIterator[Int256, Int256Stride] {
	return NewInt256Iterator(s)
}

func (s *Int256Stride) Matcher() BigIntMatcher[Int256] {
	// unused due to circular dependency, see query/match_i256.go
	return nil
}

func (s *Int256Stride) Slice() *Int256Stride {
	return s
}
