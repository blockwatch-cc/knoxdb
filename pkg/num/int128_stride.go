// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package num

// represents a Int128 slice in two strides for higher and lower qword
// used for vector match algorithms
type Int128Stride struct {
	X0 []int64
	X1 []uint64
}

func (s Int128Stride) IsNil() bool {
	return s.X0 == nil || s.X1 == nil
}

func (s Int128Stride) Elem(i int) Int128 {
	return Int128{uint64(s.X0[i]), s.X1[i]}
}

func (s Int128Stride) Set(i int, val Int128) {
	s.X0[i], s.X1[i] = int64(val[0]), val[1]
}

func MakeInt128Stride(sz int) Int128Stride {
	return Int128Stride{make([]int64, sz), make([]uint64, sz)}
}

func (s *Int128Stride) Append(val Int128) Int128Stride {
	s.X0 = append(s.X0, int64(val[0]))
	s.X1 = append(s.X1, val[1])
	return *s
}

func (dst *Int128Stride) AppendFrom(src Int128Stride) Int128Stride {
	dst.X0 = append(dst.X0, src.X0...)
	dst.X1 = append(dst.X1, src.X1...)
	return *dst
}

func (dst *Int128Stride) Delete(pos, n int) Int128Stride {
	dst.X0 = append(dst.X0[:pos], dst.X0[pos+n:]...)
	dst.X1 = append(dst.X1[:pos], dst.X1[pos+n:]...)
	return *dst
}

func (s Int128Stride) Swap(i, j int) {
	s.X0[i], s.X0[j] = s.X0[j], s.X0[i]
	s.X1[i], s.X1[j] = s.X1[j], s.X1[i]
}

func (s Int128Stride) Len() int {
	return len(s.X0)
}

func (s Int128Stride) Cap() int {
	return cap(s.X0)
}

func (s Int128Stride) MinMax() (Int128, Int128) {
	var min, max Int128

	switch l := s.Len(); l {
	case 0:
		// nothing
	case 1:
		min, max = s.Elem(0), s.Elem(0)
	default:
		// If there is more than one element, then initialize min and max
		s0 := s.Elem(0)
		s1 := s.Elem(1)
		if s0.Lt(s1) {
			max = s0
			min = s1
		} else {
			max = s1
			min = s0
		}

		for i := 2; i < l; i++ {
			si := s.Elem(i)
			if si.Gt(max) {
				max = si
			} else if si.Lt(min) {
				min = si
			}
		}
	}

	return min, max
}

func Int128Optimize(s []Int128) Int128Stride {
	var res Int128Stride
	res.X0 = make([]int64, len(s))
	res.X1 = make([]uint64, len(s))
	for i, v := range s {
		res.X0[i] = int64(v[0])
		res.X1[i] = v[1]
	}
	return res
}

func (s Int128Stride) Materialize() []Int128 {
	res := make([]Int128, s.Len())
	for i, v := range res {
		v[0] = uint64(s.X0[i])
		v[1] = s.X1[i]
	}
	return res
}

func (s Int128Stride) Subslice(start, end int) Int128Stride {
	return Int128Stride{s.X0[start:end], s.X1[start:end]}
}

func (s Int128Stride) Tail(start int) Int128Stride {
	return Int128Stride{s.X0[start:], s.X1[start:]}
}

func (dst Int128Stride) Copy(src Int128Stride, dstPos, srcPos, n int) {
	copy(dst.X0[dstPos:], src.X0[srcPos:srcPos+n])
	copy(dst.X1[dstPos:], src.X1[srcPos:srcPos+n])
}

func (s *Int128Stride) Insert(k int, vs Int128Stride) {
	if n := s.Len() + vs.Len(); n <= s.Cap() {
		(*s) = s.Subslice(0, n)
		s.Copy(*s, k+vs.Len(), k, vs.Len()-k)
		s.Copy(vs, k, 0, vs.Len())
		return
	}
	s2 := MakeInt128Stride(s.Len() + vs.Len())
	s2.Copy(*s, 0, 0, k)
	s2.Copy(vs, k, 0, vs.Len())
	s2.Copy(*s, k+vs.Len(), k, vs.Len()-k)
	*s = s2
}
