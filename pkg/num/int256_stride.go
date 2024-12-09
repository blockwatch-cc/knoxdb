// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// https://github.com/chfast/intx
// https://github.com/holiman/uint256

package num

// represents a Int256 slice in four strides fom highest to lowest qword
// used for vector match algorithms
type Int256Stride struct {
	X0 []int64
	X1 []uint64
	X2 []uint64
	X3 []uint64
}

func (s Int256Stride) IsNil() bool {
	return s.X0 == nil || s.X1 == nil || s.X2 == nil || s.X3 == nil
}

func (s Int256Stride) Elem(i int) Int256 {
	return Int256{uint64(s.X0[i]), s.X1[i], s.X2[i], s.X3[i]}
}

func (s Int256Stride) Set(i int, val Int256) {
	s.X0[i], s.X1[i], s.X2[i], s.X3[i] = int64(val[0]), val[1], val[2], val[3]
}

func MakeInt256Stride(sz int) Int256Stride {
	return Int256Stride{make([]int64, sz), make([]uint64, sz), make([]uint64, sz), make([]uint64, sz)}
}

func (s *Int256Stride) Append(val Int256) Int256Stride {
	s.X0 = append(s.X0, int64(val[0]))
	s.X1 = append(s.X1, val[1])
	s.X2 = append(s.X2, val[2])
	s.X3 = append(s.X3, val[3])
	return *s
}

func (dst *Int256Stride) AppendFrom(src Int256Stride) Int256Stride {
	dst.X0 = append(dst.X0, src.X0...)
	dst.X1 = append(dst.X1, src.X1...)
	dst.X2 = append(dst.X2, src.X2...)
	dst.X3 = append(dst.X3, src.X3...)
	return *dst
}

func (dst *Int256Stride) Delete(pos, n int) Int256Stride {
	dst.X0 = append(dst.X0[:pos], dst.X0[pos+n:]...)
	dst.X1 = append(dst.X1[:pos], dst.X1[pos+n:]...)
	dst.X2 = append(dst.X2[:pos], dst.X2[pos+n:]...)
	dst.X3 = append(dst.X3[:pos], dst.X3[pos+n:]...)
	return *dst
}

func (s Int256Stride) Swap(i, j int) {
	s.X0[i], s.X0[j] = s.X0[j], s.X0[i]
	s.X1[i], s.X1[j] = s.X1[j], s.X1[i]
	s.X2[i], s.X2[j] = s.X2[j], s.X2[i]
	s.X3[i], s.X3[j] = s.X3[j], s.X3[i]
}

func (s Int256Stride) Len() int {
	return len(s.X0)
}

func (s Int256Stride) Cap() int {
	return cap(s.X0)
}

func (s Int256Stride) MinMax() (Int256, Int256) {
	var min, max Int256

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

func Int256Optimize(s []Int256) Int256Stride {
	var res Int256Stride
	res.X0 = make([]int64, len(s))
	res.X1 = make([]uint64, len(s))
	res.X2 = make([]uint64, len(s))
	res.X3 = make([]uint64, len(s))
	for i, v := range s {
		res.X0[i] = int64(v[0])
		res.X1[i] = v[1]
		res.X2[i] = v[2]
		res.X3[i] = v[3]
	}
	return res
}

func (s Int256Stride) Materialize() []Int256 {
	res := make([]Int256, s.Len())
	for i, v := range res {
		v[0] = uint64(s.X0[i])
		v[1] = s.X1[i]
		v[2] = s.X2[i]
		v[3] = s.X3[i]
	}
	return res
}

func (s Int256Stride) Subslice(start, end int) Int256Stride {
	return Int256Stride{s.X0[start:end], s.X1[start:end], s.X2[start:end], s.X3[start:end]}
}

func (s Int256Stride) Tail(start int) Int256Stride {
	return Int256Stride{s.X0[start:], s.X1[start:], s.X2[start:], s.X3[start:]}
}

func (dst Int256Stride) Copy(src Int256Stride, dstPos, srcPos, n int) {
	copy(dst.X0[dstPos:], src.X0[srcPos:srcPos+n])
	copy(dst.X1[dstPos:], src.X1[srcPos:srcPos+n])
	copy(dst.X2[dstPos:], src.X2[srcPos:srcPos+n])
	copy(dst.X3[dstPos:], src.X3[srcPos:srcPos+n])
}

func (s *Int256Stride) Insert(k int, vs Int256Stride) {
	if n := s.Len() + vs.Len(); n <= s.Cap() {
		(*s) = s.Subslice(0, n)
		s.Copy(*s, k+vs.Len(), k, vs.Len()-k)
		s.Copy(vs, k, 0, vs.Len())
		return
	}
	s2 := MakeInt256Stride(s.Len() + vs.Len())
	s2.Copy(*s, 0, 0, k)
	s2.Copy(vs, k, 0, vs.Len())
	s2.Copy(*s, k+vs.Len(), k, vs.Len()-k)
	*s = s2
}
