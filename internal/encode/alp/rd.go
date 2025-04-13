// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package alp

import (
	"math"
	"math/bits"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/encode/hashprobe"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
)

// Note: keep these calculations in sync with integer container costs
func dictCosts(n, w, c int) int {
	return 1 + bitPackCosts(n, bits.Len(uint(c-1))) + bitPackCosts(c, w)
}

func bitPackCosts(n, w int) int {
	return 2 + 2*num.MaxVarintLen32 + (n*w+7)/8
}

func EstimateShift[T types.Float](sample []T, unique []uint16) int {
	var (
		bestShift int
		bestSize  int = math.MaxInt32
		sz        int = len(sample)
		w         int = int(unsafe.Sizeof(T(0)))
	)
	for i := 1; i <= 16; i++ {
		shift := 64 - i
		mask := uint64(1<<shift - 1)

		var (
			lmin, lmax uint16 = 0, math.MaxUint16
			rmin, rmax uint64 = 0, math.MaxUint64
		)

		switch w {
		case 8:
			// min/max
			for _, v := range util.ReinterpretSlice[T, uint64](sample) {
				l, r := uint16(v>>shift), v&mask
				if l < lmin {
					lmin = l
				} else if l > lmax {
					lmax = l
				}
				if r < rmin {
					rmin = r
				} else if r > rmax {
					rmax = r
				}
			}
			// mark uniques
			for _, v := range util.ReinterpretSlice[T, uint64](sample) {
				unique[uint16(v>>shift)-lmin] = 1
			}

		case 4:
			// min/max
			for _, v := range util.ReinterpretSlice[T, uint32](sample) {
				l, r := uint16(v>>shift), uint64(v)&mask
				if l < lmin {
					lmin = l
				} else if l > lmax {
					lmax = l
				}
				if r < rmin {
					rmin = r
				} else if r > rmax {
					rmax = r
				}
				unique[l] = 1
			}
			// mark uniques
			for _, v := range util.ReinterpretSlice[T, uint32](sample) {
				unique[uint16(v>>shift)-lmin] = 1
			}
		}

		// count uniques
		var lunique int
		for _, v := range unique[:lmax-lmin+1] {
			lunique = util.Bool2int(v > 0)
		}
		lbits, rbits := bits.Len16(lmax-lmin), bits.Len64(rmax-rmin)

		// estimate encoded size
		// - left side may be dict compressed
		// - right side will be bitpacked
		ldcost := dictCosts(sz, lbits, lunique)
		lbcost := bitPackCosts(sz, lbits)

		var maxSz int
		if lunique <= hashprobe.MAX_DICT_LIMIT && ldcost < lbcost {
			maxSz += ldcost
		} else {
			maxSz += lbcost
		}
		maxSz += bitPackCosts(sz, rbits) // bitpack only

		// compare against previous know best ratio and keep best containers
		if maxSz <= bestSize {
			bestSize = maxSz
			bestShift = shift
		}

		// cleanup
		clear(unique[:lmax-lmin+1])
	}
	return bestShift
}

func Split[T types.Float](src []T, left []uint16, right []uint64, shift int) {
	switch unsafe.Sizeof(T(0)) {
	case 4:
		u32 := util.ReinterpretSlice[T, uint32](src)
		split32(u32, left, right, shift)
	case 8:
		u64 := util.ReinterpretSlice[T, uint64](src)
		split64(u64, left, right, shift)
	}
}

func split64(src []uint64, left []uint16, right []uint64, shift int) {
	if len(src) == 0 {
		return
	}
	var i int
	sp := unsafe.Pointer(&src[0])
	lp := unsafe.Pointer(&left[0])
	rp := unsafe.Pointer(&right[0])
	for range len(src) / 128 {
		s := (*[128]uint64)(unsafe.Add(sp, i*8))
		l := (*[128]uint16)(unsafe.Add(lp, i*2))
		r := (*[128]uint64)(unsafe.Add(rp, i*8))
		splitCore64(s, l, r, shift)
		i += 128
	}

	mask := uint64(1<<shift) - 1
	for i < len(src) {
		val := src[i]
		left[i] = uint16(val >> shift)
		right[i] = val & mask
		i++
	}
}

func splitCore64(src *[128]uint64, left *[128]uint16, right *[128]uint64, shift int) {
	mask := uint64(1<<shift) - 1
	for i := 0; i < len(src); i += 16 {
		v0 := src[i]
		left[i], right[i] = uint16(v0>>shift), v0&mask
		v1 := src[i+1]
		left[i+1], right[i+1] = uint16(v1>>shift), v1&mask
		v2 := src[i+2]
		left[i+2], right[i+2] = uint16(v2>>shift), v2&mask
		v3 := src[i+3]
		left[i+3], right[i+3] = uint16(v3>>shift), v3&mask
		v4 := src[i+4]
		left[i+4], right[i+4] = uint16(v4>>shift), v4&mask
		v5 := src[i+5]
		left[i+5], right[i+5] = uint16(v5>>shift), v5&mask
		v6 := src[i+6]
		left[i+6], right[i+6] = uint16(v6>>shift), v6&mask
		v7 := src[i+7]
		left[i+7], right[i+7] = uint16(v7>>shift), v7&mask
		v8 := src[i+8]
		left[i+8], right[i+8] = uint16(v8>>shift), v8&mask
		v9 := src[i+9]
		left[i+9], right[i+9] = uint16(v9>>shift), v9&mask
		v10 := src[i+10]
		left[i+10], right[i+10] = uint16(v10>>shift), v10&mask
		v11 := src[i+11]
		left[i+11], right[i+11] = uint16(v11>>shift), v11&mask
		v12 := src[i+12]
		left[i+12], right[i+12] = uint16(v12>>shift), v12&mask
		v13 := src[i+13]
		left[i+13], right[i+13] = uint16(v13>>shift), v13&mask
		v14 := src[i+14]
		left[i+14], right[i+14] = uint16(v14>>shift), v14&mask
		v15 := src[i+15]
		left[i+15], right[i+15] = uint16(v15>>shift), v15&mask
	}
}

func split32(src []uint32, left []uint16, right []uint64, shift int) {
	if len(src) == 0 {
		return
	}
	var i int
	sp := unsafe.Pointer(&src[0])
	lp := unsafe.Pointer(&left[0])
	rp := unsafe.Pointer(&right[0])
	for range len(src) / 128 {
		s := (*[128]uint32)(unsafe.Add(sp, i*4))
		l := (*[128]uint16)(unsafe.Add(lp, i*2))
		r := (*[128]uint64)(unsafe.Add(rp, i*8))
		splitCore32(s, l, r, shift)
		i += 128
	}

	mask := uint32(1<<shift) - 1
	for i < len(src) {
		val := src[i]
		left[i] = uint16(val >> shift)
		right[i] = uint64(val & mask)
		i++
	}
}

func splitCore32(src *[128]uint32, left *[128]uint16, right *[128]uint64, shift int) {
	mask := uint32(1<<shift) - 1
	for i := 0; i < len(src); i += 16 {
		v0 := src[i]
		left[i], right[i] = uint16(v0>>shift), uint64(v0&mask)
		v1 := src[i+1]
		left[i+1], right[i+1] = uint16(v1>>shift), uint64(v1&mask)
		v2 := src[i+2]
		left[i+2], right[i+2] = uint16(v2>>shift), uint64(v2&mask)
		v3 := src[i+3]
		left[i+3], right[i+3] = uint16(v3>>shift), uint64(v3&mask)
		v4 := src[i+4]
		left[i+4], right[i+4] = uint16(v4>>shift), uint64(v4&mask)
		v5 := src[i+5]
		left[i+5], right[i+5] = uint16(v5>>shift), uint64(v5&mask)
		v6 := src[i+6]
		left[i+6], right[i+6] = uint16(v6>>shift), uint64(v6&mask)
		v7 := src[i+7]
		left[i+7], right[i+7] = uint16(v7>>shift), uint64(v7&mask)
		v8 := src[i+8]
		left[i+8], right[i+8] = uint16(v8>>shift), uint64(v8&mask)
		v9 := src[i+9]
		left[i+9], right[i+9] = uint16(v9>>shift), uint64(v9&mask)
		v10 := src[i+10]
		left[i+10], right[i+10] = uint16(v10>>shift), uint64(v10&mask)
		v11 := src[i+11]
		left[i+11], right[i+11] = uint16(v11>>shift), uint64(v11&mask)
		v12 := src[i+12]
		left[i+12], right[i+12] = uint16(v12>>shift), uint64(v12&mask)
		v13 := src[i+13]
		left[i+13], right[i+13] = uint16(v13>>shift), uint64(v13&mask)
		v14 := src[i+14]
		left[i+14], right[i+14] = uint16(v14>>shift), uint64(v14&mask)
		v15 := src[i+15]
		left[i+15], right[i+15] = uint16(v15>>shift), uint64(v15&mask)
	}
}

func Merge[T types.Float](dst []T, left []uint16, right []uint64, shift int) {
	switch unsafe.Sizeof(T(0)) {
	case 4:
		u32 := util.ReinterpretSlice[T, uint32](dst)
		merge32(u32, left, right, shift)
	case 8:
		u64 := util.ReinterpretSlice[T, uint64](dst)
		merge64(u64, left, right, shift)
	}
}

func merge64(dst []uint64, left []uint16, right []uint64, shift int) {
	if len(dst) == 0 {
		return
	}
	var i int
	dp := unsafe.Pointer(&dst[0])
	lp := unsafe.Pointer(&left[0])
	rp := unsafe.Pointer(&right[0])
	for range len(dst) / 128 {
		d := (*[128]uint64)(unsafe.Add(dp, i*8))
		l := (*[128]uint16)(unsafe.Add(lp, i*2))
		r := (*[128]uint64)(unsafe.Add(rp, i*8))
		mergeCore64(d, l, r, shift)
		i += 128
	}

	for i < len(dst) {
		dst[i] = uint64(left[i]<<shift) | right[i]
		i++
	}
}

func mergeCore64(dst *[128]uint64, left *[128]uint16, right *[128]uint64, shift int) {
	for i := 0; i < len(dst); i += 16 {
		dst[i] = uint64(left[i])<<shift | right[i]
		dst[i+1] = uint64(left[i+1])<<shift | right[i+1]
		dst[i+2] = uint64(left[i+2])<<shift | right[i+2]
		dst[i+3] = uint64(left[i+3])<<shift | right[i+3]
		dst[i+4] = uint64(left[i+4])<<shift | right[i+4]
		dst[i+5] = uint64(left[i+5])<<shift | right[i+5]
		dst[i+6] = uint64(left[i+6])<<shift | right[i+6]
		dst[i+7] = uint64(left[i+7])<<shift | right[i+7]
		dst[i+8] = uint64(left[i+8])<<shift | right[i+8]
		dst[i+9] = uint64(left[i+9])<<shift | right[i+9]
		dst[i+10] = uint64(left[i+10])<<shift | right[i+10]
		dst[i+11] = uint64(left[i+11])<<shift | right[i+11]
		dst[i+12] = uint64(left[i+12])<<shift | right[i+12]
		dst[i+13] = uint64(left[i+13])<<shift | right[i+13]
		dst[i+14] = uint64(left[i+14])<<shift | right[i+14]
		dst[i+15] = uint64(left[i+15])<<shift | right[i+15]
	}
}

func merge32(dst []uint32, left []uint16, right []uint64, shift int) {
	if len(dst) == 0 {
		return
	}
	var i int
	dp := unsafe.Pointer(&dst[0])
	lp := unsafe.Pointer(&left[0])
	rp := unsafe.Pointer(&right[0])
	for range len(dst) / 128 {
		d := (*[128]uint32)(unsafe.Add(dp, i*4))
		l := (*[128]uint16)(unsafe.Add(lp, i*2))
		r := (*[128]uint64)(unsafe.Add(rp, i*8))
		mergeCore32(d, l, r, shift)
		i += 128
	}

	for i < len(dst) {
		dst[i] = uint32(left[i]<<shift) | uint32(right[i])
		i++
	}
}

func mergeCore32(dst *[128]uint32, left *[128]uint16, right *[128]uint64, shift int) {
	for i := 0; i < len(dst); i += 16 {
		dst[i] = uint32(left[i]<<shift) | uint32(right[i])
		dst[i+1] = uint32(left[i+1]<<shift) | uint32(right[i+1])
		dst[i+2] = uint32(left[i+2]<<shift) | uint32(right[i+2])
		dst[i+3] = uint32(left[i+3]<<shift) | uint32(right[i+3])
		dst[i+4] = uint32(left[i+4]<<shift) | uint32(right[i+4])
		dst[i+5] = uint32(left[i+5]<<shift) | uint32(right[i+5])
		dst[i+6] = uint32(left[i+6]<<shift) | uint32(right[i+6])
		dst[i+7] = uint32(left[i+7]<<shift) | uint32(right[i+7])
		dst[i+8] = uint32(left[i+8]<<shift) | uint32(right[i+8])
		dst[i+9] = uint32(left[i+9]<<shift) | uint32(right[i+9])
		dst[i+10] = uint32(left[i+10]<<shift) | uint32(right[i+10])
		dst[i+11] = uint32(left[i+11]<<shift) | uint32(right[i+11])
		dst[i+12] = uint32(left[i+12]<<shift) | uint32(right[i+12])
		dst[i+13] = uint32(left[i+13]<<shift) | uint32(right[i+13])
		dst[i+14] = uint32(left[i+14]<<shift) | uint32(right[i+14])
		dst[i+15] = uint32(left[i+15]<<shift) | uint32(right[i+15])
	}
}
