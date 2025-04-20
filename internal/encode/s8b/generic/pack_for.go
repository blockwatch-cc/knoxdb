// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"unsafe"

	"blockwatch.cc/knoxdb/internal/types"
)

// pack_for_zero packs 128 ones from in using 1 bit each
func pack_for_zero[T types.Integer](_ unsafe.Pointer, _ uint64) uint64 {
	return 0
}

// pack_for_one packs 128 ones from in using 1 bit each
func pack_for_one[T types.Integer](_ unsafe.Pointer, _ uint64) uint64 {
	return 1 << 60
}

// pack_for_60 packs 60 values from in using 1 bit each
func pack_for_60[T types.Integer](p unsafe.Pointer, minv uint64) uint64 {
	src := (*[60]T)(p)
	return 2<<60 |
		(uint64(src[0]) - minv) |
		(uint64(src[1])-minv)<<1 |
		(uint64(src[2])-minv)<<2 |
		(uint64(src[3])-minv)<<3 |
		(uint64(src[4])-minv)<<4 |
		(uint64(src[5])-minv)<<5 |
		(uint64(src[6])-minv)<<6 |
		(uint64(src[7])-minv)<<7 |
		(uint64(src[8])-minv)<<8 |
		(uint64(src[9])-minv)<<9 |
		(uint64(src[10])-minv)<<10 |
		(uint64(src[11])-minv)<<11 |
		(uint64(src[12])-minv)<<12 |
		(uint64(src[13])-minv)<<13 |
		(uint64(src[14])-minv)<<14 |
		(uint64(src[15])-minv)<<15 |
		(uint64(src[16])-minv)<<16 |
		(uint64(src[17])-minv)<<17 |
		(uint64(src[18])-minv)<<18 |
		(uint64(src[19])-minv)<<19 |
		(uint64(src[20])-minv)<<20 |
		(uint64(src[21])-minv)<<21 |
		(uint64(src[22])-minv)<<22 |
		(uint64(src[23])-minv)<<23 |
		(uint64(src[24])-minv)<<24 |
		(uint64(src[25])-minv)<<25 |
		(uint64(src[26])-minv)<<26 |
		(uint64(src[27])-minv)<<27 |
		(uint64(src[28])-minv)<<28 |
		(uint64(src[29])-minv)<<29 |
		(uint64(src[30])-minv)<<30 |
		(uint64(src[31])-minv)<<31 |
		(uint64(src[32])-minv)<<32 |
		(uint64(src[33])-minv)<<33 |
		(uint64(src[34])-minv)<<34 |
		(uint64(src[35])-minv)<<35 |
		(uint64(src[36])-minv)<<36 |
		(uint64(src[37])-minv)<<37 |
		(uint64(src[38])-minv)<<38 |
		(uint64(src[39])-minv)<<39 |
		(uint64(src[40])-minv)<<40 |
		(uint64(src[41])-minv)<<41 |
		(uint64(src[42])-minv)<<42 |
		(uint64(src[43])-minv)<<43 |
		(uint64(src[44])-minv)<<44 |
		(uint64(src[45])-minv)<<45 |
		(uint64(src[46])-minv)<<46 |
		(uint64(src[47])-minv)<<47 |
		(uint64(src[48])-minv)<<48 |
		(uint64(src[49])-minv)<<49 |
		(uint64(src[50])-minv)<<50 |
		(uint64(src[51])-minv)<<51 |
		(uint64(src[52])-minv)<<52 |
		(uint64(src[53])-minv)<<53 |
		(uint64(src[54])-minv)<<54 |
		(uint64(src[55])-minv)<<55 |
		(uint64(src[56])-minv)<<56 |
		(uint64(src[57])-minv)<<57 |
		(uint64(src[58])-minv)<<58 |
		(uint64(src[59])-minv)<<59
}

// pack_for_30 packs 30 values from in using 2 bits each
func pack_for_30[T types.Integer](p unsafe.Pointer, minv uint64) uint64 {
	src := (*[30]T)(p)
	return 3<<60 |
		(uint64(src[0]) - minv) |
		(uint64(src[1])-minv)<<2 |
		(uint64(src[2])-minv)<<4 |
		(uint64(src[3])-minv)<<6 |
		(uint64(src[4])-minv)<<8 |
		(uint64(src[5])-minv)<<10 |
		(uint64(src[6])-minv)<<12 |
		(uint64(src[7])-minv)<<14 |
		(uint64(src[8])-minv)<<16 |
		(uint64(src[9])-minv)<<18 |
		(uint64(src[10])-minv)<<20 |
		(uint64(src[11])-minv)<<22 |
		(uint64(src[12])-minv)<<24 |
		(uint64(src[13])-minv)<<26 |
		(uint64(src[14])-minv)<<28 |
		(uint64(src[15])-minv)<<30 |
		(uint64(src[16])-minv)<<32 |
		(uint64(src[17])-minv)<<34 |
		(uint64(src[18])-minv)<<36 |
		(uint64(src[19])-minv)<<38 |
		(uint64(src[20])-minv)<<40 |
		(uint64(src[21])-minv)<<42 |
		(uint64(src[22])-minv)<<44 |
		(uint64(src[23])-minv)<<46 |
		(uint64(src[24])-minv)<<48 |
		(uint64(src[25])-minv)<<50 |
		(uint64(src[26])-minv)<<52 |
		(uint64(src[27])-minv)<<54 |
		(uint64(src[28])-minv)<<56 |
		(uint64(src[29])-minv)<<58
}

// pack_for_20 packs 20 values from in using 3 bits each
func pack_for_20[T types.Integer](p unsafe.Pointer, minv uint64) uint64 {
	src := (*[20]T)(p)
	return 4<<60 |
		(uint64(src[0]) - minv) |
		(uint64(src[1])-minv)<<3 |
		(uint64(src[2])-minv)<<6 |
		(uint64(src[3])-minv)<<9 |
		(uint64(src[4])-minv)<<12 |
		(uint64(src[5])-minv)<<15 |
		(uint64(src[6])-minv)<<18 |
		(uint64(src[7])-minv)<<21 |
		(uint64(src[8])-minv)<<24 |
		(uint64(src[9])-minv)<<27 |
		(uint64(src[10])-minv)<<30 |
		(uint64(src[11])-minv)<<33 |
		(uint64(src[12])-minv)<<36 |
		(uint64(src[13])-minv)<<39 |
		(uint64(src[14])-minv)<<42 |
		(uint64(src[15])-minv)<<45 |
		(uint64(src[16])-minv)<<48 |
		(uint64(src[17])-minv)<<51 |
		(uint64(src[18])-minv)<<54 |
		(uint64(src[19])-minv)<<57
}

// pack_for_15 packs 15 values from in using 3 bits each
func pack_for_15[T types.Integer](p unsafe.Pointer, minv uint64) uint64 {
	src := (*[15]T)(p)
	return 5<<60 |
		(uint64(src[0]) - minv) |
		(uint64(src[1])-minv)<<4 |
		(uint64(src[2])-minv)<<8 |
		(uint64(src[3])-minv)<<12 |
		(uint64(src[4])-minv)<<16 |
		(uint64(src[5])-minv)<<20 |
		(uint64(src[6])-minv)<<24 |
		(uint64(src[7])-minv)<<28 |
		(uint64(src[8])-minv)<<32 |
		(uint64(src[9])-minv)<<36 |
		(uint64(src[10])-minv)<<40 |
		(uint64(src[11])-minv)<<44 |
		(uint64(src[12])-minv)<<48 |
		(uint64(src[13])-minv)<<52 |
		(uint64(src[14])-minv)<<56
}

// pack_for_12 packs 12 values from in using 5 bits each
func pack_for_12[T types.Integer](p unsafe.Pointer, minv uint64) uint64 {
	src := (*[12]T)(p)
	return 6<<60 |
		(uint64(src[0]) - minv) |
		(uint64(src[1])-minv)<<5 |
		(uint64(src[2])-minv)<<10 |
		(uint64(src[3])-minv)<<15 |
		(uint64(src[4])-minv)<<20 |
		(uint64(src[5])-minv)<<25 |
		(uint64(src[6])-minv)<<30 |
		(uint64(src[7])-minv)<<35 |
		(uint64(src[8])-minv)<<40 |
		(uint64(src[9])-minv)<<45 |
		(uint64(src[10])-minv)<<50 |
		(uint64(src[11])-minv)<<55
}

// pack_for_10 packs 10 values from in using 6 bits each
func pack_for_10[T types.Integer](p unsafe.Pointer, minv uint64) uint64 {
	src := (*[10]T)(p)
	return 7<<60 |
		(uint64(src[0]) - minv) |
		(uint64(src[1])-minv)<<6 |
		(uint64(src[2])-minv)<<12 |
		(uint64(src[3])-minv)<<18 |
		(uint64(src[4])-minv)<<24 |
		(uint64(src[5])-minv)<<30 |
		(uint64(src[6])-minv)<<36 |
		(uint64(src[7])-minv)<<42 |
		(uint64(src[8])-minv)<<48 |
		(uint64(src[9])-minv)<<54
}

// pack_for_8 packs 8 values from in using 7 bits each
func pack_for_8[T types.Integer](p unsafe.Pointer, minv uint64) uint64 {
	src := (*[8]T)(p)
	return 8<<60 |
		(uint64(src[0]) - minv) |
		(uint64(src[1])-minv)<<7 |
		(uint64(src[2])-minv)<<14 |
		(uint64(src[3])-minv)<<21 |
		(uint64(src[4])-minv)<<28 |
		(uint64(src[5])-minv)<<35 |
		(uint64(src[6])-minv)<<42 |
		(uint64(src[7])-minv)<<49
}

// pack_for_7 packs 7 values from in using 8 bits each
func pack_for_7[T types.Integer](p unsafe.Pointer, minv uint64) uint64 {
	src := (*[7]T)(p)
	return 9<<60 |
		(uint64(src[0]) - minv) |
		(uint64(src[1])-minv)<<8 |
		(uint64(src[2])-minv)<<16 |
		(uint64(src[3])-minv)<<24 |
		(uint64(src[4])-minv)<<32 |
		(uint64(src[5])-minv)<<40 |
		(uint64(src[6])-minv)<<48
}

// pack_for_6 packs 6 values from in using 10 bits each
func pack_for_6[T types.Integer](p unsafe.Pointer, minv uint64) uint64 {
	src := (*[6]T)(p)
	return 10<<60 |
		(uint64(src[0]) - minv) |
		(uint64(src[1])-minv)<<10 |
		(uint64(src[2])-minv)<<20 |
		(uint64(src[3])-minv)<<30 |
		(uint64(src[4])-minv)<<40 |
		(uint64(src[5])-minv)<<50
}

// pack_for_5 packs 5 values from in using 12 bits each
func pack_for_5[T types.Integer](p unsafe.Pointer, minv uint64) uint64 {
	src := (*[5]T)(p)
	return 11<<60 |
		(uint64(src[0]) - minv) |
		(uint64(src[1])-minv)<<12 |
		(uint64(src[2])-minv)<<24 |
		(uint64(src[3])-minv)<<36 |
		(uint64(src[4])-minv)<<48
}

// pack_for_4 packs 4 values from in using 15 bits each
func pack_for_4[T types.Integer](p unsafe.Pointer, minv uint64) uint64 {
	src := (*[4]T)(p)
	return 12<<60 |
		(uint64(src[0]) - minv) |
		(uint64(src[1])-minv)<<15 |
		(uint64(src[2])-minv)<<30 |
		(uint64(src[3])-minv)<<45
}

// pack_for_3 packs 3 values from in using 20 bits each
func pack_for_3[T types.Integer](p unsafe.Pointer, minv uint64) uint64 {
	src := (*[3]T)(p)
	return 13<<60 |
		(uint64(src[0]) - minv) |
		(uint64(src[1])-minv)<<20 |
		(uint64(src[2])-minv)<<40
}

// pack_for_2 packs 2 values from in using 30 bits each
func pack_for_2[T types.Integer](p unsafe.Pointer, minv uint64) uint64 {
	src := (*[2]T)(p)
	return 14<<60 |
		(uint64(src[0]) - minv) |
		(uint64(src[1])-minv)<<30
}

// pack_for_1 packs 1 values from in using 60 bits each
func pack_for_1[T types.Integer](p unsafe.Pointer, minv uint64) uint64 {
	src := (*[1]T)(p)
	return 15<<60 |
		(uint64(src[0]) - minv)
}
