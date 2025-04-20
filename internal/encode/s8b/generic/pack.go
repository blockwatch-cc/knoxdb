// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"unsafe"

	"blockwatch.cc/knoxdb/internal/types"
)

// pack_zero packs 128 ones from in using 1 bit each
func pack_zero[T types.Integer](_ unsafe.Pointer, _ uint64) uint64 {
	return 0
}

// pack_one packs 128 ones from in using 1 bit each
func pack_one[T types.Integer](_ unsafe.Pointer, _ uint64) uint64 {
	return 1 << 60
}

// pack_60 packs 60 values from in using 1 bit each
func pack_60[T types.Integer](p unsafe.Pointer, _ uint64) uint64 {
	src := (*[60]T)(p)
	return 2<<60 |
		uint64(src[0]) |
		uint64(src[1])<<1 |
		uint64(src[2])<<2 |
		uint64(src[3])<<3 |
		uint64(src[4])<<4 |
		uint64(src[5])<<5 |
		uint64(src[6])<<6 |
		uint64(src[7])<<7 |
		uint64(src[8])<<8 |
		uint64(src[9])<<9 |
		uint64(src[10])<<10 |
		uint64(src[11])<<11 |
		uint64(src[12])<<12 |
		uint64(src[13])<<13 |
		uint64(src[14])<<14 |
		uint64(src[15])<<15 |
		uint64(src[16])<<16 |
		uint64(src[17])<<17 |
		uint64(src[18])<<18 |
		uint64(src[19])<<19 |
		uint64(src[20])<<20 |
		uint64(src[21])<<21 |
		uint64(src[22])<<22 |
		uint64(src[23])<<23 |
		uint64(src[24])<<24 |
		uint64(src[25])<<25 |
		uint64(src[26])<<26 |
		uint64(src[27])<<27 |
		uint64(src[28])<<28 |
		uint64(src[29])<<29 |
		uint64(src[30])<<30 |
		uint64(src[31])<<31 |
		uint64(src[32])<<32 |
		uint64(src[33])<<33 |
		uint64(src[34])<<34 |
		uint64(src[35])<<35 |
		uint64(src[36])<<36 |
		uint64(src[37])<<37 |
		uint64(src[38])<<38 |
		uint64(src[39])<<39 |
		uint64(src[40])<<40 |
		uint64(src[41])<<41 |
		uint64(src[42])<<42 |
		uint64(src[43])<<43 |
		uint64(src[44])<<44 |
		uint64(src[45])<<45 |
		uint64(src[46])<<46 |
		uint64(src[47])<<47 |
		uint64(src[48])<<48 |
		uint64(src[49])<<49 |
		uint64(src[50])<<50 |
		uint64(src[51])<<51 |
		uint64(src[52])<<52 |
		uint64(src[53])<<53 |
		uint64(src[54])<<54 |
		uint64(src[55])<<55 |
		uint64(src[56])<<56 |
		uint64(src[57])<<57 |
		uint64(src[58])<<58 |
		uint64(src[59])<<59
}

// pack_30 packs 30 values from in using 2 bits each
func pack_30[T types.Integer](p unsafe.Pointer, _ uint64) uint64 {
	src := (*[30]T)(p)
	return 3<<60 |
		uint64(src[0]) |
		uint64(src[1])<<2 |
		uint64(src[2])<<4 |
		uint64(src[3])<<6 |
		uint64(src[4])<<8 |
		uint64(src[5])<<10 |
		uint64(src[6])<<12 |
		uint64(src[7])<<14 |
		uint64(src[8])<<16 |
		uint64(src[9])<<18 |
		uint64(src[10])<<20 |
		uint64(src[11])<<22 |
		uint64(src[12])<<24 |
		uint64(src[13])<<26 |
		uint64(src[14])<<28 |
		uint64(src[15])<<30 |
		uint64(src[16])<<32 |
		uint64(src[17])<<34 |
		uint64(src[18])<<36 |
		uint64(src[19])<<38 |
		uint64(src[20])<<40 |
		uint64(src[21])<<42 |
		uint64(src[22])<<44 |
		uint64(src[23])<<46 |
		uint64(src[24])<<48 |
		uint64(src[25])<<50 |
		uint64(src[26])<<52 |
		uint64(src[27])<<54 |
		uint64(src[28])<<56 |
		uint64(src[29])<<58
}

// pack_20 packs 20 values from in using 3 bits each
func pack_20[T types.Integer](p unsafe.Pointer, _ uint64) uint64 {
	src := (*[20]T)(p)
	return 4<<60 |
		uint64(src[0]) |
		uint64(src[1])<<3 |
		uint64(src[2])<<6 |
		uint64(src[3])<<9 |
		uint64(src[4])<<12 |
		uint64(src[5])<<15 |
		uint64(src[6])<<18 |
		uint64(src[7])<<21 |
		uint64(src[8])<<24 |
		uint64(src[9])<<27 |
		uint64(src[10])<<30 |
		uint64(src[11])<<33 |
		uint64(src[12])<<36 |
		uint64(src[13])<<39 |
		uint64(src[14])<<42 |
		uint64(src[15])<<45 |
		uint64(src[16])<<48 |
		uint64(src[17])<<51 |
		uint64(src[18])<<54 |
		uint64(src[19])<<57
}

// pack_15 packs 15 values from in using 3 bits each
func pack_15[T types.Integer](p unsafe.Pointer, _ uint64) uint64 {
	src := (*[15]T)(p)
	return 5<<60 |
		uint64(src[0]) |
		uint64(src[1])<<4 |
		uint64(src[2])<<8 |
		uint64(src[3])<<12 |
		uint64(src[4])<<16 |
		uint64(src[5])<<20 |
		uint64(src[6])<<24 |
		uint64(src[7])<<28 |
		uint64(src[8])<<32 |
		uint64(src[9])<<36 |
		uint64(src[10])<<40 |
		uint64(src[11])<<44 |
		uint64(src[12])<<48 |
		uint64(src[13])<<52 |
		uint64(src[14])<<56
}

// pack_12 packs 12 values from in using 5 bits each
func pack_12[T types.Integer](p unsafe.Pointer, _ uint64) uint64 {
	src := (*[12]T)(p)
	return 6<<60 |
		uint64(src[0]) |
		uint64(src[1])<<5 |
		uint64(src[2])<<10 |
		uint64(src[3])<<15 |
		uint64(src[4])<<20 |
		uint64(src[5])<<25 |
		uint64(src[6])<<30 |
		uint64(src[7])<<35 |
		uint64(src[8])<<40 |
		uint64(src[9])<<45 |
		uint64(src[10])<<50 |
		uint64(src[11])<<55
}

// pack_10 packs 10 values from in using 6 bits each
func pack_10[T types.Integer](p unsafe.Pointer, _ uint64) uint64 {
	src := (*[10]T)(p)
	return 7<<60 |
		uint64(src[0]) |
		uint64(src[1])<<6 |
		uint64(src[2])<<12 |
		uint64(src[3])<<18 |
		uint64(src[4])<<24 |
		uint64(src[5])<<30 |
		uint64(src[6])<<36 |
		uint64(src[7])<<42 |
		uint64(src[8])<<48 |
		uint64(src[9])<<54
}

// pack_8 packs 8 values from in using 7 bits each
func pack_8[T types.Integer](p unsafe.Pointer, _ uint64) uint64 {
	src := (*[8]T)(p)
	return 8<<60 |
		uint64(src[0]) |
		uint64(src[1])<<7 |
		uint64(src[2])<<14 |
		uint64(src[3])<<21 |
		uint64(src[4])<<28 |
		uint64(src[5])<<35 |
		uint64(src[6])<<42 |
		uint64(src[7])<<49
}

// pack_7 packs 7 values from in using 8 bits each
func pack_7[T types.Integer](p unsafe.Pointer, _ uint64) uint64 {
	src := (*[7]T)(p)
	return 9<<60 |
		uint64(src[0]) |
		uint64(src[1])<<8 |
		uint64(src[2])<<16 |
		uint64(src[3])<<24 |
		uint64(src[4])<<32 |
		uint64(src[5])<<40 |
		uint64(src[6])<<48
}

// pack_6 packs 6 values from in using 10 bits each
func pack_6[T types.Integer](p unsafe.Pointer, _ uint64) uint64 {
	src := (*[6]T)(p)
	return 10<<60 |
		uint64(src[0]) |
		uint64(src[1])<<10 |
		uint64(src[2])<<20 |
		uint64(src[3])<<30 |
		uint64(src[4])<<40 |
		uint64(src[5])<<50
}

// pack_5 packs 5 values from in using 12 bits each
func pack_5[T types.Integer](p unsafe.Pointer, _ uint64) uint64 {
	src := (*[5]T)(p)
	return 11<<60 |
		uint64(src[0]) |
		uint64(src[1])<<12 |
		uint64(src[2])<<24 |
		uint64(src[3])<<36 |
		uint64(src[4])<<48
}

// pack_4 packs 4 values from in using 15 bits each
func pack_4[T types.Integer](p unsafe.Pointer, _ uint64) uint64 {
	src := (*[4]T)(p)
	return 12<<60 |
		uint64(src[0]) |
		uint64(src[1])<<15 |
		uint64(src[2])<<30 |
		uint64(src[3])<<45
}

// pack_3 packs 3 values from in using 20 bits each
func pack_3[T types.Integer](p unsafe.Pointer, _ uint64) uint64 {
	src := (*[3]T)(p)
	return 13<<60 |
		uint64(src[0]) |
		uint64(src[1])<<20 |
		uint64(src[2])<<40
}

// pack_2 packs 2 values from in using 30 bits each
func pack_2[T types.Integer](p unsafe.Pointer, _ uint64) uint64 {
	src := (*[2]T)(p)
	return 14<<60 |
		uint64(src[0]) |
		uint64(src[1])<<30
}

// pack_1 packs 1 values from in using 60 bits each
func pack_1[T types.Integer](p unsafe.Pointer, _ uint64) uint64 {
	src := (*[1]T)(p)
	return 15<<60 |
		uint64(src[0])
}
