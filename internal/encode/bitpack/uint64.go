// Copyright (c) 2025 Blockwatch Data Inc.
// Code automatically generated - DO NOT EDIT.
// Any manual changes will be lost.

package bitpack

import (
	"unsafe"
)

// Packer
var pack_u64 = [65]func(in *[64]uint64, p unsafe.Pointer, minv uint64){
	bp64_0, bp64_1, bp64_2, bp64_3, bp64_4, bp64_5, bp64_6, bp64_7,
	bp64_8, bp64_9, bp64_10, bp64_11, bp64_12, bp64_13, bp64_14, bp64_15,
	bp64_16, bp64_17, bp64_18, bp64_19, bp64_20, bp64_21, bp64_22, bp64_23,
	bp64_24, bp64_25, bp64_26, bp64_27, bp64_28, bp64_29, bp64_30, bp64_31,
	bp64_32, bp64_33, bp64_34, bp64_35, bp64_36, bp64_37, bp64_38, bp64_39,
	bp64_40, bp64_41, bp64_42, bp64_43, bp64_44, bp64_45, bp64_46, bp64_47,
	bp64_48, bp64_49, bp64_50, bp64_51, bp64_52, bp64_53, bp64_54, bp64_55,
	bp64_56, bp64_57, bp64_58, bp64_59, bp64_60, bp64_61, bp64_62, bp64_63,
	bp64_64,
}

func bp64_0(in *[64]uint64, p unsafe.Pointer, minv uint64) {
}

func bp64_1(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[1]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<1 |
			uint64(in[2]-minv)<<2 |
			uint64(in[3]-minv)<<3 |
			uint64(in[4]-minv)<<4 |
			uint64(in[5]-minv)<<5 |
			uint64(in[6]-minv)<<6 |
			uint64(in[7]-minv)<<7 |
			uint64(in[8]-minv)<<8 |
			uint64(in[9]-minv)<<9 |
			uint64(in[10]-minv)<<10 |
			uint64(in[11]-minv)<<11 |
			uint64(in[12]-minv)<<12 |
			uint64(in[13]-minv)<<13 |
			uint64(in[14]-minv)<<14 |
			uint64(in[15]-minv)<<15 |
			uint64(in[16]-minv)<<16 |
			uint64(in[17]-minv)<<17 |
			uint64(in[18]-minv)<<18 |
			uint64(in[19]-minv)<<19 |
			uint64(in[20]-minv)<<20 |
			uint64(in[21]-minv)<<21 |
			uint64(in[22]-minv)<<22 |
			uint64(in[23]-minv)<<23 |
			uint64(in[24]-minv)<<24 |
			uint64(in[25]-minv)<<25 |
			uint64(in[26]-minv)<<26 |
			uint64(in[27]-minv)<<27 |
			uint64(in[28]-minv)<<28 |
			uint64(in[29]-minv)<<29 |
			uint64(in[30]-minv)<<30 |
			uint64(in[31]-minv)<<31 |
			uint64(in[32]-minv)<<32 |
			uint64(in[33]-minv)<<33 |
			uint64(in[34]-minv)<<34 |
			uint64(in[35]-minv)<<35 |
			uint64(in[36]-minv)<<36 |
			uint64(in[37]-minv)<<37 |
			uint64(in[38]-minv)<<38 |
			uint64(in[39]-minv)<<39 |
			uint64(in[40]-minv)<<40 |
			uint64(in[41]-minv)<<41 |
			uint64(in[42]-minv)<<42 |
			uint64(in[43]-minv)<<43 |
			uint64(in[44]-minv)<<44 |
			uint64(in[45]-minv)<<45 |
			uint64(in[46]-minv)<<46 |
			uint64(in[47]-minv)<<47 |
			uint64(in[48]-minv)<<48 |
			uint64(in[49]-minv)<<49 |
			uint64(in[50]-minv)<<50 |
			uint64(in[51]-minv)<<51 |
			uint64(in[52]-minv)<<52 |
			uint64(in[53]-minv)<<53 |
			uint64(in[54]-minv)<<54 |
			uint64(in[55]-minv)<<55 |
			uint64(in[56]-minv)<<56 |
			uint64(in[57]-minv)<<57 |
			uint64(in[58]-minv)<<58 |
			uint64(in[59]-minv)<<59 |
			uint64(in[60]-minv)<<60 |
			uint64(in[61]-minv)<<61 |
			uint64(in[62]-minv)<<62 |
			uint64(in[63]-minv)<<63
}

func bp64_2(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[2]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<2 |
			uint64(in[2]-minv)<<4 |
			uint64(in[3]-minv)<<6 |
			uint64(in[4]-minv)<<8 |
			uint64(in[5]-minv)<<10 |
			uint64(in[6]-minv)<<12 |
			uint64(in[7]-minv)<<14 |
			uint64(in[8]-minv)<<16 |
			uint64(in[9]-minv)<<18 |
			uint64(in[10]-minv)<<20 |
			uint64(in[11]-minv)<<22 |
			uint64(in[12]-minv)<<24 |
			uint64(in[13]-minv)<<26 |
			uint64(in[14]-minv)<<28 |
			uint64(in[15]-minv)<<30 |
			uint64(in[16]-minv)<<32 |
			uint64(in[17]-minv)<<34 |
			uint64(in[18]-minv)<<36 |
			uint64(in[19]-minv)<<38 |
			uint64(in[20]-minv)<<40 |
			uint64(in[21]-minv)<<42 |
			uint64(in[22]-minv)<<44 |
			uint64(in[23]-minv)<<46 |
			uint64(in[24]-minv)<<48 |
			uint64(in[25]-minv)<<50 |
			uint64(in[26]-minv)<<52 |
			uint64(in[27]-minv)<<54 |
			uint64(in[28]-minv)<<56 |
			uint64(in[29]-minv)<<58 |
			uint64(in[30]-minv)<<60 |
			uint64(in[31]-minv)<<62

	out[1] =
		uint64(in[32]-minv) |
			uint64(in[33]-minv)<<2 |
			uint64(in[34]-minv)<<4 |
			uint64(in[35]-minv)<<6 |
			uint64(in[36]-minv)<<8 |
			uint64(in[37]-minv)<<10 |
			uint64(in[38]-minv)<<12 |
			uint64(in[39]-minv)<<14 |
			uint64(in[40]-minv)<<16 |
			uint64(in[41]-minv)<<18 |
			uint64(in[42]-minv)<<20 |
			uint64(in[43]-minv)<<22 |
			uint64(in[44]-minv)<<24 |
			uint64(in[45]-minv)<<26 |
			uint64(in[46]-minv)<<28 |
			uint64(in[47]-minv)<<30 |
			uint64(in[48]-minv)<<32 |
			uint64(in[49]-minv)<<34 |
			uint64(in[50]-minv)<<36 |
			uint64(in[51]-minv)<<38 |
			uint64(in[52]-minv)<<40 |
			uint64(in[53]-minv)<<42 |
			uint64(in[54]-minv)<<44 |
			uint64(in[55]-minv)<<46 |
			uint64(in[56]-minv)<<48 |
			uint64(in[57]-minv)<<50 |
			uint64(in[58]-minv)<<52 |
			uint64(in[59]-minv)<<54 |
			uint64(in[60]-minv)<<56 |
			uint64(in[61]-minv)<<58 |
			uint64(in[62]-minv)<<60 |
			uint64(in[63]-minv)<<62
}

func bp64_3(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[3]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<3 |
			uint64(in[2]-minv)<<6 |
			uint64(in[3]-minv)<<9 |
			uint64(in[4]-minv)<<12 |
			uint64(in[5]-minv)<<15 |
			uint64(in[6]-minv)<<18 |
			uint64(in[7]-minv)<<21 |
			uint64(in[8]-minv)<<24 |
			uint64(in[9]-minv)<<27 |
			uint64(in[10]-minv)<<30 |
			uint64(in[11]-minv)<<33 |
			uint64(in[12]-minv)<<36 |
			uint64(in[13]-minv)<<39 |
			uint64(in[14]-minv)<<42 |
			uint64(in[15]-minv)<<45 |
			uint64(in[16]-minv)<<48 |
			uint64(in[17]-minv)<<51 |
			uint64(in[18]-minv)<<54 |
			uint64(in[19]-minv)<<57 |
			uint64(in[20]-minv)<<60 |
			uint64(in[21]-minv)<<63

	out[1] =
		uint64(in[21]-minv)>>1 |
			uint64(in[22]-minv)<<2 |
			uint64(in[23]-minv)<<5 |
			uint64(in[24]-minv)<<8 |
			uint64(in[25]-minv)<<11 |
			uint64(in[26]-minv)<<14 |
			uint64(in[27]-minv)<<17 |
			uint64(in[28]-minv)<<20 |
			uint64(in[29]-minv)<<23 |
			uint64(in[30]-minv)<<26 |
			uint64(in[31]-minv)<<29 |
			uint64(in[32]-minv)<<32 |
			uint64(in[33]-minv)<<35 |
			uint64(in[34]-minv)<<38 |
			uint64(in[35]-minv)<<41 |
			uint64(in[36]-minv)<<44 |
			uint64(in[37]-minv)<<47 |
			uint64(in[38]-minv)<<50 |
			uint64(in[39]-minv)<<53 |
			uint64(in[40]-minv)<<56 |
			uint64(in[41]-minv)<<59 |
			uint64(in[42]-minv)<<62

	out[2] =
		uint64(in[42]-minv)>>2 |
			uint64(in[43]-minv)<<1 |
			uint64(in[44]-minv)<<4 |
			uint64(in[45]-minv)<<7 |
			uint64(in[46]-minv)<<10 |
			uint64(in[47]-minv)<<13 |
			uint64(in[48]-minv)<<16 |
			uint64(in[49]-minv)<<19 |
			uint64(in[50]-minv)<<22 |
			uint64(in[51]-minv)<<25 |
			uint64(in[52]-minv)<<28 |
			uint64(in[53]-minv)<<31 |
			uint64(in[54]-minv)<<34 |
			uint64(in[55]-minv)<<37 |
			uint64(in[56]-minv)<<40 |
			uint64(in[57]-minv)<<43 |
			uint64(in[58]-minv)<<46 |
			uint64(in[59]-minv)<<49 |
			uint64(in[60]-minv)<<52 |
			uint64(in[61]-minv)<<55 |
			uint64(in[62]-minv)<<58 |
			uint64(in[63]-minv)<<61
}

func bp64_4(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[4]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<4 |
			uint64(in[2]-minv)<<8 |
			uint64(in[3]-minv)<<12 |
			uint64(in[4]-minv)<<16 |
			uint64(in[5]-minv)<<20 |
			uint64(in[6]-minv)<<24 |
			uint64(in[7]-minv)<<28 |
			uint64(in[8]-minv)<<32 |
			uint64(in[9]-minv)<<36 |
			uint64(in[10]-minv)<<40 |
			uint64(in[11]-minv)<<44 |
			uint64(in[12]-minv)<<48 |
			uint64(in[13]-minv)<<52 |
			uint64(in[14]-minv)<<56 |
			uint64(in[15]-minv)<<60

	out[1] =
		uint64(in[16]-minv) |
			uint64(in[17]-minv)<<4 |
			uint64(in[18]-minv)<<8 |
			uint64(in[19]-minv)<<12 |
			uint64(in[20]-minv)<<16 |
			uint64(in[21]-minv)<<20 |
			uint64(in[22]-minv)<<24 |
			uint64(in[23]-minv)<<28 |
			uint64(in[24]-minv)<<32 |
			uint64(in[25]-minv)<<36 |
			uint64(in[26]-minv)<<40 |
			uint64(in[27]-minv)<<44 |
			uint64(in[28]-minv)<<48 |
			uint64(in[29]-minv)<<52 |
			uint64(in[30]-minv)<<56 |
			uint64(in[31]-minv)<<60

	out[2] =
		uint64(in[32]-minv) |
			uint64(in[33]-minv)<<4 |
			uint64(in[34]-minv)<<8 |
			uint64(in[35]-minv)<<12 |
			uint64(in[36]-minv)<<16 |
			uint64(in[37]-minv)<<20 |
			uint64(in[38]-minv)<<24 |
			uint64(in[39]-minv)<<28 |
			uint64(in[40]-minv)<<32 |
			uint64(in[41]-minv)<<36 |
			uint64(in[42]-minv)<<40 |
			uint64(in[43]-minv)<<44 |
			uint64(in[44]-minv)<<48 |
			uint64(in[45]-minv)<<52 |
			uint64(in[46]-minv)<<56 |
			uint64(in[47]-minv)<<60

	out[3] =
		uint64(in[48]-minv) |
			uint64(in[49]-minv)<<4 |
			uint64(in[50]-minv)<<8 |
			uint64(in[51]-minv)<<12 |
			uint64(in[52]-minv)<<16 |
			uint64(in[53]-minv)<<20 |
			uint64(in[54]-minv)<<24 |
			uint64(in[55]-minv)<<28 |
			uint64(in[56]-minv)<<32 |
			uint64(in[57]-minv)<<36 |
			uint64(in[58]-minv)<<40 |
			uint64(in[59]-minv)<<44 |
			uint64(in[60]-minv)<<48 |
			uint64(in[61]-minv)<<52 |
			uint64(in[62]-minv)<<56 |
			uint64(in[63]-minv)<<60
}

func bp64_5(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[5]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<5 |
			uint64(in[2]-minv)<<10 |
			uint64(in[3]-minv)<<15 |
			uint64(in[4]-minv)<<20 |
			uint64(in[5]-minv)<<25 |
			uint64(in[6]-minv)<<30 |
			uint64(in[7]-minv)<<35 |
			uint64(in[8]-minv)<<40 |
			uint64(in[9]-minv)<<45 |
			uint64(in[10]-minv)<<50 |
			uint64(in[11]-minv)<<55 |
			uint64(in[12]-minv)<<60

	out[1] =
		uint64(in[12]-minv)>>4 |
			uint64(in[13]-minv)<<1 |
			uint64(in[14]-minv)<<6 |
			uint64(in[15]-minv)<<11 |
			uint64(in[16]-minv)<<16 |
			uint64(in[17]-minv)<<21 |
			uint64(in[18]-minv)<<26 |
			uint64(in[19]-minv)<<31 |
			uint64(in[20]-minv)<<36 |
			uint64(in[21]-minv)<<41 |
			uint64(in[22]-minv)<<46 |
			uint64(in[23]-minv)<<51 |
			uint64(in[24]-minv)<<56 |
			uint64(in[25]-minv)<<61

	out[2] =
		uint64(in[25]-minv)>>3 |
			uint64(in[26]-minv)<<2 |
			uint64(in[27]-minv)<<7 |
			uint64(in[28]-minv)<<12 |
			uint64(in[29]-minv)<<17 |
			uint64(in[30]-minv)<<22 |
			uint64(in[31]-minv)<<27 |
			uint64(in[32]-minv)<<32 |
			uint64(in[33]-minv)<<37 |
			uint64(in[34]-minv)<<42 |
			uint64(in[35]-minv)<<47 |
			uint64(in[36]-minv)<<52 |
			uint64(in[37]-minv)<<57 |
			uint64(in[38]-minv)<<62

	out[3] =
		uint64(in[38]-minv)>>2 |
			uint64(in[39]-minv)<<3 |
			uint64(in[40]-minv)<<8 |
			uint64(in[41]-minv)<<13 |
			uint64(in[42]-minv)<<18 |
			uint64(in[43]-minv)<<23 |
			uint64(in[44]-minv)<<28 |
			uint64(in[45]-minv)<<33 |
			uint64(in[46]-minv)<<38 |
			uint64(in[47]-minv)<<43 |
			uint64(in[48]-minv)<<48 |
			uint64(in[49]-minv)<<53 |
			uint64(in[50]-minv)<<58 |
			uint64(in[51]-minv)<<63

	out[4] =
		uint64(in[51]-minv)>>1 |
			uint64(in[52]-minv)<<4 |
			uint64(in[53]-minv)<<9 |
			uint64(in[54]-minv)<<14 |
			uint64(in[55]-minv)<<19 |
			uint64(in[56]-minv)<<24 |
			uint64(in[57]-minv)<<29 |
			uint64(in[58]-minv)<<34 |
			uint64(in[59]-minv)<<39 |
			uint64(in[60]-minv)<<44 |
			uint64(in[61]-minv)<<49 |
			uint64(in[62]-minv)<<54 |
			uint64(in[63]-minv)<<59
}

func bp64_6(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[6]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<6 |
			uint64(in[2]-minv)<<12 |
			uint64(in[3]-minv)<<18 |
			uint64(in[4]-minv)<<24 |
			uint64(in[5]-minv)<<30 |
			uint64(in[6]-minv)<<36 |
			uint64(in[7]-minv)<<42 |
			uint64(in[8]-minv)<<48 |
			uint64(in[9]-minv)<<54 |
			uint64(in[10]-minv)<<60

	out[1] =
		uint64(in[10]-minv)>>4 |
			uint64(in[11]-minv)<<2 |
			uint64(in[12]-minv)<<8 |
			uint64(in[13]-minv)<<14 |
			uint64(in[14]-minv)<<20 |
			uint64(in[15]-minv)<<26 |
			uint64(in[16]-minv)<<32 |
			uint64(in[17]-minv)<<38 |
			uint64(in[18]-minv)<<44 |
			uint64(in[19]-minv)<<50 |
			uint64(in[20]-minv)<<56 |
			uint64(in[21]-minv)<<62

	out[2] =
		uint64(in[21]-minv)>>2 |
			uint64(in[22]-minv)<<4 |
			uint64(in[23]-minv)<<10 |
			uint64(in[24]-minv)<<16 |
			uint64(in[25]-minv)<<22 |
			uint64(in[26]-minv)<<28 |
			uint64(in[27]-minv)<<34 |
			uint64(in[28]-minv)<<40 |
			uint64(in[29]-minv)<<46 |
			uint64(in[30]-minv)<<52 |
			uint64(in[31]-minv)<<58

	out[3] =
		uint64(in[31]-minv)>>6 |
			uint64(in[32]-minv) |
			uint64(in[33]-minv)<<6 |
			uint64(in[34]-minv)<<12 |
			uint64(in[35]-minv)<<18 |
			uint64(in[36]-minv)<<24 |
			uint64(in[37]-minv)<<30 |
			uint64(in[38]-minv)<<36 |
			uint64(in[39]-minv)<<42 |
			uint64(in[40]-minv)<<48 |
			uint64(in[41]-minv)<<54 |
			uint64(in[42]-minv)<<60

	out[4] =
		uint64(in[42]-minv)>>4 |
			uint64(in[43]-minv)<<2 |
			uint64(in[44]-minv)<<8 |
			uint64(in[45]-minv)<<14 |
			uint64(in[46]-minv)<<20 |
			uint64(in[47]-minv)<<26 |
			uint64(in[48]-minv)<<32 |
			uint64(in[49]-minv)<<38 |
			uint64(in[50]-minv)<<44 |
			uint64(in[51]-minv)<<50 |
			uint64(in[52]-minv)<<56 |
			uint64(in[53]-minv)<<62

	out[5] =
		uint64(in[53]-minv)>>2 |
			uint64(in[54]-minv)<<4 |
			uint64(in[55]-minv)<<10 |
			uint64(in[56]-minv)<<16 |
			uint64(in[57]-minv)<<22 |
			uint64(in[58]-minv)<<28 |
			uint64(in[59]-minv)<<34 |
			uint64(in[60]-minv)<<40 |
			uint64(in[61]-minv)<<46 |
			uint64(in[62]-minv)<<52 |
			uint64(in[63]-minv)<<58
}

func bp64_7(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[7]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<7 |
			uint64(in[2]-minv)<<14 |
			uint64(in[3]-minv)<<21 |
			uint64(in[4]-minv)<<28 |
			uint64(in[5]-minv)<<35 |
			uint64(in[6]-minv)<<42 |
			uint64(in[7]-minv)<<49 |
			uint64(in[8]-minv)<<56 |
			uint64(in[9]-minv)<<63

	out[1] =
		uint64(in[9]-minv)>>1 |
			uint64(in[10]-minv)<<6 |
			uint64(in[11]-minv)<<13 |
			uint64(in[12]-minv)<<20 |
			uint64(in[13]-minv)<<27 |
			uint64(in[14]-minv)<<34 |
			uint64(in[15]-minv)<<41 |
			uint64(in[16]-minv)<<48 |
			uint64(in[17]-minv)<<55 |
			uint64(in[18]-minv)<<62

	out[2] =
		uint64(in[18]-minv)>>2 |
			uint64(in[19]-minv)<<5 |
			uint64(in[20]-minv)<<12 |
			uint64(in[21]-minv)<<19 |
			uint64(in[22]-minv)<<26 |
			uint64(in[23]-minv)<<33 |
			uint64(in[24]-minv)<<40 |
			uint64(in[25]-minv)<<47 |
			uint64(in[26]-minv)<<54 |
			uint64(in[27]-minv)<<61

	out[3] =
		uint64(in[27]-minv)>>3 |
			uint64(in[28]-minv)<<4 |
			uint64(in[29]-minv)<<11 |
			uint64(in[30]-minv)<<18 |
			uint64(in[31]-minv)<<25 |
			uint64(in[32]-minv)<<32 |
			uint64(in[33]-minv)<<39 |
			uint64(in[34]-minv)<<46 |
			uint64(in[35]-minv)<<53 |
			uint64(in[36]-minv)<<60

	out[4] =
		uint64(in[36]-minv)>>4 |
			uint64(in[37]-minv)<<3 |
			uint64(in[38]-minv)<<10 |
			uint64(in[39]-minv)<<17 |
			uint64(in[40]-minv)<<24 |
			uint64(in[41]-minv)<<31 |
			uint64(in[42]-minv)<<38 |
			uint64(in[43]-minv)<<45 |
			uint64(in[44]-minv)<<52 |
			uint64(in[45]-minv)<<59

	out[5] =
		uint64(in[45]-minv)>>5 |
			uint64(in[46]-minv)<<2 |
			uint64(in[47]-minv)<<9 |
			uint64(in[48]-minv)<<16 |
			uint64(in[49]-minv)<<23 |
			uint64(in[50]-minv)<<30 |
			uint64(in[51]-minv)<<37 |
			uint64(in[52]-minv)<<44 |
			uint64(in[53]-minv)<<51 |
			uint64(in[54]-minv)<<58

	out[6] =
		uint64(in[54]-minv)>>6 |
			uint64(in[55]-minv)<<1 |
			uint64(in[56]-minv)<<8 |
			uint64(in[57]-minv)<<15 |
			uint64(in[58]-minv)<<22 |
			uint64(in[59]-minv)<<29 |
			uint64(in[60]-minv)<<36 |
			uint64(in[61]-minv)<<43 |
			uint64(in[62]-minv)<<50 |
			uint64(in[63]-minv)<<57
}

func bp64_8(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[8]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<8 |
			uint64(in[2]-minv)<<16 |
			uint64(in[3]-minv)<<24 |
			uint64(in[4]-minv)<<32 |
			uint64(in[5]-minv)<<40 |
			uint64(in[6]-minv)<<48 |
			uint64(in[7]-minv)<<56

	out[1] =
		uint64(in[8]-minv) |
			uint64(in[9]-minv)<<8 |
			uint64(in[10]-minv)<<16 |
			uint64(in[11]-minv)<<24 |
			uint64(in[12]-minv)<<32 |
			uint64(in[13]-minv)<<40 |
			uint64(in[14]-minv)<<48 |
			uint64(in[15]-minv)<<56

	out[2] =
		uint64(in[16]-minv) |
			uint64(in[17]-minv)<<8 |
			uint64(in[18]-minv)<<16 |
			uint64(in[19]-minv)<<24 |
			uint64(in[20]-minv)<<32 |
			uint64(in[21]-minv)<<40 |
			uint64(in[22]-minv)<<48 |
			uint64(in[23]-minv)<<56

	out[3] =
		uint64(in[24]-minv) |
			uint64(in[25]-minv)<<8 |
			uint64(in[26]-minv)<<16 |
			uint64(in[27]-minv)<<24 |
			uint64(in[28]-minv)<<32 |
			uint64(in[29]-minv)<<40 |
			uint64(in[30]-minv)<<48 |
			uint64(in[31]-minv)<<56

	out[4] =
		uint64(in[32]-minv) |
			uint64(in[33]-minv)<<8 |
			uint64(in[34]-minv)<<16 |
			uint64(in[35]-minv)<<24 |
			uint64(in[36]-minv)<<32 |
			uint64(in[37]-minv)<<40 |
			uint64(in[38]-minv)<<48 |
			uint64(in[39]-minv)<<56

	out[5] =
		uint64(in[40]-minv) |
			uint64(in[41]-minv)<<8 |
			uint64(in[42]-minv)<<16 |
			uint64(in[43]-minv)<<24 |
			uint64(in[44]-minv)<<32 |
			uint64(in[45]-minv)<<40 |
			uint64(in[46]-minv)<<48 |
			uint64(in[47]-minv)<<56

	out[6] =
		uint64(in[48]-minv) |
			uint64(in[49]-minv)<<8 |
			uint64(in[50]-minv)<<16 |
			uint64(in[51]-minv)<<24 |
			uint64(in[52]-minv)<<32 |
			uint64(in[53]-minv)<<40 |
			uint64(in[54]-minv)<<48 |
			uint64(in[55]-minv)<<56

	out[7] =
		uint64(in[56]-minv) |
			uint64(in[57]-minv)<<8 |
			uint64(in[58]-minv)<<16 |
			uint64(in[59]-minv)<<24 |
			uint64(in[60]-minv)<<32 |
			uint64(in[61]-minv)<<40 |
			uint64(in[62]-minv)<<48 |
			uint64(in[63]-minv)<<56
}

func bp64_9(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[9]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<9 |
			uint64(in[2]-minv)<<18 |
			uint64(in[3]-minv)<<27 |
			uint64(in[4]-minv)<<36 |
			uint64(in[5]-minv)<<45 |
			uint64(in[6]-minv)<<54 |
			uint64(in[7]-minv)<<63

	out[1] =
		uint64(in[7]-minv)>>1 |
			uint64(in[8]-minv)<<8 |
			uint64(in[9]-minv)<<17 |
			uint64(in[10]-minv)<<26 |
			uint64(in[11]-minv)<<35 |
			uint64(in[12]-minv)<<44 |
			uint64(in[13]-minv)<<53 |
			uint64(in[14]-minv)<<62

	out[2] =
		uint64(in[14]-minv)>>2 |
			uint64(in[15]-minv)<<7 |
			uint64(in[16]-minv)<<16 |
			uint64(in[17]-minv)<<25 |
			uint64(in[18]-minv)<<34 |
			uint64(in[19]-minv)<<43 |
			uint64(in[20]-minv)<<52 |
			uint64(in[21]-minv)<<61

	out[3] =
		uint64(in[21]-minv)>>3 |
			uint64(in[22]-minv)<<6 |
			uint64(in[23]-minv)<<15 |
			uint64(in[24]-minv)<<24 |
			uint64(in[25]-minv)<<33 |
			uint64(in[26]-minv)<<42 |
			uint64(in[27]-minv)<<51 |
			uint64(in[28]-minv)<<60

	out[4] =
		uint64(in[28]-minv)>>4 |
			uint64(in[29]-minv)<<5 |
			uint64(in[30]-minv)<<14 |
			uint64(in[31]-minv)<<23 |
			uint64(in[32]-minv)<<32 |
			uint64(in[33]-minv)<<41 |
			uint64(in[34]-minv)<<50 |
			uint64(in[35]-minv)<<59

	out[5] =
		uint64(in[35]-minv)>>5 |
			uint64(in[36]-minv)<<4 |
			uint64(in[37]-minv)<<13 |
			uint64(in[38]-minv)<<22 |
			uint64(in[39]-minv)<<31 |
			uint64(in[40]-minv)<<40 |
			uint64(in[41]-minv)<<49 |
			uint64(in[42]-minv)<<58

	out[6] =
		uint64(in[42]-minv)>>6 |
			uint64(in[43]-minv)<<3 |
			uint64(in[44]-minv)<<12 |
			uint64(in[45]-minv)<<21 |
			uint64(in[46]-minv)<<30 |
			uint64(in[47]-minv)<<39 |
			uint64(in[48]-minv)<<48 |
			uint64(in[49]-minv)<<57

	out[7] =
		uint64(in[49]-minv)>>7 |
			uint64(in[50]-minv)<<2 |
			uint64(in[51]-minv)<<11 |
			uint64(in[52]-minv)<<20 |
			uint64(in[53]-minv)<<29 |
			uint64(in[54]-minv)<<38 |
			uint64(in[55]-minv)<<47 |
			uint64(in[56]-minv)<<56

	out[8] =
		uint64(in[56]-minv)>>8 |
			uint64(in[57]-minv)<<1 |
			uint64(in[58]-minv)<<10 |
			uint64(in[59]-minv)<<19 |
			uint64(in[60]-minv)<<28 |
			uint64(in[61]-minv)<<37 |
			uint64(in[62]-minv)<<46 |
			uint64(in[63]-minv)<<55
}

func bp64_10(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[10]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<10 |
			uint64(in[2]-minv)<<20 |
			uint64(in[3]-minv)<<30 |
			uint64(in[4]-minv)<<40 |
			uint64(in[5]-minv)<<50 |
			uint64(in[6]-minv)<<60

	out[1] =
		uint64(in[6]-minv)>>4 |
			uint64(in[7]-minv)<<6 |
			uint64(in[8]-minv)<<16 |
			uint64(in[9]-minv)<<26 |
			uint64(in[10]-minv)<<36 |
			uint64(in[11]-minv)<<46 |
			uint64(in[12]-minv)<<56

	out[2] =
		uint64(in[12]-minv)>>8 |
			uint64(in[13]-minv)<<2 |
			uint64(in[14]-minv)<<12 |
			uint64(in[15]-minv)<<22 |
			uint64(in[16]-minv)<<32 |
			uint64(in[17]-minv)<<42 |
			uint64(in[18]-minv)<<52 |
			uint64(in[19]-minv)<<62

	out[3] =
		uint64(in[19]-minv)>>2 |
			uint64(in[20]-minv)<<8 |
			uint64(in[21]-minv)<<18 |
			uint64(in[22]-minv)<<28 |
			uint64(in[23]-minv)<<38 |
			uint64(in[24]-minv)<<48 |
			uint64(in[25]-minv)<<58

	out[4] =
		uint64(in[25]-minv)>>6 |
			uint64(in[26]-minv)<<4 |
			uint64(in[27]-minv)<<14 |
			uint64(in[28]-minv)<<24 |
			uint64(in[29]-minv)<<34 |
			uint64(in[30]-minv)<<44 |
			uint64(in[31]-minv)<<54

	out[5] =
		uint64(in[31]-minv)>>10 |
			uint64(in[32]-minv) |
			uint64(in[33]-minv)<<10 |
			uint64(in[34]-minv)<<20 |
			uint64(in[35]-minv)<<30 |
			uint64(in[36]-minv)<<40 |
			uint64(in[37]-minv)<<50 |
			uint64(in[38]-minv)<<60

	out[6] =
		uint64(in[38]-minv)>>4 |
			uint64(in[39]-minv)<<6 |
			uint64(in[40]-minv)<<16 |
			uint64(in[41]-minv)<<26 |
			uint64(in[42]-minv)<<36 |
			uint64(in[43]-minv)<<46 |
			uint64(in[44]-minv)<<56

	out[7] =
		uint64(in[44]-minv)>>8 |
			uint64(in[45]-minv)<<2 |
			uint64(in[46]-minv)<<12 |
			uint64(in[47]-minv)<<22 |
			uint64(in[48]-minv)<<32 |
			uint64(in[49]-minv)<<42 |
			uint64(in[50]-minv)<<52 |
			uint64(in[51]-minv)<<62

	out[8] =
		uint64(in[51]-minv)>>2 |
			uint64(in[52]-minv)<<8 |
			uint64(in[53]-minv)<<18 |
			uint64(in[54]-minv)<<28 |
			uint64(in[55]-minv)<<38 |
			uint64(in[56]-minv)<<48 |
			uint64(in[57]-minv)<<58

	out[9] =
		uint64(in[57]-minv)>>6 |
			uint64(in[58]-minv)<<4 |
			uint64(in[59]-minv)<<14 |
			uint64(in[60]-minv)<<24 |
			uint64(in[61]-minv)<<34 |
			uint64(in[62]-minv)<<44 |
			uint64(in[63]-minv)<<54
}

func bp64_11(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[11]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<11 |
			uint64(in[2]-minv)<<22 |
			uint64(in[3]-minv)<<33 |
			uint64(in[4]-minv)<<44 |
			uint64(in[5]-minv)<<55

	out[1] =
		uint64(in[5]-minv)>>9 |
			uint64(in[6]-minv)<<2 |
			uint64(in[7]-minv)<<13 |
			uint64(in[8]-minv)<<24 |
			uint64(in[9]-minv)<<35 |
			uint64(in[10]-minv)<<46 |
			uint64(in[11]-minv)<<57

	out[2] =
		uint64(in[11]-minv)>>7 |
			uint64(in[12]-minv)<<4 |
			uint64(in[13]-minv)<<15 |
			uint64(in[14]-minv)<<26 |
			uint64(in[15]-minv)<<37 |
			uint64(in[16]-minv)<<48 |
			uint64(in[17]-minv)<<59

	out[3] =
		uint64(in[17]-minv)>>5 |
			uint64(in[18]-minv)<<6 |
			uint64(in[19]-minv)<<17 |
			uint64(in[20]-minv)<<28 |
			uint64(in[21]-minv)<<39 |
			uint64(in[22]-minv)<<50 |
			uint64(in[23]-minv)<<61

	out[4] =
		uint64(in[23]-minv)>>3 |
			uint64(in[24]-minv)<<8 |
			uint64(in[25]-minv)<<19 |
			uint64(in[26]-minv)<<30 |
			uint64(in[27]-minv)<<41 |
			uint64(in[28]-minv)<<52 |
			uint64(in[29]-minv)<<63

	out[5] =
		uint64(in[29]-minv)>>1 |
			uint64(in[30]-minv)<<10 |
			uint64(in[31]-minv)<<21 |
			uint64(in[32]-minv)<<32 |
			uint64(in[33]-minv)<<43 |
			uint64(in[34]-minv)<<54

	out[6] =
		uint64(in[34]-minv)>>10 |
			uint64(in[35]-minv)<<1 |
			uint64(in[36]-minv)<<12 |
			uint64(in[37]-minv)<<23 |
			uint64(in[38]-minv)<<34 |
			uint64(in[39]-minv)<<45 |
			uint64(in[40]-minv)<<56

	out[7] =
		uint64(in[40]-minv)>>8 |
			uint64(in[41]-minv)<<3 |
			uint64(in[42]-minv)<<14 |
			uint64(in[43]-minv)<<25 |
			uint64(in[44]-minv)<<36 |
			uint64(in[45]-minv)<<47 |
			uint64(in[46]-minv)<<58

	out[8] =
		uint64(in[46]-minv)>>6 |
			uint64(in[47]-minv)<<5 |
			uint64(in[48]-minv)<<16 |
			uint64(in[49]-minv)<<27 |
			uint64(in[50]-minv)<<38 |
			uint64(in[51]-minv)<<49 |
			uint64(in[52]-minv)<<60

	out[9] =
		uint64(in[52]-minv)>>4 |
			uint64(in[53]-minv)<<7 |
			uint64(in[54]-minv)<<18 |
			uint64(in[55]-minv)<<29 |
			uint64(in[56]-minv)<<40 |
			uint64(in[57]-minv)<<51 |
			uint64(in[58]-minv)<<62

	out[10] =
		uint64(in[58]-minv)>>2 |
			uint64(in[59]-minv)<<9 |
			uint64(in[60]-minv)<<20 |
			uint64(in[61]-minv)<<31 |
			uint64(in[62]-minv)<<42 |
			uint64(in[63]-minv)<<53
}

func bp64_12(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[12]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<12 |
			uint64(in[2]-minv)<<24 |
			uint64(in[3]-minv)<<36 |
			uint64(in[4]-minv)<<48 |
			uint64(in[5]-minv)<<60

	out[1] =
		uint64(in[5]-minv)>>4 |
			uint64(in[6]-minv)<<8 |
			uint64(in[7]-minv)<<20 |
			uint64(in[8]-minv)<<32 |
			uint64(in[9]-minv)<<44 |
			uint64(in[10]-minv)<<56

	out[2] =
		uint64(in[10]-minv)>>8 |
			uint64(in[11]-minv)<<4 |
			uint64(in[12]-minv)<<16 |
			uint64(in[13]-minv)<<28 |
			uint64(in[14]-minv)<<40 |
			uint64(in[15]-minv)<<52

	out[3] =
		uint64(in[15]-minv)>>12 |
			uint64(in[16]-minv) |
			uint64(in[17]-minv)<<12 |
			uint64(in[18]-minv)<<24 |
			uint64(in[19]-minv)<<36 |
			uint64(in[20]-minv)<<48 |
			uint64(in[21]-minv)<<60

	out[4] =
		uint64(in[21]-minv)>>4 |
			uint64(in[22]-minv)<<8 |
			uint64(in[23]-minv)<<20 |
			uint64(in[24]-minv)<<32 |
			uint64(in[25]-minv)<<44 |
			uint64(in[26]-minv)<<56

	out[5] =
		uint64(in[26]-minv)>>8 |
			uint64(in[27]-minv)<<4 |
			uint64(in[28]-minv)<<16 |
			uint64(in[29]-minv)<<28 |
			uint64(in[30]-minv)<<40 |
			uint64(in[31]-minv)<<52

	out[6] =
		uint64(in[31]-minv)>>12 |
			uint64(in[32]-minv) |
			uint64(in[33]-minv)<<12 |
			uint64(in[34]-minv)<<24 |
			uint64(in[35]-minv)<<36 |
			uint64(in[36]-minv)<<48 |
			uint64(in[37]-minv)<<60

	out[7] =
		uint64(in[37]-minv)>>4 |
			uint64(in[38]-minv)<<8 |
			uint64(in[39]-minv)<<20 |
			uint64(in[40]-minv)<<32 |
			uint64(in[41]-minv)<<44 |
			uint64(in[42]-minv)<<56

	out[8] =
		uint64(in[42]-minv)>>8 |
			uint64(in[43]-minv)<<4 |
			uint64(in[44]-minv)<<16 |
			uint64(in[45]-minv)<<28 |
			uint64(in[46]-minv)<<40 |
			uint64(in[47]-minv)<<52

	out[9] =
		uint64(in[47]-minv)>>12 |
			uint64(in[48]-minv) |
			uint64(in[49]-minv)<<12 |
			uint64(in[50]-minv)<<24 |
			uint64(in[51]-minv)<<36 |
			uint64(in[52]-minv)<<48 |
			uint64(in[53]-minv)<<60

	out[10] =
		uint64(in[53]-minv)>>4 |
			uint64(in[54]-minv)<<8 |
			uint64(in[55]-minv)<<20 |
			uint64(in[56]-minv)<<32 |
			uint64(in[57]-minv)<<44 |
			uint64(in[58]-minv)<<56

	out[11] =
		uint64(in[58]-minv)>>8 |
			uint64(in[59]-minv)<<4 |
			uint64(in[60]-minv)<<16 |
			uint64(in[61]-minv)<<28 |
			uint64(in[62]-minv)<<40 |
			uint64(in[63]-minv)<<52
}

func bp64_13(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[13]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<13 |
			uint64(in[2]-minv)<<26 |
			uint64(in[3]-minv)<<39 |
			uint64(in[4]-minv)<<52

	out[1] =
		uint64(in[4]-minv)>>12 |
			uint64(in[5]-minv)<<1 |
			uint64(in[6]-minv)<<14 |
			uint64(in[7]-minv)<<27 |
			uint64(in[8]-minv)<<40 |
			uint64(in[9]-minv)<<53

	out[2] =
		uint64(in[9]-minv)>>11 |
			uint64(in[10]-minv)<<2 |
			uint64(in[11]-minv)<<15 |
			uint64(in[12]-minv)<<28 |
			uint64(in[13]-minv)<<41 |
			uint64(in[14]-minv)<<54

	out[3] =
		uint64(in[14]-minv)>>10 |
			uint64(in[15]-minv)<<3 |
			uint64(in[16]-minv)<<16 |
			uint64(in[17]-minv)<<29 |
			uint64(in[18]-minv)<<42 |
			uint64(in[19]-minv)<<55

	out[4] =
		uint64(in[19]-minv)>>9 |
			uint64(in[20]-minv)<<4 |
			uint64(in[21]-minv)<<17 |
			uint64(in[22]-minv)<<30 |
			uint64(in[23]-minv)<<43 |
			uint64(in[24]-minv)<<56

	out[5] =
		uint64(in[24]-minv)>>8 |
			uint64(in[25]-minv)<<5 |
			uint64(in[26]-minv)<<18 |
			uint64(in[27]-minv)<<31 |
			uint64(in[28]-minv)<<44 |
			uint64(in[29]-minv)<<57

	out[6] =
		uint64(in[29]-minv)>>7 |
			uint64(in[30]-minv)<<6 |
			uint64(in[31]-minv)<<19 |
			uint64(in[32]-minv)<<32 |
			uint64(in[33]-minv)<<45 |
			uint64(in[34]-minv)<<58

	out[7] =
		uint64(in[34]-minv)>>6 |
			uint64(in[35]-minv)<<7 |
			uint64(in[36]-minv)<<20 |
			uint64(in[37]-minv)<<33 |
			uint64(in[38]-minv)<<46 |
			uint64(in[39]-minv)<<59

	out[8] =
		uint64(in[39]-minv)>>5 |
			uint64(in[40]-minv)<<8 |
			uint64(in[41]-minv)<<21 |
			uint64(in[42]-minv)<<34 |
			uint64(in[43]-minv)<<47 |
			uint64(in[44]-minv)<<60

	out[9] =
		uint64(in[44]-minv)>>4 |
			uint64(in[45]-minv)<<9 |
			uint64(in[46]-minv)<<22 |
			uint64(in[47]-minv)<<35 |
			uint64(in[48]-minv)<<48 |
			uint64(in[49]-minv)<<61

	out[10] =
		uint64(in[49]-minv)>>3 |
			uint64(in[50]-minv)<<10 |
			uint64(in[51]-minv)<<23 |
			uint64(in[52]-minv)<<36 |
			uint64(in[53]-minv)<<49 |
			uint64(in[54]-minv)<<62

	out[11] =
		uint64(in[54]-minv)>>2 |
			uint64(in[55]-minv)<<11 |
			uint64(in[56]-minv)<<24 |
			uint64(in[57]-minv)<<37 |
			uint64(in[58]-minv)<<50 |
			uint64(in[59]-minv)<<63

	out[12] =
		uint64(in[59]-minv)>>1 |
			uint64(in[60]-minv)<<12 |
			uint64(in[61]-minv)<<25 |
			uint64(in[62]-minv)<<38 |
			uint64(in[63]-minv)<<51
}

func bp64_14(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[14]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<14 |
			uint64(in[2]-minv)<<28 |
			uint64(in[3]-minv)<<42 |
			uint64(in[4]-minv)<<56

	out[1] =
		uint64(in[4]-minv)>>8 |
			uint64(in[5]-minv)<<6 |
			uint64(in[6]-minv)<<20 |
			uint64(in[7]-minv)<<34 |
			uint64(in[8]-minv)<<48 |
			uint64(in[9]-minv)<<62

	out[2] =
		uint64(in[9]-minv)>>2 |
			uint64(in[10]-minv)<<12 |
			uint64(in[11]-minv)<<26 |
			uint64(in[12]-minv)<<40 |
			uint64(in[13]-minv)<<54

	out[3] =
		uint64(in[13]-minv)>>10 |
			uint64(in[14]-minv)<<4 |
			uint64(in[15]-minv)<<18 |
			uint64(in[16]-minv)<<32 |
			uint64(in[17]-minv)<<46 |
			uint64(in[18]-minv)<<60

	out[4] =
		uint64(in[18]-minv)>>4 |
			uint64(in[19]-minv)<<10 |
			uint64(in[20]-minv)<<24 |
			uint64(in[21]-minv)<<38 |
			uint64(in[22]-minv)<<52

	out[5] =
		uint64(in[22]-minv)>>12 |
			uint64(in[23]-minv)<<2 |
			uint64(in[24]-minv)<<16 |
			uint64(in[25]-minv)<<30 |
			uint64(in[26]-minv)<<44 |
			uint64(in[27]-minv)<<58

	out[6] =
		uint64(in[27]-minv)>>6 |
			uint64(in[28]-minv)<<8 |
			uint64(in[29]-minv)<<22 |
			uint64(in[30]-minv)<<36 |
			uint64(in[31]-minv)<<50

	out[7] =
		uint64(in[31]-minv)>>14 |
			uint64(in[32]-minv) |
			uint64(in[33]-minv)<<14 |
			uint64(in[34]-minv)<<28 |
			uint64(in[35]-minv)<<42 |
			uint64(in[36]-minv)<<56

	out[8] =
		uint64(in[36]-minv)>>8 |
			uint64(in[37]-minv)<<6 |
			uint64(in[38]-minv)<<20 |
			uint64(in[39]-minv)<<34 |
			uint64(in[40]-minv)<<48 |
			uint64(in[41]-minv)<<62

	out[9] =
		uint64(in[41]-minv)>>2 |
			uint64(in[42]-minv)<<12 |
			uint64(in[43]-minv)<<26 |
			uint64(in[44]-minv)<<40 |
			uint64(in[45]-minv)<<54

	out[10] =
		uint64(in[45]-minv)>>10 |
			uint64(in[46]-minv)<<4 |
			uint64(in[47]-minv)<<18 |
			uint64(in[48]-minv)<<32 |
			uint64(in[49]-minv)<<46 |
			uint64(in[50]-minv)<<60

	out[11] =
		uint64(in[50]-minv)>>4 |
			uint64(in[51]-minv)<<10 |
			uint64(in[52]-minv)<<24 |
			uint64(in[53]-minv)<<38 |
			uint64(in[54]-minv)<<52

	out[12] =
		uint64(in[54]-minv)>>12 |
			uint64(in[55]-minv)<<2 |
			uint64(in[56]-minv)<<16 |
			uint64(in[57]-minv)<<30 |
			uint64(in[58]-minv)<<44 |
			uint64(in[59]-minv)<<58

	out[13] =
		uint64(in[59]-minv)>>6 |
			uint64(in[60]-minv)<<8 |
			uint64(in[61]-minv)<<22 |
			uint64(in[62]-minv)<<36 |
			uint64(in[63]-minv)<<50
}

func bp64_15(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[15]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<15 |
			uint64(in[2]-minv)<<30 |
			uint64(in[3]-minv)<<45 |
			uint64(in[4]-minv)<<60

	out[1] =
		uint64(in[4]-minv)>>4 |
			uint64(in[5]-minv)<<11 |
			uint64(in[6]-minv)<<26 |
			uint64(in[7]-minv)<<41 |
			uint64(in[8]-minv)<<56

	out[2] =
		uint64(in[8]-minv)>>8 |
			uint64(in[9]-minv)<<7 |
			uint64(in[10]-minv)<<22 |
			uint64(in[11]-minv)<<37 |
			uint64(in[12]-minv)<<52

	out[3] =
		uint64(in[12]-minv)>>12 |
			uint64(in[13]-minv)<<3 |
			uint64(in[14]-minv)<<18 |
			uint64(in[15]-minv)<<33 |
			uint64(in[16]-minv)<<48 |
			uint64(in[17]-minv)<<63

	out[4] =
		uint64(in[17]-minv)>>1 |
			uint64(in[18]-minv)<<14 |
			uint64(in[19]-minv)<<29 |
			uint64(in[20]-minv)<<44 |
			uint64(in[21]-minv)<<59

	out[5] =
		uint64(in[21]-minv)>>5 |
			uint64(in[22]-minv)<<10 |
			uint64(in[23]-minv)<<25 |
			uint64(in[24]-minv)<<40 |
			uint64(in[25]-minv)<<55

	out[6] =
		uint64(in[25]-minv)>>9 |
			uint64(in[26]-minv)<<6 |
			uint64(in[27]-minv)<<21 |
			uint64(in[28]-minv)<<36 |
			uint64(in[29]-minv)<<51

	out[7] =
		uint64(in[29]-minv)>>13 |
			uint64(in[30]-minv)<<2 |
			uint64(in[31]-minv)<<17 |
			uint64(in[32]-minv)<<32 |
			uint64(in[33]-minv)<<47 |
			uint64(in[34]-minv)<<62

	out[8] =
		uint64(in[34]-minv)>>2 |
			uint64(in[35]-minv)<<13 |
			uint64(in[36]-minv)<<28 |
			uint64(in[37]-minv)<<43 |
			uint64(in[38]-minv)<<58

	out[9] =
		uint64(in[38]-minv)>>6 |
			uint64(in[39]-minv)<<9 |
			uint64(in[40]-minv)<<24 |
			uint64(in[41]-minv)<<39 |
			uint64(in[42]-minv)<<54

	out[10] =
		uint64(in[42]-minv)>>10 |
			uint64(in[43]-minv)<<5 |
			uint64(in[44]-minv)<<20 |
			uint64(in[45]-minv)<<35 |
			uint64(in[46]-minv)<<50

	out[11] =
		uint64(in[46]-minv)>>14 |
			uint64(in[47]-minv)<<1 |
			uint64(in[48]-minv)<<16 |
			uint64(in[49]-minv)<<31 |
			uint64(in[50]-minv)<<46 |
			uint64(in[51]-minv)<<61

	out[12] =
		uint64(in[51]-minv)>>3 |
			uint64(in[52]-minv)<<12 |
			uint64(in[53]-minv)<<27 |
			uint64(in[54]-minv)<<42 |
			uint64(in[55]-minv)<<57

	out[13] =
		uint64(in[55]-minv)>>7 |
			uint64(in[56]-minv)<<8 |
			uint64(in[57]-minv)<<23 |
			uint64(in[58]-minv)<<38 |
			uint64(in[59]-minv)<<53

	out[14] =
		uint64(in[59]-minv)>>11 |
			uint64(in[60]-minv)<<4 |
			uint64(in[61]-minv)<<19 |
			uint64(in[62]-minv)<<34 |
			uint64(in[63]-minv)<<49
}

func bp64_16(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[16]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<16 |
			uint64(in[2]-minv)<<32 |
			uint64(in[3]-minv)<<48

	out[1] =
		uint64(in[4]-minv) |
			uint64(in[5]-minv)<<16 |
			uint64(in[6]-minv)<<32 |
			uint64(in[7]-minv)<<48

	out[2] =
		uint64(in[8]-minv) |
			uint64(in[9]-minv)<<16 |
			uint64(in[10]-minv)<<32 |
			uint64(in[11]-minv)<<48

	out[3] =
		uint64(in[12]-minv) |
			uint64(in[13]-minv)<<16 |
			uint64(in[14]-minv)<<32 |
			uint64(in[15]-minv)<<48

	out[4] =
		uint64(in[16]-minv) |
			uint64(in[17]-minv)<<16 |
			uint64(in[18]-minv)<<32 |
			uint64(in[19]-minv)<<48

	out[5] =
		uint64(in[20]-minv) |
			uint64(in[21]-minv)<<16 |
			uint64(in[22]-minv)<<32 |
			uint64(in[23]-minv)<<48

	out[6] =
		uint64(in[24]-minv) |
			uint64(in[25]-minv)<<16 |
			uint64(in[26]-minv)<<32 |
			uint64(in[27]-minv)<<48

	out[7] =
		uint64(in[28]-minv) |
			uint64(in[29]-minv)<<16 |
			uint64(in[30]-minv)<<32 |
			uint64(in[31]-minv)<<48

	out[8] =
		uint64(in[32]-minv) |
			uint64(in[33]-minv)<<16 |
			uint64(in[34]-minv)<<32 |
			uint64(in[35]-minv)<<48

	out[9] =
		uint64(in[36]-minv) |
			uint64(in[37]-minv)<<16 |
			uint64(in[38]-minv)<<32 |
			uint64(in[39]-minv)<<48

	out[10] =
		uint64(in[40]-minv) |
			uint64(in[41]-minv)<<16 |
			uint64(in[42]-minv)<<32 |
			uint64(in[43]-minv)<<48

	out[11] =
		uint64(in[44]-minv) |
			uint64(in[45]-minv)<<16 |
			uint64(in[46]-minv)<<32 |
			uint64(in[47]-minv)<<48

	out[12] =
		uint64(in[48]-minv) |
			uint64(in[49]-minv)<<16 |
			uint64(in[50]-minv)<<32 |
			uint64(in[51]-minv)<<48

	out[13] =
		uint64(in[52]-minv) |
			uint64(in[53]-minv)<<16 |
			uint64(in[54]-minv)<<32 |
			uint64(in[55]-minv)<<48

	out[14] =
		uint64(in[56]-minv) |
			uint64(in[57]-minv)<<16 |
			uint64(in[58]-minv)<<32 |
			uint64(in[59]-minv)<<48

	out[15] =
		uint64(in[60]-minv) |
			uint64(in[61]-minv)<<16 |
			uint64(in[62]-minv)<<32 |
			uint64(in[63]-minv)<<48
}

func bp64_17(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[17]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<17 |
			uint64(in[2]-minv)<<34 |
			uint64(in[3]-minv)<<51

	out[1] =
		uint64(in[3]-minv)>>13 |
			uint64(in[4]-minv)<<4 |
			uint64(in[5]-minv)<<21 |
			uint64(in[6]-minv)<<38 |
			uint64(in[7]-minv)<<55

	out[2] =
		uint64(in[7]-minv)>>9 |
			uint64(in[8]-minv)<<8 |
			uint64(in[9]-minv)<<25 |
			uint64(in[10]-minv)<<42 |
			uint64(in[11]-minv)<<59

	out[3] =
		uint64(in[11]-minv)>>5 |
			uint64(in[12]-minv)<<12 |
			uint64(in[13]-minv)<<29 |
			uint64(in[14]-minv)<<46 |
			uint64(in[15]-minv)<<63

	out[4] =
		uint64(in[15]-minv)>>1 |
			uint64(in[16]-minv)<<16 |
			uint64(in[17]-minv)<<33 |
			uint64(in[18]-minv)<<50

	out[5] =
		uint64(in[18]-minv)>>14 |
			uint64(in[19]-minv)<<3 |
			uint64(in[20]-minv)<<20 |
			uint64(in[21]-minv)<<37 |
			uint64(in[22]-minv)<<54

	out[6] =
		uint64(in[22]-minv)>>10 |
			uint64(in[23]-minv)<<7 |
			uint64(in[24]-minv)<<24 |
			uint64(in[25]-minv)<<41 |
			uint64(in[26]-minv)<<58

	out[7] =
		uint64(in[26]-minv)>>6 |
			uint64(in[27]-minv)<<11 |
			uint64(in[28]-minv)<<28 |
			uint64(in[29]-minv)<<45 |
			uint64(in[30]-minv)<<62

	out[8] =
		uint64(in[30]-minv)>>2 |
			uint64(in[31]-minv)<<15 |
			uint64(in[32]-minv)<<32 |
			uint64(in[33]-minv)<<49

	out[9] =
		uint64(in[33]-minv)>>15 |
			uint64(in[34]-minv)<<2 |
			uint64(in[35]-minv)<<19 |
			uint64(in[36]-minv)<<36 |
			uint64(in[37]-minv)<<53

	out[10] =
		uint64(in[37]-minv)>>11 |
			uint64(in[38]-minv)<<6 |
			uint64(in[39]-minv)<<23 |
			uint64(in[40]-minv)<<40 |
			uint64(in[41]-minv)<<57

	out[11] =
		uint64(in[41]-minv)>>7 |
			uint64(in[42]-minv)<<10 |
			uint64(in[43]-minv)<<27 |
			uint64(in[44]-minv)<<44 |
			uint64(in[45]-minv)<<61

	out[12] =
		uint64(in[45]-minv)>>3 |
			uint64(in[46]-minv)<<14 |
			uint64(in[47]-minv)<<31 |
			uint64(in[48]-minv)<<48

	out[13] =
		uint64(in[48]-minv)>>16 |
			uint64(in[49]-minv)<<1 |
			uint64(in[50]-minv)<<18 |
			uint64(in[51]-minv)<<35 |
			uint64(in[52]-minv)<<52

	out[14] =
		uint64(in[52]-minv)>>12 |
			uint64(in[53]-minv)<<5 |
			uint64(in[54]-minv)<<22 |
			uint64(in[55]-minv)<<39 |
			uint64(in[56]-minv)<<56

	out[15] =
		uint64(in[56]-minv)>>8 |
			uint64(in[57]-minv)<<9 |
			uint64(in[58]-minv)<<26 |
			uint64(in[59]-minv)<<43 |
			uint64(in[60]-minv)<<60

	out[16] =
		uint64(in[60]-minv)>>4 |
			uint64(in[61]-minv)<<13 |
			uint64(in[62]-minv)<<30 |
			uint64(in[63]-minv)<<47
}

func bp64_18(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[18]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<18 |
			uint64(in[2]-minv)<<36 |
			uint64(in[3]-minv)<<54

	out[1] =
		uint64(in[3]-minv)>>10 |
			uint64(in[4]-minv)<<8 |
			uint64(in[5]-minv)<<26 |
			uint64(in[6]-minv)<<44 |
			uint64(in[7]-minv)<<62

	out[2] =
		uint64(in[7]-minv)>>2 |
			uint64(in[8]-minv)<<16 |
			uint64(in[9]-minv)<<34 |
			uint64(in[10]-minv)<<52

	out[3] =
		uint64(in[10]-minv)>>12 |
			uint64(in[11]-minv)<<6 |
			uint64(in[12]-minv)<<24 |
			uint64(in[13]-minv)<<42 |
			uint64(in[14]-minv)<<60

	out[4] =
		uint64(in[14]-minv)>>4 |
			uint64(in[15]-minv)<<14 |
			uint64(in[16]-minv)<<32 |
			uint64(in[17]-minv)<<50

	out[5] =
		uint64(in[17]-minv)>>14 |
			uint64(in[18]-minv)<<4 |
			uint64(in[19]-minv)<<22 |
			uint64(in[20]-minv)<<40 |
			uint64(in[21]-minv)<<58

	out[6] =
		uint64(in[21]-minv)>>6 |
			uint64(in[22]-minv)<<12 |
			uint64(in[23]-minv)<<30 |
			uint64(in[24]-minv)<<48

	out[7] =
		uint64(in[24]-minv)>>16 |
			uint64(in[25]-minv)<<2 |
			uint64(in[26]-minv)<<20 |
			uint64(in[27]-minv)<<38 |
			uint64(in[28]-minv)<<56

	out[8] =
		uint64(in[28]-minv)>>8 |
			uint64(in[29]-minv)<<10 |
			uint64(in[30]-minv)<<28 |
			uint64(in[31]-minv)<<46

	out[9] =
		uint64(in[31]-minv)>>18 |
			uint64(in[32]-minv) |
			uint64(in[33]-minv)<<18 |
			uint64(in[34]-minv)<<36 |
			uint64(in[35]-minv)<<54

	out[10] =
		uint64(in[35]-minv)>>10 |
			uint64(in[36]-minv)<<8 |
			uint64(in[37]-minv)<<26 |
			uint64(in[38]-minv)<<44 |
			uint64(in[39]-minv)<<62

	out[11] =
		uint64(in[39]-minv)>>2 |
			uint64(in[40]-minv)<<16 |
			uint64(in[41]-minv)<<34 |
			uint64(in[42]-minv)<<52

	out[12] =
		uint64(in[42]-minv)>>12 |
			uint64(in[43]-minv)<<6 |
			uint64(in[44]-minv)<<24 |
			uint64(in[45]-minv)<<42 |
			uint64(in[46]-minv)<<60

	out[13] =
		uint64(in[46]-minv)>>4 |
			uint64(in[47]-minv)<<14 |
			uint64(in[48]-minv)<<32 |
			uint64(in[49]-minv)<<50

	out[14] =
		uint64(in[49]-minv)>>14 |
			uint64(in[50]-minv)<<4 |
			uint64(in[51]-minv)<<22 |
			uint64(in[52]-minv)<<40 |
			uint64(in[53]-minv)<<58

	out[15] =
		uint64(in[53]-minv)>>6 |
			uint64(in[54]-minv)<<12 |
			uint64(in[55]-minv)<<30 |
			uint64(in[56]-minv)<<48

	out[16] =
		uint64(in[56]-minv)>>16 |
			uint64(in[57]-minv)<<2 |
			uint64(in[58]-minv)<<20 |
			uint64(in[59]-minv)<<38 |
			uint64(in[60]-minv)<<56

	out[17] =
		uint64(in[60]-minv)>>8 |
			uint64(in[61]-minv)<<10 |
			uint64(in[62]-minv)<<28 |
			uint64(in[63]-minv)<<46
}

func bp64_19(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[19]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<19 |
			uint64(in[2]-minv)<<38 |
			uint64(in[3]-minv)<<57

	out[1] =
		uint64(in[3]-minv)>>7 |
			uint64(in[4]-minv)<<12 |
			uint64(in[5]-minv)<<31 |
			uint64(in[6]-minv)<<50

	out[2] =
		uint64(in[6]-minv)>>14 |
			uint64(in[7]-minv)<<5 |
			uint64(in[8]-minv)<<24 |
			uint64(in[9]-minv)<<43 |
			uint64(in[10]-minv)<<62

	out[3] =
		uint64(in[10]-minv)>>2 |
			uint64(in[11]-minv)<<17 |
			uint64(in[12]-minv)<<36 |
			uint64(in[13]-minv)<<55

	out[4] =
		uint64(in[13]-minv)>>9 |
			uint64(in[14]-minv)<<10 |
			uint64(in[15]-minv)<<29 |
			uint64(in[16]-minv)<<48

	out[5] =
		uint64(in[16]-minv)>>16 |
			uint64(in[17]-minv)<<3 |
			uint64(in[18]-minv)<<22 |
			uint64(in[19]-minv)<<41 |
			uint64(in[20]-minv)<<60

	out[6] =
		uint64(in[20]-minv)>>4 |
			uint64(in[21]-minv)<<15 |
			uint64(in[22]-minv)<<34 |
			uint64(in[23]-minv)<<53

	out[7] =
		uint64(in[23]-minv)>>11 |
			uint64(in[24]-minv)<<8 |
			uint64(in[25]-minv)<<27 |
			uint64(in[26]-minv)<<46

	out[8] =
		uint64(in[26]-minv)>>18 |
			uint64(in[27]-minv)<<1 |
			uint64(in[28]-minv)<<20 |
			uint64(in[29]-minv)<<39 |
			uint64(in[30]-minv)<<58

	out[9] =
		uint64(in[30]-minv)>>6 |
			uint64(in[31]-minv)<<13 |
			uint64(in[32]-minv)<<32 |
			uint64(in[33]-minv)<<51

	out[10] =
		uint64(in[33]-minv)>>13 |
			uint64(in[34]-minv)<<6 |
			uint64(in[35]-minv)<<25 |
			uint64(in[36]-minv)<<44 |
			uint64(in[37]-minv)<<63

	out[11] =
		uint64(in[37]-minv)>>1 |
			uint64(in[38]-minv)<<18 |
			uint64(in[39]-minv)<<37 |
			uint64(in[40]-minv)<<56

	out[12] =
		uint64(in[40]-minv)>>8 |
			uint64(in[41]-minv)<<11 |
			uint64(in[42]-minv)<<30 |
			uint64(in[43]-minv)<<49

	out[13] =
		uint64(in[43]-minv)>>15 |
			uint64(in[44]-minv)<<4 |
			uint64(in[45]-minv)<<23 |
			uint64(in[46]-minv)<<42 |
			uint64(in[47]-minv)<<61

	out[14] =
		uint64(in[47]-minv)>>3 |
			uint64(in[48]-minv)<<16 |
			uint64(in[49]-minv)<<35 |
			uint64(in[50]-minv)<<54

	out[15] =
		uint64(in[50]-minv)>>10 |
			uint64(in[51]-minv)<<9 |
			uint64(in[52]-minv)<<28 |
			uint64(in[53]-minv)<<47

	out[16] =
		uint64(in[53]-minv)>>17 |
			uint64(in[54]-minv)<<2 |
			uint64(in[55]-minv)<<21 |
			uint64(in[56]-minv)<<40 |
			uint64(in[57]-minv)<<59

	out[17] =
		uint64(in[57]-minv)>>5 |
			uint64(in[58]-minv)<<14 |
			uint64(in[59]-minv)<<33 |
			uint64(in[60]-minv)<<52

	out[18] =
		uint64(in[60]-minv)>>12 |
			uint64(in[61]-minv)<<7 |
			uint64(in[62]-minv)<<26 |
			uint64(in[63]-minv)<<45
}

func bp64_20(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[20]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<20 |
			uint64(in[2]-minv)<<40 |
			uint64(in[3]-minv)<<60

	out[1] =
		uint64(in[3]-minv)>>4 |
			uint64(in[4]-minv)<<16 |
			uint64(in[5]-minv)<<36 |
			uint64(in[6]-minv)<<56

	out[2] =
		uint64(in[6]-minv)>>8 |
			uint64(in[7]-minv)<<12 |
			uint64(in[8]-minv)<<32 |
			uint64(in[9]-minv)<<52

	out[3] =
		uint64(in[9]-minv)>>12 |
			uint64(in[10]-minv)<<8 |
			uint64(in[11]-minv)<<28 |
			uint64(in[12]-minv)<<48

	out[4] =
		uint64(in[12]-minv)>>16 |
			uint64(in[13]-minv)<<4 |
			uint64(in[14]-minv)<<24 |
			uint64(in[15]-minv)<<44

	out[5] =
		uint64(in[15]-minv)>>20 |
			uint64(in[16]-minv) |
			uint64(in[17]-minv)<<20 |
			uint64(in[18]-minv)<<40 |
			uint64(in[19]-minv)<<60

	out[6] =
		uint64(in[19]-minv)>>4 |
			uint64(in[20]-minv)<<16 |
			uint64(in[21]-minv)<<36 |
			uint64(in[22]-minv)<<56

	out[7] =
		uint64(in[22]-minv)>>8 |
			uint64(in[23]-minv)<<12 |
			uint64(in[24]-minv)<<32 |
			uint64(in[25]-minv)<<52

	out[8] =
		uint64(in[25]-minv)>>12 |
			uint64(in[26]-minv)<<8 |
			uint64(in[27]-minv)<<28 |
			uint64(in[28]-minv)<<48

	out[9] =
		uint64(in[28]-minv)>>16 |
			uint64(in[29]-minv)<<4 |
			uint64(in[30]-minv)<<24 |
			uint64(in[31]-minv)<<44

	out[10] =
		uint64(in[31]-minv)>>20 |
			uint64(in[32]-minv) |
			uint64(in[33]-minv)<<20 |
			uint64(in[34]-minv)<<40 |
			uint64(in[35]-minv)<<60

	out[11] =
		uint64(in[35]-minv)>>4 |
			uint64(in[36]-minv)<<16 |
			uint64(in[37]-minv)<<36 |
			uint64(in[38]-minv)<<56

	out[12] =
		uint64(in[38]-minv)>>8 |
			uint64(in[39]-minv)<<12 |
			uint64(in[40]-minv)<<32 |
			uint64(in[41]-minv)<<52

	out[13] =
		uint64(in[41]-minv)>>12 |
			uint64(in[42]-minv)<<8 |
			uint64(in[43]-minv)<<28 |
			uint64(in[44]-minv)<<48

	out[14] =
		uint64(in[44]-minv)>>16 |
			uint64(in[45]-minv)<<4 |
			uint64(in[46]-minv)<<24 |
			uint64(in[47]-minv)<<44

	out[15] =
		uint64(in[47]-minv)>>20 |
			uint64(in[48]-minv) |
			uint64(in[49]-minv)<<20 |
			uint64(in[50]-minv)<<40 |
			uint64(in[51]-minv)<<60

	out[16] =
		uint64(in[51]-minv)>>4 |
			uint64(in[52]-minv)<<16 |
			uint64(in[53]-minv)<<36 |
			uint64(in[54]-minv)<<56

	out[17] =
		uint64(in[54]-minv)>>8 |
			uint64(in[55]-minv)<<12 |
			uint64(in[56]-minv)<<32 |
			uint64(in[57]-minv)<<52

	out[18] =
		uint64(in[57]-minv)>>12 |
			uint64(in[58]-minv)<<8 |
			uint64(in[59]-minv)<<28 |
			uint64(in[60]-minv)<<48

	out[19] =
		uint64(in[60]-minv)>>16 |
			uint64(in[61]-minv)<<4 |
			uint64(in[62]-minv)<<24 |
			uint64(in[63]-minv)<<44
}

func bp64_21(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[21]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<21 |
			uint64(in[2]-minv)<<42 |
			uint64(in[3]-minv)<<63

	out[1] =
		uint64(in[3]-minv)>>1 |
			uint64(in[4]-minv)<<20 |
			uint64(in[5]-minv)<<41 |
			uint64(in[6]-minv)<<62

	out[2] =
		uint64(in[6]-minv)>>2 |
			uint64(in[7]-minv)<<19 |
			uint64(in[8]-minv)<<40 |
			uint64(in[9]-minv)<<61

	out[3] =
		uint64(in[9]-minv)>>3 |
			uint64(in[10]-minv)<<18 |
			uint64(in[11]-minv)<<39 |
			uint64(in[12]-minv)<<60

	out[4] =
		uint64(in[12]-minv)>>4 |
			uint64(in[13]-minv)<<17 |
			uint64(in[14]-minv)<<38 |
			uint64(in[15]-minv)<<59

	out[5] =
		uint64(in[15]-minv)>>5 |
			uint64(in[16]-minv)<<16 |
			uint64(in[17]-minv)<<37 |
			uint64(in[18]-minv)<<58

	out[6] =
		uint64(in[18]-minv)>>6 |
			uint64(in[19]-minv)<<15 |
			uint64(in[20]-minv)<<36 |
			uint64(in[21]-minv)<<57

	out[7] =
		uint64(in[21]-minv)>>7 |
			uint64(in[22]-minv)<<14 |
			uint64(in[23]-minv)<<35 |
			uint64(in[24]-minv)<<56

	out[8] =
		uint64(in[24]-minv)>>8 |
			uint64(in[25]-minv)<<13 |
			uint64(in[26]-minv)<<34 |
			uint64(in[27]-minv)<<55

	out[9] =
		uint64(in[27]-minv)>>9 |
			uint64(in[28]-minv)<<12 |
			uint64(in[29]-minv)<<33 |
			uint64(in[30]-minv)<<54

	out[10] =
		uint64(in[30]-minv)>>10 |
			uint64(in[31]-minv)<<11 |
			uint64(in[32]-minv)<<32 |
			uint64(in[33]-minv)<<53

	out[11] =
		uint64(in[33]-minv)>>11 |
			uint64(in[34]-minv)<<10 |
			uint64(in[35]-minv)<<31 |
			uint64(in[36]-minv)<<52

	out[12] =
		uint64(in[36]-minv)>>12 |
			uint64(in[37]-minv)<<9 |
			uint64(in[38]-minv)<<30 |
			uint64(in[39]-minv)<<51

	out[13] =
		uint64(in[39]-minv)>>13 |
			uint64(in[40]-minv)<<8 |
			uint64(in[41]-minv)<<29 |
			uint64(in[42]-minv)<<50

	out[14] =
		uint64(in[42]-minv)>>14 |
			uint64(in[43]-minv)<<7 |
			uint64(in[44]-minv)<<28 |
			uint64(in[45]-minv)<<49

	out[15] =
		uint64(in[45]-minv)>>15 |
			uint64(in[46]-minv)<<6 |
			uint64(in[47]-minv)<<27 |
			uint64(in[48]-minv)<<48

	out[16] =
		uint64(in[48]-minv)>>16 |
			uint64(in[49]-minv)<<5 |
			uint64(in[50]-minv)<<26 |
			uint64(in[51]-minv)<<47

	out[17] =
		uint64(in[51]-minv)>>17 |
			uint64(in[52]-minv)<<4 |
			uint64(in[53]-minv)<<25 |
			uint64(in[54]-minv)<<46

	out[18] =
		uint64(in[54]-minv)>>18 |
			uint64(in[55]-minv)<<3 |
			uint64(in[56]-minv)<<24 |
			uint64(in[57]-minv)<<45

	out[19] =
		uint64(in[57]-minv)>>19 |
			uint64(in[58]-minv)<<2 |
			uint64(in[59]-minv)<<23 |
			uint64(in[60]-minv)<<44

	out[20] =
		uint64(in[60]-minv)>>20 |
			uint64(in[61]-minv)<<1 |
			uint64(in[62]-minv)<<22 |
			uint64(in[63]-minv)<<43
}

func bp64_22(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[22]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<22 |
			uint64(in[2]-minv)<<44

	out[1] =
		uint64(in[2]-minv)>>20 |
			uint64(in[3]-minv)<<2 |
			uint64(in[4]-minv)<<24 |
			uint64(in[5]-minv)<<46

	out[2] =
		uint64(in[5]-minv)>>18 |
			uint64(in[6]-minv)<<4 |
			uint64(in[7]-minv)<<26 |
			uint64(in[8]-minv)<<48

	out[3] =
		uint64(in[8]-minv)>>16 |
			uint64(in[9]-minv)<<6 |
			uint64(in[10]-minv)<<28 |
			uint64(in[11]-minv)<<50

	out[4] =
		uint64(in[11]-minv)>>14 |
			uint64(in[12]-minv)<<8 |
			uint64(in[13]-minv)<<30 |
			uint64(in[14]-minv)<<52

	out[5] =
		uint64(in[14]-minv)>>12 |
			uint64(in[15]-minv)<<10 |
			uint64(in[16]-minv)<<32 |
			uint64(in[17]-minv)<<54

	out[6] =
		uint64(in[17]-minv)>>10 |
			uint64(in[18]-minv)<<12 |
			uint64(in[19]-minv)<<34 |
			uint64(in[20]-minv)<<56

	out[7] =
		uint64(in[20]-minv)>>8 |
			uint64(in[21]-minv)<<14 |
			uint64(in[22]-minv)<<36 |
			uint64(in[23]-minv)<<58

	out[8] =
		uint64(in[23]-minv)>>6 |
			uint64(in[24]-minv)<<16 |
			uint64(in[25]-minv)<<38 |
			uint64(in[26]-minv)<<60

	out[9] =
		uint64(in[26]-minv)>>4 |
			uint64(in[27]-minv)<<18 |
			uint64(in[28]-minv)<<40 |
			uint64(in[29]-minv)<<62

	out[10] =
		uint64(in[29]-minv)>>2 |
			uint64(in[30]-minv)<<20 |
			uint64(in[31]-minv)<<42

	out[11] =
		uint64(in[31]-minv)>>22 |
			uint64(in[32]-minv) |
			uint64(in[33]-minv)<<22 |
			uint64(in[34]-minv)<<44

	out[12] =
		uint64(in[34]-minv)>>20 |
			uint64(in[35]-minv)<<2 |
			uint64(in[36]-minv)<<24 |
			uint64(in[37]-minv)<<46

	out[13] =
		uint64(in[37]-minv)>>18 |
			uint64(in[38]-minv)<<4 |
			uint64(in[39]-minv)<<26 |
			uint64(in[40]-minv)<<48

	out[14] =
		uint64(in[40]-minv)>>16 |
			uint64(in[41]-minv)<<6 |
			uint64(in[42]-minv)<<28 |
			uint64(in[43]-minv)<<50

	out[15] =
		uint64(in[43]-minv)>>14 |
			uint64(in[44]-minv)<<8 |
			uint64(in[45]-minv)<<30 |
			uint64(in[46]-minv)<<52

	out[16] =
		uint64(in[46]-minv)>>12 |
			uint64(in[47]-minv)<<10 |
			uint64(in[48]-minv)<<32 |
			uint64(in[49]-minv)<<54

	out[17] =
		uint64(in[49]-minv)>>10 |
			uint64(in[50]-minv)<<12 |
			uint64(in[51]-minv)<<34 |
			uint64(in[52]-minv)<<56

	out[18] =
		uint64(in[52]-minv)>>8 |
			uint64(in[53]-minv)<<14 |
			uint64(in[54]-minv)<<36 |
			uint64(in[55]-minv)<<58

	out[19] =
		uint64(in[55]-minv)>>6 |
			uint64(in[56]-minv)<<16 |
			uint64(in[57]-minv)<<38 |
			uint64(in[58]-minv)<<60

	out[20] =
		uint64(in[58]-minv)>>4 |
			uint64(in[59]-minv)<<18 |
			uint64(in[60]-minv)<<40 |
			uint64(in[61]-minv)<<62

	out[21] =
		uint64(in[61]-minv)>>2 |
			uint64(in[62]-minv)<<20 |
			uint64(in[63]-minv)<<42
}

func bp64_23(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[23]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<23 |
			uint64(in[2]-minv)<<46

	out[1] =
		uint64(in[2]-minv)>>18 |
			uint64(in[3]-minv)<<5 |
			uint64(in[4]-minv)<<28 |
			uint64(in[5]-minv)<<51

	out[2] =
		uint64(in[5]-minv)>>13 |
			uint64(in[6]-minv)<<10 |
			uint64(in[7]-minv)<<33 |
			uint64(in[8]-minv)<<56

	out[3] =
		uint64(in[8]-minv)>>8 |
			uint64(in[9]-minv)<<15 |
			uint64(in[10]-minv)<<38 |
			uint64(in[11]-minv)<<61

	out[4] =
		uint64(in[11]-minv)>>3 |
			uint64(in[12]-minv)<<20 |
			uint64(in[13]-minv)<<43

	out[5] =
		uint64(in[13]-minv)>>21 |
			uint64(in[14]-minv)<<2 |
			uint64(in[15]-minv)<<25 |
			uint64(in[16]-minv)<<48

	out[6] =
		uint64(in[16]-minv)>>16 |
			uint64(in[17]-minv)<<7 |
			uint64(in[18]-minv)<<30 |
			uint64(in[19]-minv)<<53

	out[7] =
		uint64(in[19]-minv)>>11 |
			uint64(in[20]-minv)<<12 |
			uint64(in[21]-minv)<<35 |
			uint64(in[22]-minv)<<58

	out[8] =
		uint64(in[22]-minv)>>6 |
			uint64(in[23]-minv)<<17 |
			uint64(in[24]-minv)<<40 |
			uint64(in[25]-minv)<<63

	out[9] =
		uint64(in[25]-minv)>>1 |
			uint64(in[26]-minv)<<22 |
			uint64(in[27]-minv)<<45

	out[10] =
		uint64(in[27]-minv)>>19 |
			uint64(in[28]-minv)<<4 |
			uint64(in[29]-minv)<<27 |
			uint64(in[30]-minv)<<50

	out[11] =
		uint64(in[30]-minv)>>14 |
			uint64(in[31]-minv)<<9 |
			uint64(in[32]-minv)<<32 |
			uint64(in[33]-minv)<<55

	out[12] =
		uint64(in[33]-minv)>>9 |
			uint64(in[34]-minv)<<14 |
			uint64(in[35]-minv)<<37 |
			uint64(in[36]-minv)<<60

	out[13] =
		uint64(in[36]-minv)>>4 |
			uint64(in[37]-minv)<<19 |
			uint64(in[38]-minv)<<42

	out[14] =
		uint64(in[38]-minv)>>22 |
			uint64(in[39]-minv)<<1 |
			uint64(in[40]-minv)<<24 |
			uint64(in[41]-minv)<<47

	out[15] =
		uint64(in[41]-minv)>>17 |
			uint64(in[42]-minv)<<6 |
			uint64(in[43]-minv)<<29 |
			uint64(in[44]-minv)<<52

	out[16] =
		uint64(in[44]-minv)>>12 |
			uint64(in[45]-minv)<<11 |
			uint64(in[46]-minv)<<34 |
			uint64(in[47]-minv)<<57

	out[17] =
		uint64(in[47]-minv)>>7 |
			uint64(in[48]-minv)<<16 |
			uint64(in[49]-minv)<<39 |
			uint64(in[50]-minv)<<62

	out[18] =
		uint64(in[50]-minv)>>2 |
			uint64(in[51]-minv)<<21 |
			uint64(in[52]-minv)<<44

	out[19] =
		uint64(in[52]-minv)>>20 |
			uint64(in[53]-minv)<<3 |
			uint64(in[54]-minv)<<26 |
			uint64(in[55]-minv)<<49

	out[20] =
		uint64(in[55]-minv)>>15 |
			uint64(in[56]-minv)<<8 |
			uint64(in[57]-minv)<<31 |
			uint64(in[58]-minv)<<54

	out[21] =
		uint64(in[58]-minv)>>10 |
			uint64(in[59]-minv)<<13 |
			uint64(in[60]-minv)<<36 |
			uint64(in[61]-minv)<<59

	out[22] =
		uint64(in[61]-minv)>>5 |
			uint64(in[62]-minv)<<18 |
			uint64(in[63]-minv)<<41
}

func bp64_24(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[24]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<24 |
			uint64(in[2]-minv)<<48

	out[1] =
		uint64(in[2]-minv)>>16 |
			uint64(in[3]-minv)<<8 |
			uint64(in[4]-minv)<<32 |
			uint64(in[5]-minv)<<56

	out[2] =
		uint64(in[5]-minv)>>8 |
			uint64(in[6]-minv)<<16 |
			uint64(in[7]-minv)<<40

	out[3] =
		uint64(in[7]-minv)>>24 |
			uint64(in[8]-minv) |
			uint64(in[9]-minv)<<24 |
			uint64(in[10]-minv)<<48

	out[4] =
		uint64(in[10]-minv)>>16 |
			uint64(in[11]-minv)<<8 |
			uint64(in[12]-minv)<<32 |
			uint64(in[13]-minv)<<56

	out[5] =
		uint64(in[13]-minv)>>8 |
			uint64(in[14]-minv)<<16 |
			uint64(in[15]-minv)<<40

	out[6] =
		uint64(in[15]-minv)>>24 |
			uint64(in[16]-minv) |
			uint64(in[17]-minv)<<24 |
			uint64(in[18]-minv)<<48

	out[7] =
		uint64(in[18]-minv)>>16 |
			uint64(in[19]-minv)<<8 |
			uint64(in[20]-minv)<<32 |
			uint64(in[21]-minv)<<56

	out[8] =
		uint64(in[21]-minv)>>8 |
			uint64(in[22]-minv)<<16 |
			uint64(in[23]-minv)<<40

	out[9] =
		uint64(in[23]-minv)>>24 |
			uint64(in[24]-minv) |
			uint64(in[25]-minv)<<24 |
			uint64(in[26]-minv)<<48

	out[10] =
		uint64(in[26]-minv)>>16 |
			uint64(in[27]-minv)<<8 |
			uint64(in[28]-minv)<<32 |
			uint64(in[29]-minv)<<56

	out[11] =
		uint64(in[29]-minv)>>8 |
			uint64(in[30]-minv)<<16 |
			uint64(in[31]-minv)<<40

	out[12] =
		uint64(in[31]-minv)>>24 |
			uint64(in[32]-minv) |
			uint64(in[33]-minv)<<24 |
			uint64(in[34]-minv)<<48

	out[13] =
		uint64(in[34]-minv)>>16 |
			uint64(in[35]-minv)<<8 |
			uint64(in[36]-minv)<<32 |
			uint64(in[37]-minv)<<56

	out[14] =
		uint64(in[37]-minv)>>8 |
			uint64(in[38]-minv)<<16 |
			uint64(in[39]-minv)<<40

	out[15] =
		uint64(in[39]-minv)>>24 |
			uint64(in[40]-minv) |
			uint64(in[41]-minv)<<24 |
			uint64(in[42]-minv)<<48

	out[16] =
		uint64(in[42]-minv)>>16 |
			uint64(in[43]-minv)<<8 |
			uint64(in[44]-minv)<<32 |
			uint64(in[45]-minv)<<56

	out[17] =
		uint64(in[45]-minv)>>8 |
			uint64(in[46]-minv)<<16 |
			uint64(in[47]-minv)<<40

	out[18] =
		uint64(in[47]-minv)>>24 |
			uint64(in[48]-minv) |
			uint64(in[49]-minv)<<24 |
			uint64(in[50]-minv)<<48

	out[19] =
		uint64(in[50]-minv)>>16 |
			uint64(in[51]-minv)<<8 |
			uint64(in[52]-minv)<<32 |
			uint64(in[53]-minv)<<56

	out[20] =
		uint64(in[53]-minv)>>8 |
			uint64(in[54]-minv)<<16 |
			uint64(in[55]-minv)<<40

	out[21] =
		uint64(in[55]-minv)>>24 |
			uint64(in[56]-minv) |
			uint64(in[57]-minv)<<24 |
			uint64(in[58]-minv)<<48

	out[22] =
		uint64(in[58]-minv)>>16 |
			uint64(in[59]-minv)<<8 |
			uint64(in[60]-minv)<<32 |
			uint64(in[61]-minv)<<56

	out[23] =
		uint64(in[61]-minv)>>8 |
			uint64(in[62]-minv)<<16 |
			uint64(in[63]-minv)<<40
}

func bp64_25(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[25]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<25 |
			uint64(in[2]-minv)<<50

	out[1] =
		uint64(in[2]-minv)>>14 |
			uint64(in[3]-minv)<<11 |
			uint64(in[4]-minv)<<36 |
			uint64(in[5]-minv)<<61

	out[2] =
		uint64(in[5]-minv)>>3 |
			uint64(in[6]-minv)<<22 |
			uint64(in[7]-minv)<<47

	out[3] =
		uint64(in[7]-minv)>>17 |
			uint64(in[8]-minv)<<8 |
			uint64(in[9]-minv)<<33 |
			uint64(in[10]-minv)<<58

	out[4] =
		uint64(in[10]-minv)>>6 |
			uint64(in[11]-minv)<<19 |
			uint64(in[12]-minv)<<44

	out[5] =
		uint64(in[12]-minv)>>20 |
			uint64(in[13]-minv)<<5 |
			uint64(in[14]-minv)<<30 |
			uint64(in[15]-minv)<<55

	out[6] =
		uint64(in[15]-minv)>>9 |
			uint64(in[16]-minv)<<16 |
			uint64(in[17]-minv)<<41

	out[7] =
		uint64(in[17]-minv)>>23 |
			uint64(in[18]-minv)<<2 |
			uint64(in[19]-minv)<<27 |
			uint64(in[20]-minv)<<52

	out[8] =
		uint64(in[20]-minv)>>12 |
			uint64(in[21]-minv)<<13 |
			uint64(in[22]-minv)<<38 |
			uint64(in[23]-minv)<<63

	out[9] =
		uint64(in[23]-minv)>>1 |
			uint64(in[24]-minv)<<24 |
			uint64(in[25]-minv)<<49

	out[10] =
		uint64(in[25]-minv)>>15 |
			uint64(in[26]-minv)<<10 |
			uint64(in[27]-minv)<<35 |
			uint64(in[28]-minv)<<60

	out[11] =
		uint64(in[28]-minv)>>4 |
			uint64(in[29]-minv)<<21 |
			uint64(in[30]-minv)<<46

	out[12] =
		uint64(in[30]-minv)>>18 |
			uint64(in[31]-minv)<<7 |
			uint64(in[32]-minv)<<32 |
			uint64(in[33]-minv)<<57

	out[13] =
		uint64(in[33]-minv)>>7 |
			uint64(in[34]-minv)<<18 |
			uint64(in[35]-minv)<<43

	out[14] =
		uint64(in[35]-minv)>>21 |
			uint64(in[36]-minv)<<4 |
			uint64(in[37]-minv)<<29 |
			uint64(in[38]-minv)<<54

	out[15] =
		uint64(in[38]-minv)>>10 |
			uint64(in[39]-minv)<<15 |
			uint64(in[40]-minv)<<40

	out[16] =
		uint64(in[40]-minv)>>24 |
			uint64(in[41]-minv)<<1 |
			uint64(in[42]-minv)<<26 |
			uint64(in[43]-minv)<<51

	out[17] =
		uint64(in[43]-minv)>>13 |
			uint64(in[44]-minv)<<12 |
			uint64(in[45]-minv)<<37 |
			uint64(in[46]-minv)<<62

	out[18] =
		uint64(in[46]-minv)>>2 |
			uint64(in[47]-minv)<<23 |
			uint64(in[48]-minv)<<48

	out[19] =
		uint64(in[48]-minv)>>16 |
			uint64(in[49]-minv)<<9 |
			uint64(in[50]-minv)<<34 |
			uint64(in[51]-minv)<<59

	out[20] =
		uint64(in[51]-minv)>>5 |
			uint64(in[52]-minv)<<20 |
			uint64(in[53]-minv)<<45

	out[21] =
		uint64(in[53]-minv)>>19 |
			uint64(in[54]-minv)<<6 |
			uint64(in[55]-minv)<<31 |
			uint64(in[56]-minv)<<56

	out[22] =
		uint64(in[56]-minv)>>8 |
			uint64(in[57]-minv)<<17 |
			uint64(in[58]-minv)<<42

	out[23] =
		uint64(in[58]-minv)>>22 |
			uint64(in[59]-minv)<<3 |
			uint64(in[60]-minv)<<28 |
			uint64(in[61]-minv)<<53

	out[24] =
		uint64(in[61]-minv)>>11 |
			uint64(in[62]-minv)<<14 |
			uint64(in[63]-minv)<<39
}

func bp64_26(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[26]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<26 |
			uint64(in[2]-minv)<<52

	out[1] =
		uint64(in[2]-minv)>>12 |
			uint64(in[3]-minv)<<14 |
			uint64(in[4]-minv)<<40

	out[2] =
		uint64(in[4]-minv)>>24 |
			uint64(in[5]-minv)<<2 |
			uint64(in[6]-minv)<<28 |
			uint64(in[7]-minv)<<54

	out[3] =
		uint64(in[7]-minv)>>10 |
			uint64(in[8]-minv)<<16 |
			uint64(in[9]-minv)<<42

	out[4] =
		uint64(in[9]-minv)>>22 |
			uint64(in[10]-minv)<<4 |
			uint64(in[11]-minv)<<30 |
			uint64(in[12]-minv)<<56

	out[5] =
		uint64(in[12]-minv)>>8 |
			uint64(in[13]-minv)<<18 |
			uint64(in[14]-minv)<<44

	out[6] =
		uint64(in[14]-minv)>>20 |
			uint64(in[15]-minv)<<6 |
			uint64(in[16]-minv)<<32 |
			uint64(in[17]-minv)<<58

	out[7] =
		uint64(in[17]-minv)>>6 |
			uint64(in[18]-minv)<<20 |
			uint64(in[19]-minv)<<46

	out[8] =
		uint64(in[19]-minv)>>18 |
			uint64(in[20]-minv)<<8 |
			uint64(in[21]-minv)<<34 |
			uint64(in[22]-minv)<<60

	out[9] =
		uint64(in[22]-minv)>>4 |
			uint64(in[23]-minv)<<22 |
			uint64(in[24]-minv)<<48

	out[10] =
		uint64(in[24]-minv)>>16 |
			uint64(in[25]-minv)<<10 |
			uint64(in[26]-minv)<<36 |
			uint64(in[27]-minv)<<62

	out[11] =
		uint64(in[27]-minv)>>2 |
			uint64(in[28]-minv)<<24 |
			uint64(in[29]-minv)<<50

	out[12] =
		uint64(in[29]-minv)>>14 |
			uint64(in[30]-minv)<<12 |
			uint64(in[31]-minv)<<38

	out[13] =
		uint64(in[31]-minv)>>26 |
			uint64(in[32]-minv) |
			uint64(in[33]-minv)<<26 |
			uint64(in[34]-minv)<<52

	out[14] =
		uint64(in[34]-minv)>>12 |
			uint64(in[35]-minv)<<14 |
			uint64(in[36]-minv)<<40

	out[15] =
		uint64(in[36]-minv)>>24 |
			uint64(in[37]-minv)<<2 |
			uint64(in[38]-minv)<<28 |
			uint64(in[39]-minv)<<54

	out[16] =
		uint64(in[39]-minv)>>10 |
			uint64(in[40]-minv)<<16 |
			uint64(in[41]-minv)<<42

	out[17] =
		uint64(in[41]-minv)>>22 |
			uint64(in[42]-minv)<<4 |
			uint64(in[43]-minv)<<30 |
			uint64(in[44]-minv)<<56

	out[18] =
		uint64(in[44]-minv)>>8 |
			uint64(in[45]-minv)<<18 |
			uint64(in[46]-minv)<<44

	out[19] =
		uint64(in[46]-minv)>>20 |
			uint64(in[47]-minv)<<6 |
			uint64(in[48]-minv)<<32 |
			uint64(in[49]-minv)<<58

	out[20] =
		uint64(in[49]-minv)>>6 |
			uint64(in[50]-minv)<<20 |
			uint64(in[51]-minv)<<46

	out[21] =
		uint64(in[51]-minv)>>18 |
			uint64(in[52]-minv)<<8 |
			uint64(in[53]-minv)<<34 |
			uint64(in[54]-minv)<<60

	out[22] =
		uint64(in[54]-minv)>>4 |
			uint64(in[55]-minv)<<22 |
			uint64(in[56]-minv)<<48

	out[23] =
		uint64(in[56]-minv)>>16 |
			uint64(in[57]-minv)<<10 |
			uint64(in[58]-minv)<<36 |
			uint64(in[59]-minv)<<62

	out[24] =
		uint64(in[59]-minv)>>2 |
			uint64(in[60]-minv)<<24 |
			uint64(in[61]-minv)<<50

	out[25] =
		uint64(in[61]-minv)>>14 |
			uint64(in[62]-minv)<<12 |
			uint64(in[63]-minv)<<38
}

func bp64_27(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[27]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<27 |
			uint64(in[2]-minv)<<54

	out[1] =
		uint64(in[2]-minv)>>10 |
			uint64(in[3]-minv)<<17 |
			uint64(in[4]-minv)<<44

	out[2] =
		uint64(in[4]-minv)>>20 |
			uint64(in[5]-minv)<<7 |
			uint64(in[6]-minv)<<34 |
			uint64(in[7]-minv)<<61

	out[3] =
		uint64(in[7]-minv)>>3 |
			uint64(in[8]-minv)<<24 |
			uint64(in[9]-minv)<<51

	out[4] =
		uint64(in[9]-minv)>>13 |
			uint64(in[10]-minv)<<14 |
			uint64(in[11]-minv)<<41

	out[5] =
		uint64(in[11]-minv)>>23 |
			uint64(in[12]-minv)<<4 |
			uint64(in[13]-minv)<<31 |
			uint64(in[14]-minv)<<58

	out[6] =
		uint64(in[14]-minv)>>6 |
			uint64(in[15]-minv)<<21 |
			uint64(in[16]-minv)<<48

	out[7] =
		uint64(in[16]-minv)>>16 |
			uint64(in[17]-minv)<<11 |
			uint64(in[18]-minv)<<38

	out[8] =
		uint64(in[18]-minv)>>26 |
			uint64(in[19]-minv)<<1 |
			uint64(in[20]-minv)<<28 |
			uint64(in[21]-minv)<<55

	out[9] =
		uint64(in[21]-minv)>>9 |
			uint64(in[22]-minv)<<18 |
			uint64(in[23]-minv)<<45

	out[10] =
		uint64(in[23]-minv)>>19 |
			uint64(in[24]-minv)<<8 |
			uint64(in[25]-minv)<<35 |
			uint64(in[26]-minv)<<62

	out[11] =
		uint64(in[26]-minv)>>2 |
			uint64(in[27]-minv)<<25 |
			uint64(in[28]-minv)<<52

	out[12] =
		uint64(in[28]-minv)>>12 |
			uint64(in[29]-minv)<<15 |
			uint64(in[30]-minv)<<42

	out[13] =
		uint64(in[30]-minv)>>22 |
			uint64(in[31]-minv)<<5 |
			uint64(in[32]-minv)<<32 |
			uint64(in[33]-minv)<<59

	out[14] =
		uint64(in[33]-minv)>>5 |
			uint64(in[34]-minv)<<22 |
			uint64(in[35]-minv)<<49

	out[15] =
		uint64(in[35]-minv)>>15 |
			uint64(in[36]-minv)<<12 |
			uint64(in[37]-minv)<<39

	out[16] =
		uint64(in[37]-minv)>>25 |
			uint64(in[38]-minv)<<2 |
			uint64(in[39]-minv)<<29 |
			uint64(in[40]-minv)<<56

	out[17] =
		uint64(in[40]-minv)>>8 |
			uint64(in[41]-minv)<<19 |
			uint64(in[42]-minv)<<46

	out[18] =
		uint64(in[42]-minv)>>18 |
			uint64(in[43]-minv)<<9 |
			uint64(in[44]-minv)<<36 |
			uint64(in[45]-minv)<<63

	out[19] =
		uint64(in[45]-minv)>>1 |
			uint64(in[46]-minv)<<26 |
			uint64(in[47]-minv)<<53

	out[20] =
		uint64(in[47]-minv)>>11 |
			uint64(in[48]-minv)<<16 |
			uint64(in[49]-minv)<<43

	out[21] =
		uint64(in[49]-minv)>>21 |
			uint64(in[50]-minv)<<6 |
			uint64(in[51]-minv)<<33 |
			uint64(in[52]-minv)<<60

	out[22] =
		uint64(in[52]-minv)>>4 |
			uint64(in[53]-minv)<<23 |
			uint64(in[54]-minv)<<50

	out[23] =
		uint64(in[54]-minv)>>14 |
			uint64(in[55]-minv)<<13 |
			uint64(in[56]-minv)<<40

	out[24] =
		uint64(in[56]-minv)>>24 |
			uint64(in[57]-minv)<<3 |
			uint64(in[58]-minv)<<30 |
			uint64(in[59]-minv)<<57

	out[25] =
		uint64(in[59]-minv)>>7 |
			uint64(in[60]-minv)<<20 |
			uint64(in[61]-minv)<<47

	out[26] =
		uint64(in[61]-minv)>>17 |
			uint64(in[62]-minv)<<10 |
			uint64(in[63]-minv)<<37
}

func bp64_28(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[28]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<28 |
			uint64(in[2]-minv)<<56

	out[1] =
		uint64(in[2]-minv)>>8 |
			uint64(in[3]-minv)<<20 |
			uint64(in[4]-minv)<<48

	out[2] =
		uint64(in[4]-minv)>>16 |
			uint64(in[5]-minv)<<12 |
			uint64(in[6]-minv)<<40

	out[3] =
		uint64(in[6]-minv)>>24 |
			uint64(in[7]-minv)<<4 |
			uint64(in[8]-minv)<<32 |
			uint64(in[9]-minv)<<60

	out[4] =
		uint64(in[9]-minv)>>4 |
			uint64(in[10]-minv)<<24 |
			uint64(in[11]-minv)<<52

	out[5] =
		uint64(in[11]-minv)>>12 |
			uint64(in[12]-minv)<<16 |
			uint64(in[13]-minv)<<44

	out[6] =
		uint64(in[13]-minv)>>20 |
			uint64(in[14]-minv)<<8 |
			uint64(in[15]-minv)<<36

	out[7] =
		uint64(in[15]-minv)>>28 |
			uint64(in[16]-minv) |
			uint64(in[17]-minv)<<28 |
			uint64(in[18]-minv)<<56

	out[8] =
		uint64(in[18]-minv)>>8 |
			uint64(in[19]-minv)<<20 |
			uint64(in[20]-minv)<<48

	out[9] =
		uint64(in[20]-minv)>>16 |
			uint64(in[21]-minv)<<12 |
			uint64(in[22]-minv)<<40

	out[10] =
		uint64(in[22]-minv)>>24 |
			uint64(in[23]-minv)<<4 |
			uint64(in[24]-minv)<<32 |
			uint64(in[25]-minv)<<60

	out[11] =
		uint64(in[25]-minv)>>4 |
			uint64(in[26]-minv)<<24 |
			uint64(in[27]-minv)<<52

	out[12] =
		uint64(in[27]-minv)>>12 |
			uint64(in[28]-minv)<<16 |
			uint64(in[29]-minv)<<44

	out[13] =
		uint64(in[29]-minv)>>20 |
			uint64(in[30]-minv)<<8 |
			uint64(in[31]-minv)<<36

	out[14] =
		uint64(in[31]-minv)>>28 |
			uint64(in[32]-minv) |
			uint64(in[33]-minv)<<28 |
			uint64(in[34]-minv)<<56

	out[15] =
		uint64(in[34]-minv)>>8 |
			uint64(in[35]-minv)<<20 |
			uint64(in[36]-minv)<<48

	out[16] =
		uint64(in[36]-minv)>>16 |
			uint64(in[37]-minv)<<12 |
			uint64(in[38]-minv)<<40

	out[17] =
		uint64(in[38]-minv)>>24 |
			uint64(in[39]-minv)<<4 |
			uint64(in[40]-minv)<<32 |
			uint64(in[41]-minv)<<60

	out[18] =
		uint64(in[41]-minv)>>4 |
			uint64(in[42]-minv)<<24 |
			uint64(in[43]-minv)<<52

	out[19] =
		uint64(in[43]-minv)>>12 |
			uint64(in[44]-minv)<<16 |
			uint64(in[45]-minv)<<44

	out[20] =
		uint64(in[45]-minv)>>20 |
			uint64(in[46]-minv)<<8 |
			uint64(in[47]-minv)<<36

	out[21] =
		uint64(in[47]-minv)>>28 |
			uint64(in[48]-minv) |
			uint64(in[49]-minv)<<28 |
			uint64(in[50]-minv)<<56

	out[22] =
		uint64(in[50]-minv)>>8 |
			uint64(in[51]-minv)<<20 |
			uint64(in[52]-minv)<<48

	out[23] =
		uint64(in[52]-minv)>>16 |
			uint64(in[53]-minv)<<12 |
			uint64(in[54]-minv)<<40

	out[24] =
		uint64(in[54]-minv)>>24 |
			uint64(in[55]-minv)<<4 |
			uint64(in[56]-minv)<<32 |
			uint64(in[57]-minv)<<60

	out[25] =
		uint64(in[57]-minv)>>4 |
			uint64(in[58]-minv)<<24 |
			uint64(in[59]-minv)<<52

	out[26] =
		uint64(in[59]-minv)>>12 |
			uint64(in[60]-minv)<<16 |
			uint64(in[61]-minv)<<44

	out[27] =
		uint64(in[61]-minv)>>20 |
			uint64(in[62]-minv)<<8 |
			uint64(in[63]-minv)<<36
}

func bp64_29(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[29]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<29 |
			uint64(in[2]-minv)<<58

	out[1] =
		uint64(in[2]-minv)>>6 |
			uint64(in[3]-minv)<<23 |
			uint64(in[4]-minv)<<52

	out[2] =
		uint64(in[4]-minv)>>12 |
			uint64(in[5]-minv)<<17 |
			uint64(in[6]-minv)<<46

	out[3] =
		uint64(in[6]-minv)>>18 |
			uint64(in[7]-minv)<<11 |
			uint64(in[8]-minv)<<40

	out[4] =
		uint64(in[8]-minv)>>24 |
			uint64(in[9]-minv)<<5 |
			uint64(in[10]-minv)<<34 |
			uint64(in[11]-minv)<<63

	out[5] =
		uint64(in[11]-minv)>>1 |
			uint64(in[12]-minv)<<28 |
			uint64(in[13]-minv)<<57

	out[6] =
		uint64(in[13]-minv)>>7 |
			uint64(in[14]-minv)<<22 |
			uint64(in[15]-minv)<<51

	out[7] =
		uint64(in[15]-minv)>>13 |
			uint64(in[16]-minv)<<16 |
			uint64(in[17]-minv)<<45

	out[8] =
		uint64(in[17]-minv)>>19 |
			uint64(in[18]-minv)<<10 |
			uint64(in[19]-minv)<<39

	out[9] =
		uint64(in[19]-minv)>>25 |
			uint64(in[20]-minv)<<4 |
			uint64(in[21]-minv)<<33 |
			uint64(in[22]-minv)<<62

	out[10] =
		uint64(in[22]-minv)>>2 |
			uint64(in[23]-minv)<<27 |
			uint64(in[24]-minv)<<56

	out[11] =
		uint64(in[24]-minv)>>8 |
			uint64(in[25]-minv)<<21 |
			uint64(in[26]-minv)<<50

	out[12] =
		uint64(in[26]-minv)>>14 |
			uint64(in[27]-minv)<<15 |
			uint64(in[28]-minv)<<44

	out[13] =
		uint64(in[28]-minv)>>20 |
			uint64(in[29]-minv)<<9 |
			uint64(in[30]-minv)<<38

	out[14] =
		uint64(in[30]-minv)>>26 |
			uint64(in[31]-minv)<<3 |
			uint64(in[32]-minv)<<32 |
			uint64(in[33]-minv)<<61

	out[15] =
		uint64(in[33]-minv)>>3 |
			uint64(in[34]-minv)<<26 |
			uint64(in[35]-minv)<<55

	out[16] =
		uint64(in[35]-minv)>>9 |
			uint64(in[36]-minv)<<20 |
			uint64(in[37]-minv)<<49

	out[17] =
		uint64(in[37]-minv)>>15 |
			uint64(in[38]-minv)<<14 |
			uint64(in[39]-minv)<<43

	out[18] =
		uint64(in[39]-minv)>>21 |
			uint64(in[40]-minv)<<8 |
			uint64(in[41]-minv)<<37

	out[19] =
		uint64(in[41]-minv)>>27 |
			uint64(in[42]-minv)<<2 |
			uint64(in[43]-minv)<<31 |
			uint64(in[44]-minv)<<60

	out[20] =
		uint64(in[44]-minv)>>4 |
			uint64(in[45]-minv)<<25 |
			uint64(in[46]-minv)<<54

	out[21] =
		uint64(in[46]-minv)>>10 |
			uint64(in[47]-minv)<<19 |
			uint64(in[48]-minv)<<48

	out[22] =
		uint64(in[48]-minv)>>16 |
			uint64(in[49]-minv)<<13 |
			uint64(in[50]-minv)<<42

	out[23] =
		uint64(in[50]-minv)>>22 |
			uint64(in[51]-minv)<<7 |
			uint64(in[52]-minv)<<36

	out[24] =
		uint64(in[52]-minv)>>28 |
			uint64(in[53]-minv)<<1 |
			uint64(in[54]-minv)<<30 |
			uint64(in[55]-minv)<<59

	out[25] =
		uint64(in[55]-minv)>>5 |
			uint64(in[56]-minv)<<24 |
			uint64(in[57]-minv)<<53

	out[26] =
		uint64(in[57]-minv)>>11 |
			uint64(in[58]-minv)<<18 |
			uint64(in[59]-minv)<<47

	out[27] =
		uint64(in[59]-minv)>>17 |
			uint64(in[60]-minv)<<12 |
			uint64(in[61]-minv)<<41

	out[28] =
		uint64(in[61]-minv)>>23 |
			uint64(in[62]-minv)<<6 |
			uint64(in[63]-minv)<<35
}

func bp64_30(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[30]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<30 |
			uint64(in[2]-minv)<<60

	out[1] =
		uint64(in[2]-minv)>>4 |
			uint64(in[3]-minv)<<26 |
			uint64(in[4]-minv)<<56

	out[2] =
		uint64(in[4]-minv)>>8 |
			uint64(in[5]-minv)<<22 |
			uint64(in[6]-minv)<<52

	out[3] =
		uint64(in[6]-minv)>>12 |
			uint64(in[7]-minv)<<18 |
			uint64(in[8]-minv)<<48

	out[4] =
		uint64(in[8]-minv)>>16 |
			uint64(in[9]-minv)<<14 |
			uint64(in[10]-minv)<<44

	out[5] =
		uint64(in[10]-minv)>>20 |
			uint64(in[11]-minv)<<10 |
			uint64(in[12]-minv)<<40

	out[6] =
		uint64(in[12]-minv)>>24 |
			uint64(in[13]-minv)<<6 |
			uint64(in[14]-minv)<<36

	out[7] =
		uint64(in[14]-minv)>>28 |
			uint64(in[15]-minv)<<2 |
			uint64(in[16]-minv)<<32 |
			uint64(in[17]-minv)<<62

	out[8] =
		uint64(in[17]-minv)>>2 |
			uint64(in[18]-minv)<<28 |
			uint64(in[19]-minv)<<58

	out[9] =
		uint64(in[19]-minv)>>6 |
			uint64(in[20]-minv)<<24 |
			uint64(in[21]-minv)<<54

	out[10] =
		uint64(in[21]-minv)>>10 |
			uint64(in[22]-minv)<<20 |
			uint64(in[23]-minv)<<50

	out[11] =
		uint64(in[23]-minv)>>14 |
			uint64(in[24]-minv)<<16 |
			uint64(in[25]-minv)<<46

	out[12] =
		uint64(in[25]-minv)>>18 |
			uint64(in[26]-minv)<<12 |
			uint64(in[27]-minv)<<42

	out[13] =
		uint64(in[27]-minv)>>22 |
			uint64(in[28]-minv)<<8 |
			uint64(in[29]-minv)<<38

	out[14] =
		uint64(in[29]-minv)>>26 |
			uint64(in[30]-minv)<<4 |
			uint64(in[31]-minv)<<34

	out[15] =
		uint64(in[31]-minv)>>30 |
			uint64(in[32]-minv) |
			uint64(in[33]-minv)<<30 |
			uint64(in[34]-minv)<<60

	out[16] =
		uint64(in[34]-minv)>>4 |
			uint64(in[35]-minv)<<26 |
			uint64(in[36]-minv)<<56

	out[17] =
		uint64(in[36]-minv)>>8 |
			uint64(in[37]-minv)<<22 |
			uint64(in[38]-minv)<<52

	out[18] =
		uint64(in[38]-minv)>>12 |
			uint64(in[39]-minv)<<18 |
			uint64(in[40]-minv)<<48

	out[19] =
		uint64(in[40]-minv)>>16 |
			uint64(in[41]-minv)<<14 |
			uint64(in[42]-minv)<<44

	out[20] =
		uint64(in[42]-minv)>>20 |
			uint64(in[43]-minv)<<10 |
			uint64(in[44]-minv)<<40

	out[21] =
		uint64(in[44]-minv)>>24 |
			uint64(in[45]-minv)<<6 |
			uint64(in[46]-minv)<<36

	out[22] =
		uint64(in[46]-minv)>>28 |
			uint64(in[47]-minv)<<2 |
			uint64(in[48]-minv)<<32 |
			uint64(in[49]-minv)<<62

	out[23] =
		uint64(in[49]-minv)>>2 |
			uint64(in[50]-minv)<<28 |
			uint64(in[51]-minv)<<58

	out[24] =
		uint64(in[51]-minv)>>6 |
			uint64(in[52]-minv)<<24 |
			uint64(in[53]-minv)<<54

	out[25] =
		uint64(in[53]-minv)>>10 |
			uint64(in[54]-minv)<<20 |
			uint64(in[55]-minv)<<50

	out[26] =
		uint64(in[55]-minv)>>14 |
			uint64(in[56]-minv)<<16 |
			uint64(in[57]-minv)<<46

	out[27] =
		uint64(in[57]-minv)>>18 |
			uint64(in[58]-minv)<<12 |
			uint64(in[59]-minv)<<42

	out[28] =
		uint64(in[59]-minv)>>22 |
			uint64(in[60]-minv)<<8 |
			uint64(in[61]-minv)<<38

	out[29] =
		uint64(in[61]-minv)>>26 |
			uint64(in[62]-minv)<<4 |
			uint64(in[63]-minv)<<34
}

func bp64_31(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[31]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<31 |
			uint64(in[2]-minv)<<62

	out[1] =
		uint64(in[2]-minv)>>2 |
			uint64(in[3]-minv)<<29 |
			uint64(in[4]-minv)<<60

	out[2] =
		uint64(in[4]-minv)>>4 |
			uint64(in[5]-minv)<<27 |
			uint64(in[6]-minv)<<58

	out[3] =
		uint64(in[6]-minv)>>6 |
			uint64(in[7]-minv)<<25 |
			uint64(in[8]-minv)<<56

	out[4] =
		uint64(in[8]-minv)>>8 |
			uint64(in[9]-minv)<<23 |
			uint64(in[10]-minv)<<54

	out[5] =
		uint64(in[10]-minv)>>10 |
			uint64(in[11]-minv)<<21 |
			uint64(in[12]-minv)<<52

	out[6] =
		uint64(in[12]-minv)>>12 |
			uint64(in[13]-minv)<<19 |
			uint64(in[14]-minv)<<50

	out[7] =
		uint64(in[14]-minv)>>14 |
			uint64(in[15]-minv)<<17 |
			uint64(in[16]-minv)<<48

	out[8] =
		uint64(in[16]-minv)>>16 |
			uint64(in[17]-minv)<<15 |
			uint64(in[18]-minv)<<46

	out[9] =
		uint64(in[18]-minv)>>18 |
			uint64(in[19]-minv)<<13 |
			uint64(in[20]-minv)<<44

	out[10] =
		uint64(in[20]-minv)>>20 |
			uint64(in[21]-minv)<<11 |
			uint64(in[22]-minv)<<42

	out[11] =
		uint64(in[22]-minv)>>22 |
			uint64(in[23]-minv)<<9 |
			uint64(in[24]-minv)<<40

	out[12] =
		uint64(in[24]-minv)>>24 |
			uint64(in[25]-minv)<<7 |
			uint64(in[26]-minv)<<38

	out[13] =
		uint64(in[26]-minv)>>26 |
			uint64(in[27]-minv)<<5 |
			uint64(in[28]-minv)<<36

	out[14] =
		uint64(in[28]-minv)>>28 |
			uint64(in[29]-minv)<<3 |
			uint64(in[30]-minv)<<34

	out[15] =
		uint64(in[30]-minv)>>30 |
			uint64(in[31]-minv)<<1 |
			uint64(in[32]-minv)<<32 |
			uint64(in[33]-minv)<<63

	out[16] =
		uint64(in[33]-minv)>>1 |
			uint64(in[34]-minv)<<30 |
			uint64(in[35]-minv)<<61

	out[17] =
		uint64(in[35]-minv)>>3 |
			uint64(in[36]-minv)<<28 |
			uint64(in[37]-minv)<<59

	out[18] =
		uint64(in[37]-minv)>>5 |
			uint64(in[38]-minv)<<26 |
			uint64(in[39]-minv)<<57

	out[19] =
		uint64(in[39]-minv)>>7 |
			uint64(in[40]-minv)<<24 |
			uint64(in[41]-minv)<<55

	out[20] =
		uint64(in[41]-minv)>>9 |
			uint64(in[42]-minv)<<22 |
			uint64(in[43]-minv)<<53

	out[21] =
		uint64(in[43]-minv)>>11 |
			uint64(in[44]-minv)<<20 |
			uint64(in[45]-minv)<<51

	out[22] =
		uint64(in[45]-minv)>>13 |
			uint64(in[46]-minv)<<18 |
			uint64(in[47]-minv)<<49

	out[23] =
		uint64(in[47]-minv)>>15 |
			uint64(in[48]-minv)<<16 |
			uint64(in[49]-minv)<<47

	out[24] =
		uint64(in[49]-minv)>>17 |
			uint64(in[50]-minv)<<14 |
			uint64(in[51]-minv)<<45

	out[25] =
		uint64(in[51]-minv)>>19 |
			uint64(in[52]-minv)<<12 |
			uint64(in[53]-minv)<<43

	out[26] =
		uint64(in[53]-minv)>>21 |
			uint64(in[54]-minv)<<10 |
			uint64(in[55]-minv)<<41

	out[27] =
		uint64(in[55]-minv)>>23 |
			uint64(in[56]-minv)<<8 |
			uint64(in[57]-minv)<<39

	out[28] =
		uint64(in[57]-minv)>>25 |
			uint64(in[58]-minv)<<6 |
			uint64(in[59]-minv)<<37

	out[29] =
		uint64(in[59]-minv)>>27 |
			uint64(in[60]-minv)<<4 |
			uint64(in[61]-minv)<<35

	out[30] =
		uint64(in[61]-minv)>>29 |
			uint64(in[62]-minv)<<2 |
			uint64(in[63]-minv)<<33
}

func bp64_32(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[32]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<32

	out[1] =
		uint64(in[2]-minv) |
			uint64(in[3]-minv)<<32

	out[2] =
		uint64(in[4]-minv) |
			uint64(in[5]-minv)<<32

	out[3] =
		uint64(in[6]-minv) |
			uint64(in[7]-minv)<<32

	out[4] =
		uint64(in[8]-minv) |
			uint64(in[9]-minv)<<32

	out[5] =
		uint64(in[10]-minv) |
			uint64(in[11]-minv)<<32

	out[6] =
		uint64(in[12]-minv) |
			uint64(in[13]-minv)<<32

	out[7] =
		uint64(in[14]-minv) |
			uint64(in[15]-minv)<<32

	out[8] =
		uint64(in[16]-minv) |
			uint64(in[17]-minv)<<32

	out[9] =
		uint64(in[18]-minv) |
			uint64(in[19]-minv)<<32

	out[10] =
		uint64(in[20]-minv) |
			uint64(in[21]-minv)<<32

	out[11] =
		uint64(in[22]-minv) |
			uint64(in[23]-minv)<<32

	out[12] =
		uint64(in[24]-minv) |
			uint64(in[25]-minv)<<32

	out[13] =
		uint64(in[26]-minv) |
			uint64(in[27]-minv)<<32

	out[14] =
		uint64(in[28]-minv) |
			uint64(in[29]-minv)<<32

	out[15] =
		uint64(in[30]-minv) |
			uint64(in[31]-minv)<<32

	out[16] =
		uint64(in[32]-minv) |
			uint64(in[33]-minv)<<32

	out[17] =
		uint64(in[34]-minv) |
			uint64(in[35]-minv)<<32

	out[18] =
		uint64(in[36]-minv) |
			uint64(in[37]-minv)<<32

	out[19] =
		uint64(in[38]-minv) |
			uint64(in[39]-minv)<<32

	out[20] =
		uint64(in[40]-minv) |
			uint64(in[41]-minv)<<32

	out[21] =
		uint64(in[42]-minv) |
			uint64(in[43]-minv)<<32

	out[22] =
		uint64(in[44]-minv) |
			uint64(in[45]-minv)<<32

	out[23] =
		uint64(in[46]-minv) |
			uint64(in[47]-minv)<<32

	out[24] =
		uint64(in[48]-minv) |
			uint64(in[49]-minv)<<32

	out[25] =
		uint64(in[50]-minv) |
			uint64(in[51]-minv)<<32

	out[26] =
		uint64(in[52]-minv) |
			uint64(in[53]-minv)<<32

	out[27] =
		uint64(in[54]-minv) |
			uint64(in[55]-minv)<<32

	out[28] =
		uint64(in[56]-minv) |
			uint64(in[57]-minv)<<32

	out[29] =
		uint64(in[58]-minv) |
			uint64(in[59]-minv)<<32

	out[30] =
		uint64(in[60]-minv) |
			uint64(in[61]-minv)<<32

	out[31] =
		uint64(in[62]-minv) |
			uint64(in[63]-minv)<<32
}

func bp64_33(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[33]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<33

	out[1] =
		uint64(in[1]-minv)>>31 |
			uint64(in[2]-minv)<<2 |
			uint64(in[3]-minv)<<35

	out[2] =
		uint64(in[3]-minv)>>29 |
			uint64(in[4]-minv)<<4 |
			uint64(in[5]-minv)<<37

	out[3] =
		uint64(in[5]-minv)>>27 |
			uint64(in[6]-minv)<<6 |
			uint64(in[7]-minv)<<39

	out[4] =
		uint64(in[7]-minv)>>25 |
			uint64(in[8]-minv)<<8 |
			uint64(in[9]-minv)<<41

	out[5] =
		uint64(in[9]-minv)>>23 |
			uint64(in[10]-minv)<<10 |
			uint64(in[11]-minv)<<43

	out[6] =
		uint64(in[11]-minv)>>21 |
			uint64(in[12]-minv)<<12 |
			uint64(in[13]-minv)<<45

	out[7] =
		uint64(in[13]-minv)>>19 |
			uint64(in[14]-minv)<<14 |
			uint64(in[15]-minv)<<47

	out[8] =
		uint64(in[15]-minv)>>17 |
			uint64(in[16]-minv)<<16 |
			uint64(in[17]-minv)<<49

	out[9] =
		uint64(in[17]-minv)>>15 |
			uint64(in[18]-minv)<<18 |
			uint64(in[19]-minv)<<51

	out[10] =
		uint64(in[19]-minv)>>13 |
			uint64(in[20]-minv)<<20 |
			uint64(in[21]-minv)<<53

	out[11] =
		uint64(in[21]-minv)>>11 |
			uint64(in[22]-minv)<<22 |
			uint64(in[23]-minv)<<55

	out[12] =
		uint64(in[23]-minv)>>9 |
			uint64(in[24]-minv)<<24 |
			uint64(in[25]-minv)<<57

	out[13] =
		uint64(in[25]-minv)>>7 |
			uint64(in[26]-minv)<<26 |
			uint64(in[27]-minv)<<59

	out[14] =
		uint64(in[27]-minv)>>5 |
			uint64(in[28]-minv)<<28 |
			uint64(in[29]-minv)<<61

	out[15] =
		uint64(in[29]-minv)>>3 |
			uint64(in[30]-minv)<<30 |
			uint64(in[31]-minv)<<63

	out[16] =
		uint64(in[31]-minv)>>1 |
			uint64(in[32]-minv)<<32

	out[17] =
		uint64(in[32]-minv)>>32 |
			uint64(in[33]-minv)<<1 |
			uint64(in[34]-minv)<<34

	out[18] =
		uint64(in[34]-minv)>>30 |
			uint64(in[35]-minv)<<3 |
			uint64(in[36]-minv)<<36

	out[19] =
		uint64(in[36]-minv)>>28 |
			uint64(in[37]-minv)<<5 |
			uint64(in[38]-minv)<<38

	out[20] =
		uint64(in[38]-minv)>>26 |
			uint64(in[39]-minv)<<7 |
			uint64(in[40]-minv)<<40

	out[21] =
		uint64(in[40]-minv)>>24 |
			uint64(in[41]-minv)<<9 |
			uint64(in[42]-minv)<<42

	out[22] =
		uint64(in[42]-minv)>>22 |
			uint64(in[43]-minv)<<11 |
			uint64(in[44]-minv)<<44

	out[23] =
		uint64(in[44]-minv)>>20 |
			uint64(in[45]-minv)<<13 |
			uint64(in[46]-minv)<<46

	out[24] =
		uint64(in[46]-minv)>>18 |
			uint64(in[47]-minv)<<15 |
			uint64(in[48]-minv)<<48

	out[25] =
		uint64(in[48]-minv)>>16 |
			uint64(in[49]-minv)<<17 |
			uint64(in[50]-minv)<<50

	out[26] =
		uint64(in[50]-minv)>>14 |
			uint64(in[51]-minv)<<19 |
			uint64(in[52]-minv)<<52

	out[27] =
		uint64(in[52]-minv)>>12 |
			uint64(in[53]-minv)<<21 |
			uint64(in[54]-minv)<<54

	out[28] =
		uint64(in[54]-minv)>>10 |
			uint64(in[55]-minv)<<23 |
			uint64(in[56]-minv)<<56

	out[29] =
		uint64(in[56]-minv)>>8 |
			uint64(in[57]-minv)<<25 |
			uint64(in[58]-minv)<<58

	out[30] =
		uint64(in[58]-minv)>>6 |
			uint64(in[59]-minv)<<27 |
			uint64(in[60]-minv)<<60

	out[31] =
		uint64(in[60]-minv)>>4 |
			uint64(in[61]-minv)<<29 |
			uint64(in[62]-minv)<<62

	out[32] =
		uint64(in[62]-minv)>>2 |
			uint64(in[63]-minv)<<31
}

func bp64_34(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[34]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<34

	out[1] =
		uint64(in[1]-minv)>>30 |
			uint64(in[2]-minv)<<4 |
			uint64(in[3]-minv)<<38

	out[2] =
		uint64(in[3]-minv)>>26 |
			uint64(in[4]-minv)<<8 |
			uint64(in[5]-minv)<<42

	out[3] =
		uint64(in[5]-minv)>>22 |
			uint64(in[6]-minv)<<12 |
			uint64(in[7]-minv)<<46

	out[4] =
		uint64(in[7]-minv)>>18 |
			uint64(in[8]-minv)<<16 |
			uint64(in[9]-minv)<<50

	out[5] =
		uint64(in[9]-minv)>>14 |
			uint64(in[10]-minv)<<20 |
			uint64(in[11]-minv)<<54

	out[6] =
		uint64(in[11]-minv)>>10 |
			uint64(in[12]-minv)<<24 |
			uint64(in[13]-minv)<<58

	out[7] =
		uint64(in[13]-minv)>>6 |
			uint64(in[14]-minv)<<28 |
			uint64(in[15]-minv)<<62

	out[8] =
		uint64(in[15]-minv)>>2 |
			uint64(in[16]-minv)<<32

	out[9] =
		uint64(in[16]-minv)>>32 |
			uint64(in[17]-minv)<<2 |
			uint64(in[18]-minv)<<36

	out[10] =
		uint64(in[18]-minv)>>28 |
			uint64(in[19]-minv)<<6 |
			uint64(in[20]-minv)<<40

	out[11] =
		uint64(in[20]-minv)>>24 |
			uint64(in[21]-minv)<<10 |
			uint64(in[22]-minv)<<44

	out[12] =
		uint64(in[22]-minv)>>20 |
			uint64(in[23]-minv)<<14 |
			uint64(in[24]-minv)<<48

	out[13] =
		uint64(in[24]-minv)>>16 |
			uint64(in[25]-minv)<<18 |
			uint64(in[26]-minv)<<52

	out[14] =
		uint64(in[26]-minv)>>12 |
			uint64(in[27]-minv)<<22 |
			uint64(in[28]-minv)<<56

	out[15] =
		uint64(in[28]-minv)>>8 |
			uint64(in[29]-minv)<<26 |
			uint64(in[30]-minv)<<60

	out[16] =
		uint64(in[30]-minv)>>4 |
			uint64(in[31]-minv)<<30

	out[17] =
		uint64(in[31]-minv)>>34 |
			uint64(in[32]-minv) |
			uint64(in[33]-minv)<<34

	out[18] =
		uint64(in[33]-minv)>>30 |
			uint64(in[34]-minv)<<4 |
			uint64(in[35]-minv)<<38

	out[19] =
		uint64(in[35]-minv)>>26 |
			uint64(in[36]-minv)<<8 |
			uint64(in[37]-minv)<<42

	out[20] =
		uint64(in[37]-minv)>>22 |
			uint64(in[38]-minv)<<12 |
			uint64(in[39]-minv)<<46

	out[21] =
		uint64(in[39]-minv)>>18 |
			uint64(in[40]-minv)<<16 |
			uint64(in[41]-minv)<<50

	out[22] =
		uint64(in[41]-minv)>>14 |
			uint64(in[42]-minv)<<20 |
			uint64(in[43]-minv)<<54

	out[23] =
		uint64(in[43]-minv)>>10 |
			uint64(in[44]-minv)<<24 |
			uint64(in[45]-minv)<<58

	out[24] =
		uint64(in[45]-minv)>>6 |
			uint64(in[46]-minv)<<28 |
			uint64(in[47]-minv)<<62

	out[25] =
		uint64(in[47]-minv)>>2 |
			uint64(in[48]-minv)<<32

	out[26] =
		uint64(in[48]-minv)>>32 |
			uint64(in[49]-minv)<<2 |
			uint64(in[50]-minv)<<36

	out[27] =
		uint64(in[50]-minv)>>28 |
			uint64(in[51]-minv)<<6 |
			uint64(in[52]-minv)<<40

	out[28] =
		uint64(in[52]-minv)>>24 |
			uint64(in[53]-minv)<<10 |
			uint64(in[54]-minv)<<44

	out[29] =
		uint64(in[54]-minv)>>20 |
			uint64(in[55]-minv)<<14 |
			uint64(in[56]-minv)<<48

	out[30] =
		uint64(in[56]-minv)>>16 |
			uint64(in[57]-minv)<<18 |
			uint64(in[58]-minv)<<52

	out[31] =
		uint64(in[58]-minv)>>12 |
			uint64(in[59]-minv)<<22 |
			uint64(in[60]-minv)<<56

	out[32] =
		uint64(in[60]-minv)>>8 |
			uint64(in[61]-minv)<<26 |
			uint64(in[62]-minv)<<60

	out[33] =
		uint64(in[62]-minv)>>4 |
			uint64(in[63]-minv)<<30
}

func bp64_35(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[35]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<35

	out[1] =
		uint64(in[1]-minv)>>29 |
			uint64(in[2]-minv)<<6 |
			uint64(in[3]-minv)<<41

	out[2] =
		uint64(in[3]-minv)>>23 |
			uint64(in[4]-minv)<<12 |
			uint64(in[5]-minv)<<47

	out[3] =
		uint64(in[5]-minv)>>17 |
			uint64(in[6]-minv)<<18 |
			uint64(in[7]-minv)<<53

	out[4] =
		uint64(in[7]-minv)>>11 |
			uint64(in[8]-minv)<<24 |
			uint64(in[9]-minv)<<59

	out[5] =
		uint64(in[9]-minv)>>5 |
			uint64(in[10]-minv)<<30

	out[6] =
		uint64(in[10]-minv)>>34 |
			uint64(in[11]-minv)<<1 |
			uint64(in[12]-minv)<<36

	out[7] =
		uint64(in[12]-minv)>>28 |
			uint64(in[13]-minv)<<7 |
			uint64(in[14]-minv)<<42

	out[8] =
		uint64(in[14]-minv)>>22 |
			uint64(in[15]-minv)<<13 |
			uint64(in[16]-minv)<<48

	out[9] =
		uint64(in[16]-minv)>>16 |
			uint64(in[17]-minv)<<19 |
			uint64(in[18]-minv)<<54

	out[10] =
		uint64(in[18]-minv)>>10 |
			uint64(in[19]-minv)<<25 |
			uint64(in[20]-minv)<<60

	out[11] =
		uint64(in[20]-minv)>>4 |
			uint64(in[21]-minv)<<31

	out[12] =
		uint64(in[21]-minv)>>33 |
			uint64(in[22]-minv)<<2 |
			uint64(in[23]-minv)<<37

	out[13] =
		uint64(in[23]-minv)>>27 |
			uint64(in[24]-minv)<<8 |
			uint64(in[25]-minv)<<43

	out[14] =
		uint64(in[25]-minv)>>21 |
			uint64(in[26]-minv)<<14 |
			uint64(in[27]-minv)<<49

	out[15] =
		uint64(in[27]-minv)>>15 |
			uint64(in[28]-minv)<<20 |
			uint64(in[29]-minv)<<55

	out[16] =
		uint64(in[29]-minv)>>9 |
			uint64(in[30]-minv)<<26 |
			uint64(in[31]-minv)<<61

	out[17] =
		uint64(in[31]-minv)>>3 |
			uint64(in[32]-minv)<<32

	out[18] =
		uint64(in[32]-minv)>>32 |
			uint64(in[33]-minv)<<3 |
			uint64(in[34]-minv)<<38

	out[19] =
		uint64(in[34]-minv)>>26 |
			uint64(in[35]-minv)<<9 |
			uint64(in[36]-minv)<<44

	out[20] =
		uint64(in[36]-minv)>>20 |
			uint64(in[37]-minv)<<15 |
			uint64(in[38]-minv)<<50

	out[21] =
		uint64(in[38]-minv)>>14 |
			uint64(in[39]-minv)<<21 |
			uint64(in[40]-minv)<<56

	out[22] =
		uint64(in[40]-minv)>>8 |
			uint64(in[41]-minv)<<27 |
			uint64(in[42]-minv)<<62

	out[23] =
		uint64(in[42]-minv)>>2 |
			uint64(in[43]-minv)<<33

	out[24] =
		uint64(in[43]-minv)>>31 |
			uint64(in[44]-minv)<<4 |
			uint64(in[45]-minv)<<39

	out[25] =
		uint64(in[45]-minv)>>25 |
			uint64(in[46]-minv)<<10 |
			uint64(in[47]-minv)<<45

	out[26] =
		uint64(in[47]-minv)>>19 |
			uint64(in[48]-minv)<<16 |
			uint64(in[49]-minv)<<51

	out[27] =
		uint64(in[49]-minv)>>13 |
			uint64(in[50]-minv)<<22 |
			uint64(in[51]-minv)<<57

	out[28] =
		uint64(in[51]-minv)>>7 |
			uint64(in[52]-minv)<<28 |
			uint64(in[53]-minv)<<63

	out[29] =
		uint64(in[53]-minv)>>1 |
			uint64(in[54]-minv)<<34

	out[30] =
		uint64(in[54]-minv)>>30 |
			uint64(in[55]-minv)<<5 |
			uint64(in[56]-minv)<<40

	out[31] =
		uint64(in[56]-minv)>>24 |
			uint64(in[57]-minv)<<11 |
			uint64(in[58]-minv)<<46

	out[32] =
		uint64(in[58]-minv)>>18 |
			uint64(in[59]-minv)<<17 |
			uint64(in[60]-minv)<<52

	out[33] =
		uint64(in[60]-minv)>>12 |
			uint64(in[61]-minv)<<23 |
			uint64(in[62]-minv)<<58

	out[34] =
		uint64(in[62]-minv)>>6 |
			uint64(in[63]-minv)<<29
}

func bp64_36(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[36]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<36

	out[1] =
		uint64(in[1]-minv)>>28 |
			uint64(in[2]-minv)<<8 |
			uint64(in[3]-minv)<<44

	out[2] =
		uint64(in[3]-minv)>>20 |
			uint64(in[4]-minv)<<16 |
			uint64(in[5]-minv)<<52

	out[3] =
		uint64(in[5]-minv)>>12 |
			uint64(in[6]-minv)<<24 |
			uint64(in[7]-minv)<<60

	out[4] =
		uint64(in[7]-minv)>>4 |
			uint64(in[8]-minv)<<32

	out[5] =
		uint64(in[8]-minv)>>32 |
			uint64(in[9]-minv)<<4 |
			uint64(in[10]-minv)<<40

	out[6] =
		uint64(in[10]-minv)>>24 |
			uint64(in[11]-minv)<<12 |
			uint64(in[12]-minv)<<48

	out[7] =
		uint64(in[12]-minv)>>16 |
			uint64(in[13]-minv)<<20 |
			uint64(in[14]-minv)<<56

	out[8] =
		uint64(in[14]-minv)>>8 |
			uint64(in[15]-minv)<<28

	out[9] =
		uint64(in[15]-minv)>>36 |
			uint64(in[16]-minv) |
			uint64(in[17]-minv)<<36

	out[10] =
		uint64(in[17]-minv)>>28 |
			uint64(in[18]-minv)<<8 |
			uint64(in[19]-minv)<<44

	out[11] =
		uint64(in[19]-minv)>>20 |
			uint64(in[20]-minv)<<16 |
			uint64(in[21]-minv)<<52

	out[12] =
		uint64(in[21]-minv)>>12 |
			uint64(in[22]-minv)<<24 |
			uint64(in[23]-minv)<<60

	out[13] =
		uint64(in[23]-minv)>>4 |
			uint64(in[24]-minv)<<32

	out[14] =
		uint64(in[24]-minv)>>32 |
			uint64(in[25]-minv)<<4 |
			uint64(in[26]-minv)<<40

	out[15] =
		uint64(in[26]-minv)>>24 |
			uint64(in[27]-minv)<<12 |
			uint64(in[28]-minv)<<48

	out[16] =
		uint64(in[28]-minv)>>16 |
			uint64(in[29]-minv)<<20 |
			uint64(in[30]-minv)<<56

	out[17] =
		uint64(in[30]-minv)>>8 |
			uint64(in[31]-minv)<<28

	out[18] =
		uint64(in[31]-minv)>>36 |
			uint64(in[32]-minv) |
			uint64(in[33]-minv)<<36

	out[19] =
		uint64(in[33]-minv)>>28 |
			uint64(in[34]-minv)<<8 |
			uint64(in[35]-minv)<<44

	out[20] =
		uint64(in[35]-minv)>>20 |
			uint64(in[36]-minv)<<16 |
			uint64(in[37]-minv)<<52

	out[21] =
		uint64(in[37]-minv)>>12 |
			uint64(in[38]-minv)<<24 |
			uint64(in[39]-minv)<<60

	out[22] =
		uint64(in[39]-minv)>>4 |
			uint64(in[40]-minv)<<32

	out[23] =
		uint64(in[40]-minv)>>32 |
			uint64(in[41]-minv)<<4 |
			uint64(in[42]-minv)<<40

	out[24] =
		uint64(in[42]-minv)>>24 |
			uint64(in[43]-minv)<<12 |
			uint64(in[44]-minv)<<48

	out[25] =
		uint64(in[44]-minv)>>16 |
			uint64(in[45]-minv)<<20 |
			uint64(in[46]-minv)<<56

	out[26] =
		uint64(in[46]-minv)>>8 |
			uint64(in[47]-minv)<<28

	out[27] =
		uint64(in[47]-minv)>>36 |
			uint64(in[48]-minv) |
			uint64(in[49]-minv)<<36

	out[28] =
		uint64(in[49]-minv)>>28 |
			uint64(in[50]-minv)<<8 |
			uint64(in[51]-minv)<<44

	out[29] =
		uint64(in[51]-minv)>>20 |
			uint64(in[52]-minv)<<16 |
			uint64(in[53]-minv)<<52

	out[30] =
		uint64(in[53]-minv)>>12 |
			uint64(in[54]-minv)<<24 |
			uint64(in[55]-minv)<<60

	out[31] =
		uint64(in[55]-minv)>>4 |
			uint64(in[56]-minv)<<32

	out[32] =
		uint64(in[56]-minv)>>32 |
			uint64(in[57]-minv)<<4 |
			uint64(in[58]-minv)<<40

	out[33] =
		uint64(in[58]-minv)>>24 |
			uint64(in[59]-minv)<<12 |
			uint64(in[60]-minv)<<48

	out[34] =
		uint64(in[60]-minv)>>16 |
			uint64(in[61]-minv)<<20 |
			uint64(in[62]-minv)<<56

	out[35] =
		uint64(in[62]-minv)>>8 |
			uint64(in[63]-minv)<<28
}

func bp64_37(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[37]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<37

	out[1] =
		uint64(in[1]-minv)>>27 |
			uint64(in[2]-minv)<<10 |
			uint64(in[3]-minv)<<47

	out[2] =
		uint64(in[3]-minv)>>17 |
			uint64(in[4]-minv)<<20 |
			uint64(in[5]-minv)<<57

	out[3] =
		uint64(in[5]-minv)>>7 |
			uint64(in[6]-minv)<<30

	out[4] =
		uint64(in[6]-minv)>>34 |
			uint64(in[7]-minv)<<3 |
			uint64(in[8]-minv)<<40

	out[5] =
		uint64(in[8]-minv)>>24 |
			uint64(in[9]-minv)<<13 |
			uint64(in[10]-minv)<<50

	out[6] =
		uint64(in[10]-minv)>>14 |
			uint64(in[11]-minv)<<23 |
			uint64(in[12]-minv)<<60

	out[7] =
		uint64(in[12]-minv)>>4 |
			uint64(in[13]-minv)<<33

	out[8] =
		uint64(in[13]-minv)>>31 |
			uint64(in[14]-minv)<<6 |
			uint64(in[15]-minv)<<43

	out[9] =
		uint64(in[15]-minv)>>21 |
			uint64(in[16]-minv)<<16 |
			uint64(in[17]-minv)<<53

	out[10] =
		uint64(in[17]-minv)>>11 |
			uint64(in[18]-minv)<<26 |
			uint64(in[19]-minv)<<63

	out[11] =
		uint64(in[19]-minv)>>1 |
			uint64(in[20]-minv)<<36

	out[12] =
		uint64(in[20]-minv)>>28 |
			uint64(in[21]-minv)<<9 |
			uint64(in[22]-minv)<<46

	out[13] =
		uint64(in[22]-minv)>>18 |
			uint64(in[23]-minv)<<19 |
			uint64(in[24]-minv)<<56

	out[14] =
		uint64(in[24]-minv)>>8 |
			uint64(in[25]-minv)<<29

	out[15] =
		uint64(in[25]-minv)>>35 |
			uint64(in[26]-minv)<<2 |
			uint64(in[27]-minv)<<39

	out[16] =
		uint64(in[27]-minv)>>25 |
			uint64(in[28]-minv)<<12 |
			uint64(in[29]-minv)<<49

	out[17] =
		uint64(in[29]-minv)>>15 |
			uint64(in[30]-minv)<<22 |
			uint64(in[31]-minv)<<59

	out[18] =
		uint64(in[31]-minv)>>5 |
			uint64(in[32]-minv)<<32

	out[19] =
		uint64(in[32]-minv)>>32 |
			uint64(in[33]-minv)<<5 |
			uint64(in[34]-minv)<<42

	out[20] =
		uint64(in[34]-minv)>>22 |
			uint64(in[35]-minv)<<15 |
			uint64(in[36]-minv)<<52

	out[21] =
		uint64(in[36]-minv)>>12 |
			uint64(in[37]-minv)<<25 |
			uint64(in[38]-minv)<<62

	out[22] =
		uint64(in[38]-minv)>>2 |
			uint64(in[39]-minv)<<35

	out[23] =
		uint64(in[39]-minv)>>29 |
			uint64(in[40]-minv)<<8 |
			uint64(in[41]-minv)<<45

	out[24] =
		uint64(in[41]-minv)>>19 |
			uint64(in[42]-minv)<<18 |
			uint64(in[43]-minv)<<55

	out[25] =
		uint64(in[43]-minv)>>9 |
			uint64(in[44]-minv)<<28

	out[26] =
		uint64(in[44]-minv)>>36 |
			uint64(in[45]-minv)<<1 |
			uint64(in[46]-minv)<<38

	out[27] =
		uint64(in[46]-minv)>>26 |
			uint64(in[47]-minv)<<11 |
			uint64(in[48]-minv)<<48

	out[28] =
		uint64(in[48]-minv)>>16 |
			uint64(in[49]-minv)<<21 |
			uint64(in[50]-minv)<<58

	out[29] =
		uint64(in[50]-minv)>>6 |
			uint64(in[51]-minv)<<31

	out[30] =
		uint64(in[51]-minv)>>33 |
			uint64(in[52]-minv)<<4 |
			uint64(in[53]-minv)<<41

	out[31] =
		uint64(in[53]-minv)>>23 |
			uint64(in[54]-minv)<<14 |
			uint64(in[55]-minv)<<51

	out[32] =
		uint64(in[55]-minv)>>13 |
			uint64(in[56]-minv)<<24 |
			uint64(in[57]-minv)<<61

	out[33] =
		uint64(in[57]-minv)>>3 |
			uint64(in[58]-minv)<<34

	out[34] =
		uint64(in[58]-minv)>>30 |
			uint64(in[59]-minv)<<7 |
			uint64(in[60]-minv)<<44

	out[35] =
		uint64(in[60]-minv)>>20 |
			uint64(in[61]-minv)<<17 |
			uint64(in[62]-minv)<<54

	out[36] =
		uint64(in[62]-minv)>>10 |
			uint64(in[63]-minv)<<27
}

func bp64_38(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[38]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<38

	out[1] =
		uint64(in[1]-minv)>>26 |
			uint64(in[2]-minv)<<12 |
			uint64(in[3]-minv)<<50

	out[2] =
		uint64(in[3]-minv)>>14 |
			uint64(in[4]-minv)<<24 |
			uint64(in[5]-minv)<<62

	out[3] =
		uint64(in[5]-minv)>>2 |
			uint64(in[6]-minv)<<36

	out[4] =
		uint64(in[6]-minv)>>28 |
			uint64(in[7]-minv)<<10 |
			uint64(in[8]-minv)<<48

	out[5] =
		uint64(in[8]-minv)>>16 |
			uint64(in[9]-minv)<<22 |
			uint64(in[10]-minv)<<60

	out[6] =
		uint64(in[10]-minv)>>4 |
			uint64(in[11]-minv)<<34

	out[7] =
		uint64(in[11]-minv)>>30 |
			uint64(in[12]-minv)<<8 |
			uint64(in[13]-minv)<<46

	out[8] =
		uint64(in[13]-minv)>>18 |
			uint64(in[14]-minv)<<20 |
			uint64(in[15]-minv)<<58

	out[9] =
		uint64(in[15]-minv)>>6 |
			uint64(in[16]-minv)<<32

	out[10] =
		uint64(in[16]-minv)>>32 |
			uint64(in[17]-minv)<<6 |
			uint64(in[18]-minv)<<44

	out[11] =
		uint64(in[18]-minv)>>20 |
			uint64(in[19]-minv)<<18 |
			uint64(in[20]-minv)<<56

	out[12] =
		uint64(in[20]-minv)>>8 |
			uint64(in[21]-minv)<<30

	out[13] =
		uint64(in[21]-minv)>>34 |
			uint64(in[22]-minv)<<4 |
			uint64(in[23]-minv)<<42

	out[14] =
		uint64(in[23]-minv)>>22 |
			uint64(in[24]-minv)<<16 |
			uint64(in[25]-minv)<<54

	out[15] =
		uint64(in[25]-minv)>>10 |
			uint64(in[26]-minv)<<28

	out[16] =
		uint64(in[26]-minv)>>36 |
			uint64(in[27]-minv)<<2 |
			uint64(in[28]-minv)<<40

	out[17] =
		uint64(in[28]-minv)>>24 |
			uint64(in[29]-minv)<<14 |
			uint64(in[30]-minv)<<52

	out[18] =
		uint64(in[30]-minv)>>12 |
			uint64(in[31]-minv)<<26

	out[19] =
		uint64(in[31]-minv)>>38 |
			uint64(in[32]-minv) |
			uint64(in[33]-minv)<<38

	out[20] =
		uint64(in[33]-minv)>>26 |
			uint64(in[34]-minv)<<12 |
			uint64(in[35]-minv)<<50

	out[21] =
		uint64(in[35]-minv)>>14 |
			uint64(in[36]-minv)<<24 |
			uint64(in[37]-minv)<<62

	out[22] =
		uint64(in[37]-minv)>>2 |
			uint64(in[38]-minv)<<36

	out[23] =
		uint64(in[38]-minv)>>28 |
			uint64(in[39]-minv)<<10 |
			uint64(in[40]-minv)<<48

	out[24] =
		uint64(in[40]-minv)>>16 |
			uint64(in[41]-minv)<<22 |
			uint64(in[42]-minv)<<60

	out[25] =
		uint64(in[42]-minv)>>4 |
			uint64(in[43]-minv)<<34

	out[26] =
		uint64(in[43]-minv)>>30 |
			uint64(in[44]-minv)<<8 |
			uint64(in[45]-minv)<<46

	out[27] =
		uint64(in[45]-minv)>>18 |
			uint64(in[46]-minv)<<20 |
			uint64(in[47]-minv)<<58

	out[28] =
		uint64(in[47]-minv)>>6 |
			uint64(in[48]-minv)<<32

	out[29] =
		uint64(in[48]-minv)>>32 |
			uint64(in[49]-minv)<<6 |
			uint64(in[50]-minv)<<44

	out[30] =
		uint64(in[50]-minv)>>20 |
			uint64(in[51]-minv)<<18 |
			uint64(in[52]-minv)<<56

	out[31] =
		uint64(in[52]-minv)>>8 |
			uint64(in[53]-minv)<<30

	out[32] =
		uint64(in[53]-minv)>>34 |
			uint64(in[54]-minv)<<4 |
			uint64(in[55]-minv)<<42

	out[33] =
		uint64(in[55]-minv)>>22 |
			uint64(in[56]-minv)<<16 |
			uint64(in[57]-minv)<<54

	out[34] =
		uint64(in[57]-minv)>>10 |
			uint64(in[58]-minv)<<28

	out[35] =
		uint64(in[58]-minv)>>36 |
			uint64(in[59]-minv)<<2 |
			uint64(in[60]-minv)<<40

	out[36] =
		uint64(in[60]-minv)>>24 |
			uint64(in[61]-minv)<<14 |
			uint64(in[62]-minv)<<52

	out[37] =
		uint64(in[62]-minv)>>12 |
			uint64(in[63]-minv)<<26
}

func bp64_39(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[39]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<39

	out[1] =
		uint64(in[1]-minv)>>25 |
			uint64(in[2]-minv)<<14 |
			uint64(in[3]-minv)<<53

	out[2] =
		uint64(in[3]-minv)>>11 |
			uint64(in[4]-minv)<<28

	out[3] =
		uint64(in[4]-minv)>>36 |
			uint64(in[5]-minv)<<3 |
			uint64(in[6]-minv)<<42

	out[4] =
		uint64(in[6]-minv)>>22 |
			uint64(in[7]-minv)<<17 |
			uint64(in[8]-minv)<<56

	out[5] =
		uint64(in[8]-minv)>>8 |
			uint64(in[9]-minv)<<31

	out[6] =
		uint64(in[9]-minv)>>33 |
			uint64(in[10]-minv)<<6 |
			uint64(in[11]-minv)<<45

	out[7] =
		uint64(in[11]-minv)>>19 |
			uint64(in[12]-minv)<<20 |
			uint64(in[13]-minv)<<59

	out[8] =
		uint64(in[13]-minv)>>5 |
			uint64(in[14]-minv)<<34

	out[9] =
		uint64(in[14]-minv)>>30 |
			uint64(in[15]-minv)<<9 |
			uint64(in[16]-minv)<<48

	out[10] =
		uint64(in[16]-minv)>>16 |
			uint64(in[17]-minv)<<23 |
			uint64(in[18]-minv)<<62

	out[11] =
		uint64(in[18]-minv)>>2 |
			uint64(in[19]-minv)<<37

	out[12] =
		uint64(in[19]-minv)>>27 |
			uint64(in[20]-minv)<<12 |
			uint64(in[21]-minv)<<51

	out[13] =
		uint64(in[21]-minv)>>13 |
			uint64(in[22]-minv)<<26

	out[14] =
		uint64(in[22]-minv)>>38 |
			uint64(in[23]-minv)<<1 |
			uint64(in[24]-minv)<<40

	out[15] =
		uint64(in[24]-minv)>>24 |
			uint64(in[25]-minv)<<15 |
			uint64(in[26]-minv)<<54

	out[16] =
		uint64(in[26]-minv)>>10 |
			uint64(in[27]-minv)<<29

	out[17] =
		uint64(in[27]-minv)>>35 |
			uint64(in[28]-minv)<<4 |
			uint64(in[29]-minv)<<43

	out[18] =
		uint64(in[29]-minv)>>21 |
			uint64(in[30]-minv)<<18 |
			uint64(in[31]-minv)<<57

	out[19] =
		uint64(in[31]-minv)>>7 |
			uint64(in[32]-minv)<<32

	out[20] =
		uint64(in[32]-minv)>>32 |
			uint64(in[33]-minv)<<7 |
			uint64(in[34]-minv)<<46

	out[21] =
		uint64(in[34]-minv)>>18 |
			uint64(in[35]-minv)<<21 |
			uint64(in[36]-minv)<<60

	out[22] =
		uint64(in[36]-minv)>>4 |
			uint64(in[37]-minv)<<35

	out[23] =
		uint64(in[37]-minv)>>29 |
			uint64(in[38]-minv)<<10 |
			uint64(in[39]-minv)<<49

	out[24] =
		uint64(in[39]-minv)>>15 |
			uint64(in[40]-minv)<<24 |
			uint64(in[41]-minv)<<63

	out[25] =
		uint64(in[41]-minv)>>1 |
			uint64(in[42]-minv)<<38

	out[26] =
		uint64(in[42]-minv)>>26 |
			uint64(in[43]-minv)<<13 |
			uint64(in[44]-minv)<<52

	out[27] =
		uint64(in[44]-minv)>>12 |
			uint64(in[45]-minv)<<27

	out[28] =
		uint64(in[45]-minv)>>37 |
			uint64(in[46]-minv)<<2 |
			uint64(in[47]-minv)<<41

	out[29] =
		uint64(in[47]-minv)>>23 |
			uint64(in[48]-minv)<<16 |
			uint64(in[49]-minv)<<55

	out[30] =
		uint64(in[49]-minv)>>9 |
			uint64(in[50]-minv)<<30

	out[31] =
		uint64(in[50]-minv)>>34 |
			uint64(in[51]-minv)<<5 |
			uint64(in[52]-minv)<<44

	out[32] =
		uint64(in[52]-minv)>>20 |
			uint64(in[53]-minv)<<19 |
			uint64(in[54]-minv)<<58

	out[33] =
		uint64(in[54]-minv)>>6 |
			uint64(in[55]-minv)<<33

	out[34] =
		uint64(in[55]-minv)>>31 |
			uint64(in[56]-minv)<<8 |
			uint64(in[57]-minv)<<47

	out[35] =
		uint64(in[57]-minv)>>17 |
			uint64(in[58]-minv)<<22 |
			uint64(in[59]-minv)<<61

	out[36] =
		uint64(in[59]-minv)>>3 |
			uint64(in[60]-minv)<<36

	out[37] =
		uint64(in[60]-minv)>>28 |
			uint64(in[61]-minv)<<11 |
			uint64(in[62]-minv)<<50

	out[38] =
		uint64(in[62]-minv)>>14 |
			uint64(in[63]-minv)<<25
}

func bp64_40(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[40]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<40

	out[1] =
		uint64(in[1]-minv)>>24 |
			uint64(in[2]-minv)<<16 |
			uint64(in[3]-minv)<<56

	out[2] =
		uint64(in[3]-minv)>>8 |
			uint64(in[4]-minv)<<32

	out[3] =
		uint64(in[4]-minv)>>32 |
			uint64(in[5]-minv)<<8 |
			uint64(in[6]-minv)<<48

	out[4] =
		uint64(in[6]-minv)>>16 |
			uint64(in[7]-minv)<<24

	out[5] =
		uint64(in[7]-minv)>>40 |
			uint64(in[8]-minv) |
			uint64(in[9]-minv)<<40

	out[6] =
		uint64(in[9]-minv)>>24 |
			uint64(in[10]-minv)<<16 |
			uint64(in[11]-minv)<<56

	out[7] =
		uint64(in[11]-minv)>>8 |
			uint64(in[12]-minv)<<32

	out[8] =
		uint64(in[12]-minv)>>32 |
			uint64(in[13]-minv)<<8 |
			uint64(in[14]-minv)<<48

	out[9] =
		uint64(in[14]-minv)>>16 |
			uint64(in[15]-minv)<<24

	out[10] =
		uint64(in[15]-minv)>>40 |
			uint64(in[16]-minv) |
			uint64(in[17]-minv)<<40

	out[11] =
		uint64(in[17]-minv)>>24 |
			uint64(in[18]-minv)<<16 |
			uint64(in[19]-minv)<<56

	out[12] =
		uint64(in[19]-minv)>>8 |
			uint64(in[20]-minv)<<32

	out[13] =
		uint64(in[20]-minv)>>32 |
			uint64(in[21]-minv)<<8 |
			uint64(in[22]-minv)<<48

	out[14] =
		uint64(in[22]-minv)>>16 |
			uint64(in[23]-minv)<<24

	out[15] =
		uint64(in[23]-minv)>>40 |
			uint64(in[24]-minv) |
			uint64(in[25]-minv)<<40

	out[16] =
		uint64(in[25]-minv)>>24 |
			uint64(in[26]-minv)<<16 |
			uint64(in[27]-minv)<<56

	out[17] =
		uint64(in[27]-minv)>>8 |
			uint64(in[28]-minv)<<32

	out[18] =
		uint64(in[28]-minv)>>32 |
			uint64(in[29]-minv)<<8 |
			uint64(in[30]-minv)<<48

	out[19] =
		uint64(in[30]-minv)>>16 |
			uint64(in[31]-minv)<<24

	out[20] =
		uint64(in[31]-minv)>>40 |
			uint64(in[32]-minv) |
			uint64(in[33]-minv)<<40

	out[21] =
		uint64(in[33]-minv)>>24 |
			uint64(in[34]-minv)<<16 |
			uint64(in[35]-minv)<<56

	out[22] =
		uint64(in[35]-minv)>>8 |
			uint64(in[36]-minv)<<32

	out[23] =
		uint64(in[36]-minv)>>32 |
			uint64(in[37]-minv)<<8 |
			uint64(in[38]-minv)<<48

	out[24] =
		uint64(in[38]-minv)>>16 |
			uint64(in[39]-minv)<<24

	out[25] =
		uint64(in[39]-minv)>>40 |
			uint64(in[40]-minv) |
			uint64(in[41]-minv)<<40

	out[26] =
		uint64(in[41]-minv)>>24 |
			uint64(in[42]-minv)<<16 |
			uint64(in[43]-minv)<<56

	out[27] =
		uint64(in[43]-minv)>>8 |
			uint64(in[44]-minv)<<32

	out[28] =
		uint64(in[44]-minv)>>32 |
			uint64(in[45]-minv)<<8 |
			uint64(in[46]-minv)<<48

	out[29] =
		uint64(in[46]-minv)>>16 |
			uint64(in[47]-minv)<<24

	out[30] =
		uint64(in[47]-minv)>>40 |
			uint64(in[48]-minv) |
			uint64(in[49]-minv)<<40

	out[31] =
		uint64(in[49]-minv)>>24 |
			uint64(in[50]-minv)<<16 |
			uint64(in[51]-minv)<<56

	out[32] =
		uint64(in[51]-minv)>>8 |
			uint64(in[52]-minv)<<32

	out[33] =
		uint64(in[52]-minv)>>32 |
			uint64(in[53]-minv)<<8 |
			uint64(in[54]-minv)<<48

	out[34] =
		uint64(in[54]-minv)>>16 |
			uint64(in[55]-minv)<<24

	out[35] =
		uint64(in[55]-minv)>>40 |
			uint64(in[56]-minv) |
			uint64(in[57]-minv)<<40

	out[36] =
		uint64(in[57]-minv)>>24 |
			uint64(in[58]-minv)<<16 |
			uint64(in[59]-minv)<<56

	out[37] =
		uint64(in[59]-minv)>>8 |
			uint64(in[60]-minv)<<32

	out[38] =
		uint64(in[60]-minv)>>32 |
			uint64(in[61]-minv)<<8 |
			uint64(in[62]-minv)<<48

	out[39] =
		uint64(in[62]-minv)>>16 |
			uint64(in[63]-minv)<<24
}

func bp64_41(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[41]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<41

	out[1] =
		uint64(in[1]-minv)>>23 |
			uint64(in[2]-minv)<<18 |
			uint64(in[3]-minv)<<59

	out[2] =
		uint64(in[3]-minv)>>5 |
			uint64(in[4]-minv)<<36

	out[3] =
		uint64(in[4]-minv)>>28 |
			uint64(in[5]-minv)<<13 |
			uint64(in[6]-minv)<<54

	out[4] =
		uint64(in[6]-minv)>>10 |
			uint64(in[7]-minv)<<31

	out[5] =
		uint64(in[7]-minv)>>33 |
			uint64(in[8]-minv)<<8 |
			uint64(in[9]-minv)<<49

	out[6] =
		uint64(in[9]-minv)>>15 |
			uint64(in[10]-minv)<<26

	out[7] =
		uint64(in[10]-minv)>>38 |
			uint64(in[11]-minv)<<3 |
			uint64(in[12]-minv)<<44

	out[8] =
		uint64(in[12]-minv)>>20 |
			uint64(in[13]-minv)<<21 |
			uint64(in[14]-minv)<<62

	out[9] =
		uint64(in[14]-minv)>>2 |
			uint64(in[15]-minv)<<39

	out[10] =
		uint64(in[15]-minv)>>25 |
			uint64(in[16]-minv)<<16 |
			uint64(in[17]-minv)<<57

	out[11] =
		uint64(in[17]-minv)>>7 |
			uint64(in[18]-minv)<<34

	out[12] =
		uint64(in[18]-minv)>>30 |
			uint64(in[19]-minv)<<11 |
			uint64(in[20]-minv)<<52

	out[13] =
		uint64(in[20]-minv)>>12 |
			uint64(in[21]-minv)<<29

	out[14] =
		uint64(in[21]-minv)>>35 |
			uint64(in[22]-minv)<<6 |
			uint64(in[23]-minv)<<47

	out[15] =
		uint64(in[23]-minv)>>17 |
			uint64(in[24]-minv)<<24

	out[16] =
		uint64(in[24]-minv)>>40 |
			uint64(in[25]-minv)<<1 |
			uint64(in[26]-minv)<<42

	out[17] =
		uint64(in[26]-minv)>>22 |
			uint64(in[27]-minv)<<19 |
			uint64(in[28]-minv)<<60

	out[18] =
		uint64(in[28]-minv)>>4 |
			uint64(in[29]-minv)<<37

	out[19] =
		uint64(in[29]-minv)>>27 |
			uint64(in[30]-minv)<<14 |
			uint64(in[31]-minv)<<55

	out[20] =
		uint64(in[31]-minv)>>9 |
			uint64(in[32]-minv)<<32

	out[21] =
		uint64(in[32]-minv)>>32 |
			uint64(in[33]-minv)<<9 |
			uint64(in[34]-minv)<<50

	out[22] =
		uint64(in[34]-minv)>>14 |
			uint64(in[35]-minv)<<27

	out[23] =
		uint64(in[35]-minv)>>37 |
			uint64(in[36]-minv)<<4 |
			uint64(in[37]-minv)<<45

	out[24] =
		uint64(in[37]-minv)>>19 |
			uint64(in[38]-minv)<<22 |
			uint64(in[39]-minv)<<63

	out[25] =
		uint64(in[39]-minv)>>1 |
			uint64(in[40]-minv)<<40

	out[26] =
		uint64(in[40]-minv)>>24 |
			uint64(in[41]-minv)<<17 |
			uint64(in[42]-minv)<<58

	out[27] =
		uint64(in[42]-minv)>>6 |
			uint64(in[43]-minv)<<35

	out[28] =
		uint64(in[43]-minv)>>29 |
			uint64(in[44]-minv)<<12 |
			uint64(in[45]-minv)<<53

	out[29] =
		uint64(in[45]-minv)>>11 |
			uint64(in[46]-minv)<<30

	out[30] =
		uint64(in[46]-minv)>>34 |
			uint64(in[47]-minv)<<7 |
			uint64(in[48]-minv)<<48

	out[31] =
		uint64(in[48]-minv)>>16 |
			uint64(in[49]-minv)<<25

	out[32] =
		uint64(in[49]-minv)>>39 |
			uint64(in[50]-minv)<<2 |
			uint64(in[51]-minv)<<43

	out[33] =
		uint64(in[51]-minv)>>21 |
			uint64(in[52]-minv)<<20 |
			uint64(in[53]-minv)<<61

	out[34] =
		uint64(in[53]-minv)>>3 |
			uint64(in[54]-minv)<<38

	out[35] =
		uint64(in[54]-minv)>>26 |
			uint64(in[55]-minv)<<15 |
			uint64(in[56]-minv)<<56

	out[36] =
		uint64(in[56]-minv)>>8 |
			uint64(in[57]-minv)<<33

	out[37] =
		uint64(in[57]-minv)>>31 |
			uint64(in[58]-minv)<<10 |
			uint64(in[59]-minv)<<51

	out[38] =
		uint64(in[59]-minv)>>13 |
			uint64(in[60]-minv)<<28

	out[39] =
		uint64(in[60]-minv)>>36 |
			uint64(in[61]-minv)<<5 |
			uint64(in[62]-minv)<<46

	out[40] =
		uint64(in[62]-minv)>>18 |
			uint64(in[63]-minv)<<23
}

func bp64_42(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[42]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<42

	out[1] =
		uint64(in[1]-minv)>>22 |
			uint64(in[2]-minv)<<20 |
			uint64(in[3]-minv)<<62

	out[2] =
		uint64(in[3]-minv)>>2 |
			uint64(in[4]-minv)<<40

	out[3] =
		uint64(in[4]-minv)>>24 |
			uint64(in[5]-minv)<<18 |
			uint64(in[6]-minv)<<60

	out[4] =
		uint64(in[6]-minv)>>4 |
			uint64(in[7]-minv)<<38

	out[5] =
		uint64(in[7]-minv)>>26 |
			uint64(in[8]-minv)<<16 |
			uint64(in[9]-minv)<<58

	out[6] =
		uint64(in[9]-minv)>>6 |
			uint64(in[10]-minv)<<36

	out[7] =
		uint64(in[10]-minv)>>28 |
			uint64(in[11]-minv)<<14 |
			uint64(in[12]-minv)<<56

	out[8] =
		uint64(in[12]-minv)>>8 |
			uint64(in[13]-minv)<<34

	out[9] =
		uint64(in[13]-minv)>>30 |
			uint64(in[14]-minv)<<12 |
			uint64(in[15]-minv)<<54

	out[10] =
		uint64(in[15]-minv)>>10 |
			uint64(in[16]-minv)<<32

	out[11] =
		uint64(in[16]-minv)>>32 |
			uint64(in[17]-minv)<<10 |
			uint64(in[18]-minv)<<52

	out[12] =
		uint64(in[18]-minv)>>12 |
			uint64(in[19]-minv)<<30

	out[13] =
		uint64(in[19]-minv)>>34 |
			uint64(in[20]-minv)<<8 |
			uint64(in[21]-minv)<<50

	out[14] =
		uint64(in[21]-minv)>>14 |
			uint64(in[22]-minv)<<28

	out[15] =
		uint64(in[22]-minv)>>36 |
			uint64(in[23]-minv)<<6 |
			uint64(in[24]-minv)<<48

	out[16] =
		uint64(in[24]-minv)>>16 |
			uint64(in[25]-minv)<<26

	out[17] =
		uint64(in[25]-minv)>>38 |
			uint64(in[26]-minv)<<4 |
			uint64(in[27]-minv)<<46

	out[18] =
		uint64(in[27]-minv)>>18 |
			uint64(in[28]-minv)<<24

	out[19] =
		uint64(in[28]-minv)>>40 |
			uint64(in[29]-minv)<<2 |
			uint64(in[30]-minv)<<44

	out[20] =
		uint64(in[30]-minv)>>20 |
			uint64(in[31]-minv)<<22

	out[21] =
		uint64(in[31]-minv)>>42 |
			uint64(in[32]-minv) |
			uint64(in[33]-minv)<<42

	out[22] =
		uint64(in[33]-minv)>>22 |
			uint64(in[34]-minv)<<20 |
			uint64(in[35]-minv)<<62

	out[23] =
		uint64(in[35]-minv)>>2 |
			uint64(in[36]-minv)<<40

	out[24] =
		uint64(in[36]-minv)>>24 |
			uint64(in[37]-minv)<<18 |
			uint64(in[38]-minv)<<60

	out[25] =
		uint64(in[38]-minv)>>4 |
			uint64(in[39]-minv)<<38

	out[26] =
		uint64(in[39]-minv)>>26 |
			uint64(in[40]-minv)<<16 |
			uint64(in[41]-minv)<<58

	out[27] =
		uint64(in[41]-minv)>>6 |
			uint64(in[42]-minv)<<36

	out[28] =
		uint64(in[42]-minv)>>28 |
			uint64(in[43]-minv)<<14 |
			uint64(in[44]-minv)<<56

	out[29] =
		uint64(in[44]-minv)>>8 |
			uint64(in[45]-minv)<<34

	out[30] =
		uint64(in[45]-minv)>>30 |
			uint64(in[46]-minv)<<12 |
			uint64(in[47]-minv)<<54

	out[31] =
		uint64(in[47]-minv)>>10 |
			uint64(in[48]-minv)<<32

	out[32] =
		uint64(in[48]-minv)>>32 |
			uint64(in[49]-minv)<<10 |
			uint64(in[50]-minv)<<52

	out[33] =
		uint64(in[50]-minv)>>12 |
			uint64(in[51]-minv)<<30

	out[34] =
		uint64(in[51]-minv)>>34 |
			uint64(in[52]-minv)<<8 |
			uint64(in[53]-minv)<<50

	out[35] =
		uint64(in[53]-minv)>>14 |
			uint64(in[54]-minv)<<28

	out[36] =
		uint64(in[54]-minv)>>36 |
			uint64(in[55]-minv)<<6 |
			uint64(in[56]-minv)<<48

	out[37] =
		uint64(in[56]-minv)>>16 |
			uint64(in[57]-minv)<<26

	out[38] =
		uint64(in[57]-minv)>>38 |
			uint64(in[58]-minv)<<4 |
			uint64(in[59]-minv)<<46

	out[39] =
		uint64(in[59]-minv)>>18 |
			uint64(in[60]-minv)<<24

	out[40] =
		uint64(in[60]-minv)>>40 |
			uint64(in[61]-minv)<<2 |
			uint64(in[62]-minv)<<44

	out[41] =
		uint64(in[62]-minv)>>20 |
			uint64(in[63]-minv)<<22
}

func bp64_43(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[43]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<43

	out[1] =
		uint64(in[1]-minv)>>21 |
			uint64(in[2]-minv)<<22

	out[2] =
		uint64(in[2]-minv)>>42 |
			uint64(in[3]-minv)<<1 |
			uint64(in[4]-minv)<<44

	out[3] =
		uint64(in[4]-minv)>>20 |
			uint64(in[5]-minv)<<23

	out[4] =
		uint64(in[5]-minv)>>41 |
			uint64(in[6]-minv)<<2 |
			uint64(in[7]-minv)<<45

	out[5] =
		uint64(in[7]-minv)>>19 |
			uint64(in[8]-minv)<<24

	out[6] =
		uint64(in[8]-minv)>>40 |
			uint64(in[9]-minv)<<3 |
			uint64(in[10]-minv)<<46

	out[7] =
		uint64(in[10]-minv)>>18 |
			uint64(in[11]-minv)<<25

	out[8] =
		uint64(in[11]-minv)>>39 |
			uint64(in[12]-minv)<<4 |
			uint64(in[13]-minv)<<47

	out[9] =
		uint64(in[13]-minv)>>17 |
			uint64(in[14]-minv)<<26

	out[10] =
		uint64(in[14]-minv)>>38 |
			uint64(in[15]-minv)<<5 |
			uint64(in[16]-minv)<<48

	out[11] =
		uint64(in[16]-minv)>>16 |
			uint64(in[17]-minv)<<27

	out[12] =
		uint64(in[17]-minv)>>37 |
			uint64(in[18]-minv)<<6 |
			uint64(in[19]-minv)<<49

	out[13] =
		uint64(in[19]-minv)>>15 |
			uint64(in[20]-minv)<<28

	out[14] =
		uint64(in[20]-minv)>>36 |
			uint64(in[21]-minv)<<7 |
			uint64(in[22]-minv)<<50

	out[15] =
		uint64(in[22]-minv)>>14 |
			uint64(in[23]-minv)<<29

	out[16] =
		uint64(in[23]-minv)>>35 |
			uint64(in[24]-minv)<<8 |
			uint64(in[25]-minv)<<51

	out[17] =
		uint64(in[25]-minv)>>13 |
			uint64(in[26]-minv)<<30

	out[18] =
		uint64(in[26]-minv)>>34 |
			uint64(in[27]-minv)<<9 |
			uint64(in[28]-minv)<<52

	out[19] =
		uint64(in[28]-minv)>>12 |
			uint64(in[29]-minv)<<31

	out[20] =
		uint64(in[29]-minv)>>33 |
			uint64(in[30]-minv)<<10 |
			uint64(in[31]-minv)<<53

	out[21] =
		uint64(in[31]-minv)>>11 |
			uint64(in[32]-minv)<<32

	out[22] =
		uint64(in[32]-minv)>>32 |
			uint64(in[33]-minv)<<11 |
			uint64(in[34]-minv)<<54

	out[23] =
		uint64(in[34]-minv)>>10 |
			uint64(in[35]-minv)<<33

	out[24] =
		uint64(in[35]-minv)>>31 |
			uint64(in[36]-minv)<<12 |
			uint64(in[37]-minv)<<55

	out[25] =
		uint64(in[37]-minv)>>9 |
			uint64(in[38]-minv)<<34

	out[26] =
		uint64(in[38]-minv)>>30 |
			uint64(in[39]-minv)<<13 |
			uint64(in[40]-minv)<<56

	out[27] =
		uint64(in[40]-minv)>>8 |
			uint64(in[41]-minv)<<35

	out[28] =
		uint64(in[41]-minv)>>29 |
			uint64(in[42]-minv)<<14 |
			uint64(in[43]-minv)<<57

	out[29] =
		uint64(in[43]-minv)>>7 |
			uint64(in[44]-minv)<<36

	out[30] =
		uint64(in[44]-minv)>>28 |
			uint64(in[45]-minv)<<15 |
			uint64(in[46]-minv)<<58

	out[31] =
		uint64(in[46]-minv)>>6 |
			uint64(in[47]-minv)<<37

	out[32] =
		uint64(in[47]-minv)>>27 |
			uint64(in[48]-minv)<<16 |
			uint64(in[49]-minv)<<59

	out[33] =
		uint64(in[49]-minv)>>5 |
			uint64(in[50]-minv)<<38

	out[34] =
		uint64(in[50]-minv)>>26 |
			uint64(in[51]-minv)<<17 |
			uint64(in[52]-minv)<<60

	out[35] =
		uint64(in[52]-minv)>>4 |
			uint64(in[53]-minv)<<39

	out[36] =
		uint64(in[53]-minv)>>25 |
			uint64(in[54]-minv)<<18 |
			uint64(in[55]-minv)<<61

	out[37] =
		uint64(in[55]-minv)>>3 |
			uint64(in[56]-minv)<<40

	out[38] =
		uint64(in[56]-minv)>>24 |
			uint64(in[57]-minv)<<19 |
			uint64(in[58]-minv)<<62

	out[39] =
		uint64(in[58]-minv)>>2 |
			uint64(in[59]-minv)<<41

	out[40] =
		uint64(in[59]-minv)>>23 |
			uint64(in[60]-minv)<<20 |
			uint64(in[61]-minv)<<63

	out[41] =
		uint64(in[61]-minv)>>1 |
			uint64(in[62]-minv)<<42

	out[42] =
		uint64(in[62]-minv)>>22 |
			uint64(in[63]-minv)<<21
}

func bp64_44(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[44]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<44

	out[1] =
		uint64(in[1]-minv)>>20 |
			uint64(in[2]-minv)<<24

	out[2] =
		uint64(in[2]-minv)>>40 |
			uint64(in[3]-minv)<<4 |
			uint64(in[4]-minv)<<48

	out[3] =
		uint64(in[4]-minv)>>16 |
			uint64(in[5]-minv)<<28

	out[4] =
		uint64(in[5]-minv)>>36 |
			uint64(in[6]-minv)<<8 |
			uint64(in[7]-minv)<<52

	out[5] =
		uint64(in[7]-minv)>>12 |
			uint64(in[8]-minv)<<32

	out[6] =
		uint64(in[8]-minv)>>32 |
			uint64(in[9]-minv)<<12 |
			uint64(in[10]-minv)<<56

	out[7] =
		uint64(in[10]-minv)>>8 |
			uint64(in[11]-minv)<<36

	out[8] =
		uint64(in[11]-minv)>>28 |
			uint64(in[12]-minv)<<16 |
			uint64(in[13]-minv)<<60

	out[9] =
		uint64(in[13]-minv)>>4 |
			uint64(in[14]-minv)<<40

	out[10] =
		uint64(in[14]-minv)>>24 |
			uint64(in[15]-minv)<<20

	out[11] =
		uint64(in[15]-minv)>>44 |
			uint64(in[16]-minv) |
			uint64(in[17]-minv)<<44

	out[12] =
		uint64(in[17]-minv)>>20 |
			uint64(in[18]-minv)<<24

	out[13] =
		uint64(in[18]-minv)>>40 |
			uint64(in[19]-minv)<<4 |
			uint64(in[20]-minv)<<48

	out[14] =
		uint64(in[20]-minv)>>16 |
			uint64(in[21]-minv)<<28

	out[15] =
		uint64(in[21]-minv)>>36 |
			uint64(in[22]-minv)<<8 |
			uint64(in[23]-minv)<<52

	out[16] =
		uint64(in[23]-minv)>>12 |
			uint64(in[24]-minv)<<32

	out[17] =
		uint64(in[24]-minv)>>32 |
			uint64(in[25]-minv)<<12 |
			uint64(in[26]-minv)<<56

	out[18] =
		uint64(in[26]-minv)>>8 |
			uint64(in[27]-minv)<<36

	out[19] =
		uint64(in[27]-minv)>>28 |
			uint64(in[28]-minv)<<16 |
			uint64(in[29]-minv)<<60

	out[20] =
		uint64(in[29]-minv)>>4 |
			uint64(in[30]-minv)<<40

	out[21] =
		uint64(in[30]-minv)>>24 |
			uint64(in[31]-minv)<<20

	out[22] =
		uint64(in[31]-minv)>>44 |
			uint64(in[32]-minv) |
			uint64(in[33]-minv)<<44

	out[23] =
		uint64(in[33]-minv)>>20 |
			uint64(in[34]-minv)<<24

	out[24] =
		uint64(in[34]-minv)>>40 |
			uint64(in[35]-minv)<<4 |
			uint64(in[36]-minv)<<48

	out[25] =
		uint64(in[36]-minv)>>16 |
			uint64(in[37]-minv)<<28

	out[26] =
		uint64(in[37]-minv)>>36 |
			uint64(in[38]-minv)<<8 |
			uint64(in[39]-minv)<<52

	out[27] =
		uint64(in[39]-minv)>>12 |
			uint64(in[40]-minv)<<32

	out[28] =
		uint64(in[40]-minv)>>32 |
			uint64(in[41]-minv)<<12 |
			uint64(in[42]-minv)<<56

	out[29] =
		uint64(in[42]-minv)>>8 |
			uint64(in[43]-minv)<<36

	out[30] =
		uint64(in[43]-minv)>>28 |
			uint64(in[44]-minv)<<16 |
			uint64(in[45]-minv)<<60

	out[31] =
		uint64(in[45]-minv)>>4 |
			uint64(in[46]-minv)<<40

	out[32] =
		uint64(in[46]-minv)>>24 |
			uint64(in[47]-minv)<<20

	out[33] =
		uint64(in[47]-minv)>>44 |
			uint64(in[48]-minv) |
			uint64(in[49]-minv)<<44

	out[34] =
		uint64(in[49]-minv)>>20 |
			uint64(in[50]-minv)<<24

	out[35] =
		uint64(in[50]-minv)>>40 |
			uint64(in[51]-minv)<<4 |
			uint64(in[52]-minv)<<48

	out[36] =
		uint64(in[52]-minv)>>16 |
			uint64(in[53]-minv)<<28

	out[37] =
		uint64(in[53]-minv)>>36 |
			uint64(in[54]-minv)<<8 |
			uint64(in[55]-minv)<<52

	out[38] =
		uint64(in[55]-minv)>>12 |
			uint64(in[56]-minv)<<32

	out[39] =
		uint64(in[56]-minv)>>32 |
			uint64(in[57]-minv)<<12 |
			uint64(in[58]-minv)<<56

	out[40] =
		uint64(in[58]-minv)>>8 |
			uint64(in[59]-minv)<<36

	out[41] =
		uint64(in[59]-minv)>>28 |
			uint64(in[60]-minv)<<16 |
			uint64(in[61]-minv)<<60

	out[42] =
		uint64(in[61]-minv)>>4 |
			uint64(in[62]-minv)<<40

	out[43] =
		uint64(in[62]-minv)>>24 |
			uint64(in[63]-minv)<<20
}

func bp64_45(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[45]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<45

	out[1] =
		uint64(in[1]-minv)>>19 |
			uint64(in[2]-minv)<<26

	out[2] =
		uint64(in[2]-minv)>>38 |
			uint64(in[3]-minv)<<7 |
			uint64(in[4]-minv)<<52

	out[3] =
		uint64(in[4]-minv)>>12 |
			uint64(in[5]-minv)<<33

	out[4] =
		uint64(in[5]-minv)>>31 |
			uint64(in[6]-minv)<<14 |
			uint64(in[7]-minv)<<59

	out[5] =
		uint64(in[7]-minv)>>5 |
			uint64(in[8]-minv)<<40

	out[6] =
		uint64(in[8]-minv)>>24 |
			uint64(in[9]-minv)<<21

	out[7] =
		uint64(in[9]-minv)>>43 |
			uint64(in[10]-minv)<<2 |
			uint64(in[11]-minv)<<47

	out[8] =
		uint64(in[11]-minv)>>17 |
			uint64(in[12]-minv)<<28

	out[9] =
		uint64(in[12]-minv)>>36 |
			uint64(in[13]-minv)<<9 |
			uint64(in[14]-minv)<<54

	out[10] =
		uint64(in[14]-minv)>>10 |
			uint64(in[15]-minv)<<35

	out[11] =
		uint64(in[15]-minv)>>29 |
			uint64(in[16]-minv)<<16 |
			uint64(in[17]-minv)<<61

	out[12] =
		uint64(in[17]-minv)>>3 |
			uint64(in[18]-minv)<<42

	out[13] =
		uint64(in[18]-minv)>>22 |
			uint64(in[19]-minv)<<23

	out[14] =
		uint64(in[19]-minv)>>41 |
			uint64(in[20]-minv)<<4 |
			uint64(in[21]-minv)<<49

	out[15] =
		uint64(in[21]-minv)>>15 |
			uint64(in[22]-minv)<<30

	out[16] =
		uint64(in[22]-minv)>>34 |
			uint64(in[23]-minv)<<11 |
			uint64(in[24]-minv)<<56

	out[17] =
		uint64(in[24]-minv)>>8 |
			uint64(in[25]-minv)<<37

	out[18] =
		uint64(in[25]-minv)>>27 |
			uint64(in[26]-minv)<<18 |
			uint64(in[27]-minv)<<63

	out[19] =
		uint64(in[27]-minv)>>1 |
			uint64(in[28]-minv)<<44

	out[20] =
		uint64(in[28]-minv)>>20 |
			uint64(in[29]-minv)<<25

	out[21] =
		uint64(in[29]-minv)>>39 |
			uint64(in[30]-minv)<<6 |
			uint64(in[31]-minv)<<51

	out[22] =
		uint64(in[31]-minv)>>13 |
			uint64(in[32]-minv)<<32

	out[23] =
		uint64(in[32]-minv)>>32 |
			uint64(in[33]-minv)<<13 |
			uint64(in[34]-minv)<<58

	out[24] =
		uint64(in[34]-minv)>>6 |
			uint64(in[35]-minv)<<39

	out[25] =
		uint64(in[35]-minv)>>25 |
			uint64(in[36]-minv)<<20

	out[26] =
		uint64(in[36]-minv)>>44 |
			uint64(in[37]-minv)<<1 |
			uint64(in[38]-minv)<<46

	out[27] =
		uint64(in[38]-minv)>>18 |
			uint64(in[39]-minv)<<27

	out[28] =
		uint64(in[39]-minv)>>37 |
			uint64(in[40]-minv)<<8 |
			uint64(in[41]-minv)<<53

	out[29] =
		uint64(in[41]-minv)>>11 |
			uint64(in[42]-minv)<<34

	out[30] =
		uint64(in[42]-minv)>>30 |
			uint64(in[43]-minv)<<15 |
			uint64(in[44]-minv)<<60

	out[31] =
		uint64(in[44]-minv)>>4 |
			uint64(in[45]-minv)<<41

	out[32] =
		uint64(in[45]-minv)>>23 |
			uint64(in[46]-minv)<<22

	out[33] =
		uint64(in[46]-minv)>>42 |
			uint64(in[47]-minv)<<3 |
			uint64(in[48]-minv)<<48

	out[34] =
		uint64(in[48]-minv)>>16 |
			uint64(in[49]-minv)<<29

	out[35] =
		uint64(in[49]-minv)>>35 |
			uint64(in[50]-minv)<<10 |
			uint64(in[51]-minv)<<55

	out[36] =
		uint64(in[51]-minv)>>9 |
			uint64(in[52]-minv)<<36

	out[37] =
		uint64(in[52]-minv)>>28 |
			uint64(in[53]-minv)<<17 |
			uint64(in[54]-minv)<<62

	out[38] =
		uint64(in[54]-minv)>>2 |
			uint64(in[55]-minv)<<43

	out[39] =
		uint64(in[55]-minv)>>21 |
			uint64(in[56]-minv)<<24

	out[40] =
		uint64(in[56]-minv)>>40 |
			uint64(in[57]-minv)<<5 |
			uint64(in[58]-minv)<<50

	out[41] =
		uint64(in[58]-minv)>>14 |
			uint64(in[59]-minv)<<31

	out[42] =
		uint64(in[59]-minv)>>33 |
			uint64(in[60]-minv)<<12 |
			uint64(in[61]-minv)<<57

	out[43] =
		uint64(in[61]-minv)>>7 |
			uint64(in[62]-minv)<<38

	out[44] =
		uint64(in[62]-minv)>>26 |
			uint64(in[63]-minv)<<19
}

func bp64_46(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[46]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<46

	out[1] =
		uint64(in[1]-minv)>>18 |
			uint64(in[2]-minv)<<28

	out[2] =
		uint64(in[2]-minv)>>36 |
			uint64(in[3]-minv)<<10 |
			uint64(in[4]-minv)<<56

	out[3] =
		uint64(in[4]-minv)>>8 |
			uint64(in[5]-minv)<<38

	out[4] =
		uint64(in[5]-minv)>>26 |
			uint64(in[6]-minv)<<20

	out[5] =
		uint64(in[6]-minv)>>44 |
			uint64(in[7]-minv)<<2 |
			uint64(in[8]-minv)<<48

	out[6] =
		uint64(in[8]-minv)>>16 |
			uint64(in[9]-minv)<<30

	out[7] =
		uint64(in[9]-minv)>>34 |
			uint64(in[10]-minv)<<12 |
			uint64(in[11]-minv)<<58

	out[8] =
		uint64(in[11]-minv)>>6 |
			uint64(in[12]-minv)<<40

	out[9] =
		uint64(in[12]-minv)>>24 |
			uint64(in[13]-minv)<<22

	out[10] =
		uint64(in[13]-minv)>>42 |
			uint64(in[14]-minv)<<4 |
			uint64(in[15]-minv)<<50

	out[11] =
		uint64(in[15]-minv)>>14 |
			uint64(in[16]-minv)<<32

	out[12] =
		uint64(in[16]-minv)>>32 |
			uint64(in[17]-minv)<<14 |
			uint64(in[18]-minv)<<60

	out[13] =
		uint64(in[18]-minv)>>4 |
			uint64(in[19]-minv)<<42

	out[14] =
		uint64(in[19]-minv)>>22 |
			uint64(in[20]-minv)<<24

	out[15] =
		uint64(in[20]-minv)>>40 |
			uint64(in[21]-minv)<<6 |
			uint64(in[22]-minv)<<52

	out[16] =
		uint64(in[22]-minv)>>12 |
			uint64(in[23]-minv)<<34

	out[17] =
		uint64(in[23]-minv)>>30 |
			uint64(in[24]-minv)<<16 |
			uint64(in[25]-minv)<<62

	out[18] =
		uint64(in[25]-minv)>>2 |
			uint64(in[26]-minv)<<44

	out[19] =
		uint64(in[26]-minv)>>20 |
			uint64(in[27]-minv)<<26

	out[20] =
		uint64(in[27]-minv)>>38 |
			uint64(in[28]-minv)<<8 |
			uint64(in[29]-minv)<<54

	out[21] =
		uint64(in[29]-minv)>>10 |
			uint64(in[30]-minv)<<36

	out[22] =
		uint64(in[30]-minv)>>28 |
			uint64(in[31]-minv)<<18

	out[23] =
		uint64(in[31]-minv)>>46 |
			uint64(in[32]-minv) |
			uint64(in[33]-minv)<<46

	out[24] =
		uint64(in[33]-minv)>>18 |
			uint64(in[34]-minv)<<28

	out[25] =
		uint64(in[34]-minv)>>36 |
			uint64(in[35]-minv)<<10 |
			uint64(in[36]-minv)<<56

	out[26] =
		uint64(in[36]-minv)>>8 |
			uint64(in[37]-minv)<<38

	out[27] =
		uint64(in[37]-minv)>>26 |
			uint64(in[38]-minv)<<20

	out[28] =
		uint64(in[38]-minv)>>44 |
			uint64(in[39]-minv)<<2 |
			uint64(in[40]-minv)<<48

	out[29] =
		uint64(in[40]-minv)>>16 |
			uint64(in[41]-minv)<<30

	out[30] =
		uint64(in[41]-minv)>>34 |
			uint64(in[42]-minv)<<12 |
			uint64(in[43]-minv)<<58

	out[31] =
		uint64(in[43]-minv)>>6 |
			uint64(in[44]-minv)<<40

	out[32] =
		uint64(in[44]-minv)>>24 |
			uint64(in[45]-minv)<<22

	out[33] =
		uint64(in[45]-minv)>>42 |
			uint64(in[46]-minv)<<4 |
			uint64(in[47]-minv)<<50

	out[34] =
		uint64(in[47]-minv)>>14 |
			uint64(in[48]-minv)<<32

	out[35] =
		uint64(in[48]-minv)>>32 |
			uint64(in[49]-minv)<<14 |
			uint64(in[50]-minv)<<60

	out[36] =
		uint64(in[50]-minv)>>4 |
			uint64(in[51]-minv)<<42

	out[37] =
		uint64(in[51]-minv)>>22 |
			uint64(in[52]-minv)<<24

	out[38] =
		uint64(in[52]-minv)>>40 |
			uint64(in[53]-minv)<<6 |
			uint64(in[54]-minv)<<52

	out[39] =
		uint64(in[54]-minv)>>12 |
			uint64(in[55]-minv)<<34

	out[40] =
		uint64(in[55]-minv)>>30 |
			uint64(in[56]-minv)<<16 |
			uint64(in[57]-minv)<<62

	out[41] =
		uint64(in[57]-minv)>>2 |
			uint64(in[58]-minv)<<44

	out[42] =
		uint64(in[58]-minv)>>20 |
			uint64(in[59]-minv)<<26

	out[43] =
		uint64(in[59]-minv)>>38 |
			uint64(in[60]-minv)<<8 |
			uint64(in[61]-minv)<<54

	out[44] =
		uint64(in[61]-minv)>>10 |
			uint64(in[62]-minv)<<36

	out[45] =
		uint64(in[62]-minv)>>28 |
			uint64(in[63]-minv)<<18
}

func bp64_47(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[47]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<47

	out[1] =
		uint64(in[1]-minv)>>17 |
			uint64(in[2]-minv)<<30

	out[2] =
		uint64(in[2]-minv)>>34 |
			uint64(in[3]-minv)<<13 |
			uint64(in[4]-minv)<<60

	out[3] =
		uint64(in[4]-minv)>>4 |
			uint64(in[5]-minv)<<43

	out[4] =
		uint64(in[5]-minv)>>21 |
			uint64(in[6]-minv)<<26

	out[5] =
		uint64(in[6]-minv)>>38 |
			uint64(in[7]-minv)<<9 |
			uint64(in[8]-minv)<<56

	out[6] =
		uint64(in[8]-minv)>>8 |
			uint64(in[9]-minv)<<39

	out[7] =
		uint64(in[9]-minv)>>25 |
			uint64(in[10]-minv)<<22

	out[8] =
		uint64(in[10]-minv)>>42 |
			uint64(in[11]-minv)<<5 |
			uint64(in[12]-minv)<<52

	out[9] =
		uint64(in[12]-minv)>>12 |
			uint64(in[13]-minv)<<35

	out[10] =
		uint64(in[13]-minv)>>29 |
			uint64(in[14]-minv)<<18

	out[11] =
		uint64(in[14]-minv)>>46 |
			uint64(in[15]-minv)<<1 |
			uint64(in[16]-minv)<<48

	out[12] =
		uint64(in[16]-minv)>>16 |
			uint64(in[17]-minv)<<31

	out[13] =
		uint64(in[17]-minv)>>33 |
			uint64(in[18]-minv)<<14 |
			uint64(in[19]-minv)<<61

	out[14] =
		uint64(in[19]-minv)>>3 |
			uint64(in[20]-minv)<<44

	out[15] =
		uint64(in[20]-minv)>>20 |
			uint64(in[21]-minv)<<27

	out[16] =
		uint64(in[21]-minv)>>37 |
			uint64(in[22]-minv)<<10 |
			uint64(in[23]-minv)<<57

	out[17] =
		uint64(in[23]-minv)>>7 |
			uint64(in[24]-minv)<<40

	out[18] =
		uint64(in[24]-minv)>>24 |
			uint64(in[25]-minv)<<23

	out[19] =
		uint64(in[25]-minv)>>41 |
			uint64(in[26]-minv)<<6 |
			uint64(in[27]-minv)<<53

	out[20] =
		uint64(in[27]-minv)>>11 |
			uint64(in[28]-minv)<<36

	out[21] =
		uint64(in[28]-minv)>>28 |
			uint64(in[29]-minv)<<19

	out[22] =
		uint64(in[29]-minv)>>45 |
			uint64(in[30]-minv)<<2 |
			uint64(in[31]-minv)<<49

	out[23] =
		uint64(in[31]-minv)>>15 |
			uint64(in[32]-minv)<<32

	out[24] =
		uint64(in[32]-minv)>>32 |
			uint64(in[33]-minv)<<15 |
			uint64(in[34]-minv)<<62

	out[25] =
		uint64(in[34]-minv)>>2 |
			uint64(in[35]-minv)<<45

	out[26] =
		uint64(in[35]-minv)>>19 |
			uint64(in[36]-minv)<<28

	out[27] =
		uint64(in[36]-minv)>>36 |
			uint64(in[37]-minv)<<11 |
			uint64(in[38]-minv)<<58

	out[28] =
		uint64(in[38]-minv)>>6 |
			uint64(in[39]-minv)<<41

	out[29] =
		uint64(in[39]-minv)>>23 |
			uint64(in[40]-minv)<<24

	out[30] =
		uint64(in[40]-minv)>>40 |
			uint64(in[41]-minv)<<7 |
			uint64(in[42]-minv)<<54

	out[31] =
		uint64(in[42]-minv)>>10 |
			uint64(in[43]-minv)<<37

	out[32] =
		uint64(in[43]-minv)>>27 |
			uint64(in[44]-minv)<<20

	out[33] =
		uint64(in[44]-minv)>>44 |
			uint64(in[45]-minv)<<3 |
			uint64(in[46]-minv)<<50

	out[34] =
		uint64(in[46]-minv)>>14 |
			uint64(in[47]-minv)<<33

	out[35] =
		uint64(in[47]-minv)>>31 |
			uint64(in[48]-minv)<<16 |
			uint64(in[49]-minv)<<63

	out[36] =
		uint64(in[49]-minv)>>1 |
			uint64(in[50]-minv)<<46

	out[37] =
		uint64(in[50]-minv)>>18 |
			uint64(in[51]-minv)<<29

	out[38] =
		uint64(in[51]-minv)>>35 |
			uint64(in[52]-minv)<<12 |
			uint64(in[53]-minv)<<59

	out[39] =
		uint64(in[53]-minv)>>5 |
			uint64(in[54]-minv)<<42

	out[40] =
		uint64(in[54]-minv)>>22 |
			uint64(in[55]-minv)<<25

	out[41] =
		uint64(in[55]-minv)>>39 |
			uint64(in[56]-minv)<<8 |
			uint64(in[57]-minv)<<55

	out[42] =
		uint64(in[57]-minv)>>9 |
			uint64(in[58]-minv)<<38

	out[43] =
		uint64(in[58]-minv)>>26 |
			uint64(in[59]-minv)<<21

	out[44] =
		uint64(in[59]-minv)>>43 |
			uint64(in[60]-minv)<<4 |
			uint64(in[61]-minv)<<51

	out[45] =
		uint64(in[61]-minv)>>13 |
			uint64(in[62]-minv)<<34

	out[46] =
		uint64(in[62]-minv)>>30 |
			uint64(in[63]-minv)<<17
}

func bp64_48(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[48]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<48

	out[1] =
		uint64(in[1]-minv)>>16 |
			uint64(in[2]-minv)<<32

	out[2] =
		uint64(in[2]-minv)>>32 |
			uint64(in[3]-minv)<<16

	out[3] =
		uint64(in[3]-minv)>>48 |
			uint64(in[4]-minv) |
			uint64(in[5]-minv)<<48

	out[4] =
		uint64(in[5]-minv)>>16 |
			uint64(in[6]-minv)<<32

	out[5] =
		uint64(in[6]-minv)>>32 |
			uint64(in[7]-minv)<<16

	out[6] =
		uint64(in[7]-minv)>>48 |
			uint64(in[8]-minv) |
			uint64(in[9]-minv)<<48

	out[7] =
		uint64(in[9]-minv)>>16 |
			uint64(in[10]-minv)<<32

	out[8] =
		uint64(in[10]-minv)>>32 |
			uint64(in[11]-minv)<<16

	out[9] =
		uint64(in[11]-minv)>>48 |
			uint64(in[12]-minv) |
			uint64(in[13]-minv)<<48

	out[10] =
		uint64(in[13]-minv)>>16 |
			uint64(in[14]-minv)<<32

	out[11] =
		uint64(in[14]-minv)>>32 |
			uint64(in[15]-minv)<<16

	out[12] =
		uint64(in[15]-minv)>>48 |
			uint64(in[16]-minv) |
			uint64(in[17]-minv)<<48

	out[13] =
		uint64(in[17]-minv)>>16 |
			uint64(in[18]-minv)<<32

	out[14] =
		uint64(in[18]-minv)>>32 |
			uint64(in[19]-minv)<<16

	out[15] =
		uint64(in[19]-minv)>>48 |
			uint64(in[20]-minv) |
			uint64(in[21]-minv)<<48

	out[16] =
		uint64(in[21]-minv)>>16 |
			uint64(in[22]-minv)<<32

	out[17] =
		uint64(in[22]-minv)>>32 |
			uint64(in[23]-minv)<<16

	out[18] =
		uint64(in[23]-minv)>>48 |
			uint64(in[24]-minv) |
			uint64(in[25]-minv)<<48

	out[19] =
		uint64(in[25]-minv)>>16 |
			uint64(in[26]-minv)<<32

	out[20] =
		uint64(in[26]-minv)>>32 |
			uint64(in[27]-minv)<<16

	out[21] =
		uint64(in[27]-minv)>>48 |
			uint64(in[28]-minv) |
			uint64(in[29]-minv)<<48

	out[22] =
		uint64(in[29]-minv)>>16 |
			uint64(in[30]-minv)<<32

	out[23] =
		uint64(in[30]-minv)>>32 |
			uint64(in[31]-minv)<<16

	out[24] =
		uint64(in[31]-minv)>>48 |
			uint64(in[32]-minv) |
			uint64(in[33]-minv)<<48

	out[25] =
		uint64(in[33]-minv)>>16 |
			uint64(in[34]-minv)<<32

	out[26] =
		uint64(in[34]-minv)>>32 |
			uint64(in[35]-minv)<<16

	out[27] =
		uint64(in[35]-minv)>>48 |
			uint64(in[36]-minv) |
			uint64(in[37]-minv)<<48

	out[28] =
		uint64(in[37]-minv)>>16 |
			uint64(in[38]-minv)<<32

	out[29] =
		uint64(in[38]-minv)>>32 |
			uint64(in[39]-minv)<<16

	out[30] =
		uint64(in[39]-minv)>>48 |
			uint64(in[40]-minv) |
			uint64(in[41]-minv)<<48

	out[31] =
		uint64(in[41]-minv)>>16 |
			uint64(in[42]-minv)<<32

	out[32] =
		uint64(in[42]-minv)>>32 |
			uint64(in[43]-minv)<<16

	out[33] =
		uint64(in[43]-minv)>>48 |
			uint64(in[44]-minv) |
			uint64(in[45]-minv)<<48

	out[34] =
		uint64(in[45]-minv)>>16 |
			uint64(in[46]-minv)<<32

	out[35] =
		uint64(in[46]-minv)>>32 |
			uint64(in[47]-minv)<<16

	out[36] =
		uint64(in[47]-minv)>>48 |
			uint64(in[48]-minv) |
			uint64(in[49]-minv)<<48

	out[37] =
		uint64(in[49]-minv)>>16 |
			uint64(in[50]-minv)<<32

	out[38] =
		uint64(in[50]-minv)>>32 |
			uint64(in[51]-minv)<<16

	out[39] =
		uint64(in[51]-minv)>>48 |
			uint64(in[52]-minv) |
			uint64(in[53]-minv)<<48

	out[40] =
		uint64(in[53]-minv)>>16 |
			uint64(in[54]-minv)<<32

	out[41] =
		uint64(in[54]-minv)>>32 |
			uint64(in[55]-minv)<<16

	out[42] =
		uint64(in[55]-minv)>>48 |
			uint64(in[56]-minv) |
			uint64(in[57]-minv)<<48

	out[43] =
		uint64(in[57]-minv)>>16 |
			uint64(in[58]-minv)<<32

	out[44] =
		uint64(in[58]-minv)>>32 |
			uint64(in[59]-minv)<<16

	out[45] =
		uint64(in[59]-minv)>>48 |
			uint64(in[60]-minv) |
			uint64(in[61]-minv)<<48

	out[46] =
		uint64(in[61]-minv)>>16 |
			uint64(in[62]-minv)<<32

	out[47] =
		uint64(in[62]-minv)>>32 |
			uint64(in[63]-minv)<<16
}

func bp64_49(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[49]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<49

	out[1] =
		uint64(in[1]-minv)>>15 |
			uint64(in[2]-minv)<<34

	out[2] =
		uint64(in[2]-minv)>>30 |
			uint64(in[3]-minv)<<19

	out[3] =
		uint64(in[3]-minv)>>45 |
			uint64(in[4]-minv)<<4 |
			uint64(in[5]-minv)<<53

	out[4] =
		uint64(in[5]-minv)>>11 |
			uint64(in[6]-minv)<<38

	out[5] =
		uint64(in[6]-minv)>>26 |
			uint64(in[7]-minv)<<23

	out[6] =
		uint64(in[7]-minv)>>41 |
			uint64(in[8]-minv)<<8 |
			uint64(in[9]-minv)<<57

	out[7] =
		uint64(in[9]-minv)>>7 |
			uint64(in[10]-minv)<<42

	out[8] =
		uint64(in[10]-minv)>>22 |
			uint64(in[11]-minv)<<27

	out[9] =
		uint64(in[11]-minv)>>37 |
			uint64(in[12]-minv)<<12 |
			uint64(in[13]-minv)<<61

	out[10] =
		uint64(in[13]-minv)>>3 |
			uint64(in[14]-minv)<<46

	out[11] =
		uint64(in[14]-minv)>>18 |
			uint64(in[15]-minv)<<31

	out[12] =
		uint64(in[15]-minv)>>33 |
			uint64(in[16]-minv)<<16

	out[13] =
		uint64(in[16]-minv)>>48 |
			uint64(in[17]-minv)<<1 |
			uint64(in[18]-minv)<<50

	out[14] =
		uint64(in[18]-minv)>>14 |
			uint64(in[19]-minv)<<35

	out[15] =
		uint64(in[19]-minv)>>29 |
			uint64(in[20]-minv)<<20

	out[16] =
		uint64(in[20]-minv)>>44 |
			uint64(in[21]-minv)<<5 |
			uint64(in[22]-minv)<<54

	out[17] =
		uint64(in[22]-minv)>>10 |
			uint64(in[23]-minv)<<39

	out[18] =
		uint64(in[23]-minv)>>25 |
			uint64(in[24]-minv)<<24

	out[19] =
		uint64(in[24]-minv)>>40 |
			uint64(in[25]-minv)<<9 |
			uint64(in[26]-minv)<<58

	out[20] =
		uint64(in[26]-minv)>>6 |
			uint64(in[27]-minv)<<43

	out[21] =
		uint64(in[27]-minv)>>21 |
			uint64(in[28]-minv)<<28

	out[22] =
		uint64(in[28]-minv)>>36 |
			uint64(in[29]-minv)<<13 |
			uint64(in[30]-minv)<<62

	out[23] =
		uint64(in[30]-minv)>>2 |
			uint64(in[31]-minv)<<47

	out[24] =
		uint64(in[31]-minv)>>17 |
			uint64(in[32]-minv)<<32

	out[25] =
		uint64(in[32]-minv)>>32 |
			uint64(in[33]-minv)<<17

	out[26] =
		uint64(in[33]-minv)>>47 |
			uint64(in[34]-minv)<<2 |
			uint64(in[35]-minv)<<51

	out[27] =
		uint64(in[35]-minv)>>13 |
			uint64(in[36]-minv)<<36

	out[28] =
		uint64(in[36]-minv)>>28 |
			uint64(in[37]-minv)<<21

	out[29] =
		uint64(in[37]-minv)>>43 |
			uint64(in[38]-minv)<<6 |
			uint64(in[39]-minv)<<55

	out[30] =
		uint64(in[39]-minv)>>9 |
			uint64(in[40]-minv)<<40

	out[31] =
		uint64(in[40]-minv)>>24 |
			uint64(in[41]-minv)<<25

	out[32] =
		uint64(in[41]-minv)>>39 |
			uint64(in[42]-minv)<<10 |
			uint64(in[43]-minv)<<59

	out[33] =
		uint64(in[43]-minv)>>5 |
			uint64(in[44]-minv)<<44

	out[34] =
		uint64(in[44]-minv)>>20 |
			uint64(in[45]-minv)<<29

	out[35] =
		uint64(in[45]-minv)>>35 |
			uint64(in[46]-minv)<<14 |
			uint64(in[47]-minv)<<63

	out[36] =
		uint64(in[47]-minv)>>1 |
			uint64(in[48]-minv)<<48

	out[37] =
		uint64(in[48]-minv)>>16 |
			uint64(in[49]-minv)<<33

	out[38] =
		uint64(in[49]-minv)>>31 |
			uint64(in[50]-minv)<<18

	out[39] =
		uint64(in[50]-minv)>>46 |
			uint64(in[51]-minv)<<3 |
			uint64(in[52]-minv)<<52

	out[40] =
		uint64(in[52]-minv)>>12 |
			uint64(in[53]-minv)<<37

	out[41] =
		uint64(in[53]-minv)>>27 |
			uint64(in[54]-minv)<<22

	out[42] =
		uint64(in[54]-minv)>>42 |
			uint64(in[55]-minv)<<7 |
			uint64(in[56]-minv)<<56

	out[43] =
		uint64(in[56]-minv)>>8 |
			uint64(in[57]-minv)<<41

	out[44] =
		uint64(in[57]-minv)>>23 |
			uint64(in[58]-minv)<<26

	out[45] =
		uint64(in[58]-minv)>>38 |
			uint64(in[59]-minv)<<11 |
			uint64(in[60]-minv)<<60

	out[46] =
		uint64(in[60]-minv)>>4 |
			uint64(in[61]-minv)<<45

	out[47] =
		uint64(in[61]-minv)>>19 |
			uint64(in[62]-minv)<<30

	out[48] =
		uint64(in[62]-minv)>>34 |
			uint64(in[63]-minv)<<15
}

func bp64_50(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[50]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<50

	out[1] =
		uint64(in[1]-minv)>>14 |
			uint64(in[2]-minv)<<36

	out[2] =
		uint64(in[2]-minv)>>28 |
			uint64(in[3]-minv)<<22

	out[3] =
		uint64(in[3]-minv)>>42 |
			uint64(in[4]-minv)<<8 |
			uint64(in[5]-minv)<<58

	out[4] =
		uint64(in[5]-minv)>>6 |
			uint64(in[6]-minv)<<44

	out[5] =
		uint64(in[6]-minv)>>20 |
			uint64(in[7]-minv)<<30

	out[6] =
		uint64(in[7]-minv)>>34 |
			uint64(in[8]-minv)<<16

	out[7] =
		uint64(in[8]-minv)>>48 |
			uint64(in[9]-minv)<<2 |
			uint64(in[10]-minv)<<52

	out[8] =
		uint64(in[10]-minv)>>12 |
			uint64(in[11]-minv)<<38

	out[9] =
		uint64(in[11]-minv)>>26 |
			uint64(in[12]-minv)<<24

	out[10] =
		uint64(in[12]-minv)>>40 |
			uint64(in[13]-minv)<<10 |
			uint64(in[14]-minv)<<60

	out[11] =
		uint64(in[14]-minv)>>4 |
			uint64(in[15]-minv)<<46

	out[12] =
		uint64(in[15]-minv)>>18 |
			uint64(in[16]-minv)<<32

	out[13] =
		uint64(in[16]-minv)>>32 |
			uint64(in[17]-minv)<<18

	out[14] =
		uint64(in[17]-minv)>>46 |
			uint64(in[18]-minv)<<4 |
			uint64(in[19]-minv)<<54

	out[15] =
		uint64(in[19]-minv)>>10 |
			uint64(in[20]-minv)<<40

	out[16] =
		uint64(in[20]-minv)>>24 |
			uint64(in[21]-minv)<<26

	out[17] =
		uint64(in[21]-minv)>>38 |
			uint64(in[22]-minv)<<12 |
			uint64(in[23]-minv)<<62

	out[18] =
		uint64(in[23]-minv)>>2 |
			uint64(in[24]-minv)<<48

	out[19] =
		uint64(in[24]-minv)>>16 |
			uint64(in[25]-minv)<<34

	out[20] =
		uint64(in[25]-minv)>>30 |
			uint64(in[26]-minv)<<20

	out[21] =
		uint64(in[26]-minv)>>44 |
			uint64(in[27]-minv)<<6 |
			uint64(in[28]-minv)<<56

	out[22] =
		uint64(in[28]-minv)>>8 |
			uint64(in[29]-minv)<<42

	out[23] =
		uint64(in[29]-minv)>>22 |
			uint64(in[30]-minv)<<28

	out[24] =
		uint64(in[30]-minv)>>36 |
			uint64(in[31]-minv)<<14

	out[25] =
		uint64(in[31]-minv)>>50 |
			uint64(in[32]-minv) |
			uint64(in[33]-minv)<<50

	out[26] =
		uint64(in[33]-minv)>>14 |
			uint64(in[34]-minv)<<36

	out[27] =
		uint64(in[34]-minv)>>28 |
			uint64(in[35]-minv)<<22

	out[28] =
		uint64(in[35]-minv)>>42 |
			uint64(in[36]-minv)<<8 |
			uint64(in[37]-minv)<<58

	out[29] =
		uint64(in[37]-minv)>>6 |
			uint64(in[38]-minv)<<44

	out[30] =
		uint64(in[38]-minv)>>20 |
			uint64(in[39]-minv)<<30

	out[31] =
		uint64(in[39]-minv)>>34 |
			uint64(in[40]-minv)<<16

	out[32] =
		uint64(in[40]-minv)>>48 |
			uint64(in[41]-minv)<<2 |
			uint64(in[42]-minv)<<52

	out[33] =
		uint64(in[42]-minv)>>12 |
			uint64(in[43]-minv)<<38

	out[34] =
		uint64(in[43]-minv)>>26 |
			uint64(in[44]-minv)<<24

	out[35] =
		uint64(in[44]-minv)>>40 |
			uint64(in[45]-minv)<<10 |
			uint64(in[46]-minv)<<60

	out[36] =
		uint64(in[46]-minv)>>4 |
			uint64(in[47]-minv)<<46

	out[37] =
		uint64(in[47]-minv)>>18 |
			uint64(in[48]-minv)<<32

	out[38] =
		uint64(in[48]-minv)>>32 |
			uint64(in[49]-minv)<<18

	out[39] =
		uint64(in[49]-minv)>>46 |
			uint64(in[50]-minv)<<4 |
			uint64(in[51]-minv)<<54

	out[40] =
		uint64(in[51]-minv)>>10 |
			uint64(in[52]-minv)<<40

	out[41] =
		uint64(in[52]-minv)>>24 |
			uint64(in[53]-minv)<<26

	out[42] =
		uint64(in[53]-minv)>>38 |
			uint64(in[54]-minv)<<12 |
			uint64(in[55]-minv)<<62

	out[43] =
		uint64(in[55]-minv)>>2 |
			uint64(in[56]-minv)<<48

	out[44] =
		uint64(in[56]-minv)>>16 |
			uint64(in[57]-minv)<<34

	out[45] =
		uint64(in[57]-minv)>>30 |
			uint64(in[58]-minv)<<20

	out[46] =
		uint64(in[58]-minv)>>44 |
			uint64(in[59]-minv)<<6 |
			uint64(in[60]-minv)<<56

	out[47] =
		uint64(in[60]-minv)>>8 |
			uint64(in[61]-minv)<<42

	out[48] =
		uint64(in[61]-minv)>>22 |
			uint64(in[62]-minv)<<28

	out[49] =
		uint64(in[62]-minv)>>36 |
			uint64(in[63]-minv)<<14
}

func bp64_51(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[51]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<51

	out[1] =
		uint64(in[1]-minv)>>13 |
			uint64(in[2]-minv)<<38

	out[2] =
		uint64(in[2]-minv)>>26 |
			uint64(in[3]-minv)<<25

	out[3] =
		uint64(in[3]-minv)>>39 |
			uint64(in[4]-minv)<<12 |
			uint64(in[5]-minv)<<63

	out[4] =
		uint64(in[5]-minv)>>1 |
			uint64(in[6]-minv)<<50

	out[5] =
		uint64(in[6]-minv)>>14 |
			uint64(in[7]-minv)<<37

	out[6] =
		uint64(in[7]-minv)>>27 |
			uint64(in[8]-minv)<<24

	out[7] =
		uint64(in[8]-minv)>>40 |
			uint64(in[9]-minv)<<11 |
			uint64(in[10]-minv)<<62

	out[8] =
		uint64(in[10]-minv)>>2 |
			uint64(in[11]-minv)<<49

	out[9] =
		uint64(in[11]-minv)>>15 |
			uint64(in[12]-minv)<<36

	out[10] =
		uint64(in[12]-minv)>>28 |
			uint64(in[13]-minv)<<23

	out[11] =
		uint64(in[13]-minv)>>41 |
			uint64(in[14]-minv)<<10 |
			uint64(in[15]-minv)<<61

	out[12] =
		uint64(in[15]-minv)>>3 |
			uint64(in[16]-minv)<<48

	out[13] =
		uint64(in[16]-minv)>>16 |
			uint64(in[17]-minv)<<35

	out[14] =
		uint64(in[17]-minv)>>29 |
			uint64(in[18]-minv)<<22

	out[15] =
		uint64(in[18]-minv)>>42 |
			uint64(in[19]-minv)<<9 |
			uint64(in[20]-minv)<<60

	out[16] =
		uint64(in[20]-minv)>>4 |
			uint64(in[21]-minv)<<47

	out[17] =
		uint64(in[21]-minv)>>17 |
			uint64(in[22]-minv)<<34

	out[18] =
		uint64(in[22]-minv)>>30 |
			uint64(in[23]-minv)<<21

	out[19] =
		uint64(in[23]-minv)>>43 |
			uint64(in[24]-minv)<<8 |
			uint64(in[25]-minv)<<59

	out[20] =
		uint64(in[25]-minv)>>5 |
			uint64(in[26]-minv)<<46

	out[21] =
		uint64(in[26]-minv)>>18 |
			uint64(in[27]-minv)<<33

	out[22] =
		uint64(in[27]-minv)>>31 |
			uint64(in[28]-minv)<<20

	out[23] =
		uint64(in[28]-minv)>>44 |
			uint64(in[29]-minv)<<7 |
			uint64(in[30]-minv)<<58

	out[24] =
		uint64(in[30]-minv)>>6 |
			uint64(in[31]-minv)<<45

	out[25] =
		uint64(in[31]-minv)>>19 |
			uint64(in[32]-minv)<<32

	out[26] =
		uint64(in[32]-minv)>>32 |
			uint64(in[33]-minv)<<19

	out[27] =
		uint64(in[33]-minv)>>45 |
			uint64(in[34]-minv)<<6 |
			uint64(in[35]-minv)<<57

	out[28] =
		uint64(in[35]-minv)>>7 |
			uint64(in[36]-minv)<<44

	out[29] =
		uint64(in[36]-minv)>>20 |
			uint64(in[37]-minv)<<31

	out[30] =
		uint64(in[37]-minv)>>33 |
			uint64(in[38]-minv)<<18

	out[31] =
		uint64(in[38]-minv)>>46 |
			uint64(in[39]-minv)<<5 |
			uint64(in[40]-minv)<<56

	out[32] =
		uint64(in[40]-minv)>>8 |
			uint64(in[41]-minv)<<43

	out[33] =
		uint64(in[41]-minv)>>21 |
			uint64(in[42]-minv)<<30

	out[34] =
		uint64(in[42]-minv)>>34 |
			uint64(in[43]-minv)<<17

	out[35] =
		uint64(in[43]-minv)>>47 |
			uint64(in[44]-minv)<<4 |
			uint64(in[45]-minv)<<55

	out[36] =
		uint64(in[45]-minv)>>9 |
			uint64(in[46]-minv)<<42

	out[37] =
		uint64(in[46]-minv)>>22 |
			uint64(in[47]-minv)<<29

	out[38] =
		uint64(in[47]-minv)>>35 |
			uint64(in[48]-minv)<<16

	out[39] =
		uint64(in[48]-minv)>>48 |
			uint64(in[49]-minv)<<3 |
			uint64(in[50]-minv)<<54

	out[40] =
		uint64(in[50]-minv)>>10 |
			uint64(in[51]-minv)<<41

	out[41] =
		uint64(in[51]-minv)>>23 |
			uint64(in[52]-minv)<<28

	out[42] =
		uint64(in[52]-minv)>>36 |
			uint64(in[53]-minv)<<15

	out[43] =
		uint64(in[53]-minv)>>49 |
			uint64(in[54]-minv)<<2 |
			uint64(in[55]-minv)<<53

	out[44] =
		uint64(in[55]-minv)>>11 |
			uint64(in[56]-minv)<<40

	out[45] =
		uint64(in[56]-minv)>>24 |
			uint64(in[57]-minv)<<27

	out[46] =
		uint64(in[57]-minv)>>37 |
			uint64(in[58]-minv)<<14

	out[47] =
		uint64(in[58]-minv)>>50 |
			uint64(in[59]-minv)<<1 |
			uint64(in[60]-minv)<<52

	out[48] =
		uint64(in[60]-minv)>>12 |
			uint64(in[61]-minv)<<39

	out[49] =
		uint64(in[61]-minv)>>25 |
			uint64(in[62]-minv)<<26

	out[50] =
		uint64(in[62]-minv)>>38 |
			uint64(in[63]-minv)<<13
}

func bp64_52(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[52]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<52

	out[1] =
		uint64(in[1]-minv)>>12 |
			uint64(in[2]-minv)<<40

	out[2] =
		uint64(in[2]-minv)>>24 |
			uint64(in[3]-minv)<<28

	out[3] =
		uint64(in[3]-minv)>>36 |
			uint64(in[4]-minv)<<16

	out[4] =
		uint64(in[4]-minv)>>48 |
			uint64(in[5]-minv)<<4 |
			uint64(in[6]-minv)<<56

	out[5] =
		uint64(in[6]-minv)>>8 |
			uint64(in[7]-minv)<<44

	out[6] =
		uint64(in[7]-minv)>>20 |
			uint64(in[8]-minv)<<32

	out[7] =
		uint64(in[8]-minv)>>32 |
			uint64(in[9]-minv)<<20

	out[8] =
		uint64(in[9]-minv)>>44 |
			uint64(in[10]-minv)<<8 |
			uint64(in[11]-minv)<<60

	out[9] =
		uint64(in[11]-minv)>>4 |
			uint64(in[12]-minv)<<48

	out[10] =
		uint64(in[12]-minv)>>16 |
			uint64(in[13]-minv)<<36

	out[11] =
		uint64(in[13]-minv)>>28 |
			uint64(in[14]-minv)<<24

	out[12] =
		uint64(in[14]-minv)>>40 |
			uint64(in[15]-minv)<<12

	out[13] =
		uint64(in[15]-minv)>>52 |
			uint64(in[16]-minv) |
			uint64(in[17]-minv)<<52

	out[14] =
		uint64(in[17]-minv)>>12 |
			uint64(in[18]-minv)<<40

	out[15] =
		uint64(in[18]-minv)>>24 |
			uint64(in[19]-minv)<<28

	out[16] =
		uint64(in[19]-minv)>>36 |
			uint64(in[20]-minv)<<16

	out[17] =
		uint64(in[20]-minv)>>48 |
			uint64(in[21]-minv)<<4 |
			uint64(in[22]-minv)<<56

	out[18] =
		uint64(in[22]-minv)>>8 |
			uint64(in[23]-minv)<<44

	out[19] =
		uint64(in[23]-minv)>>20 |
			uint64(in[24]-minv)<<32

	out[20] =
		uint64(in[24]-minv)>>32 |
			uint64(in[25]-minv)<<20

	out[21] =
		uint64(in[25]-minv)>>44 |
			uint64(in[26]-minv)<<8 |
			uint64(in[27]-minv)<<60

	out[22] =
		uint64(in[27]-minv)>>4 |
			uint64(in[28]-minv)<<48

	out[23] =
		uint64(in[28]-minv)>>16 |
			uint64(in[29]-minv)<<36

	out[24] =
		uint64(in[29]-minv)>>28 |
			uint64(in[30]-minv)<<24

	out[25] =
		uint64(in[30]-minv)>>40 |
			uint64(in[31]-minv)<<12

	out[26] =
		uint64(in[31]-minv)>>52 |
			uint64(in[32]-minv) |
			uint64(in[33]-minv)<<52

	out[27] =
		uint64(in[33]-minv)>>12 |
			uint64(in[34]-minv)<<40

	out[28] =
		uint64(in[34]-minv)>>24 |
			uint64(in[35]-minv)<<28

	out[29] =
		uint64(in[35]-minv)>>36 |
			uint64(in[36]-minv)<<16

	out[30] =
		uint64(in[36]-minv)>>48 |
			uint64(in[37]-minv)<<4 |
			uint64(in[38]-minv)<<56

	out[31] =
		uint64(in[38]-minv)>>8 |
			uint64(in[39]-minv)<<44

	out[32] =
		uint64(in[39]-minv)>>20 |
			uint64(in[40]-minv)<<32

	out[33] =
		uint64(in[40]-minv)>>32 |
			uint64(in[41]-minv)<<20

	out[34] =
		uint64(in[41]-minv)>>44 |
			uint64(in[42]-minv)<<8 |
			uint64(in[43]-minv)<<60

	out[35] =
		uint64(in[43]-minv)>>4 |
			uint64(in[44]-minv)<<48

	out[36] =
		uint64(in[44]-minv)>>16 |
			uint64(in[45]-minv)<<36

	out[37] =
		uint64(in[45]-minv)>>28 |
			uint64(in[46]-minv)<<24

	out[38] =
		uint64(in[46]-minv)>>40 |
			uint64(in[47]-minv)<<12

	out[39] =
		uint64(in[47]-minv)>>52 |
			uint64(in[48]-minv) |
			uint64(in[49]-minv)<<52

	out[40] =
		uint64(in[49]-minv)>>12 |
			uint64(in[50]-minv)<<40

	out[41] =
		uint64(in[50]-minv)>>24 |
			uint64(in[51]-minv)<<28

	out[42] =
		uint64(in[51]-minv)>>36 |
			uint64(in[52]-minv)<<16

	out[43] =
		uint64(in[52]-minv)>>48 |
			uint64(in[53]-minv)<<4 |
			uint64(in[54]-minv)<<56

	out[44] =
		uint64(in[54]-minv)>>8 |
			uint64(in[55]-minv)<<44

	out[45] =
		uint64(in[55]-minv)>>20 |
			uint64(in[56]-minv)<<32

	out[46] =
		uint64(in[56]-minv)>>32 |
			uint64(in[57]-minv)<<20

	out[47] =
		uint64(in[57]-minv)>>44 |
			uint64(in[58]-minv)<<8 |
			uint64(in[59]-minv)<<60

	out[48] =
		uint64(in[59]-minv)>>4 |
			uint64(in[60]-minv)<<48

	out[49] =
		uint64(in[60]-minv)>>16 |
			uint64(in[61]-minv)<<36

	out[50] =
		uint64(in[61]-minv)>>28 |
			uint64(in[62]-minv)<<24

	out[51] =
		uint64(in[62]-minv)>>40 |
			uint64(in[63]-minv)<<12
}

func bp64_53(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[53]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<53

	out[1] =
		uint64(in[1]-minv)>>11 |
			uint64(in[2]-minv)<<42

	out[2] =
		uint64(in[2]-minv)>>22 |
			uint64(in[3]-minv)<<31

	out[3] =
		uint64(in[3]-minv)>>33 |
			uint64(in[4]-minv)<<20

	out[4] =
		uint64(in[4]-minv)>>44 |
			uint64(in[5]-minv)<<9 |
			uint64(in[6]-minv)<<62

	out[5] =
		uint64(in[6]-minv)>>2 |
			uint64(in[7]-minv)<<51

	out[6] =
		uint64(in[7]-minv)>>13 |
			uint64(in[8]-minv)<<40

	out[7] =
		uint64(in[8]-minv)>>24 |
			uint64(in[9]-minv)<<29

	out[8] =
		uint64(in[9]-minv)>>35 |
			uint64(in[10]-minv)<<18

	out[9] =
		uint64(in[10]-minv)>>46 |
			uint64(in[11]-minv)<<7 |
			uint64(in[12]-minv)<<60

	out[10] =
		uint64(in[12]-minv)>>4 |
			uint64(in[13]-minv)<<49

	out[11] =
		uint64(in[13]-minv)>>15 |
			uint64(in[14]-minv)<<38

	out[12] =
		uint64(in[14]-minv)>>26 |
			uint64(in[15]-minv)<<27

	out[13] =
		uint64(in[15]-minv)>>37 |
			uint64(in[16]-minv)<<16

	out[14] =
		uint64(in[16]-minv)>>48 |
			uint64(in[17]-minv)<<5 |
			uint64(in[18]-minv)<<58

	out[15] =
		uint64(in[18]-minv)>>6 |
			uint64(in[19]-minv)<<47

	out[16] =
		uint64(in[19]-minv)>>17 |
			uint64(in[20]-minv)<<36

	out[17] =
		uint64(in[20]-minv)>>28 |
			uint64(in[21]-minv)<<25

	out[18] =
		uint64(in[21]-minv)>>39 |
			uint64(in[22]-minv)<<14

	out[19] =
		uint64(in[22]-minv)>>50 |
			uint64(in[23]-minv)<<3 |
			uint64(in[24]-minv)<<56

	out[20] =
		uint64(in[24]-minv)>>8 |
			uint64(in[25]-minv)<<45

	out[21] =
		uint64(in[25]-minv)>>19 |
			uint64(in[26]-minv)<<34

	out[22] =
		uint64(in[26]-minv)>>30 |
			uint64(in[27]-minv)<<23

	out[23] =
		uint64(in[27]-minv)>>41 |
			uint64(in[28]-minv)<<12

	out[24] =
		uint64(in[28]-minv)>>52 |
			uint64(in[29]-minv)<<1 |
			uint64(in[30]-minv)<<54

	out[25] =
		uint64(in[30]-minv)>>10 |
			uint64(in[31]-minv)<<43

	out[26] =
		uint64(in[31]-minv)>>21 |
			uint64(in[32]-minv)<<32

	out[27] =
		uint64(in[32]-minv)>>32 |
			uint64(in[33]-minv)<<21

	out[28] =
		uint64(in[33]-minv)>>43 |
			uint64(in[34]-minv)<<10 |
			uint64(in[35]-minv)<<63

	out[29] =
		uint64(in[35]-minv)>>1 |
			uint64(in[36]-minv)<<52

	out[30] =
		uint64(in[36]-minv)>>12 |
			uint64(in[37]-minv)<<41

	out[31] =
		uint64(in[37]-minv)>>23 |
			uint64(in[38]-minv)<<30

	out[32] =
		uint64(in[38]-minv)>>34 |
			uint64(in[39]-minv)<<19

	out[33] =
		uint64(in[39]-minv)>>45 |
			uint64(in[40]-minv)<<8 |
			uint64(in[41]-minv)<<61

	out[34] =
		uint64(in[41]-minv)>>3 |
			uint64(in[42]-minv)<<50

	out[35] =
		uint64(in[42]-minv)>>14 |
			uint64(in[43]-minv)<<39

	out[36] =
		uint64(in[43]-minv)>>25 |
			uint64(in[44]-minv)<<28

	out[37] =
		uint64(in[44]-minv)>>36 |
			uint64(in[45]-minv)<<17

	out[38] =
		uint64(in[45]-minv)>>47 |
			uint64(in[46]-minv)<<6 |
			uint64(in[47]-minv)<<59

	out[39] =
		uint64(in[47]-minv)>>5 |
			uint64(in[48]-minv)<<48

	out[40] =
		uint64(in[48]-minv)>>16 |
			uint64(in[49]-minv)<<37

	out[41] =
		uint64(in[49]-minv)>>27 |
			uint64(in[50]-minv)<<26

	out[42] =
		uint64(in[50]-minv)>>38 |
			uint64(in[51]-minv)<<15

	out[43] =
		uint64(in[51]-minv)>>49 |
			uint64(in[52]-minv)<<4 |
			uint64(in[53]-minv)<<57

	out[44] =
		uint64(in[53]-minv)>>7 |
			uint64(in[54]-minv)<<46

	out[45] =
		uint64(in[54]-minv)>>18 |
			uint64(in[55]-minv)<<35

	out[46] =
		uint64(in[55]-minv)>>29 |
			uint64(in[56]-minv)<<24

	out[47] =
		uint64(in[56]-minv)>>40 |
			uint64(in[57]-minv)<<13

	out[48] =
		uint64(in[57]-minv)>>51 |
			uint64(in[58]-minv)<<2 |
			uint64(in[59]-minv)<<55

	out[49] =
		uint64(in[59]-minv)>>9 |
			uint64(in[60]-minv)<<44

	out[50] =
		uint64(in[60]-minv)>>20 |
			uint64(in[61]-minv)<<33

	out[51] =
		uint64(in[61]-minv)>>31 |
			uint64(in[62]-minv)<<22

	out[52] =
		uint64(in[62]-minv)>>42 |
			uint64(in[63]-minv)<<11
}

func bp64_54(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[54]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<54

	out[1] =
		uint64(in[1]-minv)>>10 |
			uint64(in[2]-minv)<<44

	out[2] =
		uint64(in[2]-minv)>>20 |
			uint64(in[3]-minv)<<34

	out[3] =
		uint64(in[3]-minv)>>30 |
			uint64(in[4]-minv)<<24

	out[4] =
		uint64(in[4]-minv)>>40 |
			uint64(in[5]-minv)<<14

	out[5] =
		uint64(in[5]-minv)>>50 |
			uint64(in[6]-minv)<<4 |
			uint64(in[7]-minv)<<58

	out[6] =
		uint64(in[7]-minv)>>6 |
			uint64(in[8]-minv)<<48

	out[7] =
		uint64(in[8]-minv)>>16 |
			uint64(in[9]-minv)<<38

	out[8] =
		uint64(in[9]-minv)>>26 |
			uint64(in[10]-minv)<<28

	out[9] =
		uint64(in[10]-minv)>>36 |
			uint64(in[11]-minv)<<18

	out[10] =
		uint64(in[11]-minv)>>46 |
			uint64(in[12]-minv)<<8 |
			uint64(in[13]-minv)<<62

	out[11] =
		uint64(in[13]-minv)>>2 |
			uint64(in[14]-minv)<<52

	out[12] =
		uint64(in[14]-minv)>>12 |
			uint64(in[15]-minv)<<42

	out[13] =
		uint64(in[15]-minv)>>22 |
			uint64(in[16]-minv)<<32

	out[14] =
		uint64(in[16]-minv)>>32 |
			uint64(in[17]-minv)<<22

	out[15] =
		uint64(in[17]-minv)>>42 |
			uint64(in[18]-minv)<<12

	out[16] =
		uint64(in[18]-minv)>>52 |
			uint64(in[19]-minv)<<2 |
			uint64(in[20]-minv)<<56

	out[17] =
		uint64(in[20]-minv)>>8 |
			uint64(in[21]-minv)<<46

	out[18] =
		uint64(in[21]-minv)>>18 |
			uint64(in[22]-minv)<<36

	out[19] =
		uint64(in[22]-minv)>>28 |
			uint64(in[23]-minv)<<26

	out[20] =
		uint64(in[23]-minv)>>38 |
			uint64(in[24]-minv)<<16

	out[21] =
		uint64(in[24]-minv)>>48 |
			uint64(in[25]-minv)<<6 |
			uint64(in[26]-minv)<<60

	out[22] =
		uint64(in[26]-minv)>>4 |
			uint64(in[27]-minv)<<50

	out[23] =
		uint64(in[27]-minv)>>14 |
			uint64(in[28]-minv)<<40

	out[24] =
		uint64(in[28]-minv)>>24 |
			uint64(in[29]-minv)<<30

	out[25] =
		uint64(in[29]-minv)>>34 |
			uint64(in[30]-minv)<<20

	out[26] =
		uint64(in[30]-minv)>>44 |
			uint64(in[31]-minv)<<10

	out[27] =
		uint64(in[31]-minv)>>54 |
			uint64(in[32]-minv) |
			uint64(in[33]-minv)<<54

	out[28] =
		uint64(in[33]-minv)>>10 |
			uint64(in[34]-minv)<<44

	out[29] =
		uint64(in[34]-minv)>>20 |
			uint64(in[35]-minv)<<34

	out[30] =
		uint64(in[35]-minv)>>30 |
			uint64(in[36]-minv)<<24

	out[31] =
		uint64(in[36]-minv)>>40 |
			uint64(in[37]-minv)<<14

	out[32] =
		uint64(in[37]-minv)>>50 |
			uint64(in[38]-minv)<<4 |
			uint64(in[39]-minv)<<58

	out[33] =
		uint64(in[39]-minv)>>6 |
			uint64(in[40]-minv)<<48

	out[34] =
		uint64(in[40]-minv)>>16 |
			uint64(in[41]-minv)<<38

	out[35] =
		uint64(in[41]-minv)>>26 |
			uint64(in[42]-minv)<<28

	out[36] =
		uint64(in[42]-minv)>>36 |
			uint64(in[43]-minv)<<18

	out[37] =
		uint64(in[43]-minv)>>46 |
			uint64(in[44]-minv)<<8 |
			uint64(in[45]-minv)<<62

	out[38] =
		uint64(in[45]-minv)>>2 |
			uint64(in[46]-minv)<<52

	out[39] =
		uint64(in[46]-minv)>>12 |
			uint64(in[47]-minv)<<42

	out[40] =
		uint64(in[47]-minv)>>22 |
			uint64(in[48]-minv)<<32

	out[41] =
		uint64(in[48]-minv)>>32 |
			uint64(in[49]-minv)<<22

	out[42] =
		uint64(in[49]-minv)>>42 |
			uint64(in[50]-minv)<<12

	out[43] =
		uint64(in[50]-minv)>>52 |
			uint64(in[51]-minv)<<2 |
			uint64(in[52]-minv)<<56

	out[44] =
		uint64(in[52]-minv)>>8 |
			uint64(in[53]-minv)<<46

	out[45] =
		uint64(in[53]-minv)>>18 |
			uint64(in[54]-minv)<<36

	out[46] =
		uint64(in[54]-minv)>>28 |
			uint64(in[55]-minv)<<26

	out[47] =
		uint64(in[55]-minv)>>38 |
			uint64(in[56]-minv)<<16

	out[48] =
		uint64(in[56]-minv)>>48 |
			uint64(in[57]-minv)<<6 |
			uint64(in[58]-minv)<<60

	out[49] =
		uint64(in[58]-minv)>>4 |
			uint64(in[59]-minv)<<50

	out[50] =
		uint64(in[59]-minv)>>14 |
			uint64(in[60]-minv)<<40

	out[51] =
		uint64(in[60]-minv)>>24 |
			uint64(in[61]-minv)<<30

	out[52] =
		uint64(in[61]-minv)>>34 |
			uint64(in[62]-minv)<<20

	out[53] =
		uint64(in[62]-minv)>>44 |
			uint64(in[63]-minv)<<10
}

func bp64_55(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[55]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<55

	out[1] =
		uint64(in[1]-minv)>>9 |
			uint64(in[2]-minv)<<46

	out[2] =
		uint64(in[2]-minv)>>18 |
			uint64(in[3]-minv)<<37

	out[3] =
		uint64(in[3]-minv)>>27 |
			uint64(in[4]-minv)<<28

	out[4] =
		uint64(in[4]-minv)>>36 |
			uint64(in[5]-minv)<<19

	out[5] =
		uint64(in[5]-minv)>>45 |
			uint64(in[6]-minv)<<10

	out[6] =
		uint64(in[6]-minv)>>54 |
			uint64(in[7]-minv)<<1 |
			uint64(in[8]-minv)<<56

	out[7] =
		uint64(in[8]-minv)>>8 |
			uint64(in[9]-minv)<<47

	out[8] =
		uint64(in[9]-minv)>>17 |
			uint64(in[10]-minv)<<38

	out[9] =
		uint64(in[10]-minv)>>26 |
			uint64(in[11]-minv)<<29

	out[10] =
		uint64(in[11]-minv)>>35 |
			uint64(in[12]-minv)<<20

	out[11] =
		uint64(in[12]-minv)>>44 |
			uint64(in[13]-minv)<<11

	out[12] =
		uint64(in[13]-minv)>>53 |
			uint64(in[14]-minv)<<2 |
			uint64(in[15]-minv)<<57

	out[13] =
		uint64(in[15]-minv)>>7 |
			uint64(in[16]-minv)<<48

	out[14] =
		uint64(in[16]-minv)>>16 |
			uint64(in[17]-minv)<<39

	out[15] =
		uint64(in[17]-minv)>>25 |
			uint64(in[18]-minv)<<30

	out[16] =
		uint64(in[18]-minv)>>34 |
			uint64(in[19]-minv)<<21

	out[17] =
		uint64(in[19]-minv)>>43 |
			uint64(in[20]-minv)<<12

	out[18] =
		uint64(in[20]-minv)>>52 |
			uint64(in[21]-minv)<<3 |
			uint64(in[22]-minv)<<58

	out[19] =
		uint64(in[22]-minv)>>6 |
			uint64(in[23]-minv)<<49

	out[20] =
		uint64(in[23]-minv)>>15 |
			uint64(in[24]-minv)<<40

	out[21] =
		uint64(in[24]-minv)>>24 |
			uint64(in[25]-minv)<<31

	out[22] =
		uint64(in[25]-minv)>>33 |
			uint64(in[26]-minv)<<22

	out[23] =
		uint64(in[26]-minv)>>42 |
			uint64(in[27]-minv)<<13

	out[24] =
		uint64(in[27]-minv)>>51 |
			uint64(in[28]-minv)<<4 |
			uint64(in[29]-minv)<<59

	out[25] =
		uint64(in[29]-minv)>>5 |
			uint64(in[30]-minv)<<50

	out[26] =
		uint64(in[30]-minv)>>14 |
			uint64(in[31]-minv)<<41

	out[27] =
		uint64(in[31]-minv)>>23 |
			uint64(in[32]-minv)<<32

	out[28] =
		uint64(in[32]-minv)>>32 |
			uint64(in[33]-minv)<<23

	out[29] =
		uint64(in[33]-minv)>>41 |
			uint64(in[34]-minv)<<14

	out[30] =
		uint64(in[34]-minv)>>50 |
			uint64(in[35]-minv)<<5 |
			uint64(in[36]-minv)<<60

	out[31] =
		uint64(in[36]-minv)>>4 |
			uint64(in[37]-minv)<<51

	out[32] =
		uint64(in[37]-minv)>>13 |
			uint64(in[38]-minv)<<42

	out[33] =
		uint64(in[38]-minv)>>22 |
			uint64(in[39]-minv)<<33

	out[34] =
		uint64(in[39]-minv)>>31 |
			uint64(in[40]-minv)<<24

	out[35] =
		uint64(in[40]-minv)>>40 |
			uint64(in[41]-minv)<<15

	out[36] =
		uint64(in[41]-minv)>>49 |
			uint64(in[42]-minv)<<6 |
			uint64(in[43]-minv)<<61

	out[37] =
		uint64(in[43]-minv)>>3 |
			uint64(in[44]-minv)<<52

	out[38] =
		uint64(in[44]-minv)>>12 |
			uint64(in[45]-minv)<<43

	out[39] =
		uint64(in[45]-minv)>>21 |
			uint64(in[46]-minv)<<34

	out[40] =
		uint64(in[46]-minv)>>30 |
			uint64(in[47]-minv)<<25

	out[41] =
		uint64(in[47]-minv)>>39 |
			uint64(in[48]-minv)<<16

	out[42] =
		uint64(in[48]-minv)>>48 |
			uint64(in[49]-minv)<<7 |
			uint64(in[50]-minv)<<62

	out[43] =
		uint64(in[50]-minv)>>2 |
			uint64(in[51]-minv)<<53

	out[44] =
		uint64(in[51]-minv)>>11 |
			uint64(in[52]-minv)<<44

	out[45] =
		uint64(in[52]-minv)>>20 |
			uint64(in[53]-minv)<<35

	out[46] =
		uint64(in[53]-minv)>>29 |
			uint64(in[54]-minv)<<26

	out[47] =
		uint64(in[54]-minv)>>38 |
			uint64(in[55]-minv)<<17

	out[48] =
		uint64(in[55]-minv)>>47 |
			uint64(in[56]-minv)<<8 |
			uint64(in[57]-minv)<<63

	out[49] =
		uint64(in[57]-minv)>>1 |
			uint64(in[58]-minv)<<54

	out[50] =
		uint64(in[58]-minv)>>10 |
			uint64(in[59]-minv)<<45

	out[51] =
		uint64(in[59]-minv)>>19 |
			uint64(in[60]-minv)<<36

	out[52] =
		uint64(in[60]-minv)>>28 |
			uint64(in[61]-minv)<<27

	out[53] =
		uint64(in[61]-minv)>>37 |
			uint64(in[62]-minv)<<18

	out[54] =
		uint64(in[62]-minv)>>46 |
			uint64(in[63]-minv)<<9
}

func bp64_56(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[56]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<56

	out[1] =
		uint64(in[1]-minv)>>8 |
			uint64(in[2]-minv)<<48

	out[2] =
		uint64(in[2]-minv)>>16 |
			uint64(in[3]-minv)<<40

	out[3] =
		uint64(in[3]-minv)>>24 |
			uint64(in[4]-minv)<<32

	out[4] =
		uint64(in[4]-minv)>>32 |
			uint64(in[5]-minv)<<24

	out[5] =
		uint64(in[5]-minv)>>40 |
			uint64(in[6]-minv)<<16

	out[6] =
		uint64(in[6]-minv)>>48 |
			uint64(in[7]-minv)<<8

	out[7] =
		uint64(in[7]-minv)>>56 |
			uint64(in[8]-minv) |
			uint64(in[9]-minv)<<56

	out[8] =
		uint64(in[9]-minv)>>8 |
			uint64(in[10]-minv)<<48

	out[9] =
		uint64(in[10]-minv)>>16 |
			uint64(in[11]-minv)<<40

	out[10] =
		uint64(in[11]-minv)>>24 |
			uint64(in[12]-minv)<<32

	out[11] =
		uint64(in[12]-minv)>>32 |
			uint64(in[13]-minv)<<24

	out[12] =
		uint64(in[13]-minv)>>40 |
			uint64(in[14]-minv)<<16

	out[13] =
		uint64(in[14]-minv)>>48 |
			uint64(in[15]-minv)<<8

	out[14] =
		uint64(in[15]-minv)>>56 |
			uint64(in[16]-minv) |
			uint64(in[17]-minv)<<56

	out[15] =
		uint64(in[17]-minv)>>8 |
			uint64(in[18]-minv)<<48

	out[16] =
		uint64(in[18]-minv)>>16 |
			uint64(in[19]-minv)<<40

	out[17] =
		uint64(in[19]-minv)>>24 |
			uint64(in[20]-minv)<<32

	out[18] =
		uint64(in[20]-minv)>>32 |
			uint64(in[21]-minv)<<24

	out[19] =
		uint64(in[21]-minv)>>40 |
			uint64(in[22]-minv)<<16

	out[20] =
		uint64(in[22]-minv)>>48 |
			uint64(in[23]-minv)<<8

	out[21] =
		uint64(in[23]-minv)>>56 |
			uint64(in[24]-minv) |
			uint64(in[25]-minv)<<56

	out[22] =
		uint64(in[25]-minv)>>8 |
			uint64(in[26]-minv)<<48

	out[23] =
		uint64(in[26]-minv)>>16 |
			uint64(in[27]-minv)<<40

	out[24] =
		uint64(in[27]-minv)>>24 |
			uint64(in[28]-minv)<<32

	out[25] =
		uint64(in[28]-minv)>>32 |
			uint64(in[29]-minv)<<24

	out[26] =
		uint64(in[29]-minv)>>40 |
			uint64(in[30]-minv)<<16

	out[27] =
		uint64(in[30]-minv)>>48 |
			uint64(in[31]-minv)<<8

	out[28] =
		uint64(in[31]-minv)>>56 |
			uint64(in[32]-minv) |
			uint64(in[33]-minv)<<56

	out[29] =
		uint64(in[33]-minv)>>8 |
			uint64(in[34]-minv)<<48

	out[30] =
		uint64(in[34]-minv)>>16 |
			uint64(in[35]-minv)<<40

	out[31] =
		uint64(in[35]-minv)>>24 |
			uint64(in[36]-minv)<<32

	out[32] =
		uint64(in[36]-minv)>>32 |
			uint64(in[37]-minv)<<24

	out[33] =
		uint64(in[37]-minv)>>40 |
			uint64(in[38]-minv)<<16

	out[34] =
		uint64(in[38]-minv)>>48 |
			uint64(in[39]-minv)<<8

	out[35] =
		uint64(in[39]-minv)>>56 |
			uint64(in[40]-minv) |
			uint64(in[41]-minv)<<56

	out[36] =
		uint64(in[41]-minv)>>8 |
			uint64(in[42]-minv)<<48

	out[37] =
		uint64(in[42]-minv)>>16 |
			uint64(in[43]-minv)<<40

	out[38] =
		uint64(in[43]-minv)>>24 |
			uint64(in[44]-minv)<<32

	out[39] =
		uint64(in[44]-minv)>>32 |
			uint64(in[45]-minv)<<24

	out[40] =
		uint64(in[45]-minv)>>40 |
			uint64(in[46]-minv)<<16

	out[41] =
		uint64(in[46]-minv)>>48 |
			uint64(in[47]-minv)<<8

	out[42] =
		uint64(in[47]-minv)>>56 |
			uint64(in[48]-minv) |
			uint64(in[49]-minv)<<56

	out[43] =
		uint64(in[49]-minv)>>8 |
			uint64(in[50]-minv)<<48

	out[44] =
		uint64(in[50]-minv)>>16 |
			uint64(in[51]-minv)<<40

	out[45] =
		uint64(in[51]-minv)>>24 |
			uint64(in[52]-minv)<<32

	out[46] =
		uint64(in[52]-minv)>>32 |
			uint64(in[53]-minv)<<24

	out[47] =
		uint64(in[53]-minv)>>40 |
			uint64(in[54]-minv)<<16

	out[48] =
		uint64(in[54]-minv)>>48 |
			uint64(in[55]-minv)<<8

	out[49] =
		uint64(in[55]-minv)>>56 |
			uint64(in[56]-minv) |
			uint64(in[57]-minv)<<56

	out[50] =
		uint64(in[57]-minv)>>8 |
			uint64(in[58]-minv)<<48

	out[51] =
		uint64(in[58]-minv)>>16 |
			uint64(in[59]-minv)<<40

	out[52] =
		uint64(in[59]-minv)>>24 |
			uint64(in[60]-minv)<<32

	out[53] =
		uint64(in[60]-minv)>>32 |
			uint64(in[61]-minv)<<24

	out[54] =
		uint64(in[61]-minv)>>40 |
			uint64(in[62]-minv)<<16

	out[55] =
		uint64(in[62]-minv)>>48 |
			uint64(in[63]-minv)<<8
}

func bp64_57(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[57]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<57

	out[1] =
		uint64(in[1]-minv)>>7 |
			uint64(in[2]-minv)<<50

	out[2] =
		uint64(in[2]-minv)>>14 |
			uint64(in[3]-minv)<<43

	out[3] =
		uint64(in[3]-minv)>>21 |
			uint64(in[4]-minv)<<36

	out[4] =
		uint64(in[4]-minv)>>28 |
			uint64(in[5]-minv)<<29

	out[5] =
		uint64(in[5]-minv)>>35 |
			uint64(in[6]-minv)<<22

	out[6] =
		uint64(in[6]-minv)>>42 |
			uint64(in[7]-minv)<<15

	out[7] =
		uint64(in[7]-minv)>>49 |
			uint64(in[8]-minv)<<8

	out[8] =
		uint64(in[8]-minv)>>56 |
			uint64(in[9]-minv)<<1 |
			uint64(in[10]-minv)<<58

	out[9] =
		uint64(in[10]-minv)>>6 |
			uint64(in[11]-minv)<<51

	out[10] =
		uint64(in[11]-minv)>>13 |
			uint64(in[12]-minv)<<44

	out[11] =
		uint64(in[12]-minv)>>20 |
			uint64(in[13]-minv)<<37

	out[12] =
		uint64(in[13]-minv)>>27 |
			uint64(in[14]-minv)<<30

	out[13] =
		uint64(in[14]-minv)>>34 |
			uint64(in[15]-minv)<<23

	out[14] =
		uint64(in[15]-minv)>>41 |
			uint64(in[16]-minv)<<16

	out[15] =
		uint64(in[16]-minv)>>48 |
			uint64(in[17]-minv)<<9

	out[16] =
		uint64(in[17]-minv)>>55 |
			uint64(in[18]-minv)<<2 |
			uint64(in[19]-minv)<<59

	out[17] =
		uint64(in[19]-minv)>>5 |
			uint64(in[20]-minv)<<52

	out[18] =
		uint64(in[20]-minv)>>12 |
			uint64(in[21]-minv)<<45

	out[19] =
		uint64(in[21]-minv)>>19 |
			uint64(in[22]-minv)<<38

	out[20] =
		uint64(in[22]-minv)>>26 |
			uint64(in[23]-minv)<<31

	out[21] =
		uint64(in[23]-minv)>>33 |
			uint64(in[24]-minv)<<24

	out[22] =
		uint64(in[24]-minv)>>40 |
			uint64(in[25]-minv)<<17

	out[23] =
		uint64(in[25]-minv)>>47 |
			uint64(in[26]-minv)<<10

	out[24] =
		uint64(in[26]-minv)>>54 |
			uint64(in[27]-minv)<<3 |
			uint64(in[28]-minv)<<60

	out[25] =
		uint64(in[28]-minv)>>4 |
			uint64(in[29]-minv)<<53

	out[26] =
		uint64(in[29]-minv)>>11 |
			uint64(in[30]-minv)<<46

	out[27] =
		uint64(in[30]-minv)>>18 |
			uint64(in[31]-minv)<<39

	out[28] =
		uint64(in[31]-minv)>>25 |
			uint64(in[32]-minv)<<32

	out[29] =
		uint64(in[32]-minv)>>32 |
			uint64(in[33]-minv)<<25

	out[30] =
		uint64(in[33]-minv)>>39 |
			uint64(in[34]-minv)<<18

	out[31] =
		uint64(in[34]-minv)>>46 |
			uint64(in[35]-minv)<<11

	out[32] =
		uint64(in[35]-minv)>>53 |
			uint64(in[36]-minv)<<4 |
			uint64(in[37]-minv)<<61

	out[33] =
		uint64(in[37]-minv)>>3 |
			uint64(in[38]-minv)<<54

	out[34] =
		uint64(in[38]-minv)>>10 |
			uint64(in[39]-minv)<<47

	out[35] =
		uint64(in[39]-minv)>>17 |
			uint64(in[40]-minv)<<40

	out[36] =
		uint64(in[40]-minv)>>24 |
			uint64(in[41]-minv)<<33

	out[37] =
		uint64(in[41]-minv)>>31 |
			uint64(in[42]-minv)<<26

	out[38] =
		uint64(in[42]-minv)>>38 |
			uint64(in[43]-minv)<<19

	out[39] =
		uint64(in[43]-minv)>>45 |
			uint64(in[44]-minv)<<12

	out[40] =
		uint64(in[44]-minv)>>52 |
			uint64(in[45]-minv)<<5 |
			uint64(in[46]-minv)<<62

	out[41] =
		uint64(in[46]-minv)>>2 |
			uint64(in[47]-minv)<<55

	out[42] =
		uint64(in[47]-minv)>>9 |
			uint64(in[48]-minv)<<48

	out[43] =
		uint64(in[48]-minv)>>16 |
			uint64(in[49]-minv)<<41

	out[44] =
		uint64(in[49]-minv)>>23 |
			uint64(in[50]-minv)<<34

	out[45] =
		uint64(in[50]-minv)>>30 |
			uint64(in[51]-minv)<<27

	out[46] =
		uint64(in[51]-minv)>>37 |
			uint64(in[52]-minv)<<20

	out[47] =
		uint64(in[52]-minv)>>44 |
			uint64(in[53]-minv)<<13

	out[48] =
		uint64(in[53]-minv)>>51 |
			uint64(in[54]-minv)<<6 |
			uint64(in[55]-minv)<<63

	out[49] =
		uint64(in[55]-minv)>>1 |
			uint64(in[56]-minv)<<56

	out[50] =
		uint64(in[56]-minv)>>8 |
			uint64(in[57]-minv)<<49

	out[51] =
		uint64(in[57]-minv)>>15 |
			uint64(in[58]-minv)<<42

	out[52] =
		uint64(in[58]-minv)>>22 |
			uint64(in[59]-minv)<<35

	out[53] =
		uint64(in[59]-minv)>>29 |
			uint64(in[60]-minv)<<28

	out[54] =
		uint64(in[60]-minv)>>36 |
			uint64(in[61]-minv)<<21

	out[55] =
		uint64(in[61]-minv)>>43 |
			uint64(in[62]-minv)<<14

	out[56] =
		uint64(in[62]-minv)>>50 |
			uint64(in[63]-minv)<<7
}

func bp64_58(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[58]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<58

	out[1] =
		uint64(in[1]-minv)>>6 |
			uint64(in[2]-minv)<<52

	out[2] =
		uint64(in[2]-minv)>>12 |
			uint64(in[3]-minv)<<46

	out[3] =
		uint64(in[3]-minv)>>18 |
			uint64(in[4]-minv)<<40

	out[4] =
		uint64(in[4]-minv)>>24 |
			uint64(in[5]-minv)<<34

	out[5] =
		uint64(in[5]-minv)>>30 |
			uint64(in[6]-minv)<<28

	out[6] =
		uint64(in[6]-minv)>>36 |
			uint64(in[7]-minv)<<22

	out[7] =
		uint64(in[7]-minv)>>42 |
			uint64(in[8]-minv)<<16

	out[8] =
		uint64(in[8]-minv)>>48 |
			uint64(in[9]-minv)<<10

	out[9] =
		uint64(in[9]-minv)>>54 |
			uint64(in[10]-minv)<<4 |
			uint64(in[11]-minv)<<62

	out[10] =
		uint64(in[11]-minv)>>2 |
			uint64(in[12]-minv)<<56

	out[11] =
		uint64(in[12]-minv)>>8 |
			uint64(in[13]-minv)<<50

	out[12] =
		uint64(in[13]-minv)>>14 |
			uint64(in[14]-minv)<<44

	out[13] =
		uint64(in[14]-minv)>>20 |
			uint64(in[15]-minv)<<38

	out[14] =
		uint64(in[15]-minv)>>26 |
			uint64(in[16]-minv)<<32

	out[15] =
		uint64(in[16]-minv)>>32 |
			uint64(in[17]-minv)<<26

	out[16] =
		uint64(in[17]-minv)>>38 |
			uint64(in[18]-minv)<<20

	out[17] =
		uint64(in[18]-minv)>>44 |
			uint64(in[19]-minv)<<14

	out[18] =
		uint64(in[19]-minv)>>50 |
			uint64(in[20]-minv)<<8

	out[19] =
		uint64(in[20]-minv)>>56 |
			uint64(in[21]-minv)<<2 |
			uint64(in[22]-minv)<<60

	out[20] =
		uint64(in[22]-minv)>>4 |
			uint64(in[23]-minv)<<54

	out[21] =
		uint64(in[23]-minv)>>10 |
			uint64(in[24]-minv)<<48

	out[22] =
		uint64(in[24]-minv)>>16 |
			uint64(in[25]-minv)<<42

	out[23] =
		uint64(in[25]-minv)>>22 |
			uint64(in[26]-minv)<<36

	out[24] =
		uint64(in[26]-minv)>>28 |
			uint64(in[27]-minv)<<30

	out[25] =
		uint64(in[27]-minv)>>34 |
			uint64(in[28]-minv)<<24

	out[26] =
		uint64(in[28]-minv)>>40 |
			uint64(in[29]-minv)<<18

	out[27] =
		uint64(in[29]-minv)>>46 |
			uint64(in[30]-minv)<<12

	out[28] =
		uint64(in[30]-minv)>>52 |
			uint64(in[31]-minv)<<6

	out[29] =
		uint64(in[31]-minv)>>58 |
			uint64(in[32]-minv) |
			uint64(in[33]-minv)<<58

	out[30] =
		uint64(in[33]-minv)>>6 |
			uint64(in[34]-minv)<<52

	out[31] =
		uint64(in[34]-minv)>>12 |
			uint64(in[35]-minv)<<46

	out[32] =
		uint64(in[35]-minv)>>18 |
			uint64(in[36]-minv)<<40

	out[33] =
		uint64(in[36]-minv)>>24 |
			uint64(in[37]-minv)<<34

	out[34] =
		uint64(in[37]-minv)>>30 |
			uint64(in[38]-minv)<<28

	out[35] =
		uint64(in[38]-minv)>>36 |
			uint64(in[39]-minv)<<22

	out[36] =
		uint64(in[39]-minv)>>42 |
			uint64(in[40]-minv)<<16

	out[37] =
		uint64(in[40]-minv)>>48 |
			uint64(in[41]-minv)<<10

	out[38] =
		uint64(in[41]-minv)>>54 |
			uint64(in[42]-minv)<<4 |
			uint64(in[43]-minv)<<62

	out[39] =
		uint64(in[43]-minv)>>2 |
			uint64(in[44]-minv)<<56

	out[40] =
		uint64(in[44]-minv)>>8 |
			uint64(in[45]-minv)<<50

	out[41] =
		uint64(in[45]-minv)>>14 |
			uint64(in[46]-minv)<<44

	out[42] =
		uint64(in[46]-minv)>>20 |
			uint64(in[47]-minv)<<38

	out[43] =
		uint64(in[47]-minv)>>26 |
			uint64(in[48]-minv)<<32

	out[44] =
		uint64(in[48]-minv)>>32 |
			uint64(in[49]-minv)<<26

	out[45] =
		uint64(in[49]-minv)>>38 |
			uint64(in[50]-minv)<<20

	out[46] =
		uint64(in[50]-minv)>>44 |
			uint64(in[51]-minv)<<14

	out[47] =
		uint64(in[51]-minv)>>50 |
			uint64(in[52]-minv)<<8

	out[48] =
		uint64(in[52]-minv)>>56 |
			uint64(in[53]-minv)<<2 |
			uint64(in[54]-minv)<<60

	out[49] =
		uint64(in[54]-minv)>>4 |
			uint64(in[55]-minv)<<54

	out[50] =
		uint64(in[55]-minv)>>10 |
			uint64(in[56]-minv)<<48

	out[51] =
		uint64(in[56]-minv)>>16 |
			uint64(in[57]-minv)<<42

	out[52] =
		uint64(in[57]-minv)>>22 |
			uint64(in[58]-minv)<<36

	out[53] =
		uint64(in[58]-minv)>>28 |
			uint64(in[59]-minv)<<30

	out[54] =
		uint64(in[59]-minv)>>34 |
			uint64(in[60]-minv)<<24

	out[55] =
		uint64(in[60]-minv)>>40 |
			uint64(in[61]-minv)<<18

	out[56] =
		uint64(in[61]-minv)>>46 |
			uint64(in[62]-minv)<<12

	out[57] =
		uint64(in[62]-minv)>>52 |
			uint64(in[63]-minv)<<6
}

func bp64_59(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[59]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<59

	out[1] =
		uint64(in[1]-minv)>>5 |
			uint64(in[2]-minv)<<54

	out[2] =
		uint64(in[2]-minv)>>10 |
			uint64(in[3]-minv)<<49

	out[3] =
		uint64(in[3]-minv)>>15 |
			uint64(in[4]-minv)<<44

	out[4] =
		uint64(in[4]-minv)>>20 |
			uint64(in[5]-minv)<<39

	out[5] =
		uint64(in[5]-minv)>>25 |
			uint64(in[6]-minv)<<34

	out[6] =
		uint64(in[6]-minv)>>30 |
			uint64(in[7]-minv)<<29

	out[7] =
		uint64(in[7]-minv)>>35 |
			uint64(in[8]-minv)<<24

	out[8] =
		uint64(in[8]-minv)>>40 |
			uint64(in[9]-minv)<<19

	out[9] =
		uint64(in[9]-minv)>>45 |
			uint64(in[10]-minv)<<14

	out[10] =
		uint64(in[10]-minv)>>50 |
			uint64(in[11]-minv)<<9

	out[11] =
		uint64(in[11]-minv)>>55 |
			uint64(in[12]-minv)<<4 |
			uint64(in[13]-minv)<<63

	out[12] =
		uint64(in[13]-minv)>>1 |
			uint64(in[14]-minv)<<58

	out[13] =
		uint64(in[14]-minv)>>6 |
			uint64(in[15]-minv)<<53

	out[14] =
		uint64(in[15]-minv)>>11 |
			uint64(in[16]-minv)<<48

	out[15] =
		uint64(in[16]-minv)>>16 |
			uint64(in[17]-minv)<<43

	out[16] =
		uint64(in[17]-minv)>>21 |
			uint64(in[18]-minv)<<38

	out[17] =
		uint64(in[18]-minv)>>26 |
			uint64(in[19]-minv)<<33

	out[18] =
		uint64(in[19]-minv)>>31 |
			uint64(in[20]-minv)<<28

	out[19] =
		uint64(in[20]-minv)>>36 |
			uint64(in[21]-minv)<<23

	out[20] =
		uint64(in[21]-minv)>>41 |
			uint64(in[22]-minv)<<18

	out[21] =
		uint64(in[22]-minv)>>46 |
			uint64(in[23]-minv)<<13

	out[22] =
		uint64(in[23]-minv)>>51 |
			uint64(in[24]-minv)<<8

	out[23] =
		uint64(in[24]-minv)>>56 |
			uint64(in[25]-minv)<<3 |
			uint64(in[26]-minv)<<62

	out[24] =
		uint64(in[26]-minv)>>2 |
			uint64(in[27]-minv)<<57

	out[25] =
		uint64(in[27]-minv)>>7 |
			uint64(in[28]-minv)<<52

	out[26] =
		uint64(in[28]-minv)>>12 |
			uint64(in[29]-minv)<<47

	out[27] =
		uint64(in[29]-minv)>>17 |
			uint64(in[30]-minv)<<42

	out[28] =
		uint64(in[30]-minv)>>22 |
			uint64(in[31]-minv)<<37

	out[29] =
		uint64(in[31]-minv)>>27 |
			uint64(in[32]-minv)<<32

	out[30] =
		uint64(in[32]-minv)>>32 |
			uint64(in[33]-minv)<<27

	out[31] =
		uint64(in[33]-minv)>>37 |
			uint64(in[34]-minv)<<22

	out[32] =
		uint64(in[34]-minv)>>42 |
			uint64(in[35]-minv)<<17

	out[33] =
		uint64(in[35]-minv)>>47 |
			uint64(in[36]-minv)<<12

	out[34] =
		uint64(in[36]-minv)>>52 |
			uint64(in[37]-minv)<<7

	out[35] =
		uint64(in[37]-minv)>>57 |
			uint64(in[38]-minv)<<2 |
			uint64(in[39]-minv)<<61

	out[36] =
		uint64(in[39]-minv)>>3 |
			uint64(in[40]-minv)<<56

	out[37] =
		uint64(in[40]-minv)>>8 |
			uint64(in[41]-minv)<<51

	out[38] =
		uint64(in[41]-minv)>>13 |
			uint64(in[42]-minv)<<46

	out[39] =
		uint64(in[42]-minv)>>18 |
			uint64(in[43]-minv)<<41

	out[40] =
		uint64(in[43]-minv)>>23 |
			uint64(in[44]-minv)<<36

	out[41] =
		uint64(in[44]-minv)>>28 |
			uint64(in[45]-minv)<<31

	out[42] =
		uint64(in[45]-minv)>>33 |
			uint64(in[46]-minv)<<26

	out[43] =
		uint64(in[46]-minv)>>38 |
			uint64(in[47]-minv)<<21

	out[44] =
		uint64(in[47]-minv)>>43 |
			uint64(in[48]-minv)<<16

	out[45] =
		uint64(in[48]-minv)>>48 |
			uint64(in[49]-minv)<<11

	out[46] =
		uint64(in[49]-minv)>>53 |
			uint64(in[50]-minv)<<6

	out[47] =
		uint64(in[50]-minv)>>58 |
			uint64(in[51]-minv)<<1 |
			uint64(in[52]-minv)<<60

	out[48] =
		uint64(in[52]-minv)>>4 |
			uint64(in[53]-minv)<<55

	out[49] =
		uint64(in[53]-minv)>>9 |
			uint64(in[54]-minv)<<50

	out[50] =
		uint64(in[54]-minv)>>14 |
			uint64(in[55]-minv)<<45

	out[51] =
		uint64(in[55]-minv)>>19 |
			uint64(in[56]-minv)<<40

	out[52] =
		uint64(in[56]-minv)>>24 |
			uint64(in[57]-minv)<<35

	out[53] =
		uint64(in[57]-minv)>>29 |
			uint64(in[58]-minv)<<30

	out[54] =
		uint64(in[58]-minv)>>34 |
			uint64(in[59]-minv)<<25

	out[55] =
		uint64(in[59]-minv)>>39 |
			uint64(in[60]-minv)<<20

	out[56] =
		uint64(in[60]-minv)>>44 |
			uint64(in[61]-minv)<<15

	out[57] =
		uint64(in[61]-minv)>>49 |
			uint64(in[62]-minv)<<10

	out[58] =
		uint64(in[62]-minv)>>54 |
			uint64(in[63]-minv)<<5
}

func bp64_60(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[60]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<60

	out[1] =
		uint64(in[1]-minv)>>4 |
			uint64(in[2]-minv)<<56

	out[2] =
		uint64(in[2]-minv)>>8 |
			uint64(in[3]-minv)<<52

	out[3] =
		uint64(in[3]-minv)>>12 |
			uint64(in[4]-minv)<<48

	out[4] =
		uint64(in[4]-minv)>>16 |
			uint64(in[5]-minv)<<44

	out[5] =
		uint64(in[5]-minv)>>20 |
			uint64(in[6]-minv)<<40

	out[6] =
		uint64(in[6]-minv)>>24 |
			uint64(in[7]-minv)<<36

	out[7] =
		uint64(in[7]-minv)>>28 |
			uint64(in[8]-minv)<<32

	out[8] =
		uint64(in[8]-minv)>>32 |
			uint64(in[9]-minv)<<28

	out[9] =
		uint64(in[9]-minv)>>36 |
			uint64(in[10]-minv)<<24

	out[10] =
		uint64(in[10]-minv)>>40 |
			uint64(in[11]-minv)<<20

	out[11] =
		uint64(in[11]-minv)>>44 |
			uint64(in[12]-minv)<<16

	out[12] =
		uint64(in[12]-minv)>>48 |
			uint64(in[13]-minv)<<12

	out[13] =
		uint64(in[13]-minv)>>52 |
			uint64(in[14]-minv)<<8

	out[14] =
		uint64(in[14]-minv)>>56 |
			uint64(in[15]-minv)<<4

	out[15] =
		uint64(in[15]-minv)>>60 |
			uint64(in[16]-minv) |
			uint64(in[17]-minv)<<60

	out[16] =
		uint64(in[17]-minv)>>4 |
			uint64(in[18]-minv)<<56

	out[17] =
		uint64(in[18]-minv)>>8 |
			uint64(in[19]-minv)<<52

	out[18] =
		uint64(in[19]-minv)>>12 |
			uint64(in[20]-minv)<<48

	out[19] =
		uint64(in[20]-minv)>>16 |
			uint64(in[21]-minv)<<44

	out[20] =
		uint64(in[21]-minv)>>20 |
			uint64(in[22]-minv)<<40

	out[21] =
		uint64(in[22]-minv)>>24 |
			uint64(in[23]-minv)<<36

	out[22] =
		uint64(in[23]-minv)>>28 |
			uint64(in[24]-minv)<<32

	out[23] =
		uint64(in[24]-minv)>>32 |
			uint64(in[25]-minv)<<28

	out[24] =
		uint64(in[25]-minv)>>36 |
			uint64(in[26]-minv)<<24

	out[25] =
		uint64(in[26]-minv)>>40 |
			uint64(in[27]-minv)<<20

	out[26] =
		uint64(in[27]-minv)>>44 |
			uint64(in[28]-minv)<<16

	out[27] =
		uint64(in[28]-minv)>>48 |
			uint64(in[29]-minv)<<12

	out[28] =
		uint64(in[29]-minv)>>52 |
			uint64(in[30]-minv)<<8

	out[29] =
		uint64(in[30]-minv)>>56 |
			uint64(in[31]-minv)<<4

	out[30] =
		uint64(in[31]-minv)>>60 |
			uint64(in[32]-minv) |
			uint64(in[33]-minv)<<60

	out[31] =
		uint64(in[33]-minv)>>4 |
			uint64(in[34]-minv)<<56

	out[32] =
		uint64(in[34]-minv)>>8 |
			uint64(in[35]-minv)<<52

	out[33] =
		uint64(in[35]-minv)>>12 |
			uint64(in[36]-minv)<<48

	out[34] =
		uint64(in[36]-minv)>>16 |
			uint64(in[37]-minv)<<44

	out[35] =
		uint64(in[37]-minv)>>20 |
			uint64(in[38]-minv)<<40

	out[36] =
		uint64(in[38]-minv)>>24 |
			uint64(in[39]-minv)<<36

	out[37] =
		uint64(in[39]-minv)>>28 |
			uint64(in[40]-minv)<<32

	out[38] =
		uint64(in[40]-minv)>>32 |
			uint64(in[41]-minv)<<28

	out[39] =
		uint64(in[41]-minv)>>36 |
			uint64(in[42]-minv)<<24

	out[40] =
		uint64(in[42]-minv)>>40 |
			uint64(in[43]-minv)<<20

	out[41] =
		uint64(in[43]-minv)>>44 |
			uint64(in[44]-minv)<<16

	out[42] =
		uint64(in[44]-minv)>>48 |
			uint64(in[45]-minv)<<12

	out[43] =
		uint64(in[45]-minv)>>52 |
			uint64(in[46]-minv)<<8

	out[44] =
		uint64(in[46]-minv)>>56 |
			uint64(in[47]-minv)<<4

	out[45] =
		uint64(in[47]-minv)>>60 |
			uint64(in[48]-minv) |
			uint64(in[49]-minv)<<60

	out[46] =
		uint64(in[49]-minv)>>4 |
			uint64(in[50]-minv)<<56

	out[47] =
		uint64(in[50]-minv)>>8 |
			uint64(in[51]-minv)<<52

	out[48] =
		uint64(in[51]-minv)>>12 |
			uint64(in[52]-minv)<<48

	out[49] =
		uint64(in[52]-minv)>>16 |
			uint64(in[53]-minv)<<44

	out[50] =
		uint64(in[53]-minv)>>20 |
			uint64(in[54]-minv)<<40

	out[51] =
		uint64(in[54]-minv)>>24 |
			uint64(in[55]-minv)<<36

	out[52] =
		uint64(in[55]-minv)>>28 |
			uint64(in[56]-minv)<<32

	out[53] =
		uint64(in[56]-minv)>>32 |
			uint64(in[57]-minv)<<28

	out[54] =
		uint64(in[57]-minv)>>36 |
			uint64(in[58]-minv)<<24

	out[55] =
		uint64(in[58]-minv)>>40 |
			uint64(in[59]-minv)<<20

	out[56] =
		uint64(in[59]-minv)>>44 |
			uint64(in[60]-minv)<<16

	out[57] =
		uint64(in[60]-minv)>>48 |
			uint64(in[61]-minv)<<12

	out[58] =
		uint64(in[61]-minv)>>52 |
			uint64(in[62]-minv)<<8

	out[59] =
		uint64(in[62]-minv)>>56 |
			uint64(in[63]-minv)<<4
}

func bp64_61(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[61]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<61

	out[1] =
		uint64(in[1]-minv)>>3 |
			uint64(in[2]-minv)<<58

	out[2] =
		uint64(in[2]-minv)>>6 |
			uint64(in[3]-minv)<<55

	out[3] =
		uint64(in[3]-minv)>>9 |
			uint64(in[4]-minv)<<52

	out[4] =
		uint64(in[4]-minv)>>12 |
			uint64(in[5]-minv)<<49

	out[5] =
		uint64(in[5]-minv)>>15 |
			uint64(in[6]-minv)<<46

	out[6] =
		uint64(in[6]-minv)>>18 |
			uint64(in[7]-minv)<<43

	out[7] =
		uint64(in[7]-minv)>>21 |
			uint64(in[8]-minv)<<40

	out[8] =
		uint64(in[8]-minv)>>24 |
			uint64(in[9]-minv)<<37

	out[9] =
		uint64(in[9]-minv)>>27 |
			uint64(in[10]-minv)<<34

	out[10] =
		uint64(in[10]-minv)>>30 |
			uint64(in[11]-minv)<<31

	out[11] =
		uint64(in[11]-minv)>>33 |
			uint64(in[12]-minv)<<28

	out[12] =
		uint64(in[12]-minv)>>36 |
			uint64(in[13]-minv)<<25

	out[13] =
		uint64(in[13]-minv)>>39 |
			uint64(in[14]-minv)<<22

	out[14] =
		uint64(in[14]-minv)>>42 |
			uint64(in[15]-minv)<<19

	out[15] =
		uint64(in[15]-minv)>>45 |
			uint64(in[16]-minv)<<16

	out[16] =
		uint64(in[16]-minv)>>48 |
			uint64(in[17]-minv)<<13

	out[17] =
		uint64(in[17]-minv)>>51 |
			uint64(in[18]-minv)<<10

	out[18] =
		uint64(in[18]-minv)>>54 |
			uint64(in[19]-minv)<<7

	out[19] =
		uint64(in[19]-minv)>>57 |
			uint64(in[20]-minv)<<4

	out[20] =
		uint64(in[20]-minv)>>60 |
			uint64(in[21]-minv)<<1 |
			uint64(in[22]-minv)<<62

	out[21] =
		uint64(in[22]-minv)>>2 |
			uint64(in[23]-minv)<<59

	out[22] =
		uint64(in[23]-minv)>>5 |
			uint64(in[24]-minv)<<56

	out[23] =
		uint64(in[24]-minv)>>8 |
			uint64(in[25]-minv)<<53

	out[24] =
		uint64(in[25]-minv)>>11 |
			uint64(in[26]-minv)<<50

	out[25] =
		uint64(in[26]-minv)>>14 |
			uint64(in[27]-minv)<<47

	out[26] =
		uint64(in[27]-minv)>>17 |
			uint64(in[28]-minv)<<44

	out[27] =
		uint64(in[28]-minv)>>20 |
			uint64(in[29]-minv)<<41

	out[28] =
		uint64(in[29]-minv)>>23 |
			uint64(in[30]-minv)<<38

	out[29] =
		uint64(in[30]-minv)>>26 |
			uint64(in[31]-minv)<<35

	out[30] =
		uint64(in[31]-minv)>>29 |
			uint64(in[32]-minv)<<32

	out[31] =
		uint64(in[32]-minv)>>32 |
			uint64(in[33]-minv)<<29

	out[32] =
		uint64(in[33]-minv)>>35 |
			uint64(in[34]-minv)<<26

	out[33] =
		uint64(in[34]-minv)>>38 |
			uint64(in[35]-minv)<<23

	out[34] =
		uint64(in[35]-minv)>>41 |
			uint64(in[36]-minv)<<20

	out[35] =
		uint64(in[36]-minv)>>44 |
			uint64(in[37]-minv)<<17

	out[36] =
		uint64(in[37]-minv)>>47 |
			uint64(in[38]-minv)<<14

	out[37] =
		uint64(in[38]-minv)>>50 |
			uint64(in[39]-minv)<<11

	out[38] =
		uint64(in[39]-minv)>>53 |
			uint64(in[40]-minv)<<8

	out[39] =
		uint64(in[40]-minv)>>56 |
			uint64(in[41]-minv)<<5

	out[40] =
		uint64(in[41]-minv)>>59 |
			uint64(in[42]-minv)<<2 |
			uint64(in[43]-minv)<<63

	out[41] =
		uint64(in[43]-minv)>>1 |
			uint64(in[44]-minv)<<60

	out[42] =
		uint64(in[44]-minv)>>4 |
			uint64(in[45]-minv)<<57

	out[43] =
		uint64(in[45]-minv)>>7 |
			uint64(in[46]-minv)<<54

	out[44] =
		uint64(in[46]-minv)>>10 |
			uint64(in[47]-minv)<<51

	out[45] =
		uint64(in[47]-minv)>>13 |
			uint64(in[48]-minv)<<48

	out[46] =
		uint64(in[48]-minv)>>16 |
			uint64(in[49]-minv)<<45

	out[47] =
		uint64(in[49]-minv)>>19 |
			uint64(in[50]-minv)<<42

	out[48] =
		uint64(in[50]-minv)>>22 |
			uint64(in[51]-minv)<<39

	out[49] =
		uint64(in[51]-minv)>>25 |
			uint64(in[52]-minv)<<36

	out[50] =
		uint64(in[52]-minv)>>28 |
			uint64(in[53]-minv)<<33

	out[51] =
		uint64(in[53]-minv)>>31 |
			uint64(in[54]-minv)<<30

	out[52] =
		uint64(in[54]-minv)>>34 |
			uint64(in[55]-minv)<<27

	out[53] =
		uint64(in[55]-minv)>>37 |
			uint64(in[56]-minv)<<24

	out[54] =
		uint64(in[56]-minv)>>40 |
			uint64(in[57]-minv)<<21

	out[55] =
		uint64(in[57]-minv)>>43 |
			uint64(in[58]-minv)<<18

	out[56] =
		uint64(in[58]-minv)>>46 |
			uint64(in[59]-minv)<<15

	out[57] =
		uint64(in[59]-minv)>>49 |
			uint64(in[60]-minv)<<12

	out[58] =
		uint64(in[60]-minv)>>52 |
			uint64(in[61]-minv)<<9

	out[59] =
		uint64(in[61]-minv)>>55 |
			uint64(in[62]-minv)<<6

	out[60] =
		uint64(in[62]-minv)>>58 |
			uint64(in[63]-minv)<<3
}

func bp64_62(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[62]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<62

	out[1] =
		uint64(in[1]-minv)>>2 |
			uint64(in[2]-minv)<<60

	out[2] =
		uint64(in[2]-minv)>>4 |
			uint64(in[3]-minv)<<58

	out[3] =
		uint64(in[3]-minv)>>6 |
			uint64(in[4]-minv)<<56

	out[4] =
		uint64(in[4]-minv)>>8 |
			uint64(in[5]-minv)<<54

	out[5] =
		uint64(in[5]-minv)>>10 |
			uint64(in[6]-minv)<<52

	out[6] =
		uint64(in[6]-minv)>>12 |
			uint64(in[7]-minv)<<50

	out[7] =
		uint64(in[7]-minv)>>14 |
			uint64(in[8]-minv)<<48

	out[8] =
		uint64(in[8]-minv)>>16 |
			uint64(in[9]-minv)<<46

	out[9] =
		uint64(in[9]-minv)>>18 |
			uint64(in[10]-minv)<<44

	out[10] =
		uint64(in[10]-minv)>>20 |
			uint64(in[11]-minv)<<42

	out[11] =
		uint64(in[11]-minv)>>22 |
			uint64(in[12]-minv)<<40

	out[12] =
		uint64(in[12]-minv)>>24 |
			uint64(in[13]-minv)<<38

	out[13] =
		uint64(in[13]-minv)>>26 |
			uint64(in[14]-minv)<<36

	out[14] =
		uint64(in[14]-minv)>>28 |
			uint64(in[15]-minv)<<34

	out[15] =
		uint64(in[15]-minv)>>30 |
			uint64(in[16]-minv)<<32

	out[16] =
		uint64(in[16]-minv)>>32 |
			uint64(in[17]-minv)<<30

	out[17] =
		uint64(in[17]-minv)>>34 |
			uint64(in[18]-minv)<<28

	out[18] =
		uint64(in[18]-minv)>>36 |
			uint64(in[19]-minv)<<26

	out[19] =
		uint64(in[19]-minv)>>38 |
			uint64(in[20]-minv)<<24

	out[20] =
		uint64(in[20]-minv)>>40 |
			uint64(in[21]-minv)<<22

	out[21] =
		uint64(in[21]-minv)>>42 |
			uint64(in[22]-minv)<<20

	out[22] =
		uint64(in[22]-minv)>>44 |
			uint64(in[23]-minv)<<18

	out[23] =
		uint64(in[23]-minv)>>46 |
			uint64(in[24]-minv)<<16

	out[24] =
		uint64(in[24]-minv)>>48 |
			uint64(in[25]-minv)<<14

	out[25] =
		uint64(in[25]-minv)>>50 |
			uint64(in[26]-minv)<<12

	out[26] =
		uint64(in[26]-minv)>>52 |
			uint64(in[27]-minv)<<10

	out[27] =
		uint64(in[27]-minv)>>54 |
			uint64(in[28]-minv)<<8

	out[28] =
		uint64(in[28]-minv)>>56 |
			uint64(in[29]-minv)<<6

	out[29] =
		uint64(in[29]-minv)>>58 |
			uint64(in[30]-minv)<<4

	out[30] =
		uint64(in[30]-minv)>>60 |
			uint64(in[31]-minv)<<2

	out[31] =
		uint64(in[31]-minv)>>62 |
			uint64(in[32]-minv) |
			uint64(in[33]-minv)<<62

	out[32] =
		uint64(in[33]-minv)>>2 |
			uint64(in[34]-minv)<<60

	out[33] =
		uint64(in[34]-minv)>>4 |
			uint64(in[35]-minv)<<58

	out[34] =
		uint64(in[35]-minv)>>6 |
			uint64(in[36]-minv)<<56

	out[35] =
		uint64(in[36]-minv)>>8 |
			uint64(in[37]-minv)<<54

	out[36] =
		uint64(in[37]-minv)>>10 |
			uint64(in[38]-minv)<<52

	out[37] =
		uint64(in[38]-minv)>>12 |
			uint64(in[39]-minv)<<50

	out[38] =
		uint64(in[39]-minv)>>14 |
			uint64(in[40]-minv)<<48

	out[39] =
		uint64(in[40]-minv)>>16 |
			uint64(in[41]-minv)<<46

	out[40] =
		uint64(in[41]-minv)>>18 |
			uint64(in[42]-minv)<<44

	out[41] =
		uint64(in[42]-minv)>>20 |
			uint64(in[43]-minv)<<42

	out[42] =
		uint64(in[43]-minv)>>22 |
			uint64(in[44]-minv)<<40

	out[43] =
		uint64(in[44]-minv)>>24 |
			uint64(in[45]-minv)<<38

	out[44] =
		uint64(in[45]-minv)>>26 |
			uint64(in[46]-minv)<<36

	out[45] =
		uint64(in[46]-minv)>>28 |
			uint64(in[47]-minv)<<34

	out[46] =
		uint64(in[47]-minv)>>30 |
			uint64(in[48]-minv)<<32

	out[47] =
		uint64(in[48]-minv)>>32 |
			uint64(in[49]-minv)<<30

	out[48] =
		uint64(in[49]-minv)>>34 |
			uint64(in[50]-minv)<<28

	out[49] =
		uint64(in[50]-minv)>>36 |
			uint64(in[51]-minv)<<26

	out[50] =
		uint64(in[51]-minv)>>38 |
			uint64(in[52]-minv)<<24

	out[51] =
		uint64(in[52]-minv)>>40 |
			uint64(in[53]-minv)<<22

	out[52] =
		uint64(in[53]-minv)>>42 |
			uint64(in[54]-minv)<<20

	out[53] =
		uint64(in[54]-minv)>>44 |
			uint64(in[55]-minv)<<18

	out[54] =
		uint64(in[55]-minv)>>46 |
			uint64(in[56]-minv)<<16

	out[55] =
		uint64(in[56]-minv)>>48 |
			uint64(in[57]-minv)<<14

	out[56] =
		uint64(in[57]-minv)>>50 |
			uint64(in[58]-minv)<<12

	out[57] =
		uint64(in[58]-minv)>>52 |
			uint64(in[59]-minv)<<10

	out[58] =
		uint64(in[59]-minv)>>54 |
			uint64(in[60]-minv)<<8

	out[59] =
		uint64(in[60]-minv)>>56 |
			uint64(in[61]-minv)<<6

	out[60] =
		uint64(in[61]-minv)>>58 |
			uint64(in[62]-minv)<<4

	out[61] =
		uint64(in[62]-minv)>>60 |
			uint64(in[63]-minv)<<2
}

func bp64_63(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[63]uint64)(p)
	out[0] =
		uint64(in[0]-minv) |
			uint64(in[1]-minv)<<63

	out[1] =
		uint64(in[1]-minv)>>1 |
			uint64(in[2]-minv)<<62

	out[2] =
		uint64(in[2]-minv)>>2 |
			uint64(in[3]-minv)<<61

	out[3] =
		uint64(in[3]-minv)>>3 |
			uint64(in[4]-minv)<<60

	out[4] =
		uint64(in[4]-minv)>>4 |
			uint64(in[5]-minv)<<59

	out[5] =
		uint64(in[5]-minv)>>5 |
			uint64(in[6]-minv)<<58

	out[6] =
		uint64(in[6]-minv)>>6 |
			uint64(in[7]-minv)<<57

	out[7] =
		uint64(in[7]-minv)>>7 |
			uint64(in[8]-minv)<<56

	out[8] =
		uint64(in[8]-minv)>>8 |
			uint64(in[9]-minv)<<55

	out[9] =
		uint64(in[9]-minv)>>9 |
			uint64(in[10]-minv)<<54

	out[10] =
		uint64(in[10]-minv)>>10 |
			uint64(in[11]-minv)<<53

	out[11] =
		uint64(in[11]-minv)>>11 |
			uint64(in[12]-minv)<<52

	out[12] =
		uint64(in[12]-minv)>>12 |
			uint64(in[13]-minv)<<51

	out[13] =
		uint64(in[13]-minv)>>13 |
			uint64(in[14]-minv)<<50

	out[14] =
		uint64(in[14]-minv)>>14 |
			uint64(in[15]-minv)<<49

	out[15] =
		uint64(in[15]-minv)>>15 |
			uint64(in[16]-minv)<<48

	out[16] =
		uint64(in[16]-minv)>>16 |
			uint64(in[17]-minv)<<47

	out[17] =
		uint64(in[17]-minv)>>17 |
			uint64(in[18]-minv)<<46

	out[18] =
		uint64(in[18]-minv)>>18 |
			uint64(in[19]-minv)<<45

	out[19] =
		uint64(in[19]-minv)>>19 |
			uint64(in[20]-minv)<<44

	out[20] =
		uint64(in[20]-minv)>>20 |
			uint64(in[21]-minv)<<43

	out[21] =
		uint64(in[21]-minv)>>21 |
			uint64(in[22]-minv)<<42

	out[22] =
		uint64(in[22]-minv)>>22 |
			uint64(in[23]-minv)<<41

	out[23] =
		uint64(in[23]-minv)>>23 |
			uint64(in[24]-minv)<<40

	out[24] =
		uint64(in[24]-minv)>>24 |
			uint64(in[25]-minv)<<39

	out[25] =
		uint64(in[25]-minv)>>25 |
			uint64(in[26]-minv)<<38

	out[26] =
		uint64(in[26]-minv)>>26 |
			uint64(in[27]-minv)<<37

	out[27] =
		uint64(in[27]-minv)>>27 |
			uint64(in[28]-minv)<<36

	out[28] =
		uint64(in[28]-minv)>>28 |
			uint64(in[29]-minv)<<35

	out[29] =
		uint64(in[29]-minv)>>29 |
			uint64(in[30]-minv)<<34

	out[30] =
		uint64(in[30]-minv)>>30 |
			uint64(in[31]-minv)<<33

	out[31] =
		uint64(in[31]-minv)>>31 |
			uint64(in[32]-minv)<<32

	out[32] =
		uint64(in[32]-minv)>>32 |
			uint64(in[33]-minv)<<31

	out[33] =
		uint64(in[33]-minv)>>33 |
			uint64(in[34]-minv)<<30

	out[34] =
		uint64(in[34]-minv)>>34 |
			uint64(in[35]-minv)<<29

	out[35] =
		uint64(in[35]-minv)>>35 |
			uint64(in[36]-minv)<<28

	out[36] =
		uint64(in[36]-minv)>>36 |
			uint64(in[37]-minv)<<27

	out[37] =
		uint64(in[37]-minv)>>37 |
			uint64(in[38]-minv)<<26

	out[38] =
		uint64(in[38]-minv)>>38 |
			uint64(in[39]-minv)<<25

	out[39] =
		uint64(in[39]-minv)>>39 |
			uint64(in[40]-minv)<<24

	out[40] =
		uint64(in[40]-minv)>>40 |
			uint64(in[41]-minv)<<23

	out[41] =
		uint64(in[41]-minv)>>41 |
			uint64(in[42]-minv)<<22

	out[42] =
		uint64(in[42]-minv)>>42 |
			uint64(in[43]-minv)<<21

	out[43] =
		uint64(in[43]-minv)>>43 |
			uint64(in[44]-minv)<<20

	out[44] =
		uint64(in[44]-minv)>>44 |
			uint64(in[45]-minv)<<19

	out[45] =
		uint64(in[45]-minv)>>45 |
			uint64(in[46]-minv)<<18

	out[46] =
		uint64(in[46]-minv)>>46 |
			uint64(in[47]-minv)<<17

	out[47] =
		uint64(in[47]-minv)>>47 |
			uint64(in[48]-minv)<<16

	out[48] =
		uint64(in[48]-minv)>>48 |
			uint64(in[49]-minv)<<15

	out[49] =
		uint64(in[49]-minv)>>49 |
			uint64(in[50]-minv)<<14

	out[50] =
		uint64(in[50]-minv)>>50 |
			uint64(in[51]-minv)<<13

	out[51] =
		uint64(in[51]-minv)>>51 |
			uint64(in[52]-minv)<<12

	out[52] =
		uint64(in[52]-minv)>>52 |
			uint64(in[53]-minv)<<11

	out[53] =
		uint64(in[53]-minv)>>53 |
			uint64(in[54]-minv)<<10

	out[54] =
		uint64(in[54]-minv)>>54 |
			uint64(in[55]-minv)<<9

	out[55] =
		uint64(in[55]-minv)>>55 |
			uint64(in[56]-minv)<<8

	out[56] =
		uint64(in[56]-minv)>>56 |
			uint64(in[57]-minv)<<7

	out[57] =
		uint64(in[57]-minv)>>57 |
			uint64(in[58]-minv)<<6

	out[58] =
		uint64(in[58]-minv)>>58 |
			uint64(in[59]-minv)<<5

	out[59] =
		uint64(in[59]-minv)>>59 |
			uint64(in[60]-minv)<<4

	out[60] =
		uint64(in[60]-minv)>>60 |
			uint64(in[61]-minv)<<3

	out[61] =
		uint64(in[61]-minv)>>61 |
			uint64(in[62]-minv)<<2

	out[62] =
		uint64(in[62]-minv)>>62 |
			uint64(in[63]-minv)<<1
}

func bp64_64(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[64]uint64)(p)
	var i int
	for range 4 {
		out[i] = in[i] - minv
		out[i+1] = in[i+1] - minv
		out[i+2] = in[i+2] - minv
		out[i+3] = in[i+3] - minv
		out[i+4] = in[i+4] - minv
		out[i+5] = in[i+5] - minv
		out[i+6] = in[i+6] - minv
		out[i+7] = in[i+7] - minv
		out[i+8] = in[i+8] - minv
		out[i+9] = in[i+9] - minv
		out[i+10] = in[i+10] - minv
		out[i+11] = in[i+11] - minv
		out[i+12] = in[i+12] - minv
		out[i+13] = in[i+13] - minv
		out[i+14] = in[i+14] - minv
		out[i+15] = in[i+15] - minv
		i += 16
	}
}

// Reader
var unpack_u64 = [65]func(out *[64]uint64, p unsafe.Pointer, minv uint64){
	br64_0,
	br64_1,
	br64_2,
	br64_3,
	br64_4,
	br64_5,
	br64_6,
	br64_7,
	br64_8,
	br64_9,
	br64_10,
	br64_11,
	br64_12,
	br64_13,
	br64_14,
	br64_15,
	br64_16,
	br64_17,
	br64_18,
	br64_19,
	br64_20,
	br64_21,
	br64_22,
	br64_23,
	br64_24,
	br64_25,
	br64_26,
	br64_27,
	br64_28,
	br64_29,
	br64_30,
	br64_31,
	br64_32,
	br64_33,
	br64_34,
	br64_35,
	br64_36,
	br64_37,
	br64_38,
	br64_39,
	br64_40,
	br64_41,
	br64_42,
	br64_43,
	br64_44,
	br64_45,
	br64_46,
	br64_47,
	br64_48,
	br64_49,
	br64_50,
	br64_51,
	br64_52,
	br64_53,
	br64_54,
	br64_55,
	br64_56,
	br64_57,
	br64_58,
	br64_59,
	br64_60,
	br64_61,
	br64_62,
	br64_63,
	br64_64,
}

func br64_0(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	for i := range out {
		out[i] = uint64(minv)
	}
}

func br64_1(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[1]uint64)(p)
	mask := uint64((1 << 1) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>1)&mask + minv)
	out[2] = uint64((in[0]>>2)&mask + minv)
	out[3] = uint64((in[0]>>3)&mask + minv)
	out[4] = uint64((in[0]>>4)&mask + minv)
	out[5] = uint64((in[0]>>5)&mask + minv)
	out[6] = uint64((in[0]>>6)&mask + minv)
	out[7] = uint64((in[0]>>7)&mask + minv)
	out[8] = uint64((in[0]>>8)&mask + minv)
	out[9] = uint64((in[0]>>9)&mask + minv)
	out[10] = uint64((in[0]>>10)&mask + minv)
	out[11] = uint64((in[0]>>11)&mask + minv)
	out[12] = uint64((in[0]>>12)&mask + minv)
	out[13] = uint64((in[0]>>13)&mask + minv)
	out[14] = uint64((in[0]>>14)&mask + minv)
	out[15] = uint64((in[0]>>15)&mask + minv)
	out[16] = uint64((in[0]>>16)&mask + minv)
	out[17] = uint64((in[0]>>17)&mask + minv)
	out[18] = uint64((in[0]>>18)&mask + minv)
	out[19] = uint64((in[0]>>19)&mask + minv)
	out[20] = uint64((in[0]>>20)&mask + minv)
	out[21] = uint64((in[0]>>21)&mask + minv)
	out[22] = uint64((in[0]>>22)&mask + minv)
	out[23] = uint64((in[0]>>23)&mask + minv)
	out[24] = uint64((in[0]>>24)&mask + minv)
	out[25] = uint64((in[0]>>25)&mask + minv)
	out[26] = uint64((in[0]>>26)&mask + minv)
	out[27] = uint64((in[0]>>27)&mask + minv)
	out[28] = uint64((in[0]>>28)&mask + minv)
	out[29] = uint64((in[0]>>29)&mask + minv)
	out[30] = uint64((in[0]>>30)&mask + minv)
	out[31] = uint64((in[0]>>31)&mask + minv)
	out[32] = uint64((in[0]>>32)&mask + minv)
	out[33] = uint64((in[0]>>33)&mask + minv)
	out[34] = uint64((in[0]>>34)&mask + minv)
	out[35] = uint64((in[0]>>35)&mask + minv)
	out[36] = uint64((in[0]>>36)&mask + minv)
	out[37] = uint64((in[0]>>37)&mask + minv)
	out[38] = uint64((in[0]>>38)&mask + minv)
	out[39] = uint64((in[0]>>39)&mask + minv)
	out[40] = uint64((in[0]>>40)&mask + minv)
	out[41] = uint64((in[0]>>41)&mask + minv)
	out[42] = uint64((in[0]>>42)&mask + minv)
	out[43] = uint64((in[0]>>43)&mask + minv)
	out[44] = uint64((in[0]>>44)&mask + minv)
	out[45] = uint64((in[0]>>45)&mask + minv)
	out[46] = uint64((in[0]>>46)&mask + minv)
	out[47] = uint64((in[0]>>47)&mask + minv)
	out[48] = uint64((in[0]>>48)&mask + minv)
	out[49] = uint64((in[0]>>49)&mask + minv)
	out[50] = uint64((in[0]>>50)&mask + minv)
	out[51] = uint64((in[0]>>51)&mask + minv)
	out[52] = uint64((in[0]>>52)&mask + minv)
	out[53] = uint64((in[0]>>53)&mask + minv)
	out[54] = uint64((in[0]>>54)&mask + minv)
	out[55] = uint64((in[0]>>55)&mask + minv)
	out[56] = uint64((in[0]>>56)&mask + minv)
	out[57] = uint64((in[0]>>57)&mask + minv)
	out[58] = uint64((in[0]>>58)&mask + minv)
	out[59] = uint64((in[0]>>59)&mask + minv)
	out[60] = uint64((in[0]>>60)&mask + minv)
	out[61] = uint64((in[0]>>61)&mask + minv)
	out[62] = uint64((in[0]>>62)&mask + minv)
	out[63] = uint64((in[0]>>63)&mask + minv)
}

func br64_2(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[2]uint64)(p)
	mask := uint64((1 << 2) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>2)&mask + minv)
	out[2] = uint64((in[0]>>4)&mask + minv)
	out[3] = uint64((in[0]>>6)&mask + minv)
	out[4] = uint64((in[0]>>8)&mask + minv)
	out[5] = uint64((in[0]>>10)&mask + minv)
	out[6] = uint64((in[0]>>12)&mask + minv)
	out[7] = uint64((in[0]>>14)&mask + minv)
	out[8] = uint64((in[0]>>16)&mask + minv)
	out[9] = uint64((in[0]>>18)&mask + minv)
	out[10] = uint64((in[0]>>20)&mask + minv)
	out[11] = uint64((in[0]>>22)&mask + minv)
	out[12] = uint64((in[0]>>24)&mask + minv)
	out[13] = uint64((in[0]>>26)&mask + minv)
	out[14] = uint64((in[0]>>28)&mask + minv)
	out[15] = uint64((in[0]>>30)&mask + minv)
	out[16] = uint64((in[0]>>32)&mask + minv)
	out[17] = uint64((in[0]>>34)&mask + minv)
	out[18] = uint64((in[0]>>36)&mask + minv)
	out[19] = uint64((in[0]>>38)&mask + minv)
	out[20] = uint64((in[0]>>40)&mask + minv)
	out[21] = uint64((in[0]>>42)&mask + minv)
	out[22] = uint64((in[0]>>44)&mask + minv)
	out[23] = uint64((in[0]>>46)&mask + minv)
	out[24] = uint64((in[0]>>48)&mask + minv)
	out[25] = uint64((in[0]>>50)&mask + minv)
	out[26] = uint64((in[0]>>52)&mask + minv)
	out[27] = uint64((in[0]>>54)&mask + minv)
	out[28] = uint64((in[0]>>56)&mask + minv)
	out[29] = uint64((in[0]>>58)&mask + minv)
	out[30] = uint64((in[0]>>60)&mask + minv)
	out[31] = uint64((in[0]>>62)&mask + minv)
	out[32] = uint64(in[1]&mask + minv)
	out[33] = uint64((in[1]>>2)&mask + minv)
	out[34] = uint64((in[1]>>4)&mask + minv)
	out[35] = uint64((in[1]>>6)&mask + minv)
	out[36] = uint64((in[1]>>8)&mask + minv)
	out[37] = uint64((in[1]>>10)&mask + minv)
	out[38] = uint64((in[1]>>12)&mask + minv)
	out[39] = uint64((in[1]>>14)&mask + minv)
	out[40] = uint64((in[1]>>16)&mask + minv)
	out[41] = uint64((in[1]>>18)&mask + minv)
	out[42] = uint64((in[1]>>20)&mask + minv)
	out[43] = uint64((in[1]>>22)&mask + minv)
	out[44] = uint64((in[1]>>24)&mask + minv)
	out[45] = uint64((in[1]>>26)&mask + minv)
	out[46] = uint64((in[1]>>28)&mask + minv)
	out[47] = uint64((in[1]>>30)&mask + minv)
	out[48] = uint64((in[1]>>32)&mask + minv)
	out[49] = uint64((in[1]>>34)&mask + minv)
	out[50] = uint64((in[1]>>36)&mask + minv)
	out[51] = uint64((in[1]>>38)&mask + minv)
	out[52] = uint64((in[1]>>40)&mask + minv)
	out[53] = uint64((in[1]>>42)&mask + minv)
	out[54] = uint64((in[1]>>44)&mask + minv)
	out[55] = uint64((in[1]>>46)&mask + minv)
	out[56] = uint64((in[1]>>48)&mask + minv)
	out[57] = uint64((in[1]>>50)&mask + minv)
	out[58] = uint64((in[1]>>52)&mask + minv)
	out[59] = uint64((in[1]>>54)&mask + minv)
	out[60] = uint64((in[1]>>56)&mask + minv)
	out[61] = uint64((in[1]>>58)&mask + minv)
	out[62] = uint64((in[1]>>60)&mask + minv)
	out[63] = uint64((in[1]>>62)&mask + minv)
}

func br64_3(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[3]uint64)(p)
	mask := uint64((1 << 3) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>3)&mask + minv)
	out[2] = uint64((in[0]>>6)&mask + minv)
	out[3] = uint64((in[0]>>9)&mask + minv)
	out[4] = uint64((in[0]>>12)&mask + minv)
	out[5] = uint64((in[0]>>15)&mask + minv)
	out[6] = uint64((in[0]>>18)&mask + minv)
	out[7] = uint64((in[0]>>21)&mask + minv)
	out[8] = uint64((in[0]>>24)&mask + minv)
	out[9] = uint64((in[0]>>27)&mask + minv)
	out[10] = uint64((in[0]>>30)&mask + minv)
	out[11] = uint64((in[0]>>33)&mask + minv)
	out[12] = uint64((in[0]>>36)&mask + minv)
	out[13] = uint64((in[0]>>39)&mask + minv)
	out[14] = uint64((in[0]>>42)&mask + minv)
	out[15] = uint64((in[0]>>45)&mask + minv)
	out[16] = uint64((in[0]>>48)&mask + minv)
	out[17] = uint64((in[0]>>51)&mask + minv)
	out[18] = uint64((in[0]>>54)&mask + minv)
	out[19] = uint64((in[0]>>57)&mask + minv)
	out[20] = uint64((in[0]>>60)&mask + minv)
	out[21] = uint64((in[0]>>63|in[1]<<1)&mask + minv)
	out[22] = uint64((in[1]>>2)&mask + minv)
	out[23] = uint64((in[1]>>5)&mask + minv)
	out[24] = uint64((in[1]>>8)&mask + minv)
	out[25] = uint64((in[1]>>11)&mask + minv)
	out[26] = uint64((in[1]>>14)&mask + minv)
	out[27] = uint64((in[1]>>17)&mask + minv)
	out[28] = uint64((in[1]>>20)&mask + minv)
	out[29] = uint64((in[1]>>23)&mask + minv)
	out[30] = uint64((in[1]>>26)&mask + minv)
	out[31] = uint64((in[1]>>29)&mask + minv)
	out[32] = uint64((in[1]>>32)&mask + minv)
	out[33] = uint64((in[1]>>35)&mask + minv)
	out[34] = uint64((in[1]>>38)&mask + minv)
	out[35] = uint64((in[1]>>41)&mask + minv)
	out[36] = uint64((in[1]>>44)&mask + minv)
	out[37] = uint64((in[1]>>47)&mask + minv)
	out[38] = uint64((in[1]>>50)&mask + minv)
	out[39] = uint64((in[1]>>53)&mask + minv)
	out[40] = uint64((in[1]>>56)&mask + minv)
	out[41] = uint64((in[1]>>59)&mask + minv)
	out[42] = uint64((in[1]>>62|in[2]<<2)&mask + minv)
	out[43] = uint64((in[2]>>1)&mask + minv)
	out[44] = uint64((in[2]>>4)&mask + minv)
	out[45] = uint64((in[2]>>7)&mask + minv)
	out[46] = uint64((in[2]>>10)&mask + minv)
	out[47] = uint64((in[2]>>13)&mask + minv)
	out[48] = uint64((in[2]>>16)&mask + minv)
	out[49] = uint64((in[2]>>19)&mask + minv)
	out[50] = uint64((in[2]>>22)&mask + minv)
	out[51] = uint64((in[2]>>25)&mask + minv)
	out[52] = uint64((in[2]>>28)&mask + minv)
	out[53] = uint64((in[2]>>31)&mask + minv)
	out[54] = uint64((in[2]>>34)&mask + minv)
	out[55] = uint64((in[2]>>37)&mask + minv)
	out[56] = uint64((in[2]>>40)&mask + minv)
	out[57] = uint64((in[2]>>43)&mask + minv)
	out[58] = uint64((in[2]>>46)&mask + minv)
	out[59] = uint64((in[2]>>49)&mask + minv)
	out[60] = uint64((in[2]>>52)&mask + minv)
	out[61] = uint64((in[2]>>55)&mask + minv)
	out[62] = uint64((in[2]>>58)&mask + minv)
	out[63] = uint64((in[2]>>61)&mask + minv)
}

func br64_4(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[4]uint64)(p)
	mask := uint64((1 << 4) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>4)&mask + minv)
	out[2] = uint64((in[0]>>8)&mask + minv)
	out[3] = uint64((in[0]>>12)&mask + minv)
	out[4] = uint64((in[0]>>16)&mask + minv)
	out[5] = uint64((in[0]>>20)&mask + minv)
	out[6] = uint64((in[0]>>24)&mask + minv)
	out[7] = uint64((in[0]>>28)&mask + minv)
	out[8] = uint64((in[0]>>32)&mask + minv)
	out[9] = uint64((in[0]>>36)&mask + minv)
	out[10] = uint64((in[0]>>40)&mask + minv)
	out[11] = uint64((in[0]>>44)&mask + minv)
	out[12] = uint64((in[0]>>48)&mask + minv)
	out[13] = uint64((in[0]>>52)&mask + minv)
	out[14] = uint64((in[0]>>56)&mask + minv)
	out[15] = uint64((in[0]>>60)&mask + minv)
	out[16] = uint64(in[1]&mask + minv)
	out[17] = uint64((in[1]>>4)&mask + minv)
	out[18] = uint64((in[1]>>8)&mask + minv)
	out[19] = uint64((in[1]>>12)&mask + minv)
	out[20] = uint64((in[1]>>16)&mask + minv)
	out[21] = uint64((in[1]>>20)&mask + minv)
	out[22] = uint64((in[1]>>24)&mask + minv)
	out[23] = uint64((in[1]>>28)&mask + minv)
	out[24] = uint64((in[1]>>32)&mask + minv)
	out[25] = uint64((in[1]>>36)&mask + minv)
	out[26] = uint64((in[1]>>40)&mask + minv)
	out[27] = uint64((in[1]>>44)&mask + minv)
	out[28] = uint64((in[1]>>48)&mask + minv)
	out[29] = uint64((in[1]>>52)&mask + minv)
	out[30] = uint64((in[1]>>56)&mask + minv)
	out[31] = uint64((in[1]>>60)&mask + minv)
	out[32] = uint64(in[2]&mask + minv)
	out[33] = uint64((in[2]>>4)&mask + minv)
	out[34] = uint64((in[2]>>8)&mask + minv)
	out[35] = uint64((in[2]>>12)&mask + minv)
	out[36] = uint64((in[2]>>16)&mask + minv)
	out[37] = uint64((in[2]>>20)&mask + minv)
	out[38] = uint64((in[2]>>24)&mask + minv)
	out[39] = uint64((in[2]>>28)&mask + minv)
	out[40] = uint64((in[2]>>32)&mask + minv)
	out[41] = uint64((in[2]>>36)&mask + minv)
	out[42] = uint64((in[2]>>40)&mask + minv)
	out[43] = uint64((in[2]>>44)&mask + minv)
	out[44] = uint64((in[2]>>48)&mask + minv)
	out[45] = uint64((in[2]>>52)&mask + minv)
	out[46] = uint64((in[2]>>56)&mask + minv)
	out[47] = uint64((in[2]>>60)&mask + minv)
	out[48] = uint64(in[3]&mask + minv)
	out[49] = uint64((in[3]>>4)&mask + minv)
	out[50] = uint64((in[3]>>8)&mask + minv)
	out[51] = uint64((in[3]>>12)&mask + minv)
	out[52] = uint64((in[3]>>16)&mask + minv)
	out[53] = uint64((in[3]>>20)&mask + minv)
	out[54] = uint64((in[3]>>24)&mask + minv)
	out[55] = uint64((in[3]>>28)&mask + minv)
	out[56] = uint64((in[3]>>32)&mask + minv)
	out[57] = uint64((in[3]>>36)&mask + minv)
	out[58] = uint64((in[3]>>40)&mask + minv)
	out[59] = uint64((in[3]>>44)&mask + minv)
	out[60] = uint64((in[3]>>48)&mask + minv)
	out[61] = uint64((in[3]>>52)&mask + minv)
	out[62] = uint64((in[3]>>56)&mask + minv)
	out[63] = uint64((in[3]>>60)&mask + minv)
}

func br64_5(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[5]uint64)(p)
	mask := uint64((1 << 5) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>5)&mask + minv)
	out[2] = uint64((in[0]>>10)&mask + minv)
	out[3] = uint64((in[0]>>15)&mask + minv)
	out[4] = uint64((in[0]>>20)&mask + minv)
	out[5] = uint64((in[0]>>25)&mask + minv)
	out[6] = uint64((in[0]>>30)&mask + minv)
	out[7] = uint64((in[0]>>35)&mask + minv)
	out[8] = uint64((in[0]>>40)&mask + minv)
	out[9] = uint64((in[0]>>45)&mask + minv)
	out[10] = uint64((in[0]>>50)&mask + minv)
	out[11] = uint64((in[0]>>55)&mask + minv)
	out[12] = uint64((in[0]>>60|in[1]<<4)&mask + minv)
	out[13] = uint64((in[1]>>1)&mask + minv)
	out[14] = uint64((in[1]>>6)&mask + minv)
	out[15] = uint64((in[1]>>11)&mask + minv)
	out[16] = uint64((in[1]>>16)&mask + minv)
	out[17] = uint64((in[1]>>21)&mask + minv)
	out[18] = uint64((in[1]>>26)&mask + minv)
	out[19] = uint64((in[1]>>31)&mask + minv)
	out[20] = uint64((in[1]>>36)&mask + minv)
	out[21] = uint64((in[1]>>41)&mask + minv)
	out[22] = uint64((in[1]>>46)&mask + minv)
	out[23] = uint64((in[1]>>51)&mask + minv)
	out[24] = uint64((in[1]>>56)&mask + minv)
	out[25] = uint64((in[1]>>61|in[2]<<3)&mask + minv)
	out[26] = uint64((in[2]>>2)&mask + minv)
	out[27] = uint64((in[2]>>7)&mask + minv)
	out[28] = uint64((in[2]>>12)&mask + minv)
	out[29] = uint64((in[2]>>17)&mask + minv)
	out[30] = uint64((in[2]>>22)&mask + minv)
	out[31] = uint64((in[2]>>27)&mask + minv)
	out[32] = uint64((in[2]>>32)&mask + minv)
	out[33] = uint64((in[2]>>37)&mask + minv)
	out[34] = uint64((in[2]>>42)&mask + minv)
	out[35] = uint64((in[2]>>47)&mask + minv)
	out[36] = uint64((in[2]>>52)&mask + minv)
	out[37] = uint64((in[2]>>57)&mask + minv)
	out[38] = uint64((in[2]>>62|in[3]<<2)&mask + minv)
	out[39] = uint64((in[3]>>3)&mask + minv)
	out[40] = uint64((in[3]>>8)&mask + minv)
	out[41] = uint64((in[3]>>13)&mask + minv)
	out[42] = uint64((in[3]>>18)&mask + minv)
	out[43] = uint64((in[3]>>23)&mask + minv)
	out[44] = uint64((in[3]>>28)&mask + minv)
	out[45] = uint64((in[3]>>33)&mask + minv)
	out[46] = uint64((in[3]>>38)&mask + minv)
	out[47] = uint64((in[3]>>43)&mask + minv)
	out[48] = uint64((in[3]>>48)&mask + minv)
	out[49] = uint64((in[3]>>53)&mask + minv)
	out[50] = uint64((in[3]>>58)&mask + minv)
	out[51] = uint64((in[3]>>63|in[4]<<1)&mask + minv)
	out[52] = uint64((in[4]>>4)&mask + minv)
	out[53] = uint64((in[4]>>9)&mask + minv)
	out[54] = uint64((in[4]>>14)&mask + minv)
	out[55] = uint64((in[4]>>19)&mask + minv)
	out[56] = uint64((in[4]>>24)&mask + minv)
	out[57] = uint64((in[4]>>29)&mask + minv)
	out[58] = uint64((in[4]>>34)&mask + minv)
	out[59] = uint64((in[4]>>39)&mask + minv)
	out[60] = uint64((in[4]>>44)&mask + minv)
	out[61] = uint64((in[4]>>49)&mask + minv)
	out[62] = uint64((in[4]>>54)&mask + minv)
	out[63] = uint64((in[4]>>59)&mask + minv)
}

func br64_6(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[6]uint64)(p)
	mask := uint64((1 << 6) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>6)&mask + minv)
	out[2] = uint64((in[0]>>12)&mask + minv)
	out[3] = uint64((in[0]>>18)&mask + minv)
	out[4] = uint64((in[0]>>24)&mask + minv)
	out[5] = uint64((in[0]>>30)&mask + minv)
	out[6] = uint64((in[0]>>36)&mask + minv)
	out[7] = uint64((in[0]>>42)&mask + minv)
	out[8] = uint64((in[0]>>48)&mask + minv)
	out[9] = uint64((in[0]>>54)&mask + minv)
	out[10] = uint64((in[0]>>60|in[1]<<4)&mask + minv)
	out[11] = uint64((in[1]>>2)&mask + minv)
	out[12] = uint64((in[1]>>8)&mask + minv)
	out[13] = uint64((in[1]>>14)&mask + minv)
	out[14] = uint64((in[1]>>20)&mask + minv)
	out[15] = uint64((in[1]>>26)&mask + minv)
	out[16] = uint64((in[1]>>32)&mask + minv)
	out[17] = uint64((in[1]>>38)&mask + minv)
	out[18] = uint64((in[1]>>44)&mask + minv)
	out[19] = uint64((in[1]>>50)&mask + minv)
	out[20] = uint64((in[1]>>56)&mask + minv)
	out[21] = uint64((in[1]>>62|in[2]<<2)&mask + minv)
	out[22] = uint64((in[2]>>4)&mask + minv)
	out[23] = uint64((in[2]>>10)&mask + minv)
	out[24] = uint64((in[2]>>16)&mask + minv)
	out[25] = uint64((in[2]>>22)&mask + minv)
	out[26] = uint64((in[2]>>28)&mask + minv)
	out[27] = uint64((in[2]>>34)&mask + minv)
	out[28] = uint64((in[2]>>40)&mask + minv)
	out[29] = uint64((in[2]>>46)&mask + minv)
	out[30] = uint64((in[2]>>52)&mask + minv)
	out[31] = uint64((in[2]>>58)&mask + minv)
	out[32] = uint64(in[3]&mask + minv)
	out[33] = uint64((in[3]>>6)&mask + minv)
	out[34] = uint64((in[3]>>12)&mask + minv)
	out[35] = uint64((in[3]>>18)&mask + minv)
	out[36] = uint64((in[3]>>24)&mask + minv)
	out[37] = uint64((in[3]>>30)&mask + minv)
	out[38] = uint64((in[3]>>36)&mask + minv)
	out[39] = uint64((in[3]>>42)&mask + minv)
	out[40] = uint64((in[3]>>48)&mask + minv)
	out[41] = uint64((in[3]>>54)&mask + minv)
	out[42] = uint64((in[3]>>60|in[4]<<4)&mask + minv)
	out[43] = uint64((in[4]>>2)&mask + minv)
	out[44] = uint64((in[4]>>8)&mask + minv)
	out[45] = uint64((in[4]>>14)&mask + minv)
	out[46] = uint64((in[4]>>20)&mask + minv)
	out[47] = uint64((in[4]>>26)&mask + minv)
	out[48] = uint64((in[4]>>32)&mask + minv)
	out[49] = uint64((in[4]>>38)&mask + minv)
	out[50] = uint64((in[4]>>44)&mask + minv)
	out[51] = uint64((in[4]>>50)&mask + minv)
	out[52] = uint64((in[4]>>56)&mask + minv)
	out[53] = uint64((in[4]>>62|in[5]<<2)&mask + minv)
	out[54] = uint64((in[5]>>4)&mask + minv)
	out[55] = uint64((in[5]>>10)&mask + minv)
	out[56] = uint64((in[5]>>16)&mask + minv)
	out[57] = uint64((in[5]>>22)&mask + minv)
	out[58] = uint64((in[5]>>28)&mask + minv)
	out[59] = uint64((in[5]>>34)&mask + minv)
	out[60] = uint64((in[5]>>40)&mask + minv)
	out[61] = uint64((in[5]>>46)&mask + minv)
	out[62] = uint64((in[5]>>52)&mask + minv)
	out[63] = uint64((in[5]>>58)&mask + minv)
}

func br64_7(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[7]uint64)(p)
	mask := uint64((1 << 7) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>7)&mask + minv)
	out[2] = uint64((in[0]>>14)&mask + minv)
	out[3] = uint64((in[0]>>21)&mask + minv)
	out[4] = uint64((in[0]>>28)&mask + minv)
	out[5] = uint64((in[0]>>35)&mask + minv)
	out[6] = uint64((in[0]>>42)&mask + minv)
	out[7] = uint64((in[0]>>49)&mask + minv)
	out[8] = uint64((in[0]>>56)&mask + minv)
	out[9] = uint64((in[0]>>63|in[1]<<1)&mask + minv)
	out[10] = uint64((in[1]>>6)&mask + minv)
	out[11] = uint64((in[1]>>13)&mask + minv)
	out[12] = uint64((in[1]>>20)&mask + minv)
	out[13] = uint64((in[1]>>27)&mask + minv)
	out[14] = uint64((in[1]>>34)&mask + minv)
	out[15] = uint64((in[1]>>41)&mask + minv)
	out[16] = uint64((in[1]>>48)&mask + minv)
	out[17] = uint64((in[1]>>55)&mask + minv)
	out[18] = uint64((in[1]>>62|in[2]<<2)&mask + minv)
	out[19] = uint64((in[2]>>5)&mask + minv)
	out[20] = uint64((in[2]>>12)&mask + minv)
	out[21] = uint64((in[2]>>19)&mask + minv)
	out[22] = uint64((in[2]>>26)&mask + minv)
	out[23] = uint64((in[2]>>33)&mask + minv)
	out[24] = uint64((in[2]>>40)&mask + minv)
	out[25] = uint64((in[2]>>47)&mask + minv)
	out[26] = uint64((in[2]>>54)&mask + minv)
	out[27] = uint64((in[2]>>61|in[3]<<3)&mask + minv)
	out[28] = uint64((in[3]>>4)&mask + minv)
	out[29] = uint64((in[3]>>11)&mask + minv)
	out[30] = uint64((in[3]>>18)&mask + minv)
	out[31] = uint64((in[3]>>25)&mask + minv)
	out[32] = uint64((in[3]>>32)&mask + minv)
	out[33] = uint64((in[3]>>39)&mask + minv)
	out[34] = uint64((in[3]>>46)&mask + minv)
	out[35] = uint64((in[3]>>53)&mask + minv)
	out[36] = uint64((in[3]>>60|in[4]<<4)&mask + minv)
	out[37] = uint64((in[4]>>3)&mask + minv)
	out[38] = uint64((in[4]>>10)&mask + minv)
	out[39] = uint64((in[4]>>17)&mask + minv)
	out[40] = uint64((in[4]>>24)&mask + minv)
	out[41] = uint64((in[4]>>31)&mask + minv)
	out[42] = uint64((in[4]>>38)&mask + minv)
	out[43] = uint64((in[4]>>45)&mask + minv)
	out[44] = uint64((in[4]>>52)&mask + minv)
	out[45] = uint64((in[4]>>59|in[5]<<5)&mask + minv)
	out[46] = uint64((in[5]>>2)&mask + minv)
	out[47] = uint64((in[5]>>9)&mask + minv)
	out[48] = uint64((in[5]>>16)&mask + minv)
	out[49] = uint64((in[5]>>23)&mask + minv)
	out[50] = uint64((in[5]>>30)&mask + minv)
	out[51] = uint64((in[5]>>37)&mask + minv)
	out[52] = uint64((in[5]>>44)&mask + minv)
	out[53] = uint64((in[5]>>51)&mask + minv)
	out[54] = uint64((in[5]>>58|in[6]<<6)&mask + minv)
	out[55] = uint64((in[6]>>1)&mask + minv)
	out[56] = uint64((in[6]>>8)&mask + minv)
	out[57] = uint64((in[6]>>15)&mask + minv)
	out[58] = uint64((in[6]>>22)&mask + minv)
	out[59] = uint64((in[6]>>29)&mask + minv)
	out[60] = uint64((in[6]>>36)&mask + minv)
	out[61] = uint64((in[6]>>43)&mask + minv)
	out[62] = uint64((in[6]>>50)&mask + minv)
	out[63] = uint64((in[6]>>57)&mask + minv)
}

func br64_8(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[8]uint64)(p)
	mask := uint64((1 << 8) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>8)&mask + minv)
	out[2] = uint64((in[0]>>16)&mask + minv)
	out[3] = uint64((in[0]>>24)&mask + minv)
	out[4] = uint64((in[0]>>32)&mask + minv)
	out[5] = uint64((in[0]>>40)&mask + minv)
	out[6] = uint64((in[0]>>48)&mask + minv)
	out[7] = uint64((in[0]>>56)&mask + minv)
	out[8] = uint64(in[1]&mask + minv)
	out[9] = uint64((in[1]>>8)&mask + minv)
	out[10] = uint64((in[1]>>16)&mask + minv)
	out[11] = uint64((in[1]>>24)&mask + minv)
	out[12] = uint64((in[1]>>32)&mask + minv)
	out[13] = uint64((in[1]>>40)&mask + minv)
	out[14] = uint64((in[1]>>48)&mask + minv)
	out[15] = uint64((in[1]>>56)&mask + minv)
	out[16] = uint64(in[2]&mask + minv)
	out[17] = uint64((in[2]>>8)&mask + minv)
	out[18] = uint64((in[2]>>16)&mask + minv)
	out[19] = uint64((in[2]>>24)&mask + minv)
	out[20] = uint64((in[2]>>32)&mask + minv)
	out[21] = uint64((in[2]>>40)&mask + minv)
	out[22] = uint64((in[2]>>48)&mask + minv)
	out[23] = uint64((in[2]>>56)&mask + minv)
	out[24] = uint64(in[3]&mask + minv)
	out[25] = uint64((in[3]>>8)&mask + minv)
	out[26] = uint64((in[3]>>16)&mask + minv)
	out[27] = uint64((in[3]>>24)&mask + minv)
	out[28] = uint64((in[3]>>32)&mask + minv)
	out[29] = uint64((in[3]>>40)&mask + minv)
	out[30] = uint64((in[3]>>48)&mask + minv)
	out[31] = uint64((in[3]>>56)&mask + minv)
	out[32] = uint64(in[4]&mask + minv)
	out[33] = uint64((in[4]>>8)&mask + minv)
	out[34] = uint64((in[4]>>16)&mask + minv)
	out[35] = uint64((in[4]>>24)&mask + minv)
	out[36] = uint64((in[4]>>32)&mask + minv)
	out[37] = uint64((in[4]>>40)&mask + minv)
	out[38] = uint64((in[4]>>48)&mask + minv)
	out[39] = uint64((in[4]>>56)&mask + minv)
	out[40] = uint64(in[5]&mask + minv)
	out[41] = uint64((in[5]>>8)&mask + minv)
	out[42] = uint64((in[5]>>16)&mask + minv)
	out[43] = uint64((in[5]>>24)&mask + minv)
	out[44] = uint64((in[5]>>32)&mask + minv)
	out[45] = uint64((in[5]>>40)&mask + minv)
	out[46] = uint64((in[5]>>48)&mask + minv)
	out[47] = uint64((in[5]>>56)&mask + minv)
	out[48] = uint64(in[6]&mask + minv)
	out[49] = uint64((in[6]>>8)&mask + minv)
	out[50] = uint64((in[6]>>16)&mask + minv)
	out[51] = uint64((in[6]>>24)&mask + minv)
	out[52] = uint64((in[6]>>32)&mask + minv)
	out[53] = uint64((in[6]>>40)&mask + minv)
	out[54] = uint64((in[6]>>48)&mask + minv)
	out[55] = uint64((in[6]>>56)&mask + minv)
	out[56] = uint64(in[7]&mask + minv)
	out[57] = uint64((in[7]>>8)&mask + minv)
	out[58] = uint64((in[7]>>16)&mask + minv)
	out[59] = uint64((in[7]>>24)&mask + minv)
	out[60] = uint64((in[7]>>32)&mask + minv)
	out[61] = uint64((in[7]>>40)&mask + minv)
	out[62] = uint64((in[7]>>48)&mask + minv)
	out[63] = uint64((in[7]>>56)&mask + minv)
}

func br64_9(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[9]uint64)(p)
	mask := uint64((1 << 9) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>9)&mask + minv)
	out[2] = uint64((in[0]>>18)&mask + minv)
	out[3] = uint64((in[0]>>27)&mask + minv)
	out[4] = uint64((in[0]>>36)&mask + minv)
	out[5] = uint64((in[0]>>45)&mask + minv)
	out[6] = uint64((in[0]>>54)&mask + minv)
	out[7] = uint64((in[0]>>63|in[1]<<1)&mask + minv)
	out[8] = uint64((in[1]>>8)&mask + minv)
	out[9] = uint64((in[1]>>17)&mask + minv)
	out[10] = uint64((in[1]>>26)&mask + minv)
	out[11] = uint64((in[1]>>35)&mask + minv)
	out[12] = uint64((in[1]>>44)&mask + minv)
	out[13] = uint64((in[1]>>53)&mask + minv)
	out[14] = uint64((in[1]>>62|in[2]<<2)&mask + minv)
	out[15] = uint64((in[2]>>7)&mask + minv)
	out[16] = uint64((in[2]>>16)&mask + minv)
	out[17] = uint64((in[2]>>25)&mask + minv)
	out[18] = uint64((in[2]>>34)&mask + minv)
	out[19] = uint64((in[2]>>43)&mask + minv)
	out[20] = uint64((in[2]>>52)&mask + minv)
	out[21] = uint64((in[2]>>61|in[3]<<3)&mask + minv)
	out[22] = uint64((in[3]>>6)&mask + minv)
	out[23] = uint64((in[3]>>15)&mask + minv)
	out[24] = uint64((in[3]>>24)&mask + minv)
	out[25] = uint64((in[3]>>33)&mask + minv)
	out[26] = uint64((in[3]>>42)&mask + minv)
	out[27] = uint64((in[3]>>51)&mask + minv)
	out[28] = uint64((in[3]>>60|in[4]<<4)&mask + minv)
	out[29] = uint64((in[4]>>5)&mask + minv)
	out[30] = uint64((in[4]>>14)&mask + minv)
	out[31] = uint64((in[4]>>23)&mask + minv)
	out[32] = uint64((in[4]>>32)&mask + minv)
	out[33] = uint64((in[4]>>41)&mask + minv)
	out[34] = uint64((in[4]>>50)&mask + minv)
	out[35] = uint64((in[4]>>59|in[5]<<5)&mask + minv)
	out[36] = uint64((in[5]>>4)&mask + minv)
	out[37] = uint64((in[5]>>13)&mask + minv)
	out[38] = uint64((in[5]>>22)&mask + minv)
	out[39] = uint64((in[5]>>31)&mask + minv)
	out[40] = uint64((in[5]>>40)&mask + minv)
	out[41] = uint64((in[5]>>49)&mask + minv)
	out[42] = uint64((in[5]>>58|in[6]<<6)&mask + minv)
	out[43] = uint64((in[6]>>3)&mask + minv)
	out[44] = uint64((in[6]>>12)&mask + minv)
	out[45] = uint64((in[6]>>21)&mask + minv)
	out[46] = uint64((in[6]>>30)&mask + minv)
	out[47] = uint64((in[6]>>39)&mask + minv)
	out[48] = uint64((in[6]>>48)&mask + minv)
	out[49] = uint64((in[6]>>57|in[7]<<7)&mask + minv)
	out[50] = uint64((in[7]>>2)&mask + minv)
	out[51] = uint64((in[7]>>11)&mask + minv)
	out[52] = uint64((in[7]>>20)&mask + minv)
	out[53] = uint64((in[7]>>29)&mask + minv)
	out[54] = uint64((in[7]>>38)&mask + minv)
	out[55] = uint64((in[7]>>47)&mask + minv)
	out[56] = uint64((in[7]>>56|in[8]<<8)&mask + minv)
	out[57] = uint64((in[8]>>1)&mask + minv)
	out[58] = uint64((in[8]>>10)&mask + minv)
	out[59] = uint64((in[8]>>19)&mask + minv)
	out[60] = uint64((in[8]>>28)&mask + minv)
	out[61] = uint64((in[8]>>37)&mask + minv)
	out[62] = uint64((in[8]>>46)&mask + minv)
	out[63] = uint64((in[8]>>55)&mask + minv)
}

func br64_10(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[10]uint64)(p)
	mask := uint64((1 << 10) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>10)&mask + minv)
	out[2] = uint64((in[0]>>20)&mask + minv)
	out[3] = uint64((in[0]>>30)&mask + minv)
	out[4] = uint64((in[0]>>40)&mask + minv)
	out[5] = uint64((in[0]>>50)&mask + minv)
	out[6] = uint64((in[0]>>60|in[1]<<4)&mask + minv)
	out[7] = uint64((in[1]>>6)&mask + minv)
	out[8] = uint64((in[1]>>16)&mask + minv)
	out[9] = uint64((in[1]>>26)&mask + minv)
	out[10] = uint64((in[1]>>36)&mask + minv)
	out[11] = uint64((in[1]>>46)&mask + minv)
	out[12] = uint64((in[1]>>56|in[2]<<8)&mask + minv)
	out[13] = uint64((in[2]>>2)&mask + minv)
	out[14] = uint64((in[2]>>12)&mask + minv)
	out[15] = uint64((in[2]>>22)&mask + minv)
	out[16] = uint64((in[2]>>32)&mask + minv)
	out[17] = uint64((in[2]>>42)&mask + minv)
	out[18] = uint64((in[2]>>52)&mask + minv)
	out[19] = uint64((in[2]>>62|in[3]<<2)&mask + minv)
	out[20] = uint64((in[3]>>8)&mask + minv)
	out[21] = uint64((in[3]>>18)&mask + minv)
	out[22] = uint64((in[3]>>28)&mask + minv)
	out[23] = uint64((in[3]>>38)&mask + minv)
	out[24] = uint64((in[3]>>48)&mask + minv)
	out[25] = uint64((in[3]>>58|in[4]<<6)&mask + minv)
	out[26] = uint64((in[4]>>4)&mask + minv)
	out[27] = uint64((in[4]>>14)&mask + minv)
	out[28] = uint64((in[4]>>24)&mask + minv)
	out[29] = uint64((in[4]>>34)&mask + minv)
	out[30] = uint64((in[4]>>44)&mask + minv)
	out[31] = uint64((in[4]>>54)&mask + minv)
	out[32] = uint64(in[5]&mask + minv)
	out[33] = uint64((in[5]>>10)&mask + minv)
	out[34] = uint64((in[5]>>20)&mask + minv)
	out[35] = uint64((in[5]>>30)&mask + minv)
	out[36] = uint64((in[5]>>40)&mask + minv)
	out[37] = uint64((in[5]>>50)&mask + minv)
	out[38] = uint64((in[5]>>60|in[6]<<4)&mask + minv)
	out[39] = uint64((in[6]>>6)&mask + minv)
	out[40] = uint64((in[6]>>16)&mask + minv)
	out[41] = uint64((in[6]>>26)&mask + minv)
	out[42] = uint64((in[6]>>36)&mask + minv)
	out[43] = uint64((in[6]>>46)&mask + minv)
	out[44] = uint64((in[6]>>56|in[7]<<8)&mask + minv)
	out[45] = uint64((in[7]>>2)&mask + minv)
	out[46] = uint64((in[7]>>12)&mask + minv)
	out[47] = uint64((in[7]>>22)&mask + minv)
	out[48] = uint64((in[7]>>32)&mask + minv)
	out[49] = uint64((in[7]>>42)&mask + minv)
	out[50] = uint64((in[7]>>52)&mask + minv)
	out[51] = uint64((in[7]>>62|in[8]<<2)&mask + minv)
	out[52] = uint64((in[8]>>8)&mask + minv)
	out[53] = uint64((in[8]>>18)&mask + minv)
	out[54] = uint64((in[8]>>28)&mask + minv)
	out[55] = uint64((in[8]>>38)&mask + minv)
	out[56] = uint64((in[8]>>48)&mask + minv)
	out[57] = uint64((in[8]>>58|in[9]<<6)&mask + minv)
	out[58] = uint64((in[9]>>4)&mask + minv)
	out[59] = uint64((in[9]>>14)&mask + minv)
	out[60] = uint64((in[9]>>24)&mask + minv)
	out[61] = uint64((in[9]>>34)&mask + minv)
	out[62] = uint64((in[9]>>44)&mask + minv)
	out[63] = uint64((in[9]>>54)&mask + minv)
}

func br64_11(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[11]uint64)(p)
	mask := uint64((1 << 11) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>11)&mask + minv)
	out[2] = uint64((in[0]>>22)&mask + minv)
	out[3] = uint64((in[0]>>33)&mask + minv)
	out[4] = uint64((in[0]>>44)&mask + minv)
	out[5] = uint64((in[0]>>55|in[1]<<9)&mask + minv)
	out[6] = uint64((in[1]>>2)&mask + minv)
	out[7] = uint64((in[1]>>13)&mask + minv)
	out[8] = uint64((in[1]>>24)&mask + minv)
	out[9] = uint64((in[1]>>35)&mask + minv)
	out[10] = uint64((in[1]>>46)&mask + minv)
	out[11] = uint64((in[1]>>57|in[2]<<7)&mask + minv)
	out[12] = uint64((in[2]>>4)&mask + minv)
	out[13] = uint64((in[2]>>15)&mask + minv)
	out[14] = uint64((in[2]>>26)&mask + minv)
	out[15] = uint64((in[2]>>37)&mask + minv)
	out[16] = uint64((in[2]>>48)&mask + minv)
	out[17] = uint64((in[2]>>59|in[3]<<5)&mask + minv)
	out[18] = uint64((in[3]>>6)&mask + minv)
	out[19] = uint64((in[3]>>17)&mask + minv)
	out[20] = uint64((in[3]>>28)&mask + minv)
	out[21] = uint64((in[3]>>39)&mask + minv)
	out[22] = uint64((in[3]>>50)&mask + minv)
	out[23] = uint64((in[3]>>61|in[4]<<3)&mask + minv)
	out[24] = uint64((in[4]>>8)&mask + minv)
	out[25] = uint64((in[4]>>19)&mask + minv)
	out[26] = uint64((in[4]>>30)&mask + minv)
	out[27] = uint64((in[4]>>41)&mask + minv)
	out[28] = uint64((in[4]>>52)&mask + minv)
	out[29] = uint64((in[4]>>63|in[5]<<1)&mask + minv)
	out[30] = uint64((in[5]>>10)&mask + minv)
	out[31] = uint64((in[5]>>21)&mask + minv)
	out[32] = uint64((in[5]>>32)&mask + minv)
	out[33] = uint64((in[5]>>43)&mask + minv)
	out[34] = uint64((in[5]>>54|in[6]<<10)&mask + minv)
	out[35] = uint64((in[6]>>1)&mask + minv)
	out[36] = uint64((in[6]>>12)&mask + minv)
	out[37] = uint64((in[6]>>23)&mask + minv)
	out[38] = uint64((in[6]>>34)&mask + minv)
	out[39] = uint64((in[6]>>45)&mask + minv)
	out[40] = uint64((in[6]>>56|in[7]<<8)&mask + minv)
	out[41] = uint64((in[7]>>3)&mask + minv)
	out[42] = uint64((in[7]>>14)&mask + minv)
	out[43] = uint64((in[7]>>25)&mask + minv)
	out[44] = uint64((in[7]>>36)&mask + minv)
	out[45] = uint64((in[7]>>47)&mask + minv)
	out[46] = uint64((in[7]>>58|in[8]<<6)&mask + minv)
	out[47] = uint64((in[8]>>5)&mask + minv)
	out[48] = uint64((in[8]>>16)&mask + minv)
	out[49] = uint64((in[8]>>27)&mask + minv)
	out[50] = uint64((in[8]>>38)&mask + minv)
	out[51] = uint64((in[8]>>49)&mask + minv)
	out[52] = uint64((in[8]>>60|in[9]<<4)&mask + minv)
	out[53] = uint64((in[9]>>7)&mask + minv)
	out[54] = uint64((in[9]>>18)&mask + minv)
	out[55] = uint64((in[9]>>29)&mask + minv)
	out[56] = uint64((in[9]>>40)&mask + minv)
	out[57] = uint64((in[9]>>51)&mask + minv)
	out[58] = uint64((in[9]>>62|in[10]<<2)&mask + minv)
	out[59] = uint64((in[10]>>9)&mask + minv)
	out[60] = uint64((in[10]>>20)&mask + minv)
	out[61] = uint64((in[10]>>31)&mask + minv)
	out[62] = uint64((in[10]>>42)&mask + minv)
	out[63] = uint64((in[10]>>53)&mask + minv)
}

func br64_12(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[12]uint64)(p)
	mask := uint64((1 << 12) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>12)&mask + minv)
	out[2] = uint64((in[0]>>24)&mask + minv)
	out[3] = uint64((in[0]>>36)&mask + minv)
	out[4] = uint64((in[0]>>48)&mask + minv)
	out[5] = uint64((in[0]>>60|in[1]<<4)&mask + minv)
	out[6] = uint64((in[1]>>8)&mask + minv)
	out[7] = uint64((in[1]>>20)&mask + minv)
	out[8] = uint64((in[1]>>32)&mask + minv)
	out[9] = uint64((in[1]>>44)&mask + minv)
	out[10] = uint64((in[1]>>56|in[2]<<8)&mask + minv)
	out[11] = uint64((in[2]>>4)&mask + minv)
	out[12] = uint64((in[2]>>16)&mask + minv)
	out[13] = uint64((in[2]>>28)&mask + minv)
	out[14] = uint64((in[2]>>40)&mask + minv)
	out[15] = uint64((in[2]>>52)&mask + minv)
	out[16] = uint64(in[3]&mask + minv)
	out[17] = uint64((in[3]>>12)&mask + minv)
	out[18] = uint64((in[3]>>24)&mask + minv)
	out[19] = uint64((in[3]>>36)&mask + minv)
	out[20] = uint64((in[3]>>48)&mask + minv)
	out[21] = uint64((in[3]>>60|in[4]<<4)&mask + minv)
	out[22] = uint64((in[4]>>8)&mask + minv)
	out[23] = uint64((in[4]>>20)&mask + minv)
	out[24] = uint64((in[4]>>32)&mask + minv)
	out[25] = uint64((in[4]>>44)&mask + minv)
	out[26] = uint64((in[4]>>56|in[5]<<8)&mask + minv)
	out[27] = uint64((in[5]>>4)&mask + minv)
	out[28] = uint64((in[5]>>16)&mask + minv)
	out[29] = uint64((in[5]>>28)&mask + minv)
	out[30] = uint64((in[5]>>40)&mask + minv)
	out[31] = uint64((in[5]>>52)&mask + minv)
	out[32] = uint64(in[6]&mask + minv)
	out[33] = uint64((in[6]>>12)&mask + minv)
	out[34] = uint64((in[6]>>24)&mask + minv)
	out[35] = uint64((in[6]>>36)&mask + minv)
	out[36] = uint64((in[6]>>48)&mask + minv)
	out[37] = uint64((in[6]>>60|in[7]<<4)&mask + minv)
	out[38] = uint64((in[7]>>8)&mask + minv)
	out[39] = uint64((in[7]>>20)&mask + minv)
	out[40] = uint64((in[7]>>32)&mask + minv)
	out[41] = uint64((in[7]>>44)&mask + minv)
	out[42] = uint64((in[7]>>56|in[8]<<8)&mask + minv)
	out[43] = uint64((in[8]>>4)&mask + minv)
	out[44] = uint64((in[8]>>16)&mask + minv)
	out[45] = uint64((in[8]>>28)&mask + minv)
	out[46] = uint64((in[8]>>40)&mask + minv)
	out[47] = uint64((in[8]>>52)&mask + minv)
	out[48] = uint64(in[9]&mask + minv)
	out[49] = uint64((in[9]>>12)&mask + minv)
	out[50] = uint64((in[9]>>24)&mask + minv)
	out[51] = uint64((in[9]>>36)&mask + minv)
	out[52] = uint64((in[9]>>48)&mask + minv)
	out[53] = uint64((in[9]>>60|in[10]<<4)&mask + minv)
	out[54] = uint64((in[10]>>8)&mask + minv)
	out[55] = uint64((in[10]>>20)&mask + minv)
	out[56] = uint64((in[10]>>32)&mask + minv)
	out[57] = uint64((in[10]>>44)&mask + minv)
	out[58] = uint64((in[10]>>56|in[11]<<8)&mask + minv)
	out[59] = uint64((in[11]>>4)&mask + minv)
	out[60] = uint64((in[11]>>16)&mask + minv)
	out[61] = uint64((in[11]>>28)&mask + minv)
	out[62] = uint64((in[11]>>40)&mask + minv)
	out[63] = uint64((in[11]>>52)&mask + minv)
}

func br64_13(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[13]uint64)(p)
	mask := uint64((1 << 13) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>13)&mask + minv)
	out[2] = uint64((in[0]>>26)&mask + minv)
	out[3] = uint64((in[0]>>39)&mask + minv)
	out[4] = uint64((in[0]>>52|in[1]<<12)&mask + minv)
	out[5] = uint64((in[1]>>1)&mask + minv)
	out[6] = uint64((in[1]>>14)&mask + minv)
	out[7] = uint64((in[1]>>27)&mask + minv)
	out[8] = uint64((in[1]>>40)&mask + minv)
	out[9] = uint64((in[1]>>53|in[2]<<11)&mask + minv)
	out[10] = uint64((in[2]>>2)&mask + minv)
	out[11] = uint64((in[2]>>15)&mask + minv)
	out[12] = uint64((in[2]>>28)&mask + minv)
	out[13] = uint64((in[2]>>41)&mask + minv)
	out[14] = uint64((in[2]>>54|in[3]<<10)&mask + minv)
	out[15] = uint64((in[3]>>3)&mask + minv)
	out[16] = uint64((in[3]>>16)&mask + minv)
	out[17] = uint64((in[3]>>29)&mask + minv)
	out[18] = uint64((in[3]>>42)&mask + minv)
	out[19] = uint64((in[3]>>55|in[4]<<9)&mask + minv)
	out[20] = uint64((in[4]>>4)&mask + minv)
	out[21] = uint64((in[4]>>17)&mask + minv)
	out[22] = uint64((in[4]>>30)&mask + minv)
	out[23] = uint64((in[4]>>43)&mask + minv)
	out[24] = uint64((in[4]>>56|in[5]<<8)&mask + minv)
	out[25] = uint64((in[5]>>5)&mask + minv)
	out[26] = uint64((in[5]>>18)&mask + minv)
	out[27] = uint64((in[5]>>31)&mask + minv)
	out[28] = uint64((in[5]>>44)&mask + minv)
	out[29] = uint64((in[5]>>57|in[6]<<7)&mask + minv)
	out[30] = uint64((in[6]>>6)&mask + minv)
	out[31] = uint64((in[6]>>19)&mask + minv)
	out[32] = uint64((in[6]>>32)&mask + minv)
	out[33] = uint64((in[6]>>45)&mask + minv)
	out[34] = uint64((in[6]>>58|in[7]<<6)&mask + minv)
	out[35] = uint64((in[7]>>7)&mask + minv)
	out[36] = uint64((in[7]>>20)&mask + minv)
	out[37] = uint64((in[7]>>33)&mask + minv)
	out[38] = uint64((in[7]>>46)&mask + minv)
	out[39] = uint64((in[7]>>59|in[8]<<5)&mask + minv)
	out[40] = uint64((in[8]>>8)&mask + minv)
	out[41] = uint64((in[8]>>21)&mask + minv)
	out[42] = uint64((in[8]>>34)&mask + minv)
	out[43] = uint64((in[8]>>47)&mask + minv)
	out[44] = uint64((in[8]>>60|in[9]<<4)&mask + minv)
	out[45] = uint64((in[9]>>9)&mask + minv)
	out[46] = uint64((in[9]>>22)&mask + minv)
	out[47] = uint64((in[9]>>35)&mask + minv)
	out[48] = uint64((in[9]>>48)&mask + minv)
	out[49] = uint64((in[9]>>61|in[10]<<3)&mask + minv)
	out[50] = uint64((in[10]>>10)&mask + minv)
	out[51] = uint64((in[10]>>23)&mask + minv)
	out[52] = uint64((in[10]>>36)&mask + minv)
	out[53] = uint64((in[10]>>49)&mask + minv)
	out[54] = uint64((in[10]>>62|in[11]<<2)&mask + minv)
	out[55] = uint64((in[11]>>11)&mask + minv)
	out[56] = uint64((in[11]>>24)&mask + minv)
	out[57] = uint64((in[11]>>37)&mask + minv)
	out[58] = uint64((in[11]>>50)&mask + minv)
	out[59] = uint64((in[11]>>63|in[12]<<1)&mask + minv)
	out[60] = uint64((in[12]>>12)&mask + minv)
	out[61] = uint64((in[12]>>25)&mask + minv)
	out[62] = uint64((in[12]>>38)&mask + minv)
	out[63] = uint64((in[12]>>51)&mask + minv)
}

func br64_14(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[14]uint64)(p)
	mask := uint64((1 << 14) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>14)&mask + minv)
	out[2] = uint64((in[0]>>28)&mask + minv)
	out[3] = uint64((in[0]>>42)&mask + minv)
	out[4] = uint64((in[0]>>56|in[1]<<8)&mask + minv)
	out[5] = uint64((in[1]>>6)&mask + minv)
	out[6] = uint64((in[1]>>20)&mask + minv)
	out[7] = uint64((in[1]>>34)&mask + minv)
	out[8] = uint64((in[1]>>48)&mask + minv)
	out[9] = uint64((in[1]>>62|in[2]<<2)&mask + minv)
	out[10] = uint64((in[2]>>12)&mask + minv)
	out[11] = uint64((in[2]>>26)&mask + minv)
	out[12] = uint64((in[2]>>40)&mask + minv)
	out[13] = uint64((in[2]>>54|in[3]<<10)&mask + minv)
	out[14] = uint64((in[3]>>4)&mask + minv)
	out[15] = uint64((in[3]>>18)&mask + minv)
	out[16] = uint64((in[3]>>32)&mask + minv)
	out[17] = uint64((in[3]>>46)&mask + minv)
	out[18] = uint64((in[3]>>60|in[4]<<4)&mask + minv)
	out[19] = uint64((in[4]>>10)&mask + minv)
	out[20] = uint64((in[4]>>24)&mask + minv)
	out[21] = uint64((in[4]>>38)&mask + minv)
	out[22] = uint64((in[4]>>52|in[5]<<12)&mask + minv)
	out[23] = uint64((in[5]>>2)&mask + minv)
	out[24] = uint64((in[5]>>16)&mask + minv)
	out[25] = uint64((in[5]>>30)&mask + minv)
	out[26] = uint64((in[5]>>44)&mask + minv)
	out[27] = uint64((in[5]>>58|in[6]<<6)&mask + minv)
	out[28] = uint64((in[6]>>8)&mask + minv)
	out[29] = uint64((in[6]>>22)&mask + minv)
	out[30] = uint64((in[6]>>36)&mask + minv)
	out[31] = uint64((in[6]>>50)&mask + minv)
	out[32] = uint64(in[7]&mask + minv)
	out[33] = uint64((in[7]>>14)&mask + minv)
	out[34] = uint64((in[7]>>28)&mask + minv)
	out[35] = uint64((in[7]>>42)&mask + minv)
	out[36] = uint64((in[7]>>56|in[8]<<8)&mask + minv)
	out[37] = uint64((in[8]>>6)&mask + minv)
	out[38] = uint64((in[8]>>20)&mask + minv)
	out[39] = uint64((in[8]>>34)&mask + minv)
	out[40] = uint64((in[8]>>48)&mask + minv)
	out[41] = uint64((in[8]>>62|in[9]<<2)&mask + minv)
	out[42] = uint64((in[9]>>12)&mask + minv)
	out[43] = uint64((in[9]>>26)&mask + minv)
	out[44] = uint64((in[9]>>40)&mask + minv)
	out[45] = uint64((in[9]>>54|in[10]<<10)&mask + minv)
	out[46] = uint64((in[10]>>4)&mask + minv)
	out[47] = uint64((in[10]>>18)&mask + minv)
	out[48] = uint64((in[10]>>32)&mask + minv)
	out[49] = uint64((in[10]>>46)&mask + minv)
	out[50] = uint64((in[10]>>60|in[11]<<4)&mask + minv)
	out[51] = uint64((in[11]>>10)&mask + minv)
	out[52] = uint64((in[11]>>24)&mask + minv)
	out[53] = uint64((in[11]>>38)&mask + minv)
	out[54] = uint64((in[11]>>52|in[12]<<12)&mask + minv)
	out[55] = uint64((in[12]>>2)&mask + minv)
	out[56] = uint64((in[12]>>16)&mask + minv)
	out[57] = uint64((in[12]>>30)&mask + minv)
	out[58] = uint64((in[12]>>44)&mask + minv)
	out[59] = uint64((in[12]>>58|in[13]<<6)&mask + minv)
	out[60] = uint64((in[13]>>8)&mask + minv)
	out[61] = uint64((in[13]>>22)&mask + minv)
	out[62] = uint64((in[13]>>36)&mask + minv)
	out[63] = uint64((in[13]>>50)&mask + minv)
}

func br64_15(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[15]uint64)(p)
	mask := uint64((1 << 15) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>15)&mask + minv)
	out[2] = uint64((in[0]>>30)&mask + minv)
	out[3] = uint64((in[0]>>45)&mask + minv)
	out[4] = uint64((in[0]>>60|in[1]<<4)&mask + minv)
	out[5] = uint64((in[1]>>11)&mask + minv)
	out[6] = uint64((in[1]>>26)&mask + minv)
	out[7] = uint64((in[1]>>41)&mask + minv)
	out[8] = uint64((in[1]>>56|in[2]<<8)&mask + minv)
	out[9] = uint64((in[2]>>7)&mask + minv)
	out[10] = uint64((in[2]>>22)&mask + minv)
	out[11] = uint64((in[2]>>37)&mask + minv)
	out[12] = uint64((in[2]>>52|in[3]<<12)&mask + minv)
	out[13] = uint64((in[3]>>3)&mask + minv)
	out[14] = uint64((in[3]>>18)&mask + minv)
	out[15] = uint64((in[3]>>33)&mask + minv)
	out[16] = uint64((in[3]>>48)&mask + minv)
	out[17] = uint64((in[3]>>63|in[4]<<1)&mask + minv)
	out[18] = uint64((in[4]>>14)&mask + minv)
	out[19] = uint64((in[4]>>29)&mask + minv)
	out[20] = uint64((in[4]>>44)&mask + minv)
	out[21] = uint64((in[4]>>59|in[5]<<5)&mask + minv)
	out[22] = uint64((in[5]>>10)&mask + minv)
	out[23] = uint64((in[5]>>25)&mask + minv)
	out[24] = uint64((in[5]>>40)&mask + minv)
	out[25] = uint64((in[5]>>55|in[6]<<9)&mask + minv)
	out[26] = uint64((in[6]>>6)&mask + minv)
	out[27] = uint64((in[6]>>21)&mask + minv)
	out[28] = uint64((in[6]>>36)&mask + minv)
	out[29] = uint64((in[6]>>51|in[7]<<13)&mask + minv)
	out[30] = uint64((in[7]>>2)&mask + minv)
	out[31] = uint64((in[7]>>17)&mask + minv)
	out[32] = uint64((in[7]>>32)&mask + minv)
	out[33] = uint64((in[7]>>47)&mask + minv)
	out[34] = uint64((in[7]>>62|in[8]<<2)&mask + minv)
	out[35] = uint64((in[8]>>13)&mask + minv)
	out[36] = uint64((in[8]>>28)&mask + minv)
	out[37] = uint64((in[8]>>43)&mask + minv)
	out[38] = uint64((in[8]>>58|in[9]<<6)&mask + minv)
	out[39] = uint64((in[9]>>9)&mask + minv)
	out[40] = uint64((in[9]>>24)&mask + minv)
	out[41] = uint64((in[9]>>39)&mask + minv)
	out[42] = uint64((in[9]>>54|in[10]<<10)&mask + minv)
	out[43] = uint64((in[10]>>5)&mask + minv)
	out[44] = uint64((in[10]>>20)&mask + minv)
	out[45] = uint64((in[10]>>35)&mask + minv)
	out[46] = uint64((in[10]>>50|in[11]<<14)&mask + minv)
	out[47] = uint64((in[11]>>1)&mask + minv)
	out[48] = uint64((in[11]>>16)&mask + minv)
	out[49] = uint64((in[11]>>31)&mask + minv)
	out[50] = uint64((in[11]>>46)&mask + minv)
	out[51] = uint64((in[11]>>61|in[12]<<3)&mask + minv)
	out[52] = uint64((in[12]>>12)&mask + minv)
	out[53] = uint64((in[12]>>27)&mask + minv)
	out[54] = uint64((in[12]>>42)&mask + minv)
	out[55] = uint64((in[12]>>57|in[13]<<7)&mask + minv)
	out[56] = uint64((in[13]>>8)&mask + minv)
	out[57] = uint64((in[13]>>23)&mask + minv)
	out[58] = uint64((in[13]>>38)&mask + minv)
	out[59] = uint64((in[13]>>53|in[14]<<11)&mask + minv)
	out[60] = uint64((in[14]>>4)&mask + minv)
	out[61] = uint64((in[14]>>19)&mask + minv)
	out[62] = uint64((in[14]>>34)&mask + minv)
	out[63] = uint64((in[14]>>49)&mask + minv)
}

func br64_16(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[16]uint64)(p)
	mask := uint64((1 << 16) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>16)&mask + minv)
	out[2] = uint64((in[0]>>32)&mask + minv)
	out[3] = uint64((in[0]>>48)&mask + minv)
	out[4] = uint64(in[1]&mask + minv)
	out[5] = uint64((in[1]>>16)&mask + minv)
	out[6] = uint64((in[1]>>32)&mask + minv)
	out[7] = uint64((in[1]>>48)&mask + minv)
	out[8] = uint64(in[2]&mask + minv)
	out[9] = uint64((in[2]>>16)&mask + minv)
	out[10] = uint64((in[2]>>32)&mask + minv)
	out[11] = uint64((in[2]>>48)&mask + minv)
	out[12] = uint64(in[3]&mask + minv)
	out[13] = uint64((in[3]>>16)&mask + minv)
	out[14] = uint64((in[3]>>32)&mask + minv)
	out[15] = uint64((in[3]>>48)&mask + minv)
	out[16] = uint64(in[4]&mask + minv)
	out[17] = uint64((in[4]>>16)&mask + minv)
	out[18] = uint64((in[4]>>32)&mask + minv)
	out[19] = uint64((in[4]>>48)&mask + minv)
	out[20] = uint64(in[5]&mask + minv)
	out[21] = uint64((in[5]>>16)&mask + minv)
	out[22] = uint64((in[5]>>32)&mask + minv)
	out[23] = uint64((in[5]>>48)&mask + minv)
	out[24] = uint64(in[6]&mask + minv)
	out[25] = uint64((in[6]>>16)&mask + minv)
	out[26] = uint64((in[6]>>32)&mask + minv)
	out[27] = uint64((in[6]>>48)&mask + minv)
	out[28] = uint64(in[7]&mask + minv)
	out[29] = uint64((in[7]>>16)&mask + minv)
	out[30] = uint64((in[7]>>32)&mask + minv)
	out[31] = uint64((in[7]>>48)&mask + minv)
	out[32] = uint64(in[8]&mask + minv)
	out[33] = uint64((in[8]>>16)&mask + minv)
	out[34] = uint64((in[8]>>32)&mask + minv)
	out[35] = uint64((in[8]>>48)&mask + minv)
	out[36] = uint64(in[9]&mask + minv)
	out[37] = uint64((in[9]>>16)&mask + minv)
	out[38] = uint64((in[9]>>32)&mask + minv)
	out[39] = uint64((in[9]>>48)&mask + minv)
	out[40] = uint64(in[10]&mask + minv)
	out[41] = uint64((in[10]>>16)&mask + minv)
	out[42] = uint64((in[10]>>32)&mask + minv)
	out[43] = uint64((in[10]>>48)&mask + minv)
	out[44] = uint64(in[11]&mask + minv)
	out[45] = uint64((in[11]>>16)&mask + minv)
	out[46] = uint64((in[11]>>32)&mask + minv)
	out[47] = uint64((in[11]>>48)&mask + minv)
	out[48] = uint64(in[12]&mask + minv)
	out[49] = uint64((in[12]>>16)&mask + minv)
	out[50] = uint64((in[12]>>32)&mask + minv)
	out[51] = uint64((in[12]>>48)&mask + minv)
	out[52] = uint64(in[13]&mask + minv)
	out[53] = uint64((in[13]>>16)&mask + minv)
	out[54] = uint64((in[13]>>32)&mask + minv)
	out[55] = uint64((in[13]>>48)&mask + minv)
	out[56] = uint64(in[14]&mask + minv)
	out[57] = uint64((in[14]>>16)&mask + minv)
	out[58] = uint64((in[14]>>32)&mask + minv)
	out[59] = uint64((in[14]>>48)&mask + minv)
	out[60] = uint64(in[15]&mask + minv)
	out[61] = uint64((in[15]>>16)&mask + minv)
	out[62] = uint64((in[15]>>32)&mask + minv)
	out[63] = uint64((in[15]>>48)&mask + minv)
}

func br64_17(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[17]uint64)(p)
	mask := uint64((1 << 17) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>17)&mask + minv)
	out[2] = uint64((in[0]>>34)&mask + minv)
	out[3] = uint64((in[0]>>51|in[1]<<13)&mask + minv)
	out[4] = uint64((in[1]>>4)&mask + minv)
	out[5] = uint64((in[1]>>21)&mask + minv)
	out[6] = uint64((in[1]>>38)&mask + minv)
	out[7] = uint64((in[1]>>55|in[2]<<9)&mask + minv)
	out[8] = uint64((in[2]>>8)&mask + minv)
	out[9] = uint64((in[2]>>25)&mask + minv)
	out[10] = uint64((in[2]>>42)&mask + minv)
	out[11] = uint64((in[2]>>59|in[3]<<5)&mask + minv)
	out[12] = uint64((in[3]>>12)&mask + minv)
	out[13] = uint64((in[3]>>29)&mask + minv)
	out[14] = uint64((in[3]>>46)&mask + minv)
	out[15] = uint64((in[3]>>63|in[4]<<1)&mask + minv)
	out[16] = uint64((in[4]>>16)&mask + minv)
	out[17] = uint64((in[4]>>33)&mask + minv)
	out[18] = uint64((in[4]>>50|in[5]<<14)&mask + minv)
	out[19] = uint64((in[5]>>3)&mask + minv)
	out[20] = uint64((in[5]>>20)&mask + minv)
	out[21] = uint64((in[5]>>37)&mask + minv)
	out[22] = uint64((in[5]>>54|in[6]<<10)&mask + minv)
	out[23] = uint64((in[6]>>7)&mask + minv)
	out[24] = uint64((in[6]>>24)&mask + minv)
	out[25] = uint64((in[6]>>41)&mask + minv)
	out[26] = uint64((in[6]>>58|in[7]<<6)&mask + minv)
	out[27] = uint64((in[7]>>11)&mask + minv)
	out[28] = uint64((in[7]>>28)&mask + minv)
	out[29] = uint64((in[7]>>45)&mask + minv)
	out[30] = uint64((in[7]>>62|in[8]<<2)&mask + minv)
	out[31] = uint64((in[8]>>15)&mask + minv)
	out[32] = uint64((in[8]>>32)&mask + minv)
	out[33] = uint64((in[8]>>49|in[9]<<15)&mask + minv)
	out[34] = uint64((in[9]>>2)&mask + minv)
	out[35] = uint64((in[9]>>19)&mask + minv)
	out[36] = uint64((in[9]>>36)&mask + minv)
	out[37] = uint64((in[9]>>53|in[10]<<11)&mask + minv)
	out[38] = uint64((in[10]>>6)&mask + minv)
	out[39] = uint64((in[10]>>23)&mask + minv)
	out[40] = uint64((in[10]>>40)&mask + minv)
	out[41] = uint64((in[10]>>57|in[11]<<7)&mask + minv)
	out[42] = uint64((in[11]>>10)&mask + minv)
	out[43] = uint64((in[11]>>27)&mask + minv)
	out[44] = uint64((in[11]>>44)&mask + minv)
	out[45] = uint64((in[11]>>61|in[12]<<3)&mask + minv)
	out[46] = uint64((in[12]>>14)&mask + minv)
	out[47] = uint64((in[12]>>31)&mask + minv)
	out[48] = uint64((in[12]>>48|in[13]<<16)&mask + minv)
	out[49] = uint64((in[13]>>1)&mask + minv)
	out[50] = uint64((in[13]>>18)&mask + minv)
	out[51] = uint64((in[13]>>35)&mask + minv)
	out[52] = uint64((in[13]>>52|in[14]<<12)&mask + minv)
	out[53] = uint64((in[14]>>5)&mask + minv)
	out[54] = uint64((in[14]>>22)&mask + minv)
	out[55] = uint64((in[14]>>39)&mask + minv)
	out[56] = uint64((in[14]>>56|in[15]<<8)&mask + minv)
	out[57] = uint64((in[15]>>9)&mask + minv)
	out[58] = uint64((in[15]>>26)&mask + minv)
	out[59] = uint64((in[15]>>43)&mask + minv)
	out[60] = uint64((in[15]>>60|in[16]<<4)&mask + minv)
	out[61] = uint64((in[16]>>13)&mask + minv)
	out[62] = uint64((in[16]>>30)&mask + minv)
	out[63] = uint64((in[16]>>47)&mask + minv)
}

func br64_18(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[18]uint64)(p)
	mask := uint64((1 << 18) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>18)&mask + minv)
	out[2] = uint64((in[0]>>36)&mask + minv)
	out[3] = uint64((in[0]>>54|in[1]<<10)&mask + minv)
	out[4] = uint64((in[1]>>8)&mask + minv)
	out[5] = uint64((in[1]>>26)&mask + minv)
	out[6] = uint64((in[1]>>44)&mask + minv)
	out[7] = uint64((in[1]>>62|in[2]<<2)&mask + minv)
	out[8] = uint64((in[2]>>16)&mask + minv)
	out[9] = uint64((in[2]>>34)&mask + minv)
	out[10] = uint64((in[2]>>52|in[3]<<12)&mask + minv)
	out[11] = uint64((in[3]>>6)&mask + minv)
	out[12] = uint64((in[3]>>24)&mask + minv)
	out[13] = uint64((in[3]>>42)&mask + minv)
	out[14] = uint64((in[3]>>60|in[4]<<4)&mask + minv)
	out[15] = uint64((in[4]>>14)&mask + minv)
	out[16] = uint64((in[4]>>32)&mask + minv)
	out[17] = uint64((in[4]>>50|in[5]<<14)&mask + minv)
	out[18] = uint64((in[5]>>4)&mask + minv)
	out[19] = uint64((in[5]>>22)&mask + minv)
	out[20] = uint64((in[5]>>40)&mask + minv)
	out[21] = uint64((in[5]>>58|in[6]<<6)&mask + minv)
	out[22] = uint64((in[6]>>12)&mask + minv)
	out[23] = uint64((in[6]>>30)&mask + minv)
	out[24] = uint64((in[6]>>48|in[7]<<16)&mask + minv)
	out[25] = uint64((in[7]>>2)&mask + minv)
	out[26] = uint64((in[7]>>20)&mask + minv)
	out[27] = uint64((in[7]>>38)&mask + minv)
	out[28] = uint64((in[7]>>56|in[8]<<8)&mask + minv)
	out[29] = uint64((in[8]>>10)&mask + minv)
	out[30] = uint64((in[8]>>28)&mask + minv)
	out[31] = uint64((in[8]>>46)&mask + minv)
	out[32] = uint64(in[9]&mask + minv)
	out[33] = uint64((in[9]>>18)&mask + minv)
	out[34] = uint64((in[9]>>36)&mask + minv)
	out[35] = uint64((in[9]>>54|in[10]<<10)&mask + minv)
	out[36] = uint64((in[10]>>8)&mask + minv)
	out[37] = uint64((in[10]>>26)&mask + minv)
	out[38] = uint64((in[10]>>44)&mask + minv)
	out[39] = uint64((in[10]>>62|in[11]<<2)&mask + minv)
	out[40] = uint64((in[11]>>16)&mask + minv)
	out[41] = uint64((in[11]>>34)&mask + minv)
	out[42] = uint64((in[11]>>52|in[12]<<12)&mask + minv)
	out[43] = uint64((in[12]>>6)&mask + minv)
	out[44] = uint64((in[12]>>24)&mask + minv)
	out[45] = uint64((in[12]>>42)&mask + minv)
	out[46] = uint64((in[12]>>60|in[13]<<4)&mask + minv)
	out[47] = uint64((in[13]>>14)&mask + minv)
	out[48] = uint64((in[13]>>32)&mask + minv)
	out[49] = uint64((in[13]>>50|in[14]<<14)&mask + minv)
	out[50] = uint64((in[14]>>4)&mask + minv)
	out[51] = uint64((in[14]>>22)&mask + minv)
	out[52] = uint64((in[14]>>40)&mask + minv)
	out[53] = uint64((in[14]>>58|in[15]<<6)&mask + minv)
	out[54] = uint64((in[15]>>12)&mask + minv)
	out[55] = uint64((in[15]>>30)&mask + minv)
	out[56] = uint64((in[15]>>48|in[16]<<16)&mask + minv)
	out[57] = uint64((in[16]>>2)&mask + minv)
	out[58] = uint64((in[16]>>20)&mask + minv)
	out[59] = uint64((in[16]>>38)&mask + minv)
	out[60] = uint64((in[16]>>56|in[17]<<8)&mask + minv)
	out[61] = uint64((in[17]>>10)&mask + minv)
	out[62] = uint64((in[17]>>28)&mask + minv)
	out[63] = uint64((in[17]>>46)&mask + minv)
}

func br64_19(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[19]uint64)(p)
	mask := uint64((1 << 19) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>19)&mask + minv)
	out[2] = uint64((in[0]>>38)&mask + minv)
	out[3] = uint64((in[0]>>57|in[1]<<7)&mask + minv)
	out[4] = uint64((in[1]>>12)&mask + minv)
	out[5] = uint64((in[1]>>31)&mask + minv)
	out[6] = uint64((in[1]>>50|in[2]<<14)&mask + minv)
	out[7] = uint64((in[2]>>5)&mask + minv)
	out[8] = uint64((in[2]>>24)&mask + minv)
	out[9] = uint64((in[2]>>43)&mask + minv)
	out[10] = uint64((in[2]>>62|in[3]<<2)&mask + minv)
	out[11] = uint64((in[3]>>17)&mask + minv)
	out[12] = uint64((in[3]>>36)&mask + minv)
	out[13] = uint64((in[3]>>55|in[4]<<9)&mask + minv)
	out[14] = uint64((in[4]>>10)&mask + minv)
	out[15] = uint64((in[4]>>29)&mask + minv)
	out[16] = uint64((in[4]>>48|in[5]<<16)&mask + minv)
	out[17] = uint64((in[5]>>3)&mask + minv)
	out[18] = uint64((in[5]>>22)&mask + minv)
	out[19] = uint64((in[5]>>41)&mask + minv)
	out[20] = uint64((in[5]>>60|in[6]<<4)&mask + minv)
	out[21] = uint64((in[6]>>15)&mask + minv)
	out[22] = uint64((in[6]>>34)&mask + minv)
	out[23] = uint64((in[6]>>53|in[7]<<11)&mask + minv)
	out[24] = uint64((in[7]>>8)&mask + minv)
	out[25] = uint64((in[7]>>27)&mask + minv)
	out[26] = uint64((in[7]>>46|in[8]<<18)&mask + minv)
	out[27] = uint64((in[8]>>1)&mask + minv)
	out[28] = uint64((in[8]>>20)&mask + minv)
	out[29] = uint64((in[8]>>39)&mask + minv)
	out[30] = uint64((in[8]>>58|in[9]<<6)&mask + minv)
	out[31] = uint64((in[9]>>13)&mask + minv)
	out[32] = uint64((in[9]>>32)&mask + minv)
	out[33] = uint64((in[9]>>51|in[10]<<13)&mask + minv)
	out[34] = uint64((in[10]>>6)&mask + minv)
	out[35] = uint64((in[10]>>25)&mask + minv)
	out[36] = uint64((in[10]>>44)&mask + minv)
	out[37] = uint64((in[10]>>63|in[11]<<1)&mask + minv)
	out[38] = uint64((in[11]>>18)&mask + minv)
	out[39] = uint64((in[11]>>37)&mask + minv)
	out[40] = uint64((in[11]>>56|in[12]<<8)&mask + minv)
	out[41] = uint64((in[12]>>11)&mask + minv)
	out[42] = uint64((in[12]>>30)&mask + minv)
	out[43] = uint64((in[12]>>49|in[13]<<15)&mask + minv)
	out[44] = uint64((in[13]>>4)&mask + minv)
	out[45] = uint64((in[13]>>23)&mask + minv)
	out[46] = uint64((in[13]>>42)&mask + minv)
	out[47] = uint64((in[13]>>61|in[14]<<3)&mask + minv)
	out[48] = uint64((in[14]>>16)&mask + minv)
	out[49] = uint64((in[14]>>35)&mask + minv)
	out[50] = uint64((in[14]>>54|in[15]<<10)&mask + minv)
	out[51] = uint64((in[15]>>9)&mask + minv)
	out[52] = uint64((in[15]>>28)&mask + minv)
	out[53] = uint64((in[15]>>47|in[16]<<17)&mask + minv)
	out[54] = uint64((in[16]>>2)&mask + minv)
	out[55] = uint64((in[16]>>21)&mask + minv)
	out[56] = uint64((in[16]>>40)&mask + minv)
	out[57] = uint64((in[16]>>59|in[17]<<5)&mask + minv)
	out[58] = uint64((in[17]>>14)&mask + minv)
	out[59] = uint64((in[17]>>33)&mask + minv)
	out[60] = uint64((in[17]>>52|in[18]<<12)&mask + minv)
	out[61] = uint64((in[18]>>7)&mask + minv)
	out[62] = uint64((in[18]>>26)&mask + minv)
	out[63] = uint64((in[18]>>45)&mask + minv)
}

func br64_20(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[20]uint64)(p)
	mask := uint64((1 << 20) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>20)&mask + minv)
	out[2] = uint64((in[0]>>40)&mask + minv)
	out[3] = uint64((in[0]>>60|in[1]<<4)&mask + minv)
	out[4] = uint64((in[1]>>16)&mask + minv)
	out[5] = uint64((in[1]>>36)&mask + minv)
	out[6] = uint64((in[1]>>56|in[2]<<8)&mask + minv)
	out[7] = uint64((in[2]>>12)&mask + minv)
	out[8] = uint64((in[2]>>32)&mask + minv)
	out[9] = uint64((in[2]>>52|in[3]<<12)&mask + minv)
	out[10] = uint64((in[3]>>8)&mask + minv)
	out[11] = uint64((in[3]>>28)&mask + minv)
	out[12] = uint64((in[3]>>48|in[4]<<16)&mask + minv)
	out[13] = uint64((in[4]>>4)&mask + minv)
	out[14] = uint64((in[4]>>24)&mask + minv)
	out[15] = uint64((in[4]>>44)&mask + minv)
	out[16] = uint64(in[5]&mask + minv)
	out[17] = uint64((in[5]>>20)&mask + minv)
	out[18] = uint64((in[5]>>40)&mask + minv)
	out[19] = uint64((in[5]>>60|in[6]<<4)&mask + minv)
	out[20] = uint64((in[6]>>16)&mask + minv)
	out[21] = uint64((in[6]>>36)&mask + minv)
	out[22] = uint64((in[6]>>56|in[7]<<8)&mask + minv)
	out[23] = uint64((in[7]>>12)&mask + minv)
	out[24] = uint64((in[7]>>32)&mask + minv)
	out[25] = uint64((in[7]>>52|in[8]<<12)&mask + minv)
	out[26] = uint64((in[8]>>8)&mask + minv)
	out[27] = uint64((in[8]>>28)&mask + minv)
	out[28] = uint64((in[8]>>48|in[9]<<16)&mask + minv)
	out[29] = uint64((in[9]>>4)&mask + minv)
	out[30] = uint64((in[9]>>24)&mask + minv)
	out[31] = uint64((in[9]>>44)&mask + minv)
	out[32] = uint64(in[10]&mask + minv)
	out[33] = uint64((in[10]>>20)&mask + minv)
	out[34] = uint64((in[10]>>40)&mask + minv)
	out[35] = uint64((in[10]>>60|in[11]<<4)&mask + minv)
	out[36] = uint64((in[11]>>16)&mask + minv)
	out[37] = uint64((in[11]>>36)&mask + minv)
	out[38] = uint64((in[11]>>56|in[12]<<8)&mask + minv)
	out[39] = uint64((in[12]>>12)&mask + minv)
	out[40] = uint64((in[12]>>32)&mask + minv)
	out[41] = uint64((in[12]>>52|in[13]<<12)&mask + minv)
	out[42] = uint64((in[13]>>8)&mask + minv)
	out[43] = uint64((in[13]>>28)&mask + minv)
	out[44] = uint64((in[13]>>48|in[14]<<16)&mask + minv)
	out[45] = uint64((in[14]>>4)&mask + minv)
	out[46] = uint64((in[14]>>24)&mask + minv)
	out[47] = uint64((in[14]>>44)&mask + minv)
	out[48] = uint64(in[15]&mask + minv)
	out[49] = uint64((in[15]>>20)&mask + minv)
	out[50] = uint64((in[15]>>40)&mask + minv)
	out[51] = uint64((in[15]>>60|in[16]<<4)&mask + minv)
	out[52] = uint64((in[16]>>16)&mask + minv)
	out[53] = uint64((in[16]>>36)&mask + minv)
	out[54] = uint64((in[16]>>56|in[17]<<8)&mask + minv)
	out[55] = uint64((in[17]>>12)&mask + minv)
	out[56] = uint64((in[17]>>32)&mask + minv)
	out[57] = uint64((in[17]>>52|in[18]<<12)&mask + minv)
	out[58] = uint64((in[18]>>8)&mask + minv)
	out[59] = uint64((in[18]>>28)&mask + minv)
	out[60] = uint64((in[18]>>48|in[19]<<16)&mask + minv)
	out[61] = uint64((in[19]>>4)&mask + minv)
	out[62] = uint64((in[19]>>24)&mask + minv)
	out[63] = uint64((in[19]>>44)&mask + minv)
}

func br64_21(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[21]uint64)(p)
	mask := uint64((1 << 21) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>21)&mask + minv)
	out[2] = uint64((in[0]>>42)&mask + minv)
	out[3] = uint64((in[0]>>63|in[1]<<1)&mask + minv)
	out[4] = uint64((in[1]>>20)&mask + minv)
	out[5] = uint64((in[1]>>41)&mask + minv)
	out[6] = uint64((in[1]>>62|in[2]<<2)&mask + minv)
	out[7] = uint64((in[2]>>19)&mask + minv)
	out[8] = uint64((in[2]>>40)&mask + minv)
	out[9] = uint64((in[2]>>61|in[3]<<3)&mask + minv)
	out[10] = uint64((in[3]>>18)&mask + minv)
	out[11] = uint64((in[3]>>39)&mask + minv)
	out[12] = uint64((in[3]>>60|in[4]<<4)&mask + minv)
	out[13] = uint64((in[4]>>17)&mask + minv)
	out[14] = uint64((in[4]>>38)&mask + minv)
	out[15] = uint64((in[4]>>59|in[5]<<5)&mask + minv)
	out[16] = uint64((in[5]>>16)&mask + minv)
	out[17] = uint64((in[5]>>37)&mask + minv)
	out[18] = uint64((in[5]>>58|in[6]<<6)&mask + minv)
	out[19] = uint64((in[6]>>15)&mask + minv)
	out[20] = uint64((in[6]>>36)&mask + minv)
	out[21] = uint64((in[6]>>57|in[7]<<7)&mask + minv)
	out[22] = uint64((in[7]>>14)&mask + minv)
	out[23] = uint64((in[7]>>35)&mask + minv)
	out[24] = uint64((in[7]>>56|in[8]<<8)&mask + minv)
	out[25] = uint64((in[8]>>13)&mask + minv)
	out[26] = uint64((in[8]>>34)&mask + minv)
	out[27] = uint64((in[8]>>55|in[9]<<9)&mask + minv)
	out[28] = uint64((in[9]>>12)&mask + minv)
	out[29] = uint64((in[9]>>33)&mask + minv)
	out[30] = uint64((in[9]>>54|in[10]<<10)&mask + minv)
	out[31] = uint64((in[10]>>11)&mask + minv)
	out[32] = uint64((in[10]>>32)&mask + minv)
	out[33] = uint64((in[10]>>53|in[11]<<11)&mask + minv)
	out[34] = uint64((in[11]>>10)&mask + minv)
	out[35] = uint64((in[11]>>31)&mask + minv)
	out[36] = uint64((in[11]>>52|in[12]<<12)&mask + minv)
	out[37] = uint64((in[12]>>9)&mask + minv)
	out[38] = uint64((in[12]>>30)&mask + minv)
	out[39] = uint64((in[12]>>51|in[13]<<13)&mask + minv)
	out[40] = uint64((in[13]>>8)&mask + minv)
	out[41] = uint64((in[13]>>29)&mask + minv)
	out[42] = uint64((in[13]>>50|in[14]<<14)&mask + minv)
	out[43] = uint64((in[14]>>7)&mask + minv)
	out[44] = uint64((in[14]>>28)&mask + minv)
	out[45] = uint64((in[14]>>49|in[15]<<15)&mask + minv)
	out[46] = uint64((in[15]>>6)&mask + minv)
	out[47] = uint64((in[15]>>27)&mask + minv)
	out[48] = uint64((in[15]>>48|in[16]<<16)&mask + minv)
	out[49] = uint64((in[16]>>5)&mask + minv)
	out[50] = uint64((in[16]>>26)&mask + minv)
	out[51] = uint64((in[16]>>47|in[17]<<17)&mask + minv)
	out[52] = uint64((in[17]>>4)&mask + minv)
	out[53] = uint64((in[17]>>25)&mask + minv)
	out[54] = uint64((in[17]>>46|in[18]<<18)&mask + minv)
	out[55] = uint64((in[18]>>3)&mask + minv)
	out[56] = uint64((in[18]>>24)&mask + minv)
	out[57] = uint64((in[18]>>45|in[19]<<19)&mask + minv)
	out[58] = uint64((in[19]>>2)&mask + minv)
	out[59] = uint64((in[19]>>23)&mask + minv)
	out[60] = uint64((in[19]>>44|in[20]<<20)&mask + minv)
	out[61] = uint64((in[20]>>1)&mask + minv)
	out[62] = uint64((in[20]>>22)&mask + minv)
	out[63] = uint64((in[20]>>43)&mask + minv)
}

func br64_22(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[22]uint64)(p)
	mask := uint64((1 << 22) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>22)&mask + minv)
	out[2] = uint64((in[0]>>44|in[1]<<20)&mask + minv)
	out[3] = uint64((in[1]>>2)&mask + minv)
	out[4] = uint64((in[1]>>24)&mask + minv)
	out[5] = uint64((in[1]>>46|in[2]<<18)&mask + minv)
	out[6] = uint64((in[2]>>4)&mask + minv)
	out[7] = uint64((in[2]>>26)&mask + minv)
	out[8] = uint64((in[2]>>48|in[3]<<16)&mask + minv)
	out[9] = uint64((in[3]>>6)&mask + minv)
	out[10] = uint64((in[3]>>28)&mask + minv)
	out[11] = uint64((in[3]>>50|in[4]<<14)&mask + minv)
	out[12] = uint64((in[4]>>8)&mask + minv)
	out[13] = uint64((in[4]>>30)&mask + minv)
	out[14] = uint64((in[4]>>52|in[5]<<12)&mask + minv)
	out[15] = uint64((in[5]>>10)&mask + minv)
	out[16] = uint64((in[5]>>32)&mask + minv)
	out[17] = uint64((in[5]>>54|in[6]<<10)&mask + minv)
	out[18] = uint64((in[6]>>12)&mask + minv)
	out[19] = uint64((in[6]>>34)&mask + minv)
	out[20] = uint64((in[6]>>56|in[7]<<8)&mask + minv)
	out[21] = uint64((in[7]>>14)&mask + minv)
	out[22] = uint64((in[7]>>36)&mask + minv)
	out[23] = uint64((in[7]>>58|in[8]<<6)&mask + minv)
	out[24] = uint64((in[8]>>16)&mask + minv)
	out[25] = uint64((in[8]>>38)&mask + minv)
	out[26] = uint64((in[8]>>60|in[9]<<4)&mask + minv)
	out[27] = uint64((in[9]>>18)&mask + minv)
	out[28] = uint64((in[9]>>40)&mask + minv)
	out[29] = uint64((in[9]>>62|in[10]<<2)&mask + minv)
	out[30] = uint64((in[10]>>20)&mask + minv)
	out[31] = uint64((in[10]>>42)&mask + minv)
	out[32] = uint64(in[11]&mask + minv)
	out[33] = uint64((in[11]>>22)&mask + minv)
	out[34] = uint64((in[11]>>44|in[12]<<20)&mask + minv)
	out[35] = uint64((in[12]>>2)&mask + minv)
	out[36] = uint64((in[12]>>24)&mask + minv)
	out[37] = uint64((in[12]>>46|in[13]<<18)&mask + minv)
	out[38] = uint64((in[13]>>4)&mask + minv)
	out[39] = uint64((in[13]>>26)&mask + minv)
	out[40] = uint64((in[13]>>48|in[14]<<16)&mask + minv)
	out[41] = uint64((in[14]>>6)&mask + minv)
	out[42] = uint64((in[14]>>28)&mask + minv)
	out[43] = uint64((in[14]>>50|in[15]<<14)&mask + minv)
	out[44] = uint64((in[15]>>8)&mask + minv)
	out[45] = uint64((in[15]>>30)&mask + minv)
	out[46] = uint64((in[15]>>52|in[16]<<12)&mask + minv)
	out[47] = uint64((in[16]>>10)&mask + minv)
	out[48] = uint64((in[16]>>32)&mask + minv)
	out[49] = uint64((in[16]>>54|in[17]<<10)&mask + minv)
	out[50] = uint64((in[17]>>12)&mask + minv)
	out[51] = uint64((in[17]>>34)&mask + minv)
	out[52] = uint64((in[17]>>56|in[18]<<8)&mask + minv)
	out[53] = uint64((in[18]>>14)&mask + minv)
	out[54] = uint64((in[18]>>36)&mask + minv)
	out[55] = uint64((in[18]>>58|in[19]<<6)&mask + minv)
	out[56] = uint64((in[19]>>16)&mask + minv)
	out[57] = uint64((in[19]>>38)&mask + minv)
	out[58] = uint64((in[19]>>60|in[20]<<4)&mask + minv)
	out[59] = uint64((in[20]>>18)&mask + minv)
	out[60] = uint64((in[20]>>40)&mask + minv)
	out[61] = uint64((in[20]>>62|in[21]<<2)&mask + minv)
	out[62] = uint64((in[21]>>20)&mask + minv)
	out[63] = uint64((in[21]>>42)&mask + minv)
}

func br64_23(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[23]uint64)(p)
	mask := uint64((1 << 23) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>23)&mask + minv)
	out[2] = uint64((in[0]>>46|in[1]<<18)&mask + minv)
	out[3] = uint64((in[1]>>5)&mask + minv)
	out[4] = uint64((in[1]>>28)&mask + minv)
	out[5] = uint64((in[1]>>51|in[2]<<13)&mask + minv)
	out[6] = uint64((in[2]>>10)&mask + minv)
	out[7] = uint64((in[2]>>33)&mask + minv)
	out[8] = uint64((in[2]>>56|in[3]<<8)&mask + minv)
	out[9] = uint64((in[3]>>15)&mask + minv)
	out[10] = uint64((in[3]>>38)&mask + minv)
	out[11] = uint64((in[3]>>61|in[4]<<3)&mask + minv)
	out[12] = uint64((in[4]>>20)&mask + minv)
	out[13] = uint64((in[4]>>43|in[5]<<21)&mask + minv)
	out[14] = uint64((in[5]>>2)&mask + minv)
	out[15] = uint64((in[5]>>25)&mask + minv)
	out[16] = uint64((in[5]>>48|in[6]<<16)&mask + minv)
	out[17] = uint64((in[6]>>7)&mask + minv)
	out[18] = uint64((in[6]>>30)&mask + minv)
	out[19] = uint64((in[6]>>53|in[7]<<11)&mask + minv)
	out[20] = uint64((in[7]>>12)&mask + minv)
	out[21] = uint64((in[7]>>35)&mask + minv)
	out[22] = uint64((in[7]>>58|in[8]<<6)&mask + minv)
	out[23] = uint64((in[8]>>17)&mask + minv)
	out[24] = uint64((in[8]>>40)&mask + minv)
	out[25] = uint64((in[8]>>63|in[9]<<1)&mask + minv)
	out[26] = uint64((in[9]>>22)&mask + minv)
	out[27] = uint64((in[9]>>45|in[10]<<19)&mask + minv)
	out[28] = uint64((in[10]>>4)&mask + minv)
	out[29] = uint64((in[10]>>27)&mask + minv)
	out[30] = uint64((in[10]>>50|in[11]<<14)&mask + minv)
	out[31] = uint64((in[11]>>9)&mask + minv)
	out[32] = uint64((in[11]>>32)&mask + minv)
	out[33] = uint64((in[11]>>55|in[12]<<9)&mask + minv)
	out[34] = uint64((in[12]>>14)&mask + minv)
	out[35] = uint64((in[12]>>37)&mask + minv)
	out[36] = uint64((in[12]>>60|in[13]<<4)&mask + minv)
	out[37] = uint64((in[13]>>19)&mask + minv)
	out[38] = uint64((in[13]>>42|in[14]<<22)&mask + minv)
	out[39] = uint64((in[14]>>1)&mask + minv)
	out[40] = uint64((in[14]>>24)&mask + minv)
	out[41] = uint64((in[14]>>47|in[15]<<17)&mask + minv)
	out[42] = uint64((in[15]>>6)&mask + minv)
	out[43] = uint64((in[15]>>29)&mask + minv)
	out[44] = uint64((in[15]>>52|in[16]<<12)&mask + minv)
	out[45] = uint64((in[16]>>11)&mask + minv)
	out[46] = uint64((in[16]>>34)&mask + minv)
	out[47] = uint64((in[16]>>57|in[17]<<7)&mask + minv)
	out[48] = uint64((in[17]>>16)&mask + minv)
	out[49] = uint64((in[17]>>39)&mask + minv)
	out[50] = uint64((in[17]>>62|in[18]<<2)&mask + minv)
	out[51] = uint64((in[18]>>21)&mask + minv)
	out[52] = uint64((in[18]>>44|in[19]<<20)&mask + minv)
	out[53] = uint64((in[19]>>3)&mask + minv)
	out[54] = uint64((in[19]>>26)&mask + minv)
	out[55] = uint64((in[19]>>49|in[20]<<15)&mask + minv)
	out[56] = uint64((in[20]>>8)&mask + minv)
	out[57] = uint64((in[20]>>31)&mask + minv)
	out[58] = uint64((in[20]>>54|in[21]<<10)&mask + minv)
	out[59] = uint64((in[21]>>13)&mask + minv)
	out[60] = uint64((in[21]>>36)&mask + minv)
	out[61] = uint64((in[21]>>59|in[22]<<5)&mask + minv)
	out[62] = uint64((in[22]>>18)&mask + minv)
	out[63] = uint64((in[22]>>41)&mask + minv)
}

func br64_24(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[24]uint64)(p)
	mask := uint64((1 << 24) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>24)&mask + minv)
	out[2] = uint64((in[0]>>48|in[1]<<16)&mask + minv)
	out[3] = uint64((in[1]>>8)&mask + minv)
	out[4] = uint64((in[1]>>32)&mask + minv)
	out[5] = uint64((in[1]>>56|in[2]<<8)&mask + minv)
	out[6] = uint64((in[2]>>16)&mask + minv)
	out[7] = uint64((in[2]>>40)&mask + minv)
	out[8] = uint64(in[3]&mask + minv)
	out[9] = uint64((in[3]>>24)&mask + minv)
	out[10] = uint64((in[3]>>48|in[4]<<16)&mask + minv)
	out[11] = uint64((in[4]>>8)&mask + minv)
	out[12] = uint64((in[4]>>32)&mask + minv)
	out[13] = uint64((in[4]>>56|in[5]<<8)&mask + minv)
	out[14] = uint64((in[5]>>16)&mask + minv)
	out[15] = uint64((in[5]>>40)&mask + minv)
	out[16] = uint64(in[6]&mask + minv)
	out[17] = uint64((in[6]>>24)&mask + minv)
	out[18] = uint64((in[6]>>48|in[7]<<16)&mask + minv)
	out[19] = uint64((in[7]>>8)&mask + minv)
	out[20] = uint64((in[7]>>32)&mask + minv)
	out[21] = uint64((in[7]>>56|in[8]<<8)&mask + minv)
	out[22] = uint64((in[8]>>16)&mask + minv)
	out[23] = uint64((in[8]>>40)&mask + minv)
	out[24] = uint64(in[9]&mask + minv)
	out[25] = uint64((in[9]>>24)&mask + minv)
	out[26] = uint64((in[9]>>48|in[10]<<16)&mask + minv)
	out[27] = uint64((in[10]>>8)&mask + minv)
	out[28] = uint64((in[10]>>32)&mask + minv)
	out[29] = uint64((in[10]>>56|in[11]<<8)&mask + minv)
	out[30] = uint64((in[11]>>16)&mask + minv)
	out[31] = uint64((in[11]>>40)&mask + minv)
	out[32] = uint64(in[12]&mask + minv)
	out[33] = uint64((in[12]>>24)&mask + minv)
	out[34] = uint64((in[12]>>48|in[13]<<16)&mask + minv)
	out[35] = uint64((in[13]>>8)&mask + minv)
	out[36] = uint64((in[13]>>32)&mask + minv)
	out[37] = uint64((in[13]>>56|in[14]<<8)&mask + minv)
	out[38] = uint64((in[14]>>16)&mask + minv)
	out[39] = uint64((in[14]>>40)&mask + minv)
	out[40] = uint64(in[15]&mask + minv)
	out[41] = uint64((in[15]>>24)&mask + minv)
	out[42] = uint64((in[15]>>48|in[16]<<16)&mask + minv)
	out[43] = uint64((in[16]>>8)&mask + minv)
	out[44] = uint64((in[16]>>32)&mask + minv)
	out[45] = uint64((in[16]>>56|in[17]<<8)&mask + minv)
	out[46] = uint64((in[17]>>16)&mask + minv)
	out[47] = uint64((in[17]>>40)&mask + minv)
	out[48] = uint64(in[18]&mask + minv)
	out[49] = uint64((in[18]>>24)&mask + minv)
	out[50] = uint64((in[18]>>48|in[19]<<16)&mask + minv)
	out[51] = uint64((in[19]>>8)&mask + minv)
	out[52] = uint64((in[19]>>32)&mask + minv)
	out[53] = uint64((in[19]>>56|in[20]<<8)&mask + minv)
	out[54] = uint64((in[20]>>16)&mask + minv)
	out[55] = uint64((in[20]>>40)&mask + minv)
	out[56] = uint64(in[21]&mask + minv)
	out[57] = uint64((in[21]>>24)&mask + minv)
	out[58] = uint64((in[21]>>48|in[22]<<16)&mask + minv)
	out[59] = uint64((in[22]>>8)&mask + minv)
	out[60] = uint64((in[22]>>32)&mask + minv)
	out[61] = uint64((in[22]>>56|in[23]<<8)&mask + minv)
	out[62] = uint64((in[23]>>16)&mask + minv)
	out[63] = uint64((in[23]>>40)&mask + minv)
}

func br64_25(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[25]uint64)(p)
	mask := uint64((1 << 25) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>25)&mask + minv)
	out[2] = uint64((in[0]>>50|in[1]<<14)&mask + minv)
	out[3] = uint64((in[1]>>11)&mask + minv)
	out[4] = uint64((in[1]>>36)&mask + minv)
	out[5] = uint64((in[1]>>61|in[2]<<3)&mask + minv)
	out[6] = uint64((in[2]>>22)&mask + minv)
	out[7] = uint64((in[2]>>47|in[3]<<17)&mask + minv)
	out[8] = uint64((in[3]>>8)&mask + minv)
	out[9] = uint64((in[3]>>33)&mask + minv)
	out[10] = uint64((in[3]>>58|in[4]<<6)&mask + minv)
	out[11] = uint64((in[4]>>19)&mask + minv)
	out[12] = uint64((in[4]>>44|in[5]<<20)&mask + minv)
	out[13] = uint64((in[5]>>5)&mask + minv)
	out[14] = uint64((in[5]>>30)&mask + minv)
	out[15] = uint64((in[5]>>55|in[6]<<9)&mask + minv)
	out[16] = uint64((in[6]>>16)&mask + minv)
	out[17] = uint64((in[6]>>41|in[7]<<23)&mask + minv)
	out[18] = uint64((in[7]>>2)&mask + minv)
	out[19] = uint64((in[7]>>27)&mask + minv)
	out[20] = uint64((in[7]>>52|in[8]<<12)&mask + minv)
	out[21] = uint64((in[8]>>13)&mask + minv)
	out[22] = uint64((in[8]>>38)&mask + minv)
	out[23] = uint64((in[8]>>63|in[9]<<1)&mask + minv)
	out[24] = uint64((in[9]>>24)&mask + minv)
	out[25] = uint64((in[9]>>49|in[10]<<15)&mask + minv)
	out[26] = uint64((in[10]>>10)&mask + minv)
	out[27] = uint64((in[10]>>35)&mask + minv)
	out[28] = uint64((in[10]>>60|in[11]<<4)&mask + minv)
	out[29] = uint64((in[11]>>21)&mask + minv)
	out[30] = uint64((in[11]>>46|in[12]<<18)&mask + minv)
	out[31] = uint64((in[12]>>7)&mask + minv)
	out[32] = uint64((in[12]>>32)&mask + minv)
	out[33] = uint64((in[12]>>57|in[13]<<7)&mask + minv)
	out[34] = uint64((in[13]>>18)&mask + minv)
	out[35] = uint64((in[13]>>43|in[14]<<21)&mask + minv)
	out[36] = uint64((in[14]>>4)&mask + minv)
	out[37] = uint64((in[14]>>29)&mask + minv)
	out[38] = uint64((in[14]>>54|in[15]<<10)&mask + minv)
	out[39] = uint64((in[15]>>15)&mask + minv)
	out[40] = uint64((in[15]>>40|in[16]<<24)&mask + minv)
	out[41] = uint64((in[16]>>1)&mask + minv)
	out[42] = uint64((in[16]>>26)&mask + minv)
	out[43] = uint64((in[16]>>51|in[17]<<13)&mask + minv)
	out[44] = uint64((in[17]>>12)&mask + minv)
	out[45] = uint64((in[17]>>37)&mask + minv)
	out[46] = uint64((in[17]>>62|in[18]<<2)&mask + minv)
	out[47] = uint64((in[18]>>23)&mask + minv)
	out[48] = uint64((in[18]>>48|in[19]<<16)&mask + minv)
	out[49] = uint64((in[19]>>9)&mask + minv)
	out[50] = uint64((in[19]>>34)&mask + minv)
	out[51] = uint64((in[19]>>59|in[20]<<5)&mask + minv)
	out[52] = uint64((in[20]>>20)&mask + minv)
	out[53] = uint64((in[20]>>45|in[21]<<19)&mask + minv)
	out[54] = uint64((in[21]>>6)&mask + minv)
	out[55] = uint64((in[21]>>31)&mask + minv)
	out[56] = uint64((in[21]>>56|in[22]<<8)&mask + minv)
	out[57] = uint64((in[22]>>17)&mask + minv)
	out[58] = uint64((in[22]>>42|in[23]<<22)&mask + minv)
	out[59] = uint64((in[23]>>3)&mask + minv)
	out[60] = uint64((in[23]>>28)&mask + minv)
	out[61] = uint64((in[23]>>53|in[24]<<11)&mask + minv)
	out[62] = uint64((in[24]>>14)&mask + minv)
	out[63] = uint64((in[24]>>39)&mask + minv)
}

func br64_26(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[26]uint64)(p)
	mask := uint64((1 << 26) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>26)&mask + minv)
	out[2] = uint64((in[0]>>52|in[1]<<12)&mask + minv)
	out[3] = uint64((in[1]>>14)&mask + minv)
	out[4] = uint64((in[1]>>40|in[2]<<24)&mask + minv)
	out[5] = uint64((in[2]>>2)&mask + minv)
	out[6] = uint64((in[2]>>28)&mask + minv)
	out[7] = uint64((in[2]>>54|in[3]<<10)&mask + minv)
	out[8] = uint64((in[3]>>16)&mask + minv)
	out[9] = uint64((in[3]>>42|in[4]<<22)&mask + minv)
	out[10] = uint64((in[4]>>4)&mask + minv)
	out[11] = uint64((in[4]>>30)&mask + minv)
	out[12] = uint64((in[4]>>56|in[5]<<8)&mask + minv)
	out[13] = uint64((in[5]>>18)&mask + minv)
	out[14] = uint64((in[5]>>44|in[6]<<20)&mask + minv)
	out[15] = uint64((in[6]>>6)&mask + minv)
	out[16] = uint64((in[6]>>32)&mask + minv)
	out[17] = uint64((in[6]>>58|in[7]<<6)&mask + minv)
	out[18] = uint64((in[7]>>20)&mask + minv)
	out[19] = uint64((in[7]>>46|in[8]<<18)&mask + minv)
	out[20] = uint64((in[8]>>8)&mask + minv)
	out[21] = uint64((in[8]>>34)&mask + minv)
	out[22] = uint64((in[8]>>60|in[9]<<4)&mask + minv)
	out[23] = uint64((in[9]>>22)&mask + minv)
	out[24] = uint64((in[9]>>48|in[10]<<16)&mask + minv)
	out[25] = uint64((in[10]>>10)&mask + minv)
	out[26] = uint64((in[10]>>36)&mask + minv)
	out[27] = uint64((in[10]>>62|in[11]<<2)&mask + minv)
	out[28] = uint64((in[11]>>24)&mask + minv)
	out[29] = uint64((in[11]>>50|in[12]<<14)&mask + minv)
	out[30] = uint64((in[12]>>12)&mask + minv)
	out[31] = uint64((in[12]>>38)&mask + minv)
	out[32] = uint64(in[13]&mask + minv)
	out[33] = uint64((in[13]>>26)&mask + minv)
	out[34] = uint64((in[13]>>52|in[14]<<12)&mask + minv)
	out[35] = uint64((in[14]>>14)&mask + minv)
	out[36] = uint64((in[14]>>40|in[15]<<24)&mask + minv)
	out[37] = uint64((in[15]>>2)&mask + minv)
	out[38] = uint64((in[15]>>28)&mask + minv)
	out[39] = uint64((in[15]>>54|in[16]<<10)&mask + minv)
	out[40] = uint64((in[16]>>16)&mask + minv)
	out[41] = uint64((in[16]>>42|in[17]<<22)&mask + minv)
	out[42] = uint64((in[17]>>4)&mask + minv)
	out[43] = uint64((in[17]>>30)&mask + minv)
	out[44] = uint64((in[17]>>56|in[18]<<8)&mask + minv)
	out[45] = uint64((in[18]>>18)&mask + minv)
	out[46] = uint64((in[18]>>44|in[19]<<20)&mask + minv)
	out[47] = uint64((in[19]>>6)&mask + minv)
	out[48] = uint64((in[19]>>32)&mask + minv)
	out[49] = uint64((in[19]>>58|in[20]<<6)&mask + minv)
	out[50] = uint64((in[20]>>20)&mask + minv)
	out[51] = uint64((in[20]>>46|in[21]<<18)&mask + minv)
	out[52] = uint64((in[21]>>8)&mask + minv)
	out[53] = uint64((in[21]>>34)&mask + minv)
	out[54] = uint64((in[21]>>60|in[22]<<4)&mask + minv)
	out[55] = uint64((in[22]>>22)&mask + minv)
	out[56] = uint64((in[22]>>48|in[23]<<16)&mask + minv)
	out[57] = uint64((in[23]>>10)&mask + minv)
	out[58] = uint64((in[23]>>36)&mask + minv)
	out[59] = uint64((in[23]>>62|in[24]<<2)&mask + minv)
	out[60] = uint64((in[24]>>24)&mask + minv)
	out[61] = uint64((in[24]>>50|in[25]<<14)&mask + minv)
	out[62] = uint64((in[25]>>12)&mask + minv)
	out[63] = uint64((in[25]>>38)&mask + minv)
}

func br64_27(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[27]uint64)(p)
	mask := uint64((1 << 27) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>27)&mask + minv)
	out[2] = uint64((in[0]>>54|in[1]<<10)&mask + minv)
	out[3] = uint64((in[1]>>17)&mask + minv)
	out[4] = uint64((in[1]>>44|in[2]<<20)&mask + minv)
	out[5] = uint64((in[2]>>7)&mask + minv)
	out[6] = uint64((in[2]>>34)&mask + minv)
	out[7] = uint64((in[2]>>61|in[3]<<3)&mask + minv)
	out[8] = uint64((in[3]>>24)&mask + minv)
	out[9] = uint64((in[3]>>51|in[4]<<13)&mask + minv)
	out[10] = uint64((in[4]>>14)&mask + minv)
	out[11] = uint64((in[4]>>41|in[5]<<23)&mask + minv)
	out[12] = uint64((in[5]>>4)&mask + minv)
	out[13] = uint64((in[5]>>31)&mask + minv)
	out[14] = uint64((in[5]>>58|in[6]<<6)&mask + minv)
	out[15] = uint64((in[6]>>21)&mask + minv)
	out[16] = uint64((in[6]>>48|in[7]<<16)&mask + minv)
	out[17] = uint64((in[7]>>11)&mask + minv)
	out[18] = uint64((in[7]>>38|in[8]<<26)&mask + minv)
	out[19] = uint64((in[8]>>1)&mask + minv)
	out[20] = uint64((in[8]>>28)&mask + minv)
	out[21] = uint64((in[8]>>55|in[9]<<9)&mask + minv)
	out[22] = uint64((in[9]>>18)&mask + minv)
	out[23] = uint64((in[9]>>45|in[10]<<19)&mask + minv)
	out[24] = uint64((in[10]>>8)&mask + minv)
	out[25] = uint64((in[10]>>35)&mask + minv)
	out[26] = uint64((in[10]>>62|in[11]<<2)&mask + minv)
	out[27] = uint64((in[11]>>25)&mask + minv)
	out[28] = uint64((in[11]>>52|in[12]<<12)&mask + minv)
	out[29] = uint64((in[12]>>15)&mask + minv)
	out[30] = uint64((in[12]>>42|in[13]<<22)&mask + minv)
	out[31] = uint64((in[13]>>5)&mask + minv)
	out[32] = uint64((in[13]>>32)&mask + minv)
	out[33] = uint64((in[13]>>59|in[14]<<5)&mask + minv)
	out[34] = uint64((in[14]>>22)&mask + minv)
	out[35] = uint64((in[14]>>49|in[15]<<15)&mask + minv)
	out[36] = uint64((in[15]>>12)&mask + minv)
	out[37] = uint64((in[15]>>39|in[16]<<25)&mask + minv)
	out[38] = uint64((in[16]>>2)&mask + minv)
	out[39] = uint64((in[16]>>29)&mask + minv)
	out[40] = uint64((in[16]>>56|in[17]<<8)&mask + minv)
	out[41] = uint64((in[17]>>19)&mask + minv)
	out[42] = uint64((in[17]>>46|in[18]<<18)&mask + minv)
	out[43] = uint64((in[18]>>9)&mask + minv)
	out[44] = uint64((in[18]>>36)&mask + minv)
	out[45] = uint64((in[18]>>63|in[19]<<1)&mask + minv)
	out[46] = uint64((in[19]>>26)&mask + minv)
	out[47] = uint64((in[19]>>53|in[20]<<11)&mask + minv)
	out[48] = uint64((in[20]>>16)&mask + minv)
	out[49] = uint64((in[20]>>43|in[21]<<21)&mask + minv)
	out[50] = uint64((in[21]>>6)&mask + minv)
	out[51] = uint64((in[21]>>33)&mask + minv)
	out[52] = uint64((in[21]>>60|in[22]<<4)&mask + minv)
	out[53] = uint64((in[22]>>23)&mask + minv)
	out[54] = uint64((in[22]>>50|in[23]<<14)&mask + minv)
	out[55] = uint64((in[23]>>13)&mask + minv)
	out[56] = uint64((in[23]>>40|in[24]<<24)&mask + minv)
	out[57] = uint64((in[24]>>3)&mask + minv)
	out[58] = uint64((in[24]>>30)&mask + minv)
	out[59] = uint64((in[24]>>57|in[25]<<7)&mask + minv)
	out[60] = uint64((in[25]>>20)&mask + minv)
	out[61] = uint64((in[25]>>47|in[26]<<17)&mask + minv)
	out[62] = uint64((in[26]>>10)&mask + minv)
	out[63] = uint64((in[26]>>37)&mask + minv)
}

func br64_28(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[28]uint64)(p)
	mask := uint64((1 << 28) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>28)&mask + minv)
	out[2] = uint64((in[0]>>56|in[1]<<8)&mask + minv)
	out[3] = uint64((in[1]>>20)&mask + minv)
	out[4] = uint64((in[1]>>48|in[2]<<16)&mask + minv)
	out[5] = uint64((in[2]>>12)&mask + minv)
	out[6] = uint64((in[2]>>40|in[3]<<24)&mask + minv)
	out[7] = uint64((in[3]>>4)&mask + minv)
	out[8] = uint64((in[3]>>32)&mask + minv)
	out[9] = uint64((in[3]>>60|in[4]<<4)&mask + minv)
	out[10] = uint64((in[4]>>24)&mask + minv)
	out[11] = uint64((in[4]>>52|in[5]<<12)&mask + minv)
	out[12] = uint64((in[5]>>16)&mask + minv)
	out[13] = uint64((in[5]>>44|in[6]<<20)&mask + minv)
	out[14] = uint64((in[6]>>8)&mask + minv)
	out[15] = uint64((in[6]>>36)&mask + minv)
	out[16] = uint64(in[7]&mask + minv)
	out[17] = uint64((in[7]>>28)&mask + minv)
	out[18] = uint64((in[7]>>56|in[8]<<8)&mask + minv)
	out[19] = uint64((in[8]>>20)&mask + minv)
	out[20] = uint64((in[8]>>48|in[9]<<16)&mask + minv)
	out[21] = uint64((in[9]>>12)&mask + minv)
	out[22] = uint64((in[9]>>40|in[10]<<24)&mask + minv)
	out[23] = uint64((in[10]>>4)&mask + minv)
	out[24] = uint64((in[10]>>32)&mask + minv)
	out[25] = uint64((in[10]>>60|in[11]<<4)&mask + minv)
	out[26] = uint64((in[11]>>24)&mask + minv)
	out[27] = uint64((in[11]>>52|in[12]<<12)&mask + minv)
	out[28] = uint64((in[12]>>16)&mask + minv)
	out[29] = uint64((in[12]>>44|in[13]<<20)&mask + minv)
	out[30] = uint64((in[13]>>8)&mask + minv)
	out[31] = uint64((in[13]>>36)&mask + minv)
	out[32] = uint64(in[14]&mask + minv)
	out[33] = uint64((in[14]>>28)&mask + minv)
	out[34] = uint64((in[14]>>56|in[15]<<8)&mask + minv)
	out[35] = uint64((in[15]>>20)&mask + minv)
	out[36] = uint64((in[15]>>48|in[16]<<16)&mask + minv)
	out[37] = uint64((in[16]>>12)&mask + minv)
	out[38] = uint64((in[16]>>40|in[17]<<24)&mask + minv)
	out[39] = uint64((in[17]>>4)&mask + minv)
	out[40] = uint64((in[17]>>32)&mask + minv)
	out[41] = uint64((in[17]>>60|in[18]<<4)&mask + minv)
	out[42] = uint64((in[18]>>24)&mask + minv)
	out[43] = uint64((in[18]>>52|in[19]<<12)&mask + minv)
	out[44] = uint64((in[19]>>16)&mask + minv)
	out[45] = uint64((in[19]>>44|in[20]<<20)&mask + minv)
	out[46] = uint64((in[20]>>8)&mask + minv)
	out[47] = uint64((in[20]>>36)&mask + minv)
	out[48] = uint64(in[21]&mask + minv)
	out[49] = uint64((in[21]>>28)&mask + minv)
	out[50] = uint64((in[21]>>56|in[22]<<8)&mask + minv)
	out[51] = uint64((in[22]>>20)&mask + minv)
	out[52] = uint64((in[22]>>48|in[23]<<16)&mask + minv)
	out[53] = uint64((in[23]>>12)&mask + minv)
	out[54] = uint64((in[23]>>40|in[24]<<24)&mask + minv)
	out[55] = uint64((in[24]>>4)&mask + minv)
	out[56] = uint64((in[24]>>32)&mask + minv)
	out[57] = uint64((in[24]>>60|in[25]<<4)&mask + minv)
	out[58] = uint64((in[25]>>24)&mask + minv)
	out[59] = uint64((in[25]>>52|in[26]<<12)&mask + minv)
	out[60] = uint64((in[26]>>16)&mask + minv)
	out[61] = uint64((in[26]>>44|in[27]<<20)&mask + minv)
	out[62] = uint64((in[27]>>8)&mask + minv)
	out[63] = uint64((in[27]>>36)&mask + minv)
}

func br64_29(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[29]uint64)(p)
	mask := uint64((1 << 29) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>29)&mask + minv)
	out[2] = uint64((in[0]>>58|in[1]<<6)&mask + minv)
	out[3] = uint64((in[1]>>23)&mask + minv)
	out[4] = uint64((in[1]>>52|in[2]<<12)&mask + minv)
	out[5] = uint64((in[2]>>17)&mask + minv)
	out[6] = uint64((in[2]>>46|in[3]<<18)&mask + minv)
	out[7] = uint64((in[3]>>11)&mask + minv)
	out[8] = uint64((in[3]>>40|in[4]<<24)&mask + minv)
	out[9] = uint64((in[4]>>5)&mask + minv)
	out[10] = uint64((in[4]>>34)&mask + minv)
	out[11] = uint64((in[4]>>63|in[5]<<1)&mask + minv)
	out[12] = uint64((in[5]>>28)&mask + minv)
	out[13] = uint64((in[5]>>57|in[6]<<7)&mask + minv)
	out[14] = uint64((in[6]>>22)&mask + minv)
	out[15] = uint64((in[6]>>51|in[7]<<13)&mask + minv)
	out[16] = uint64((in[7]>>16)&mask + minv)
	out[17] = uint64((in[7]>>45|in[8]<<19)&mask + minv)
	out[18] = uint64((in[8]>>10)&mask + minv)
	out[19] = uint64((in[8]>>39|in[9]<<25)&mask + minv)
	out[20] = uint64((in[9]>>4)&mask + minv)
	out[21] = uint64((in[9]>>33)&mask + minv)
	out[22] = uint64((in[9]>>62|in[10]<<2)&mask + minv)
	out[23] = uint64((in[10]>>27)&mask + minv)
	out[24] = uint64((in[10]>>56|in[11]<<8)&mask + minv)
	out[25] = uint64((in[11]>>21)&mask + minv)
	out[26] = uint64((in[11]>>50|in[12]<<14)&mask + minv)
	out[27] = uint64((in[12]>>15)&mask + minv)
	out[28] = uint64((in[12]>>44|in[13]<<20)&mask + minv)
	out[29] = uint64((in[13]>>9)&mask + minv)
	out[30] = uint64((in[13]>>38|in[14]<<26)&mask + minv)
	out[31] = uint64((in[14]>>3)&mask + minv)
	out[32] = uint64((in[14]>>32)&mask + minv)
	out[33] = uint64((in[14]>>61|in[15]<<3)&mask + minv)
	out[34] = uint64((in[15]>>26)&mask + minv)
	out[35] = uint64((in[15]>>55|in[16]<<9)&mask + minv)
	out[36] = uint64((in[16]>>20)&mask + minv)
	out[37] = uint64((in[16]>>49|in[17]<<15)&mask + minv)
	out[38] = uint64((in[17]>>14)&mask + minv)
	out[39] = uint64((in[17]>>43|in[18]<<21)&mask + minv)
	out[40] = uint64((in[18]>>8)&mask + minv)
	out[41] = uint64((in[18]>>37|in[19]<<27)&mask + minv)
	out[42] = uint64((in[19]>>2)&mask + minv)
	out[43] = uint64((in[19]>>31)&mask + minv)
	out[44] = uint64((in[19]>>60|in[20]<<4)&mask + minv)
	out[45] = uint64((in[20]>>25)&mask + minv)
	out[46] = uint64((in[20]>>54|in[21]<<10)&mask + minv)
	out[47] = uint64((in[21]>>19)&mask + minv)
	out[48] = uint64((in[21]>>48|in[22]<<16)&mask + minv)
	out[49] = uint64((in[22]>>13)&mask + minv)
	out[50] = uint64((in[22]>>42|in[23]<<22)&mask + minv)
	out[51] = uint64((in[23]>>7)&mask + minv)
	out[52] = uint64((in[23]>>36|in[24]<<28)&mask + minv)
	out[53] = uint64((in[24]>>1)&mask + minv)
	out[54] = uint64((in[24]>>30)&mask + minv)
	out[55] = uint64((in[24]>>59|in[25]<<5)&mask + minv)
	out[56] = uint64((in[25]>>24)&mask + minv)
	out[57] = uint64((in[25]>>53|in[26]<<11)&mask + minv)
	out[58] = uint64((in[26]>>18)&mask + minv)
	out[59] = uint64((in[26]>>47|in[27]<<17)&mask + minv)
	out[60] = uint64((in[27]>>12)&mask + minv)
	out[61] = uint64((in[27]>>41|in[28]<<23)&mask + minv)
	out[62] = uint64((in[28]>>6)&mask + minv)
	out[63] = uint64((in[28]>>35)&mask + minv)
}

func br64_30(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[30]uint64)(p)
	mask := uint64((1 << 30) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>30)&mask + minv)
	out[2] = uint64((in[0]>>60|in[1]<<4)&mask + minv)
	out[3] = uint64((in[1]>>26)&mask + minv)
	out[4] = uint64((in[1]>>56|in[2]<<8)&mask + minv)
	out[5] = uint64((in[2]>>22)&mask + minv)
	out[6] = uint64((in[2]>>52|in[3]<<12)&mask + minv)
	out[7] = uint64((in[3]>>18)&mask + minv)
	out[8] = uint64((in[3]>>48|in[4]<<16)&mask + minv)
	out[9] = uint64((in[4]>>14)&mask + minv)
	out[10] = uint64((in[4]>>44|in[5]<<20)&mask + minv)
	out[11] = uint64((in[5]>>10)&mask + minv)
	out[12] = uint64((in[5]>>40|in[6]<<24)&mask + minv)
	out[13] = uint64((in[6]>>6)&mask + minv)
	out[14] = uint64((in[6]>>36|in[7]<<28)&mask + minv)
	out[15] = uint64((in[7]>>2)&mask + minv)
	out[16] = uint64((in[7]>>32)&mask + minv)
	out[17] = uint64((in[7]>>62|in[8]<<2)&mask + minv)
	out[18] = uint64((in[8]>>28)&mask + minv)
	out[19] = uint64((in[8]>>58|in[9]<<6)&mask + minv)
	out[20] = uint64((in[9]>>24)&mask + minv)
	out[21] = uint64((in[9]>>54|in[10]<<10)&mask + minv)
	out[22] = uint64((in[10]>>20)&mask + minv)
	out[23] = uint64((in[10]>>50|in[11]<<14)&mask + minv)
	out[24] = uint64((in[11]>>16)&mask + minv)
	out[25] = uint64((in[11]>>46|in[12]<<18)&mask + minv)
	out[26] = uint64((in[12]>>12)&mask + minv)
	out[27] = uint64((in[12]>>42|in[13]<<22)&mask + minv)
	out[28] = uint64((in[13]>>8)&mask + minv)
	out[29] = uint64((in[13]>>38|in[14]<<26)&mask + minv)
	out[30] = uint64((in[14]>>4)&mask + minv)
	out[31] = uint64((in[14]>>34)&mask + minv)
	out[32] = uint64(in[15]&mask + minv)
	out[33] = uint64((in[15]>>30)&mask + minv)
	out[34] = uint64((in[15]>>60|in[16]<<4)&mask + minv)
	out[35] = uint64((in[16]>>26)&mask + minv)
	out[36] = uint64((in[16]>>56|in[17]<<8)&mask + minv)
	out[37] = uint64((in[17]>>22)&mask + minv)
	out[38] = uint64((in[17]>>52|in[18]<<12)&mask + minv)
	out[39] = uint64((in[18]>>18)&mask + minv)
	out[40] = uint64((in[18]>>48|in[19]<<16)&mask + minv)
	out[41] = uint64((in[19]>>14)&mask + minv)
	out[42] = uint64((in[19]>>44|in[20]<<20)&mask + minv)
	out[43] = uint64((in[20]>>10)&mask + minv)
	out[44] = uint64((in[20]>>40|in[21]<<24)&mask + minv)
	out[45] = uint64((in[21]>>6)&mask + minv)
	out[46] = uint64((in[21]>>36|in[22]<<28)&mask + minv)
	out[47] = uint64((in[22]>>2)&mask + minv)
	out[48] = uint64((in[22]>>32)&mask + minv)
	out[49] = uint64((in[22]>>62|in[23]<<2)&mask + minv)
	out[50] = uint64((in[23]>>28)&mask + minv)
	out[51] = uint64((in[23]>>58|in[24]<<6)&mask + minv)
	out[52] = uint64((in[24]>>24)&mask + minv)
	out[53] = uint64((in[24]>>54|in[25]<<10)&mask + minv)
	out[54] = uint64((in[25]>>20)&mask + minv)
	out[55] = uint64((in[25]>>50|in[26]<<14)&mask + minv)
	out[56] = uint64((in[26]>>16)&mask + minv)
	out[57] = uint64((in[26]>>46|in[27]<<18)&mask + minv)
	out[58] = uint64((in[27]>>12)&mask + minv)
	out[59] = uint64((in[27]>>42|in[28]<<22)&mask + minv)
	out[60] = uint64((in[28]>>8)&mask + minv)
	out[61] = uint64((in[28]>>38|in[29]<<26)&mask + minv)
	out[62] = uint64((in[29]>>4)&mask + minv)
	out[63] = uint64((in[29]>>34)&mask + minv)
}

func br64_31(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[31]uint64)(p)
	mask := uint64((1 << 31) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>31)&mask + minv)
	out[2] = uint64((in[0]>>62|in[1]<<2)&mask + minv)
	out[3] = uint64((in[1]>>29)&mask + minv)
	out[4] = uint64((in[1]>>60|in[2]<<4)&mask + minv)
	out[5] = uint64((in[2]>>27)&mask + minv)
	out[6] = uint64((in[2]>>58|in[3]<<6)&mask + minv)
	out[7] = uint64((in[3]>>25)&mask + minv)
	out[8] = uint64((in[3]>>56|in[4]<<8)&mask + minv)
	out[9] = uint64((in[4]>>23)&mask + minv)
	out[10] = uint64((in[4]>>54|in[5]<<10)&mask + minv)
	out[11] = uint64((in[5]>>21)&mask + minv)
	out[12] = uint64((in[5]>>52|in[6]<<12)&mask + minv)
	out[13] = uint64((in[6]>>19)&mask + minv)
	out[14] = uint64((in[6]>>50|in[7]<<14)&mask + minv)
	out[15] = uint64((in[7]>>17)&mask + minv)
	out[16] = uint64((in[7]>>48|in[8]<<16)&mask + minv)
	out[17] = uint64((in[8]>>15)&mask + minv)
	out[18] = uint64((in[8]>>46|in[9]<<18)&mask + minv)
	out[19] = uint64((in[9]>>13)&mask + minv)
	out[20] = uint64((in[9]>>44|in[10]<<20)&mask + minv)
	out[21] = uint64((in[10]>>11)&mask + minv)
	out[22] = uint64((in[10]>>42|in[11]<<22)&mask + minv)
	out[23] = uint64((in[11]>>9)&mask + minv)
	out[24] = uint64((in[11]>>40|in[12]<<24)&mask + minv)
	out[25] = uint64((in[12]>>7)&mask + minv)
	out[26] = uint64((in[12]>>38|in[13]<<26)&mask + minv)
	out[27] = uint64((in[13]>>5)&mask + minv)
	out[28] = uint64((in[13]>>36|in[14]<<28)&mask + minv)
	out[29] = uint64((in[14]>>3)&mask + minv)
	out[30] = uint64((in[14]>>34|in[15]<<30)&mask + minv)
	out[31] = uint64((in[15]>>1)&mask + minv)
	out[32] = uint64((in[15]>>32)&mask + minv)
	out[33] = uint64((in[15]>>63|in[16]<<1)&mask + minv)
	out[34] = uint64((in[16]>>30)&mask + minv)
	out[35] = uint64((in[16]>>61|in[17]<<3)&mask + minv)
	out[36] = uint64((in[17]>>28)&mask + minv)
	out[37] = uint64((in[17]>>59|in[18]<<5)&mask + minv)
	out[38] = uint64((in[18]>>26)&mask + minv)
	out[39] = uint64((in[18]>>57|in[19]<<7)&mask + minv)
	out[40] = uint64((in[19]>>24)&mask + minv)
	out[41] = uint64((in[19]>>55|in[20]<<9)&mask + minv)
	out[42] = uint64((in[20]>>22)&mask + minv)
	out[43] = uint64((in[20]>>53|in[21]<<11)&mask + minv)
	out[44] = uint64((in[21]>>20)&mask + minv)
	out[45] = uint64((in[21]>>51|in[22]<<13)&mask + minv)
	out[46] = uint64((in[22]>>18)&mask + minv)
	out[47] = uint64((in[22]>>49|in[23]<<15)&mask + minv)
	out[48] = uint64((in[23]>>16)&mask + minv)
	out[49] = uint64((in[23]>>47|in[24]<<17)&mask + minv)
	out[50] = uint64((in[24]>>14)&mask + minv)
	out[51] = uint64((in[24]>>45|in[25]<<19)&mask + minv)
	out[52] = uint64((in[25]>>12)&mask + minv)
	out[53] = uint64((in[25]>>43|in[26]<<21)&mask + minv)
	out[54] = uint64((in[26]>>10)&mask + minv)
	out[55] = uint64((in[26]>>41|in[27]<<23)&mask + minv)
	out[56] = uint64((in[27]>>8)&mask + minv)
	out[57] = uint64((in[27]>>39|in[28]<<25)&mask + minv)
	out[58] = uint64((in[28]>>6)&mask + minv)
	out[59] = uint64((in[28]>>37|in[29]<<27)&mask + minv)
	out[60] = uint64((in[29]>>4)&mask + minv)
	out[61] = uint64((in[29]>>35|in[30]<<29)&mask + minv)
	out[62] = uint64((in[30]>>2)&mask + minv)
	out[63] = uint64((in[30]>>33)&mask + minv)
}

func br64_32(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[32]uint64)(p)
	mask := uint64((1 << 32) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>32)&mask + minv)
	out[2] = uint64(in[1]&mask + minv)
	out[3] = uint64((in[1]>>32)&mask + minv)
	out[4] = uint64(in[2]&mask + minv)
	out[5] = uint64((in[2]>>32)&mask + minv)
	out[6] = uint64(in[3]&mask + minv)
	out[7] = uint64((in[3]>>32)&mask + minv)
	out[8] = uint64(in[4]&mask + minv)
	out[9] = uint64((in[4]>>32)&mask + minv)
	out[10] = uint64(in[5]&mask + minv)
	out[11] = uint64((in[5]>>32)&mask + minv)
	out[12] = uint64(in[6]&mask + minv)
	out[13] = uint64((in[6]>>32)&mask + minv)
	out[14] = uint64(in[7]&mask + minv)
	out[15] = uint64((in[7]>>32)&mask + minv)
	out[16] = uint64(in[8]&mask + minv)
	out[17] = uint64((in[8]>>32)&mask + minv)
	out[18] = uint64(in[9]&mask + minv)
	out[19] = uint64((in[9]>>32)&mask + minv)
	out[20] = uint64(in[10]&mask + minv)
	out[21] = uint64((in[10]>>32)&mask + minv)
	out[22] = uint64(in[11]&mask + minv)
	out[23] = uint64((in[11]>>32)&mask + minv)
	out[24] = uint64(in[12]&mask + minv)
	out[25] = uint64((in[12]>>32)&mask + minv)
	out[26] = uint64(in[13]&mask + minv)
	out[27] = uint64((in[13]>>32)&mask + minv)
	out[28] = uint64(in[14]&mask + minv)
	out[29] = uint64((in[14]>>32)&mask + minv)
	out[30] = uint64(in[15]&mask + minv)
	out[31] = uint64((in[15]>>32)&mask + minv)
	out[32] = uint64(in[16]&mask + minv)
	out[33] = uint64((in[16]>>32)&mask + minv)
	out[34] = uint64(in[17]&mask + minv)
	out[35] = uint64((in[17]>>32)&mask + minv)
	out[36] = uint64(in[18]&mask + minv)
	out[37] = uint64((in[18]>>32)&mask + minv)
	out[38] = uint64(in[19]&mask + minv)
	out[39] = uint64((in[19]>>32)&mask + minv)
	out[40] = uint64(in[20]&mask + minv)
	out[41] = uint64((in[20]>>32)&mask + minv)
	out[42] = uint64(in[21]&mask + minv)
	out[43] = uint64((in[21]>>32)&mask + minv)
	out[44] = uint64(in[22]&mask + minv)
	out[45] = uint64((in[22]>>32)&mask + minv)
	out[46] = uint64(in[23]&mask + minv)
	out[47] = uint64((in[23]>>32)&mask + minv)
	out[48] = uint64(in[24]&mask + minv)
	out[49] = uint64((in[24]>>32)&mask + minv)
	out[50] = uint64(in[25]&mask + minv)
	out[51] = uint64((in[25]>>32)&mask + minv)
	out[52] = uint64(in[26]&mask + minv)
	out[53] = uint64((in[26]>>32)&mask + minv)
	out[54] = uint64(in[27]&mask + minv)
	out[55] = uint64((in[27]>>32)&mask + minv)
	out[56] = uint64(in[28]&mask + minv)
	out[57] = uint64((in[28]>>32)&mask + minv)
	out[58] = uint64(in[29]&mask + minv)
	out[59] = uint64((in[29]>>32)&mask + minv)
	out[60] = uint64(in[30]&mask + minv)
	out[61] = uint64((in[30]>>32)&mask + minv)
	out[62] = uint64(in[31]&mask + minv)
	out[63] = uint64((in[31]>>32)&mask + minv)
}

func br64_33(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[33]uint64)(p)
	mask := uint64((1 << 33) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>33|in[1]<<31)&mask + minv)
	out[2] = uint64((in[1]>>2)&mask + minv)
	out[3] = uint64((in[1]>>35|in[2]<<29)&mask + minv)
	out[4] = uint64((in[2]>>4)&mask + minv)
	out[5] = uint64((in[2]>>37|in[3]<<27)&mask + minv)
	out[6] = uint64((in[3]>>6)&mask + minv)
	out[7] = uint64((in[3]>>39|in[4]<<25)&mask + minv)
	out[8] = uint64((in[4]>>8)&mask + minv)
	out[9] = uint64((in[4]>>41|in[5]<<23)&mask + minv)
	out[10] = uint64((in[5]>>10)&mask + minv)
	out[11] = uint64((in[5]>>43|in[6]<<21)&mask + minv)
	out[12] = uint64((in[6]>>12)&mask + minv)
	out[13] = uint64((in[6]>>45|in[7]<<19)&mask + minv)
	out[14] = uint64((in[7]>>14)&mask + minv)
	out[15] = uint64((in[7]>>47|in[8]<<17)&mask + minv)
	out[16] = uint64((in[8]>>16)&mask + minv)
	out[17] = uint64((in[8]>>49|in[9]<<15)&mask + minv)
	out[18] = uint64((in[9]>>18)&mask + minv)
	out[19] = uint64((in[9]>>51|in[10]<<13)&mask + minv)
	out[20] = uint64((in[10]>>20)&mask + minv)
	out[21] = uint64((in[10]>>53|in[11]<<11)&mask + minv)
	out[22] = uint64((in[11]>>22)&mask + minv)
	out[23] = uint64((in[11]>>55|in[12]<<9)&mask + minv)
	out[24] = uint64((in[12]>>24)&mask + minv)
	out[25] = uint64((in[12]>>57|in[13]<<7)&mask + minv)
	out[26] = uint64((in[13]>>26)&mask + minv)
	out[27] = uint64((in[13]>>59|in[14]<<5)&mask + minv)
	out[28] = uint64((in[14]>>28)&mask + minv)
	out[29] = uint64((in[14]>>61|in[15]<<3)&mask + minv)
	out[30] = uint64((in[15]>>30)&mask + minv)
	out[31] = uint64((in[15]>>63|in[16]<<1)&mask + minv)
	out[32] = uint64((in[16]>>32|in[17]<<32)&mask + minv)
	out[33] = uint64((in[17]>>1)&mask + minv)
	out[34] = uint64((in[17]>>34|in[18]<<30)&mask + minv)
	out[35] = uint64((in[18]>>3)&mask + minv)
	out[36] = uint64((in[18]>>36|in[19]<<28)&mask + minv)
	out[37] = uint64((in[19]>>5)&mask + minv)
	out[38] = uint64((in[19]>>38|in[20]<<26)&mask + minv)
	out[39] = uint64((in[20]>>7)&mask + minv)
	out[40] = uint64((in[20]>>40|in[21]<<24)&mask + minv)
	out[41] = uint64((in[21]>>9)&mask + minv)
	out[42] = uint64((in[21]>>42|in[22]<<22)&mask + minv)
	out[43] = uint64((in[22]>>11)&mask + minv)
	out[44] = uint64((in[22]>>44|in[23]<<20)&mask + minv)
	out[45] = uint64((in[23]>>13)&mask + minv)
	out[46] = uint64((in[23]>>46|in[24]<<18)&mask + minv)
	out[47] = uint64((in[24]>>15)&mask + minv)
	out[48] = uint64((in[24]>>48|in[25]<<16)&mask + minv)
	out[49] = uint64((in[25]>>17)&mask + minv)
	out[50] = uint64((in[25]>>50|in[26]<<14)&mask + minv)
	out[51] = uint64((in[26]>>19)&mask + minv)
	out[52] = uint64((in[26]>>52|in[27]<<12)&mask + minv)
	out[53] = uint64((in[27]>>21)&mask + minv)
	out[54] = uint64((in[27]>>54|in[28]<<10)&mask + minv)
	out[55] = uint64((in[28]>>23)&mask + minv)
	out[56] = uint64((in[28]>>56|in[29]<<8)&mask + minv)
	out[57] = uint64((in[29]>>25)&mask + minv)
	out[58] = uint64((in[29]>>58|in[30]<<6)&mask + minv)
	out[59] = uint64((in[30]>>27)&mask + minv)
	out[60] = uint64((in[30]>>60|in[31]<<4)&mask + minv)
	out[61] = uint64((in[31]>>29)&mask + minv)
	out[62] = uint64((in[31]>>62|in[32]<<2)&mask + minv)
	out[63] = uint64((in[32]>>31)&mask + minv)
}

func br64_34(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[34]uint64)(p)
	mask := uint64((1 << 34) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>34|in[1]<<30)&mask + minv)
	out[2] = uint64((in[1]>>4)&mask + minv)
	out[3] = uint64((in[1]>>38|in[2]<<26)&mask + minv)
	out[4] = uint64((in[2]>>8)&mask + minv)
	out[5] = uint64((in[2]>>42|in[3]<<22)&mask + minv)
	out[6] = uint64((in[3]>>12)&mask + minv)
	out[7] = uint64((in[3]>>46|in[4]<<18)&mask + minv)
	out[8] = uint64((in[4]>>16)&mask + minv)
	out[9] = uint64((in[4]>>50|in[5]<<14)&mask + minv)
	out[10] = uint64((in[5]>>20)&mask + minv)
	out[11] = uint64((in[5]>>54|in[6]<<10)&mask + minv)
	out[12] = uint64((in[6]>>24)&mask + minv)
	out[13] = uint64((in[6]>>58|in[7]<<6)&mask + minv)
	out[14] = uint64((in[7]>>28)&mask + minv)
	out[15] = uint64((in[7]>>62|in[8]<<2)&mask + minv)
	out[16] = uint64((in[8]>>32|in[9]<<32)&mask + minv)
	out[17] = uint64((in[9]>>2)&mask + minv)
	out[18] = uint64((in[9]>>36|in[10]<<28)&mask + minv)
	out[19] = uint64((in[10]>>6)&mask + minv)
	out[20] = uint64((in[10]>>40|in[11]<<24)&mask + minv)
	out[21] = uint64((in[11]>>10)&mask + minv)
	out[22] = uint64((in[11]>>44|in[12]<<20)&mask + minv)
	out[23] = uint64((in[12]>>14)&mask + minv)
	out[24] = uint64((in[12]>>48|in[13]<<16)&mask + minv)
	out[25] = uint64((in[13]>>18)&mask + minv)
	out[26] = uint64((in[13]>>52|in[14]<<12)&mask + minv)
	out[27] = uint64((in[14]>>22)&mask + minv)
	out[28] = uint64((in[14]>>56|in[15]<<8)&mask + minv)
	out[29] = uint64((in[15]>>26)&mask + minv)
	out[30] = uint64((in[15]>>60|in[16]<<4)&mask + minv)
	out[31] = uint64((in[16]>>30)&mask + minv)
	out[32] = uint64(in[17]&mask + minv)
	out[33] = uint64((in[17]>>34|in[18]<<30)&mask + minv)
	out[34] = uint64((in[18]>>4)&mask + minv)
	out[35] = uint64((in[18]>>38|in[19]<<26)&mask + minv)
	out[36] = uint64((in[19]>>8)&mask + minv)
	out[37] = uint64((in[19]>>42|in[20]<<22)&mask + minv)
	out[38] = uint64((in[20]>>12)&mask + minv)
	out[39] = uint64((in[20]>>46|in[21]<<18)&mask + minv)
	out[40] = uint64((in[21]>>16)&mask + minv)
	out[41] = uint64((in[21]>>50|in[22]<<14)&mask + minv)
	out[42] = uint64((in[22]>>20)&mask + minv)
	out[43] = uint64((in[22]>>54|in[23]<<10)&mask + minv)
	out[44] = uint64((in[23]>>24)&mask + minv)
	out[45] = uint64((in[23]>>58|in[24]<<6)&mask + minv)
	out[46] = uint64((in[24]>>28)&mask + minv)
	out[47] = uint64((in[24]>>62|in[25]<<2)&mask + minv)
	out[48] = uint64((in[25]>>32|in[26]<<32)&mask + minv)
	out[49] = uint64((in[26]>>2)&mask + minv)
	out[50] = uint64((in[26]>>36|in[27]<<28)&mask + minv)
	out[51] = uint64((in[27]>>6)&mask + minv)
	out[52] = uint64((in[27]>>40|in[28]<<24)&mask + minv)
	out[53] = uint64((in[28]>>10)&mask + minv)
	out[54] = uint64((in[28]>>44|in[29]<<20)&mask + minv)
	out[55] = uint64((in[29]>>14)&mask + minv)
	out[56] = uint64((in[29]>>48|in[30]<<16)&mask + minv)
	out[57] = uint64((in[30]>>18)&mask + minv)
	out[58] = uint64((in[30]>>52|in[31]<<12)&mask + minv)
	out[59] = uint64((in[31]>>22)&mask + minv)
	out[60] = uint64((in[31]>>56|in[32]<<8)&mask + minv)
	out[61] = uint64((in[32]>>26)&mask + minv)
	out[62] = uint64((in[32]>>60|in[33]<<4)&mask + minv)
	out[63] = uint64((in[33]>>30)&mask + minv)
}

func br64_35(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[35]uint64)(p)
	mask := uint64((1 << 35) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>35|in[1]<<29)&mask + minv)
	out[2] = uint64((in[1]>>6)&mask + minv)
	out[3] = uint64((in[1]>>41|in[2]<<23)&mask + minv)
	out[4] = uint64((in[2]>>12)&mask + minv)
	out[5] = uint64((in[2]>>47|in[3]<<17)&mask + minv)
	out[6] = uint64((in[3]>>18)&mask + minv)
	out[7] = uint64((in[3]>>53|in[4]<<11)&mask + minv)
	out[8] = uint64((in[4]>>24)&mask + minv)
	out[9] = uint64((in[4]>>59|in[5]<<5)&mask + minv)
	out[10] = uint64((in[5]>>30|in[6]<<34)&mask + minv)
	out[11] = uint64((in[6]>>1)&mask + minv)
	out[12] = uint64((in[6]>>36|in[7]<<28)&mask + minv)
	out[13] = uint64((in[7]>>7)&mask + minv)
	out[14] = uint64((in[7]>>42|in[8]<<22)&mask + minv)
	out[15] = uint64((in[8]>>13)&mask + minv)
	out[16] = uint64((in[8]>>48|in[9]<<16)&mask + minv)
	out[17] = uint64((in[9]>>19)&mask + minv)
	out[18] = uint64((in[9]>>54|in[10]<<10)&mask + minv)
	out[19] = uint64((in[10]>>25)&mask + minv)
	out[20] = uint64((in[10]>>60|in[11]<<4)&mask + minv)
	out[21] = uint64((in[11]>>31|in[12]<<33)&mask + minv)
	out[22] = uint64((in[12]>>2)&mask + minv)
	out[23] = uint64((in[12]>>37|in[13]<<27)&mask + minv)
	out[24] = uint64((in[13]>>8)&mask + minv)
	out[25] = uint64((in[13]>>43|in[14]<<21)&mask + minv)
	out[26] = uint64((in[14]>>14)&mask + minv)
	out[27] = uint64((in[14]>>49|in[15]<<15)&mask + minv)
	out[28] = uint64((in[15]>>20)&mask + minv)
	out[29] = uint64((in[15]>>55|in[16]<<9)&mask + minv)
	out[30] = uint64((in[16]>>26)&mask + minv)
	out[31] = uint64((in[16]>>61|in[17]<<3)&mask + minv)
	out[32] = uint64((in[17]>>32|in[18]<<32)&mask + minv)
	out[33] = uint64((in[18]>>3)&mask + minv)
	out[34] = uint64((in[18]>>38|in[19]<<26)&mask + minv)
	out[35] = uint64((in[19]>>9)&mask + minv)
	out[36] = uint64((in[19]>>44|in[20]<<20)&mask + minv)
	out[37] = uint64((in[20]>>15)&mask + minv)
	out[38] = uint64((in[20]>>50|in[21]<<14)&mask + minv)
	out[39] = uint64((in[21]>>21)&mask + minv)
	out[40] = uint64((in[21]>>56|in[22]<<8)&mask + minv)
	out[41] = uint64((in[22]>>27)&mask + minv)
	out[42] = uint64((in[22]>>62|in[23]<<2)&mask + minv)
	out[43] = uint64((in[23]>>33|in[24]<<31)&mask + minv)
	out[44] = uint64((in[24]>>4)&mask + minv)
	out[45] = uint64((in[24]>>39|in[25]<<25)&mask + minv)
	out[46] = uint64((in[25]>>10)&mask + minv)
	out[47] = uint64((in[25]>>45|in[26]<<19)&mask + minv)
	out[48] = uint64((in[26]>>16)&mask + minv)
	out[49] = uint64((in[26]>>51|in[27]<<13)&mask + minv)
	out[50] = uint64((in[27]>>22)&mask + minv)
	out[51] = uint64((in[27]>>57|in[28]<<7)&mask + minv)
	out[52] = uint64((in[28]>>28)&mask + minv)
	out[53] = uint64((in[28]>>63|in[29]<<1)&mask + minv)
	out[54] = uint64((in[29]>>34|in[30]<<30)&mask + minv)
	out[55] = uint64((in[30]>>5)&mask + minv)
	out[56] = uint64((in[30]>>40|in[31]<<24)&mask + minv)
	out[57] = uint64((in[31]>>11)&mask + minv)
	out[58] = uint64((in[31]>>46|in[32]<<18)&mask + minv)
	out[59] = uint64((in[32]>>17)&mask + minv)
	out[60] = uint64((in[32]>>52|in[33]<<12)&mask + minv)
	out[61] = uint64((in[33]>>23)&mask + minv)
	out[62] = uint64((in[33]>>58|in[34]<<6)&mask + minv)
	out[63] = uint64((in[34]>>29)&mask + minv)
}

func br64_36(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[36]uint64)(p)
	mask := uint64((1 << 36) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>36|in[1]<<28)&mask + minv)
	out[2] = uint64((in[1]>>8)&mask + minv)
	out[3] = uint64((in[1]>>44|in[2]<<20)&mask + minv)
	out[4] = uint64((in[2]>>16)&mask + minv)
	out[5] = uint64((in[2]>>52|in[3]<<12)&mask + minv)
	out[6] = uint64((in[3]>>24)&mask + minv)
	out[7] = uint64((in[3]>>60|in[4]<<4)&mask + minv)
	out[8] = uint64((in[4]>>32|in[5]<<32)&mask + minv)
	out[9] = uint64((in[5]>>4)&mask + minv)
	out[10] = uint64((in[5]>>40|in[6]<<24)&mask + minv)
	out[11] = uint64((in[6]>>12)&mask + minv)
	out[12] = uint64((in[6]>>48|in[7]<<16)&mask + minv)
	out[13] = uint64((in[7]>>20)&mask + minv)
	out[14] = uint64((in[7]>>56|in[8]<<8)&mask + minv)
	out[15] = uint64((in[8]>>28)&mask + minv)
	out[16] = uint64(in[9]&mask + minv)
	out[17] = uint64((in[9]>>36|in[10]<<28)&mask + minv)
	out[18] = uint64((in[10]>>8)&mask + minv)
	out[19] = uint64((in[10]>>44|in[11]<<20)&mask + minv)
	out[20] = uint64((in[11]>>16)&mask + minv)
	out[21] = uint64((in[11]>>52|in[12]<<12)&mask + minv)
	out[22] = uint64((in[12]>>24)&mask + minv)
	out[23] = uint64((in[12]>>60|in[13]<<4)&mask + minv)
	out[24] = uint64((in[13]>>32|in[14]<<32)&mask + minv)
	out[25] = uint64((in[14]>>4)&mask + minv)
	out[26] = uint64((in[14]>>40|in[15]<<24)&mask + minv)
	out[27] = uint64((in[15]>>12)&mask + minv)
	out[28] = uint64((in[15]>>48|in[16]<<16)&mask + minv)
	out[29] = uint64((in[16]>>20)&mask + minv)
	out[30] = uint64((in[16]>>56|in[17]<<8)&mask + minv)
	out[31] = uint64((in[17]>>28)&mask + minv)
	out[32] = uint64(in[18]&mask + minv)
	out[33] = uint64((in[18]>>36|in[19]<<28)&mask + minv)
	out[34] = uint64((in[19]>>8)&mask + minv)
	out[35] = uint64((in[19]>>44|in[20]<<20)&mask + minv)
	out[36] = uint64((in[20]>>16)&mask + minv)
	out[37] = uint64((in[20]>>52|in[21]<<12)&mask + minv)
	out[38] = uint64((in[21]>>24)&mask + minv)
	out[39] = uint64((in[21]>>60|in[22]<<4)&mask + minv)
	out[40] = uint64((in[22]>>32|in[23]<<32)&mask + minv)
	out[41] = uint64((in[23]>>4)&mask + minv)
	out[42] = uint64((in[23]>>40|in[24]<<24)&mask + minv)
	out[43] = uint64((in[24]>>12)&mask + minv)
	out[44] = uint64((in[24]>>48|in[25]<<16)&mask + minv)
	out[45] = uint64((in[25]>>20)&mask + minv)
	out[46] = uint64((in[25]>>56|in[26]<<8)&mask + minv)
	out[47] = uint64((in[26]>>28)&mask + minv)
	out[48] = uint64(in[27]&mask + minv)
	out[49] = uint64((in[27]>>36|in[28]<<28)&mask + minv)
	out[50] = uint64((in[28]>>8)&mask + minv)
	out[51] = uint64((in[28]>>44|in[29]<<20)&mask + minv)
	out[52] = uint64((in[29]>>16)&mask + minv)
	out[53] = uint64((in[29]>>52|in[30]<<12)&mask + minv)
	out[54] = uint64((in[30]>>24)&mask + minv)
	out[55] = uint64((in[30]>>60|in[31]<<4)&mask + minv)
	out[56] = uint64((in[31]>>32|in[32]<<32)&mask + minv)
	out[57] = uint64((in[32]>>4)&mask + minv)
	out[58] = uint64((in[32]>>40|in[33]<<24)&mask + minv)
	out[59] = uint64((in[33]>>12)&mask + minv)
	out[60] = uint64((in[33]>>48|in[34]<<16)&mask + minv)
	out[61] = uint64((in[34]>>20)&mask + minv)
	out[62] = uint64((in[34]>>56|in[35]<<8)&mask + minv)
	out[63] = uint64((in[35]>>28)&mask + minv)
}

func br64_37(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[37]uint64)(p)
	mask := uint64((1 << 37) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>37|in[1]<<27)&mask + minv)
	out[2] = uint64((in[1]>>10)&mask + minv)
	out[3] = uint64((in[1]>>47|in[2]<<17)&mask + minv)
	out[4] = uint64((in[2]>>20)&mask + minv)
	out[5] = uint64((in[2]>>57|in[3]<<7)&mask + minv)
	out[6] = uint64((in[3]>>30|in[4]<<34)&mask + minv)
	out[7] = uint64((in[4]>>3)&mask + minv)
	out[8] = uint64((in[4]>>40|in[5]<<24)&mask + minv)
	out[9] = uint64((in[5]>>13)&mask + minv)
	out[10] = uint64((in[5]>>50|in[6]<<14)&mask + minv)
	out[11] = uint64((in[6]>>23)&mask + minv)
	out[12] = uint64((in[6]>>60|in[7]<<4)&mask + minv)
	out[13] = uint64((in[7]>>33|in[8]<<31)&mask + minv)
	out[14] = uint64((in[8]>>6)&mask + minv)
	out[15] = uint64((in[8]>>43|in[9]<<21)&mask + minv)
	out[16] = uint64((in[9]>>16)&mask + minv)
	out[17] = uint64((in[9]>>53|in[10]<<11)&mask + minv)
	out[18] = uint64((in[10]>>26)&mask + minv)
	out[19] = uint64((in[10]>>63|in[11]<<1)&mask + minv)
	out[20] = uint64((in[11]>>36|in[12]<<28)&mask + minv)
	out[21] = uint64((in[12]>>9)&mask + minv)
	out[22] = uint64((in[12]>>46|in[13]<<18)&mask + minv)
	out[23] = uint64((in[13]>>19)&mask + minv)
	out[24] = uint64((in[13]>>56|in[14]<<8)&mask + minv)
	out[25] = uint64((in[14]>>29|in[15]<<35)&mask + minv)
	out[26] = uint64((in[15]>>2)&mask + minv)
	out[27] = uint64((in[15]>>39|in[16]<<25)&mask + minv)
	out[28] = uint64((in[16]>>12)&mask + minv)
	out[29] = uint64((in[16]>>49|in[17]<<15)&mask + minv)
	out[30] = uint64((in[17]>>22)&mask + minv)
	out[31] = uint64((in[17]>>59|in[18]<<5)&mask + minv)
	out[32] = uint64((in[18]>>32|in[19]<<32)&mask + minv)
	out[33] = uint64((in[19]>>5)&mask + minv)
	out[34] = uint64((in[19]>>42|in[20]<<22)&mask + minv)
	out[35] = uint64((in[20]>>15)&mask + minv)
	out[36] = uint64((in[20]>>52|in[21]<<12)&mask + minv)
	out[37] = uint64((in[21]>>25)&mask + minv)
	out[38] = uint64((in[21]>>62|in[22]<<2)&mask + minv)
	out[39] = uint64((in[22]>>35|in[23]<<29)&mask + minv)
	out[40] = uint64((in[23]>>8)&mask + minv)
	out[41] = uint64((in[23]>>45|in[24]<<19)&mask + minv)
	out[42] = uint64((in[24]>>18)&mask + minv)
	out[43] = uint64((in[24]>>55|in[25]<<9)&mask + minv)
	out[44] = uint64((in[25]>>28|in[26]<<36)&mask + minv)
	out[45] = uint64((in[26]>>1)&mask + minv)
	out[46] = uint64((in[26]>>38|in[27]<<26)&mask + minv)
	out[47] = uint64((in[27]>>11)&mask + minv)
	out[48] = uint64((in[27]>>48|in[28]<<16)&mask + minv)
	out[49] = uint64((in[28]>>21)&mask + minv)
	out[50] = uint64((in[28]>>58|in[29]<<6)&mask + minv)
	out[51] = uint64((in[29]>>31|in[30]<<33)&mask + minv)
	out[52] = uint64((in[30]>>4)&mask + minv)
	out[53] = uint64((in[30]>>41|in[31]<<23)&mask + minv)
	out[54] = uint64((in[31]>>14)&mask + minv)
	out[55] = uint64((in[31]>>51|in[32]<<13)&mask + minv)
	out[56] = uint64((in[32]>>24)&mask + minv)
	out[57] = uint64((in[32]>>61|in[33]<<3)&mask + minv)
	out[58] = uint64((in[33]>>34|in[34]<<30)&mask + minv)
	out[59] = uint64((in[34]>>7)&mask + minv)
	out[60] = uint64((in[34]>>44|in[35]<<20)&mask + minv)
	out[61] = uint64((in[35]>>17)&mask + minv)
	out[62] = uint64((in[35]>>54|in[36]<<10)&mask + minv)
	out[63] = uint64((in[36]>>27)&mask + minv)
}

func br64_38(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[38]uint64)(p)
	mask := uint64((1 << 38) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>38|in[1]<<26)&mask + minv)
	out[2] = uint64((in[1]>>12)&mask + minv)
	out[3] = uint64((in[1]>>50|in[2]<<14)&mask + minv)
	out[4] = uint64((in[2]>>24)&mask + minv)
	out[5] = uint64((in[2]>>62|in[3]<<2)&mask + minv)
	out[6] = uint64((in[3]>>36|in[4]<<28)&mask + minv)
	out[7] = uint64((in[4]>>10)&mask + minv)
	out[8] = uint64((in[4]>>48|in[5]<<16)&mask + minv)
	out[9] = uint64((in[5]>>22)&mask + minv)
	out[10] = uint64((in[5]>>60|in[6]<<4)&mask + minv)
	out[11] = uint64((in[6]>>34|in[7]<<30)&mask + minv)
	out[12] = uint64((in[7]>>8)&mask + minv)
	out[13] = uint64((in[7]>>46|in[8]<<18)&mask + minv)
	out[14] = uint64((in[8]>>20)&mask + minv)
	out[15] = uint64((in[8]>>58|in[9]<<6)&mask + minv)
	out[16] = uint64((in[9]>>32|in[10]<<32)&mask + minv)
	out[17] = uint64((in[10]>>6)&mask + minv)
	out[18] = uint64((in[10]>>44|in[11]<<20)&mask + minv)
	out[19] = uint64((in[11]>>18)&mask + minv)
	out[20] = uint64((in[11]>>56|in[12]<<8)&mask + minv)
	out[21] = uint64((in[12]>>30|in[13]<<34)&mask + minv)
	out[22] = uint64((in[13]>>4)&mask + minv)
	out[23] = uint64((in[13]>>42|in[14]<<22)&mask + minv)
	out[24] = uint64((in[14]>>16)&mask + minv)
	out[25] = uint64((in[14]>>54|in[15]<<10)&mask + minv)
	out[26] = uint64((in[15]>>28|in[16]<<36)&mask + minv)
	out[27] = uint64((in[16]>>2)&mask + minv)
	out[28] = uint64((in[16]>>40|in[17]<<24)&mask + minv)
	out[29] = uint64((in[17]>>14)&mask + minv)
	out[30] = uint64((in[17]>>52|in[18]<<12)&mask + minv)
	out[31] = uint64((in[18]>>26)&mask + minv)
	out[32] = uint64(in[19]&mask + minv)
	out[33] = uint64((in[19]>>38|in[20]<<26)&mask + minv)
	out[34] = uint64((in[20]>>12)&mask + minv)
	out[35] = uint64((in[20]>>50|in[21]<<14)&mask + minv)
	out[36] = uint64((in[21]>>24)&mask + minv)
	out[37] = uint64((in[21]>>62|in[22]<<2)&mask + minv)
	out[38] = uint64((in[22]>>36|in[23]<<28)&mask + minv)
	out[39] = uint64((in[23]>>10)&mask + minv)
	out[40] = uint64((in[23]>>48|in[24]<<16)&mask + minv)
	out[41] = uint64((in[24]>>22)&mask + minv)
	out[42] = uint64((in[24]>>60|in[25]<<4)&mask + minv)
	out[43] = uint64((in[25]>>34|in[26]<<30)&mask + minv)
	out[44] = uint64((in[26]>>8)&mask + minv)
	out[45] = uint64((in[26]>>46|in[27]<<18)&mask + minv)
	out[46] = uint64((in[27]>>20)&mask + minv)
	out[47] = uint64((in[27]>>58|in[28]<<6)&mask + minv)
	out[48] = uint64((in[28]>>32|in[29]<<32)&mask + minv)
	out[49] = uint64((in[29]>>6)&mask + minv)
	out[50] = uint64((in[29]>>44|in[30]<<20)&mask + minv)
	out[51] = uint64((in[30]>>18)&mask + minv)
	out[52] = uint64((in[30]>>56|in[31]<<8)&mask + minv)
	out[53] = uint64((in[31]>>30|in[32]<<34)&mask + minv)
	out[54] = uint64((in[32]>>4)&mask + minv)
	out[55] = uint64((in[32]>>42|in[33]<<22)&mask + minv)
	out[56] = uint64((in[33]>>16)&mask + minv)
	out[57] = uint64((in[33]>>54|in[34]<<10)&mask + minv)
	out[58] = uint64((in[34]>>28|in[35]<<36)&mask + minv)
	out[59] = uint64((in[35]>>2)&mask + minv)
	out[60] = uint64((in[35]>>40|in[36]<<24)&mask + minv)
	out[61] = uint64((in[36]>>14)&mask + minv)
	out[62] = uint64((in[36]>>52|in[37]<<12)&mask + minv)
	out[63] = uint64((in[37]>>26)&mask + minv)
}

func br64_39(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[39]uint64)(p)
	mask := uint64((1 << 39) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>39|in[1]<<25)&mask + minv)
	out[2] = uint64((in[1]>>14)&mask + minv)
	out[3] = uint64((in[1]>>53|in[2]<<11)&mask + minv)
	out[4] = uint64((in[2]>>28|in[3]<<36)&mask + minv)
	out[5] = uint64((in[3]>>3)&mask + minv)
	out[6] = uint64((in[3]>>42|in[4]<<22)&mask + minv)
	out[7] = uint64((in[4]>>17)&mask + minv)
	out[8] = uint64((in[4]>>56|in[5]<<8)&mask + minv)
	out[9] = uint64((in[5]>>31|in[6]<<33)&mask + minv)
	out[10] = uint64((in[6]>>6)&mask + minv)
	out[11] = uint64((in[6]>>45|in[7]<<19)&mask + minv)
	out[12] = uint64((in[7]>>20)&mask + minv)
	out[13] = uint64((in[7]>>59|in[8]<<5)&mask + minv)
	out[14] = uint64((in[8]>>34|in[9]<<30)&mask + minv)
	out[15] = uint64((in[9]>>9)&mask + minv)
	out[16] = uint64((in[9]>>48|in[10]<<16)&mask + minv)
	out[17] = uint64((in[10]>>23)&mask + minv)
	out[18] = uint64((in[10]>>62|in[11]<<2)&mask + minv)
	out[19] = uint64((in[11]>>37|in[12]<<27)&mask + minv)
	out[20] = uint64((in[12]>>12)&mask + minv)
	out[21] = uint64((in[12]>>51|in[13]<<13)&mask + minv)
	out[22] = uint64((in[13]>>26|in[14]<<38)&mask + minv)
	out[23] = uint64((in[14]>>1)&mask + minv)
	out[24] = uint64((in[14]>>40|in[15]<<24)&mask + minv)
	out[25] = uint64((in[15]>>15)&mask + minv)
	out[26] = uint64((in[15]>>54|in[16]<<10)&mask + minv)
	out[27] = uint64((in[16]>>29|in[17]<<35)&mask + minv)
	out[28] = uint64((in[17]>>4)&mask + minv)
	out[29] = uint64((in[17]>>43|in[18]<<21)&mask + minv)
	out[30] = uint64((in[18]>>18)&mask + minv)
	out[31] = uint64((in[18]>>57|in[19]<<7)&mask + minv)
	out[32] = uint64((in[19]>>32|in[20]<<32)&mask + minv)
	out[33] = uint64((in[20]>>7)&mask + minv)
	out[34] = uint64((in[20]>>46|in[21]<<18)&mask + minv)
	out[35] = uint64((in[21]>>21)&mask + minv)
	out[36] = uint64((in[21]>>60|in[22]<<4)&mask + minv)
	out[37] = uint64((in[22]>>35|in[23]<<29)&mask + minv)
	out[38] = uint64((in[23]>>10)&mask + minv)
	out[39] = uint64((in[23]>>49|in[24]<<15)&mask + minv)
	out[40] = uint64((in[24]>>24)&mask + minv)
	out[41] = uint64((in[24]>>63|in[25]<<1)&mask + minv)
	out[42] = uint64((in[25]>>38|in[26]<<26)&mask + minv)
	out[43] = uint64((in[26]>>13)&mask + minv)
	out[44] = uint64((in[26]>>52|in[27]<<12)&mask + minv)
	out[45] = uint64((in[27]>>27|in[28]<<37)&mask + minv)
	out[46] = uint64((in[28]>>2)&mask + minv)
	out[47] = uint64((in[28]>>41|in[29]<<23)&mask + minv)
	out[48] = uint64((in[29]>>16)&mask + minv)
	out[49] = uint64((in[29]>>55|in[30]<<9)&mask + minv)
	out[50] = uint64((in[30]>>30|in[31]<<34)&mask + minv)
	out[51] = uint64((in[31]>>5)&mask + minv)
	out[52] = uint64((in[31]>>44|in[32]<<20)&mask + minv)
	out[53] = uint64((in[32]>>19)&mask + minv)
	out[54] = uint64((in[32]>>58|in[33]<<6)&mask + minv)
	out[55] = uint64((in[33]>>33|in[34]<<31)&mask + minv)
	out[56] = uint64((in[34]>>8)&mask + minv)
	out[57] = uint64((in[34]>>47|in[35]<<17)&mask + minv)
	out[58] = uint64((in[35]>>22)&mask + minv)
	out[59] = uint64((in[35]>>61|in[36]<<3)&mask + minv)
	out[60] = uint64((in[36]>>36|in[37]<<28)&mask + minv)
	out[61] = uint64((in[37]>>11)&mask + minv)
	out[62] = uint64((in[37]>>50|in[38]<<14)&mask + minv)
	out[63] = uint64((in[38]>>25)&mask + minv)
}

func br64_40(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[40]uint64)(p)
	mask := uint64((1 << 40) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>40|in[1]<<24)&mask + minv)
	out[2] = uint64((in[1]>>16)&mask + minv)
	out[3] = uint64((in[1]>>56|in[2]<<8)&mask + minv)
	out[4] = uint64((in[2]>>32|in[3]<<32)&mask + minv)
	out[5] = uint64((in[3]>>8)&mask + minv)
	out[6] = uint64((in[3]>>48|in[4]<<16)&mask + minv)
	out[7] = uint64((in[4]>>24)&mask + minv)
	out[8] = uint64(in[5]&mask + minv)
	out[9] = uint64((in[5]>>40|in[6]<<24)&mask + minv)
	out[10] = uint64((in[6]>>16)&mask + minv)
	out[11] = uint64((in[6]>>56|in[7]<<8)&mask + minv)
	out[12] = uint64((in[7]>>32|in[8]<<32)&mask + minv)
	out[13] = uint64((in[8]>>8)&mask + minv)
	out[14] = uint64((in[8]>>48|in[9]<<16)&mask + minv)
	out[15] = uint64((in[9]>>24)&mask + minv)
	out[16] = uint64(in[10]&mask + minv)
	out[17] = uint64((in[10]>>40|in[11]<<24)&mask + minv)
	out[18] = uint64((in[11]>>16)&mask + minv)
	out[19] = uint64((in[11]>>56|in[12]<<8)&mask + minv)
	out[20] = uint64((in[12]>>32|in[13]<<32)&mask + minv)
	out[21] = uint64((in[13]>>8)&mask + minv)
	out[22] = uint64((in[13]>>48|in[14]<<16)&mask + minv)
	out[23] = uint64((in[14]>>24)&mask + minv)
	out[24] = uint64(in[15]&mask + minv)
	out[25] = uint64((in[15]>>40|in[16]<<24)&mask + minv)
	out[26] = uint64((in[16]>>16)&mask + minv)
	out[27] = uint64((in[16]>>56|in[17]<<8)&mask + minv)
	out[28] = uint64((in[17]>>32|in[18]<<32)&mask + minv)
	out[29] = uint64((in[18]>>8)&mask + minv)
	out[30] = uint64((in[18]>>48|in[19]<<16)&mask + minv)
	out[31] = uint64((in[19]>>24)&mask + minv)
	out[32] = uint64(in[20]&mask + minv)
	out[33] = uint64((in[20]>>40|in[21]<<24)&mask + minv)
	out[34] = uint64((in[21]>>16)&mask + minv)
	out[35] = uint64((in[21]>>56|in[22]<<8)&mask + minv)
	out[36] = uint64((in[22]>>32|in[23]<<32)&mask + minv)
	out[37] = uint64((in[23]>>8)&mask + minv)
	out[38] = uint64((in[23]>>48|in[24]<<16)&mask + minv)
	out[39] = uint64((in[24]>>24)&mask + minv)
	out[40] = uint64(in[25]&mask + minv)
	out[41] = uint64((in[25]>>40|in[26]<<24)&mask + minv)
	out[42] = uint64((in[26]>>16)&mask + minv)
	out[43] = uint64((in[26]>>56|in[27]<<8)&mask + minv)
	out[44] = uint64((in[27]>>32|in[28]<<32)&mask + minv)
	out[45] = uint64((in[28]>>8)&mask + minv)
	out[46] = uint64((in[28]>>48|in[29]<<16)&mask + minv)
	out[47] = uint64((in[29]>>24)&mask + minv)
	out[48] = uint64(in[30]&mask + minv)
	out[49] = uint64((in[30]>>40|in[31]<<24)&mask + minv)
	out[50] = uint64((in[31]>>16)&mask + minv)
	out[51] = uint64((in[31]>>56|in[32]<<8)&mask + minv)
	out[52] = uint64((in[32]>>32|in[33]<<32)&mask + minv)
	out[53] = uint64((in[33]>>8)&mask + minv)
	out[54] = uint64((in[33]>>48|in[34]<<16)&mask + minv)
	out[55] = uint64((in[34]>>24)&mask + minv)
	out[56] = uint64(in[35]&mask + minv)
	out[57] = uint64((in[35]>>40|in[36]<<24)&mask + minv)
	out[58] = uint64((in[36]>>16)&mask + minv)
	out[59] = uint64((in[36]>>56|in[37]<<8)&mask + minv)
	out[60] = uint64((in[37]>>32|in[38]<<32)&mask + minv)
	out[61] = uint64((in[38]>>8)&mask + minv)
	out[62] = uint64((in[38]>>48|in[39]<<16)&mask + minv)
	out[63] = uint64((in[39]>>24)&mask + minv)
}

func br64_41(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[41]uint64)(p)
	mask := uint64((1 << 41) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>41|in[1]<<23)&mask + minv)
	out[2] = uint64((in[1]>>18)&mask + minv)
	out[3] = uint64((in[1]>>59|in[2]<<5)&mask + minv)
	out[4] = uint64((in[2]>>36|in[3]<<28)&mask + minv)
	out[5] = uint64((in[3]>>13)&mask + minv)
	out[6] = uint64((in[3]>>54|in[4]<<10)&mask + minv)
	out[7] = uint64((in[4]>>31|in[5]<<33)&mask + minv)
	out[8] = uint64((in[5]>>8)&mask + minv)
	out[9] = uint64((in[5]>>49|in[6]<<15)&mask + minv)
	out[10] = uint64((in[6]>>26|in[7]<<38)&mask + minv)
	out[11] = uint64((in[7]>>3)&mask + minv)
	out[12] = uint64((in[7]>>44|in[8]<<20)&mask + minv)
	out[13] = uint64((in[8]>>21)&mask + minv)
	out[14] = uint64((in[8]>>62|in[9]<<2)&mask + minv)
	out[15] = uint64((in[9]>>39|in[10]<<25)&mask + minv)
	out[16] = uint64((in[10]>>16)&mask + minv)
	out[17] = uint64((in[10]>>57|in[11]<<7)&mask + minv)
	out[18] = uint64((in[11]>>34|in[12]<<30)&mask + minv)
	out[19] = uint64((in[12]>>11)&mask + minv)
	out[20] = uint64((in[12]>>52|in[13]<<12)&mask + minv)
	out[21] = uint64((in[13]>>29|in[14]<<35)&mask + minv)
	out[22] = uint64((in[14]>>6)&mask + minv)
	out[23] = uint64((in[14]>>47|in[15]<<17)&mask + minv)
	out[24] = uint64((in[15]>>24|in[16]<<40)&mask + minv)
	out[25] = uint64((in[16]>>1)&mask + minv)
	out[26] = uint64((in[16]>>42|in[17]<<22)&mask + minv)
	out[27] = uint64((in[17]>>19)&mask + minv)
	out[28] = uint64((in[17]>>60|in[18]<<4)&mask + minv)
	out[29] = uint64((in[18]>>37|in[19]<<27)&mask + minv)
	out[30] = uint64((in[19]>>14)&mask + minv)
	out[31] = uint64((in[19]>>55|in[20]<<9)&mask + minv)
	out[32] = uint64((in[20]>>32|in[21]<<32)&mask + minv)
	out[33] = uint64((in[21]>>9)&mask + minv)
	out[34] = uint64((in[21]>>50|in[22]<<14)&mask + minv)
	out[35] = uint64((in[22]>>27|in[23]<<37)&mask + minv)
	out[36] = uint64((in[23]>>4)&mask + minv)
	out[37] = uint64((in[23]>>45|in[24]<<19)&mask + minv)
	out[38] = uint64((in[24]>>22)&mask + minv)
	out[39] = uint64((in[24]>>63|in[25]<<1)&mask + minv)
	out[40] = uint64((in[25]>>40|in[26]<<24)&mask + minv)
	out[41] = uint64((in[26]>>17)&mask + minv)
	out[42] = uint64((in[26]>>58|in[27]<<6)&mask + minv)
	out[43] = uint64((in[27]>>35|in[28]<<29)&mask + minv)
	out[44] = uint64((in[28]>>12)&mask + minv)
	out[45] = uint64((in[28]>>53|in[29]<<11)&mask + minv)
	out[46] = uint64((in[29]>>30|in[30]<<34)&mask + minv)
	out[47] = uint64((in[30]>>7)&mask + minv)
	out[48] = uint64((in[30]>>48|in[31]<<16)&mask + minv)
	out[49] = uint64((in[31]>>25|in[32]<<39)&mask + minv)
	out[50] = uint64((in[32]>>2)&mask + minv)
	out[51] = uint64((in[32]>>43|in[33]<<21)&mask + minv)
	out[52] = uint64((in[33]>>20)&mask + minv)
	out[53] = uint64((in[33]>>61|in[34]<<3)&mask + minv)
	out[54] = uint64((in[34]>>38|in[35]<<26)&mask + minv)
	out[55] = uint64((in[35]>>15)&mask + minv)
	out[56] = uint64((in[35]>>56|in[36]<<8)&mask + minv)
	out[57] = uint64((in[36]>>33|in[37]<<31)&mask + minv)
	out[58] = uint64((in[37]>>10)&mask + minv)
	out[59] = uint64((in[37]>>51|in[38]<<13)&mask + minv)
	out[60] = uint64((in[38]>>28|in[39]<<36)&mask + minv)
	out[61] = uint64((in[39]>>5)&mask + minv)
	out[62] = uint64((in[39]>>46|in[40]<<18)&mask + minv)
	out[63] = uint64((in[40]>>23)&mask + minv)
}

func br64_42(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[42]uint64)(p)
	mask := uint64((1 << 42) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>42|in[1]<<22)&mask + minv)
	out[2] = uint64((in[1]>>20)&mask + minv)
	out[3] = uint64((in[1]>>62|in[2]<<2)&mask + minv)
	out[4] = uint64((in[2]>>40|in[3]<<24)&mask + minv)
	out[5] = uint64((in[3]>>18)&mask + minv)
	out[6] = uint64((in[3]>>60|in[4]<<4)&mask + minv)
	out[7] = uint64((in[4]>>38|in[5]<<26)&mask + minv)
	out[8] = uint64((in[5]>>16)&mask + minv)
	out[9] = uint64((in[5]>>58|in[6]<<6)&mask + minv)
	out[10] = uint64((in[6]>>36|in[7]<<28)&mask + minv)
	out[11] = uint64((in[7]>>14)&mask + minv)
	out[12] = uint64((in[7]>>56|in[8]<<8)&mask + minv)
	out[13] = uint64((in[8]>>34|in[9]<<30)&mask + minv)
	out[14] = uint64((in[9]>>12)&mask + minv)
	out[15] = uint64((in[9]>>54|in[10]<<10)&mask + minv)
	out[16] = uint64((in[10]>>32|in[11]<<32)&mask + minv)
	out[17] = uint64((in[11]>>10)&mask + minv)
	out[18] = uint64((in[11]>>52|in[12]<<12)&mask + minv)
	out[19] = uint64((in[12]>>30|in[13]<<34)&mask + minv)
	out[20] = uint64((in[13]>>8)&mask + minv)
	out[21] = uint64((in[13]>>50|in[14]<<14)&mask + minv)
	out[22] = uint64((in[14]>>28|in[15]<<36)&mask + minv)
	out[23] = uint64((in[15]>>6)&mask + minv)
	out[24] = uint64((in[15]>>48|in[16]<<16)&mask + minv)
	out[25] = uint64((in[16]>>26|in[17]<<38)&mask + minv)
	out[26] = uint64((in[17]>>4)&mask + minv)
	out[27] = uint64((in[17]>>46|in[18]<<18)&mask + minv)
	out[28] = uint64((in[18]>>24|in[19]<<40)&mask + minv)
	out[29] = uint64((in[19]>>2)&mask + minv)
	out[30] = uint64((in[19]>>44|in[20]<<20)&mask + minv)
	out[31] = uint64((in[20]>>22)&mask + minv)
	out[32] = uint64(in[21]&mask + minv)
	out[33] = uint64((in[21]>>42|in[22]<<22)&mask + minv)
	out[34] = uint64((in[22]>>20)&mask + minv)
	out[35] = uint64((in[22]>>62|in[23]<<2)&mask + minv)
	out[36] = uint64((in[23]>>40|in[24]<<24)&mask + minv)
	out[37] = uint64((in[24]>>18)&mask + minv)
	out[38] = uint64((in[24]>>60|in[25]<<4)&mask + minv)
	out[39] = uint64((in[25]>>38|in[26]<<26)&mask + minv)
	out[40] = uint64((in[26]>>16)&mask + minv)
	out[41] = uint64((in[26]>>58|in[27]<<6)&mask + minv)
	out[42] = uint64((in[27]>>36|in[28]<<28)&mask + minv)
	out[43] = uint64((in[28]>>14)&mask + minv)
	out[44] = uint64((in[28]>>56|in[29]<<8)&mask + minv)
	out[45] = uint64((in[29]>>34|in[30]<<30)&mask + minv)
	out[46] = uint64((in[30]>>12)&mask + minv)
	out[47] = uint64((in[30]>>54|in[31]<<10)&mask + minv)
	out[48] = uint64((in[31]>>32|in[32]<<32)&mask + minv)
	out[49] = uint64((in[32]>>10)&mask + minv)
	out[50] = uint64((in[32]>>52|in[33]<<12)&mask + minv)
	out[51] = uint64((in[33]>>30|in[34]<<34)&mask + minv)
	out[52] = uint64((in[34]>>8)&mask + minv)
	out[53] = uint64((in[34]>>50|in[35]<<14)&mask + minv)
	out[54] = uint64((in[35]>>28|in[36]<<36)&mask + minv)
	out[55] = uint64((in[36]>>6)&mask + minv)
	out[56] = uint64((in[36]>>48|in[37]<<16)&mask + minv)
	out[57] = uint64((in[37]>>26|in[38]<<38)&mask + minv)
	out[58] = uint64((in[38]>>4)&mask + minv)
	out[59] = uint64((in[38]>>46|in[39]<<18)&mask + minv)
	out[60] = uint64((in[39]>>24|in[40]<<40)&mask + minv)
	out[61] = uint64((in[40]>>2)&mask + minv)
	out[62] = uint64((in[40]>>44|in[41]<<20)&mask + minv)
	out[63] = uint64((in[41]>>22)&mask + minv)
}

func br64_43(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[43]uint64)(p)
	mask := uint64((1 << 43) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>43|in[1]<<21)&mask + minv)
	out[2] = uint64((in[1]>>22|in[2]<<42)&mask + minv)
	out[3] = uint64((in[2]>>1)&mask + minv)
	out[4] = uint64((in[2]>>44|in[3]<<20)&mask + minv)
	out[5] = uint64((in[3]>>23|in[4]<<41)&mask + minv)
	out[6] = uint64((in[4]>>2)&mask + minv)
	out[7] = uint64((in[4]>>45|in[5]<<19)&mask + minv)
	out[8] = uint64((in[5]>>24|in[6]<<40)&mask + minv)
	out[9] = uint64((in[6]>>3)&mask + minv)
	out[10] = uint64((in[6]>>46|in[7]<<18)&mask + minv)
	out[11] = uint64((in[7]>>25|in[8]<<39)&mask + minv)
	out[12] = uint64((in[8]>>4)&mask + minv)
	out[13] = uint64((in[8]>>47|in[9]<<17)&mask + minv)
	out[14] = uint64((in[9]>>26|in[10]<<38)&mask + minv)
	out[15] = uint64((in[10]>>5)&mask + minv)
	out[16] = uint64((in[10]>>48|in[11]<<16)&mask + minv)
	out[17] = uint64((in[11]>>27|in[12]<<37)&mask + minv)
	out[18] = uint64((in[12]>>6)&mask + minv)
	out[19] = uint64((in[12]>>49|in[13]<<15)&mask + minv)
	out[20] = uint64((in[13]>>28|in[14]<<36)&mask + minv)
	out[21] = uint64((in[14]>>7)&mask + minv)
	out[22] = uint64((in[14]>>50|in[15]<<14)&mask + minv)
	out[23] = uint64((in[15]>>29|in[16]<<35)&mask + minv)
	out[24] = uint64((in[16]>>8)&mask + minv)
	out[25] = uint64((in[16]>>51|in[17]<<13)&mask + minv)
	out[26] = uint64((in[17]>>30|in[18]<<34)&mask + minv)
	out[27] = uint64((in[18]>>9)&mask + minv)
	out[28] = uint64((in[18]>>52|in[19]<<12)&mask + minv)
	out[29] = uint64((in[19]>>31|in[20]<<33)&mask + minv)
	out[30] = uint64((in[20]>>10)&mask + minv)
	out[31] = uint64((in[20]>>53|in[21]<<11)&mask + minv)
	out[32] = uint64((in[21]>>32|in[22]<<32)&mask + minv)
	out[33] = uint64((in[22]>>11)&mask + minv)
	out[34] = uint64((in[22]>>54|in[23]<<10)&mask + minv)
	out[35] = uint64((in[23]>>33|in[24]<<31)&mask + minv)
	out[36] = uint64((in[24]>>12)&mask + minv)
	out[37] = uint64((in[24]>>55|in[25]<<9)&mask + minv)
	out[38] = uint64((in[25]>>34|in[26]<<30)&mask + minv)
	out[39] = uint64((in[26]>>13)&mask + minv)
	out[40] = uint64((in[26]>>56|in[27]<<8)&mask + minv)
	out[41] = uint64((in[27]>>35|in[28]<<29)&mask + minv)
	out[42] = uint64((in[28]>>14)&mask + minv)
	out[43] = uint64((in[28]>>57|in[29]<<7)&mask + minv)
	out[44] = uint64((in[29]>>36|in[30]<<28)&mask + minv)
	out[45] = uint64((in[30]>>15)&mask + minv)
	out[46] = uint64((in[30]>>58|in[31]<<6)&mask + minv)
	out[47] = uint64((in[31]>>37|in[32]<<27)&mask + minv)
	out[48] = uint64((in[32]>>16)&mask + minv)
	out[49] = uint64((in[32]>>59|in[33]<<5)&mask + minv)
	out[50] = uint64((in[33]>>38|in[34]<<26)&mask + minv)
	out[51] = uint64((in[34]>>17)&mask + minv)
	out[52] = uint64((in[34]>>60|in[35]<<4)&mask + minv)
	out[53] = uint64((in[35]>>39|in[36]<<25)&mask + minv)
	out[54] = uint64((in[36]>>18)&mask + minv)
	out[55] = uint64((in[36]>>61|in[37]<<3)&mask + minv)
	out[56] = uint64((in[37]>>40|in[38]<<24)&mask + minv)
	out[57] = uint64((in[38]>>19)&mask + minv)
	out[58] = uint64((in[38]>>62|in[39]<<2)&mask + minv)
	out[59] = uint64((in[39]>>41|in[40]<<23)&mask + minv)
	out[60] = uint64((in[40]>>20)&mask + minv)
	out[61] = uint64((in[40]>>63|in[41]<<1)&mask + minv)
	out[62] = uint64((in[41]>>42|in[42]<<22)&mask + minv)
	out[63] = uint64((in[42]>>21)&mask + minv)
}

func br64_44(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[44]uint64)(p)
	mask := uint64((1 << 44) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>44|in[1]<<20)&mask + minv)
	out[2] = uint64((in[1]>>24|in[2]<<40)&mask + minv)
	out[3] = uint64((in[2]>>4)&mask + minv)
	out[4] = uint64((in[2]>>48|in[3]<<16)&mask + minv)
	out[5] = uint64((in[3]>>28|in[4]<<36)&mask + minv)
	out[6] = uint64((in[4]>>8)&mask + minv)
	out[7] = uint64((in[4]>>52|in[5]<<12)&mask + minv)
	out[8] = uint64((in[5]>>32|in[6]<<32)&mask + minv)
	out[9] = uint64((in[6]>>12)&mask + minv)
	out[10] = uint64((in[6]>>56|in[7]<<8)&mask + minv)
	out[11] = uint64((in[7]>>36|in[8]<<28)&mask + minv)
	out[12] = uint64((in[8]>>16)&mask + minv)
	out[13] = uint64((in[8]>>60|in[9]<<4)&mask + minv)
	out[14] = uint64((in[9]>>40|in[10]<<24)&mask + minv)
	out[15] = uint64((in[10]>>20)&mask + minv)
	out[16] = uint64(in[11]&mask + minv)
	out[17] = uint64((in[11]>>44|in[12]<<20)&mask + minv)
	out[18] = uint64((in[12]>>24|in[13]<<40)&mask + minv)
	out[19] = uint64((in[13]>>4)&mask + minv)
	out[20] = uint64((in[13]>>48|in[14]<<16)&mask + minv)
	out[21] = uint64((in[14]>>28|in[15]<<36)&mask + minv)
	out[22] = uint64((in[15]>>8)&mask + minv)
	out[23] = uint64((in[15]>>52|in[16]<<12)&mask + minv)
	out[24] = uint64((in[16]>>32|in[17]<<32)&mask + minv)
	out[25] = uint64((in[17]>>12)&mask + minv)
	out[26] = uint64((in[17]>>56|in[18]<<8)&mask + minv)
	out[27] = uint64((in[18]>>36|in[19]<<28)&mask + minv)
	out[28] = uint64((in[19]>>16)&mask + minv)
	out[29] = uint64((in[19]>>60|in[20]<<4)&mask + minv)
	out[30] = uint64((in[20]>>40|in[21]<<24)&mask + minv)
	out[31] = uint64((in[21]>>20)&mask + minv)
	out[32] = uint64(in[22]&mask + minv)
	out[33] = uint64((in[22]>>44|in[23]<<20)&mask + minv)
	out[34] = uint64((in[23]>>24|in[24]<<40)&mask + minv)
	out[35] = uint64((in[24]>>4)&mask + minv)
	out[36] = uint64((in[24]>>48|in[25]<<16)&mask + minv)
	out[37] = uint64((in[25]>>28|in[26]<<36)&mask + minv)
	out[38] = uint64((in[26]>>8)&mask + minv)
	out[39] = uint64((in[26]>>52|in[27]<<12)&mask + minv)
	out[40] = uint64((in[27]>>32|in[28]<<32)&mask + minv)
	out[41] = uint64((in[28]>>12)&mask + minv)
	out[42] = uint64((in[28]>>56|in[29]<<8)&mask + minv)
	out[43] = uint64((in[29]>>36|in[30]<<28)&mask + minv)
	out[44] = uint64((in[30]>>16)&mask + minv)
	out[45] = uint64((in[30]>>60|in[31]<<4)&mask + minv)
	out[46] = uint64((in[31]>>40|in[32]<<24)&mask + minv)
	out[47] = uint64((in[32]>>20)&mask + minv)
	out[48] = uint64(in[33]&mask + minv)
	out[49] = uint64((in[33]>>44|in[34]<<20)&mask + minv)
	out[50] = uint64((in[34]>>24|in[35]<<40)&mask + minv)
	out[51] = uint64((in[35]>>4)&mask + minv)
	out[52] = uint64((in[35]>>48|in[36]<<16)&mask + minv)
	out[53] = uint64((in[36]>>28|in[37]<<36)&mask + minv)
	out[54] = uint64((in[37]>>8)&mask + minv)
	out[55] = uint64((in[37]>>52|in[38]<<12)&mask + minv)
	out[56] = uint64((in[38]>>32|in[39]<<32)&mask + minv)
	out[57] = uint64((in[39]>>12)&mask + minv)
	out[58] = uint64((in[39]>>56|in[40]<<8)&mask + minv)
	out[59] = uint64((in[40]>>36|in[41]<<28)&mask + minv)
	out[60] = uint64((in[41]>>16)&mask + minv)
	out[61] = uint64((in[41]>>60|in[42]<<4)&mask + minv)
	out[62] = uint64((in[42]>>40|in[43]<<24)&mask + minv)
	out[63] = uint64((in[43]>>20)&mask + minv)
}

func br64_45(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[45]uint64)(p)
	mask := uint64((1 << 45) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>45|in[1]<<19)&mask + minv)
	out[2] = uint64((in[1]>>26|in[2]<<38)&mask + minv)
	out[3] = uint64((in[2]>>7)&mask + minv)
	out[4] = uint64((in[2]>>52|in[3]<<12)&mask + minv)
	out[5] = uint64((in[3]>>33|in[4]<<31)&mask + minv)
	out[6] = uint64((in[4]>>14)&mask + minv)
	out[7] = uint64((in[4]>>59|in[5]<<5)&mask + minv)
	out[8] = uint64((in[5]>>40|in[6]<<24)&mask + minv)
	out[9] = uint64((in[6]>>21|in[7]<<43)&mask + minv)
	out[10] = uint64((in[7]>>2)&mask + minv)
	out[11] = uint64((in[7]>>47|in[8]<<17)&mask + minv)
	out[12] = uint64((in[8]>>28|in[9]<<36)&mask + minv)
	out[13] = uint64((in[9]>>9)&mask + minv)
	out[14] = uint64((in[9]>>54|in[10]<<10)&mask + minv)
	out[15] = uint64((in[10]>>35|in[11]<<29)&mask + minv)
	out[16] = uint64((in[11]>>16)&mask + minv)
	out[17] = uint64((in[11]>>61|in[12]<<3)&mask + minv)
	out[18] = uint64((in[12]>>42|in[13]<<22)&mask + minv)
	out[19] = uint64((in[13]>>23|in[14]<<41)&mask + minv)
	out[20] = uint64((in[14]>>4)&mask + minv)
	out[21] = uint64((in[14]>>49|in[15]<<15)&mask + minv)
	out[22] = uint64((in[15]>>30|in[16]<<34)&mask + minv)
	out[23] = uint64((in[16]>>11)&mask + minv)
	out[24] = uint64((in[16]>>56|in[17]<<8)&mask + minv)
	out[25] = uint64((in[17]>>37|in[18]<<27)&mask + minv)
	out[26] = uint64((in[18]>>18)&mask + minv)
	out[27] = uint64((in[18]>>63|in[19]<<1)&mask + minv)
	out[28] = uint64((in[19]>>44|in[20]<<20)&mask + minv)
	out[29] = uint64((in[20]>>25|in[21]<<39)&mask + minv)
	out[30] = uint64((in[21]>>6)&mask + minv)
	out[31] = uint64((in[21]>>51|in[22]<<13)&mask + minv)
	out[32] = uint64((in[22]>>32|in[23]<<32)&mask + minv)
	out[33] = uint64((in[23]>>13)&mask + minv)
	out[34] = uint64((in[23]>>58|in[24]<<6)&mask + minv)
	out[35] = uint64((in[24]>>39|in[25]<<25)&mask + minv)
	out[36] = uint64((in[25]>>20|in[26]<<44)&mask + minv)
	out[37] = uint64((in[26]>>1)&mask + minv)
	out[38] = uint64((in[26]>>46|in[27]<<18)&mask + minv)
	out[39] = uint64((in[27]>>27|in[28]<<37)&mask + minv)
	out[40] = uint64((in[28]>>8)&mask + minv)
	out[41] = uint64((in[28]>>53|in[29]<<11)&mask + minv)
	out[42] = uint64((in[29]>>34|in[30]<<30)&mask + minv)
	out[43] = uint64((in[30]>>15)&mask + minv)
	out[44] = uint64((in[30]>>60|in[31]<<4)&mask + minv)
	out[45] = uint64((in[31]>>41|in[32]<<23)&mask + minv)
	out[46] = uint64((in[32]>>22|in[33]<<42)&mask + minv)
	out[47] = uint64((in[33]>>3)&mask + minv)
	out[48] = uint64((in[33]>>48|in[34]<<16)&mask + minv)
	out[49] = uint64((in[34]>>29|in[35]<<35)&mask + minv)
	out[50] = uint64((in[35]>>10)&mask + minv)
	out[51] = uint64((in[35]>>55|in[36]<<9)&mask + minv)
	out[52] = uint64((in[36]>>36|in[37]<<28)&mask + minv)
	out[53] = uint64((in[37]>>17)&mask + minv)
	out[54] = uint64((in[37]>>62|in[38]<<2)&mask + minv)
	out[55] = uint64((in[38]>>43|in[39]<<21)&mask + minv)
	out[56] = uint64((in[39]>>24|in[40]<<40)&mask + minv)
	out[57] = uint64((in[40]>>5)&mask + minv)
	out[58] = uint64((in[40]>>50|in[41]<<14)&mask + minv)
	out[59] = uint64((in[41]>>31|in[42]<<33)&mask + minv)
	out[60] = uint64((in[42]>>12)&mask + minv)
	out[61] = uint64((in[42]>>57|in[43]<<7)&mask + minv)
	out[62] = uint64((in[43]>>38|in[44]<<26)&mask + minv)
	out[63] = uint64((in[44]>>19)&mask + minv)
}

func br64_46(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[46]uint64)(p)
	mask := uint64((1 << 46) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>46|in[1]<<18)&mask + minv)
	out[2] = uint64((in[1]>>28|in[2]<<36)&mask + minv)
	out[3] = uint64((in[2]>>10)&mask + minv)
	out[4] = uint64((in[2]>>56|in[3]<<8)&mask + minv)
	out[5] = uint64((in[3]>>38|in[4]<<26)&mask + minv)
	out[6] = uint64((in[4]>>20|in[5]<<44)&mask + minv)
	out[7] = uint64((in[5]>>2)&mask + minv)
	out[8] = uint64((in[5]>>48|in[6]<<16)&mask + minv)
	out[9] = uint64((in[6]>>30|in[7]<<34)&mask + minv)
	out[10] = uint64((in[7]>>12)&mask + minv)
	out[11] = uint64((in[7]>>58|in[8]<<6)&mask + minv)
	out[12] = uint64((in[8]>>40|in[9]<<24)&mask + minv)
	out[13] = uint64((in[9]>>22|in[10]<<42)&mask + minv)
	out[14] = uint64((in[10]>>4)&mask + minv)
	out[15] = uint64((in[10]>>50|in[11]<<14)&mask + minv)
	out[16] = uint64((in[11]>>32|in[12]<<32)&mask + minv)
	out[17] = uint64((in[12]>>14)&mask + minv)
	out[18] = uint64((in[12]>>60|in[13]<<4)&mask + minv)
	out[19] = uint64((in[13]>>42|in[14]<<22)&mask + minv)
	out[20] = uint64((in[14]>>24|in[15]<<40)&mask + minv)
	out[21] = uint64((in[15]>>6)&mask + minv)
	out[22] = uint64((in[15]>>52|in[16]<<12)&mask + minv)
	out[23] = uint64((in[16]>>34|in[17]<<30)&mask + minv)
	out[24] = uint64((in[17]>>16)&mask + minv)
	out[25] = uint64((in[17]>>62|in[18]<<2)&mask + minv)
	out[26] = uint64((in[18]>>44|in[19]<<20)&mask + minv)
	out[27] = uint64((in[19]>>26|in[20]<<38)&mask + minv)
	out[28] = uint64((in[20]>>8)&mask + minv)
	out[29] = uint64((in[20]>>54|in[21]<<10)&mask + minv)
	out[30] = uint64((in[21]>>36|in[22]<<28)&mask + minv)
	out[31] = uint64((in[22]>>18)&mask + minv)
	out[32] = uint64(in[23]&mask + minv)
	out[33] = uint64((in[23]>>46|in[24]<<18)&mask + minv)
	out[34] = uint64((in[24]>>28|in[25]<<36)&mask + minv)
	out[35] = uint64((in[25]>>10)&mask + minv)
	out[36] = uint64((in[25]>>56|in[26]<<8)&mask + minv)
	out[37] = uint64((in[26]>>38|in[27]<<26)&mask + minv)
	out[38] = uint64((in[27]>>20|in[28]<<44)&mask + minv)
	out[39] = uint64((in[28]>>2)&mask + minv)
	out[40] = uint64((in[28]>>48|in[29]<<16)&mask + minv)
	out[41] = uint64((in[29]>>30|in[30]<<34)&mask + minv)
	out[42] = uint64((in[30]>>12)&mask + minv)
	out[43] = uint64((in[30]>>58|in[31]<<6)&mask + minv)
	out[44] = uint64((in[31]>>40|in[32]<<24)&mask + minv)
	out[45] = uint64((in[32]>>22|in[33]<<42)&mask + minv)
	out[46] = uint64((in[33]>>4)&mask + minv)
	out[47] = uint64((in[33]>>50|in[34]<<14)&mask + minv)
	out[48] = uint64((in[34]>>32|in[35]<<32)&mask + minv)
	out[49] = uint64((in[35]>>14)&mask + minv)
	out[50] = uint64((in[35]>>60|in[36]<<4)&mask + minv)
	out[51] = uint64((in[36]>>42|in[37]<<22)&mask + minv)
	out[52] = uint64((in[37]>>24|in[38]<<40)&mask + minv)
	out[53] = uint64((in[38]>>6)&mask + minv)
	out[54] = uint64((in[38]>>52|in[39]<<12)&mask + minv)
	out[55] = uint64((in[39]>>34|in[40]<<30)&mask + minv)
	out[56] = uint64((in[40]>>16)&mask + minv)
	out[57] = uint64((in[40]>>62|in[41]<<2)&mask + minv)
	out[58] = uint64((in[41]>>44|in[42]<<20)&mask + minv)
	out[59] = uint64((in[42]>>26|in[43]<<38)&mask + minv)
	out[60] = uint64((in[43]>>8)&mask + minv)
	out[61] = uint64((in[43]>>54|in[44]<<10)&mask + minv)
	out[62] = uint64((in[44]>>36|in[45]<<28)&mask + minv)
	out[63] = uint64((in[45]>>18)&mask + minv)
}

func br64_47(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[47]uint64)(p)
	mask := uint64((1 << 47) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>47|in[1]<<17)&mask + minv)
	out[2] = uint64((in[1]>>30|in[2]<<34)&mask + minv)
	out[3] = uint64((in[2]>>13)&mask + minv)
	out[4] = uint64((in[2]>>60|in[3]<<4)&mask + minv)
	out[5] = uint64((in[3]>>43|in[4]<<21)&mask + minv)
	out[6] = uint64((in[4]>>26|in[5]<<38)&mask + minv)
	out[7] = uint64((in[5]>>9)&mask + minv)
	out[8] = uint64((in[5]>>56|in[6]<<8)&mask + minv)
	out[9] = uint64((in[6]>>39|in[7]<<25)&mask + minv)
	out[10] = uint64((in[7]>>22|in[8]<<42)&mask + minv)
	out[11] = uint64((in[8]>>5)&mask + minv)
	out[12] = uint64((in[8]>>52|in[9]<<12)&mask + minv)
	out[13] = uint64((in[9]>>35|in[10]<<29)&mask + minv)
	out[14] = uint64((in[10]>>18|in[11]<<46)&mask + minv)
	out[15] = uint64((in[11]>>1)&mask + minv)
	out[16] = uint64((in[11]>>48|in[12]<<16)&mask + minv)
	out[17] = uint64((in[12]>>31|in[13]<<33)&mask + minv)
	out[18] = uint64((in[13]>>14)&mask + minv)
	out[19] = uint64((in[13]>>61|in[14]<<3)&mask + minv)
	out[20] = uint64((in[14]>>44|in[15]<<20)&mask + minv)
	out[21] = uint64((in[15]>>27|in[16]<<37)&mask + minv)
	out[22] = uint64((in[16]>>10)&mask + minv)
	out[23] = uint64((in[16]>>57|in[17]<<7)&mask + minv)
	out[24] = uint64((in[17]>>40|in[18]<<24)&mask + minv)
	out[25] = uint64((in[18]>>23|in[19]<<41)&mask + minv)
	out[26] = uint64((in[19]>>6)&mask + minv)
	out[27] = uint64((in[19]>>53|in[20]<<11)&mask + minv)
	out[28] = uint64((in[20]>>36|in[21]<<28)&mask + minv)
	out[29] = uint64((in[21]>>19|in[22]<<45)&mask + minv)
	out[30] = uint64((in[22]>>2)&mask + minv)
	out[31] = uint64((in[22]>>49|in[23]<<15)&mask + minv)
	out[32] = uint64((in[23]>>32|in[24]<<32)&mask + minv)
	out[33] = uint64((in[24]>>15)&mask + minv)
	out[34] = uint64((in[24]>>62|in[25]<<2)&mask + minv)
	out[35] = uint64((in[25]>>45|in[26]<<19)&mask + minv)
	out[36] = uint64((in[26]>>28|in[27]<<36)&mask + minv)
	out[37] = uint64((in[27]>>11)&mask + minv)
	out[38] = uint64((in[27]>>58|in[28]<<6)&mask + minv)
	out[39] = uint64((in[28]>>41|in[29]<<23)&mask + minv)
	out[40] = uint64((in[29]>>24|in[30]<<40)&mask + minv)
	out[41] = uint64((in[30]>>7)&mask + minv)
	out[42] = uint64((in[30]>>54|in[31]<<10)&mask + minv)
	out[43] = uint64((in[31]>>37|in[32]<<27)&mask + minv)
	out[44] = uint64((in[32]>>20|in[33]<<44)&mask + minv)
	out[45] = uint64((in[33]>>3)&mask + minv)
	out[46] = uint64((in[33]>>50|in[34]<<14)&mask + minv)
	out[47] = uint64((in[34]>>33|in[35]<<31)&mask + minv)
	out[48] = uint64((in[35]>>16)&mask + minv)
	out[49] = uint64((in[35]>>63|in[36]<<1)&mask + minv)
	out[50] = uint64((in[36]>>46|in[37]<<18)&mask + minv)
	out[51] = uint64((in[37]>>29|in[38]<<35)&mask + minv)
	out[52] = uint64((in[38]>>12)&mask + minv)
	out[53] = uint64((in[38]>>59|in[39]<<5)&mask + minv)
	out[54] = uint64((in[39]>>42|in[40]<<22)&mask + minv)
	out[55] = uint64((in[40]>>25|in[41]<<39)&mask + minv)
	out[56] = uint64((in[41]>>8)&mask + minv)
	out[57] = uint64((in[41]>>55|in[42]<<9)&mask + minv)
	out[58] = uint64((in[42]>>38|in[43]<<26)&mask + minv)
	out[59] = uint64((in[43]>>21|in[44]<<43)&mask + minv)
	out[60] = uint64((in[44]>>4)&mask + minv)
	out[61] = uint64((in[44]>>51|in[45]<<13)&mask + minv)
	out[62] = uint64((in[45]>>34|in[46]<<30)&mask + minv)
	out[63] = uint64((in[46]>>17)&mask + minv)
}

func br64_48(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[48]uint64)(p)
	mask := uint64((1 << 48) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>48|in[1]<<16)&mask + minv)
	out[2] = uint64((in[1]>>32|in[2]<<32)&mask + minv)
	out[3] = uint64((in[2]>>16)&mask + minv)
	out[4] = uint64(in[3]&mask + minv)
	out[5] = uint64((in[3]>>48|in[4]<<16)&mask + minv)
	out[6] = uint64((in[4]>>32|in[5]<<32)&mask + minv)
	out[7] = uint64((in[5]>>16)&mask + minv)
	out[8] = uint64(in[6]&mask + minv)
	out[9] = uint64((in[6]>>48|in[7]<<16)&mask + minv)
	out[10] = uint64((in[7]>>32|in[8]<<32)&mask + minv)
	out[11] = uint64((in[8]>>16)&mask + minv)
	out[12] = uint64(in[9]&mask + minv)
	out[13] = uint64((in[9]>>48|in[10]<<16)&mask + minv)
	out[14] = uint64((in[10]>>32|in[11]<<32)&mask + minv)
	out[15] = uint64((in[11]>>16)&mask + minv)
	out[16] = uint64(in[12]&mask + minv)
	out[17] = uint64((in[12]>>48|in[13]<<16)&mask + minv)
	out[18] = uint64((in[13]>>32|in[14]<<32)&mask + minv)
	out[19] = uint64((in[14]>>16)&mask + minv)
	out[20] = uint64(in[15]&mask + minv)
	out[21] = uint64((in[15]>>48|in[16]<<16)&mask + minv)
	out[22] = uint64((in[16]>>32|in[17]<<32)&mask + minv)
	out[23] = uint64((in[17]>>16)&mask + minv)
	out[24] = uint64(in[18]&mask + minv)
	out[25] = uint64((in[18]>>48|in[19]<<16)&mask + minv)
	out[26] = uint64((in[19]>>32|in[20]<<32)&mask + minv)
	out[27] = uint64((in[20]>>16)&mask + minv)
	out[28] = uint64(in[21]&mask + minv)
	out[29] = uint64((in[21]>>48|in[22]<<16)&mask + minv)
	out[30] = uint64((in[22]>>32|in[23]<<32)&mask + minv)
	out[31] = uint64((in[23]>>16)&mask + minv)
	out[32] = uint64(in[24]&mask + minv)
	out[33] = uint64((in[24]>>48|in[25]<<16)&mask + minv)
	out[34] = uint64((in[25]>>32|in[26]<<32)&mask + minv)
	out[35] = uint64((in[26]>>16)&mask + minv)
	out[36] = uint64(in[27]&mask + minv)
	out[37] = uint64((in[27]>>48|in[28]<<16)&mask + minv)
	out[38] = uint64((in[28]>>32|in[29]<<32)&mask + minv)
	out[39] = uint64((in[29]>>16)&mask + minv)
	out[40] = uint64(in[30]&mask + minv)
	out[41] = uint64((in[30]>>48|in[31]<<16)&mask + minv)
	out[42] = uint64((in[31]>>32|in[32]<<32)&mask + minv)
	out[43] = uint64((in[32]>>16)&mask + minv)
	out[44] = uint64(in[33]&mask + minv)
	out[45] = uint64((in[33]>>48|in[34]<<16)&mask + minv)
	out[46] = uint64((in[34]>>32|in[35]<<32)&mask + minv)
	out[47] = uint64((in[35]>>16)&mask + minv)
	out[48] = uint64(in[36]&mask + minv)
	out[49] = uint64((in[36]>>48|in[37]<<16)&mask + minv)
	out[50] = uint64((in[37]>>32|in[38]<<32)&mask + minv)
	out[51] = uint64((in[38]>>16)&mask + minv)
	out[52] = uint64(in[39]&mask + minv)
	out[53] = uint64((in[39]>>48|in[40]<<16)&mask + minv)
	out[54] = uint64((in[40]>>32|in[41]<<32)&mask + minv)
	out[55] = uint64((in[41]>>16)&mask + minv)
	out[56] = uint64(in[42]&mask + minv)
	out[57] = uint64((in[42]>>48|in[43]<<16)&mask + minv)
	out[58] = uint64((in[43]>>32|in[44]<<32)&mask + minv)
	out[59] = uint64((in[44]>>16)&mask + minv)
	out[60] = uint64(in[45]&mask + minv)
	out[61] = uint64((in[45]>>48|in[46]<<16)&mask + minv)
	out[62] = uint64((in[46]>>32|in[47]<<32)&mask + minv)
	out[63] = uint64((in[47]>>16)&mask + minv)
}

func br64_49(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[49]uint64)(p)
	mask := uint64((1 << 49) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>49|in[1]<<15)&mask + minv)
	out[2] = uint64((in[1]>>34|in[2]<<30)&mask + minv)
	out[3] = uint64((in[2]>>19|in[3]<<45)&mask + minv)
	out[4] = uint64((in[3]>>4)&mask + minv)
	out[5] = uint64((in[3]>>53|in[4]<<11)&mask + minv)
	out[6] = uint64((in[4]>>38|in[5]<<26)&mask + minv)
	out[7] = uint64((in[5]>>23|in[6]<<41)&mask + minv)
	out[8] = uint64((in[6]>>8)&mask + minv)
	out[9] = uint64((in[6]>>57|in[7]<<7)&mask + minv)
	out[10] = uint64((in[7]>>42|in[8]<<22)&mask + minv)
	out[11] = uint64((in[8]>>27|in[9]<<37)&mask + minv)
	out[12] = uint64((in[9]>>12)&mask + minv)
	out[13] = uint64((in[9]>>61|in[10]<<3)&mask + minv)
	out[14] = uint64((in[10]>>46|in[11]<<18)&mask + minv)
	out[15] = uint64((in[11]>>31|in[12]<<33)&mask + minv)
	out[16] = uint64((in[12]>>16|in[13]<<48)&mask + minv)
	out[17] = uint64((in[13]>>1)&mask + minv)
	out[18] = uint64((in[13]>>50|in[14]<<14)&mask + minv)
	out[19] = uint64((in[14]>>35|in[15]<<29)&mask + minv)
	out[20] = uint64((in[15]>>20|in[16]<<44)&mask + minv)
	out[21] = uint64((in[16]>>5)&mask + minv)
	out[22] = uint64((in[16]>>54|in[17]<<10)&mask + minv)
	out[23] = uint64((in[17]>>39|in[18]<<25)&mask + minv)
	out[24] = uint64((in[18]>>24|in[19]<<40)&mask + minv)
	out[25] = uint64((in[19]>>9)&mask + minv)
	out[26] = uint64((in[19]>>58|in[20]<<6)&mask + minv)
	out[27] = uint64((in[20]>>43|in[21]<<21)&mask + minv)
	out[28] = uint64((in[21]>>28|in[22]<<36)&mask + minv)
	out[29] = uint64((in[22]>>13)&mask + minv)
	out[30] = uint64((in[22]>>62|in[23]<<2)&mask + minv)
	out[31] = uint64((in[23]>>47|in[24]<<17)&mask + minv)
	out[32] = uint64((in[24]>>32|in[25]<<32)&mask + minv)
	out[33] = uint64((in[25]>>17|in[26]<<47)&mask + minv)
	out[34] = uint64((in[26]>>2)&mask + minv)
	out[35] = uint64((in[26]>>51|in[27]<<13)&mask + minv)
	out[36] = uint64((in[27]>>36|in[28]<<28)&mask + minv)
	out[37] = uint64((in[28]>>21|in[29]<<43)&mask + minv)
	out[38] = uint64((in[29]>>6)&mask + minv)
	out[39] = uint64((in[29]>>55|in[30]<<9)&mask + minv)
	out[40] = uint64((in[30]>>40|in[31]<<24)&mask + minv)
	out[41] = uint64((in[31]>>25|in[32]<<39)&mask + minv)
	out[42] = uint64((in[32]>>10)&mask + minv)
	out[43] = uint64((in[32]>>59|in[33]<<5)&mask + minv)
	out[44] = uint64((in[33]>>44|in[34]<<20)&mask + minv)
	out[45] = uint64((in[34]>>29|in[35]<<35)&mask + minv)
	out[46] = uint64((in[35]>>14)&mask + minv)
	out[47] = uint64((in[35]>>63|in[36]<<1)&mask + minv)
	out[48] = uint64((in[36]>>48|in[37]<<16)&mask + minv)
	out[49] = uint64((in[37]>>33|in[38]<<31)&mask + minv)
	out[50] = uint64((in[38]>>18|in[39]<<46)&mask + minv)
	out[51] = uint64((in[39]>>3)&mask + minv)
	out[52] = uint64((in[39]>>52|in[40]<<12)&mask + minv)
	out[53] = uint64((in[40]>>37|in[41]<<27)&mask + minv)
	out[54] = uint64((in[41]>>22|in[42]<<42)&mask + minv)
	out[55] = uint64((in[42]>>7)&mask + minv)
	out[56] = uint64((in[42]>>56|in[43]<<8)&mask + minv)
	out[57] = uint64((in[43]>>41|in[44]<<23)&mask + minv)
	out[58] = uint64((in[44]>>26|in[45]<<38)&mask + minv)
	out[59] = uint64((in[45]>>11)&mask + minv)
	out[60] = uint64((in[45]>>60|in[46]<<4)&mask + minv)
	out[61] = uint64((in[46]>>45|in[47]<<19)&mask + minv)
	out[62] = uint64((in[47]>>30|in[48]<<34)&mask + minv)
	out[63] = uint64((in[48]>>15)&mask + minv)
}

func br64_50(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[50]uint64)(p)
	mask := uint64((1 << 50) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>50|in[1]<<14)&mask + minv)
	out[2] = uint64((in[1]>>36|in[2]<<28)&mask + minv)
	out[3] = uint64((in[2]>>22|in[3]<<42)&mask + minv)
	out[4] = uint64((in[3]>>8)&mask + minv)
	out[5] = uint64((in[3]>>58|in[4]<<6)&mask + minv)
	out[6] = uint64((in[4]>>44|in[5]<<20)&mask + minv)
	out[7] = uint64((in[5]>>30|in[6]<<34)&mask + minv)
	out[8] = uint64((in[6]>>16|in[7]<<48)&mask + minv)
	out[9] = uint64((in[7]>>2)&mask + minv)
	out[10] = uint64((in[7]>>52|in[8]<<12)&mask + minv)
	out[11] = uint64((in[8]>>38|in[9]<<26)&mask + minv)
	out[12] = uint64((in[9]>>24|in[10]<<40)&mask + minv)
	out[13] = uint64((in[10]>>10)&mask + minv)
	out[14] = uint64((in[10]>>60|in[11]<<4)&mask + minv)
	out[15] = uint64((in[11]>>46|in[12]<<18)&mask + minv)
	out[16] = uint64((in[12]>>32|in[13]<<32)&mask + minv)
	out[17] = uint64((in[13]>>18|in[14]<<46)&mask + minv)
	out[18] = uint64((in[14]>>4)&mask + minv)
	out[19] = uint64((in[14]>>54|in[15]<<10)&mask + minv)
	out[20] = uint64((in[15]>>40|in[16]<<24)&mask + minv)
	out[21] = uint64((in[16]>>26|in[17]<<38)&mask + minv)
	out[22] = uint64((in[17]>>12)&mask + minv)
	out[23] = uint64((in[17]>>62|in[18]<<2)&mask + minv)
	out[24] = uint64((in[18]>>48|in[19]<<16)&mask + minv)
	out[25] = uint64((in[19]>>34|in[20]<<30)&mask + minv)
	out[26] = uint64((in[20]>>20|in[21]<<44)&mask + minv)
	out[27] = uint64((in[21]>>6)&mask + minv)
	out[28] = uint64((in[21]>>56|in[22]<<8)&mask + minv)
	out[29] = uint64((in[22]>>42|in[23]<<22)&mask + minv)
	out[30] = uint64((in[23]>>28|in[24]<<36)&mask + minv)
	out[31] = uint64((in[24]>>14)&mask + minv)
	out[32] = uint64(in[25]&mask + minv)
	out[33] = uint64((in[25]>>50|in[26]<<14)&mask + minv)
	out[34] = uint64((in[26]>>36|in[27]<<28)&mask + minv)
	out[35] = uint64((in[27]>>22|in[28]<<42)&mask + minv)
	out[36] = uint64((in[28]>>8)&mask + minv)
	out[37] = uint64((in[28]>>58|in[29]<<6)&mask + minv)
	out[38] = uint64((in[29]>>44|in[30]<<20)&mask + minv)
	out[39] = uint64((in[30]>>30|in[31]<<34)&mask + minv)
	out[40] = uint64((in[31]>>16|in[32]<<48)&mask + minv)
	out[41] = uint64((in[32]>>2)&mask + minv)
	out[42] = uint64((in[32]>>52|in[33]<<12)&mask + minv)
	out[43] = uint64((in[33]>>38|in[34]<<26)&mask + minv)
	out[44] = uint64((in[34]>>24|in[35]<<40)&mask + minv)
	out[45] = uint64((in[35]>>10)&mask + minv)
	out[46] = uint64((in[35]>>60|in[36]<<4)&mask + minv)
	out[47] = uint64((in[36]>>46|in[37]<<18)&mask + minv)
	out[48] = uint64((in[37]>>32|in[38]<<32)&mask + minv)
	out[49] = uint64((in[38]>>18|in[39]<<46)&mask + minv)
	out[50] = uint64((in[39]>>4)&mask + minv)
	out[51] = uint64((in[39]>>54|in[40]<<10)&mask + minv)
	out[52] = uint64((in[40]>>40|in[41]<<24)&mask + minv)
	out[53] = uint64((in[41]>>26|in[42]<<38)&mask + minv)
	out[54] = uint64((in[42]>>12)&mask + minv)
	out[55] = uint64((in[42]>>62|in[43]<<2)&mask + minv)
	out[56] = uint64((in[43]>>48|in[44]<<16)&mask + minv)
	out[57] = uint64((in[44]>>34|in[45]<<30)&mask + minv)
	out[58] = uint64((in[45]>>20|in[46]<<44)&mask + minv)
	out[59] = uint64((in[46]>>6)&mask + minv)
	out[60] = uint64((in[46]>>56|in[47]<<8)&mask + minv)
	out[61] = uint64((in[47]>>42|in[48]<<22)&mask + minv)
	out[62] = uint64((in[48]>>28|in[49]<<36)&mask + minv)
	out[63] = uint64((in[49]>>14)&mask + minv)
}

func br64_51(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[51]uint64)(p)
	mask := uint64((1 << 51) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>51|in[1]<<13)&mask + minv)
	out[2] = uint64((in[1]>>38|in[2]<<26)&mask + minv)
	out[3] = uint64((in[2]>>25|in[3]<<39)&mask + minv)
	out[4] = uint64((in[3]>>12)&mask + minv)
	out[5] = uint64((in[3]>>63|in[4]<<1)&mask + minv)
	out[6] = uint64((in[4]>>50|in[5]<<14)&mask + minv)
	out[7] = uint64((in[5]>>37|in[6]<<27)&mask + minv)
	out[8] = uint64((in[6]>>24|in[7]<<40)&mask + minv)
	out[9] = uint64((in[7]>>11)&mask + minv)
	out[10] = uint64((in[7]>>62|in[8]<<2)&mask + minv)
	out[11] = uint64((in[8]>>49|in[9]<<15)&mask + minv)
	out[12] = uint64((in[9]>>36|in[10]<<28)&mask + minv)
	out[13] = uint64((in[10]>>23|in[11]<<41)&mask + minv)
	out[14] = uint64((in[11]>>10)&mask + minv)
	out[15] = uint64((in[11]>>61|in[12]<<3)&mask + minv)
	out[16] = uint64((in[12]>>48|in[13]<<16)&mask + minv)
	out[17] = uint64((in[13]>>35|in[14]<<29)&mask + minv)
	out[18] = uint64((in[14]>>22|in[15]<<42)&mask + minv)
	out[19] = uint64((in[15]>>9)&mask + minv)
	out[20] = uint64((in[15]>>60|in[16]<<4)&mask + minv)
	out[21] = uint64((in[16]>>47|in[17]<<17)&mask + minv)
	out[22] = uint64((in[17]>>34|in[18]<<30)&mask + minv)
	out[23] = uint64((in[18]>>21|in[19]<<43)&mask + minv)
	out[24] = uint64((in[19]>>8)&mask + minv)
	out[25] = uint64((in[19]>>59|in[20]<<5)&mask + minv)
	out[26] = uint64((in[20]>>46|in[21]<<18)&mask + minv)
	out[27] = uint64((in[21]>>33|in[22]<<31)&mask + minv)
	out[28] = uint64((in[22]>>20|in[23]<<44)&mask + minv)
	out[29] = uint64((in[23]>>7)&mask + minv)
	out[30] = uint64((in[23]>>58|in[24]<<6)&mask + minv)
	out[31] = uint64((in[24]>>45|in[25]<<19)&mask + minv)
	out[32] = uint64((in[25]>>32|in[26]<<32)&mask + minv)
	out[33] = uint64((in[26]>>19|in[27]<<45)&mask + minv)
	out[34] = uint64((in[27]>>6)&mask + minv)
	out[35] = uint64((in[27]>>57|in[28]<<7)&mask + minv)
	out[36] = uint64((in[28]>>44|in[29]<<20)&mask + minv)
	out[37] = uint64((in[29]>>31|in[30]<<33)&mask + minv)
	out[38] = uint64((in[30]>>18|in[31]<<46)&mask + minv)
	out[39] = uint64((in[31]>>5)&mask + minv)
	out[40] = uint64((in[31]>>56|in[32]<<8)&mask + minv)
	out[41] = uint64((in[32]>>43|in[33]<<21)&mask + minv)
	out[42] = uint64((in[33]>>30|in[34]<<34)&mask + minv)
	out[43] = uint64((in[34]>>17|in[35]<<47)&mask + minv)
	out[44] = uint64((in[35]>>4)&mask + minv)
	out[45] = uint64((in[35]>>55|in[36]<<9)&mask + minv)
	out[46] = uint64((in[36]>>42|in[37]<<22)&mask + minv)
	out[47] = uint64((in[37]>>29|in[38]<<35)&mask + minv)
	out[48] = uint64((in[38]>>16|in[39]<<48)&mask + minv)
	out[49] = uint64((in[39]>>3)&mask + minv)
	out[50] = uint64((in[39]>>54|in[40]<<10)&mask + minv)
	out[51] = uint64((in[40]>>41|in[41]<<23)&mask + minv)
	out[52] = uint64((in[41]>>28|in[42]<<36)&mask + minv)
	out[53] = uint64((in[42]>>15|in[43]<<49)&mask + minv)
	out[54] = uint64((in[43]>>2)&mask + minv)
	out[55] = uint64((in[43]>>53|in[44]<<11)&mask + minv)
	out[56] = uint64((in[44]>>40|in[45]<<24)&mask + minv)
	out[57] = uint64((in[45]>>27|in[46]<<37)&mask + minv)
	out[58] = uint64((in[46]>>14|in[47]<<50)&mask + minv)
	out[59] = uint64((in[47]>>1)&mask + minv)
	out[60] = uint64((in[47]>>52|in[48]<<12)&mask + minv)
	out[61] = uint64((in[48]>>39|in[49]<<25)&mask + minv)
	out[62] = uint64((in[49]>>26|in[50]<<38)&mask + minv)
	out[63] = uint64((in[50]>>13)&mask + minv)
}

func br64_52(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[52]uint64)(p)
	mask := uint64((1 << 52) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>52|in[1]<<12)&mask + minv)
	out[2] = uint64((in[1]>>40|in[2]<<24)&mask + minv)
	out[3] = uint64((in[2]>>28|in[3]<<36)&mask + minv)
	out[4] = uint64((in[3]>>16|in[4]<<48)&mask + minv)
	out[5] = uint64((in[4]>>4)&mask + minv)
	out[6] = uint64((in[4]>>56|in[5]<<8)&mask + minv)
	out[7] = uint64((in[5]>>44|in[6]<<20)&mask + minv)
	out[8] = uint64((in[6]>>32|in[7]<<32)&mask + minv)
	out[9] = uint64((in[7]>>20|in[8]<<44)&mask + minv)
	out[10] = uint64((in[8]>>8)&mask + minv)
	out[11] = uint64((in[8]>>60|in[9]<<4)&mask + minv)
	out[12] = uint64((in[9]>>48|in[10]<<16)&mask + minv)
	out[13] = uint64((in[10]>>36|in[11]<<28)&mask + minv)
	out[14] = uint64((in[11]>>24|in[12]<<40)&mask + minv)
	out[15] = uint64((in[12]>>12)&mask + minv)
	out[16] = uint64(in[13]&mask + minv)
	out[17] = uint64((in[13]>>52|in[14]<<12)&mask + minv)
	out[18] = uint64((in[14]>>40|in[15]<<24)&mask + minv)
	out[19] = uint64((in[15]>>28|in[16]<<36)&mask + minv)
	out[20] = uint64((in[16]>>16|in[17]<<48)&mask + minv)
	out[21] = uint64((in[17]>>4)&mask + minv)
	out[22] = uint64((in[17]>>56|in[18]<<8)&mask + minv)
	out[23] = uint64((in[18]>>44|in[19]<<20)&mask + minv)
	out[24] = uint64((in[19]>>32|in[20]<<32)&mask + minv)
	out[25] = uint64((in[20]>>20|in[21]<<44)&mask + minv)
	out[26] = uint64((in[21]>>8)&mask + minv)
	out[27] = uint64((in[21]>>60|in[22]<<4)&mask + minv)
	out[28] = uint64((in[22]>>48|in[23]<<16)&mask + minv)
	out[29] = uint64((in[23]>>36|in[24]<<28)&mask + minv)
	out[30] = uint64((in[24]>>24|in[25]<<40)&mask + minv)
	out[31] = uint64((in[25]>>12)&mask + minv)
	out[32] = uint64(in[26]&mask + minv)
	out[33] = uint64((in[26]>>52|in[27]<<12)&mask + minv)
	out[34] = uint64((in[27]>>40|in[28]<<24)&mask + minv)
	out[35] = uint64((in[28]>>28|in[29]<<36)&mask + minv)
	out[36] = uint64((in[29]>>16|in[30]<<48)&mask + minv)
	out[37] = uint64((in[30]>>4)&mask + minv)
	out[38] = uint64((in[30]>>56|in[31]<<8)&mask + minv)
	out[39] = uint64((in[31]>>44|in[32]<<20)&mask + minv)
	out[40] = uint64((in[32]>>32|in[33]<<32)&mask + minv)
	out[41] = uint64((in[33]>>20|in[34]<<44)&mask + minv)
	out[42] = uint64((in[34]>>8)&mask + minv)
	out[43] = uint64((in[34]>>60|in[35]<<4)&mask + minv)
	out[44] = uint64((in[35]>>48|in[36]<<16)&mask + minv)
	out[45] = uint64((in[36]>>36|in[37]<<28)&mask + minv)
	out[46] = uint64((in[37]>>24|in[38]<<40)&mask + minv)
	out[47] = uint64((in[38]>>12)&mask + minv)
	out[48] = uint64(in[39]&mask + minv)
	out[49] = uint64((in[39]>>52|in[40]<<12)&mask + minv)
	out[50] = uint64((in[40]>>40|in[41]<<24)&mask + minv)
	out[51] = uint64((in[41]>>28|in[42]<<36)&mask + minv)
	out[52] = uint64((in[42]>>16|in[43]<<48)&mask + minv)
	out[53] = uint64((in[43]>>4)&mask + minv)
	out[54] = uint64((in[43]>>56|in[44]<<8)&mask + minv)
	out[55] = uint64((in[44]>>44|in[45]<<20)&mask + minv)
	out[56] = uint64((in[45]>>32|in[46]<<32)&mask + minv)
	out[57] = uint64((in[46]>>20|in[47]<<44)&mask + minv)
	out[58] = uint64((in[47]>>8)&mask + minv)
	out[59] = uint64((in[47]>>60|in[48]<<4)&mask + minv)
	out[60] = uint64((in[48]>>48|in[49]<<16)&mask + minv)
	out[61] = uint64((in[49]>>36|in[50]<<28)&mask + minv)
	out[62] = uint64((in[50]>>24|in[51]<<40)&mask + minv)
	out[63] = uint64((in[51]>>12)&mask + minv)
}

func br64_53(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[53]uint64)(p)
	mask := uint64((1 << 53) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>53|in[1]<<11)&mask + minv)
	out[2] = uint64((in[1]>>42|in[2]<<22)&mask + minv)
	out[3] = uint64((in[2]>>31|in[3]<<33)&mask + minv)
	out[4] = uint64((in[3]>>20|in[4]<<44)&mask + minv)
	out[5] = uint64((in[4]>>9)&mask + minv)
	out[6] = uint64((in[4]>>62|in[5]<<2)&mask + minv)
	out[7] = uint64((in[5]>>51|in[6]<<13)&mask + minv)
	out[8] = uint64((in[6]>>40|in[7]<<24)&mask + minv)
	out[9] = uint64((in[7]>>29|in[8]<<35)&mask + minv)
	out[10] = uint64((in[8]>>18|in[9]<<46)&mask + minv)
	out[11] = uint64((in[9]>>7)&mask + minv)
	out[12] = uint64((in[9]>>60|in[10]<<4)&mask + minv)
	out[13] = uint64((in[10]>>49|in[11]<<15)&mask + minv)
	out[14] = uint64((in[11]>>38|in[12]<<26)&mask + minv)
	out[15] = uint64((in[12]>>27|in[13]<<37)&mask + minv)
	out[16] = uint64((in[13]>>16|in[14]<<48)&mask + minv)
	out[17] = uint64((in[14]>>5)&mask + minv)
	out[18] = uint64((in[14]>>58|in[15]<<6)&mask + minv)
	out[19] = uint64((in[15]>>47|in[16]<<17)&mask + minv)
	out[20] = uint64((in[16]>>36|in[17]<<28)&mask + minv)
	out[21] = uint64((in[17]>>25|in[18]<<39)&mask + minv)
	out[22] = uint64((in[18]>>14|in[19]<<50)&mask + minv)
	out[23] = uint64((in[19]>>3)&mask + minv)
	out[24] = uint64((in[19]>>56|in[20]<<8)&mask + minv)
	out[25] = uint64((in[20]>>45|in[21]<<19)&mask + minv)
	out[26] = uint64((in[21]>>34|in[22]<<30)&mask + minv)
	out[27] = uint64((in[22]>>23|in[23]<<41)&mask + minv)
	out[28] = uint64((in[23]>>12|in[24]<<52)&mask + minv)
	out[29] = uint64((in[24]>>1)&mask + minv)
	out[30] = uint64((in[24]>>54|in[25]<<10)&mask + minv)
	out[31] = uint64((in[25]>>43|in[26]<<21)&mask + minv)
	out[32] = uint64((in[26]>>32|in[27]<<32)&mask + minv)
	out[33] = uint64((in[27]>>21|in[28]<<43)&mask + minv)
	out[34] = uint64((in[28]>>10)&mask + minv)
	out[35] = uint64((in[28]>>63|in[29]<<1)&mask + minv)
	out[36] = uint64((in[29]>>52|in[30]<<12)&mask + minv)
	out[37] = uint64((in[30]>>41|in[31]<<23)&mask + minv)
	out[38] = uint64((in[31]>>30|in[32]<<34)&mask + minv)
	out[39] = uint64((in[32]>>19|in[33]<<45)&mask + minv)
	out[40] = uint64((in[33]>>8)&mask + minv)
	out[41] = uint64((in[33]>>61|in[34]<<3)&mask + minv)
	out[42] = uint64((in[34]>>50|in[35]<<14)&mask + minv)
	out[43] = uint64((in[35]>>39|in[36]<<25)&mask + minv)
	out[44] = uint64((in[36]>>28|in[37]<<36)&mask + minv)
	out[45] = uint64((in[37]>>17|in[38]<<47)&mask + minv)
	out[46] = uint64((in[38]>>6)&mask + minv)
	out[47] = uint64((in[38]>>59|in[39]<<5)&mask + minv)
	out[48] = uint64((in[39]>>48|in[40]<<16)&mask + minv)
	out[49] = uint64((in[40]>>37|in[41]<<27)&mask + minv)
	out[50] = uint64((in[41]>>26|in[42]<<38)&mask + minv)
	out[51] = uint64((in[42]>>15|in[43]<<49)&mask + minv)
	out[52] = uint64((in[43]>>4)&mask + minv)
	out[53] = uint64((in[43]>>57|in[44]<<7)&mask + minv)
	out[54] = uint64((in[44]>>46|in[45]<<18)&mask + minv)
	out[55] = uint64((in[45]>>35|in[46]<<29)&mask + minv)
	out[56] = uint64((in[46]>>24|in[47]<<40)&mask + minv)
	out[57] = uint64((in[47]>>13|in[48]<<51)&mask + minv)
	out[58] = uint64((in[48]>>2)&mask + minv)
	out[59] = uint64((in[48]>>55|in[49]<<9)&mask + minv)
	out[60] = uint64((in[49]>>44|in[50]<<20)&mask + minv)
	out[61] = uint64((in[50]>>33|in[51]<<31)&mask + minv)
	out[62] = uint64((in[51]>>22|in[52]<<42)&mask + minv)
	out[63] = uint64((in[52]>>11)&mask + minv)
}

func br64_54(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[54]uint64)(p)
	mask := uint64((1 << 54) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>54|in[1]<<10)&mask + minv)
	out[2] = uint64((in[1]>>44|in[2]<<20)&mask + minv)
	out[3] = uint64((in[2]>>34|in[3]<<30)&mask + minv)
	out[4] = uint64((in[3]>>24|in[4]<<40)&mask + minv)
	out[5] = uint64((in[4]>>14|in[5]<<50)&mask + minv)
	out[6] = uint64((in[5]>>4)&mask + minv)
	out[7] = uint64((in[5]>>58|in[6]<<6)&mask + minv)
	out[8] = uint64((in[6]>>48|in[7]<<16)&mask + minv)
	out[9] = uint64((in[7]>>38|in[8]<<26)&mask + minv)
	out[10] = uint64((in[8]>>28|in[9]<<36)&mask + minv)
	out[11] = uint64((in[9]>>18|in[10]<<46)&mask + minv)
	out[12] = uint64((in[10]>>8)&mask + minv)
	out[13] = uint64((in[10]>>62|in[11]<<2)&mask + minv)
	out[14] = uint64((in[11]>>52|in[12]<<12)&mask + minv)
	out[15] = uint64((in[12]>>42|in[13]<<22)&mask + minv)
	out[16] = uint64((in[13]>>32|in[14]<<32)&mask + minv)
	out[17] = uint64((in[14]>>22|in[15]<<42)&mask + minv)
	out[18] = uint64((in[15]>>12|in[16]<<52)&mask + minv)
	out[19] = uint64((in[16]>>2)&mask + minv)
	out[20] = uint64((in[16]>>56|in[17]<<8)&mask + minv)
	out[21] = uint64((in[17]>>46|in[18]<<18)&mask + minv)
	out[22] = uint64((in[18]>>36|in[19]<<28)&mask + minv)
	out[23] = uint64((in[19]>>26|in[20]<<38)&mask + minv)
	out[24] = uint64((in[20]>>16|in[21]<<48)&mask + minv)
	out[25] = uint64((in[21]>>6)&mask + minv)
	out[26] = uint64((in[21]>>60|in[22]<<4)&mask + minv)
	out[27] = uint64((in[22]>>50|in[23]<<14)&mask + minv)
	out[28] = uint64((in[23]>>40|in[24]<<24)&mask + minv)
	out[29] = uint64((in[24]>>30|in[25]<<34)&mask + minv)
	out[30] = uint64((in[25]>>20|in[26]<<44)&mask + minv)
	out[31] = uint64((in[26]>>10)&mask + minv)
	out[32] = uint64(in[27]&mask + minv)
	out[33] = uint64((in[27]>>54|in[28]<<10)&mask + minv)
	out[34] = uint64((in[28]>>44|in[29]<<20)&mask + minv)
	out[35] = uint64((in[29]>>34|in[30]<<30)&mask + minv)
	out[36] = uint64((in[30]>>24|in[31]<<40)&mask + minv)
	out[37] = uint64((in[31]>>14|in[32]<<50)&mask + minv)
	out[38] = uint64((in[32]>>4)&mask + minv)
	out[39] = uint64((in[32]>>58|in[33]<<6)&mask + minv)
	out[40] = uint64((in[33]>>48|in[34]<<16)&mask + minv)
	out[41] = uint64((in[34]>>38|in[35]<<26)&mask + minv)
	out[42] = uint64((in[35]>>28|in[36]<<36)&mask + minv)
	out[43] = uint64((in[36]>>18|in[37]<<46)&mask + minv)
	out[44] = uint64((in[37]>>8)&mask + minv)
	out[45] = uint64((in[37]>>62|in[38]<<2)&mask + minv)
	out[46] = uint64((in[38]>>52|in[39]<<12)&mask + minv)
	out[47] = uint64((in[39]>>42|in[40]<<22)&mask + minv)
	out[48] = uint64((in[40]>>32|in[41]<<32)&mask + minv)
	out[49] = uint64((in[41]>>22|in[42]<<42)&mask + minv)
	out[50] = uint64((in[42]>>12|in[43]<<52)&mask + minv)
	out[51] = uint64((in[43]>>2)&mask + minv)
	out[52] = uint64((in[43]>>56|in[44]<<8)&mask + minv)
	out[53] = uint64((in[44]>>46|in[45]<<18)&mask + minv)
	out[54] = uint64((in[45]>>36|in[46]<<28)&mask + minv)
	out[55] = uint64((in[46]>>26|in[47]<<38)&mask + minv)
	out[56] = uint64((in[47]>>16|in[48]<<48)&mask + minv)
	out[57] = uint64((in[48]>>6)&mask + minv)
	out[58] = uint64((in[48]>>60|in[49]<<4)&mask + minv)
	out[59] = uint64((in[49]>>50|in[50]<<14)&mask + minv)
	out[60] = uint64((in[50]>>40|in[51]<<24)&mask + minv)
	out[61] = uint64((in[51]>>30|in[52]<<34)&mask + minv)
	out[62] = uint64((in[52]>>20|in[53]<<44)&mask + minv)
	out[63] = uint64((in[53]>>10)&mask + minv)
}

func br64_55(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[55]uint64)(p)
	mask := uint64((1 << 55) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>55|in[1]<<9)&mask + minv)
	out[2] = uint64((in[1]>>46|in[2]<<18)&mask + minv)
	out[3] = uint64((in[2]>>37|in[3]<<27)&mask + minv)
	out[4] = uint64((in[3]>>28|in[4]<<36)&mask + minv)
	out[5] = uint64((in[4]>>19|in[5]<<45)&mask + minv)
	out[6] = uint64((in[5]>>10|in[6]<<54)&mask + minv)
	out[7] = uint64((in[6]>>1)&mask + minv)
	out[8] = uint64((in[6]>>56|in[7]<<8)&mask + minv)
	out[9] = uint64((in[7]>>47|in[8]<<17)&mask + minv)
	out[10] = uint64((in[8]>>38|in[9]<<26)&mask + minv)
	out[11] = uint64((in[9]>>29|in[10]<<35)&mask + minv)
	out[12] = uint64((in[10]>>20|in[11]<<44)&mask + minv)
	out[13] = uint64((in[11]>>11|in[12]<<53)&mask + minv)
	out[14] = uint64((in[12]>>2)&mask + minv)
	out[15] = uint64((in[12]>>57|in[13]<<7)&mask + minv)
	out[16] = uint64((in[13]>>48|in[14]<<16)&mask + minv)
	out[17] = uint64((in[14]>>39|in[15]<<25)&mask + minv)
	out[18] = uint64((in[15]>>30|in[16]<<34)&mask + minv)
	out[19] = uint64((in[16]>>21|in[17]<<43)&mask + minv)
	out[20] = uint64((in[17]>>12|in[18]<<52)&mask + minv)
	out[21] = uint64((in[18]>>3)&mask + minv)
	out[22] = uint64((in[18]>>58|in[19]<<6)&mask + minv)
	out[23] = uint64((in[19]>>49|in[20]<<15)&mask + minv)
	out[24] = uint64((in[20]>>40|in[21]<<24)&mask + minv)
	out[25] = uint64((in[21]>>31|in[22]<<33)&mask + minv)
	out[26] = uint64((in[22]>>22|in[23]<<42)&mask + minv)
	out[27] = uint64((in[23]>>13|in[24]<<51)&mask + minv)
	out[28] = uint64((in[24]>>4)&mask + minv)
	out[29] = uint64((in[24]>>59|in[25]<<5)&mask + minv)
	out[30] = uint64((in[25]>>50|in[26]<<14)&mask + minv)
	out[31] = uint64((in[26]>>41|in[27]<<23)&mask + minv)
	out[32] = uint64((in[27]>>32|in[28]<<32)&mask + minv)
	out[33] = uint64((in[28]>>23|in[29]<<41)&mask + minv)
	out[34] = uint64((in[29]>>14|in[30]<<50)&mask + minv)
	out[35] = uint64((in[30]>>5)&mask + minv)
	out[36] = uint64((in[30]>>60|in[31]<<4)&mask + minv)
	out[37] = uint64((in[31]>>51|in[32]<<13)&mask + minv)
	out[38] = uint64((in[32]>>42|in[33]<<22)&mask + minv)
	out[39] = uint64((in[33]>>33|in[34]<<31)&mask + minv)
	out[40] = uint64((in[34]>>24|in[35]<<40)&mask + minv)
	out[41] = uint64((in[35]>>15|in[36]<<49)&mask + minv)
	out[42] = uint64((in[36]>>6)&mask + minv)
	out[43] = uint64((in[36]>>61|in[37]<<3)&mask + minv)
	out[44] = uint64((in[37]>>52|in[38]<<12)&mask + minv)
	out[45] = uint64((in[38]>>43|in[39]<<21)&mask + minv)
	out[46] = uint64((in[39]>>34|in[40]<<30)&mask + minv)
	out[47] = uint64((in[40]>>25|in[41]<<39)&mask + minv)
	out[48] = uint64((in[41]>>16|in[42]<<48)&mask + minv)
	out[49] = uint64((in[42]>>7)&mask + minv)
	out[50] = uint64((in[42]>>62|in[43]<<2)&mask + minv)
	out[51] = uint64((in[43]>>53|in[44]<<11)&mask + minv)
	out[52] = uint64((in[44]>>44|in[45]<<20)&mask + minv)
	out[53] = uint64((in[45]>>35|in[46]<<29)&mask + minv)
	out[54] = uint64((in[46]>>26|in[47]<<38)&mask + minv)
	out[55] = uint64((in[47]>>17|in[48]<<47)&mask + minv)
	out[56] = uint64((in[48]>>8)&mask + minv)
	out[57] = uint64((in[48]>>63|in[49]<<1)&mask + minv)
	out[58] = uint64((in[49]>>54|in[50]<<10)&mask + minv)
	out[59] = uint64((in[50]>>45|in[51]<<19)&mask + minv)
	out[60] = uint64((in[51]>>36|in[52]<<28)&mask + minv)
	out[61] = uint64((in[52]>>27|in[53]<<37)&mask + minv)
	out[62] = uint64((in[53]>>18|in[54]<<46)&mask + minv)
	out[63] = uint64((in[54]>>9)&mask + minv)
}

func br64_56(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[56]uint64)(p)
	mask := uint64((1 << 56) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>56|in[1]<<8)&mask + minv)
	out[2] = uint64((in[1]>>48|in[2]<<16)&mask + minv)
	out[3] = uint64((in[2]>>40|in[3]<<24)&mask + minv)
	out[4] = uint64((in[3]>>32|in[4]<<32)&mask + minv)
	out[5] = uint64((in[4]>>24|in[5]<<40)&mask + minv)
	out[6] = uint64((in[5]>>16|in[6]<<48)&mask + minv)
	out[7] = uint64((in[6]>>8)&mask + minv)
	out[8] = uint64(in[7]&mask + minv)
	out[9] = uint64((in[7]>>56|in[8]<<8)&mask + minv)
	out[10] = uint64((in[8]>>48|in[9]<<16)&mask + minv)
	out[11] = uint64((in[9]>>40|in[10]<<24)&mask + minv)
	out[12] = uint64((in[10]>>32|in[11]<<32)&mask + minv)
	out[13] = uint64((in[11]>>24|in[12]<<40)&mask + minv)
	out[14] = uint64((in[12]>>16|in[13]<<48)&mask + minv)
	out[15] = uint64((in[13]>>8)&mask + minv)
	out[16] = uint64(in[14]&mask + minv)
	out[17] = uint64((in[14]>>56|in[15]<<8)&mask + minv)
	out[18] = uint64((in[15]>>48|in[16]<<16)&mask + minv)
	out[19] = uint64((in[16]>>40|in[17]<<24)&mask + minv)
	out[20] = uint64((in[17]>>32|in[18]<<32)&mask + minv)
	out[21] = uint64((in[18]>>24|in[19]<<40)&mask + minv)
	out[22] = uint64((in[19]>>16|in[20]<<48)&mask + minv)
	out[23] = uint64((in[20]>>8)&mask + minv)
	out[24] = uint64(in[21]&mask + minv)
	out[25] = uint64((in[21]>>56|in[22]<<8)&mask + minv)
	out[26] = uint64((in[22]>>48|in[23]<<16)&mask + minv)
	out[27] = uint64((in[23]>>40|in[24]<<24)&mask + minv)
	out[28] = uint64((in[24]>>32|in[25]<<32)&mask + minv)
	out[29] = uint64((in[25]>>24|in[26]<<40)&mask + minv)
	out[30] = uint64((in[26]>>16|in[27]<<48)&mask + minv)
	out[31] = uint64((in[27]>>8)&mask + minv)
	out[32] = uint64(in[28]&mask + minv)
	out[33] = uint64((in[28]>>56|in[29]<<8)&mask + minv)
	out[34] = uint64((in[29]>>48|in[30]<<16)&mask + minv)
	out[35] = uint64((in[30]>>40|in[31]<<24)&mask + minv)
	out[36] = uint64((in[31]>>32|in[32]<<32)&mask + minv)
	out[37] = uint64((in[32]>>24|in[33]<<40)&mask + minv)
	out[38] = uint64((in[33]>>16|in[34]<<48)&mask + minv)
	out[39] = uint64((in[34]>>8)&mask + minv)
	out[40] = uint64(in[35]&mask + minv)
	out[41] = uint64((in[35]>>56|in[36]<<8)&mask + minv)
	out[42] = uint64((in[36]>>48|in[37]<<16)&mask + minv)
	out[43] = uint64((in[37]>>40|in[38]<<24)&mask + minv)
	out[44] = uint64((in[38]>>32|in[39]<<32)&mask + minv)
	out[45] = uint64((in[39]>>24|in[40]<<40)&mask + minv)
	out[46] = uint64((in[40]>>16|in[41]<<48)&mask + minv)
	out[47] = uint64((in[41]>>8)&mask + minv)
	out[48] = uint64(in[42]&mask + minv)
	out[49] = uint64((in[42]>>56|in[43]<<8)&mask + minv)
	out[50] = uint64((in[43]>>48|in[44]<<16)&mask + minv)
	out[51] = uint64((in[44]>>40|in[45]<<24)&mask + minv)
	out[52] = uint64((in[45]>>32|in[46]<<32)&mask + minv)
	out[53] = uint64((in[46]>>24|in[47]<<40)&mask + minv)
	out[54] = uint64((in[47]>>16|in[48]<<48)&mask + minv)
	out[55] = uint64((in[48]>>8)&mask + minv)
	out[56] = uint64(in[49]&mask + minv)
	out[57] = uint64((in[49]>>56|in[50]<<8)&mask + minv)
	out[58] = uint64((in[50]>>48|in[51]<<16)&mask + minv)
	out[59] = uint64((in[51]>>40|in[52]<<24)&mask + minv)
	out[60] = uint64((in[52]>>32|in[53]<<32)&mask + minv)
	out[61] = uint64((in[53]>>24|in[54]<<40)&mask + minv)
	out[62] = uint64((in[54]>>16|in[55]<<48)&mask + minv)
	out[63] = uint64((in[55]>>8)&mask + minv)
}

func br64_57(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[57]uint64)(p)
	mask := uint64((1 << 57) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>57|in[1]<<7)&mask + minv)
	out[2] = uint64((in[1]>>50|in[2]<<14)&mask + minv)
	out[3] = uint64((in[2]>>43|in[3]<<21)&mask + minv)
	out[4] = uint64((in[3]>>36|in[4]<<28)&mask + minv)
	out[5] = uint64((in[4]>>29|in[5]<<35)&mask + minv)
	out[6] = uint64((in[5]>>22|in[6]<<42)&mask + minv)
	out[7] = uint64((in[6]>>15|in[7]<<49)&mask + minv)
	out[8] = uint64((in[7]>>8|in[8]<<56)&mask + minv)
	out[9] = uint64((in[8]>>1)&mask + minv)
	out[10] = uint64((in[8]>>58|in[9]<<6)&mask + minv)
	out[11] = uint64((in[9]>>51|in[10]<<13)&mask + minv)
	out[12] = uint64((in[10]>>44|in[11]<<20)&mask + minv)
	out[13] = uint64((in[11]>>37|in[12]<<27)&mask + minv)
	out[14] = uint64((in[12]>>30|in[13]<<34)&mask + minv)
	out[15] = uint64((in[13]>>23|in[14]<<41)&mask + minv)
	out[16] = uint64((in[14]>>16|in[15]<<48)&mask + minv)
	out[17] = uint64((in[15]>>9|in[16]<<55)&mask + minv)
	out[18] = uint64((in[16]>>2)&mask + minv)
	out[19] = uint64((in[16]>>59|in[17]<<5)&mask + minv)
	out[20] = uint64((in[17]>>52|in[18]<<12)&mask + minv)
	out[21] = uint64((in[18]>>45|in[19]<<19)&mask + minv)
	out[22] = uint64((in[19]>>38|in[20]<<26)&mask + minv)
	out[23] = uint64((in[20]>>31|in[21]<<33)&mask + minv)
	out[24] = uint64((in[21]>>24|in[22]<<40)&mask + minv)
	out[25] = uint64((in[22]>>17|in[23]<<47)&mask + minv)
	out[26] = uint64((in[23]>>10|in[24]<<54)&mask + minv)
	out[27] = uint64((in[24]>>3)&mask + minv)
	out[28] = uint64((in[24]>>60|in[25]<<4)&mask + minv)
	out[29] = uint64((in[25]>>53|in[26]<<11)&mask + minv)
	out[30] = uint64((in[26]>>46|in[27]<<18)&mask + minv)
	out[31] = uint64((in[27]>>39|in[28]<<25)&mask + minv)
	out[32] = uint64((in[28]>>32|in[29]<<32)&mask + minv)
	out[33] = uint64((in[29]>>25|in[30]<<39)&mask + minv)
	out[34] = uint64((in[30]>>18|in[31]<<46)&mask + minv)
	out[35] = uint64((in[31]>>11|in[32]<<53)&mask + minv)
	out[36] = uint64((in[32]>>4)&mask + minv)
	out[37] = uint64((in[32]>>61|in[33]<<3)&mask + minv)
	out[38] = uint64((in[33]>>54|in[34]<<10)&mask + minv)
	out[39] = uint64((in[34]>>47|in[35]<<17)&mask + minv)
	out[40] = uint64((in[35]>>40|in[36]<<24)&mask + minv)
	out[41] = uint64((in[36]>>33|in[37]<<31)&mask + minv)
	out[42] = uint64((in[37]>>26|in[38]<<38)&mask + minv)
	out[43] = uint64((in[38]>>19|in[39]<<45)&mask + minv)
	out[44] = uint64((in[39]>>12|in[40]<<52)&mask + minv)
	out[45] = uint64((in[40]>>5)&mask + minv)
	out[46] = uint64((in[40]>>62|in[41]<<2)&mask + minv)
	out[47] = uint64((in[41]>>55|in[42]<<9)&mask + minv)
	out[48] = uint64((in[42]>>48|in[43]<<16)&mask + minv)
	out[49] = uint64((in[43]>>41|in[44]<<23)&mask + minv)
	out[50] = uint64((in[44]>>34|in[45]<<30)&mask + minv)
	out[51] = uint64((in[45]>>27|in[46]<<37)&mask + minv)
	out[52] = uint64((in[46]>>20|in[47]<<44)&mask + minv)
	out[53] = uint64((in[47]>>13|in[48]<<51)&mask + minv)
	out[54] = uint64((in[48]>>6)&mask + minv)
	out[55] = uint64((in[48]>>63|in[49]<<1)&mask + minv)
	out[56] = uint64((in[49]>>56|in[50]<<8)&mask + minv)
	out[57] = uint64((in[50]>>49|in[51]<<15)&mask + minv)
	out[58] = uint64((in[51]>>42|in[52]<<22)&mask + minv)
	out[59] = uint64((in[52]>>35|in[53]<<29)&mask + minv)
	out[60] = uint64((in[53]>>28|in[54]<<36)&mask + minv)
	out[61] = uint64((in[54]>>21|in[55]<<43)&mask + minv)
	out[62] = uint64((in[55]>>14|in[56]<<50)&mask + minv)
	out[63] = uint64((in[56]>>7)&mask + minv)
}

func br64_58(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[58]uint64)(p)
	mask := uint64((1 << 58) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>58|in[1]<<6)&mask + minv)
	out[2] = uint64((in[1]>>52|in[2]<<12)&mask + minv)
	out[3] = uint64((in[2]>>46|in[3]<<18)&mask + minv)
	out[4] = uint64((in[3]>>40|in[4]<<24)&mask + minv)
	out[5] = uint64((in[4]>>34|in[5]<<30)&mask + minv)
	out[6] = uint64((in[5]>>28|in[6]<<36)&mask + minv)
	out[7] = uint64((in[6]>>22|in[7]<<42)&mask + minv)
	out[8] = uint64((in[7]>>16|in[8]<<48)&mask + minv)
	out[9] = uint64((in[8]>>10|in[9]<<54)&mask + minv)
	out[10] = uint64((in[9]>>4)&mask + minv)
	out[11] = uint64((in[9]>>62|in[10]<<2)&mask + minv)
	out[12] = uint64((in[10]>>56|in[11]<<8)&mask + minv)
	out[13] = uint64((in[11]>>50|in[12]<<14)&mask + minv)
	out[14] = uint64((in[12]>>44|in[13]<<20)&mask + minv)
	out[15] = uint64((in[13]>>38|in[14]<<26)&mask + minv)
	out[16] = uint64((in[14]>>32|in[15]<<32)&mask + minv)
	out[17] = uint64((in[15]>>26|in[16]<<38)&mask + minv)
	out[18] = uint64((in[16]>>20|in[17]<<44)&mask + minv)
	out[19] = uint64((in[17]>>14|in[18]<<50)&mask + minv)
	out[20] = uint64((in[18]>>8|in[19]<<56)&mask + minv)
	out[21] = uint64((in[19]>>2)&mask + minv)
	out[22] = uint64((in[19]>>60|in[20]<<4)&mask + minv)
	out[23] = uint64((in[20]>>54|in[21]<<10)&mask + minv)
	out[24] = uint64((in[21]>>48|in[22]<<16)&mask + minv)
	out[25] = uint64((in[22]>>42|in[23]<<22)&mask + minv)
	out[26] = uint64((in[23]>>36|in[24]<<28)&mask + minv)
	out[27] = uint64((in[24]>>30|in[25]<<34)&mask + minv)
	out[28] = uint64((in[25]>>24|in[26]<<40)&mask + minv)
	out[29] = uint64((in[26]>>18|in[27]<<46)&mask + minv)
	out[30] = uint64((in[27]>>12|in[28]<<52)&mask + minv)
	out[31] = uint64((in[28]>>6)&mask + minv)
	out[32] = uint64(in[29]&mask + minv)
	out[33] = uint64((in[29]>>58|in[30]<<6)&mask + minv)
	out[34] = uint64((in[30]>>52|in[31]<<12)&mask + minv)
	out[35] = uint64((in[31]>>46|in[32]<<18)&mask + minv)
	out[36] = uint64((in[32]>>40|in[33]<<24)&mask + minv)
	out[37] = uint64((in[33]>>34|in[34]<<30)&mask + minv)
	out[38] = uint64((in[34]>>28|in[35]<<36)&mask + minv)
	out[39] = uint64((in[35]>>22|in[36]<<42)&mask + minv)
	out[40] = uint64((in[36]>>16|in[37]<<48)&mask + minv)
	out[41] = uint64((in[37]>>10|in[38]<<54)&mask + minv)
	out[42] = uint64((in[38]>>4)&mask + minv)
	out[43] = uint64((in[38]>>62|in[39]<<2)&mask + minv)
	out[44] = uint64((in[39]>>56|in[40]<<8)&mask + minv)
	out[45] = uint64((in[40]>>50|in[41]<<14)&mask + minv)
	out[46] = uint64((in[41]>>44|in[42]<<20)&mask + minv)
	out[47] = uint64((in[42]>>38|in[43]<<26)&mask + minv)
	out[48] = uint64((in[43]>>32|in[44]<<32)&mask + minv)
	out[49] = uint64((in[44]>>26|in[45]<<38)&mask + minv)
	out[50] = uint64((in[45]>>20|in[46]<<44)&mask + minv)
	out[51] = uint64((in[46]>>14|in[47]<<50)&mask + minv)
	out[52] = uint64((in[47]>>8|in[48]<<56)&mask + minv)
	out[53] = uint64((in[48]>>2)&mask + minv)
	out[54] = uint64((in[48]>>60|in[49]<<4)&mask + minv)
	out[55] = uint64((in[49]>>54|in[50]<<10)&mask + minv)
	out[56] = uint64((in[50]>>48|in[51]<<16)&mask + minv)
	out[57] = uint64((in[51]>>42|in[52]<<22)&mask + minv)
	out[58] = uint64((in[52]>>36|in[53]<<28)&mask + minv)
	out[59] = uint64((in[53]>>30|in[54]<<34)&mask + minv)
	out[60] = uint64((in[54]>>24|in[55]<<40)&mask + minv)
	out[61] = uint64((in[55]>>18|in[56]<<46)&mask + minv)
	out[62] = uint64((in[56]>>12|in[57]<<52)&mask + minv)
	out[63] = uint64((in[57]>>6)&mask + minv)
}

func br64_59(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[59]uint64)(p)
	mask := uint64((1 << 59) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>59|in[1]<<5)&mask + minv)
	out[2] = uint64((in[1]>>54|in[2]<<10)&mask + minv)
	out[3] = uint64((in[2]>>49|in[3]<<15)&mask + minv)
	out[4] = uint64((in[3]>>44|in[4]<<20)&mask + minv)
	out[5] = uint64((in[4]>>39|in[5]<<25)&mask + minv)
	out[6] = uint64((in[5]>>34|in[6]<<30)&mask + minv)
	out[7] = uint64((in[6]>>29|in[7]<<35)&mask + minv)
	out[8] = uint64((in[7]>>24|in[8]<<40)&mask + minv)
	out[9] = uint64((in[8]>>19|in[9]<<45)&mask + minv)
	out[10] = uint64((in[9]>>14|in[10]<<50)&mask + minv)
	out[11] = uint64((in[10]>>9|in[11]<<55)&mask + minv)
	out[12] = uint64((in[11]>>4)&mask + minv)
	out[13] = uint64((in[11]>>63|in[12]<<1)&mask + minv)
	out[14] = uint64((in[12]>>58|in[13]<<6)&mask + minv)
	out[15] = uint64((in[13]>>53|in[14]<<11)&mask + minv)
	out[16] = uint64((in[14]>>48|in[15]<<16)&mask + minv)
	out[17] = uint64((in[15]>>43|in[16]<<21)&mask + minv)
	out[18] = uint64((in[16]>>38|in[17]<<26)&mask + minv)
	out[19] = uint64((in[17]>>33|in[18]<<31)&mask + minv)
	out[20] = uint64((in[18]>>28|in[19]<<36)&mask + minv)
	out[21] = uint64((in[19]>>23|in[20]<<41)&mask + minv)
	out[22] = uint64((in[20]>>18|in[21]<<46)&mask + minv)
	out[23] = uint64((in[21]>>13|in[22]<<51)&mask + minv)
	out[24] = uint64((in[22]>>8|in[23]<<56)&mask + minv)
	out[25] = uint64((in[23]>>3)&mask + minv)
	out[26] = uint64((in[23]>>62|in[24]<<2)&mask + minv)
	out[27] = uint64((in[24]>>57|in[25]<<7)&mask + minv)
	out[28] = uint64((in[25]>>52|in[26]<<12)&mask + minv)
	out[29] = uint64((in[26]>>47|in[27]<<17)&mask + minv)
	out[30] = uint64((in[27]>>42|in[28]<<22)&mask + minv)
	out[31] = uint64((in[28]>>37|in[29]<<27)&mask + minv)
	out[32] = uint64((in[29]>>32|in[30]<<32)&mask + minv)
	out[33] = uint64((in[30]>>27|in[31]<<37)&mask + minv)
	out[34] = uint64((in[31]>>22|in[32]<<42)&mask + minv)
	out[35] = uint64((in[32]>>17|in[33]<<47)&mask + minv)
	out[36] = uint64((in[33]>>12|in[34]<<52)&mask + minv)
	out[37] = uint64((in[34]>>7|in[35]<<57)&mask + minv)
	out[38] = uint64((in[35]>>2)&mask + minv)
	out[39] = uint64((in[35]>>61|in[36]<<3)&mask + minv)
	out[40] = uint64((in[36]>>56|in[37]<<8)&mask + minv)
	out[41] = uint64((in[37]>>51|in[38]<<13)&mask + minv)
	out[42] = uint64((in[38]>>46|in[39]<<18)&mask + minv)
	out[43] = uint64((in[39]>>41|in[40]<<23)&mask + minv)
	out[44] = uint64((in[40]>>36|in[41]<<28)&mask + minv)
	out[45] = uint64((in[41]>>31|in[42]<<33)&mask + minv)
	out[46] = uint64((in[42]>>26|in[43]<<38)&mask + minv)
	out[47] = uint64((in[43]>>21|in[44]<<43)&mask + minv)
	out[48] = uint64((in[44]>>16|in[45]<<48)&mask + minv)
	out[49] = uint64((in[45]>>11|in[46]<<53)&mask + minv)
	out[50] = uint64((in[46]>>6|in[47]<<58)&mask + minv)
	out[51] = uint64((in[47]>>1)&mask + minv)
	out[52] = uint64((in[47]>>60|in[48]<<4)&mask + minv)
	out[53] = uint64((in[48]>>55|in[49]<<9)&mask + minv)
	out[54] = uint64((in[49]>>50|in[50]<<14)&mask + minv)
	out[55] = uint64((in[50]>>45|in[51]<<19)&mask + minv)
	out[56] = uint64((in[51]>>40|in[52]<<24)&mask + minv)
	out[57] = uint64((in[52]>>35|in[53]<<29)&mask + minv)
	out[58] = uint64((in[53]>>30|in[54]<<34)&mask + minv)
	out[59] = uint64((in[54]>>25|in[55]<<39)&mask + minv)
	out[60] = uint64((in[55]>>20|in[56]<<44)&mask + minv)
	out[61] = uint64((in[56]>>15|in[57]<<49)&mask + minv)
	out[62] = uint64((in[57]>>10|in[58]<<54)&mask + minv)
	out[63] = uint64((in[58]>>5)&mask + minv)
}

func br64_60(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[60]uint64)(p)
	mask := uint64((1 << 60) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>60|in[1]<<4)&mask + minv)
	out[2] = uint64((in[1]>>56|in[2]<<8)&mask + minv)
	out[3] = uint64((in[2]>>52|in[3]<<12)&mask + minv)
	out[4] = uint64((in[3]>>48|in[4]<<16)&mask + minv)
	out[5] = uint64((in[4]>>44|in[5]<<20)&mask + minv)
	out[6] = uint64((in[5]>>40|in[6]<<24)&mask + minv)
	out[7] = uint64((in[6]>>36|in[7]<<28)&mask + minv)
	out[8] = uint64((in[7]>>32|in[8]<<32)&mask + minv)
	out[9] = uint64((in[8]>>28|in[9]<<36)&mask + minv)
	out[10] = uint64((in[9]>>24|in[10]<<40)&mask + minv)
	out[11] = uint64((in[10]>>20|in[11]<<44)&mask + minv)
	out[12] = uint64((in[11]>>16|in[12]<<48)&mask + minv)
	out[13] = uint64((in[12]>>12|in[13]<<52)&mask + minv)
	out[14] = uint64((in[13]>>8|in[14]<<56)&mask + minv)
	out[15] = uint64((in[14]>>4)&mask + minv)
	out[16] = uint64(in[15]&mask + minv)
	out[17] = uint64((in[15]>>60|in[16]<<4)&mask + minv)
	out[18] = uint64((in[16]>>56|in[17]<<8)&mask + minv)
	out[19] = uint64((in[17]>>52|in[18]<<12)&mask + minv)
	out[20] = uint64((in[18]>>48|in[19]<<16)&mask + minv)
	out[21] = uint64((in[19]>>44|in[20]<<20)&mask + minv)
	out[22] = uint64((in[20]>>40|in[21]<<24)&mask + minv)
	out[23] = uint64((in[21]>>36|in[22]<<28)&mask + minv)
	out[24] = uint64((in[22]>>32|in[23]<<32)&mask + minv)
	out[25] = uint64((in[23]>>28|in[24]<<36)&mask + minv)
	out[26] = uint64((in[24]>>24|in[25]<<40)&mask + minv)
	out[27] = uint64((in[25]>>20|in[26]<<44)&mask + minv)
	out[28] = uint64((in[26]>>16|in[27]<<48)&mask + minv)
	out[29] = uint64((in[27]>>12|in[28]<<52)&mask + minv)
	out[30] = uint64((in[28]>>8|in[29]<<56)&mask + minv)
	out[31] = uint64((in[29]>>4)&mask + minv)
	out[32] = uint64(in[30]&mask + minv)
	out[33] = uint64((in[30]>>60|in[31]<<4)&mask + minv)
	out[34] = uint64((in[31]>>56|in[32]<<8)&mask + minv)
	out[35] = uint64((in[32]>>52|in[33]<<12)&mask + minv)
	out[36] = uint64((in[33]>>48|in[34]<<16)&mask + minv)
	out[37] = uint64((in[34]>>44|in[35]<<20)&mask + minv)
	out[38] = uint64((in[35]>>40|in[36]<<24)&mask + minv)
	out[39] = uint64((in[36]>>36|in[37]<<28)&mask + minv)
	out[40] = uint64((in[37]>>32|in[38]<<32)&mask + minv)
	out[41] = uint64((in[38]>>28|in[39]<<36)&mask + minv)
	out[42] = uint64((in[39]>>24|in[40]<<40)&mask + minv)
	out[43] = uint64((in[40]>>20|in[41]<<44)&mask + minv)
	out[44] = uint64((in[41]>>16|in[42]<<48)&mask + minv)
	out[45] = uint64((in[42]>>12|in[43]<<52)&mask + minv)
	out[46] = uint64((in[43]>>8|in[44]<<56)&mask + minv)
	out[47] = uint64((in[44]>>4)&mask + minv)
	out[48] = uint64(in[45]&mask + minv)
	out[49] = uint64((in[45]>>60|in[46]<<4)&mask + minv)
	out[50] = uint64((in[46]>>56|in[47]<<8)&mask + minv)
	out[51] = uint64((in[47]>>52|in[48]<<12)&mask + minv)
	out[52] = uint64((in[48]>>48|in[49]<<16)&mask + minv)
	out[53] = uint64((in[49]>>44|in[50]<<20)&mask + minv)
	out[54] = uint64((in[50]>>40|in[51]<<24)&mask + minv)
	out[55] = uint64((in[51]>>36|in[52]<<28)&mask + minv)
	out[56] = uint64((in[52]>>32|in[53]<<32)&mask + minv)
	out[57] = uint64((in[53]>>28|in[54]<<36)&mask + minv)
	out[58] = uint64((in[54]>>24|in[55]<<40)&mask + minv)
	out[59] = uint64((in[55]>>20|in[56]<<44)&mask + minv)
	out[60] = uint64((in[56]>>16|in[57]<<48)&mask + minv)
	out[61] = uint64((in[57]>>12|in[58]<<52)&mask + minv)
	out[62] = uint64((in[58]>>8|in[59]<<56)&mask + minv)
	out[63] = uint64((in[59]>>4)&mask + minv)
}

func br64_61(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[61]uint64)(p)
	mask := uint64((1 << 61) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>61|in[1]<<3)&mask + minv)
	out[2] = uint64((in[1]>>58|in[2]<<6)&mask + minv)
	out[3] = uint64((in[2]>>55|in[3]<<9)&mask + minv)
	out[4] = uint64((in[3]>>52|in[4]<<12)&mask + minv)
	out[5] = uint64((in[4]>>49|in[5]<<15)&mask + minv)
	out[6] = uint64((in[5]>>46|in[6]<<18)&mask + minv)
	out[7] = uint64((in[6]>>43|in[7]<<21)&mask + minv)
	out[8] = uint64((in[7]>>40|in[8]<<24)&mask + minv)
	out[9] = uint64((in[8]>>37|in[9]<<27)&mask + minv)
	out[10] = uint64((in[9]>>34|in[10]<<30)&mask + minv)
	out[11] = uint64((in[10]>>31|in[11]<<33)&mask + minv)
	out[12] = uint64((in[11]>>28|in[12]<<36)&mask + minv)
	out[13] = uint64((in[12]>>25|in[13]<<39)&mask + minv)
	out[14] = uint64((in[13]>>22|in[14]<<42)&mask + minv)
	out[15] = uint64((in[14]>>19|in[15]<<45)&mask + minv)
	out[16] = uint64((in[15]>>16|in[16]<<48)&mask + minv)
	out[17] = uint64((in[16]>>13|in[17]<<51)&mask + minv)
	out[18] = uint64((in[17]>>10|in[18]<<54)&mask + minv)
	out[19] = uint64((in[18]>>7|in[19]<<57)&mask + minv)
	out[20] = uint64((in[19]>>4|in[20]<<60)&mask + minv)
	out[21] = uint64((in[20]>>1)&mask + minv)
	out[22] = uint64((in[20]>>62|in[21]<<2)&mask + minv)
	out[23] = uint64((in[21]>>59|in[22]<<5)&mask + minv)
	out[24] = uint64((in[22]>>56|in[23]<<8)&mask + minv)
	out[25] = uint64((in[23]>>53|in[24]<<11)&mask + minv)
	out[26] = uint64((in[24]>>50|in[25]<<14)&mask + minv)
	out[27] = uint64((in[25]>>47|in[26]<<17)&mask + minv)
	out[28] = uint64((in[26]>>44|in[27]<<20)&mask + minv)
	out[29] = uint64((in[27]>>41|in[28]<<23)&mask + minv)
	out[30] = uint64((in[28]>>38|in[29]<<26)&mask + minv)
	out[31] = uint64((in[29]>>35|in[30]<<29)&mask + minv)
	out[32] = uint64((in[30]>>32|in[31]<<32)&mask + minv)
	out[33] = uint64((in[31]>>29|in[32]<<35)&mask + minv)
	out[34] = uint64((in[32]>>26|in[33]<<38)&mask + minv)
	out[35] = uint64((in[33]>>23|in[34]<<41)&mask + minv)
	out[36] = uint64((in[34]>>20|in[35]<<44)&mask + minv)
	out[37] = uint64((in[35]>>17|in[36]<<47)&mask + minv)
	out[38] = uint64((in[36]>>14|in[37]<<50)&mask + minv)
	out[39] = uint64((in[37]>>11|in[38]<<53)&mask + minv)
	out[40] = uint64((in[38]>>8|in[39]<<56)&mask + minv)
	out[41] = uint64((in[39]>>5|in[40]<<59)&mask + minv)
	out[42] = uint64((in[40]>>2)&mask + minv)
	out[43] = uint64((in[40]>>63|in[41]<<1)&mask + minv)
	out[44] = uint64((in[41]>>60|in[42]<<4)&mask + minv)
	out[45] = uint64((in[42]>>57|in[43]<<7)&mask + minv)
	out[46] = uint64((in[43]>>54|in[44]<<10)&mask + minv)
	out[47] = uint64((in[44]>>51|in[45]<<13)&mask + minv)
	out[48] = uint64((in[45]>>48|in[46]<<16)&mask + minv)
	out[49] = uint64((in[46]>>45|in[47]<<19)&mask + minv)
	out[50] = uint64((in[47]>>42|in[48]<<22)&mask + minv)
	out[51] = uint64((in[48]>>39|in[49]<<25)&mask + minv)
	out[52] = uint64((in[49]>>36|in[50]<<28)&mask + minv)
	out[53] = uint64((in[50]>>33|in[51]<<31)&mask + minv)
	out[54] = uint64((in[51]>>30|in[52]<<34)&mask + minv)
	out[55] = uint64((in[52]>>27|in[53]<<37)&mask + minv)
	out[56] = uint64((in[53]>>24|in[54]<<40)&mask + minv)
	out[57] = uint64((in[54]>>21|in[55]<<43)&mask + minv)
	out[58] = uint64((in[55]>>18|in[56]<<46)&mask + minv)
	out[59] = uint64((in[56]>>15|in[57]<<49)&mask + minv)
	out[60] = uint64((in[57]>>12|in[58]<<52)&mask + minv)
	out[61] = uint64((in[58]>>9|in[59]<<55)&mask + minv)
	out[62] = uint64((in[59]>>6|in[60]<<58)&mask + minv)
	out[63] = uint64((in[60]>>3)&mask + minv)
}

func br64_62(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[62]uint64)(p)
	mask := uint64((1 << 62) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>62|in[1]<<2)&mask + minv)
	out[2] = uint64((in[1]>>60|in[2]<<4)&mask + minv)
	out[3] = uint64((in[2]>>58|in[3]<<6)&mask + minv)
	out[4] = uint64((in[3]>>56|in[4]<<8)&mask + minv)
	out[5] = uint64((in[4]>>54|in[5]<<10)&mask + minv)
	out[6] = uint64((in[5]>>52|in[6]<<12)&mask + minv)
	out[7] = uint64((in[6]>>50|in[7]<<14)&mask + minv)
	out[8] = uint64((in[7]>>48|in[8]<<16)&mask + minv)
	out[9] = uint64((in[8]>>46|in[9]<<18)&mask + minv)
	out[10] = uint64((in[9]>>44|in[10]<<20)&mask + minv)
	out[11] = uint64((in[10]>>42|in[11]<<22)&mask + minv)
	out[12] = uint64((in[11]>>40|in[12]<<24)&mask + minv)
	out[13] = uint64((in[12]>>38|in[13]<<26)&mask + minv)
	out[14] = uint64((in[13]>>36|in[14]<<28)&mask + minv)
	out[15] = uint64((in[14]>>34|in[15]<<30)&mask + minv)
	out[16] = uint64((in[15]>>32|in[16]<<32)&mask + minv)
	out[17] = uint64((in[16]>>30|in[17]<<34)&mask + minv)
	out[18] = uint64((in[17]>>28|in[18]<<36)&mask + minv)
	out[19] = uint64((in[18]>>26|in[19]<<38)&mask + minv)
	out[20] = uint64((in[19]>>24|in[20]<<40)&mask + minv)
	out[21] = uint64((in[20]>>22|in[21]<<42)&mask + minv)
	out[22] = uint64((in[21]>>20|in[22]<<44)&mask + minv)
	out[23] = uint64((in[22]>>18|in[23]<<46)&mask + minv)
	out[24] = uint64((in[23]>>16|in[24]<<48)&mask + minv)
	out[25] = uint64((in[24]>>14|in[25]<<50)&mask + minv)
	out[26] = uint64((in[25]>>12|in[26]<<52)&mask + minv)
	out[27] = uint64((in[26]>>10|in[27]<<54)&mask + minv)
	out[28] = uint64((in[27]>>8|in[28]<<56)&mask + minv)
	out[29] = uint64((in[28]>>6|in[29]<<58)&mask + minv)
	out[30] = uint64((in[29]>>4|in[30]<<60)&mask + minv)
	out[31] = uint64((in[30]>>2)&mask + minv)
	out[32] = uint64(in[31]&mask + minv)
	out[33] = uint64((in[31]>>62|in[32]<<2)&mask + minv)
	out[34] = uint64((in[32]>>60|in[33]<<4)&mask + minv)
	out[35] = uint64((in[33]>>58|in[34]<<6)&mask + minv)
	out[36] = uint64((in[34]>>56|in[35]<<8)&mask + minv)
	out[37] = uint64((in[35]>>54|in[36]<<10)&mask + minv)
	out[38] = uint64((in[36]>>52|in[37]<<12)&mask + minv)
	out[39] = uint64((in[37]>>50|in[38]<<14)&mask + minv)
	out[40] = uint64((in[38]>>48|in[39]<<16)&mask + minv)
	out[41] = uint64((in[39]>>46|in[40]<<18)&mask + minv)
	out[42] = uint64((in[40]>>44|in[41]<<20)&mask + minv)
	out[43] = uint64((in[41]>>42|in[42]<<22)&mask + minv)
	out[44] = uint64((in[42]>>40|in[43]<<24)&mask + minv)
	out[45] = uint64((in[43]>>38|in[44]<<26)&mask + minv)
	out[46] = uint64((in[44]>>36|in[45]<<28)&mask + minv)
	out[47] = uint64((in[45]>>34|in[46]<<30)&mask + minv)
	out[48] = uint64((in[46]>>32|in[47]<<32)&mask + minv)
	out[49] = uint64((in[47]>>30|in[48]<<34)&mask + minv)
	out[50] = uint64((in[48]>>28|in[49]<<36)&mask + minv)
	out[51] = uint64((in[49]>>26|in[50]<<38)&mask + minv)
	out[52] = uint64((in[50]>>24|in[51]<<40)&mask + minv)
	out[53] = uint64((in[51]>>22|in[52]<<42)&mask + minv)
	out[54] = uint64((in[52]>>20|in[53]<<44)&mask + minv)
	out[55] = uint64((in[53]>>18|in[54]<<46)&mask + minv)
	out[56] = uint64((in[54]>>16|in[55]<<48)&mask + minv)
	out[57] = uint64((in[55]>>14|in[56]<<50)&mask + minv)
	out[58] = uint64((in[56]>>12|in[57]<<52)&mask + minv)
	out[59] = uint64((in[57]>>10|in[58]<<54)&mask + minv)
	out[60] = uint64((in[58]>>8|in[59]<<56)&mask + minv)
	out[61] = uint64((in[59]>>6|in[60]<<58)&mask + minv)
	out[62] = uint64((in[60]>>4|in[61]<<60)&mask + minv)
	out[63] = uint64((in[61]>>2)&mask + minv)
}

func br64_63(out *[64]uint64, p unsafe.Pointer, minv uint64) {
	in := (*[63]uint64)(p)
	mask := uint64((1 << 63) - 1)
	out[0] = uint64(in[0]&mask + minv)
	out[1] = uint64((in[0]>>63|in[1]<<1)&mask + minv)
	out[2] = uint64((in[1]>>62|in[2]<<2)&mask + minv)
	out[3] = uint64((in[2]>>61|in[3]<<3)&mask + minv)
	out[4] = uint64((in[3]>>60|in[4]<<4)&mask + minv)
	out[5] = uint64((in[4]>>59|in[5]<<5)&mask + minv)
	out[6] = uint64((in[5]>>58|in[6]<<6)&mask + minv)
	out[7] = uint64((in[6]>>57|in[7]<<7)&mask + minv)
	out[8] = uint64((in[7]>>56|in[8]<<8)&mask + minv)
	out[9] = uint64((in[8]>>55|in[9]<<9)&mask + minv)
	out[10] = uint64((in[9]>>54|in[10]<<10)&mask + minv)
	out[11] = uint64((in[10]>>53|in[11]<<11)&mask + minv)
	out[12] = uint64((in[11]>>52|in[12]<<12)&mask + minv)
	out[13] = uint64((in[12]>>51|in[13]<<13)&mask + minv)
	out[14] = uint64((in[13]>>50|in[14]<<14)&mask + minv)
	out[15] = uint64((in[14]>>49|in[15]<<15)&mask + minv)
	out[16] = uint64((in[15]>>48|in[16]<<16)&mask + minv)
	out[17] = uint64((in[16]>>47|in[17]<<17)&mask + minv)
	out[18] = uint64((in[17]>>46|in[18]<<18)&mask + minv)
	out[19] = uint64((in[18]>>45|in[19]<<19)&mask + minv)
	out[20] = uint64((in[19]>>44|in[20]<<20)&mask + minv)
	out[21] = uint64((in[20]>>43|in[21]<<21)&mask + minv)
	out[22] = uint64((in[21]>>42|in[22]<<22)&mask + minv)
	out[23] = uint64((in[22]>>41|in[23]<<23)&mask + minv)
	out[24] = uint64((in[23]>>40|in[24]<<24)&mask + minv)
	out[25] = uint64((in[24]>>39|in[25]<<25)&mask + minv)
	out[26] = uint64((in[25]>>38|in[26]<<26)&mask + minv)
	out[27] = uint64((in[26]>>37|in[27]<<27)&mask + minv)
	out[28] = uint64((in[27]>>36|in[28]<<28)&mask + minv)
	out[29] = uint64((in[28]>>35|in[29]<<29)&mask + minv)
	out[30] = uint64((in[29]>>34|in[30]<<30)&mask + minv)
	out[31] = uint64((in[30]>>33|in[31]<<31)&mask + minv)
	out[32] = uint64((in[31]>>32|in[32]<<32)&mask + minv)
	out[33] = uint64((in[32]>>31|in[33]<<33)&mask + minv)
	out[34] = uint64((in[33]>>30|in[34]<<34)&mask + minv)
	out[35] = uint64((in[34]>>29|in[35]<<35)&mask + minv)
	out[36] = uint64((in[35]>>28|in[36]<<36)&mask + minv)
	out[37] = uint64((in[36]>>27|in[37]<<37)&mask + minv)
	out[38] = uint64((in[37]>>26|in[38]<<38)&mask + minv)
	out[39] = uint64((in[38]>>25|in[39]<<39)&mask + minv)
	out[40] = uint64((in[39]>>24|in[40]<<40)&mask + minv)
	out[41] = uint64((in[40]>>23|in[41]<<41)&mask + minv)
	out[42] = uint64((in[41]>>22|in[42]<<42)&mask + minv)
	out[43] = uint64((in[42]>>21|in[43]<<43)&mask + minv)
	out[44] = uint64((in[43]>>20|in[44]<<44)&mask + minv)
	out[45] = uint64((in[44]>>19|in[45]<<45)&mask + minv)
	out[46] = uint64((in[45]>>18|in[46]<<46)&mask + minv)
	out[47] = uint64((in[46]>>17|in[47]<<47)&mask + minv)
	out[48] = uint64((in[47]>>16|in[48]<<48)&mask + minv)
	out[49] = uint64((in[48]>>15|in[49]<<49)&mask + minv)
	out[50] = uint64((in[49]>>14|in[50]<<50)&mask + minv)
	out[51] = uint64((in[50]>>13|in[51]<<51)&mask + minv)
	out[52] = uint64((in[51]>>12|in[52]<<52)&mask + minv)
	out[53] = uint64((in[52]>>11|in[53]<<53)&mask + minv)
	out[54] = uint64((in[53]>>10|in[54]<<54)&mask + minv)
	out[55] = uint64((in[54]>>9|in[55]<<55)&mask + minv)
	out[56] = uint64((in[55]>>8|in[56]<<56)&mask + minv)
	out[57] = uint64((in[56]>>7|in[57]<<57)&mask + minv)
	out[58] = uint64((in[57]>>6|in[58]<<58)&mask + minv)
	out[59] = uint64((in[58]>>5|in[59]<<59)&mask + minv)
	out[60] = uint64((in[59]>>4|in[60]<<60)&mask + minv)
	out[61] = uint64((in[60]>>3|in[61]<<61)&mask + minv)
	out[62] = uint64((in[61]>>2|in[62]<<62)&mask + minv)
	out[63] = uint64((in[62]>>1)&mask + minv)
}

func br64_64(in *[64]uint64, p unsafe.Pointer, minv uint64) {
	out := (*[64]uint64)(p)
	var i int
	for range 4 {
		out[i] = in[i] + uint64(minv)
		out[i+1] = in[i+1] + uint64(minv)
		out[i+2] = in[i+2] + uint64(minv)
		out[i+3] = in[i+3] + uint64(minv)
		out[i+4] = in[i+4] + uint64(minv)
		out[i+5] = in[i+5] + uint64(minv)
		out[i+6] = in[i+6] + uint64(minv)
		out[i+7] = in[i+7] + uint64(minv)
		out[i+8] = in[i+8] + uint64(minv)
		out[i+9] = in[i+9] + uint64(minv)
		out[i+10] = in[i+10] + uint64(minv)
		out[i+11] = in[i+11] + uint64(minv)
		out[i+12] = in[i+12] + uint64(minv)
		out[i+13] = in[i+13] + uint64(minv)
		out[i+14] = in[i+14] + uint64(minv)
		out[i+15] = in[i+15] + uint64(minv)
		i += 16
	}
}
