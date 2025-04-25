// Copyright (c) 2025 Blockwatch Data Inc.
// Code automatically generated - DO NOT EDIT.
// Any manual changes will be lost.

package bitpack

// Compare lt

func cmp_lt(in []uint64, val uint64, log2 int) uint64 {
	switch log2 {
	case 0:
		return cmp_bp_0_lt((*[0]uint64)(in), val)
	case 1:
		return cmp_bp_1_lt((*[1]uint64)(in), val)
	case 2:
		return cmp_bp_2_lt((*[2]uint64)(in), val)
	case 3:
		return cmp_bp_3_lt((*[3]uint64)(in), val)
	case 4:
		return cmp_bp_4_lt((*[4]uint64)(in), val)
	case 5:
		return cmp_bp_5_lt((*[5]uint64)(in), val)
	case 6:
		return cmp_bp_6_lt((*[6]uint64)(in), val)
	case 7:
		return cmp_bp_7_lt((*[7]uint64)(in), val)
	case 8:
		return cmp_bp_8_lt((*[8]uint64)(in), val)
	case 9:
		return cmp_bp_9_lt((*[9]uint64)(in), val)
	case 10:
		return cmp_bp_10_lt((*[10]uint64)(in), val)
	case 11:
		return cmp_bp_11_lt((*[11]uint64)(in), val)
	case 12:
		return cmp_bp_12_lt((*[12]uint64)(in), val)
	case 13:
		return cmp_bp_13_lt((*[13]uint64)(in), val)
	case 14:
		return cmp_bp_14_lt((*[14]uint64)(in), val)
	case 15:
		return cmp_bp_15_lt((*[15]uint64)(in), val)
	case 16:
		return cmp_bp_16_lt((*[16]uint64)(in), val)
	case 17:
		return cmp_bp_17_lt((*[17]uint64)(in), val)
	case 18:
		return cmp_bp_18_lt((*[18]uint64)(in), val)
	case 19:
		return cmp_bp_19_lt((*[19]uint64)(in), val)
	case 20:
		return cmp_bp_20_lt((*[20]uint64)(in), val)
	case 21:
		return cmp_bp_21_lt((*[21]uint64)(in), val)
	case 22:
		return cmp_bp_22_lt((*[22]uint64)(in), val)
	case 23:
		return cmp_bp_23_lt((*[23]uint64)(in), val)
	case 24:
		return cmp_bp_24_lt((*[24]uint64)(in), val)
	case 25:
		return cmp_bp_25_lt((*[25]uint64)(in), val)
	case 26:
		return cmp_bp_26_lt((*[26]uint64)(in), val)
	case 27:
		return cmp_bp_27_lt((*[27]uint64)(in), val)
	case 28:
		return cmp_bp_28_lt((*[28]uint64)(in), val)
	case 29:
		return cmp_bp_29_lt((*[29]uint64)(in), val)
	case 30:
		return cmp_bp_30_lt((*[30]uint64)(in), val)
	case 31:
		return cmp_bp_31_lt((*[31]uint64)(in), val)
	case 32:
		return cmp_bp_32_lt((*[32]uint64)(in), val)
	case 33:
		return cmp_bp_33_lt((*[33]uint64)(in), val)
	case 34:
		return cmp_bp_34_lt((*[34]uint64)(in), val)
	case 35:
		return cmp_bp_35_lt((*[35]uint64)(in), val)
	case 36:
		return cmp_bp_36_lt((*[36]uint64)(in), val)
	case 37:
		return cmp_bp_37_lt((*[37]uint64)(in), val)
	case 38:
		return cmp_bp_38_lt((*[38]uint64)(in), val)
	case 39:
		return cmp_bp_39_lt((*[39]uint64)(in), val)
	case 40:
		return cmp_bp_40_lt((*[40]uint64)(in), val)
	case 41:
		return cmp_bp_41_lt((*[41]uint64)(in), val)
	case 42:
		return cmp_bp_42_lt((*[42]uint64)(in), val)
	case 43:
		return cmp_bp_43_lt((*[43]uint64)(in), val)
	case 44:
		return cmp_bp_44_lt((*[44]uint64)(in), val)
	case 45:
		return cmp_bp_45_lt((*[45]uint64)(in), val)
	case 46:
		return cmp_bp_46_lt((*[46]uint64)(in), val)
	case 47:
		return cmp_bp_47_lt((*[47]uint64)(in), val)
	case 48:
		return cmp_bp_48_lt((*[48]uint64)(in), val)
	case 49:
		return cmp_bp_49_lt((*[49]uint64)(in), val)
	case 50:
		return cmp_bp_50_lt((*[50]uint64)(in), val)
	case 51:
		return cmp_bp_51_lt((*[51]uint64)(in), val)
	case 52:
		return cmp_bp_52_lt((*[52]uint64)(in), val)
	case 53:
		return cmp_bp_53_lt((*[53]uint64)(in), val)
	case 54:
		return cmp_bp_54_lt((*[54]uint64)(in), val)
	case 55:
		return cmp_bp_55_lt((*[55]uint64)(in), val)
	case 56:
		return cmp_bp_56_lt((*[56]uint64)(in), val)
	case 57:
		return cmp_bp_57_lt((*[57]uint64)(in), val)
	case 58:
		return cmp_bp_58_lt((*[58]uint64)(in), val)
	case 59:
		return cmp_bp_59_lt((*[59]uint64)(in), val)
	case 60:
		return cmp_bp_60_lt((*[60]uint64)(in), val)
	case 61:
		return cmp_bp_61_lt((*[61]uint64)(in), val)
	case 62:
		return cmp_bp_62_lt((*[62]uint64)(in), val)
	case 63:
		return cmp_bp_63_lt((*[63]uint64)(in), val)
	}
	return 0
}
func cmp_bp_0_lt(in *[0]uint64, val uint64) uint64 {

	if val > 0 {
		return uint64(0xFFFFFFFF)
	}
	return 0

}
func cmp_bp_1_lt(in *[1]uint64, val uint64) uint64 {
	mask := uint64((1 << 1) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>1)&mask < val)<<1 |
		b2u64((in[0]>>2)&mask < val)<<2 |
		b2u64((in[0]>>3)&mask < val)<<3 |
		b2u64((in[0]>>4)&mask < val)<<4 |
		b2u64((in[0]>>5)&mask < val)<<5 |
		b2u64((in[0]>>6)&mask < val)<<6 |
		b2u64((in[0]>>7)&mask < val)<<7 |
		b2u64((in[0]>>8)&mask < val)<<8 |
		b2u64((in[0]>>9)&mask < val)<<9 |
		b2u64((in[0]>>10)&mask < val)<<10 |
		b2u64((in[0]>>11)&mask < val)<<11 |
		b2u64((in[0]>>12)&mask < val)<<12 |
		b2u64((in[0]>>13)&mask < val)<<13 |
		b2u64((in[0]>>14)&mask < val)<<14 |
		b2u64((in[0]>>15)&mask < val)<<15 |
		b2u64((in[0]>>16)&mask < val)<<16 |
		b2u64((in[0]>>17)&mask < val)<<17 |
		b2u64((in[0]>>18)&mask < val)<<18 |
		b2u64((in[0]>>19)&mask < val)<<19 |
		b2u64((in[0]>>20)&mask < val)<<20 |
		b2u64((in[0]>>21)&mask < val)<<21 |
		b2u64((in[0]>>22)&mask < val)<<22 |
		b2u64((in[0]>>23)&mask < val)<<23 |
		b2u64((in[0]>>24)&mask < val)<<24 |
		b2u64((in[0]>>25)&mask < val)<<25 |
		b2u64((in[0]>>26)&mask < val)<<26 |
		b2u64((in[0]>>27)&mask < val)<<27 |
		b2u64((in[0]>>28)&mask < val)<<28 |
		b2u64((in[0]>>29)&mask < val)<<29 |
		b2u64((in[0]>>30)&mask < val)<<30 |
		b2u64((in[0]>>31)&mask < val)<<31 |
		b2u64((in[0]>>32)&mask < val)<<32 |
		b2u64((in[0]>>33)&mask < val)<<33 |
		b2u64((in[0]>>34)&mask < val)<<34 |
		b2u64((in[0]>>35)&mask < val)<<35 |
		b2u64((in[0]>>36)&mask < val)<<36 |
		b2u64((in[0]>>37)&mask < val)<<37 |
		b2u64((in[0]>>38)&mask < val)<<38 |
		b2u64((in[0]>>39)&mask < val)<<39 |
		b2u64((in[0]>>40)&mask < val)<<40 |
		b2u64((in[0]>>41)&mask < val)<<41 |
		b2u64((in[0]>>42)&mask < val)<<42 |
		b2u64((in[0]>>43)&mask < val)<<43 |
		b2u64((in[0]>>44)&mask < val)<<44 |
		b2u64((in[0]>>45)&mask < val)<<45 |
		b2u64((in[0]>>46)&mask < val)<<46 |
		b2u64((in[0]>>47)&mask < val)<<47 |
		b2u64((in[0]>>48)&mask < val)<<48 |
		b2u64((in[0]>>49)&mask < val)<<49 |
		b2u64((in[0]>>50)&mask < val)<<50 |
		b2u64((in[0]>>51)&mask < val)<<51 |
		b2u64((in[0]>>52)&mask < val)<<52 |
		b2u64((in[0]>>53)&mask < val)<<53 |
		b2u64((in[0]>>54)&mask < val)<<54 |
		b2u64((in[0]>>55)&mask < val)<<55 |
		b2u64((in[0]>>56)&mask < val)<<56 |
		b2u64((in[0]>>57)&mask < val)<<57 |
		b2u64((in[0]>>58)&mask < val)<<58 |
		b2u64((in[0]>>59)&mask < val)<<59 |
		b2u64((in[0]>>60)&mask < val)<<60 |
		b2u64((in[0]>>61)&mask < val)<<61 |
		b2u64((in[0]>>62)&mask < val)<<62 |
		b2u64((in[0]>>63)&mask < val)<<63)

}
func cmp_bp_2_lt(in *[2]uint64, val uint64) uint64 {
	mask := uint64((1 << 2) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>2)&mask < val)<<1 |
		b2u64((in[0]>>4)&mask < val)<<2 |
		b2u64((in[0]>>6)&mask < val)<<3 |
		b2u64((in[0]>>8)&mask < val)<<4 |
		b2u64((in[0]>>10)&mask < val)<<5 |
		b2u64((in[0]>>12)&mask < val)<<6 |
		b2u64((in[0]>>14)&mask < val)<<7 |
		b2u64((in[0]>>16)&mask < val)<<8 |
		b2u64((in[0]>>18)&mask < val)<<9 |
		b2u64((in[0]>>20)&mask < val)<<10 |
		b2u64((in[0]>>22)&mask < val)<<11 |
		b2u64((in[0]>>24)&mask < val)<<12 |
		b2u64((in[0]>>26)&mask < val)<<13 |
		b2u64((in[0]>>28)&mask < val)<<14 |
		b2u64((in[0]>>30)&mask < val)<<15 |
		b2u64((in[0]>>32)&mask < val)<<16 |
		b2u64((in[0]>>34)&mask < val)<<17 |
		b2u64((in[0]>>36)&mask < val)<<18 |
		b2u64((in[0]>>38)&mask < val)<<19 |
		b2u64((in[0]>>40)&mask < val)<<20 |
		b2u64((in[0]>>42)&mask < val)<<21 |
		b2u64((in[0]>>44)&mask < val)<<22 |
		b2u64((in[0]>>46)&mask < val)<<23 |
		b2u64((in[0]>>48)&mask < val)<<24 |
		b2u64((in[0]>>50)&mask < val)<<25 |
		b2u64((in[0]>>52)&mask < val)<<26 |
		b2u64((in[0]>>54)&mask < val)<<27 |
		b2u64((in[0]>>56)&mask < val)<<28 |
		b2u64((in[0]>>58)&mask < val)<<29 |
		b2u64((in[0]>>60)&mask < val)<<30 |
		b2u64((in[0]>>62)&mask < val)<<31 |
		b2u64((in[1]>>0)&mask < val)<<32 |
		b2u64((in[1]>>2)&mask < val)<<33 |
		b2u64((in[1]>>4)&mask < val)<<34 |
		b2u64((in[1]>>6)&mask < val)<<35 |
		b2u64((in[1]>>8)&mask < val)<<36 |
		b2u64((in[1]>>10)&mask < val)<<37 |
		b2u64((in[1]>>12)&mask < val)<<38 |
		b2u64((in[1]>>14)&mask < val)<<39 |
		b2u64((in[1]>>16)&mask < val)<<40 |
		b2u64((in[1]>>18)&mask < val)<<41 |
		b2u64((in[1]>>20)&mask < val)<<42 |
		b2u64((in[1]>>22)&mask < val)<<43 |
		b2u64((in[1]>>24)&mask < val)<<44 |
		b2u64((in[1]>>26)&mask < val)<<45 |
		b2u64((in[1]>>28)&mask < val)<<46 |
		b2u64((in[1]>>30)&mask < val)<<47 |
		b2u64((in[1]>>32)&mask < val)<<48 |
		b2u64((in[1]>>34)&mask < val)<<49 |
		b2u64((in[1]>>36)&mask < val)<<50 |
		b2u64((in[1]>>38)&mask < val)<<51 |
		b2u64((in[1]>>40)&mask < val)<<52 |
		b2u64((in[1]>>42)&mask < val)<<53 |
		b2u64((in[1]>>44)&mask < val)<<54 |
		b2u64((in[1]>>46)&mask < val)<<55 |
		b2u64((in[1]>>48)&mask < val)<<56 |
		b2u64((in[1]>>50)&mask < val)<<57 |
		b2u64((in[1]>>52)&mask < val)<<58 |
		b2u64((in[1]>>54)&mask < val)<<59 |
		b2u64((in[1]>>56)&mask < val)<<60 |
		b2u64((in[1]>>58)&mask < val)<<61 |
		b2u64((in[1]>>60)&mask < val)<<62 |
		b2u64((in[1]>>62)&mask < val)<<63)

}
func cmp_bp_3_lt(in *[3]uint64, val uint64) uint64 {
	mask := uint64((1 << 3) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>3)&mask < val)<<1 |
		b2u64((in[0]>>6)&mask < val)<<2 |
		b2u64((in[0]>>9)&mask < val)<<3 |
		b2u64((in[0]>>12)&mask < val)<<4 |
		b2u64((in[0]>>15)&mask < val)<<5 |
		b2u64((in[0]>>18)&mask < val)<<6 |
		b2u64((in[0]>>21)&mask < val)<<7 |
		b2u64((in[0]>>24)&mask < val)<<8 |
		b2u64((in[0]>>27)&mask < val)<<9 |
		b2u64((in[0]>>30)&mask < val)<<10 |
		b2u64((in[0]>>33)&mask < val)<<11 |
		b2u64((in[0]>>36)&mask < val)<<12 |
		b2u64((in[0]>>39)&mask < val)<<13 |
		b2u64((in[0]>>42)&mask < val)<<14 |
		b2u64((in[0]>>45)&mask < val)<<15 |
		b2u64((in[0]>>48)&mask < val)<<16 |
		b2u64((in[0]>>51)&mask < val)<<17 |
		b2u64((in[0]>>54)&mask < val)<<18 |
		b2u64((in[0]>>57)&mask < val)<<19 |
		b2u64((in[0]>>60)&mask < val)<<20 |
		b2u64((in[0]>>63)&mask|
			(in[1]<<1)&mask < val)<<21 |
		b2u64((in[1]>>2)&mask < val)<<22 |
		b2u64((in[1]>>5)&mask < val)<<23 |
		b2u64((in[1]>>8)&mask < val)<<24 |
		b2u64((in[1]>>11)&mask < val)<<25 |
		b2u64((in[1]>>14)&mask < val)<<26 |
		b2u64((in[1]>>17)&mask < val)<<27 |
		b2u64((in[1]>>20)&mask < val)<<28 |
		b2u64((in[1]>>23)&mask < val)<<29 |
		b2u64((in[1]>>26)&mask < val)<<30 |
		b2u64((in[1]>>29)&mask < val)<<31 |
		b2u64((in[1]>>32)&mask < val)<<32 |
		b2u64((in[1]>>35)&mask < val)<<33 |
		b2u64((in[1]>>38)&mask < val)<<34 |
		b2u64((in[1]>>41)&mask < val)<<35 |
		b2u64((in[1]>>44)&mask < val)<<36 |
		b2u64((in[1]>>47)&mask < val)<<37 |
		b2u64((in[1]>>50)&mask < val)<<38 |
		b2u64((in[1]>>53)&mask < val)<<39 |
		b2u64((in[1]>>56)&mask < val)<<40 |
		b2u64((in[1]>>59)&mask < val)<<41 |
		b2u64((in[1]>>62)&mask|
			(in[2]<<2)&mask < val)<<42 |
		b2u64((in[2]>>1)&mask < val)<<43 |
		b2u64((in[2]>>4)&mask < val)<<44 |
		b2u64((in[2]>>7)&mask < val)<<45 |
		b2u64((in[2]>>10)&mask < val)<<46 |
		b2u64((in[2]>>13)&mask < val)<<47 |
		b2u64((in[2]>>16)&mask < val)<<48 |
		b2u64((in[2]>>19)&mask < val)<<49 |
		b2u64((in[2]>>22)&mask < val)<<50 |
		b2u64((in[2]>>25)&mask < val)<<51 |
		b2u64((in[2]>>28)&mask < val)<<52 |
		b2u64((in[2]>>31)&mask < val)<<53 |
		b2u64((in[2]>>34)&mask < val)<<54 |
		b2u64((in[2]>>37)&mask < val)<<55 |
		b2u64((in[2]>>40)&mask < val)<<56 |
		b2u64((in[2]>>43)&mask < val)<<57 |
		b2u64((in[2]>>46)&mask < val)<<58 |
		b2u64((in[2]>>49)&mask < val)<<59 |
		b2u64((in[2]>>52)&mask < val)<<60 |
		b2u64((in[2]>>55)&mask < val)<<61 |
		b2u64((in[2]>>58)&mask < val)<<62 |
		b2u64((in[2]>>61)&mask < val)<<63)

}
func cmp_bp_4_lt(in *[4]uint64, val uint64) uint64 {
	mask := uint64((1 << 4) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>4)&mask < val)<<1 |
		b2u64((in[0]>>8)&mask < val)<<2 |
		b2u64((in[0]>>12)&mask < val)<<3 |
		b2u64((in[0]>>16)&mask < val)<<4 |
		b2u64((in[0]>>20)&mask < val)<<5 |
		b2u64((in[0]>>24)&mask < val)<<6 |
		b2u64((in[0]>>28)&mask < val)<<7 |
		b2u64((in[0]>>32)&mask < val)<<8 |
		b2u64((in[0]>>36)&mask < val)<<9 |
		b2u64((in[0]>>40)&mask < val)<<10 |
		b2u64((in[0]>>44)&mask < val)<<11 |
		b2u64((in[0]>>48)&mask < val)<<12 |
		b2u64((in[0]>>52)&mask < val)<<13 |
		b2u64((in[0]>>56)&mask < val)<<14 |
		b2u64((in[0]>>60)&mask < val)<<15 |
		b2u64((in[1]>>0)&mask < val)<<16 |
		b2u64((in[1]>>4)&mask < val)<<17 |
		b2u64((in[1]>>8)&mask < val)<<18 |
		b2u64((in[1]>>12)&mask < val)<<19 |
		b2u64((in[1]>>16)&mask < val)<<20 |
		b2u64((in[1]>>20)&mask < val)<<21 |
		b2u64((in[1]>>24)&mask < val)<<22 |
		b2u64((in[1]>>28)&mask < val)<<23 |
		b2u64((in[1]>>32)&mask < val)<<24 |
		b2u64((in[1]>>36)&mask < val)<<25 |
		b2u64((in[1]>>40)&mask < val)<<26 |
		b2u64((in[1]>>44)&mask < val)<<27 |
		b2u64((in[1]>>48)&mask < val)<<28 |
		b2u64((in[1]>>52)&mask < val)<<29 |
		b2u64((in[1]>>56)&mask < val)<<30 |
		b2u64((in[1]>>60)&mask < val)<<31 |
		b2u64((in[2]>>0)&mask < val)<<32 |
		b2u64((in[2]>>4)&mask < val)<<33 |
		b2u64((in[2]>>8)&mask < val)<<34 |
		b2u64((in[2]>>12)&mask < val)<<35 |
		b2u64((in[2]>>16)&mask < val)<<36 |
		b2u64((in[2]>>20)&mask < val)<<37 |
		b2u64((in[2]>>24)&mask < val)<<38 |
		b2u64((in[2]>>28)&mask < val)<<39 |
		b2u64((in[2]>>32)&mask < val)<<40 |
		b2u64((in[2]>>36)&mask < val)<<41 |
		b2u64((in[2]>>40)&mask < val)<<42 |
		b2u64((in[2]>>44)&mask < val)<<43 |
		b2u64((in[2]>>48)&mask < val)<<44 |
		b2u64((in[2]>>52)&mask < val)<<45 |
		b2u64((in[2]>>56)&mask < val)<<46 |
		b2u64((in[2]>>60)&mask < val)<<47 |
		b2u64((in[3]>>0)&mask < val)<<48 |
		b2u64((in[3]>>4)&mask < val)<<49 |
		b2u64((in[3]>>8)&mask < val)<<50 |
		b2u64((in[3]>>12)&mask < val)<<51 |
		b2u64((in[3]>>16)&mask < val)<<52 |
		b2u64((in[3]>>20)&mask < val)<<53 |
		b2u64((in[3]>>24)&mask < val)<<54 |
		b2u64((in[3]>>28)&mask < val)<<55 |
		b2u64((in[3]>>32)&mask < val)<<56 |
		b2u64((in[3]>>36)&mask < val)<<57 |
		b2u64((in[3]>>40)&mask < val)<<58 |
		b2u64((in[3]>>44)&mask < val)<<59 |
		b2u64((in[3]>>48)&mask < val)<<60 |
		b2u64((in[3]>>52)&mask < val)<<61 |
		b2u64((in[3]>>56)&mask < val)<<62 |
		b2u64((in[3]>>60)&mask < val)<<63)

}
func cmp_bp_5_lt(in *[5]uint64, val uint64) uint64 {
	mask := uint64((1 << 5) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>5)&mask < val)<<1 |
		b2u64((in[0]>>10)&mask < val)<<2 |
		b2u64((in[0]>>15)&mask < val)<<3 |
		b2u64((in[0]>>20)&mask < val)<<4 |
		b2u64((in[0]>>25)&mask < val)<<5 |
		b2u64((in[0]>>30)&mask < val)<<6 |
		b2u64((in[0]>>35)&mask < val)<<7 |
		b2u64((in[0]>>40)&mask < val)<<8 |
		b2u64((in[0]>>45)&mask < val)<<9 |
		b2u64((in[0]>>50)&mask < val)<<10 |
		b2u64((in[0]>>55)&mask < val)<<11 |
		b2u64((in[0]>>60)&mask|
			(in[1]<<4)&mask < val)<<12 |
		b2u64((in[1]>>1)&mask < val)<<13 |
		b2u64((in[1]>>6)&mask < val)<<14 |
		b2u64((in[1]>>11)&mask < val)<<15 |
		b2u64((in[1]>>16)&mask < val)<<16 |
		b2u64((in[1]>>21)&mask < val)<<17 |
		b2u64((in[1]>>26)&mask < val)<<18 |
		b2u64((in[1]>>31)&mask < val)<<19 |
		b2u64((in[1]>>36)&mask < val)<<20 |
		b2u64((in[1]>>41)&mask < val)<<21 |
		b2u64((in[1]>>46)&mask < val)<<22 |
		b2u64((in[1]>>51)&mask < val)<<23 |
		b2u64((in[1]>>56)&mask < val)<<24 |
		b2u64((in[1]>>61)&mask|
			(in[2]<<3)&mask < val)<<25 |
		b2u64((in[2]>>2)&mask < val)<<26 |
		b2u64((in[2]>>7)&mask < val)<<27 |
		b2u64((in[2]>>12)&mask < val)<<28 |
		b2u64((in[2]>>17)&mask < val)<<29 |
		b2u64((in[2]>>22)&mask < val)<<30 |
		b2u64((in[2]>>27)&mask < val)<<31 |
		b2u64((in[2]>>32)&mask < val)<<32 |
		b2u64((in[2]>>37)&mask < val)<<33 |
		b2u64((in[2]>>42)&mask < val)<<34 |
		b2u64((in[2]>>47)&mask < val)<<35 |
		b2u64((in[2]>>52)&mask < val)<<36 |
		b2u64((in[2]>>57)&mask < val)<<37 |
		b2u64((in[2]>>62)&mask|
			(in[3]<<2)&mask < val)<<38 |
		b2u64((in[3]>>3)&mask < val)<<39 |
		b2u64((in[3]>>8)&mask < val)<<40 |
		b2u64((in[3]>>13)&mask < val)<<41 |
		b2u64((in[3]>>18)&mask < val)<<42 |
		b2u64((in[3]>>23)&mask < val)<<43 |
		b2u64((in[3]>>28)&mask < val)<<44 |
		b2u64((in[3]>>33)&mask < val)<<45 |
		b2u64((in[3]>>38)&mask < val)<<46 |
		b2u64((in[3]>>43)&mask < val)<<47 |
		b2u64((in[3]>>48)&mask < val)<<48 |
		b2u64((in[3]>>53)&mask < val)<<49 |
		b2u64((in[3]>>58)&mask < val)<<50 |
		b2u64((in[3]>>63)&mask|
			(in[4]<<1)&mask < val)<<51 |
		b2u64((in[4]>>4)&mask < val)<<52 |
		b2u64((in[4]>>9)&mask < val)<<53 |
		b2u64((in[4]>>14)&mask < val)<<54 |
		b2u64((in[4]>>19)&mask < val)<<55 |
		b2u64((in[4]>>24)&mask < val)<<56 |
		b2u64((in[4]>>29)&mask < val)<<57 |
		b2u64((in[4]>>34)&mask < val)<<58 |
		b2u64((in[4]>>39)&mask < val)<<59 |
		b2u64((in[4]>>44)&mask < val)<<60 |
		b2u64((in[4]>>49)&mask < val)<<61 |
		b2u64((in[4]>>54)&mask < val)<<62 |
		b2u64((in[4]>>59)&mask < val)<<63)

}
func cmp_bp_6_lt(in *[6]uint64, val uint64) uint64 {
	mask := uint64((1 << 6) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>6)&mask < val)<<1 |
		b2u64((in[0]>>12)&mask < val)<<2 |
		b2u64((in[0]>>18)&mask < val)<<3 |
		b2u64((in[0]>>24)&mask < val)<<4 |
		b2u64((in[0]>>30)&mask < val)<<5 |
		b2u64((in[0]>>36)&mask < val)<<6 |
		b2u64((in[0]>>42)&mask < val)<<7 |
		b2u64((in[0]>>48)&mask < val)<<8 |
		b2u64((in[0]>>54)&mask < val)<<9 |
		b2u64((in[0]>>60)&mask|
			(in[1]<<4)&mask < val)<<10 |
		b2u64((in[1]>>2)&mask < val)<<11 |
		b2u64((in[1]>>8)&mask < val)<<12 |
		b2u64((in[1]>>14)&mask < val)<<13 |
		b2u64((in[1]>>20)&mask < val)<<14 |
		b2u64((in[1]>>26)&mask < val)<<15 |
		b2u64((in[1]>>32)&mask < val)<<16 |
		b2u64((in[1]>>38)&mask < val)<<17 |
		b2u64((in[1]>>44)&mask < val)<<18 |
		b2u64((in[1]>>50)&mask < val)<<19 |
		b2u64((in[1]>>56)&mask < val)<<20 |
		b2u64((in[1]>>62)&mask|
			(in[2]<<2)&mask < val)<<21 |
		b2u64((in[2]>>4)&mask < val)<<22 |
		b2u64((in[2]>>10)&mask < val)<<23 |
		b2u64((in[2]>>16)&mask < val)<<24 |
		b2u64((in[2]>>22)&mask < val)<<25 |
		b2u64((in[2]>>28)&mask < val)<<26 |
		b2u64((in[2]>>34)&mask < val)<<27 |
		b2u64((in[2]>>40)&mask < val)<<28 |
		b2u64((in[2]>>46)&mask < val)<<29 |
		b2u64((in[2]>>52)&mask < val)<<30 |
		b2u64((in[2]>>58)&mask < val)<<31 |
		b2u64((in[3]>>0)&mask < val)<<32 |
		b2u64((in[3]>>6)&mask < val)<<33 |
		b2u64((in[3]>>12)&mask < val)<<34 |
		b2u64((in[3]>>18)&mask < val)<<35 |
		b2u64((in[3]>>24)&mask < val)<<36 |
		b2u64((in[3]>>30)&mask < val)<<37 |
		b2u64((in[3]>>36)&mask < val)<<38 |
		b2u64((in[3]>>42)&mask < val)<<39 |
		b2u64((in[3]>>48)&mask < val)<<40 |
		b2u64((in[3]>>54)&mask < val)<<41 |
		b2u64((in[3]>>60)&mask|
			(in[4]<<4)&mask < val)<<42 |
		b2u64((in[4]>>2)&mask < val)<<43 |
		b2u64((in[4]>>8)&mask < val)<<44 |
		b2u64((in[4]>>14)&mask < val)<<45 |
		b2u64((in[4]>>20)&mask < val)<<46 |
		b2u64((in[4]>>26)&mask < val)<<47 |
		b2u64((in[4]>>32)&mask < val)<<48 |
		b2u64((in[4]>>38)&mask < val)<<49 |
		b2u64((in[4]>>44)&mask < val)<<50 |
		b2u64((in[4]>>50)&mask < val)<<51 |
		b2u64((in[4]>>56)&mask < val)<<52 |
		b2u64((in[4]>>62)&mask|
			(in[5]<<2)&mask < val)<<53 |
		b2u64((in[5]>>4)&mask < val)<<54 |
		b2u64((in[5]>>10)&mask < val)<<55 |
		b2u64((in[5]>>16)&mask < val)<<56 |
		b2u64((in[5]>>22)&mask < val)<<57 |
		b2u64((in[5]>>28)&mask < val)<<58 |
		b2u64((in[5]>>34)&mask < val)<<59 |
		b2u64((in[5]>>40)&mask < val)<<60 |
		b2u64((in[5]>>46)&mask < val)<<61 |
		b2u64((in[5]>>52)&mask < val)<<62 |
		b2u64((in[5]>>58)&mask < val)<<63)

}
func cmp_bp_7_lt(in *[7]uint64, val uint64) uint64 {
	mask := uint64((1 << 7) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>7)&mask < val)<<1 |
		b2u64((in[0]>>14)&mask < val)<<2 |
		b2u64((in[0]>>21)&mask < val)<<3 |
		b2u64((in[0]>>28)&mask < val)<<4 |
		b2u64((in[0]>>35)&mask < val)<<5 |
		b2u64((in[0]>>42)&mask < val)<<6 |
		b2u64((in[0]>>49)&mask < val)<<7 |
		b2u64((in[0]>>56)&mask < val)<<8 |
		b2u64((in[0]>>63)&mask|
			(in[1]<<1)&mask < val)<<9 |
		b2u64((in[1]>>6)&mask < val)<<10 |
		b2u64((in[1]>>13)&mask < val)<<11 |
		b2u64((in[1]>>20)&mask < val)<<12 |
		b2u64((in[1]>>27)&mask < val)<<13 |
		b2u64((in[1]>>34)&mask < val)<<14 |
		b2u64((in[1]>>41)&mask < val)<<15 |
		b2u64((in[1]>>48)&mask < val)<<16 |
		b2u64((in[1]>>55)&mask < val)<<17 |
		b2u64((in[1]>>62)&mask|
			(in[2]<<2)&mask < val)<<18 |
		b2u64((in[2]>>5)&mask < val)<<19 |
		b2u64((in[2]>>12)&mask < val)<<20 |
		b2u64((in[2]>>19)&mask < val)<<21 |
		b2u64((in[2]>>26)&mask < val)<<22 |
		b2u64((in[2]>>33)&mask < val)<<23 |
		b2u64((in[2]>>40)&mask < val)<<24 |
		b2u64((in[2]>>47)&mask < val)<<25 |
		b2u64((in[2]>>54)&mask < val)<<26 |
		b2u64((in[2]>>61)&mask|
			(in[3]<<3)&mask < val)<<27 |
		b2u64((in[3]>>4)&mask < val)<<28 |
		b2u64((in[3]>>11)&mask < val)<<29 |
		b2u64((in[3]>>18)&mask < val)<<30 |
		b2u64((in[3]>>25)&mask < val)<<31 |
		b2u64((in[3]>>32)&mask < val)<<32 |
		b2u64((in[3]>>39)&mask < val)<<33 |
		b2u64((in[3]>>46)&mask < val)<<34 |
		b2u64((in[3]>>53)&mask < val)<<35 |
		b2u64((in[3]>>60)&mask|
			(in[4]<<4)&mask < val)<<36 |
		b2u64((in[4]>>3)&mask < val)<<37 |
		b2u64((in[4]>>10)&mask < val)<<38 |
		b2u64((in[4]>>17)&mask < val)<<39 |
		b2u64((in[4]>>24)&mask < val)<<40 |
		b2u64((in[4]>>31)&mask < val)<<41 |
		b2u64((in[4]>>38)&mask < val)<<42 |
		b2u64((in[4]>>45)&mask < val)<<43 |
		b2u64((in[4]>>52)&mask < val)<<44 |
		b2u64((in[4]>>59)&mask|
			(in[5]<<5)&mask < val)<<45 |
		b2u64((in[5]>>2)&mask < val)<<46 |
		b2u64((in[5]>>9)&mask < val)<<47 |
		b2u64((in[5]>>16)&mask < val)<<48 |
		b2u64((in[5]>>23)&mask < val)<<49 |
		b2u64((in[5]>>30)&mask < val)<<50 |
		b2u64((in[5]>>37)&mask < val)<<51 |
		b2u64((in[5]>>44)&mask < val)<<52 |
		b2u64((in[5]>>51)&mask < val)<<53 |
		b2u64((in[5]>>58)&mask|
			(in[6]<<6)&mask < val)<<54 |
		b2u64((in[6]>>1)&mask < val)<<55 |
		b2u64((in[6]>>8)&mask < val)<<56 |
		b2u64((in[6]>>15)&mask < val)<<57 |
		b2u64((in[6]>>22)&mask < val)<<58 |
		b2u64((in[6]>>29)&mask < val)<<59 |
		b2u64((in[6]>>36)&mask < val)<<60 |
		b2u64((in[6]>>43)&mask < val)<<61 |
		b2u64((in[6]>>50)&mask < val)<<62 |
		b2u64((in[6]>>57)&mask < val)<<63)

}
func cmp_bp_8_lt(in *[8]uint64, val uint64) uint64 {
	mask := uint64((1 << 8) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>8)&mask < val)<<1 |
		b2u64((in[0]>>16)&mask < val)<<2 |
		b2u64((in[0]>>24)&mask < val)<<3 |
		b2u64((in[0]>>32)&mask < val)<<4 |
		b2u64((in[0]>>40)&mask < val)<<5 |
		b2u64((in[0]>>48)&mask < val)<<6 |
		b2u64((in[0]>>56)&mask < val)<<7 |
		b2u64((in[1]>>0)&mask < val)<<8 |
		b2u64((in[1]>>8)&mask < val)<<9 |
		b2u64((in[1]>>16)&mask < val)<<10 |
		b2u64((in[1]>>24)&mask < val)<<11 |
		b2u64((in[1]>>32)&mask < val)<<12 |
		b2u64((in[1]>>40)&mask < val)<<13 |
		b2u64((in[1]>>48)&mask < val)<<14 |
		b2u64((in[1]>>56)&mask < val)<<15 |
		b2u64((in[2]>>0)&mask < val)<<16 |
		b2u64((in[2]>>8)&mask < val)<<17 |
		b2u64((in[2]>>16)&mask < val)<<18 |
		b2u64((in[2]>>24)&mask < val)<<19 |
		b2u64((in[2]>>32)&mask < val)<<20 |
		b2u64((in[2]>>40)&mask < val)<<21 |
		b2u64((in[2]>>48)&mask < val)<<22 |
		b2u64((in[2]>>56)&mask < val)<<23 |
		b2u64((in[3]>>0)&mask < val)<<24 |
		b2u64((in[3]>>8)&mask < val)<<25 |
		b2u64((in[3]>>16)&mask < val)<<26 |
		b2u64((in[3]>>24)&mask < val)<<27 |
		b2u64((in[3]>>32)&mask < val)<<28 |
		b2u64((in[3]>>40)&mask < val)<<29 |
		b2u64((in[3]>>48)&mask < val)<<30 |
		b2u64((in[3]>>56)&mask < val)<<31 |
		b2u64((in[4]>>0)&mask < val)<<32 |
		b2u64((in[4]>>8)&mask < val)<<33 |
		b2u64((in[4]>>16)&mask < val)<<34 |
		b2u64((in[4]>>24)&mask < val)<<35 |
		b2u64((in[4]>>32)&mask < val)<<36 |
		b2u64((in[4]>>40)&mask < val)<<37 |
		b2u64((in[4]>>48)&mask < val)<<38 |
		b2u64((in[4]>>56)&mask < val)<<39 |
		b2u64((in[5]>>0)&mask < val)<<40 |
		b2u64((in[5]>>8)&mask < val)<<41 |
		b2u64((in[5]>>16)&mask < val)<<42 |
		b2u64((in[5]>>24)&mask < val)<<43 |
		b2u64((in[5]>>32)&mask < val)<<44 |
		b2u64((in[5]>>40)&mask < val)<<45 |
		b2u64((in[5]>>48)&mask < val)<<46 |
		b2u64((in[5]>>56)&mask < val)<<47 |
		b2u64((in[6]>>0)&mask < val)<<48 |
		b2u64((in[6]>>8)&mask < val)<<49 |
		b2u64((in[6]>>16)&mask < val)<<50 |
		b2u64((in[6]>>24)&mask < val)<<51 |
		b2u64((in[6]>>32)&mask < val)<<52 |
		b2u64((in[6]>>40)&mask < val)<<53 |
		b2u64((in[6]>>48)&mask < val)<<54 |
		b2u64((in[6]>>56)&mask < val)<<55 |
		b2u64((in[7]>>0)&mask < val)<<56 |
		b2u64((in[7]>>8)&mask < val)<<57 |
		b2u64((in[7]>>16)&mask < val)<<58 |
		b2u64((in[7]>>24)&mask < val)<<59 |
		b2u64((in[7]>>32)&mask < val)<<60 |
		b2u64((in[7]>>40)&mask < val)<<61 |
		b2u64((in[7]>>48)&mask < val)<<62 |
		b2u64((in[7]>>56)&mask < val)<<63)

}
func cmp_bp_9_lt(in *[9]uint64, val uint64) uint64 {
	mask := uint64((1 << 9) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>9)&mask < val)<<1 |
		b2u64((in[0]>>18)&mask < val)<<2 |
		b2u64((in[0]>>27)&mask < val)<<3 |
		b2u64((in[0]>>36)&mask < val)<<4 |
		b2u64((in[0]>>45)&mask < val)<<5 |
		b2u64((in[0]>>54)&mask < val)<<6 |
		b2u64((in[0]>>63)&mask|
			(in[1]<<1)&mask < val)<<7 |
		b2u64((in[1]>>8)&mask < val)<<8 |
		b2u64((in[1]>>17)&mask < val)<<9 |
		b2u64((in[1]>>26)&mask < val)<<10 |
		b2u64((in[1]>>35)&mask < val)<<11 |
		b2u64((in[1]>>44)&mask < val)<<12 |
		b2u64((in[1]>>53)&mask < val)<<13 |
		b2u64((in[1]>>62)&mask|
			(in[2]<<2)&mask < val)<<14 |
		b2u64((in[2]>>7)&mask < val)<<15 |
		b2u64((in[2]>>16)&mask < val)<<16 |
		b2u64((in[2]>>25)&mask < val)<<17 |
		b2u64((in[2]>>34)&mask < val)<<18 |
		b2u64((in[2]>>43)&mask < val)<<19 |
		b2u64((in[2]>>52)&mask < val)<<20 |
		b2u64((in[2]>>61)&mask|
			(in[3]<<3)&mask < val)<<21 |
		b2u64((in[3]>>6)&mask < val)<<22 |
		b2u64((in[3]>>15)&mask < val)<<23 |
		b2u64((in[3]>>24)&mask < val)<<24 |
		b2u64((in[3]>>33)&mask < val)<<25 |
		b2u64((in[3]>>42)&mask < val)<<26 |
		b2u64((in[3]>>51)&mask < val)<<27 |
		b2u64((in[3]>>60)&mask|
			(in[4]<<4)&mask < val)<<28 |
		b2u64((in[4]>>5)&mask < val)<<29 |
		b2u64((in[4]>>14)&mask < val)<<30 |
		b2u64((in[4]>>23)&mask < val)<<31 |
		b2u64((in[4]>>32)&mask < val)<<32 |
		b2u64((in[4]>>41)&mask < val)<<33 |
		b2u64((in[4]>>50)&mask < val)<<34 |
		b2u64((in[4]>>59)&mask|
			(in[5]<<5)&mask < val)<<35 |
		b2u64((in[5]>>4)&mask < val)<<36 |
		b2u64((in[5]>>13)&mask < val)<<37 |
		b2u64((in[5]>>22)&mask < val)<<38 |
		b2u64((in[5]>>31)&mask < val)<<39 |
		b2u64((in[5]>>40)&mask < val)<<40 |
		b2u64((in[5]>>49)&mask < val)<<41 |
		b2u64((in[5]>>58)&mask|
			(in[6]<<6)&mask < val)<<42 |
		b2u64((in[6]>>3)&mask < val)<<43 |
		b2u64((in[6]>>12)&mask < val)<<44 |
		b2u64((in[6]>>21)&mask < val)<<45 |
		b2u64((in[6]>>30)&mask < val)<<46 |
		b2u64((in[6]>>39)&mask < val)<<47 |
		b2u64((in[6]>>48)&mask < val)<<48 |
		b2u64((in[6]>>57)&mask|
			(in[7]<<7)&mask < val)<<49 |
		b2u64((in[7]>>2)&mask < val)<<50 |
		b2u64((in[7]>>11)&mask < val)<<51 |
		b2u64((in[7]>>20)&mask < val)<<52 |
		b2u64((in[7]>>29)&mask < val)<<53 |
		b2u64((in[7]>>38)&mask < val)<<54 |
		b2u64((in[7]>>47)&mask < val)<<55 |
		b2u64((in[7]>>56)&mask|
			(in[8]<<8)&mask < val)<<56 |
		b2u64((in[8]>>1)&mask < val)<<57 |
		b2u64((in[8]>>10)&mask < val)<<58 |
		b2u64((in[8]>>19)&mask < val)<<59 |
		b2u64((in[8]>>28)&mask < val)<<60 |
		b2u64((in[8]>>37)&mask < val)<<61 |
		b2u64((in[8]>>46)&mask < val)<<62 |
		b2u64((in[8]>>55)&mask < val)<<63)

}
func cmp_bp_10_lt(in *[10]uint64, val uint64) uint64 {
	mask := uint64((1 << 10) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>10)&mask < val)<<1 |
		b2u64((in[0]>>20)&mask < val)<<2 |
		b2u64((in[0]>>30)&mask < val)<<3 |
		b2u64((in[0]>>40)&mask < val)<<4 |
		b2u64((in[0]>>50)&mask < val)<<5 |
		b2u64((in[0]>>60)&mask|
			(in[1]<<4)&mask < val)<<6 |
		b2u64((in[1]>>6)&mask < val)<<7 |
		b2u64((in[1]>>16)&mask < val)<<8 |
		b2u64((in[1]>>26)&mask < val)<<9 |
		b2u64((in[1]>>36)&mask < val)<<10 |
		b2u64((in[1]>>46)&mask < val)<<11 |
		b2u64((in[1]>>56)&mask|
			(in[2]<<8)&mask < val)<<12 |
		b2u64((in[2]>>2)&mask < val)<<13 |
		b2u64((in[2]>>12)&mask < val)<<14 |
		b2u64((in[2]>>22)&mask < val)<<15 |
		b2u64((in[2]>>32)&mask < val)<<16 |
		b2u64((in[2]>>42)&mask < val)<<17 |
		b2u64((in[2]>>52)&mask < val)<<18 |
		b2u64((in[2]>>62)&mask|
			(in[3]<<2)&mask < val)<<19 |
		b2u64((in[3]>>8)&mask < val)<<20 |
		b2u64((in[3]>>18)&mask < val)<<21 |
		b2u64((in[3]>>28)&mask < val)<<22 |
		b2u64((in[3]>>38)&mask < val)<<23 |
		b2u64((in[3]>>48)&mask < val)<<24 |
		b2u64((in[3]>>58)&mask|
			(in[4]<<6)&mask < val)<<25 |
		b2u64((in[4]>>4)&mask < val)<<26 |
		b2u64((in[4]>>14)&mask < val)<<27 |
		b2u64((in[4]>>24)&mask < val)<<28 |
		b2u64((in[4]>>34)&mask < val)<<29 |
		b2u64((in[4]>>44)&mask < val)<<30 |
		b2u64((in[4]>>54)&mask < val)<<31 |
		b2u64((in[5]>>0)&mask < val)<<32 |
		b2u64((in[5]>>10)&mask < val)<<33 |
		b2u64((in[5]>>20)&mask < val)<<34 |
		b2u64((in[5]>>30)&mask < val)<<35 |
		b2u64((in[5]>>40)&mask < val)<<36 |
		b2u64((in[5]>>50)&mask < val)<<37 |
		b2u64((in[5]>>60)&mask|
			(in[6]<<4)&mask < val)<<38 |
		b2u64((in[6]>>6)&mask < val)<<39 |
		b2u64((in[6]>>16)&mask < val)<<40 |
		b2u64((in[6]>>26)&mask < val)<<41 |
		b2u64((in[6]>>36)&mask < val)<<42 |
		b2u64((in[6]>>46)&mask < val)<<43 |
		b2u64((in[6]>>56)&mask|
			(in[7]<<8)&mask < val)<<44 |
		b2u64((in[7]>>2)&mask < val)<<45 |
		b2u64((in[7]>>12)&mask < val)<<46 |
		b2u64((in[7]>>22)&mask < val)<<47 |
		b2u64((in[7]>>32)&mask < val)<<48 |
		b2u64((in[7]>>42)&mask < val)<<49 |
		b2u64((in[7]>>52)&mask < val)<<50 |
		b2u64((in[7]>>62)&mask|
			(in[8]<<2)&mask < val)<<51 |
		b2u64((in[8]>>8)&mask < val)<<52 |
		b2u64((in[8]>>18)&mask < val)<<53 |
		b2u64((in[8]>>28)&mask < val)<<54 |
		b2u64((in[8]>>38)&mask < val)<<55 |
		b2u64((in[8]>>48)&mask < val)<<56 |
		b2u64((in[8]>>58)&mask|
			(in[9]<<6)&mask < val)<<57 |
		b2u64((in[9]>>4)&mask < val)<<58 |
		b2u64((in[9]>>14)&mask < val)<<59 |
		b2u64((in[9]>>24)&mask < val)<<60 |
		b2u64((in[9]>>34)&mask < val)<<61 |
		b2u64((in[9]>>44)&mask < val)<<62 |
		b2u64((in[9]>>54)&mask < val)<<63)

}
func cmp_bp_11_lt(in *[11]uint64, val uint64) uint64 {
	mask := uint64((1 << 11) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>11)&mask < val)<<1 |
		b2u64((in[0]>>22)&mask < val)<<2 |
		b2u64((in[0]>>33)&mask < val)<<3 |
		b2u64((in[0]>>44)&mask < val)<<4 |
		b2u64((in[0]>>55)&mask|
			(in[1]<<9)&mask < val)<<5 |
		b2u64((in[1]>>2)&mask < val)<<6 |
		b2u64((in[1]>>13)&mask < val)<<7 |
		b2u64((in[1]>>24)&mask < val)<<8 |
		b2u64((in[1]>>35)&mask < val)<<9 |
		b2u64((in[1]>>46)&mask < val)<<10 |
		b2u64((in[1]>>57)&mask|
			(in[2]<<7)&mask < val)<<11 |
		b2u64((in[2]>>4)&mask < val)<<12 |
		b2u64((in[2]>>15)&mask < val)<<13 |
		b2u64((in[2]>>26)&mask < val)<<14 |
		b2u64((in[2]>>37)&mask < val)<<15 |
		b2u64((in[2]>>48)&mask < val)<<16 |
		b2u64((in[2]>>59)&mask|
			(in[3]<<5)&mask < val)<<17 |
		b2u64((in[3]>>6)&mask < val)<<18 |
		b2u64((in[3]>>17)&mask < val)<<19 |
		b2u64((in[3]>>28)&mask < val)<<20 |
		b2u64((in[3]>>39)&mask < val)<<21 |
		b2u64((in[3]>>50)&mask < val)<<22 |
		b2u64((in[3]>>61)&mask|
			(in[4]<<3)&mask < val)<<23 |
		b2u64((in[4]>>8)&mask < val)<<24 |
		b2u64((in[4]>>19)&mask < val)<<25 |
		b2u64((in[4]>>30)&mask < val)<<26 |
		b2u64((in[4]>>41)&mask < val)<<27 |
		b2u64((in[4]>>52)&mask < val)<<28 |
		b2u64((in[4]>>63)&mask|
			(in[5]<<1)&mask < val)<<29 |
		b2u64((in[5]>>10)&mask < val)<<30 |
		b2u64((in[5]>>21)&mask < val)<<31 |
		b2u64((in[5]>>32)&mask < val)<<32 |
		b2u64((in[5]>>43)&mask < val)<<33 |
		b2u64((in[5]>>54)&mask|
			(in[6]<<10)&mask < val)<<34 |
		b2u64((in[6]>>1)&mask < val)<<35 |
		b2u64((in[6]>>12)&mask < val)<<36 |
		b2u64((in[6]>>23)&mask < val)<<37 |
		b2u64((in[6]>>34)&mask < val)<<38 |
		b2u64((in[6]>>45)&mask < val)<<39 |
		b2u64((in[6]>>56)&mask|
			(in[7]<<8)&mask < val)<<40 |
		b2u64((in[7]>>3)&mask < val)<<41 |
		b2u64((in[7]>>14)&mask < val)<<42 |
		b2u64((in[7]>>25)&mask < val)<<43 |
		b2u64((in[7]>>36)&mask < val)<<44 |
		b2u64((in[7]>>47)&mask < val)<<45 |
		b2u64((in[7]>>58)&mask|
			(in[8]<<6)&mask < val)<<46 |
		b2u64((in[8]>>5)&mask < val)<<47 |
		b2u64((in[8]>>16)&mask < val)<<48 |
		b2u64((in[8]>>27)&mask < val)<<49 |
		b2u64((in[8]>>38)&mask < val)<<50 |
		b2u64((in[8]>>49)&mask < val)<<51 |
		b2u64((in[8]>>60)&mask|
			(in[9]<<4)&mask < val)<<52 |
		b2u64((in[9]>>7)&mask < val)<<53 |
		b2u64((in[9]>>18)&mask < val)<<54 |
		b2u64((in[9]>>29)&mask < val)<<55 |
		b2u64((in[9]>>40)&mask < val)<<56 |
		b2u64((in[9]>>51)&mask < val)<<57 |
		b2u64((in[9]>>62)&mask|
			(in[10]<<2)&mask < val)<<58 |
		b2u64((in[10]>>9)&mask < val)<<59 |
		b2u64((in[10]>>20)&mask < val)<<60 |
		b2u64((in[10]>>31)&mask < val)<<61 |
		b2u64((in[10]>>42)&mask < val)<<62 |
		b2u64((in[10]>>53)&mask < val)<<63)

}
func cmp_bp_12_lt(in *[12]uint64, val uint64) uint64 {
	mask := uint64((1 << 12) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>12)&mask < val)<<1 |
		b2u64((in[0]>>24)&mask < val)<<2 |
		b2u64((in[0]>>36)&mask < val)<<3 |
		b2u64((in[0]>>48)&mask < val)<<4 |
		b2u64((in[0]>>60)&mask|
			(in[1]<<4)&mask < val)<<5 |
		b2u64((in[1]>>8)&mask < val)<<6 |
		b2u64((in[1]>>20)&mask < val)<<7 |
		b2u64((in[1]>>32)&mask < val)<<8 |
		b2u64((in[1]>>44)&mask < val)<<9 |
		b2u64((in[1]>>56)&mask|
			(in[2]<<8)&mask < val)<<10 |
		b2u64((in[2]>>4)&mask < val)<<11 |
		b2u64((in[2]>>16)&mask < val)<<12 |
		b2u64((in[2]>>28)&mask < val)<<13 |
		b2u64((in[2]>>40)&mask < val)<<14 |
		b2u64((in[2]>>52)&mask < val)<<15 |
		b2u64((in[3]>>0)&mask < val)<<16 |
		b2u64((in[3]>>12)&mask < val)<<17 |
		b2u64((in[3]>>24)&mask < val)<<18 |
		b2u64((in[3]>>36)&mask < val)<<19 |
		b2u64((in[3]>>48)&mask < val)<<20 |
		b2u64((in[3]>>60)&mask|
			(in[4]<<4)&mask < val)<<21 |
		b2u64((in[4]>>8)&mask < val)<<22 |
		b2u64((in[4]>>20)&mask < val)<<23 |
		b2u64((in[4]>>32)&mask < val)<<24 |
		b2u64((in[4]>>44)&mask < val)<<25 |
		b2u64((in[4]>>56)&mask|
			(in[5]<<8)&mask < val)<<26 |
		b2u64((in[5]>>4)&mask < val)<<27 |
		b2u64((in[5]>>16)&mask < val)<<28 |
		b2u64((in[5]>>28)&mask < val)<<29 |
		b2u64((in[5]>>40)&mask < val)<<30 |
		b2u64((in[5]>>52)&mask < val)<<31 |
		b2u64((in[6]>>0)&mask < val)<<32 |
		b2u64((in[6]>>12)&mask < val)<<33 |
		b2u64((in[6]>>24)&mask < val)<<34 |
		b2u64((in[6]>>36)&mask < val)<<35 |
		b2u64((in[6]>>48)&mask < val)<<36 |
		b2u64((in[6]>>60)&mask|
			(in[7]<<4)&mask < val)<<37 |
		b2u64((in[7]>>8)&mask < val)<<38 |
		b2u64((in[7]>>20)&mask < val)<<39 |
		b2u64((in[7]>>32)&mask < val)<<40 |
		b2u64((in[7]>>44)&mask < val)<<41 |
		b2u64((in[7]>>56)&mask|
			(in[8]<<8)&mask < val)<<42 |
		b2u64((in[8]>>4)&mask < val)<<43 |
		b2u64((in[8]>>16)&mask < val)<<44 |
		b2u64((in[8]>>28)&mask < val)<<45 |
		b2u64((in[8]>>40)&mask < val)<<46 |
		b2u64((in[8]>>52)&mask < val)<<47 |
		b2u64((in[9]>>0)&mask < val)<<48 |
		b2u64((in[9]>>12)&mask < val)<<49 |
		b2u64((in[9]>>24)&mask < val)<<50 |
		b2u64((in[9]>>36)&mask < val)<<51 |
		b2u64((in[9]>>48)&mask < val)<<52 |
		b2u64((in[9]>>60)&mask|
			(in[10]<<4)&mask < val)<<53 |
		b2u64((in[10]>>8)&mask < val)<<54 |
		b2u64((in[10]>>20)&mask < val)<<55 |
		b2u64((in[10]>>32)&mask < val)<<56 |
		b2u64((in[10]>>44)&mask < val)<<57 |
		b2u64((in[10]>>56)&mask|
			(in[11]<<8)&mask < val)<<58 |
		b2u64((in[11]>>4)&mask < val)<<59 |
		b2u64((in[11]>>16)&mask < val)<<60 |
		b2u64((in[11]>>28)&mask < val)<<61 |
		b2u64((in[11]>>40)&mask < val)<<62 |
		b2u64((in[11]>>52)&mask < val)<<63)

}
func cmp_bp_13_lt(in *[13]uint64, val uint64) uint64 {
	mask := uint64((1 << 13) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>13)&mask < val)<<1 |
		b2u64((in[0]>>26)&mask < val)<<2 |
		b2u64((in[0]>>39)&mask < val)<<3 |
		b2u64((in[0]>>52)&mask|
			(in[1]<<12)&mask < val)<<4 |
		b2u64((in[1]>>1)&mask < val)<<5 |
		b2u64((in[1]>>14)&mask < val)<<6 |
		b2u64((in[1]>>27)&mask < val)<<7 |
		b2u64((in[1]>>40)&mask < val)<<8 |
		b2u64((in[1]>>53)&mask|
			(in[2]<<11)&mask < val)<<9 |
		b2u64((in[2]>>2)&mask < val)<<10 |
		b2u64((in[2]>>15)&mask < val)<<11 |
		b2u64((in[2]>>28)&mask < val)<<12 |
		b2u64((in[2]>>41)&mask < val)<<13 |
		b2u64((in[2]>>54)&mask|
			(in[3]<<10)&mask < val)<<14 |
		b2u64((in[3]>>3)&mask < val)<<15 |
		b2u64((in[3]>>16)&mask < val)<<16 |
		b2u64((in[3]>>29)&mask < val)<<17 |
		b2u64((in[3]>>42)&mask < val)<<18 |
		b2u64((in[3]>>55)&mask|
			(in[4]<<9)&mask < val)<<19 |
		b2u64((in[4]>>4)&mask < val)<<20 |
		b2u64((in[4]>>17)&mask < val)<<21 |
		b2u64((in[4]>>30)&mask < val)<<22 |
		b2u64((in[4]>>43)&mask < val)<<23 |
		b2u64((in[4]>>56)&mask|
			(in[5]<<8)&mask < val)<<24 |
		b2u64((in[5]>>5)&mask < val)<<25 |
		b2u64((in[5]>>18)&mask < val)<<26 |
		b2u64((in[5]>>31)&mask < val)<<27 |
		b2u64((in[5]>>44)&mask < val)<<28 |
		b2u64((in[5]>>57)&mask|
			(in[6]<<7)&mask < val)<<29 |
		b2u64((in[6]>>6)&mask < val)<<30 |
		b2u64((in[6]>>19)&mask < val)<<31 |
		b2u64((in[6]>>32)&mask < val)<<32 |
		b2u64((in[6]>>45)&mask < val)<<33 |
		b2u64((in[6]>>58)&mask|
			(in[7]<<6)&mask < val)<<34 |
		b2u64((in[7]>>7)&mask < val)<<35 |
		b2u64((in[7]>>20)&mask < val)<<36 |
		b2u64((in[7]>>33)&mask < val)<<37 |
		b2u64((in[7]>>46)&mask < val)<<38 |
		b2u64((in[7]>>59)&mask|
			(in[8]<<5)&mask < val)<<39 |
		b2u64((in[8]>>8)&mask < val)<<40 |
		b2u64((in[8]>>21)&mask < val)<<41 |
		b2u64((in[8]>>34)&mask < val)<<42 |
		b2u64((in[8]>>47)&mask < val)<<43 |
		b2u64((in[8]>>60)&mask|
			(in[9]<<4)&mask < val)<<44 |
		b2u64((in[9]>>9)&mask < val)<<45 |
		b2u64((in[9]>>22)&mask < val)<<46 |
		b2u64((in[9]>>35)&mask < val)<<47 |
		b2u64((in[9]>>48)&mask < val)<<48 |
		b2u64((in[9]>>61)&mask|
			(in[10]<<3)&mask < val)<<49 |
		b2u64((in[10]>>10)&mask < val)<<50 |
		b2u64((in[10]>>23)&mask < val)<<51 |
		b2u64((in[10]>>36)&mask < val)<<52 |
		b2u64((in[10]>>49)&mask < val)<<53 |
		b2u64((in[10]>>62)&mask|
			(in[11]<<2)&mask < val)<<54 |
		b2u64((in[11]>>11)&mask < val)<<55 |
		b2u64((in[11]>>24)&mask < val)<<56 |
		b2u64((in[11]>>37)&mask < val)<<57 |
		b2u64((in[11]>>50)&mask < val)<<58 |
		b2u64((in[11]>>63)&mask|
			(in[12]<<1)&mask < val)<<59 |
		b2u64((in[12]>>12)&mask < val)<<60 |
		b2u64((in[12]>>25)&mask < val)<<61 |
		b2u64((in[12]>>38)&mask < val)<<62 |
		b2u64((in[12]>>51)&mask < val)<<63)

}
func cmp_bp_14_lt(in *[14]uint64, val uint64) uint64 {
	mask := uint64((1 << 14) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>14)&mask < val)<<1 |
		b2u64((in[0]>>28)&mask < val)<<2 |
		b2u64((in[0]>>42)&mask < val)<<3 |
		b2u64((in[0]>>56)&mask|
			(in[1]<<8)&mask < val)<<4 |
		b2u64((in[1]>>6)&mask < val)<<5 |
		b2u64((in[1]>>20)&mask < val)<<6 |
		b2u64((in[1]>>34)&mask < val)<<7 |
		b2u64((in[1]>>48)&mask < val)<<8 |
		b2u64((in[1]>>62)&mask|
			(in[2]<<2)&mask < val)<<9 |
		b2u64((in[2]>>12)&mask < val)<<10 |
		b2u64((in[2]>>26)&mask < val)<<11 |
		b2u64((in[2]>>40)&mask < val)<<12 |
		b2u64((in[2]>>54)&mask|
			(in[3]<<10)&mask < val)<<13 |
		b2u64((in[3]>>4)&mask < val)<<14 |
		b2u64((in[3]>>18)&mask < val)<<15 |
		b2u64((in[3]>>32)&mask < val)<<16 |
		b2u64((in[3]>>46)&mask < val)<<17 |
		b2u64((in[3]>>60)&mask|
			(in[4]<<4)&mask < val)<<18 |
		b2u64((in[4]>>10)&mask < val)<<19 |
		b2u64((in[4]>>24)&mask < val)<<20 |
		b2u64((in[4]>>38)&mask < val)<<21 |
		b2u64((in[4]>>52)&mask|
			(in[5]<<12)&mask < val)<<22 |
		b2u64((in[5]>>2)&mask < val)<<23 |
		b2u64((in[5]>>16)&mask < val)<<24 |
		b2u64((in[5]>>30)&mask < val)<<25 |
		b2u64((in[5]>>44)&mask < val)<<26 |
		b2u64((in[5]>>58)&mask|
			(in[6]<<6)&mask < val)<<27 |
		b2u64((in[6]>>8)&mask < val)<<28 |
		b2u64((in[6]>>22)&mask < val)<<29 |
		b2u64((in[6]>>36)&mask < val)<<30 |
		b2u64((in[6]>>50)&mask < val)<<31 |
		b2u64((in[7]>>0)&mask < val)<<32 |
		b2u64((in[7]>>14)&mask < val)<<33 |
		b2u64((in[7]>>28)&mask < val)<<34 |
		b2u64((in[7]>>42)&mask < val)<<35 |
		b2u64((in[7]>>56)&mask|
			(in[8]<<8)&mask < val)<<36 |
		b2u64((in[8]>>6)&mask < val)<<37 |
		b2u64((in[8]>>20)&mask < val)<<38 |
		b2u64((in[8]>>34)&mask < val)<<39 |
		b2u64((in[8]>>48)&mask < val)<<40 |
		b2u64((in[8]>>62)&mask|
			(in[9]<<2)&mask < val)<<41 |
		b2u64((in[9]>>12)&mask < val)<<42 |
		b2u64((in[9]>>26)&mask < val)<<43 |
		b2u64((in[9]>>40)&mask < val)<<44 |
		b2u64((in[9]>>54)&mask|
			(in[10]<<10)&mask < val)<<45 |
		b2u64((in[10]>>4)&mask < val)<<46 |
		b2u64((in[10]>>18)&mask < val)<<47 |
		b2u64((in[10]>>32)&mask < val)<<48 |
		b2u64((in[10]>>46)&mask < val)<<49 |
		b2u64((in[10]>>60)&mask|
			(in[11]<<4)&mask < val)<<50 |
		b2u64((in[11]>>10)&mask < val)<<51 |
		b2u64((in[11]>>24)&mask < val)<<52 |
		b2u64((in[11]>>38)&mask < val)<<53 |
		b2u64((in[11]>>52)&mask|
			(in[12]<<12)&mask < val)<<54 |
		b2u64((in[12]>>2)&mask < val)<<55 |
		b2u64((in[12]>>16)&mask < val)<<56 |
		b2u64((in[12]>>30)&mask < val)<<57 |
		b2u64((in[12]>>44)&mask < val)<<58 |
		b2u64((in[12]>>58)&mask|
			(in[13]<<6)&mask < val)<<59 |
		b2u64((in[13]>>8)&mask < val)<<60 |
		b2u64((in[13]>>22)&mask < val)<<61 |
		b2u64((in[13]>>36)&mask < val)<<62 |
		b2u64((in[13]>>50)&mask < val)<<63)

}
func cmp_bp_15_lt(in *[15]uint64, val uint64) uint64 {
	mask := uint64((1 << 15) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>15)&mask < val)<<1 |
		b2u64((in[0]>>30)&mask < val)<<2 |
		b2u64((in[0]>>45)&mask < val)<<3 |
		b2u64((in[0]>>60)&mask|
			(in[1]<<4)&mask < val)<<4 |
		b2u64((in[1]>>11)&mask < val)<<5 |
		b2u64((in[1]>>26)&mask < val)<<6 |
		b2u64((in[1]>>41)&mask < val)<<7 |
		b2u64((in[1]>>56)&mask|
			(in[2]<<8)&mask < val)<<8 |
		b2u64((in[2]>>7)&mask < val)<<9 |
		b2u64((in[2]>>22)&mask < val)<<10 |
		b2u64((in[2]>>37)&mask < val)<<11 |
		b2u64((in[2]>>52)&mask|
			(in[3]<<12)&mask < val)<<12 |
		b2u64((in[3]>>3)&mask < val)<<13 |
		b2u64((in[3]>>18)&mask < val)<<14 |
		b2u64((in[3]>>33)&mask < val)<<15 |
		b2u64((in[3]>>48)&mask < val)<<16 |
		b2u64((in[3]>>63)&mask|
			(in[4]<<1)&mask < val)<<17 |
		b2u64((in[4]>>14)&mask < val)<<18 |
		b2u64((in[4]>>29)&mask < val)<<19 |
		b2u64((in[4]>>44)&mask < val)<<20 |
		b2u64((in[4]>>59)&mask|
			(in[5]<<5)&mask < val)<<21 |
		b2u64((in[5]>>10)&mask < val)<<22 |
		b2u64((in[5]>>25)&mask < val)<<23 |
		b2u64((in[5]>>40)&mask < val)<<24 |
		b2u64((in[5]>>55)&mask|
			(in[6]<<9)&mask < val)<<25 |
		b2u64((in[6]>>6)&mask < val)<<26 |
		b2u64((in[6]>>21)&mask < val)<<27 |
		b2u64((in[6]>>36)&mask < val)<<28 |
		b2u64((in[6]>>51)&mask|
			(in[7]<<13)&mask < val)<<29 |
		b2u64((in[7]>>2)&mask < val)<<30 |
		b2u64((in[7]>>17)&mask < val)<<31 |
		b2u64((in[7]>>32)&mask < val)<<32 |
		b2u64((in[7]>>47)&mask < val)<<33 |
		b2u64((in[7]>>62)&mask|
			(in[8]<<2)&mask < val)<<34 |
		b2u64((in[8]>>13)&mask < val)<<35 |
		b2u64((in[8]>>28)&mask < val)<<36 |
		b2u64((in[8]>>43)&mask < val)<<37 |
		b2u64((in[8]>>58)&mask|
			(in[9]<<6)&mask < val)<<38 |
		b2u64((in[9]>>9)&mask < val)<<39 |
		b2u64((in[9]>>24)&mask < val)<<40 |
		b2u64((in[9]>>39)&mask < val)<<41 |
		b2u64((in[9]>>54)&mask|
			(in[10]<<10)&mask < val)<<42 |
		b2u64((in[10]>>5)&mask < val)<<43 |
		b2u64((in[10]>>20)&mask < val)<<44 |
		b2u64((in[10]>>35)&mask < val)<<45 |
		b2u64((in[10]>>50)&mask|
			(in[11]<<14)&mask < val)<<46 |
		b2u64((in[11]>>1)&mask < val)<<47 |
		b2u64((in[11]>>16)&mask < val)<<48 |
		b2u64((in[11]>>31)&mask < val)<<49 |
		b2u64((in[11]>>46)&mask < val)<<50 |
		b2u64((in[11]>>61)&mask|
			(in[12]<<3)&mask < val)<<51 |
		b2u64((in[12]>>12)&mask < val)<<52 |
		b2u64((in[12]>>27)&mask < val)<<53 |
		b2u64((in[12]>>42)&mask < val)<<54 |
		b2u64((in[12]>>57)&mask|
			(in[13]<<7)&mask < val)<<55 |
		b2u64((in[13]>>8)&mask < val)<<56 |
		b2u64((in[13]>>23)&mask < val)<<57 |
		b2u64((in[13]>>38)&mask < val)<<58 |
		b2u64((in[13]>>53)&mask|
			(in[14]<<11)&mask < val)<<59 |
		b2u64((in[14]>>4)&mask < val)<<60 |
		b2u64((in[14]>>19)&mask < val)<<61 |
		b2u64((in[14]>>34)&mask < val)<<62 |
		b2u64((in[14]>>49)&mask < val)<<63)

}
func cmp_bp_16_lt(in *[16]uint64, val uint64) uint64 {
	mask := uint64((1 << 16) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>16)&mask < val)<<1 |
		b2u64((in[0]>>32)&mask < val)<<2 |
		b2u64((in[0]>>48)&mask < val)<<3 |
		b2u64((in[1]>>0)&mask < val)<<4 |
		b2u64((in[1]>>16)&mask < val)<<5 |
		b2u64((in[1]>>32)&mask < val)<<6 |
		b2u64((in[1]>>48)&mask < val)<<7 |
		b2u64((in[2]>>0)&mask < val)<<8 |
		b2u64((in[2]>>16)&mask < val)<<9 |
		b2u64((in[2]>>32)&mask < val)<<10 |
		b2u64((in[2]>>48)&mask < val)<<11 |
		b2u64((in[3]>>0)&mask < val)<<12 |
		b2u64((in[3]>>16)&mask < val)<<13 |
		b2u64((in[3]>>32)&mask < val)<<14 |
		b2u64((in[3]>>48)&mask < val)<<15 |
		b2u64((in[4]>>0)&mask < val)<<16 |
		b2u64((in[4]>>16)&mask < val)<<17 |
		b2u64((in[4]>>32)&mask < val)<<18 |
		b2u64((in[4]>>48)&mask < val)<<19 |
		b2u64((in[5]>>0)&mask < val)<<20 |
		b2u64((in[5]>>16)&mask < val)<<21 |
		b2u64((in[5]>>32)&mask < val)<<22 |
		b2u64((in[5]>>48)&mask < val)<<23 |
		b2u64((in[6]>>0)&mask < val)<<24 |
		b2u64((in[6]>>16)&mask < val)<<25 |
		b2u64((in[6]>>32)&mask < val)<<26 |
		b2u64((in[6]>>48)&mask < val)<<27 |
		b2u64((in[7]>>0)&mask < val)<<28 |
		b2u64((in[7]>>16)&mask < val)<<29 |
		b2u64((in[7]>>32)&mask < val)<<30 |
		b2u64((in[7]>>48)&mask < val)<<31 |
		b2u64((in[8]>>0)&mask < val)<<32 |
		b2u64((in[8]>>16)&mask < val)<<33 |
		b2u64((in[8]>>32)&mask < val)<<34 |
		b2u64((in[8]>>48)&mask < val)<<35 |
		b2u64((in[9]>>0)&mask < val)<<36 |
		b2u64((in[9]>>16)&mask < val)<<37 |
		b2u64((in[9]>>32)&mask < val)<<38 |
		b2u64((in[9]>>48)&mask < val)<<39 |
		b2u64((in[10]>>0)&mask < val)<<40 |
		b2u64((in[10]>>16)&mask < val)<<41 |
		b2u64((in[10]>>32)&mask < val)<<42 |
		b2u64((in[10]>>48)&mask < val)<<43 |
		b2u64((in[11]>>0)&mask < val)<<44 |
		b2u64((in[11]>>16)&mask < val)<<45 |
		b2u64((in[11]>>32)&mask < val)<<46 |
		b2u64((in[11]>>48)&mask < val)<<47 |
		b2u64((in[12]>>0)&mask < val)<<48 |
		b2u64((in[12]>>16)&mask < val)<<49 |
		b2u64((in[12]>>32)&mask < val)<<50 |
		b2u64((in[12]>>48)&mask < val)<<51 |
		b2u64((in[13]>>0)&mask < val)<<52 |
		b2u64((in[13]>>16)&mask < val)<<53 |
		b2u64((in[13]>>32)&mask < val)<<54 |
		b2u64((in[13]>>48)&mask < val)<<55 |
		b2u64((in[14]>>0)&mask < val)<<56 |
		b2u64((in[14]>>16)&mask < val)<<57 |
		b2u64((in[14]>>32)&mask < val)<<58 |
		b2u64((in[14]>>48)&mask < val)<<59 |
		b2u64((in[15]>>0)&mask < val)<<60 |
		b2u64((in[15]>>16)&mask < val)<<61 |
		b2u64((in[15]>>32)&mask < val)<<62 |
		b2u64((in[15]>>48)&mask < val)<<63)

}
func cmp_bp_17_lt(in *[17]uint64, val uint64) uint64 {
	mask := uint64((1 << 17) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>17)&mask < val)<<1 |
		b2u64((in[0]>>34)&mask < val)<<2 |
		b2u64((in[0]>>51)&mask|
			(in[1]<<13)&mask < val)<<3 |
		b2u64((in[1]>>4)&mask < val)<<4 |
		b2u64((in[1]>>21)&mask < val)<<5 |
		b2u64((in[1]>>38)&mask < val)<<6 |
		b2u64((in[1]>>55)&mask|
			(in[2]<<9)&mask < val)<<7 |
		b2u64((in[2]>>8)&mask < val)<<8 |
		b2u64((in[2]>>25)&mask < val)<<9 |
		b2u64((in[2]>>42)&mask < val)<<10 |
		b2u64((in[2]>>59)&mask|
			(in[3]<<5)&mask < val)<<11 |
		b2u64((in[3]>>12)&mask < val)<<12 |
		b2u64((in[3]>>29)&mask < val)<<13 |
		b2u64((in[3]>>46)&mask < val)<<14 |
		b2u64((in[3]>>63)&mask|
			(in[4]<<1)&mask < val)<<15 |
		b2u64((in[4]>>16)&mask < val)<<16 |
		b2u64((in[4]>>33)&mask < val)<<17 |
		b2u64((in[4]>>50)&mask|
			(in[5]<<14)&mask < val)<<18 |
		b2u64((in[5]>>3)&mask < val)<<19 |
		b2u64((in[5]>>20)&mask < val)<<20 |
		b2u64((in[5]>>37)&mask < val)<<21 |
		b2u64((in[5]>>54)&mask|
			(in[6]<<10)&mask < val)<<22 |
		b2u64((in[6]>>7)&mask < val)<<23 |
		b2u64((in[6]>>24)&mask < val)<<24 |
		b2u64((in[6]>>41)&mask < val)<<25 |
		b2u64((in[6]>>58)&mask|
			(in[7]<<6)&mask < val)<<26 |
		b2u64((in[7]>>11)&mask < val)<<27 |
		b2u64((in[7]>>28)&mask < val)<<28 |
		b2u64((in[7]>>45)&mask < val)<<29 |
		b2u64((in[7]>>62)&mask|
			(in[8]<<2)&mask < val)<<30 |
		b2u64((in[8]>>15)&mask < val)<<31 |
		b2u64((in[8]>>32)&mask < val)<<32 |
		b2u64((in[8]>>49)&mask|
			(in[9]<<15)&mask < val)<<33 |
		b2u64((in[9]>>2)&mask < val)<<34 |
		b2u64((in[9]>>19)&mask < val)<<35 |
		b2u64((in[9]>>36)&mask < val)<<36 |
		b2u64((in[9]>>53)&mask|
			(in[10]<<11)&mask < val)<<37 |
		b2u64((in[10]>>6)&mask < val)<<38 |
		b2u64((in[10]>>23)&mask < val)<<39 |
		b2u64((in[10]>>40)&mask < val)<<40 |
		b2u64((in[10]>>57)&mask|
			(in[11]<<7)&mask < val)<<41 |
		b2u64((in[11]>>10)&mask < val)<<42 |
		b2u64((in[11]>>27)&mask < val)<<43 |
		b2u64((in[11]>>44)&mask < val)<<44 |
		b2u64((in[11]>>61)&mask|
			(in[12]<<3)&mask < val)<<45 |
		b2u64((in[12]>>14)&mask < val)<<46 |
		b2u64((in[12]>>31)&mask < val)<<47 |
		b2u64((in[12]>>48)&mask|
			(in[13]<<16)&mask < val)<<48 |
		b2u64((in[13]>>1)&mask < val)<<49 |
		b2u64((in[13]>>18)&mask < val)<<50 |
		b2u64((in[13]>>35)&mask < val)<<51 |
		b2u64((in[13]>>52)&mask|
			(in[14]<<12)&mask < val)<<52 |
		b2u64((in[14]>>5)&mask < val)<<53 |
		b2u64((in[14]>>22)&mask < val)<<54 |
		b2u64((in[14]>>39)&mask < val)<<55 |
		b2u64((in[14]>>56)&mask|
			(in[15]<<8)&mask < val)<<56 |
		b2u64((in[15]>>9)&mask < val)<<57 |
		b2u64((in[15]>>26)&mask < val)<<58 |
		b2u64((in[15]>>43)&mask < val)<<59 |
		b2u64((in[15]>>60)&mask|
			(in[16]<<4)&mask < val)<<60 |
		b2u64((in[16]>>13)&mask < val)<<61 |
		b2u64((in[16]>>30)&mask < val)<<62 |
		b2u64((in[16]>>47)&mask < val)<<63)

}
func cmp_bp_18_lt(in *[18]uint64, val uint64) uint64 {
	mask := uint64((1 << 18) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>18)&mask < val)<<1 |
		b2u64((in[0]>>36)&mask < val)<<2 |
		b2u64((in[0]>>54)&mask|
			(in[1]<<10)&mask < val)<<3 |
		b2u64((in[1]>>8)&mask < val)<<4 |
		b2u64((in[1]>>26)&mask < val)<<5 |
		b2u64((in[1]>>44)&mask < val)<<6 |
		b2u64((in[1]>>62)&mask|
			(in[2]<<2)&mask < val)<<7 |
		b2u64((in[2]>>16)&mask < val)<<8 |
		b2u64((in[2]>>34)&mask < val)<<9 |
		b2u64((in[2]>>52)&mask|
			(in[3]<<12)&mask < val)<<10 |
		b2u64((in[3]>>6)&mask < val)<<11 |
		b2u64((in[3]>>24)&mask < val)<<12 |
		b2u64((in[3]>>42)&mask < val)<<13 |
		b2u64((in[3]>>60)&mask|
			(in[4]<<4)&mask < val)<<14 |
		b2u64((in[4]>>14)&mask < val)<<15 |
		b2u64((in[4]>>32)&mask < val)<<16 |
		b2u64((in[4]>>50)&mask|
			(in[5]<<14)&mask < val)<<17 |
		b2u64((in[5]>>4)&mask < val)<<18 |
		b2u64((in[5]>>22)&mask < val)<<19 |
		b2u64((in[5]>>40)&mask < val)<<20 |
		b2u64((in[5]>>58)&mask|
			(in[6]<<6)&mask < val)<<21 |
		b2u64((in[6]>>12)&mask < val)<<22 |
		b2u64((in[6]>>30)&mask < val)<<23 |
		b2u64((in[6]>>48)&mask|
			(in[7]<<16)&mask < val)<<24 |
		b2u64((in[7]>>2)&mask < val)<<25 |
		b2u64((in[7]>>20)&mask < val)<<26 |
		b2u64((in[7]>>38)&mask < val)<<27 |
		b2u64((in[7]>>56)&mask|
			(in[8]<<8)&mask < val)<<28 |
		b2u64((in[8]>>10)&mask < val)<<29 |
		b2u64((in[8]>>28)&mask < val)<<30 |
		b2u64((in[8]>>46)&mask < val)<<31 |
		b2u64((in[9]>>0)&mask < val)<<32 |
		b2u64((in[9]>>18)&mask < val)<<33 |
		b2u64((in[9]>>36)&mask < val)<<34 |
		b2u64((in[9]>>54)&mask|
			(in[10]<<10)&mask < val)<<35 |
		b2u64((in[10]>>8)&mask < val)<<36 |
		b2u64((in[10]>>26)&mask < val)<<37 |
		b2u64((in[10]>>44)&mask < val)<<38 |
		b2u64((in[10]>>62)&mask|
			(in[11]<<2)&mask < val)<<39 |
		b2u64((in[11]>>16)&mask < val)<<40 |
		b2u64((in[11]>>34)&mask < val)<<41 |
		b2u64((in[11]>>52)&mask|
			(in[12]<<12)&mask < val)<<42 |
		b2u64((in[12]>>6)&mask < val)<<43 |
		b2u64((in[12]>>24)&mask < val)<<44 |
		b2u64((in[12]>>42)&mask < val)<<45 |
		b2u64((in[12]>>60)&mask|
			(in[13]<<4)&mask < val)<<46 |
		b2u64((in[13]>>14)&mask < val)<<47 |
		b2u64((in[13]>>32)&mask < val)<<48 |
		b2u64((in[13]>>50)&mask|
			(in[14]<<14)&mask < val)<<49 |
		b2u64((in[14]>>4)&mask < val)<<50 |
		b2u64((in[14]>>22)&mask < val)<<51 |
		b2u64((in[14]>>40)&mask < val)<<52 |
		b2u64((in[14]>>58)&mask|
			(in[15]<<6)&mask < val)<<53 |
		b2u64((in[15]>>12)&mask < val)<<54 |
		b2u64((in[15]>>30)&mask < val)<<55 |
		b2u64((in[15]>>48)&mask|
			(in[16]<<16)&mask < val)<<56 |
		b2u64((in[16]>>2)&mask < val)<<57 |
		b2u64((in[16]>>20)&mask < val)<<58 |
		b2u64((in[16]>>38)&mask < val)<<59 |
		b2u64((in[16]>>56)&mask|
			(in[17]<<8)&mask < val)<<60 |
		b2u64((in[17]>>10)&mask < val)<<61 |
		b2u64((in[17]>>28)&mask < val)<<62 |
		b2u64((in[17]>>46)&mask < val)<<63)

}
func cmp_bp_19_lt(in *[19]uint64, val uint64) uint64 {
	mask := uint64((1 << 19) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>19)&mask < val)<<1 |
		b2u64((in[0]>>38)&mask < val)<<2 |
		b2u64((in[0]>>57)&mask|
			(in[1]<<7)&mask < val)<<3 |
		b2u64((in[1]>>12)&mask < val)<<4 |
		b2u64((in[1]>>31)&mask < val)<<5 |
		b2u64((in[1]>>50)&mask|
			(in[2]<<14)&mask < val)<<6 |
		b2u64((in[2]>>5)&mask < val)<<7 |
		b2u64((in[2]>>24)&mask < val)<<8 |
		b2u64((in[2]>>43)&mask < val)<<9 |
		b2u64((in[2]>>62)&mask|
			(in[3]<<2)&mask < val)<<10 |
		b2u64((in[3]>>17)&mask < val)<<11 |
		b2u64((in[3]>>36)&mask < val)<<12 |
		b2u64((in[3]>>55)&mask|
			(in[4]<<9)&mask < val)<<13 |
		b2u64((in[4]>>10)&mask < val)<<14 |
		b2u64((in[4]>>29)&mask < val)<<15 |
		b2u64((in[4]>>48)&mask|
			(in[5]<<16)&mask < val)<<16 |
		b2u64((in[5]>>3)&mask < val)<<17 |
		b2u64((in[5]>>22)&mask < val)<<18 |
		b2u64((in[5]>>41)&mask < val)<<19 |
		b2u64((in[5]>>60)&mask|
			(in[6]<<4)&mask < val)<<20 |
		b2u64((in[6]>>15)&mask < val)<<21 |
		b2u64((in[6]>>34)&mask < val)<<22 |
		b2u64((in[6]>>53)&mask|
			(in[7]<<11)&mask < val)<<23 |
		b2u64((in[7]>>8)&mask < val)<<24 |
		b2u64((in[7]>>27)&mask < val)<<25 |
		b2u64((in[7]>>46)&mask|
			(in[8]<<18)&mask < val)<<26 |
		b2u64((in[8]>>1)&mask < val)<<27 |
		b2u64((in[8]>>20)&mask < val)<<28 |
		b2u64((in[8]>>39)&mask < val)<<29 |
		b2u64((in[8]>>58)&mask|
			(in[9]<<6)&mask < val)<<30 |
		b2u64((in[9]>>13)&mask < val)<<31 |
		b2u64((in[9]>>32)&mask < val)<<32 |
		b2u64((in[9]>>51)&mask|
			(in[10]<<13)&mask < val)<<33 |
		b2u64((in[10]>>6)&mask < val)<<34 |
		b2u64((in[10]>>25)&mask < val)<<35 |
		b2u64((in[10]>>44)&mask < val)<<36 |
		b2u64((in[10]>>63)&mask|
			(in[11]<<1)&mask < val)<<37 |
		b2u64((in[11]>>18)&mask < val)<<38 |
		b2u64((in[11]>>37)&mask < val)<<39 |
		b2u64((in[11]>>56)&mask|
			(in[12]<<8)&mask < val)<<40 |
		b2u64((in[12]>>11)&mask < val)<<41 |
		b2u64((in[12]>>30)&mask < val)<<42 |
		b2u64((in[12]>>49)&mask|
			(in[13]<<15)&mask < val)<<43 |
		b2u64((in[13]>>4)&mask < val)<<44 |
		b2u64((in[13]>>23)&mask < val)<<45 |
		b2u64((in[13]>>42)&mask < val)<<46 |
		b2u64((in[13]>>61)&mask|
			(in[14]<<3)&mask < val)<<47 |
		b2u64((in[14]>>16)&mask < val)<<48 |
		b2u64((in[14]>>35)&mask < val)<<49 |
		b2u64((in[14]>>54)&mask|
			(in[15]<<10)&mask < val)<<50 |
		b2u64((in[15]>>9)&mask < val)<<51 |
		b2u64((in[15]>>28)&mask < val)<<52 |
		b2u64((in[15]>>47)&mask|
			(in[16]<<17)&mask < val)<<53 |
		b2u64((in[16]>>2)&mask < val)<<54 |
		b2u64((in[16]>>21)&mask < val)<<55 |
		b2u64((in[16]>>40)&mask < val)<<56 |
		b2u64((in[16]>>59)&mask|
			(in[17]<<5)&mask < val)<<57 |
		b2u64((in[17]>>14)&mask < val)<<58 |
		b2u64((in[17]>>33)&mask < val)<<59 |
		b2u64((in[17]>>52)&mask|
			(in[18]<<12)&mask < val)<<60 |
		b2u64((in[18]>>7)&mask < val)<<61 |
		b2u64((in[18]>>26)&mask < val)<<62 |
		b2u64((in[18]>>45)&mask < val)<<63)

}
func cmp_bp_20_lt(in *[20]uint64, val uint64) uint64 {
	mask := uint64((1 << 20) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>20)&mask < val)<<1 |
		b2u64((in[0]>>40)&mask < val)<<2 |
		b2u64((in[0]>>60)&mask|
			(in[1]<<4)&mask < val)<<3 |
		b2u64((in[1]>>16)&mask < val)<<4 |
		b2u64((in[1]>>36)&mask < val)<<5 |
		b2u64((in[1]>>56)&mask|
			(in[2]<<8)&mask < val)<<6 |
		b2u64((in[2]>>12)&mask < val)<<7 |
		b2u64((in[2]>>32)&mask < val)<<8 |
		b2u64((in[2]>>52)&mask|
			(in[3]<<12)&mask < val)<<9 |
		b2u64((in[3]>>8)&mask < val)<<10 |
		b2u64((in[3]>>28)&mask < val)<<11 |
		b2u64((in[3]>>48)&mask|
			(in[4]<<16)&mask < val)<<12 |
		b2u64((in[4]>>4)&mask < val)<<13 |
		b2u64((in[4]>>24)&mask < val)<<14 |
		b2u64((in[4]>>44)&mask < val)<<15 |
		b2u64((in[5]>>0)&mask < val)<<16 |
		b2u64((in[5]>>20)&mask < val)<<17 |
		b2u64((in[5]>>40)&mask < val)<<18 |
		b2u64((in[5]>>60)&mask|
			(in[6]<<4)&mask < val)<<19 |
		b2u64((in[6]>>16)&mask < val)<<20 |
		b2u64((in[6]>>36)&mask < val)<<21 |
		b2u64((in[6]>>56)&mask|
			(in[7]<<8)&mask < val)<<22 |
		b2u64((in[7]>>12)&mask < val)<<23 |
		b2u64((in[7]>>32)&mask < val)<<24 |
		b2u64((in[7]>>52)&mask|
			(in[8]<<12)&mask < val)<<25 |
		b2u64((in[8]>>8)&mask < val)<<26 |
		b2u64((in[8]>>28)&mask < val)<<27 |
		b2u64((in[8]>>48)&mask|
			(in[9]<<16)&mask < val)<<28 |
		b2u64((in[9]>>4)&mask < val)<<29 |
		b2u64((in[9]>>24)&mask < val)<<30 |
		b2u64((in[9]>>44)&mask < val)<<31 |
		b2u64((in[10]>>0)&mask < val)<<32 |
		b2u64((in[10]>>20)&mask < val)<<33 |
		b2u64((in[10]>>40)&mask < val)<<34 |
		b2u64((in[10]>>60)&mask|
			(in[11]<<4)&mask < val)<<35 |
		b2u64((in[11]>>16)&mask < val)<<36 |
		b2u64((in[11]>>36)&mask < val)<<37 |
		b2u64((in[11]>>56)&mask|
			(in[12]<<8)&mask < val)<<38 |
		b2u64((in[12]>>12)&mask < val)<<39 |
		b2u64((in[12]>>32)&mask < val)<<40 |
		b2u64((in[12]>>52)&mask|
			(in[13]<<12)&mask < val)<<41 |
		b2u64((in[13]>>8)&mask < val)<<42 |
		b2u64((in[13]>>28)&mask < val)<<43 |
		b2u64((in[13]>>48)&mask|
			(in[14]<<16)&mask < val)<<44 |
		b2u64((in[14]>>4)&mask < val)<<45 |
		b2u64((in[14]>>24)&mask < val)<<46 |
		b2u64((in[14]>>44)&mask < val)<<47 |
		b2u64((in[15]>>0)&mask < val)<<48 |
		b2u64((in[15]>>20)&mask < val)<<49 |
		b2u64((in[15]>>40)&mask < val)<<50 |
		b2u64((in[15]>>60)&mask|
			(in[16]<<4)&mask < val)<<51 |
		b2u64((in[16]>>16)&mask < val)<<52 |
		b2u64((in[16]>>36)&mask < val)<<53 |
		b2u64((in[16]>>56)&mask|
			(in[17]<<8)&mask < val)<<54 |
		b2u64((in[17]>>12)&mask < val)<<55 |
		b2u64((in[17]>>32)&mask < val)<<56 |
		b2u64((in[17]>>52)&mask|
			(in[18]<<12)&mask < val)<<57 |
		b2u64((in[18]>>8)&mask < val)<<58 |
		b2u64((in[18]>>28)&mask < val)<<59 |
		b2u64((in[18]>>48)&mask|
			(in[19]<<16)&mask < val)<<60 |
		b2u64((in[19]>>4)&mask < val)<<61 |
		b2u64((in[19]>>24)&mask < val)<<62 |
		b2u64((in[19]>>44)&mask < val)<<63)

}
func cmp_bp_21_lt(in *[21]uint64, val uint64) uint64 {
	mask := uint64((1 << 21) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>21)&mask < val)<<1 |
		b2u64((in[0]>>42)&mask < val)<<2 |
		b2u64((in[0]>>63)&mask|
			(in[1]<<1)&mask < val)<<3 |
		b2u64((in[1]>>20)&mask < val)<<4 |
		b2u64((in[1]>>41)&mask < val)<<5 |
		b2u64((in[1]>>62)&mask|
			(in[2]<<2)&mask < val)<<6 |
		b2u64((in[2]>>19)&mask < val)<<7 |
		b2u64((in[2]>>40)&mask < val)<<8 |
		b2u64((in[2]>>61)&mask|
			(in[3]<<3)&mask < val)<<9 |
		b2u64((in[3]>>18)&mask < val)<<10 |
		b2u64((in[3]>>39)&mask < val)<<11 |
		b2u64((in[3]>>60)&mask|
			(in[4]<<4)&mask < val)<<12 |
		b2u64((in[4]>>17)&mask < val)<<13 |
		b2u64((in[4]>>38)&mask < val)<<14 |
		b2u64((in[4]>>59)&mask|
			(in[5]<<5)&mask < val)<<15 |
		b2u64((in[5]>>16)&mask < val)<<16 |
		b2u64((in[5]>>37)&mask < val)<<17 |
		b2u64((in[5]>>58)&mask|
			(in[6]<<6)&mask < val)<<18 |
		b2u64((in[6]>>15)&mask < val)<<19 |
		b2u64((in[6]>>36)&mask < val)<<20 |
		b2u64((in[6]>>57)&mask|
			(in[7]<<7)&mask < val)<<21 |
		b2u64((in[7]>>14)&mask < val)<<22 |
		b2u64((in[7]>>35)&mask < val)<<23 |
		b2u64((in[7]>>56)&mask|
			(in[8]<<8)&mask < val)<<24 |
		b2u64((in[8]>>13)&mask < val)<<25 |
		b2u64((in[8]>>34)&mask < val)<<26 |
		b2u64((in[8]>>55)&mask|
			(in[9]<<9)&mask < val)<<27 |
		b2u64((in[9]>>12)&mask < val)<<28 |
		b2u64((in[9]>>33)&mask < val)<<29 |
		b2u64((in[9]>>54)&mask|
			(in[10]<<10)&mask < val)<<30 |
		b2u64((in[10]>>11)&mask < val)<<31 |
		b2u64((in[10]>>32)&mask < val)<<32 |
		b2u64((in[10]>>53)&mask|
			(in[11]<<11)&mask < val)<<33 |
		b2u64((in[11]>>10)&mask < val)<<34 |
		b2u64((in[11]>>31)&mask < val)<<35 |
		b2u64((in[11]>>52)&mask|
			(in[12]<<12)&mask < val)<<36 |
		b2u64((in[12]>>9)&mask < val)<<37 |
		b2u64((in[12]>>30)&mask < val)<<38 |
		b2u64((in[12]>>51)&mask|
			(in[13]<<13)&mask < val)<<39 |
		b2u64((in[13]>>8)&mask < val)<<40 |
		b2u64((in[13]>>29)&mask < val)<<41 |
		b2u64((in[13]>>50)&mask|
			(in[14]<<14)&mask < val)<<42 |
		b2u64((in[14]>>7)&mask < val)<<43 |
		b2u64((in[14]>>28)&mask < val)<<44 |
		b2u64((in[14]>>49)&mask|
			(in[15]<<15)&mask < val)<<45 |
		b2u64((in[15]>>6)&mask < val)<<46 |
		b2u64((in[15]>>27)&mask < val)<<47 |
		b2u64((in[15]>>48)&mask|
			(in[16]<<16)&mask < val)<<48 |
		b2u64((in[16]>>5)&mask < val)<<49 |
		b2u64((in[16]>>26)&mask < val)<<50 |
		b2u64((in[16]>>47)&mask|
			(in[17]<<17)&mask < val)<<51 |
		b2u64((in[17]>>4)&mask < val)<<52 |
		b2u64((in[17]>>25)&mask < val)<<53 |
		b2u64((in[17]>>46)&mask|
			(in[18]<<18)&mask < val)<<54 |
		b2u64((in[18]>>3)&mask < val)<<55 |
		b2u64((in[18]>>24)&mask < val)<<56 |
		b2u64((in[18]>>45)&mask|
			(in[19]<<19)&mask < val)<<57 |
		b2u64((in[19]>>2)&mask < val)<<58 |
		b2u64((in[19]>>23)&mask < val)<<59 |
		b2u64((in[19]>>44)&mask|
			(in[20]<<20)&mask < val)<<60 |
		b2u64((in[20]>>1)&mask < val)<<61 |
		b2u64((in[20]>>22)&mask < val)<<62 |
		b2u64((in[20]>>43)&mask < val)<<63)

}
func cmp_bp_22_lt(in *[22]uint64, val uint64) uint64 {
	mask := uint64((1 << 22) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>22)&mask < val)<<1 |
		b2u64((in[0]>>44)&mask|
			(in[1]<<20)&mask < val)<<2 |
		b2u64((in[1]>>2)&mask < val)<<3 |
		b2u64((in[1]>>24)&mask < val)<<4 |
		b2u64((in[1]>>46)&mask|
			(in[2]<<18)&mask < val)<<5 |
		b2u64((in[2]>>4)&mask < val)<<6 |
		b2u64((in[2]>>26)&mask < val)<<7 |
		b2u64((in[2]>>48)&mask|
			(in[3]<<16)&mask < val)<<8 |
		b2u64((in[3]>>6)&mask < val)<<9 |
		b2u64((in[3]>>28)&mask < val)<<10 |
		b2u64((in[3]>>50)&mask|
			(in[4]<<14)&mask < val)<<11 |
		b2u64((in[4]>>8)&mask < val)<<12 |
		b2u64((in[4]>>30)&mask < val)<<13 |
		b2u64((in[4]>>52)&mask|
			(in[5]<<12)&mask < val)<<14 |
		b2u64((in[5]>>10)&mask < val)<<15 |
		b2u64((in[5]>>32)&mask < val)<<16 |
		b2u64((in[5]>>54)&mask|
			(in[6]<<10)&mask < val)<<17 |
		b2u64((in[6]>>12)&mask < val)<<18 |
		b2u64((in[6]>>34)&mask < val)<<19 |
		b2u64((in[6]>>56)&mask|
			(in[7]<<8)&mask < val)<<20 |
		b2u64((in[7]>>14)&mask < val)<<21 |
		b2u64((in[7]>>36)&mask < val)<<22 |
		b2u64((in[7]>>58)&mask|
			(in[8]<<6)&mask < val)<<23 |
		b2u64((in[8]>>16)&mask < val)<<24 |
		b2u64((in[8]>>38)&mask < val)<<25 |
		b2u64((in[8]>>60)&mask|
			(in[9]<<4)&mask < val)<<26 |
		b2u64((in[9]>>18)&mask < val)<<27 |
		b2u64((in[9]>>40)&mask < val)<<28 |
		b2u64((in[9]>>62)&mask|
			(in[10]<<2)&mask < val)<<29 |
		b2u64((in[10]>>20)&mask < val)<<30 |
		b2u64((in[10]>>42)&mask < val)<<31 |
		b2u64((in[11]>>0)&mask < val)<<32 |
		b2u64((in[11]>>22)&mask < val)<<33 |
		b2u64((in[11]>>44)&mask|
			(in[12]<<20)&mask < val)<<34 |
		b2u64((in[12]>>2)&mask < val)<<35 |
		b2u64((in[12]>>24)&mask < val)<<36 |
		b2u64((in[12]>>46)&mask|
			(in[13]<<18)&mask < val)<<37 |
		b2u64((in[13]>>4)&mask < val)<<38 |
		b2u64((in[13]>>26)&mask < val)<<39 |
		b2u64((in[13]>>48)&mask|
			(in[14]<<16)&mask < val)<<40 |
		b2u64((in[14]>>6)&mask < val)<<41 |
		b2u64((in[14]>>28)&mask < val)<<42 |
		b2u64((in[14]>>50)&mask|
			(in[15]<<14)&mask < val)<<43 |
		b2u64((in[15]>>8)&mask < val)<<44 |
		b2u64((in[15]>>30)&mask < val)<<45 |
		b2u64((in[15]>>52)&mask|
			(in[16]<<12)&mask < val)<<46 |
		b2u64((in[16]>>10)&mask < val)<<47 |
		b2u64((in[16]>>32)&mask < val)<<48 |
		b2u64((in[16]>>54)&mask|
			(in[17]<<10)&mask < val)<<49 |
		b2u64((in[17]>>12)&mask < val)<<50 |
		b2u64((in[17]>>34)&mask < val)<<51 |
		b2u64((in[17]>>56)&mask|
			(in[18]<<8)&mask < val)<<52 |
		b2u64((in[18]>>14)&mask < val)<<53 |
		b2u64((in[18]>>36)&mask < val)<<54 |
		b2u64((in[18]>>58)&mask|
			(in[19]<<6)&mask < val)<<55 |
		b2u64((in[19]>>16)&mask < val)<<56 |
		b2u64((in[19]>>38)&mask < val)<<57 |
		b2u64((in[19]>>60)&mask|
			(in[20]<<4)&mask < val)<<58 |
		b2u64((in[20]>>18)&mask < val)<<59 |
		b2u64((in[20]>>40)&mask < val)<<60 |
		b2u64((in[20]>>62)&mask|
			(in[21]<<2)&mask < val)<<61 |
		b2u64((in[21]>>20)&mask < val)<<62 |
		b2u64((in[21]>>42)&mask < val)<<63)

}
func cmp_bp_23_lt(in *[23]uint64, val uint64) uint64 {
	mask := uint64((1 << 23) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>23)&mask < val)<<1 |
		b2u64((in[0]>>46)&mask|
			(in[1]<<18)&mask < val)<<2 |
		b2u64((in[1]>>5)&mask < val)<<3 |
		b2u64((in[1]>>28)&mask < val)<<4 |
		b2u64((in[1]>>51)&mask|
			(in[2]<<13)&mask < val)<<5 |
		b2u64((in[2]>>10)&mask < val)<<6 |
		b2u64((in[2]>>33)&mask < val)<<7 |
		b2u64((in[2]>>56)&mask|
			(in[3]<<8)&mask < val)<<8 |
		b2u64((in[3]>>15)&mask < val)<<9 |
		b2u64((in[3]>>38)&mask < val)<<10 |
		b2u64((in[3]>>61)&mask|
			(in[4]<<3)&mask < val)<<11 |
		b2u64((in[4]>>20)&mask < val)<<12 |
		b2u64((in[4]>>43)&mask|
			(in[5]<<21)&mask < val)<<13 |
		b2u64((in[5]>>2)&mask < val)<<14 |
		b2u64((in[5]>>25)&mask < val)<<15 |
		b2u64((in[5]>>48)&mask|
			(in[6]<<16)&mask < val)<<16 |
		b2u64((in[6]>>7)&mask < val)<<17 |
		b2u64((in[6]>>30)&mask < val)<<18 |
		b2u64((in[6]>>53)&mask|
			(in[7]<<11)&mask < val)<<19 |
		b2u64((in[7]>>12)&mask < val)<<20 |
		b2u64((in[7]>>35)&mask < val)<<21 |
		b2u64((in[7]>>58)&mask|
			(in[8]<<6)&mask < val)<<22 |
		b2u64((in[8]>>17)&mask < val)<<23 |
		b2u64((in[8]>>40)&mask < val)<<24 |
		b2u64((in[8]>>63)&mask|
			(in[9]<<1)&mask < val)<<25 |
		b2u64((in[9]>>22)&mask < val)<<26 |
		b2u64((in[9]>>45)&mask|
			(in[10]<<19)&mask < val)<<27 |
		b2u64((in[10]>>4)&mask < val)<<28 |
		b2u64((in[10]>>27)&mask < val)<<29 |
		b2u64((in[10]>>50)&mask|
			(in[11]<<14)&mask < val)<<30 |
		b2u64((in[11]>>9)&mask < val)<<31 |
		b2u64((in[11]>>32)&mask < val)<<32 |
		b2u64((in[11]>>55)&mask|
			(in[12]<<9)&mask < val)<<33 |
		b2u64((in[12]>>14)&mask < val)<<34 |
		b2u64((in[12]>>37)&mask < val)<<35 |
		b2u64((in[12]>>60)&mask|
			(in[13]<<4)&mask < val)<<36 |
		b2u64((in[13]>>19)&mask < val)<<37 |
		b2u64((in[13]>>42)&mask|
			(in[14]<<22)&mask < val)<<38 |
		b2u64((in[14]>>1)&mask < val)<<39 |
		b2u64((in[14]>>24)&mask < val)<<40 |
		b2u64((in[14]>>47)&mask|
			(in[15]<<17)&mask < val)<<41 |
		b2u64((in[15]>>6)&mask < val)<<42 |
		b2u64((in[15]>>29)&mask < val)<<43 |
		b2u64((in[15]>>52)&mask|
			(in[16]<<12)&mask < val)<<44 |
		b2u64((in[16]>>11)&mask < val)<<45 |
		b2u64((in[16]>>34)&mask < val)<<46 |
		b2u64((in[16]>>57)&mask|
			(in[17]<<7)&mask < val)<<47 |
		b2u64((in[17]>>16)&mask < val)<<48 |
		b2u64((in[17]>>39)&mask < val)<<49 |
		b2u64((in[17]>>62)&mask|
			(in[18]<<2)&mask < val)<<50 |
		b2u64((in[18]>>21)&mask < val)<<51 |
		b2u64((in[18]>>44)&mask|
			(in[19]<<20)&mask < val)<<52 |
		b2u64((in[19]>>3)&mask < val)<<53 |
		b2u64((in[19]>>26)&mask < val)<<54 |
		b2u64((in[19]>>49)&mask|
			(in[20]<<15)&mask < val)<<55 |
		b2u64((in[20]>>8)&mask < val)<<56 |
		b2u64((in[20]>>31)&mask < val)<<57 |
		b2u64((in[20]>>54)&mask|
			(in[21]<<10)&mask < val)<<58 |
		b2u64((in[21]>>13)&mask < val)<<59 |
		b2u64((in[21]>>36)&mask < val)<<60 |
		b2u64((in[21]>>59)&mask|
			(in[22]<<5)&mask < val)<<61 |
		b2u64((in[22]>>18)&mask < val)<<62 |
		b2u64((in[22]>>41)&mask < val)<<63)

}
func cmp_bp_24_lt(in *[24]uint64, val uint64) uint64 {
	mask := uint64((1 << 24) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>24)&mask < val)<<1 |
		b2u64((in[0]>>48)&mask|
			(in[1]<<16)&mask < val)<<2 |
		b2u64((in[1]>>8)&mask < val)<<3 |
		b2u64((in[1]>>32)&mask < val)<<4 |
		b2u64((in[1]>>56)&mask|
			(in[2]<<8)&mask < val)<<5 |
		b2u64((in[2]>>16)&mask < val)<<6 |
		b2u64((in[2]>>40)&mask < val)<<7 |
		b2u64((in[3]>>0)&mask < val)<<8 |
		b2u64((in[3]>>24)&mask < val)<<9 |
		b2u64((in[3]>>48)&mask|
			(in[4]<<16)&mask < val)<<10 |
		b2u64((in[4]>>8)&mask < val)<<11 |
		b2u64((in[4]>>32)&mask < val)<<12 |
		b2u64((in[4]>>56)&mask|
			(in[5]<<8)&mask < val)<<13 |
		b2u64((in[5]>>16)&mask < val)<<14 |
		b2u64((in[5]>>40)&mask < val)<<15 |
		b2u64((in[6]>>0)&mask < val)<<16 |
		b2u64((in[6]>>24)&mask < val)<<17 |
		b2u64((in[6]>>48)&mask|
			(in[7]<<16)&mask < val)<<18 |
		b2u64((in[7]>>8)&mask < val)<<19 |
		b2u64((in[7]>>32)&mask < val)<<20 |
		b2u64((in[7]>>56)&mask|
			(in[8]<<8)&mask < val)<<21 |
		b2u64((in[8]>>16)&mask < val)<<22 |
		b2u64((in[8]>>40)&mask < val)<<23 |
		b2u64((in[9]>>0)&mask < val)<<24 |
		b2u64((in[9]>>24)&mask < val)<<25 |
		b2u64((in[9]>>48)&mask|
			(in[10]<<16)&mask < val)<<26 |
		b2u64((in[10]>>8)&mask < val)<<27 |
		b2u64((in[10]>>32)&mask < val)<<28 |
		b2u64((in[10]>>56)&mask|
			(in[11]<<8)&mask < val)<<29 |
		b2u64((in[11]>>16)&mask < val)<<30 |
		b2u64((in[11]>>40)&mask < val)<<31 |
		b2u64((in[12]>>0)&mask < val)<<32 |
		b2u64((in[12]>>24)&mask < val)<<33 |
		b2u64((in[12]>>48)&mask|
			(in[13]<<16)&mask < val)<<34 |
		b2u64((in[13]>>8)&mask < val)<<35 |
		b2u64((in[13]>>32)&mask < val)<<36 |
		b2u64((in[13]>>56)&mask|
			(in[14]<<8)&mask < val)<<37 |
		b2u64((in[14]>>16)&mask < val)<<38 |
		b2u64((in[14]>>40)&mask < val)<<39 |
		b2u64((in[15]>>0)&mask < val)<<40 |
		b2u64((in[15]>>24)&mask < val)<<41 |
		b2u64((in[15]>>48)&mask|
			(in[16]<<16)&mask < val)<<42 |
		b2u64((in[16]>>8)&mask < val)<<43 |
		b2u64((in[16]>>32)&mask < val)<<44 |
		b2u64((in[16]>>56)&mask|
			(in[17]<<8)&mask < val)<<45 |
		b2u64((in[17]>>16)&mask < val)<<46 |
		b2u64((in[17]>>40)&mask < val)<<47 |
		b2u64((in[18]>>0)&mask < val)<<48 |
		b2u64((in[18]>>24)&mask < val)<<49 |
		b2u64((in[18]>>48)&mask|
			(in[19]<<16)&mask < val)<<50 |
		b2u64((in[19]>>8)&mask < val)<<51 |
		b2u64((in[19]>>32)&mask < val)<<52 |
		b2u64((in[19]>>56)&mask|
			(in[20]<<8)&mask < val)<<53 |
		b2u64((in[20]>>16)&mask < val)<<54 |
		b2u64((in[20]>>40)&mask < val)<<55 |
		b2u64((in[21]>>0)&mask < val)<<56 |
		b2u64((in[21]>>24)&mask < val)<<57 |
		b2u64((in[21]>>48)&mask|
			(in[22]<<16)&mask < val)<<58 |
		b2u64((in[22]>>8)&mask < val)<<59 |
		b2u64((in[22]>>32)&mask < val)<<60 |
		b2u64((in[22]>>56)&mask|
			(in[23]<<8)&mask < val)<<61 |
		b2u64((in[23]>>16)&mask < val)<<62 |
		b2u64((in[23]>>40)&mask < val)<<63)

}
func cmp_bp_25_lt(in *[25]uint64, val uint64) uint64 {
	mask := uint64((1 << 25) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>25)&mask < val)<<1 |
		b2u64((in[0]>>50)&mask|
			(in[1]<<14)&mask < val)<<2 |
		b2u64((in[1]>>11)&mask < val)<<3 |
		b2u64((in[1]>>36)&mask < val)<<4 |
		b2u64((in[1]>>61)&mask|
			(in[2]<<3)&mask < val)<<5 |
		b2u64((in[2]>>22)&mask < val)<<6 |
		b2u64((in[2]>>47)&mask|
			(in[3]<<17)&mask < val)<<7 |
		b2u64((in[3]>>8)&mask < val)<<8 |
		b2u64((in[3]>>33)&mask < val)<<9 |
		b2u64((in[3]>>58)&mask|
			(in[4]<<6)&mask < val)<<10 |
		b2u64((in[4]>>19)&mask < val)<<11 |
		b2u64((in[4]>>44)&mask|
			(in[5]<<20)&mask < val)<<12 |
		b2u64((in[5]>>5)&mask < val)<<13 |
		b2u64((in[5]>>30)&mask < val)<<14 |
		b2u64((in[5]>>55)&mask|
			(in[6]<<9)&mask < val)<<15 |
		b2u64((in[6]>>16)&mask < val)<<16 |
		b2u64((in[6]>>41)&mask|
			(in[7]<<23)&mask < val)<<17 |
		b2u64((in[7]>>2)&mask < val)<<18 |
		b2u64((in[7]>>27)&mask < val)<<19 |
		b2u64((in[7]>>52)&mask|
			(in[8]<<12)&mask < val)<<20 |
		b2u64((in[8]>>13)&mask < val)<<21 |
		b2u64((in[8]>>38)&mask < val)<<22 |
		b2u64((in[8]>>63)&mask|
			(in[9]<<1)&mask < val)<<23 |
		b2u64((in[9]>>24)&mask < val)<<24 |
		b2u64((in[9]>>49)&mask|
			(in[10]<<15)&mask < val)<<25 |
		b2u64((in[10]>>10)&mask < val)<<26 |
		b2u64((in[10]>>35)&mask < val)<<27 |
		b2u64((in[10]>>60)&mask|
			(in[11]<<4)&mask < val)<<28 |
		b2u64((in[11]>>21)&mask < val)<<29 |
		b2u64((in[11]>>46)&mask|
			(in[12]<<18)&mask < val)<<30 |
		b2u64((in[12]>>7)&mask < val)<<31 |
		b2u64((in[12]>>32)&mask < val)<<32 |
		b2u64((in[12]>>57)&mask|
			(in[13]<<7)&mask < val)<<33 |
		b2u64((in[13]>>18)&mask < val)<<34 |
		b2u64((in[13]>>43)&mask|
			(in[14]<<21)&mask < val)<<35 |
		b2u64((in[14]>>4)&mask < val)<<36 |
		b2u64((in[14]>>29)&mask < val)<<37 |
		b2u64((in[14]>>54)&mask|
			(in[15]<<10)&mask < val)<<38 |
		b2u64((in[15]>>15)&mask < val)<<39 |
		b2u64((in[15]>>40)&mask|
			(in[16]<<24)&mask < val)<<40 |
		b2u64((in[16]>>1)&mask < val)<<41 |
		b2u64((in[16]>>26)&mask < val)<<42 |
		b2u64((in[16]>>51)&mask|
			(in[17]<<13)&mask < val)<<43 |
		b2u64((in[17]>>12)&mask < val)<<44 |
		b2u64((in[17]>>37)&mask < val)<<45 |
		b2u64((in[17]>>62)&mask|
			(in[18]<<2)&mask < val)<<46 |
		b2u64((in[18]>>23)&mask < val)<<47 |
		b2u64((in[18]>>48)&mask|
			(in[19]<<16)&mask < val)<<48 |
		b2u64((in[19]>>9)&mask < val)<<49 |
		b2u64((in[19]>>34)&mask < val)<<50 |
		b2u64((in[19]>>59)&mask|
			(in[20]<<5)&mask < val)<<51 |
		b2u64((in[20]>>20)&mask < val)<<52 |
		b2u64((in[20]>>45)&mask|
			(in[21]<<19)&mask < val)<<53 |
		b2u64((in[21]>>6)&mask < val)<<54 |
		b2u64((in[21]>>31)&mask < val)<<55 |
		b2u64((in[21]>>56)&mask|
			(in[22]<<8)&mask < val)<<56 |
		b2u64((in[22]>>17)&mask < val)<<57 |
		b2u64((in[22]>>42)&mask|
			(in[23]<<22)&mask < val)<<58 |
		b2u64((in[23]>>3)&mask < val)<<59 |
		b2u64((in[23]>>28)&mask < val)<<60 |
		b2u64((in[23]>>53)&mask|
			(in[24]<<11)&mask < val)<<61 |
		b2u64((in[24]>>14)&mask < val)<<62 |
		b2u64((in[24]>>39)&mask < val)<<63)

}
func cmp_bp_26_lt(in *[26]uint64, val uint64) uint64 {
	mask := uint64((1 << 26) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>26)&mask < val)<<1 |
		b2u64((in[0]>>52)&mask|
			(in[1]<<12)&mask < val)<<2 |
		b2u64((in[1]>>14)&mask < val)<<3 |
		b2u64((in[1]>>40)&mask|
			(in[2]<<24)&mask < val)<<4 |
		b2u64((in[2]>>2)&mask < val)<<5 |
		b2u64((in[2]>>28)&mask < val)<<6 |
		b2u64((in[2]>>54)&mask|
			(in[3]<<10)&mask < val)<<7 |
		b2u64((in[3]>>16)&mask < val)<<8 |
		b2u64((in[3]>>42)&mask|
			(in[4]<<22)&mask < val)<<9 |
		b2u64((in[4]>>4)&mask < val)<<10 |
		b2u64((in[4]>>30)&mask < val)<<11 |
		b2u64((in[4]>>56)&mask|
			(in[5]<<8)&mask < val)<<12 |
		b2u64((in[5]>>18)&mask < val)<<13 |
		b2u64((in[5]>>44)&mask|
			(in[6]<<20)&mask < val)<<14 |
		b2u64((in[6]>>6)&mask < val)<<15 |
		b2u64((in[6]>>32)&mask < val)<<16 |
		b2u64((in[6]>>58)&mask|
			(in[7]<<6)&mask < val)<<17 |
		b2u64((in[7]>>20)&mask < val)<<18 |
		b2u64((in[7]>>46)&mask|
			(in[8]<<18)&mask < val)<<19 |
		b2u64((in[8]>>8)&mask < val)<<20 |
		b2u64((in[8]>>34)&mask < val)<<21 |
		b2u64((in[8]>>60)&mask|
			(in[9]<<4)&mask < val)<<22 |
		b2u64((in[9]>>22)&mask < val)<<23 |
		b2u64((in[9]>>48)&mask|
			(in[10]<<16)&mask < val)<<24 |
		b2u64((in[10]>>10)&mask < val)<<25 |
		b2u64((in[10]>>36)&mask < val)<<26 |
		b2u64((in[10]>>62)&mask|
			(in[11]<<2)&mask < val)<<27 |
		b2u64((in[11]>>24)&mask < val)<<28 |
		b2u64((in[11]>>50)&mask|
			(in[12]<<14)&mask < val)<<29 |
		b2u64((in[12]>>12)&mask < val)<<30 |
		b2u64((in[12]>>38)&mask < val)<<31 |
		b2u64((in[13]>>0)&mask < val)<<32 |
		b2u64((in[13]>>26)&mask < val)<<33 |
		b2u64((in[13]>>52)&mask|
			(in[14]<<12)&mask < val)<<34 |
		b2u64((in[14]>>14)&mask < val)<<35 |
		b2u64((in[14]>>40)&mask|
			(in[15]<<24)&mask < val)<<36 |
		b2u64((in[15]>>2)&mask < val)<<37 |
		b2u64((in[15]>>28)&mask < val)<<38 |
		b2u64((in[15]>>54)&mask|
			(in[16]<<10)&mask < val)<<39 |
		b2u64((in[16]>>16)&mask < val)<<40 |
		b2u64((in[16]>>42)&mask|
			(in[17]<<22)&mask < val)<<41 |
		b2u64((in[17]>>4)&mask < val)<<42 |
		b2u64((in[17]>>30)&mask < val)<<43 |
		b2u64((in[17]>>56)&mask|
			(in[18]<<8)&mask < val)<<44 |
		b2u64((in[18]>>18)&mask < val)<<45 |
		b2u64((in[18]>>44)&mask|
			(in[19]<<20)&mask < val)<<46 |
		b2u64((in[19]>>6)&mask < val)<<47 |
		b2u64((in[19]>>32)&mask < val)<<48 |
		b2u64((in[19]>>58)&mask|
			(in[20]<<6)&mask < val)<<49 |
		b2u64((in[20]>>20)&mask < val)<<50 |
		b2u64((in[20]>>46)&mask|
			(in[21]<<18)&mask < val)<<51 |
		b2u64((in[21]>>8)&mask < val)<<52 |
		b2u64((in[21]>>34)&mask < val)<<53 |
		b2u64((in[21]>>60)&mask|
			(in[22]<<4)&mask < val)<<54 |
		b2u64((in[22]>>22)&mask < val)<<55 |
		b2u64((in[22]>>48)&mask|
			(in[23]<<16)&mask < val)<<56 |
		b2u64((in[23]>>10)&mask < val)<<57 |
		b2u64((in[23]>>36)&mask < val)<<58 |
		b2u64((in[23]>>62)&mask|
			(in[24]<<2)&mask < val)<<59 |
		b2u64((in[24]>>24)&mask < val)<<60 |
		b2u64((in[24]>>50)&mask|
			(in[25]<<14)&mask < val)<<61 |
		b2u64((in[25]>>12)&mask < val)<<62 |
		b2u64((in[25]>>38)&mask < val)<<63)

}
func cmp_bp_27_lt(in *[27]uint64, val uint64) uint64 {
	mask := uint64((1 << 27) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>27)&mask < val)<<1 |
		b2u64((in[0]>>54)&mask|
			(in[1]<<10)&mask < val)<<2 |
		b2u64((in[1]>>17)&mask < val)<<3 |
		b2u64((in[1]>>44)&mask|
			(in[2]<<20)&mask < val)<<4 |
		b2u64((in[2]>>7)&mask < val)<<5 |
		b2u64((in[2]>>34)&mask < val)<<6 |
		b2u64((in[2]>>61)&mask|
			(in[3]<<3)&mask < val)<<7 |
		b2u64((in[3]>>24)&mask < val)<<8 |
		b2u64((in[3]>>51)&mask|
			(in[4]<<13)&mask < val)<<9 |
		b2u64((in[4]>>14)&mask < val)<<10 |
		b2u64((in[4]>>41)&mask|
			(in[5]<<23)&mask < val)<<11 |
		b2u64((in[5]>>4)&mask < val)<<12 |
		b2u64((in[5]>>31)&mask < val)<<13 |
		b2u64((in[5]>>58)&mask|
			(in[6]<<6)&mask < val)<<14 |
		b2u64((in[6]>>21)&mask < val)<<15 |
		b2u64((in[6]>>48)&mask|
			(in[7]<<16)&mask < val)<<16 |
		b2u64((in[7]>>11)&mask < val)<<17 |
		b2u64((in[7]>>38)&mask|
			(in[8]<<26)&mask < val)<<18 |
		b2u64((in[8]>>1)&mask < val)<<19 |
		b2u64((in[8]>>28)&mask < val)<<20 |
		b2u64((in[8]>>55)&mask|
			(in[9]<<9)&mask < val)<<21 |
		b2u64((in[9]>>18)&mask < val)<<22 |
		b2u64((in[9]>>45)&mask|
			(in[10]<<19)&mask < val)<<23 |
		b2u64((in[10]>>8)&mask < val)<<24 |
		b2u64((in[10]>>35)&mask < val)<<25 |
		b2u64((in[10]>>62)&mask|
			(in[11]<<2)&mask < val)<<26 |
		b2u64((in[11]>>25)&mask < val)<<27 |
		b2u64((in[11]>>52)&mask|
			(in[12]<<12)&mask < val)<<28 |
		b2u64((in[12]>>15)&mask < val)<<29 |
		b2u64((in[12]>>42)&mask|
			(in[13]<<22)&mask < val)<<30 |
		b2u64((in[13]>>5)&mask < val)<<31 |
		b2u64((in[13]>>32)&mask < val)<<32 |
		b2u64((in[13]>>59)&mask|
			(in[14]<<5)&mask < val)<<33 |
		b2u64((in[14]>>22)&mask < val)<<34 |
		b2u64((in[14]>>49)&mask|
			(in[15]<<15)&mask < val)<<35 |
		b2u64((in[15]>>12)&mask < val)<<36 |
		b2u64((in[15]>>39)&mask|
			(in[16]<<25)&mask < val)<<37 |
		b2u64((in[16]>>2)&mask < val)<<38 |
		b2u64((in[16]>>29)&mask < val)<<39 |
		b2u64((in[16]>>56)&mask|
			(in[17]<<8)&mask < val)<<40 |
		b2u64((in[17]>>19)&mask < val)<<41 |
		b2u64((in[17]>>46)&mask|
			(in[18]<<18)&mask < val)<<42 |
		b2u64((in[18]>>9)&mask < val)<<43 |
		b2u64((in[18]>>36)&mask < val)<<44 |
		b2u64((in[18]>>63)&mask|
			(in[19]<<1)&mask < val)<<45 |
		b2u64((in[19]>>26)&mask < val)<<46 |
		b2u64((in[19]>>53)&mask|
			(in[20]<<11)&mask < val)<<47 |
		b2u64((in[20]>>16)&mask < val)<<48 |
		b2u64((in[20]>>43)&mask|
			(in[21]<<21)&mask < val)<<49 |
		b2u64((in[21]>>6)&mask < val)<<50 |
		b2u64((in[21]>>33)&mask < val)<<51 |
		b2u64((in[21]>>60)&mask|
			(in[22]<<4)&mask < val)<<52 |
		b2u64((in[22]>>23)&mask < val)<<53 |
		b2u64((in[22]>>50)&mask|
			(in[23]<<14)&mask < val)<<54 |
		b2u64((in[23]>>13)&mask < val)<<55 |
		b2u64((in[23]>>40)&mask|
			(in[24]<<24)&mask < val)<<56 |
		b2u64((in[24]>>3)&mask < val)<<57 |
		b2u64((in[24]>>30)&mask < val)<<58 |
		b2u64((in[24]>>57)&mask|
			(in[25]<<7)&mask < val)<<59 |
		b2u64((in[25]>>20)&mask < val)<<60 |
		b2u64((in[25]>>47)&mask|
			(in[26]<<17)&mask < val)<<61 |
		b2u64((in[26]>>10)&mask < val)<<62 |
		b2u64((in[26]>>37)&mask < val)<<63)

}
func cmp_bp_28_lt(in *[28]uint64, val uint64) uint64 {
	mask := uint64((1 << 28) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>28)&mask < val)<<1 |
		b2u64((in[0]>>56)&mask|
			(in[1]<<8)&mask < val)<<2 |
		b2u64((in[1]>>20)&mask < val)<<3 |
		b2u64((in[1]>>48)&mask|
			(in[2]<<16)&mask < val)<<4 |
		b2u64((in[2]>>12)&mask < val)<<5 |
		b2u64((in[2]>>40)&mask|
			(in[3]<<24)&mask < val)<<6 |
		b2u64((in[3]>>4)&mask < val)<<7 |
		b2u64((in[3]>>32)&mask < val)<<8 |
		b2u64((in[3]>>60)&mask|
			(in[4]<<4)&mask < val)<<9 |
		b2u64((in[4]>>24)&mask < val)<<10 |
		b2u64((in[4]>>52)&mask|
			(in[5]<<12)&mask < val)<<11 |
		b2u64((in[5]>>16)&mask < val)<<12 |
		b2u64((in[5]>>44)&mask|
			(in[6]<<20)&mask < val)<<13 |
		b2u64((in[6]>>8)&mask < val)<<14 |
		b2u64((in[6]>>36)&mask < val)<<15 |
		b2u64((in[7]>>0)&mask < val)<<16 |
		b2u64((in[7]>>28)&mask < val)<<17 |
		b2u64((in[7]>>56)&mask|
			(in[8]<<8)&mask < val)<<18 |
		b2u64((in[8]>>20)&mask < val)<<19 |
		b2u64((in[8]>>48)&mask|
			(in[9]<<16)&mask < val)<<20 |
		b2u64((in[9]>>12)&mask < val)<<21 |
		b2u64((in[9]>>40)&mask|
			(in[10]<<24)&mask < val)<<22 |
		b2u64((in[10]>>4)&mask < val)<<23 |
		b2u64((in[10]>>32)&mask < val)<<24 |
		b2u64((in[10]>>60)&mask|
			(in[11]<<4)&mask < val)<<25 |
		b2u64((in[11]>>24)&mask < val)<<26 |
		b2u64((in[11]>>52)&mask|
			(in[12]<<12)&mask < val)<<27 |
		b2u64((in[12]>>16)&mask < val)<<28 |
		b2u64((in[12]>>44)&mask|
			(in[13]<<20)&mask < val)<<29 |
		b2u64((in[13]>>8)&mask < val)<<30 |
		b2u64((in[13]>>36)&mask < val)<<31 |
		b2u64((in[14]>>0)&mask < val)<<32 |
		b2u64((in[14]>>28)&mask < val)<<33 |
		b2u64((in[14]>>56)&mask|
			(in[15]<<8)&mask < val)<<34 |
		b2u64((in[15]>>20)&mask < val)<<35 |
		b2u64((in[15]>>48)&mask|
			(in[16]<<16)&mask < val)<<36 |
		b2u64((in[16]>>12)&mask < val)<<37 |
		b2u64((in[16]>>40)&mask|
			(in[17]<<24)&mask < val)<<38 |
		b2u64((in[17]>>4)&mask < val)<<39 |
		b2u64((in[17]>>32)&mask < val)<<40 |
		b2u64((in[17]>>60)&mask|
			(in[18]<<4)&mask < val)<<41 |
		b2u64((in[18]>>24)&mask < val)<<42 |
		b2u64((in[18]>>52)&mask|
			(in[19]<<12)&mask < val)<<43 |
		b2u64((in[19]>>16)&mask < val)<<44 |
		b2u64((in[19]>>44)&mask|
			(in[20]<<20)&mask < val)<<45 |
		b2u64((in[20]>>8)&mask < val)<<46 |
		b2u64((in[20]>>36)&mask < val)<<47 |
		b2u64((in[21]>>0)&mask < val)<<48 |
		b2u64((in[21]>>28)&mask < val)<<49 |
		b2u64((in[21]>>56)&mask|
			(in[22]<<8)&mask < val)<<50 |
		b2u64((in[22]>>20)&mask < val)<<51 |
		b2u64((in[22]>>48)&mask|
			(in[23]<<16)&mask < val)<<52 |
		b2u64((in[23]>>12)&mask < val)<<53 |
		b2u64((in[23]>>40)&mask|
			(in[24]<<24)&mask < val)<<54 |
		b2u64((in[24]>>4)&mask < val)<<55 |
		b2u64((in[24]>>32)&mask < val)<<56 |
		b2u64((in[24]>>60)&mask|
			(in[25]<<4)&mask < val)<<57 |
		b2u64((in[25]>>24)&mask < val)<<58 |
		b2u64((in[25]>>52)&mask|
			(in[26]<<12)&mask < val)<<59 |
		b2u64((in[26]>>16)&mask < val)<<60 |
		b2u64((in[26]>>44)&mask|
			(in[27]<<20)&mask < val)<<61 |
		b2u64((in[27]>>8)&mask < val)<<62 |
		b2u64((in[27]>>36)&mask < val)<<63)

}
func cmp_bp_29_lt(in *[29]uint64, val uint64) uint64 {
	mask := uint64((1 << 29) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>29)&mask < val)<<1 |
		b2u64((in[0]>>58)&mask|
			(in[1]<<6)&mask < val)<<2 |
		b2u64((in[1]>>23)&mask < val)<<3 |
		b2u64((in[1]>>52)&mask|
			(in[2]<<12)&mask < val)<<4 |
		b2u64((in[2]>>17)&mask < val)<<5 |
		b2u64((in[2]>>46)&mask|
			(in[3]<<18)&mask < val)<<6 |
		b2u64((in[3]>>11)&mask < val)<<7 |
		b2u64((in[3]>>40)&mask|
			(in[4]<<24)&mask < val)<<8 |
		b2u64((in[4]>>5)&mask < val)<<9 |
		b2u64((in[4]>>34)&mask < val)<<10 |
		b2u64((in[4]>>63)&mask|
			(in[5]<<1)&mask < val)<<11 |
		b2u64((in[5]>>28)&mask < val)<<12 |
		b2u64((in[5]>>57)&mask|
			(in[6]<<7)&mask < val)<<13 |
		b2u64((in[6]>>22)&mask < val)<<14 |
		b2u64((in[6]>>51)&mask|
			(in[7]<<13)&mask < val)<<15 |
		b2u64((in[7]>>16)&mask < val)<<16 |
		b2u64((in[7]>>45)&mask|
			(in[8]<<19)&mask < val)<<17 |
		b2u64((in[8]>>10)&mask < val)<<18 |
		b2u64((in[8]>>39)&mask|
			(in[9]<<25)&mask < val)<<19 |
		b2u64((in[9]>>4)&mask < val)<<20 |
		b2u64((in[9]>>33)&mask < val)<<21 |
		b2u64((in[9]>>62)&mask|
			(in[10]<<2)&mask < val)<<22 |
		b2u64((in[10]>>27)&mask < val)<<23 |
		b2u64((in[10]>>56)&mask|
			(in[11]<<8)&mask < val)<<24 |
		b2u64((in[11]>>21)&mask < val)<<25 |
		b2u64((in[11]>>50)&mask|
			(in[12]<<14)&mask < val)<<26 |
		b2u64((in[12]>>15)&mask < val)<<27 |
		b2u64((in[12]>>44)&mask|
			(in[13]<<20)&mask < val)<<28 |
		b2u64((in[13]>>9)&mask < val)<<29 |
		b2u64((in[13]>>38)&mask|
			(in[14]<<26)&mask < val)<<30 |
		b2u64((in[14]>>3)&mask < val)<<31 |
		b2u64((in[14]>>32)&mask < val)<<32 |
		b2u64((in[14]>>61)&mask|
			(in[15]<<3)&mask < val)<<33 |
		b2u64((in[15]>>26)&mask < val)<<34 |
		b2u64((in[15]>>55)&mask|
			(in[16]<<9)&mask < val)<<35 |
		b2u64((in[16]>>20)&mask < val)<<36 |
		b2u64((in[16]>>49)&mask|
			(in[17]<<15)&mask < val)<<37 |
		b2u64((in[17]>>14)&mask < val)<<38 |
		b2u64((in[17]>>43)&mask|
			(in[18]<<21)&mask < val)<<39 |
		b2u64((in[18]>>8)&mask < val)<<40 |
		b2u64((in[18]>>37)&mask|
			(in[19]<<27)&mask < val)<<41 |
		b2u64((in[19]>>2)&mask < val)<<42 |
		b2u64((in[19]>>31)&mask < val)<<43 |
		b2u64((in[19]>>60)&mask|
			(in[20]<<4)&mask < val)<<44 |
		b2u64((in[20]>>25)&mask < val)<<45 |
		b2u64((in[20]>>54)&mask|
			(in[21]<<10)&mask < val)<<46 |
		b2u64((in[21]>>19)&mask < val)<<47 |
		b2u64((in[21]>>48)&mask|
			(in[22]<<16)&mask < val)<<48 |
		b2u64((in[22]>>13)&mask < val)<<49 |
		b2u64((in[22]>>42)&mask|
			(in[23]<<22)&mask < val)<<50 |
		b2u64((in[23]>>7)&mask < val)<<51 |
		b2u64((in[23]>>36)&mask|
			(in[24]<<28)&mask < val)<<52 |
		b2u64((in[24]>>1)&mask < val)<<53 |
		b2u64((in[24]>>30)&mask < val)<<54 |
		b2u64((in[24]>>59)&mask|
			(in[25]<<5)&mask < val)<<55 |
		b2u64((in[25]>>24)&mask < val)<<56 |
		b2u64((in[25]>>53)&mask|
			(in[26]<<11)&mask < val)<<57 |
		b2u64((in[26]>>18)&mask < val)<<58 |
		b2u64((in[26]>>47)&mask|
			(in[27]<<17)&mask < val)<<59 |
		b2u64((in[27]>>12)&mask < val)<<60 |
		b2u64((in[27]>>41)&mask|
			(in[28]<<23)&mask < val)<<61 |
		b2u64((in[28]>>6)&mask < val)<<62 |
		b2u64((in[28]>>35)&mask < val)<<63)

}
func cmp_bp_30_lt(in *[30]uint64, val uint64) uint64 {
	mask := uint64((1 << 30) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>30)&mask < val)<<1 |
		b2u64((in[0]>>60)&mask|
			(in[1]<<4)&mask < val)<<2 |
		b2u64((in[1]>>26)&mask < val)<<3 |
		b2u64((in[1]>>56)&mask|
			(in[2]<<8)&mask < val)<<4 |
		b2u64((in[2]>>22)&mask < val)<<5 |
		b2u64((in[2]>>52)&mask|
			(in[3]<<12)&mask < val)<<6 |
		b2u64((in[3]>>18)&mask < val)<<7 |
		b2u64((in[3]>>48)&mask|
			(in[4]<<16)&mask < val)<<8 |
		b2u64((in[4]>>14)&mask < val)<<9 |
		b2u64((in[4]>>44)&mask|
			(in[5]<<20)&mask < val)<<10 |
		b2u64((in[5]>>10)&mask < val)<<11 |
		b2u64((in[5]>>40)&mask|
			(in[6]<<24)&mask < val)<<12 |
		b2u64((in[6]>>6)&mask < val)<<13 |
		b2u64((in[6]>>36)&mask|
			(in[7]<<28)&mask < val)<<14 |
		b2u64((in[7]>>2)&mask < val)<<15 |
		b2u64((in[7]>>32)&mask < val)<<16 |
		b2u64((in[7]>>62)&mask|
			(in[8]<<2)&mask < val)<<17 |
		b2u64((in[8]>>28)&mask < val)<<18 |
		b2u64((in[8]>>58)&mask|
			(in[9]<<6)&mask < val)<<19 |
		b2u64((in[9]>>24)&mask < val)<<20 |
		b2u64((in[9]>>54)&mask|
			(in[10]<<10)&mask < val)<<21 |
		b2u64((in[10]>>20)&mask < val)<<22 |
		b2u64((in[10]>>50)&mask|
			(in[11]<<14)&mask < val)<<23 |
		b2u64((in[11]>>16)&mask < val)<<24 |
		b2u64((in[11]>>46)&mask|
			(in[12]<<18)&mask < val)<<25 |
		b2u64((in[12]>>12)&mask < val)<<26 |
		b2u64((in[12]>>42)&mask|
			(in[13]<<22)&mask < val)<<27 |
		b2u64((in[13]>>8)&mask < val)<<28 |
		b2u64((in[13]>>38)&mask|
			(in[14]<<26)&mask < val)<<29 |
		b2u64((in[14]>>4)&mask < val)<<30 |
		b2u64((in[14]>>34)&mask < val)<<31 |
		b2u64((in[15]>>0)&mask < val)<<32 |
		b2u64((in[15]>>30)&mask < val)<<33 |
		b2u64((in[15]>>60)&mask|
			(in[16]<<4)&mask < val)<<34 |
		b2u64((in[16]>>26)&mask < val)<<35 |
		b2u64((in[16]>>56)&mask|
			(in[17]<<8)&mask < val)<<36 |
		b2u64((in[17]>>22)&mask < val)<<37 |
		b2u64((in[17]>>52)&mask|
			(in[18]<<12)&mask < val)<<38 |
		b2u64((in[18]>>18)&mask < val)<<39 |
		b2u64((in[18]>>48)&mask|
			(in[19]<<16)&mask < val)<<40 |
		b2u64((in[19]>>14)&mask < val)<<41 |
		b2u64((in[19]>>44)&mask|
			(in[20]<<20)&mask < val)<<42 |
		b2u64((in[20]>>10)&mask < val)<<43 |
		b2u64((in[20]>>40)&mask|
			(in[21]<<24)&mask < val)<<44 |
		b2u64((in[21]>>6)&mask < val)<<45 |
		b2u64((in[21]>>36)&mask|
			(in[22]<<28)&mask < val)<<46 |
		b2u64((in[22]>>2)&mask < val)<<47 |
		b2u64((in[22]>>32)&mask < val)<<48 |
		b2u64((in[22]>>62)&mask|
			(in[23]<<2)&mask < val)<<49 |
		b2u64((in[23]>>28)&mask < val)<<50 |
		b2u64((in[23]>>58)&mask|
			(in[24]<<6)&mask < val)<<51 |
		b2u64((in[24]>>24)&mask < val)<<52 |
		b2u64((in[24]>>54)&mask|
			(in[25]<<10)&mask < val)<<53 |
		b2u64((in[25]>>20)&mask < val)<<54 |
		b2u64((in[25]>>50)&mask|
			(in[26]<<14)&mask < val)<<55 |
		b2u64((in[26]>>16)&mask < val)<<56 |
		b2u64((in[26]>>46)&mask|
			(in[27]<<18)&mask < val)<<57 |
		b2u64((in[27]>>12)&mask < val)<<58 |
		b2u64((in[27]>>42)&mask|
			(in[28]<<22)&mask < val)<<59 |
		b2u64((in[28]>>8)&mask < val)<<60 |
		b2u64((in[28]>>38)&mask|
			(in[29]<<26)&mask < val)<<61 |
		b2u64((in[29]>>4)&mask < val)<<62 |
		b2u64((in[29]>>34)&mask < val)<<63)

}
func cmp_bp_31_lt(in *[31]uint64, val uint64) uint64 {
	mask := uint64((1 << 31) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>31)&mask < val)<<1 |
		b2u64((in[0]>>62)&mask|
			(in[1]<<2)&mask < val)<<2 |
		b2u64((in[1]>>29)&mask < val)<<3 |
		b2u64((in[1]>>60)&mask|
			(in[2]<<4)&mask < val)<<4 |
		b2u64((in[2]>>27)&mask < val)<<5 |
		b2u64((in[2]>>58)&mask|
			(in[3]<<6)&mask < val)<<6 |
		b2u64((in[3]>>25)&mask < val)<<7 |
		b2u64((in[3]>>56)&mask|
			(in[4]<<8)&mask < val)<<8 |
		b2u64((in[4]>>23)&mask < val)<<9 |
		b2u64((in[4]>>54)&mask|
			(in[5]<<10)&mask < val)<<10 |
		b2u64((in[5]>>21)&mask < val)<<11 |
		b2u64((in[5]>>52)&mask|
			(in[6]<<12)&mask < val)<<12 |
		b2u64((in[6]>>19)&mask < val)<<13 |
		b2u64((in[6]>>50)&mask|
			(in[7]<<14)&mask < val)<<14 |
		b2u64((in[7]>>17)&mask < val)<<15 |
		b2u64((in[7]>>48)&mask|
			(in[8]<<16)&mask < val)<<16 |
		b2u64((in[8]>>15)&mask < val)<<17 |
		b2u64((in[8]>>46)&mask|
			(in[9]<<18)&mask < val)<<18 |
		b2u64((in[9]>>13)&mask < val)<<19 |
		b2u64((in[9]>>44)&mask|
			(in[10]<<20)&mask < val)<<20 |
		b2u64((in[10]>>11)&mask < val)<<21 |
		b2u64((in[10]>>42)&mask|
			(in[11]<<22)&mask < val)<<22 |
		b2u64((in[11]>>9)&mask < val)<<23 |
		b2u64((in[11]>>40)&mask|
			(in[12]<<24)&mask < val)<<24 |
		b2u64((in[12]>>7)&mask < val)<<25 |
		b2u64((in[12]>>38)&mask|
			(in[13]<<26)&mask < val)<<26 |
		b2u64((in[13]>>5)&mask < val)<<27 |
		b2u64((in[13]>>36)&mask|
			(in[14]<<28)&mask < val)<<28 |
		b2u64((in[14]>>3)&mask < val)<<29 |
		b2u64((in[14]>>34)&mask|
			(in[15]<<30)&mask < val)<<30 |
		b2u64((in[15]>>1)&mask < val)<<31 |
		b2u64((in[15]>>32)&mask < val)<<32 |
		b2u64((in[15]>>63)&mask|
			(in[16]<<1)&mask < val)<<33 |
		b2u64((in[16]>>30)&mask < val)<<34 |
		b2u64((in[16]>>61)&mask|
			(in[17]<<3)&mask < val)<<35 |
		b2u64((in[17]>>28)&mask < val)<<36 |
		b2u64((in[17]>>59)&mask|
			(in[18]<<5)&mask < val)<<37 |
		b2u64((in[18]>>26)&mask < val)<<38 |
		b2u64((in[18]>>57)&mask|
			(in[19]<<7)&mask < val)<<39 |
		b2u64((in[19]>>24)&mask < val)<<40 |
		b2u64((in[19]>>55)&mask|
			(in[20]<<9)&mask < val)<<41 |
		b2u64((in[20]>>22)&mask < val)<<42 |
		b2u64((in[20]>>53)&mask|
			(in[21]<<11)&mask < val)<<43 |
		b2u64((in[21]>>20)&mask < val)<<44 |
		b2u64((in[21]>>51)&mask|
			(in[22]<<13)&mask < val)<<45 |
		b2u64((in[22]>>18)&mask < val)<<46 |
		b2u64((in[22]>>49)&mask|
			(in[23]<<15)&mask < val)<<47 |
		b2u64((in[23]>>16)&mask < val)<<48 |
		b2u64((in[23]>>47)&mask|
			(in[24]<<17)&mask < val)<<49 |
		b2u64((in[24]>>14)&mask < val)<<50 |
		b2u64((in[24]>>45)&mask|
			(in[25]<<19)&mask < val)<<51 |
		b2u64((in[25]>>12)&mask < val)<<52 |
		b2u64((in[25]>>43)&mask|
			(in[26]<<21)&mask < val)<<53 |
		b2u64((in[26]>>10)&mask < val)<<54 |
		b2u64((in[26]>>41)&mask|
			(in[27]<<23)&mask < val)<<55 |
		b2u64((in[27]>>8)&mask < val)<<56 |
		b2u64((in[27]>>39)&mask|
			(in[28]<<25)&mask < val)<<57 |
		b2u64((in[28]>>6)&mask < val)<<58 |
		b2u64((in[28]>>37)&mask|
			(in[29]<<27)&mask < val)<<59 |
		b2u64((in[29]>>4)&mask < val)<<60 |
		b2u64((in[29]>>35)&mask|
			(in[30]<<29)&mask < val)<<61 |
		b2u64((in[30]>>2)&mask < val)<<62 |
		b2u64((in[30]>>33)&mask < val)<<63)

}
func cmp_bp_32_lt(in *[32]uint64, val uint64) uint64 {
	mask := uint64((1 << 32) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>32)&mask < val)<<1 |
		b2u64((in[1]>>0)&mask < val)<<2 |
		b2u64((in[1]>>32)&mask < val)<<3 |
		b2u64((in[2]>>0)&mask < val)<<4 |
		b2u64((in[2]>>32)&mask < val)<<5 |
		b2u64((in[3]>>0)&mask < val)<<6 |
		b2u64((in[3]>>32)&mask < val)<<7 |
		b2u64((in[4]>>0)&mask < val)<<8 |
		b2u64((in[4]>>32)&mask < val)<<9 |
		b2u64((in[5]>>0)&mask < val)<<10 |
		b2u64((in[5]>>32)&mask < val)<<11 |
		b2u64((in[6]>>0)&mask < val)<<12 |
		b2u64((in[6]>>32)&mask < val)<<13 |
		b2u64((in[7]>>0)&mask < val)<<14 |
		b2u64((in[7]>>32)&mask < val)<<15 |
		b2u64((in[8]>>0)&mask < val)<<16 |
		b2u64((in[8]>>32)&mask < val)<<17 |
		b2u64((in[9]>>0)&mask < val)<<18 |
		b2u64((in[9]>>32)&mask < val)<<19 |
		b2u64((in[10]>>0)&mask < val)<<20 |
		b2u64((in[10]>>32)&mask < val)<<21 |
		b2u64((in[11]>>0)&mask < val)<<22 |
		b2u64((in[11]>>32)&mask < val)<<23 |
		b2u64((in[12]>>0)&mask < val)<<24 |
		b2u64((in[12]>>32)&mask < val)<<25 |
		b2u64((in[13]>>0)&mask < val)<<26 |
		b2u64((in[13]>>32)&mask < val)<<27 |
		b2u64((in[14]>>0)&mask < val)<<28 |
		b2u64((in[14]>>32)&mask < val)<<29 |
		b2u64((in[15]>>0)&mask < val)<<30 |
		b2u64((in[15]>>32)&mask < val)<<31 |
		b2u64((in[16]>>0)&mask < val)<<32 |
		b2u64((in[16]>>32)&mask < val)<<33 |
		b2u64((in[17]>>0)&mask < val)<<34 |
		b2u64((in[17]>>32)&mask < val)<<35 |
		b2u64((in[18]>>0)&mask < val)<<36 |
		b2u64((in[18]>>32)&mask < val)<<37 |
		b2u64((in[19]>>0)&mask < val)<<38 |
		b2u64((in[19]>>32)&mask < val)<<39 |
		b2u64((in[20]>>0)&mask < val)<<40 |
		b2u64((in[20]>>32)&mask < val)<<41 |
		b2u64((in[21]>>0)&mask < val)<<42 |
		b2u64((in[21]>>32)&mask < val)<<43 |
		b2u64((in[22]>>0)&mask < val)<<44 |
		b2u64((in[22]>>32)&mask < val)<<45 |
		b2u64((in[23]>>0)&mask < val)<<46 |
		b2u64((in[23]>>32)&mask < val)<<47 |
		b2u64((in[24]>>0)&mask < val)<<48 |
		b2u64((in[24]>>32)&mask < val)<<49 |
		b2u64((in[25]>>0)&mask < val)<<50 |
		b2u64((in[25]>>32)&mask < val)<<51 |
		b2u64((in[26]>>0)&mask < val)<<52 |
		b2u64((in[26]>>32)&mask < val)<<53 |
		b2u64((in[27]>>0)&mask < val)<<54 |
		b2u64((in[27]>>32)&mask < val)<<55 |
		b2u64((in[28]>>0)&mask < val)<<56 |
		b2u64((in[28]>>32)&mask < val)<<57 |
		b2u64((in[29]>>0)&mask < val)<<58 |
		b2u64((in[29]>>32)&mask < val)<<59 |
		b2u64((in[30]>>0)&mask < val)<<60 |
		b2u64((in[30]>>32)&mask < val)<<61 |
		b2u64((in[31]>>0)&mask < val)<<62 |
		b2u64((in[31]>>32)&mask < val)<<63)

}
func cmp_bp_33_lt(in *[33]uint64, val uint64) uint64 {
	mask := uint64((1 << 33) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>33)&mask|
			(in[1]<<31)&mask < val)<<1 |
		b2u64((in[1]>>2)&mask < val)<<2 |
		b2u64((in[1]>>35)&mask|
			(in[2]<<29)&mask < val)<<3 |
		b2u64((in[2]>>4)&mask < val)<<4 |
		b2u64((in[2]>>37)&mask|
			(in[3]<<27)&mask < val)<<5 |
		b2u64((in[3]>>6)&mask < val)<<6 |
		b2u64((in[3]>>39)&mask|
			(in[4]<<25)&mask < val)<<7 |
		b2u64((in[4]>>8)&mask < val)<<8 |
		b2u64((in[4]>>41)&mask|
			(in[5]<<23)&mask < val)<<9 |
		b2u64((in[5]>>10)&mask < val)<<10 |
		b2u64((in[5]>>43)&mask|
			(in[6]<<21)&mask < val)<<11 |
		b2u64((in[6]>>12)&mask < val)<<12 |
		b2u64((in[6]>>45)&mask|
			(in[7]<<19)&mask < val)<<13 |
		b2u64((in[7]>>14)&mask < val)<<14 |
		b2u64((in[7]>>47)&mask|
			(in[8]<<17)&mask < val)<<15 |
		b2u64((in[8]>>16)&mask < val)<<16 |
		b2u64((in[8]>>49)&mask|
			(in[9]<<15)&mask < val)<<17 |
		b2u64((in[9]>>18)&mask < val)<<18 |
		b2u64((in[9]>>51)&mask|
			(in[10]<<13)&mask < val)<<19 |
		b2u64((in[10]>>20)&mask < val)<<20 |
		b2u64((in[10]>>53)&mask|
			(in[11]<<11)&mask < val)<<21 |
		b2u64((in[11]>>22)&mask < val)<<22 |
		b2u64((in[11]>>55)&mask|
			(in[12]<<9)&mask < val)<<23 |
		b2u64((in[12]>>24)&mask < val)<<24 |
		b2u64((in[12]>>57)&mask|
			(in[13]<<7)&mask < val)<<25 |
		b2u64((in[13]>>26)&mask < val)<<26 |
		b2u64((in[13]>>59)&mask|
			(in[14]<<5)&mask < val)<<27 |
		b2u64((in[14]>>28)&mask < val)<<28 |
		b2u64((in[14]>>61)&mask|
			(in[15]<<3)&mask < val)<<29 |
		b2u64((in[15]>>30)&mask < val)<<30 |
		b2u64((in[15]>>63)&mask|
			(in[16]<<1)&mask < val)<<31 |
		b2u64((in[16]>>32)&mask|
			(in[17]<<32)&mask < val)<<32 |
		b2u64((in[17]>>1)&mask < val)<<33 |
		b2u64((in[17]>>34)&mask|
			(in[18]<<30)&mask < val)<<34 |
		b2u64((in[18]>>3)&mask < val)<<35 |
		b2u64((in[18]>>36)&mask|
			(in[19]<<28)&mask < val)<<36 |
		b2u64((in[19]>>5)&mask < val)<<37 |
		b2u64((in[19]>>38)&mask|
			(in[20]<<26)&mask < val)<<38 |
		b2u64((in[20]>>7)&mask < val)<<39 |
		b2u64((in[20]>>40)&mask|
			(in[21]<<24)&mask < val)<<40 |
		b2u64((in[21]>>9)&mask < val)<<41 |
		b2u64((in[21]>>42)&mask|
			(in[22]<<22)&mask < val)<<42 |
		b2u64((in[22]>>11)&mask < val)<<43 |
		b2u64((in[22]>>44)&mask|
			(in[23]<<20)&mask < val)<<44 |
		b2u64((in[23]>>13)&mask < val)<<45 |
		b2u64((in[23]>>46)&mask|
			(in[24]<<18)&mask < val)<<46 |
		b2u64((in[24]>>15)&mask < val)<<47 |
		b2u64((in[24]>>48)&mask|
			(in[25]<<16)&mask < val)<<48 |
		b2u64((in[25]>>17)&mask < val)<<49 |
		b2u64((in[25]>>50)&mask|
			(in[26]<<14)&mask < val)<<50 |
		b2u64((in[26]>>19)&mask < val)<<51 |
		b2u64((in[26]>>52)&mask|
			(in[27]<<12)&mask < val)<<52 |
		b2u64((in[27]>>21)&mask < val)<<53 |
		b2u64((in[27]>>54)&mask|
			(in[28]<<10)&mask < val)<<54 |
		b2u64((in[28]>>23)&mask < val)<<55 |
		b2u64((in[28]>>56)&mask|
			(in[29]<<8)&mask < val)<<56 |
		b2u64((in[29]>>25)&mask < val)<<57 |
		b2u64((in[29]>>58)&mask|
			(in[30]<<6)&mask < val)<<58 |
		b2u64((in[30]>>27)&mask < val)<<59 |
		b2u64((in[30]>>60)&mask|
			(in[31]<<4)&mask < val)<<60 |
		b2u64((in[31]>>29)&mask < val)<<61 |
		b2u64((in[31]>>62)&mask|
			(in[32]<<2)&mask < val)<<62 |
		b2u64((in[32]>>31)&mask < val)<<63)

}
func cmp_bp_34_lt(in *[34]uint64, val uint64) uint64 {
	mask := uint64((1 << 34) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>34)&mask|
			(in[1]<<30)&mask < val)<<1 |
		b2u64((in[1]>>4)&mask < val)<<2 |
		b2u64((in[1]>>38)&mask|
			(in[2]<<26)&mask < val)<<3 |
		b2u64((in[2]>>8)&mask < val)<<4 |
		b2u64((in[2]>>42)&mask|
			(in[3]<<22)&mask < val)<<5 |
		b2u64((in[3]>>12)&mask < val)<<6 |
		b2u64((in[3]>>46)&mask|
			(in[4]<<18)&mask < val)<<7 |
		b2u64((in[4]>>16)&mask < val)<<8 |
		b2u64((in[4]>>50)&mask|
			(in[5]<<14)&mask < val)<<9 |
		b2u64((in[5]>>20)&mask < val)<<10 |
		b2u64((in[5]>>54)&mask|
			(in[6]<<10)&mask < val)<<11 |
		b2u64((in[6]>>24)&mask < val)<<12 |
		b2u64((in[6]>>58)&mask|
			(in[7]<<6)&mask < val)<<13 |
		b2u64((in[7]>>28)&mask < val)<<14 |
		b2u64((in[7]>>62)&mask|
			(in[8]<<2)&mask < val)<<15 |
		b2u64((in[8]>>32)&mask|
			(in[9]<<32)&mask < val)<<16 |
		b2u64((in[9]>>2)&mask < val)<<17 |
		b2u64((in[9]>>36)&mask|
			(in[10]<<28)&mask < val)<<18 |
		b2u64((in[10]>>6)&mask < val)<<19 |
		b2u64((in[10]>>40)&mask|
			(in[11]<<24)&mask < val)<<20 |
		b2u64((in[11]>>10)&mask < val)<<21 |
		b2u64((in[11]>>44)&mask|
			(in[12]<<20)&mask < val)<<22 |
		b2u64((in[12]>>14)&mask < val)<<23 |
		b2u64((in[12]>>48)&mask|
			(in[13]<<16)&mask < val)<<24 |
		b2u64((in[13]>>18)&mask < val)<<25 |
		b2u64((in[13]>>52)&mask|
			(in[14]<<12)&mask < val)<<26 |
		b2u64((in[14]>>22)&mask < val)<<27 |
		b2u64((in[14]>>56)&mask|
			(in[15]<<8)&mask < val)<<28 |
		b2u64((in[15]>>26)&mask < val)<<29 |
		b2u64((in[15]>>60)&mask|
			(in[16]<<4)&mask < val)<<30 |
		b2u64((in[16]>>30)&mask < val)<<31 |
		b2u64((in[17]>>0)&mask < val)<<32 |
		b2u64((in[17]>>34)&mask|
			(in[18]<<30)&mask < val)<<33 |
		b2u64((in[18]>>4)&mask < val)<<34 |
		b2u64((in[18]>>38)&mask|
			(in[19]<<26)&mask < val)<<35 |
		b2u64((in[19]>>8)&mask < val)<<36 |
		b2u64((in[19]>>42)&mask|
			(in[20]<<22)&mask < val)<<37 |
		b2u64((in[20]>>12)&mask < val)<<38 |
		b2u64((in[20]>>46)&mask|
			(in[21]<<18)&mask < val)<<39 |
		b2u64((in[21]>>16)&mask < val)<<40 |
		b2u64((in[21]>>50)&mask|
			(in[22]<<14)&mask < val)<<41 |
		b2u64((in[22]>>20)&mask < val)<<42 |
		b2u64((in[22]>>54)&mask|
			(in[23]<<10)&mask < val)<<43 |
		b2u64((in[23]>>24)&mask < val)<<44 |
		b2u64((in[23]>>58)&mask|
			(in[24]<<6)&mask < val)<<45 |
		b2u64((in[24]>>28)&mask < val)<<46 |
		b2u64((in[24]>>62)&mask|
			(in[25]<<2)&mask < val)<<47 |
		b2u64((in[25]>>32)&mask|
			(in[26]<<32)&mask < val)<<48 |
		b2u64((in[26]>>2)&mask < val)<<49 |
		b2u64((in[26]>>36)&mask|
			(in[27]<<28)&mask < val)<<50 |
		b2u64((in[27]>>6)&mask < val)<<51 |
		b2u64((in[27]>>40)&mask|
			(in[28]<<24)&mask < val)<<52 |
		b2u64((in[28]>>10)&mask < val)<<53 |
		b2u64((in[28]>>44)&mask|
			(in[29]<<20)&mask < val)<<54 |
		b2u64((in[29]>>14)&mask < val)<<55 |
		b2u64((in[29]>>48)&mask|
			(in[30]<<16)&mask < val)<<56 |
		b2u64((in[30]>>18)&mask < val)<<57 |
		b2u64((in[30]>>52)&mask|
			(in[31]<<12)&mask < val)<<58 |
		b2u64((in[31]>>22)&mask < val)<<59 |
		b2u64((in[31]>>56)&mask|
			(in[32]<<8)&mask < val)<<60 |
		b2u64((in[32]>>26)&mask < val)<<61 |
		b2u64((in[32]>>60)&mask|
			(in[33]<<4)&mask < val)<<62 |
		b2u64((in[33]>>30)&mask < val)<<63)

}
func cmp_bp_35_lt(in *[35]uint64, val uint64) uint64 {
	mask := uint64((1 << 35) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>35)&mask|
			(in[1]<<29)&mask < val)<<1 |
		b2u64((in[1]>>6)&mask < val)<<2 |
		b2u64((in[1]>>41)&mask|
			(in[2]<<23)&mask < val)<<3 |
		b2u64((in[2]>>12)&mask < val)<<4 |
		b2u64((in[2]>>47)&mask|
			(in[3]<<17)&mask < val)<<5 |
		b2u64((in[3]>>18)&mask < val)<<6 |
		b2u64((in[3]>>53)&mask|
			(in[4]<<11)&mask < val)<<7 |
		b2u64((in[4]>>24)&mask < val)<<8 |
		b2u64((in[4]>>59)&mask|
			(in[5]<<5)&mask < val)<<9 |
		b2u64((in[5]>>30)&mask|
			(in[6]<<34)&mask < val)<<10 |
		b2u64((in[6]>>1)&mask < val)<<11 |
		b2u64((in[6]>>36)&mask|
			(in[7]<<28)&mask < val)<<12 |
		b2u64((in[7]>>7)&mask < val)<<13 |
		b2u64((in[7]>>42)&mask|
			(in[8]<<22)&mask < val)<<14 |
		b2u64((in[8]>>13)&mask < val)<<15 |
		b2u64((in[8]>>48)&mask|
			(in[9]<<16)&mask < val)<<16 |
		b2u64((in[9]>>19)&mask < val)<<17 |
		b2u64((in[9]>>54)&mask|
			(in[10]<<10)&mask < val)<<18 |
		b2u64((in[10]>>25)&mask < val)<<19 |
		b2u64((in[10]>>60)&mask|
			(in[11]<<4)&mask < val)<<20 |
		b2u64((in[11]>>31)&mask|
			(in[12]<<33)&mask < val)<<21 |
		b2u64((in[12]>>2)&mask < val)<<22 |
		b2u64((in[12]>>37)&mask|
			(in[13]<<27)&mask < val)<<23 |
		b2u64((in[13]>>8)&mask < val)<<24 |
		b2u64((in[13]>>43)&mask|
			(in[14]<<21)&mask < val)<<25 |
		b2u64((in[14]>>14)&mask < val)<<26 |
		b2u64((in[14]>>49)&mask|
			(in[15]<<15)&mask < val)<<27 |
		b2u64((in[15]>>20)&mask < val)<<28 |
		b2u64((in[15]>>55)&mask|
			(in[16]<<9)&mask < val)<<29 |
		b2u64((in[16]>>26)&mask < val)<<30 |
		b2u64((in[16]>>61)&mask|
			(in[17]<<3)&mask < val)<<31 |
		b2u64((in[17]>>32)&mask|
			(in[18]<<32)&mask < val)<<32 |
		b2u64((in[18]>>3)&mask < val)<<33 |
		b2u64((in[18]>>38)&mask|
			(in[19]<<26)&mask < val)<<34 |
		b2u64((in[19]>>9)&mask < val)<<35 |
		b2u64((in[19]>>44)&mask|
			(in[20]<<20)&mask < val)<<36 |
		b2u64((in[20]>>15)&mask < val)<<37 |
		b2u64((in[20]>>50)&mask|
			(in[21]<<14)&mask < val)<<38 |
		b2u64((in[21]>>21)&mask < val)<<39 |
		b2u64((in[21]>>56)&mask|
			(in[22]<<8)&mask < val)<<40 |
		b2u64((in[22]>>27)&mask < val)<<41 |
		b2u64((in[22]>>62)&mask|
			(in[23]<<2)&mask < val)<<42 |
		b2u64((in[23]>>33)&mask|
			(in[24]<<31)&mask < val)<<43 |
		b2u64((in[24]>>4)&mask < val)<<44 |
		b2u64((in[24]>>39)&mask|
			(in[25]<<25)&mask < val)<<45 |
		b2u64((in[25]>>10)&mask < val)<<46 |
		b2u64((in[25]>>45)&mask|
			(in[26]<<19)&mask < val)<<47 |
		b2u64((in[26]>>16)&mask < val)<<48 |
		b2u64((in[26]>>51)&mask|
			(in[27]<<13)&mask < val)<<49 |
		b2u64((in[27]>>22)&mask < val)<<50 |
		b2u64((in[27]>>57)&mask|
			(in[28]<<7)&mask < val)<<51 |
		b2u64((in[28]>>28)&mask < val)<<52 |
		b2u64((in[28]>>63)&mask|
			(in[29]<<1)&mask < val)<<53 |
		b2u64((in[29]>>34)&mask|
			(in[30]<<30)&mask < val)<<54 |
		b2u64((in[30]>>5)&mask < val)<<55 |
		b2u64((in[30]>>40)&mask|
			(in[31]<<24)&mask < val)<<56 |
		b2u64((in[31]>>11)&mask < val)<<57 |
		b2u64((in[31]>>46)&mask|
			(in[32]<<18)&mask < val)<<58 |
		b2u64((in[32]>>17)&mask < val)<<59 |
		b2u64((in[32]>>52)&mask|
			(in[33]<<12)&mask < val)<<60 |
		b2u64((in[33]>>23)&mask < val)<<61 |
		b2u64((in[33]>>58)&mask|
			(in[34]<<6)&mask < val)<<62 |
		b2u64((in[34]>>29)&mask < val)<<63)

}
func cmp_bp_36_lt(in *[36]uint64, val uint64) uint64 {
	mask := uint64((1 << 36) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>36)&mask|
			(in[1]<<28)&mask < val)<<1 |
		b2u64((in[1]>>8)&mask < val)<<2 |
		b2u64((in[1]>>44)&mask|
			(in[2]<<20)&mask < val)<<3 |
		b2u64((in[2]>>16)&mask < val)<<4 |
		b2u64((in[2]>>52)&mask|
			(in[3]<<12)&mask < val)<<5 |
		b2u64((in[3]>>24)&mask < val)<<6 |
		b2u64((in[3]>>60)&mask|
			(in[4]<<4)&mask < val)<<7 |
		b2u64((in[4]>>32)&mask|
			(in[5]<<32)&mask < val)<<8 |
		b2u64((in[5]>>4)&mask < val)<<9 |
		b2u64((in[5]>>40)&mask|
			(in[6]<<24)&mask < val)<<10 |
		b2u64((in[6]>>12)&mask < val)<<11 |
		b2u64((in[6]>>48)&mask|
			(in[7]<<16)&mask < val)<<12 |
		b2u64((in[7]>>20)&mask < val)<<13 |
		b2u64((in[7]>>56)&mask|
			(in[8]<<8)&mask < val)<<14 |
		b2u64((in[8]>>28)&mask < val)<<15 |
		b2u64((in[9]>>0)&mask < val)<<16 |
		b2u64((in[9]>>36)&mask|
			(in[10]<<28)&mask < val)<<17 |
		b2u64((in[10]>>8)&mask < val)<<18 |
		b2u64((in[10]>>44)&mask|
			(in[11]<<20)&mask < val)<<19 |
		b2u64((in[11]>>16)&mask < val)<<20 |
		b2u64((in[11]>>52)&mask|
			(in[12]<<12)&mask < val)<<21 |
		b2u64((in[12]>>24)&mask < val)<<22 |
		b2u64((in[12]>>60)&mask|
			(in[13]<<4)&mask < val)<<23 |
		b2u64((in[13]>>32)&mask|
			(in[14]<<32)&mask < val)<<24 |
		b2u64((in[14]>>4)&mask < val)<<25 |
		b2u64((in[14]>>40)&mask|
			(in[15]<<24)&mask < val)<<26 |
		b2u64((in[15]>>12)&mask < val)<<27 |
		b2u64((in[15]>>48)&mask|
			(in[16]<<16)&mask < val)<<28 |
		b2u64((in[16]>>20)&mask < val)<<29 |
		b2u64((in[16]>>56)&mask|
			(in[17]<<8)&mask < val)<<30 |
		b2u64((in[17]>>28)&mask < val)<<31 |
		b2u64((in[18]>>0)&mask < val)<<32 |
		b2u64((in[18]>>36)&mask|
			(in[19]<<28)&mask < val)<<33 |
		b2u64((in[19]>>8)&mask < val)<<34 |
		b2u64((in[19]>>44)&mask|
			(in[20]<<20)&mask < val)<<35 |
		b2u64((in[20]>>16)&mask < val)<<36 |
		b2u64((in[20]>>52)&mask|
			(in[21]<<12)&mask < val)<<37 |
		b2u64((in[21]>>24)&mask < val)<<38 |
		b2u64((in[21]>>60)&mask|
			(in[22]<<4)&mask < val)<<39 |
		b2u64((in[22]>>32)&mask|
			(in[23]<<32)&mask < val)<<40 |
		b2u64((in[23]>>4)&mask < val)<<41 |
		b2u64((in[23]>>40)&mask|
			(in[24]<<24)&mask < val)<<42 |
		b2u64((in[24]>>12)&mask < val)<<43 |
		b2u64((in[24]>>48)&mask|
			(in[25]<<16)&mask < val)<<44 |
		b2u64((in[25]>>20)&mask < val)<<45 |
		b2u64((in[25]>>56)&mask|
			(in[26]<<8)&mask < val)<<46 |
		b2u64((in[26]>>28)&mask < val)<<47 |
		b2u64((in[27]>>0)&mask < val)<<48 |
		b2u64((in[27]>>36)&mask|
			(in[28]<<28)&mask < val)<<49 |
		b2u64((in[28]>>8)&mask < val)<<50 |
		b2u64((in[28]>>44)&mask|
			(in[29]<<20)&mask < val)<<51 |
		b2u64((in[29]>>16)&mask < val)<<52 |
		b2u64((in[29]>>52)&mask|
			(in[30]<<12)&mask < val)<<53 |
		b2u64((in[30]>>24)&mask < val)<<54 |
		b2u64((in[30]>>60)&mask|
			(in[31]<<4)&mask < val)<<55 |
		b2u64((in[31]>>32)&mask|
			(in[32]<<32)&mask < val)<<56 |
		b2u64((in[32]>>4)&mask < val)<<57 |
		b2u64((in[32]>>40)&mask|
			(in[33]<<24)&mask < val)<<58 |
		b2u64((in[33]>>12)&mask < val)<<59 |
		b2u64((in[33]>>48)&mask|
			(in[34]<<16)&mask < val)<<60 |
		b2u64((in[34]>>20)&mask < val)<<61 |
		b2u64((in[34]>>56)&mask|
			(in[35]<<8)&mask < val)<<62 |
		b2u64((in[35]>>28)&mask < val)<<63)

}
func cmp_bp_37_lt(in *[37]uint64, val uint64) uint64 {
	mask := uint64((1 << 37) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>37)&mask|
			(in[1]<<27)&mask < val)<<1 |
		b2u64((in[1]>>10)&mask < val)<<2 |
		b2u64((in[1]>>47)&mask|
			(in[2]<<17)&mask < val)<<3 |
		b2u64((in[2]>>20)&mask < val)<<4 |
		b2u64((in[2]>>57)&mask|
			(in[3]<<7)&mask < val)<<5 |
		b2u64((in[3]>>30)&mask|
			(in[4]<<34)&mask < val)<<6 |
		b2u64((in[4]>>3)&mask < val)<<7 |
		b2u64((in[4]>>40)&mask|
			(in[5]<<24)&mask < val)<<8 |
		b2u64((in[5]>>13)&mask < val)<<9 |
		b2u64((in[5]>>50)&mask|
			(in[6]<<14)&mask < val)<<10 |
		b2u64((in[6]>>23)&mask < val)<<11 |
		b2u64((in[6]>>60)&mask|
			(in[7]<<4)&mask < val)<<12 |
		b2u64((in[7]>>33)&mask|
			(in[8]<<31)&mask < val)<<13 |
		b2u64((in[8]>>6)&mask < val)<<14 |
		b2u64((in[8]>>43)&mask|
			(in[9]<<21)&mask < val)<<15 |
		b2u64((in[9]>>16)&mask < val)<<16 |
		b2u64((in[9]>>53)&mask|
			(in[10]<<11)&mask < val)<<17 |
		b2u64((in[10]>>26)&mask < val)<<18 |
		b2u64((in[10]>>63)&mask|
			(in[11]<<1)&mask < val)<<19 |
		b2u64((in[11]>>36)&mask|
			(in[12]<<28)&mask < val)<<20 |
		b2u64((in[12]>>9)&mask < val)<<21 |
		b2u64((in[12]>>46)&mask|
			(in[13]<<18)&mask < val)<<22 |
		b2u64((in[13]>>19)&mask < val)<<23 |
		b2u64((in[13]>>56)&mask|
			(in[14]<<8)&mask < val)<<24 |
		b2u64((in[14]>>29)&mask|
			(in[15]<<35)&mask < val)<<25 |
		b2u64((in[15]>>2)&mask < val)<<26 |
		b2u64((in[15]>>39)&mask|
			(in[16]<<25)&mask < val)<<27 |
		b2u64((in[16]>>12)&mask < val)<<28 |
		b2u64((in[16]>>49)&mask|
			(in[17]<<15)&mask < val)<<29 |
		b2u64((in[17]>>22)&mask < val)<<30 |
		b2u64((in[17]>>59)&mask|
			(in[18]<<5)&mask < val)<<31 |
		b2u64((in[18]>>32)&mask|
			(in[19]<<32)&mask < val)<<32 |
		b2u64((in[19]>>5)&mask < val)<<33 |
		b2u64((in[19]>>42)&mask|
			(in[20]<<22)&mask < val)<<34 |
		b2u64((in[20]>>15)&mask < val)<<35 |
		b2u64((in[20]>>52)&mask|
			(in[21]<<12)&mask < val)<<36 |
		b2u64((in[21]>>25)&mask < val)<<37 |
		b2u64((in[21]>>62)&mask|
			(in[22]<<2)&mask < val)<<38 |
		b2u64((in[22]>>35)&mask|
			(in[23]<<29)&mask < val)<<39 |
		b2u64((in[23]>>8)&mask < val)<<40 |
		b2u64((in[23]>>45)&mask|
			(in[24]<<19)&mask < val)<<41 |
		b2u64((in[24]>>18)&mask < val)<<42 |
		b2u64((in[24]>>55)&mask|
			(in[25]<<9)&mask < val)<<43 |
		b2u64((in[25]>>28)&mask|
			(in[26]<<36)&mask < val)<<44 |
		b2u64((in[26]>>1)&mask < val)<<45 |
		b2u64((in[26]>>38)&mask|
			(in[27]<<26)&mask < val)<<46 |
		b2u64((in[27]>>11)&mask < val)<<47 |
		b2u64((in[27]>>48)&mask|
			(in[28]<<16)&mask < val)<<48 |
		b2u64((in[28]>>21)&mask < val)<<49 |
		b2u64((in[28]>>58)&mask|
			(in[29]<<6)&mask < val)<<50 |
		b2u64((in[29]>>31)&mask|
			(in[30]<<33)&mask < val)<<51 |
		b2u64((in[30]>>4)&mask < val)<<52 |
		b2u64((in[30]>>41)&mask|
			(in[31]<<23)&mask < val)<<53 |
		b2u64((in[31]>>14)&mask < val)<<54 |
		b2u64((in[31]>>51)&mask|
			(in[32]<<13)&mask < val)<<55 |
		b2u64((in[32]>>24)&mask < val)<<56 |
		b2u64((in[32]>>61)&mask|
			(in[33]<<3)&mask < val)<<57 |
		b2u64((in[33]>>34)&mask|
			(in[34]<<30)&mask < val)<<58 |
		b2u64((in[34]>>7)&mask < val)<<59 |
		b2u64((in[34]>>44)&mask|
			(in[35]<<20)&mask < val)<<60 |
		b2u64((in[35]>>17)&mask < val)<<61 |
		b2u64((in[35]>>54)&mask|
			(in[36]<<10)&mask < val)<<62 |
		b2u64((in[36]>>27)&mask < val)<<63)

}
func cmp_bp_38_lt(in *[38]uint64, val uint64) uint64 {
	mask := uint64((1 << 38) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>38)&mask|
			(in[1]<<26)&mask < val)<<1 |
		b2u64((in[1]>>12)&mask < val)<<2 |
		b2u64((in[1]>>50)&mask|
			(in[2]<<14)&mask < val)<<3 |
		b2u64((in[2]>>24)&mask < val)<<4 |
		b2u64((in[2]>>62)&mask|
			(in[3]<<2)&mask < val)<<5 |
		b2u64((in[3]>>36)&mask|
			(in[4]<<28)&mask < val)<<6 |
		b2u64((in[4]>>10)&mask < val)<<7 |
		b2u64((in[4]>>48)&mask|
			(in[5]<<16)&mask < val)<<8 |
		b2u64((in[5]>>22)&mask < val)<<9 |
		b2u64((in[5]>>60)&mask|
			(in[6]<<4)&mask < val)<<10 |
		b2u64((in[6]>>34)&mask|
			(in[7]<<30)&mask < val)<<11 |
		b2u64((in[7]>>8)&mask < val)<<12 |
		b2u64((in[7]>>46)&mask|
			(in[8]<<18)&mask < val)<<13 |
		b2u64((in[8]>>20)&mask < val)<<14 |
		b2u64((in[8]>>58)&mask|
			(in[9]<<6)&mask < val)<<15 |
		b2u64((in[9]>>32)&mask|
			(in[10]<<32)&mask < val)<<16 |
		b2u64((in[10]>>6)&mask < val)<<17 |
		b2u64((in[10]>>44)&mask|
			(in[11]<<20)&mask < val)<<18 |
		b2u64((in[11]>>18)&mask < val)<<19 |
		b2u64((in[11]>>56)&mask|
			(in[12]<<8)&mask < val)<<20 |
		b2u64((in[12]>>30)&mask|
			(in[13]<<34)&mask < val)<<21 |
		b2u64((in[13]>>4)&mask < val)<<22 |
		b2u64((in[13]>>42)&mask|
			(in[14]<<22)&mask < val)<<23 |
		b2u64((in[14]>>16)&mask < val)<<24 |
		b2u64((in[14]>>54)&mask|
			(in[15]<<10)&mask < val)<<25 |
		b2u64((in[15]>>28)&mask|
			(in[16]<<36)&mask < val)<<26 |
		b2u64((in[16]>>2)&mask < val)<<27 |
		b2u64((in[16]>>40)&mask|
			(in[17]<<24)&mask < val)<<28 |
		b2u64((in[17]>>14)&mask < val)<<29 |
		b2u64((in[17]>>52)&mask|
			(in[18]<<12)&mask < val)<<30 |
		b2u64((in[18]>>26)&mask < val)<<31 |
		b2u64((in[19]>>0)&mask < val)<<32 |
		b2u64((in[19]>>38)&mask|
			(in[20]<<26)&mask < val)<<33 |
		b2u64((in[20]>>12)&mask < val)<<34 |
		b2u64((in[20]>>50)&mask|
			(in[21]<<14)&mask < val)<<35 |
		b2u64((in[21]>>24)&mask < val)<<36 |
		b2u64((in[21]>>62)&mask|
			(in[22]<<2)&mask < val)<<37 |
		b2u64((in[22]>>36)&mask|
			(in[23]<<28)&mask < val)<<38 |
		b2u64((in[23]>>10)&mask < val)<<39 |
		b2u64((in[23]>>48)&mask|
			(in[24]<<16)&mask < val)<<40 |
		b2u64((in[24]>>22)&mask < val)<<41 |
		b2u64((in[24]>>60)&mask|
			(in[25]<<4)&mask < val)<<42 |
		b2u64((in[25]>>34)&mask|
			(in[26]<<30)&mask < val)<<43 |
		b2u64((in[26]>>8)&mask < val)<<44 |
		b2u64((in[26]>>46)&mask|
			(in[27]<<18)&mask < val)<<45 |
		b2u64((in[27]>>20)&mask < val)<<46 |
		b2u64((in[27]>>58)&mask|
			(in[28]<<6)&mask < val)<<47 |
		b2u64((in[28]>>32)&mask|
			(in[29]<<32)&mask < val)<<48 |
		b2u64((in[29]>>6)&mask < val)<<49 |
		b2u64((in[29]>>44)&mask|
			(in[30]<<20)&mask < val)<<50 |
		b2u64((in[30]>>18)&mask < val)<<51 |
		b2u64((in[30]>>56)&mask|
			(in[31]<<8)&mask < val)<<52 |
		b2u64((in[31]>>30)&mask|
			(in[32]<<34)&mask < val)<<53 |
		b2u64((in[32]>>4)&mask < val)<<54 |
		b2u64((in[32]>>42)&mask|
			(in[33]<<22)&mask < val)<<55 |
		b2u64((in[33]>>16)&mask < val)<<56 |
		b2u64((in[33]>>54)&mask|
			(in[34]<<10)&mask < val)<<57 |
		b2u64((in[34]>>28)&mask|
			(in[35]<<36)&mask < val)<<58 |
		b2u64((in[35]>>2)&mask < val)<<59 |
		b2u64((in[35]>>40)&mask|
			(in[36]<<24)&mask < val)<<60 |
		b2u64((in[36]>>14)&mask < val)<<61 |
		b2u64((in[36]>>52)&mask|
			(in[37]<<12)&mask < val)<<62 |
		b2u64((in[37]>>26)&mask < val)<<63)

}
func cmp_bp_39_lt(in *[39]uint64, val uint64) uint64 {
	mask := uint64((1 << 39) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>39)&mask|
			(in[1]<<25)&mask < val)<<1 |
		b2u64((in[1]>>14)&mask < val)<<2 |
		b2u64((in[1]>>53)&mask|
			(in[2]<<11)&mask < val)<<3 |
		b2u64((in[2]>>28)&mask|
			(in[3]<<36)&mask < val)<<4 |
		b2u64((in[3]>>3)&mask < val)<<5 |
		b2u64((in[3]>>42)&mask|
			(in[4]<<22)&mask < val)<<6 |
		b2u64((in[4]>>17)&mask < val)<<7 |
		b2u64((in[4]>>56)&mask|
			(in[5]<<8)&mask < val)<<8 |
		b2u64((in[5]>>31)&mask|
			(in[6]<<33)&mask < val)<<9 |
		b2u64((in[6]>>6)&mask < val)<<10 |
		b2u64((in[6]>>45)&mask|
			(in[7]<<19)&mask < val)<<11 |
		b2u64((in[7]>>20)&mask < val)<<12 |
		b2u64((in[7]>>59)&mask|
			(in[8]<<5)&mask < val)<<13 |
		b2u64((in[8]>>34)&mask|
			(in[9]<<30)&mask < val)<<14 |
		b2u64((in[9]>>9)&mask < val)<<15 |
		b2u64((in[9]>>48)&mask|
			(in[10]<<16)&mask < val)<<16 |
		b2u64((in[10]>>23)&mask < val)<<17 |
		b2u64((in[10]>>62)&mask|
			(in[11]<<2)&mask < val)<<18 |
		b2u64((in[11]>>37)&mask|
			(in[12]<<27)&mask < val)<<19 |
		b2u64((in[12]>>12)&mask < val)<<20 |
		b2u64((in[12]>>51)&mask|
			(in[13]<<13)&mask < val)<<21 |
		b2u64((in[13]>>26)&mask|
			(in[14]<<38)&mask < val)<<22 |
		b2u64((in[14]>>1)&mask < val)<<23 |
		b2u64((in[14]>>40)&mask|
			(in[15]<<24)&mask < val)<<24 |
		b2u64((in[15]>>15)&mask < val)<<25 |
		b2u64((in[15]>>54)&mask|
			(in[16]<<10)&mask < val)<<26 |
		b2u64((in[16]>>29)&mask|
			(in[17]<<35)&mask < val)<<27 |
		b2u64((in[17]>>4)&mask < val)<<28 |
		b2u64((in[17]>>43)&mask|
			(in[18]<<21)&mask < val)<<29 |
		b2u64((in[18]>>18)&mask < val)<<30 |
		b2u64((in[18]>>57)&mask|
			(in[19]<<7)&mask < val)<<31 |
		b2u64((in[19]>>32)&mask|
			(in[20]<<32)&mask < val)<<32 |
		b2u64((in[20]>>7)&mask < val)<<33 |
		b2u64((in[20]>>46)&mask|
			(in[21]<<18)&mask < val)<<34 |
		b2u64((in[21]>>21)&mask < val)<<35 |
		b2u64((in[21]>>60)&mask|
			(in[22]<<4)&mask < val)<<36 |
		b2u64((in[22]>>35)&mask|
			(in[23]<<29)&mask < val)<<37 |
		b2u64((in[23]>>10)&mask < val)<<38 |
		b2u64((in[23]>>49)&mask|
			(in[24]<<15)&mask < val)<<39 |
		b2u64((in[24]>>24)&mask < val)<<40 |
		b2u64((in[24]>>63)&mask|
			(in[25]<<1)&mask < val)<<41 |
		b2u64((in[25]>>38)&mask|
			(in[26]<<26)&mask < val)<<42 |
		b2u64((in[26]>>13)&mask < val)<<43 |
		b2u64((in[26]>>52)&mask|
			(in[27]<<12)&mask < val)<<44 |
		b2u64((in[27]>>27)&mask|
			(in[28]<<37)&mask < val)<<45 |
		b2u64((in[28]>>2)&mask < val)<<46 |
		b2u64((in[28]>>41)&mask|
			(in[29]<<23)&mask < val)<<47 |
		b2u64((in[29]>>16)&mask < val)<<48 |
		b2u64((in[29]>>55)&mask|
			(in[30]<<9)&mask < val)<<49 |
		b2u64((in[30]>>30)&mask|
			(in[31]<<34)&mask < val)<<50 |
		b2u64((in[31]>>5)&mask < val)<<51 |
		b2u64((in[31]>>44)&mask|
			(in[32]<<20)&mask < val)<<52 |
		b2u64((in[32]>>19)&mask < val)<<53 |
		b2u64((in[32]>>58)&mask|
			(in[33]<<6)&mask < val)<<54 |
		b2u64((in[33]>>33)&mask|
			(in[34]<<31)&mask < val)<<55 |
		b2u64((in[34]>>8)&mask < val)<<56 |
		b2u64((in[34]>>47)&mask|
			(in[35]<<17)&mask < val)<<57 |
		b2u64((in[35]>>22)&mask < val)<<58 |
		b2u64((in[35]>>61)&mask|
			(in[36]<<3)&mask < val)<<59 |
		b2u64((in[36]>>36)&mask|
			(in[37]<<28)&mask < val)<<60 |
		b2u64((in[37]>>11)&mask < val)<<61 |
		b2u64((in[37]>>50)&mask|
			(in[38]<<14)&mask < val)<<62 |
		b2u64((in[38]>>25)&mask < val)<<63)

}
func cmp_bp_40_lt(in *[40]uint64, val uint64) uint64 {
	mask := uint64((1 << 40) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>40)&mask|
			(in[1]<<24)&mask < val)<<1 |
		b2u64((in[1]>>16)&mask < val)<<2 |
		b2u64((in[1]>>56)&mask|
			(in[2]<<8)&mask < val)<<3 |
		b2u64((in[2]>>32)&mask|
			(in[3]<<32)&mask < val)<<4 |
		b2u64((in[3]>>8)&mask < val)<<5 |
		b2u64((in[3]>>48)&mask|
			(in[4]<<16)&mask < val)<<6 |
		b2u64((in[4]>>24)&mask < val)<<7 |
		b2u64((in[5]>>0)&mask < val)<<8 |
		b2u64((in[5]>>40)&mask|
			(in[6]<<24)&mask < val)<<9 |
		b2u64((in[6]>>16)&mask < val)<<10 |
		b2u64((in[6]>>56)&mask|
			(in[7]<<8)&mask < val)<<11 |
		b2u64((in[7]>>32)&mask|
			(in[8]<<32)&mask < val)<<12 |
		b2u64((in[8]>>8)&mask < val)<<13 |
		b2u64((in[8]>>48)&mask|
			(in[9]<<16)&mask < val)<<14 |
		b2u64((in[9]>>24)&mask < val)<<15 |
		b2u64((in[10]>>0)&mask < val)<<16 |
		b2u64((in[10]>>40)&mask|
			(in[11]<<24)&mask < val)<<17 |
		b2u64((in[11]>>16)&mask < val)<<18 |
		b2u64((in[11]>>56)&mask|
			(in[12]<<8)&mask < val)<<19 |
		b2u64((in[12]>>32)&mask|
			(in[13]<<32)&mask < val)<<20 |
		b2u64((in[13]>>8)&mask < val)<<21 |
		b2u64((in[13]>>48)&mask|
			(in[14]<<16)&mask < val)<<22 |
		b2u64((in[14]>>24)&mask < val)<<23 |
		b2u64((in[15]>>0)&mask < val)<<24 |
		b2u64((in[15]>>40)&mask|
			(in[16]<<24)&mask < val)<<25 |
		b2u64((in[16]>>16)&mask < val)<<26 |
		b2u64((in[16]>>56)&mask|
			(in[17]<<8)&mask < val)<<27 |
		b2u64((in[17]>>32)&mask|
			(in[18]<<32)&mask < val)<<28 |
		b2u64((in[18]>>8)&mask < val)<<29 |
		b2u64((in[18]>>48)&mask|
			(in[19]<<16)&mask < val)<<30 |
		b2u64((in[19]>>24)&mask < val)<<31 |
		b2u64((in[20]>>0)&mask < val)<<32 |
		b2u64((in[20]>>40)&mask|
			(in[21]<<24)&mask < val)<<33 |
		b2u64((in[21]>>16)&mask < val)<<34 |
		b2u64((in[21]>>56)&mask|
			(in[22]<<8)&mask < val)<<35 |
		b2u64((in[22]>>32)&mask|
			(in[23]<<32)&mask < val)<<36 |
		b2u64((in[23]>>8)&mask < val)<<37 |
		b2u64((in[23]>>48)&mask|
			(in[24]<<16)&mask < val)<<38 |
		b2u64((in[24]>>24)&mask < val)<<39 |
		b2u64((in[25]>>0)&mask < val)<<40 |
		b2u64((in[25]>>40)&mask|
			(in[26]<<24)&mask < val)<<41 |
		b2u64((in[26]>>16)&mask < val)<<42 |
		b2u64((in[26]>>56)&mask|
			(in[27]<<8)&mask < val)<<43 |
		b2u64((in[27]>>32)&mask|
			(in[28]<<32)&mask < val)<<44 |
		b2u64((in[28]>>8)&mask < val)<<45 |
		b2u64((in[28]>>48)&mask|
			(in[29]<<16)&mask < val)<<46 |
		b2u64((in[29]>>24)&mask < val)<<47 |
		b2u64((in[30]>>0)&mask < val)<<48 |
		b2u64((in[30]>>40)&mask|
			(in[31]<<24)&mask < val)<<49 |
		b2u64((in[31]>>16)&mask < val)<<50 |
		b2u64((in[31]>>56)&mask|
			(in[32]<<8)&mask < val)<<51 |
		b2u64((in[32]>>32)&mask|
			(in[33]<<32)&mask < val)<<52 |
		b2u64((in[33]>>8)&mask < val)<<53 |
		b2u64((in[33]>>48)&mask|
			(in[34]<<16)&mask < val)<<54 |
		b2u64((in[34]>>24)&mask < val)<<55 |
		b2u64((in[35]>>0)&mask < val)<<56 |
		b2u64((in[35]>>40)&mask|
			(in[36]<<24)&mask < val)<<57 |
		b2u64((in[36]>>16)&mask < val)<<58 |
		b2u64((in[36]>>56)&mask|
			(in[37]<<8)&mask < val)<<59 |
		b2u64((in[37]>>32)&mask|
			(in[38]<<32)&mask < val)<<60 |
		b2u64((in[38]>>8)&mask < val)<<61 |
		b2u64((in[38]>>48)&mask|
			(in[39]<<16)&mask < val)<<62 |
		b2u64((in[39]>>24)&mask < val)<<63)

}
func cmp_bp_41_lt(in *[41]uint64, val uint64) uint64 {
	mask := uint64((1 << 41) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>41)&mask|
			(in[1]<<23)&mask < val)<<1 |
		b2u64((in[1]>>18)&mask < val)<<2 |
		b2u64((in[1]>>59)&mask|
			(in[2]<<5)&mask < val)<<3 |
		b2u64((in[2]>>36)&mask|
			(in[3]<<28)&mask < val)<<4 |
		b2u64((in[3]>>13)&mask < val)<<5 |
		b2u64((in[3]>>54)&mask|
			(in[4]<<10)&mask < val)<<6 |
		b2u64((in[4]>>31)&mask|
			(in[5]<<33)&mask < val)<<7 |
		b2u64((in[5]>>8)&mask < val)<<8 |
		b2u64((in[5]>>49)&mask|
			(in[6]<<15)&mask < val)<<9 |
		b2u64((in[6]>>26)&mask|
			(in[7]<<38)&mask < val)<<10 |
		b2u64((in[7]>>3)&mask < val)<<11 |
		b2u64((in[7]>>44)&mask|
			(in[8]<<20)&mask < val)<<12 |
		b2u64((in[8]>>21)&mask < val)<<13 |
		b2u64((in[8]>>62)&mask|
			(in[9]<<2)&mask < val)<<14 |
		b2u64((in[9]>>39)&mask|
			(in[10]<<25)&mask < val)<<15 |
		b2u64((in[10]>>16)&mask < val)<<16 |
		b2u64((in[10]>>57)&mask|
			(in[11]<<7)&mask < val)<<17 |
		b2u64((in[11]>>34)&mask|
			(in[12]<<30)&mask < val)<<18 |
		b2u64((in[12]>>11)&mask < val)<<19 |
		b2u64((in[12]>>52)&mask|
			(in[13]<<12)&mask < val)<<20 |
		b2u64((in[13]>>29)&mask|
			(in[14]<<35)&mask < val)<<21 |
		b2u64((in[14]>>6)&mask < val)<<22 |
		b2u64((in[14]>>47)&mask|
			(in[15]<<17)&mask < val)<<23 |
		b2u64((in[15]>>24)&mask|
			(in[16]<<40)&mask < val)<<24 |
		b2u64((in[16]>>1)&mask < val)<<25 |
		b2u64((in[16]>>42)&mask|
			(in[17]<<22)&mask < val)<<26 |
		b2u64((in[17]>>19)&mask < val)<<27 |
		b2u64((in[17]>>60)&mask|
			(in[18]<<4)&mask < val)<<28 |
		b2u64((in[18]>>37)&mask|
			(in[19]<<27)&mask < val)<<29 |
		b2u64((in[19]>>14)&mask < val)<<30 |
		b2u64((in[19]>>55)&mask|
			(in[20]<<9)&mask < val)<<31 |
		b2u64((in[20]>>32)&mask|
			(in[21]<<32)&mask < val)<<32 |
		b2u64((in[21]>>9)&mask < val)<<33 |
		b2u64((in[21]>>50)&mask|
			(in[22]<<14)&mask < val)<<34 |
		b2u64((in[22]>>27)&mask|
			(in[23]<<37)&mask < val)<<35 |
		b2u64((in[23]>>4)&mask < val)<<36 |
		b2u64((in[23]>>45)&mask|
			(in[24]<<19)&mask < val)<<37 |
		b2u64((in[24]>>22)&mask < val)<<38 |
		b2u64((in[24]>>63)&mask|
			(in[25]<<1)&mask < val)<<39 |
		b2u64((in[25]>>40)&mask|
			(in[26]<<24)&mask < val)<<40 |
		b2u64((in[26]>>17)&mask < val)<<41 |
		b2u64((in[26]>>58)&mask|
			(in[27]<<6)&mask < val)<<42 |
		b2u64((in[27]>>35)&mask|
			(in[28]<<29)&mask < val)<<43 |
		b2u64((in[28]>>12)&mask < val)<<44 |
		b2u64((in[28]>>53)&mask|
			(in[29]<<11)&mask < val)<<45 |
		b2u64((in[29]>>30)&mask|
			(in[30]<<34)&mask < val)<<46 |
		b2u64((in[30]>>7)&mask < val)<<47 |
		b2u64((in[30]>>48)&mask|
			(in[31]<<16)&mask < val)<<48 |
		b2u64((in[31]>>25)&mask|
			(in[32]<<39)&mask < val)<<49 |
		b2u64((in[32]>>2)&mask < val)<<50 |
		b2u64((in[32]>>43)&mask|
			(in[33]<<21)&mask < val)<<51 |
		b2u64((in[33]>>20)&mask < val)<<52 |
		b2u64((in[33]>>61)&mask|
			(in[34]<<3)&mask < val)<<53 |
		b2u64((in[34]>>38)&mask|
			(in[35]<<26)&mask < val)<<54 |
		b2u64((in[35]>>15)&mask < val)<<55 |
		b2u64((in[35]>>56)&mask|
			(in[36]<<8)&mask < val)<<56 |
		b2u64((in[36]>>33)&mask|
			(in[37]<<31)&mask < val)<<57 |
		b2u64((in[37]>>10)&mask < val)<<58 |
		b2u64((in[37]>>51)&mask|
			(in[38]<<13)&mask < val)<<59 |
		b2u64((in[38]>>28)&mask|
			(in[39]<<36)&mask < val)<<60 |
		b2u64((in[39]>>5)&mask < val)<<61 |
		b2u64((in[39]>>46)&mask|
			(in[40]<<18)&mask < val)<<62 |
		b2u64((in[40]>>23)&mask < val)<<63)

}
func cmp_bp_42_lt(in *[42]uint64, val uint64) uint64 {
	mask := uint64((1 << 42) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>42)&mask|
			(in[1]<<22)&mask < val)<<1 |
		b2u64((in[1]>>20)&mask < val)<<2 |
		b2u64((in[1]>>62)&mask|
			(in[2]<<2)&mask < val)<<3 |
		b2u64((in[2]>>40)&mask|
			(in[3]<<24)&mask < val)<<4 |
		b2u64((in[3]>>18)&mask < val)<<5 |
		b2u64((in[3]>>60)&mask|
			(in[4]<<4)&mask < val)<<6 |
		b2u64((in[4]>>38)&mask|
			(in[5]<<26)&mask < val)<<7 |
		b2u64((in[5]>>16)&mask < val)<<8 |
		b2u64((in[5]>>58)&mask|
			(in[6]<<6)&mask < val)<<9 |
		b2u64((in[6]>>36)&mask|
			(in[7]<<28)&mask < val)<<10 |
		b2u64((in[7]>>14)&mask < val)<<11 |
		b2u64((in[7]>>56)&mask|
			(in[8]<<8)&mask < val)<<12 |
		b2u64((in[8]>>34)&mask|
			(in[9]<<30)&mask < val)<<13 |
		b2u64((in[9]>>12)&mask < val)<<14 |
		b2u64((in[9]>>54)&mask|
			(in[10]<<10)&mask < val)<<15 |
		b2u64((in[10]>>32)&mask|
			(in[11]<<32)&mask < val)<<16 |
		b2u64((in[11]>>10)&mask < val)<<17 |
		b2u64((in[11]>>52)&mask|
			(in[12]<<12)&mask < val)<<18 |
		b2u64((in[12]>>30)&mask|
			(in[13]<<34)&mask < val)<<19 |
		b2u64((in[13]>>8)&mask < val)<<20 |
		b2u64((in[13]>>50)&mask|
			(in[14]<<14)&mask < val)<<21 |
		b2u64((in[14]>>28)&mask|
			(in[15]<<36)&mask < val)<<22 |
		b2u64((in[15]>>6)&mask < val)<<23 |
		b2u64((in[15]>>48)&mask|
			(in[16]<<16)&mask < val)<<24 |
		b2u64((in[16]>>26)&mask|
			(in[17]<<38)&mask < val)<<25 |
		b2u64((in[17]>>4)&mask < val)<<26 |
		b2u64((in[17]>>46)&mask|
			(in[18]<<18)&mask < val)<<27 |
		b2u64((in[18]>>24)&mask|
			(in[19]<<40)&mask < val)<<28 |
		b2u64((in[19]>>2)&mask < val)<<29 |
		b2u64((in[19]>>44)&mask|
			(in[20]<<20)&mask < val)<<30 |
		b2u64((in[20]>>22)&mask < val)<<31 |
		b2u64((in[21]>>0)&mask < val)<<32 |
		b2u64((in[21]>>42)&mask|
			(in[22]<<22)&mask < val)<<33 |
		b2u64((in[22]>>20)&mask < val)<<34 |
		b2u64((in[22]>>62)&mask|
			(in[23]<<2)&mask < val)<<35 |
		b2u64((in[23]>>40)&mask|
			(in[24]<<24)&mask < val)<<36 |
		b2u64((in[24]>>18)&mask < val)<<37 |
		b2u64((in[24]>>60)&mask|
			(in[25]<<4)&mask < val)<<38 |
		b2u64((in[25]>>38)&mask|
			(in[26]<<26)&mask < val)<<39 |
		b2u64((in[26]>>16)&mask < val)<<40 |
		b2u64((in[26]>>58)&mask|
			(in[27]<<6)&mask < val)<<41 |
		b2u64((in[27]>>36)&mask|
			(in[28]<<28)&mask < val)<<42 |
		b2u64((in[28]>>14)&mask < val)<<43 |
		b2u64((in[28]>>56)&mask|
			(in[29]<<8)&mask < val)<<44 |
		b2u64((in[29]>>34)&mask|
			(in[30]<<30)&mask < val)<<45 |
		b2u64((in[30]>>12)&mask < val)<<46 |
		b2u64((in[30]>>54)&mask|
			(in[31]<<10)&mask < val)<<47 |
		b2u64((in[31]>>32)&mask|
			(in[32]<<32)&mask < val)<<48 |
		b2u64((in[32]>>10)&mask < val)<<49 |
		b2u64((in[32]>>52)&mask|
			(in[33]<<12)&mask < val)<<50 |
		b2u64((in[33]>>30)&mask|
			(in[34]<<34)&mask < val)<<51 |
		b2u64((in[34]>>8)&mask < val)<<52 |
		b2u64((in[34]>>50)&mask|
			(in[35]<<14)&mask < val)<<53 |
		b2u64((in[35]>>28)&mask|
			(in[36]<<36)&mask < val)<<54 |
		b2u64((in[36]>>6)&mask < val)<<55 |
		b2u64((in[36]>>48)&mask|
			(in[37]<<16)&mask < val)<<56 |
		b2u64((in[37]>>26)&mask|
			(in[38]<<38)&mask < val)<<57 |
		b2u64((in[38]>>4)&mask < val)<<58 |
		b2u64((in[38]>>46)&mask|
			(in[39]<<18)&mask < val)<<59 |
		b2u64((in[39]>>24)&mask|
			(in[40]<<40)&mask < val)<<60 |
		b2u64((in[40]>>2)&mask < val)<<61 |
		b2u64((in[40]>>44)&mask|
			(in[41]<<20)&mask < val)<<62 |
		b2u64((in[41]>>22)&mask < val)<<63)

}
func cmp_bp_43_lt(in *[43]uint64, val uint64) uint64 {
	mask := uint64((1 << 43) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>43)&mask|
			(in[1]<<21)&mask < val)<<1 |
		b2u64((in[1]>>22)&mask|
			(in[2]<<42)&mask < val)<<2 |
		b2u64((in[2]>>1)&mask < val)<<3 |
		b2u64((in[2]>>44)&mask|
			(in[3]<<20)&mask < val)<<4 |
		b2u64((in[3]>>23)&mask|
			(in[4]<<41)&mask < val)<<5 |
		b2u64((in[4]>>2)&mask < val)<<6 |
		b2u64((in[4]>>45)&mask|
			(in[5]<<19)&mask < val)<<7 |
		b2u64((in[5]>>24)&mask|
			(in[6]<<40)&mask < val)<<8 |
		b2u64((in[6]>>3)&mask < val)<<9 |
		b2u64((in[6]>>46)&mask|
			(in[7]<<18)&mask < val)<<10 |
		b2u64((in[7]>>25)&mask|
			(in[8]<<39)&mask < val)<<11 |
		b2u64((in[8]>>4)&mask < val)<<12 |
		b2u64((in[8]>>47)&mask|
			(in[9]<<17)&mask < val)<<13 |
		b2u64((in[9]>>26)&mask|
			(in[10]<<38)&mask < val)<<14 |
		b2u64((in[10]>>5)&mask < val)<<15 |
		b2u64((in[10]>>48)&mask|
			(in[11]<<16)&mask < val)<<16 |
		b2u64((in[11]>>27)&mask|
			(in[12]<<37)&mask < val)<<17 |
		b2u64((in[12]>>6)&mask < val)<<18 |
		b2u64((in[12]>>49)&mask|
			(in[13]<<15)&mask < val)<<19 |
		b2u64((in[13]>>28)&mask|
			(in[14]<<36)&mask < val)<<20 |
		b2u64((in[14]>>7)&mask < val)<<21 |
		b2u64((in[14]>>50)&mask|
			(in[15]<<14)&mask < val)<<22 |
		b2u64((in[15]>>29)&mask|
			(in[16]<<35)&mask < val)<<23 |
		b2u64((in[16]>>8)&mask < val)<<24 |
		b2u64((in[16]>>51)&mask|
			(in[17]<<13)&mask < val)<<25 |
		b2u64((in[17]>>30)&mask|
			(in[18]<<34)&mask < val)<<26 |
		b2u64((in[18]>>9)&mask < val)<<27 |
		b2u64((in[18]>>52)&mask|
			(in[19]<<12)&mask < val)<<28 |
		b2u64((in[19]>>31)&mask|
			(in[20]<<33)&mask < val)<<29 |
		b2u64((in[20]>>10)&mask < val)<<30 |
		b2u64((in[20]>>53)&mask|
			(in[21]<<11)&mask < val)<<31 |
		b2u64((in[21]>>32)&mask|
			(in[22]<<32)&mask < val)<<32 |
		b2u64((in[22]>>11)&mask < val)<<33 |
		b2u64((in[22]>>54)&mask|
			(in[23]<<10)&mask < val)<<34 |
		b2u64((in[23]>>33)&mask|
			(in[24]<<31)&mask < val)<<35 |
		b2u64((in[24]>>12)&mask < val)<<36 |
		b2u64((in[24]>>55)&mask|
			(in[25]<<9)&mask < val)<<37 |
		b2u64((in[25]>>34)&mask|
			(in[26]<<30)&mask < val)<<38 |
		b2u64((in[26]>>13)&mask < val)<<39 |
		b2u64((in[26]>>56)&mask|
			(in[27]<<8)&mask < val)<<40 |
		b2u64((in[27]>>35)&mask|
			(in[28]<<29)&mask < val)<<41 |
		b2u64((in[28]>>14)&mask < val)<<42 |
		b2u64((in[28]>>57)&mask|
			(in[29]<<7)&mask < val)<<43 |
		b2u64((in[29]>>36)&mask|
			(in[30]<<28)&mask < val)<<44 |
		b2u64((in[30]>>15)&mask < val)<<45 |
		b2u64((in[30]>>58)&mask|
			(in[31]<<6)&mask < val)<<46 |
		b2u64((in[31]>>37)&mask|
			(in[32]<<27)&mask < val)<<47 |
		b2u64((in[32]>>16)&mask < val)<<48 |
		b2u64((in[32]>>59)&mask|
			(in[33]<<5)&mask < val)<<49 |
		b2u64((in[33]>>38)&mask|
			(in[34]<<26)&mask < val)<<50 |
		b2u64((in[34]>>17)&mask < val)<<51 |
		b2u64((in[34]>>60)&mask|
			(in[35]<<4)&mask < val)<<52 |
		b2u64((in[35]>>39)&mask|
			(in[36]<<25)&mask < val)<<53 |
		b2u64((in[36]>>18)&mask < val)<<54 |
		b2u64((in[36]>>61)&mask|
			(in[37]<<3)&mask < val)<<55 |
		b2u64((in[37]>>40)&mask|
			(in[38]<<24)&mask < val)<<56 |
		b2u64((in[38]>>19)&mask < val)<<57 |
		b2u64((in[38]>>62)&mask|
			(in[39]<<2)&mask < val)<<58 |
		b2u64((in[39]>>41)&mask|
			(in[40]<<23)&mask < val)<<59 |
		b2u64((in[40]>>20)&mask < val)<<60 |
		b2u64((in[40]>>63)&mask|
			(in[41]<<1)&mask < val)<<61 |
		b2u64((in[41]>>42)&mask|
			(in[42]<<22)&mask < val)<<62 |
		b2u64((in[42]>>21)&mask < val)<<63)

}
func cmp_bp_44_lt(in *[44]uint64, val uint64) uint64 {
	mask := uint64((1 << 44) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>44)&mask|
			(in[1]<<20)&mask < val)<<1 |
		b2u64((in[1]>>24)&mask|
			(in[2]<<40)&mask < val)<<2 |
		b2u64((in[2]>>4)&mask < val)<<3 |
		b2u64((in[2]>>48)&mask|
			(in[3]<<16)&mask < val)<<4 |
		b2u64((in[3]>>28)&mask|
			(in[4]<<36)&mask < val)<<5 |
		b2u64((in[4]>>8)&mask < val)<<6 |
		b2u64((in[4]>>52)&mask|
			(in[5]<<12)&mask < val)<<7 |
		b2u64((in[5]>>32)&mask|
			(in[6]<<32)&mask < val)<<8 |
		b2u64((in[6]>>12)&mask < val)<<9 |
		b2u64((in[6]>>56)&mask|
			(in[7]<<8)&mask < val)<<10 |
		b2u64((in[7]>>36)&mask|
			(in[8]<<28)&mask < val)<<11 |
		b2u64((in[8]>>16)&mask < val)<<12 |
		b2u64((in[8]>>60)&mask|
			(in[9]<<4)&mask < val)<<13 |
		b2u64((in[9]>>40)&mask|
			(in[10]<<24)&mask < val)<<14 |
		b2u64((in[10]>>20)&mask < val)<<15 |
		b2u64((in[11]>>0)&mask < val)<<16 |
		b2u64((in[11]>>44)&mask|
			(in[12]<<20)&mask < val)<<17 |
		b2u64((in[12]>>24)&mask|
			(in[13]<<40)&mask < val)<<18 |
		b2u64((in[13]>>4)&mask < val)<<19 |
		b2u64((in[13]>>48)&mask|
			(in[14]<<16)&mask < val)<<20 |
		b2u64((in[14]>>28)&mask|
			(in[15]<<36)&mask < val)<<21 |
		b2u64((in[15]>>8)&mask < val)<<22 |
		b2u64((in[15]>>52)&mask|
			(in[16]<<12)&mask < val)<<23 |
		b2u64((in[16]>>32)&mask|
			(in[17]<<32)&mask < val)<<24 |
		b2u64((in[17]>>12)&mask < val)<<25 |
		b2u64((in[17]>>56)&mask|
			(in[18]<<8)&mask < val)<<26 |
		b2u64((in[18]>>36)&mask|
			(in[19]<<28)&mask < val)<<27 |
		b2u64((in[19]>>16)&mask < val)<<28 |
		b2u64((in[19]>>60)&mask|
			(in[20]<<4)&mask < val)<<29 |
		b2u64((in[20]>>40)&mask|
			(in[21]<<24)&mask < val)<<30 |
		b2u64((in[21]>>20)&mask < val)<<31 |
		b2u64((in[22]>>0)&mask < val)<<32 |
		b2u64((in[22]>>44)&mask|
			(in[23]<<20)&mask < val)<<33 |
		b2u64((in[23]>>24)&mask|
			(in[24]<<40)&mask < val)<<34 |
		b2u64((in[24]>>4)&mask < val)<<35 |
		b2u64((in[24]>>48)&mask|
			(in[25]<<16)&mask < val)<<36 |
		b2u64((in[25]>>28)&mask|
			(in[26]<<36)&mask < val)<<37 |
		b2u64((in[26]>>8)&mask < val)<<38 |
		b2u64((in[26]>>52)&mask|
			(in[27]<<12)&mask < val)<<39 |
		b2u64((in[27]>>32)&mask|
			(in[28]<<32)&mask < val)<<40 |
		b2u64((in[28]>>12)&mask < val)<<41 |
		b2u64((in[28]>>56)&mask|
			(in[29]<<8)&mask < val)<<42 |
		b2u64((in[29]>>36)&mask|
			(in[30]<<28)&mask < val)<<43 |
		b2u64((in[30]>>16)&mask < val)<<44 |
		b2u64((in[30]>>60)&mask|
			(in[31]<<4)&mask < val)<<45 |
		b2u64((in[31]>>40)&mask|
			(in[32]<<24)&mask < val)<<46 |
		b2u64((in[32]>>20)&mask < val)<<47 |
		b2u64((in[33]>>0)&mask < val)<<48 |
		b2u64((in[33]>>44)&mask|
			(in[34]<<20)&mask < val)<<49 |
		b2u64((in[34]>>24)&mask|
			(in[35]<<40)&mask < val)<<50 |
		b2u64((in[35]>>4)&mask < val)<<51 |
		b2u64((in[35]>>48)&mask|
			(in[36]<<16)&mask < val)<<52 |
		b2u64((in[36]>>28)&mask|
			(in[37]<<36)&mask < val)<<53 |
		b2u64((in[37]>>8)&mask < val)<<54 |
		b2u64((in[37]>>52)&mask|
			(in[38]<<12)&mask < val)<<55 |
		b2u64((in[38]>>32)&mask|
			(in[39]<<32)&mask < val)<<56 |
		b2u64((in[39]>>12)&mask < val)<<57 |
		b2u64((in[39]>>56)&mask|
			(in[40]<<8)&mask < val)<<58 |
		b2u64((in[40]>>36)&mask|
			(in[41]<<28)&mask < val)<<59 |
		b2u64((in[41]>>16)&mask < val)<<60 |
		b2u64((in[41]>>60)&mask|
			(in[42]<<4)&mask < val)<<61 |
		b2u64((in[42]>>40)&mask|
			(in[43]<<24)&mask < val)<<62 |
		b2u64((in[43]>>20)&mask < val)<<63)

}
func cmp_bp_45_lt(in *[45]uint64, val uint64) uint64 {
	mask := uint64((1 << 45) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>45)&mask|
			(in[1]<<19)&mask < val)<<1 |
		b2u64((in[1]>>26)&mask|
			(in[2]<<38)&mask < val)<<2 |
		b2u64((in[2]>>7)&mask < val)<<3 |
		b2u64((in[2]>>52)&mask|
			(in[3]<<12)&mask < val)<<4 |
		b2u64((in[3]>>33)&mask|
			(in[4]<<31)&mask < val)<<5 |
		b2u64((in[4]>>14)&mask < val)<<6 |
		b2u64((in[4]>>59)&mask|
			(in[5]<<5)&mask < val)<<7 |
		b2u64((in[5]>>40)&mask|
			(in[6]<<24)&mask < val)<<8 |
		b2u64((in[6]>>21)&mask|
			(in[7]<<43)&mask < val)<<9 |
		b2u64((in[7]>>2)&mask < val)<<10 |
		b2u64((in[7]>>47)&mask|
			(in[8]<<17)&mask < val)<<11 |
		b2u64((in[8]>>28)&mask|
			(in[9]<<36)&mask < val)<<12 |
		b2u64((in[9]>>9)&mask < val)<<13 |
		b2u64((in[9]>>54)&mask|
			(in[10]<<10)&mask < val)<<14 |
		b2u64((in[10]>>35)&mask|
			(in[11]<<29)&mask < val)<<15 |
		b2u64((in[11]>>16)&mask < val)<<16 |
		b2u64((in[11]>>61)&mask|
			(in[12]<<3)&mask < val)<<17 |
		b2u64((in[12]>>42)&mask|
			(in[13]<<22)&mask < val)<<18 |
		b2u64((in[13]>>23)&mask|
			(in[14]<<41)&mask < val)<<19 |
		b2u64((in[14]>>4)&mask < val)<<20 |
		b2u64((in[14]>>49)&mask|
			(in[15]<<15)&mask < val)<<21 |
		b2u64((in[15]>>30)&mask|
			(in[16]<<34)&mask < val)<<22 |
		b2u64((in[16]>>11)&mask < val)<<23 |
		b2u64((in[16]>>56)&mask|
			(in[17]<<8)&mask < val)<<24 |
		b2u64((in[17]>>37)&mask|
			(in[18]<<27)&mask < val)<<25 |
		b2u64((in[18]>>18)&mask < val)<<26 |
		b2u64((in[18]>>63)&mask|
			(in[19]<<1)&mask < val)<<27 |
		b2u64((in[19]>>44)&mask|
			(in[20]<<20)&mask < val)<<28 |
		b2u64((in[20]>>25)&mask|
			(in[21]<<39)&mask < val)<<29 |
		b2u64((in[21]>>6)&mask < val)<<30 |
		b2u64((in[21]>>51)&mask|
			(in[22]<<13)&mask < val)<<31 |
		b2u64((in[22]>>32)&mask|
			(in[23]<<32)&mask < val)<<32 |
		b2u64((in[23]>>13)&mask < val)<<33 |
		b2u64((in[23]>>58)&mask|
			(in[24]<<6)&mask < val)<<34 |
		b2u64((in[24]>>39)&mask|
			(in[25]<<25)&mask < val)<<35 |
		b2u64((in[25]>>20)&mask|
			(in[26]<<44)&mask < val)<<36 |
		b2u64((in[26]>>1)&mask < val)<<37 |
		b2u64((in[26]>>46)&mask|
			(in[27]<<18)&mask < val)<<38 |
		b2u64((in[27]>>27)&mask|
			(in[28]<<37)&mask < val)<<39 |
		b2u64((in[28]>>8)&mask < val)<<40 |
		b2u64((in[28]>>53)&mask|
			(in[29]<<11)&mask < val)<<41 |
		b2u64((in[29]>>34)&mask|
			(in[30]<<30)&mask < val)<<42 |
		b2u64((in[30]>>15)&mask < val)<<43 |
		b2u64((in[30]>>60)&mask|
			(in[31]<<4)&mask < val)<<44 |
		b2u64((in[31]>>41)&mask|
			(in[32]<<23)&mask < val)<<45 |
		b2u64((in[32]>>22)&mask|
			(in[33]<<42)&mask < val)<<46 |
		b2u64((in[33]>>3)&mask < val)<<47 |
		b2u64((in[33]>>48)&mask|
			(in[34]<<16)&mask < val)<<48 |
		b2u64((in[34]>>29)&mask|
			(in[35]<<35)&mask < val)<<49 |
		b2u64((in[35]>>10)&mask < val)<<50 |
		b2u64((in[35]>>55)&mask|
			(in[36]<<9)&mask < val)<<51 |
		b2u64((in[36]>>36)&mask|
			(in[37]<<28)&mask < val)<<52 |
		b2u64((in[37]>>17)&mask < val)<<53 |
		b2u64((in[37]>>62)&mask|
			(in[38]<<2)&mask < val)<<54 |
		b2u64((in[38]>>43)&mask|
			(in[39]<<21)&mask < val)<<55 |
		b2u64((in[39]>>24)&mask|
			(in[40]<<40)&mask < val)<<56 |
		b2u64((in[40]>>5)&mask < val)<<57 |
		b2u64((in[40]>>50)&mask|
			(in[41]<<14)&mask < val)<<58 |
		b2u64((in[41]>>31)&mask|
			(in[42]<<33)&mask < val)<<59 |
		b2u64((in[42]>>12)&mask < val)<<60 |
		b2u64((in[42]>>57)&mask|
			(in[43]<<7)&mask < val)<<61 |
		b2u64((in[43]>>38)&mask|
			(in[44]<<26)&mask < val)<<62 |
		b2u64((in[44]>>19)&mask < val)<<63)

}
func cmp_bp_46_lt(in *[46]uint64, val uint64) uint64 {
	mask := uint64((1 << 46) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>46)&mask|
			(in[1]<<18)&mask < val)<<1 |
		b2u64((in[1]>>28)&mask|
			(in[2]<<36)&mask < val)<<2 |
		b2u64((in[2]>>10)&mask < val)<<3 |
		b2u64((in[2]>>56)&mask|
			(in[3]<<8)&mask < val)<<4 |
		b2u64((in[3]>>38)&mask|
			(in[4]<<26)&mask < val)<<5 |
		b2u64((in[4]>>20)&mask|
			(in[5]<<44)&mask < val)<<6 |
		b2u64((in[5]>>2)&mask < val)<<7 |
		b2u64((in[5]>>48)&mask|
			(in[6]<<16)&mask < val)<<8 |
		b2u64((in[6]>>30)&mask|
			(in[7]<<34)&mask < val)<<9 |
		b2u64((in[7]>>12)&mask < val)<<10 |
		b2u64((in[7]>>58)&mask|
			(in[8]<<6)&mask < val)<<11 |
		b2u64((in[8]>>40)&mask|
			(in[9]<<24)&mask < val)<<12 |
		b2u64((in[9]>>22)&mask|
			(in[10]<<42)&mask < val)<<13 |
		b2u64((in[10]>>4)&mask < val)<<14 |
		b2u64((in[10]>>50)&mask|
			(in[11]<<14)&mask < val)<<15 |
		b2u64((in[11]>>32)&mask|
			(in[12]<<32)&mask < val)<<16 |
		b2u64((in[12]>>14)&mask < val)<<17 |
		b2u64((in[12]>>60)&mask|
			(in[13]<<4)&mask < val)<<18 |
		b2u64((in[13]>>42)&mask|
			(in[14]<<22)&mask < val)<<19 |
		b2u64((in[14]>>24)&mask|
			(in[15]<<40)&mask < val)<<20 |
		b2u64((in[15]>>6)&mask < val)<<21 |
		b2u64((in[15]>>52)&mask|
			(in[16]<<12)&mask < val)<<22 |
		b2u64((in[16]>>34)&mask|
			(in[17]<<30)&mask < val)<<23 |
		b2u64((in[17]>>16)&mask < val)<<24 |
		b2u64((in[17]>>62)&mask|
			(in[18]<<2)&mask < val)<<25 |
		b2u64((in[18]>>44)&mask|
			(in[19]<<20)&mask < val)<<26 |
		b2u64((in[19]>>26)&mask|
			(in[20]<<38)&mask < val)<<27 |
		b2u64((in[20]>>8)&mask < val)<<28 |
		b2u64((in[20]>>54)&mask|
			(in[21]<<10)&mask < val)<<29 |
		b2u64((in[21]>>36)&mask|
			(in[22]<<28)&mask < val)<<30 |
		b2u64((in[22]>>18)&mask < val)<<31 |
		b2u64((in[23]>>0)&mask < val)<<32 |
		b2u64((in[23]>>46)&mask|
			(in[24]<<18)&mask < val)<<33 |
		b2u64((in[24]>>28)&mask|
			(in[25]<<36)&mask < val)<<34 |
		b2u64((in[25]>>10)&mask < val)<<35 |
		b2u64((in[25]>>56)&mask|
			(in[26]<<8)&mask < val)<<36 |
		b2u64((in[26]>>38)&mask|
			(in[27]<<26)&mask < val)<<37 |
		b2u64((in[27]>>20)&mask|
			(in[28]<<44)&mask < val)<<38 |
		b2u64((in[28]>>2)&mask < val)<<39 |
		b2u64((in[28]>>48)&mask|
			(in[29]<<16)&mask < val)<<40 |
		b2u64((in[29]>>30)&mask|
			(in[30]<<34)&mask < val)<<41 |
		b2u64((in[30]>>12)&mask < val)<<42 |
		b2u64((in[30]>>58)&mask|
			(in[31]<<6)&mask < val)<<43 |
		b2u64((in[31]>>40)&mask|
			(in[32]<<24)&mask < val)<<44 |
		b2u64((in[32]>>22)&mask|
			(in[33]<<42)&mask < val)<<45 |
		b2u64((in[33]>>4)&mask < val)<<46 |
		b2u64((in[33]>>50)&mask|
			(in[34]<<14)&mask < val)<<47 |
		b2u64((in[34]>>32)&mask|
			(in[35]<<32)&mask < val)<<48 |
		b2u64((in[35]>>14)&mask < val)<<49 |
		b2u64((in[35]>>60)&mask|
			(in[36]<<4)&mask < val)<<50 |
		b2u64((in[36]>>42)&mask|
			(in[37]<<22)&mask < val)<<51 |
		b2u64((in[37]>>24)&mask|
			(in[38]<<40)&mask < val)<<52 |
		b2u64((in[38]>>6)&mask < val)<<53 |
		b2u64((in[38]>>52)&mask|
			(in[39]<<12)&mask < val)<<54 |
		b2u64((in[39]>>34)&mask|
			(in[40]<<30)&mask < val)<<55 |
		b2u64((in[40]>>16)&mask < val)<<56 |
		b2u64((in[40]>>62)&mask|
			(in[41]<<2)&mask < val)<<57 |
		b2u64((in[41]>>44)&mask|
			(in[42]<<20)&mask < val)<<58 |
		b2u64((in[42]>>26)&mask|
			(in[43]<<38)&mask < val)<<59 |
		b2u64((in[43]>>8)&mask < val)<<60 |
		b2u64((in[43]>>54)&mask|
			(in[44]<<10)&mask < val)<<61 |
		b2u64((in[44]>>36)&mask|
			(in[45]<<28)&mask < val)<<62 |
		b2u64((in[45]>>18)&mask < val)<<63)

}
func cmp_bp_47_lt(in *[47]uint64, val uint64) uint64 {
	mask := uint64((1 << 47) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>47)&mask|
			(in[1]<<17)&mask < val)<<1 |
		b2u64((in[1]>>30)&mask|
			(in[2]<<34)&mask < val)<<2 |
		b2u64((in[2]>>13)&mask < val)<<3 |
		b2u64((in[2]>>60)&mask|
			(in[3]<<4)&mask < val)<<4 |
		b2u64((in[3]>>43)&mask|
			(in[4]<<21)&mask < val)<<5 |
		b2u64((in[4]>>26)&mask|
			(in[5]<<38)&mask < val)<<6 |
		b2u64((in[5]>>9)&mask < val)<<7 |
		b2u64((in[5]>>56)&mask|
			(in[6]<<8)&mask < val)<<8 |
		b2u64((in[6]>>39)&mask|
			(in[7]<<25)&mask < val)<<9 |
		b2u64((in[7]>>22)&mask|
			(in[8]<<42)&mask < val)<<10 |
		b2u64((in[8]>>5)&mask < val)<<11 |
		b2u64((in[8]>>52)&mask|
			(in[9]<<12)&mask < val)<<12 |
		b2u64((in[9]>>35)&mask|
			(in[10]<<29)&mask < val)<<13 |
		b2u64((in[10]>>18)&mask|
			(in[11]<<46)&mask < val)<<14 |
		b2u64((in[11]>>1)&mask < val)<<15 |
		b2u64((in[11]>>48)&mask|
			(in[12]<<16)&mask < val)<<16 |
		b2u64((in[12]>>31)&mask|
			(in[13]<<33)&mask < val)<<17 |
		b2u64((in[13]>>14)&mask < val)<<18 |
		b2u64((in[13]>>61)&mask|
			(in[14]<<3)&mask < val)<<19 |
		b2u64((in[14]>>44)&mask|
			(in[15]<<20)&mask < val)<<20 |
		b2u64((in[15]>>27)&mask|
			(in[16]<<37)&mask < val)<<21 |
		b2u64((in[16]>>10)&mask < val)<<22 |
		b2u64((in[16]>>57)&mask|
			(in[17]<<7)&mask < val)<<23 |
		b2u64((in[17]>>40)&mask|
			(in[18]<<24)&mask < val)<<24 |
		b2u64((in[18]>>23)&mask|
			(in[19]<<41)&mask < val)<<25 |
		b2u64((in[19]>>6)&mask < val)<<26 |
		b2u64((in[19]>>53)&mask|
			(in[20]<<11)&mask < val)<<27 |
		b2u64((in[20]>>36)&mask|
			(in[21]<<28)&mask < val)<<28 |
		b2u64((in[21]>>19)&mask|
			(in[22]<<45)&mask < val)<<29 |
		b2u64((in[22]>>2)&mask < val)<<30 |
		b2u64((in[22]>>49)&mask|
			(in[23]<<15)&mask < val)<<31 |
		b2u64((in[23]>>32)&mask|
			(in[24]<<32)&mask < val)<<32 |
		b2u64((in[24]>>15)&mask < val)<<33 |
		b2u64((in[24]>>62)&mask|
			(in[25]<<2)&mask < val)<<34 |
		b2u64((in[25]>>45)&mask|
			(in[26]<<19)&mask < val)<<35 |
		b2u64((in[26]>>28)&mask|
			(in[27]<<36)&mask < val)<<36 |
		b2u64((in[27]>>11)&mask < val)<<37 |
		b2u64((in[27]>>58)&mask|
			(in[28]<<6)&mask < val)<<38 |
		b2u64((in[28]>>41)&mask|
			(in[29]<<23)&mask < val)<<39 |
		b2u64((in[29]>>24)&mask|
			(in[30]<<40)&mask < val)<<40 |
		b2u64((in[30]>>7)&mask < val)<<41 |
		b2u64((in[30]>>54)&mask|
			(in[31]<<10)&mask < val)<<42 |
		b2u64((in[31]>>37)&mask|
			(in[32]<<27)&mask < val)<<43 |
		b2u64((in[32]>>20)&mask|
			(in[33]<<44)&mask < val)<<44 |
		b2u64((in[33]>>3)&mask < val)<<45 |
		b2u64((in[33]>>50)&mask|
			(in[34]<<14)&mask < val)<<46 |
		b2u64((in[34]>>33)&mask|
			(in[35]<<31)&mask < val)<<47 |
		b2u64((in[35]>>16)&mask < val)<<48 |
		b2u64((in[35]>>63)&mask|
			(in[36]<<1)&mask < val)<<49 |
		b2u64((in[36]>>46)&mask|
			(in[37]<<18)&mask < val)<<50 |
		b2u64((in[37]>>29)&mask|
			(in[38]<<35)&mask < val)<<51 |
		b2u64((in[38]>>12)&mask < val)<<52 |
		b2u64((in[38]>>59)&mask|
			(in[39]<<5)&mask < val)<<53 |
		b2u64((in[39]>>42)&mask|
			(in[40]<<22)&mask < val)<<54 |
		b2u64((in[40]>>25)&mask|
			(in[41]<<39)&mask < val)<<55 |
		b2u64((in[41]>>8)&mask < val)<<56 |
		b2u64((in[41]>>55)&mask|
			(in[42]<<9)&mask < val)<<57 |
		b2u64((in[42]>>38)&mask|
			(in[43]<<26)&mask < val)<<58 |
		b2u64((in[43]>>21)&mask|
			(in[44]<<43)&mask < val)<<59 |
		b2u64((in[44]>>4)&mask < val)<<60 |
		b2u64((in[44]>>51)&mask|
			(in[45]<<13)&mask < val)<<61 |
		b2u64((in[45]>>34)&mask|
			(in[46]<<30)&mask < val)<<62 |
		b2u64((in[46]>>17)&mask < val)<<63)

}
func cmp_bp_48_lt(in *[48]uint64, val uint64) uint64 {
	mask := uint64((1 << 48) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>48)&mask|
			(in[1]<<16)&mask < val)<<1 |
		b2u64((in[1]>>32)&mask|
			(in[2]<<32)&mask < val)<<2 |
		b2u64((in[2]>>16)&mask < val)<<3 |
		b2u64((in[3]>>0)&mask < val)<<4 |
		b2u64((in[3]>>48)&mask|
			(in[4]<<16)&mask < val)<<5 |
		b2u64((in[4]>>32)&mask|
			(in[5]<<32)&mask < val)<<6 |
		b2u64((in[5]>>16)&mask < val)<<7 |
		b2u64((in[6]>>0)&mask < val)<<8 |
		b2u64((in[6]>>48)&mask|
			(in[7]<<16)&mask < val)<<9 |
		b2u64((in[7]>>32)&mask|
			(in[8]<<32)&mask < val)<<10 |
		b2u64((in[8]>>16)&mask < val)<<11 |
		b2u64((in[9]>>0)&mask < val)<<12 |
		b2u64((in[9]>>48)&mask|
			(in[10]<<16)&mask < val)<<13 |
		b2u64((in[10]>>32)&mask|
			(in[11]<<32)&mask < val)<<14 |
		b2u64((in[11]>>16)&mask < val)<<15 |
		b2u64((in[12]>>0)&mask < val)<<16 |
		b2u64((in[12]>>48)&mask|
			(in[13]<<16)&mask < val)<<17 |
		b2u64((in[13]>>32)&mask|
			(in[14]<<32)&mask < val)<<18 |
		b2u64((in[14]>>16)&mask < val)<<19 |
		b2u64((in[15]>>0)&mask < val)<<20 |
		b2u64((in[15]>>48)&mask|
			(in[16]<<16)&mask < val)<<21 |
		b2u64((in[16]>>32)&mask|
			(in[17]<<32)&mask < val)<<22 |
		b2u64((in[17]>>16)&mask < val)<<23 |
		b2u64((in[18]>>0)&mask < val)<<24 |
		b2u64((in[18]>>48)&mask|
			(in[19]<<16)&mask < val)<<25 |
		b2u64((in[19]>>32)&mask|
			(in[20]<<32)&mask < val)<<26 |
		b2u64((in[20]>>16)&mask < val)<<27 |
		b2u64((in[21]>>0)&mask < val)<<28 |
		b2u64((in[21]>>48)&mask|
			(in[22]<<16)&mask < val)<<29 |
		b2u64((in[22]>>32)&mask|
			(in[23]<<32)&mask < val)<<30 |
		b2u64((in[23]>>16)&mask < val)<<31 |
		b2u64((in[24]>>0)&mask < val)<<32 |
		b2u64((in[24]>>48)&mask|
			(in[25]<<16)&mask < val)<<33 |
		b2u64((in[25]>>32)&mask|
			(in[26]<<32)&mask < val)<<34 |
		b2u64((in[26]>>16)&mask < val)<<35 |
		b2u64((in[27]>>0)&mask < val)<<36 |
		b2u64((in[27]>>48)&mask|
			(in[28]<<16)&mask < val)<<37 |
		b2u64((in[28]>>32)&mask|
			(in[29]<<32)&mask < val)<<38 |
		b2u64((in[29]>>16)&mask < val)<<39 |
		b2u64((in[30]>>0)&mask < val)<<40 |
		b2u64((in[30]>>48)&mask|
			(in[31]<<16)&mask < val)<<41 |
		b2u64((in[31]>>32)&mask|
			(in[32]<<32)&mask < val)<<42 |
		b2u64((in[32]>>16)&mask < val)<<43 |
		b2u64((in[33]>>0)&mask < val)<<44 |
		b2u64((in[33]>>48)&mask|
			(in[34]<<16)&mask < val)<<45 |
		b2u64((in[34]>>32)&mask|
			(in[35]<<32)&mask < val)<<46 |
		b2u64((in[35]>>16)&mask < val)<<47 |
		b2u64((in[36]>>0)&mask < val)<<48 |
		b2u64((in[36]>>48)&mask|
			(in[37]<<16)&mask < val)<<49 |
		b2u64((in[37]>>32)&mask|
			(in[38]<<32)&mask < val)<<50 |
		b2u64((in[38]>>16)&mask < val)<<51 |
		b2u64((in[39]>>0)&mask < val)<<52 |
		b2u64((in[39]>>48)&mask|
			(in[40]<<16)&mask < val)<<53 |
		b2u64((in[40]>>32)&mask|
			(in[41]<<32)&mask < val)<<54 |
		b2u64((in[41]>>16)&mask < val)<<55 |
		b2u64((in[42]>>0)&mask < val)<<56 |
		b2u64((in[42]>>48)&mask|
			(in[43]<<16)&mask < val)<<57 |
		b2u64((in[43]>>32)&mask|
			(in[44]<<32)&mask < val)<<58 |
		b2u64((in[44]>>16)&mask < val)<<59 |
		b2u64((in[45]>>0)&mask < val)<<60 |
		b2u64((in[45]>>48)&mask|
			(in[46]<<16)&mask < val)<<61 |
		b2u64((in[46]>>32)&mask|
			(in[47]<<32)&mask < val)<<62 |
		b2u64((in[47]>>16)&mask < val)<<63)

}
func cmp_bp_49_lt(in *[49]uint64, val uint64) uint64 {
	mask := uint64((1 << 49) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>49)&mask|
			(in[1]<<15)&mask < val)<<1 |
		b2u64((in[1]>>34)&mask|
			(in[2]<<30)&mask < val)<<2 |
		b2u64((in[2]>>19)&mask|
			(in[3]<<45)&mask < val)<<3 |
		b2u64((in[3]>>4)&mask < val)<<4 |
		b2u64((in[3]>>53)&mask|
			(in[4]<<11)&mask < val)<<5 |
		b2u64((in[4]>>38)&mask|
			(in[5]<<26)&mask < val)<<6 |
		b2u64((in[5]>>23)&mask|
			(in[6]<<41)&mask < val)<<7 |
		b2u64((in[6]>>8)&mask < val)<<8 |
		b2u64((in[6]>>57)&mask|
			(in[7]<<7)&mask < val)<<9 |
		b2u64((in[7]>>42)&mask|
			(in[8]<<22)&mask < val)<<10 |
		b2u64((in[8]>>27)&mask|
			(in[9]<<37)&mask < val)<<11 |
		b2u64((in[9]>>12)&mask < val)<<12 |
		b2u64((in[9]>>61)&mask|
			(in[10]<<3)&mask < val)<<13 |
		b2u64((in[10]>>46)&mask|
			(in[11]<<18)&mask < val)<<14 |
		b2u64((in[11]>>31)&mask|
			(in[12]<<33)&mask < val)<<15 |
		b2u64((in[12]>>16)&mask|
			(in[13]<<48)&mask < val)<<16 |
		b2u64((in[13]>>1)&mask < val)<<17 |
		b2u64((in[13]>>50)&mask|
			(in[14]<<14)&mask < val)<<18 |
		b2u64((in[14]>>35)&mask|
			(in[15]<<29)&mask < val)<<19 |
		b2u64((in[15]>>20)&mask|
			(in[16]<<44)&mask < val)<<20 |
		b2u64((in[16]>>5)&mask < val)<<21 |
		b2u64((in[16]>>54)&mask|
			(in[17]<<10)&mask < val)<<22 |
		b2u64((in[17]>>39)&mask|
			(in[18]<<25)&mask < val)<<23 |
		b2u64((in[18]>>24)&mask|
			(in[19]<<40)&mask < val)<<24 |
		b2u64((in[19]>>9)&mask < val)<<25 |
		b2u64((in[19]>>58)&mask|
			(in[20]<<6)&mask < val)<<26 |
		b2u64((in[20]>>43)&mask|
			(in[21]<<21)&mask < val)<<27 |
		b2u64((in[21]>>28)&mask|
			(in[22]<<36)&mask < val)<<28 |
		b2u64((in[22]>>13)&mask < val)<<29 |
		b2u64((in[22]>>62)&mask|
			(in[23]<<2)&mask < val)<<30 |
		b2u64((in[23]>>47)&mask|
			(in[24]<<17)&mask < val)<<31 |
		b2u64((in[24]>>32)&mask|
			(in[25]<<32)&mask < val)<<32 |
		b2u64((in[25]>>17)&mask|
			(in[26]<<47)&mask < val)<<33 |
		b2u64((in[26]>>2)&mask < val)<<34 |
		b2u64((in[26]>>51)&mask|
			(in[27]<<13)&mask < val)<<35 |
		b2u64((in[27]>>36)&mask|
			(in[28]<<28)&mask < val)<<36 |
		b2u64((in[28]>>21)&mask|
			(in[29]<<43)&mask < val)<<37 |
		b2u64((in[29]>>6)&mask < val)<<38 |
		b2u64((in[29]>>55)&mask|
			(in[30]<<9)&mask < val)<<39 |
		b2u64((in[30]>>40)&mask|
			(in[31]<<24)&mask < val)<<40 |
		b2u64((in[31]>>25)&mask|
			(in[32]<<39)&mask < val)<<41 |
		b2u64((in[32]>>10)&mask < val)<<42 |
		b2u64((in[32]>>59)&mask|
			(in[33]<<5)&mask < val)<<43 |
		b2u64((in[33]>>44)&mask|
			(in[34]<<20)&mask < val)<<44 |
		b2u64((in[34]>>29)&mask|
			(in[35]<<35)&mask < val)<<45 |
		b2u64((in[35]>>14)&mask < val)<<46 |
		b2u64((in[35]>>63)&mask|
			(in[36]<<1)&mask < val)<<47 |
		b2u64((in[36]>>48)&mask|
			(in[37]<<16)&mask < val)<<48 |
		b2u64((in[37]>>33)&mask|
			(in[38]<<31)&mask < val)<<49 |
		b2u64((in[38]>>18)&mask|
			(in[39]<<46)&mask < val)<<50 |
		b2u64((in[39]>>3)&mask < val)<<51 |
		b2u64((in[39]>>52)&mask|
			(in[40]<<12)&mask < val)<<52 |
		b2u64((in[40]>>37)&mask|
			(in[41]<<27)&mask < val)<<53 |
		b2u64((in[41]>>22)&mask|
			(in[42]<<42)&mask < val)<<54 |
		b2u64((in[42]>>7)&mask < val)<<55 |
		b2u64((in[42]>>56)&mask|
			(in[43]<<8)&mask < val)<<56 |
		b2u64((in[43]>>41)&mask|
			(in[44]<<23)&mask < val)<<57 |
		b2u64((in[44]>>26)&mask|
			(in[45]<<38)&mask < val)<<58 |
		b2u64((in[45]>>11)&mask < val)<<59 |
		b2u64((in[45]>>60)&mask|
			(in[46]<<4)&mask < val)<<60 |
		b2u64((in[46]>>45)&mask|
			(in[47]<<19)&mask < val)<<61 |
		b2u64((in[47]>>30)&mask|
			(in[48]<<34)&mask < val)<<62 |
		b2u64((in[48]>>15)&mask < val)<<63)

}
func cmp_bp_50_lt(in *[50]uint64, val uint64) uint64 {
	mask := uint64((1 << 50) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>50)&mask|
			(in[1]<<14)&mask < val)<<1 |
		b2u64((in[1]>>36)&mask|
			(in[2]<<28)&mask < val)<<2 |
		b2u64((in[2]>>22)&mask|
			(in[3]<<42)&mask < val)<<3 |
		b2u64((in[3]>>8)&mask < val)<<4 |
		b2u64((in[3]>>58)&mask|
			(in[4]<<6)&mask < val)<<5 |
		b2u64((in[4]>>44)&mask|
			(in[5]<<20)&mask < val)<<6 |
		b2u64((in[5]>>30)&mask|
			(in[6]<<34)&mask < val)<<7 |
		b2u64((in[6]>>16)&mask|
			(in[7]<<48)&mask < val)<<8 |
		b2u64((in[7]>>2)&mask < val)<<9 |
		b2u64((in[7]>>52)&mask|
			(in[8]<<12)&mask < val)<<10 |
		b2u64((in[8]>>38)&mask|
			(in[9]<<26)&mask < val)<<11 |
		b2u64((in[9]>>24)&mask|
			(in[10]<<40)&mask < val)<<12 |
		b2u64((in[10]>>10)&mask < val)<<13 |
		b2u64((in[10]>>60)&mask|
			(in[11]<<4)&mask < val)<<14 |
		b2u64((in[11]>>46)&mask|
			(in[12]<<18)&mask < val)<<15 |
		b2u64((in[12]>>32)&mask|
			(in[13]<<32)&mask < val)<<16 |
		b2u64((in[13]>>18)&mask|
			(in[14]<<46)&mask < val)<<17 |
		b2u64((in[14]>>4)&mask < val)<<18 |
		b2u64((in[14]>>54)&mask|
			(in[15]<<10)&mask < val)<<19 |
		b2u64((in[15]>>40)&mask|
			(in[16]<<24)&mask < val)<<20 |
		b2u64((in[16]>>26)&mask|
			(in[17]<<38)&mask < val)<<21 |
		b2u64((in[17]>>12)&mask < val)<<22 |
		b2u64((in[17]>>62)&mask|
			(in[18]<<2)&mask < val)<<23 |
		b2u64((in[18]>>48)&mask|
			(in[19]<<16)&mask < val)<<24 |
		b2u64((in[19]>>34)&mask|
			(in[20]<<30)&mask < val)<<25 |
		b2u64((in[20]>>20)&mask|
			(in[21]<<44)&mask < val)<<26 |
		b2u64((in[21]>>6)&mask < val)<<27 |
		b2u64((in[21]>>56)&mask|
			(in[22]<<8)&mask < val)<<28 |
		b2u64((in[22]>>42)&mask|
			(in[23]<<22)&mask < val)<<29 |
		b2u64((in[23]>>28)&mask|
			(in[24]<<36)&mask < val)<<30 |
		b2u64((in[24]>>14)&mask < val)<<31 |
		b2u64((in[25]>>0)&mask < val)<<32 |
		b2u64((in[25]>>50)&mask|
			(in[26]<<14)&mask < val)<<33 |
		b2u64((in[26]>>36)&mask|
			(in[27]<<28)&mask < val)<<34 |
		b2u64((in[27]>>22)&mask|
			(in[28]<<42)&mask < val)<<35 |
		b2u64((in[28]>>8)&mask < val)<<36 |
		b2u64((in[28]>>58)&mask|
			(in[29]<<6)&mask < val)<<37 |
		b2u64((in[29]>>44)&mask|
			(in[30]<<20)&mask < val)<<38 |
		b2u64((in[30]>>30)&mask|
			(in[31]<<34)&mask < val)<<39 |
		b2u64((in[31]>>16)&mask|
			(in[32]<<48)&mask < val)<<40 |
		b2u64((in[32]>>2)&mask < val)<<41 |
		b2u64((in[32]>>52)&mask|
			(in[33]<<12)&mask < val)<<42 |
		b2u64((in[33]>>38)&mask|
			(in[34]<<26)&mask < val)<<43 |
		b2u64((in[34]>>24)&mask|
			(in[35]<<40)&mask < val)<<44 |
		b2u64((in[35]>>10)&mask < val)<<45 |
		b2u64((in[35]>>60)&mask|
			(in[36]<<4)&mask < val)<<46 |
		b2u64((in[36]>>46)&mask|
			(in[37]<<18)&mask < val)<<47 |
		b2u64((in[37]>>32)&mask|
			(in[38]<<32)&mask < val)<<48 |
		b2u64((in[38]>>18)&mask|
			(in[39]<<46)&mask < val)<<49 |
		b2u64((in[39]>>4)&mask < val)<<50 |
		b2u64((in[39]>>54)&mask|
			(in[40]<<10)&mask < val)<<51 |
		b2u64((in[40]>>40)&mask|
			(in[41]<<24)&mask < val)<<52 |
		b2u64((in[41]>>26)&mask|
			(in[42]<<38)&mask < val)<<53 |
		b2u64((in[42]>>12)&mask < val)<<54 |
		b2u64((in[42]>>62)&mask|
			(in[43]<<2)&mask < val)<<55 |
		b2u64((in[43]>>48)&mask|
			(in[44]<<16)&mask < val)<<56 |
		b2u64((in[44]>>34)&mask|
			(in[45]<<30)&mask < val)<<57 |
		b2u64((in[45]>>20)&mask|
			(in[46]<<44)&mask < val)<<58 |
		b2u64((in[46]>>6)&mask < val)<<59 |
		b2u64((in[46]>>56)&mask|
			(in[47]<<8)&mask < val)<<60 |
		b2u64((in[47]>>42)&mask|
			(in[48]<<22)&mask < val)<<61 |
		b2u64((in[48]>>28)&mask|
			(in[49]<<36)&mask < val)<<62 |
		b2u64((in[49]>>14)&mask < val)<<63)

}
func cmp_bp_51_lt(in *[51]uint64, val uint64) uint64 {
	mask := uint64((1 << 51) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>51)&mask|
			(in[1]<<13)&mask < val)<<1 |
		b2u64((in[1]>>38)&mask|
			(in[2]<<26)&mask < val)<<2 |
		b2u64((in[2]>>25)&mask|
			(in[3]<<39)&mask < val)<<3 |
		b2u64((in[3]>>12)&mask < val)<<4 |
		b2u64((in[3]>>63)&mask|
			(in[4]<<1)&mask < val)<<5 |
		b2u64((in[4]>>50)&mask|
			(in[5]<<14)&mask < val)<<6 |
		b2u64((in[5]>>37)&mask|
			(in[6]<<27)&mask < val)<<7 |
		b2u64((in[6]>>24)&mask|
			(in[7]<<40)&mask < val)<<8 |
		b2u64((in[7]>>11)&mask < val)<<9 |
		b2u64((in[7]>>62)&mask|
			(in[8]<<2)&mask < val)<<10 |
		b2u64((in[8]>>49)&mask|
			(in[9]<<15)&mask < val)<<11 |
		b2u64((in[9]>>36)&mask|
			(in[10]<<28)&mask < val)<<12 |
		b2u64((in[10]>>23)&mask|
			(in[11]<<41)&mask < val)<<13 |
		b2u64((in[11]>>10)&mask < val)<<14 |
		b2u64((in[11]>>61)&mask|
			(in[12]<<3)&mask < val)<<15 |
		b2u64((in[12]>>48)&mask|
			(in[13]<<16)&mask < val)<<16 |
		b2u64((in[13]>>35)&mask|
			(in[14]<<29)&mask < val)<<17 |
		b2u64((in[14]>>22)&mask|
			(in[15]<<42)&mask < val)<<18 |
		b2u64((in[15]>>9)&mask < val)<<19 |
		b2u64((in[15]>>60)&mask|
			(in[16]<<4)&mask < val)<<20 |
		b2u64((in[16]>>47)&mask|
			(in[17]<<17)&mask < val)<<21 |
		b2u64((in[17]>>34)&mask|
			(in[18]<<30)&mask < val)<<22 |
		b2u64((in[18]>>21)&mask|
			(in[19]<<43)&mask < val)<<23 |
		b2u64((in[19]>>8)&mask < val)<<24 |
		b2u64((in[19]>>59)&mask|
			(in[20]<<5)&mask < val)<<25 |
		b2u64((in[20]>>46)&mask|
			(in[21]<<18)&mask < val)<<26 |
		b2u64((in[21]>>33)&mask|
			(in[22]<<31)&mask < val)<<27 |
		b2u64((in[22]>>20)&mask|
			(in[23]<<44)&mask < val)<<28 |
		b2u64((in[23]>>7)&mask < val)<<29 |
		b2u64((in[23]>>58)&mask|
			(in[24]<<6)&mask < val)<<30 |
		b2u64((in[24]>>45)&mask|
			(in[25]<<19)&mask < val)<<31 |
		b2u64((in[25]>>32)&mask|
			(in[26]<<32)&mask < val)<<32 |
		b2u64((in[26]>>19)&mask|
			(in[27]<<45)&mask < val)<<33 |
		b2u64((in[27]>>6)&mask < val)<<34 |
		b2u64((in[27]>>57)&mask|
			(in[28]<<7)&mask < val)<<35 |
		b2u64((in[28]>>44)&mask|
			(in[29]<<20)&mask < val)<<36 |
		b2u64((in[29]>>31)&mask|
			(in[30]<<33)&mask < val)<<37 |
		b2u64((in[30]>>18)&mask|
			(in[31]<<46)&mask < val)<<38 |
		b2u64((in[31]>>5)&mask < val)<<39 |
		b2u64((in[31]>>56)&mask|
			(in[32]<<8)&mask < val)<<40 |
		b2u64((in[32]>>43)&mask|
			(in[33]<<21)&mask < val)<<41 |
		b2u64((in[33]>>30)&mask|
			(in[34]<<34)&mask < val)<<42 |
		b2u64((in[34]>>17)&mask|
			(in[35]<<47)&mask < val)<<43 |
		b2u64((in[35]>>4)&mask < val)<<44 |
		b2u64((in[35]>>55)&mask|
			(in[36]<<9)&mask < val)<<45 |
		b2u64((in[36]>>42)&mask|
			(in[37]<<22)&mask < val)<<46 |
		b2u64((in[37]>>29)&mask|
			(in[38]<<35)&mask < val)<<47 |
		b2u64((in[38]>>16)&mask|
			(in[39]<<48)&mask < val)<<48 |
		b2u64((in[39]>>3)&mask < val)<<49 |
		b2u64((in[39]>>54)&mask|
			(in[40]<<10)&mask < val)<<50 |
		b2u64((in[40]>>41)&mask|
			(in[41]<<23)&mask < val)<<51 |
		b2u64((in[41]>>28)&mask|
			(in[42]<<36)&mask < val)<<52 |
		b2u64((in[42]>>15)&mask|
			(in[43]<<49)&mask < val)<<53 |
		b2u64((in[43]>>2)&mask < val)<<54 |
		b2u64((in[43]>>53)&mask|
			(in[44]<<11)&mask < val)<<55 |
		b2u64((in[44]>>40)&mask|
			(in[45]<<24)&mask < val)<<56 |
		b2u64((in[45]>>27)&mask|
			(in[46]<<37)&mask < val)<<57 |
		b2u64((in[46]>>14)&mask|
			(in[47]<<50)&mask < val)<<58 |
		b2u64((in[47]>>1)&mask < val)<<59 |
		b2u64((in[47]>>52)&mask|
			(in[48]<<12)&mask < val)<<60 |
		b2u64((in[48]>>39)&mask|
			(in[49]<<25)&mask < val)<<61 |
		b2u64((in[49]>>26)&mask|
			(in[50]<<38)&mask < val)<<62 |
		b2u64((in[50]>>13)&mask < val)<<63)

}
func cmp_bp_52_lt(in *[52]uint64, val uint64) uint64 {
	mask := uint64((1 << 52) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>52)&mask|
			(in[1]<<12)&mask < val)<<1 |
		b2u64((in[1]>>40)&mask|
			(in[2]<<24)&mask < val)<<2 |
		b2u64((in[2]>>28)&mask|
			(in[3]<<36)&mask < val)<<3 |
		b2u64((in[3]>>16)&mask|
			(in[4]<<48)&mask < val)<<4 |
		b2u64((in[4]>>4)&mask < val)<<5 |
		b2u64((in[4]>>56)&mask|
			(in[5]<<8)&mask < val)<<6 |
		b2u64((in[5]>>44)&mask|
			(in[6]<<20)&mask < val)<<7 |
		b2u64((in[6]>>32)&mask|
			(in[7]<<32)&mask < val)<<8 |
		b2u64((in[7]>>20)&mask|
			(in[8]<<44)&mask < val)<<9 |
		b2u64((in[8]>>8)&mask < val)<<10 |
		b2u64((in[8]>>60)&mask|
			(in[9]<<4)&mask < val)<<11 |
		b2u64((in[9]>>48)&mask|
			(in[10]<<16)&mask < val)<<12 |
		b2u64((in[10]>>36)&mask|
			(in[11]<<28)&mask < val)<<13 |
		b2u64((in[11]>>24)&mask|
			(in[12]<<40)&mask < val)<<14 |
		b2u64((in[12]>>12)&mask < val)<<15 |
		b2u64((in[13]>>0)&mask < val)<<16 |
		b2u64((in[13]>>52)&mask|
			(in[14]<<12)&mask < val)<<17 |
		b2u64((in[14]>>40)&mask|
			(in[15]<<24)&mask < val)<<18 |
		b2u64((in[15]>>28)&mask|
			(in[16]<<36)&mask < val)<<19 |
		b2u64((in[16]>>16)&mask|
			(in[17]<<48)&mask < val)<<20 |
		b2u64((in[17]>>4)&mask < val)<<21 |
		b2u64((in[17]>>56)&mask|
			(in[18]<<8)&mask < val)<<22 |
		b2u64((in[18]>>44)&mask|
			(in[19]<<20)&mask < val)<<23 |
		b2u64((in[19]>>32)&mask|
			(in[20]<<32)&mask < val)<<24 |
		b2u64((in[20]>>20)&mask|
			(in[21]<<44)&mask < val)<<25 |
		b2u64((in[21]>>8)&mask < val)<<26 |
		b2u64((in[21]>>60)&mask|
			(in[22]<<4)&mask < val)<<27 |
		b2u64((in[22]>>48)&mask|
			(in[23]<<16)&mask < val)<<28 |
		b2u64((in[23]>>36)&mask|
			(in[24]<<28)&mask < val)<<29 |
		b2u64((in[24]>>24)&mask|
			(in[25]<<40)&mask < val)<<30 |
		b2u64((in[25]>>12)&mask < val)<<31 |
		b2u64((in[26]>>0)&mask < val)<<32 |
		b2u64((in[26]>>52)&mask|
			(in[27]<<12)&mask < val)<<33 |
		b2u64((in[27]>>40)&mask|
			(in[28]<<24)&mask < val)<<34 |
		b2u64((in[28]>>28)&mask|
			(in[29]<<36)&mask < val)<<35 |
		b2u64((in[29]>>16)&mask|
			(in[30]<<48)&mask < val)<<36 |
		b2u64((in[30]>>4)&mask < val)<<37 |
		b2u64((in[30]>>56)&mask|
			(in[31]<<8)&mask < val)<<38 |
		b2u64((in[31]>>44)&mask|
			(in[32]<<20)&mask < val)<<39 |
		b2u64((in[32]>>32)&mask|
			(in[33]<<32)&mask < val)<<40 |
		b2u64((in[33]>>20)&mask|
			(in[34]<<44)&mask < val)<<41 |
		b2u64((in[34]>>8)&mask < val)<<42 |
		b2u64((in[34]>>60)&mask|
			(in[35]<<4)&mask < val)<<43 |
		b2u64((in[35]>>48)&mask|
			(in[36]<<16)&mask < val)<<44 |
		b2u64((in[36]>>36)&mask|
			(in[37]<<28)&mask < val)<<45 |
		b2u64((in[37]>>24)&mask|
			(in[38]<<40)&mask < val)<<46 |
		b2u64((in[38]>>12)&mask < val)<<47 |
		b2u64((in[39]>>0)&mask < val)<<48 |
		b2u64((in[39]>>52)&mask|
			(in[40]<<12)&mask < val)<<49 |
		b2u64((in[40]>>40)&mask|
			(in[41]<<24)&mask < val)<<50 |
		b2u64((in[41]>>28)&mask|
			(in[42]<<36)&mask < val)<<51 |
		b2u64((in[42]>>16)&mask|
			(in[43]<<48)&mask < val)<<52 |
		b2u64((in[43]>>4)&mask < val)<<53 |
		b2u64((in[43]>>56)&mask|
			(in[44]<<8)&mask < val)<<54 |
		b2u64((in[44]>>44)&mask|
			(in[45]<<20)&mask < val)<<55 |
		b2u64((in[45]>>32)&mask|
			(in[46]<<32)&mask < val)<<56 |
		b2u64((in[46]>>20)&mask|
			(in[47]<<44)&mask < val)<<57 |
		b2u64((in[47]>>8)&mask < val)<<58 |
		b2u64((in[47]>>60)&mask|
			(in[48]<<4)&mask < val)<<59 |
		b2u64((in[48]>>48)&mask|
			(in[49]<<16)&mask < val)<<60 |
		b2u64((in[49]>>36)&mask|
			(in[50]<<28)&mask < val)<<61 |
		b2u64((in[50]>>24)&mask|
			(in[51]<<40)&mask < val)<<62 |
		b2u64((in[51]>>12)&mask < val)<<63)

}
func cmp_bp_53_lt(in *[53]uint64, val uint64) uint64 {
	mask := uint64((1 << 53) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>53)&mask|
			(in[1]<<11)&mask < val)<<1 |
		b2u64((in[1]>>42)&mask|
			(in[2]<<22)&mask < val)<<2 |
		b2u64((in[2]>>31)&mask|
			(in[3]<<33)&mask < val)<<3 |
		b2u64((in[3]>>20)&mask|
			(in[4]<<44)&mask < val)<<4 |
		b2u64((in[4]>>9)&mask < val)<<5 |
		b2u64((in[4]>>62)&mask|
			(in[5]<<2)&mask < val)<<6 |
		b2u64((in[5]>>51)&mask|
			(in[6]<<13)&mask < val)<<7 |
		b2u64((in[6]>>40)&mask|
			(in[7]<<24)&mask < val)<<8 |
		b2u64((in[7]>>29)&mask|
			(in[8]<<35)&mask < val)<<9 |
		b2u64((in[8]>>18)&mask|
			(in[9]<<46)&mask < val)<<10 |
		b2u64((in[9]>>7)&mask < val)<<11 |
		b2u64((in[9]>>60)&mask|
			(in[10]<<4)&mask < val)<<12 |
		b2u64((in[10]>>49)&mask|
			(in[11]<<15)&mask < val)<<13 |
		b2u64((in[11]>>38)&mask|
			(in[12]<<26)&mask < val)<<14 |
		b2u64((in[12]>>27)&mask|
			(in[13]<<37)&mask < val)<<15 |
		b2u64((in[13]>>16)&mask|
			(in[14]<<48)&mask < val)<<16 |
		b2u64((in[14]>>5)&mask < val)<<17 |
		b2u64((in[14]>>58)&mask|
			(in[15]<<6)&mask < val)<<18 |
		b2u64((in[15]>>47)&mask|
			(in[16]<<17)&mask < val)<<19 |
		b2u64((in[16]>>36)&mask|
			(in[17]<<28)&mask < val)<<20 |
		b2u64((in[17]>>25)&mask|
			(in[18]<<39)&mask < val)<<21 |
		b2u64((in[18]>>14)&mask|
			(in[19]<<50)&mask < val)<<22 |
		b2u64((in[19]>>3)&mask < val)<<23 |
		b2u64((in[19]>>56)&mask|
			(in[20]<<8)&mask < val)<<24 |
		b2u64((in[20]>>45)&mask|
			(in[21]<<19)&mask < val)<<25 |
		b2u64((in[21]>>34)&mask|
			(in[22]<<30)&mask < val)<<26 |
		b2u64((in[22]>>23)&mask|
			(in[23]<<41)&mask < val)<<27 |
		b2u64((in[23]>>12)&mask|
			(in[24]<<52)&mask < val)<<28 |
		b2u64((in[24]>>1)&mask < val)<<29 |
		b2u64((in[24]>>54)&mask|
			(in[25]<<10)&mask < val)<<30 |
		b2u64((in[25]>>43)&mask|
			(in[26]<<21)&mask < val)<<31 |
		b2u64((in[26]>>32)&mask|
			(in[27]<<32)&mask < val)<<32 |
		b2u64((in[27]>>21)&mask|
			(in[28]<<43)&mask < val)<<33 |
		b2u64((in[28]>>10)&mask < val)<<34 |
		b2u64((in[28]>>63)&mask|
			(in[29]<<1)&mask < val)<<35 |
		b2u64((in[29]>>52)&mask|
			(in[30]<<12)&mask < val)<<36 |
		b2u64((in[30]>>41)&mask|
			(in[31]<<23)&mask < val)<<37 |
		b2u64((in[31]>>30)&mask|
			(in[32]<<34)&mask < val)<<38 |
		b2u64((in[32]>>19)&mask|
			(in[33]<<45)&mask < val)<<39 |
		b2u64((in[33]>>8)&mask < val)<<40 |
		b2u64((in[33]>>61)&mask|
			(in[34]<<3)&mask < val)<<41 |
		b2u64((in[34]>>50)&mask|
			(in[35]<<14)&mask < val)<<42 |
		b2u64((in[35]>>39)&mask|
			(in[36]<<25)&mask < val)<<43 |
		b2u64((in[36]>>28)&mask|
			(in[37]<<36)&mask < val)<<44 |
		b2u64((in[37]>>17)&mask|
			(in[38]<<47)&mask < val)<<45 |
		b2u64((in[38]>>6)&mask < val)<<46 |
		b2u64((in[38]>>59)&mask|
			(in[39]<<5)&mask < val)<<47 |
		b2u64((in[39]>>48)&mask|
			(in[40]<<16)&mask < val)<<48 |
		b2u64((in[40]>>37)&mask|
			(in[41]<<27)&mask < val)<<49 |
		b2u64((in[41]>>26)&mask|
			(in[42]<<38)&mask < val)<<50 |
		b2u64((in[42]>>15)&mask|
			(in[43]<<49)&mask < val)<<51 |
		b2u64((in[43]>>4)&mask < val)<<52 |
		b2u64((in[43]>>57)&mask|
			(in[44]<<7)&mask < val)<<53 |
		b2u64((in[44]>>46)&mask|
			(in[45]<<18)&mask < val)<<54 |
		b2u64((in[45]>>35)&mask|
			(in[46]<<29)&mask < val)<<55 |
		b2u64((in[46]>>24)&mask|
			(in[47]<<40)&mask < val)<<56 |
		b2u64((in[47]>>13)&mask|
			(in[48]<<51)&mask < val)<<57 |
		b2u64((in[48]>>2)&mask < val)<<58 |
		b2u64((in[48]>>55)&mask|
			(in[49]<<9)&mask < val)<<59 |
		b2u64((in[49]>>44)&mask|
			(in[50]<<20)&mask < val)<<60 |
		b2u64((in[50]>>33)&mask|
			(in[51]<<31)&mask < val)<<61 |
		b2u64((in[51]>>22)&mask|
			(in[52]<<42)&mask < val)<<62 |
		b2u64((in[52]>>11)&mask < val)<<63)

}
func cmp_bp_54_lt(in *[54]uint64, val uint64) uint64 {
	mask := uint64((1 << 54) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>54)&mask|
			(in[1]<<10)&mask < val)<<1 |
		b2u64((in[1]>>44)&mask|
			(in[2]<<20)&mask < val)<<2 |
		b2u64((in[2]>>34)&mask|
			(in[3]<<30)&mask < val)<<3 |
		b2u64((in[3]>>24)&mask|
			(in[4]<<40)&mask < val)<<4 |
		b2u64((in[4]>>14)&mask|
			(in[5]<<50)&mask < val)<<5 |
		b2u64((in[5]>>4)&mask < val)<<6 |
		b2u64((in[5]>>58)&mask|
			(in[6]<<6)&mask < val)<<7 |
		b2u64((in[6]>>48)&mask|
			(in[7]<<16)&mask < val)<<8 |
		b2u64((in[7]>>38)&mask|
			(in[8]<<26)&mask < val)<<9 |
		b2u64((in[8]>>28)&mask|
			(in[9]<<36)&mask < val)<<10 |
		b2u64((in[9]>>18)&mask|
			(in[10]<<46)&mask < val)<<11 |
		b2u64((in[10]>>8)&mask < val)<<12 |
		b2u64((in[10]>>62)&mask|
			(in[11]<<2)&mask < val)<<13 |
		b2u64((in[11]>>52)&mask|
			(in[12]<<12)&mask < val)<<14 |
		b2u64((in[12]>>42)&mask|
			(in[13]<<22)&mask < val)<<15 |
		b2u64((in[13]>>32)&mask|
			(in[14]<<32)&mask < val)<<16 |
		b2u64((in[14]>>22)&mask|
			(in[15]<<42)&mask < val)<<17 |
		b2u64((in[15]>>12)&mask|
			(in[16]<<52)&mask < val)<<18 |
		b2u64((in[16]>>2)&mask < val)<<19 |
		b2u64((in[16]>>56)&mask|
			(in[17]<<8)&mask < val)<<20 |
		b2u64((in[17]>>46)&mask|
			(in[18]<<18)&mask < val)<<21 |
		b2u64((in[18]>>36)&mask|
			(in[19]<<28)&mask < val)<<22 |
		b2u64((in[19]>>26)&mask|
			(in[20]<<38)&mask < val)<<23 |
		b2u64((in[20]>>16)&mask|
			(in[21]<<48)&mask < val)<<24 |
		b2u64((in[21]>>6)&mask < val)<<25 |
		b2u64((in[21]>>60)&mask|
			(in[22]<<4)&mask < val)<<26 |
		b2u64((in[22]>>50)&mask|
			(in[23]<<14)&mask < val)<<27 |
		b2u64((in[23]>>40)&mask|
			(in[24]<<24)&mask < val)<<28 |
		b2u64((in[24]>>30)&mask|
			(in[25]<<34)&mask < val)<<29 |
		b2u64((in[25]>>20)&mask|
			(in[26]<<44)&mask < val)<<30 |
		b2u64((in[26]>>10)&mask < val)<<31 |
		b2u64((in[27]>>0)&mask < val)<<32 |
		b2u64((in[27]>>54)&mask|
			(in[28]<<10)&mask < val)<<33 |
		b2u64((in[28]>>44)&mask|
			(in[29]<<20)&mask < val)<<34 |
		b2u64((in[29]>>34)&mask|
			(in[30]<<30)&mask < val)<<35 |
		b2u64((in[30]>>24)&mask|
			(in[31]<<40)&mask < val)<<36 |
		b2u64((in[31]>>14)&mask|
			(in[32]<<50)&mask < val)<<37 |
		b2u64((in[32]>>4)&mask < val)<<38 |
		b2u64((in[32]>>58)&mask|
			(in[33]<<6)&mask < val)<<39 |
		b2u64((in[33]>>48)&mask|
			(in[34]<<16)&mask < val)<<40 |
		b2u64((in[34]>>38)&mask|
			(in[35]<<26)&mask < val)<<41 |
		b2u64((in[35]>>28)&mask|
			(in[36]<<36)&mask < val)<<42 |
		b2u64((in[36]>>18)&mask|
			(in[37]<<46)&mask < val)<<43 |
		b2u64((in[37]>>8)&mask < val)<<44 |
		b2u64((in[37]>>62)&mask|
			(in[38]<<2)&mask < val)<<45 |
		b2u64((in[38]>>52)&mask|
			(in[39]<<12)&mask < val)<<46 |
		b2u64((in[39]>>42)&mask|
			(in[40]<<22)&mask < val)<<47 |
		b2u64((in[40]>>32)&mask|
			(in[41]<<32)&mask < val)<<48 |
		b2u64((in[41]>>22)&mask|
			(in[42]<<42)&mask < val)<<49 |
		b2u64((in[42]>>12)&mask|
			(in[43]<<52)&mask < val)<<50 |
		b2u64((in[43]>>2)&mask < val)<<51 |
		b2u64((in[43]>>56)&mask|
			(in[44]<<8)&mask < val)<<52 |
		b2u64((in[44]>>46)&mask|
			(in[45]<<18)&mask < val)<<53 |
		b2u64((in[45]>>36)&mask|
			(in[46]<<28)&mask < val)<<54 |
		b2u64((in[46]>>26)&mask|
			(in[47]<<38)&mask < val)<<55 |
		b2u64((in[47]>>16)&mask|
			(in[48]<<48)&mask < val)<<56 |
		b2u64((in[48]>>6)&mask < val)<<57 |
		b2u64((in[48]>>60)&mask|
			(in[49]<<4)&mask < val)<<58 |
		b2u64((in[49]>>50)&mask|
			(in[50]<<14)&mask < val)<<59 |
		b2u64((in[50]>>40)&mask|
			(in[51]<<24)&mask < val)<<60 |
		b2u64((in[51]>>30)&mask|
			(in[52]<<34)&mask < val)<<61 |
		b2u64((in[52]>>20)&mask|
			(in[53]<<44)&mask < val)<<62 |
		b2u64((in[53]>>10)&mask < val)<<63)

}
func cmp_bp_55_lt(in *[55]uint64, val uint64) uint64 {
	mask := uint64((1 << 55) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>55)&mask|
			(in[1]<<9)&mask < val)<<1 |
		b2u64((in[1]>>46)&mask|
			(in[2]<<18)&mask < val)<<2 |
		b2u64((in[2]>>37)&mask|
			(in[3]<<27)&mask < val)<<3 |
		b2u64((in[3]>>28)&mask|
			(in[4]<<36)&mask < val)<<4 |
		b2u64((in[4]>>19)&mask|
			(in[5]<<45)&mask < val)<<5 |
		b2u64((in[5]>>10)&mask|
			(in[6]<<54)&mask < val)<<6 |
		b2u64((in[6]>>1)&mask < val)<<7 |
		b2u64((in[6]>>56)&mask|
			(in[7]<<8)&mask < val)<<8 |
		b2u64((in[7]>>47)&mask|
			(in[8]<<17)&mask < val)<<9 |
		b2u64((in[8]>>38)&mask|
			(in[9]<<26)&mask < val)<<10 |
		b2u64((in[9]>>29)&mask|
			(in[10]<<35)&mask < val)<<11 |
		b2u64((in[10]>>20)&mask|
			(in[11]<<44)&mask < val)<<12 |
		b2u64((in[11]>>11)&mask|
			(in[12]<<53)&mask < val)<<13 |
		b2u64((in[12]>>2)&mask < val)<<14 |
		b2u64((in[12]>>57)&mask|
			(in[13]<<7)&mask < val)<<15 |
		b2u64((in[13]>>48)&mask|
			(in[14]<<16)&mask < val)<<16 |
		b2u64((in[14]>>39)&mask|
			(in[15]<<25)&mask < val)<<17 |
		b2u64((in[15]>>30)&mask|
			(in[16]<<34)&mask < val)<<18 |
		b2u64((in[16]>>21)&mask|
			(in[17]<<43)&mask < val)<<19 |
		b2u64((in[17]>>12)&mask|
			(in[18]<<52)&mask < val)<<20 |
		b2u64((in[18]>>3)&mask < val)<<21 |
		b2u64((in[18]>>58)&mask|
			(in[19]<<6)&mask < val)<<22 |
		b2u64((in[19]>>49)&mask|
			(in[20]<<15)&mask < val)<<23 |
		b2u64((in[20]>>40)&mask|
			(in[21]<<24)&mask < val)<<24 |
		b2u64((in[21]>>31)&mask|
			(in[22]<<33)&mask < val)<<25 |
		b2u64((in[22]>>22)&mask|
			(in[23]<<42)&mask < val)<<26 |
		b2u64((in[23]>>13)&mask|
			(in[24]<<51)&mask < val)<<27 |
		b2u64((in[24]>>4)&mask < val)<<28 |
		b2u64((in[24]>>59)&mask|
			(in[25]<<5)&mask < val)<<29 |
		b2u64((in[25]>>50)&mask|
			(in[26]<<14)&mask < val)<<30 |
		b2u64((in[26]>>41)&mask|
			(in[27]<<23)&mask < val)<<31 |
		b2u64((in[27]>>32)&mask|
			(in[28]<<32)&mask < val)<<32 |
		b2u64((in[28]>>23)&mask|
			(in[29]<<41)&mask < val)<<33 |
		b2u64((in[29]>>14)&mask|
			(in[30]<<50)&mask < val)<<34 |
		b2u64((in[30]>>5)&mask < val)<<35 |
		b2u64((in[30]>>60)&mask|
			(in[31]<<4)&mask < val)<<36 |
		b2u64((in[31]>>51)&mask|
			(in[32]<<13)&mask < val)<<37 |
		b2u64((in[32]>>42)&mask|
			(in[33]<<22)&mask < val)<<38 |
		b2u64((in[33]>>33)&mask|
			(in[34]<<31)&mask < val)<<39 |
		b2u64((in[34]>>24)&mask|
			(in[35]<<40)&mask < val)<<40 |
		b2u64((in[35]>>15)&mask|
			(in[36]<<49)&mask < val)<<41 |
		b2u64((in[36]>>6)&mask < val)<<42 |
		b2u64((in[36]>>61)&mask|
			(in[37]<<3)&mask < val)<<43 |
		b2u64((in[37]>>52)&mask|
			(in[38]<<12)&mask < val)<<44 |
		b2u64((in[38]>>43)&mask|
			(in[39]<<21)&mask < val)<<45 |
		b2u64((in[39]>>34)&mask|
			(in[40]<<30)&mask < val)<<46 |
		b2u64((in[40]>>25)&mask|
			(in[41]<<39)&mask < val)<<47 |
		b2u64((in[41]>>16)&mask|
			(in[42]<<48)&mask < val)<<48 |
		b2u64((in[42]>>7)&mask < val)<<49 |
		b2u64((in[42]>>62)&mask|
			(in[43]<<2)&mask < val)<<50 |
		b2u64((in[43]>>53)&mask|
			(in[44]<<11)&mask < val)<<51 |
		b2u64((in[44]>>44)&mask|
			(in[45]<<20)&mask < val)<<52 |
		b2u64((in[45]>>35)&mask|
			(in[46]<<29)&mask < val)<<53 |
		b2u64((in[46]>>26)&mask|
			(in[47]<<38)&mask < val)<<54 |
		b2u64((in[47]>>17)&mask|
			(in[48]<<47)&mask < val)<<55 |
		b2u64((in[48]>>8)&mask < val)<<56 |
		b2u64((in[48]>>63)&mask|
			(in[49]<<1)&mask < val)<<57 |
		b2u64((in[49]>>54)&mask|
			(in[50]<<10)&mask < val)<<58 |
		b2u64((in[50]>>45)&mask|
			(in[51]<<19)&mask < val)<<59 |
		b2u64((in[51]>>36)&mask|
			(in[52]<<28)&mask < val)<<60 |
		b2u64((in[52]>>27)&mask|
			(in[53]<<37)&mask < val)<<61 |
		b2u64((in[53]>>18)&mask|
			(in[54]<<46)&mask < val)<<62 |
		b2u64((in[54]>>9)&mask < val)<<63)

}
func cmp_bp_56_lt(in *[56]uint64, val uint64) uint64 {
	mask := uint64((1 << 56) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>56)&mask|
			(in[1]<<8)&mask < val)<<1 |
		b2u64((in[1]>>48)&mask|
			(in[2]<<16)&mask < val)<<2 |
		b2u64((in[2]>>40)&mask|
			(in[3]<<24)&mask < val)<<3 |
		b2u64((in[3]>>32)&mask|
			(in[4]<<32)&mask < val)<<4 |
		b2u64((in[4]>>24)&mask|
			(in[5]<<40)&mask < val)<<5 |
		b2u64((in[5]>>16)&mask|
			(in[6]<<48)&mask < val)<<6 |
		b2u64((in[6]>>8)&mask < val)<<7 |
		b2u64((in[7]>>0)&mask < val)<<8 |
		b2u64((in[7]>>56)&mask|
			(in[8]<<8)&mask < val)<<9 |
		b2u64((in[8]>>48)&mask|
			(in[9]<<16)&mask < val)<<10 |
		b2u64((in[9]>>40)&mask|
			(in[10]<<24)&mask < val)<<11 |
		b2u64((in[10]>>32)&mask|
			(in[11]<<32)&mask < val)<<12 |
		b2u64((in[11]>>24)&mask|
			(in[12]<<40)&mask < val)<<13 |
		b2u64((in[12]>>16)&mask|
			(in[13]<<48)&mask < val)<<14 |
		b2u64((in[13]>>8)&mask < val)<<15 |
		b2u64((in[14]>>0)&mask < val)<<16 |
		b2u64((in[14]>>56)&mask|
			(in[15]<<8)&mask < val)<<17 |
		b2u64((in[15]>>48)&mask|
			(in[16]<<16)&mask < val)<<18 |
		b2u64((in[16]>>40)&mask|
			(in[17]<<24)&mask < val)<<19 |
		b2u64((in[17]>>32)&mask|
			(in[18]<<32)&mask < val)<<20 |
		b2u64((in[18]>>24)&mask|
			(in[19]<<40)&mask < val)<<21 |
		b2u64((in[19]>>16)&mask|
			(in[20]<<48)&mask < val)<<22 |
		b2u64((in[20]>>8)&mask < val)<<23 |
		b2u64((in[21]>>0)&mask < val)<<24 |
		b2u64((in[21]>>56)&mask|
			(in[22]<<8)&mask < val)<<25 |
		b2u64((in[22]>>48)&mask|
			(in[23]<<16)&mask < val)<<26 |
		b2u64((in[23]>>40)&mask|
			(in[24]<<24)&mask < val)<<27 |
		b2u64((in[24]>>32)&mask|
			(in[25]<<32)&mask < val)<<28 |
		b2u64((in[25]>>24)&mask|
			(in[26]<<40)&mask < val)<<29 |
		b2u64((in[26]>>16)&mask|
			(in[27]<<48)&mask < val)<<30 |
		b2u64((in[27]>>8)&mask < val)<<31 |
		b2u64((in[28]>>0)&mask < val)<<32 |
		b2u64((in[28]>>56)&mask|
			(in[29]<<8)&mask < val)<<33 |
		b2u64((in[29]>>48)&mask|
			(in[30]<<16)&mask < val)<<34 |
		b2u64((in[30]>>40)&mask|
			(in[31]<<24)&mask < val)<<35 |
		b2u64((in[31]>>32)&mask|
			(in[32]<<32)&mask < val)<<36 |
		b2u64((in[32]>>24)&mask|
			(in[33]<<40)&mask < val)<<37 |
		b2u64((in[33]>>16)&mask|
			(in[34]<<48)&mask < val)<<38 |
		b2u64((in[34]>>8)&mask < val)<<39 |
		b2u64((in[35]>>0)&mask < val)<<40 |
		b2u64((in[35]>>56)&mask|
			(in[36]<<8)&mask < val)<<41 |
		b2u64((in[36]>>48)&mask|
			(in[37]<<16)&mask < val)<<42 |
		b2u64((in[37]>>40)&mask|
			(in[38]<<24)&mask < val)<<43 |
		b2u64((in[38]>>32)&mask|
			(in[39]<<32)&mask < val)<<44 |
		b2u64((in[39]>>24)&mask|
			(in[40]<<40)&mask < val)<<45 |
		b2u64((in[40]>>16)&mask|
			(in[41]<<48)&mask < val)<<46 |
		b2u64((in[41]>>8)&mask < val)<<47 |
		b2u64((in[42]>>0)&mask < val)<<48 |
		b2u64((in[42]>>56)&mask|
			(in[43]<<8)&mask < val)<<49 |
		b2u64((in[43]>>48)&mask|
			(in[44]<<16)&mask < val)<<50 |
		b2u64((in[44]>>40)&mask|
			(in[45]<<24)&mask < val)<<51 |
		b2u64((in[45]>>32)&mask|
			(in[46]<<32)&mask < val)<<52 |
		b2u64((in[46]>>24)&mask|
			(in[47]<<40)&mask < val)<<53 |
		b2u64((in[47]>>16)&mask|
			(in[48]<<48)&mask < val)<<54 |
		b2u64((in[48]>>8)&mask < val)<<55 |
		b2u64((in[49]>>0)&mask < val)<<56 |
		b2u64((in[49]>>56)&mask|
			(in[50]<<8)&mask < val)<<57 |
		b2u64((in[50]>>48)&mask|
			(in[51]<<16)&mask < val)<<58 |
		b2u64((in[51]>>40)&mask|
			(in[52]<<24)&mask < val)<<59 |
		b2u64((in[52]>>32)&mask|
			(in[53]<<32)&mask < val)<<60 |
		b2u64((in[53]>>24)&mask|
			(in[54]<<40)&mask < val)<<61 |
		b2u64((in[54]>>16)&mask|
			(in[55]<<48)&mask < val)<<62 |
		b2u64((in[55]>>8)&mask < val)<<63)

}
func cmp_bp_57_lt(in *[57]uint64, val uint64) uint64 {
	mask := uint64((1 << 57) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>57)&mask|
			(in[1]<<7)&mask < val)<<1 |
		b2u64((in[1]>>50)&mask|
			(in[2]<<14)&mask < val)<<2 |
		b2u64((in[2]>>43)&mask|
			(in[3]<<21)&mask < val)<<3 |
		b2u64((in[3]>>36)&mask|
			(in[4]<<28)&mask < val)<<4 |
		b2u64((in[4]>>29)&mask|
			(in[5]<<35)&mask < val)<<5 |
		b2u64((in[5]>>22)&mask|
			(in[6]<<42)&mask < val)<<6 |
		b2u64((in[6]>>15)&mask|
			(in[7]<<49)&mask < val)<<7 |
		b2u64((in[7]>>8)&mask|
			(in[8]<<56)&mask < val)<<8 |
		b2u64((in[8]>>1)&mask < val)<<9 |
		b2u64((in[8]>>58)&mask|
			(in[9]<<6)&mask < val)<<10 |
		b2u64((in[9]>>51)&mask|
			(in[10]<<13)&mask < val)<<11 |
		b2u64((in[10]>>44)&mask|
			(in[11]<<20)&mask < val)<<12 |
		b2u64((in[11]>>37)&mask|
			(in[12]<<27)&mask < val)<<13 |
		b2u64((in[12]>>30)&mask|
			(in[13]<<34)&mask < val)<<14 |
		b2u64((in[13]>>23)&mask|
			(in[14]<<41)&mask < val)<<15 |
		b2u64((in[14]>>16)&mask|
			(in[15]<<48)&mask < val)<<16 |
		b2u64((in[15]>>9)&mask|
			(in[16]<<55)&mask < val)<<17 |
		b2u64((in[16]>>2)&mask < val)<<18 |
		b2u64((in[16]>>59)&mask|
			(in[17]<<5)&mask < val)<<19 |
		b2u64((in[17]>>52)&mask|
			(in[18]<<12)&mask < val)<<20 |
		b2u64((in[18]>>45)&mask|
			(in[19]<<19)&mask < val)<<21 |
		b2u64((in[19]>>38)&mask|
			(in[20]<<26)&mask < val)<<22 |
		b2u64((in[20]>>31)&mask|
			(in[21]<<33)&mask < val)<<23 |
		b2u64((in[21]>>24)&mask|
			(in[22]<<40)&mask < val)<<24 |
		b2u64((in[22]>>17)&mask|
			(in[23]<<47)&mask < val)<<25 |
		b2u64((in[23]>>10)&mask|
			(in[24]<<54)&mask < val)<<26 |
		b2u64((in[24]>>3)&mask < val)<<27 |
		b2u64((in[24]>>60)&mask|
			(in[25]<<4)&mask < val)<<28 |
		b2u64((in[25]>>53)&mask|
			(in[26]<<11)&mask < val)<<29 |
		b2u64((in[26]>>46)&mask|
			(in[27]<<18)&mask < val)<<30 |
		b2u64((in[27]>>39)&mask|
			(in[28]<<25)&mask < val)<<31 |
		b2u64((in[28]>>32)&mask|
			(in[29]<<32)&mask < val)<<32 |
		b2u64((in[29]>>25)&mask|
			(in[30]<<39)&mask < val)<<33 |
		b2u64((in[30]>>18)&mask|
			(in[31]<<46)&mask < val)<<34 |
		b2u64((in[31]>>11)&mask|
			(in[32]<<53)&mask < val)<<35 |
		b2u64((in[32]>>4)&mask < val)<<36 |
		b2u64((in[32]>>61)&mask|
			(in[33]<<3)&mask < val)<<37 |
		b2u64((in[33]>>54)&mask|
			(in[34]<<10)&mask < val)<<38 |
		b2u64((in[34]>>47)&mask|
			(in[35]<<17)&mask < val)<<39 |
		b2u64((in[35]>>40)&mask|
			(in[36]<<24)&mask < val)<<40 |
		b2u64((in[36]>>33)&mask|
			(in[37]<<31)&mask < val)<<41 |
		b2u64((in[37]>>26)&mask|
			(in[38]<<38)&mask < val)<<42 |
		b2u64((in[38]>>19)&mask|
			(in[39]<<45)&mask < val)<<43 |
		b2u64((in[39]>>12)&mask|
			(in[40]<<52)&mask < val)<<44 |
		b2u64((in[40]>>5)&mask < val)<<45 |
		b2u64((in[40]>>62)&mask|
			(in[41]<<2)&mask < val)<<46 |
		b2u64((in[41]>>55)&mask|
			(in[42]<<9)&mask < val)<<47 |
		b2u64((in[42]>>48)&mask|
			(in[43]<<16)&mask < val)<<48 |
		b2u64((in[43]>>41)&mask|
			(in[44]<<23)&mask < val)<<49 |
		b2u64((in[44]>>34)&mask|
			(in[45]<<30)&mask < val)<<50 |
		b2u64((in[45]>>27)&mask|
			(in[46]<<37)&mask < val)<<51 |
		b2u64((in[46]>>20)&mask|
			(in[47]<<44)&mask < val)<<52 |
		b2u64((in[47]>>13)&mask|
			(in[48]<<51)&mask < val)<<53 |
		b2u64((in[48]>>6)&mask < val)<<54 |
		b2u64((in[48]>>63)&mask|
			(in[49]<<1)&mask < val)<<55 |
		b2u64((in[49]>>56)&mask|
			(in[50]<<8)&mask < val)<<56 |
		b2u64((in[50]>>49)&mask|
			(in[51]<<15)&mask < val)<<57 |
		b2u64((in[51]>>42)&mask|
			(in[52]<<22)&mask < val)<<58 |
		b2u64((in[52]>>35)&mask|
			(in[53]<<29)&mask < val)<<59 |
		b2u64((in[53]>>28)&mask|
			(in[54]<<36)&mask < val)<<60 |
		b2u64((in[54]>>21)&mask|
			(in[55]<<43)&mask < val)<<61 |
		b2u64((in[55]>>14)&mask|
			(in[56]<<50)&mask < val)<<62 |
		b2u64((in[56]>>7)&mask < val)<<63)

}
func cmp_bp_58_lt(in *[58]uint64, val uint64) uint64 {
	mask := uint64((1 << 58) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>58)&mask|
			(in[1]<<6)&mask < val)<<1 |
		b2u64((in[1]>>52)&mask|
			(in[2]<<12)&mask < val)<<2 |
		b2u64((in[2]>>46)&mask|
			(in[3]<<18)&mask < val)<<3 |
		b2u64((in[3]>>40)&mask|
			(in[4]<<24)&mask < val)<<4 |
		b2u64((in[4]>>34)&mask|
			(in[5]<<30)&mask < val)<<5 |
		b2u64((in[5]>>28)&mask|
			(in[6]<<36)&mask < val)<<6 |
		b2u64((in[6]>>22)&mask|
			(in[7]<<42)&mask < val)<<7 |
		b2u64((in[7]>>16)&mask|
			(in[8]<<48)&mask < val)<<8 |
		b2u64((in[8]>>10)&mask|
			(in[9]<<54)&mask < val)<<9 |
		b2u64((in[9]>>4)&mask < val)<<10 |
		b2u64((in[9]>>62)&mask|
			(in[10]<<2)&mask < val)<<11 |
		b2u64((in[10]>>56)&mask|
			(in[11]<<8)&mask < val)<<12 |
		b2u64((in[11]>>50)&mask|
			(in[12]<<14)&mask < val)<<13 |
		b2u64((in[12]>>44)&mask|
			(in[13]<<20)&mask < val)<<14 |
		b2u64((in[13]>>38)&mask|
			(in[14]<<26)&mask < val)<<15 |
		b2u64((in[14]>>32)&mask|
			(in[15]<<32)&mask < val)<<16 |
		b2u64((in[15]>>26)&mask|
			(in[16]<<38)&mask < val)<<17 |
		b2u64((in[16]>>20)&mask|
			(in[17]<<44)&mask < val)<<18 |
		b2u64((in[17]>>14)&mask|
			(in[18]<<50)&mask < val)<<19 |
		b2u64((in[18]>>8)&mask|
			(in[19]<<56)&mask < val)<<20 |
		b2u64((in[19]>>2)&mask < val)<<21 |
		b2u64((in[19]>>60)&mask|
			(in[20]<<4)&mask < val)<<22 |
		b2u64((in[20]>>54)&mask|
			(in[21]<<10)&mask < val)<<23 |
		b2u64((in[21]>>48)&mask|
			(in[22]<<16)&mask < val)<<24 |
		b2u64((in[22]>>42)&mask|
			(in[23]<<22)&mask < val)<<25 |
		b2u64((in[23]>>36)&mask|
			(in[24]<<28)&mask < val)<<26 |
		b2u64((in[24]>>30)&mask|
			(in[25]<<34)&mask < val)<<27 |
		b2u64((in[25]>>24)&mask|
			(in[26]<<40)&mask < val)<<28 |
		b2u64((in[26]>>18)&mask|
			(in[27]<<46)&mask < val)<<29 |
		b2u64((in[27]>>12)&mask|
			(in[28]<<52)&mask < val)<<30 |
		b2u64((in[28]>>6)&mask < val)<<31 |
		b2u64((in[29]>>0)&mask < val)<<32 |
		b2u64((in[29]>>58)&mask|
			(in[30]<<6)&mask < val)<<33 |
		b2u64((in[30]>>52)&mask|
			(in[31]<<12)&mask < val)<<34 |
		b2u64((in[31]>>46)&mask|
			(in[32]<<18)&mask < val)<<35 |
		b2u64((in[32]>>40)&mask|
			(in[33]<<24)&mask < val)<<36 |
		b2u64((in[33]>>34)&mask|
			(in[34]<<30)&mask < val)<<37 |
		b2u64((in[34]>>28)&mask|
			(in[35]<<36)&mask < val)<<38 |
		b2u64((in[35]>>22)&mask|
			(in[36]<<42)&mask < val)<<39 |
		b2u64((in[36]>>16)&mask|
			(in[37]<<48)&mask < val)<<40 |
		b2u64((in[37]>>10)&mask|
			(in[38]<<54)&mask < val)<<41 |
		b2u64((in[38]>>4)&mask < val)<<42 |
		b2u64((in[38]>>62)&mask|
			(in[39]<<2)&mask < val)<<43 |
		b2u64((in[39]>>56)&mask|
			(in[40]<<8)&mask < val)<<44 |
		b2u64((in[40]>>50)&mask|
			(in[41]<<14)&mask < val)<<45 |
		b2u64((in[41]>>44)&mask|
			(in[42]<<20)&mask < val)<<46 |
		b2u64((in[42]>>38)&mask|
			(in[43]<<26)&mask < val)<<47 |
		b2u64((in[43]>>32)&mask|
			(in[44]<<32)&mask < val)<<48 |
		b2u64((in[44]>>26)&mask|
			(in[45]<<38)&mask < val)<<49 |
		b2u64((in[45]>>20)&mask|
			(in[46]<<44)&mask < val)<<50 |
		b2u64((in[46]>>14)&mask|
			(in[47]<<50)&mask < val)<<51 |
		b2u64((in[47]>>8)&mask|
			(in[48]<<56)&mask < val)<<52 |
		b2u64((in[48]>>2)&mask < val)<<53 |
		b2u64((in[48]>>60)&mask|
			(in[49]<<4)&mask < val)<<54 |
		b2u64((in[49]>>54)&mask|
			(in[50]<<10)&mask < val)<<55 |
		b2u64((in[50]>>48)&mask|
			(in[51]<<16)&mask < val)<<56 |
		b2u64((in[51]>>42)&mask|
			(in[52]<<22)&mask < val)<<57 |
		b2u64((in[52]>>36)&mask|
			(in[53]<<28)&mask < val)<<58 |
		b2u64((in[53]>>30)&mask|
			(in[54]<<34)&mask < val)<<59 |
		b2u64((in[54]>>24)&mask|
			(in[55]<<40)&mask < val)<<60 |
		b2u64((in[55]>>18)&mask|
			(in[56]<<46)&mask < val)<<61 |
		b2u64((in[56]>>12)&mask|
			(in[57]<<52)&mask < val)<<62 |
		b2u64((in[57]>>6)&mask < val)<<63)

}
func cmp_bp_59_lt(in *[59]uint64, val uint64) uint64 {
	mask := uint64((1 << 59) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>59)&mask|
			(in[1]<<5)&mask < val)<<1 |
		b2u64((in[1]>>54)&mask|
			(in[2]<<10)&mask < val)<<2 |
		b2u64((in[2]>>49)&mask|
			(in[3]<<15)&mask < val)<<3 |
		b2u64((in[3]>>44)&mask|
			(in[4]<<20)&mask < val)<<4 |
		b2u64((in[4]>>39)&mask|
			(in[5]<<25)&mask < val)<<5 |
		b2u64((in[5]>>34)&mask|
			(in[6]<<30)&mask < val)<<6 |
		b2u64((in[6]>>29)&mask|
			(in[7]<<35)&mask < val)<<7 |
		b2u64((in[7]>>24)&mask|
			(in[8]<<40)&mask < val)<<8 |
		b2u64((in[8]>>19)&mask|
			(in[9]<<45)&mask < val)<<9 |
		b2u64((in[9]>>14)&mask|
			(in[10]<<50)&mask < val)<<10 |
		b2u64((in[10]>>9)&mask|
			(in[11]<<55)&mask < val)<<11 |
		b2u64((in[11]>>4)&mask < val)<<12 |
		b2u64((in[11]>>63)&mask|
			(in[12]<<1)&mask < val)<<13 |
		b2u64((in[12]>>58)&mask|
			(in[13]<<6)&mask < val)<<14 |
		b2u64((in[13]>>53)&mask|
			(in[14]<<11)&mask < val)<<15 |
		b2u64((in[14]>>48)&mask|
			(in[15]<<16)&mask < val)<<16 |
		b2u64((in[15]>>43)&mask|
			(in[16]<<21)&mask < val)<<17 |
		b2u64((in[16]>>38)&mask|
			(in[17]<<26)&mask < val)<<18 |
		b2u64((in[17]>>33)&mask|
			(in[18]<<31)&mask < val)<<19 |
		b2u64((in[18]>>28)&mask|
			(in[19]<<36)&mask < val)<<20 |
		b2u64((in[19]>>23)&mask|
			(in[20]<<41)&mask < val)<<21 |
		b2u64((in[20]>>18)&mask|
			(in[21]<<46)&mask < val)<<22 |
		b2u64((in[21]>>13)&mask|
			(in[22]<<51)&mask < val)<<23 |
		b2u64((in[22]>>8)&mask|
			(in[23]<<56)&mask < val)<<24 |
		b2u64((in[23]>>3)&mask < val)<<25 |
		b2u64((in[23]>>62)&mask|
			(in[24]<<2)&mask < val)<<26 |
		b2u64((in[24]>>57)&mask|
			(in[25]<<7)&mask < val)<<27 |
		b2u64((in[25]>>52)&mask|
			(in[26]<<12)&mask < val)<<28 |
		b2u64((in[26]>>47)&mask|
			(in[27]<<17)&mask < val)<<29 |
		b2u64((in[27]>>42)&mask|
			(in[28]<<22)&mask < val)<<30 |
		b2u64((in[28]>>37)&mask|
			(in[29]<<27)&mask < val)<<31 |
		b2u64((in[29]>>32)&mask|
			(in[30]<<32)&mask < val)<<32 |
		b2u64((in[30]>>27)&mask|
			(in[31]<<37)&mask < val)<<33 |
		b2u64((in[31]>>22)&mask|
			(in[32]<<42)&mask < val)<<34 |
		b2u64((in[32]>>17)&mask|
			(in[33]<<47)&mask < val)<<35 |
		b2u64((in[33]>>12)&mask|
			(in[34]<<52)&mask < val)<<36 |
		b2u64((in[34]>>7)&mask|
			(in[35]<<57)&mask < val)<<37 |
		b2u64((in[35]>>2)&mask < val)<<38 |
		b2u64((in[35]>>61)&mask|
			(in[36]<<3)&mask < val)<<39 |
		b2u64((in[36]>>56)&mask|
			(in[37]<<8)&mask < val)<<40 |
		b2u64((in[37]>>51)&mask|
			(in[38]<<13)&mask < val)<<41 |
		b2u64((in[38]>>46)&mask|
			(in[39]<<18)&mask < val)<<42 |
		b2u64((in[39]>>41)&mask|
			(in[40]<<23)&mask < val)<<43 |
		b2u64((in[40]>>36)&mask|
			(in[41]<<28)&mask < val)<<44 |
		b2u64((in[41]>>31)&mask|
			(in[42]<<33)&mask < val)<<45 |
		b2u64((in[42]>>26)&mask|
			(in[43]<<38)&mask < val)<<46 |
		b2u64((in[43]>>21)&mask|
			(in[44]<<43)&mask < val)<<47 |
		b2u64((in[44]>>16)&mask|
			(in[45]<<48)&mask < val)<<48 |
		b2u64((in[45]>>11)&mask|
			(in[46]<<53)&mask < val)<<49 |
		b2u64((in[46]>>6)&mask|
			(in[47]<<58)&mask < val)<<50 |
		b2u64((in[47]>>1)&mask < val)<<51 |
		b2u64((in[47]>>60)&mask|
			(in[48]<<4)&mask < val)<<52 |
		b2u64((in[48]>>55)&mask|
			(in[49]<<9)&mask < val)<<53 |
		b2u64((in[49]>>50)&mask|
			(in[50]<<14)&mask < val)<<54 |
		b2u64((in[50]>>45)&mask|
			(in[51]<<19)&mask < val)<<55 |
		b2u64((in[51]>>40)&mask|
			(in[52]<<24)&mask < val)<<56 |
		b2u64((in[52]>>35)&mask|
			(in[53]<<29)&mask < val)<<57 |
		b2u64((in[53]>>30)&mask|
			(in[54]<<34)&mask < val)<<58 |
		b2u64((in[54]>>25)&mask|
			(in[55]<<39)&mask < val)<<59 |
		b2u64((in[55]>>20)&mask|
			(in[56]<<44)&mask < val)<<60 |
		b2u64((in[56]>>15)&mask|
			(in[57]<<49)&mask < val)<<61 |
		b2u64((in[57]>>10)&mask|
			(in[58]<<54)&mask < val)<<62 |
		b2u64((in[58]>>5)&mask < val)<<63)

}
func cmp_bp_60_lt(in *[60]uint64, val uint64) uint64 {
	mask := uint64((1 << 60) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>60)&mask|
			(in[1]<<4)&mask < val)<<1 |
		b2u64((in[1]>>56)&mask|
			(in[2]<<8)&mask < val)<<2 |
		b2u64((in[2]>>52)&mask|
			(in[3]<<12)&mask < val)<<3 |
		b2u64((in[3]>>48)&mask|
			(in[4]<<16)&mask < val)<<4 |
		b2u64((in[4]>>44)&mask|
			(in[5]<<20)&mask < val)<<5 |
		b2u64((in[5]>>40)&mask|
			(in[6]<<24)&mask < val)<<6 |
		b2u64((in[6]>>36)&mask|
			(in[7]<<28)&mask < val)<<7 |
		b2u64((in[7]>>32)&mask|
			(in[8]<<32)&mask < val)<<8 |
		b2u64((in[8]>>28)&mask|
			(in[9]<<36)&mask < val)<<9 |
		b2u64((in[9]>>24)&mask|
			(in[10]<<40)&mask < val)<<10 |
		b2u64((in[10]>>20)&mask|
			(in[11]<<44)&mask < val)<<11 |
		b2u64((in[11]>>16)&mask|
			(in[12]<<48)&mask < val)<<12 |
		b2u64((in[12]>>12)&mask|
			(in[13]<<52)&mask < val)<<13 |
		b2u64((in[13]>>8)&mask|
			(in[14]<<56)&mask < val)<<14 |
		b2u64((in[14]>>4)&mask < val)<<15 |
		b2u64((in[15]>>0)&mask < val)<<16 |
		b2u64((in[15]>>60)&mask|
			(in[16]<<4)&mask < val)<<17 |
		b2u64((in[16]>>56)&mask|
			(in[17]<<8)&mask < val)<<18 |
		b2u64((in[17]>>52)&mask|
			(in[18]<<12)&mask < val)<<19 |
		b2u64((in[18]>>48)&mask|
			(in[19]<<16)&mask < val)<<20 |
		b2u64((in[19]>>44)&mask|
			(in[20]<<20)&mask < val)<<21 |
		b2u64((in[20]>>40)&mask|
			(in[21]<<24)&mask < val)<<22 |
		b2u64((in[21]>>36)&mask|
			(in[22]<<28)&mask < val)<<23 |
		b2u64((in[22]>>32)&mask|
			(in[23]<<32)&mask < val)<<24 |
		b2u64((in[23]>>28)&mask|
			(in[24]<<36)&mask < val)<<25 |
		b2u64((in[24]>>24)&mask|
			(in[25]<<40)&mask < val)<<26 |
		b2u64((in[25]>>20)&mask|
			(in[26]<<44)&mask < val)<<27 |
		b2u64((in[26]>>16)&mask|
			(in[27]<<48)&mask < val)<<28 |
		b2u64((in[27]>>12)&mask|
			(in[28]<<52)&mask < val)<<29 |
		b2u64((in[28]>>8)&mask|
			(in[29]<<56)&mask < val)<<30 |
		b2u64((in[29]>>4)&mask < val)<<31 |
		b2u64((in[30]>>0)&mask < val)<<32 |
		b2u64((in[30]>>60)&mask|
			(in[31]<<4)&mask < val)<<33 |
		b2u64((in[31]>>56)&mask|
			(in[32]<<8)&mask < val)<<34 |
		b2u64((in[32]>>52)&mask|
			(in[33]<<12)&mask < val)<<35 |
		b2u64((in[33]>>48)&mask|
			(in[34]<<16)&mask < val)<<36 |
		b2u64((in[34]>>44)&mask|
			(in[35]<<20)&mask < val)<<37 |
		b2u64((in[35]>>40)&mask|
			(in[36]<<24)&mask < val)<<38 |
		b2u64((in[36]>>36)&mask|
			(in[37]<<28)&mask < val)<<39 |
		b2u64((in[37]>>32)&mask|
			(in[38]<<32)&mask < val)<<40 |
		b2u64((in[38]>>28)&mask|
			(in[39]<<36)&mask < val)<<41 |
		b2u64((in[39]>>24)&mask|
			(in[40]<<40)&mask < val)<<42 |
		b2u64((in[40]>>20)&mask|
			(in[41]<<44)&mask < val)<<43 |
		b2u64((in[41]>>16)&mask|
			(in[42]<<48)&mask < val)<<44 |
		b2u64((in[42]>>12)&mask|
			(in[43]<<52)&mask < val)<<45 |
		b2u64((in[43]>>8)&mask|
			(in[44]<<56)&mask < val)<<46 |
		b2u64((in[44]>>4)&mask < val)<<47 |
		b2u64((in[45]>>0)&mask < val)<<48 |
		b2u64((in[45]>>60)&mask|
			(in[46]<<4)&mask < val)<<49 |
		b2u64((in[46]>>56)&mask|
			(in[47]<<8)&mask < val)<<50 |
		b2u64((in[47]>>52)&mask|
			(in[48]<<12)&mask < val)<<51 |
		b2u64((in[48]>>48)&mask|
			(in[49]<<16)&mask < val)<<52 |
		b2u64((in[49]>>44)&mask|
			(in[50]<<20)&mask < val)<<53 |
		b2u64((in[50]>>40)&mask|
			(in[51]<<24)&mask < val)<<54 |
		b2u64((in[51]>>36)&mask|
			(in[52]<<28)&mask < val)<<55 |
		b2u64((in[52]>>32)&mask|
			(in[53]<<32)&mask < val)<<56 |
		b2u64((in[53]>>28)&mask|
			(in[54]<<36)&mask < val)<<57 |
		b2u64((in[54]>>24)&mask|
			(in[55]<<40)&mask < val)<<58 |
		b2u64((in[55]>>20)&mask|
			(in[56]<<44)&mask < val)<<59 |
		b2u64((in[56]>>16)&mask|
			(in[57]<<48)&mask < val)<<60 |
		b2u64((in[57]>>12)&mask|
			(in[58]<<52)&mask < val)<<61 |
		b2u64((in[58]>>8)&mask|
			(in[59]<<56)&mask < val)<<62 |
		b2u64((in[59]>>4)&mask < val)<<63)

}
func cmp_bp_61_lt(in *[61]uint64, val uint64) uint64 {
	mask := uint64((1 << 61) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>61)&mask|
			(in[1]<<3)&mask < val)<<1 |
		b2u64((in[1]>>58)&mask|
			(in[2]<<6)&mask < val)<<2 |
		b2u64((in[2]>>55)&mask|
			(in[3]<<9)&mask < val)<<3 |
		b2u64((in[3]>>52)&mask|
			(in[4]<<12)&mask < val)<<4 |
		b2u64((in[4]>>49)&mask|
			(in[5]<<15)&mask < val)<<5 |
		b2u64((in[5]>>46)&mask|
			(in[6]<<18)&mask < val)<<6 |
		b2u64((in[6]>>43)&mask|
			(in[7]<<21)&mask < val)<<7 |
		b2u64((in[7]>>40)&mask|
			(in[8]<<24)&mask < val)<<8 |
		b2u64((in[8]>>37)&mask|
			(in[9]<<27)&mask < val)<<9 |
		b2u64((in[9]>>34)&mask|
			(in[10]<<30)&mask < val)<<10 |
		b2u64((in[10]>>31)&mask|
			(in[11]<<33)&mask < val)<<11 |
		b2u64((in[11]>>28)&mask|
			(in[12]<<36)&mask < val)<<12 |
		b2u64((in[12]>>25)&mask|
			(in[13]<<39)&mask < val)<<13 |
		b2u64((in[13]>>22)&mask|
			(in[14]<<42)&mask < val)<<14 |
		b2u64((in[14]>>19)&mask|
			(in[15]<<45)&mask < val)<<15 |
		b2u64((in[15]>>16)&mask|
			(in[16]<<48)&mask < val)<<16 |
		b2u64((in[16]>>13)&mask|
			(in[17]<<51)&mask < val)<<17 |
		b2u64((in[17]>>10)&mask|
			(in[18]<<54)&mask < val)<<18 |
		b2u64((in[18]>>7)&mask|
			(in[19]<<57)&mask < val)<<19 |
		b2u64((in[19]>>4)&mask|
			(in[20]<<60)&mask < val)<<20 |
		b2u64((in[20]>>1)&mask < val)<<21 |
		b2u64((in[20]>>62)&mask|
			(in[21]<<2)&mask < val)<<22 |
		b2u64((in[21]>>59)&mask|
			(in[22]<<5)&mask < val)<<23 |
		b2u64((in[22]>>56)&mask|
			(in[23]<<8)&mask < val)<<24 |
		b2u64((in[23]>>53)&mask|
			(in[24]<<11)&mask < val)<<25 |
		b2u64((in[24]>>50)&mask|
			(in[25]<<14)&mask < val)<<26 |
		b2u64((in[25]>>47)&mask|
			(in[26]<<17)&mask < val)<<27 |
		b2u64((in[26]>>44)&mask|
			(in[27]<<20)&mask < val)<<28 |
		b2u64((in[27]>>41)&mask|
			(in[28]<<23)&mask < val)<<29 |
		b2u64((in[28]>>38)&mask|
			(in[29]<<26)&mask < val)<<30 |
		b2u64((in[29]>>35)&mask|
			(in[30]<<29)&mask < val)<<31 |
		b2u64((in[30]>>32)&mask|
			(in[31]<<32)&mask < val)<<32 |
		b2u64((in[31]>>29)&mask|
			(in[32]<<35)&mask < val)<<33 |
		b2u64((in[32]>>26)&mask|
			(in[33]<<38)&mask < val)<<34 |
		b2u64((in[33]>>23)&mask|
			(in[34]<<41)&mask < val)<<35 |
		b2u64((in[34]>>20)&mask|
			(in[35]<<44)&mask < val)<<36 |
		b2u64((in[35]>>17)&mask|
			(in[36]<<47)&mask < val)<<37 |
		b2u64((in[36]>>14)&mask|
			(in[37]<<50)&mask < val)<<38 |
		b2u64((in[37]>>11)&mask|
			(in[38]<<53)&mask < val)<<39 |
		b2u64((in[38]>>8)&mask|
			(in[39]<<56)&mask < val)<<40 |
		b2u64((in[39]>>5)&mask|
			(in[40]<<59)&mask < val)<<41 |
		b2u64((in[40]>>2)&mask < val)<<42 |
		b2u64((in[40]>>63)&mask|
			(in[41]<<1)&mask < val)<<43 |
		b2u64((in[41]>>60)&mask|
			(in[42]<<4)&mask < val)<<44 |
		b2u64((in[42]>>57)&mask|
			(in[43]<<7)&mask < val)<<45 |
		b2u64((in[43]>>54)&mask|
			(in[44]<<10)&mask < val)<<46 |
		b2u64((in[44]>>51)&mask|
			(in[45]<<13)&mask < val)<<47 |
		b2u64((in[45]>>48)&mask|
			(in[46]<<16)&mask < val)<<48 |
		b2u64((in[46]>>45)&mask|
			(in[47]<<19)&mask < val)<<49 |
		b2u64((in[47]>>42)&mask|
			(in[48]<<22)&mask < val)<<50 |
		b2u64((in[48]>>39)&mask|
			(in[49]<<25)&mask < val)<<51 |
		b2u64((in[49]>>36)&mask|
			(in[50]<<28)&mask < val)<<52 |
		b2u64((in[50]>>33)&mask|
			(in[51]<<31)&mask < val)<<53 |
		b2u64((in[51]>>30)&mask|
			(in[52]<<34)&mask < val)<<54 |
		b2u64((in[52]>>27)&mask|
			(in[53]<<37)&mask < val)<<55 |
		b2u64((in[53]>>24)&mask|
			(in[54]<<40)&mask < val)<<56 |
		b2u64((in[54]>>21)&mask|
			(in[55]<<43)&mask < val)<<57 |
		b2u64((in[55]>>18)&mask|
			(in[56]<<46)&mask < val)<<58 |
		b2u64((in[56]>>15)&mask|
			(in[57]<<49)&mask < val)<<59 |
		b2u64((in[57]>>12)&mask|
			(in[58]<<52)&mask < val)<<60 |
		b2u64((in[58]>>9)&mask|
			(in[59]<<55)&mask < val)<<61 |
		b2u64((in[59]>>6)&mask|
			(in[60]<<58)&mask < val)<<62 |
		b2u64((in[60]>>3)&mask < val)<<63)

}
func cmp_bp_62_lt(in *[62]uint64, val uint64) uint64 {
	mask := uint64((1 << 62) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>62)&mask|
			(in[1]<<2)&mask < val)<<1 |
		b2u64((in[1]>>60)&mask|
			(in[2]<<4)&mask < val)<<2 |
		b2u64((in[2]>>58)&mask|
			(in[3]<<6)&mask < val)<<3 |
		b2u64((in[3]>>56)&mask|
			(in[4]<<8)&mask < val)<<4 |
		b2u64((in[4]>>54)&mask|
			(in[5]<<10)&mask < val)<<5 |
		b2u64((in[5]>>52)&mask|
			(in[6]<<12)&mask < val)<<6 |
		b2u64((in[6]>>50)&mask|
			(in[7]<<14)&mask < val)<<7 |
		b2u64((in[7]>>48)&mask|
			(in[8]<<16)&mask < val)<<8 |
		b2u64((in[8]>>46)&mask|
			(in[9]<<18)&mask < val)<<9 |
		b2u64((in[9]>>44)&mask|
			(in[10]<<20)&mask < val)<<10 |
		b2u64((in[10]>>42)&mask|
			(in[11]<<22)&mask < val)<<11 |
		b2u64((in[11]>>40)&mask|
			(in[12]<<24)&mask < val)<<12 |
		b2u64((in[12]>>38)&mask|
			(in[13]<<26)&mask < val)<<13 |
		b2u64((in[13]>>36)&mask|
			(in[14]<<28)&mask < val)<<14 |
		b2u64((in[14]>>34)&mask|
			(in[15]<<30)&mask < val)<<15 |
		b2u64((in[15]>>32)&mask|
			(in[16]<<32)&mask < val)<<16 |
		b2u64((in[16]>>30)&mask|
			(in[17]<<34)&mask < val)<<17 |
		b2u64((in[17]>>28)&mask|
			(in[18]<<36)&mask < val)<<18 |
		b2u64((in[18]>>26)&mask|
			(in[19]<<38)&mask < val)<<19 |
		b2u64((in[19]>>24)&mask|
			(in[20]<<40)&mask < val)<<20 |
		b2u64((in[20]>>22)&mask|
			(in[21]<<42)&mask < val)<<21 |
		b2u64((in[21]>>20)&mask|
			(in[22]<<44)&mask < val)<<22 |
		b2u64((in[22]>>18)&mask|
			(in[23]<<46)&mask < val)<<23 |
		b2u64((in[23]>>16)&mask|
			(in[24]<<48)&mask < val)<<24 |
		b2u64((in[24]>>14)&mask|
			(in[25]<<50)&mask < val)<<25 |
		b2u64((in[25]>>12)&mask|
			(in[26]<<52)&mask < val)<<26 |
		b2u64((in[26]>>10)&mask|
			(in[27]<<54)&mask < val)<<27 |
		b2u64((in[27]>>8)&mask|
			(in[28]<<56)&mask < val)<<28 |
		b2u64((in[28]>>6)&mask|
			(in[29]<<58)&mask < val)<<29 |
		b2u64((in[29]>>4)&mask|
			(in[30]<<60)&mask < val)<<30 |
		b2u64((in[30]>>2)&mask < val)<<31 |
		b2u64((in[31]>>0)&mask < val)<<32 |
		b2u64((in[31]>>62)&mask|
			(in[32]<<2)&mask < val)<<33 |
		b2u64((in[32]>>60)&mask|
			(in[33]<<4)&mask < val)<<34 |
		b2u64((in[33]>>58)&mask|
			(in[34]<<6)&mask < val)<<35 |
		b2u64((in[34]>>56)&mask|
			(in[35]<<8)&mask < val)<<36 |
		b2u64((in[35]>>54)&mask|
			(in[36]<<10)&mask < val)<<37 |
		b2u64((in[36]>>52)&mask|
			(in[37]<<12)&mask < val)<<38 |
		b2u64((in[37]>>50)&mask|
			(in[38]<<14)&mask < val)<<39 |
		b2u64((in[38]>>48)&mask|
			(in[39]<<16)&mask < val)<<40 |
		b2u64((in[39]>>46)&mask|
			(in[40]<<18)&mask < val)<<41 |
		b2u64((in[40]>>44)&mask|
			(in[41]<<20)&mask < val)<<42 |
		b2u64((in[41]>>42)&mask|
			(in[42]<<22)&mask < val)<<43 |
		b2u64((in[42]>>40)&mask|
			(in[43]<<24)&mask < val)<<44 |
		b2u64((in[43]>>38)&mask|
			(in[44]<<26)&mask < val)<<45 |
		b2u64((in[44]>>36)&mask|
			(in[45]<<28)&mask < val)<<46 |
		b2u64((in[45]>>34)&mask|
			(in[46]<<30)&mask < val)<<47 |
		b2u64((in[46]>>32)&mask|
			(in[47]<<32)&mask < val)<<48 |
		b2u64((in[47]>>30)&mask|
			(in[48]<<34)&mask < val)<<49 |
		b2u64((in[48]>>28)&mask|
			(in[49]<<36)&mask < val)<<50 |
		b2u64((in[49]>>26)&mask|
			(in[50]<<38)&mask < val)<<51 |
		b2u64((in[50]>>24)&mask|
			(in[51]<<40)&mask < val)<<52 |
		b2u64((in[51]>>22)&mask|
			(in[52]<<42)&mask < val)<<53 |
		b2u64((in[52]>>20)&mask|
			(in[53]<<44)&mask < val)<<54 |
		b2u64((in[53]>>18)&mask|
			(in[54]<<46)&mask < val)<<55 |
		b2u64((in[54]>>16)&mask|
			(in[55]<<48)&mask < val)<<56 |
		b2u64((in[55]>>14)&mask|
			(in[56]<<50)&mask < val)<<57 |
		b2u64((in[56]>>12)&mask|
			(in[57]<<52)&mask < val)<<58 |
		b2u64((in[57]>>10)&mask|
			(in[58]<<54)&mask < val)<<59 |
		b2u64((in[58]>>8)&mask|
			(in[59]<<56)&mask < val)<<60 |
		b2u64((in[59]>>6)&mask|
			(in[60]<<58)&mask < val)<<61 |
		b2u64((in[60]>>4)&mask|
			(in[61]<<60)&mask < val)<<62 |
		b2u64((in[61]>>2)&mask < val)<<63)

}
func cmp_bp_63_lt(in *[63]uint64, val uint64) uint64 {
	mask := uint64((1 << 63) - 1)

	return (b2u64((in[0]>>0)&mask < val)<<0 |
		b2u64((in[0]>>63)&mask|
			(in[1]<<1)&mask < val)<<1 |
		b2u64((in[1]>>62)&mask|
			(in[2]<<2)&mask < val)<<2 |
		b2u64((in[2]>>61)&mask|
			(in[3]<<3)&mask < val)<<3 |
		b2u64((in[3]>>60)&mask|
			(in[4]<<4)&mask < val)<<4 |
		b2u64((in[4]>>59)&mask|
			(in[5]<<5)&mask < val)<<5 |
		b2u64((in[5]>>58)&mask|
			(in[6]<<6)&mask < val)<<6 |
		b2u64((in[6]>>57)&mask|
			(in[7]<<7)&mask < val)<<7 |
		b2u64((in[7]>>56)&mask|
			(in[8]<<8)&mask < val)<<8 |
		b2u64((in[8]>>55)&mask|
			(in[9]<<9)&mask < val)<<9 |
		b2u64((in[9]>>54)&mask|
			(in[10]<<10)&mask < val)<<10 |
		b2u64((in[10]>>53)&mask|
			(in[11]<<11)&mask < val)<<11 |
		b2u64((in[11]>>52)&mask|
			(in[12]<<12)&mask < val)<<12 |
		b2u64((in[12]>>51)&mask|
			(in[13]<<13)&mask < val)<<13 |
		b2u64((in[13]>>50)&mask|
			(in[14]<<14)&mask < val)<<14 |
		b2u64((in[14]>>49)&mask|
			(in[15]<<15)&mask < val)<<15 |
		b2u64((in[15]>>48)&mask|
			(in[16]<<16)&mask < val)<<16 |
		b2u64((in[16]>>47)&mask|
			(in[17]<<17)&mask < val)<<17 |
		b2u64((in[17]>>46)&mask|
			(in[18]<<18)&mask < val)<<18 |
		b2u64((in[18]>>45)&mask|
			(in[19]<<19)&mask < val)<<19 |
		b2u64((in[19]>>44)&mask|
			(in[20]<<20)&mask < val)<<20 |
		b2u64((in[20]>>43)&mask|
			(in[21]<<21)&mask < val)<<21 |
		b2u64((in[21]>>42)&mask|
			(in[22]<<22)&mask < val)<<22 |
		b2u64((in[22]>>41)&mask|
			(in[23]<<23)&mask < val)<<23 |
		b2u64((in[23]>>40)&mask|
			(in[24]<<24)&mask < val)<<24 |
		b2u64((in[24]>>39)&mask|
			(in[25]<<25)&mask < val)<<25 |
		b2u64((in[25]>>38)&mask|
			(in[26]<<26)&mask < val)<<26 |
		b2u64((in[26]>>37)&mask|
			(in[27]<<27)&mask < val)<<27 |
		b2u64((in[27]>>36)&mask|
			(in[28]<<28)&mask < val)<<28 |
		b2u64((in[28]>>35)&mask|
			(in[29]<<29)&mask < val)<<29 |
		b2u64((in[29]>>34)&mask|
			(in[30]<<30)&mask < val)<<30 |
		b2u64((in[30]>>33)&mask|
			(in[31]<<31)&mask < val)<<31 |
		b2u64((in[31]>>32)&mask|
			(in[32]<<32)&mask < val)<<32 |
		b2u64((in[32]>>31)&mask|
			(in[33]<<33)&mask < val)<<33 |
		b2u64((in[33]>>30)&mask|
			(in[34]<<34)&mask < val)<<34 |
		b2u64((in[34]>>29)&mask|
			(in[35]<<35)&mask < val)<<35 |
		b2u64((in[35]>>28)&mask|
			(in[36]<<36)&mask < val)<<36 |
		b2u64((in[36]>>27)&mask|
			(in[37]<<37)&mask < val)<<37 |
		b2u64((in[37]>>26)&mask|
			(in[38]<<38)&mask < val)<<38 |
		b2u64((in[38]>>25)&mask|
			(in[39]<<39)&mask < val)<<39 |
		b2u64((in[39]>>24)&mask|
			(in[40]<<40)&mask < val)<<40 |
		b2u64((in[40]>>23)&mask|
			(in[41]<<41)&mask < val)<<41 |
		b2u64((in[41]>>22)&mask|
			(in[42]<<42)&mask < val)<<42 |
		b2u64((in[42]>>21)&mask|
			(in[43]<<43)&mask < val)<<43 |
		b2u64((in[43]>>20)&mask|
			(in[44]<<44)&mask < val)<<44 |
		b2u64((in[44]>>19)&mask|
			(in[45]<<45)&mask < val)<<45 |
		b2u64((in[45]>>18)&mask|
			(in[46]<<46)&mask < val)<<46 |
		b2u64((in[46]>>17)&mask|
			(in[47]<<47)&mask < val)<<47 |
		b2u64((in[47]>>16)&mask|
			(in[48]<<48)&mask < val)<<48 |
		b2u64((in[48]>>15)&mask|
			(in[49]<<49)&mask < val)<<49 |
		b2u64((in[49]>>14)&mask|
			(in[50]<<50)&mask < val)<<50 |
		b2u64((in[50]>>13)&mask|
			(in[51]<<51)&mask < val)<<51 |
		b2u64((in[51]>>12)&mask|
			(in[52]<<52)&mask < val)<<52 |
		b2u64((in[52]>>11)&mask|
			(in[53]<<53)&mask < val)<<53 |
		b2u64((in[53]>>10)&mask|
			(in[54]<<54)&mask < val)<<54 |
		b2u64((in[54]>>9)&mask|
			(in[55]<<55)&mask < val)<<55 |
		b2u64((in[55]>>8)&mask|
			(in[56]<<56)&mask < val)<<56 |
		b2u64((in[56]>>7)&mask|
			(in[57]<<57)&mask < val)<<57 |
		b2u64((in[57]>>6)&mask|
			(in[58]<<58)&mask < val)<<58 |
		b2u64((in[58]>>5)&mask|
			(in[59]<<59)&mask < val)<<59 |
		b2u64((in[59]>>4)&mask|
			(in[60]<<60)&mask < val)<<60 |
		b2u64((in[60]>>3)&mask|
			(in[61]<<61)&mask < val)<<61 |
		b2u64((in[61]>>2)&mask|
			(in[62]<<62)&mask < val)<<62 |
		b2u64((in[62]>>1)&mask < val)<<63)

}
