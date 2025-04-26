// Copyright (c) 2025 Blockwatch Data Inc.
// Code automatically generated - DO NOT EDIT.
// Any manual changes will be lost.

package bitpack

// Packer
func bitpack32[T uint32 | int32](minv T, in []T, out []uint64, log2 int) {
	switch log2 {
	case 0:
		bp32_0((*[64]T)(in), (*[0]uint64)(out), uint64(minv))
	case 1:
		bp32_1((*[64]T)(in), (*[1]uint64)(out), uint64(minv))
	case 2:
		bp32_2((*[64]T)(in), (*[2]uint64)(out), uint64(minv))
	case 3:
		bp32_3((*[64]T)(in), (*[3]uint64)(out), uint64(minv))
	case 4:
		bp32_4((*[64]T)(in), (*[4]uint64)(out), uint64(minv))
	case 5:
		bp32_5((*[64]T)(in), (*[5]uint64)(out), uint64(minv))
	case 6:
		bp32_6((*[64]T)(in), (*[6]uint64)(out), uint64(minv))
	case 7:
		bp32_7((*[64]T)(in), (*[7]uint64)(out), uint64(minv))
	case 8:
		bp32_8((*[64]T)(in), (*[8]uint64)(out), uint64(minv))
	case 9:
		bp32_9((*[64]T)(in), (*[9]uint64)(out), uint64(minv))
	case 10:
		bp32_10((*[64]T)(in), (*[10]uint64)(out), uint64(minv))
	case 11:
		bp32_11((*[64]T)(in), (*[11]uint64)(out), uint64(minv))
	case 12:
		bp32_12((*[64]T)(in), (*[12]uint64)(out), uint64(minv))
	case 13:
		bp32_13((*[64]T)(in), (*[13]uint64)(out), uint64(minv))
	case 14:
		bp32_14((*[64]T)(in), (*[14]uint64)(out), uint64(minv))
	case 15:
		bp32_15((*[64]T)(in), (*[15]uint64)(out), uint64(minv))
	case 16:
		bp32_16((*[64]T)(in), (*[16]uint64)(out), uint64(minv))
	case 17:
		bp32_17((*[64]T)(in), (*[17]uint64)(out), uint64(minv))
	case 18:
		bp32_18((*[64]T)(in), (*[18]uint64)(out), uint64(minv))
	case 19:
		bp32_19((*[64]T)(in), (*[19]uint64)(out), uint64(minv))
	case 20:
		bp32_20((*[64]T)(in), (*[20]uint64)(out), uint64(minv))
	case 21:
		bp32_21((*[64]T)(in), (*[21]uint64)(out), uint64(minv))
	case 22:
		bp32_22((*[64]T)(in), (*[22]uint64)(out), uint64(minv))
	case 23:
		bp32_23((*[64]T)(in), (*[23]uint64)(out), uint64(minv))
	case 24:
		bp32_24((*[64]T)(in), (*[24]uint64)(out), uint64(minv))
	case 25:
		bp32_25((*[64]T)(in), (*[25]uint64)(out), uint64(minv))
	case 26:
		bp32_26((*[64]T)(in), (*[26]uint64)(out), uint64(minv))
	case 27:
		bp32_27((*[64]T)(in), (*[27]uint64)(out), uint64(minv))
	case 28:
		bp32_28((*[64]T)(in), (*[28]uint64)(out), uint64(minv))
	case 29:
		bp32_29((*[64]T)(in), (*[29]uint64)(out), uint64(minv))
	case 30:
		bp32_30((*[64]T)(in), (*[30]uint64)(out), uint64(minv))
	case 31:
		bp32_31((*[64]T)(in), (*[31]uint64)(out), uint64(minv))
	}
}
func bp32_0[T uint32 | int32](in *[64]T, out *[0]uint64, minv uint64) {
}
func bp32_1[T uint32 | int32](in *[64]T, out *[1]uint64, minv uint64) {
	out[0] =
		(uint64(in[0])-minv)<<0 |
			(uint64(in[1])-minv)<<1 |
			(uint64(in[2])-minv)<<2 |
			(uint64(in[3])-minv)<<3 |
			(uint64(in[4])-minv)<<4 |
			(uint64(in[5])-minv)<<5 |
			(uint64(in[6])-minv)<<6 |
			(uint64(in[7])-minv)<<7 |
			(uint64(in[8])-minv)<<8 |
			(uint64(in[9])-minv)<<9 |
			(uint64(in[10])-minv)<<10 |
			(uint64(in[11])-minv)<<11 |
			(uint64(in[12])-minv)<<12 |
			(uint64(in[13])-minv)<<13 |
			(uint64(in[14])-minv)<<14 |
			(uint64(in[15])-minv)<<15 |
			(uint64(in[16])-minv)<<16 |
			(uint64(in[17])-minv)<<17 |
			(uint64(in[18])-minv)<<18 |
			(uint64(in[19])-minv)<<19 |
			(uint64(in[20])-minv)<<20 |
			(uint64(in[21])-minv)<<21 |
			(uint64(in[22])-minv)<<22 |
			(uint64(in[23])-minv)<<23 |
			(uint64(in[24])-minv)<<24 |
			(uint64(in[25])-minv)<<25 |
			(uint64(in[26])-minv)<<26 |
			(uint64(in[27])-minv)<<27 |
			(uint64(in[28])-minv)<<28 |
			(uint64(in[29])-minv)<<29 |
			(uint64(in[30])-minv)<<30 |
			(uint64(in[31])-minv)<<31 |
			(uint64(in[32])-minv)<<32 |
			(uint64(in[33])-minv)<<33 |
			(uint64(in[34])-minv)<<34 |
			(uint64(in[35])-minv)<<35 |
			(uint64(in[36])-minv)<<36 |
			(uint64(in[37])-minv)<<37 |
			(uint64(in[38])-minv)<<38 |
			(uint64(in[39])-minv)<<39 |
			(uint64(in[40])-minv)<<40 |
			(uint64(in[41])-minv)<<41 |
			(uint64(in[42])-minv)<<42 |
			(uint64(in[43])-minv)<<43 |
			(uint64(in[44])-minv)<<44 |
			(uint64(in[45])-minv)<<45 |
			(uint64(in[46])-minv)<<46 |
			(uint64(in[47])-minv)<<47 |
			(uint64(in[48])-minv)<<48 |
			(uint64(in[49])-minv)<<49 |
			(uint64(in[50])-minv)<<50 |
			(uint64(in[51])-minv)<<51 |
			(uint64(in[52])-minv)<<52 |
			(uint64(in[53])-minv)<<53 |
			(uint64(in[54])-minv)<<54 |
			(uint64(in[55])-minv)<<55 |
			(uint64(in[56])-minv)<<56 |
			(uint64(in[57])-minv)<<57 |
			(uint64(in[58])-minv)<<58 |
			(uint64(in[59])-minv)<<59 |
			(uint64(in[60])-minv)<<60 |
			(uint64(in[61])-minv)<<61 |
			(uint64(in[62])-minv)<<62 |
			(uint64(in[63])-minv)<<63

}
func bp32_2[T uint32 | int32](in *[64]T, out *[2]uint64, minv uint64) {
	out[0] =
		(uint64(in[0])-minv)<<0 |
			(uint64(in[1])-minv)<<2 |
			(uint64(in[2])-minv)<<4 |
			(uint64(in[3])-minv)<<6 |
			(uint64(in[4])-minv)<<8 |
			(uint64(in[5])-minv)<<10 |
			(uint64(in[6])-minv)<<12 |
			(uint64(in[7])-minv)<<14 |
			(uint64(in[8])-minv)<<16 |
			(uint64(in[9])-minv)<<18 |
			(uint64(in[10])-minv)<<20 |
			(uint64(in[11])-minv)<<22 |
			(uint64(in[12])-minv)<<24 |
			(uint64(in[13])-minv)<<26 |
			(uint64(in[14])-minv)<<28 |
			(uint64(in[15])-minv)<<30 |
			(uint64(in[16])-minv)<<32 |
			(uint64(in[17])-minv)<<34 |
			(uint64(in[18])-minv)<<36 |
			(uint64(in[19])-minv)<<38 |
			(uint64(in[20])-minv)<<40 |
			(uint64(in[21])-minv)<<42 |
			(uint64(in[22])-minv)<<44 |
			(uint64(in[23])-minv)<<46 |
			(uint64(in[24])-minv)<<48 |
			(uint64(in[25])-minv)<<50 |
			(uint64(in[26])-minv)<<52 |
			(uint64(in[27])-minv)<<54 |
			(uint64(in[28])-minv)<<56 |
			(uint64(in[29])-minv)<<58 |
			(uint64(in[30])-minv)<<60 |
			(uint64(in[31])-minv)<<62

	out[1] =
		(uint64(in[32])-minv)<<0 |
			(uint64(in[33])-minv)<<2 |
			(uint64(in[34])-minv)<<4 |
			(uint64(in[35])-minv)<<6 |
			(uint64(in[36])-minv)<<8 |
			(uint64(in[37])-minv)<<10 |
			(uint64(in[38])-minv)<<12 |
			(uint64(in[39])-minv)<<14 |
			(uint64(in[40])-minv)<<16 |
			(uint64(in[41])-minv)<<18 |
			(uint64(in[42])-minv)<<20 |
			(uint64(in[43])-minv)<<22 |
			(uint64(in[44])-minv)<<24 |
			(uint64(in[45])-minv)<<26 |
			(uint64(in[46])-minv)<<28 |
			(uint64(in[47])-minv)<<30 |
			(uint64(in[48])-minv)<<32 |
			(uint64(in[49])-minv)<<34 |
			(uint64(in[50])-minv)<<36 |
			(uint64(in[51])-minv)<<38 |
			(uint64(in[52])-minv)<<40 |
			(uint64(in[53])-minv)<<42 |
			(uint64(in[54])-minv)<<44 |
			(uint64(in[55])-minv)<<46 |
			(uint64(in[56])-minv)<<48 |
			(uint64(in[57])-minv)<<50 |
			(uint64(in[58])-minv)<<52 |
			(uint64(in[59])-minv)<<54 |
			(uint64(in[60])-minv)<<56 |
			(uint64(in[61])-minv)<<58 |
			(uint64(in[62])-minv)<<60 |
			(uint64(in[63])-minv)<<62

}
func bp32_3[T uint32 | int32](in *[64]T, out *[3]uint64, minv uint64) {
	out[0] =
		(uint64(in[0])-minv)<<0 |
			(uint64(in[1])-minv)<<3 |
			(uint64(in[2])-minv)<<6 |
			(uint64(in[3])-minv)<<9 |
			(uint64(in[4])-minv)<<12 |
			(uint64(in[5])-minv)<<15 |
			(uint64(in[6])-minv)<<18 |
			(uint64(in[7])-minv)<<21 |
			(uint64(in[8])-minv)<<24 |
			(uint64(in[9])-minv)<<27 |
			(uint64(in[10])-minv)<<30 |
			(uint64(in[11])-minv)<<33 |
			(uint64(in[12])-minv)<<36 |
			(uint64(in[13])-minv)<<39 |
			(uint64(in[14])-minv)<<42 |
			(uint64(in[15])-minv)<<45 |
			(uint64(in[16])-minv)<<48 |
			(uint64(in[17])-minv)<<51 |
			(uint64(in[18])-minv)<<54 |
			(uint64(in[19])-minv)<<57 |
			(uint64(in[20])-minv)<<60 |
			(uint64(in[21])-minv)<<63

	out[1] =
		(uint64(in[21])-minv)>>1 |

			(uint64(in[22])-minv)<<2 |
			(uint64(in[23])-minv)<<5 |
			(uint64(in[24])-minv)<<8 |
			(uint64(in[25])-minv)<<11 |
			(uint64(in[26])-minv)<<14 |
			(uint64(in[27])-minv)<<17 |
			(uint64(in[28])-minv)<<20 |
			(uint64(in[29])-minv)<<23 |
			(uint64(in[30])-minv)<<26 |
			(uint64(in[31])-minv)<<29 |
			(uint64(in[32])-minv)<<32 |
			(uint64(in[33])-minv)<<35 |
			(uint64(in[34])-minv)<<38 |
			(uint64(in[35])-minv)<<41 |
			(uint64(in[36])-minv)<<44 |
			(uint64(in[37])-minv)<<47 |
			(uint64(in[38])-minv)<<50 |
			(uint64(in[39])-minv)<<53 |
			(uint64(in[40])-minv)<<56 |
			(uint64(in[41])-minv)<<59 |
			(uint64(in[42])-minv)<<62

	out[2] =
		(uint64(in[42])-minv)>>2 |

			(uint64(in[43])-minv)<<1 |
			(uint64(in[44])-minv)<<4 |
			(uint64(in[45])-minv)<<7 |
			(uint64(in[46])-minv)<<10 |
			(uint64(in[47])-minv)<<13 |
			(uint64(in[48])-minv)<<16 |
			(uint64(in[49])-minv)<<19 |
			(uint64(in[50])-minv)<<22 |
			(uint64(in[51])-minv)<<25 |
			(uint64(in[52])-minv)<<28 |
			(uint64(in[53])-minv)<<31 |
			(uint64(in[54])-minv)<<34 |
			(uint64(in[55])-minv)<<37 |
			(uint64(in[56])-minv)<<40 |
			(uint64(in[57])-minv)<<43 |
			(uint64(in[58])-minv)<<46 |
			(uint64(in[59])-minv)<<49 |
			(uint64(in[60])-minv)<<52 |
			(uint64(in[61])-minv)<<55 |
			(uint64(in[62])-minv)<<58 |
			(uint64(in[63])-minv)<<61

}
func bp32_4[T uint32 | int32](in *[64]T, out *[4]uint64, minv uint64) {
	out[0] =
		(uint64(in[0])-minv)<<0 |
			(uint64(in[1])-minv)<<4 |
			(uint64(in[2])-minv)<<8 |
			(uint64(in[3])-minv)<<12 |
			(uint64(in[4])-minv)<<16 |
			(uint64(in[5])-minv)<<20 |
			(uint64(in[6])-minv)<<24 |
			(uint64(in[7])-minv)<<28 |
			(uint64(in[8])-minv)<<32 |
			(uint64(in[9])-minv)<<36 |
			(uint64(in[10])-minv)<<40 |
			(uint64(in[11])-minv)<<44 |
			(uint64(in[12])-minv)<<48 |
			(uint64(in[13])-minv)<<52 |
			(uint64(in[14])-minv)<<56 |
			(uint64(in[15])-minv)<<60

	out[1] =
		(uint64(in[16])-minv)<<0 |
			(uint64(in[17])-minv)<<4 |
			(uint64(in[18])-minv)<<8 |
			(uint64(in[19])-minv)<<12 |
			(uint64(in[20])-minv)<<16 |
			(uint64(in[21])-minv)<<20 |
			(uint64(in[22])-minv)<<24 |
			(uint64(in[23])-minv)<<28 |
			(uint64(in[24])-minv)<<32 |
			(uint64(in[25])-minv)<<36 |
			(uint64(in[26])-minv)<<40 |
			(uint64(in[27])-minv)<<44 |
			(uint64(in[28])-minv)<<48 |
			(uint64(in[29])-minv)<<52 |
			(uint64(in[30])-minv)<<56 |
			(uint64(in[31])-minv)<<60

	out[2] =
		(uint64(in[32])-minv)<<0 |
			(uint64(in[33])-minv)<<4 |
			(uint64(in[34])-minv)<<8 |
			(uint64(in[35])-minv)<<12 |
			(uint64(in[36])-minv)<<16 |
			(uint64(in[37])-minv)<<20 |
			(uint64(in[38])-minv)<<24 |
			(uint64(in[39])-minv)<<28 |
			(uint64(in[40])-minv)<<32 |
			(uint64(in[41])-minv)<<36 |
			(uint64(in[42])-minv)<<40 |
			(uint64(in[43])-minv)<<44 |
			(uint64(in[44])-minv)<<48 |
			(uint64(in[45])-minv)<<52 |
			(uint64(in[46])-minv)<<56 |
			(uint64(in[47])-minv)<<60

	out[3] =
		(uint64(in[48])-minv)<<0 |
			(uint64(in[49])-minv)<<4 |
			(uint64(in[50])-minv)<<8 |
			(uint64(in[51])-minv)<<12 |
			(uint64(in[52])-minv)<<16 |
			(uint64(in[53])-minv)<<20 |
			(uint64(in[54])-minv)<<24 |
			(uint64(in[55])-minv)<<28 |
			(uint64(in[56])-minv)<<32 |
			(uint64(in[57])-minv)<<36 |
			(uint64(in[58])-minv)<<40 |
			(uint64(in[59])-minv)<<44 |
			(uint64(in[60])-minv)<<48 |
			(uint64(in[61])-minv)<<52 |
			(uint64(in[62])-minv)<<56 |
			(uint64(in[63])-minv)<<60

}
func bp32_5[T uint32 | int32](in *[64]T, out *[5]uint64, minv uint64) {
	out[0] =
		(uint64(in[0])-minv)<<0 |
			(uint64(in[1])-minv)<<5 |
			(uint64(in[2])-minv)<<10 |
			(uint64(in[3])-minv)<<15 |
			(uint64(in[4])-minv)<<20 |
			(uint64(in[5])-minv)<<25 |
			(uint64(in[6])-minv)<<30 |
			(uint64(in[7])-minv)<<35 |
			(uint64(in[8])-minv)<<40 |
			(uint64(in[9])-minv)<<45 |
			(uint64(in[10])-minv)<<50 |
			(uint64(in[11])-minv)<<55 |
			(uint64(in[12])-minv)<<60

	out[1] =
		(uint64(in[12])-minv)>>4 |

			(uint64(in[13])-minv)<<1 |
			(uint64(in[14])-minv)<<6 |
			(uint64(in[15])-minv)<<11 |
			(uint64(in[16])-minv)<<16 |
			(uint64(in[17])-minv)<<21 |
			(uint64(in[18])-minv)<<26 |
			(uint64(in[19])-minv)<<31 |
			(uint64(in[20])-minv)<<36 |
			(uint64(in[21])-minv)<<41 |
			(uint64(in[22])-minv)<<46 |
			(uint64(in[23])-minv)<<51 |
			(uint64(in[24])-minv)<<56 |
			(uint64(in[25])-minv)<<61

	out[2] =
		(uint64(in[25])-minv)>>3 |

			(uint64(in[26])-minv)<<2 |
			(uint64(in[27])-minv)<<7 |
			(uint64(in[28])-minv)<<12 |
			(uint64(in[29])-minv)<<17 |
			(uint64(in[30])-minv)<<22 |
			(uint64(in[31])-minv)<<27 |
			(uint64(in[32])-minv)<<32 |
			(uint64(in[33])-minv)<<37 |
			(uint64(in[34])-minv)<<42 |
			(uint64(in[35])-minv)<<47 |
			(uint64(in[36])-minv)<<52 |
			(uint64(in[37])-minv)<<57 |
			(uint64(in[38])-minv)<<62

	out[3] =
		(uint64(in[38])-minv)>>2 |

			(uint64(in[39])-minv)<<3 |
			(uint64(in[40])-minv)<<8 |
			(uint64(in[41])-minv)<<13 |
			(uint64(in[42])-minv)<<18 |
			(uint64(in[43])-minv)<<23 |
			(uint64(in[44])-minv)<<28 |
			(uint64(in[45])-minv)<<33 |
			(uint64(in[46])-minv)<<38 |
			(uint64(in[47])-minv)<<43 |
			(uint64(in[48])-minv)<<48 |
			(uint64(in[49])-minv)<<53 |
			(uint64(in[50])-minv)<<58 |
			(uint64(in[51])-minv)<<63

	out[4] =
		(uint64(in[51])-minv)>>1 |

			(uint64(in[52])-minv)<<4 |
			(uint64(in[53])-minv)<<9 |
			(uint64(in[54])-minv)<<14 |
			(uint64(in[55])-minv)<<19 |
			(uint64(in[56])-minv)<<24 |
			(uint64(in[57])-minv)<<29 |
			(uint64(in[58])-minv)<<34 |
			(uint64(in[59])-minv)<<39 |
			(uint64(in[60])-minv)<<44 |
			(uint64(in[61])-minv)<<49 |
			(uint64(in[62])-minv)<<54 |
			(uint64(in[63])-minv)<<59

}
func bp32_6[T uint32 | int32](in *[64]T, out *[6]uint64, minv uint64) {
	out[0] =
		(uint64(in[0])-minv)<<0 |
			(uint64(in[1])-minv)<<6 |
			(uint64(in[2])-minv)<<12 |
			(uint64(in[3])-minv)<<18 |
			(uint64(in[4])-minv)<<24 |
			(uint64(in[5])-minv)<<30 |
			(uint64(in[6])-minv)<<36 |
			(uint64(in[7])-minv)<<42 |
			(uint64(in[8])-minv)<<48 |
			(uint64(in[9])-minv)<<54 |
			(uint64(in[10])-minv)<<60

	out[1] =
		(uint64(in[10])-minv)>>4 |

			(uint64(in[11])-minv)<<2 |
			(uint64(in[12])-minv)<<8 |
			(uint64(in[13])-minv)<<14 |
			(uint64(in[14])-minv)<<20 |
			(uint64(in[15])-minv)<<26 |
			(uint64(in[16])-minv)<<32 |
			(uint64(in[17])-minv)<<38 |
			(uint64(in[18])-minv)<<44 |
			(uint64(in[19])-minv)<<50 |
			(uint64(in[20])-minv)<<56 |
			(uint64(in[21])-minv)<<62

	out[2] =
		(uint64(in[21])-minv)>>2 |

			(uint64(in[22])-minv)<<4 |
			(uint64(in[23])-minv)<<10 |
			(uint64(in[24])-minv)<<16 |
			(uint64(in[25])-minv)<<22 |
			(uint64(in[26])-minv)<<28 |
			(uint64(in[27])-minv)<<34 |
			(uint64(in[28])-minv)<<40 |
			(uint64(in[29])-minv)<<46 |
			(uint64(in[30])-minv)<<52 |
			(uint64(in[31])-minv)<<58

	out[3] =
		(uint64(in[31])-minv)>>6 |

			(uint64(in[32])-minv)<<0 |
			(uint64(in[33])-minv)<<6 |
			(uint64(in[34])-minv)<<12 |
			(uint64(in[35])-minv)<<18 |
			(uint64(in[36])-minv)<<24 |
			(uint64(in[37])-minv)<<30 |
			(uint64(in[38])-minv)<<36 |
			(uint64(in[39])-minv)<<42 |
			(uint64(in[40])-minv)<<48 |
			(uint64(in[41])-minv)<<54 |
			(uint64(in[42])-minv)<<60

	out[4] =
		(uint64(in[42])-minv)>>4 |

			(uint64(in[43])-minv)<<2 |
			(uint64(in[44])-minv)<<8 |
			(uint64(in[45])-minv)<<14 |
			(uint64(in[46])-minv)<<20 |
			(uint64(in[47])-minv)<<26 |
			(uint64(in[48])-minv)<<32 |
			(uint64(in[49])-minv)<<38 |
			(uint64(in[50])-minv)<<44 |
			(uint64(in[51])-minv)<<50 |
			(uint64(in[52])-minv)<<56 |
			(uint64(in[53])-minv)<<62

	out[5] =
		(uint64(in[53])-minv)>>2 |

			(uint64(in[54])-minv)<<4 |
			(uint64(in[55])-minv)<<10 |
			(uint64(in[56])-minv)<<16 |
			(uint64(in[57])-minv)<<22 |
			(uint64(in[58])-minv)<<28 |
			(uint64(in[59])-minv)<<34 |
			(uint64(in[60])-minv)<<40 |
			(uint64(in[61])-minv)<<46 |
			(uint64(in[62])-minv)<<52 |
			(uint64(in[63])-minv)<<58

}
func bp32_7[T uint32 | int32](in *[64]T, out *[7]uint64, minv uint64) {
	out[0] =
		(uint64(in[0])-minv)<<0 |
			(uint64(in[1])-minv)<<7 |
			(uint64(in[2])-minv)<<14 |
			(uint64(in[3])-minv)<<21 |
			(uint64(in[4])-minv)<<28 |
			(uint64(in[5])-minv)<<35 |
			(uint64(in[6])-minv)<<42 |
			(uint64(in[7])-minv)<<49 |
			(uint64(in[8])-minv)<<56 |
			(uint64(in[9])-minv)<<63

	out[1] =
		(uint64(in[9])-minv)>>1 |

			(uint64(in[10])-minv)<<6 |
			(uint64(in[11])-minv)<<13 |
			(uint64(in[12])-minv)<<20 |
			(uint64(in[13])-minv)<<27 |
			(uint64(in[14])-minv)<<34 |
			(uint64(in[15])-minv)<<41 |
			(uint64(in[16])-minv)<<48 |
			(uint64(in[17])-minv)<<55 |
			(uint64(in[18])-minv)<<62

	out[2] =
		(uint64(in[18])-minv)>>2 |

			(uint64(in[19])-minv)<<5 |
			(uint64(in[20])-minv)<<12 |
			(uint64(in[21])-minv)<<19 |
			(uint64(in[22])-minv)<<26 |
			(uint64(in[23])-minv)<<33 |
			(uint64(in[24])-minv)<<40 |
			(uint64(in[25])-minv)<<47 |
			(uint64(in[26])-minv)<<54 |
			(uint64(in[27])-minv)<<61

	out[3] =
		(uint64(in[27])-minv)>>3 |

			(uint64(in[28])-minv)<<4 |
			(uint64(in[29])-minv)<<11 |
			(uint64(in[30])-minv)<<18 |
			(uint64(in[31])-minv)<<25 |
			(uint64(in[32])-minv)<<32 |
			(uint64(in[33])-minv)<<39 |
			(uint64(in[34])-minv)<<46 |
			(uint64(in[35])-minv)<<53 |
			(uint64(in[36])-minv)<<60

	out[4] =
		(uint64(in[36])-minv)>>4 |

			(uint64(in[37])-minv)<<3 |
			(uint64(in[38])-minv)<<10 |
			(uint64(in[39])-minv)<<17 |
			(uint64(in[40])-minv)<<24 |
			(uint64(in[41])-minv)<<31 |
			(uint64(in[42])-minv)<<38 |
			(uint64(in[43])-minv)<<45 |
			(uint64(in[44])-minv)<<52 |
			(uint64(in[45])-minv)<<59

	out[5] =
		(uint64(in[45])-minv)>>5 |

			(uint64(in[46])-minv)<<2 |
			(uint64(in[47])-minv)<<9 |
			(uint64(in[48])-minv)<<16 |
			(uint64(in[49])-minv)<<23 |
			(uint64(in[50])-minv)<<30 |
			(uint64(in[51])-minv)<<37 |
			(uint64(in[52])-minv)<<44 |
			(uint64(in[53])-minv)<<51 |
			(uint64(in[54])-minv)<<58

	out[6] =
		(uint64(in[54])-minv)>>6 |

			(uint64(in[55])-minv)<<1 |
			(uint64(in[56])-minv)<<8 |
			(uint64(in[57])-minv)<<15 |
			(uint64(in[58])-minv)<<22 |
			(uint64(in[59])-minv)<<29 |
			(uint64(in[60])-minv)<<36 |
			(uint64(in[61])-minv)<<43 |
			(uint64(in[62])-minv)<<50 |
			(uint64(in[63])-minv)<<57

}
func bp32_8[T uint32 | int32](in *[64]T, out *[8]uint64, minv uint64) {
	out[0] =
		(uint64(in[0])-minv)<<0 |
			(uint64(in[1])-minv)<<8 |
			(uint64(in[2])-minv)<<16 |
			(uint64(in[3])-minv)<<24 |
			(uint64(in[4])-minv)<<32 |
			(uint64(in[5])-minv)<<40 |
			(uint64(in[6])-minv)<<48 |
			(uint64(in[7])-minv)<<56

	out[1] =
		(uint64(in[8])-minv)<<0 |
			(uint64(in[9])-minv)<<8 |
			(uint64(in[10])-minv)<<16 |
			(uint64(in[11])-minv)<<24 |
			(uint64(in[12])-minv)<<32 |
			(uint64(in[13])-minv)<<40 |
			(uint64(in[14])-minv)<<48 |
			(uint64(in[15])-minv)<<56

	out[2] =
		(uint64(in[16])-minv)<<0 |
			(uint64(in[17])-minv)<<8 |
			(uint64(in[18])-minv)<<16 |
			(uint64(in[19])-minv)<<24 |
			(uint64(in[20])-minv)<<32 |
			(uint64(in[21])-minv)<<40 |
			(uint64(in[22])-minv)<<48 |
			(uint64(in[23])-minv)<<56

	out[3] =
		(uint64(in[24])-minv)<<0 |
			(uint64(in[25])-minv)<<8 |
			(uint64(in[26])-minv)<<16 |
			(uint64(in[27])-minv)<<24 |
			(uint64(in[28])-minv)<<32 |
			(uint64(in[29])-minv)<<40 |
			(uint64(in[30])-minv)<<48 |
			(uint64(in[31])-minv)<<56

	out[4] =
		(uint64(in[32])-minv)<<0 |
			(uint64(in[33])-minv)<<8 |
			(uint64(in[34])-minv)<<16 |
			(uint64(in[35])-minv)<<24 |
			(uint64(in[36])-minv)<<32 |
			(uint64(in[37])-minv)<<40 |
			(uint64(in[38])-minv)<<48 |
			(uint64(in[39])-minv)<<56

	out[5] =
		(uint64(in[40])-minv)<<0 |
			(uint64(in[41])-minv)<<8 |
			(uint64(in[42])-minv)<<16 |
			(uint64(in[43])-minv)<<24 |
			(uint64(in[44])-minv)<<32 |
			(uint64(in[45])-minv)<<40 |
			(uint64(in[46])-minv)<<48 |
			(uint64(in[47])-minv)<<56

	out[6] =
		(uint64(in[48])-minv)<<0 |
			(uint64(in[49])-minv)<<8 |
			(uint64(in[50])-minv)<<16 |
			(uint64(in[51])-minv)<<24 |
			(uint64(in[52])-minv)<<32 |
			(uint64(in[53])-minv)<<40 |
			(uint64(in[54])-minv)<<48 |
			(uint64(in[55])-minv)<<56

	out[7] =
		(uint64(in[56])-minv)<<0 |
			(uint64(in[57])-minv)<<8 |
			(uint64(in[58])-minv)<<16 |
			(uint64(in[59])-minv)<<24 |
			(uint64(in[60])-minv)<<32 |
			(uint64(in[61])-minv)<<40 |
			(uint64(in[62])-minv)<<48 |
			(uint64(in[63])-minv)<<56

}
func bp32_9[T uint32 | int32](in *[64]T, out *[9]uint64, minv uint64) {
	out[0] =
		(uint64(in[0])-minv)<<0 |
			(uint64(in[1])-minv)<<9 |
			(uint64(in[2])-minv)<<18 |
			(uint64(in[3])-minv)<<27 |
			(uint64(in[4])-minv)<<36 |
			(uint64(in[5])-minv)<<45 |
			(uint64(in[6])-minv)<<54 |
			(uint64(in[7])-minv)<<63

	out[1] =
		(uint64(in[7])-minv)>>1 |

			(uint64(in[8])-minv)<<8 |
			(uint64(in[9])-minv)<<17 |
			(uint64(in[10])-minv)<<26 |
			(uint64(in[11])-minv)<<35 |
			(uint64(in[12])-minv)<<44 |
			(uint64(in[13])-minv)<<53 |
			(uint64(in[14])-minv)<<62

	out[2] =
		(uint64(in[14])-minv)>>2 |

			(uint64(in[15])-minv)<<7 |
			(uint64(in[16])-minv)<<16 |
			(uint64(in[17])-minv)<<25 |
			(uint64(in[18])-minv)<<34 |
			(uint64(in[19])-minv)<<43 |
			(uint64(in[20])-minv)<<52 |
			(uint64(in[21])-minv)<<61

	out[3] =
		(uint64(in[21])-minv)>>3 |

			(uint64(in[22])-minv)<<6 |
			(uint64(in[23])-minv)<<15 |
			(uint64(in[24])-minv)<<24 |
			(uint64(in[25])-minv)<<33 |
			(uint64(in[26])-minv)<<42 |
			(uint64(in[27])-minv)<<51 |
			(uint64(in[28])-minv)<<60

	out[4] =
		(uint64(in[28])-minv)>>4 |

			(uint64(in[29])-minv)<<5 |
			(uint64(in[30])-minv)<<14 |
			(uint64(in[31])-minv)<<23 |
			(uint64(in[32])-minv)<<32 |
			(uint64(in[33])-minv)<<41 |
			(uint64(in[34])-minv)<<50 |
			(uint64(in[35])-minv)<<59

	out[5] =
		(uint64(in[35])-minv)>>5 |

			(uint64(in[36])-minv)<<4 |
			(uint64(in[37])-minv)<<13 |
			(uint64(in[38])-minv)<<22 |
			(uint64(in[39])-minv)<<31 |
			(uint64(in[40])-minv)<<40 |
			(uint64(in[41])-minv)<<49 |
			(uint64(in[42])-minv)<<58

	out[6] =
		(uint64(in[42])-minv)>>6 |

			(uint64(in[43])-minv)<<3 |
			(uint64(in[44])-minv)<<12 |
			(uint64(in[45])-minv)<<21 |
			(uint64(in[46])-minv)<<30 |
			(uint64(in[47])-minv)<<39 |
			(uint64(in[48])-minv)<<48 |
			(uint64(in[49])-minv)<<57

	out[7] =
		(uint64(in[49])-minv)>>7 |

			(uint64(in[50])-minv)<<2 |
			(uint64(in[51])-minv)<<11 |
			(uint64(in[52])-minv)<<20 |
			(uint64(in[53])-minv)<<29 |
			(uint64(in[54])-minv)<<38 |
			(uint64(in[55])-minv)<<47 |
			(uint64(in[56])-minv)<<56

	out[8] =
		(uint64(in[56])-minv)>>8 |

			(uint64(in[57])-minv)<<1 |
			(uint64(in[58])-minv)<<10 |
			(uint64(in[59])-minv)<<19 |
			(uint64(in[60])-minv)<<28 |
			(uint64(in[61])-minv)<<37 |
			(uint64(in[62])-minv)<<46 |
			(uint64(in[63])-minv)<<55

}
func bp32_10[T uint32 | int32](in *[64]T, out *[10]uint64, minv uint64) {
	out[0] =
		(uint64(in[0])-minv)<<0 |
			(uint64(in[1])-minv)<<10 |
			(uint64(in[2])-minv)<<20 |
			(uint64(in[3])-minv)<<30 |
			(uint64(in[4])-minv)<<40 |
			(uint64(in[5])-minv)<<50 |
			(uint64(in[6])-minv)<<60

	out[1] =
		(uint64(in[6])-minv)>>4 |

			(uint64(in[7])-minv)<<6 |
			(uint64(in[8])-minv)<<16 |
			(uint64(in[9])-minv)<<26 |
			(uint64(in[10])-minv)<<36 |
			(uint64(in[11])-minv)<<46 |
			(uint64(in[12])-minv)<<56

	out[2] =
		(uint64(in[12])-minv)>>8 |

			(uint64(in[13])-minv)<<2 |
			(uint64(in[14])-minv)<<12 |
			(uint64(in[15])-minv)<<22 |
			(uint64(in[16])-minv)<<32 |
			(uint64(in[17])-minv)<<42 |
			(uint64(in[18])-minv)<<52 |
			(uint64(in[19])-minv)<<62

	out[3] =
		(uint64(in[19])-minv)>>2 |

			(uint64(in[20])-minv)<<8 |
			(uint64(in[21])-minv)<<18 |
			(uint64(in[22])-minv)<<28 |
			(uint64(in[23])-minv)<<38 |
			(uint64(in[24])-minv)<<48 |
			(uint64(in[25])-minv)<<58

	out[4] =
		(uint64(in[25])-minv)>>6 |

			(uint64(in[26])-minv)<<4 |
			(uint64(in[27])-minv)<<14 |
			(uint64(in[28])-minv)<<24 |
			(uint64(in[29])-minv)<<34 |
			(uint64(in[30])-minv)<<44 |
			(uint64(in[31])-minv)<<54

	out[5] =
		(uint64(in[31])-minv)>>10 |

			(uint64(in[32])-minv)<<0 |
			(uint64(in[33])-minv)<<10 |
			(uint64(in[34])-minv)<<20 |
			(uint64(in[35])-minv)<<30 |
			(uint64(in[36])-minv)<<40 |
			(uint64(in[37])-minv)<<50 |
			(uint64(in[38])-minv)<<60

	out[6] =
		(uint64(in[38])-minv)>>4 |

			(uint64(in[39])-minv)<<6 |
			(uint64(in[40])-minv)<<16 |
			(uint64(in[41])-minv)<<26 |
			(uint64(in[42])-minv)<<36 |
			(uint64(in[43])-minv)<<46 |
			(uint64(in[44])-minv)<<56

	out[7] =
		(uint64(in[44])-minv)>>8 |

			(uint64(in[45])-minv)<<2 |
			(uint64(in[46])-minv)<<12 |
			(uint64(in[47])-minv)<<22 |
			(uint64(in[48])-minv)<<32 |
			(uint64(in[49])-minv)<<42 |
			(uint64(in[50])-minv)<<52 |
			(uint64(in[51])-minv)<<62

	out[8] =
		(uint64(in[51])-minv)>>2 |

			(uint64(in[52])-minv)<<8 |
			(uint64(in[53])-minv)<<18 |
			(uint64(in[54])-minv)<<28 |
			(uint64(in[55])-minv)<<38 |
			(uint64(in[56])-minv)<<48 |
			(uint64(in[57])-minv)<<58

	out[9] =
		(uint64(in[57])-minv)>>6 |

			(uint64(in[58])-minv)<<4 |
			(uint64(in[59])-minv)<<14 |
			(uint64(in[60])-minv)<<24 |
			(uint64(in[61])-minv)<<34 |
			(uint64(in[62])-minv)<<44 |
			(uint64(in[63])-minv)<<54

}
func bp32_11[T uint32 | int32](in *[64]T, out *[11]uint64, minv uint64) {
	out[0] =
		(uint64(in[0])-minv)<<0 |
			(uint64(in[1])-minv)<<11 |
			(uint64(in[2])-minv)<<22 |
			(uint64(in[3])-minv)<<33 |
			(uint64(in[4])-minv)<<44 |
			(uint64(in[5])-minv)<<55

	out[1] =
		(uint64(in[5])-minv)>>9 |

			(uint64(in[6])-minv)<<2 |
			(uint64(in[7])-minv)<<13 |
			(uint64(in[8])-minv)<<24 |
			(uint64(in[9])-minv)<<35 |
			(uint64(in[10])-minv)<<46 |
			(uint64(in[11])-minv)<<57

	out[2] =
		(uint64(in[11])-minv)>>7 |

			(uint64(in[12])-minv)<<4 |
			(uint64(in[13])-minv)<<15 |
			(uint64(in[14])-minv)<<26 |
			(uint64(in[15])-minv)<<37 |
			(uint64(in[16])-minv)<<48 |
			(uint64(in[17])-minv)<<59

	out[3] =
		(uint64(in[17])-minv)>>5 |

			(uint64(in[18])-minv)<<6 |
			(uint64(in[19])-minv)<<17 |
			(uint64(in[20])-minv)<<28 |
			(uint64(in[21])-minv)<<39 |
			(uint64(in[22])-minv)<<50 |
			(uint64(in[23])-minv)<<61

	out[4] =
		(uint64(in[23])-minv)>>3 |

			(uint64(in[24])-minv)<<8 |
			(uint64(in[25])-minv)<<19 |
			(uint64(in[26])-minv)<<30 |
			(uint64(in[27])-minv)<<41 |
			(uint64(in[28])-minv)<<52 |
			(uint64(in[29])-minv)<<63

	out[5] =
		(uint64(in[29])-minv)>>1 |

			(uint64(in[30])-minv)<<10 |
			(uint64(in[31])-minv)<<21 |
			(uint64(in[32])-minv)<<32 |
			(uint64(in[33])-minv)<<43 |
			(uint64(in[34])-minv)<<54

	out[6] =
		(uint64(in[34])-minv)>>10 |

			(uint64(in[35])-minv)<<1 |
			(uint64(in[36])-minv)<<12 |
			(uint64(in[37])-minv)<<23 |
			(uint64(in[38])-minv)<<34 |
			(uint64(in[39])-minv)<<45 |
			(uint64(in[40])-minv)<<56

	out[7] =
		(uint64(in[40])-minv)>>8 |

			(uint64(in[41])-minv)<<3 |
			(uint64(in[42])-minv)<<14 |
			(uint64(in[43])-minv)<<25 |
			(uint64(in[44])-minv)<<36 |
			(uint64(in[45])-minv)<<47 |
			(uint64(in[46])-minv)<<58

	out[8] =
		(uint64(in[46])-minv)>>6 |

			(uint64(in[47])-minv)<<5 |
			(uint64(in[48])-minv)<<16 |
			(uint64(in[49])-minv)<<27 |
			(uint64(in[50])-minv)<<38 |
			(uint64(in[51])-minv)<<49 |
			(uint64(in[52])-minv)<<60

	out[9] =
		(uint64(in[52])-minv)>>4 |

			(uint64(in[53])-minv)<<7 |
			(uint64(in[54])-minv)<<18 |
			(uint64(in[55])-minv)<<29 |
			(uint64(in[56])-minv)<<40 |
			(uint64(in[57])-minv)<<51 |
			(uint64(in[58])-minv)<<62

	out[10] =
		(uint64(in[58])-minv)>>2 |

			(uint64(in[59])-minv)<<9 |
			(uint64(in[60])-minv)<<20 |
			(uint64(in[61])-minv)<<31 |
			(uint64(in[62])-minv)<<42 |
			(uint64(in[63])-minv)<<53

}
func bp32_12[T uint32 | int32](in *[64]T, out *[12]uint64, minv uint64) {
	out[0] =
		(uint64(in[0])-minv)<<0 |
			(uint64(in[1])-minv)<<12 |
			(uint64(in[2])-minv)<<24 |
			(uint64(in[3])-minv)<<36 |
			(uint64(in[4])-minv)<<48 |
			(uint64(in[5])-minv)<<60

	out[1] =
		(uint64(in[5])-minv)>>4 |

			(uint64(in[6])-minv)<<8 |
			(uint64(in[7])-minv)<<20 |
			(uint64(in[8])-minv)<<32 |
			(uint64(in[9])-minv)<<44 |
			(uint64(in[10])-minv)<<56

	out[2] =
		(uint64(in[10])-minv)>>8 |

			(uint64(in[11])-minv)<<4 |
			(uint64(in[12])-minv)<<16 |
			(uint64(in[13])-minv)<<28 |
			(uint64(in[14])-minv)<<40 |
			(uint64(in[15])-minv)<<52

	out[3] =
		(uint64(in[15])-minv)>>12 |

			(uint64(in[16])-minv)<<0 |
			(uint64(in[17])-minv)<<12 |
			(uint64(in[18])-minv)<<24 |
			(uint64(in[19])-minv)<<36 |
			(uint64(in[20])-minv)<<48 |
			(uint64(in[21])-minv)<<60

	out[4] =
		(uint64(in[21])-minv)>>4 |

			(uint64(in[22])-minv)<<8 |
			(uint64(in[23])-minv)<<20 |
			(uint64(in[24])-minv)<<32 |
			(uint64(in[25])-minv)<<44 |
			(uint64(in[26])-minv)<<56

	out[5] =
		(uint64(in[26])-minv)>>8 |

			(uint64(in[27])-minv)<<4 |
			(uint64(in[28])-minv)<<16 |
			(uint64(in[29])-minv)<<28 |
			(uint64(in[30])-minv)<<40 |
			(uint64(in[31])-minv)<<52

	out[6] =
		(uint64(in[31])-minv)>>12 |

			(uint64(in[32])-minv)<<0 |
			(uint64(in[33])-minv)<<12 |
			(uint64(in[34])-minv)<<24 |
			(uint64(in[35])-minv)<<36 |
			(uint64(in[36])-minv)<<48 |
			(uint64(in[37])-minv)<<60

	out[7] =
		(uint64(in[37])-minv)>>4 |

			(uint64(in[38])-minv)<<8 |
			(uint64(in[39])-minv)<<20 |
			(uint64(in[40])-minv)<<32 |
			(uint64(in[41])-minv)<<44 |
			(uint64(in[42])-minv)<<56

	out[8] =
		(uint64(in[42])-minv)>>8 |

			(uint64(in[43])-minv)<<4 |
			(uint64(in[44])-minv)<<16 |
			(uint64(in[45])-minv)<<28 |
			(uint64(in[46])-minv)<<40 |
			(uint64(in[47])-minv)<<52

	out[9] =
		(uint64(in[47])-minv)>>12 |

			(uint64(in[48])-minv)<<0 |
			(uint64(in[49])-minv)<<12 |
			(uint64(in[50])-minv)<<24 |
			(uint64(in[51])-minv)<<36 |
			(uint64(in[52])-minv)<<48 |
			(uint64(in[53])-minv)<<60

	out[10] =
		(uint64(in[53])-minv)>>4 |

			(uint64(in[54])-minv)<<8 |
			(uint64(in[55])-minv)<<20 |
			(uint64(in[56])-minv)<<32 |
			(uint64(in[57])-minv)<<44 |
			(uint64(in[58])-minv)<<56

	out[11] =
		(uint64(in[58])-minv)>>8 |

			(uint64(in[59])-minv)<<4 |
			(uint64(in[60])-minv)<<16 |
			(uint64(in[61])-minv)<<28 |
			(uint64(in[62])-minv)<<40 |
			(uint64(in[63])-minv)<<52

}
func bp32_13[T uint32 | int32](in *[64]T, out *[13]uint64, minv uint64) {
	out[0] =
		(uint64(in[0])-minv)<<0 |
			(uint64(in[1])-minv)<<13 |
			(uint64(in[2])-minv)<<26 |
			(uint64(in[3])-minv)<<39 |
			(uint64(in[4])-minv)<<52

	out[1] =
		(uint64(in[4])-minv)>>12 |

			(uint64(in[5])-minv)<<1 |
			(uint64(in[6])-minv)<<14 |
			(uint64(in[7])-minv)<<27 |
			(uint64(in[8])-minv)<<40 |
			(uint64(in[9])-minv)<<53

	out[2] =
		(uint64(in[9])-minv)>>11 |

			(uint64(in[10])-minv)<<2 |
			(uint64(in[11])-minv)<<15 |
			(uint64(in[12])-minv)<<28 |
			(uint64(in[13])-minv)<<41 |
			(uint64(in[14])-minv)<<54

	out[3] =
		(uint64(in[14])-minv)>>10 |

			(uint64(in[15])-minv)<<3 |
			(uint64(in[16])-minv)<<16 |
			(uint64(in[17])-minv)<<29 |
			(uint64(in[18])-minv)<<42 |
			(uint64(in[19])-minv)<<55

	out[4] =
		(uint64(in[19])-minv)>>9 |

			(uint64(in[20])-minv)<<4 |
			(uint64(in[21])-minv)<<17 |
			(uint64(in[22])-minv)<<30 |
			(uint64(in[23])-minv)<<43 |
			(uint64(in[24])-minv)<<56

	out[5] =
		(uint64(in[24])-minv)>>8 |

			(uint64(in[25])-minv)<<5 |
			(uint64(in[26])-minv)<<18 |
			(uint64(in[27])-minv)<<31 |
			(uint64(in[28])-minv)<<44 |
			(uint64(in[29])-minv)<<57

	out[6] =
		(uint64(in[29])-minv)>>7 |

			(uint64(in[30])-minv)<<6 |
			(uint64(in[31])-minv)<<19 |
			(uint64(in[32])-minv)<<32 |
			(uint64(in[33])-minv)<<45 |
			(uint64(in[34])-minv)<<58

	out[7] =
		(uint64(in[34])-minv)>>6 |

			(uint64(in[35])-minv)<<7 |
			(uint64(in[36])-minv)<<20 |
			(uint64(in[37])-minv)<<33 |
			(uint64(in[38])-minv)<<46 |
			(uint64(in[39])-minv)<<59

	out[8] =
		(uint64(in[39])-minv)>>5 |

			(uint64(in[40])-minv)<<8 |
			(uint64(in[41])-minv)<<21 |
			(uint64(in[42])-minv)<<34 |
			(uint64(in[43])-minv)<<47 |
			(uint64(in[44])-minv)<<60

	out[9] =
		(uint64(in[44])-minv)>>4 |

			(uint64(in[45])-minv)<<9 |
			(uint64(in[46])-minv)<<22 |
			(uint64(in[47])-minv)<<35 |
			(uint64(in[48])-minv)<<48 |
			(uint64(in[49])-minv)<<61

	out[10] =
		(uint64(in[49])-minv)>>3 |

			(uint64(in[50])-minv)<<10 |
			(uint64(in[51])-minv)<<23 |
			(uint64(in[52])-minv)<<36 |
			(uint64(in[53])-minv)<<49 |
			(uint64(in[54])-minv)<<62

	out[11] =
		(uint64(in[54])-minv)>>2 |

			(uint64(in[55])-minv)<<11 |
			(uint64(in[56])-minv)<<24 |
			(uint64(in[57])-minv)<<37 |
			(uint64(in[58])-minv)<<50 |
			(uint64(in[59])-minv)<<63

	out[12] =
		(uint64(in[59])-minv)>>1 |

			(uint64(in[60])-minv)<<12 |
			(uint64(in[61])-minv)<<25 |
			(uint64(in[62])-minv)<<38 |
			(uint64(in[63])-minv)<<51

}
func bp32_14[T uint32 | int32](in *[64]T, out *[14]uint64, minv uint64) {
	out[0] =
		(uint64(in[0])-minv)<<0 |
			(uint64(in[1])-minv)<<14 |
			(uint64(in[2])-minv)<<28 |
			(uint64(in[3])-minv)<<42 |
			(uint64(in[4])-minv)<<56

	out[1] =
		(uint64(in[4])-minv)>>8 |

			(uint64(in[5])-minv)<<6 |
			(uint64(in[6])-minv)<<20 |
			(uint64(in[7])-minv)<<34 |
			(uint64(in[8])-minv)<<48 |
			(uint64(in[9])-minv)<<62

	out[2] =
		(uint64(in[9])-minv)>>2 |

			(uint64(in[10])-minv)<<12 |
			(uint64(in[11])-minv)<<26 |
			(uint64(in[12])-minv)<<40 |
			(uint64(in[13])-minv)<<54

	out[3] =
		(uint64(in[13])-minv)>>10 |

			(uint64(in[14])-minv)<<4 |
			(uint64(in[15])-minv)<<18 |
			(uint64(in[16])-minv)<<32 |
			(uint64(in[17])-minv)<<46 |
			(uint64(in[18])-minv)<<60

	out[4] =
		(uint64(in[18])-minv)>>4 |

			(uint64(in[19])-minv)<<10 |
			(uint64(in[20])-minv)<<24 |
			(uint64(in[21])-minv)<<38 |
			(uint64(in[22])-minv)<<52

	out[5] =
		(uint64(in[22])-minv)>>12 |

			(uint64(in[23])-minv)<<2 |
			(uint64(in[24])-minv)<<16 |
			(uint64(in[25])-minv)<<30 |
			(uint64(in[26])-minv)<<44 |
			(uint64(in[27])-minv)<<58

	out[6] =
		(uint64(in[27])-minv)>>6 |

			(uint64(in[28])-minv)<<8 |
			(uint64(in[29])-minv)<<22 |
			(uint64(in[30])-minv)<<36 |
			(uint64(in[31])-minv)<<50

	out[7] =
		(uint64(in[31])-minv)>>14 |

			(uint64(in[32])-minv)<<0 |
			(uint64(in[33])-minv)<<14 |
			(uint64(in[34])-minv)<<28 |
			(uint64(in[35])-minv)<<42 |
			(uint64(in[36])-minv)<<56

	out[8] =
		(uint64(in[36])-minv)>>8 |

			(uint64(in[37])-minv)<<6 |
			(uint64(in[38])-minv)<<20 |
			(uint64(in[39])-minv)<<34 |
			(uint64(in[40])-minv)<<48 |
			(uint64(in[41])-minv)<<62

	out[9] =
		(uint64(in[41])-minv)>>2 |

			(uint64(in[42])-minv)<<12 |
			(uint64(in[43])-minv)<<26 |
			(uint64(in[44])-minv)<<40 |
			(uint64(in[45])-minv)<<54

	out[10] =
		(uint64(in[45])-minv)>>10 |

			(uint64(in[46])-minv)<<4 |
			(uint64(in[47])-minv)<<18 |
			(uint64(in[48])-minv)<<32 |
			(uint64(in[49])-minv)<<46 |
			(uint64(in[50])-minv)<<60

	out[11] =
		(uint64(in[50])-minv)>>4 |

			(uint64(in[51])-minv)<<10 |
			(uint64(in[52])-minv)<<24 |
			(uint64(in[53])-minv)<<38 |
			(uint64(in[54])-minv)<<52

	out[12] =
		(uint64(in[54])-minv)>>12 |

			(uint64(in[55])-minv)<<2 |
			(uint64(in[56])-minv)<<16 |
			(uint64(in[57])-minv)<<30 |
			(uint64(in[58])-minv)<<44 |
			(uint64(in[59])-minv)<<58

	out[13] =
		(uint64(in[59])-minv)>>6 |

			(uint64(in[60])-minv)<<8 |
			(uint64(in[61])-minv)<<22 |
			(uint64(in[62])-minv)<<36 |
			(uint64(in[63])-minv)<<50

}
func bp32_15[T uint32 | int32](in *[64]T, out *[15]uint64, minv uint64) {
	out[0] =
		(uint64(in[0])-minv)<<0 |
			(uint64(in[1])-minv)<<15 |
			(uint64(in[2])-minv)<<30 |
			(uint64(in[3])-minv)<<45 |
			(uint64(in[4])-minv)<<60

	out[1] =
		(uint64(in[4])-minv)>>4 |

			(uint64(in[5])-minv)<<11 |
			(uint64(in[6])-minv)<<26 |
			(uint64(in[7])-minv)<<41 |
			(uint64(in[8])-minv)<<56

	out[2] =
		(uint64(in[8])-minv)>>8 |

			(uint64(in[9])-minv)<<7 |
			(uint64(in[10])-minv)<<22 |
			(uint64(in[11])-minv)<<37 |
			(uint64(in[12])-minv)<<52

	out[3] =
		(uint64(in[12])-minv)>>12 |

			(uint64(in[13])-minv)<<3 |
			(uint64(in[14])-minv)<<18 |
			(uint64(in[15])-minv)<<33 |
			(uint64(in[16])-minv)<<48 |
			(uint64(in[17])-minv)<<63

	out[4] =
		(uint64(in[17])-minv)>>1 |

			(uint64(in[18])-minv)<<14 |
			(uint64(in[19])-minv)<<29 |
			(uint64(in[20])-minv)<<44 |
			(uint64(in[21])-minv)<<59

	out[5] =
		(uint64(in[21])-minv)>>5 |

			(uint64(in[22])-minv)<<10 |
			(uint64(in[23])-minv)<<25 |
			(uint64(in[24])-minv)<<40 |
			(uint64(in[25])-minv)<<55

	out[6] =
		(uint64(in[25])-minv)>>9 |

			(uint64(in[26])-minv)<<6 |
			(uint64(in[27])-minv)<<21 |
			(uint64(in[28])-minv)<<36 |
			(uint64(in[29])-minv)<<51

	out[7] =
		(uint64(in[29])-minv)>>13 |

			(uint64(in[30])-minv)<<2 |
			(uint64(in[31])-minv)<<17 |
			(uint64(in[32])-minv)<<32 |
			(uint64(in[33])-minv)<<47 |
			(uint64(in[34])-minv)<<62

	out[8] =
		(uint64(in[34])-minv)>>2 |

			(uint64(in[35])-minv)<<13 |
			(uint64(in[36])-minv)<<28 |
			(uint64(in[37])-minv)<<43 |
			(uint64(in[38])-minv)<<58

	out[9] =
		(uint64(in[38])-minv)>>6 |

			(uint64(in[39])-minv)<<9 |
			(uint64(in[40])-minv)<<24 |
			(uint64(in[41])-minv)<<39 |
			(uint64(in[42])-minv)<<54

	out[10] =
		(uint64(in[42])-minv)>>10 |

			(uint64(in[43])-minv)<<5 |
			(uint64(in[44])-minv)<<20 |
			(uint64(in[45])-minv)<<35 |
			(uint64(in[46])-minv)<<50

	out[11] =
		(uint64(in[46])-minv)>>14 |

			(uint64(in[47])-minv)<<1 |
			(uint64(in[48])-minv)<<16 |
			(uint64(in[49])-minv)<<31 |
			(uint64(in[50])-minv)<<46 |
			(uint64(in[51])-minv)<<61

	out[12] =
		(uint64(in[51])-minv)>>3 |

			(uint64(in[52])-minv)<<12 |
			(uint64(in[53])-minv)<<27 |
			(uint64(in[54])-minv)<<42 |
			(uint64(in[55])-minv)<<57

	out[13] =
		(uint64(in[55])-minv)>>7 |

			(uint64(in[56])-minv)<<8 |
			(uint64(in[57])-minv)<<23 |
			(uint64(in[58])-minv)<<38 |
			(uint64(in[59])-minv)<<53

	out[14] =
		(uint64(in[59])-minv)>>11 |

			(uint64(in[60])-minv)<<4 |
			(uint64(in[61])-minv)<<19 |
			(uint64(in[62])-minv)<<34 |
			(uint64(in[63])-minv)<<49

}
func bp32_16[T uint32 | int32](in *[64]T, out *[16]uint64, minv uint64) {
	out[0] =
		(uint64(in[0])-minv)<<0 |
			(uint64(in[1])-minv)<<16 |
			(uint64(in[2])-minv)<<32 |
			(uint64(in[3])-minv)<<48

	out[1] =
		(uint64(in[4])-minv)<<0 |
			(uint64(in[5])-minv)<<16 |
			(uint64(in[6])-minv)<<32 |
			(uint64(in[7])-minv)<<48

	out[2] =
		(uint64(in[8])-minv)<<0 |
			(uint64(in[9])-minv)<<16 |
			(uint64(in[10])-minv)<<32 |
			(uint64(in[11])-minv)<<48

	out[3] =
		(uint64(in[12])-minv)<<0 |
			(uint64(in[13])-minv)<<16 |
			(uint64(in[14])-minv)<<32 |
			(uint64(in[15])-minv)<<48

	out[4] =
		(uint64(in[16])-minv)<<0 |
			(uint64(in[17])-minv)<<16 |
			(uint64(in[18])-minv)<<32 |
			(uint64(in[19])-minv)<<48

	out[5] =
		(uint64(in[20])-minv)<<0 |
			(uint64(in[21])-minv)<<16 |
			(uint64(in[22])-minv)<<32 |
			(uint64(in[23])-minv)<<48

	out[6] =
		(uint64(in[24])-minv)<<0 |
			(uint64(in[25])-minv)<<16 |
			(uint64(in[26])-minv)<<32 |
			(uint64(in[27])-minv)<<48

	out[7] =
		(uint64(in[28])-minv)<<0 |
			(uint64(in[29])-minv)<<16 |
			(uint64(in[30])-minv)<<32 |
			(uint64(in[31])-minv)<<48

	out[8] =
		(uint64(in[32])-minv)<<0 |
			(uint64(in[33])-minv)<<16 |
			(uint64(in[34])-minv)<<32 |
			(uint64(in[35])-minv)<<48

	out[9] =
		(uint64(in[36])-minv)<<0 |
			(uint64(in[37])-minv)<<16 |
			(uint64(in[38])-minv)<<32 |
			(uint64(in[39])-minv)<<48

	out[10] =
		(uint64(in[40])-minv)<<0 |
			(uint64(in[41])-minv)<<16 |
			(uint64(in[42])-minv)<<32 |
			(uint64(in[43])-minv)<<48

	out[11] =
		(uint64(in[44])-minv)<<0 |
			(uint64(in[45])-minv)<<16 |
			(uint64(in[46])-minv)<<32 |
			(uint64(in[47])-minv)<<48

	out[12] =
		(uint64(in[48])-minv)<<0 |
			(uint64(in[49])-minv)<<16 |
			(uint64(in[50])-minv)<<32 |
			(uint64(in[51])-minv)<<48

	out[13] =
		(uint64(in[52])-minv)<<0 |
			(uint64(in[53])-minv)<<16 |
			(uint64(in[54])-minv)<<32 |
			(uint64(in[55])-minv)<<48

	out[14] =
		(uint64(in[56])-minv)<<0 |
			(uint64(in[57])-minv)<<16 |
			(uint64(in[58])-minv)<<32 |
			(uint64(in[59])-minv)<<48

	out[15] =
		(uint64(in[60])-minv)<<0 |
			(uint64(in[61])-minv)<<16 |
			(uint64(in[62])-minv)<<32 |
			(uint64(in[63])-minv)<<48

}
func bp32_17[T uint32 | int32](in *[64]T, out *[17]uint64, minv uint64) {
	out[0] =
		(uint64(in[0])-minv)<<0 |
			(uint64(in[1])-minv)<<17 |
			(uint64(in[2])-minv)<<34 |
			(uint64(in[3])-minv)<<51

	out[1] =
		(uint64(in[3])-minv)>>13 |

			(uint64(in[4])-minv)<<4 |
			(uint64(in[5])-minv)<<21 |
			(uint64(in[6])-minv)<<38 |
			(uint64(in[7])-minv)<<55

	out[2] =
		(uint64(in[7])-minv)>>9 |

			(uint64(in[8])-minv)<<8 |
			(uint64(in[9])-minv)<<25 |
			(uint64(in[10])-minv)<<42 |
			(uint64(in[11])-minv)<<59

	out[3] =
		(uint64(in[11])-minv)>>5 |

			(uint64(in[12])-minv)<<12 |
			(uint64(in[13])-minv)<<29 |
			(uint64(in[14])-minv)<<46 |
			(uint64(in[15])-minv)<<63

	out[4] =
		(uint64(in[15])-minv)>>1 |

			(uint64(in[16])-minv)<<16 |
			(uint64(in[17])-minv)<<33 |
			(uint64(in[18])-minv)<<50

	out[5] =
		(uint64(in[18])-minv)>>14 |

			(uint64(in[19])-minv)<<3 |
			(uint64(in[20])-minv)<<20 |
			(uint64(in[21])-minv)<<37 |
			(uint64(in[22])-minv)<<54

	out[6] =
		(uint64(in[22])-minv)>>10 |

			(uint64(in[23])-minv)<<7 |
			(uint64(in[24])-minv)<<24 |
			(uint64(in[25])-minv)<<41 |
			(uint64(in[26])-minv)<<58

	out[7] =
		(uint64(in[26])-minv)>>6 |

			(uint64(in[27])-minv)<<11 |
			(uint64(in[28])-minv)<<28 |
			(uint64(in[29])-minv)<<45 |
			(uint64(in[30])-minv)<<62

	out[8] =
		(uint64(in[30])-minv)>>2 |

			(uint64(in[31])-minv)<<15 |
			(uint64(in[32])-minv)<<32 |
			(uint64(in[33])-minv)<<49

	out[9] =
		(uint64(in[33])-minv)>>15 |

			(uint64(in[34])-minv)<<2 |
			(uint64(in[35])-minv)<<19 |
			(uint64(in[36])-minv)<<36 |
			(uint64(in[37])-minv)<<53

	out[10] =
		(uint64(in[37])-minv)>>11 |

			(uint64(in[38])-minv)<<6 |
			(uint64(in[39])-minv)<<23 |
			(uint64(in[40])-minv)<<40 |
			(uint64(in[41])-minv)<<57

	out[11] =
		(uint64(in[41])-minv)>>7 |

			(uint64(in[42])-minv)<<10 |
			(uint64(in[43])-minv)<<27 |
			(uint64(in[44])-minv)<<44 |
			(uint64(in[45])-minv)<<61

	out[12] =
		(uint64(in[45])-minv)>>3 |

			(uint64(in[46])-minv)<<14 |
			(uint64(in[47])-minv)<<31 |
			(uint64(in[48])-minv)<<48

	out[13] =
		(uint64(in[48])-minv)>>16 |

			(uint64(in[49])-minv)<<1 |
			(uint64(in[50])-minv)<<18 |
			(uint64(in[51])-minv)<<35 |
			(uint64(in[52])-minv)<<52

	out[14] =
		(uint64(in[52])-minv)>>12 |

			(uint64(in[53])-minv)<<5 |
			(uint64(in[54])-minv)<<22 |
			(uint64(in[55])-minv)<<39 |
			(uint64(in[56])-minv)<<56

	out[15] =
		(uint64(in[56])-minv)>>8 |

			(uint64(in[57])-minv)<<9 |
			(uint64(in[58])-minv)<<26 |
			(uint64(in[59])-minv)<<43 |
			(uint64(in[60])-minv)<<60

	out[16] =
		(uint64(in[60])-minv)>>4 |

			(uint64(in[61])-minv)<<13 |
			(uint64(in[62])-minv)<<30 |
			(uint64(in[63])-minv)<<47

}
func bp32_18[T uint32 | int32](in *[64]T, out *[18]uint64, minv uint64) {
	out[0] =
		(uint64(in[0])-minv)<<0 |
			(uint64(in[1])-minv)<<18 |
			(uint64(in[2])-minv)<<36 |
			(uint64(in[3])-minv)<<54

	out[1] =
		(uint64(in[3])-minv)>>10 |

			(uint64(in[4])-minv)<<8 |
			(uint64(in[5])-minv)<<26 |
			(uint64(in[6])-minv)<<44 |
			(uint64(in[7])-minv)<<62

	out[2] =
		(uint64(in[7])-minv)>>2 |

			(uint64(in[8])-minv)<<16 |
			(uint64(in[9])-minv)<<34 |
			(uint64(in[10])-minv)<<52

	out[3] =
		(uint64(in[10])-minv)>>12 |

			(uint64(in[11])-minv)<<6 |
			(uint64(in[12])-minv)<<24 |
			(uint64(in[13])-minv)<<42 |
			(uint64(in[14])-minv)<<60

	out[4] =
		(uint64(in[14])-minv)>>4 |

			(uint64(in[15])-minv)<<14 |
			(uint64(in[16])-minv)<<32 |
			(uint64(in[17])-minv)<<50

	out[5] =
		(uint64(in[17])-minv)>>14 |

			(uint64(in[18])-minv)<<4 |
			(uint64(in[19])-minv)<<22 |
			(uint64(in[20])-minv)<<40 |
			(uint64(in[21])-minv)<<58

	out[6] =
		(uint64(in[21])-minv)>>6 |

			(uint64(in[22])-minv)<<12 |
			(uint64(in[23])-minv)<<30 |
			(uint64(in[24])-minv)<<48

	out[7] =
		(uint64(in[24])-minv)>>16 |

			(uint64(in[25])-minv)<<2 |
			(uint64(in[26])-minv)<<20 |
			(uint64(in[27])-minv)<<38 |
			(uint64(in[28])-minv)<<56

	out[8] =
		(uint64(in[28])-minv)>>8 |

			(uint64(in[29])-minv)<<10 |
			(uint64(in[30])-minv)<<28 |
			(uint64(in[31])-minv)<<46

	out[9] =
		(uint64(in[31])-minv)>>18 |

			(uint64(in[32])-minv)<<0 |
			(uint64(in[33])-minv)<<18 |
			(uint64(in[34])-minv)<<36 |
			(uint64(in[35])-minv)<<54

	out[10] =
		(uint64(in[35])-minv)>>10 |

			(uint64(in[36])-minv)<<8 |
			(uint64(in[37])-minv)<<26 |
			(uint64(in[38])-minv)<<44 |
			(uint64(in[39])-minv)<<62

	out[11] =
		(uint64(in[39])-minv)>>2 |

			(uint64(in[40])-minv)<<16 |
			(uint64(in[41])-minv)<<34 |
			(uint64(in[42])-minv)<<52

	out[12] =
		(uint64(in[42])-minv)>>12 |

			(uint64(in[43])-minv)<<6 |
			(uint64(in[44])-minv)<<24 |
			(uint64(in[45])-minv)<<42 |
			(uint64(in[46])-minv)<<60

	out[13] =
		(uint64(in[46])-minv)>>4 |

			(uint64(in[47])-minv)<<14 |
			(uint64(in[48])-minv)<<32 |
			(uint64(in[49])-minv)<<50

	out[14] =
		(uint64(in[49])-minv)>>14 |

			(uint64(in[50])-minv)<<4 |
			(uint64(in[51])-minv)<<22 |
			(uint64(in[52])-minv)<<40 |
			(uint64(in[53])-minv)<<58

	out[15] =
		(uint64(in[53])-minv)>>6 |

			(uint64(in[54])-minv)<<12 |
			(uint64(in[55])-minv)<<30 |
			(uint64(in[56])-minv)<<48

	out[16] =
		(uint64(in[56])-minv)>>16 |

			(uint64(in[57])-minv)<<2 |
			(uint64(in[58])-minv)<<20 |
			(uint64(in[59])-minv)<<38 |
			(uint64(in[60])-minv)<<56

	out[17] =
		(uint64(in[60])-minv)>>8 |

			(uint64(in[61])-minv)<<10 |
			(uint64(in[62])-minv)<<28 |
			(uint64(in[63])-minv)<<46

}
func bp32_19[T uint32 | int32](in *[64]T, out *[19]uint64, minv uint64) {
	out[0] =
		(uint64(in[0])-minv)<<0 |
			(uint64(in[1])-minv)<<19 |
			(uint64(in[2])-minv)<<38 |
			(uint64(in[3])-minv)<<57

	out[1] =
		(uint64(in[3])-minv)>>7 |

			(uint64(in[4])-minv)<<12 |
			(uint64(in[5])-minv)<<31 |
			(uint64(in[6])-minv)<<50

	out[2] =
		(uint64(in[6])-minv)>>14 |

			(uint64(in[7])-minv)<<5 |
			(uint64(in[8])-minv)<<24 |
			(uint64(in[9])-minv)<<43 |
			(uint64(in[10])-minv)<<62

	out[3] =
		(uint64(in[10])-minv)>>2 |

			(uint64(in[11])-minv)<<17 |
			(uint64(in[12])-minv)<<36 |
			(uint64(in[13])-minv)<<55

	out[4] =
		(uint64(in[13])-minv)>>9 |

			(uint64(in[14])-minv)<<10 |
			(uint64(in[15])-minv)<<29 |
			(uint64(in[16])-minv)<<48

	out[5] =
		(uint64(in[16])-minv)>>16 |

			(uint64(in[17])-minv)<<3 |
			(uint64(in[18])-minv)<<22 |
			(uint64(in[19])-minv)<<41 |
			(uint64(in[20])-minv)<<60

	out[6] =
		(uint64(in[20])-minv)>>4 |

			(uint64(in[21])-minv)<<15 |
			(uint64(in[22])-minv)<<34 |
			(uint64(in[23])-minv)<<53

	out[7] =
		(uint64(in[23])-minv)>>11 |

			(uint64(in[24])-minv)<<8 |
			(uint64(in[25])-minv)<<27 |
			(uint64(in[26])-minv)<<46

	out[8] =
		(uint64(in[26])-minv)>>18 |

			(uint64(in[27])-minv)<<1 |
			(uint64(in[28])-minv)<<20 |
			(uint64(in[29])-minv)<<39 |
			(uint64(in[30])-minv)<<58

	out[9] =
		(uint64(in[30])-minv)>>6 |

			(uint64(in[31])-minv)<<13 |
			(uint64(in[32])-minv)<<32 |
			(uint64(in[33])-minv)<<51

	out[10] =
		(uint64(in[33])-minv)>>13 |

			(uint64(in[34])-minv)<<6 |
			(uint64(in[35])-minv)<<25 |
			(uint64(in[36])-minv)<<44 |
			(uint64(in[37])-minv)<<63

	out[11] =
		(uint64(in[37])-minv)>>1 |

			(uint64(in[38])-minv)<<18 |
			(uint64(in[39])-minv)<<37 |
			(uint64(in[40])-minv)<<56

	out[12] =
		(uint64(in[40])-minv)>>8 |

			(uint64(in[41])-minv)<<11 |
			(uint64(in[42])-minv)<<30 |
			(uint64(in[43])-minv)<<49

	out[13] =
		(uint64(in[43])-minv)>>15 |

			(uint64(in[44])-minv)<<4 |
			(uint64(in[45])-minv)<<23 |
			(uint64(in[46])-minv)<<42 |
			(uint64(in[47])-minv)<<61

	out[14] =
		(uint64(in[47])-minv)>>3 |

			(uint64(in[48])-minv)<<16 |
			(uint64(in[49])-minv)<<35 |
			(uint64(in[50])-minv)<<54

	out[15] =
		(uint64(in[50])-minv)>>10 |

			(uint64(in[51])-minv)<<9 |
			(uint64(in[52])-minv)<<28 |
			(uint64(in[53])-minv)<<47

	out[16] =
		(uint64(in[53])-minv)>>17 |

			(uint64(in[54])-minv)<<2 |
			(uint64(in[55])-minv)<<21 |
			(uint64(in[56])-minv)<<40 |
			(uint64(in[57])-minv)<<59

	out[17] =
		(uint64(in[57])-minv)>>5 |

			(uint64(in[58])-minv)<<14 |
			(uint64(in[59])-minv)<<33 |
			(uint64(in[60])-minv)<<52

	out[18] =
		(uint64(in[60])-minv)>>12 |

			(uint64(in[61])-minv)<<7 |
			(uint64(in[62])-minv)<<26 |
			(uint64(in[63])-minv)<<45

}
func bp32_20[T uint32 | int32](in *[64]T, out *[20]uint64, minv uint64) {
	out[0] =
		(uint64(in[0])-minv)<<0 |
			(uint64(in[1])-minv)<<20 |
			(uint64(in[2])-minv)<<40 |
			(uint64(in[3])-minv)<<60

	out[1] =
		(uint64(in[3])-minv)>>4 |

			(uint64(in[4])-minv)<<16 |
			(uint64(in[5])-minv)<<36 |
			(uint64(in[6])-minv)<<56

	out[2] =
		(uint64(in[6])-minv)>>8 |

			(uint64(in[7])-minv)<<12 |
			(uint64(in[8])-minv)<<32 |
			(uint64(in[9])-minv)<<52

	out[3] =
		(uint64(in[9])-minv)>>12 |

			(uint64(in[10])-minv)<<8 |
			(uint64(in[11])-minv)<<28 |
			(uint64(in[12])-minv)<<48

	out[4] =
		(uint64(in[12])-minv)>>16 |

			(uint64(in[13])-minv)<<4 |
			(uint64(in[14])-minv)<<24 |
			(uint64(in[15])-minv)<<44

	out[5] =
		(uint64(in[15])-minv)>>20 |

			(uint64(in[16])-minv)<<0 |
			(uint64(in[17])-minv)<<20 |
			(uint64(in[18])-minv)<<40 |
			(uint64(in[19])-minv)<<60

	out[6] =
		(uint64(in[19])-minv)>>4 |

			(uint64(in[20])-minv)<<16 |
			(uint64(in[21])-minv)<<36 |
			(uint64(in[22])-minv)<<56

	out[7] =
		(uint64(in[22])-minv)>>8 |

			(uint64(in[23])-minv)<<12 |
			(uint64(in[24])-minv)<<32 |
			(uint64(in[25])-minv)<<52

	out[8] =
		(uint64(in[25])-minv)>>12 |

			(uint64(in[26])-minv)<<8 |
			(uint64(in[27])-minv)<<28 |
			(uint64(in[28])-minv)<<48

	out[9] =
		(uint64(in[28])-minv)>>16 |

			(uint64(in[29])-minv)<<4 |
			(uint64(in[30])-minv)<<24 |
			(uint64(in[31])-minv)<<44

	out[10] =
		(uint64(in[31])-minv)>>20 |

			(uint64(in[32])-minv)<<0 |
			(uint64(in[33])-minv)<<20 |
			(uint64(in[34])-minv)<<40 |
			(uint64(in[35])-minv)<<60

	out[11] =
		(uint64(in[35])-minv)>>4 |

			(uint64(in[36])-minv)<<16 |
			(uint64(in[37])-minv)<<36 |
			(uint64(in[38])-minv)<<56

	out[12] =
		(uint64(in[38])-minv)>>8 |

			(uint64(in[39])-minv)<<12 |
			(uint64(in[40])-minv)<<32 |
			(uint64(in[41])-minv)<<52

	out[13] =
		(uint64(in[41])-minv)>>12 |

			(uint64(in[42])-minv)<<8 |
			(uint64(in[43])-minv)<<28 |
			(uint64(in[44])-minv)<<48

	out[14] =
		(uint64(in[44])-minv)>>16 |

			(uint64(in[45])-minv)<<4 |
			(uint64(in[46])-minv)<<24 |
			(uint64(in[47])-minv)<<44

	out[15] =
		(uint64(in[47])-minv)>>20 |

			(uint64(in[48])-minv)<<0 |
			(uint64(in[49])-minv)<<20 |
			(uint64(in[50])-minv)<<40 |
			(uint64(in[51])-minv)<<60

	out[16] =
		(uint64(in[51])-minv)>>4 |

			(uint64(in[52])-minv)<<16 |
			(uint64(in[53])-minv)<<36 |
			(uint64(in[54])-minv)<<56

	out[17] =
		(uint64(in[54])-minv)>>8 |

			(uint64(in[55])-minv)<<12 |
			(uint64(in[56])-minv)<<32 |
			(uint64(in[57])-minv)<<52

	out[18] =
		(uint64(in[57])-minv)>>12 |

			(uint64(in[58])-minv)<<8 |
			(uint64(in[59])-minv)<<28 |
			(uint64(in[60])-minv)<<48

	out[19] =
		(uint64(in[60])-minv)>>16 |

			(uint64(in[61])-minv)<<4 |
			(uint64(in[62])-minv)<<24 |
			(uint64(in[63])-minv)<<44

}
func bp32_21[T uint32 | int32](in *[64]T, out *[21]uint64, minv uint64) {
	out[0] =
		(uint64(in[0])-minv)<<0 |
			(uint64(in[1])-minv)<<21 |
			(uint64(in[2])-minv)<<42 |
			(uint64(in[3])-minv)<<63

	out[1] =
		(uint64(in[3])-minv)>>1 |

			(uint64(in[4])-minv)<<20 |
			(uint64(in[5])-minv)<<41 |
			(uint64(in[6])-minv)<<62

	out[2] =
		(uint64(in[6])-minv)>>2 |

			(uint64(in[7])-minv)<<19 |
			(uint64(in[8])-minv)<<40 |
			(uint64(in[9])-minv)<<61

	out[3] =
		(uint64(in[9])-minv)>>3 |

			(uint64(in[10])-minv)<<18 |
			(uint64(in[11])-minv)<<39 |
			(uint64(in[12])-minv)<<60

	out[4] =
		(uint64(in[12])-minv)>>4 |

			(uint64(in[13])-minv)<<17 |
			(uint64(in[14])-minv)<<38 |
			(uint64(in[15])-minv)<<59

	out[5] =
		(uint64(in[15])-minv)>>5 |

			(uint64(in[16])-minv)<<16 |
			(uint64(in[17])-minv)<<37 |
			(uint64(in[18])-minv)<<58

	out[6] =
		(uint64(in[18])-minv)>>6 |

			(uint64(in[19])-minv)<<15 |
			(uint64(in[20])-minv)<<36 |
			(uint64(in[21])-minv)<<57

	out[7] =
		(uint64(in[21])-minv)>>7 |

			(uint64(in[22])-minv)<<14 |
			(uint64(in[23])-minv)<<35 |
			(uint64(in[24])-minv)<<56

	out[8] =
		(uint64(in[24])-minv)>>8 |

			(uint64(in[25])-minv)<<13 |
			(uint64(in[26])-minv)<<34 |
			(uint64(in[27])-minv)<<55

	out[9] =
		(uint64(in[27])-minv)>>9 |

			(uint64(in[28])-minv)<<12 |
			(uint64(in[29])-minv)<<33 |
			(uint64(in[30])-minv)<<54

	out[10] =
		(uint64(in[30])-minv)>>10 |

			(uint64(in[31])-minv)<<11 |
			(uint64(in[32])-minv)<<32 |
			(uint64(in[33])-minv)<<53

	out[11] =
		(uint64(in[33])-minv)>>11 |

			(uint64(in[34])-minv)<<10 |
			(uint64(in[35])-minv)<<31 |
			(uint64(in[36])-minv)<<52

	out[12] =
		(uint64(in[36])-minv)>>12 |

			(uint64(in[37])-minv)<<9 |
			(uint64(in[38])-minv)<<30 |
			(uint64(in[39])-minv)<<51

	out[13] =
		(uint64(in[39])-minv)>>13 |

			(uint64(in[40])-minv)<<8 |
			(uint64(in[41])-minv)<<29 |
			(uint64(in[42])-minv)<<50

	out[14] =
		(uint64(in[42])-minv)>>14 |

			(uint64(in[43])-minv)<<7 |
			(uint64(in[44])-minv)<<28 |
			(uint64(in[45])-minv)<<49

	out[15] =
		(uint64(in[45])-minv)>>15 |

			(uint64(in[46])-minv)<<6 |
			(uint64(in[47])-minv)<<27 |
			(uint64(in[48])-minv)<<48

	out[16] =
		(uint64(in[48])-minv)>>16 |

			(uint64(in[49])-minv)<<5 |
			(uint64(in[50])-minv)<<26 |
			(uint64(in[51])-minv)<<47

	out[17] =
		(uint64(in[51])-minv)>>17 |

			(uint64(in[52])-minv)<<4 |
			(uint64(in[53])-minv)<<25 |
			(uint64(in[54])-minv)<<46

	out[18] =
		(uint64(in[54])-minv)>>18 |

			(uint64(in[55])-minv)<<3 |
			(uint64(in[56])-minv)<<24 |
			(uint64(in[57])-minv)<<45

	out[19] =
		(uint64(in[57])-minv)>>19 |

			(uint64(in[58])-minv)<<2 |
			(uint64(in[59])-minv)<<23 |
			(uint64(in[60])-minv)<<44

	out[20] =
		(uint64(in[60])-minv)>>20 |

			(uint64(in[61])-minv)<<1 |
			(uint64(in[62])-minv)<<22 |
			(uint64(in[63])-minv)<<43

}
func bp32_22[T uint32 | int32](in *[64]T, out *[22]uint64, minv uint64) {
	out[0] =
		(uint64(in[0])-minv)<<0 |
			(uint64(in[1])-minv)<<22 |
			(uint64(in[2])-minv)<<44

	out[1] =
		(uint64(in[2])-minv)>>20 |

			(uint64(in[3])-minv)<<2 |
			(uint64(in[4])-minv)<<24 |
			(uint64(in[5])-minv)<<46

	out[2] =
		(uint64(in[5])-minv)>>18 |

			(uint64(in[6])-minv)<<4 |
			(uint64(in[7])-minv)<<26 |
			(uint64(in[8])-minv)<<48

	out[3] =
		(uint64(in[8])-minv)>>16 |

			(uint64(in[9])-minv)<<6 |
			(uint64(in[10])-minv)<<28 |
			(uint64(in[11])-minv)<<50

	out[4] =
		(uint64(in[11])-minv)>>14 |

			(uint64(in[12])-minv)<<8 |
			(uint64(in[13])-minv)<<30 |
			(uint64(in[14])-minv)<<52

	out[5] =
		(uint64(in[14])-minv)>>12 |

			(uint64(in[15])-minv)<<10 |
			(uint64(in[16])-minv)<<32 |
			(uint64(in[17])-minv)<<54

	out[6] =
		(uint64(in[17])-minv)>>10 |

			(uint64(in[18])-minv)<<12 |
			(uint64(in[19])-minv)<<34 |
			(uint64(in[20])-minv)<<56

	out[7] =
		(uint64(in[20])-minv)>>8 |

			(uint64(in[21])-minv)<<14 |
			(uint64(in[22])-minv)<<36 |
			(uint64(in[23])-minv)<<58

	out[8] =
		(uint64(in[23])-minv)>>6 |

			(uint64(in[24])-minv)<<16 |
			(uint64(in[25])-minv)<<38 |
			(uint64(in[26])-minv)<<60

	out[9] =
		(uint64(in[26])-minv)>>4 |

			(uint64(in[27])-minv)<<18 |
			(uint64(in[28])-minv)<<40 |
			(uint64(in[29])-minv)<<62

	out[10] =
		(uint64(in[29])-minv)>>2 |

			(uint64(in[30])-minv)<<20 |
			(uint64(in[31])-minv)<<42

	out[11] =
		(uint64(in[31])-minv)>>22 |

			(uint64(in[32])-minv)<<0 |
			(uint64(in[33])-minv)<<22 |
			(uint64(in[34])-minv)<<44

	out[12] =
		(uint64(in[34])-minv)>>20 |

			(uint64(in[35])-minv)<<2 |
			(uint64(in[36])-minv)<<24 |
			(uint64(in[37])-minv)<<46

	out[13] =
		(uint64(in[37])-minv)>>18 |

			(uint64(in[38])-minv)<<4 |
			(uint64(in[39])-minv)<<26 |
			(uint64(in[40])-minv)<<48

	out[14] =
		(uint64(in[40])-minv)>>16 |

			(uint64(in[41])-minv)<<6 |
			(uint64(in[42])-minv)<<28 |
			(uint64(in[43])-minv)<<50

	out[15] =
		(uint64(in[43])-minv)>>14 |

			(uint64(in[44])-minv)<<8 |
			(uint64(in[45])-minv)<<30 |
			(uint64(in[46])-minv)<<52

	out[16] =
		(uint64(in[46])-minv)>>12 |

			(uint64(in[47])-minv)<<10 |
			(uint64(in[48])-minv)<<32 |
			(uint64(in[49])-minv)<<54

	out[17] =
		(uint64(in[49])-minv)>>10 |

			(uint64(in[50])-minv)<<12 |
			(uint64(in[51])-minv)<<34 |
			(uint64(in[52])-minv)<<56

	out[18] =
		(uint64(in[52])-minv)>>8 |

			(uint64(in[53])-minv)<<14 |
			(uint64(in[54])-minv)<<36 |
			(uint64(in[55])-minv)<<58

	out[19] =
		(uint64(in[55])-minv)>>6 |

			(uint64(in[56])-minv)<<16 |
			(uint64(in[57])-minv)<<38 |
			(uint64(in[58])-minv)<<60

	out[20] =
		(uint64(in[58])-minv)>>4 |

			(uint64(in[59])-minv)<<18 |
			(uint64(in[60])-minv)<<40 |
			(uint64(in[61])-minv)<<62

	out[21] =
		(uint64(in[61])-minv)>>2 |

			(uint64(in[62])-minv)<<20 |
			(uint64(in[63])-minv)<<42

}
func bp32_23[T uint32 | int32](in *[64]T, out *[23]uint64, minv uint64) {
	out[0] =
		(uint64(in[0])-minv)<<0 |
			(uint64(in[1])-minv)<<23 |
			(uint64(in[2])-minv)<<46

	out[1] =
		(uint64(in[2])-minv)>>18 |

			(uint64(in[3])-minv)<<5 |
			(uint64(in[4])-minv)<<28 |
			(uint64(in[5])-minv)<<51

	out[2] =
		(uint64(in[5])-minv)>>13 |

			(uint64(in[6])-minv)<<10 |
			(uint64(in[7])-minv)<<33 |
			(uint64(in[8])-minv)<<56

	out[3] =
		(uint64(in[8])-minv)>>8 |

			(uint64(in[9])-minv)<<15 |
			(uint64(in[10])-minv)<<38 |
			(uint64(in[11])-minv)<<61

	out[4] =
		(uint64(in[11])-minv)>>3 |

			(uint64(in[12])-minv)<<20 |
			(uint64(in[13])-minv)<<43

	out[5] =
		(uint64(in[13])-minv)>>21 |

			(uint64(in[14])-minv)<<2 |
			(uint64(in[15])-minv)<<25 |
			(uint64(in[16])-minv)<<48

	out[6] =
		(uint64(in[16])-minv)>>16 |

			(uint64(in[17])-minv)<<7 |
			(uint64(in[18])-minv)<<30 |
			(uint64(in[19])-minv)<<53

	out[7] =
		(uint64(in[19])-minv)>>11 |

			(uint64(in[20])-minv)<<12 |
			(uint64(in[21])-minv)<<35 |
			(uint64(in[22])-minv)<<58

	out[8] =
		(uint64(in[22])-minv)>>6 |

			(uint64(in[23])-minv)<<17 |
			(uint64(in[24])-minv)<<40 |
			(uint64(in[25])-minv)<<63

	out[9] =
		(uint64(in[25])-minv)>>1 |

			(uint64(in[26])-minv)<<22 |
			(uint64(in[27])-minv)<<45

	out[10] =
		(uint64(in[27])-minv)>>19 |

			(uint64(in[28])-minv)<<4 |
			(uint64(in[29])-minv)<<27 |
			(uint64(in[30])-minv)<<50

	out[11] =
		(uint64(in[30])-minv)>>14 |

			(uint64(in[31])-minv)<<9 |
			(uint64(in[32])-minv)<<32 |
			(uint64(in[33])-minv)<<55

	out[12] =
		(uint64(in[33])-minv)>>9 |

			(uint64(in[34])-minv)<<14 |
			(uint64(in[35])-minv)<<37 |
			(uint64(in[36])-minv)<<60

	out[13] =
		(uint64(in[36])-minv)>>4 |

			(uint64(in[37])-minv)<<19 |
			(uint64(in[38])-minv)<<42

	out[14] =
		(uint64(in[38])-minv)>>22 |

			(uint64(in[39])-minv)<<1 |
			(uint64(in[40])-minv)<<24 |
			(uint64(in[41])-minv)<<47

	out[15] =
		(uint64(in[41])-minv)>>17 |

			(uint64(in[42])-minv)<<6 |
			(uint64(in[43])-minv)<<29 |
			(uint64(in[44])-minv)<<52

	out[16] =
		(uint64(in[44])-minv)>>12 |

			(uint64(in[45])-minv)<<11 |
			(uint64(in[46])-minv)<<34 |
			(uint64(in[47])-minv)<<57

	out[17] =
		(uint64(in[47])-minv)>>7 |

			(uint64(in[48])-minv)<<16 |
			(uint64(in[49])-minv)<<39 |
			(uint64(in[50])-minv)<<62

	out[18] =
		(uint64(in[50])-minv)>>2 |

			(uint64(in[51])-minv)<<21 |
			(uint64(in[52])-minv)<<44

	out[19] =
		(uint64(in[52])-minv)>>20 |

			(uint64(in[53])-minv)<<3 |
			(uint64(in[54])-minv)<<26 |
			(uint64(in[55])-minv)<<49

	out[20] =
		(uint64(in[55])-minv)>>15 |

			(uint64(in[56])-minv)<<8 |
			(uint64(in[57])-minv)<<31 |
			(uint64(in[58])-minv)<<54

	out[21] =
		(uint64(in[58])-minv)>>10 |

			(uint64(in[59])-minv)<<13 |
			(uint64(in[60])-minv)<<36 |
			(uint64(in[61])-minv)<<59

	out[22] =
		(uint64(in[61])-minv)>>5 |

			(uint64(in[62])-minv)<<18 |
			(uint64(in[63])-minv)<<41

}
func bp32_24[T uint32 | int32](in *[64]T, out *[24]uint64, minv uint64) {
	out[0] =
		(uint64(in[0])-minv)<<0 |
			(uint64(in[1])-minv)<<24 |
			(uint64(in[2])-minv)<<48

	out[1] =
		(uint64(in[2])-minv)>>16 |

			(uint64(in[3])-minv)<<8 |
			(uint64(in[4])-minv)<<32 |
			(uint64(in[5])-minv)<<56

	out[2] =
		(uint64(in[5])-minv)>>8 |

			(uint64(in[6])-minv)<<16 |
			(uint64(in[7])-minv)<<40

	out[3] =
		(uint64(in[7])-minv)>>24 |

			(uint64(in[8])-minv)<<0 |
			(uint64(in[9])-minv)<<24 |
			(uint64(in[10])-minv)<<48

	out[4] =
		(uint64(in[10])-minv)>>16 |

			(uint64(in[11])-minv)<<8 |
			(uint64(in[12])-minv)<<32 |
			(uint64(in[13])-minv)<<56

	out[5] =
		(uint64(in[13])-minv)>>8 |

			(uint64(in[14])-minv)<<16 |
			(uint64(in[15])-minv)<<40

	out[6] =
		(uint64(in[15])-minv)>>24 |

			(uint64(in[16])-minv)<<0 |
			(uint64(in[17])-minv)<<24 |
			(uint64(in[18])-minv)<<48

	out[7] =
		(uint64(in[18])-minv)>>16 |

			(uint64(in[19])-minv)<<8 |
			(uint64(in[20])-minv)<<32 |
			(uint64(in[21])-minv)<<56

	out[8] =
		(uint64(in[21])-minv)>>8 |

			(uint64(in[22])-minv)<<16 |
			(uint64(in[23])-minv)<<40

	out[9] =
		(uint64(in[23])-minv)>>24 |

			(uint64(in[24])-minv)<<0 |
			(uint64(in[25])-minv)<<24 |
			(uint64(in[26])-minv)<<48

	out[10] =
		(uint64(in[26])-minv)>>16 |

			(uint64(in[27])-minv)<<8 |
			(uint64(in[28])-minv)<<32 |
			(uint64(in[29])-minv)<<56

	out[11] =
		(uint64(in[29])-minv)>>8 |

			(uint64(in[30])-minv)<<16 |
			(uint64(in[31])-minv)<<40

	out[12] =
		(uint64(in[31])-minv)>>24 |

			(uint64(in[32])-minv)<<0 |
			(uint64(in[33])-minv)<<24 |
			(uint64(in[34])-minv)<<48

	out[13] =
		(uint64(in[34])-minv)>>16 |

			(uint64(in[35])-minv)<<8 |
			(uint64(in[36])-minv)<<32 |
			(uint64(in[37])-minv)<<56

	out[14] =
		(uint64(in[37])-minv)>>8 |

			(uint64(in[38])-minv)<<16 |
			(uint64(in[39])-minv)<<40

	out[15] =
		(uint64(in[39])-minv)>>24 |

			(uint64(in[40])-minv)<<0 |
			(uint64(in[41])-minv)<<24 |
			(uint64(in[42])-minv)<<48

	out[16] =
		(uint64(in[42])-minv)>>16 |

			(uint64(in[43])-minv)<<8 |
			(uint64(in[44])-minv)<<32 |
			(uint64(in[45])-minv)<<56

	out[17] =
		(uint64(in[45])-minv)>>8 |

			(uint64(in[46])-minv)<<16 |
			(uint64(in[47])-minv)<<40

	out[18] =
		(uint64(in[47])-minv)>>24 |

			(uint64(in[48])-minv)<<0 |
			(uint64(in[49])-minv)<<24 |
			(uint64(in[50])-minv)<<48

	out[19] =
		(uint64(in[50])-minv)>>16 |

			(uint64(in[51])-minv)<<8 |
			(uint64(in[52])-minv)<<32 |
			(uint64(in[53])-minv)<<56

	out[20] =
		(uint64(in[53])-minv)>>8 |

			(uint64(in[54])-minv)<<16 |
			(uint64(in[55])-minv)<<40

	out[21] =
		(uint64(in[55])-minv)>>24 |

			(uint64(in[56])-minv)<<0 |
			(uint64(in[57])-minv)<<24 |
			(uint64(in[58])-minv)<<48

	out[22] =
		(uint64(in[58])-minv)>>16 |

			(uint64(in[59])-minv)<<8 |
			(uint64(in[60])-minv)<<32 |
			(uint64(in[61])-minv)<<56

	out[23] =
		(uint64(in[61])-minv)>>8 |

			(uint64(in[62])-minv)<<16 |
			(uint64(in[63])-minv)<<40

}
func bp32_25[T uint32 | int32](in *[64]T, out *[25]uint64, minv uint64) {
	out[0] =
		(uint64(in[0])-minv)<<0 |
			(uint64(in[1])-minv)<<25 |
			(uint64(in[2])-minv)<<50

	out[1] =
		(uint64(in[2])-minv)>>14 |

			(uint64(in[3])-minv)<<11 |
			(uint64(in[4])-minv)<<36 |
			(uint64(in[5])-minv)<<61

	out[2] =
		(uint64(in[5])-minv)>>3 |

			(uint64(in[6])-minv)<<22 |
			(uint64(in[7])-minv)<<47

	out[3] =
		(uint64(in[7])-minv)>>17 |

			(uint64(in[8])-minv)<<8 |
			(uint64(in[9])-minv)<<33 |
			(uint64(in[10])-minv)<<58

	out[4] =
		(uint64(in[10])-minv)>>6 |

			(uint64(in[11])-minv)<<19 |
			(uint64(in[12])-minv)<<44

	out[5] =
		(uint64(in[12])-minv)>>20 |

			(uint64(in[13])-minv)<<5 |
			(uint64(in[14])-minv)<<30 |
			(uint64(in[15])-minv)<<55

	out[6] =
		(uint64(in[15])-minv)>>9 |

			(uint64(in[16])-minv)<<16 |
			(uint64(in[17])-minv)<<41

	out[7] =
		(uint64(in[17])-minv)>>23 |

			(uint64(in[18])-minv)<<2 |
			(uint64(in[19])-minv)<<27 |
			(uint64(in[20])-minv)<<52

	out[8] =
		(uint64(in[20])-minv)>>12 |

			(uint64(in[21])-minv)<<13 |
			(uint64(in[22])-minv)<<38 |
			(uint64(in[23])-minv)<<63

	out[9] =
		(uint64(in[23])-minv)>>1 |

			(uint64(in[24])-minv)<<24 |
			(uint64(in[25])-minv)<<49

	out[10] =
		(uint64(in[25])-minv)>>15 |

			(uint64(in[26])-minv)<<10 |
			(uint64(in[27])-minv)<<35 |
			(uint64(in[28])-minv)<<60

	out[11] =
		(uint64(in[28])-minv)>>4 |

			(uint64(in[29])-minv)<<21 |
			(uint64(in[30])-minv)<<46

	out[12] =
		(uint64(in[30])-minv)>>18 |

			(uint64(in[31])-minv)<<7 |
			(uint64(in[32])-minv)<<32 |
			(uint64(in[33])-minv)<<57

	out[13] =
		(uint64(in[33])-minv)>>7 |

			(uint64(in[34])-minv)<<18 |
			(uint64(in[35])-minv)<<43

	out[14] =
		(uint64(in[35])-minv)>>21 |

			(uint64(in[36])-minv)<<4 |
			(uint64(in[37])-minv)<<29 |
			(uint64(in[38])-minv)<<54

	out[15] =
		(uint64(in[38])-minv)>>10 |

			(uint64(in[39])-minv)<<15 |
			(uint64(in[40])-minv)<<40

	out[16] =
		(uint64(in[40])-minv)>>24 |

			(uint64(in[41])-minv)<<1 |
			(uint64(in[42])-minv)<<26 |
			(uint64(in[43])-minv)<<51

	out[17] =
		(uint64(in[43])-minv)>>13 |

			(uint64(in[44])-minv)<<12 |
			(uint64(in[45])-minv)<<37 |
			(uint64(in[46])-minv)<<62

	out[18] =
		(uint64(in[46])-minv)>>2 |

			(uint64(in[47])-minv)<<23 |
			(uint64(in[48])-minv)<<48

	out[19] =
		(uint64(in[48])-minv)>>16 |

			(uint64(in[49])-minv)<<9 |
			(uint64(in[50])-minv)<<34 |
			(uint64(in[51])-minv)<<59

	out[20] =
		(uint64(in[51])-minv)>>5 |

			(uint64(in[52])-minv)<<20 |
			(uint64(in[53])-minv)<<45

	out[21] =
		(uint64(in[53])-minv)>>19 |

			(uint64(in[54])-minv)<<6 |
			(uint64(in[55])-minv)<<31 |
			(uint64(in[56])-minv)<<56

	out[22] =
		(uint64(in[56])-minv)>>8 |

			(uint64(in[57])-minv)<<17 |
			(uint64(in[58])-minv)<<42

	out[23] =
		(uint64(in[58])-minv)>>22 |

			(uint64(in[59])-minv)<<3 |
			(uint64(in[60])-minv)<<28 |
			(uint64(in[61])-minv)<<53

	out[24] =
		(uint64(in[61])-minv)>>11 |

			(uint64(in[62])-minv)<<14 |
			(uint64(in[63])-minv)<<39

}
func bp32_26[T uint32 | int32](in *[64]T, out *[26]uint64, minv uint64) {
	out[0] =
		(uint64(in[0])-minv)<<0 |
			(uint64(in[1])-minv)<<26 |
			(uint64(in[2])-minv)<<52

	out[1] =
		(uint64(in[2])-minv)>>12 |

			(uint64(in[3])-minv)<<14 |
			(uint64(in[4])-minv)<<40

	out[2] =
		(uint64(in[4])-minv)>>24 |

			(uint64(in[5])-minv)<<2 |
			(uint64(in[6])-minv)<<28 |
			(uint64(in[7])-minv)<<54

	out[3] =
		(uint64(in[7])-minv)>>10 |

			(uint64(in[8])-minv)<<16 |
			(uint64(in[9])-minv)<<42

	out[4] =
		(uint64(in[9])-minv)>>22 |

			(uint64(in[10])-minv)<<4 |
			(uint64(in[11])-minv)<<30 |
			(uint64(in[12])-minv)<<56

	out[5] =
		(uint64(in[12])-minv)>>8 |

			(uint64(in[13])-minv)<<18 |
			(uint64(in[14])-minv)<<44

	out[6] =
		(uint64(in[14])-minv)>>20 |

			(uint64(in[15])-minv)<<6 |
			(uint64(in[16])-minv)<<32 |
			(uint64(in[17])-minv)<<58

	out[7] =
		(uint64(in[17])-minv)>>6 |

			(uint64(in[18])-minv)<<20 |
			(uint64(in[19])-minv)<<46

	out[8] =
		(uint64(in[19])-minv)>>18 |

			(uint64(in[20])-minv)<<8 |
			(uint64(in[21])-minv)<<34 |
			(uint64(in[22])-minv)<<60

	out[9] =
		(uint64(in[22])-minv)>>4 |

			(uint64(in[23])-minv)<<22 |
			(uint64(in[24])-minv)<<48

	out[10] =
		(uint64(in[24])-minv)>>16 |

			(uint64(in[25])-minv)<<10 |
			(uint64(in[26])-minv)<<36 |
			(uint64(in[27])-minv)<<62

	out[11] =
		(uint64(in[27])-minv)>>2 |

			(uint64(in[28])-minv)<<24 |
			(uint64(in[29])-minv)<<50

	out[12] =
		(uint64(in[29])-minv)>>14 |

			(uint64(in[30])-minv)<<12 |
			(uint64(in[31])-minv)<<38

	out[13] =
		(uint64(in[31])-minv)>>26 |

			(uint64(in[32])-minv)<<0 |
			(uint64(in[33])-minv)<<26 |
			(uint64(in[34])-minv)<<52

	out[14] =
		(uint64(in[34])-minv)>>12 |

			(uint64(in[35])-minv)<<14 |
			(uint64(in[36])-minv)<<40

	out[15] =
		(uint64(in[36])-minv)>>24 |

			(uint64(in[37])-minv)<<2 |
			(uint64(in[38])-minv)<<28 |
			(uint64(in[39])-minv)<<54

	out[16] =
		(uint64(in[39])-minv)>>10 |

			(uint64(in[40])-minv)<<16 |
			(uint64(in[41])-minv)<<42

	out[17] =
		(uint64(in[41])-minv)>>22 |

			(uint64(in[42])-minv)<<4 |
			(uint64(in[43])-minv)<<30 |
			(uint64(in[44])-minv)<<56

	out[18] =
		(uint64(in[44])-minv)>>8 |

			(uint64(in[45])-minv)<<18 |
			(uint64(in[46])-minv)<<44

	out[19] =
		(uint64(in[46])-minv)>>20 |

			(uint64(in[47])-minv)<<6 |
			(uint64(in[48])-minv)<<32 |
			(uint64(in[49])-minv)<<58

	out[20] =
		(uint64(in[49])-minv)>>6 |

			(uint64(in[50])-minv)<<20 |
			(uint64(in[51])-minv)<<46

	out[21] =
		(uint64(in[51])-minv)>>18 |

			(uint64(in[52])-minv)<<8 |
			(uint64(in[53])-minv)<<34 |
			(uint64(in[54])-minv)<<60

	out[22] =
		(uint64(in[54])-minv)>>4 |

			(uint64(in[55])-minv)<<22 |
			(uint64(in[56])-minv)<<48

	out[23] =
		(uint64(in[56])-minv)>>16 |

			(uint64(in[57])-minv)<<10 |
			(uint64(in[58])-minv)<<36 |
			(uint64(in[59])-minv)<<62

	out[24] =
		(uint64(in[59])-minv)>>2 |

			(uint64(in[60])-minv)<<24 |
			(uint64(in[61])-minv)<<50

	out[25] =
		(uint64(in[61])-minv)>>14 |

			(uint64(in[62])-minv)<<12 |
			(uint64(in[63])-minv)<<38

}
func bp32_27[T uint32 | int32](in *[64]T, out *[27]uint64, minv uint64) {
	out[0] =
		(uint64(in[0])-minv)<<0 |
			(uint64(in[1])-minv)<<27 |
			(uint64(in[2])-minv)<<54

	out[1] =
		(uint64(in[2])-minv)>>10 |

			(uint64(in[3])-minv)<<17 |
			(uint64(in[4])-minv)<<44

	out[2] =
		(uint64(in[4])-minv)>>20 |

			(uint64(in[5])-minv)<<7 |
			(uint64(in[6])-minv)<<34 |
			(uint64(in[7])-minv)<<61

	out[3] =
		(uint64(in[7])-minv)>>3 |

			(uint64(in[8])-minv)<<24 |
			(uint64(in[9])-minv)<<51

	out[4] =
		(uint64(in[9])-minv)>>13 |

			(uint64(in[10])-minv)<<14 |
			(uint64(in[11])-minv)<<41

	out[5] =
		(uint64(in[11])-minv)>>23 |

			(uint64(in[12])-minv)<<4 |
			(uint64(in[13])-minv)<<31 |
			(uint64(in[14])-minv)<<58

	out[6] =
		(uint64(in[14])-minv)>>6 |

			(uint64(in[15])-minv)<<21 |
			(uint64(in[16])-minv)<<48

	out[7] =
		(uint64(in[16])-minv)>>16 |

			(uint64(in[17])-minv)<<11 |
			(uint64(in[18])-minv)<<38

	out[8] =
		(uint64(in[18])-minv)>>26 |

			(uint64(in[19])-minv)<<1 |
			(uint64(in[20])-minv)<<28 |
			(uint64(in[21])-minv)<<55

	out[9] =
		(uint64(in[21])-minv)>>9 |

			(uint64(in[22])-minv)<<18 |
			(uint64(in[23])-minv)<<45

	out[10] =
		(uint64(in[23])-minv)>>19 |

			(uint64(in[24])-minv)<<8 |
			(uint64(in[25])-minv)<<35 |
			(uint64(in[26])-minv)<<62

	out[11] =
		(uint64(in[26])-minv)>>2 |

			(uint64(in[27])-minv)<<25 |
			(uint64(in[28])-minv)<<52

	out[12] =
		(uint64(in[28])-minv)>>12 |

			(uint64(in[29])-minv)<<15 |
			(uint64(in[30])-minv)<<42

	out[13] =
		(uint64(in[30])-minv)>>22 |

			(uint64(in[31])-minv)<<5 |
			(uint64(in[32])-minv)<<32 |
			(uint64(in[33])-minv)<<59

	out[14] =
		(uint64(in[33])-minv)>>5 |

			(uint64(in[34])-minv)<<22 |
			(uint64(in[35])-minv)<<49

	out[15] =
		(uint64(in[35])-minv)>>15 |

			(uint64(in[36])-minv)<<12 |
			(uint64(in[37])-minv)<<39

	out[16] =
		(uint64(in[37])-minv)>>25 |

			(uint64(in[38])-minv)<<2 |
			(uint64(in[39])-minv)<<29 |
			(uint64(in[40])-minv)<<56

	out[17] =
		(uint64(in[40])-minv)>>8 |

			(uint64(in[41])-minv)<<19 |
			(uint64(in[42])-minv)<<46

	out[18] =
		(uint64(in[42])-minv)>>18 |

			(uint64(in[43])-minv)<<9 |
			(uint64(in[44])-minv)<<36 |
			(uint64(in[45])-minv)<<63

	out[19] =
		(uint64(in[45])-minv)>>1 |

			(uint64(in[46])-minv)<<26 |
			(uint64(in[47])-minv)<<53

	out[20] =
		(uint64(in[47])-minv)>>11 |

			(uint64(in[48])-minv)<<16 |
			(uint64(in[49])-minv)<<43

	out[21] =
		(uint64(in[49])-minv)>>21 |

			(uint64(in[50])-minv)<<6 |
			(uint64(in[51])-minv)<<33 |
			(uint64(in[52])-minv)<<60

	out[22] =
		(uint64(in[52])-minv)>>4 |

			(uint64(in[53])-minv)<<23 |
			(uint64(in[54])-minv)<<50

	out[23] =
		(uint64(in[54])-minv)>>14 |

			(uint64(in[55])-minv)<<13 |
			(uint64(in[56])-minv)<<40

	out[24] =
		(uint64(in[56])-minv)>>24 |

			(uint64(in[57])-minv)<<3 |
			(uint64(in[58])-minv)<<30 |
			(uint64(in[59])-minv)<<57

	out[25] =
		(uint64(in[59])-minv)>>7 |

			(uint64(in[60])-minv)<<20 |
			(uint64(in[61])-minv)<<47

	out[26] =
		(uint64(in[61])-minv)>>17 |

			(uint64(in[62])-minv)<<10 |
			(uint64(in[63])-minv)<<37

}
func bp32_28[T uint32 | int32](in *[64]T, out *[28]uint64, minv uint64) {
	out[0] =
		(uint64(in[0])-minv)<<0 |
			(uint64(in[1])-minv)<<28 |
			(uint64(in[2])-minv)<<56

	out[1] =
		(uint64(in[2])-minv)>>8 |

			(uint64(in[3])-minv)<<20 |
			(uint64(in[4])-minv)<<48

	out[2] =
		(uint64(in[4])-minv)>>16 |

			(uint64(in[5])-minv)<<12 |
			(uint64(in[6])-minv)<<40

	out[3] =
		(uint64(in[6])-minv)>>24 |

			(uint64(in[7])-minv)<<4 |
			(uint64(in[8])-minv)<<32 |
			(uint64(in[9])-minv)<<60

	out[4] =
		(uint64(in[9])-minv)>>4 |

			(uint64(in[10])-minv)<<24 |
			(uint64(in[11])-minv)<<52

	out[5] =
		(uint64(in[11])-minv)>>12 |

			(uint64(in[12])-minv)<<16 |
			(uint64(in[13])-minv)<<44

	out[6] =
		(uint64(in[13])-minv)>>20 |

			(uint64(in[14])-minv)<<8 |
			(uint64(in[15])-minv)<<36

	out[7] =
		(uint64(in[15])-minv)>>28 |

			(uint64(in[16])-minv)<<0 |
			(uint64(in[17])-minv)<<28 |
			(uint64(in[18])-minv)<<56

	out[8] =
		(uint64(in[18])-minv)>>8 |

			(uint64(in[19])-minv)<<20 |
			(uint64(in[20])-minv)<<48

	out[9] =
		(uint64(in[20])-minv)>>16 |

			(uint64(in[21])-minv)<<12 |
			(uint64(in[22])-minv)<<40

	out[10] =
		(uint64(in[22])-minv)>>24 |

			(uint64(in[23])-minv)<<4 |
			(uint64(in[24])-minv)<<32 |
			(uint64(in[25])-minv)<<60

	out[11] =
		(uint64(in[25])-minv)>>4 |

			(uint64(in[26])-minv)<<24 |
			(uint64(in[27])-minv)<<52

	out[12] =
		(uint64(in[27])-minv)>>12 |

			(uint64(in[28])-minv)<<16 |
			(uint64(in[29])-minv)<<44

	out[13] =
		(uint64(in[29])-minv)>>20 |

			(uint64(in[30])-minv)<<8 |
			(uint64(in[31])-minv)<<36

	out[14] =
		(uint64(in[31])-minv)>>28 |

			(uint64(in[32])-minv)<<0 |
			(uint64(in[33])-minv)<<28 |
			(uint64(in[34])-minv)<<56

	out[15] =
		(uint64(in[34])-minv)>>8 |

			(uint64(in[35])-minv)<<20 |
			(uint64(in[36])-minv)<<48

	out[16] =
		(uint64(in[36])-minv)>>16 |

			(uint64(in[37])-minv)<<12 |
			(uint64(in[38])-minv)<<40

	out[17] =
		(uint64(in[38])-minv)>>24 |

			(uint64(in[39])-minv)<<4 |
			(uint64(in[40])-minv)<<32 |
			(uint64(in[41])-minv)<<60

	out[18] =
		(uint64(in[41])-minv)>>4 |

			(uint64(in[42])-minv)<<24 |
			(uint64(in[43])-minv)<<52

	out[19] =
		(uint64(in[43])-minv)>>12 |

			(uint64(in[44])-minv)<<16 |
			(uint64(in[45])-minv)<<44

	out[20] =
		(uint64(in[45])-minv)>>20 |

			(uint64(in[46])-minv)<<8 |
			(uint64(in[47])-minv)<<36

	out[21] =
		(uint64(in[47])-minv)>>28 |

			(uint64(in[48])-minv)<<0 |
			(uint64(in[49])-minv)<<28 |
			(uint64(in[50])-minv)<<56

	out[22] =
		(uint64(in[50])-minv)>>8 |

			(uint64(in[51])-minv)<<20 |
			(uint64(in[52])-minv)<<48

	out[23] =
		(uint64(in[52])-minv)>>16 |

			(uint64(in[53])-minv)<<12 |
			(uint64(in[54])-minv)<<40

	out[24] =
		(uint64(in[54])-minv)>>24 |

			(uint64(in[55])-minv)<<4 |
			(uint64(in[56])-minv)<<32 |
			(uint64(in[57])-minv)<<60

	out[25] =
		(uint64(in[57])-minv)>>4 |

			(uint64(in[58])-minv)<<24 |
			(uint64(in[59])-minv)<<52

	out[26] =
		(uint64(in[59])-minv)>>12 |

			(uint64(in[60])-minv)<<16 |
			(uint64(in[61])-minv)<<44

	out[27] =
		(uint64(in[61])-minv)>>20 |

			(uint64(in[62])-minv)<<8 |
			(uint64(in[63])-minv)<<36

}
func bp32_29[T uint32 | int32](in *[64]T, out *[29]uint64, minv uint64) {
	out[0] =
		(uint64(in[0])-minv)<<0 |
			(uint64(in[1])-minv)<<29 |
			(uint64(in[2])-minv)<<58

	out[1] =
		(uint64(in[2])-minv)>>6 |

			(uint64(in[3])-minv)<<23 |
			(uint64(in[4])-minv)<<52

	out[2] =
		(uint64(in[4])-minv)>>12 |

			(uint64(in[5])-minv)<<17 |
			(uint64(in[6])-minv)<<46

	out[3] =
		(uint64(in[6])-minv)>>18 |

			(uint64(in[7])-minv)<<11 |
			(uint64(in[8])-minv)<<40

	out[4] =
		(uint64(in[8])-minv)>>24 |

			(uint64(in[9])-minv)<<5 |
			(uint64(in[10])-minv)<<34 |
			(uint64(in[11])-minv)<<63

	out[5] =
		(uint64(in[11])-minv)>>1 |

			(uint64(in[12])-minv)<<28 |
			(uint64(in[13])-minv)<<57

	out[6] =
		(uint64(in[13])-minv)>>7 |

			(uint64(in[14])-minv)<<22 |
			(uint64(in[15])-minv)<<51

	out[7] =
		(uint64(in[15])-minv)>>13 |

			(uint64(in[16])-minv)<<16 |
			(uint64(in[17])-minv)<<45

	out[8] =
		(uint64(in[17])-minv)>>19 |

			(uint64(in[18])-minv)<<10 |
			(uint64(in[19])-minv)<<39

	out[9] =
		(uint64(in[19])-minv)>>25 |

			(uint64(in[20])-minv)<<4 |
			(uint64(in[21])-minv)<<33 |
			(uint64(in[22])-minv)<<62

	out[10] =
		(uint64(in[22])-minv)>>2 |

			(uint64(in[23])-minv)<<27 |
			(uint64(in[24])-minv)<<56

	out[11] =
		(uint64(in[24])-minv)>>8 |

			(uint64(in[25])-minv)<<21 |
			(uint64(in[26])-minv)<<50

	out[12] =
		(uint64(in[26])-minv)>>14 |

			(uint64(in[27])-minv)<<15 |
			(uint64(in[28])-minv)<<44

	out[13] =
		(uint64(in[28])-minv)>>20 |

			(uint64(in[29])-minv)<<9 |
			(uint64(in[30])-minv)<<38

	out[14] =
		(uint64(in[30])-minv)>>26 |

			(uint64(in[31])-minv)<<3 |
			(uint64(in[32])-minv)<<32 |
			(uint64(in[33])-minv)<<61

	out[15] =
		(uint64(in[33])-minv)>>3 |

			(uint64(in[34])-minv)<<26 |
			(uint64(in[35])-minv)<<55

	out[16] =
		(uint64(in[35])-minv)>>9 |

			(uint64(in[36])-minv)<<20 |
			(uint64(in[37])-minv)<<49

	out[17] =
		(uint64(in[37])-minv)>>15 |

			(uint64(in[38])-minv)<<14 |
			(uint64(in[39])-minv)<<43

	out[18] =
		(uint64(in[39])-minv)>>21 |

			(uint64(in[40])-minv)<<8 |
			(uint64(in[41])-minv)<<37

	out[19] =
		(uint64(in[41])-minv)>>27 |

			(uint64(in[42])-minv)<<2 |
			(uint64(in[43])-minv)<<31 |
			(uint64(in[44])-minv)<<60

	out[20] =
		(uint64(in[44])-minv)>>4 |

			(uint64(in[45])-minv)<<25 |
			(uint64(in[46])-minv)<<54

	out[21] =
		(uint64(in[46])-minv)>>10 |

			(uint64(in[47])-minv)<<19 |
			(uint64(in[48])-minv)<<48

	out[22] =
		(uint64(in[48])-minv)>>16 |

			(uint64(in[49])-minv)<<13 |
			(uint64(in[50])-minv)<<42

	out[23] =
		(uint64(in[50])-minv)>>22 |

			(uint64(in[51])-minv)<<7 |
			(uint64(in[52])-minv)<<36

	out[24] =
		(uint64(in[52])-minv)>>28 |

			(uint64(in[53])-minv)<<1 |
			(uint64(in[54])-minv)<<30 |
			(uint64(in[55])-minv)<<59

	out[25] =
		(uint64(in[55])-minv)>>5 |

			(uint64(in[56])-minv)<<24 |
			(uint64(in[57])-minv)<<53

	out[26] =
		(uint64(in[57])-minv)>>11 |

			(uint64(in[58])-minv)<<18 |
			(uint64(in[59])-minv)<<47

	out[27] =
		(uint64(in[59])-minv)>>17 |

			(uint64(in[60])-minv)<<12 |
			(uint64(in[61])-minv)<<41

	out[28] =
		(uint64(in[61])-minv)>>23 |

			(uint64(in[62])-minv)<<6 |
			(uint64(in[63])-minv)<<35

}
func bp32_30[T uint32 | int32](in *[64]T, out *[30]uint64, minv uint64) {
	out[0] =
		(uint64(in[0])-minv)<<0 |
			(uint64(in[1])-minv)<<30 |
			(uint64(in[2])-minv)<<60

	out[1] =
		(uint64(in[2])-minv)>>4 |

			(uint64(in[3])-minv)<<26 |
			(uint64(in[4])-minv)<<56

	out[2] =
		(uint64(in[4])-minv)>>8 |

			(uint64(in[5])-minv)<<22 |
			(uint64(in[6])-minv)<<52

	out[3] =
		(uint64(in[6])-minv)>>12 |

			(uint64(in[7])-minv)<<18 |
			(uint64(in[8])-minv)<<48

	out[4] =
		(uint64(in[8])-minv)>>16 |

			(uint64(in[9])-minv)<<14 |
			(uint64(in[10])-minv)<<44

	out[5] =
		(uint64(in[10])-minv)>>20 |

			(uint64(in[11])-minv)<<10 |
			(uint64(in[12])-minv)<<40

	out[6] =
		(uint64(in[12])-minv)>>24 |

			(uint64(in[13])-minv)<<6 |
			(uint64(in[14])-minv)<<36

	out[7] =
		(uint64(in[14])-minv)>>28 |

			(uint64(in[15])-minv)<<2 |
			(uint64(in[16])-minv)<<32 |
			(uint64(in[17])-minv)<<62

	out[8] =
		(uint64(in[17])-minv)>>2 |

			(uint64(in[18])-minv)<<28 |
			(uint64(in[19])-minv)<<58

	out[9] =
		(uint64(in[19])-minv)>>6 |

			(uint64(in[20])-minv)<<24 |
			(uint64(in[21])-minv)<<54

	out[10] =
		(uint64(in[21])-minv)>>10 |

			(uint64(in[22])-minv)<<20 |
			(uint64(in[23])-minv)<<50

	out[11] =
		(uint64(in[23])-minv)>>14 |

			(uint64(in[24])-minv)<<16 |
			(uint64(in[25])-minv)<<46

	out[12] =
		(uint64(in[25])-minv)>>18 |

			(uint64(in[26])-minv)<<12 |
			(uint64(in[27])-minv)<<42

	out[13] =
		(uint64(in[27])-minv)>>22 |

			(uint64(in[28])-minv)<<8 |
			(uint64(in[29])-minv)<<38

	out[14] =
		(uint64(in[29])-minv)>>26 |

			(uint64(in[30])-minv)<<4 |
			(uint64(in[31])-minv)<<34

	out[15] =
		(uint64(in[31])-minv)>>30 |

			(uint64(in[32])-minv)<<0 |
			(uint64(in[33])-minv)<<30 |
			(uint64(in[34])-minv)<<60

	out[16] =
		(uint64(in[34])-minv)>>4 |

			(uint64(in[35])-minv)<<26 |
			(uint64(in[36])-minv)<<56

	out[17] =
		(uint64(in[36])-minv)>>8 |

			(uint64(in[37])-minv)<<22 |
			(uint64(in[38])-minv)<<52

	out[18] =
		(uint64(in[38])-minv)>>12 |

			(uint64(in[39])-minv)<<18 |
			(uint64(in[40])-minv)<<48

	out[19] =
		(uint64(in[40])-minv)>>16 |

			(uint64(in[41])-minv)<<14 |
			(uint64(in[42])-minv)<<44

	out[20] =
		(uint64(in[42])-minv)>>20 |

			(uint64(in[43])-minv)<<10 |
			(uint64(in[44])-minv)<<40

	out[21] =
		(uint64(in[44])-minv)>>24 |

			(uint64(in[45])-minv)<<6 |
			(uint64(in[46])-minv)<<36

	out[22] =
		(uint64(in[46])-minv)>>28 |

			(uint64(in[47])-minv)<<2 |
			(uint64(in[48])-minv)<<32 |
			(uint64(in[49])-minv)<<62

	out[23] =
		(uint64(in[49])-minv)>>2 |

			(uint64(in[50])-minv)<<28 |
			(uint64(in[51])-minv)<<58

	out[24] =
		(uint64(in[51])-minv)>>6 |

			(uint64(in[52])-minv)<<24 |
			(uint64(in[53])-minv)<<54

	out[25] =
		(uint64(in[53])-minv)>>10 |

			(uint64(in[54])-minv)<<20 |
			(uint64(in[55])-minv)<<50

	out[26] =
		(uint64(in[55])-minv)>>14 |

			(uint64(in[56])-minv)<<16 |
			(uint64(in[57])-minv)<<46

	out[27] =
		(uint64(in[57])-minv)>>18 |

			(uint64(in[58])-minv)<<12 |
			(uint64(in[59])-minv)<<42

	out[28] =
		(uint64(in[59])-minv)>>22 |

			(uint64(in[60])-minv)<<8 |
			(uint64(in[61])-minv)<<38

	out[29] =
		(uint64(in[61])-minv)>>26 |

			(uint64(in[62])-minv)<<4 |
			(uint64(in[63])-minv)<<34

}
func bp32_31[T uint32 | int32](in *[64]T, out *[31]uint64, minv uint64) {
	out[0] =
		(uint64(in[0])-minv)<<0 |
			(uint64(in[1])-minv)<<31 |
			(uint64(in[2])-minv)<<62

	out[1] =
		(uint64(in[2])-minv)>>2 |

			(uint64(in[3])-minv)<<29 |
			(uint64(in[4])-minv)<<60

	out[2] =
		(uint64(in[4])-minv)>>4 |

			(uint64(in[5])-minv)<<27 |
			(uint64(in[6])-minv)<<58

	out[3] =
		(uint64(in[6])-minv)>>6 |

			(uint64(in[7])-minv)<<25 |
			(uint64(in[8])-minv)<<56

	out[4] =
		(uint64(in[8])-minv)>>8 |

			(uint64(in[9])-minv)<<23 |
			(uint64(in[10])-minv)<<54

	out[5] =
		(uint64(in[10])-minv)>>10 |

			(uint64(in[11])-minv)<<21 |
			(uint64(in[12])-minv)<<52

	out[6] =
		(uint64(in[12])-minv)>>12 |

			(uint64(in[13])-minv)<<19 |
			(uint64(in[14])-minv)<<50

	out[7] =
		(uint64(in[14])-minv)>>14 |

			(uint64(in[15])-minv)<<17 |
			(uint64(in[16])-minv)<<48

	out[8] =
		(uint64(in[16])-minv)>>16 |

			(uint64(in[17])-minv)<<15 |
			(uint64(in[18])-minv)<<46

	out[9] =
		(uint64(in[18])-minv)>>18 |

			(uint64(in[19])-minv)<<13 |
			(uint64(in[20])-minv)<<44

	out[10] =
		(uint64(in[20])-minv)>>20 |

			(uint64(in[21])-minv)<<11 |
			(uint64(in[22])-minv)<<42

	out[11] =
		(uint64(in[22])-minv)>>22 |

			(uint64(in[23])-minv)<<9 |
			(uint64(in[24])-minv)<<40

	out[12] =
		(uint64(in[24])-minv)>>24 |

			(uint64(in[25])-minv)<<7 |
			(uint64(in[26])-minv)<<38

	out[13] =
		(uint64(in[26])-minv)>>26 |

			(uint64(in[27])-minv)<<5 |
			(uint64(in[28])-minv)<<36

	out[14] =
		(uint64(in[28])-minv)>>28 |

			(uint64(in[29])-minv)<<3 |
			(uint64(in[30])-minv)<<34

	out[15] =
		(uint64(in[30])-minv)>>30 |

			(uint64(in[31])-minv)<<1 |
			(uint64(in[32])-minv)<<32 |
			(uint64(in[33])-minv)<<63

	out[16] =
		(uint64(in[33])-minv)>>1 |

			(uint64(in[34])-minv)<<30 |
			(uint64(in[35])-minv)<<61

	out[17] =
		(uint64(in[35])-minv)>>3 |

			(uint64(in[36])-minv)<<28 |
			(uint64(in[37])-minv)<<59

	out[18] =
		(uint64(in[37])-minv)>>5 |

			(uint64(in[38])-minv)<<26 |
			(uint64(in[39])-minv)<<57

	out[19] =
		(uint64(in[39])-minv)>>7 |

			(uint64(in[40])-minv)<<24 |
			(uint64(in[41])-minv)<<55

	out[20] =
		(uint64(in[41])-minv)>>9 |

			(uint64(in[42])-minv)<<22 |
			(uint64(in[43])-minv)<<53

	out[21] =
		(uint64(in[43])-minv)>>11 |

			(uint64(in[44])-minv)<<20 |
			(uint64(in[45])-minv)<<51

	out[22] =
		(uint64(in[45])-minv)>>13 |

			(uint64(in[46])-minv)<<18 |
			(uint64(in[47])-minv)<<49

	out[23] =
		(uint64(in[47])-minv)>>15 |

			(uint64(in[48])-minv)<<16 |
			(uint64(in[49])-minv)<<47

	out[24] =
		(uint64(in[49])-minv)>>17 |

			(uint64(in[50])-minv)<<14 |
			(uint64(in[51])-minv)<<45

	out[25] =
		(uint64(in[51])-minv)>>19 |

			(uint64(in[52])-minv)<<12 |
			(uint64(in[53])-minv)<<43

	out[26] =
		(uint64(in[53])-minv)>>21 |

			(uint64(in[54])-minv)<<10 |
			(uint64(in[55])-minv)<<41

	out[27] =
		(uint64(in[55])-minv)>>23 |

			(uint64(in[56])-minv)<<8 |
			(uint64(in[57])-minv)<<39

	out[28] =
		(uint64(in[57])-minv)>>25 |

			(uint64(in[58])-minv)<<6 |
			(uint64(in[59])-minv)<<37

	out[29] =
		(uint64(in[59])-minv)>>27 |

			(uint64(in[60])-minv)<<4 |
			(uint64(in[61])-minv)<<35

	out[30] =
		(uint64(in[61])-minv)>>29 |

			(uint64(in[62])-minv)<<2 |
			(uint64(in[63])-minv)<<33

}

// Reader
func bitread32[T uint32 | int32](out []T, in []uint64, log2 int, minv T) {
	switch log2 {
	case 0:
		br32_0((*[64]T)(out), (*[0]uint64)(in), uint64(minv))
	case 1:
		br32_1((*[64]T)(out), (*[1]uint64)(in), uint64(minv))
	case 2:
		br32_2((*[64]T)(out), (*[2]uint64)(in), uint64(minv))
	case 3:
		br32_3((*[64]T)(out), (*[3]uint64)(in), uint64(minv))
	case 4:
		br32_4((*[64]T)(out), (*[4]uint64)(in), uint64(minv))
	case 5:
		br32_5((*[64]T)(out), (*[5]uint64)(in), uint64(minv))
	case 6:
		br32_6((*[64]T)(out), (*[6]uint64)(in), uint64(minv))
	case 7:
		br32_7((*[64]T)(out), (*[7]uint64)(in), uint64(minv))
	case 8:
		br32_8((*[64]T)(out), (*[8]uint64)(in), uint64(minv))
	case 9:
		br32_9((*[64]T)(out), (*[9]uint64)(in), uint64(minv))
	case 10:
		br32_10((*[64]T)(out), (*[10]uint64)(in), uint64(minv))
	case 11:
		br32_11((*[64]T)(out), (*[11]uint64)(in), uint64(minv))
	case 12:
		br32_12((*[64]T)(out), (*[12]uint64)(in), uint64(minv))
	case 13:
		br32_13((*[64]T)(out), (*[13]uint64)(in), uint64(minv))
	case 14:
		br32_14((*[64]T)(out), (*[14]uint64)(in), uint64(minv))
	case 15:
		br32_15((*[64]T)(out), (*[15]uint64)(in), uint64(minv))
	case 16:
		br32_16((*[64]T)(out), (*[16]uint64)(in), uint64(minv))
	case 17:
		br32_17((*[64]T)(out), (*[17]uint64)(in), uint64(minv))
	case 18:
		br32_18((*[64]T)(out), (*[18]uint64)(in), uint64(minv))
	case 19:
		br32_19((*[64]T)(out), (*[19]uint64)(in), uint64(minv))
	case 20:
		br32_20((*[64]T)(out), (*[20]uint64)(in), uint64(minv))
	case 21:
		br32_21((*[64]T)(out), (*[21]uint64)(in), uint64(minv))
	case 22:
		br32_22((*[64]T)(out), (*[22]uint64)(in), uint64(minv))
	case 23:
		br32_23((*[64]T)(out), (*[23]uint64)(in), uint64(minv))
	case 24:
		br32_24((*[64]T)(out), (*[24]uint64)(in), uint64(minv))
	case 25:
		br32_25((*[64]T)(out), (*[25]uint64)(in), uint64(minv))
	case 26:
		br32_26((*[64]T)(out), (*[26]uint64)(in), uint64(minv))
	case 27:
		br32_27((*[64]T)(out), (*[27]uint64)(in), uint64(minv))
	case 28:
		br32_28((*[64]T)(out), (*[28]uint64)(in), uint64(minv))
	case 29:
		br32_29((*[64]T)(out), (*[29]uint64)(in), uint64(minv))
	case 30:
		br32_30((*[64]T)(out), (*[30]uint64)(in), uint64(minv))
	case 31:
		br32_31((*[64]T)(out), (*[31]uint64)(in), uint64(minv))
	}
}
func br32_0[T uint32 | int32](out *[64]T, in *[0]uint64, minv uint64) {
	for i := range out {
		out[i] = T(minv)
	}
}
func br32_1[T uint32 | int32](out *[64]T, in *[1]uint64, minv uint64) {
	mask := uint64((1 << 1) - 1)
	out[0] = T((in[0]>>0)&mask + minv)
	out[1] = T((in[0]>>1)&mask + minv)
	out[2] = T((in[0]>>2)&mask + minv)
	out[3] = T((in[0]>>3)&mask + minv)
	out[4] = T((in[0]>>4)&mask + minv)
	out[5] = T((in[0]>>5)&mask + minv)
	out[6] = T((in[0]>>6)&mask + minv)
	out[7] = T((in[0]>>7)&mask + minv)
	out[8] = T((in[0]>>8)&mask + minv)
	out[9] = T((in[0]>>9)&mask + minv)
	out[10] = T((in[0]>>10)&mask + minv)
	out[11] = T((in[0]>>11)&mask + minv)
	out[12] = T((in[0]>>12)&mask + minv)
	out[13] = T((in[0]>>13)&mask + minv)
	out[14] = T((in[0]>>14)&mask + minv)
	out[15] = T((in[0]>>15)&mask + minv)
	out[16] = T((in[0]>>16)&mask + minv)
	out[17] = T((in[0]>>17)&mask + minv)
	out[18] = T((in[0]>>18)&mask + minv)
	out[19] = T((in[0]>>19)&mask + minv)
	out[20] = T((in[0]>>20)&mask + minv)
	out[21] = T((in[0]>>21)&mask + minv)
	out[22] = T((in[0]>>22)&mask + minv)
	out[23] = T((in[0]>>23)&mask + minv)
	out[24] = T((in[0]>>24)&mask + minv)
	out[25] = T((in[0]>>25)&mask + minv)
	out[26] = T((in[0]>>26)&mask + minv)
	out[27] = T((in[0]>>27)&mask + minv)
	out[28] = T((in[0]>>28)&mask + minv)
	out[29] = T((in[0]>>29)&mask + minv)
	out[30] = T((in[0]>>30)&mask + minv)
	out[31] = T((in[0]>>31)&mask + minv)
	out[32] = T((in[0]>>32)&mask + minv)
	out[33] = T((in[0]>>33)&mask + minv)
	out[34] = T((in[0]>>34)&mask + minv)
	out[35] = T((in[0]>>35)&mask + minv)
	out[36] = T((in[0]>>36)&mask + minv)
	out[37] = T((in[0]>>37)&mask + minv)
	out[38] = T((in[0]>>38)&mask + minv)
	out[39] = T((in[0]>>39)&mask + minv)
	out[40] = T((in[0]>>40)&mask + minv)
	out[41] = T((in[0]>>41)&mask + minv)
	out[42] = T((in[0]>>42)&mask + minv)
	out[43] = T((in[0]>>43)&mask + minv)
	out[44] = T((in[0]>>44)&mask + minv)
	out[45] = T((in[0]>>45)&mask + minv)
	out[46] = T((in[0]>>46)&mask + minv)
	out[47] = T((in[0]>>47)&mask + minv)
	out[48] = T((in[0]>>48)&mask + minv)
	out[49] = T((in[0]>>49)&mask + minv)
	out[50] = T((in[0]>>50)&mask + minv)
	out[51] = T((in[0]>>51)&mask + minv)
	out[52] = T((in[0]>>52)&mask + minv)
	out[53] = T((in[0]>>53)&mask + minv)
	out[54] = T((in[0]>>54)&mask + minv)
	out[55] = T((in[0]>>55)&mask + minv)
	out[56] = T((in[0]>>56)&mask + minv)
	out[57] = T((in[0]>>57)&mask + minv)
	out[58] = T((in[0]>>58)&mask + minv)
	out[59] = T((in[0]>>59)&mask + minv)
	out[60] = T((in[0]>>60)&mask + minv)
	out[61] = T((in[0]>>61)&mask + minv)
	out[62] = T((in[0]>>62)&mask + minv)
	out[63] = T((in[0]>>63)&mask + minv)

}
func br32_2[T uint32 | int32](out *[64]T, in *[2]uint64, minv uint64) {
	mask := uint64((1 << 2) - 1)
	out[0] = T((in[0]>>0)&mask + minv)
	out[1] = T((in[0]>>2)&mask + minv)
	out[2] = T((in[0]>>4)&mask + minv)
	out[3] = T((in[0]>>6)&mask + minv)
	out[4] = T((in[0]>>8)&mask + minv)
	out[5] = T((in[0]>>10)&mask + minv)
	out[6] = T((in[0]>>12)&mask + minv)
	out[7] = T((in[0]>>14)&mask + minv)
	out[8] = T((in[0]>>16)&mask + minv)
	out[9] = T((in[0]>>18)&mask + minv)
	out[10] = T((in[0]>>20)&mask + minv)
	out[11] = T((in[0]>>22)&mask + minv)
	out[12] = T((in[0]>>24)&mask + minv)
	out[13] = T((in[0]>>26)&mask + minv)
	out[14] = T((in[0]>>28)&mask + minv)
	out[15] = T((in[0]>>30)&mask + minv)
	out[16] = T((in[0]>>32)&mask + minv)
	out[17] = T((in[0]>>34)&mask + minv)
	out[18] = T((in[0]>>36)&mask + minv)
	out[19] = T((in[0]>>38)&mask + minv)
	out[20] = T((in[0]>>40)&mask + minv)
	out[21] = T((in[0]>>42)&mask + minv)
	out[22] = T((in[0]>>44)&mask + minv)
	out[23] = T((in[0]>>46)&mask + minv)
	out[24] = T((in[0]>>48)&mask + minv)
	out[25] = T((in[0]>>50)&mask + minv)
	out[26] = T((in[0]>>52)&mask + minv)
	out[27] = T((in[0]>>54)&mask + minv)
	out[28] = T((in[0]>>56)&mask + minv)
	out[29] = T((in[0]>>58)&mask + minv)
	out[30] = T((in[0]>>60)&mask + minv)
	out[31] = T((in[0]>>62)&mask + minv)
	out[32] = T((in[1]>>0)&mask + minv)
	out[33] = T((in[1]>>2)&mask + minv)
	out[34] = T((in[1]>>4)&mask + minv)
	out[35] = T((in[1]>>6)&mask + minv)
	out[36] = T((in[1]>>8)&mask + minv)
	out[37] = T((in[1]>>10)&mask + minv)
	out[38] = T((in[1]>>12)&mask + minv)
	out[39] = T((in[1]>>14)&mask + minv)
	out[40] = T((in[1]>>16)&mask + minv)
	out[41] = T((in[1]>>18)&mask + minv)
	out[42] = T((in[1]>>20)&mask + minv)
	out[43] = T((in[1]>>22)&mask + minv)
	out[44] = T((in[1]>>24)&mask + minv)
	out[45] = T((in[1]>>26)&mask + minv)
	out[46] = T((in[1]>>28)&mask + minv)
	out[47] = T((in[1]>>30)&mask + minv)
	out[48] = T((in[1]>>32)&mask + minv)
	out[49] = T((in[1]>>34)&mask + minv)
	out[50] = T((in[1]>>36)&mask + minv)
	out[51] = T((in[1]>>38)&mask + minv)
	out[52] = T((in[1]>>40)&mask + minv)
	out[53] = T((in[1]>>42)&mask + minv)
	out[54] = T((in[1]>>44)&mask + minv)
	out[55] = T((in[1]>>46)&mask + minv)
	out[56] = T((in[1]>>48)&mask + minv)
	out[57] = T((in[1]>>50)&mask + minv)
	out[58] = T((in[1]>>52)&mask + minv)
	out[59] = T((in[1]>>54)&mask + minv)
	out[60] = T((in[1]>>56)&mask + minv)
	out[61] = T((in[1]>>58)&mask + minv)
	out[62] = T((in[1]>>60)&mask + minv)
	out[63] = T((in[1]>>62)&mask + minv)

}
func br32_3[T uint32 | int32](out *[64]T, in *[3]uint64, minv uint64) {
	mask := uint64((1 << 3) - 1)
	out[0] = T((in[0]>>0)&mask + minv)
	out[1] = T((in[0]>>3)&mask + minv)
	out[2] = T((in[0]>>6)&mask + minv)
	out[3] = T((in[0]>>9)&mask + minv)
	out[4] = T((in[0]>>12)&mask + minv)
	out[5] = T((in[0]>>15)&mask + minv)
	out[6] = T((in[0]>>18)&mask + minv)
	out[7] = T((in[0]>>21)&mask + minv)
	out[8] = T((in[0]>>24)&mask + minv)
	out[9] = T((in[0]>>27)&mask + minv)
	out[10] = T((in[0]>>30)&mask + minv)
	out[11] = T((in[0]>>33)&mask + minv)
	out[12] = T((in[0]>>36)&mask + minv)
	out[13] = T((in[0]>>39)&mask + minv)
	out[14] = T((in[0]>>42)&mask + minv)
	out[15] = T((in[0]>>45)&mask + minv)
	out[16] = T((in[0]>>48)&mask + minv)
	out[17] = T((in[0]>>51)&mask + minv)
	out[18] = T((in[0]>>54)&mask + minv)
	out[19] = T((in[0]>>57)&mask + minv)
	out[20] = T((in[0]>>60)&mask + minv)
	out[21] = T((in[0]>>63)&mask |
		(in[1]<<1)&mask + minv)
	out[22] = T((in[1]>>2)&mask + minv)
	out[23] = T((in[1]>>5)&mask + minv)
	out[24] = T((in[1]>>8)&mask + minv)
	out[25] = T((in[1]>>11)&mask + minv)
	out[26] = T((in[1]>>14)&mask + minv)
	out[27] = T((in[1]>>17)&mask + minv)
	out[28] = T((in[1]>>20)&mask + minv)
	out[29] = T((in[1]>>23)&mask + minv)
	out[30] = T((in[1]>>26)&mask + minv)
	out[31] = T((in[1]>>29)&mask + minv)
	out[32] = T((in[1]>>32)&mask + minv)
	out[33] = T((in[1]>>35)&mask + minv)
	out[34] = T((in[1]>>38)&mask + minv)
	out[35] = T((in[1]>>41)&mask + minv)
	out[36] = T((in[1]>>44)&mask + minv)
	out[37] = T((in[1]>>47)&mask + minv)
	out[38] = T((in[1]>>50)&mask + minv)
	out[39] = T((in[1]>>53)&mask + minv)
	out[40] = T((in[1]>>56)&mask + minv)
	out[41] = T((in[1]>>59)&mask + minv)
	out[42] = T((in[1]>>62)&mask |
		(in[2]<<2)&mask + minv)
	out[43] = T((in[2]>>1)&mask + minv)
	out[44] = T((in[2]>>4)&mask + minv)
	out[45] = T((in[2]>>7)&mask + minv)
	out[46] = T((in[2]>>10)&mask + minv)
	out[47] = T((in[2]>>13)&mask + minv)
	out[48] = T((in[2]>>16)&mask + minv)
	out[49] = T((in[2]>>19)&mask + minv)
	out[50] = T((in[2]>>22)&mask + minv)
	out[51] = T((in[2]>>25)&mask + minv)
	out[52] = T((in[2]>>28)&mask + minv)
	out[53] = T((in[2]>>31)&mask + minv)
	out[54] = T((in[2]>>34)&mask + minv)
	out[55] = T((in[2]>>37)&mask + minv)
	out[56] = T((in[2]>>40)&mask + minv)
	out[57] = T((in[2]>>43)&mask + minv)
	out[58] = T((in[2]>>46)&mask + minv)
	out[59] = T((in[2]>>49)&mask + minv)
	out[60] = T((in[2]>>52)&mask + minv)
	out[61] = T((in[2]>>55)&mask + minv)
	out[62] = T((in[2]>>58)&mask + minv)
	out[63] = T((in[2]>>61)&mask + minv)

}
func br32_4[T uint32 | int32](out *[64]T, in *[4]uint64, minv uint64) {
	mask := uint64((1 << 4) - 1)
	out[0] = T((in[0]>>0)&mask + minv)
	out[1] = T((in[0]>>4)&mask + minv)
	out[2] = T((in[0]>>8)&mask + minv)
	out[3] = T((in[0]>>12)&mask + minv)
	out[4] = T((in[0]>>16)&mask + minv)
	out[5] = T((in[0]>>20)&mask + minv)
	out[6] = T((in[0]>>24)&mask + minv)
	out[7] = T((in[0]>>28)&mask + minv)
	out[8] = T((in[0]>>32)&mask + minv)
	out[9] = T((in[0]>>36)&mask + minv)
	out[10] = T((in[0]>>40)&mask + minv)
	out[11] = T((in[0]>>44)&mask + minv)
	out[12] = T((in[0]>>48)&mask + minv)
	out[13] = T((in[0]>>52)&mask + minv)
	out[14] = T((in[0]>>56)&mask + minv)
	out[15] = T((in[0]>>60)&mask + minv)
	out[16] = T((in[1]>>0)&mask + minv)
	out[17] = T((in[1]>>4)&mask + minv)
	out[18] = T((in[1]>>8)&mask + minv)
	out[19] = T((in[1]>>12)&mask + minv)
	out[20] = T((in[1]>>16)&mask + minv)
	out[21] = T((in[1]>>20)&mask + minv)
	out[22] = T((in[1]>>24)&mask + minv)
	out[23] = T((in[1]>>28)&mask + minv)
	out[24] = T((in[1]>>32)&mask + minv)
	out[25] = T((in[1]>>36)&mask + minv)
	out[26] = T((in[1]>>40)&mask + minv)
	out[27] = T((in[1]>>44)&mask + minv)
	out[28] = T((in[1]>>48)&mask + minv)
	out[29] = T((in[1]>>52)&mask + minv)
	out[30] = T((in[1]>>56)&mask + minv)
	out[31] = T((in[1]>>60)&mask + minv)
	out[32] = T((in[2]>>0)&mask + minv)
	out[33] = T((in[2]>>4)&mask + minv)
	out[34] = T((in[2]>>8)&mask + minv)
	out[35] = T((in[2]>>12)&mask + minv)
	out[36] = T((in[2]>>16)&mask + minv)
	out[37] = T((in[2]>>20)&mask + minv)
	out[38] = T((in[2]>>24)&mask + minv)
	out[39] = T((in[2]>>28)&mask + minv)
	out[40] = T((in[2]>>32)&mask + minv)
	out[41] = T((in[2]>>36)&mask + minv)
	out[42] = T((in[2]>>40)&mask + minv)
	out[43] = T((in[2]>>44)&mask + minv)
	out[44] = T((in[2]>>48)&mask + minv)
	out[45] = T((in[2]>>52)&mask + minv)
	out[46] = T((in[2]>>56)&mask + minv)
	out[47] = T((in[2]>>60)&mask + minv)
	out[48] = T((in[3]>>0)&mask + minv)
	out[49] = T((in[3]>>4)&mask + minv)
	out[50] = T((in[3]>>8)&mask + minv)
	out[51] = T((in[3]>>12)&mask + minv)
	out[52] = T((in[3]>>16)&mask + minv)
	out[53] = T((in[3]>>20)&mask + minv)
	out[54] = T((in[3]>>24)&mask + minv)
	out[55] = T((in[3]>>28)&mask + minv)
	out[56] = T((in[3]>>32)&mask + minv)
	out[57] = T((in[3]>>36)&mask + minv)
	out[58] = T((in[3]>>40)&mask + minv)
	out[59] = T((in[3]>>44)&mask + minv)
	out[60] = T((in[3]>>48)&mask + minv)
	out[61] = T((in[3]>>52)&mask + minv)
	out[62] = T((in[3]>>56)&mask + minv)
	out[63] = T((in[3]>>60)&mask + minv)

}
func br32_5[T uint32 | int32](out *[64]T, in *[5]uint64, minv uint64) {
	mask := uint64((1 << 5) - 1)
	out[0] = T((in[0]>>0)&mask + minv)
	out[1] = T((in[0]>>5)&mask + minv)
	out[2] = T((in[0]>>10)&mask + minv)
	out[3] = T((in[0]>>15)&mask + minv)
	out[4] = T((in[0]>>20)&mask + minv)
	out[5] = T((in[0]>>25)&mask + minv)
	out[6] = T((in[0]>>30)&mask + minv)
	out[7] = T((in[0]>>35)&mask + minv)
	out[8] = T((in[0]>>40)&mask + minv)
	out[9] = T((in[0]>>45)&mask + minv)
	out[10] = T((in[0]>>50)&mask + minv)
	out[11] = T((in[0]>>55)&mask + minv)
	out[12] = T((in[0]>>60)&mask |
		(in[1]<<4)&mask + minv)
	out[13] = T((in[1]>>1)&mask + minv)
	out[14] = T((in[1]>>6)&mask + minv)
	out[15] = T((in[1]>>11)&mask + minv)
	out[16] = T((in[1]>>16)&mask + minv)
	out[17] = T((in[1]>>21)&mask + minv)
	out[18] = T((in[1]>>26)&mask + minv)
	out[19] = T((in[1]>>31)&mask + minv)
	out[20] = T((in[1]>>36)&mask + minv)
	out[21] = T((in[1]>>41)&mask + minv)
	out[22] = T((in[1]>>46)&mask + minv)
	out[23] = T((in[1]>>51)&mask + minv)
	out[24] = T((in[1]>>56)&mask + minv)
	out[25] = T((in[1]>>61)&mask |
		(in[2]<<3)&mask + minv)
	out[26] = T((in[2]>>2)&mask + minv)
	out[27] = T((in[2]>>7)&mask + minv)
	out[28] = T((in[2]>>12)&mask + minv)
	out[29] = T((in[2]>>17)&mask + minv)
	out[30] = T((in[2]>>22)&mask + minv)
	out[31] = T((in[2]>>27)&mask + minv)
	out[32] = T((in[2]>>32)&mask + minv)
	out[33] = T((in[2]>>37)&mask + minv)
	out[34] = T((in[2]>>42)&mask + minv)
	out[35] = T((in[2]>>47)&mask + minv)
	out[36] = T((in[2]>>52)&mask + minv)
	out[37] = T((in[2]>>57)&mask + minv)
	out[38] = T((in[2]>>62)&mask |
		(in[3]<<2)&mask + minv)
	out[39] = T((in[3]>>3)&mask + minv)
	out[40] = T((in[3]>>8)&mask + minv)
	out[41] = T((in[3]>>13)&mask + minv)
	out[42] = T((in[3]>>18)&mask + minv)
	out[43] = T((in[3]>>23)&mask + minv)
	out[44] = T((in[3]>>28)&mask + minv)
	out[45] = T((in[3]>>33)&mask + minv)
	out[46] = T((in[3]>>38)&mask + minv)
	out[47] = T((in[3]>>43)&mask + minv)
	out[48] = T((in[3]>>48)&mask + minv)
	out[49] = T((in[3]>>53)&mask + minv)
	out[50] = T((in[3]>>58)&mask + minv)
	out[51] = T((in[3]>>63)&mask |
		(in[4]<<1)&mask + minv)
	out[52] = T((in[4]>>4)&mask + minv)
	out[53] = T((in[4]>>9)&mask + minv)
	out[54] = T((in[4]>>14)&mask + minv)
	out[55] = T((in[4]>>19)&mask + minv)
	out[56] = T((in[4]>>24)&mask + minv)
	out[57] = T((in[4]>>29)&mask + minv)
	out[58] = T((in[4]>>34)&mask + minv)
	out[59] = T((in[4]>>39)&mask + minv)
	out[60] = T((in[4]>>44)&mask + minv)
	out[61] = T((in[4]>>49)&mask + minv)
	out[62] = T((in[4]>>54)&mask + minv)
	out[63] = T((in[4]>>59)&mask + minv)

}
func br32_6[T uint32 | int32](out *[64]T, in *[6]uint64, minv uint64) {
	mask := uint64((1 << 6) - 1)
	out[0] = T((in[0]>>0)&mask + minv)
	out[1] = T((in[0]>>6)&mask + minv)
	out[2] = T((in[0]>>12)&mask + minv)
	out[3] = T((in[0]>>18)&mask + minv)
	out[4] = T((in[0]>>24)&mask + minv)
	out[5] = T((in[0]>>30)&mask + minv)
	out[6] = T((in[0]>>36)&mask + minv)
	out[7] = T((in[0]>>42)&mask + minv)
	out[8] = T((in[0]>>48)&mask + minv)
	out[9] = T((in[0]>>54)&mask + minv)
	out[10] = T((in[0]>>60)&mask |
		(in[1]<<4)&mask + minv)
	out[11] = T((in[1]>>2)&mask + minv)
	out[12] = T((in[1]>>8)&mask + minv)
	out[13] = T((in[1]>>14)&mask + minv)
	out[14] = T((in[1]>>20)&mask + minv)
	out[15] = T((in[1]>>26)&mask + minv)
	out[16] = T((in[1]>>32)&mask + minv)
	out[17] = T((in[1]>>38)&mask + minv)
	out[18] = T((in[1]>>44)&mask + minv)
	out[19] = T((in[1]>>50)&mask + minv)
	out[20] = T((in[1]>>56)&mask + minv)
	out[21] = T((in[1]>>62)&mask |
		(in[2]<<2)&mask + minv)
	out[22] = T((in[2]>>4)&mask + minv)
	out[23] = T((in[2]>>10)&mask + minv)
	out[24] = T((in[2]>>16)&mask + minv)
	out[25] = T((in[2]>>22)&mask + minv)
	out[26] = T((in[2]>>28)&mask + minv)
	out[27] = T((in[2]>>34)&mask + minv)
	out[28] = T((in[2]>>40)&mask + minv)
	out[29] = T((in[2]>>46)&mask + minv)
	out[30] = T((in[2]>>52)&mask + minv)
	out[31] = T((in[2]>>58)&mask + minv)
	out[32] = T((in[3]>>0)&mask + minv)
	out[33] = T((in[3]>>6)&mask + minv)
	out[34] = T((in[3]>>12)&mask + minv)
	out[35] = T((in[3]>>18)&mask + minv)
	out[36] = T((in[3]>>24)&mask + minv)
	out[37] = T((in[3]>>30)&mask + minv)
	out[38] = T((in[3]>>36)&mask + minv)
	out[39] = T((in[3]>>42)&mask + minv)
	out[40] = T((in[3]>>48)&mask + minv)
	out[41] = T((in[3]>>54)&mask + minv)
	out[42] = T((in[3]>>60)&mask |
		(in[4]<<4)&mask + minv)
	out[43] = T((in[4]>>2)&mask + minv)
	out[44] = T((in[4]>>8)&mask + minv)
	out[45] = T((in[4]>>14)&mask + minv)
	out[46] = T((in[4]>>20)&mask + minv)
	out[47] = T((in[4]>>26)&mask + minv)
	out[48] = T((in[4]>>32)&mask + minv)
	out[49] = T((in[4]>>38)&mask + minv)
	out[50] = T((in[4]>>44)&mask + minv)
	out[51] = T((in[4]>>50)&mask + minv)
	out[52] = T((in[4]>>56)&mask + minv)
	out[53] = T((in[4]>>62)&mask |
		(in[5]<<2)&mask + minv)
	out[54] = T((in[5]>>4)&mask + minv)
	out[55] = T((in[5]>>10)&mask + minv)
	out[56] = T((in[5]>>16)&mask + minv)
	out[57] = T((in[5]>>22)&mask + minv)
	out[58] = T((in[5]>>28)&mask + minv)
	out[59] = T((in[5]>>34)&mask + minv)
	out[60] = T((in[5]>>40)&mask + minv)
	out[61] = T((in[5]>>46)&mask + minv)
	out[62] = T((in[5]>>52)&mask + minv)
	out[63] = T((in[5]>>58)&mask + minv)

}
func br32_7[T uint32 | int32](out *[64]T, in *[7]uint64, minv uint64) {
	mask := uint64((1 << 7) - 1)
	out[0] = T((in[0]>>0)&mask + minv)
	out[1] = T((in[0]>>7)&mask + minv)
	out[2] = T((in[0]>>14)&mask + minv)
	out[3] = T((in[0]>>21)&mask + minv)
	out[4] = T((in[0]>>28)&mask + minv)
	out[5] = T((in[0]>>35)&mask + minv)
	out[6] = T((in[0]>>42)&mask + minv)
	out[7] = T((in[0]>>49)&mask + minv)
	out[8] = T((in[0]>>56)&mask + minv)
	out[9] = T((in[0]>>63)&mask |
		(in[1]<<1)&mask + minv)
	out[10] = T((in[1]>>6)&mask + minv)
	out[11] = T((in[1]>>13)&mask + minv)
	out[12] = T((in[1]>>20)&mask + minv)
	out[13] = T((in[1]>>27)&mask + minv)
	out[14] = T((in[1]>>34)&mask + minv)
	out[15] = T((in[1]>>41)&mask + minv)
	out[16] = T((in[1]>>48)&mask + minv)
	out[17] = T((in[1]>>55)&mask + minv)
	out[18] = T((in[1]>>62)&mask |
		(in[2]<<2)&mask + minv)
	out[19] = T((in[2]>>5)&mask + minv)
	out[20] = T((in[2]>>12)&mask + minv)
	out[21] = T((in[2]>>19)&mask + minv)
	out[22] = T((in[2]>>26)&mask + minv)
	out[23] = T((in[2]>>33)&mask + minv)
	out[24] = T((in[2]>>40)&mask + minv)
	out[25] = T((in[2]>>47)&mask + minv)
	out[26] = T((in[2]>>54)&mask + minv)
	out[27] = T((in[2]>>61)&mask |
		(in[3]<<3)&mask + minv)
	out[28] = T((in[3]>>4)&mask + minv)
	out[29] = T((in[3]>>11)&mask + minv)
	out[30] = T((in[3]>>18)&mask + minv)
	out[31] = T((in[3]>>25)&mask + minv)
	out[32] = T((in[3]>>32)&mask + minv)
	out[33] = T((in[3]>>39)&mask + minv)
	out[34] = T((in[3]>>46)&mask + minv)
	out[35] = T((in[3]>>53)&mask + minv)
	out[36] = T((in[3]>>60)&mask |
		(in[4]<<4)&mask + minv)
	out[37] = T((in[4]>>3)&mask + minv)
	out[38] = T((in[4]>>10)&mask + minv)
	out[39] = T((in[4]>>17)&mask + minv)
	out[40] = T((in[4]>>24)&mask + minv)
	out[41] = T((in[4]>>31)&mask + minv)
	out[42] = T((in[4]>>38)&mask + minv)
	out[43] = T((in[4]>>45)&mask + minv)
	out[44] = T((in[4]>>52)&mask + minv)
	out[45] = T((in[4]>>59)&mask |
		(in[5]<<5)&mask + minv)
	out[46] = T((in[5]>>2)&mask + minv)
	out[47] = T((in[5]>>9)&mask + minv)
	out[48] = T((in[5]>>16)&mask + minv)
	out[49] = T((in[5]>>23)&mask + minv)
	out[50] = T((in[5]>>30)&mask + minv)
	out[51] = T((in[5]>>37)&mask + minv)
	out[52] = T((in[5]>>44)&mask + minv)
	out[53] = T((in[5]>>51)&mask + minv)
	out[54] = T((in[5]>>58)&mask |
		(in[6]<<6)&mask + minv)
	out[55] = T((in[6]>>1)&mask + minv)
	out[56] = T((in[6]>>8)&mask + minv)
	out[57] = T((in[6]>>15)&mask + minv)
	out[58] = T((in[6]>>22)&mask + minv)
	out[59] = T((in[6]>>29)&mask + minv)
	out[60] = T((in[6]>>36)&mask + minv)
	out[61] = T((in[6]>>43)&mask + minv)
	out[62] = T((in[6]>>50)&mask + minv)
	out[63] = T((in[6]>>57)&mask + minv)

}
func br32_8[T uint32 | int32](out *[64]T, in *[8]uint64, minv uint64) {
	mask := uint64((1 << 8) - 1)
	out[0] = T((in[0]>>0)&mask + minv)
	out[1] = T((in[0]>>8)&mask + minv)
	out[2] = T((in[0]>>16)&mask + minv)
	out[3] = T((in[0]>>24)&mask + minv)
	out[4] = T((in[0]>>32)&mask + minv)
	out[5] = T((in[0]>>40)&mask + minv)
	out[6] = T((in[0]>>48)&mask + minv)
	out[7] = T((in[0]>>56)&mask + minv)
	out[8] = T((in[1]>>0)&mask + minv)
	out[9] = T((in[1]>>8)&mask + minv)
	out[10] = T((in[1]>>16)&mask + minv)
	out[11] = T((in[1]>>24)&mask + minv)
	out[12] = T((in[1]>>32)&mask + minv)
	out[13] = T((in[1]>>40)&mask + minv)
	out[14] = T((in[1]>>48)&mask + minv)
	out[15] = T((in[1]>>56)&mask + minv)
	out[16] = T((in[2]>>0)&mask + minv)
	out[17] = T((in[2]>>8)&mask + minv)
	out[18] = T((in[2]>>16)&mask + minv)
	out[19] = T((in[2]>>24)&mask + minv)
	out[20] = T((in[2]>>32)&mask + minv)
	out[21] = T((in[2]>>40)&mask + minv)
	out[22] = T((in[2]>>48)&mask + minv)
	out[23] = T((in[2]>>56)&mask + minv)
	out[24] = T((in[3]>>0)&mask + minv)
	out[25] = T((in[3]>>8)&mask + minv)
	out[26] = T((in[3]>>16)&mask + minv)
	out[27] = T((in[3]>>24)&mask + minv)
	out[28] = T((in[3]>>32)&mask + minv)
	out[29] = T((in[3]>>40)&mask + minv)
	out[30] = T((in[3]>>48)&mask + minv)
	out[31] = T((in[3]>>56)&mask + minv)
	out[32] = T((in[4]>>0)&mask + minv)
	out[33] = T((in[4]>>8)&mask + minv)
	out[34] = T((in[4]>>16)&mask + minv)
	out[35] = T((in[4]>>24)&mask + minv)
	out[36] = T((in[4]>>32)&mask + minv)
	out[37] = T((in[4]>>40)&mask + minv)
	out[38] = T((in[4]>>48)&mask + minv)
	out[39] = T((in[4]>>56)&mask + minv)
	out[40] = T((in[5]>>0)&mask + minv)
	out[41] = T((in[5]>>8)&mask + minv)
	out[42] = T((in[5]>>16)&mask + minv)
	out[43] = T((in[5]>>24)&mask + minv)
	out[44] = T((in[5]>>32)&mask + minv)
	out[45] = T((in[5]>>40)&mask + minv)
	out[46] = T((in[5]>>48)&mask + minv)
	out[47] = T((in[5]>>56)&mask + minv)
	out[48] = T((in[6]>>0)&mask + minv)
	out[49] = T((in[6]>>8)&mask + minv)
	out[50] = T((in[6]>>16)&mask + minv)
	out[51] = T((in[6]>>24)&mask + minv)
	out[52] = T((in[6]>>32)&mask + minv)
	out[53] = T((in[6]>>40)&mask + minv)
	out[54] = T((in[6]>>48)&mask + minv)
	out[55] = T((in[6]>>56)&mask + minv)
	out[56] = T((in[7]>>0)&mask + minv)
	out[57] = T((in[7]>>8)&mask + minv)
	out[58] = T((in[7]>>16)&mask + minv)
	out[59] = T((in[7]>>24)&mask + minv)
	out[60] = T((in[7]>>32)&mask + minv)
	out[61] = T((in[7]>>40)&mask + minv)
	out[62] = T((in[7]>>48)&mask + minv)
	out[63] = T((in[7]>>56)&mask + minv)

}
func br32_9[T uint32 | int32](out *[64]T, in *[9]uint64, minv uint64) {
	mask := uint64((1 << 9) - 1)
	out[0] = T((in[0]>>0)&mask + minv)
	out[1] = T((in[0]>>9)&mask + minv)
	out[2] = T((in[0]>>18)&mask + minv)
	out[3] = T((in[0]>>27)&mask + minv)
	out[4] = T((in[0]>>36)&mask + minv)
	out[5] = T((in[0]>>45)&mask + minv)
	out[6] = T((in[0]>>54)&mask + minv)
	out[7] = T((in[0]>>63)&mask |
		(in[1]<<1)&mask + minv)
	out[8] = T((in[1]>>8)&mask + minv)
	out[9] = T((in[1]>>17)&mask + minv)
	out[10] = T((in[1]>>26)&mask + minv)
	out[11] = T((in[1]>>35)&mask + minv)
	out[12] = T((in[1]>>44)&mask + minv)
	out[13] = T((in[1]>>53)&mask + minv)
	out[14] = T((in[1]>>62)&mask |
		(in[2]<<2)&mask + minv)
	out[15] = T((in[2]>>7)&mask + minv)
	out[16] = T((in[2]>>16)&mask + minv)
	out[17] = T((in[2]>>25)&mask + minv)
	out[18] = T((in[2]>>34)&mask + minv)
	out[19] = T((in[2]>>43)&mask + minv)
	out[20] = T((in[2]>>52)&mask + minv)
	out[21] = T((in[2]>>61)&mask |
		(in[3]<<3)&mask + minv)
	out[22] = T((in[3]>>6)&mask + minv)
	out[23] = T((in[3]>>15)&mask + minv)
	out[24] = T((in[3]>>24)&mask + minv)
	out[25] = T((in[3]>>33)&mask + minv)
	out[26] = T((in[3]>>42)&mask + minv)
	out[27] = T((in[3]>>51)&mask + minv)
	out[28] = T((in[3]>>60)&mask |
		(in[4]<<4)&mask + minv)
	out[29] = T((in[4]>>5)&mask + minv)
	out[30] = T((in[4]>>14)&mask + minv)
	out[31] = T((in[4]>>23)&mask + minv)
	out[32] = T((in[4]>>32)&mask + minv)
	out[33] = T((in[4]>>41)&mask + minv)
	out[34] = T((in[4]>>50)&mask + minv)
	out[35] = T((in[4]>>59)&mask |
		(in[5]<<5)&mask + minv)
	out[36] = T((in[5]>>4)&mask + minv)
	out[37] = T((in[5]>>13)&mask + minv)
	out[38] = T((in[5]>>22)&mask + minv)
	out[39] = T((in[5]>>31)&mask + minv)
	out[40] = T((in[5]>>40)&mask + minv)
	out[41] = T((in[5]>>49)&mask + minv)
	out[42] = T((in[5]>>58)&mask |
		(in[6]<<6)&mask + minv)
	out[43] = T((in[6]>>3)&mask + minv)
	out[44] = T((in[6]>>12)&mask + minv)
	out[45] = T((in[6]>>21)&mask + minv)
	out[46] = T((in[6]>>30)&mask + minv)
	out[47] = T((in[6]>>39)&mask + minv)
	out[48] = T((in[6]>>48)&mask + minv)
	out[49] = T((in[6]>>57)&mask |
		(in[7]<<7)&mask + minv)
	out[50] = T((in[7]>>2)&mask + minv)
	out[51] = T((in[7]>>11)&mask + minv)
	out[52] = T((in[7]>>20)&mask + minv)
	out[53] = T((in[7]>>29)&mask + minv)
	out[54] = T((in[7]>>38)&mask + minv)
	out[55] = T((in[7]>>47)&mask + minv)
	out[56] = T((in[7]>>56)&mask |
		(in[8]<<8)&mask + minv)
	out[57] = T((in[8]>>1)&mask + minv)
	out[58] = T((in[8]>>10)&mask + minv)
	out[59] = T((in[8]>>19)&mask + minv)
	out[60] = T((in[8]>>28)&mask + minv)
	out[61] = T((in[8]>>37)&mask + minv)
	out[62] = T((in[8]>>46)&mask + minv)
	out[63] = T((in[8]>>55)&mask + minv)

}
func br32_10[T uint32 | int32](out *[64]T, in *[10]uint64, minv uint64) {
	mask := uint64((1 << 10) - 1)
	out[0] = T((in[0]>>0)&mask + minv)
	out[1] = T((in[0]>>10)&mask + minv)
	out[2] = T((in[0]>>20)&mask + minv)
	out[3] = T((in[0]>>30)&mask + minv)
	out[4] = T((in[0]>>40)&mask + minv)
	out[5] = T((in[0]>>50)&mask + minv)
	out[6] = T((in[0]>>60)&mask |
		(in[1]<<4)&mask + minv)
	out[7] = T((in[1]>>6)&mask + minv)
	out[8] = T((in[1]>>16)&mask + minv)
	out[9] = T((in[1]>>26)&mask + minv)
	out[10] = T((in[1]>>36)&mask + minv)
	out[11] = T((in[1]>>46)&mask + minv)
	out[12] = T((in[1]>>56)&mask |
		(in[2]<<8)&mask + minv)
	out[13] = T((in[2]>>2)&mask + minv)
	out[14] = T((in[2]>>12)&mask + minv)
	out[15] = T((in[2]>>22)&mask + minv)
	out[16] = T((in[2]>>32)&mask + minv)
	out[17] = T((in[2]>>42)&mask + minv)
	out[18] = T((in[2]>>52)&mask + minv)
	out[19] = T((in[2]>>62)&mask |
		(in[3]<<2)&mask + minv)
	out[20] = T((in[3]>>8)&mask + minv)
	out[21] = T((in[3]>>18)&mask + minv)
	out[22] = T((in[3]>>28)&mask + minv)
	out[23] = T((in[3]>>38)&mask + minv)
	out[24] = T((in[3]>>48)&mask + minv)
	out[25] = T((in[3]>>58)&mask |
		(in[4]<<6)&mask + minv)
	out[26] = T((in[4]>>4)&mask + minv)
	out[27] = T((in[4]>>14)&mask + minv)
	out[28] = T((in[4]>>24)&mask + minv)
	out[29] = T((in[4]>>34)&mask + minv)
	out[30] = T((in[4]>>44)&mask + minv)
	out[31] = T((in[4]>>54)&mask + minv)
	out[32] = T((in[5]>>0)&mask + minv)
	out[33] = T((in[5]>>10)&mask + minv)
	out[34] = T((in[5]>>20)&mask + minv)
	out[35] = T((in[5]>>30)&mask + minv)
	out[36] = T((in[5]>>40)&mask + minv)
	out[37] = T((in[5]>>50)&mask + minv)
	out[38] = T((in[5]>>60)&mask |
		(in[6]<<4)&mask + minv)
	out[39] = T((in[6]>>6)&mask + minv)
	out[40] = T((in[6]>>16)&mask + minv)
	out[41] = T((in[6]>>26)&mask + minv)
	out[42] = T((in[6]>>36)&mask + minv)
	out[43] = T((in[6]>>46)&mask + minv)
	out[44] = T((in[6]>>56)&mask |
		(in[7]<<8)&mask + minv)
	out[45] = T((in[7]>>2)&mask + minv)
	out[46] = T((in[7]>>12)&mask + minv)
	out[47] = T((in[7]>>22)&mask + minv)
	out[48] = T((in[7]>>32)&mask + minv)
	out[49] = T((in[7]>>42)&mask + minv)
	out[50] = T((in[7]>>52)&mask + minv)
	out[51] = T((in[7]>>62)&mask |
		(in[8]<<2)&mask + minv)
	out[52] = T((in[8]>>8)&mask + minv)
	out[53] = T((in[8]>>18)&mask + minv)
	out[54] = T((in[8]>>28)&mask + minv)
	out[55] = T((in[8]>>38)&mask + minv)
	out[56] = T((in[8]>>48)&mask + minv)
	out[57] = T((in[8]>>58)&mask |
		(in[9]<<6)&mask + minv)
	out[58] = T((in[9]>>4)&mask + minv)
	out[59] = T((in[9]>>14)&mask + minv)
	out[60] = T((in[9]>>24)&mask + minv)
	out[61] = T((in[9]>>34)&mask + minv)
	out[62] = T((in[9]>>44)&mask + minv)
	out[63] = T((in[9]>>54)&mask + minv)

}
func br32_11[T uint32 | int32](out *[64]T, in *[11]uint64, minv uint64) {
	mask := uint64((1 << 11) - 1)
	out[0] = T((in[0]>>0)&mask + minv)
	out[1] = T((in[0]>>11)&mask + minv)
	out[2] = T((in[0]>>22)&mask + minv)
	out[3] = T((in[0]>>33)&mask + minv)
	out[4] = T((in[0]>>44)&mask + minv)
	out[5] = T((in[0]>>55)&mask |
		(in[1]<<9)&mask + minv)
	out[6] = T((in[1]>>2)&mask + minv)
	out[7] = T((in[1]>>13)&mask + minv)
	out[8] = T((in[1]>>24)&mask + minv)
	out[9] = T((in[1]>>35)&mask + minv)
	out[10] = T((in[1]>>46)&mask + minv)
	out[11] = T((in[1]>>57)&mask |
		(in[2]<<7)&mask + minv)
	out[12] = T((in[2]>>4)&mask + minv)
	out[13] = T((in[2]>>15)&mask + minv)
	out[14] = T((in[2]>>26)&mask + minv)
	out[15] = T((in[2]>>37)&mask + minv)
	out[16] = T((in[2]>>48)&mask + minv)
	out[17] = T((in[2]>>59)&mask |
		(in[3]<<5)&mask + minv)
	out[18] = T((in[3]>>6)&mask + minv)
	out[19] = T((in[3]>>17)&mask + minv)
	out[20] = T((in[3]>>28)&mask + minv)
	out[21] = T((in[3]>>39)&mask + minv)
	out[22] = T((in[3]>>50)&mask + minv)
	out[23] = T((in[3]>>61)&mask |
		(in[4]<<3)&mask + minv)
	out[24] = T((in[4]>>8)&mask + minv)
	out[25] = T((in[4]>>19)&mask + minv)
	out[26] = T((in[4]>>30)&mask + minv)
	out[27] = T((in[4]>>41)&mask + minv)
	out[28] = T((in[4]>>52)&mask + minv)
	out[29] = T((in[4]>>63)&mask |
		(in[5]<<1)&mask + minv)
	out[30] = T((in[5]>>10)&mask + minv)
	out[31] = T((in[5]>>21)&mask + minv)
	out[32] = T((in[5]>>32)&mask + minv)
	out[33] = T((in[5]>>43)&mask + minv)
	out[34] = T((in[5]>>54)&mask |
		(in[6]<<10)&mask + minv)
	out[35] = T((in[6]>>1)&mask + minv)
	out[36] = T((in[6]>>12)&mask + minv)
	out[37] = T((in[6]>>23)&mask + minv)
	out[38] = T((in[6]>>34)&mask + minv)
	out[39] = T((in[6]>>45)&mask + minv)
	out[40] = T((in[6]>>56)&mask |
		(in[7]<<8)&mask + minv)
	out[41] = T((in[7]>>3)&mask + minv)
	out[42] = T((in[7]>>14)&mask + minv)
	out[43] = T((in[7]>>25)&mask + minv)
	out[44] = T((in[7]>>36)&mask + minv)
	out[45] = T((in[7]>>47)&mask + minv)
	out[46] = T((in[7]>>58)&mask |
		(in[8]<<6)&mask + minv)
	out[47] = T((in[8]>>5)&mask + minv)
	out[48] = T((in[8]>>16)&mask + minv)
	out[49] = T((in[8]>>27)&mask + minv)
	out[50] = T((in[8]>>38)&mask + minv)
	out[51] = T((in[8]>>49)&mask + minv)
	out[52] = T((in[8]>>60)&mask |
		(in[9]<<4)&mask + minv)
	out[53] = T((in[9]>>7)&mask + minv)
	out[54] = T((in[9]>>18)&mask + minv)
	out[55] = T((in[9]>>29)&mask + minv)
	out[56] = T((in[9]>>40)&mask + minv)
	out[57] = T((in[9]>>51)&mask + minv)
	out[58] = T((in[9]>>62)&mask |
		(in[10]<<2)&mask + minv)
	out[59] = T((in[10]>>9)&mask + minv)
	out[60] = T((in[10]>>20)&mask + minv)
	out[61] = T((in[10]>>31)&mask + minv)
	out[62] = T((in[10]>>42)&mask + minv)
	out[63] = T((in[10]>>53)&mask + minv)

}
func br32_12[T uint32 | int32](out *[64]T, in *[12]uint64, minv uint64) {
	mask := uint64((1 << 12) - 1)
	out[0] = T((in[0]>>0)&mask + minv)
	out[1] = T((in[0]>>12)&mask + minv)
	out[2] = T((in[0]>>24)&mask + minv)
	out[3] = T((in[0]>>36)&mask + minv)
	out[4] = T((in[0]>>48)&mask + minv)
	out[5] = T((in[0]>>60)&mask |
		(in[1]<<4)&mask + minv)
	out[6] = T((in[1]>>8)&mask + minv)
	out[7] = T((in[1]>>20)&mask + minv)
	out[8] = T((in[1]>>32)&mask + minv)
	out[9] = T((in[1]>>44)&mask + minv)
	out[10] = T((in[1]>>56)&mask |
		(in[2]<<8)&mask + minv)
	out[11] = T((in[2]>>4)&mask + minv)
	out[12] = T((in[2]>>16)&mask + minv)
	out[13] = T((in[2]>>28)&mask + minv)
	out[14] = T((in[2]>>40)&mask + minv)
	out[15] = T((in[2]>>52)&mask + minv)
	out[16] = T((in[3]>>0)&mask + minv)
	out[17] = T((in[3]>>12)&mask + minv)
	out[18] = T((in[3]>>24)&mask + minv)
	out[19] = T((in[3]>>36)&mask + minv)
	out[20] = T((in[3]>>48)&mask + minv)
	out[21] = T((in[3]>>60)&mask |
		(in[4]<<4)&mask + minv)
	out[22] = T((in[4]>>8)&mask + minv)
	out[23] = T((in[4]>>20)&mask + minv)
	out[24] = T((in[4]>>32)&mask + minv)
	out[25] = T((in[4]>>44)&mask + minv)
	out[26] = T((in[4]>>56)&mask |
		(in[5]<<8)&mask + minv)
	out[27] = T((in[5]>>4)&mask + minv)
	out[28] = T((in[5]>>16)&mask + minv)
	out[29] = T((in[5]>>28)&mask + minv)
	out[30] = T((in[5]>>40)&mask + minv)
	out[31] = T((in[5]>>52)&mask + minv)
	out[32] = T((in[6]>>0)&mask + minv)
	out[33] = T((in[6]>>12)&mask + minv)
	out[34] = T((in[6]>>24)&mask + minv)
	out[35] = T((in[6]>>36)&mask + minv)
	out[36] = T((in[6]>>48)&mask + minv)
	out[37] = T((in[6]>>60)&mask |
		(in[7]<<4)&mask + minv)
	out[38] = T((in[7]>>8)&mask + minv)
	out[39] = T((in[7]>>20)&mask + minv)
	out[40] = T((in[7]>>32)&mask + minv)
	out[41] = T((in[7]>>44)&mask + minv)
	out[42] = T((in[7]>>56)&mask |
		(in[8]<<8)&mask + minv)
	out[43] = T((in[8]>>4)&mask + minv)
	out[44] = T((in[8]>>16)&mask + minv)
	out[45] = T((in[8]>>28)&mask + minv)
	out[46] = T((in[8]>>40)&mask + minv)
	out[47] = T((in[8]>>52)&mask + minv)
	out[48] = T((in[9]>>0)&mask + minv)
	out[49] = T((in[9]>>12)&mask + minv)
	out[50] = T((in[9]>>24)&mask + minv)
	out[51] = T((in[9]>>36)&mask + minv)
	out[52] = T((in[9]>>48)&mask + minv)
	out[53] = T((in[9]>>60)&mask |
		(in[10]<<4)&mask + minv)
	out[54] = T((in[10]>>8)&mask + minv)
	out[55] = T((in[10]>>20)&mask + minv)
	out[56] = T((in[10]>>32)&mask + minv)
	out[57] = T((in[10]>>44)&mask + minv)
	out[58] = T((in[10]>>56)&mask |
		(in[11]<<8)&mask + minv)
	out[59] = T((in[11]>>4)&mask + minv)
	out[60] = T((in[11]>>16)&mask + minv)
	out[61] = T((in[11]>>28)&mask + minv)
	out[62] = T((in[11]>>40)&mask + minv)
	out[63] = T((in[11]>>52)&mask + minv)

}
func br32_13[T uint32 | int32](out *[64]T, in *[13]uint64, minv uint64) {
	mask := uint64((1 << 13) - 1)
	out[0] = T((in[0]>>0)&mask + minv)
	out[1] = T((in[0]>>13)&mask + minv)
	out[2] = T((in[0]>>26)&mask + minv)
	out[3] = T((in[0]>>39)&mask + minv)
	out[4] = T((in[0]>>52)&mask |
		(in[1]<<12)&mask + minv)
	out[5] = T((in[1]>>1)&mask + minv)
	out[6] = T((in[1]>>14)&mask + minv)
	out[7] = T((in[1]>>27)&mask + minv)
	out[8] = T((in[1]>>40)&mask + minv)
	out[9] = T((in[1]>>53)&mask |
		(in[2]<<11)&mask + minv)
	out[10] = T((in[2]>>2)&mask + minv)
	out[11] = T((in[2]>>15)&mask + minv)
	out[12] = T((in[2]>>28)&mask + minv)
	out[13] = T((in[2]>>41)&mask + minv)
	out[14] = T((in[2]>>54)&mask |
		(in[3]<<10)&mask + minv)
	out[15] = T((in[3]>>3)&mask + minv)
	out[16] = T((in[3]>>16)&mask + minv)
	out[17] = T((in[3]>>29)&mask + minv)
	out[18] = T((in[3]>>42)&mask + minv)
	out[19] = T((in[3]>>55)&mask |
		(in[4]<<9)&mask + minv)
	out[20] = T((in[4]>>4)&mask + minv)
	out[21] = T((in[4]>>17)&mask + minv)
	out[22] = T((in[4]>>30)&mask + minv)
	out[23] = T((in[4]>>43)&mask + minv)
	out[24] = T((in[4]>>56)&mask |
		(in[5]<<8)&mask + minv)
	out[25] = T((in[5]>>5)&mask + minv)
	out[26] = T((in[5]>>18)&mask + minv)
	out[27] = T((in[5]>>31)&mask + minv)
	out[28] = T((in[5]>>44)&mask + minv)
	out[29] = T((in[5]>>57)&mask |
		(in[6]<<7)&mask + minv)
	out[30] = T((in[6]>>6)&mask + minv)
	out[31] = T((in[6]>>19)&mask + minv)
	out[32] = T((in[6]>>32)&mask + minv)
	out[33] = T((in[6]>>45)&mask + minv)
	out[34] = T((in[6]>>58)&mask |
		(in[7]<<6)&mask + minv)
	out[35] = T((in[7]>>7)&mask + minv)
	out[36] = T((in[7]>>20)&mask + minv)
	out[37] = T((in[7]>>33)&mask + minv)
	out[38] = T((in[7]>>46)&mask + minv)
	out[39] = T((in[7]>>59)&mask |
		(in[8]<<5)&mask + minv)
	out[40] = T((in[8]>>8)&mask + minv)
	out[41] = T((in[8]>>21)&mask + minv)
	out[42] = T((in[8]>>34)&mask + minv)
	out[43] = T((in[8]>>47)&mask + minv)
	out[44] = T((in[8]>>60)&mask |
		(in[9]<<4)&mask + minv)
	out[45] = T((in[9]>>9)&mask + minv)
	out[46] = T((in[9]>>22)&mask + minv)
	out[47] = T((in[9]>>35)&mask + minv)
	out[48] = T((in[9]>>48)&mask + minv)
	out[49] = T((in[9]>>61)&mask |
		(in[10]<<3)&mask + minv)
	out[50] = T((in[10]>>10)&mask + minv)
	out[51] = T((in[10]>>23)&mask + minv)
	out[52] = T((in[10]>>36)&mask + minv)
	out[53] = T((in[10]>>49)&mask + minv)
	out[54] = T((in[10]>>62)&mask |
		(in[11]<<2)&mask + minv)
	out[55] = T((in[11]>>11)&mask + minv)
	out[56] = T((in[11]>>24)&mask + minv)
	out[57] = T((in[11]>>37)&mask + minv)
	out[58] = T((in[11]>>50)&mask + minv)
	out[59] = T((in[11]>>63)&mask |
		(in[12]<<1)&mask + minv)
	out[60] = T((in[12]>>12)&mask + minv)
	out[61] = T((in[12]>>25)&mask + minv)
	out[62] = T((in[12]>>38)&mask + minv)
	out[63] = T((in[12]>>51)&mask + minv)

}
func br32_14[T uint32 | int32](out *[64]T, in *[14]uint64, minv uint64) {
	mask := uint64((1 << 14) - 1)
	out[0] = T((in[0]>>0)&mask + minv)
	out[1] = T((in[0]>>14)&mask + minv)
	out[2] = T((in[0]>>28)&mask + minv)
	out[3] = T((in[0]>>42)&mask + minv)
	out[4] = T((in[0]>>56)&mask |
		(in[1]<<8)&mask + minv)
	out[5] = T((in[1]>>6)&mask + minv)
	out[6] = T((in[1]>>20)&mask + minv)
	out[7] = T((in[1]>>34)&mask + minv)
	out[8] = T((in[1]>>48)&mask + minv)
	out[9] = T((in[1]>>62)&mask |
		(in[2]<<2)&mask + minv)
	out[10] = T((in[2]>>12)&mask + minv)
	out[11] = T((in[2]>>26)&mask + minv)
	out[12] = T((in[2]>>40)&mask + minv)
	out[13] = T((in[2]>>54)&mask |
		(in[3]<<10)&mask + minv)
	out[14] = T((in[3]>>4)&mask + minv)
	out[15] = T((in[3]>>18)&mask + minv)
	out[16] = T((in[3]>>32)&mask + minv)
	out[17] = T((in[3]>>46)&mask + minv)
	out[18] = T((in[3]>>60)&mask |
		(in[4]<<4)&mask + minv)
	out[19] = T((in[4]>>10)&mask + minv)
	out[20] = T((in[4]>>24)&mask + minv)
	out[21] = T((in[4]>>38)&mask + minv)
	out[22] = T((in[4]>>52)&mask |
		(in[5]<<12)&mask + minv)
	out[23] = T((in[5]>>2)&mask + minv)
	out[24] = T((in[5]>>16)&mask + minv)
	out[25] = T((in[5]>>30)&mask + minv)
	out[26] = T((in[5]>>44)&mask + minv)
	out[27] = T((in[5]>>58)&mask |
		(in[6]<<6)&mask + minv)
	out[28] = T((in[6]>>8)&mask + minv)
	out[29] = T((in[6]>>22)&mask + minv)
	out[30] = T((in[6]>>36)&mask + minv)
	out[31] = T((in[6]>>50)&mask + minv)
	out[32] = T((in[7]>>0)&mask + minv)
	out[33] = T((in[7]>>14)&mask + minv)
	out[34] = T((in[7]>>28)&mask + minv)
	out[35] = T((in[7]>>42)&mask + minv)
	out[36] = T((in[7]>>56)&mask |
		(in[8]<<8)&mask + minv)
	out[37] = T((in[8]>>6)&mask + minv)
	out[38] = T((in[8]>>20)&mask + minv)
	out[39] = T((in[8]>>34)&mask + minv)
	out[40] = T((in[8]>>48)&mask + minv)
	out[41] = T((in[8]>>62)&mask |
		(in[9]<<2)&mask + minv)
	out[42] = T((in[9]>>12)&mask + minv)
	out[43] = T((in[9]>>26)&mask + minv)
	out[44] = T((in[9]>>40)&mask + minv)
	out[45] = T((in[9]>>54)&mask |
		(in[10]<<10)&mask + minv)
	out[46] = T((in[10]>>4)&mask + minv)
	out[47] = T((in[10]>>18)&mask + minv)
	out[48] = T((in[10]>>32)&mask + minv)
	out[49] = T((in[10]>>46)&mask + minv)
	out[50] = T((in[10]>>60)&mask |
		(in[11]<<4)&mask + minv)
	out[51] = T((in[11]>>10)&mask + minv)
	out[52] = T((in[11]>>24)&mask + minv)
	out[53] = T((in[11]>>38)&mask + minv)
	out[54] = T((in[11]>>52)&mask |
		(in[12]<<12)&mask + minv)
	out[55] = T((in[12]>>2)&mask + minv)
	out[56] = T((in[12]>>16)&mask + minv)
	out[57] = T((in[12]>>30)&mask + minv)
	out[58] = T((in[12]>>44)&mask + minv)
	out[59] = T((in[12]>>58)&mask |
		(in[13]<<6)&mask + minv)
	out[60] = T((in[13]>>8)&mask + minv)
	out[61] = T((in[13]>>22)&mask + minv)
	out[62] = T((in[13]>>36)&mask + minv)
	out[63] = T((in[13]>>50)&mask + minv)

}
func br32_15[T uint32 | int32](out *[64]T, in *[15]uint64, minv uint64) {
	mask := uint64((1 << 15) - 1)
	out[0] = T((in[0]>>0)&mask + minv)
	out[1] = T((in[0]>>15)&mask + minv)
	out[2] = T((in[0]>>30)&mask + minv)
	out[3] = T((in[0]>>45)&mask + minv)
	out[4] = T((in[0]>>60)&mask |
		(in[1]<<4)&mask + minv)
	out[5] = T((in[1]>>11)&mask + minv)
	out[6] = T((in[1]>>26)&mask + minv)
	out[7] = T((in[1]>>41)&mask + minv)
	out[8] = T((in[1]>>56)&mask |
		(in[2]<<8)&mask + minv)
	out[9] = T((in[2]>>7)&mask + minv)
	out[10] = T((in[2]>>22)&mask + minv)
	out[11] = T((in[2]>>37)&mask + minv)
	out[12] = T((in[2]>>52)&mask |
		(in[3]<<12)&mask + minv)
	out[13] = T((in[3]>>3)&mask + minv)
	out[14] = T((in[3]>>18)&mask + minv)
	out[15] = T((in[3]>>33)&mask + minv)
	out[16] = T((in[3]>>48)&mask + minv)
	out[17] = T((in[3]>>63)&mask |
		(in[4]<<1)&mask + minv)
	out[18] = T((in[4]>>14)&mask + minv)
	out[19] = T((in[4]>>29)&mask + minv)
	out[20] = T((in[4]>>44)&mask + minv)
	out[21] = T((in[4]>>59)&mask |
		(in[5]<<5)&mask + minv)
	out[22] = T((in[5]>>10)&mask + minv)
	out[23] = T((in[5]>>25)&mask + minv)
	out[24] = T((in[5]>>40)&mask + minv)
	out[25] = T((in[5]>>55)&mask |
		(in[6]<<9)&mask + minv)
	out[26] = T((in[6]>>6)&mask + minv)
	out[27] = T((in[6]>>21)&mask + minv)
	out[28] = T((in[6]>>36)&mask + minv)
	out[29] = T((in[6]>>51)&mask |
		(in[7]<<13)&mask + minv)
	out[30] = T((in[7]>>2)&mask + minv)
	out[31] = T((in[7]>>17)&mask + minv)
	out[32] = T((in[7]>>32)&mask + minv)
	out[33] = T((in[7]>>47)&mask + minv)
	out[34] = T((in[7]>>62)&mask |
		(in[8]<<2)&mask + minv)
	out[35] = T((in[8]>>13)&mask + minv)
	out[36] = T((in[8]>>28)&mask + minv)
	out[37] = T((in[8]>>43)&mask + minv)
	out[38] = T((in[8]>>58)&mask |
		(in[9]<<6)&mask + minv)
	out[39] = T((in[9]>>9)&mask + minv)
	out[40] = T((in[9]>>24)&mask + minv)
	out[41] = T((in[9]>>39)&mask + minv)
	out[42] = T((in[9]>>54)&mask |
		(in[10]<<10)&mask + minv)
	out[43] = T((in[10]>>5)&mask + minv)
	out[44] = T((in[10]>>20)&mask + minv)
	out[45] = T((in[10]>>35)&mask + minv)
	out[46] = T((in[10]>>50)&mask |
		(in[11]<<14)&mask + minv)
	out[47] = T((in[11]>>1)&mask + minv)
	out[48] = T((in[11]>>16)&mask + minv)
	out[49] = T((in[11]>>31)&mask + minv)
	out[50] = T((in[11]>>46)&mask + minv)
	out[51] = T((in[11]>>61)&mask |
		(in[12]<<3)&mask + minv)
	out[52] = T((in[12]>>12)&mask + minv)
	out[53] = T((in[12]>>27)&mask + minv)
	out[54] = T((in[12]>>42)&mask + minv)
	out[55] = T((in[12]>>57)&mask |
		(in[13]<<7)&mask + minv)
	out[56] = T((in[13]>>8)&mask + minv)
	out[57] = T((in[13]>>23)&mask + minv)
	out[58] = T((in[13]>>38)&mask + minv)
	out[59] = T((in[13]>>53)&mask |
		(in[14]<<11)&mask + minv)
	out[60] = T((in[14]>>4)&mask + minv)
	out[61] = T((in[14]>>19)&mask + minv)
	out[62] = T((in[14]>>34)&mask + minv)
	out[63] = T((in[14]>>49)&mask + minv)

}
func br32_16[T uint32 | int32](out *[64]T, in *[16]uint64, minv uint64) {
	mask := uint64((1 << 16) - 1)
	out[0] = T((in[0]>>0)&mask + minv)
	out[1] = T((in[0]>>16)&mask + minv)
	out[2] = T((in[0]>>32)&mask + minv)
	out[3] = T((in[0]>>48)&mask + minv)
	out[4] = T((in[1]>>0)&mask + minv)
	out[5] = T((in[1]>>16)&mask + minv)
	out[6] = T((in[1]>>32)&mask + minv)
	out[7] = T((in[1]>>48)&mask + minv)
	out[8] = T((in[2]>>0)&mask + minv)
	out[9] = T((in[2]>>16)&mask + minv)
	out[10] = T((in[2]>>32)&mask + minv)
	out[11] = T((in[2]>>48)&mask + minv)
	out[12] = T((in[3]>>0)&mask + minv)
	out[13] = T((in[3]>>16)&mask + minv)
	out[14] = T((in[3]>>32)&mask + minv)
	out[15] = T((in[3]>>48)&mask + minv)
	out[16] = T((in[4]>>0)&mask + minv)
	out[17] = T((in[4]>>16)&mask + minv)
	out[18] = T((in[4]>>32)&mask + minv)
	out[19] = T((in[4]>>48)&mask + minv)
	out[20] = T((in[5]>>0)&mask + minv)
	out[21] = T((in[5]>>16)&mask + minv)
	out[22] = T((in[5]>>32)&mask + minv)
	out[23] = T((in[5]>>48)&mask + minv)
	out[24] = T((in[6]>>0)&mask + minv)
	out[25] = T((in[6]>>16)&mask + minv)
	out[26] = T((in[6]>>32)&mask + minv)
	out[27] = T((in[6]>>48)&mask + minv)
	out[28] = T((in[7]>>0)&mask + minv)
	out[29] = T((in[7]>>16)&mask + minv)
	out[30] = T((in[7]>>32)&mask + minv)
	out[31] = T((in[7]>>48)&mask + minv)
	out[32] = T((in[8]>>0)&mask + minv)
	out[33] = T((in[8]>>16)&mask + minv)
	out[34] = T((in[8]>>32)&mask + minv)
	out[35] = T((in[8]>>48)&mask + minv)
	out[36] = T((in[9]>>0)&mask + minv)
	out[37] = T((in[9]>>16)&mask + minv)
	out[38] = T((in[9]>>32)&mask + minv)
	out[39] = T((in[9]>>48)&mask + minv)
	out[40] = T((in[10]>>0)&mask + minv)
	out[41] = T((in[10]>>16)&mask + minv)
	out[42] = T((in[10]>>32)&mask + minv)
	out[43] = T((in[10]>>48)&mask + minv)
	out[44] = T((in[11]>>0)&mask + minv)
	out[45] = T((in[11]>>16)&mask + minv)
	out[46] = T((in[11]>>32)&mask + minv)
	out[47] = T((in[11]>>48)&mask + minv)
	out[48] = T((in[12]>>0)&mask + minv)
	out[49] = T((in[12]>>16)&mask + minv)
	out[50] = T((in[12]>>32)&mask + minv)
	out[51] = T((in[12]>>48)&mask + minv)
	out[52] = T((in[13]>>0)&mask + minv)
	out[53] = T((in[13]>>16)&mask + minv)
	out[54] = T((in[13]>>32)&mask + minv)
	out[55] = T((in[13]>>48)&mask + minv)
	out[56] = T((in[14]>>0)&mask + minv)
	out[57] = T((in[14]>>16)&mask + minv)
	out[58] = T((in[14]>>32)&mask + minv)
	out[59] = T((in[14]>>48)&mask + minv)
	out[60] = T((in[15]>>0)&mask + minv)
	out[61] = T((in[15]>>16)&mask + minv)
	out[62] = T((in[15]>>32)&mask + minv)
	out[63] = T((in[15]>>48)&mask + minv)

}
func br32_17[T uint32 | int32](out *[64]T, in *[17]uint64, minv uint64) {
	mask := uint64((1 << 17) - 1)
	out[0] = T((in[0]>>0)&mask + minv)
	out[1] = T((in[0]>>17)&mask + minv)
	out[2] = T((in[0]>>34)&mask + minv)
	out[3] = T((in[0]>>51)&mask |
		(in[1]<<13)&mask + minv)
	out[4] = T((in[1]>>4)&mask + minv)
	out[5] = T((in[1]>>21)&mask + minv)
	out[6] = T((in[1]>>38)&mask + minv)
	out[7] = T((in[1]>>55)&mask |
		(in[2]<<9)&mask + minv)
	out[8] = T((in[2]>>8)&mask + minv)
	out[9] = T((in[2]>>25)&mask + minv)
	out[10] = T((in[2]>>42)&mask + minv)
	out[11] = T((in[2]>>59)&mask |
		(in[3]<<5)&mask + minv)
	out[12] = T((in[3]>>12)&mask + minv)
	out[13] = T((in[3]>>29)&mask + minv)
	out[14] = T((in[3]>>46)&mask + minv)
	out[15] = T((in[3]>>63)&mask |
		(in[4]<<1)&mask + minv)
	out[16] = T((in[4]>>16)&mask + minv)
	out[17] = T((in[4]>>33)&mask + minv)
	out[18] = T((in[4]>>50)&mask |
		(in[5]<<14)&mask + minv)
	out[19] = T((in[5]>>3)&mask + minv)
	out[20] = T((in[5]>>20)&mask + minv)
	out[21] = T((in[5]>>37)&mask + minv)
	out[22] = T((in[5]>>54)&mask |
		(in[6]<<10)&mask + minv)
	out[23] = T((in[6]>>7)&mask + minv)
	out[24] = T((in[6]>>24)&mask + minv)
	out[25] = T((in[6]>>41)&mask + minv)
	out[26] = T((in[6]>>58)&mask |
		(in[7]<<6)&mask + minv)
	out[27] = T((in[7]>>11)&mask + minv)
	out[28] = T((in[7]>>28)&mask + minv)
	out[29] = T((in[7]>>45)&mask + minv)
	out[30] = T((in[7]>>62)&mask |
		(in[8]<<2)&mask + minv)
	out[31] = T((in[8]>>15)&mask + minv)
	out[32] = T((in[8]>>32)&mask + minv)
	out[33] = T((in[8]>>49)&mask |
		(in[9]<<15)&mask + minv)
	out[34] = T((in[9]>>2)&mask + minv)
	out[35] = T((in[9]>>19)&mask + minv)
	out[36] = T((in[9]>>36)&mask + minv)
	out[37] = T((in[9]>>53)&mask |
		(in[10]<<11)&mask + minv)
	out[38] = T((in[10]>>6)&mask + minv)
	out[39] = T((in[10]>>23)&mask + minv)
	out[40] = T((in[10]>>40)&mask + minv)
	out[41] = T((in[10]>>57)&mask |
		(in[11]<<7)&mask + minv)
	out[42] = T((in[11]>>10)&mask + minv)
	out[43] = T((in[11]>>27)&mask + minv)
	out[44] = T((in[11]>>44)&mask + minv)
	out[45] = T((in[11]>>61)&mask |
		(in[12]<<3)&mask + minv)
	out[46] = T((in[12]>>14)&mask + minv)
	out[47] = T((in[12]>>31)&mask + minv)
	out[48] = T((in[12]>>48)&mask |
		(in[13]<<16)&mask + minv)
	out[49] = T((in[13]>>1)&mask + minv)
	out[50] = T((in[13]>>18)&mask + minv)
	out[51] = T((in[13]>>35)&mask + minv)
	out[52] = T((in[13]>>52)&mask |
		(in[14]<<12)&mask + minv)
	out[53] = T((in[14]>>5)&mask + minv)
	out[54] = T((in[14]>>22)&mask + minv)
	out[55] = T((in[14]>>39)&mask + minv)
	out[56] = T((in[14]>>56)&mask |
		(in[15]<<8)&mask + minv)
	out[57] = T((in[15]>>9)&mask + minv)
	out[58] = T((in[15]>>26)&mask + minv)
	out[59] = T((in[15]>>43)&mask + minv)
	out[60] = T((in[15]>>60)&mask |
		(in[16]<<4)&mask + minv)
	out[61] = T((in[16]>>13)&mask + minv)
	out[62] = T((in[16]>>30)&mask + minv)
	out[63] = T((in[16]>>47)&mask + minv)

}
func br32_18[T uint32 | int32](out *[64]T, in *[18]uint64, minv uint64) {
	mask := uint64((1 << 18) - 1)
	out[0] = T((in[0]>>0)&mask + minv)
	out[1] = T((in[0]>>18)&mask + minv)
	out[2] = T((in[0]>>36)&mask + minv)
	out[3] = T((in[0]>>54)&mask |
		(in[1]<<10)&mask + minv)
	out[4] = T((in[1]>>8)&mask + minv)
	out[5] = T((in[1]>>26)&mask + minv)
	out[6] = T((in[1]>>44)&mask + minv)
	out[7] = T((in[1]>>62)&mask |
		(in[2]<<2)&mask + minv)
	out[8] = T((in[2]>>16)&mask + minv)
	out[9] = T((in[2]>>34)&mask + minv)
	out[10] = T((in[2]>>52)&mask |
		(in[3]<<12)&mask + minv)
	out[11] = T((in[3]>>6)&mask + minv)
	out[12] = T((in[3]>>24)&mask + minv)
	out[13] = T((in[3]>>42)&mask + minv)
	out[14] = T((in[3]>>60)&mask |
		(in[4]<<4)&mask + minv)
	out[15] = T((in[4]>>14)&mask + minv)
	out[16] = T((in[4]>>32)&mask + minv)
	out[17] = T((in[4]>>50)&mask |
		(in[5]<<14)&mask + minv)
	out[18] = T((in[5]>>4)&mask + minv)
	out[19] = T((in[5]>>22)&mask + minv)
	out[20] = T((in[5]>>40)&mask + minv)
	out[21] = T((in[5]>>58)&mask |
		(in[6]<<6)&mask + minv)
	out[22] = T((in[6]>>12)&mask + minv)
	out[23] = T((in[6]>>30)&mask + minv)
	out[24] = T((in[6]>>48)&mask |
		(in[7]<<16)&mask + minv)
	out[25] = T((in[7]>>2)&mask + minv)
	out[26] = T((in[7]>>20)&mask + minv)
	out[27] = T((in[7]>>38)&mask + minv)
	out[28] = T((in[7]>>56)&mask |
		(in[8]<<8)&mask + minv)
	out[29] = T((in[8]>>10)&mask + minv)
	out[30] = T((in[8]>>28)&mask + minv)
	out[31] = T((in[8]>>46)&mask + minv)
	out[32] = T((in[9]>>0)&mask + minv)
	out[33] = T((in[9]>>18)&mask + minv)
	out[34] = T((in[9]>>36)&mask + minv)
	out[35] = T((in[9]>>54)&mask |
		(in[10]<<10)&mask + minv)
	out[36] = T((in[10]>>8)&mask + minv)
	out[37] = T((in[10]>>26)&mask + minv)
	out[38] = T((in[10]>>44)&mask + minv)
	out[39] = T((in[10]>>62)&mask |
		(in[11]<<2)&mask + minv)
	out[40] = T((in[11]>>16)&mask + minv)
	out[41] = T((in[11]>>34)&mask + minv)
	out[42] = T((in[11]>>52)&mask |
		(in[12]<<12)&mask + minv)
	out[43] = T((in[12]>>6)&mask + minv)
	out[44] = T((in[12]>>24)&mask + minv)
	out[45] = T((in[12]>>42)&mask + minv)
	out[46] = T((in[12]>>60)&mask |
		(in[13]<<4)&mask + minv)
	out[47] = T((in[13]>>14)&mask + minv)
	out[48] = T((in[13]>>32)&mask + minv)
	out[49] = T((in[13]>>50)&mask |
		(in[14]<<14)&mask + minv)
	out[50] = T((in[14]>>4)&mask + minv)
	out[51] = T((in[14]>>22)&mask + minv)
	out[52] = T((in[14]>>40)&mask + minv)
	out[53] = T((in[14]>>58)&mask |
		(in[15]<<6)&mask + minv)
	out[54] = T((in[15]>>12)&mask + minv)
	out[55] = T((in[15]>>30)&mask + minv)
	out[56] = T((in[15]>>48)&mask |
		(in[16]<<16)&mask + minv)
	out[57] = T((in[16]>>2)&mask + minv)
	out[58] = T((in[16]>>20)&mask + minv)
	out[59] = T((in[16]>>38)&mask + minv)
	out[60] = T((in[16]>>56)&mask |
		(in[17]<<8)&mask + minv)
	out[61] = T((in[17]>>10)&mask + minv)
	out[62] = T((in[17]>>28)&mask + minv)
	out[63] = T((in[17]>>46)&mask + minv)

}
func br32_19[T uint32 | int32](out *[64]T, in *[19]uint64, minv uint64) {
	mask := uint64((1 << 19) - 1)
	out[0] = T((in[0]>>0)&mask + minv)
	out[1] = T((in[0]>>19)&mask + minv)
	out[2] = T((in[0]>>38)&mask + minv)
	out[3] = T((in[0]>>57)&mask |
		(in[1]<<7)&mask + minv)
	out[4] = T((in[1]>>12)&mask + minv)
	out[5] = T((in[1]>>31)&mask + minv)
	out[6] = T((in[1]>>50)&mask |
		(in[2]<<14)&mask + minv)
	out[7] = T((in[2]>>5)&mask + minv)
	out[8] = T((in[2]>>24)&mask + minv)
	out[9] = T((in[2]>>43)&mask + minv)
	out[10] = T((in[2]>>62)&mask |
		(in[3]<<2)&mask + minv)
	out[11] = T((in[3]>>17)&mask + minv)
	out[12] = T((in[3]>>36)&mask + minv)
	out[13] = T((in[3]>>55)&mask |
		(in[4]<<9)&mask + minv)
	out[14] = T((in[4]>>10)&mask + minv)
	out[15] = T((in[4]>>29)&mask + minv)
	out[16] = T((in[4]>>48)&mask |
		(in[5]<<16)&mask + minv)
	out[17] = T((in[5]>>3)&mask + minv)
	out[18] = T((in[5]>>22)&mask + minv)
	out[19] = T((in[5]>>41)&mask + minv)
	out[20] = T((in[5]>>60)&mask |
		(in[6]<<4)&mask + minv)
	out[21] = T((in[6]>>15)&mask + minv)
	out[22] = T((in[6]>>34)&mask + minv)
	out[23] = T((in[6]>>53)&mask |
		(in[7]<<11)&mask + minv)
	out[24] = T((in[7]>>8)&mask + minv)
	out[25] = T((in[7]>>27)&mask + minv)
	out[26] = T((in[7]>>46)&mask |
		(in[8]<<18)&mask + minv)
	out[27] = T((in[8]>>1)&mask + minv)
	out[28] = T((in[8]>>20)&mask + minv)
	out[29] = T((in[8]>>39)&mask + minv)
	out[30] = T((in[8]>>58)&mask |
		(in[9]<<6)&mask + minv)
	out[31] = T((in[9]>>13)&mask + minv)
	out[32] = T((in[9]>>32)&mask + minv)
	out[33] = T((in[9]>>51)&mask |
		(in[10]<<13)&mask + minv)
	out[34] = T((in[10]>>6)&mask + minv)
	out[35] = T((in[10]>>25)&mask + minv)
	out[36] = T((in[10]>>44)&mask + minv)
	out[37] = T((in[10]>>63)&mask |
		(in[11]<<1)&mask + minv)
	out[38] = T((in[11]>>18)&mask + minv)
	out[39] = T((in[11]>>37)&mask + minv)
	out[40] = T((in[11]>>56)&mask |
		(in[12]<<8)&mask + minv)
	out[41] = T((in[12]>>11)&mask + minv)
	out[42] = T((in[12]>>30)&mask + minv)
	out[43] = T((in[12]>>49)&mask |
		(in[13]<<15)&mask + minv)
	out[44] = T((in[13]>>4)&mask + minv)
	out[45] = T((in[13]>>23)&mask + minv)
	out[46] = T((in[13]>>42)&mask + minv)
	out[47] = T((in[13]>>61)&mask |
		(in[14]<<3)&mask + minv)
	out[48] = T((in[14]>>16)&mask + minv)
	out[49] = T((in[14]>>35)&mask + minv)
	out[50] = T((in[14]>>54)&mask |
		(in[15]<<10)&mask + minv)
	out[51] = T((in[15]>>9)&mask + minv)
	out[52] = T((in[15]>>28)&mask + minv)
	out[53] = T((in[15]>>47)&mask |
		(in[16]<<17)&mask + minv)
	out[54] = T((in[16]>>2)&mask + minv)
	out[55] = T((in[16]>>21)&mask + minv)
	out[56] = T((in[16]>>40)&mask + minv)
	out[57] = T((in[16]>>59)&mask |
		(in[17]<<5)&mask + minv)
	out[58] = T((in[17]>>14)&mask + minv)
	out[59] = T((in[17]>>33)&mask + minv)
	out[60] = T((in[17]>>52)&mask |
		(in[18]<<12)&mask + minv)
	out[61] = T((in[18]>>7)&mask + minv)
	out[62] = T((in[18]>>26)&mask + minv)
	out[63] = T((in[18]>>45)&mask + minv)

}
func br32_20[T uint32 | int32](out *[64]T, in *[20]uint64, minv uint64) {
	mask := uint64((1 << 20) - 1)
	out[0] = T((in[0]>>0)&mask + minv)
	out[1] = T((in[0]>>20)&mask + minv)
	out[2] = T((in[0]>>40)&mask + minv)
	out[3] = T((in[0]>>60)&mask |
		(in[1]<<4)&mask + minv)
	out[4] = T((in[1]>>16)&mask + minv)
	out[5] = T((in[1]>>36)&mask + minv)
	out[6] = T((in[1]>>56)&mask |
		(in[2]<<8)&mask + minv)
	out[7] = T((in[2]>>12)&mask + minv)
	out[8] = T((in[2]>>32)&mask + minv)
	out[9] = T((in[2]>>52)&mask |
		(in[3]<<12)&mask + minv)
	out[10] = T((in[3]>>8)&mask + minv)
	out[11] = T((in[3]>>28)&mask + minv)
	out[12] = T((in[3]>>48)&mask |
		(in[4]<<16)&mask + minv)
	out[13] = T((in[4]>>4)&mask + minv)
	out[14] = T((in[4]>>24)&mask + minv)
	out[15] = T((in[4]>>44)&mask + minv)
	out[16] = T((in[5]>>0)&mask + minv)
	out[17] = T((in[5]>>20)&mask + minv)
	out[18] = T((in[5]>>40)&mask + minv)
	out[19] = T((in[5]>>60)&mask |
		(in[6]<<4)&mask + minv)
	out[20] = T((in[6]>>16)&mask + minv)
	out[21] = T((in[6]>>36)&mask + minv)
	out[22] = T((in[6]>>56)&mask |
		(in[7]<<8)&mask + minv)
	out[23] = T((in[7]>>12)&mask + minv)
	out[24] = T((in[7]>>32)&mask + minv)
	out[25] = T((in[7]>>52)&mask |
		(in[8]<<12)&mask + minv)
	out[26] = T((in[8]>>8)&mask + minv)
	out[27] = T((in[8]>>28)&mask + minv)
	out[28] = T((in[8]>>48)&mask |
		(in[9]<<16)&mask + minv)
	out[29] = T((in[9]>>4)&mask + minv)
	out[30] = T((in[9]>>24)&mask + minv)
	out[31] = T((in[9]>>44)&mask + minv)
	out[32] = T((in[10]>>0)&mask + minv)
	out[33] = T((in[10]>>20)&mask + minv)
	out[34] = T((in[10]>>40)&mask + minv)
	out[35] = T((in[10]>>60)&mask |
		(in[11]<<4)&mask + minv)
	out[36] = T((in[11]>>16)&mask + minv)
	out[37] = T((in[11]>>36)&mask + minv)
	out[38] = T((in[11]>>56)&mask |
		(in[12]<<8)&mask + minv)
	out[39] = T((in[12]>>12)&mask + minv)
	out[40] = T((in[12]>>32)&mask + minv)
	out[41] = T((in[12]>>52)&mask |
		(in[13]<<12)&mask + minv)
	out[42] = T((in[13]>>8)&mask + minv)
	out[43] = T((in[13]>>28)&mask + minv)
	out[44] = T((in[13]>>48)&mask |
		(in[14]<<16)&mask + minv)
	out[45] = T((in[14]>>4)&mask + minv)
	out[46] = T((in[14]>>24)&mask + minv)
	out[47] = T((in[14]>>44)&mask + minv)
	out[48] = T((in[15]>>0)&mask + minv)
	out[49] = T((in[15]>>20)&mask + minv)
	out[50] = T((in[15]>>40)&mask + minv)
	out[51] = T((in[15]>>60)&mask |
		(in[16]<<4)&mask + minv)
	out[52] = T((in[16]>>16)&mask + minv)
	out[53] = T((in[16]>>36)&mask + minv)
	out[54] = T((in[16]>>56)&mask |
		(in[17]<<8)&mask + minv)
	out[55] = T((in[17]>>12)&mask + minv)
	out[56] = T((in[17]>>32)&mask + minv)
	out[57] = T((in[17]>>52)&mask |
		(in[18]<<12)&mask + minv)
	out[58] = T((in[18]>>8)&mask + minv)
	out[59] = T((in[18]>>28)&mask + minv)
	out[60] = T((in[18]>>48)&mask |
		(in[19]<<16)&mask + minv)
	out[61] = T((in[19]>>4)&mask + minv)
	out[62] = T((in[19]>>24)&mask + minv)
	out[63] = T((in[19]>>44)&mask + minv)

}
func br32_21[T uint32 | int32](out *[64]T, in *[21]uint64, minv uint64) {
	mask := uint64((1 << 21) - 1)
	out[0] = T((in[0]>>0)&mask + minv)
	out[1] = T((in[0]>>21)&mask + minv)
	out[2] = T((in[0]>>42)&mask + minv)
	out[3] = T((in[0]>>63)&mask |
		(in[1]<<1)&mask + minv)
	out[4] = T((in[1]>>20)&mask + minv)
	out[5] = T((in[1]>>41)&mask + minv)
	out[6] = T((in[1]>>62)&mask |
		(in[2]<<2)&mask + minv)
	out[7] = T((in[2]>>19)&mask + minv)
	out[8] = T((in[2]>>40)&mask + minv)
	out[9] = T((in[2]>>61)&mask |
		(in[3]<<3)&mask + minv)
	out[10] = T((in[3]>>18)&mask + minv)
	out[11] = T((in[3]>>39)&mask + minv)
	out[12] = T((in[3]>>60)&mask |
		(in[4]<<4)&mask + minv)
	out[13] = T((in[4]>>17)&mask + minv)
	out[14] = T((in[4]>>38)&mask + minv)
	out[15] = T((in[4]>>59)&mask |
		(in[5]<<5)&mask + minv)
	out[16] = T((in[5]>>16)&mask + minv)
	out[17] = T((in[5]>>37)&mask + minv)
	out[18] = T((in[5]>>58)&mask |
		(in[6]<<6)&mask + minv)
	out[19] = T((in[6]>>15)&mask + minv)
	out[20] = T((in[6]>>36)&mask + minv)
	out[21] = T((in[6]>>57)&mask |
		(in[7]<<7)&mask + minv)
	out[22] = T((in[7]>>14)&mask + minv)
	out[23] = T((in[7]>>35)&mask + minv)
	out[24] = T((in[7]>>56)&mask |
		(in[8]<<8)&mask + minv)
	out[25] = T((in[8]>>13)&mask + minv)
	out[26] = T((in[8]>>34)&mask + minv)
	out[27] = T((in[8]>>55)&mask |
		(in[9]<<9)&mask + minv)
	out[28] = T((in[9]>>12)&mask + minv)
	out[29] = T((in[9]>>33)&mask + minv)
	out[30] = T((in[9]>>54)&mask |
		(in[10]<<10)&mask + minv)
	out[31] = T((in[10]>>11)&mask + minv)
	out[32] = T((in[10]>>32)&mask + minv)
	out[33] = T((in[10]>>53)&mask |
		(in[11]<<11)&mask + minv)
	out[34] = T((in[11]>>10)&mask + minv)
	out[35] = T((in[11]>>31)&mask + minv)
	out[36] = T((in[11]>>52)&mask |
		(in[12]<<12)&mask + minv)
	out[37] = T((in[12]>>9)&mask + minv)
	out[38] = T((in[12]>>30)&mask + minv)
	out[39] = T((in[12]>>51)&mask |
		(in[13]<<13)&mask + minv)
	out[40] = T((in[13]>>8)&mask + minv)
	out[41] = T((in[13]>>29)&mask + minv)
	out[42] = T((in[13]>>50)&mask |
		(in[14]<<14)&mask + minv)
	out[43] = T((in[14]>>7)&mask + minv)
	out[44] = T((in[14]>>28)&mask + minv)
	out[45] = T((in[14]>>49)&mask |
		(in[15]<<15)&mask + minv)
	out[46] = T((in[15]>>6)&mask + minv)
	out[47] = T((in[15]>>27)&mask + minv)
	out[48] = T((in[15]>>48)&mask |
		(in[16]<<16)&mask + minv)
	out[49] = T((in[16]>>5)&mask + minv)
	out[50] = T((in[16]>>26)&mask + minv)
	out[51] = T((in[16]>>47)&mask |
		(in[17]<<17)&mask + minv)
	out[52] = T((in[17]>>4)&mask + minv)
	out[53] = T((in[17]>>25)&mask + minv)
	out[54] = T((in[17]>>46)&mask |
		(in[18]<<18)&mask + minv)
	out[55] = T((in[18]>>3)&mask + minv)
	out[56] = T((in[18]>>24)&mask + minv)
	out[57] = T((in[18]>>45)&mask |
		(in[19]<<19)&mask + minv)
	out[58] = T((in[19]>>2)&mask + minv)
	out[59] = T((in[19]>>23)&mask + minv)
	out[60] = T((in[19]>>44)&mask |
		(in[20]<<20)&mask + minv)
	out[61] = T((in[20]>>1)&mask + minv)
	out[62] = T((in[20]>>22)&mask + minv)
	out[63] = T((in[20]>>43)&mask + minv)

}
func br32_22[T uint32 | int32](out *[64]T, in *[22]uint64, minv uint64) {
	mask := uint64((1 << 22) - 1)
	out[0] = T((in[0]>>0)&mask + minv)
	out[1] = T((in[0]>>22)&mask + minv)
	out[2] = T((in[0]>>44)&mask |
		(in[1]<<20)&mask + minv)
	out[3] = T((in[1]>>2)&mask + minv)
	out[4] = T((in[1]>>24)&mask + minv)
	out[5] = T((in[1]>>46)&mask |
		(in[2]<<18)&mask + minv)
	out[6] = T((in[2]>>4)&mask + minv)
	out[7] = T((in[2]>>26)&mask + minv)
	out[8] = T((in[2]>>48)&mask |
		(in[3]<<16)&mask + minv)
	out[9] = T((in[3]>>6)&mask + minv)
	out[10] = T((in[3]>>28)&mask + minv)
	out[11] = T((in[3]>>50)&mask |
		(in[4]<<14)&mask + minv)
	out[12] = T((in[4]>>8)&mask + minv)
	out[13] = T((in[4]>>30)&mask + minv)
	out[14] = T((in[4]>>52)&mask |
		(in[5]<<12)&mask + minv)
	out[15] = T((in[5]>>10)&mask + minv)
	out[16] = T((in[5]>>32)&mask + minv)
	out[17] = T((in[5]>>54)&mask |
		(in[6]<<10)&mask + minv)
	out[18] = T((in[6]>>12)&mask + minv)
	out[19] = T((in[6]>>34)&mask + minv)
	out[20] = T((in[6]>>56)&mask |
		(in[7]<<8)&mask + minv)
	out[21] = T((in[7]>>14)&mask + minv)
	out[22] = T((in[7]>>36)&mask + minv)
	out[23] = T((in[7]>>58)&mask |
		(in[8]<<6)&mask + minv)
	out[24] = T((in[8]>>16)&mask + minv)
	out[25] = T((in[8]>>38)&mask + minv)
	out[26] = T((in[8]>>60)&mask |
		(in[9]<<4)&mask + minv)
	out[27] = T((in[9]>>18)&mask + minv)
	out[28] = T((in[9]>>40)&mask + minv)
	out[29] = T((in[9]>>62)&mask |
		(in[10]<<2)&mask + minv)
	out[30] = T((in[10]>>20)&mask + minv)
	out[31] = T((in[10]>>42)&mask + minv)
	out[32] = T((in[11]>>0)&mask + minv)
	out[33] = T((in[11]>>22)&mask + minv)
	out[34] = T((in[11]>>44)&mask |
		(in[12]<<20)&mask + minv)
	out[35] = T((in[12]>>2)&mask + minv)
	out[36] = T((in[12]>>24)&mask + minv)
	out[37] = T((in[12]>>46)&mask |
		(in[13]<<18)&mask + minv)
	out[38] = T((in[13]>>4)&mask + minv)
	out[39] = T((in[13]>>26)&mask + minv)
	out[40] = T((in[13]>>48)&mask |
		(in[14]<<16)&mask + minv)
	out[41] = T((in[14]>>6)&mask + minv)
	out[42] = T((in[14]>>28)&mask + minv)
	out[43] = T((in[14]>>50)&mask |
		(in[15]<<14)&mask + minv)
	out[44] = T((in[15]>>8)&mask + minv)
	out[45] = T((in[15]>>30)&mask + minv)
	out[46] = T((in[15]>>52)&mask |
		(in[16]<<12)&mask + minv)
	out[47] = T((in[16]>>10)&mask + minv)
	out[48] = T((in[16]>>32)&mask + minv)
	out[49] = T((in[16]>>54)&mask |
		(in[17]<<10)&mask + minv)
	out[50] = T((in[17]>>12)&mask + minv)
	out[51] = T((in[17]>>34)&mask + minv)
	out[52] = T((in[17]>>56)&mask |
		(in[18]<<8)&mask + minv)
	out[53] = T((in[18]>>14)&mask + minv)
	out[54] = T((in[18]>>36)&mask + minv)
	out[55] = T((in[18]>>58)&mask |
		(in[19]<<6)&mask + minv)
	out[56] = T((in[19]>>16)&mask + minv)
	out[57] = T((in[19]>>38)&mask + minv)
	out[58] = T((in[19]>>60)&mask |
		(in[20]<<4)&mask + minv)
	out[59] = T((in[20]>>18)&mask + minv)
	out[60] = T((in[20]>>40)&mask + minv)
	out[61] = T((in[20]>>62)&mask |
		(in[21]<<2)&mask + minv)
	out[62] = T((in[21]>>20)&mask + minv)
	out[63] = T((in[21]>>42)&mask + minv)

}
func br32_23[T uint32 | int32](out *[64]T, in *[23]uint64, minv uint64) {
	mask := uint64((1 << 23) - 1)
	out[0] = T((in[0]>>0)&mask + minv)
	out[1] = T((in[0]>>23)&mask + minv)
	out[2] = T((in[0]>>46)&mask |
		(in[1]<<18)&mask + minv)
	out[3] = T((in[1]>>5)&mask + minv)
	out[4] = T((in[1]>>28)&mask + minv)
	out[5] = T((in[1]>>51)&mask |
		(in[2]<<13)&mask + minv)
	out[6] = T((in[2]>>10)&mask + minv)
	out[7] = T((in[2]>>33)&mask + minv)
	out[8] = T((in[2]>>56)&mask |
		(in[3]<<8)&mask + minv)
	out[9] = T((in[3]>>15)&mask + minv)
	out[10] = T((in[3]>>38)&mask + minv)
	out[11] = T((in[3]>>61)&mask |
		(in[4]<<3)&mask + minv)
	out[12] = T((in[4]>>20)&mask + minv)
	out[13] = T((in[4]>>43)&mask |
		(in[5]<<21)&mask + minv)
	out[14] = T((in[5]>>2)&mask + minv)
	out[15] = T((in[5]>>25)&mask + minv)
	out[16] = T((in[5]>>48)&mask |
		(in[6]<<16)&mask + minv)
	out[17] = T((in[6]>>7)&mask + minv)
	out[18] = T((in[6]>>30)&mask + minv)
	out[19] = T((in[6]>>53)&mask |
		(in[7]<<11)&mask + minv)
	out[20] = T((in[7]>>12)&mask + minv)
	out[21] = T((in[7]>>35)&mask + minv)
	out[22] = T((in[7]>>58)&mask |
		(in[8]<<6)&mask + minv)
	out[23] = T((in[8]>>17)&mask + minv)
	out[24] = T((in[8]>>40)&mask + minv)
	out[25] = T((in[8]>>63)&mask |
		(in[9]<<1)&mask + minv)
	out[26] = T((in[9]>>22)&mask + minv)
	out[27] = T((in[9]>>45)&mask |
		(in[10]<<19)&mask + minv)
	out[28] = T((in[10]>>4)&mask + minv)
	out[29] = T((in[10]>>27)&mask + minv)
	out[30] = T((in[10]>>50)&mask |
		(in[11]<<14)&mask + minv)
	out[31] = T((in[11]>>9)&mask + minv)
	out[32] = T((in[11]>>32)&mask + minv)
	out[33] = T((in[11]>>55)&mask |
		(in[12]<<9)&mask + minv)
	out[34] = T((in[12]>>14)&mask + minv)
	out[35] = T((in[12]>>37)&mask + minv)
	out[36] = T((in[12]>>60)&mask |
		(in[13]<<4)&mask + minv)
	out[37] = T((in[13]>>19)&mask + minv)
	out[38] = T((in[13]>>42)&mask |
		(in[14]<<22)&mask + minv)
	out[39] = T((in[14]>>1)&mask + minv)
	out[40] = T((in[14]>>24)&mask + minv)
	out[41] = T((in[14]>>47)&mask |
		(in[15]<<17)&mask + minv)
	out[42] = T((in[15]>>6)&mask + minv)
	out[43] = T((in[15]>>29)&mask + minv)
	out[44] = T((in[15]>>52)&mask |
		(in[16]<<12)&mask + minv)
	out[45] = T((in[16]>>11)&mask + minv)
	out[46] = T((in[16]>>34)&mask + minv)
	out[47] = T((in[16]>>57)&mask |
		(in[17]<<7)&mask + minv)
	out[48] = T((in[17]>>16)&mask + minv)
	out[49] = T((in[17]>>39)&mask + minv)
	out[50] = T((in[17]>>62)&mask |
		(in[18]<<2)&mask + minv)
	out[51] = T((in[18]>>21)&mask + minv)
	out[52] = T((in[18]>>44)&mask |
		(in[19]<<20)&mask + minv)
	out[53] = T((in[19]>>3)&mask + minv)
	out[54] = T((in[19]>>26)&mask + minv)
	out[55] = T((in[19]>>49)&mask |
		(in[20]<<15)&mask + minv)
	out[56] = T((in[20]>>8)&mask + minv)
	out[57] = T((in[20]>>31)&mask + minv)
	out[58] = T((in[20]>>54)&mask |
		(in[21]<<10)&mask + minv)
	out[59] = T((in[21]>>13)&mask + minv)
	out[60] = T((in[21]>>36)&mask + minv)
	out[61] = T((in[21]>>59)&mask |
		(in[22]<<5)&mask + minv)
	out[62] = T((in[22]>>18)&mask + minv)
	out[63] = T((in[22]>>41)&mask + minv)

}
func br32_24[T uint32 | int32](out *[64]T, in *[24]uint64, minv uint64) {
	mask := uint64((1 << 24) - 1)
	out[0] = T((in[0]>>0)&mask + minv)
	out[1] = T((in[0]>>24)&mask + minv)
	out[2] = T((in[0]>>48)&mask |
		(in[1]<<16)&mask + minv)
	out[3] = T((in[1]>>8)&mask + minv)
	out[4] = T((in[1]>>32)&mask + minv)
	out[5] = T((in[1]>>56)&mask |
		(in[2]<<8)&mask + minv)
	out[6] = T((in[2]>>16)&mask + minv)
	out[7] = T((in[2]>>40)&mask + minv)
	out[8] = T((in[3]>>0)&mask + minv)
	out[9] = T((in[3]>>24)&mask + minv)
	out[10] = T((in[3]>>48)&mask |
		(in[4]<<16)&mask + minv)
	out[11] = T((in[4]>>8)&mask + minv)
	out[12] = T((in[4]>>32)&mask + minv)
	out[13] = T((in[4]>>56)&mask |
		(in[5]<<8)&mask + minv)
	out[14] = T((in[5]>>16)&mask + minv)
	out[15] = T((in[5]>>40)&mask + minv)
	out[16] = T((in[6]>>0)&mask + minv)
	out[17] = T((in[6]>>24)&mask + minv)
	out[18] = T((in[6]>>48)&mask |
		(in[7]<<16)&mask + minv)
	out[19] = T((in[7]>>8)&mask + minv)
	out[20] = T((in[7]>>32)&mask + minv)
	out[21] = T((in[7]>>56)&mask |
		(in[8]<<8)&mask + minv)
	out[22] = T((in[8]>>16)&mask + minv)
	out[23] = T((in[8]>>40)&mask + minv)
	out[24] = T((in[9]>>0)&mask + minv)
	out[25] = T((in[9]>>24)&mask + minv)
	out[26] = T((in[9]>>48)&mask |
		(in[10]<<16)&mask + minv)
	out[27] = T((in[10]>>8)&mask + minv)
	out[28] = T((in[10]>>32)&mask + minv)
	out[29] = T((in[10]>>56)&mask |
		(in[11]<<8)&mask + minv)
	out[30] = T((in[11]>>16)&mask + minv)
	out[31] = T((in[11]>>40)&mask + minv)
	out[32] = T((in[12]>>0)&mask + minv)
	out[33] = T((in[12]>>24)&mask + minv)
	out[34] = T((in[12]>>48)&mask |
		(in[13]<<16)&mask + minv)
	out[35] = T((in[13]>>8)&mask + minv)
	out[36] = T((in[13]>>32)&mask + minv)
	out[37] = T((in[13]>>56)&mask |
		(in[14]<<8)&mask + minv)
	out[38] = T((in[14]>>16)&mask + minv)
	out[39] = T((in[14]>>40)&mask + minv)
	out[40] = T((in[15]>>0)&mask + minv)
	out[41] = T((in[15]>>24)&mask + minv)
	out[42] = T((in[15]>>48)&mask |
		(in[16]<<16)&mask + minv)
	out[43] = T((in[16]>>8)&mask + minv)
	out[44] = T((in[16]>>32)&mask + minv)
	out[45] = T((in[16]>>56)&mask |
		(in[17]<<8)&mask + minv)
	out[46] = T((in[17]>>16)&mask + minv)
	out[47] = T((in[17]>>40)&mask + minv)
	out[48] = T((in[18]>>0)&mask + minv)
	out[49] = T((in[18]>>24)&mask + minv)
	out[50] = T((in[18]>>48)&mask |
		(in[19]<<16)&mask + minv)
	out[51] = T((in[19]>>8)&mask + minv)
	out[52] = T((in[19]>>32)&mask + minv)
	out[53] = T((in[19]>>56)&mask |
		(in[20]<<8)&mask + minv)
	out[54] = T((in[20]>>16)&mask + minv)
	out[55] = T((in[20]>>40)&mask + minv)
	out[56] = T((in[21]>>0)&mask + minv)
	out[57] = T((in[21]>>24)&mask + minv)
	out[58] = T((in[21]>>48)&mask |
		(in[22]<<16)&mask + minv)
	out[59] = T((in[22]>>8)&mask + minv)
	out[60] = T((in[22]>>32)&mask + minv)
	out[61] = T((in[22]>>56)&mask |
		(in[23]<<8)&mask + minv)
	out[62] = T((in[23]>>16)&mask + minv)
	out[63] = T((in[23]>>40)&mask + minv)

}
func br32_25[T uint32 | int32](out *[64]T, in *[25]uint64, minv uint64) {
	mask := uint64((1 << 25) - 1)
	out[0] = T((in[0]>>0)&mask + minv)
	out[1] = T((in[0]>>25)&mask + minv)
	out[2] = T((in[0]>>50)&mask |
		(in[1]<<14)&mask + minv)
	out[3] = T((in[1]>>11)&mask + minv)
	out[4] = T((in[1]>>36)&mask + minv)
	out[5] = T((in[1]>>61)&mask |
		(in[2]<<3)&mask + minv)
	out[6] = T((in[2]>>22)&mask + minv)
	out[7] = T((in[2]>>47)&mask |
		(in[3]<<17)&mask + minv)
	out[8] = T((in[3]>>8)&mask + minv)
	out[9] = T((in[3]>>33)&mask + minv)
	out[10] = T((in[3]>>58)&mask |
		(in[4]<<6)&mask + minv)
	out[11] = T((in[4]>>19)&mask + minv)
	out[12] = T((in[4]>>44)&mask |
		(in[5]<<20)&mask + minv)
	out[13] = T((in[5]>>5)&mask + minv)
	out[14] = T((in[5]>>30)&mask + minv)
	out[15] = T((in[5]>>55)&mask |
		(in[6]<<9)&mask + minv)
	out[16] = T((in[6]>>16)&mask + minv)
	out[17] = T((in[6]>>41)&mask |
		(in[7]<<23)&mask + minv)
	out[18] = T((in[7]>>2)&mask + minv)
	out[19] = T((in[7]>>27)&mask + minv)
	out[20] = T((in[7]>>52)&mask |
		(in[8]<<12)&mask + minv)
	out[21] = T((in[8]>>13)&mask + minv)
	out[22] = T((in[8]>>38)&mask + minv)
	out[23] = T((in[8]>>63)&mask |
		(in[9]<<1)&mask + minv)
	out[24] = T((in[9]>>24)&mask + minv)
	out[25] = T((in[9]>>49)&mask |
		(in[10]<<15)&mask + minv)
	out[26] = T((in[10]>>10)&mask + minv)
	out[27] = T((in[10]>>35)&mask + minv)
	out[28] = T((in[10]>>60)&mask |
		(in[11]<<4)&mask + minv)
	out[29] = T((in[11]>>21)&mask + minv)
	out[30] = T((in[11]>>46)&mask |
		(in[12]<<18)&mask + minv)
	out[31] = T((in[12]>>7)&mask + minv)
	out[32] = T((in[12]>>32)&mask + minv)
	out[33] = T((in[12]>>57)&mask |
		(in[13]<<7)&mask + minv)
	out[34] = T((in[13]>>18)&mask + minv)
	out[35] = T((in[13]>>43)&mask |
		(in[14]<<21)&mask + minv)
	out[36] = T((in[14]>>4)&mask + minv)
	out[37] = T((in[14]>>29)&mask + minv)
	out[38] = T((in[14]>>54)&mask |
		(in[15]<<10)&mask + minv)
	out[39] = T((in[15]>>15)&mask + minv)
	out[40] = T((in[15]>>40)&mask |
		(in[16]<<24)&mask + minv)
	out[41] = T((in[16]>>1)&mask + minv)
	out[42] = T((in[16]>>26)&mask + minv)
	out[43] = T((in[16]>>51)&mask |
		(in[17]<<13)&mask + minv)
	out[44] = T((in[17]>>12)&mask + minv)
	out[45] = T((in[17]>>37)&mask + minv)
	out[46] = T((in[17]>>62)&mask |
		(in[18]<<2)&mask + minv)
	out[47] = T((in[18]>>23)&mask + minv)
	out[48] = T((in[18]>>48)&mask |
		(in[19]<<16)&mask + minv)
	out[49] = T((in[19]>>9)&mask + minv)
	out[50] = T((in[19]>>34)&mask + minv)
	out[51] = T((in[19]>>59)&mask |
		(in[20]<<5)&mask + minv)
	out[52] = T((in[20]>>20)&mask + minv)
	out[53] = T((in[20]>>45)&mask |
		(in[21]<<19)&mask + minv)
	out[54] = T((in[21]>>6)&mask + minv)
	out[55] = T((in[21]>>31)&mask + minv)
	out[56] = T((in[21]>>56)&mask |
		(in[22]<<8)&mask + minv)
	out[57] = T((in[22]>>17)&mask + minv)
	out[58] = T((in[22]>>42)&mask |
		(in[23]<<22)&mask + minv)
	out[59] = T((in[23]>>3)&mask + minv)
	out[60] = T((in[23]>>28)&mask + minv)
	out[61] = T((in[23]>>53)&mask |
		(in[24]<<11)&mask + minv)
	out[62] = T((in[24]>>14)&mask + minv)
	out[63] = T((in[24]>>39)&mask + minv)

}
func br32_26[T uint32 | int32](out *[64]T, in *[26]uint64, minv uint64) {
	mask := uint64((1 << 26) - 1)
	out[0] = T((in[0]>>0)&mask + minv)
	out[1] = T((in[0]>>26)&mask + minv)
	out[2] = T((in[0]>>52)&mask |
		(in[1]<<12)&mask + minv)
	out[3] = T((in[1]>>14)&mask + minv)
	out[4] = T((in[1]>>40)&mask |
		(in[2]<<24)&mask + minv)
	out[5] = T((in[2]>>2)&mask + minv)
	out[6] = T((in[2]>>28)&mask + minv)
	out[7] = T((in[2]>>54)&mask |
		(in[3]<<10)&mask + minv)
	out[8] = T((in[3]>>16)&mask + minv)
	out[9] = T((in[3]>>42)&mask |
		(in[4]<<22)&mask + minv)
	out[10] = T((in[4]>>4)&mask + minv)
	out[11] = T((in[4]>>30)&mask + minv)
	out[12] = T((in[4]>>56)&mask |
		(in[5]<<8)&mask + minv)
	out[13] = T((in[5]>>18)&mask + minv)
	out[14] = T((in[5]>>44)&mask |
		(in[6]<<20)&mask + minv)
	out[15] = T((in[6]>>6)&mask + minv)
	out[16] = T((in[6]>>32)&mask + minv)
	out[17] = T((in[6]>>58)&mask |
		(in[7]<<6)&mask + minv)
	out[18] = T((in[7]>>20)&mask + minv)
	out[19] = T((in[7]>>46)&mask |
		(in[8]<<18)&mask + minv)
	out[20] = T((in[8]>>8)&mask + minv)
	out[21] = T((in[8]>>34)&mask + minv)
	out[22] = T((in[8]>>60)&mask |
		(in[9]<<4)&mask + minv)
	out[23] = T((in[9]>>22)&mask + minv)
	out[24] = T((in[9]>>48)&mask |
		(in[10]<<16)&mask + minv)
	out[25] = T((in[10]>>10)&mask + minv)
	out[26] = T((in[10]>>36)&mask + minv)
	out[27] = T((in[10]>>62)&mask |
		(in[11]<<2)&mask + minv)
	out[28] = T((in[11]>>24)&mask + minv)
	out[29] = T((in[11]>>50)&mask |
		(in[12]<<14)&mask + minv)
	out[30] = T((in[12]>>12)&mask + minv)
	out[31] = T((in[12]>>38)&mask + minv)
	out[32] = T((in[13]>>0)&mask + minv)
	out[33] = T((in[13]>>26)&mask + minv)
	out[34] = T((in[13]>>52)&mask |
		(in[14]<<12)&mask + minv)
	out[35] = T((in[14]>>14)&mask + minv)
	out[36] = T((in[14]>>40)&mask |
		(in[15]<<24)&mask + minv)
	out[37] = T((in[15]>>2)&mask + minv)
	out[38] = T((in[15]>>28)&mask + minv)
	out[39] = T((in[15]>>54)&mask |
		(in[16]<<10)&mask + minv)
	out[40] = T((in[16]>>16)&mask + minv)
	out[41] = T((in[16]>>42)&mask |
		(in[17]<<22)&mask + minv)
	out[42] = T((in[17]>>4)&mask + minv)
	out[43] = T((in[17]>>30)&mask + minv)
	out[44] = T((in[17]>>56)&mask |
		(in[18]<<8)&mask + minv)
	out[45] = T((in[18]>>18)&mask + minv)
	out[46] = T((in[18]>>44)&mask |
		(in[19]<<20)&mask + minv)
	out[47] = T((in[19]>>6)&mask + minv)
	out[48] = T((in[19]>>32)&mask + minv)
	out[49] = T((in[19]>>58)&mask |
		(in[20]<<6)&mask + minv)
	out[50] = T((in[20]>>20)&mask + minv)
	out[51] = T((in[20]>>46)&mask |
		(in[21]<<18)&mask + minv)
	out[52] = T((in[21]>>8)&mask + minv)
	out[53] = T((in[21]>>34)&mask + minv)
	out[54] = T((in[21]>>60)&mask |
		(in[22]<<4)&mask + minv)
	out[55] = T((in[22]>>22)&mask + minv)
	out[56] = T((in[22]>>48)&mask |
		(in[23]<<16)&mask + minv)
	out[57] = T((in[23]>>10)&mask + minv)
	out[58] = T((in[23]>>36)&mask + minv)
	out[59] = T((in[23]>>62)&mask |
		(in[24]<<2)&mask + minv)
	out[60] = T((in[24]>>24)&mask + minv)
	out[61] = T((in[24]>>50)&mask |
		(in[25]<<14)&mask + minv)
	out[62] = T((in[25]>>12)&mask + minv)
	out[63] = T((in[25]>>38)&mask + minv)

}
func br32_27[T uint32 | int32](out *[64]T, in *[27]uint64, minv uint64) {
	mask := uint64((1 << 27) - 1)
	out[0] = T((in[0]>>0)&mask + minv)
	out[1] = T((in[0]>>27)&mask + minv)
	out[2] = T((in[0]>>54)&mask |
		(in[1]<<10)&mask + minv)
	out[3] = T((in[1]>>17)&mask + minv)
	out[4] = T((in[1]>>44)&mask |
		(in[2]<<20)&mask + minv)
	out[5] = T((in[2]>>7)&mask + minv)
	out[6] = T((in[2]>>34)&mask + minv)
	out[7] = T((in[2]>>61)&mask |
		(in[3]<<3)&mask + minv)
	out[8] = T((in[3]>>24)&mask + minv)
	out[9] = T((in[3]>>51)&mask |
		(in[4]<<13)&mask + minv)
	out[10] = T((in[4]>>14)&mask + minv)
	out[11] = T((in[4]>>41)&mask |
		(in[5]<<23)&mask + minv)
	out[12] = T((in[5]>>4)&mask + minv)
	out[13] = T((in[5]>>31)&mask + minv)
	out[14] = T((in[5]>>58)&mask |
		(in[6]<<6)&mask + minv)
	out[15] = T((in[6]>>21)&mask + minv)
	out[16] = T((in[6]>>48)&mask |
		(in[7]<<16)&mask + minv)
	out[17] = T((in[7]>>11)&mask + minv)
	out[18] = T((in[7]>>38)&mask |
		(in[8]<<26)&mask + minv)
	out[19] = T((in[8]>>1)&mask + minv)
	out[20] = T((in[8]>>28)&mask + minv)
	out[21] = T((in[8]>>55)&mask |
		(in[9]<<9)&mask + minv)
	out[22] = T((in[9]>>18)&mask + minv)
	out[23] = T((in[9]>>45)&mask |
		(in[10]<<19)&mask + minv)
	out[24] = T((in[10]>>8)&mask + minv)
	out[25] = T((in[10]>>35)&mask + minv)
	out[26] = T((in[10]>>62)&mask |
		(in[11]<<2)&mask + minv)
	out[27] = T((in[11]>>25)&mask + minv)
	out[28] = T((in[11]>>52)&mask |
		(in[12]<<12)&mask + minv)
	out[29] = T((in[12]>>15)&mask + minv)
	out[30] = T((in[12]>>42)&mask |
		(in[13]<<22)&mask + minv)
	out[31] = T((in[13]>>5)&mask + minv)
	out[32] = T((in[13]>>32)&mask + minv)
	out[33] = T((in[13]>>59)&mask |
		(in[14]<<5)&mask + minv)
	out[34] = T((in[14]>>22)&mask + minv)
	out[35] = T((in[14]>>49)&mask |
		(in[15]<<15)&mask + minv)
	out[36] = T((in[15]>>12)&mask + minv)
	out[37] = T((in[15]>>39)&mask |
		(in[16]<<25)&mask + minv)
	out[38] = T((in[16]>>2)&mask + minv)
	out[39] = T((in[16]>>29)&mask + minv)
	out[40] = T((in[16]>>56)&mask |
		(in[17]<<8)&mask + minv)
	out[41] = T((in[17]>>19)&mask + minv)
	out[42] = T((in[17]>>46)&mask |
		(in[18]<<18)&mask + minv)
	out[43] = T((in[18]>>9)&mask + minv)
	out[44] = T((in[18]>>36)&mask + minv)
	out[45] = T((in[18]>>63)&mask |
		(in[19]<<1)&mask + minv)
	out[46] = T((in[19]>>26)&mask + minv)
	out[47] = T((in[19]>>53)&mask |
		(in[20]<<11)&mask + minv)
	out[48] = T((in[20]>>16)&mask + minv)
	out[49] = T((in[20]>>43)&mask |
		(in[21]<<21)&mask + minv)
	out[50] = T((in[21]>>6)&mask + minv)
	out[51] = T((in[21]>>33)&mask + minv)
	out[52] = T((in[21]>>60)&mask |
		(in[22]<<4)&mask + minv)
	out[53] = T((in[22]>>23)&mask + minv)
	out[54] = T((in[22]>>50)&mask |
		(in[23]<<14)&mask + minv)
	out[55] = T((in[23]>>13)&mask + minv)
	out[56] = T((in[23]>>40)&mask |
		(in[24]<<24)&mask + minv)
	out[57] = T((in[24]>>3)&mask + minv)
	out[58] = T((in[24]>>30)&mask + minv)
	out[59] = T((in[24]>>57)&mask |
		(in[25]<<7)&mask + minv)
	out[60] = T((in[25]>>20)&mask + minv)
	out[61] = T((in[25]>>47)&mask |
		(in[26]<<17)&mask + minv)
	out[62] = T((in[26]>>10)&mask + minv)
	out[63] = T((in[26]>>37)&mask + minv)

}
func br32_28[T uint32 | int32](out *[64]T, in *[28]uint64, minv uint64) {
	mask := uint64((1 << 28) - 1)
	out[0] = T((in[0]>>0)&mask + minv)
	out[1] = T((in[0]>>28)&mask + minv)
	out[2] = T((in[0]>>56)&mask |
		(in[1]<<8)&mask + minv)
	out[3] = T((in[1]>>20)&mask + minv)
	out[4] = T((in[1]>>48)&mask |
		(in[2]<<16)&mask + minv)
	out[5] = T((in[2]>>12)&mask + minv)
	out[6] = T((in[2]>>40)&mask |
		(in[3]<<24)&mask + minv)
	out[7] = T((in[3]>>4)&mask + minv)
	out[8] = T((in[3]>>32)&mask + minv)
	out[9] = T((in[3]>>60)&mask |
		(in[4]<<4)&mask + minv)
	out[10] = T((in[4]>>24)&mask + minv)
	out[11] = T((in[4]>>52)&mask |
		(in[5]<<12)&mask + minv)
	out[12] = T((in[5]>>16)&mask + minv)
	out[13] = T((in[5]>>44)&mask |
		(in[6]<<20)&mask + minv)
	out[14] = T((in[6]>>8)&mask + minv)
	out[15] = T((in[6]>>36)&mask + minv)
	out[16] = T((in[7]>>0)&mask + minv)
	out[17] = T((in[7]>>28)&mask + minv)
	out[18] = T((in[7]>>56)&mask |
		(in[8]<<8)&mask + minv)
	out[19] = T((in[8]>>20)&mask + minv)
	out[20] = T((in[8]>>48)&mask |
		(in[9]<<16)&mask + minv)
	out[21] = T((in[9]>>12)&mask + minv)
	out[22] = T((in[9]>>40)&mask |
		(in[10]<<24)&mask + minv)
	out[23] = T((in[10]>>4)&mask + minv)
	out[24] = T((in[10]>>32)&mask + minv)
	out[25] = T((in[10]>>60)&mask |
		(in[11]<<4)&mask + minv)
	out[26] = T((in[11]>>24)&mask + minv)
	out[27] = T((in[11]>>52)&mask |
		(in[12]<<12)&mask + minv)
	out[28] = T((in[12]>>16)&mask + minv)
	out[29] = T((in[12]>>44)&mask |
		(in[13]<<20)&mask + minv)
	out[30] = T((in[13]>>8)&mask + minv)
	out[31] = T((in[13]>>36)&mask + minv)
	out[32] = T((in[14]>>0)&mask + minv)
	out[33] = T((in[14]>>28)&mask + minv)
	out[34] = T((in[14]>>56)&mask |
		(in[15]<<8)&mask + minv)
	out[35] = T((in[15]>>20)&mask + minv)
	out[36] = T((in[15]>>48)&mask |
		(in[16]<<16)&mask + minv)
	out[37] = T((in[16]>>12)&mask + minv)
	out[38] = T((in[16]>>40)&mask |
		(in[17]<<24)&mask + minv)
	out[39] = T((in[17]>>4)&mask + minv)
	out[40] = T((in[17]>>32)&mask + minv)
	out[41] = T((in[17]>>60)&mask |
		(in[18]<<4)&mask + minv)
	out[42] = T((in[18]>>24)&mask + minv)
	out[43] = T((in[18]>>52)&mask |
		(in[19]<<12)&mask + minv)
	out[44] = T((in[19]>>16)&mask + minv)
	out[45] = T((in[19]>>44)&mask |
		(in[20]<<20)&mask + minv)
	out[46] = T((in[20]>>8)&mask + minv)
	out[47] = T((in[20]>>36)&mask + minv)
	out[48] = T((in[21]>>0)&mask + minv)
	out[49] = T((in[21]>>28)&mask + minv)
	out[50] = T((in[21]>>56)&mask |
		(in[22]<<8)&mask + minv)
	out[51] = T((in[22]>>20)&mask + minv)
	out[52] = T((in[22]>>48)&mask |
		(in[23]<<16)&mask + minv)
	out[53] = T((in[23]>>12)&mask + minv)
	out[54] = T((in[23]>>40)&mask |
		(in[24]<<24)&mask + minv)
	out[55] = T((in[24]>>4)&mask + minv)
	out[56] = T((in[24]>>32)&mask + minv)
	out[57] = T((in[24]>>60)&mask |
		(in[25]<<4)&mask + minv)
	out[58] = T((in[25]>>24)&mask + minv)
	out[59] = T((in[25]>>52)&mask |
		(in[26]<<12)&mask + minv)
	out[60] = T((in[26]>>16)&mask + minv)
	out[61] = T((in[26]>>44)&mask |
		(in[27]<<20)&mask + minv)
	out[62] = T((in[27]>>8)&mask + minv)
	out[63] = T((in[27]>>36)&mask + minv)

}
func br32_29[T uint32 | int32](out *[64]T, in *[29]uint64, minv uint64) {
	mask := uint64((1 << 29) - 1)
	out[0] = T((in[0]>>0)&mask + minv)
	out[1] = T((in[0]>>29)&mask + minv)
	out[2] = T((in[0]>>58)&mask |
		(in[1]<<6)&mask + minv)
	out[3] = T((in[1]>>23)&mask + minv)
	out[4] = T((in[1]>>52)&mask |
		(in[2]<<12)&mask + minv)
	out[5] = T((in[2]>>17)&mask + minv)
	out[6] = T((in[2]>>46)&mask |
		(in[3]<<18)&mask + minv)
	out[7] = T((in[3]>>11)&mask + minv)
	out[8] = T((in[3]>>40)&mask |
		(in[4]<<24)&mask + minv)
	out[9] = T((in[4]>>5)&mask + minv)
	out[10] = T((in[4]>>34)&mask + minv)
	out[11] = T((in[4]>>63)&mask |
		(in[5]<<1)&mask + minv)
	out[12] = T((in[5]>>28)&mask + minv)
	out[13] = T((in[5]>>57)&mask |
		(in[6]<<7)&mask + minv)
	out[14] = T((in[6]>>22)&mask + minv)
	out[15] = T((in[6]>>51)&mask |
		(in[7]<<13)&mask + minv)
	out[16] = T((in[7]>>16)&mask + minv)
	out[17] = T((in[7]>>45)&mask |
		(in[8]<<19)&mask + minv)
	out[18] = T((in[8]>>10)&mask + minv)
	out[19] = T((in[8]>>39)&mask |
		(in[9]<<25)&mask + minv)
	out[20] = T((in[9]>>4)&mask + minv)
	out[21] = T((in[9]>>33)&mask + minv)
	out[22] = T((in[9]>>62)&mask |
		(in[10]<<2)&mask + minv)
	out[23] = T((in[10]>>27)&mask + minv)
	out[24] = T((in[10]>>56)&mask |
		(in[11]<<8)&mask + minv)
	out[25] = T((in[11]>>21)&mask + minv)
	out[26] = T((in[11]>>50)&mask |
		(in[12]<<14)&mask + minv)
	out[27] = T((in[12]>>15)&mask + minv)
	out[28] = T((in[12]>>44)&mask |
		(in[13]<<20)&mask + minv)
	out[29] = T((in[13]>>9)&mask + minv)
	out[30] = T((in[13]>>38)&mask |
		(in[14]<<26)&mask + minv)
	out[31] = T((in[14]>>3)&mask + minv)
	out[32] = T((in[14]>>32)&mask + minv)
	out[33] = T((in[14]>>61)&mask |
		(in[15]<<3)&mask + minv)
	out[34] = T((in[15]>>26)&mask + minv)
	out[35] = T((in[15]>>55)&mask |
		(in[16]<<9)&mask + minv)
	out[36] = T((in[16]>>20)&mask + minv)
	out[37] = T((in[16]>>49)&mask |
		(in[17]<<15)&mask + minv)
	out[38] = T((in[17]>>14)&mask + minv)
	out[39] = T((in[17]>>43)&mask |
		(in[18]<<21)&mask + minv)
	out[40] = T((in[18]>>8)&mask + minv)
	out[41] = T((in[18]>>37)&mask |
		(in[19]<<27)&mask + minv)
	out[42] = T((in[19]>>2)&mask + minv)
	out[43] = T((in[19]>>31)&mask + minv)
	out[44] = T((in[19]>>60)&mask |
		(in[20]<<4)&mask + minv)
	out[45] = T((in[20]>>25)&mask + minv)
	out[46] = T((in[20]>>54)&mask |
		(in[21]<<10)&mask + minv)
	out[47] = T((in[21]>>19)&mask + minv)
	out[48] = T((in[21]>>48)&mask |
		(in[22]<<16)&mask + minv)
	out[49] = T((in[22]>>13)&mask + minv)
	out[50] = T((in[22]>>42)&mask |
		(in[23]<<22)&mask + minv)
	out[51] = T((in[23]>>7)&mask + minv)
	out[52] = T((in[23]>>36)&mask |
		(in[24]<<28)&mask + minv)
	out[53] = T((in[24]>>1)&mask + minv)
	out[54] = T((in[24]>>30)&mask + minv)
	out[55] = T((in[24]>>59)&mask |
		(in[25]<<5)&mask + minv)
	out[56] = T((in[25]>>24)&mask + minv)
	out[57] = T((in[25]>>53)&mask |
		(in[26]<<11)&mask + minv)
	out[58] = T((in[26]>>18)&mask + minv)
	out[59] = T((in[26]>>47)&mask |
		(in[27]<<17)&mask + minv)
	out[60] = T((in[27]>>12)&mask + minv)
	out[61] = T((in[27]>>41)&mask |
		(in[28]<<23)&mask + minv)
	out[62] = T((in[28]>>6)&mask + minv)
	out[63] = T((in[28]>>35)&mask + minv)

}
func br32_30[T uint32 | int32](out *[64]T, in *[30]uint64, minv uint64) {
	mask := uint64((1 << 30) - 1)
	out[0] = T((in[0]>>0)&mask + minv)
	out[1] = T((in[0]>>30)&mask + minv)
	out[2] = T((in[0]>>60)&mask |
		(in[1]<<4)&mask + minv)
	out[3] = T((in[1]>>26)&mask + minv)
	out[4] = T((in[1]>>56)&mask |
		(in[2]<<8)&mask + minv)
	out[5] = T((in[2]>>22)&mask + minv)
	out[6] = T((in[2]>>52)&mask |
		(in[3]<<12)&mask + minv)
	out[7] = T((in[3]>>18)&mask + minv)
	out[8] = T((in[3]>>48)&mask |
		(in[4]<<16)&mask + minv)
	out[9] = T((in[4]>>14)&mask + minv)
	out[10] = T((in[4]>>44)&mask |
		(in[5]<<20)&mask + minv)
	out[11] = T((in[5]>>10)&mask + minv)
	out[12] = T((in[5]>>40)&mask |
		(in[6]<<24)&mask + minv)
	out[13] = T((in[6]>>6)&mask + minv)
	out[14] = T((in[6]>>36)&mask |
		(in[7]<<28)&mask + minv)
	out[15] = T((in[7]>>2)&mask + minv)
	out[16] = T((in[7]>>32)&mask + minv)
	out[17] = T((in[7]>>62)&mask |
		(in[8]<<2)&mask + minv)
	out[18] = T((in[8]>>28)&mask + minv)
	out[19] = T((in[8]>>58)&mask |
		(in[9]<<6)&mask + minv)
	out[20] = T((in[9]>>24)&mask + minv)
	out[21] = T((in[9]>>54)&mask |
		(in[10]<<10)&mask + minv)
	out[22] = T((in[10]>>20)&mask + minv)
	out[23] = T((in[10]>>50)&mask |
		(in[11]<<14)&mask + minv)
	out[24] = T((in[11]>>16)&mask + minv)
	out[25] = T((in[11]>>46)&mask |
		(in[12]<<18)&mask + minv)
	out[26] = T((in[12]>>12)&mask + minv)
	out[27] = T((in[12]>>42)&mask |
		(in[13]<<22)&mask + minv)
	out[28] = T((in[13]>>8)&mask + minv)
	out[29] = T((in[13]>>38)&mask |
		(in[14]<<26)&mask + minv)
	out[30] = T((in[14]>>4)&mask + minv)
	out[31] = T((in[14]>>34)&mask + minv)
	out[32] = T((in[15]>>0)&mask + minv)
	out[33] = T((in[15]>>30)&mask + minv)
	out[34] = T((in[15]>>60)&mask |
		(in[16]<<4)&mask + minv)
	out[35] = T((in[16]>>26)&mask + minv)
	out[36] = T((in[16]>>56)&mask |
		(in[17]<<8)&mask + minv)
	out[37] = T((in[17]>>22)&mask + minv)
	out[38] = T((in[17]>>52)&mask |
		(in[18]<<12)&mask + minv)
	out[39] = T((in[18]>>18)&mask + minv)
	out[40] = T((in[18]>>48)&mask |
		(in[19]<<16)&mask + minv)
	out[41] = T((in[19]>>14)&mask + minv)
	out[42] = T((in[19]>>44)&mask |
		(in[20]<<20)&mask + minv)
	out[43] = T((in[20]>>10)&mask + minv)
	out[44] = T((in[20]>>40)&mask |
		(in[21]<<24)&mask + minv)
	out[45] = T((in[21]>>6)&mask + minv)
	out[46] = T((in[21]>>36)&mask |
		(in[22]<<28)&mask + minv)
	out[47] = T((in[22]>>2)&mask + minv)
	out[48] = T((in[22]>>32)&mask + minv)
	out[49] = T((in[22]>>62)&mask |
		(in[23]<<2)&mask + minv)
	out[50] = T((in[23]>>28)&mask + minv)
	out[51] = T((in[23]>>58)&mask |
		(in[24]<<6)&mask + minv)
	out[52] = T((in[24]>>24)&mask + minv)
	out[53] = T((in[24]>>54)&mask |
		(in[25]<<10)&mask + minv)
	out[54] = T((in[25]>>20)&mask + minv)
	out[55] = T((in[25]>>50)&mask |
		(in[26]<<14)&mask + minv)
	out[56] = T((in[26]>>16)&mask + minv)
	out[57] = T((in[26]>>46)&mask |
		(in[27]<<18)&mask + minv)
	out[58] = T((in[27]>>12)&mask + minv)
	out[59] = T((in[27]>>42)&mask |
		(in[28]<<22)&mask + minv)
	out[60] = T((in[28]>>8)&mask + minv)
	out[61] = T((in[28]>>38)&mask |
		(in[29]<<26)&mask + minv)
	out[62] = T((in[29]>>4)&mask + minv)
	out[63] = T((in[29]>>34)&mask + minv)

}
func br32_31[T uint32 | int32](out *[64]T, in *[31]uint64, minv uint64) {
	mask := uint64((1 << 31) - 1)
	out[0] = T((in[0]>>0)&mask + minv)
	out[1] = T((in[0]>>31)&mask + minv)
	out[2] = T((in[0]>>62)&mask |
		(in[1]<<2)&mask + minv)
	out[3] = T((in[1]>>29)&mask + minv)
	out[4] = T((in[1]>>60)&mask |
		(in[2]<<4)&mask + minv)
	out[5] = T((in[2]>>27)&mask + minv)
	out[6] = T((in[2]>>58)&mask |
		(in[3]<<6)&mask + minv)
	out[7] = T((in[3]>>25)&mask + minv)
	out[8] = T((in[3]>>56)&mask |
		(in[4]<<8)&mask + minv)
	out[9] = T((in[4]>>23)&mask + minv)
	out[10] = T((in[4]>>54)&mask |
		(in[5]<<10)&mask + minv)
	out[11] = T((in[5]>>21)&mask + minv)
	out[12] = T((in[5]>>52)&mask |
		(in[6]<<12)&mask + minv)
	out[13] = T((in[6]>>19)&mask + minv)
	out[14] = T((in[6]>>50)&mask |
		(in[7]<<14)&mask + minv)
	out[15] = T((in[7]>>17)&mask + minv)
	out[16] = T((in[7]>>48)&mask |
		(in[8]<<16)&mask + minv)
	out[17] = T((in[8]>>15)&mask + minv)
	out[18] = T((in[8]>>46)&mask |
		(in[9]<<18)&mask + minv)
	out[19] = T((in[9]>>13)&mask + minv)
	out[20] = T((in[9]>>44)&mask |
		(in[10]<<20)&mask + minv)
	out[21] = T((in[10]>>11)&mask + minv)
	out[22] = T((in[10]>>42)&mask |
		(in[11]<<22)&mask + minv)
	out[23] = T((in[11]>>9)&mask + minv)
	out[24] = T((in[11]>>40)&mask |
		(in[12]<<24)&mask + minv)
	out[25] = T((in[12]>>7)&mask + minv)
	out[26] = T((in[12]>>38)&mask |
		(in[13]<<26)&mask + minv)
	out[27] = T((in[13]>>5)&mask + minv)
	out[28] = T((in[13]>>36)&mask |
		(in[14]<<28)&mask + minv)
	out[29] = T((in[14]>>3)&mask + minv)
	out[30] = T((in[14]>>34)&mask |
		(in[15]<<30)&mask + minv)
	out[31] = T((in[15]>>1)&mask + minv)
	out[32] = T((in[15]>>32)&mask + minv)
	out[33] = T((in[15]>>63)&mask |
		(in[16]<<1)&mask + minv)
	out[34] = T((in[16]>>30)&mask + minv)
	out[35] = T((in[16]>>61)&mask |
		(in[17]<<3)&mask + minv)
	out[36] = T((in[17]>>28)&mask + minv)
	out[37] = T((in[17]>>59)&mask |
		(in[18]<<5)&mask + minv)
	out[38] = T((in[18]>>26)&mask + minv)
	out[39] = T((in[18]>>57)&mask |
		(in[19]<<7)&mask + minv)
	out[40] = T((in[19]>>24)&mask + minv)
	out[41] = T((in[19]>>55)&mask |
		(in[20]<<9)&mask + minv)
	out[42] = T((in[20]>>22)&mask + minv)
	out[43] = T((in[20]>>53)&mask |
		(in[21]<<11)&mask + minv)
	out[44] = T((in[21]>>20)&mask + minv)
	out[45] = T((in[21]>>51)&mask |
		(in[22]<<13)&mask + minv)
	out[46] = T((in[22]>>18)&mask + minv)
	out[47] = T((in[22]>>49)&mask |
		(in[23]<<15)&mask + minv)
	out[48] = T((in[23]>>16)&mask + minv)
	out[49] = T((in[23]>>47)&mask |
		(in[24]<<17)&mask + minv)
	out[50] = T((in[24]>>14)&mask + minv)
	out[51] = T((in[24]>>45)&mask |
		(in[25]<<19)&mask + minv)
	out[52] = T((in[25]>>12)&mask + minv)
	out[53] = T((in[25]>>43)&mask |
		(in[26]<<21)&mask + minv)
	out[54] = T((in[26]>>10)&mask + minv)
	out[55] = T((in[26]>>41)&mask |
		(in[27]<<23)&mask + minv)
	out[56] = T((in[27]>>8)&mask + minv)
	out[57] = T((in[27]>>39)&mask |
		(in[28]<<25)&mask + minv)
	out[58] = T((in[28]>>6)&mask + minv)
	out[59] = T((in[28]>>37)&mask |
		(in[29]<<27)&mask + minv)
	out[60] = T((in[29]>>4)&mask + minv)
	out[61] = T((in[29]>>35)&mask |
		(in[30]<<29)&mask + minv)
	out[62] = T((in[30]>>2)&mask + minv)
	out[63] = T((in[30]>>33)&mask + minv)

}
