// Copyright (c) 2025 Blockwatch Data Inc.
// Code automatically generated - DO NOT EDIT.
// Any manual changes will be lost.

package bitpack

// Packer
func bitpack8[T uint8 | int8](minv T, in []T, out []uint64, log2 int) {
	switch log2 {
	case 0:
		bp8_0((*[64]T)(in), (*[0]uint64)(out), uint64(minv))
	case 1:
		bp8_1((*[64]T)(in), (*[1]uint64)(out), uint64(minv))
	case 2:
		bp8_2((*[64]T)(in), (*[2]uint64)(out), uint64(minv))
	case 3:
		bp8_3((*[64]T)(in), (*[3]uint64)(out), uint64(minv))
	case 4:
		bp8_4((*[64]T)(in), (*[4]uint64)(out), uint64(minv))
	case 5:
		bp8_5((*[64]T)(in), (*[5]uint64)(out), uint64(minv))
	case 6:
		bp8_6((*[64]T)(in), (*[6]uint64)(out), uint64(minv))
	case 7:
		bp8_7((*[64]T)(in), (*[7]uint64)(out), uint64(minv))
	}
}
func bp8_0[T uint8 | int8](in *[64]T, out *[0]uint64, minv uint64) {
}
func bp8_1[T uint8 | int8](in *[64]T, out *[1]uint64, minv uint64) {
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
func bp8_2[T uint8 | int8](in *[64]T, out *[2]uint64, minv uint64) {
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
func bp8_3[T uint8 | int8](in *[64]T, out *[3]uint64, minv uint64) {
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
func bp8_4[T uint8 | int8](in *[64]T, out *[4]uint64, minv uint64) {
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
func bp8_5[T uint8 | int8](in *[64]T, out *[5]uint64, minv uint64) {
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
func bp8_6[T uint8 | int8](in *[64]T, out *[6]uint64, minv uint64) {
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
func bp8_7[T uint8 | int8](in *[64]T, out *[7]uint64, minv uint64) {
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

// Reader
func bitread8[T uint8 | int8](out []T, in []uint64, log2 int, minv T) {
	switch log2 {
	case 0:
		br8_0((*[64]T)(out), (*[0]uint64)(in), uint64(minv))
	case 1:
		br8_1((*[64]T)(out), (*[1]uint64)(in), uint64(minv))
	case 2:
		br8_2((*[64]T)(out), (*[2]uint64)(in), uint64(minv))
	case 3:
		br8_3((*[64]T)(out), (*[3]uint64)(in), uint64(minv))
	case 4:
		br8_4((*[64]T)(out), (*[4]uint64)(in), uint64(minv))
	case 5:
		br8_5((*[64]T)(out), (*[5]uint64)(in), uint64(minv))
	case 6:
		br8_6((*[64]T)(out), (*[6]uint64)(in), uint64(minv))
	case 7:
		br8_7((*[64]T)(out), (*[7]uint64)(in), uint64(minv))
	}
}
func br8_0[T uint8 | int8](out *[64]T, in *[0]uint64, minv uint64) {
	for i := range out {
		out[i] = T(minv)
	}
}
func br8_1[T uint8 | int8](out *[64]T, in *[1]uint64, minv uint64) {
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
func br8_2[T uint8 | int8](out *[64]T, in *[2]uint64, minv uint64) {
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
func br8_3[T uint8 | int8](out *[64]T, in *[3]uint64, minv uint64) {
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
func br8_4[T uint8 | int8](out *[64]T, in *[4]uint64, minv uint64) {
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
func br8_5[T uint8 | int8](out *[64]T, in *[5]uint64, minv uint64) {
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
func br8_6[T uint8 | int8](out *[64]T, in *[6]uint64, minv uint64) {
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
func br8_7[T uint8 | int8](out *[64]T, in *[7]uint64, minv uint64) {
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
