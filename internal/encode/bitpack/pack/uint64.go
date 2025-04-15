// Copyright (c) 2025 Blockwatch Data Inc.
// Code automatically generated - DO NOT EDIT.
// Any manual changes will be lost.

package pack

func bitpack64[T uint64 | int64](minv T, in []T, out []uint64, log2 int) {
	switch log2 {
	case 0:
		bp64_0((*[64]T)(in), (*[0]uint64)(out), minv)
	case 1:
		bp64_1((*[64]T)(in), (*[1]uint64)(out), minv)
	case 2:
		bp64_2((*[64]T)(in), (*[2]uint64)(out), minv)
	case 3:
		bp64_3((*[64]T)(in), (*[3]uint64)(out), minv)
	case 4:
		bp64_4((*[64]T)(in), (*[4]uint64)(out), minv)
	case 5:
		bp64_5((*[64]T)(in), (*[5]uint64)(out), minv)
	case 6:
		bp64_6((*[64]T)(in), (*[6]uint64)(out), minv)
	case 7:
		bp64_7((*[64]T)(in), (*[7]uint64)(out), minv)
	case 8:
		bp64_8((*[64]T)(in), (*[8]uint64)(out), minv)
	case 9:
		bp64_9((*[64]T)(in), (*[9]uint64)(out), minv)
	case 10:
		bp64_10((*[64]T)(in), (*[10]uint64)(out), minv)
	case 11:
		bp64_11((*[64]T)(in), (*[11]uint64)(out), minv)
	case 12:
		bp64_12((*[64]T)(in), (*[12]uint64)(out), minv)
	case 13:
		bp64_13((*[64]T)(in), (*[13]uint64)(out), minv)
	case 14:
		bp64_14((*[64]T)(in), (*[14]uint64)(out), minv)
	case 15:
		bp64_15((*[64]T)(in), (*[15]uint64)(out), minv)
	case 16:
		bp64_16((*[64]T)(in), (*[16]uint64)(out), minv)
	case 17:
		bp64_17((*[64]T)(in), (*[17]uint64)(out), minv)
	case 18:
		bp64_18((*[64]T)(in), (*[18]uint64)(out), minv)
	case 19:
		bp64_19((*[64]T)(in), (*[19]uint64)(out), minv)
	case 20:
		bp64_20((*[64]T)(in), (*[20]uint64)(out), minv)
	case 21:
		bp64_21((*[64]T)(in), (*[21]uint64)(out), minv)
	case 22:
		bp64_22((*[64]T)(in), (*[22]uint64)(out), minv)
	case 23:
		bp64_23((*[64]T)(in), (*[23]uint64)(out), minv)
	case 24:
		bp64_24((*[64]T)(in), (*[24]uint64)(out), minv)
	case 25:
		bp64_25((*[64]T)(in), (*[25]uint64)(out), minv)
	case 26:
		bp64_26((*[64]T)(in), (*[26]uint64)(out), minv)
	case 27:
		bp64_27((*[64]T)(in), (*[27]uint64)(out), minv)
	case 28:
		bp64_28((*[64]T)(in), (*[28]uint64)(out), minv)
	case 29:
		bp64_29((*[64]T)(in), (*[29]uint64)(out), minv)
	case 30:
		bp64_30((*[64]T)(in), (*[30]uint64)(out), minv)
	case 31:
		bp64_31((*[64]T)(in), (*[31]uint64)(out), minv)
	case 32:
		bp64_32((*[64]T)(in), (*[32]uint64)(out), minv)
	case 33:
		bp64_33((*[64]T)(in), (*[33]uint64)(out), minv)
	case 34:
		bp64_34((*[64]T)(in), (*[34]uint64)(out), minv)
	case 35:
		bp64_35((*[64]T)(in), (*[35]uint64)(out), minv)
	case 36:
		bp64_36((*[64]T)(in), (*[36]uint64)(out), minv)
	case 37:
		bp64_37((*[64]T)(in), (*[37]uint64)(out), minv)
	case 38:
		bp64_38((*[64]T)(in), (*[38]uint64)(out), minv)
	case 39:
		bp64_39((*[64]T)(in), (*[39]uint64)(out), minv)
	case 40:
		bp64_40((*[64]T)(in), (*[40]uint64)(out), minv)
	case 41:
		bp64_41((*[64]T)(in), (*[41]uint64)(out), minv)
	case 42:
		bp64_42((*[64]T)(in), (*[42]uint64)(out), minv)
	case 43:
		bp64_43((*[64]T)(in), (*[43]uint64)(out), minv)
	case 44:
		bp64_44((*[64]T)(in), (*[44]uint64)(out), minv)
	case 45:
		bp64_45((*[64]T)(in), (*[45]uint64)(out), minv)
	case 46:
		bp64_46((*[64]T)(in), (*[46]uint64)(out), minv)
	case 47:
		bp64_47((*[64]T)(in), (*[47]uint64)(out), minv)
	case 48:
		bp64_48((*[64]T)(in), (*[48]uint64)(out), minv)
	case 49:
		bp64_49((*[64]T)(in), (*[49]uint64)(out), minv)
	case 50:
		bp64_50((*[64]T)(in), (*[50]uint64)(out), minv)
	case 51:
		bp64_51((*[64]T)(in), (*[51]uint64)(out), minv)
	case 52:
		bp64_52((*[64]T)(in), (*[52]uint64)(out), minv)
	case 53:
		bp64_53((*[64]T)(in), (*[53]uint64)(out), minv)
	case 54:
		bp64_54((*[64]T)(in), (*[54]uint64)(out), minv)
	case 55:
		bp64_55((*[64]T)(in), (*[55]uint64)(out), minv)
	case 56:
		bp64_56((*[64]T)(in), (*[56]uint64)(out), minv)
	case 57:
		bp64_57((*[64]T)(in), (*[57]uint64)(out), minv)
	case 58:
		bp64_58((*[64]T)(in), (*[58]uint64)(out), minv)
	case 59:
		bp64_59((*[64]T)(in), (*[59]uint64)(out), minv)
	case 60:
		bp64_60((*[64]T)(in), (*[60]uint64)(out), minv)
	case 61:
		bp64_61((*[64]T)(in), (*[61]uint64)(out), minv)
	case 62:
		bp64_62((*[64]T)(in), (*[62]uint64)(out), minv)
	case 63:
		bp64_63((*[64]T)(in), (*[63]uint64)(out), minv)
	}

}
func bp64_0[T uint64 | int64](in *[64]T, out *[0]uint64, minv T) {
}
func bp64_1[T uint64 | int64](in *[64]T, out *[1]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 1) |
			((in[2] - minv) << 2) |
			((in[3] - minv) << 3) |
			((in[4] - minv) << 4) |
			((in[5] - minv) << 5) |
			((in[6] - minv) << 6) |
			((in[7] - minv) << 7) |
			((in[8] - minv) << 8) |
			((in[9] - minv) << 9) |
			((in[10] - minv) << 10) |
			((in[11] - minv) << 11) |
			((in[12] - minv) << 12) |
			((in[13] - minv) << 13) |
			((in[14] - minv) << 14) |
			((in[15] - minv) << 15) |
			((in[16] - minv) << 16) |
			((in[17] - minv) << 17) |
			((in[18] - minv) << 18) |
			((in[19] - minv) << 19) |
			((in[20] - minv) << 20) |
			((in[21] - minv) << 21) |
			((in[22] - minv) << 22) |
			((in[23] - minv) << 23) |
			((in[24] - minv) << 24) |
			((in[25] - minv) << 25) |
			((in[26] - minv) << 26) |
			((in[27] - minv) << 27) |
			((in[28] - minv) << 28) |
			((in[29] - minv) << 29) |
			((in[30] - minv) << 30) |
			((in[31] - minv) << 31) |
			((in[32] - minv) << 32) |
			((in[33] - minv) << 33) |
			((in[34] - minv) << 34) |
			((in[35] - minv) << 35) |
			((in[36] - minv) << 36) |
			((in[37] - minv) << 37) |
			((in[38] - minv) << 38) |
			((in[39] - minv) << 39) |
			((in[40] - minv) << 40) |
			((in[41] - minv) << 41) |
			((in[42] - minv) << 42) |
			((in[43] - minv) << 43) |
			((in[44] - minv) << 44) |
			((in[45] - minv) << 45) |
			((in[46] - minv) << 46) |
			((in[47] - minv) << 47) |
			((in[48] - minv) << 48) |
			((in[49] - minv) << 49) |
			((in[50] - minv) << 50) |
			((in[51] - minv) << 51) |
			((in[52] - minv) << 52) |
			((in[53] - minv) << 53) |
			((in[54] - minv) << 54) |
			((in[55] - minv) << 55) |
			((in[56] - minv) << 56) |
			((in[57] - minv) << 57) |
			((in[58] - minv) << 58) |
			((in[59] - minv) << 59) |
			((in[60] - minv) << 60) |
			((in[61] - minv) << 61) |
			((in[62] - minv) << 62) |
			((in[63] - minv) << 63))

}
func bp64_2[T uint64 | int64](in *[64]T, out *[2]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 2) |
			((in[2] - minv) << 4) |
			((in[3] - minv) << 6) |
			((in[4] - minv) << 8) |
			((in[5] - minv) << 10) |
			((in[6] - minv) << 12) |
			((in[7] - minv) << 14) |
			((in[8] - minv) << 16) |
			((in[9] - minv) << 18) |
			((in[10] - minv) << 20) |
			((in[11] - minv) << 22) |
			((in[12] - minv) << 24) |
			((in[13] - minv) << 26) |
			((in[14] - minv) << 28) |
			((in[15] - minv) << 30) |
			((in[16] - minv) << 32) |
			((in[17] - minv) << 34) |
			((in[18] - minv) << 36) |
			((in[19] - minv) << 38) |
			((in[20] - minv) << 40) |
			((in[21] - minv) << 42) |
			((in[22] - minv) << 44) |
			((in[23] - minv) << 46) |
			((in[24] - minv) << 48) |
			((in[25] - minv) << 50) |
			((in[26] - minv) << 52) |
			((in[27] - minv) << 54) |
			((in[28] - minv) << 56) |
			((in[29] - minv) << 58) |
			((in[30] - minv) << 60) |
			((in[31] - minv) << 62))

	out[1] = uint64(
		((in[32] - minv) << 0) |
			((in[33] - minv) << 2) |
			((in[34] - minv) << 4) |
			((in[35] - minv) << 6) |
			((in[36] - minv) << 8) |
			((in[37] - minv) << 10) |
			((in[38] - minv) << 12) |
			((in[39] - minv) << 14) |
			((in[40] - minv) << 16) |
			((in[41] - minv) << 18) |
			((in[42] - minv) << 20) |
			((in[43] - minv) << 22) |
			((in[44] - minv) << 24) |
			((in[45] - minv) << 26) |
			((in[46] - minv) << 28) |
			((in[47] - minv) << 30) |
			((in[48] - minv) << 32) |
			((in[49] - minv) << 34) |
			((in[50] - minv) << 36) |
			((in[51] - minv) << 38) |
			((in[52] - minv) << 40) |
			((in[53] - minv) << 42) |
			((in[54] - minv) << 44) |
			((in[55] - minv) << 46) |
			((in[56] - minv) << 48) |
			((in[57] - minv) << 50) |
			((in[58] - minv) << 52) |
			((in[59] - minv) << 54) |
			((in[60] - minv) << 56) |
			((in[61] - minv) << 58) |
			((in[62] - minv) << 60) |
			((in[63] - minv) << 62))

}
func bp64_3[T uint64 | int64](in *[64]T, out *[3]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 3) |
			((in[2] - minv) << 6) |
			((in[3] - minv) << 9) |
			((in[4] - minv) << 12) |
			((in[5] - minv) << 15) |
			((in[6] - minv) << 18) |
			((in[7] - minv) << 21) |
			((in[8] - minv) << 24) |
			((in[9] - minv) << 27) |
			((in[10] - minv) << 30) |
			((in[11] - minv) << 33) |
			((in[12] - minv) << 36) |
			((in[13] - minv) << 39) |
			((in[14] - minv) << 42) |
			((in[15] - minv) << 45) |
			((in[16] - minv) << 48) |
			((in[17] - minv) << 51) |
			((in[18] - minv) << 54) |
			((in[19] - minv) << 57) |
			((in[20] - minv) << 60) |
			((in[21] - minv) << 63))

	out[1] = uint64(
		((in[21] - minv) >> 1) |

			((in[22] - minv) << 2) |
			((in[23] - minv) << 5) |
			((in[24] - minv) << 8) |
			((in[25] - minv) << 11) |
			((in[26] - minv) << 14) |
			((in[27] - minv) << 17) |
			((in[28] - minv) << 20) |
			((in[29] - minv) << 23) |
			((in[30] - minv) << 26) |
			((in[31] - minv) << 29) |
			((in[32] - minv) << 32) |
			((in[33] - minv) << 35) |
			((in[34] - minv) << 38) |
			((in[35] - minv) << 41) |
			((in[36] - minv) << 44) |
			((in[37] - minv) << 47) |
			((in[38] - minv) << 50) |
			((in[39] - minv) << 53) |
			((in[40] - minv) << 56) |
			((in[41] - minv) << 59) |
			((in[42] - minv) << 62))

	out[2] = uint64(
		((in[42] - minv) >> 2) |

			((in[43] - minv) << 1) |
			((in[44] - minv) << 4) |
			((in[45] - minv) << 7) |
			((in[46] - minv) << 10) |
			((in[47] - minv) << 13) |
			((in[48] - minv) << 16) |
			((in[49] - minv) << 19) |
			((in[50] - minv) << 22) |
			((in[51] - minv) << 25) |
			((in[52] - minv) << 28) |
			((in[53] - minv) << 31) |
			((in[54] - minv) << 34) |
			((in[55] - minv) << 37) |
			((in[56] - minv) << 40) |
			((in[57] - minv) << 43) |
			((in[58] - minv) << 46) |
			((in[59] - minv) << 49) |
			((in[60] - minv) << 52) |
			((in[61] - minv) << 55) |
			((in[62] - minv) << 58) |
			((in[63] - minv) << 61))

}
func bp64_4[T uint64 | int64](in *[64]T, out *[4]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 4) |
			((in[2] - minv) << 8) |
			((in[3] - minv) << 12) |
			((in[4] - minv) << 16) |
			((in[5] - minv) << 20) |
			((in[6] - minv) << 24) |
			((in[7] - minv) << 28) |
			((in[8] - minv) << 32) |
			((in[9] - minv) << 36) |
			((in[10] - minv) << 40) |
			((in[11] - minv) << 44) |
			((in[12] - minv) << 48) |
			((in[13] - minv) << 52) |
			((in[14] - minv) << 56) |
			((in[15] - minv) << 60))

	out[1] = uint64(
		((in[16] - minv) << 0) |
			((in[17] - minv) << 4) |
			((in[18] - minv) << 8) |
			((in[19] - minv) << 12) |
			((in[20] - minv) << 16) |
			((in[21] - minv) << 20) |
			((in[22] - minv) << 24) |
			((in[23] - minv) << 28) |
			((in[24] - minv) << 32) |
			((in[25] - minv) << 36) |
			((in[26] - minv) << 40) |
			((in[27] - minv) << 44) |
			((in[28] - minv) << 48) |
			((in[29] - minv) << 52) |
			((in[30] - minv) << 56) |
			((in[31] - minv) << 60))

	out[2] = uint64(
		((in[32] - minv) << 0) |
			((in[33] - minv) << 4) |
			((in[34] - minv) << 8) |
			((in[35] - minv) << 12) |
			((in[36] - minv) << 16) |
			((in[37] - minv) << 20) |
			((in[38] - minv) << 24) |
			((in[39] - minv) << 28) |
			((in[40] - minv) << 32) |
			((in[41] - minv) << 36) |
			((in[42] - minv) << 40) |
			((in[43] - minv) << 44) |
			((in[44] - minv) << 48) |
			((in[45] - minv) << 52) |
			((in[46] - minv) << 56) |
			((in[47] - minv) << 60))

	out[3] = uint64(
		((in[48] - minv) << 0) |
			((in[49] - minv) << 4) |
			((in[50] - minv) << 8) |
			((in[51] - minv) << 12) |
			((in[52] - minv) << 16) |
			((in[53] - minv) << 20) |
			((in[54] - minv) << 24) |
			((in[55] - minv) << 28) |
			((in[56] - minv) << 32) |
			((in[57] - minv) << 36) |
			((in[58] - minv) << 40) |
			((in[59] - minv) << 44) |
			((in[60] - minv) << 48) |
			((in[61] - minv) << 52) |
			((in[62] - minv) << 56) |
			((in[63] - minv) << 60))

}
func bp64_5[T uint64 | int64](in *[64]T, out *[5]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 5) |
			((in[2] - minv) << 10) |
			((in[3] - minv) << 15) |
			((in[4] - minv) << 20) |
			((in[5] - minv) << 25) |
			((in[6] - minv) << 30) |
			((in[7] - minv) << 35) |
			((in[8] - minv) << 40) |
			((in[9] - minv) << 45) |
			((in[10] - minv) << 50) |
			((in[11] - minv) << 55) |
			((in[12] - minv) << 60))

	out[1] = uint64(
		((in[12] - minv) >> 4) |

			((in[13] - minv) << 1) |
			((in[14] - minv) << 6) |
			((in[15] - minv) << 11) |
			((in[16] - minv) << 16) |
			((in[17] - minv) << 21) |
			((in[18] - minv) << 26) |
			((in[19] - minv) << 31) |
			((in[20] - minv) << 36) |
			((in[21] - minv) << 41) |
			((in[22] - minv) << 46) |
			((in[23] - minv) << 51) |
			((in[24] - minv) << 56) |
			((in[25] - minv) << 61))

	out[2] = uint64(
		((in[25] - minv) >> 3) |

			((in[26] - minv) << 2) |
			((in[27] - minv) << 7) |
			((in[28] - minv) << 12) |
			((in[29] - minv) << 17) |
			((in[30] - minv) << 22) |
			((in[31] - minv) << 27) |
			((in[32] - minv) << 32) |
			((in[33] - minv) << 37) |
			((in[34] - minv) << 42) |
			((in[35] - minv) << 47) |
			((in[36] - minv) << 52) |
			((in[37] - minv) << 57) |
			((in[38] - minv) << 62))

	out[3] = uint64(
		((in[38] - minv) >> 2) |

			((in[39] - minv) << 3) |
			((in[40] - minv) << 8) |
			((in[41] - minv) << 13) |
			((in[42] - minv) << 18) |
			((in[43] - minv) << 23) |
			((in[44] - minv) << 28) |
			((in[45] - minv) << 33) |
			((in[46] - minv) << 38) |
			((in[47] - minv) << 43) |
			((in[48] - minv) << 48) |
			((in[49] - minv) << 53) |
			((in[50] - minv) << 58) |
			((in[51] - minv) << 63))

	out[4] = uint64(
		((in[51] - minv) >> 1) |

			((in[52] - minv) << 4) |
			((in[53] - minv) << 9) |
			((in[54] - minv) << 14) |
			((in[55] - minv) << 19) |
			((in[56] - minv) << 24) |
			((in[57] - minv) << 29) |
			((in[58] - minv) << 34) |
			((in[59] - minv) << 39) |
			((in[60] - minv) << 44) |
			((in[61] - minv) << 49) |
			((in[62] - minv) << 54) |
			((in[63] - minv) << 59))

}
func bp64_6[T uint64 | int64](in *[64]T, out *[6]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 6) |
			((in[2] - minv) << 12) |
			((in[3] - minv) << 18) |
			((in[4] - minv) << 24) |
			((in[5] - minv) << 30) |
			((in[6] - minv) << 36) |
			((in[7] - minv) << 42) |
			((in[8] - minv) << 48) |
			((in[9] - minv) << 54) |
			((in[10] - minv) << 60))

	out[1] = uint64(
		((in[10] - minv) >> 4) |

			((in[11] - minv) << 2) |
			((in[12] - minv) << 8) |
			((in[13] - minv) << 14) |
			((in[14] - minv) << 20) |
			((in[15] - minv) << 26) |
			((in[16] - minv) << 32) |
			((in[17] - minv) << 38) |
			((in[18] - minv) << 44) |
			((in[19] - minv) << 50) |
			((in[20] - minv) << 56) |
			((in[21] - minv) << 62))

	out[2] = uint64(
		((in[21] - minv) >> 2) |

			((in[22] - minv) << 4) |
			((in[23] - minv) << 10) |
			((in[24] - minv) << 16) |
			((in[25] - minv) << 22) |
			((in[26] - minv) << 28) |
			((in[27] - minv) << 34) |
			((in[28] - minv) << 40) |
			((in[29] - minv) << 46) |
			((in[30] - minv) << 52) |
			((in[31] - minv) << 58))

	out[3] = uint64(
		((in[31] - minv) >> 6) |

			((in[32] - minv) << 0) |
			((in[33] - minv) << 6) |
			((in[34] - minv) << 12) |
			((in[35] - minv) << 18) |
			((in[36] - minv) << 24) |
			((in[37] - minv) << 30) |
			((in[38] - minv) << 36) |
			((in[39] - minv) << 42) |
			((in[40] - minv) << 48) |
			((in[41] - minv) << 54) |
			((in[42] - minv) << 60))

	out[4] = uint64(
		((in[42] - minv) >> 4) |

			((in[43] - minv) << 2) |
			((in[44] - minv) << 8) |
			((in[45] - minv) << 14) |
			((in[46] - minv) << 20) |
			((in[47] - minv) << 26) |
			((in[48] - minv) << 32) |
			((in[49] - minv) << 38) |
			((in[50] - minv) << 44) |
			((in[51] - minv) << 50) |
			((in[52] - minv) << 56) |
			((in[53] - minv) << 62))

	out[5] = uint64(
		((in[53] - minv) >> 2) |

			((in[54] - minv) << 4) |
			((in[55] - minv) << 10) |
			((in[56] - minv) << 16) |
			((in[57] - minv) << 22) |
			((in[58] - minv) << 28) |
			((in[59] - minv) << 34) |
			((in[60] - minv) << 40) |
			((in[61] - minv) << 46) |
			((in[62] - minv) << 52) |
			((in[63] - minv) << 58))

}
func bp64_7[T uint64 | int64](in *[64]T, out *[7]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 7) |
			((in[2] - minv) << 14) |
			((in[3] - minv) << 21) |
			((in[4] - minv) << 28) |
			((in[5] - minv) << 35) |
			((in[6] - minv) << 42) |
			((in[7] - minv) << 49) |
			((in[8] - minv) << 56) |
			((in[9] - minv) << 63))

	out[1] = uint64(
		((in[9] - minv) >> 1) |

			((in[10] - minv) << 6) |
			((in[11] - minv) << 13) |
			((in[12] - minv) << 20) |
			((in[13] - minv) << 27) |
			((in[14] - minv) << 34) |
			((in[15] - minv) << 41) |
			((in[16] - minv) << 48) |
			((in[17] - minv) << 55) |
			((in[18] - minv) << 62))

	out[2] = uint64(
		((in[18] - minv) >> 2) |

			((in[19] - minv) << 5) |
			((in[20] - minv) << 12) |
			((in[21] - minv) << 19) |
			((in[22] - minv) << 26) |
			((in[23] - minv) << 33) |
			((in[24] - minv) << 40) |
			((in[25] - minv) << 47) |
			((in[26] - minv) << 54) |
			((in[27] - minv) << 61))

	out[3] = uint64(
		((in[27] - minv) >> 3) |

			((in[28] - minv) << 4) |
			((in[29] - minv) << 11) |
			((in[30] - minv) << 18) |
			((in[31] - minv) << 25) |
			((in[32] - minv) << 32) |
			((in[33] - minv) << 39) |
			((in[34] - minv) << 46) |
			((in[35] - minv) << 53) |
			((in[36] - minv) << 60))

	out[4] = uint64(
		((in[36] - minv) >> 4) |

			((in[37] - minv) << 3) |
			((in[38] - minv) << 10) |
			((in[39] - minv) << 17) |
			((in[40] - minv) << 24) |
			((in[41] - minv) << 31) |
			((in[42] - minv) << 38) |
			((in[43] - minv) << 45) |
			((in[44] - minv) << 52) |
			((in[45] - minv) << 59))

	out[5] = uint64(
		((in[45] - minv) >> 5) |

			((in[46] - minv) << 2) |
			((in[47] - minv) << 9) |
			((in[48] - minv) << 16) |
			((in[49] - minv) << 23) |
			((in[50] - minv) << 30) |
			((in[51] - minv) << 37) |
			((in[52] - minv) << 44) |
			((in[53] - minv) << 51) |
			((in[54] - minv) << 58))

	out[6] = uint64(
		((in[54] - minv) >> 6) |

			((in[55] - minv) << 1) |
			((in[56] - minv) << 8) |
			((in[57] - minv) << 15) |
			((in[58] - minv) << 22) |
			((in[59] - minv) << 29) |
			((in[60] - minv) << 36) |
			((in[61] - minv) << 43) |
			((in[62] - minv) << 50) |
			((in[63] - minv) << 57))

}
func bp64_8[T uint64 | int64](in *[64]T, out *[8]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 8) |
			((in[2] - minv) << 16) |
			((in[3] - minv) << 24) |
			((in[4] - minv) << 32) |
			((in[5] - minv) << 40) |
			((in[6] - minv) << 48) |
			((in[7] - minv) << 56))

	out[1] = uint64(
		((in[8] - minv) << 0) |
			((in[9] - minv) << 8) |
			((in[10] - minv) << 16) |
			((in[11] - minv) << 24) |
			((in[12] - minv) << 32) |
			((in[13] - minv) << 40) |
			((in[14] - minv) << 48) |
			((in[15] - minv) << 56))

	out[2] = uint64(
		((in[16] - minv) << 0) |
			((in[17] - minv) << 8) |
			((in[18] - minv) << 16) |
			((in[19] - minv) << 24) |
			((in[20] - minv) << 32) |
			((in[21] - minv) << 40) |
			((in[22] - minv) << 48) |
			((in[23] - minv) << 56))

	out[3] = uint64(
		((in[24] - minv) << 0) |
			((in[25] - minv) << 8) |
			((in[26] - minv) << 16) |
			((in[27] - minv) << 24) |
			((in[28] - minv) << 32) |
			((in[29] - minv) << 40) |
			((in[30] - minv) << 48) |
			((in[31] - minv) << 56))

	out[4] = uint64(
		((in[32] - minv) << 0) |
			((in[33] - minv) << 8) |
			((in[34] - minv) << 16) |
			((in[35] - minv) << 24) |
			((in[36] - minv) << 32) |
			((in[37] - minv) << 40) |
			((in[38] - minv) << 48) |
			((in[39] - minv) << 56))

	out[5] = uint64(
		((in[40] - minv) << 0) |
			((in[41] - minv) << 8) |
			((in[42] - minv) << 16) |
			((in[43] - minv) << 24) |
			((in[44] - minv) << 32) |
			((in[45] - minv) << 40) |
			((in[46] - minv) << 48) |
			((in[47] - minv) << 56))

	out[6] = uint64(
		((in[48] - minv) << 0) |
			((in[49] - minv) << 8) |
			((in[50] - minv) << 16) |
			((in[51] - minv) << 24) |
			((in[52] - minv) << 32) |
			((in[53] - minv) << 40) |
			((in[54] - minv) << 48) |
			((in[55] - minv) << 56))

	out[7] = uint64(
		((in[56] - minv) << 0) |
			((in[57] - minv) << 8) |
			((in[58] - minv) << 16) |
			((in[59] - minv) << 24) |
			((in[60] - minv) << 32) |
			((in[61] - minv) << 40) |
			((in[62] - minv) << 48) |
			((in[63] - minv) << 56))

}
func bp64_9[T uint64 | int64](in *[64]T, out *[9]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 9) |
			((in[2] - minv) << 18) |
			((in[3] - minv) << 27) |
			((in[4] - minv) << 36) |
			((in[5] - minv) << 45) |
			((in[6] - minv) << 54) |
			((in[7] - minv) << 63))

	out[1] = uint64(
		((in[7] - minv) >> 1) |

			((in[8] - minv) << 8) |
			((in[9] - minv) << 17) |
			((in[10] - minv) << 26) |
			((in[11] - minv) << 35) |
			((in[12] - minv) << 44) |
			((in[13] - minv) << 53) |
			((in[14] - minv) << 62))

	out[2] = uint64(
		((in[14] - minv) >> 2) |

			((in[15] - minv) << 7) |
			((in[16] - minv) << 16) |
			((in[17] - minv) << 25) |
			((in[18] - minv) << 34) |
			((in[19] - minv) << 43) |
			((in[20] - minv) << 52) |
			((in[21] - minv) << 61))

	out[3] = uint64(
		((in[21] - minv) >> 3) |

			((in[22] - minv) << 6) |
			((in[23] - minv) << 15) |
			((in[24] - minv) << 24) |
			((in[25] - minv) << 33) |
			((in[26] - minv) << 42) |
			((in[27] - minv) << 51) |
			((in[28] - minv) << 60))

	out[4] = uint64(
		((in[28] - minv) >> 4) |

			((in[29] - minv) << 5) |
			((in[30] - minv) << 14) |
			((in[31] - minv) << 23) |
			((in[32] - minv) << 32) |
			((in[33] - minv) << 41) |
			((in[34] - minv) << 50) |
			((in[35] - minv) << 59))

	out[5] = uint64(
		((in[35] - minv) >> 5) |

			((in[36] - minv) << 4) |
			((in[37] - minv) << 13) |
			((in[38] - minv) << 22) |
			((in[39] - minv) << 31) |
			((in[40] - minv) << 40) |
			((in[41] - minv) << 49) |
			((in[42] - minv) << 58))

	out[6] = uint64(
		((in[42] - minv) >> 6) |

			((in[43] - minv) << 3) |
			((in[44] - minv) << 12) |
			((in[45] - minv) << 21) |
			((in[46] - minv) << 30) |
			((in[47] - minv) << 39) |
			((in[48] - minv) << 48) |
			((in[49] - minv) << 57))

	out[7] = uint64(
		((in[49] - minv) >> 7) |

			((in[50] - minv) << 2) |
			((in[51] - minv) << 11) |
			((in[52] - minv) << 20) |
			((in[53] - minv) << 29) |
			((in[54] - minv) << 38) |
			((in[55] - minv) << 47) |
			((in[56] - minv) << 56))

	out[8] = uint64(
		((in[56] - minv) >> 8) |

			((in[57] - minv) << 1) |
			((in[58] - minv) << 10) |
			((in[59] - minv) << 19) |
			((in[60] - minv) << 28) |
			((in[61] - minv) << 37) |
			((in[62] - minv) << 46) |
			((in[63] - minv) << 55))

}
func bp64_10[T uint64 | int64](in *[64]T, out *[10]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 10) |
			((in[2] - minv) << 20) |
			((in[3] - minv) << 30) |
			((in[4] - minv) << 40) |
			((in[5] - minv) << 50) |
			((in[6] - minv) << 60))

	out[1] = uint64(
		((in[6] - minv) >> 4) |

			((in[7] - minv) << 6) |
			((in[8] - minv) << 16) |
			((in[9] - minv) << 26) |
			((in[10] - minv) << 36) |
			((in[11] - minv) << 46) |
			((in[12] - minv) << 56))

	out[2] = uint64(
		((in[12] - minv) >> 8) |

			((in[13] - minv) << 2) |
			((in[14] - minv) << 12) |
			((in[15] - minv) << 22) |
			((in[16] - minv) << 32) |
			((in[17] - minv) << 42) |
			((in[18] - minv) << 52) |
			((in[19] - minv) << 62))

	out[3] = uint64(
		((in[19] - minv) >> 2) |

			((in[20] - minv) << 8) |
			((in[21] - minv) << 18) |
			((in[22] - minv) << 28) |
			((in[23] - minv) << 38) |
			((in[24] - minv) << 48) |
			((in[25] - minv) << 58))

	out[4] = uint64(
		((in[25] - minv) >> 6) |

			((in[26] - minv) << 4) |
			((in[27] - minv) << 14) |
			((in[28] - minv) << 24) |
			((in[29] - minv) << 34) |
			((in[30] - minv) << 44) |
			((in[31] - minv) << 54))

	out[5] = uint64(
		((in[31] - minv) >> 10) |

			((in[32] - minv) << 0) |
			((in[33] - minv) << 10) |
			((in[34] - minv) << 20) |
			((in[35] - minv) << 30) |
			((in[36] - minv) << 40) |
			((in[37] - minv) << 50) |
			((in[38] - minv) << 60))

	out[6] = uint64(
		((in[38] - minv) >> 4) |

			((in[39] - minv) << 6) |
			((in[40] - minv) << 16) |
			((in[41] - minv) << 26) |
			((in[42] - minv) << 36) |
			((in[43] - minv) << 46) |
			((in[44] - minv) << 56))

	out[7] = uint64(
		((in[44] - minv) >> 8) |

			((in[45] - minv) << 2) |
			((in[46] - minv) << 12) |
			((in[47] - minv) << 22) |
			((in[48] - minv) << 32) |
			((in[49] - minv) << 42) |
			((in[50] - minv) << 52) |
			((in[51] - minv) << 62))

	out[8] = uint64(
		((in[51] - minv) >> 2) |

			((in[52] - minv) << 8) |
			((in[53] - minv) << 18) |
			((in[54] - minv) << 28) |
			((in[55] - minv) << 38) |
			((in[56] - minv) << 48) |
			((in[57] - minv) << 58))

	out[9] = uint64(
		((in[57] - minv) >> 6) |

			((in[58] - minv) << 4) |
			((in[59] - minv) << 14) |
			((in[60] - minv) << 24) |
			((in[61] - minv) << 34) |
			((in[62] - minv) << 44) |
			((in[63] - minv) << 54))

}
func bp64_11[T uint64 | int64](in *[64]T, out *[11]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 11) |
			((in[2] - minv) << 22) |
			((in[3] - minv) << 33) |
			((in[4] - minv) << 44) |
			((in[5] - minv) << 55))

	out[1] = uint64(
		((in[5] - minv) >> 9) |

			((in[6] - minv) << 2) |
			((in[7] - minv) << 13) |
			((in[8] - minv) << 24) |
			((in[9] - minv) << 35) |
			((in[10] - minv) << 46) |
			((in[11] - minv) << 57))

	out[2] = uint64(
		((in[11] - minv) >> 7) |

			((in[12] - minv) << 4) |
			((in[13] - minv) << 15) |
			((in[14] - minv) << 26) |
			((in[15] - minv) << 37) |
			((in[16] - minv) << 48) |
			((in[17] - minv) << 59))

	out[3] = uint64(
		((in[17] - minv) >> 5) |

			((in[18] - minv) << 6) |
			((in[19] - minv) << 17) |
			((in[20] - minv) << 28) |
			((in[21] - minv) << 39) |
			((in[22] - minv) << 50) |
			((in[23] - minv) << 61))

	out[4] = uint64(
		((in[23] - minv) >> 3) |

			((in[24] - minv) << 8) |
			((in[25] - minv) << 19) |
			((in[26] - minv) << 30) |
			((in[27] - minv) << 41) |
			((in[28] - minv) << 52) |
			((in[29] - minv) << 63))

	out[5] = uint64(
		((in[29] - minv) >> 1) |

			((in[30] - minv) << 10) |
			((in[31] - minv) << 21) |
			((in[32] - minv) << 32) |
			((in[33] - minv) << 43) |
			((in[34] - minv) << 54))

	out[6] = uint64(
		((in[34] - minv) >> 10) |

			((in[35] - minv) << 1) |
			((in[36] - minv) << 12) |
			((in[37] - minv) << 23) |
			((in[38] - minv) << 34) |
			((in[39] - minv) << 45) |
			((in[40] - minv) << 56))

	out[7] = uint64(
		((in[40] - minv) >> 8) |

			((in[41] - minv) << 3) |
			((in[42] - minv) << 14) |
			((in[43] - minv) << 25) |
			((in[44] - minv) << 36) |
			((in[45] - minv) << 47) |
			((in[46] - minv) << 58))

	out[8] = uint64(
		((in[46] - minv) >> 6) |

			((in[47] - minv) << 5) |
			((in[48] - minv) << 16) |
			((in[49] - minv) << 27) |
			((in[50] - minv) << 38) |
			((in[51] - minv) << 49) |
			((in[52] - minv) << 60))

	out[9] = uint64(
		((in[52] - minv) >> 4) |

			((in[53] - minv) << 7) |
			((in[54] - minv) << 18) |
			((in[55] - minv) << 29) |
			((in[56] - minv) << 40) |
			((in[57] - minv) << 51) |
			((in[58] - minv) << 62))

	out[10] = uint64(
		((in[58] - minv) >> 2) |

			((in[59] - minv) << 9) |
			((in[60] - minv) << 20) |
			((in[61] - minv) << 31) |
			((in[62] - minv) << 42) |
			((in[63] - minv) << 53))

}
func bp64_12[T uint64 | int64](in *[64]T, out *[12]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 12) |
			((in[2] - minv) << 24) |
			((in[3] - minv) << 36) |
			((in[4] - minv) << 48) |
			((in[5] - minv) << 60))

	out[1] = uint64(
		((in[5] - minv) >> 4) |

			((in[6] - minv) << 8) |
			((in[7] - minv) << 20) |
			((in[8] - minv) << 32) |
			((in[9] - minv) << 44) |
			((in[10] - minv) << 56))

	out[2] = uint64(
		((in[10] - minv) >> 8) |

			((in[11] - minv) << 4) |
			((in[12] - minv) << 16) |
			((in[13] - minv) << 28) |
			((in[14] - minv) << 40) |
			((in[15] - minv) << 52))

	out[3] = uint64(
		((in[15] - minv) >> 12) |

			((in[16] - minv) << 0) |
			((in[17] - minv) << 12) |
			((in[18] - minv) << 24) |
			((in[19] - minv) << 36) |
			((in[20] - minv) << 48) |
			((in[21] - minv) << 60))

	out[4] = uint64(
		((in[21] - minv) >> 4) |

			((in[22] - minv) << 8) |
			((in[23] - minv) << 20) |
			((in[24] - minv) << 32) |
			((in[25] - minv) << 44) |
			((in[26] - minv) << 56))

	out[5] = uint64(
		((in[26] - minv) >> 8) |

			((in[27] - minv) << 4) |
			((in[28] - minv) << 16) |
			((in[29] - minv) << 28) |
			((in[30] - minv) << 40) |
			((in[31] - minv) << 52))

	out[6] = uint64(
		((in[31] - minv) >> 12) |

			((in[32] - minv) << 0) |
			((in[33] - minv) << 12) |
			((in[34] - minv) << 24) |
			((in[35] - minv) << 36) |
			((in[36] - minv) << 48) |
			((in[37] - minv) << 60))

	out[7] = uint64(
		((in[37] - minv) >> 4) |

			((in[38] - minv) << 8) |
			((in[39] - minv) << 20) |
			((in[40] - minv) << 32) |
			((in[41] - minv) << 44) |
			((in[42] - minv) << 56))

	out[8] = uint64(
		((in[42] - minv) >> 8) |

			((in[43] - minv) << 4) |
			((in[44] - minv) << 16) |
			((in[45] - minv) << 28) |
			((in[46] - minv) << 40) |
			((in[47] - minv) << 52))

	out[9] = uint64(
		((in[47] - minv) >> 12) |

			((in[48] - minv) << 0) |
			((in[49] - minv) << 12) |
			((in[50] - minv) << 24) |
			((in[51] - minv) << 36) |
			((in[52] - minv) << 48) |
			((in[53] - minv) << 60))

	out[10] = uint64(
		((in[53] - minv) >> 4) |

			((in[54] - minv) << 8) |
			((in[55] - minv) << 20) |
			((in[56] - minv) << 32) |
			((in[57] - minv) << 44) |
			((in[58] - minv) << 56))

	out[11] = uint64(
		((in[58] - minv) >> 8) |

			((in[59] - minv) << 4) |
			((in[60] - minv) << 16) |
			((in[61] - minv) << 28) |
			((in[62] - minv) << 40) |
			((in[63] - minv) << 52))

}
func bp64_13[T uint64 | int64](in *[64]T, out *[13]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 13) |
			((in[2] - minv) << 26) |
			((in[3] - minv) << 39) |
			((in[4] - minv) << 52))

	out[1] = uint64(
		((in[4] - minv) >> 12) |

			((in[5] - minv) << 1) |
			((in[6] - minv) << 14) |
			((in[7] - minv) << 27) |
			((in[8] - minv) << 40) |
			((in[9] - minv) << 53))

	out[2] = uint64(
		((in[9] - minv) >> 11) |

			((in[10] - minv) << 2) |
			((in[11] - minv) << 15) |
			((in[12] - minv) << 28) |
			((in[13] - minv) << 41) |
			((in[14] - minv) << 54))

	out[3] = uint64(
		((in[14] - minv) >> 10) |

			((in[15] - minv) << 3) |
			((in[16] - minv) << 16) |
			((in[17] - minv) << 29) |
			((in[18] - minv) << 42) |
			((in[19] - minv) << 55))

	out[4] = uint64(
		((in[19] - minv) >> 9) |

			((in[20] - minv) << 4) |
			((in[21] - minv) << 17) |
			((in[22] - minv) << 30) |
			((in[23] - minv) << 43) |
			((in[24] - minv) << 56))

	out[5] = uint64(
		((in[24] - minv) >> 8) |

			((in[25] - minv) << 5) |
			((in[26] - minv) << 18) |
			((in[27] - minv) << 31) |
			((in[28] - minv) << 44) |
			((in[29] - minv) << 57))

	out[6] = uint64(
		((in[29] - minv) >> 7) |

			((in[30] - minv) << 6) |
			((in[31] - minv) << 19) |
			((in[32] - minv) << 32) |
			((in[33] - minv) << 45) |
			((in[34] - minv) << 58))

	out[7] = uint64(
		((in[34] - minv) >> 6) |

			((in[35] - minv) << 7) |
			((in[36] - minv) << 20) |
			((in[37] - minv) << 33) |
			((in[38] - minv) << 46) |
			((in[39] - minv) << 59))

	out[8] = uint64(
		((in[39] - minv) >> 5) |

			((in[40] - minv) << 8) |
			((in[41] - minv) << 21) |
			((in[42] - minv) << 34) |
			((in[43] - minv) << 47) |
			((in[44] - minv) << 60))

	out[9] = uint64(
		((in[44] - minv) >> 4) |

			((in[45] - minv) << 9) |
			((in[46] - minv) << 22) |
			((in[47] - minv) << 35) |
			((in[48] - minv) << 48) |
			((in[49] - minv) << 61))

	out[10] = uint64(
		((in[49] - minv) >> 3) |

			((in[50] - minv) << 10) |
			((in[51] - minv) << 23) |
			((in[52] - minv) << 36) |
			((in[53] - minv) << 49) |
			((in[54] - minv) << 62))

	out[11] = uint64(
		((in[54] - minv) >> 2) |

			((in[55] - minv) << 11) |
			((in[56] - minv) << 24) |
			((in[57] - minv) << 37) |
			((in[58] - minv) << 50) |
			((in[59] - minv) << 63))

	out[12] = uint64(
		((in[59] - minv) >> 1) |

			((in[60] - minv) << 12) |
			((in[61] - minv) << 25) |
			((in[62] - minv) << 38) |
			((in[63] - minv) << 51))

}
func bp64_14[T uint64 | int64](in *[64]T, out *[14]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 14) |
			((in[2] - minv) << 28) |
			((in[3] - minv) << 42) |
			((in[4] - minv) << 56))

	out[1] = uint64(
		((in[4] - minv) >> 8) |

			((in[5] - minv) << 6) |
			((in[6] - minv) << 20) |
			((in[7] - minv) << 34) |
			((in[8] - minv) << 48) |
			((in[9] - minv) << 62))

	out[2] = uint64(
		((in[9] - minv) >> 2) |

			((in[10] - minv) << 12) |
			((in[11] - minv) << 26) |
			((in[12] - minv) << 40) |
			((in[13] - minv) << 54))

	out[3] = uint64(
		((in[13] - minv) >> 10) |

			((in[14] - minv) << 4) |
			((in[15] - minv) << 18) |
			((in[16] - minv) << 32) |
			((in[17] - minv) << 46) |
			((in[18] - minv) << 60))

	out[4] = uint64(
		((in[18] - minv) >> 4) |

			((in[19] - minv) << 10) |
			((in[20] - minv) << 24) |
			((in[21] - minv) << 38) |
			((in[22] - minv) << 52))

	out[5] = uint64(
		((in[22] - minv) >> 12) |

			((in[23] - minv) << 2) |
			((in[24] - minv) << 16) |
			((in[25] - minv) << 30) |
			((in[26] - minv) << 44) |
			((in[27] - minv) << 58))

	out[6] = uint64(
		((in[27] - minv) >> 6) |

			((in[28] - minv) << 8) |
			((in[29] - minv) << 22) |
			((in[30] - minv) << 36) |
			((in[31] - minv) << 50))

	out[7] = uint64(
		((in[31] - minv) >> 14) |

			((in[32] - minv) << 0) |
			((in[33] - minv) << 14) |
			((in[34] - minv) << 28) |
			((in[35] - minv) << 42) |
			((in[36] - minv) << 56))

	out[8] = uint64(
		((in[36] - minv) >> 8) |

			((in[37] - minv) << 6) |
			((in[38] - minv) << 20) |
			((in[39] - minv) << 34) |
			((in[40] - minv) << 48) |
			((in[41] - minv) << 62))

	out[9] = uint64(
		((in[41] - minv) >> 2) |

			((in[42] - minv) << 12) |
			((in[43] - minv) << 26) |
			((in[44] - minv) << 40) |
			((in[45] - minv) << 54))

	out[10] = uint64(
		((in[45] - minv) >> 10) |

			((in[46] - minv) << 4) |
			((in[47] - minv) << 18) |
			((in[48] - minv) << 32) |
			((in[49] - minv) << 46) |
			((in[50] - minv) << 60))

	out[11] = uint64(
		((in[50] - minv) >> 4) |

			((in[51] - minv) << 10) |
			((in[52] - minv) << 24) |
			((in[53] - minv) << 38) |
			((in[54] - minv) << 52))

	out[12] = uint64(
		((in[54] - minv) >> 12) |

			((in[55] - minv) << 2) |
			((in[56] - minv) << 16) |
			((in[57] - minv) << 30) |
			((in[58] - minv) << 44) |
			((in[59] - minv) << 58))

	out[13] = uint64(
		((in[59] - minv) >> 6) |

			((in[60] - minv) << 8) |
			((in[61] - minv) << 22) |
			((in[62] - minv) << 36) |
			((in[63] - minv) << 50))

}
func bp64_15[T uint64 | int64](in *[64]T, out *[15]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 15) |
			((in[2] - minv) << 30) |
			((in[3] - minv) << 45) |
			((in[4] - minv) << 60))

	out[1] = uint64(
		((in[4] - minv) >> 4) |

			((in[5] - minv) << 11) |
			((in[6] - minv) << 26) |
			((in[7] - minv) << 41) |
			((in[8] - minv) << 56))

	out[2] = uint64(
		((in[8] - minv) >> 8) |

			((in[9] - minv) << 7) |
			((in[10] - minv) << 22) |
			((in[11] - minv) << 37) |
			((in[12] - minv) << 52))

	out[3] = uint64(
		((in[12] - minv) >> 12) |

			((in[13] - minv) << 3) |
			((in[14] - minv) << 18) |
			((in[15] - minv) << 33) |
			((in[16] - minv) << 48) |
			((in[17] - minv) << 63))

	out[4] = uint64(
		((in[17] - minv) >> 1) |

			((in[18] - minv) << 14) |
			((in[19] - minv) << 29) |
			((in[20] - minv) << 44) |
			((in[21] - minv) << 59))

	out[5] = uint64(
		((in[21] - minv) >> 5) |

			((in[22] - minv) << 10) |
			((in[23] - minv) << 25) |
			((in[24] - minv) << 40) |
			((in[25] - minv) << 55))

	out[6] = uint64(
		((in[25] - minv) >> 9) |

			((in[26] - minv) << 6) |
			((in[27] - minv) << 21) |
			((in[28] - minv) << 36) |
			((in[29] - minv) << 51))

	out[7] = uint64(
		((in[29] - minv) >> 13) |

			((in[30] - minv) << 2) |
			((in[31] - minv) << 17) |
			((in[32] - minv) << 32) |
			((in[33] - minv) << 47) |
			((in[34] - minv) << 62))

	out[8] = uint64(
		((in[34] - minv) >> 2) |

			((in[35] - minv) << 13) |
			((in[36] - minv) << 28) |
			((in[37] - minv) << 43) |
			((in[38] - minv) << 58))

	out[9] = uint64(
		((in[38] - minv) >> 6) |

			((in[39] - minv) << 9) |
			((in[40] - minv) << 24) |
			((in[41] - minv) << 39) |
			((in[42] - minv) << 54))

	out[10] = uint64(
		((in[42] - minv) >> 10) |

			((in[43] - minv) << 5) |
			((in[44] - minv) << 20) |
			((in[45] - minv) << 35) |
			((in[46] - minv) << 50))

	out[11] = uint64(
		((in[46] - minv) >> 14) |

			((in[47] - minv) << 1) |
			((in[48] - minv) << 16) |
			((in[49] - minv) << 31) |
			((in[50] - minv) << 46) |
			((in[51] - minv) << 61))

	out[12] = uint64(
		((in[51] - minv) >> 3) |

			((in[52] - minv) << 12) |
			((in[53] - minv) << 27) |
			((in[54] - minv) << 42) |
			((in[55] - minv) << 57))

	out[13] = uint64(
		((in[55] - minv) >> 7) |

			((in[56] - minv) << 8) |
			((in[57] - minv) << 23) |
			((in[58] - minv) << 38) |
			((in[59] - minv) << 53))

	out[14] = uint64(
		((in[59] - minv) >> 11) |

			((in[60] - minv) << 4) |
			((in[61] - minv) << 19) |
			((in[62] - minv) << 34) |
			((in[63] - minv) << 49))

}
func bp64_16[T uint64 | int64](in *[64]T, out *[16]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 16) |
			((in[2] - minv) << 32) |
			((in[3] - minv) << 48))

	out[1] = uint64(
		((in[4] - minv) << 0) |
			((in[5] - minv) << 16) |
			((in[6] - minv) << 32) |
			((in[7] - minv) << 48))

	out[2] = uint64(
		((in[8] - minv) << 0) |
			((in[9] - minv) << 16) |
			((in[10] - minv) << 32) |
			((in[11] - minv) << 48))

	out[3] = uint64(
		((in[12] - minv) << 0) |
			((in[13] - minv) << 16) |
			((in[14] - minv) << 32) |
			((in[15] - minv) << 48))

	out[4] = uint64(
		((in[16] - minv) << 0) |
			((in[17] - minv) << 16) |
			((in[18] - minv) << 32) |
			((in[19] - minv) << 48))

	out[5] = uint64(
		((in[20] - minv) << 0) |
			((in[21] - minv) << 16) |
			((in[22] - minv) << 32) |
			((in[23] - minv) << 48))

	out[6] = uint64(
		((in[24] - minv) << 0) |
			((in[25] - minv) << 16) |
			((in[26] - minv) << 32) |
			((in[27] - minv) << 48))

	out[7] = uint64(
		((in[28] - minv) << 0) |
			((in[29] - minv) << 16) |
			((in[30] - minv) << 32) |
			((in[31] - minv) << 48))

	out[8] = uint64(
		((in[32] - minv) << 0) |
			((in[33] - minv) << 16) |
			((in[34] - minv) << 32) |
			((in[35] - minv) << 48))

	out[9] = uint64(
		((in[36] - minv) << 0) |
			((in[37] - minv) << 16) |
			((in[38] - minv) << 32) |
			((in[39] - minv) << 48))

	out[10] = uint64(
		((in[40] - minv) << 0) |
			((in[41] - minv) << 16) |
			((in[42] - minv) << 32) |
			((in[43] - minv) << 48))

	out[11] = uint64(
		((in[44] - minv) << 0) |
			((in[45] - minv) << 16) |
			((in[46] - minv) << 32) |
			((in[47] - minv) << 48))

	out[12] = uint64(
		((in[48] - minv) << 0) |
			((in[49] - minv) << 16) |
			((in[50] - minv) << 32) |
			((in[51] - minv) << 48))

	out[13] = uint64(
		((in[52] - minv) << 0) |
			((in[53] - minv) << 16) |
			((in[54] - minv) << 32) |
			((in[55] - minv) << 48))

	out[14] = uint64(
		((in[56] - minv) << 0) |
			((in[57] - minv) << 16) |
			((in[58] - minv) << 32) |
			((in[59] - minv) << 48))

	out[15] = uint64(
		((in[60] - minv) << 0) |
			((in[61] - minv) << 16) |
			((in[62] - minv) << 32) |
			((in[63] - minv) << 48))

}
func bp64_17[T uint64 | int64](in *[64]T, out *[17]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 17) |
			((in[2] - minv) << 34) |
			((in[3] - minv) << 51))

	out[1] = uint64(
		((in[3] - minv) >> 13) |

			((in[4] - minv) << 4) |
			((in[5] - minv) << 21) |
			((in[6] - minv) << 38) |
			((in[7] - minv) << 55))

	out[2] = uint64(
		((in[7] - minv) >> 9) |

			((in[8] - minv) << 8) |
			((in[9] - minv) << 25) |
			((in[10] - minv) << 42) |
			((in[11] - minv) << 59))

	out[3] = uint64(
		((in[11] - minv) >> 5) |

			((in[12] - minv) << 12) |
			((in[13] - minv) << 29) |
			((in[14] - minv) << 46) |
			((in[15] - minv) << 63))

	out[4] = uint64(
		((in[15] - minv) >> 1) |

			((in[16] - minv) << 16) |
			((in[17] - minv) << 33) |
			((in[18] - minv) << 50))

	out[5] = uint64(
		((in[18] - minv) >> 14) |

			((in[19] - minv) << 3) |
			((in[20] - minv) << 20) |
			((in[21] - minv) << 37) |
			((in[22] - minv) << 54))

	out[6] = uint64(
		((in[22] - minv) >> 10) |

			((in[23] - minv) << 7) |
			((in[24] - minv) << 24) |
			((in[25] - minv) << 41) |
			((in[26] - minv) << 58))

	out[7] = uint64(
		((in[26] - minv) >> 6) |

			((in[27] - minv) << 11) |
			((in[28] - minv) << 28) |
			((in[29] - minv) << 45) |
			((in[30] - minv) << 62))

	out[8] = uint64(
		((in[30] - minv) >> 2) |

			((in[31] - minv) << 15) |
			((in[32] - minv) << 32) |
			((in[33] - minv) << 49))

	out[9] = uint64(
		((in[33] - minv) >> 15) |

			((in[34] - minv) << 2) |
			((in[35] - minv) << 19) |
			((in[36] - minv) << 36) |
			((in[37] - minv) << 53))

	out[10] = uint64(
		((in[37] - minv) >> 11) |

			((in[38] - minv) << 6) |
			((in[39] - minv) << 23) |
			((in[40] - minv) << 40) |
			((in[41] - minv) << 57))

	out[11] = uint64(
		((in[41] - minv) >> 7) |

			((in[42] - minv) << 10) |
			((in[43] - minv) << 27) |
			((in[44] - minv) << 44) |
			((in[45] - minv) << 61))

	out[12] = uint64(
		((in[45] - minv) >> 3) |

			((in[46] - minv) << 14) |
			((in[47] - minv) << 31) |
			((in[48] - minv) << 48))

	out[13] = uint64(
		((in[48] - minv) >> 16) |

			((in[49] - minv) << 1) |
			((in[50] - minv) << 18) |
			((in[51] - minv) << 35) |
			((in[52] - minv) << 52))

	out[14] = uint64(
		((in[52] - minv) >> 12) |

			((in[53] - minv) << 5) |
			((in[54] - minv) << 22) |
			((in[55] - minv) << 39) |
			((in[56] - minv) << 56))

	out[15] = uint64(
		((in[56] - minv) >> 8) |

			((in[57] - minv) << 9) |
			((in[58] - minv) << 26) |
			((in[59] - minv) << 43) |
			((in[60] - minv) << 60))

	out[16] = uint64(
		((in[60] - minv) >> 4) |

			((in[61] - minv) << 13) |
			((in[62] - minv) << 30) |
			((in[63] - minv) << 47))

}
func bp64_18[T uint64 | int64](in *[64]T, out *[18]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 18) |
			((in[2] - minv) << 36) |
			((in[3] - minv) << 54))

	out[1] = uint64(
		((in[3] - minv) >> 10) |

			((in[4] - minv) << 8) |
			((in[5] - minv) << 26) |
			((in[6] - minv) << 44) |
			((in[7] - minv) << 62))

	out[2] = uint64(
		((in[7] - minv) >> 2) |

			((in[8] - minv) << 16) |
			((in[9] - minv) << 34) |
			((in[10] - minv) << 52))

	out[3] = uint64(
		((in[10] - minv) >> 12) |

			((in[11] - minv) << 6) |
			((in[12] - minv) << 24) |
			((in[13] - minv) << 42) |
			((in[14] - minv) << 60))

	out[4] = uint64(
		((in[14] - minv) >> 4) |

			((in[15] - minv) << 14) |
			((in[16] - minv) << 32) |
			((in[17] - minv) << 50))

	out[5] = uint64(
		((in[17] - minv) >> 14) |

			((in[18] - minv) << 4) |
			((in[19] - minv) << 22) |
			((in[20] - minv) << 40) |
			((in[21] - minv) << 58))

	out[6] = uint64(
		((in[21] - minv) >> 6) |

			((in[22] - minv) << 12) |
			((in[23] - minv) << 30) |
			((in[24] - minv) << 48))

	out[7] = uint64(
		((in[24] - minv) >> 16) |

			((in[25] - minv) << 2) |
			((in[26] - minv) << 20) |
			((in[27] - minv) << 38) |
			((in[28] - minv) << 56))

	out[8] = uint64(
		((in[28] - minv) >> 8) |

			((in[29] - minv) << 10) |
			((in[30] - minv) << 28) |
			((in[31] - minv) << 46))

	out[9] = uint64(
		((in[31] - minv) >> 18) |

			((in[32] - minv) << 0) |
			((in[33] - minv) << 18) |
			((in[34] - minv) << 36) |
			((in[35] - minv) << 54))

	out[10] = uint64(
		((in[35] - minv) >> 10) |

			((in[36] - minv) << 8) |
			((in[37] - minv) << 26) |
			((in[38] - minv) << 44) |
			((in[39] - minv) << 62))

	out[11] = uint64(
		((in[39] - minv) >> 2) |

			((in[40] - minv) << 16) |
			((in[41] - minv) << 34) |
			((in[42] - minv) << 52))

	out[12] = uint64(
		((in[42] - minv) >> 12) |

			((in[43] - minv) << 6) |
			((in[44] - minv) << 24) |
			((in[45] - minv) << 42) |
			((in[46] - minv) << 60))

	out[13] = uint64(
		((in[46] - minv) >> 4) |

			((in[47] - minv) << 14) |
			((in[48] - minv) << 32) |
			((in[49] - minv) << 50))

	out[14] = uint64(
		((in[49] - minv) >> 14) |

			((in[50] - minv) << 4) |
			((in[51] - minv) << 22) |
			((in[52] - minv) << 40) |
			((in[53] - minv) << 58))

	out[15] = uint64(
		((in[53] - minv) >> 6) |

			((in[54] - minv) << 12) |
			((in[55] - minv) << 30) |
			((in[56] - minv) << 48))

	out[16] = uint64(
		((in[56] - minv) >> 16) |

			((in[57] - minv) << 2) |
			((in[58] - minv) << 20) |
			((in[59] - minv) << 38) |
			((in[60] - minv) << 56))

	out[17] = uint64(
		((in[60] - minv) >> 8) |

			((in[61] - minv) << 10) |
			((in[62] - minv) << 28) |
			((in[63] - minv) << 46))

}
func bp64_19[T uint64 | int64](in *[64]T, out *[19]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 19) |
			((in[2] - minv) << 38) |
			((in[3] - minv) << 57))

	out[1] = uint64(
		((in[3] - minv) >> 7) |

			((in[4] - minv) << 12) |
			((in[5] - minv) << 31) |
			((in[6] - minv) << 50))

	out[2] = uint64(
		((in[6] - minv) >> 14) |

			((in[7] - minv) << 5) |
			((in[8] - minv) << 24) |
			((in[9] - minv) << 43) |
			((in[10] - minv) << 62))

	out[3] = uint64(
		((in[10] - minv) >> 2) |

			((in[11] - minv) << 17) |
			((in[12] - minv) << 36) |
			((in[13] - minv) << 55))

	out[4] = uint64(
		((in[13] - minv) >> 9) |

			((in[14] - minv) << 10) |
			((in[15] - minv) << 29) |
			((in[16] - minv) << 48))

	out[5] = uint64(
		((in[16] - minv) >> 16) |

			((in[17] - minv) << 3) |
			((in[18] - minv) << 22) |
			((in[19] - minv) << 41) |
			((in[20] - minv) << 60))

	out[6] = uint64(
		((in[20] - minv) >> 4) |

			((in[21] - minv) << 15) |
			((in[22] - minv) << 34) |
			((in[23] - minv) << 53))

	out[7] = uint64(
		((in[23] - minv) >> 11) |

			((in[24] - minv) << 8) |
			((in[25] - minv) << 27) |
			((in[26] - minv) << 46))

	out[8] = uint64(
		((in[26] - minv) >> 18) |

			((in[27] - minv) << 1) |
			((in[28] - minv) << 20) |
			((in[29] - minv) << 39) |
			((in[30] - minv) << 58))

	out[9] = uint64(
		((in[30] - minv) >> 6) |

			((in[31] - minv) << 13) |
			((in[32] - minv) << 32) |
			((in[33] - minv) << 51))

	out[10] = uint64(
		((in[33] - minv) >> 13) |

			((in[34] - minv) << 6) |
			((in[35] - minv) << 25) |
			((in[36] - minv) << 44) |
			((in[37] - minv) << 63))

	out[11] = uint64(
		((in[37] - minv) >> 1) |

			((in[38] - minv) << 18) |
			((in[39] - minv) << 37) |
			((in[40] - minv) << 56))

	out[12] = uint64(
		((in[40] - minv) >> 8) |

			((in[41] - minv) << 11) |
			((in[42] - minv) << 30) |
			((in[43] - minv) << 49))

	out[13] = uint64(
		((in[43] - minv) >> 15) |

			((in[44] - minv) << 4) |
			((in[45] - minv) << 23) |
			((in[46] - minv) << 42) |
			((in[47] - minv) << 61))

	out[14] = uint64(
		((in[47] - minv) >> 3) |

			((in[48] - minv) << 16) |
			((in[49] - minv) << 35) |
			((in[50] - minv) << 54))

	out[15] = uint64(
		((in[50] - minv) >> 10) |

			((in[51] - minv) << 9) |
			((in[52] - minv) << 28) |
			((in[53] - minv) << 47))

	out[16] = uint64(
		((in[53] - minv) >> 17) |

			((in[54] - minv) << 2) |
			((in[55] - minv) << 21) |
			((in[56] - minv) << 40) |
			((in[57] - minv) << 59))

	out[17] = uint64(
		((in[57] - minv) >> 5) |

			((in[58] - minv) << 14) |
			((in[59] - minv) << 33) |
			((in[60] - minv) << 52))

	out[18] = uint64(
		((in[60] - minv) >> 12) |

			((in[61] - minv) << 7) |
			((in[62] - minv) << 26) |
			((in[63] - minv) << 45))

}
func bp64_20[T uint64 | int64](in *[64]T, out *[20]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 20) |
			((in[2] - minv) << 40) |
			((in[3] - minv) << 60))

	out[1] = uint64(
		((in[3] - minv) >> 4) |

			((in[4] - minv) << 16) |
			((in[5] - minv) << 36) |
			((in[6] - minv) << 56))

	out[2] = uint64(
		((in[6] - minv) >> 8) |

			((in[7] - minv) << 12) |
			((in[8] - minv) << 32) |
			((in[9] - minv) << 52))

	out[3] = uint64(
		((in[9] - minv) >> 12) |

			((in[10] - minv) << 8) |
			((in[11] - minv) << 28) |
			((in[12] - minv) << 48))

	out[4] = uint64(
		((in[12] - minv) >> 16) |

			((in[13] - minv) << 4) |
			((in[14] - minv) << 24) |
			((in[15] - minv) << 44))

	out[5] = uint64(
		((in[15] - minv) >> 20) |

			((in[16] - minv) << 0) |
			((in[17] - minv) << 20) |
			((in[18] - minv) << 40) |
			((in[19] - minv) << 60))

	out[6] = uint64(
		((in[19] - minv) >> 4) |

			((in[20] - minv) << 16) |
			((in[21] - minv) << 36) |
			((in[22] - minv) << 56))

	out[7] = uint64(
		((in[22] - minv) >> 8) |

			((in[23] - minv) << 12) |
			((in[24] - minv) << 32) |
			((in[25] - minv) << 52))

	out[8] = uint64(
		((in[25] - minv) >> 12) |

			((in[26] - minv) << 8) |
			((in[27] - minv) << 28) |
			((in[28] - minv) << 48))

	out[9] = uint64(
		((in[28] - minv) >> 16) |

			((in[29] - minv) << 4) |
			((in[30] - minv) << 24) |
			((in[31] - minv) << 44))

	out[10] = uint64(
		((in[31] - minv) >> 20) |

			((in[32] - minv) << 0) |
			((in[33] - minv) << 20) |
			((in[34] - minv) << 40) |
			((in[35] - minv) << 60))

	out[11] = uint64(
		((in[35] - minv) >> 4) |

			((in[36] - minv) << 16) |
			((in[37] - minv) << 36) |
			((in[38] - minv) << 56))

	out[12] = uint64(
		((in[38] - minv) >> 8) |

			((in[39] - minv) << 12) |
			((in[40] - minv) << 32) |
			((in[41] - minv) << 52))

	out[13] = uint64(
		((in[41] - minv) >> 12) |

			((in[42] - minv) << 8) |
			((in[43] - minv) << 28) |
			((in[44] - minv) << 48))

	out[14] = uint64(
		((in[44] - minv) >> 16) |

			((in[45] - minv) << 4) |
			((in[46] - minv) << 24) |
			((in[47] - minv) << 44))

	out[15] = uint64(
		((in[47] - minv) >> 20) |

			((in[48] - minv) << 0) |
			((in[49] - minv) << 20) |
			((in[50] - minv) << 40) |
			((in[51] - minv) << 60))

	out[16] = uint64(
		((in[51] - minv) >> 4) |

			((in[52] - minv) << 16) |
			((in[53] - minv) << 36) |
			((in[54] - minv) << 56))

	out[17] = uint64(
		((in[54] - minv) >> 8) |

			((in[55] - minv) << 12) |
			((in[56] - minv) << 32) |
			((in[57] - minv) << 52))

	out[18] = uint64(
		((in[57] - minv) >> 12) |

			((in[58] - minv) << 8) |
			((in[59] - minv) << 28) |
			((in[60] - minv) << 48))

	out[19] = uint64(
		((in[60] - minv) >> 16) |

			((in[61] - minv) << 4) |
			((in[62] - minv) << 24) |
			((in[63] - minv) << 44))

}
func bp64_21[T uint64 | int64](in *[64]T, out *[21]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 21) |
			((in[2] - minv) << 42) |
			((in[3] - minv) << 63))

	out[1] = uint64(
		((in[3] - minv) >> 1) |

			((in[4] - minv) << 20) |
			((in[5] - minv) << 41) |
			((in[6] - minv) << 62))

	out[2] = uint64(
		((in[6] - minv) >> 2) |

			((in[7] - minv) << 19) |
			((in[8] - minv) << 40) |
			((in[9] - minv) << 61))

	out[3] = uint64(
		((in[9] - minv) >> 3) |

			((in[10] - minv) << 18) |
			((in[11] - minv) << 39) |
			((in[12] - minv) << 60))

	out[4] = uint64(
		((in[12] - minv) >> 4) |

			((in[13] - minv) << 17) |
			((in[14] - minv) << 38) |
			((in[15] - minv) << 59))

	out[5] = uint64(
		((in[15] - minv) >> 5) |

			((in[16] - minv) << 16) |
			((in[17] - minv) << 37) |
			((in[18] - minv) << 58))

	out[6] = uint64(
		((in[18] - minv) >> 6) |

			((in[19] - minv) << 15) |
			((in[20] - minv) << 36) |
			((in[21] - minv) << 57))

	out[7] = uint64(
		((in[21] - minv) >> 7) |

			((in[22] - minv) << 14) |
			((in[23] - minv) << 35) |
			((in[24] - minv) << 56))

	out[8] = uint64(
		((in[24] - minv) >> 8) |

			((in[25] - minv) << 13) |
			((in[26] - minv) << 34) |
			((in[27] - minv) << 55))

	out[9] = uint64(
		((in[27] - minv) >> 9) |

			((in[28] - minv) << 12) |
			((in[29] - minv) << 33) |
			((in[30] - minv) << 54))

	out[10] = uint64(
		((in[30] - minv) >> 10) |

			((in[31] - minv) << 11) |
			((in[32] - minv) << 32) |
			((in[33] - minv) << 53))

	out[11] = uint64(
		((in[33] - minv) >> 11) |

			((in[34] - minv) << 10) |
			((in[35] - minv) << 31) |
			((in[36] - minv) << 52))

	out[12] = uint64(
		((in[36] - minv) >> 12) |

			((in[37] - minv) << 9) |
			((in[38] - minv) << 30) |
			((in[39] - minv) << 51))

	out[13] = uint64(
		((in[39] - minv) >> 13) |

			((in[40] - minv) << 8) |
			((in[41] - minv) << 29) |
			((in[42] - minv) << 50))

	out[14] = uint64(
		((in[42] - minv) >> 14) |

			((in[43] - minv) << 7) |
			((in[44] - minv) << 28) |
			((in[45] - minv) << 49))

	out[15] = uint64(
		((in[45] - minv) >> 15) |

			((in[46] - minv) << 6) |
			((in[47] - minv) << 27) |
			((in[48] - minv) << 48))

	out[16] = uint64(
		((in[48] - minv) >> 16) |

			((in[49] - minv) << 5) |
			((in[50] - minv) << 26) |
			((in[51] - minv) << 47))

	out[17] = uint64(
		((in[51] - minv) >> 17) |

			((in[52] - minv) << 4) |
			((in[53] - minv) << 25) |
			((in[54] - minv) << 46))

	out[18] = uint64(
		((in[54] - minv) >> 18) |

			((in[55] - minv) << 3) |
			((in[56] - minv) << 24) |
			((in[57] - minv) << 45))

	out[19] = uint64(
		((in[57] - minv) >> 19) |

			((in[58] - minv) << 2) |
			((in[59] - minv) << 23) |
			((in[60] - minv) << 44))

	out[20] = uint64(
		((in[60] - minv) >> 20) |

			((in[61] - minv) << 1) |
			((in[62] - minv) << 22) |
			((in[63] - minv) << 43))

}
func bp64_22[T uint64 | int64](in *[64]T, out *[22]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 22) |
			((in[2] - minv) << 44))

	out[1] = uint64(
		((in[2] - minv) >> 20) |

			((in[3] - minv) << 2) |
			((in[4] - minv) << 24) |
			((in[5] - minv) << 46))

	out[2] = uint64(
		((in[5] - minv) >> 18) |

			((in[6] - minv) << 4) |
			((in[7] - minv) << 26) |
			((in[8] - minv) << 48))

	out[3] = uint64(
		((in[8] - minv) >> 16) |

			((in[9] - minv) << 6) |
			((in[10] - minv) << 28) |
			((in[11] - minv) << 50))

	out[4] = uint64(
		((in[11] - minv) >> 14) |

			((in[12] - minv) << 8) |
			((in[13] - minv) << 30) |
			((in[14] - minv) << 52))

	out[5] = uint64(
		((in[14] - minv) >> 12) |

			((in[15] - minv) << 10) |
			((in[16] - minv) << 32) |
			((in[17] - minv) << 54))

	out[6] = uint64(
		((in[17] - minv) >> 10) |

			((in[18] - minv) << 12) |
			((in[19] - minv) << 34) |
			((in[20] - minv) << 56))

	out[7] = uint64(
		((in[20] - minv) >> 8) |

			((in[21] - minv) << 14) |
			((in[22] - minv) << 36) |
			((in[23] - minv) << 58))

	out[8] = uint64(
		((in[23] - minv) >> 6) |

			((in[24] - minv) << 16) |
			((in[25] - minv) << 38) |
			((in[26] - minv) << 60))

	out[9] = uint64(
		((in[26] - minv) >> 4) |

			((in[27] - minv) << 18) |
			((in[28] - minv) << 40) |
			((in[29] - minv) << 62))

	out[10] = uint64(
		((in[29] - minv) >> 2) |

			((in[30] - minv) << 20) |
			((in[31] - minv) << 42))

	out[11] = uint64(
		((in[31] - minv) >> 22) |

			((in[32] - minv) << 0) |
			((in[33] - minv) << 22) |
			((in[34] - minv) << 44))

	out[12] = uint64(
		((in[34] - minv) >> 20) |

			((in[35] - minv) << 2) |
			((in[36] - minv) << 24) |
			((in[37] - minv) << 46))

	out[13] = uint64(
		((in[37] - minv) >> 18) |

			((in[38] - minv) << 4) |
			((in[39] - minv) << 26) |
			((in[40] - minv) << 48))

	out[14] = uint64(
		((in[40] - minv) >> 16) |

			((in[41] - minv) << 6) |
			((in[42] - minv) << 28) |
			((in[43] - minv) << 50))

	out[15] = uint64(
		((in[43] - minv) >> 14) |

			((in[44] - minv) << 8) |
			((in[45] - minv) << 30) |
			((in[46] - minv) << 52))

	out[16] = uint64(
		((in[46] - minv) >> 12) |

			((in[47] - minv) << 10) |
			((in[48] - minv) << 32) |
			((in[49] - minv) << 54))

	out[17] = uint64(
		((in[49] - minv) >> 10) |

			((in[50] - minv) << 12) |
			((in[51] - minv) << 34) |
			((in[52] - minv) << 56))

	out[18] = uint64(
		((in[52] - minv) >> 8) |

			((in[53] - minv) << 14) |
			((in[54] - minv) << 36) |
			((in[55] - minv) << 58))

	out[19] = uint64(
		((in[55] - minv) >> 6) |

			((in[56] - minv) << 16) |
			((in[57] - minv) << 38) |
			((in[58] - minv) << 60))

	out[20] = uint64(
		((in[58] - minv) >> 4) |

			((in[59] - minv) << 18) |
			((in[60] - minv) << 40) |
			((in[61] - minv) << 62))

	out[21] = uint64(
		((in[61] - minv) >> 2) |

			((in[62] - minv) << 20) |
			((in[63] - minv) << 42))

}
func bp64_23[T uint64 | int64](in *[64]T, out *[23]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 23) |
			((in[2] - minv) << 46))

	out[1] = uint64(
		((in[2] - minv) >> 18) |

			((in[3] - minv) << 5) |
			((in[4] - minv) << 28) |
			((in[5] - minv) << 51))

	out[2] = uint64(
		((in[5] - minv) >> 13) |

			((in[6] - minv) << 10) |
			((in[7] - minv) << 33) |
			((in[8] - minv) << 56))

	out[3] = uint64(
		((in[8] - minv) >> 8) |

			((in[9] - minv) << 15) |
			((in[10] - minv) << 38) |
			((in[11] - minv) << 61))

	out[4] = uint64(
		((in[11] - minv) >> 3) |

			((in[12] - minv) << 20) |
			((in[13] - minv) << 43))

	out[5] = uint64(
		((in[13] - minv) >> 21) |

			((in[14] - minv) << 2) |
			((in[15] - minv) << 25) |
			((in[16] - minv) << 48))

	out[6] = uint64(
		((in[16] - minv) >> 16) |

			((in[17] - minv) << 7) |
			((in[18] - minv) << 30) |
			((in[19] - minv) << 53))

	out[7] = uint64(
		((in[19] - minv) >> 11) |

			((in[20] - minv) << 12) |
			((in[21] - minv) << 35) |
			((in[22] - minv) << 58))

	out[8] = uint64(
		((in[22] - minv) >> 6) |

			((in[23] - minv) << 17) |
			((in[24] - minv) << 40) |
			((in[25] - minv) << 63))

	out[9] = uint64(
		((in[25] - minv) >> 1) |

			((in[26] - minv) << 22) |
			((in[27] - minv) << 45))

	out[10] = uint64(
		((in[27] - minv) >> 19) |

			((in[28] - minv) << 4) |
			((in[29] - minv) << 27) |
			((in[30] - minv) << 50))

	out[11] = uint64(
		((in[30] - minv) >> 14) |

			((in[31] - minv) << 9) |
			((in[32] - minv) << 32) |
			((in[33] - minv) << 55))

	out[12] = uint64(
		((in[33] - minv) >> 9) |

			((in[34] - minv) << 14) |
			((in[35] - minv) << 37) |
			((in[36] - minv) << 60))

	out[13] = uint64(
		((in[36] - minv) >> 4) |

			((in[37] - minv) << 19) |
			((in[38] - minv) << 42))

	out[14] = uint64(
		((in[38] - minv) >> 22) |

			((in[39] - minv) << 1) |
			((in[40] - minv) << 24) |
			((in[41] - minv) << 47))

	out[15] = uint64(
		((in[41] - minv) >> 17) |

			((in[42] - minv) << 6) |
			((in[43] - minv) << 29) |
			((in[44] - minv) << 52))

	out[16] = uint64(
		((in[44] - minv) >> 12) |

			((in[45] - minv) << 11) |
			((in[46] - minv) << 34) |
			((in[47] - minv) << 57))

	out[17] = uint64(
		((in[47] - minv) >> 7) |

			((in[48] - minv) << 16) |
			((in[49] - minv) << 39) |
			((in[50] - minv) << 62))

	out[18] = uint64(
		((in[50] - minv) >> 2) |

			((in[51] - minv) << 21) |
			((in[52] - minv) << 44))

	out[19] = uint64(
		((in[52] - minv) >> 20) |

			((in[53] - minv) << 3) |
			((in[54] - minv) << 26) |
			((in[55] - minv) << 49))

	out[20] = uint64(
		((in[55] - minv) >> 15) |

			((in[56] - minv) << 8) |
			((in[57] - minv) << 31) |
			((in[58] - minv) << 54))

	out[21] = uint64(
		((in[58] - minv) >> 10) |

			((in[59] - minv) << 13) |
			((in[60] - minv) << 36) |
			((in[61] - minv) << 59))

	out[22] = uint64(
		((in[61] - minv) >> 5) |

			((in[62] - minv) << 18) |
			((in[63] - minv) << 41))

}
func bp64_24[T uint64 | int64](in *[64]T, out *[24]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 24) |
			((in[2] - minv) << 48))

	out[1] = uint64(
		((in[2] - minv) >> 16) |

			((in[3] - minv) << 8) |
			((in[4] - minv) << 32) |
			((in[5] - minv) << 56))

	out[2] = uint64(
		((in[5] - minv) >> 8) |

			((in[6] - minv) << 16) |
			((in[7] - minv) << 40))

	out[3] = uint64(
		((in[7] - minv) >> 24) |

			((in[8] - minv) << 0) |
			((in[9] - minv) << 24) |
			((in[10] - minv) << 48))

	out[4] = uint64(
		((in[10] - minv) >> 16) |

			((in[11] - minv) << 8) |
			((in[12] - minv) << 32) |
			((in[13] - minv) << 56))

	out[5] = uint64(
		((in[13] - minv) >> 8) |

			((in[14] - minv) << 16) |
			((in[15] - minv) << 40))

	out[6] = uint64(
		((in[15] - minv) >> 24) |

			((in[16] - minv) << 0) |
			((in[17] - minv) << 24) |
			((in[18] - minv) << 48))

	out[7] = uint64(
		((in[18] - minv) >> 16) |

			((in[19] - minv) << 8) |
			((in[20] - minv) << 32) |
			((in[21] - minv) << 56))

	out[8] = uint64(
		((in[21] - minv) >> 8) |

			((in[22] - minv) << 16) |
			((in[23] - minv) << 40))

	out[9] = uint64(
		((in[23] - minv) >> 24) |

			((in[24] - minv) << 0) |
			((in[25] - minv) << 24) |
			((in[26] - minv) << 48))

	out[10] = uint64(
		((in[26] - minv) >> 16) |

			((in[27] - minv) << 8) |
			((in[28] - minv) << 32) |
			((in[29] - minv) << 56))

	out[11] = uint64(
		((in[29] - minv) >> 8) |

			((in[30] - minv) << 16) |
			((in[31] - minv) << 40))

	out[12] = uint64(
		((in[31] - minv) >> 24) |

			((in[32] - minv) << 0) |
			((in[33] - minv) << 24) |
			((in[34] - minv) << 48))

	out[13] = uint64(
		((in[34] - minv) >> 16) |

			((in[35] - minv) << 8) |
			((in[36] - minv) << 32) |
			((in[37] - minv) << 56))

	out[14] = uint64(
		((in[37] - minv) >> 8) |

			((in[38] - minv) << 16) |
			((in[39] - minv) << 40))

	out[15] = uint64(
		((in[39] - minv) >> 24) |

			((in[40] - minv) << 0) |
			((in[41] - minv) << 24) |
			((in[42] - minv) << 48))

	out[16] = uint64(
		((in[42] - minv) >> 16) |

			((in[43] - minv) << 8) |
			((in[44] - minv) << 32) |
			((in[45] - minv) << 56))

	out[17] = uint64(
		((in[45] - minv) >> 8) |

			((in[46] - minv) << 16) |
			((in[47] - minv) << 40))

	out[18] = uint64(
		((in[47] - minv) >> 24) |

			((in[48] - minv) << 0) |
			((in[49] - minv) << 24) |
			((in[50] - minv) << 48))

	out[19] = uint64(
		((in[50] - minv) >> 16) |

			((in[51] - minv) << 8) |
			((in[52] - minv) << 32) |
			((in[53] - minv) << 56))

	out[20] = uint64(
		((in[53] - minv) >> 8) |

			((in[54] - minv) << 16) |
			((in[55] - minv) << 40))

	out[21] = uint64(
		((in[55] - minv) >> 24) |

			((in[56] - minv) << 0) |
			((in[57] - minv) << 24) |
			((in[58] - minv) << 48))

	out[22] = uint64(
		((in[58] - minv) >> 16) |

			((in[59] - minv) << 8) |
			((in[60] - minv) << 32) |
			((in[61] - minv) << 56))

	out[23] = uint64(
		((in[61] - minv) >> 8) |

			((in[62] - minv) << 16) |
			((in[63] - minv) << 40))

}
func bp64_25[T uint64 | int64](in *[64]T, out *[25]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 25) |
			((in[2] - minv) << 50))

	out[1] = uint64(
		((in[2] - minv) >> 14) |

			((in[3] - minv) << 11) |
			((in[4] - minv) << 36) |
			((in[5] - minv) << 61))

	out[2] = uint64(
		((in[5] - minv) >> 3) |

			((in[6] - minv) << 22) |
			((in[7] - minv) << 47))

	out[3] = uint64(
		((in[7] - minv) >> 17) |

			((in[8] - minv) << 8) |
			((in[9] - minv) << 33) |
			((in[10] - minv) << 58))

	out[4] = uint64(
		((in[10] - minv) >> 6) |

			((in[11] - minv) << 19) |
			((in[12] - minv) << 44))

	out[5] = uint64(
		((in[12] - minv) >> 20) |

			((in[13] - minv) << 5) |
			((in[14] - minv) << 30) |
			((in[15] - minv) << 55))

	out[6] = uint64(
		((in[15] - minv) >> 9) |

			((in[16] - minv) << 16) |
			((in[17] - minv) << 41))

	out[7] = uint64(
		((in[17] - minv) >> 23) |

			((in[18] - minv) << 2) |
			((in[19] - minv) << 27) |
			((in[20] - minv) << 52))

	out[8] = uint64(
		((in[20] - minv) >> 12) |

			((in[21] - minv) << 13) |
			((in[22] - minv) << 38) |
			((in[23] - minv) << 63))

	out[9] = uint64(
		((in[23] - minv) >> 1) |

			((in[24] - minv) << 24) |
			((in[25] - minv) << 49))

	out[10] = uint64(
		((in[25] - minv) >> 15) |

			((in[26] - minv) << 10) |
			((in[27] - minv) << 35) |
			((in[28] - minv) << 60))

	out[11] = uint64(
		((in[28] - minv) >> 4) |

			((in[29] - minv) << 21) |
			((in[30] - minv) << 46))

	out[12] = uint64(
		((in[30] - minv) >> 18) |

			((in[31] - minv) << 7) |
			((in[32] - minv) << 32) |
			((in[33] - minv) << 57))

	out[13] = uint64(
		((in[33] - minv) >> 7) |

			((in[34] - minv) << 18) |
			((in[35] - minv) << 43))

	out[14] = uint64(
		((in[35] - minv) >> 21) |

			((in[36] - minv) << 4) |
			((in[37] - minv) << 29) |
			((in[38] - minv) << 54))

	out[15] = uint64(
		((in[38] - minv) >> 10) |

			((in[39] - minv) << 15) |
			((in[40] - minv) << 40))

	out[16] = uint64(
		((in[40] - minv) >> 24) |

			((in[41] - minv) << 1) |
			((in[42] - minv) << 26) |
			((in[43] - minv) << 51))

	out[17] = uint64(
		((in[43] - minv) >> 13) |

			((in[44] - minv) << 12) |
			((in[45] - minv) << 37) |
			((in[46] - minv) << 62))

	out[18] = uint64(
		((in[46] - minv) >> 2) |

			((in[47] - minv) << 23) |
			((in[48] - minv) << 48))

	out[19] = uint64(
		((in[48] - minv) >> 16) |

			((in[49] - minv) << 9) |
			((in[50] - minv) << 34) |
			((in[51] - minv) << 59))

	out[20] = uint64(
		((in[51] - minv) >> 5) |

			((in[52] - minv) << 20) |
			((in[53] - minv) << 45))

	out[21] = uint64(
		((in[53] - minv) >> 19) |

			((in[54] - minv) << 6) |
			((in[55] - minv) << 31) |
			((in[56] - minv) << 56))

	out[22] = uint64(
		((in[56] - minv) >> 8) |

			((in[57] - minv) << 17) |
			((in[58] - minv) << 42))

	out[23] = uint64(
		((in[58] - minv) >> 22) |

			((in[59] - minv) << 3) |
			((in[60] - minv) << 28) |
			((in[61] - minv) << 53))

	out[24] = uint64(
		((in[61] - minv) >> 11) |

			((in[62] - minv) << 14) |
			((in[63] - minv) << 39))

}
func bp64_26[T uint64 | int64](in *[64]T, out *[26]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 26) |
			((in[2] - minv) << 52))

	out[1] = uint64(
		((in[2] - minv) >> 12) |

			((in[3] - minv) << 14) |
			((in[4] - minv) << 40))

	out[2] = uint64(
		((in[4] - minv) >> 24) |

			((in[5] - minv) << 2) |
			((in[6] - minv) << 28) |
			((in[7] - minv) << 54))

	out[3] = uint64(
		((in[7] - minv) >> 10) |

			((in[8] - minv) << 16) |
			((in[9] - minv) << 42))

	out[4] = uint64(
		((in[9] - minv) >> 22) |

			((in[10] - minv) << 4) |
			((in[11] - minv) << 30) |
			((in[12] - minv) << 56))

	out[5] = uint64(
		((in[12] - minv) >> 8) |

			((in[13] - minv) << 18) |
			((in[14] - minv) << 44))

	out[6] = uint64(
		((in[14] - minv) >> 20) |

			((in[15] - minv) << 6) |
			((in[16] - minv) << 32) |
			((in[17] - minv) << 58))

	out[7] = uint64(
		((in[17] - minv) >> 6) |

			((in[18] - minv) << 20) |
			((in[19] - minv) << 46))

	out[8] = uint64(
		((in[19] - minv) >> 18) |

			((in[20] - minv) << 8) |
			((in[21] - minv) << 34) |
			((in[22] - minv) << 60))

	out[9] = uint64(
		((in[22] - minv) >> 4) |

			((in[23] - minv) << 22) |
			((in[24] - minv) << 48))

	out[10] = uint64(
		((in[24] - minv) >> 16) |

			((in[25] - minv) << 10) |
			((in[26] - minv) << 36) |
			((in[27] - minv) << 62))

	out[11] = uint64(
		((in[27] - minv) >> 2) |

			((in[28] - minv) << 24) |
			((in[29] - minv) << 50))

	out[12] = uint64(
		((in[29] - minv) >> 14) |

			((in[30] - minv) << 12) |
			((in[31] - minv) << 38))

	out[13] = uint64(
		((in[31] - minv) >> 26) |

			((in[32] - minv) << 0) |
			((in[33] - minv) << 26) |
			((in[34] - minv) << 52))

	out[14] = uint64(
		((in[34] - minv) >> 12) |

			((in[35] - minv) << 14) |
			((in[36] - minv) << 40))

	out[15] = uint64(
		((in[36] - minv) >> 24) |

			((in[37] - minv) << 2) |
			((in[38] - minv) << 28) |
			((in[39] - minv) << 54))

	out[16] = uint64(
		((in[39] - minv) >> 10) |

			((in[40] - minv) << 16) |
			((in[41] - minv) << 42))

	out[17] = uint64(
		((in[41] - minv) >> 22) |

			((in[42] - minv) << 4) |
			((in[43] - minv) << 30) |
			((in[44] - minv) << 56))

	out[18] = uint64(
		((in[44] - minv) >> 8) |

			((in[45] - minv) << 18) |
			((in[46] - minv) << 44))

	out[19] = uint64(
		((in[46] - minv) >> 20) |

			((in[47] - minv) << 6) |
			((in[48] - minv) << 32) |
			((in[49] - minv) << 58))

	out[20] = uint64(
		((in[49] - minv) >> 6) |

			((in[50] - minv) << 20) |
			((in[51] - minv) << 46))

	out[21] = uint64(
		((in[51] - minv) >> 18) |

			((in[52] - minv) << 8) |
			((in[53] - minv) << 34) |
			((in[54] - minv) << 60))

	out[22] = uint64(
		((in[54] - minv) >> 4) |

			((in[55] - minv) << 22) |
			((in[56] - minv) << 48))

	out[23] = uint64(
		((in[56] - minv) >> 16) |

			((in[57] - minv) << 10) |
			((in[58] - minv) << 36) |
			((in[59] - minv) << 62))

	out[24] = uint64(
		((in[59] - minv) >> 2) |

			((in[60] - minv) << 24) |
			((in[61] - minv) << 50))

	out[25] = uint64(
		((in[61] - minv) >> 14) |

			((in[62] - minv) << 12) |
			((in[63] - minv) << 38))

}
func bp64_27[T uint64 | int64](in *[64]T, out *[27]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 27) |
			((in[2] - minv) << 54))

	out[1] = uint64(
		((in[2] - minv) >> 10) |

			((in[3] - minv) << 17) |
			((in[4] - minv) << 44))

	out[2] = uint64(
		((in[4] - minv) >> 20) |

			((in[5] - minv) << 7) |
			((in[6] - minv) << 34) |
			((in[7] - minv) << 61))

	out[3] = uint64(
		((in[7] - minv) >> 3) |

			((in[8] - minv) << 24) |
			((in[9] - minv) << 51))

	out[4] = uint64(
		((in[9] - minv) >> 13) |

			((in[10] - minv) << 14) |
			((in[11] - minv) << 41))

	out[5] = uint64(
		((in[11] - minv) >> 23) |

			((in[12] - minv) << 4) |
			((in[13] - minv) << 31) |
			((in[14] - minv) << 58))

	out[6] = uint64(
		((in[14] - minv) >> 6) |

			((in[15] - minv) << 21) |
			((in[16] - minv) << 48))

	out[7] = uint64(
		((in[16] - minv) >> 16) |

			((in[17] - minv) << 11) |
			((in[18] - minv) << 38))

	out[8] = uint64(
		((in[18] - minv) >> 26) |

			((in[19] - minv) << 1) |
			((in[20] - minv) << 28) |
			((in[21] - minv) << 55))

	out[9] = uint64(
		((in[21] - minv) >> 9) |

			((in[22] - minv) << 18) |
			((in[23] - minv) << 45))

	out[10] = uint64(
		((in[23] - minv) >> 19) |

			((in[24] - minv) << 8) |
			((in[25] - minv) << 35) |
			((in[26] - minv) << 62))

	out[11] = uint64(
		((in[26] - minv) >> 2) |

			((in[27] - minv) << 25) |
			((in[28] - minv) << 52))

	out[12] = uint64(
		((in[28] - minv) >> 12) |

			((in[29] - minv) << 15) |
			((in[30] - minv) << 42))

	out[13] = uint64(
		((in[30] - minv) >> 22) |

			((in[31] - minv) << 5) |
			((in[32] - minv) << 32) |
			((in[33] - minv) << 59))

	out[14] = uint64(
		((in[33] - minv) >> 5) |

			((in[34] - minv) << 22) |
			((in[35] - minv) << 49))

	out[15] = uint64(
		((in[35] - minv) >> 15) |

			((in[36] - minv) << 12) |
			((in[37] - minv) << 39))

	out[16] = uint64(
		((in[37] - minv) >> 25) |

			((in[38] - minv) << 2) |
			((in[39] - minv) << 29) |
			((in[40] - minv) << 56))

	out[17] = uint64(
		((in[40] - minv) >> 8) |

			((in[41] - minv) << 19) |
			((in[42] - minv) << 46))

	out[18] = uint64(
		((in[42] - minv) >> 18) |

			((in[43] - minv) << 9) |
			((in[44] - minv) << 36) |
			((in[45] - minv) << 63))

	out[19] = uint64(
		((in[45] - minv) >> 1) |

			((in[46] - minv) << 26) |
			((in[47] - minv) << 53))

	out[20] = uint64(
		((in[47] - minv) >> 11) |

			((in[48] - minv) << 16) |
			((in[49] - minv) << 43))

	out[21] = uint64(
		((in[49] - minv) >> 21) |

			((in[50] - minv) << 6) |
			((in[51] - minv) << 33) |
			((in[52] - minv) << 60))

	out[22] = uint64(
		((in[52] - minv) >> 4) |

			((in[53] - minv) << 23) |
			((in[54] - minv) << 50))

	out[23] = uint64(
		((in[54] - minv) >> 14) |

			((in[55] - minv) << 13) |
			((in[56] - minv) << 40))

	out[24] = uint64(
		((in[56] - minv) >> 24) |

			((in[57] - minv) << 3) |
			((in[58] - minv) << 30) |
			((in[59] - minv) << 57))

	out[25] = uint64(
		((in[59] - minv) >> 7) |

			((in[60] - minv) << 20) |
			((in[61] - minv) << 47))

	out[26] = uint64(
		((in[61] - minv) >> 17) |

			((in[62] - minv) << 10) |
			((in[63] - minv) << 37))

}
func bp64_28[T uint64 | int64](in *[64]T, out *[28]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 28) |
			((in[2] - minv) << 56))

	out[1] = uint64(
		((in[2] - minv) >> 8) |

			((in[3] - minv) << 20) |
			((in[4] - minv) << 48))

	out[2] = uint64(
		((in[4] - minv) >> 16) |

			((in[5] - minv) << 12) |
			((in[6] - minv) << 40))

	out[3] = uint64(
		((in[6] - minv) >> 24) |

			((in[7] - minv) << 4) |
			((in[8] - minv) << 32) |
			((in[9] - minv) << 60))

	out[4] = uint64(
		((in[9] - minv) >> 4) |

			((in[10] - minv) << 24) |
			((in[11] - minv) << 52))

	out[5] = uint64(
		((in[11] - minv) >> 12) |

			((in[12] - minv) << 16) |
			((in[13] - minv) << 44))

	out[6] = uint64(
		((in[13] - minv) >> 20) |

			((in[14] - minv) << 8) |
			((in[15] - minv) << 36))

	out[7] = uint64(
		((in[15] - minv) >> 28) |

			((in[16] - minv) << 0) |
			((in[17] - minv) << 28) |
			((in[18] - minv) << 56))

	out[8] = uint64(
		((in[18] - minv) >> 8) |

			((in[19] - minv) << 20) |
			((in[20] - minv) << 48))

	out[9] = uint64(
		((in[20] - minv) >> 16) |

			((in[21] - minv) << 12) |
			((in[22] - minv) << 40))

	out[10] = uint64(
		((in[22] - minv) >> 24) |

			((in[23] - minv) << 4) |
			((in[24] - minv) << 32) |
			((in[25] - minv) << 60))

	out[11] = uint64(
		((in[25] - minv) >> 4) |

			((in[26] - minv) << 24) |
			((in[27] - minv) << 52))

	out[12] = uint64(
		((in[27] - minv) >> 12) |

			((in[28] - minv) << 16) |
			((in[29] - minv) << 44))

	out[13] = uint64(
		((in[29] - minv) >> 20) |

			((in[30] - minv) << 8) |
			((in[31] - minv) << 36))

	out[14] = uint64(
		((in[31] - minv) >> 28) |

			((in[32] - minv) << 0) |
			((in[33] - minv) << 28) |
			((in[34] - minv) << 56))

	out[15] = uint64(
		((in[34] - minv) >> 8) |

			((in[35] - minv) << 20) |
			((in[36] - minv) << 48))

	out[16] = uint64(
		((in[36] - minv) >> 16) |

			((in[37] - minv) << 12) |
			((in[38] - minv) << 40))

	out[17] = uint64(
		((in[38] - minv) >> 24) |

			((in[39] - minv) << 4) |
			((in[40] - minv) << 32) |
			((in[41] - minv) << 60))

	out[18] = uint64(
		((in[41] - minv) >> 4) |

			((in[42] - minv) << 24) |
			((in[43] - minv) << 52))

	out[19] = uint64(
		((in[43] - minv) >> 12) |

			((in[44] - minv) << 16) |
			((in[45] - minv) << 44))

	out[20] = uint64(
		((in[45] - minv) >> 20) |

			((in[46] - minv) << 8) |
			((in[47] - minv) << 36))

	out[21] = uint64(
		((in[47] - minv) >> 28) |

			((in[48] - minv) << 0) |
			((in[49] - minv) << 28) |
			((in[50] - minv) << 56))

	out[22] = uint64(
		((in[50] - minv) >> 8) |

			((in[51] - minv) << 20) |
			((in[52] - minv) << 48))

	out[23] = uint64(
		((in[52] - minv) >> 16) |

			((in[53] - minv) << 12) |
			((in[54] - minv) << 40))

	out[24] = uint64(
		((in[54] - minv) >> 24) |

			((in[55] - minv) << 4) |
			((in[56] - minv) << 32) |
			((in[57] - minv) << 60))

	out[25] = uint64(
		((in[57] - minv) >> 4) |

			((in[58] - minv) << 24) |
			((in[59] - minv) << 52))

	out[26] = uint64(
		((in[59] - minv) >> 12) |

			((in[60] - minv) << 16) |
			((in[61] - minv) << 44))

	out[27] = uint64(
		((in[61] - minv) >> 20) |

			((in[62] - minv) << 8) |
			((in[63] - minv) << 36))

}
func bp64_29[T uint64 | int64](in *[64]T, out *[29]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 29) |
			((in[2] - minv) << 58))

	out[1] = uint64(
		((in[2] - minv) >> 6) |

			((in[3] - minv) << 23) |
			((in[4] - minv) << 52))

	out[2] = uint64(
		((in[4] - minv) >> 12) |

			((in[5] - minv) << 17) |
			((in[6] - minv) << 46))

	out[3] = uint64(
		((in[6] - minv) >> 18) |

			((in[7] - minv) << 11) |
			((in[8] - minv) << 40))

	out[4] = uint64(
		((in[8] - minv) >> 24) |

			((in[9] - minv) << 5) |
			((in[10] - minv) << 34) |
			((in[11] - minv) << 63))

	out[5] = uint64(
		((in[11] - minv) >> 1) |

			((in[12] - minv) << 28) |
			((in[13] - minv) << 57))

	out[6] = uint64(
		((in[13] - minv) >> 7) |

			((in[14] - minv) << 22) |
			((in[15] - minv) << 51))

	out[7] = uint64(
		((in[15] - minv) >> 13) |

			((in[16] - minv) << 16) |
			((in[17] - minv) << 45))

	out[8] = uint64(
		((in[17] - minv) >> 19) |

			((in[18] - minv) << 10) |
			((in[19] - minv) << 39))

	out[9] = uint64(
		((in[19] - minv) >> 25) |

			((in[20] - minv) << 4) |
			((in[21] - minv) << 33) |
			((in[22] - minv) << 62))

	out[10] = uint64(
		((in[22] - minv) >> 2) |

			((in[23] - minv) << 27) |
			((in[24] - minv) << 56))

	out[11] = uint64(
		((in[24] - minv) >> 8) |

			((in[25] - minv) << 21) |
			((in[26] - minv) << 50))

	out[12] = uint64(
		((in[26] - minv) >> 14) |

			((in[27] - minv) << 15) |
			((in[28] - minv) << 44))

	out[13] = uint64(
		((in[28] - minv) >> 20) |

			((in[29] - minv) << 9) |
			((in[30] - minv) << 38))

	out[14] = uint64(
		((in[30] - minv) >> 26) |

			((in[31] - minv) << 3) |
			((in[32] - minv) << 32) |
			((in[33] - minv) << 61))

	out[15] = uint64(
		((in[33] - minv) >> 3) |

			((in[34] - minv) << 26) |
			((in[35] - minv) << 55))

	out[16] = uint64(
		((in[35] - minv) >> 9) |

			((in[36] - minv) << 20) |
			((in[37] - minv) << 49))

	out[17] = uint64(
		((in[37] - minv) >> 15) |

			((in[38] - minv) << 14) |
			((in[39] - minv) << 43))

	out[18] = uint64(
		((in[39] - minv) >> 21) |

			((in[40] - minv) << 8) |
			((in[41] - minv) << 37))

	out[19] = uint64(
		((in[41] - minv) >> 27) |

			((in[42] - minv) << 2) |
			((in[43] - minv) << 31) |
			((in[44] - minv) << 60))

	out[20] = uint64(
		((in[44] - minv) >> 4) |

			((in[45] - minv) << 25) |
			((in[46] - minv) << 54))

	out[21] = uint64(
		((in[46] - minv) >> 10) |

			((in[47] - minv) << 19) |
			((in[48] - minv) << 48))

	out[22] = uint64(
		((in[48] - minv) >> 16) |

			((in[49] - minv) << 13) |
			((in[50] - minv) << 42))

	out[23] = uint64(
		((in[50] - minv) >> 22) |

			((in[51] - minv) << 7) |
			((in[52] - minv) << 36))

	out[24] = uint64(
		((in[52] - minv) >> 28) |

			((in[53] - minv) << 1) |
			((in[54] - minv) << 30) |
			((in[55] - minv) << 59))

	out[25] = uint64(
		((in[55] - minv) >> 5) |

			((in[56] - minv) << 24) |
			((in[57] - minv) << 53))

	out[26] = uint64(
		((in[57] - minv) >> 11) |

			((in[58] - minv) << 18) |
			((in[59] - minv) << 47))

	out[27] = uint64(
		((in[59] - minv) >> 17) |

			((in[60] - minv) << 12) |
			((in[61] - minv) << 41))

	out[28] = uint64(
		((in[61] - minv) >> 23) |

			((in[62] - minv) << 6) |
			((in[63] - minv) << 35))

}
func bp64_30[T uint64 | int64](in *[64]T, out *[30]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 30) |
			((in[2] - minv) << 60))

	out[1] = uint64(
		((in[2] - minv) >> 4) |

			((in[3] - minv) << 26) |
			((in[4] - minv) << 56))

	out[2] = uint64(
		((in[4] - minv) >> 8) |

			((in[5] - minv) << 22) |
			((in[6] - minv) << 52))

	out[3] = uint64(
		((in[6] - minv) >> 12) |

			((in[7] - minv) << 18) |
			((in[8] - minv) << 48))

	out[4] = uint64(
		((in[8] - minv) >> 16) |

			((in[9] - minv) << 14) |
			((in[10] - minv) << 44))

	out[5] = uint64(
		((in[10] - minv) >> 20) |

			((in[11] - minv) << 10) |
			((in[12] - minv) << 40))

	out[6] = uint64(
		((in[12] - minv) >> 24) |

			((in[13] - minv) << 6) |
			((in[14] - minv) << 36))

	out[7] = uint64(
		((in[14] - minv) >> 28) |

			((in[15] - minv) << 2) |
			((in[16] - minv) << 32) |
			((in[17] - minv) << 62))

	out[8] = uint64(
		((in[17] - minv) >> 2) |

			((in[18] - minv) << 28) |
			((in[19] - minv) << 58))

	out[9] = uint64(
		((in[19] - minv) >> 6) |

			((in[20] - minv) << 24) |
			((in[21] - minv) << 54))

	out[10] = uint64(
		((in[21] - minv) >> 10) |

			((in[22] - minv) << 20) |
			((in[23] - minv) << 50))

	out[11] = uint64(
		((in[23] - minv) >> 14) |

			((in[24] - minv) << 16) |
			((in[25] - minv) << 46))

	out[12] = uint64(
		((in[25] - minv) >> 18) |

			((in[26] - minv) << 12) |
			((in[27] - minv) << 42))

	out[13] = uint64(
		((in[27] - minv) >> 22) |

			((in[28] - minv) << 8) |
			((in[29] - minv) << 38))

	out[14] = uint64(
		((in[29] - minv) >> 26) |

			((in[30] - minv) << 4) |
			((in[31] - minv) << 34))

	out[15] = uint64(
		((in[31] - minv) >> 30) |

			((in[32] - minv) << 0) |
			((in[33] - minv) << 30) |
			((in[34] - minv) << 60))

	out[16] = uint64(
		((in[34] - minv) >> 4) |

			((in[35] - minv) << 26) |
			((in[36] - minv) << 56))

	out[17] = uint64(
		((in[36] - minv) >> 8) |

			((in[37] - minv) << 22) |
			((in[38] - minv) << 52))

	out[18] = uint64(
		((in[38] - minv) >> 12) |

			((in[39] - minv) << 18) |
			((in[40] - minv) << 48))

	out[19] = uint64(
		((in[40] - minv) >> 16) |

			((in[41] - minv) << 14) |
			((in[42] - minv) << 44))

	out[20] = uint64(
		((in[42] - minv) >> 20) |

			((in[43] - minv) << 10) |
			((in[44] - minv) << 40))

	out[21] = uint64(
		((in[44] - minv) >> 24) |

			((in[45] - minv) << 6) |
			((in[46] - minv) << 36))

	out[22] = uint64(
		((in[46] - minv) >> 28) |

			((in[47] - minv) << 2) |
			((in[48] - minv) << 32) |
			((in[49] - minv) << 62))

	out[23] = uint64(
		((in[49] - minv) >> 2) |

			((in[50] - minv) << 28) |
			((in[51] - minv) << 58))

	out[24] = uint64(
		((in[51] - minv) >> 6) |

			((in[52] - minv) << 24) |
			((in[53] - minv) << 54))

	out[25] = uint64(
		((in[53] - minv) >> 10) |

			((in[54] - minv) << 20) |
			((in[55] - minv) << 50))

	out[26] = uint64(
		((in[55] - minv) >> 14) |

			((in[56] - minv) << 16) |
			((in[57] - minv) << 46))

	out[27] = uint64(
		((in[57] - minv) >> 18) |

			((in[58] - minv) << 12) |
			((in[59] - minv) << 42))

	out[28] = uint64(
		((in[59] - minv) >> 22) |

			((in[60] - minv) << 8) |
			((in[61] - minv) << 38))

	out[29] = uint64(
		((in[61] - minv) >> 26) |

			((in[62] - minv) << 4) |
			((in[63] - minv) << 34))

}
func bp64_31[T uint64 | int64](in *[64]T, out *[31]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 31) |
			((in[2] - minv) << 62))

	out[1] = uint64(
		((in[2] - minv) >> 2) |

			((in[3] - minv) << 29) |
			((in[4] - minv) << 60))

	out[2] = uint64(
		((in[4] - minv) >> 4) |

			((in[5] - minv) << 27) |
			((in[6] - minv) << 58))

	out[3] = uint64(
		((in[6] - minv) >> 6) |

			((in[7] - minv) << 25) |
			((in[8] - minv) << 56))

	out[4] = uint64(
		((in[8] - minv) >> 8) |

			((in[9] - minv) << 23) |
			((in[10] - minv) << 54))

	out[5] = uint64(
		((in[10] - minv) >> 10) |

			((in[11] - minv) << 21) |
			((in[12] - minv) << 52))

	out[6] = uint64(
		((in[12] - minv) >> 12) |

			((in[13] - minv) << 19) |
			((in[14] - minv) << 50))

	out[7] = uint64(
		((in[14] - minv) >> 14) |

			((in[15] - minv) << 17) |
			((in[16] - minv) << 48))

	out[8] = uint64(
		((in[16] - minv) >> 16) |

			((in[17] - minv) << 15) |
			((in[18] - minv) << 46))

	out[9] = uint64(
		((in[18] - minv) >> 18) |

			((in[19] - minv) << 13) |
			((in[20] - minv) << 44))

	out[10] = uint64(
		((in[20] - minv) >> 20) |

			((in[21] - minv) << 11) |
			((in[22] - minv) << 42))

	out[11] = uint64(
		((in[22] - minv) >> 22) |

			((in[23] - minv) << 9) |
			((in[24] - minv) << 40))

	out[12] = uint64(
		((in[24] - minv) >> 24) |

			((in[25] - minv) << 7) |
			((in[26] - minv) << 38))

	out[13] = uint64(
		((in[26] - minv) >> 26) |

			((in[27] - minv) << 5) |
			((in[28] - minv) << 36))

	out[14] = uint64(
		((in[28] - minv) >> 28) |

			((in[29] - minv) << 3) |
			((in[30] - minv) << 34))

	out[15] = uint64(
		((in[30] - minv) >> 30) |

			((in[31] - minv) << 1) |
			((in[32] - minv) << 32) |
			((in[33] - minv) << 63))

	out[16] = uint64(
		((in[33] - minv) >> 1) |

			((in[34] - minv) << 30) |
			((in[35] - minv) << 61))

	out[17] = uint64(
		((in[35] - minv) >> 3) |

			((in[36] - minv) << 28) |
			((in[37] - minv) << 59))

	out[18] = uint64(
		((in[37] - minv) >> 5) |

			((in[38] - minv) << 26) |
			((in[39] - minv) << 57))

	out[19] = uint64(
		((in[39] - minv) >> 7) |

			((in[40] - minv) << 24) |
			((in[41] - minv) << 55))

	out[20] = uint64(
		((in[41] - minv) >> 9) |

			((in[42] - minv) << 22) |
			((in[43] - minv) << 53))

	out[21] = uint64(
		((in[43] - minv) >> 11) |

			((in[44] - minv) << 20) |
			((in[45] - minv) << 51))

	out[22] = uint64(
		((in[45] - minv) >> 13) |

			((in[46] - minv) << 18) |
			((in[47] - minv) << 49))

	out[23] = uint64(
		((in[47] - minv) >> 15) |

			((in[48] - minv) << 16) |
			((in[49] - minv) << 47))

	out[24] = uint64(
		((in[49] - minv) >> 17) |

			((in[50] - minv) << 14) |
			((in[51] - minv) << 45))

	out[25] = uint64(
		((in[51] - minv) >> 19) |

			((in[52] - minv) << 12) |
			((in[53] - minv) << 43))

	out[26] = uint64(
		((in[53] - minv) >> 21) |

			((in[54] - minv) << 10) |
			((in[55] - minv) << 41))

	out[27] = uint64(
		((in[55] - minv) >> 23) |

			((in[56] - minv) << 8) |
			((in[57] - minv) << 39))

	out[28] = uint64(
		((in[57] - minv) >> 25) |

			((in[58] - minv) << 6) |
			((in[59] - minv) << 37))

	out[29] = uint64(
		((in[59] - minv) >> 27) |

			((in[60] - minv) << 4) |
			((in[61] - minv) << 35))

	out[30] = uint64(
		((in[61] - minv) >> 29) |

			((in[62] - minv) << 2) |
			((in[63] - minv) << 33))

}
func bp64_32[T uint64 | int64](in *[64]T, out *[32]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 32))

	out[1] = uint64(
		((in[2] - minv) << 0) |
			((in[3] - minv) << 32))

	out[2] = uint64(
		((in[4] - minv) << 0) |
			((in[5] - minv) << 32))

	out[3] = uint64(
		((in[6] - minv) << 0) |
			((in[7] - minv) << 32))

	out[4] = uint64(
		((in[8] - minv) << 0) |
			((in[9] - minv) << 32))

	out[5] = uint64(
		((in[10] - minv) << 0) |
			((in[11] - minv) << 32))

	out[6] = uint64(
		((in[12] - minv) << 0) |
			((in[13] - minv) << 32))

	out[7] = uint64(
		((in[14] - minv) << 0) |
			((in[15] - minv) << 32))

	out[8] = uint64(
		((in[16] - minv) << 0) |
			((in[17] - minv) << 32))

	out[9] = uint64(
		((in[18] - minv) << 0) |
			((in[19] - minv) << 32))

	out[10] = uint64(
		((in[20] - minv) << 0) |
			((in[21] - minv) << 32))

	out[11] = uint64(
		((in[22] - minv) << 0) |
			((in[23] - minv) << 32))

	out[12] = uint64(
		((in[24] - minv) << 0) |
			((in[25] - minv) << 32))

	out[13] = uint64(
		((in[26] - minv) << 0) |
			((in[27] - minv) << 32))

	out[14] = uint64(
		((in[28] - minv) << 0) |
			((in[29] - minv) << 32))

	out[15] = uint64(
		((in[30] - minv) << 0) |
			((in[31] - minv) << 32))

	out[16] = uint64(
		((in[32] - minv) << 0) |
			((in[33] - minv) << 32))

	out[17] = uint64(
		((in[34] - minv) << 0) |
			((in[35] - minv) << 32))

	out[18] = uint64(
		((in[36] - minv) << 0) |
			((in[37] - minv) << 32))

	out[19] = uint64(
		((in[38] - minv) << 0) |
			((in[39] - minv) << 32))

	out[20] = uint64(
		((in[40] - minv) << 0) |
			((in[41] - minv) << 32))

	out[21] = uint64(
		((in[42] - minv) << 0) |
			((in[43] - minv) << 32))

	out[22] = uint64(
		((in[44] - minv) << 0) |
			((in[45] - minv) << 32))

	out[23] = uint64(
		((in[46] - minv) << 0) |
			((in[47] - minv) << 32))

	out[24] = uint64(
		((in[48] - minv) << 0) |
			((in[49] - minv) << 32))

	out[25] = uint64(
		((in[50] - minv) << 0) |
			((in[51] - minv) << 32))

	out[26] = uint64(
		((in[52] - minv) << 0) |
			((in[53] - minv) << 32))

	out[27] = uint64(
		((in[54] - minv) << 0) |
			((in[55] - minv) << 32))

	out[28] = uint64(
		((in[56] - minv) << 0) |
			((in[57] - minv) << 32))

	out[29] = uint64(
		((in[58] - minv) << 0) |
			((in[59] - minv) << 32))

	out[30] = uint64(
		((in[60] - minv) << 0) |
			((in[61] - minv) << 32))

	out[31] = uint64(
		((in[62] - minv) << 0) |
			((in[63] - minv) << 32))

}
func bp64_33[T uint64 | int64](in *[64]T, out *[33]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 33))

	out[1] = uint64(
		((in[1] - minv) >> 31) |

			((in[2] - minv) << 2) |
			((in[3] - minv) << 35))

	out[2] = uint64(
		((in[3] - minv) >> 29) |

			((in[4] - minv) << 4) |
			((in[5] - minv) << 37))

	out[3] = uint64(
		((in[5] - minv) >> 27) |

			((in[6] - minv) << 6) |
			((in[7] - minv) << 39))

	out[4] = uint64(
		((in[7] - minv) >> 25) |

			((in[8] - minv) << 8) |
			((in[9] - minv) << 41))

	out[5] = uint64(
		((in[9] - minv) >> 23) |

			((in[10] - minv) << 10) |
			((in[11] - minv) << 43))

	out[6] = uint64(
		((in[11] - minv) >> 21) |

			((in[12] - minv) << 12) |
			((in[13] - minv) << 45))

	out[7] = uint64(
		((in[13] - minv) >> 19) |

			((in[14] - minv) << 14) |
			((in[15] - minv) << 47))

	out[8] = uint64(
		((in[15] - minv) >> 17) |

			((in[16] - minv) << 16) |
			((in[17] - minv) << 49))

	out[9] = uint64(
		((in[17] - minv) >> 15) |

			((in[18] - minv) << 18) |
			((in[19] - minv) << 51))

	out[10] = uint64(
		((in[19] - minv) >> 13) |

			((in[20] - minv) << 20) |
			((in[21] - minv) << 53))

	out[11] = uint64(
		((in[21] - minv) >> 11) |

			((in[22] - minv) << 22) |
			((in[23] - minv) << 55))

	out[12] = uint64(
		((in[23] - minv) >> 9) |

			((in[24] - minv) << 24) |
			((in[25] - minv) << 57))

	out[13] = uint64(
		((in[25] - minv) >> 7) |

			((in[26] - minv) << 26) |
			((in[27] - minv) << 59))

	out[14] = uint64(
		((in[27] - minv) >> 5) |

			((in[28] - minv) << 28) |
			((in[29] - minv) << 61))

	out[15] = uint64(
		((in[29] - minv) >> 3) |

			((in[30] - minv) << 30) |
			((in[31] - minv) << 63))

	out[16] = uint64(
		((in[31] - minv) >> 1) |

			((in[32] - minv) << 32))

	out[17] = uint64(
		((in[32] - minv) >> 32) |

			((in[33] - minv) << 1) |
			((in[34] - minv) << 34))

	out[18] = uint64(
		((in[34] - minv) >> 30) |

			((in[35] - minv) << 3) |
			((in[36] - minv) << 36))

	out[19] = uint64(
		((in[36] - minv) >> 28) |

			((in[37] - minv) << 5) |
			((in[38] - minv) << 38))

	out[20] = uint64(
		((in[38] - minv) >> 26) |

			((in[39] - minv) << 7) |
			((in[40] - minv) << 40))

	out[21] = uint64(
		((in[40] - minv) >> 24) |

			((in[41] - minv) << 9) |
			((in[42] - minv) << 42))

	out[22] = uint64(
		((in[42] - minv) >> 22) |

			((in[43] - minv) << 11) |
			((in[44] - minv) << 44))

	out[23] = uint64(
		((in[44] - minv) >> 20) |

			((in[45] - minv) << 13) |
			((in[46] - minv) << 46))

	out[24] = uint64(
		((in[46] - minv) >> 18) |

			((in[47] - minv) << 15) |
			((in[48] - minv) << 48))

	out[25] = uint64(
		((in[48] - minv) >> 16) |

			((in[49] - minv) << 17) |
			((in[50] - minv) << 50))

	out[26] = uint64(
		((in[50] - minv) >> 14) |

			((in[51] - minv) << 19) |
			((in[52] - minv) << 52))

	out[27] = uint64(
		((in[52] - minv) >> 12) |

			((in[53] - minv) << 21) |
			((in[54] - minv) << 54))

	out[28] = uint64(
		((in[54] - minv) >> 10) |

			((in[55] - minv) << 23) |
			((in[56] - minv) << 56))

	out[29] = uint64(
		((in[56] - minv) >> 8) |

			((in[57] - minv) << 25) |
			((in[58] - minv) << 58))

	out[30] = uint64(
		((in[58] - minv) >> 6) |

			((in[59] - minv) << 27) |
			((in[60] - minv) << 60))

	out[31] = uint64(
		((in[60] - minv) >> 4) |

			((in[61] - minv) << 29) |
			((in[62] - minv) << 62))

	out[32] = uint64(
		((in[62] - minv) >> 2) |

			((in[63] - minv) << 31))

}
func bp64_34[T uint64 | int64](in *[64]T, out *[34]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 34))

	out[1] = uint64(
		((in[1] - minv) >> 30) |

			((in[2] - minv) << 4) |
			((in[3] - minv) << 38))

	out[2] = uint64(
		((in[3] - minv) >> 26) |

			((in[4] - minv) << 8) |
			((in[5] - minv) << 42))

	out[3] = uint64(
		((in[5] - minv) >> 22) |

			((in[6] - minv) << 12) |
			((in[7] - minv) << 46))

	out[4] = uint64(
		((in[7] - minv) >> 18) |

			((in[8] - minv) << 16) |
			((in[9] - minv) << 50))

	out[5] = uint64(
		((in[9] - minv) >> 14) |

			((in[10] - minv) << 20) |
			((in[11] - minv) << 54))

	out[6] = uint64(
		((in[11] - minv) >> 10) |

			((in[12] - minv) << 24) |
			((in[13] - minv) << 58))

	out[7] = uint64(
		((in[13] - minv) >> 6) |

			((in[14] - minv) << 28) |
			((in[15] - minv) << 62))

	out[8] = uint64(
		((in[15] - minv) >> 2) |

			((in[16] - minv) << 32))

	out[9] = uint64(
		((in[16] - minv) >> 32) |

			((in[17] - minv) << 2) |
			((in[18] - minv) << 36))

	out[10] = uint64(
		((in[18] - minv) >> 28) |

			((in[19] - minv) << 6) |
			((in[20] - minv) << 40))

	out[11] = uint64(
		((in[20] - minv) >> 24) |

			((in[21] - minv) << 10) |
			((in[22] - minv) << 44))

	out[12] = uint64(
		((in[22] - minv) >> 20) |

			((in[23] - minv) << 14) |
			((in[24] - minv) << 48))

	out[13] = uint64(
		((in[24] - minv) >> 16) |

			((in[25] - minv) << 18) |
			((in[26] - minv) << 52))

	out[14] = uint64(
		((in[26] - minv) >> 12) |

			((in[27] - minv) << 22) |
			((in[28] - minv) << 56))

	out[15] = uint64(
		((in[28] - minv) >> 8) |

			((in[29] - minv) << 26) |
			((in[30] - minv) << 60))

	out[16] = uint64(
		((in[30] - minv) >> 4) |

			((in[31] - minv) << 30))

	out[17] = uint64(
		((in[31] - minv) >> 34) |

			((in[32] - minv) << 0) |
			((in[33] - minv) << 34))

	out[18] = uint64(
		((in[33] - minv) >> 30) |

			((in[34] - minv) << 4) |
			((in[35] - minv) << 38))

	out[19] = uint64(
		((in[35] - minv) >> 26) |

			((in[36] - minv) << 8) |
			((in[37] - minv) << 42))

	out[20] = uint64(
		((in[37] - minv) >> 22) |

			((in[38] - minv) << 12) |
			((in[39] - minv) << 46))

	out[21] = uint64(
		((in[39] - minv) >> 18) |

			((in[40] - minv) << 16) |
			((in[41] - minv) << 50))

	out[22] = uint64(
		((in[41] - minv) >> 14) |

			((in[42] - minv) << 20) |
			((in[43] - minv) << 54))

	out[23] = uint64(
		((in[43] - minv) >> 10) |

			((in[44] - minv) << 24) |
			((in[45] - minv) << 58))

	out[24] = uint64(
		((in[45] - minv) >> 6) |

			((in[46] - minv) << 28) |
			((in[47] - minv) << 62))

	out[25] = uint64(
		((in[47] - minv) >> 2) |

			((in[48] - minv) << 32))

	out[26] = uint64(
		((in[48] - minv) >> 32) |

			((in[49] - minv) << 2) |
			((in[50] - minv) << 36))

	out[27] = uint64(
		((in[50] - minv) >> 28) |

			((in[51] - minv) << 6) |
			((in[52] - minv) << 40))

	out[28] = uint64(
		((in[52] - minv) >> 24) |

			((in[53] - minv) << 10) |
			((in[54] - minv) << 44))

	out[29] = uint64(
		((in[54] - minv) >> 20) |

			((in[55] - minv) << 14) |
			((in[56] - minv) << 48))

	out[30] = uint64(
		((in[56] - minv) >> 16) |

			((in[57] - minv) << 18) |
			((in[58] - minv) << 52))

	out[31] = uint64(
		((in[58] - minv) >> 12) |

			((in[59] - minv) << 22) |
			((in[60] - minv) << 56))

	out[32] = uint64(
		((in[60] - minv) >> 8) |

			((in[61] - minv) << 26) |
			((in[62] - minv) << 60))

	out[33] = uint64(
		((in[62] - minv) >> 4) |

			((in[63] - minv) << 30))

}
func bp64_35[T uint64 | int64](in *[64]T, out *[35]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 35))

	out[1] = uint64(
		((in[1] - minv) >> 29) |

			((in[2] - minv) << 6) |
			((in[3] - minv) << 41))

	out[2] = uint64(
		((in[3] - minv) >> 23) |

			((in[4] - minv) << 12) |
			((in[5] - minv) << 47))

	out[3] = uint64(
		((in[5] - minv) >> 17) |

			((in[6] - minv) << 18) |
			((in[7] - minv) << 53))

	out[4] = uint64(
		((in[7] - minv) >> 11) |

			((in[8] - minv) << 24) |
			((in[9] - minv) << 59))

	out[5] = uint64(
		((in[9] - minv) >> 5) |

			((in[10] - minv) << 30))

	out[6] = uint64(
		((in[10] - minv) >> 34) |

			((in[11] - minv) << 1) |
			((in[12] - minv) << 36))

	out[7] = uint64(
		((in[12] - minv) >> 28) |

			((in[13] - minv) << 7) |
			((in[14] - minv) << 42))

	out[8] = uint64(
		((in[14] - minv) >> 22) |

			((in[15] - minv) << 13) |
			((in[16] - minv) << 48))

	out[9] = uint64(
		((in[16] - minv) >> 16) |

			((in[17] - minv) << 19) |
			((in[18] - minv) << 54))

	out[10] = uint64(
		((in[18] - minv) >> 10) |

			((in[19] - minv) << 25) |
			((in[20] - minv) << 60))

	out[11] = uint64(
		((in[20] - minv) >> 4) |

			((in[21] - minv) << 31))

	out[12] = uint64(
		((in[21] - minv) >> 33) |

			((in[22] - minv) << 2) |
			((in[23] - minv) << 37))

	out[13] = uint64(
		((in[23] - minv) >> 27) |

			((in[24] - minv) << 8) |
			((in[25] - minv) << 43))

	out[14] = uint64(
		((in[25] - minv) >> 21) |

			((in[26] - minv) << 14) |
			((in[27] - minv) << 49))

	out[15] = uint64(
		((in[27] - minv) >> 15) |

			((in[28] - minv) << 20) |
			((in[29] - minv) << 55))

	out[16] = uint64(
		((in[29] - minv) >> 9) |

			((in[30] - minv) << 26) |
			((in[31] - minv) << 61))

	out[17] = uint64(
		((in[31] - minv) >> 3) |

			((in[32] - minv) << 32))

	out[18] = uint64(
		((in[32] - minv) >> 32) |

			((in[33] - minv) << 3) |
			((in[34] - minv) << 38))

	out[19] = uint64(
		((in[34] - minv) >> 26) |

			((in[35] - minv) << 9) |
			((in[36] - minv) << 44))

	out[20] = uint64(
		((in[36] - minv) >> 20) |

			((in[37] - minv) << 15) |
			((in[38] - minv) << 50))

	out[21] = uint64(
		((in[38] - minv) >> 14) |

			((in[39] - minv) << 21) |
			((in[40] - minv) << 56))

	out[22] = uint64(
		((in[40] - minv) >> 8) |

			((in[41] - minv) << 27) |
			((in[42] - minv) << 62))

	out[23] = uint64(
		((in[42] - minv) >> 2) |

			((in[43] - minv) << 33))

	out[24] = uint64(
		((in[43] - minv) >> 31) |

			((in[44] - minv) << 4) |
			((in[45] - minv) << 39))

	out[25] = uint64(
		((in[45] - minv) >> 25) |

			((in[46] - minv) << 10) |
			((in[47] - minv) << 45))

	out[26] = uint64(
		((in[47] - minv) >> 19) |

			((in[48] - minv) << 16) |
			((in[49] - minv) << 51))

	out[27] = uint64(
		((in[49] - minv) >> 13) |

			((in[50] - minv) << 22) |
			((in[51] - minv) << 57))

	out[28] = uint64(
		((in[51] - minv) >> 7) |

			((in[52] - minv) << 28) |
			((in[53] - minv) << 63))

	out[29] = uint64(
		((in[53] - minv) >> 1) |

			((in[54] - minv) << 34))

	out[30] = uint64(
		((in[54] - minv) >> 30) |

			((in[55] - minv) << 5) |
			((in[56] - minv) << 40))

	out[31] = uint64(
		((in[56] - minv) >> 24) |

			((in[57] - minv) << 11) |
			((in[58] - minv) << 46))

	out[32] = uint64(
		((in[58] - minv) >> 18) |

			((in[59] - minv) << 17) |
			((in[60] - minv) << 52))

	out[33] = uint64(
		((in[60] - minv) >> 12) |

			((in[61] - minv) << 23) |
			((in[62] - minv) << 58))

	out[34] = uint64(
		((in[62] - minv) >> 6) |

			((in[63] - minv) << 29))

}
func bp64_36[T uint64 | int64](in *[64]T, out *[36]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 36))

	out[1] = uint64(
		((in[1] - minv) >> 28) |

			((in[2] - minv) << 8) |
			((in[3] - minv) << 44))

	out[2] = uint64(
		((in[3] - minv) >> 20) |

			((in[4] - minv) << 16) |
			((in[5] - minv) << 52))

	out[3] = uint64(
		((in[5] - minv) >> 12) |

			((in[6] - minv) << 24) |
			((in[7] - minv) << 60))

	out[4] = uint64(
		((in[7] - minv) >> 4) |

			((in[8] - minv) << 32))

	out[5] = uint64(
		((in[8] - minv) >> 32) |

			((in[9] - minv) << 4) |
			((in[10] - minv) << 40))

	out[6] = uint64(
		((in[10] - minv) >> 24) |

			((in[11] - minv) << 12) |
			((in[12] - minv) << 48))

	out[7] = uint64(
		((in[12] - minv) >> 16) |

			((in[13] - minv) << 20) |
			((in[14] - minv) << 56))

	out[8] = uint64(
		((in[14] - minv) >> 8) |

			((in[15] - minv) << 28))

	out[9] = uint64(
		((in[15] - minv) >> 36) |

			((in[16] - minv) << 0) |
			((in[17] - minv) << 36))

	out[10] = uint64(
		((in[17] - minv) >> 28) |

			((in[18] - minv) << 8) |
			((in[19] - minv) << 44))

	out[11] = uint64(
		((in[19] - minv) >> 20) |

			((in[20] - minv) << 16) |
			((in[21] - minv) << 52))

	out[12] = uint64(
		((in[21] - minv) >> 12) |

			((in[22] - minv) << 24) |
			((in[23] - minv) << 60))

	out[13] = uint64(
		((in[23] - minv) >> 4) |

			((in[24] - minv) << 32))

	out[14] = uint64(
		((in[24] - minv) >> 32) |

			((in[25] - minv) << 4) |
			((in[26] - minv) << 40))

	out[15] = uint64(
		((in[26] - minv) >> 24) |

			((in[27] - minv) << 12) |
			((in[28] - minv) << 48))

	out[16] = uint64(
		((in[28] - minv) >> 16) |

			((in[29] - minv) << 20) |
			((in[30] - minv) << 56))

	out[17] = uint64(
		((in[30] - minv) >> 8) |

			((in[31] - minv) << 28))

	out[18] = uint64(
		((in[31] - minv) >> 36) |

			((in[32] - minv) << 0) |
			((in[33] - minv) << 36))

	out[19] = uint64(
		((in[33] - minv) >> 28) |

			((in[34] - minv) << 8) |
			((in[35] - minv) << 44))

	out[20] = uint64(
		((in[35] - minv) >> 20) |

			((in[36] - minv) << 16) |
			((in[37] - minv) << 52))

	out[21] = uint64(
		((in[37] - minv) >> 12) |

			((in[38] - minv) << 24) |
			((in[39] - minv) << 60))

	out[22] = uint64(
		((in[39] - minv) >> 4) |

			((in[40] - minv) << 32))

	out[23] = uint64(
		((in[40] - minv) >> 32) |

			((in[41] - minv) << 4) |
			((in[42] - minv) << 40))

	out[24] = uint64(
		((in[42] - minv) >> 24) |

			((in[43] - minv) << 12) |
			((in[44] - minv) << 48))

	out[25] = uint64(
		((in[44] - minv) >> 16) |

			((in[45] - minv) << 20) |
			((in[46] - minv) << 56))

	out[26] = uint64(
		((in[46] - minv) >> 8) |

			((in[47] - minv) << 28))

	out[27] = uint64(
		((in[47] - minv) >> 36) |

			((in[48] - minv) << 0) |
			((in[49] - minv) << 36))

	out[28] = uint64(
		((in[49] - minv) >> 28) |

			((in[50] - minv) << 8) |
			((in[51] - minv) << 44))

	out[29] = uint64(
		((in[51] - minv) >> 20) |

			((in[52] - minv) << 16) |
			((in[53] - minv) << 52))

	out[30] = uint64(
		((in[53] - minv) >> 12) |

			((in[54] - minv) << 24) |
			((in[55] - minv) << 60))

	out[31] = uint64(
		((in[55] - minv) >> 4) |

			((in[56] - minv) << 32))

	out[32] = uint64(
		((in[56] - minv) >> 32) |

			((in[57] - minv) << 4) |
			((in[58] - minv) << 40))

	out[33] = uint64(
		((in[58] - minv) >> 24) |

			((in[59] - minv) << 12) |
			((in[60] - minv) << 48))

	out[34] = uint64(
		((in[60] - minv) >> 16) |

			((in[61] - minv) << 20) |
			((in[62] - minv) << 56))

	out[35] = uint64(
		((in[62] - minv) >> 8) |

			((in[63] - minv) << 28))

}
func bp64_37[T uint64 | int64](in *[64]T, out *[37]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 37))

	out[1] = uint64(
		((in[1] - minv) >> 27) |

			((in[2] - minv) << 10) |
			((in[3] - minv) << 47))

	out[2] = uint64(
		((in[3] - minv) >> 17) |

			((in[4] - minv) << 20) |
			((in[5] - minv) << 57))

	out[3] = uint64(
		((in[5] - minv) >> 7) |

			((in[6] - minv) << 30))

	out[4] = uint64(
		((in[6] - minv) >> 34) |

			((in[7] - minv) << 3) |
			((in[8] - minv) << 40))

	out[5] = uint64(
		((in[8] - minv) >> 24) |

			((in[9] - minv) << 13) |
			((in[10] - minv) << 50))

	out[6] = uint64(
		((in[10] - minv) >> 14) |

			((in[11] - minv) << 23) |
			((in[12] - minv) << 60))

	out[7] = uint64(
		((in[12] - minv) >> 4) |

			((in[13] - minv) << 33))

	out[8] = uint64(
		((in[13] - minv) >> 31) |

			((in[14] - minv) << 6) |
			((in[15] - minv) << 43))

	out[9] = uint64(
		((in[15] - minv) >> 21) |

			((in[16] - minv) << 16) |
			((in[17] - minv) << 53))

	out[10] = uint64(
		((in[17] - minv) >> 11) |

			((in[18] - minv) << 26) |
			((in[19] - minv) << 63))

	out[11] = uint64(
		((in[19] - minv) >> 1) |

			((in[20] - minv) << 36))

	out[12] = uint64(
		((in[20] - minv) >> 28) |

			((in[21] - minv) << 9) |
			((in[22] - minv) << 46))

	out[13] = uint64(
		((in[22] - minv) >> 18) |

			((in[23] - minv) << 19) |
			((in[24] - minv) << 56))

	out[14] = uint64(
		((in[24] - minv) >> 8) |

			((in[25] - minv) << 29))

	out[15] = uint64(
		((in[25] - minv) >> 35) |

			((in[26] - minv) << 2) |
			((in[27] - minv) << 39))

	out[16] = uint64(
		((in[27] - minv) >> 25) |

			((in[28] - minv) << 12) |
			((in[29] - minv) << 49))

	out[17] = uint64(
		((in[29] - minv) >> 15) |

			((in[30] - minv) << 22) |
			((in[31] - minv) << 59))

	out[18] = uint64(
		((in[31] - minv) >> 5) |

			((in[32] - minv) << 32))

	out[19] = uint64(
		((in[32] - minv) >> 32) |

			((in[33] - minv) << 5) |
			((in[34] - minv) << 42))

	out[20] = uint64(
		((in[34] - minv) >> 22) |

			((in[35] - minv) << 15) |
			((in[36] - minv) << 52))

	out[21] = uint64(
		((in[36] - minv) >> 12) |

			((in[37] - minv) << 25) |
			((in[38] - minv) << 62))

	out[22] = uint64(
		((in[38] - minv) >> 2) |

			((in[39] - minv) << 35))

	out[23] = uint64(
		((in[39] - minv) >> 29) |

			((in[40] - minv) << 8) |
			((in[41] - minv) << 45))

	out[24] = uint64(
		((in[41] - minv) >> 19) |

			((in[42] - minv) << 18) |
			((in[43] - minv) << 55))

	out[25] = uint64(
		((in[43] - minv) >> 9) |

			((in[44] - minv) << 28))

	out[26] = uint64(
		((in[44] - minv) >> 36) |

			((in[45] - minv) << 1) |
			((in[46] - minv) << 38))

	out[27] = uint64(
		((in[46] - minv) >> 26) |

			((in[47] - minv) << 11) |
			((in[48] - minv) << 48))

	out[28] = uint64(
		((in[48] - minv) >> 16) |

			((in[49] - minv) << 21) |
			((in[50] - minv) << 58))

	out[29] = uint64(
		((in[50] - minv) >> 6) |

			((in[51] - minv) << 31))

	out[30] = uint64(
		((in[51] - minv) >> 33) |

			((in[52] - minv) << 4) |
			((in[53] - minv) << 41))

	out[31] = uint64(
		((in[53] - minv) >> 23) |

			((in[54] - minv) << 14) |
			((in[55] - minv) << 51))

	out[32] = uint64(
		((in[55] - minv) >> 13) |

			((in[56] - minv) << 24) |
			((in[57] - minv) << 61))

	out[33] = uint64(
		((in[57] - minv) >> 3) |

			((in[58] - minv) << 34))

	out[34] = uint64(
		((in[58] - minv) >> 30) |

			((in[59] - minv) << 7) |
			((in[60] - minv) << 44))

	out[35] = uint64(
		((in[60] - minv) >> 20) |

			((in[61] - minv) << 17) |
			((in[62] - minv) << 54))

	out[36] = uint64(
		((in[62] - minv) >> 10) |

			((in[63] - minv) << 27))

}
func bp64_38[T uint64 | int64](in *[64]T, out *[38]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 38))

	out[1] = uint64(
		((in[1] - minv) >> 26) |

			((in[2] - minv) << 12) |
			((in[3] - minv) << 50))

	out[2] = uint64(
		((in[3] - minv) >> 14) |

			((in[4] - minv) << 24) |
			((in[5] - minv) << 62))

	out[3] = uint64(
		((in[5] - minv) >> 2) |

			((in[6] - minv) << 36))

	out[4] = uint64(
		((in[6] - minv) >> 28) |

			((in[7] - minv) << 10) |
			((in[8] - minv) << 48))

	out[5] = uint64(
		((in[8] - minv) >> 16) |

			((in[9] - minv) << 22) |
			((in[10] - minv) << 60))

	out[6] = uint64(
		((in[10] - minv) >> 4) |

			((in[11] - minv) << 34))

	out[7] = uint64(
		((in[11] - minv) >> 30) |

			((in[12] - minv) << 8) |
			((in[13] - minv) << 46))

	out[8] = uint64(
		((in[13] - minv) >> 18) |

			((in[14] - minv) << 20) |
			((in[15] - minv) << 58))

	out[9] = uint64(
		((in[15] - minv) >> 6) |

			((in[16] - minv) << 32))

	out[10] = uint64(
		((in[16] - minv) >> 32) |

			((in[17] - minv) << 6) |
			((in[18] - minv) << 44))

	out[11] = uint64(
		((in[18] - minv) >> 20) |

			((in[19] - minv) << 18) |
			((in[20] - minv) << 56))

	out[12] = uint64(
		((in[20] - minv) >> 8) |

			((in[21] - minv) << 30))

	out[13] = uint64(
		((in[21] - minv) >> 34) |

			((in[22] - minv) << 4) |
			((in[23] - minv) << 42))

	out[14] = uint64(
		((in[23] - minv) >> 22) |

			((in[24] - minv) << 16) |
			((in[25] - minv) << 54))

	out[15] = uint64(
		((in[25] - minv) >> 10) |

			((in[26] - minv) << 28))

	out[16] = uint64(
		((in[26] - minv) >> 36) |

			((in[27] - minv) << 2) |
			((in[28] - minv) << 40))

	out[17] = uint64(
		((in[28] - minv) >> 24) |

			((in[29] - minv) << 14) |
			((in[30] - minv) << 52))

	out[18] = uint64(
		((in[30] - minv) >> 12) |

			((in[31] - minv) << 26))

	out[19] = uint64(
		((in[31] - minv) >> 38) |

			((in[32] - minv) << 0) |
			((in[33] - minv) << 38))

	out[20] = uint64(
		((in[33] - minv) >> 26) |

			((in[34] - minv) << 12) |
			((in[35] - minv) << 50))

	out[21] = uint64(
		((in[35] - minv) >> 14) |

			((in[36] - minv) << 24) |
			((in[37] - minv) << 62))

	out[22] = uint64(
		((in[37] - minv) >> 2) |

			((in[38] - minv) << 36))

	out[23] = uint64(
		((in[38] - minv) >> 28) |

			((in[39] - minv) << 10) |
			((in[40] - minv) << 48))

	out[24] = uint64(
		((in[40] - minv) >> 16) |

			((in[41] - minv) << 22) |
			((in[42] - minv) << 60))

	out[25] = uint64(
		((in[42] - minv) >> 4) |

			((in[43] - minv) << 34))

	out[26] = uint64(
		((in[43] - minv) >> 30) |

			((in[44] - minv) << 8) |
			((in[45] - minv) << 46))

	out[27] = uint64(
		((in[45] - minv) >> 18) |

			((in[46] - minv) << 20) |
			((in[47] - minv) << 58))

	out[28] = uint64(
		((in[47] - minv) >> 6) |

			((in[48] - minv) << 32))

	out[29] = uint64(
		((in[48] - minv) >> 32) |

			((in[49] - minv) << 6) |
			((in[50] - minv) << 44))

	out[30] = uint64(
		((in[50] - minv) >> 20) |

			((in[51] - minv) << 18) |
			((in[52] - minv) << 56))

	out[31] = uint64(
		((in[52] - minv) >> 8) |

			((in[53] - minv) << 30))

	out[32] = uint64(
		((in[53] - minv) >> 34) |

			((in[54] - minv) << 4) |
			((in[55] - minv) << 42))

	out[33] = uint64(
		((in[55] - minv) >> 22) |

			((in[56] - minv) << 16) |
			((in[57] - minv) << 54))

	out[34] = uint64(
		((in[57] - minv) >> 10) |

			((in[58] - minv) << 28))

	out[35] = uint64(
		((in[58] - minv) >> 36) |

			((in[59] - minv) << 2) |
			((in[60] - minv) << 40))

	out[36] = uint64(
		((in[60] - minv) >> 24) |

			((in[61] - minv) << 14) |
			((in[62] - minv) << 52))

	out[37] = uint64(
		((in[62] - minv) >> 12) |

			((in[63] - minv) << 26))

}
func bp64_39[T uint64 | int64](in *[64]T, out *[39]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 39))

	out[1] = uint64(
		((in[1] - minv) >> 25) |

			((in[2] - minv) << 14) |
			((in[3] - minv) << 53))

	out[2] = uint64(
		((in[3] - minv) >> 11) |

			((in[4] - minv) << 28))

	out[3] = uint64(
		((in[4] - minv) >> 36) |

			((in[5] - minv) << 3) |
			((in[6] - minv) << 42))

	out[4] = uint64(
		((in[6] - minv) >> 22) |

			((in[7] - minv) << 17) |
			((in[8] - minv) << 56))

	out[5] = uint64(
		((in[8] - minv) >> 8) |

			((in[9] - minv) << 31))

	out[6] = uint64(
		((in[9] - minv) >> 33) |

			((in[10] - minv) << 6) |
			((in[11] - minv) << 45))

	out[7] = uint64(
		((in[11] - minv) >> 19) |

			((in[12] - minv) << 20) |
			((in[13] - minv) << 59))

	out[8] = uint64(
		((in[13] - minv) >> 5) |

			((in[14] - minv) << 34))

	out[9] = uint64(
		((in[14] - minv) >> 30) |

			((in[15] - minv) << 9) |
			((in[16] - minv) << 48))

	out[10] = uint64(
		((in[16] - minv) >> 16) |

			((in[17] - minv) << 23) |
			((in[18] - minv) << 62))

	out[11] = uint64(
		((in[18] - minv) >> 2) |

			((in[19] - minv) << 37))

	out[12] = uint64(
		((in[19] - minv) >> 27) |

			((in[20] - minv) << 12) |
			((in[21] - minv) << 51))

	out[13] = uint64(
		((in[21] - minv) >> 13) |

			((in[22] - minv) << 26))

	out[14] = uint64(
		((in[22] - minv) >> 38) |

			((in[23] - minv) << 1) |
			((in[24] - minv) << 40))

	out[15] = uint64(
		((in[24] - minv) >> 24) |

			((in[25] - minv) << 15) |
			((in[26] - minv) << 54))

	out[16] = uint64(
		((in[26] - minv) >> 10) |

			((in[27] - minv) << 29))

	out[17] = uint64(
		((in[27] - minv) >> 35) |

			((in[28] - minv) << 4) |
			((in[29] - minv) << 43))

	out[18] = uint64(
		((in[29] - minv) >> 21) |

			((in[30] - minv) << 18) |
			((in[31] - minv) << 57))

	out[19] = uint64(
		((in[31] - minv) >> 7) |

			((in[32] - minv) << 32))

	out[20] = uint64(
		((in[32] - minv) >> 32) |

			((in[33] - minv) << 7) |
			((in[34] - minv) << 46))

	out[21] = uint64(
		((in[34] - minv) >> 18) |

			((in[35] - minv) << 21) |
			((in[36] - minv) << 60))

	out[22] = uint64(
		((in[36] - minv) >> 4) |

			((in[37] - minv) << 35))

	out[23] = uint64(
		((in[37] - minv) >> 29) |

			((in[38] - minv) << 10) |
			((in[39] - minv) << 49))

	out[24] = uint64(
		((in[39] - minv) >> 15) |

			((in[40] - minv) << 24) |
			((in[41] - minv) << 63))

	out[25] = uint64(
		((in[41] - minv) >> 1) |

			((in[42] - minv) << 38))

	out[26] = uint64(
		((in[42] - minv) >> 26) |

			((in[43] - minv) << 13) |
			((in[44] - minv) << 52))

	out[27] = uint64(
		((in[44] - minv) >> 12) |

			((in[45] - minv) << 27))

	out[28] = uint64(
		((in[45] - minv) >> 37) |

			((in[46] - minv) << 2) |
			((in[47] - minv) << 41))

	out[29] = uint64(
		((in[47] - minv) >> 23) |

			((in[48] - minv) << 16) |
			((in[49] - minv) << 55))

	out[30] = uint64(
		((in[49] - minv) >> 9) |

			((in[50] - minv) << 30))

	out[31] = uint64(
		((in[50] - minv) >> 34) |

			((in[51] - minv) << 5) |
			((in[52] - minv) << 44))

	out[32] = uint64(
		((in[52] - minv) >> 20) |

			((in[53] - minv) << 19) |
			((in[54] - minv) << 58))

	out[33] = uint64(
		((in[54] - minv) >> 6) |

			((in[55] - minv) << 33))

	out[34] = uint64(
		((in[55] - minv) >> 31) |

			((in[56] - minv) << 8) |
			((in[57] - minv) << 47))

	out[35] = uint64(
		((in[57] - minv) >> 17) |

			((in[58] - minv) << 22) |
			((in[59] - minv) << 61))

	out[36] = uint64(
		((in[59] - minv) >> 3) |

			((in[60] - minv) << 36))

	out[37] = uint64(
		((in[60] - minv) >> 28) |

			((in[61] - minv) << 11) |
			((in[62] - minv) << 50))

	out[38] = uint64(
		((in[62] - minv) >> 14) |

			((in[63] - minv) << 25))

}
func bp64_40[T uint64 | int64](in *[64]T, out *[40]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 40))

	out[1] = uint64(
		((in[1] - minv) >> 24) |

			((in[2] - minv) << 16) |
			((in[3] - minv) << 56))

	out[2] = uint64(
		((in[3] - minv) >> 8) |

			((in[4] - minv) << 32))

	out[3] = uint64(
		((in[4] - minv) >> 32) |

			((in[5] - minv) << 8) |
			((in[6] - minv) << 48))

	out[4] = uint64(
		((in[6] - minv) >> 16) |

			((in[7] - minv) << 24))

	out[5] = uint64(
		((in[7] - minv) >> 40) |

			((in[8] - minv) << 0) |
			((in[9] - minv) << 40))

	out[6] = uint64(
		((in[9] - minv) >> 24) |

			((in[10] - minv) << 16) |
			((in[11] - minv) << 56))

	out[7] = uint64(
		((in[11] - minv) >> 8) |

			((in[12] - minv) << 32))

	out[8] = uint64(
		((in[12] - minv) >> 32) |

			((in[13] - minv) << 8) |
			((in[14] - minv) << 48))

	out[9] = uint64(
		((in[14] - minv) >> 16) |

			((in[15] - minv) << 24))

	out[10] = uint64(
		((in[15] - minv) >> 40) |

			((in[16] - minv) << 0) |
			((in[17] - minv) << 40))

	out[11] = uint64(
		((in[17] - minv) >> 24) |

			((in[18] - minv) << 16) |
			((in[19] - minv) << 56))

	out[12] = uint64(
		((in[19] - minv) >> 8) |

			((in[20] - minv) << 32))

	out[13] = uint64(
		((in[20] - minv) >> 32) |

			((in[21] - minv) << 8) |
			((in[22] - minv) << 48))

	out[14] = uint64(
		((in[22] - minv) >> 16) |

			((in[23] - minv) << 24))

	out[15] = uint64(
		((in[23] - minv) >> 40) |

			((in[24] - minv) << 0) |
			((in[25] - minv) << 40))

	out[16] = uint64(
		((in[25] - minv) >> 24) |

			((in[26] - minv) << 16) |
			((in[27] - minv) << 56))

	out[17] = uint64(
		((in[27] - minv) >> 8) |

			((in[28] - minv) << 32))

	out[18] = uint64(
		((in[28] - minv) >> 32) |

			((in[29] - minv) << 8) |
			((in[30] - minv) << 48))

	out[19] = uint64(
		((in[30] - minv) >> 16) |

			((in[31] - minv) << 24))

	out[20] = uint64(
		((in[31] - minv) >> 40) |

			((in[32] - minv) << 0) |
			((in[33] - minv) << 40))

	out[21] = uint64(
		((in[33] - minv) >> 24) |

			((in[34] - minv) << 16) |
			((in[35] - minv) << 56))

	out[22] = uint64(
		((in[35] - minv) >> 8) |

			((in[36] - minv) << 32))

	out[23] = uint64(
		((in[36] - minv) >> 32) |

			((in[37] - minv) << 8) |
			((in[38] - minv) << 48))

	out[24] = uint64(
		((in[38] - minv) >> 16) |

			((in[39] - minv) << 24))

	out[25] = uint64(
		((in[39] - minv) >> 40) |

			((in[40] - minv) << 0) |
			((in[41] - minv) << 40))

	out[26] = uint64(
		((in[41] - minv) >> 24) |

			((in[42] - minv) << 16) |
			((in[43] - minv) << 56))

	out[27] = uint64(
		((in[43] - minv) >> 8) |

			((in[44] - minv) << 32))

	out[28] = uint64(
		((in[44] - minv) >> 32) |

			((in[45] - minv) << 8) |
			((in[46] - minv) << 48))

	out[29] = uint64(
		((in[46] - minv) >> 16) |

			((in[47] - minv) << 24))

	out[30] = uint64(
		((in[47] - minv) >> 40) |

			((in[48] - minv) << 0) |
			((in[49] - minv) << 40))

	out[31] = uint64(
		((in[49] - minv) >> 24) |

			((in[50] - minv) << 16) |
			((in[51] - minv) << 56))

	out[32] = uint64(
		((in[51] - minv) >> 8) |

			((in[52] - minv) << 32))

	out[33] = uint64(
		((in[52] - minv) >> 32) |

			((in[53] - minv) << 8) |
			((in[54] - minv) << 48))

	out[34] = uint64(
		((in[54] - minv) >> 16) |

			((in[55] - minv) << 24))

	out[35] = uint64(
		((in[55] - minv) >> 40) |

			((in[56] - minv) << 0) |
			((in[57] - minv) << 40))

	out[36] = uint64(
		((in[57] - minv) >> 24) |

			((in[58] - minv) << 16) |
			((in[59] - minv) << 56))

	out[37] = uint64(
		((in[59] - minv) >> 8) |

			((in[60] - minv) << 32))

	out[38] = uint64(
		((in[60] - minv) >> 32) |

			((in[61] - minv) << 8) |
			((in[62] - minv) << 48))

	out[39] = uint64(
		((in[62] - minv) >> 16) |

			((in[63] - minv) << 24))

}
func bp64_41[T uint64 | int64](in *[64]T, out *[41]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 41))

	out[1] = uint64(
		((in[1] - minv) >> 23) |

			((in[2] - minv) << 18) |
			((in[3] - minv) << 59))

	out[2] = uint64(
		((in[3] - minv) >> 5) |

			((in[4] - minv) << 36))

	out[3] = uint64(
		((in[4] - minv) >> 28) |

			((in[5] - minv) << 13) |
			((in[6] - minv) << 54))

	out[4] = uint64(
		((in[6] - minv) >> 10) |

			((in[7] - minv) << 31))

	out[5] = uint64(
		((in[7] - minv) >> 33) |

			((in[8] - minv) << 8) |
			((in[9] - minv) << 49))

	out[6] = uint64(
		((in[9] - minv) >> 15) |

			((in[10] - minv) << 26))

	out[7] = uint64(
		((in[10] - minv) >> 38) |

			((in[11] - minv) << 3) |
			((in[12] - minv) << 44))

	out[8] = uint64(
		((in[12] - minv) >> 20) |

			((in[13] - minv) << 21) |
			((in[14] - minv) << 62))

	out[9] = uint64(
		((in[14] - minv) >> 2) |

			((in[15] - minv) << 39))

	out[10] = uint64(
		((in[15] - minv) >> 25) |

			((in[16] - minv) << 16) |
			((in[17] - minv) << 57))

	out[11] = uint64(
		((in[17] - minv) >> 7) |

			((in[18] - minv) << 34))

	out[12] = uint64(
		((in[18] - minv) >> 30) |

			((in[19] - minv) << 11) |
			((in[20] - minv) << 52))

	out[13] = uint64(
		((in[20] - minv) >> 12) |

			((in[21] - minv) << 29))

	out[14] = uint64(
		((in[21] - minv) >> 35) |

			((in[22] - minv) << 6) |
			((in[23] - minv) << 47))

	out[15] = uint64(
		((in[23] - minv) >> 17) |

			((in[24] - minv) << 24))

	out[16] = uint64(
		((in[24] - minv) >> 40) |

			((in[25] - minv) << 1) |
			((in[26] - minv) << 42))

	out[17] = uint64(
		((in[26] - minv) >> 22) |

			((in[27] - minv) << 19) |
			((in[28] - minv) << 60))

	out[18] = uint64(
		((in[28] - minv) >> 4) |

			((in[29] - minv) << 37))

	out[19] = uint64(
		((in[29] - minv) >> 27) |

			((in[30] - minv) << 14) |
			((in[31] - minv) << 55))

	out[20] = uint64(
		((in[31] - minv) >> 9) |

			((in[32] - minv) << 32))

	out[21] = uint64(
		((in[32] - minv) >> 32) |

			((in[33] - minv) << 9) |
			((in[34] - minv) << 50))

	out[22] = uint64(
		((in[34] - minv) >> 14) |

			((in[35] - minv) << 27))

	out[23] = uint64(
		((in[35] - minv) >> 37) |

			((in[36] - minv) << 4) |
			((in[37] - minv) << 45))

	out[24] = uint64(
		((in[37] - minv) >> 19) |

			((in[38] - minv) << 22) |
			((in[39] - minv) << 63))

	out[25] = uint64(
		((in[39] - minv) >> 1) |

			((in[40] - minv) << 40))

	out[26] = uint64(
		((in[40] - minv) >> 24) |

			((in[41] - minv) << 17) |
			((in[42] - minv) << 58))

	out[27] = uint64(
		((in[42] - minv) >> 6) |

			((in[43] - minv) << 35))

	out[28] = uint64(
		((in[43] - minv) >> 29) |

			((in[44] - minv) << 12) |
			((in[45] - minv) << 53))

	out[29] = uint64(
		((in[45] - minv) >> 11) |

			((in[46] - minv) << 30))

	out[30] = uint64(
		((in[46] - minv) >> 34) |

			((in[47] - minv) << 7) |
			((in[48] - minv) << 48))

	out[31] = uint64(
		((in[48] - minv) >> 16) |

			((in[49] - minv) << 25))

	out[32] = uint64(
		((in[49] - minv) >> 39) |

			((in[50] - minv) << 2) |
			((in[51] - minv) << 43))

	out[33] = uint64(
		((in[51] - minv) >> 21) |

			((in[52] - minv) << 20) |
			((in[53] - minv) << 61))

	out[34] = uint64(
		((in[53] - minv) >> 3) |

			((in[54] - minv) << 38))

	out[35] = uint64(
		((in[54] - minv) >> 26) |

			((in[55] - minv) << 15) |
			((in[56] - minv) << 56))

	out[36] = uint64(
		((in[56] - minv) >> 8) |

			((in[57] - minv) << 33))

	out[37] = uint64(
		((in[57] - minv) >> 31) |

			((in[58] - minv) << 10) |
			((in[59] - minv) << 51))

	out[38] = uint64(
		((in[59] - minv) >> 13) |

			((in[60] - minv) << 28))

	out[39] = uint64(
		((in[60] - minv) >> 36) |

			((in[61] - minv) << 5) |
			((in[62] - minv) << 46))

	out[40] = uint64(
		((in[62] - minv) >> 18) |

			((in[63] - minv) << 23))

}
func bp64_42[T uint64 | int64](in *[64]T, out *[42]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 42))

	out[1] = uint64(
		((in[1] - minv) >> 22) |

			((in[2] - minv) << 20) |
			((in[3] - minv) << 62))

	out[2] = uint64(
		((in[3] - minv) >> 2) |

			((in[4] - minv) << 40))

	out[3] = uint64(
		((in[4] - minv) >> 24) |

			((in[5] - minv) << 18) |
			((in[6] - minv) << 60))

	out[4] = uint64(
		((in[6] - minv) >> 4) |

			((in[7] - minv) << 38))

	out[5] = uint64(
		((in[7] - minv) >> 26) |

			((in[8] - minv) << 16) |
			((in[9] - minv) << 58))

	out[6] = uint64(
		((in[9] - minv) >> 6) |

			((in[10] - minv) << 36))

	out[7] = uint64(
		((in[10] - minv) >> 28) |

			((in[11] - minv) << 14) |
			((in[12] - minv) << 56))

	out[8] = uint64(
		((in[12] - minv) >> 8) |

			((in[13] - minv) << 34))

	out[9] = uint64(
		((in[13] - minv) >> 30) |

			((in[14] - minv) << 12) |
			((in[15] - minv) << 54))

	out[10] = uint64(
		((in[15] - minv) >> 10) |

			((in[16] - minv) << 32))

	out[11] = uint64(
		((in[16] - minv) >> 32) |

			((in[17] - minv) << 10) |
			((in[18] - minv) << 52))

	out[12] = uint64(
		((in[18] - minv) >> 12) |

			((in[19] - minv) << 30))

	out[13] = uint64(
		((in[19] - minv) >> 34) |

			((in[20] - minv) << 8) |
			((in[21] - minv) << 50))

	out[14] = uint64(
		((in[21] - minv) >> 14) |

			((in[22] - minv) << 28))

	out[15] = uint64(
		((in[22] - minv) >> 36) |

			((in[23] - minv) << 6) |
			((in[24] - minv) << 48))

	out[16] = uint64(
		((in[24] - minv) >> 16) |

			((in[25] - minv) << 26))

	out[17] = uint64(
		((in[25] - minv) >> 38) |

			((in[26] - minv) << 4) |
			((in[27] - minv) << 46))

	out[18] = uint64(
		((in[27] - minv) >> 18) |

			((in[28] - minv) << 24))

	out[19] = uint64(
		((in[28] - minv) >> 40) |

			((in[29] - minv) << 2) |
			((in[30] - minv) << 44))

	out[20] = uint64(
		((in[30] - minv) >> 20) |

			((in[31] - minv) << 22))

	out[21] = uint64(
		((in[31] - minv) >> 42) |

			((in[32] - minv) << 0) |
			((in[33] - minv) << 42))

	out[22] = uint64(
		((in[33] - minv) >> 22) |

			((in[34] - minv) << 20) |
			((in[35] - minv) << 62))

	out[23] = uint64(
		((in[35] - minv) >> 2) |

			((in[36] - minv) << 40))

	out[24] = uint64(
		((in[36] - minv) >> 24) |

			((in[37] - minv) << 18) |
			((in[38] - minv) << 60))

	out[25] = uint64(
		((in[38] - minv) >> 4) |

			((in[39] - minv) << 38))

	out[26] = uint64(
		((in[39] - minv) >> 26) |

			((in[40] - minv) << 16) |
			((in[41] - minv) << 58))

	out[27] = uint64(
		((in[41] - minv) >> 6) |

			((in[42] - minv) << 36))

	out[28] = uint64(
		((in[42] - minv) >> 28) |

			((in[43] - minv) << 14) |
			((in[44] - minv) << 56))

	out[29] = uint64(
		((in[44] - minv) >> 8) |

			((in[45] - minv) << 34))

	out[30] = uint64(
		((in[45] - minv) >> 30) |

			((in[46] - minv) << 12) |
			((in[47] - minv) << 54))

	out[31] = uint64(
		((in[47] - minv) >> 10) |

			((in[48] - minv) << 32))

	out[32] = uint64(
		((in[48] - minv) >> 32) |

			((in[49] - minv) << 10) |
			((in[50] - minv) << 52))

	out[33] = uint64(
		((in[50] - minv) >> 12) |

			((in[51] - minv) << 30))

	out[34] = uint64(
		((in[51] - minv) >> 34) |

			((in[52] - minv) << 8) |
			((in[53] - minv) << 50))

	out[35] = uint64(
		((in[53] - minv) >> 14) |

			((in[54] - minv) << 28))

	out[36] = uint64(
		((in[54] - minv) >> 36) |

			((in[55] - minv) << 6) |
			((in[56] - minv) << 48))

	out[37] = uint64(
		((in[56] - minv) >> 16) |

			((in[57] - minv) << 26))

	out[38] = uint64(
		((in[57] - minv) >> 38) |

			((in[58] - minv) << 4) |
			((in[59] - minv) << 46))

	out[39] = uint64(
		((in[59] - minv) >> 18) |

			((in[60] - minv) << 24))

	out[40] = uint64(
		((in[60] - minv) >> 40) |

			((in[61] - minv) << 2) |
			((in[62] - minv) << 44))

	out[41] = uint64(
		((in[62] - minv) >> 20) |

			((in[63] - minv) << 22))

}
func bp64_43[T uint64 | int64](in *[64]T, out *[43]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 43))

	out[1] = uint64(
		((in[1] - minv) >> 21) |

			((in[2] - minv) << 22))

	out[2] = uint64(
		((in[2] - minv) >> 42) |

			((in[3] - minv) << 1) |
			((in[4] - minv) << 44))

	out[3] = uint64(
		((in[4] - minv) >> 20) |

			((in[5] - minv) << 23))

	out[4] = uint64(
		((in[5] - minv) >> 41) |

			((in[6] - minv) << 2) |
			((in[7] - minv) << 45))

	out[5] = uint64(
		((in[7] - minv) >> 19) |

			((in[8] - minv) << 24))

	out[6] = uint64(
		((in[8] - minv) >> 40) |

			((in[9] - minv) << 3) |
			((in[10] - minv) << 46))

	out[7] = uint64(
		((in[10] - minv) >> 18) |

			((in[11] - minv) << 25))

	out[8] = uint64(
		((in[11] - minv) >> 39) |

			((in[12] - minv) << 4) |
			((in[13] - minv) << 47))

	out[9] = uint64(
		((in[13] - minv) >> 17) |

			((in[14] - minv) << 26))

	out[10] = uint64(
		((in[14] - minv) >> 38) |

			((in[15] - minv) << 5) |
			((in[16] - minv) << 48))

	out[11] = uint64(
		((in[16] - minv) >> 16) |

			((in[17] - minv) << 27))

	out[12] = uint64(
		((in[17] - minv) >> 37) |

			((in[18] - minv) << 6) |
			((in[19] - minv) << 49))

	out[13] = uint64(
		((in[19] - minv) >> 15) |

			((in[20] - minv) << 28))

	out[14] = uint64(
		((in[20] - minv) >> 36) |

			((in[21] - minv) << 7) |
			((in[22] - minv) << 50))

	out[15] = uint64(
		((in[22] - minv) >> 14) |

			((in[23] - minv) << 29))

	out[16] = uint64(
		((in[23] - minv) >> 35) |

			((in[24] - minv) << 8) |
			((in[25] - minv) << 51))

	out[17] = uint64(
		((in[25] - minv) >> 13) |

			((in[26] - minv) << 30))

	out[18] = uint64(
		((in[26] - minv) >> 34) |

			((in[27] - minv) << 9) |
			((in[28] - minv) << 52))

	out[19] = uint64(
		((in[28] - minv) >> 12) |

			((in[29] - minv) << 31))

	out[20] = uint64(
		((in[29] - minv) >> 33) |

			((in[30] - minv) << 10) |
			((in[31] - minv) << 53))

	out[21] = uint64(
		((in[31] - minv) >> 11) |

			((in[32] - minv) << 32))

	out[22] = uint64(
		((in[32] - minv) >> 32) |

			((in[33] - minv) << 11) |
			((in[34] - minv) << 54))

	out[23] = uint64(
		((in[34] - minv) >> 10) |

			((in[35] - minv) << 33))

	out[24] = uint64(
		((in[35] - minv) >> 31) |

			((in[36] - minv) << 12) |
			((in[37] - minv) << 55))

	out[25] = uint64(
		((in[37] - minv) >> 9) |

			((in[38] - minv) << 34))

	out[26] = uint64(
		((in[38] - minv) >> 30) |

			((in[39] - minv) << 13) |
			((in[40] - minv) << 56))

	out[27] = uint64(
		((in[40] - minv) >> 8) |

			((in[41] - minv) << 35))

	out[28] = uint64(
		((in[41] - minv) >> 29) |

			((in[42] - minv) << 14) |
			((in[43] - minv) << 57))

	out[29] = uint64(
		((in[43] - minv) >> 7) |

			((in[44] - minv) << 36))

	out[30] = uint64(
		((in[44] - minv) >> 28) |

			((in[45] - minv) << 15) |
			((in[46] - minv) << 58))

	out[31] = uint64(
		((in[46] - minv) >> 6) |

			((in[47] - minv) << 37))

	out[32] = uint64(
		((in[47] - minv) >> 27) |

			((in[48] - minv) << 16) |
			((in[49] - minv) << 59))

	out[33] = uint64(
		((in[49] - minv) >> 5) |

			((in[50] - minv) << 38))

	out[34] = uint64(
		((in[50] - minv) >> 26) |

			((in[51] - minv) << 17) |
			((in[52] - minv) << 60))

	out[35] = uint64(
		((in[52] - minv) >> 4) |

			((in[53] - minv) << 39))

	out[36] = uint64(
		((in[53] - minv) >> 25) |

			((in[54] - minv) << 18) |
			((in[55] - minv) << 61))

	out[37] = uint64(
		((in[55] - minv) >> 3) |

			((in[56] - minv) << 40))

	out[38] = uint64(
		((in[56] - minv) >> 24) |

			((in[57] - minv) << 19) |
			((in[58] - minv) << 62))

	out[39] = uint64(
		((in[58] - minv) >> 2) |

			((in[59] - minv) << 41))

	out[40] = uint64(
		((in[59] - minv) >> 23) |

			((in[60] - minv) << 20) |
			((in[61] - minv) << 63))

	out[41] = uint64(
		((in[61] - minv) >> 1) |

			((in[62] - minv) << 42))

	out[42] = uint64(
		((in[62] - minv) >> 22) |

			((in[63] - minv) << 21))

}
func bp64_44[T uint64 | int64](in *[64]T, out *[44]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 44))

	out[1] = uint64(
		((in[1] - minv) >> 20) |

			((in[2] - minv) << 24))

	out[2] = uint64(
		((in[2] - minv) >> 40) |

			((in[3] - minv) << 4) |
			((in[4] - minv) << 48))

	out[3] = uint64(
		((in[4] - minv) >> 16) |

			((in[5] - minv) << 28))

	out[4] = uint64(
		((in[5] - minv) >> 36) |

			((in[6] - minv) << 8) |
			((in[7] - minv) << 52))

	out[5] = uint64(
		((in[7] - minv) >> 12) |

			((in[8] - minv) << 32))

	out[6] = uint64(
		((in[8] - minv) >> 32) |

			((in[9] - minv) << 12) |
			((in[10] - minv) << 56))

	out[7] = uint64(
		((in[10] - minv) >> 8) |

			((in[11] - minv) << 36))

	out[8] = uint64(
		((in[11] - minv) >> 28) |

			((in[12] - minv) << 16) |
			((in[13] - minv) << 60))

	out[9] = uint64(
		((in[13] - minv) >> 4) |

			((in[14] - minv) << 40))

	out[10] = uint64(
		((in[14] - minv) >> 24) |

			((in[15] - minv) << 20))

	out[11] = uint64(
		((in[15] - minv) >> 44) |

			((in[16] - minv) << 0) |
			((in[17] - minv) << 44))

	out[12] = uint64(
		((in[17] - minv) >> 20) |

			((in[18] - minv) << 24))

	out[13] = uint64(
		((in[18] - minv) >> 40) |

			((in[19] - minv) << 4) |
			((in[20] - minv) << 48))

	out[14] = uint64(
		((in[20] - minv) >> 16) |

			((in[21] - minv) << 28))

	out[15] = uint64(
		((in[21] - minv) >> 36) |

			((in[22] - minv) << 8) |
			((in[23] - minv) << 52))

	out[16] = uint64(
		((in[23] - minv) >> 12) |

			((in[24] - minv) << 32))

	out[17] = uint64(
		((in[24] - minv) >> 32) |

			((in[25] - minv) << 12) |
			((in[26] - minv) << 56))

	out[18] = uint64(
		((in[26] - minv) >> 8) |

			((in[27] - minv) << 36))

	out[19] = uint64(
		((in[27] - minv) >> 28) |

			((in[28] - minv) << 16) |
			((in[29] - minv) << 60))

	out[20] = uint64(
		((in[29] - minv) >> 4) |

			((in[30] - minv) << 40))

	out[21] = uint64(
		((in[30] - minv) >> 24) |

			((in[31] - minv) << 20))

	out[22] = uint64(
		((in[31] - minv) >> 44) |

			((in[32] - minv) << 0) |
			((in[33] - minv) << 44))

	out[23] = uint64(
		((in[33] - minv) >> 20) |

			((in[34] - minv) << 24))

	out[24] = uint64(
		((in[34] - minv) >> 40) |

			((in[35] - minv) << 4) |
			((in[36] - minv) << 48))

	out[25] = uint64(
		((in[36] - minv) >> 16) |

			((in[37] - minv) << 28))

	out[26] = uint64(
		((in[37] - minv) >> 36) |

			((in[38] - minv) << 8) |
			((in[39] - minv) << 52))

	out[27] = uint64(
		((in[39] - minv) >> 12) |

			((in[40] - minv) << 32))

	out[28] = uint64(
		((in[40] - minv) >> 32) |

			((in[41] - minv) << 12) |
			((in[42] - minv) << 56))

	out[29] = uint64(
		((in[42] - minv) >> 8) |

			((in[43] - minv) << 36))

	out[30] = uint64(
		((in[43] - minv) >> 28) |

			((in[44] - minv) << 16) |
			((in[45] - minv) << 60))

	out[31] = uint64(
		((in[45] - minv) >> 4) |

			((in[46] - minv) << 40))

	out[32] = uint64(
		((in[46] - minv) >> 24) |

			((in[47] - minv) << 20))

	out[33] = uint64(
		((in[47] - minv) >> 44) |

			((in[48] - minv) << 0) |
			((in[49] - minv) << 44))

	out[34] = uint64(
		((in[49] - minv) >> 20) |

			((in[50] - minv) << 24))

	out[35] = uint64(
		((in[50] - minv) >> 40) |

			((in[51] - minv) << 4) |
			((in[52] - minv) << 48))

	out[36] = uint64(
		((in[52] - minv) >> 16) |

			((in[53] - minv) << 28))

	out[37] = uint64(
		((in[53] - minv) >> 36) |

			((in[54] - minv) << 8) |
			((in[55] - minv) << 52))

	out[38] = uint64(
		((in[55] - minv) >> 12) |

			((in[56] - minv) << 32))

	out[39] = uint64(
		((in[56] - minv) >> 32) |

			((in[57] - minv) << 12) |
			((in[58] - minv) << 56))

	out[40] = uint64(
		((in[58] - minv) >> 8) |

			((in[59] - minv) << 36))

	out[41] = uint64(
		((in[59] - minv) >> 28) |

			((in[60] - minv) << 16) |
			((in[61] - minv) << 60))

	out[42] = uint64(
		((in[61] - minv) >> 4) |

			((in[62] - minv) << 40))

	out[43] = uint64(
		((in[62] - minv) >> 24) |

			((in[63] - minv) << 20))

}
func bp64_45[T uint64 | int64](in *[64]T, out *[45]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 45))

	out[1] = uint64(
		((in[1] - minv) >> 19) |

			((in[2] - minv) << 26))

	out[2] = uint64(
		((in[2] - minv) >> 38) |

			((in[3] - minv) << 7) |
			((in[4] - minv) << 52))

	out[3] = uint64(
		((in[4] - minv) >> 12) |

			((in[5] - minv) << 33))

	out[4] = uint64(
		((in[5] - minv) >> 31) |

			((in[6] - minv) << 14) |
			((in[7] - minv) << 59))

	out[5] = uint64(
		((in[7] - minv) >> 5) |

			((in[8] - minv) << 40))

	out[6] = uint64(
		((in[8] - minv) >> 24) |

			((in[9] - minv) << 21))

	out[7] = uint64(
		((in[9] - minv) >> 43) |

			((in[10] - minv) << 2) |
			((in[11] - minv) << 47))

	out[8] = uint64(
		((in[11] - minv) >> 17) |

			((in[12] - minv) << 28))

	out[9] = uint64(
		((in[12] - minv) >> 36) |

			((in[13] - minv) << 9) |
			((in[14] - minv) << 54))

	out[10] = uint64(
		((in[14] - minv) >> 10) |

			((in[15] - minv) << 35))

	out[11] = uint64(
		((in[15] - minv) >> 29) |

			((in[16] - minv) << 16) |
			((in[17] - minv) << 61))

	out[12] = uint64(
		((in[17] - minv) >> 3) |

			((in[18] - minv) << 42))

	out[13] = uint64(
		((in[18] - minv) >> 22) |

			((in[19] - minv) << 23))

	out[14] = uint64(
		((in[19] - minv) >> 41) |

			((in[20] - minv) << 4) |
			((in[21] - minv) << 49))

	out[15] = uint64(
		((in[21] - minv) >> 15) |

			((in[22] - minv) << 30))

	out[16] = uint64(
		((in[22] - minv) >> 34) |

			((in[23] - minv) << 11) |
			((in[24] - minv) << 56))

	out[17] = uint64(
		((in[24] - minv) >> 8) |

			((in[25] - minv) << 37))

	out[18] = uint64(
		((in[25] - minv) >> 27) |

			((in[26] - minv) << 18) |
			((in[27] - minv) << 63))

	out[19] = uint64(
		((in[27] - minv) >> 1) |

			((in[28] - minv) << 44))

	out[20] = uint64(
		((in[28] - minv) >> 20) |

			((in[29] - minv) << 25))

	out[21] = uint64(
		((in[29] - minv) >> 39) |

			((in[30] - minv) << 6) |
			((in[31] - minv) << 51))

	out[22] = uint64(
		((in[31] - minv) >> 13) |

			((in[32] - minv) << 32))

	out[23] = uint64(
		((in[32] - minv) >> 32) |

			((in[33] - minv) << 13) |
			((in[34] - minv) << 58))

	out[24] = uint64(
		((in[34] - minv) >> 6) |

			((in[35] - minv) << 39))

	out[25] = uint64(
		((in[35] - minv) >> 25) |

			((in[36] - minv) << 20))

	out[26] = uint64(
		((in[36] - minv) >> 44) |

			((in[37] - minv) << 1) |
			((in[38] - minv) << 46))

	out[27] = uint64(
		((in[38] - minv) >> 18) |

			((in[39] - minv) << 27))

	out[28] = uint64(
		((in[39] - minv) >> 37) |

			((in[40] - minv) << 8) |
			((in[41] - minv) << 53))

	out[29] = uint64(
		((in[41] - minv) >> 11) |

			((in[42] - minv) << 34))

	out[30] = uint64(
		((in[42] - minv) >> 30) |

			((in[43] - minv) << 15) |
			((in[44] - minv) << 60))

	out[31] = uint64(
		((in[44] - minv) >> 4) |

			((in[45] - minv) << 41))

	out[32] = uint64(
		((in[45] - minv) >> 23) |

			((in[46] - minv) << 22))

	out[33] = uint64(
		((in[46] - minv) >> 42) |

			((in[47] - minv) << 3) |
			((in[48] - minv) << 48))

	out[34] = uint64(
		((in[48] - minv) >> 16) |

			((in[49] - minv) << 29))

	out[35] = uint64(
		((in[49] - minv) >> 35) |

			((in[50] - minv) << 10) |
			((in[51] - minv) << 55))

	out[36] = uint64(
		((in[51] - minv) >> 9) |

			((in[52] - minv) << 36))

	out[37] = uint64(
		((in[52] - minv) >> 28) |

			((in[53] - minv) << 17) |
			((in[54] - minv) << 62))

	out[38] = uint64(
		((in[54] - minv) >> 2) |

			((in[55] - minv) << 43))

	out[39] = uint64(
		((in[55] - minv) >> 21) |

			((in[56] - minv) << 24))

	out[40] = uint64(
		((in[56] - minv) >> 40) |

			((in[57] - minv) << 5) |
			((in[58] - minv) << 50))

	out[41] = uint64(
		((in[58] - minv) >> 14) |

			((in[59] - minv) << 31))

	out[42] = uint64(
		((in[59] - minv) >> 33) |

			((in[60] - minv) << 12) |
			((in[61] - minv) << 57))

	out[43] = uint64(
		((in[61] - minv) >> 7) |

			((in[62] - minv) << 38))

	out[44] = uint64(
		((in[62] - minv) >> 26) |

			((in[63] - minv) << 19))

}
func bp64_46[T uint64 | int64](in *[64]T, out *[46]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 46))

	out[1] = uint64(
		((in[1] - minv) >> 18) |

			((in[2] - minv) << 28))

	out[2] = uint64(
		((in[2] - minv) >> 36) |

			((in[3] - minv) << 10) |
			((in[4] - minv) << 56))

	out[3] = uint64(
		((in[4] - minv) >> 8) |

			((in[5] - minv) << 38))

	out[4] = uint64(
		((in[5] - minv) >> 26) |

			((in[6] - minv) << 20))

	out[5] = uint64(
		((in[6] - minv) >> 44) |

			((in[7] - minv) << 2) |
			((in[8] - minv) << 48))

	out[6] = uint64(
		((in[8] - minv) >> 16) |

			((in[9] - minv) << 30))

	out[7] = uint64(
		((in[9] - minv) >> 34) |

			((in[10] - minv) << 12) |
			((in[11] - minv) << 58))

	out[8] = uint64(
		((in[11] - minv) >> 6) |

			((in[12] - minv) << 40))

	out[9] = uint64(
		((in[12] - minv) >> 24) |

			((in[13] - minv) << 22))

	out[10] = uint64(
		((in[13] - minv) >> 42) |

			((in[14] - minv) << 4) |
			((in[15] - minv) << 50))

	out[11] = uint64(
		((in[15] - minv) >> 14) |

			((in[16] - minv) << 32))

	out[12] = uint64(
		((in[16] - minv) >> 32) |

			((in[17] - minv) << 14) |
			((in[18] - minv) << 60))

	out[13] = uint64(
		((in[18] - minv) >> 4) |

			((in[19] - minv) << 42))

	out[14] = uint64(
		((in[19] - minv) >> 22) |

			((in[20] - minv) << 24))

	out[15] = uint64(
		((in[20] - minv) >> 40) |

			((in[21] - minv) << 6) |
			((in[22] - minv) << 52))

	out[16] = uint64(
		((in[22] - minv) >> 12) |

			((in[23] - minv) << 34))

	out[17] = uint64(
		((in[23] - minv) >> 30) |

			((in[24] - minv) << 16) |
			((in[25] - minv) << 62))

	out[18] = uint64(
		((in[25] - minv) >> 2) |

			((in[26] - minv) << 44))

	out[19] = uint64(
		((in[26] - minv) >> 20) |

			((in[27] - minv) << 26))

	out[20] = uint64(
		((in[27] - minv) >> 38) |

			((in[28] - minv) << 8) |
			((in[29] - minv) << 54))

	out[21] = uint64(
		((in[29] - minv) >> 10) |

			((in[30] - minv) << 36))

	out[22] = uint64(
		((in[30] - minv) >> 28) |

			((in[31] - minv) << 18))

	out[23] = uint64(
		((in[31] - minv) >> 46) |

			((in[32] - minv) << 0) |
			((in[33] - minv) << 46))

	out[24] = uint64(
		((in[33] - minv) >> 18) |

			((in[34] - minv) << 28))

	out[25] = uint64(
		((in[34] - minv) >> 36) |

			((in[35] - minv) << 10) |
			((in[36] - minv) << 56))

	out[26] = uint64(
		((in[36] - minv) >> 8) |

			((in[37] - minv) << 38))

	out[27] = uint64(
		((in[37] - minv) >> 26) |

			((in[38] - minv) << 20))

	out[28] = uint64(
		((in[38] - minv) >> 44) |

			((in[39] - minv) << 2) |
			((in[40] - minv) << 48))

	out[29] = uint64(
		((in[40] - minv) >> 16) |

			((in[41] - minv) << 30))

	out[30] = uint64(
		((in[41] - minv) >> 34) |

			((in[42] - minv) << 12) |
			((in[43] - minv) << 58))

	out[31] = uint64(
		((in[43] - minv) >> 6) |

			((in[44] - minv) << 40))

	out[32] = uint64(
		((in[44] - minv) >> 24) |

			((in[45] - minv) << 22))

	out[33] = uint64(
		((in[45] - minv) >> 42) |

			((in[46] - minv) << 4) |
			((in[47] - minv) << 50))

	out[34] = uint64(
		((in[47] - minv) >> 14) |

			((in[48] - minv) << 32))

	out[35] = uint64(
		((in[48] - minv) >> 32) |

			((in[49] - minv) << 14) |
			((in[50] - minv) << 60))

	out[36] = uint64(
		((in[50] - minv) >> 4) |

			((in[51] - minv) << 42))

	out[37] = uint64(
		((in[51] - minv) >> 22) |

			((in[52] - minv) << 24))

	out[38] = uint64(
		((in[52] - minv) >> 40) |

			((in[53] - minv) << 6) |
			((in[54] - minv) << 52))

	out[39] = uint64(
		((in[54] - minv) >> 12) |

			((in[55] - minv) << 34))

	out[40] = uint64(
		((in[55] - minv) >> 30) |

			((in[56] - minv) << 16) |
			((in[57] - minv) << 62))

	out[41] = uint64(
		((in[57] - minv) >> 2) |

			((in[58] - minv) << 44))

	out[42] = uint64(
		((in[58] - minv) >> 20) |

			((in[59] - minv) << 26))

	out[43] = uint64(
		((in[59] - minv) >> 38) |

			((in[60] - minv) << 8) |
			((in[61] - minv) << 54))

	out[44] = uint64(
		((in[61] - minv) >> 10) |

			((in[62] - minv) << 36))

	out[45] = uint64(
		((in[62] - minv) >> 28) |

			((in[63] - minv) << 18))

}
func bp64_47[T uint64 | int64](in *[64]T, out *[47]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 47))

	out[1] = uint64(
		((in[1] - minv) >> 17) |

			((in[2] - minv) << 30))

	out[2] = uint64(
		((in[2] - minv) >> 34) |

			((in[3] - minv) << 13) |
			((in[4] - minv) << 60))

	out[3] = uint64(
		((in[4] - minv) >> 4) |

			((in[5] - minv) << 43))

	out[4] = uint64(
		((in[5] - minv) >> 21) |

			((in[6] - minv) << 26))

	out[5] = uint64(
		((in[6] - minv) >> 38) |

			((in[7] - minv) << 9) |
			((in[8] - minv) << 56))

	out[6] = uint64(
		((in[8] - minv) >> 8) |

			((in[9] - minv) << 39))

	out[7] = uint64(
		((in[9] - minv) >> 25) |

			((in[10] - minv) << 22))

	out[8] = uint64(
		((in[10] - minv) >> 42) |

			((in[11] - minv) << 5) |
			((in[12] - minv) << 52))

	out[9] = uint64(
		((in[12] - minv) >> 12) |

			((in[13] - minv) << 35))

	out[10] = uint64(
		((in[13] - minv) >> 29) |

			((in[14] - minv) << 18))

	out[11] = uint64(
		((in[14] - minv) >> 46) |

			((in[15] - minv) << 1) |
			((in[16] - minv) << 48))

	out[12] = uint64(
		((in[16] - minv) >> 16) |

			((in[17] - minv) << 31))

	out[13] = uint64(
		((in[17] - minv) >> 33) |

			((in[18] - minv) << 14) |
			((in[19] - minv) << 61))

	out[14] = uint64(
		((in[19] - minv) >> 3) |

			((in[20] - minv) << 44))

	out[15] = uint64(
		((in[20] - minv) >> 20) |

			((in[21] - minv) << 27))

	out[16] = uint64(
		((in[21] - minv) >> 37) |

			((in[22] - minv) << 10) |
			((in[23] - minv) << 57))

	out[17] = uint64(
		((in[23] - minv) >> 7) |

			((in[24] - minv) << 40))

	out[18] = uint64(
		((in[24] - minv) >> 24) |

			((in[25] - minv) << 23))

	out[19] = uint64(
		((in[25] - minv) >> 41) |

			((in[26] - minv) << 6) |
			((in[27] - minv) << 53))

	out[20] = uint64(
		((in[27] - minv) >> 11) |

			((in[28] - minv) << 36))

	out[21] = uint64(
		((in[28] - minv) >> 28) |

			((in[29] - minv) << 19))

	out[22] = uint64(
		((in[29] - minv) >> 45) |

			((in[30] - minv) << 2) |
			((in[31] - minv) << 49))

	out[23] = uint64(
		((in[31] - minv) >> 15) |

			((in[32] - minv) << 32))

	out[24] = uint64(
		((in[32] - minv) >> 32) |

			((in[33] - minv) << 15) |
			((in[34] - minv) << 62))

	out[25] = uint64(
		((in[34] - minv) >> 2) |

			((in[35] - minv) << 45))

	out[26] = uint64(
		((in[35] - minv) >> 19) |

			((in[36] - minv) << 28))

	out[27] = uint64(
		((in[36] - minv) >> 36) |

			((in[37] - minv) << 11) |
			((in[38] - minv) << 58))

	out[28] = uint64(
		((in[38] - minv) >> 6) |

			((in[39] - minv) << 41))

	out[29] = uint64(
		((in[39] - minv) >> 23) |

			((in[40] - minv) << 24))

	out[30] = uint64(
		((in[40] - minv) >> 40) |

			((in[41] - minv) << 7) |
			((in[42] - minv) << 54))

	out[31] = uint64(
		((in[42] - minv) >> 10) |

			((in[43] - minv) << 37))

	out[32] = uint64(
		((in[43] - minv) >> 27) |

			((in[44] - minv) << 20))

	out[33] = uint64(
		((in[44] - minv) >> 44) |

			((in[45] - minv) << 3) |
			((in[46] - minv) << 50))

	out[34] = uint64(
		((in[46] - minv) >> 14) |

			((in[47] - minv) << 33))

	out[35] = uint64(
		((in[47] - minv) >> 31) |

			((in[48] - minv) << 16) |
			((in[49] - minv) << 63))

	out[36] = uint64(
		((in[49] - minv) >> 1) |

			((in[50] - minv) << 46))

	out[37] = uint64(
		((in[50] - minv) >> 18) |

			((in[51] - minv) << 29))

	out[38] = uint64(
		((in[51] - minv) >> 35) |

			((in[52] - minv) << 12) |
			((in[53] - minv) << 59))

	out[39] = uint64(
		((in[53] - minv) >> 5) |

			((in[54] - minv) << 42))

	out[40] = uint64(
		((in[54] - minv) >> 22) |

			((in[55] - minv) << 25))

	out[41] = uint64(
		((in[55] - minv) >> 39) |

			((in[56] - minv) << 8) |
			((in[57] - minv) << 55))

	out[42] = uint64(
		((in[57] - minv) >> 9) |

			((in[58] - minv) << 38))

	out[43] = uint64(
		((in[58] - minv) >> 26) |

			((in[59] - minv) << 21))

	out[44] = uint64(
		((in[59] - minv) >> 43) |

			((in[60] - minv) << 4) |
			((in[61] - minv) << 51))

	out[45] = uint64(
		((in[61] - minv) >> 13) |

			((in[62] - minv) << 34))

	out[46] = uint64(
		((in[62] - minv) >> 30) |

			((in[63] - minv) << 17))

}
func bp64_48[T uint64 | int64](in *[64]T, out *[48]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 48))

	out[1] = uint64(
		((in[1] - minv) >> 16) |

			((in[2] - minv) << 32))

	out[2] = uint64(
		((in[2] - minv) >> 32) |

			((in[3] - minv) << 16))

	out[3] = uint64(
		((in[3] - minv) >> 48) |

			((in[4] - minv) << 0) |
			((in[5] - minv) << 48))

	out[4] = uint64(
		((in[5] - minv) >> 16) |

			((in[6] - minv) << 32))

	out[5] = uint64(
		((in[6] - minv) >> 32) |

			((in[7] - minv) << 16))

	out[6] = uint64(
		((in[7] - minv) >> 48) |

			((in[8] - minv) << 0) |
			((in[9] - minv) << 48))

	out[7] = uint64(
		((in[9] - minv) >> 16) |

			((in[10] - minv) << 32))

	out[8] = uint64(
		((in[10] - minv) >> 32) |

			((in[11] - minv) << 16))

	out[9] = uint64(
		((in[11] - minv) >> 48) |

			((in[12] - minv) << 0) |
			((in[13] - minv) << 48))

	out[10] = uint64(
		((in[13] - minv) >> 16) |

			((in[14] - minv) << 32))

	out[11] = uint64(
		((in[14] - minv) >> 32) |

			((in[15] - minv) << 16))

	out[12] = uint64(
		((in[15] - minv) >> 48) |

			((in[16] - minv) << 0) |
			((in[17] - minv) << 48))

	out[13] = uint64(
		((in[17] - minv) >> 16) |

			((in[18] - minv) << 32))

	out[14] = uint64(
		((in[18] - minv) >> 32) |

			((in[19] - minv) << 16))

	out[15] = uint64(
		((in[19] - minv) >> 48) |

			((in[20] - minv) << 0) |
			((in[21] - minv) << 48))

	out[16] = uint64(
		((in[21] - minv) >> 16) |

			((in[22] - minv) << 32))

	out[17] = uint64(
		((in[22] - minv) >> 32) |

			((in[23] - minv) << 16))

	out[18] = uint64(
		((in[23] - minv) >> 48) |

			((in[24] - minv) << 0) |
			((in[25] - minv) << 48))

	out[19] = uint64(
		((in[25] - minv) >> 16) |

			((in[26] - minv) << 32))

	out[20] = uint64(
		((in[26] - minv) >> 32) |

			((in[27] - minv) << 16))

	out[21] = uint64(
		((in[27] - minv) >> 48) |

			((in[28] - minv) << 0) |
			((in[29] - minv) << 48))

	out[22] = uint64(
		((in[29] - minv) >> 16) |

			((in[30] - minv) << 32))

	out[23] = uint64(
		((in[30] - minv) >> 32) |

			((in[31] - minv) << 16))

	out[24] = uint64(
		((in[31] - minv) >> 48) |

			((in[32] - minv) << 0) |
			((in[33] - minv) << 48))

	out[25] = uint64(
		((in[33] - minv) >> 16) |

			((in[34] - minv) << 32))

	out[26] = uint64(
		((in[34] - minv) >> 32) |

			((in[35] - minv) << 16))

	out[27] = uint64(
		((in[35] - minv) >> 48) |

			((in[36] - minv) << 0) |
			((in[37] - minv) << 48))

	out[28] = uint64(
		((in[37] - minv) >> 16) |

			((in[38] - minv) << 32))

	out[29] = uint64(
		((in[38] - minv) >> 32) |

			((in[39] - minv) << 16))

	out[30] = uint64(
		((in[39] - minv) >> 48) |

			((in[40] - minv) << 0) |
			((in[41] - minv) << 48))

	out[31] = uint64(
		((in[41] - minv) >> 16) |

			((in[42] - minv) << 32))

	out[32] = uint64(
		((in[42] - minv) >> 32) |

			((in[43] - minv) << 16))

	out[33] = uint64(
		((in[43] - minv) >> 48) |

			((in[44] - minv) << 0) |
			((in[45] - minv) << 48))

	out[34] = uint64(
		((in[45] - minv) >> 16) |

			((in[46] - minv) << 32))

	out[35] = uint64(
		((in[46] - minv) >> 32) |

			((in[47] - minv) << 16))

	out[36] = uint64(
		((in[47] - minv) >> 48) |

			((in[48] - minv) << 0) |
			((in[49] - minv) << 48))

	out[37] = uint64(
		((in[49] - minv) >> 16) |

			((in[50] - minv) << 32))

	out[38] = uint64(
		((in[50] - minv) >> 32) |

			((in[51] - minv) << 16))

	out[39] = uint64(
		((in[51] - minv) >> 48) |

			((in[52] - minv) << 0) |
			((in[53] - minv) << 48))

	out[40] = uint64(
		((in[53] - minv) >> 16) |

			((in[54] - minv) << 32))

	out[41] = uint64(
		((in[54] - minv) >> 32) |

			((in[55] - minv) << 16))

	out[42] = uint64(
		((in[55] - minv) >> 48) |

			((in[56] - minv) << 0) |
			((in[57] - minv) << 48))

	out[43] = uint64(
		((in[57] - minv) >> 16) |

			((in[58] - minv) << 32))

	out[44] = uint64(
		((in[58] - minv) >> 32) |

			((in[59] - minv) << 16))

	out[45] = uint64(
		((in[59] - minv) >> 48) |

			((in[60] - minv) << 0) |
			((in[61] - minv) << 48))

	out[46] = uint64(
		((in[61] - minv) >> 16) |

			((in[62] - minv) << 32))

	out[47] = uint64(
		((in[62] - minv) >> 32) |

			((in[63] - minv) << 16))

}
func bp64_49[T uint64 | int64](in *[64]T, out *[49]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 49))

	out[1] = uint64(
		((in[1] - minv) >> 15) |

			((in[2] - minv) << 34))

	out[2] = uint64(
		((in[2] - minv) >> 30) |

			((in[3] - minv) << 19))

	out[3] = uint64(
		((in[3] - minv) >> 45) |

			((in[4] - minv) << 4) |
			((in[5] - minv) << 53))

	out[4] = uint64(
		((in[5] - minv) >> 11) |

			((in[6] - minv) << 38))

	out[5] = uint64(
		((in[6] - minv) >> 26) |

			((in[7] - minv) << 23))

	out[6] = uint64(
		((in[7] - minv) >> 41) |

			((in[8] - minv) << 8) |
			((in[9] - minv) << 57))

	out[7] = uint64(
		((in[9] - minv) >> 7) |

			((in[10] - minv) << 42))

	out[8] = uint64(
		((in[10] - minv) >> 22) |

			((in[11] - minv) << 27))

	out[9] = uint64(
		((in[11] - minv) >> 37) |

			((in[12] - minv) << 12) |
			((in[13] - minv) << 61))

	out[10] = uint64(
		((in[13] - minv) >> 3) |

			((in[14] - minv) << 46))

	out[11] = uint64(
		((in[14] - minv) >> 18) |

			((in[15] - minv) << 31))

	out[12] = uint64(
		((in[15] - minv) >> 33) |

			((in[16] - minv) << 16))

	out[13] = uint64(
		((in[16] - minv) >> 48) |

			((in[17] - minv) << 1) |
			((in[18] - minv) << 50))

	out[14] = uint64(
		((in[18] - minv) >> 14) |

			((in[19] - minv) << 35))

	out[15] = uint64(
		((in[19] - minv) >> 29) |

			((in[20] - minv) << 20))

	out[16] = uint64(
		((in[20] - minv) >> 44) |

			((in[21] - minv) << 5) |
			((in[22] - minv) << 54))

	out[17] = uint64(
		((in[22] - minv) >> 10) |

			((in[23] - minv) << 39))

	out[18] = uint64(
		((in[23] - minv) >> 25) |

			((in[24] - minv) << 24))

	out[19] = uint64(
		((in[24] - minv) >> 40) |

			((in[25] - minv) << 9) |
			((in[26] - minv) << 58))

	out[20] = uint64(
		((in[26] - minv) >> 6) |

			((in[27] - minv) << 43))

	out[21] = uint64(
		((in[27] - minv) >> 21) |

			((in[28] - minv) << 28))

	out[22] = uint64(
		((in[28] - minv) >> 36) |

			((in[29] - minv) << 13) |
			((in[30] - minv) << 62))

	out[23] = uint64(
		((in[30] - minv) >> 2) |

			((in[31] - minv) << 47))

	out[24] = uint64(
		((in[31] - minv) >> 17) |

			((in[32] - minv) << 32))

	out[25] = uint64(
		((in[32] - minv) >> 32) |

			((in[33] - minv) << 17))

	out[26] = uint64(
		((in[33] - minv) >> 47) |

			((in[34] - minv) << 2) |
			((in[35] - minv) << 51))

	out[27] = uint64(
		((in[35] - minv) >> 13) |

			((in[36] - minv) << 36))

	out[28] = uint64(
		((in[36] - minv) >> 28) |

			((in[37] - minv) << 21))

	out[29] = uint64(
		((in[37] - minv) >> 43) |

			((in[38] - minv) << 6) |
			((in[39] - minv) << 55))

	out[30] = uint64(
		((in[39] - minv) >> 9) |

			((in[40] - minv) << 40))

	out[31] = uint64(
		((in[40] - minv) >> 24) |

			((in[41] - minv) << 25))

	out[32] = uint64(
		((in[41] - minv) >> 39) |

			((in[42] - minv) << 10) |
			((in[43] - minv) << 59))

	out[33] = uint64(
		((in[43] - minv) >> 5) |

			((in[44] - minv) << 44))

	out[34] = uint64(
		((in[44] - minv) >> 20) |

			((in[45] - minv) << 29))

	out[35] = uint64(
		((in[45] - minv) >> 35) |

			((in[46] - minv) << 14) |
			((in[47] - minv) << 63))

	out[36] = uint64(
		((in[47] - minv) >> 1) |

			((in[48] - minv) << 48))

	out[37] = uint64(
		((in[48] - minv) >> 16) |

			((in[49] - minv) << 33))

	out[38] = uint64(
		((in[49] - minv) >> 31) |

			((in[50] - minv) << 18))

	out[39] = uint64(
		((in[50] - minv) >> 46) |

			((in[51] - minv) << 3) |
			((in[52] - minv) << 52))

	out[40] = uint64(
		((in[52] - minv) >> 12) |

			((in[53] - minv) << 37))

	out[41] = uint64(
		((in[53] - minv) >> 27) |

			((in[54] - minv) << 22))

	out[42] = uint64(
		((in[54] - minv) >> 42) |

			((in[55] - minv) << 7) |
			((in[56] - minv) << 56))

	out[43] = uint64(
		((in[56] - minv) >> 8) |

			((in[57] - minv) << 41))

	out[44] = uint64(
		((in[57] - minv) >> 23) |

			((in[58] - minv) << 26))

	out[45] = uint64(
		((in[58] - minv) >> 38) |

			((in[59] - minv) << 11) |
			((in[60] - minv) << 60))

	out[46] = uint64(
		((in[60] - minv) >> 4) |

			((in[61] - minv) << 45))

	out[47] = uint64(
		((in[61] - minv) >> 19) |

			((in[62] - minv) << 30))

	out[48] = uint64(
		((in[62] - minv) >> 34) |

			((in[63] - minv) << 15))

}
func bp64_50[T uint64 | int64](in *[64]T, out *[50]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 50))

	out[1] = uint64(
		((in[1] - minv) >> 14) |

			((in[2] - minv) << 36))

	out[2] = uint64(
		((in[2] - minv) >> 28) |

			((in[3] - minv) << 22))

	out[3] = uint64(
		((in[3] - minv) >> 42) |

			((in[4] - minv) << 8) |
			((in[5] - minv) << 58))

	out[4] = uint64(
		((in[5] - minv) >> 6) |

			((in[6] - minv) << 44))

	out[5] = uint64(
		((in[6] - minv) >> 20) |

			((in[7] - minv) << 30))

	out[6] = uint64(
		((in[7] - minv) >> 34) |

			((in[8] - minv) << 16))

	out[7] = uint64(
		((in[8] - minv) >> 48) |

			((in[9] - minv) << 2) |
			((in[10] - minv) << 52))

	out[8] = uint64(
		((in[10] - minv) >> 12) |

			((in[11] - minv) << 38))

	out[9] = uint64(
		((in[11] - minv) >> 26) |

			((in[12] - minv) << 24))

	out[10] = uint64(
		((in[12] - minv) >> 40) |

			((in[13] - minv) << 10) |
			((in[14] - minv) << 60))

	out[11] = uint64(
		((in[14] - minv) >> 4) |

			((in[15] - minv) << 46))

	out[12] = uint64(
		((in[15] - minv) >> 18) |

			((in[16] - minv) << 32))

	out[13] = uint64(
		((in[16] - minv) >> 32) |

			((in[17] - minv) << 18))

	out[14] = uint64(
		((in[17] - minv) >> 46) |

			((in[18] - minv) << 4) |
			((in[19] - minv) << 54))

	out[15] = uint64(
		((in[19] - minv) >> 10) |

			((in[20] - minv) << 40))

	out[16] = uint64(
		((in[20] - minv) >> 24) |

			((in[21] - minv) << 26))

	out[17] = uint64(
		((in[21] - minv) >> 38) |

			((in[22] - minv) << 12) |
			((in[23] - minv) << 62))

	out[18] = uint64(
		((in[23] - minv) >> 2) |

			((in[24] - minv) << 48))

	out[19] = uint64(
		((in[24] - minv) >> 16) |

			((in[25] - minv) << 34))

	out[20] = uint64(
		((in[25] - minv) >> 30) |

			((in[26] - minv) << 20))

	out[21] = uint64(
		((in[26] - minv) >> 44) |

			((in[27] - minv) << 6) |
			((in[28] - minv) << 56))

	out[22] = uint64(
		((in[28] - minv) >> 8) |

			((in[29] - minv) << 42))

	out[23] = uint64(
		((in[29] - minv) >> 22) |

			((in[30] - minv) << 28))

	out[24] = uint64(
		((in[30] - minv) >> 36) |

			((in[31] - minv) << 14))

	out[25] = uint64(
		((in[31] - minv) >> 50) |

			((in[32] - minv) << 0) |
			((in[33] - minv) << 50))

	out[26] = uint64(
		((in[33] - minv) >> 14) |

			((in[34] - minv) << 36))

	out[27] = uint64(
		((in[34] - minv) >> 28) |

			((in[35] - minv) << 22))

	out[28] = uint64(
		((in[35] - minv) >> 42) |

			((in[36] - minv) << 8) |
			((in[37] - minv) << 58))

	out[29] = uint64(
		((in[37] - minv) >> 6) |

			((in[38] - minv) << 44))

	out[30] = uint64(
		((in[38] - minv) >> 20) |

			((in[39] - minv) << 30))

	out[31] = uint64(
		((in[39] - minv) >> 34) |

			((in[40] - minv) << 16))

	out[32] = uint64(
		((in[40] - minv) >> 48) |

			((in[41] - minv) << 2) |
			((in[42] - minv) << 52))

	out[33] = uint64(
		((in[42] - minv) >> 12) |

			((in[43] - minv) << 38))

	out[34] = uint64(
		((in[43] - minv) >> 26) |

			((in[44] - minv) << 24))

	out[35] = uint64(
		((in[44] - minv) >> 40) |

			((in[45] - minv) << 10) |
			((in[46] - minv) << 60))

	out[36] = uint64(
		((in[46] - minv) >> 4) |

			((in[47] - minv) << 46))

	out[37] = uint64(
		((in[47] - minv) >> 18) |

			((in[48] - minv) << 32))

	out[38] = uint64(
		((in[48] - minv) >> 32) |

			((in[49] - minv) << 18))

	out[39] = uint64(
		((in[49] - minv) >> 46) |

			((in[50] - minv) << 4) |
			((in[51] - minv) << 54))

	out[40] = uint64(
		((in[51] - minv) >> 10) |

			((in[52] - minv) << 40))

	out[41] = uint64(
		((in[52] - minv) >> 24) |

			((in[53] - minv) << 26))

	out[42] = uint64(
		((in[53] - minv) >> 38) |

			((in[54] - minv) << 12) |
			((in[55] - minv) << 62))

	out[43] = uint64(
		((in[55] - minv) >> 2) |

			((in[56] - minv) << 48))

	out[44] = uint64(
		((in[56] - minv) >> 16) |

			((in[57] - minv) << 34))

	out[45] = uint64(
		((in[57] - minv) >> 30) |

			((in[58] - minv) << 20))

	out[46] = uint64(
		((in[58] - minv) >> 44) |

			((in[59] - minv) << 6) |
			((in[60] - minv) << 56))

	out[47] = uint64(
		((in[60] - minv) >> 8) |

			((in[61] - minv) << 42))

	out[48] = uint64(
		((in[61] - minv) >> 22) |

			((in[62] - minv) << 28))

	out[49] = uint64(
		((in[62] - minv) >> 36) |

			((in[63] - minv) << 14))

}
func bp64_51[T uint64 | int64](in *[64]T, out *[51]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 51))

	out[1] = uint64(
		((in[1] - minv) >> 13) |

			((in[2] - minv) << 38))

	out[2] = uint64(
		((in[2] - minv) >> 26) |

			((in[3] - minv) << 25))

	out[3] = uint64(
		((in[3] - minv) >> 39) |

			((in[4] - minv) << 12) |
			((in[5] - minv) << 63))

	out[4] = uint64(
		((in[5] - minv) >> 1) |

			((in[6] - minv) << 50))

	out[5] = uint64(
		((in[6] - minv) >> 14) |

			((in[7] - minv) << 37))

	out[6] = uint64(
		((in[7] - minv) >> 27) |

			((in[8] - minv) << 24))

	out[7] = uint64(
		((in[8] - minv) >> 40) |

			((in[9] - minv) << 11) |
			((in[10] - minv) << 62))

	out[8] = uint64(
		((in[10] - minv) >> 2) |

			((in[11] - minv) << 49))

	out[9] = uint64(
		((in[11] - minv) >> 15) |

			((in[12] - minv) << 36))

	out[10] = uint64(
		((in[12] - minv) >> 28) |

			((in[13] - minv) << 23))

	out[11] = uint64(
		((in[13] - minv) >> 41) |

			((in[14] - minv) << 10) |
			((in[15] - minv) << 61))

	out[12] = uint64(
		((in[15] - minv) >> 3) |

			((in[16] - minv) << 48))

	out[13] = uint64(
		((in[16] - minv) >> 16) |

			((in[17] - minv) << 35))

	out[14] = uint64(
		((in[17] - minv) >> 29) |

			((in[18] - minv) << 22))

	out[15] = uint64(
		((in[18] - minv) >> 42) |

			((in[19] - minv) << 9) |
			((in[20] - minv) << 60))

	out[16] = uint64(
		((in[20] - minv) >> 4) |

			((in[21] - minv) << 47))

	out[17] = uint64(
		((in[21] - minv) >> 17) |

			((in[22] - minv) << 34))

	out[18] = uint64(
		((in[22] - minv) >> 30) |

			((in[23] - minv) << 21))

	out[19] = uint64(
		((in[23] - minv) >> 43) |

			((in[24] - minv) << 8) |
			((in[25] - minv) << 59))

	out[20] = uint64(
		((in[25] - minv) >> 5) |

			((in[26] - minv) << 46))

	out[21] = uint64(
		((in[26] - minv) >> 18) |

			((in[27] - minv) << 33))

	out[22] = uint64(
		((in[27] - minv) >> 31) |

			((in[28] - minv) << 20))

	out[23] = uint64(
		((in[28] - minv) >> 44) |

			((in[29] - minv) << 7) |
			((in[30] - minv) << 58))

	out[24] = uint64(
		((in[30] - minv) >> 6) |

			((in[31] - minv) << 45))

	out[25] = uint64(
		((in[31] - minv) >> 19) |

			((in[32] - minv) << 32))

	out[26] = uint64(
		((in[32] - minv) >> 32) |

			((in[33] - minv) << 19))

	out[27] = uint64(
		((in[33] - minv) >> 45) |

			((in[34] - minv) << 6) |
			((in[35] - minv) << 57))

	out[28] = uint64(
		((in[35] - minv) >> 7) |

			((in[36] - minv) << 44))

	out[29] = uint64(
		((in[36] - minv) >> 20) |

			((in[37] - minv) << 31))

	out[30] = uint64(
		((in[37] - minv) >> 33) |

			((in[38] - minv) << 18))

	out[31] = uint64(
		((in[38] - minv) >> 46) |

			((in[39] - minv) << 5) |
			((in[40] - minv) << 56))

	out[32] = uint64(
		((in[40] - minv) >> 8) |

			((in[41] - minv) << 43))

	out[33] = uint64(
		((in[41] - minv) >> 21) |

			((in[42] - minv) << 30))

	out[34] = uint64(
		((in[42] - minv) >> 34) |

			((in[43] - minv) << 17))

	out[35] = uint64(
		((in[43] - minv) >> 47) |

			((in[44] - minv) << 4) |
			((in[45] - minv) << 55))

	out[36] = uint64(
		((in[45] - minv) >> 9) |

			((in[46] - minv) << 42))

	out[37] = uint64(
		((in[46] - minv) >> 22) |

			((in[47] - minv) << 29))

	out[38] = uint64(
		((in[47] - minv) >> 35) |

			((in[48] - minv) << 16))

	out[39] = uint64(
		((in[48] - minv) >> 48) |

			((in[49] - minv) << 3) |
			((in[50] - minv) << 54))

	out[40] = uint64(
		((in[50] - minv) >> 10) |

			((in[51] - minv) << 41))

	out[41] = uint64(
		((in[51] - minv) >> 23) |

			((in[52] - minv) << 28))

	out[42] = uint64(
		((in[52] - minv) >> 36) |

			((in[53] - minv) << 15))

	out[43] = uint64(
		((in[53] - minv) >> 49) |

			((in[54] - minv) << 2) |
			((in[55] - minv) << 53))

	out[44] = uint64(
		((in[55] - minv) >> 11) |

			((in[56] - minv) << 40))

	out[45] = uint64(
		((in[56] - minv) >> 24) |

			((in[57] - minv) << 27))

	out[46] = uint64(
		((in[57] - minv) >> 37) |

			((in[58] - minv) << 14))

	out[47] = uint64(
		((in[58] - minv) >> 50) |

			((in[59] - minv) << 1) |
			((in[60] - minv) << 52))

	out[48] = uint64(
		((in[60] - minv) >> 12) |

			((in[61] - minv) << 39))

	out[49] = uint64(
		((in[61] - minv) >> 25) |

			((in[62] - minv) << 26))

	out[50] = uint64(
		((in[62] - minv) >> 38) |

			((in[63] - minv) << 13))

}
func bp64_52[T uint64 | int64](in *[64]T, out *[52]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 52))

	out[1] = uint64(
		((in[1] - minv) >> 12) |

			((in[2] - minv) << 40))

	out[2] = uint64(
		((in[2] - minv) >> 24) |

			((in[3] - minv) << 28))

	out[3] = uint64(
		((in[3] - minv) >> 36) |

			((in[4] - minv) << 16))

	out[4] = uint64(
		((in[4] - minv) >> 48) |

			((in[5] - minv) << 4) |
			((in[6] - minv) << 56))

	out[5] = uint64(
		((in[6] - minv) >> 8) |

			((in[7] - minv) << 44))

	out[6] = uint64(
		((in[7] - minv) >> 20) |

			((in[8] - minv) << 32))

	out[7] = uint64(
		((in[8] - minv) >> 32) |

			((in[9] - minv) << 20))

	out[8] = uint64(
		((in[9] - minv) >> 44) |

			((in[10] - minv) << 8) |
			((in[11] - minv) << 60))

	out[9] = uint64(
		((in[11] - minv) >> 4) |

			((in[12] - minv) << 48))

	out[10] = uint64(
		((in[12] - minv) >> 16) |

			((in[13] - minv) << 36))

	out[11] = uint64(
		((in[13] - minv) >> 28) |

			((in[14] - minv) << 24))

	out[12] = uint64(
		((in[14] - minv) >> 40) |

			((in[15] - minv) << 12))

	out[13] = uint64(
		((in[15] - minv) >> 52) |

			((in[16] - minv) << 0) |
			((in[17] - minv) << 52))

	out[14] = uint64(
		((in[17] - minv) >> 12) |

			((in[18] - minv) << 40))

	out[15] = uint64(
		((in[18] - minv) >> 24) |

			((in[19] - minv) << 28))

	out[16] = uint64(
		((in[19] - minv) >> 36) |

			((in[20] - minv) << 16))

	out[17] = uint64(
		((in[20] - minv) >> 48) |

			((in[21] - minv) << 4) |
			((in[22] - minv) << 56))

	out[18] = uint64(
		((in[22] - minv) >> 8) |

			((in[23] - minv) << 44))

	out[19] = uint64(
		((in[23] - minv) >> 20) |

			((in[24] - minv) << 32))

	out[20] = uint64(
		((in[24] - minv) >> 32) |

			((in[25] - minv) << 20))

	out[21] = uint64(
		((in[25] - minv) >> 44) |

			((in[26] - minv) << 8) |
			((in[27] - minv) << 60))

	out[22] = uint64(
		((in[27] - minv) >> 4) |

			((in[28] - minv) << 48))

	out[23] = uint64(
		((in[28] - minv) >> 16) |

			((in[29] - minv) << 36))

	out[24] = uint64(
		((in[29] - minv) >> 28) |

			((in[30] - minv) << 24))

	out[25] = uint64(
		((in[30] - minv) >> 40) |

			((in[31] - minv) << 12))

	out[26] = uint64(
		((in[31] - minv) >> 52) |

			((in[32] - minv) << 0) |
			((in[33] - minv) << 52))

	out[27] = uint64(
		((in[33] - minv) >> 12) |

			((in[34] - minv) << 40))

	out[28] = uint64(
		((in[34] - minv) >> 24) |

			((in[35] - minv) << 28))

	out[29] = uint64(
		((in[35] - minv) >> 36) |

			((in[36] - minv) << 16))

	out[30] = uint64(
		((in[36] - minv) >> 48) |

			((in[37] - minv) << 4) |
			((in[38] - minv) << 56))

	out[31] = uint64(
		((in[38] - minv) >> 8) |

			((in[39] - minv) << 44))

	out[32] = uint64(
		((in[39] - minv) >> 20) |

			((in[40] - minv) << 32))

	out[33] = uint64(
		((in[40] - minv) >> 32) |

			((in[41] - minv) << 20))

	out[34] = uint64(
		((in[41] - minv) >> 44) |

			((in[42] - minv) << 8) |
			((in[43] - minv) << 60))

	out[35] = uint64(
		((in[43] - minv) >> 4) |

			((in[44] - minv) << 48))

	out[36] = uint64(
		((in[44] - minv) >> 16) |

			((in[45] - minv) << 36))

	out[37] = uint64(
		((in[45] - minv) >> 28) |

			((in[46] - minv) << 24))

	out[38] = uint64(
		((in[46] - minv) >> 40) |

			((in[47] - minv) << 12))

	out[39] = uint64(
		((in[47] - minv) >> 52) |

			((in[48] - minv) << 0) |
			((in[49] - minv) << 52))

	out[40] = uint64(
		((in[49] - minv) >> 12) |

			((in[50] - minv) << 40))

	out[41] = uint64(
		((in[50] - minv) >> 24) |

			((in[51] - minv) << 28))

	out[42] = uint64(
		((in[51] - minv) >> 36) |

			((in[52] - minv) << 16))

	out[43] = uint64(
		((in[52] - minv) >> 48) |

			((in[53] - minv) << 4) |
			((in[54] - minv) << 56))

	out[44] = uint64(
		((in[54] - minv) >> 8) |

			((in[55] - minv) << 44))

	out[45] = uint64(
		((in[55] - minv) >> 20) |

			((in[56] - minv) << 32))

	out[46] = uint64(
		((in[56] - minv) >> 32) |

			((in[57] - minv) << 20))

	out[47] = uint64(
		((in[57] - minv) >> 44) |

			((in[58] - minv) << 8) |
			((in[59] - minv) << 60))

	out[48] = uint64(
		((in[59] - minv) >> 4) |

			((in[60] - minv) << 48))

	out[49] = uint64(
		((in[60] - minv) >> 16) |

			((in[61] - minv) << 36))

	out[50] = uint64(
		((in[61] - minv) >> 28) |

			((in[62] - minv) << 24))

	out[51] = uint64(
		((in[62] - minv) >> 40) |

			((in[63] - minv) << 12))

}
func bp64_53[T uint64 | int64](in *[64]T, out *[53]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 53))

	out[1] = uint64(
		((in[1] - minv) >> 11) |

			((in[2] - minv) << 42))

	out[2] = uint64(
		((in[2] - minv) >> 22) |

			((in[3] - minv) << 31))

	out[3] = uint64(
		((in[3] - minv) >> 33) |

			((in[4] - minv) << 20))

	out[4] = uint64(
		((in[4] - minv) >> 44) |

			((in[5] - minv) << 9) |
			((in[6] - minv) << 62))

	out[5] = uint64(
		((in[6] - minv) >> 2) |

			((in[7] - minv) << 51))

	out[6] = uint64(
		((in[7] - minv) >> 13) |

			((in[8] - minv) << 40))

	out[7] = uint64(
		((in[8] - minv) >> 24) |

			((in[9] - minv) << 29))

	out[8] = uint64(
		((in[9] - minv) >> 35) |

			((in[10] - minv) << 18))

	out[9] = uint64(
		((in[10] - minv) >> 46) |

			((in[11] - minv) << 7) |
			((in[12] - minv) << 60))

	out[10] = uint64(
		((in[12] - minv) >> 4) |

			((in[13] - minv) << 49))

	out[11] = uint64(
		((in[13] - minv) >> 15) |

			((in[14] - minv) << 38))

	out[12] = uint64(
		((in[14] - minv) >> 26) |

			((in[15] - minv) << 27))

	out[13] = uint64(
		((in[15] - minv) >> 37) |

			((in[16] - minv) << 16))

	out[14] = uint64(
		((in[16] - minv) >> 48) |

			((in[17] - minv) << 5) |
			((in[18] - minv) << 58))

	out[15] = uint64(
		((in[18] - minv) >> 6) |

			((in[19] - minv) << 47))

	out[16] = uint64(
		((in[19] - minv) >> 17) |

			((in[20] - minv) << 36))

	out[17] = uint64(
		((in[20] - minv) >> 28) |

			((in[21] - minv) << 25))

	out[18] = uint64(
		((in[21] - minv) >> 39) |

			((in[22] - minv) << 14))

	out[19] = uint64(
		((in[22] - minv) >> 50) |

			((in[23] - minv) << 3) |
			((in[24] - minv) << 56))

	out[20] = uint64(
		((in[24] - minv) >> 8) |

			((in[25] - minv) << 45))

	out[21] = uint64(
		((in[25] - minv) >> 19) |

			((in[26] - minv) << 34))

	out[22] = uint64(
		((in[26] - minv) >> 30) |

			((in[27] - minv) << 23))

	out[23] = uint64(
		((in[27] - minv) >> 41) |

			((in[28] - minv) << 12))

	out[24] = uint64(
		((in[28] - minv) >> 52) |

			((in[29] - minv) << 1) |
			((in[30] - minv) << 54))

	out[25] = uint64(
		((in[30] - minv) >> 10) |

			((in[31] - minv) << 43))

	out[26] = uint64(
		((in[31] - minv) >> 21) |

			((in[32] - minv) << 32))

	out[27] = uint64(
		((in[32] - minv) >> 32) |

			((in[33] - minv) << 21))

	out[28] = uint64(
		((in[33] - minv) >> 43) |

			((in[34] - minv) << 10) |
			((in[35] - minv) << 63))

	out[29] = uint64(
		((in[35] - minv) >> 1) |

			((in[36] - minv) << 52))

	out[30] = uint64(
		((in[36] - minv) >> 12) |

			((in[37] - minv) << 41))

	out[31] = uint64(
		((in[37] - minv) >> 23) |

			((in[38] - minv) << 30))

	out[32] = uint64(
		((in[38] - minv) >> 34) |

			((in[39] - minv) << 19))

	out[33] = uint64(
		((in[39] - minv) >> 45) |

			((in[40] - minv) << 8) |
			((in[41] - minv) << 61))

	out[34] = uint64(
		((in[41] - minv) >> 3) |

			((in[42] - minv) << 50))

	out[35] = uint64(
		((in[42] - minv) >> 14) |

			((in[43] - minv) << 39))

	out[36] = uint64(
		((in[43] - minv) >> 25) |

			((in[44] - minv) << 28))

	out[37] = uint64(
		((in[44] - minv) >> 36) |

			((in[45] - minv) << 17))

	out[38] = uint64(
		((in[45] - minv) >> 47) |

			((in[46] - minv) << 6) |
			((in[47] - minv) << 59))

	out[39] = uint64(
		((in[47] - minv) >> 5) |

			((in[48] - minv) << 48))

	out[40] = uint64(
		((in[48] - minv) >> 16) |

			((in[49] - minv) << 37))

	out[41] = uint64(
		((in[49] - minv) >> 27) |

			((in[50] - minv) << 26))

	out[42] = uint64(
		((in[50] - minv) >> 38) |

			((in[51] - minv) << 15))

	out[43] = uint64(
		((in[51] - minv) >> 49) |

			((in[52] - minv) << 4) |
			((in[53] - minv) << 57))

	out[44] = uint64(
		((in[53] - minv) >> 7) |

			((in[54] - minv) << 46))

	out[45] = uint64(
		((in[54] - minv) >> 18) |

			((in[55] - minv) << 35))

	out[46] = uint64(
		((in[55] - minv) >> 29) |

			((in[56] - minv) << 24))

	out[47] = uint64(
		((in[56] - minv) >> 40) |

			((in[57] - minv) << 13))

	out[48] = uint64(
		((in[57] - minv) >> 51) |

			((in[58] - minv) << 2) |
			((in[59] - minv) << 55))

	out[49] = uint64(
		((in[59] - minv) >> 9) |

			((in[60] - minv) << 44))

	out[50] = uint64(
		((in[60] - minv) >> 20) |

			((in[61] - minv) << 33))

	out[51] = uint64(
		((in[61] - minv) >> 31) |

			((in[62] - minv) << 22))

	out[52] = uint64(
		((in[62] - minv) >> 42) |

			((in[63] - minv) << 11))

}
func bp64_54[T uint64 | int64](in *[64]T, out *[54]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 54))

	out[1] = uint64(
		((in[1] - minv) >> 10) |

			((in[2] - minv) << 44))

	out[2] = uint64(
		((in[2] - minv) >> 20) |

			((in[3] - minv) << 34))

	out[3] = uint64(
		((in[3] - minv) >> 30) |

			((in[4] - minv) << 24))

	out[4] = uint64(
		((in[4] - minv) >> 40) |

			((in[5] - minv) << 14))

	out[5] = uint64(
		((in[5] - minv) >> 50) |

			((in[6] - minv) << 4) |
			((in[7] - minv) << 58))

	out[6] = uint64(
		((in[7] - minv) >> 6) |

			((in[8] - minv) << 48))

	out[7] = uint64(
		((in[8] - minv) >> 16) |

			((in[9] - minv) << 38))

	out[8] = uint64(
		((in[9] - minv) >> 26) |

			((in[10] - minv) << 28))

	out[9] = uint64(
		((in[10] - minv) >> 36) |

			((in[11] - minv) << 18))

	out[10] = uint64(
		((in[11] - minv) >> 46) |

			((in[12] - minv) << 8) |
			((in[13] - minv) << 62))

	out[11] = uint64(
		((in[13] - minv) >> 2) |

			((in[14] - minv) << 52))

	out[12] = uint64(
		((in[14] - minv) >> 12) |

			((in[15] - minv) << 42))

	out[13] = uint64(
		((in[15] - minv) >> 22) |

			((in[16] - minv) << 32))

	out[14] = uint64(
		((in[16] - minv) >> 32) |

			((in[17] - minv) << 22))

	out[15] = uint64(
		((in[17] - minv) >> 42) |

			((in[18] - minv) << 12))

	out[16] = uint64(
		((in[18] - minv) >> 52) |

			((in[19] - minv) << 2) |
			((in[20] - minv) << 56))

	out[17] = uint64(
		((in[20] - minv) >> 8) |

			((in[21] - minv) << 46))

	out[18] = uint64(
		((in[21] - minv) >> 18) |

			((in[22] - minv) << 36))

	out[19] = uint64(
		((in[22] - minv) >> 28) |

			((in[23] - minv) << 26))

	out[20] = uint64(
		((in[23] - minv) >> 38) |

			((in[24] - minv) << 16))

	out[21] = uint64(
		((in[24] - minv) >> 48) |

			((in[25] - minv) << 6) |
			((in[26] - minv) << 60))

	out[22] = uint64(
		((in[26] - minv) >> 4) |

			((in[27] - minv) << 50))

	out[23] = uint64(
		((in[27] - minv) >> 14) |

			((in[28] - minv) << 40))

	out[24] = uint64(
		((in[28] - minv) >> 24) |

			((in[29] - minv) << 30))

	out[25] = uint64(
		((in[29] - minv) >> 34) |

			((in[30] - minv) << 20))

	out[26] = uint64(
		((in[30] - minv) >> 44) |

			((in[31] - minv) << 10))

	out[27] = uint64(
		((in[31] - minv) >> 54) |

			((in[32] - minv) << 0) |
			((in[33] - minv) << 54))

	out[28] = uint64(
		((in[33] - minv) >> 10) |

			((in[34] - minv) << 44))

	out[29] = uint64(
		((in[34] - minv) >> 20) |

			((in[35] - minv) << 34))

	out[30] = uint64(
		((in[35] - minv) >> 30) |

			((in[36] - minv) << 24))

	out[31] = uint64(
		((in[36] - minv) >> 40) |

			((in[37] - minv) << 14))

	out[32] = uint64(
		((in[37] - minv) >> 50) |

			((in[38] - minv) << 4) |
			((in[39] - minv) << 58))

	out[33] = uint64(
		((in[39] - minv) >> 6) |

			((in[40] - minv) << 48))

	out[34] = uint64(
		((in[40] - minv) >> 16) |

			((in[41] - minv) << 38))

	out[35] = uint64(
		((in[41] - minv) >> 26) |

			((in[42] - minv) << 28))

	out[36] = uint64(
		((in[42] - minv) >> 36) |

			((in[43] - minv) << 18))

	out[37] = uint64(
		((in[43] - minv) >> 46) |

			((in[44] - minv) << 8) |
			((in[45] - minv) << 62))

	out[38] = uint64(
		((in[45] - minv) >> 2) |

			((in[46] - minv) << 52))

	out[39] = uint64(
		((in[46] - minv) >> 12) |

			((in[47] - minv) << 42))

	out[40] = uint64(
		((in[47] - minv) >> 22) |

			((in[48] - minv) << 32))

	out[41] = uint64(
		((in[48] - minv) >> 32) |

			((in[49] - minv) << 22))

	out[42] = uint64(
		((in[49] - minv) >> 42) |

			((in[50] - minv) << 12))

	out[43] = uint64(
		((in[50] - minv) >> 52) |

			((in[51] - minv) << 2) |
			((in[52] - minv) << 56))

	out[44] = uint64(
		((in[52] - minv) >> 8) |

			((in[53] - minv) << 46))

	out[45] = uint64(
		((in[53] - minv) >> 18) |

			((in[54] - minv) << 36))

	out[46] = uint64(
		((in[54] - minv) >> 28) |

			((in[55] - minv) << 26))

	out[47] = uint64(
		((in[55] - minv) >> 38) |

			((in[56] - minv) << 16))

	out[48] = uint64(
		((in[56] - minv) >> 48) |

			((in[57] - minv) << 6) |
			((in[58] - minv) << 60))

	out[49] = uint64(
		((in[58] - minv) >> 4) |

			((in[59] - minv) << 50))

	out[50] = uint64(
		((in[59] - minv) >> 14) |

			((in[60] - minv) << 40))

	out[51] = uint64(
		((in[60] - minv) >> 24) |

			((in[61] - minv) << 30))

	out[52] = uint64(
		((in[61] - minv) >> 34) |

			((in[62] - minv) << 20))

	out[53] = uint64(
		((in[62] - minv) >> 44) |

			((in[63] - minv) << 10))

}
func bp64_55[T uint64 | int64](in *[64]T, out *[55]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 55))

	out[1] = uint64(
		((in[1] - minv) >> 9) |

			((in[2] - minv) << 46))

	out[2] = uint64(
		((in[2] - minv) >> 18) |

			((in[3] - minv) << 37))

	out[3] = uint64(
		((in[3] - minv) >> 27) |

			((in[4] - minv) << 28))

	out[4] = uint64(
		((in[4] - minv) >> 36) |

			((in[5] - minv) << 19))

	out[5] = uint64(
		((in[5] - minv) >> 45) |

			((in[6] - minv) << 10))

	out[6] = uint64(
		((in[6] - minv) >> 54) |

			((in[7] - minv) << 1) |
			((in[8] - minv) << 56))

	out[7] = uint64(
		((in[8] - minv) >> 8) |

			((in[9] - minv) << 47))

	out[8] = uint64(
		((in[9] - minv) >> 17) |

			((in[10] - minv) << 38))

	out[9] = uint64(
		((in[10] - minv) >> 26) |

			((in[11] - minv) << 29))

	out[10] = uint64(
		((in[11] - minv) >> 35) |

			((in[12] - minv) << 20))

	out[11] = uint64(
		((in[12] - minv) >> 44) |

			((in[13] - minv) << 11))

	out[12] = uint64(
		((in[13] - minv) >> 53) |

			((in[14] - minv) << 2) |
			((in[15] - minv) << 57))

	out[13] = uint64(
		((in[15] - minv) >> 7) |

			((in[16] - minv) << 48))

	out[14] = uint64(
		((in[16] - minv) >> 16) |

			((in[17] - minv) << 39))

	out[15] = uint64(
		((in[17] - minv) >> 25) |

			((in[18] - minv) << 30))

	out[16] = uint64(
		((in[18] - minv) >> 34) |

			((in[19] - minv) << 21))

	out[17] = uint64(
		((in[19] - minv) >> 43) |

			((in[20] - minv) << 12))

	out[18] = uint64(
		((in[20] - minv) >> 52) |

			((in[21] - minv) << 3) |
			((in[22] - minv) << 58))

	out[19] = uint64(
		((in[22] - minv) >> 6) |

			((in[23] - minv) << 49))

	out[20] = uint64(
		((in[23] - minv) >> 15) |

			((in[24] - minv) << 40))

	out[21] = uint64(
		((in[24] - minv) >> 24) |

			((in[25] - minv) << 31))

	out[22] = uint64(
		((in[25] - minv) >> 33) |

			((in[26] - minv) << 22))

	out[23] = uint64(
		((in[26] - minv) >> 42) |

			((in[27] - minv) << 13))

	out[24] = uint64(
		((in[27] - minv) >> 51) |

			((in[28] - minv) << 4) |
			((in[29] - minv) << 59))

	out[25] = uint64(
		((in[29] - minv) >> 5) |

			((in[30] - minv) << 50))

	out[26] = uint64(
		((in[30] - minv) >> 14) |

			((in[31] - minv) << 41))

	out[27] = uint64(
		((in[31] - minv) >> 23) |

			((in[32] - minv) << 32))

	out[28] = uint64(
		((in[32] - minv) >> 32) |

			((in[33] - minv) << 23))

	out[29] = uint64(
		((in[33] - minv) >> 41) |

			((in[34] - minv) << 14))

	out[30] = uint64(
		((in[34] - minv) >> 50) |

			((in[35] - minv) << 5) |
			((in[36] - minv) << 60))

	out[31] = uint64(
		((in[36] - minv) >> 4) |

			((in[37] - minv) << 51))

	out[32] = uint64(
		((in[37] - minv) >> 13) |

			((in[38] - minv) << 42))

	out[33] = uint64(
		((in[38] - minv) >> 22) |

			((in[39] - minv) << 33))

	out[34] = uint64(
		((in[39] - minv) >> 31) |

			((in[40] - minv) << 24))

	out[35] = uint64(
		((in[40] - minv) >> 40) |

			((in[41] - minv) << 15))

	out[36] = uint64(
		((in[41] - minv) >> 49) |

			((in[42] - minv) << 6) |
			((in[43] - minv) << 61))

	out[37] = uint64(
		((in[43] - minv) >> 3) |

			((in[44] - minv) << 52))

	out[38] = uint64(
		((in[44] - minv) >> 12) |

			((in[45] - minv) << 43))

	out[39] = uint64(
		((in[45] - minv) >> 21) |

			((in[46] - minv) << 34))

	out[40] = uint64(
		((in[46] - minv) >> 30) |

			((in[47] - minv) << 25))

	out[41] = uint64(
		((in[47] - minv) >> 39) |

			((in[48] - minv) << 16))

	out[42] = uint64(
		((in[48] - minv) >> 48) |

			((in[49] - minv) << 7) |
			((in[50] - minv) << 62))

	out[43] = uint64(
		((in[50] - minv) >> 2) |

			((in[51] - minv) << 53))

	out[44] = uint64(
		((in[51] - minv) >> 11) |

			((in[52] - minv) << 44))

	out[45] = uint64(
		((in[52] - minv) >> 20) |

			((in[53] - minv) << 35))

	out[46] = uint64(
		((in[53] - minv) >> 29) |

			((in[54] - minv) << 26))

	out[47] = uint64(
		((in[54] - minv) >> 38) |

			((in[55] - minv) << 17))

	out[48] = uint64(
		((in[55] - minv) >> 47) |

			((in[56] - minv) << 8) |
			((in[57] - minv) << 63))

	out[49] = uint64(
		((in[57] - minv) >> 1) |

			((in[58] - minv) << 54))

	out[50] = uint64(
		((in[58] - minv) >> 10) |

			((in[59] - minv) << 45))

	out[51] = uint64(
		((in[59] - minv) >> 19) |

			((in[60] - minv) << 36))

	out[52] = uint64(
		((in[60] - minv) >> 28) |

			((in[61] - minv) << 27))

	out[53] = uint64(
		((in[61] - minv) >> 37) |

			((in[62] - minv) << 18))

	out[54] = uint64(
		((in[62] - minv) >> 46) |

			((in[63] - minv) << 9))

}
func bp64_56[T uint64 | int64](in *[64]T, out *[56]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 56))

	out[1] = uint64(
		((in[1] - minv) >> 8) |

			((in[2] - minv) << 48))

	out[2] = uint64(
		((in[2] - minv) >> 16) |

			((in[3] - minv) << 40))

	out[3] = uint64(
		((in[3] - minv) >> 24) |

			((in[4] - minv) << 32))

	out[4] = uint64(
		((in[4] - minv) >> 32) |

			((in[5] - minv) << 24))

	out[5] = uint64(
		((in[5] - minv) >> 40) |

			((in[6] - minv) << 16))

	out[6] = uint64(
		((in[6] - minv) >> 48) |

			((in[7] - minv) << 8))

	out[7] = uint64(
		((in[7] - minv) >> 56) |

			((in[8] - minv) << 0) |
			((in[9] - minv) << 56))

	out[8] = uint64(
		((in[9] - minv) >> 8) |

			((in[10] - minv) << 48))

	out[9] = uint64(
		((in[10] - minv) >> 16) |

			((in[11] - minv) << 40))

	out[10] = uint64(
		((in[11] - minv) >> 24) |

			((in[12] - minv) << 32))

	out[11] = uint64(
		((in[12] - minv) >> 32) |

			((in[13] - minv) << 24))

	out[12] = uint64(
		((in[13] - minv) >> 40) |

			((in[14] - minv) << 16))

	out[13] = uint64(
		((in[14] - minv) >> 48) |

			((in[15] - minv) << 8))

	out[14] = uint64(
		((in[15] - minv) >> 56) |

			((in[16] - minv) << 0) |
			((in[17] - minv) << 56))

	out[15] = uint64(
		((in[17] - minv) >> 8) |

			((in[18] - minv) << 48))

	out[16] = uint64(
		((in[18] - minv) >> 16) |

			((in[19] - minv) << 40))

	out[17] = uint64(
		((in[19] - minv) >> 24) |

			((in[20] - minv) << 32))

	out[18] = uint64(
		((in[20] - minv) >> 32) |

			((in[21] - minv) << 24))

	out[19] = uint64(
		((in[21] - minv) >> 40) |

			((in[22] - minv) << 16))

	out[20] = uint64(
		((in[22] - minv) >> 48) |

			((in[23] - minv) << 8))

	out[21] = uint64(
		((in[23] - minv) >> 56) |

			((in[24] - minv) << 0) |
			((in[25] - minv) << 56))

	out[22] = uint64(
		((in[25] - minv) >> 8) |

			((in[26] - minv) << 48))

	out[23] = uint64(
		((in[26] - minv) >> 16) |

			((in[27] - minv) << 40))

	out[24] = uint64(
		((in[27] - minv) >> 24) |

			((in[28] - minv) << 32))

	out[25] = uint64(
		((in[28] - minv) >> 32) |

			((in[29] - minv) << 24))

	out[26] = uint64(
		((in[29] - minv) >> 40) |

			((in[30] - minv) << 16))

	out[27] = uint64(
		((in[30] - minv) >> 48) |

			((in[31] - minv) << 8))

	out[28] = uint64(
		((in[31] - minv) >> 56) |

			((in[32] - minv) << 0) |
			((in[33] - minv) << 56))

	out[29] = uint64(
		((in[33] - minv) >> 8) |

			((in[34] - minv) << 48))

	out[30] = uint64(
		((in[34] - minv) >> 16) |

			((in[35] - minv) << 40))

	out[31] = uint64(
		((in[35] - minv) >> 24) |

			((in[36] - minv) << 32))

	out[32] = uint64(
		((in[36] - minv) >> 32) |

			((in[37] - minv) << 24))

	out[33] = uint64(
		((in[37] - minv) >> 40) |

			((in[38] - minv) << 16))

	out[34] = uint64(
		((in[38] - minv) >> 48) |

			((in[39] - minv) << 8))

	out[35] = uint64(
		((in[39] - minv) >> 56) |

			((in[40] - minv) << 0) |
			((in[41] - minv) << 56))

	out[36] = uint64(
		((in[41] - minv) >> 8) |

			((in[42] - minv) << 48))

	out[37] = uint64(
		((in[42] - minv) >> 16) |

			((in[43] - minv) << 40))

	out[38] = uint64(
		((in[43] - minv) >> 24) |

			((in[44] - minv) << 32))

	out[39] = uint64(
		((in[44] - minv) >> 32) |

			((in[45] - minv) << 24))

	out[40] = uint64(
		((in[45] - minv) >> 40) |

			((in[46] - minv) << 16))

	out[41] = uint64(
		((in[46] - minv) >> 48) |

			((in[47] - minv) << 8))

	out[42] = uint64(
		((in[47] - minv) >> 56) |

			((in[48] - minv) << 0) |
			((in[49] - minv) << 56))

	out[43] = uint64(
		((in[49] - minv) >> 8) |

			((in[50] - minv) << 48))

	out[44] = uint64(
		((in[50] - minv) >> 16) |

			((in[51] - minv) << 40))

	out[45] = uint64(
		((in[51] - minv) >> 24) |

			((in[52] - minv) << 32))

	out[46] = uint64(
		((in[52] - minv) >> 32) |

			((in[53] - minv) << 24))

	out[47] = uint64(
		((in[53] - minv) >> 40) |

			((in[54] - minv) << 16))

	out[48] = uint64(
		((in[54] - minv) >> 48) |

			((in[55] - minv) << 8))

	out[49] = uint64(
		((in[55] - minv) >> 56) |

			((in[56] - minv) << 0) |
			((in[57] - minv) << 56))

	out[50] = uint64(
		((in[57] - minv) >> 8) |

			((in[58] - minv) << 48))

	out[51] = uint64(
		((in[58] - minv) >> 16) |

			((in[59] - minv) << 40))

	out[52] = uint64(
		((in[59] - minv) >> 24) |

			((in[60] - minv) << 32))

	out[53] = uint64(
		((in[60] - minv) >> 32) |

			((in[61] - minv) << 24))

	out[54] = uint64(
		((in[61] - minv) >> 40) |

			((in[62] - minv) << 16))

	out[55] = uint64(
		((in[62] - minv) >> 48) |

			((in[63] - minv) << 8))

}
func bp64_57[T uint64 | int64](in *[64]T, out *[57]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 57))

	out[1] = uint64(
		((in[1] - minv) >> 7) |

			((in[2] - minv) << 50))

	out[2] = uint64(
		((in[2] - minv) >> 14) |

			((in[3] - minv) << 43))

	out[3] = uint64(
		((in[3] - minv) >> 21) |

			((in[4] - minv) << 36))

	out[4] = uint64(
		((in[4] - minv) >> 28) |

			((in[5] - minv) << 29))

	out[5] = uint64(
		((in[5] - minv) >> 35) |

			((in[6] - minv) << 22))

	out[6] = uint64(
		((in[6] - minv) >> 42) |

			((in[7] - minv) << 15))

	out[7] = uint64(
		((in[7] - minv) >> 49) |

			((in[8] - minv) << 8))

	out[8] = uint64(
		((in[8] - minv) >> 56) |

			((in[9] - minv) << 1) |
			((in[10] - minv) << 58))

	out[9] = uint64(
		((in[10] - minv) >> 6) |

			((in[11] - minv) << 51))

	out[10] = uint64(
		((in[11] - minv) >> 13) |

			((in[12] - minv) << 44))

	out[11] = uint64(
		((in[12] - minv) >> 20) |

			((in[13] - minv) << 37))

	out[12] = uint64(
		((in[13] - minv) >> 27) |

			((in[14] - minv) << 30))

	out[13] = uint64(
		((in[14] - minv) >> 34) |

			((in[15] - minv) << 23))

	out[14] = uint64(
		((in[15] - minv) >> 41) |

			((in[16] - minv) << 16))

	out[15] = uint64(
		((in[16] - minv) >> 48) |

			((in[17] - minv) << 9))

	out[16] = uint64(
		((in[17] - minv) >> 55) |

			((in[18] - minv) << 2) |
			((in[19] - minv) << 59))

	out[17] = uint64(
		((in[19] - minv) >> 5) |

			((in[20] - minv) << 52))

	out[18] = uint64(
		((in[20] - minv) >> 12) |

			((in[21] - minv) << 45))

	out[19] = uint64(
		((in[21] - minv) >> 19) |

			((in[22] - minv) << 38))

	out[20] = uint64(
		((in[22] - minv) >> 26) |

			((in[23] - minv) << 31))

	out[21] = uint64(
		((in[23] - minv) >> 33) |

			((in[24] - minv) << 24))

	out[22] = uint64(
		((in[24] - minv) >> 40) |

			((in[25] - minv) << 17))

	out[23] = uint64(
		((in[25] - minv) >> 47) |

			((in[26] - minv) << 10))

	out[24] = uint64(
		((in[26] - minv) >> 54) |

			((in[27] - minv) << 3) |
			((in[28] - minv) << 60))

	out[25] = uint64(
		((in[28] - minv) >> 4) |

			((in[29] - minv) << 53))

	out[26] = uint64(
		((in[29] - minv) >> 11) |

			((in[30] - minv) << 46))

	out[27] = uint64(
		((in[30] - minv) >> 18) |

			((in[31] - minv) << 39))

	out[28] = uint64(
		((in[31] - minv) >> 25) |

			((in[32] - minv) << 32))

	out[29] = uint64(
		((in[32] - minv) >> 32) |

			((in[33] - minv) << 25))

	out[30] = uint64(
		((in[33] - minv) >> 39) |

			((in[34] - minv) << 18))

	out[31] = uint64(
		((in[34] - minv) >> 46) |

			((in[35] - minv) << 11))

	out[32] = uint64(
		((in[35] - minv) >> 53) |

			((in[36] - minv) << 4) |
			((in[37] - minv) << 61))

	out[33] = uint64(
		((in[37] - minv) >> 3) |

			((in[38] - minv) << 54))

	out[34] = uint64(
		((in[38] - minv) >> 10) |

			((in[39] - minv) << 47))

	out[35] = uint64(
		((in[39] - minv) >> 17) |

			((in[40] - minv) << 40))

	out[36] = uint64(
		((in[40] - minv) >> 24) |

			((in[41] - minv) << 33))

	out[37] = uint64(
		((in[41] - minv) >> 31) |

			((in[42] - minv) << 26))

	out[38] = uint64(
		((in[42] - minv) >> 38) |

			((in[43] - minv) << 19))

	out[39] = uint64(
		((in[43] - minv) >> 45) |

			((in[44] - minv) << 12))

	out[40] = uint64(
		((in[44] - minv) >> 52) |

			((in[45] - minv) << 5) |
			((in[46] - minv) << 62))

	out[41] = uint64(
		((in[46] - minv) >> 2) |

			((in[47] - minv) << 55))

	out[42] = uint64(
		((in[47] - minv) >> 9) |

			((in[48] - minv) << 48))

	out[43] = uint64(
		((in[48] - minv) >> 16) |

			((in[49] - minv) << 41))

	out[44] = uint64(
		((in[49] - minv) >> 23) |

			((in[50] - minv) << 34))

	out[45] = uint64(
		((in[50] - minv) >> 30) |

			((in[51] - minv) << 27))

	out[46] = uint64(
		((in[51] - minv) >> 37) |

			((in[52] - minv) << 20))

	out[47] = uint64(
		((in[52] - minv) >> 44) |

			((in[53] - minv) << 13))

	out[48] = uint64(
		((in[53] - minv) >> 51) |

			((in[54] - minv) << 6) |
			((in[55] - minv) << 63))

	out[49] = uint64(
		((in[55] - minv) >> 1) |

			((in[56] - minv) << 56))

	out[50] = uint64(
		((in[56] - minv) >> 8) |

			((in[57] - minv) << 49))

	out[51] = uint64(
		((in[57] - minv) >> 15) |

			((in[58] - minv) << 42))

	out[52] = uint64(
		((in[58] - minv) >> 22) |

			((in[59] - minv) << 35))

	out[53] = uint64(
		((in[59] - minv) >> 29) |

			((in[60] - minv) << 28))

	out[54] = uint64(
		((in[60] - minv) >> 36) |

			((in[61] - minv) << 21))

	out[55] = uint64(
		((in[61] - minv) >> 43) |

			((in[62] - minv) << 14))

	out[56] = uint64(
		((in[62] - minv) >> 50) |

			((in[63] - minv) << 7))

}
func bp64_58[T uint64 | int64](in *[64]T, out *[58]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 58))

	out[1] = uint64(
		((in[1] - minv) >> 6) |

			((in[2] - minv) << 52))

	out[2] = uint64(
		((in[2] - minv) >> 12) |

			((in[3] - minv) << 46))

	out[3] = uint64(
		((in[3] - minv) >> 18) |

			((in[4] - minv) << 40))

	out[4] = uint64(
		((in[4] - minv) >> 24) |

			((in[5] - minv) << 34))

	out[5] = uint64(
		((in[5] - minv) >> 30) |

			((in[6] - minv) << 28))

	out[6] = uint64(
		((in[6] - minv) >> 36) |

			((in[7] - minv) << 22))

	out[7] = uint64(
		((in[7] - minv) >> 42) |

			((in[8] - minv) << 16))

	out[8] = uint64(
		((in[8] - minv) >> 48) |

			((in[9] - minv) << 10))

	out[9] = uint64(
		((in[9] - minv) >> 54) |

			((in[10] - minv) << 4) |
			((in[11] - minv) << 62))

	out[10] = uint64(
		((in[11] - minv) >> 2) |

			((in[12] - minv) << 56))

	out[11] = uint64(
		((in[12] - minv) >> 8) |

			((in[13] - minv) << 50))

	out[12] = uint64(
		((in[13] - minv) >> 14) |

			((in[14] - minv) << 44))

	out[13] = uint64(
		((in[14] - minv) >> 20) |

			((in[15] - minv) << 38))

	out[14] = uint64(
		((in[15] - minv) >> 26) |

			((in[16] - minv) << 32))

	out[15] = uint64(
		((in[16] - minv) >> 32) |

			((in[17] - minv) << 26))

	out[16] = uint64(
		((in[17] - minv) >> 38) |

			((in[18] - minv) << 20))

	out[17] = uint64(
		((in[18] - minv) >> 44) |

			((in[19] - minv) << 14))

	out[18] = uint64(
		((in[19] - minv) >> 50) |

			((in[20] - minv) << 8))

	out[19] = uint64(
		((in[20] - minv) >> 56) |

			((in[21] - minv) << 2) |
			((in[22] - minv) << 60))

	out[20] = uint64(
		((in[22] - minv) >> 4) |

			((in[23] - minv) << 54))

	out[21] = uint64(
		((in[23] - minv) >> 10) |

			((in[24] - minv) << 48))

	out[22] = uint64(
		((in[24] - minv) >> 16) |

			((in[25] - minv) << 42))

	out[23] = uint64(
		((in[25] - minv) >> 22) |

			((in[26] - minv) << 36))

	out[24] = uint64(
		((in[26] - minv) >> 28) |

			((in[27] - minv) << 30))

	out[25] = uint64(
		((in[27] - minv) >> 34) |

			((in[28] - minv) << 24))

	out[26] = uint64(
		((in[28] - minv) >> 40) |

			((in[29] - minv) << 18))

	out[27] = uint64(
		((in[29] - minv) >> 46) |

			((in[30] - minv) << 12))

	out[28] = uint64(
		((in[30] - minv) >> 52) |

			((in[31] - minv) << 6))

	out[29] = uint64(
		((in[31] - minv) >> 58) |

			((in[32] - minv) << 0) |
			((in[33] - minv) << 58))

	out[30] = uint64(
		((in[33] - minv) >> 6) |

			((in[34] - minv) << 52))

	out[31] = uint64(
		((in[34] - minv) >> 12) |

			((in[35] - minv) << 46))

	out[32] = uint64(
		((in[35] - minv) >> 18) |

			((in[36] - minv) << 40))

	out[33] = uint64(
		((in[36] - minv) >> 24) |

			((in[37] - minv) << 34))

	out[34] = uint64(
		((in[37] - minv) >> 30) |

			((in[38] - minv) << 28))

	out[35] = uint64(
		((in[38] - minv) >> 36) |

			((in[39] - minv) << 22))

	out[36] = uint64(
		((in[39] - minv) >> 42) |

			((in[40] - minv) << 16))

	out[37] = uint64(
		((in[40] - minv) >> 48) |

			((in[41] - minv) << 10))

	out[38] = uint64(
		((in[41] - minv) >> 54) |

			((in[42] - minv) << 4) |
			((in[43] - minv) << 62))

	out[39] = uint64(
		((in[43] - minv) >> 2) |

			((in[44] - minv) << 56))

	out[40] = uint64(
		((in[44] - minv) >> 8) |

			((in[45] - minv) << 50))

	out[41] = uint64(
		((in[45] - minv) >> 14) |

			((in[46] - minv) << 44))

	out[42] = uint64(
		((in[46] - minv) >> 20) |

			((in[47] - minv) << 38))

	out[43] = uint64(
		((in[47] - minv) >> 26) |

			((in[48] - minv) << 32))

	out[44] = uint64(
		((in[48] - minv) >> 32) |

			((in[49] - minv) << 26))

	out[45] = uint64(
		((in[49] - minv) >> 38) |

			((in[50] - minv) << 20))

	out[46] = uint64(
		((in[50] - minv) >> 44) |

			((in[51] - minv) << 14))

	out[47] = uint64(
		((in[51] - minv) >> 50) |

			((in[52] - minv) << 8))

	out[48] = uint64(
		((in[52] - minv) >> 56) |

			((in[53] - minv) << 2) |
			((in[54] - minv) << 60))

	out[49] = uint64(
		((in[54] - minv) >> 4) |

			((in[55] - minv) << 54))

	out[50] = uint64(
		((in[55] - minv) >> 10) |

			((in[56] - minv) << 48))

	out[51] = uint64(
		((in[56] - minv) >> 16) |

			((in[57] - minv) << 42))

	out[52] = uint64(
		((in[57] - minv) >> 22) |

			((in[58] - minv) << 36))

	out[53] = uint64(
		((in[58] - minv) >> 28) |

			((in[59] - minv) << 30))

	out[54] = uint64(
		((in[59] - minv) >> 34) |

			((in[60] - minv) << 24))

	out[55] = uint64(
		((in[60] - minv) >> 40) |

			((in[61] - minv) << 18))

	out[56] = uint64(
		((in[61] - minv) >> 46) |

			((in[62] - minv) << 12))

	out[57] = uint64(
		((in[62] - minv) >> 52) |

			((in[63] - minv) << 6))

}
func bp64_59[T uint64 | int64](in *[64]T, out *[59]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 59))

	out[1] = uint64(
		((in[1] - minv) >> 5) |

			((in[2] - minv) << 54))

	out[2] = uint64(
		((in[2] - minv) >> 10) |

			((in[3] - minv) << 49))

	out[3] = uint64(
		((in[3] - minv) >> 15) |

			((in[4] - minv) << 44))

	out[4] = uint64(
		((in[4] - minv) >> 20) |

			((in[5] - minv) << 39))

	out[5] = uint64(
		((in[5] - minv) >> 25) |

			((in[6] - minv) << 34))

	out[6] = uint64(
		((in[6] - minv) >> 30) |

			((in[7] - minv) << 29))

	out[7] = uint64(
		((in[7] - minv) >> 35) |

			((in[8] - minv) << 24))

	out[8] = uint64(
		((in[8] - minv) >> 40) |

			((in[9] - minv) << 19))

	out[9] = uint64(
		((in[9] - minv) >> 45) |

			((in[10] - minv) << 14))

	out[10] = uint64(
		((in[10] - minv) >> 50) |

			((in[11] - minv) << 9))

	out[11] = uint64(
		((in[11] - minv) >> 55) |

			((in[12] - minv) << 4) |
			((in[13] - minv) << 63))

	out[12] = uint64(
		((in[13] - minv) >> 1) |

			((in[14] - minv) << 58))

	out[13] = uint64(
		((in[14] - minv) >> 6) |

			((in[15] - minv) << 53))

	out[14] = uint64(
		((in[15] - minv) >> 11) |

			((in[16] - minv) << 48))

	out[15] = uint64(
		((in[16] - minv) >> 16) |

			((in[17] - minv) << 43))

	out[16] = uint64(
		((in[17] - minv) >> 21) |

			((in[18] - minv) << 38))

	out[17] = uint64(
		((in[18] - minv) >> 26) |

			((in[19] - minv) << 33))

	out[18] = uint64(
		((in[19] - minv) >> 31) |

			((in[20] - minv) << 28))

	out[19] = uint64(
		((in[20] - minv) >> 36) |

			((in[21] - minv) << 23))

	out[20] = uint64(
		((in[21] - minv) >> 41) |

			((in[22] - minv) << 18))

	out[21] = uint64(
		((in[22] - minv) >> 46) |

			((in[23] - minv) << 13))

	out[22] = uint64(
		((in[23] - minv) >> 51) |

			((in[24] - minv) << 8))

	out[23] = uint64(
		((in[24] - minv) >> 56) |

			((in[25] - minv) << 3) |
			((in[26] - minv) << 62))

	out[24] = uint64(
		((in[26] - minv) >> 2) |

			((in[27] - minv) << 57))

	out[25] = uint64(
		((in[27] - minv) >> 7) |

			((in[28] - minv) << 52))

	out[26] = uint64(
		((in[28] - minv) >> 12) |

			((in[29] - minv) << 47))

	out[27] = uint64(
		((in[29] - minv) >> 17) |

			((in[30] - minv) << 42))

	out[28] = uint64(
		((in[30] - minv) >> 22) |

			((in[31] - minv) << 37))

	out[29] = uint64(
		((in[31] - minv) >> 27) |

			((in[32] - minv) << 32))

	out[30] = uint64(
		((in[32] - minv) >> 32) |

			((in[33] - minv) << 27))

	out[31] = uint64(
		((in[33] - minv) >> 37) |

			((in[34] - minv) << 22))

	out[32] = uint64(
		((in[34] - minv) >> 42) |

			((in[35] - minv) << 17))

	out[33] = uint64(
		((in[35] - minv) >> 47) |

			((in[36] - minv) << 12))

	out[34] = uint64(
		((in[36] - minv) >> 52) |

			((in[37] - minv) << 7))

	out[35] = uint64(
		((in[37] - minv) >> 57) |

			((in[38] - minv) << 2) |
			((in[39] - minv) << 61))

	out[36] = uint64(
		((in[39] - minv) >> 3) |

			((in[40] - minv) << 56))

	out[37] = uint64(
		((in[40] - minv) >> 8) |

			((in[41] - minv) << 51))

	out[38] = uint64(
		((in[41] - minv) >> 13) |

			((in[42] - minv) << 46))

	out[39] = uint64(
		((in[42] - minv) >> 18) |

			((in[43] - minv) << 41))

	out[40] = uint64(
		((in[43] - minv) >> 23) |

			((in[44] - minv) << 36))

	out[41] = uint64(
		((in[44] - minv) >> 28) |

			((in[45] - minv) << 31))

	out[42] = uint64(
		((in[45] - minv) >> 33) |

			((in[46] - minv) << 26))

	out[43] = uint64(
		((in[46] - minv) >> 38) |

			((in[47] - minv) << 21))

	out[44] = uint64(
		((in[47] - minv) >> 43) |

			((in[48] - minv) << 16))

	out[45] = uint64(
		((in[48] - minv) >> 48) |

			((in[49] - minv) << 11))

	out[46] = uint64(
		((in[49] - minv) >> 53) |

			((in[50] - minv) << 6))

	out[47] = uint64(
		((in[50] - minv) >> 58) |

			((in[51] - minv) << 1) |
			((in[52] - minv) << 60))

	out[48] = uint64(
		((in[52] - minv) >> 4) |

			((in[53] - minv) << 55))

	out[49] = uint64(
		((in[53] - minv) >> 9) |

			((in[54] - minv) << 50))

	out[50] = uint64(
		((in[54] - minv) >> 14) |

			((in[55] - minv) << 45))

	out[51] = uint64(
		((in[55] - minv) >> 19) |

			((in[56] - minv) << 40))

	out[52] = uint64(
		((in[56] - minv) >> 24) |

			((in[57] - minv) << 35))

	out[53] = uint64(
		((in[57] - minv) >> 29) |

			((in[58] - minv) << 30))

	out[54] = uint64(
		((in[58] - minv) >> 34) |

			((in[59] - minv) << 25))

	out[55] = uint64(
		((in[59] - minv) >> 39) |

			((in[60] - minv) << 20))

	out[56] = uint64(
		((in[60] - minv) >> 44) |

			((in[61] - minv) << 15))

	out[57] = uint64(
		((in[61] - minv) >> 49) |

			((in[62] - minv) << 10))

	out[58] = uint64(
		((in[62] - minv) >> 54) |

			((in[63] - minv) << 5))

}
func bp64_60[T uint64 | int64](in *[64]T, out *[60]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 60))

	out[1] = uint64(
		((in[1] - minv) >> 4) |

			((in[2] - minv) << 56))

	out[2] = uint64(
		((in[2] - minv) >> 8) |

			((in[3] - minv) << 52))

	out[3] = uint64(
		((in[3] - minv) >> 12) |

			((in[4] - minv) << 48))

	out[4] = uint64(
		((in[4] - minv) >> 16) |

			((in[5] - minv) << 44))

	out[5] = uint64(
		((in[5] - minv) >> 20) |

			((in[6] - minv) << 40))

	out[6] = uint64(
		((in[6] - minv) >> 24) |

			((in[7] - minv) << 36))

	out[7] = uint64(
		((in[7] - minv) >> 28) |

			((in[8] - minv) << 32))

	out[8] = uint64(
		((in[8] - minv) >> 32) |

			((in[9] - minv) << 28))

	out[9] = uint64(
		((in[9] - minv) >> 36) |

			((in[10] - minv) << 24))

	out[10] = uint64(
		((in[10] - minv) >> 40) |

			((in[11] - minv) << 20))

	out[11] = uint64(
		((in[11] - minv) >> 44) |

			((in[12] - minv) << 16))

	out[12] = uint64(
		((in[12] - minv) >> 48) |

			((in[13] - minv) << 12))

	out[13] = uint64(
		((in[13] - minv) >> 52) |

			((in[14] - minv) << 8))

	out[14] = uint64(
		((in[14] - minv) >> 56) |

			((in[15] - minv) << 4))

	out[15] = uint64(
		((in[15] - minv) >> 60) |

			((in[16] - minv) << 0) |
			((in[17] - minv) << 60))

	out[16] = uint64(
		((in[17] - minv) >> 4) |

			((in[18] - minv) << 56))

	out[17] = uint64(
		((in[18] - minv) >> 8) |

			((in[19] - minv) << 52))

	out[18] = uint64(
		((in[19] - minv) >> 12) |

			((in[20] - minv) << 48))

	out[19] = uint64(
		((in[20] - minv) >> 16) |

			((in[21] - minv) << 44))

	out[20] = uint64(
		((in[21] - minv) >> 20) |

			((in[22] - minv) << 40))

	out[21] = uint64(
		((in[22] - minv) >> 24) |

			((in[23] - minv) << 36))

	out[22] = uint64(
		((in[23] - minv) >> 28) |

			((in[24] - minv) << 32))

	out[23] = uint64(
		((in[24] - minv) >> 32) |

			((in[25] - minv) << 28))

	out[24] = uint64(
		((in[25] - minv) >> 36) |

			((in[26] - minv) << 24))

	out[25] = uint64(
		((in[26] - minv) >> 40) |

			((in[27] - minv) << 20))

	out[26] = uint64(
		((in[27] - minv) >> 44) |

			((in[28] - minv) << 16))

	out[27] = uint64(
		((in[28] - minv) >> 48) |

			((in[29] - minv) << 12))

	out[28] = uint64(
		((in[29] - minv) >> 52) |

			((in[30] - minv) << 8))

	out[29] = uint64(
		((in[30] - minv) >> 56) |

			((in[31] - minv) << 4))

	out[30] = uint64(
		((in[31] - minv) >> 60) |

			((in[32] - minv) << 0) |
			((in[33] - minv) << 60))

	out[31] = uint64(
		((in[33] - minv) >> 4) |

			((in[34] - minv) << 56))

	out[32] = uint64(
		((in[34] - minv) >> 8) |

			((in[35] - minv) << 52))

	out[33] = uint64(
		((in[35] - minv) >> 12) |

			((in[36] - minv) << 48))

	out[34] = uint64(
		((in[36] - minv) >> 16) |

			((in[37] - minv) << 44))

	out[35] = uint64(
		((in[37] - minv) >> 20) |

			((in[38] - minv) << 40))

	out[36] = uint64(
		((in[38] - minv) >> 24) |

			((in[39] - minv) << 36))

	out[37] = uint64(
		((in[39] - minv) >> 28) |

			((in[40] - minv) << 32))

	out[38] = uint64(
		((in[40] - minv) >> 32) |

			((in[41] - minv) << 28))

	out[39] = uint64(
		((in[41] - minv) >> 36) |

			((in[42] - minv) << 24))

	out[40] = uint64(
		((in[42] - minv) >> 40) |

			((in[43] - minv) << 20))

	out[41] = uint64(
		((in[43] - minv) >> 44) |

			((in[44] - minv) << 16))

	out[42] = uint64(
		((in[44] - minv) >> 48) |

			((in[45] - minv) << 12))

	out[43] = uint64(
		((in[45] - minv) >> 52) |

			((in[46] - minv) << 8))

	out[44] = uint64(
		((in[46] - minv) >> 56) |

			((in[47] - minv) << 4))

	out[45] = uint64(
		((in[47] - minv) >> 60) |

			((in[48] - minv) << 0) |
			((in[49] - minv) << 60))

	out[46] = uint64(
		((in[49] - minv) >> 4) |

			((in[50] - minv) << 56))

	out[47] = uint64(
		((in[50] - minv) >> 8) |

			((in[51] - minv) << 52))

	out[48] = uint64(
		((in[51] - minv) >> 12) |

			((in[52] - minv) << 48))

	out[49] = uint64(
		((in[52] - minv) >> 16) |

			((in[53] - minv) << 44))

	out[50] = uint64(
		((in[53] - minv) >> 20) |

			((in[54] - minv) << 40))

	out[51] = uint64(
		((in[54] - minv) >> 24) |

			((in[55] - minv) << 36))

	out[52] = uint64(
		((in[55] - minv) >> 28) |

			((in[56] - minv) << 32))

	out[53] = uint64(
		((in[56] - minv) >> 32) |

			((in[57] - minv) << 28))

	out[54] = uint64(
		((in[57] - minv) >> 36) |

			((in[58] - minv) << 24))

	out[55] = uint64(
		((in[58] - minv) >> 40) |

			((in[59] - minv) << 20))

	out[56] = uint64(
		((in[59] - minv) >> 44) |

			((in[60] - minv) << 16))

	out[57] = uint64(
		((in[60] - minv) >> 48) |

			((in[61] - minv) << 12))

	out[58] = uint64(
		((in[61] - minv) >> 52) |

			((in[62] - minv) << 8))

	out[59] = uint64(
		((in[62] - minv) >> 56) |

			((in[63] - minv) << 4))

}
func bp64_61[T uint64 | int64](in *[64]T, out *[61]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 61))

	out[1] = uint64(
		((in[1] - minv) >> 3) |

			((in[2] - minv) << 58))

	out[2] = uint64(
		((in[2] - minv) >> 6) |

			((in[3] - minv) << 55))

	out[3] = uint64(
		((in[3] - minv) >> 9) |

			((in[4] - minv) << 52))

	out[4] = uint64(
		((in[4] - minv) >> 12) |

			((in[5] - minv) << 49))

	out[5] = uint64(
		((in[5] - minv) >> 15) |

			((in[6] - minv) << 46))

	out[6] = uint64(
		((in[6] - minv) >> 18) |

			((in[7] - minv) << 43))

	out[7] = uint64(
		((in[7] - minv) >> 21) |

			((in[8] - minv) << 40))

	out[8] = uint64(
		((in[8] - minv) >> 24) |

			((in[9] - minv) << 37))

	out[9] = uint64(
		((in[9] - minv) >> 27) |

			((in[10] - minv) << 34))

	out[10] = uint64(
		((in[10] - minv) >> 30) |

			((in[11] - minv) << 31))

	out[11] = uint64(
		((in[11] - minv) >> 33) |

			((in[12] - minv) << 28))

	out[12] = uint64(
		((in[12] - minv) >> 36) |

			((in[13] - minv) << 25))

	out[13] = uint64(
		((in[13] - minv) >> 39) |

			((in[14] - minv) << 22))

	out[14] = uint64(
		((in[14] - minv) >> 42) |

			((in[15] - minv) << 19))

	out[15] = uint64(
		((in[15] - minv) >> 45) |

			((in[16] - minv) << 16))

	out[16] = uint64(
		((in[16] - minv) >> 48) |

			((in[17] - minv) << 13))

	out[17] = uint64(
		((in[17] - minv) >> 51) |

			((in[18] - minv) << 10))

	out[18] = uint64(
		((in[18] - minv) >> 54) |

			((in[19] - minv) << 7))

	out[19] = uint64(
		((in[19] - minv) >> 57) |

			((in[20] - minv) << 4))

	out[20] = uint64(
		((in[20] - minv) >> 60) |

			((in[21] - minv) << 1) |
			((in[22] - minv) << 62))

	out[21] = uint64(
		((in[22] - minv) >> 2) |

			((in[23] - minv) << 59))

	out[22] = uint64(
		((in[23] - minv) >> 5) |

			((in[24] - minv) << 56))

	out[23] = uint64(
		((in[24] - minv) >> 8) |

			((in[25] - minv) << 53))

	out[24] = uint64(
		((in[25] - minv) >> 11) |

			((in[26] - minv) << 50))

	out[25] = uint64(
		((in[26] - minv) >> 14) |

			((in[27] - minv) << 47))

	out[26] = uint64(
		((in[27] - minv) >> 17) |

			((in[28] - minv) << 44))

	out[27] = uint64(
		((in[28] - minv) >> 20) |

			((in[29] - minv) << 41))

	out[28] = uint64(
		((in[29] - minv) >> 23) |

			((in[30] - minv) << 38))

	out[29] = uint64(
		((in[30] - minv) >> 26) |

			((in[31] - minv) << 35))

	out[30] = uint64(
		((in[31] - minv) >> 29) |

			((in[32] - minv) << 32))

	out[31] = uint64(
		((in[32] - minv) >> 32) |

			((in[33] - minv) << 29))

	out[32] = uint64(
		((in[33] - minv) >> 35) |

			((in[34] - minv) << 26))

	out[33] = uint64(
		((in[34] - minv) >> 38) |

			((in[35] - minv) << 23))

	out[34] = uint64(
		((in[35] - minv) >> 41) |

			((in[36] - minv) << 20))

	out[35] = uint64(
		((in[36] - minv) >> 44) |

			((in[37] - minv) << 17))

	out[36] = uint64(
		((in[37] - minv) >> 47) |

			((in[38] - minv) << 14))

	out[37] = uint64(
		((in[38] - minv) >> 50) |

			((in[39] - minv) << 11))

	out[38] = uint64(
		((in[39] - minv) >> 53) |

			((in[40] - minv) << 8))

	out[39] = uint64(
		((in[40] - minv) >> 56) |

			((in[41] - minv) << 5))

	out[40] = uint64(
		((in[41] - minv) >> 59) |

			((in[42] - minv) << 2) |
			((in[43] - minv) << 63))

	out[41] = uint64(
		((in[43] - minv) >> 1) |

			((in[44] - minv) << 60))

	out[42] = uint64(
		((in[44] - minv) >> 4) |

			((in[45] - minv) << 57))

	out[43] = uint64(
		((in[45] - minv) >> 7) |

			((in[46] - minv) << 54))

	out[44] = uint64(
		((in[46] - minv) >> 10) |

			((in[47] - minv) << 51))

	out[45] = uint64(
		((in[47] - minv) >> 13) |

			((in[48] - minv) << 48))

	out[46] = uint64(
		((in[48] - minv) >> 16) |

			((in[49] - minv) << 45))

	out[47] = uint64(
		((in[49] - minv) >> 19) |

			((in[50] - minv) << 42))

	out[48] = uint64(
		((in[50] - minv) >> 22) |

			((in[51] - minv) << 39))

	out[49] = uint64(
		((in[51] - minv) >> 25) |

			((in[52] - minv) << 36))

	out[50] = uint64(
		((in[52] - minv) >> 28) |

			((in[53] - minv) << 33))

	out[51] = uint64(
		((in[53] - minv) >> 31) |

			((in[54] - minv) << 30))

	out[52] = uint64(
		((in[54] - minv) >> 34) |

			((in[55] - minv) << 27))

	out[53] = uint64(
		((in[55] - minv) >> 37) |

			((in[56] - minv) << 24))

	out[54] = uint64(
		((in[56] - minv) >> 40) |

			((in[57] - minv) << 21))

	out[55] = uint64(
		((in[57] - minv) >> 43) |

			((in[58] - minv) << 18))

	out[56] = uint64(
		((in[58] - minv) >> 46) |

			((in[59] - minv) << 15))

	out[57] = uint64(
		((in[59] - minv) >> 49) |

			((in[60] - minv) << 12))

	out[58] = uint64(
		((in[60] - minv) >> 52) |

			((in[61] - minv) << 9))

	out[59] = uint64(
		((in[61] - minv) >> 55) |

			((in[62] - minv) << 6))

	out[60] = uint64(
		((in[62] - minv) >> 58) |

			((in[63] - minv) << 3))

}
func bp64_62[T uint64 | int64](in *[64]T, out *[62]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 62))

	out[1] = uint64(
		((in[1] - minv) >> 2) |

			((in[2] - minv) << 60))

	out[2] = uint64(
		((in[2] - minv) >> 4) |

			((in[3] - minv) << 58))

	out[3] = uint64(
		((in[3] - minv) >> 6) |

			((in[4] - minv) << 56))

	out[4] = uint64(
		((in[4] - minv) >> 8) |

			((in[5] - minv) << 54))

	out[5] = uint64(
		((in[5] - minv) >> 10) |

			((in[6] - minv) << 52))

	out[6] = uint64(
		((in[6] - minv) >> 12) |

			((in[7] - minv) << 50))

	out[7] = uint64(
		((in[7] - minv) >> 14) |

			((in[8] - minv) << 48))

	out[8] = uint64(
		((in[8] - minv) >> 16) |

			((in[9] - minv) << 46))

	out[9] = uint64(
		((in[9] - minv) >> 18) |

			((in[10] - minv) << 44))

	out[10] = uint64(
		((in[10] - minv) >> 20) |

			((in[11] - minv) << 42))

	out[11] = uint64(
		((in[11] - minv) >> 22) |

			((in[12] - minv) << 40))

	out[12] = uint64(
		((in[12] - minv) >> 24) |

			((in[13] - minv) << 38))

	out[13] = uint64(
		((in[13] - minv) >> 26) |

			((in[14] - minv) << 36))

	out[14] = uint64(
		((in[14] - minv) >> 28) |

			((in[15] - minv) << 34))

	out[15] = uint64(
		((in[15] - minv) >> 30) |

			((in[16] - minv) << 32))

	out[16] = uint64(
		((in[16] - minv) >> 32) |

			((in[17] - minv) << 30))

	out[17] = uint64(
		((in[17] - minv) >> 34) |

			((in[18] - minv) << 28))

	out[18] = uint64(
		((in[18] - minv) >> 36) |

			((in[19] - minv) << 26))

	out[19] = uint64(
		((in[19] - minv) >> 38) |

			((in[20] - minv) << 24))

	out[20] = uint64(
		((in[20] - minv) >> 40) |

			((in[21] - minv) << 22))

	out[21] = uint64(
		((in[21] - minv) >> 42) |

			((in[22] - minv) << 20))

	out[22] = uint64(
		((in[22] - minv) >> 44) |

			((in[23] - minv) << 18))

	out[23] = uint64(
		((in[23] - minv) >> 46) |

			((in[24] - minv) << 16))

	out[24] = uint64(
		((in[24] - minv) >> 48) |

			((in[25] - minv) << 14))

	out[25] = uint64(
		((in[25] - minv) >> 50) |

			((in[26] - minv) << 12))

	out[26] = uint64(
		((in[26] - minv) >> 52) |

			((in[27] - minv) << 10))

	out[27] = uint64(
		((in[27] - minv) >> 54) |

			((in[28] - minv) << 8))

	out[28] = uint64(
		((in[28] - minv) >> 56) |

			((in[29] - minv) << 6))

	out[29] = uint64(
		((in[29] - minv) >> 58) |

			((in[30] - minv) << 4))

	out[30] = uint64(
		((in[30] - minv) >> 60) |

			((in[31] - minv) << 2))

	out[31] = uint64(
		((in[31] - minv) >> 62) |

			((in[32] - minv) << 0) |
			((in[33] - minv) << 62))

	out[32] = uint64(
		((in[33] - minv) >> 2) |

			((in[34] - minv) << 60))

	out[33] = uint64(
		((in[34] - minv) >> 4) |

			((in[35] - minv) << 58))

	out[34] = uint64(
		((in[35] - minv) >> 6) |

			((in[36] - minv) << 56))

	out[35] = uint64(
		((in[36] - minv) >> 8) |

			((in[37] - minv) << 54))

	out[36] = uint64(
		((in[37] - minv) >> 10) |

			((in[38] - minv) << 52))

	out[37] = uint64(
		((in[38] - minv) >> 12) |

			((in[39] - minv) << 50))

	out[38] = uint64(
		((in[39] - minv) >> 14) |

			((in[40] - minv) << 48))

	out[39] = uint64(
		((in[40] - minv) >> 16) |

			((in[41] - minv) << 46))

	out[40] = uint64(
		((in[41] - minv) >> 18) |

			((in[42] - minv) << 44))

	out[41] = uint64(
		((in[42] - minv) >> 20) |

			((in[43] - minv) << 42))

	out[42] = uint64(
		((in[43] - minv) >> 22) |

			((in[44] - minv) << 40))

	out[43] = uint64(
		((in[44] - minv) >> 24) |

			((in[45] - minv) << 38))

	out[44] = uint64(
		((in[45] - minv) >> 26) |

			((in[46] - minv) << 36))

	out[45] = uint64(
		((in[46] - minv) >> 28) |

			((in[47] - minv) << 34))

	out[46] = uint64(
		((in[47] - minv) >> 30) |

			((in[48] - minv) << 32))

	out[47] = uint64(
		((in[48] - minv) >> 32) |

			((in[49] - minv) << 30))

	out[48] = uint64(
		((in[49] - minv) >> 34) |

			((in[50] - minv) << 28))

	out[49] = uint64(
		((in[50] - minv) >> 36) |

			((in[51] - minv) << 26))

	out[50] = uint64(
		((in[51] - minv) >> 38) |

			((in[52] - minv) << 24))

	out[51] = uint64(
		((in[52] - minv) >> 40) |

			((in[53] - minv) << 22))

	out[52] = uint64(
		((in[53] - minv) >> 42) |

			((in[54] - minv) << 20))

	out[53] = uint64(
		((in[54] - minv) >> 44) |

			((in[55] - minv) << 18))

	out[54] = uint64(
		((in[55] - minv) >> 46) |

			((in[56] - minv) << 16))

	out[55] = uint64(
		((in[56] - minv) >> 48) |

			((in[57] - minv) << 14))

	out[56] = uint64(
		((in[57] - minv) >> 50) |

			((in[58] - minv) << 12))

	out[57] = uint64(
		((in[58] - minv) >> 52) |

			((in[59] - minv) << 10))

	out[58] = uint64(
		((in[59] - minv) >> 54) |

			((in[60] - minv) << 8))

	out[59] = uint64(
		((in[60] - minv) >> 56) |

			((in[61] - minv) << 6))

	out[60] = uint64(
		((in[61] - minv) >> 58) |

			((in[62] - minv) << 4))

	out[61] = uint64(
		((in[62] - minv) >> 60) |

			((in[63] - minv) << 2))

}
func bp64_63[T uint64 | int64](in *[64]T, out *[63]uint64, minv T) {
	out[0] = uint64(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 63))

	out[1] = uint64(
		((in[1] - minv) >> 1) |

			((in[2] - minv) << 62))

	out[2] = uint64(
		((in[2] - minv) >> 2) |

			((in[3] - minv) << 61))

	out[3] = uint64(
		((in[3] - minv) >> 3) |

			((in[4] - minv) << 60))

	out[4] = uint64(
		((in[4] - minv) >> 4) |

			((in[5] - minv) << 59))

	out[5] = uint64(
		((in[5] - minv) >> 5) |

			((in[6] - minv) << 58))

	out[6] = uint64(
		((in[6] - minv) >> 6) |

			((in[7] - minv) << 57))

	out[7] = uint64(
		((in[7] - minv) >> 7) |

			((in[8] - minv) << 56))

	out[8] = uint64(
		((in[8] - minv) >> 8) |

			((in[9] - minv) << 55))

	out[9] = uint64(
		((in[9] - minv) >> 9) |

			((in[10] - minv) << 54))

	out[10] = uint64(
		((in[10] - minv) >> 10) |

			((in[11] - minv) << 53))

	out[11] = uint64(
		((in[11] - minv) >> 11) |

			((in[12] - minv) << 52))

	out[12] = uint64(
		((in[12] - minv) >> 12) |

			((in[13] - minv) << 51))

	out[13] = uint64(
		((in[13] - minv) >> 13) |

			((in[14] - minv) << 50))

	out[14] = uint64(
		((in[14] - minv) >> 14) |

			((in[15] - minv) << 49))

	out[15] = uint64(
		((in[15] - minv) >> 15) |

			((in[16] - minv) << 48))

	out[16] = uint64(
		((in[16] - minv) >> 16) |

			((in[17] - minv) << 47))

	out[17] = uint64(
		((in[17] - minv) >> 17) |

			((in[18] - minv) << 46))

	out[18] = uint64(
		((in[18] - minv) >> 18) |

			((in[19] - minv) << 45))

	out[19] = uint64(
		((in[19] - minv) >> 19) |

			((in[20] - minv) << 44))

	out[20] = uint64(
		((in[20] - minv) >> 20) |

			((in[21] - minv) << 43))

	out[21] = uint64(
		((in[21] - minv) >> 21) |

			((in[22] - minv) << 42))

	out[22] = uint64(
		((in[22] - minv) >> 22) |

			((in[23] - minv) << 41))

	out[23] = uint64(
		((in[23] - minv) >> 23) |

			((in[24] - minv) << 40))

	out[24] = uint64(
		((in[24] - minv) >> 24) |

			((in[25] - minv) << 39))

	out[25] = uint64(
		((in[25] - minv) >> 25) |

			((in[26] - minv) << 38))

	out[26] = uint64(
		((in[26] - minv) >> 26) |

			((in[27] - minv) << 37))

	out[27] = uint64(
		((in[27] - minv) >> 27) |

			((in[28] - minv) << 36))

	out[28] = uint64(
		((in[28] - minv) >> 28) |

			((in[29] - minv) << 35))

	out[29] = uint64(
		((in[29] - minv) >> 29) |

			((in[30] - minv) << 34))

	out[30] = uint64(
		((in[30] - minv) >> 30) |

			((in[31] - minv) << 33))

	out[31] = uint64(
		((in[31] - minv) >> 31) |

			((in[32] - minv) << 32))

	out[32] = uint64(
		((in[32] - minv) >> 32) |

			((in[33] - minv) << 31))

	out[33] = uint64(
		((in[33] - minv) >> 33) |

			((in[34] - minv) << 30))

	out[34] = uint64(
		((in[34] - minv) >> 34) |

			((in[35] - minv) << 29))

	out[35] = uint64(
		((in[35] - minv) >> 35) |

			((in[36] - minv) << 28))

	out[36] = uint64(
		((in[36] - minv) >> 36) |

			((in[37] - minv) << 27))

	out[37] = uint64(
		((in[37] - minv) >> 37) |

			((in[38] - minv) << 26))

	out[38] = uint64(
		((in[38] - minv) >> 38) |

			((in[39] - minv) << 25))

	out[39] = uint64(
		((in[39] - minv) >> 39) |

			((in[40] - minv) << 24))

	out[40] = uint64(
		((in[40] - minv) >> 40) |

			((in[41] - minv) << 23))

	out[41] = uint64(
		((in[41] - minv) >> 41) |

			((in[42] - minv) << 22))

	out[42] = uint64(
		((in[42] - minv) >> 42) |

			((in[43] - minv) << 21))

	out[43] = uint64(
		((in[43] - minv) >> 43) |

			((in[44] - minv) << 20))

	out[44] = uint64(
		((in[44] - minv) >> 44) |

			((in[45] - minv) << 19))

	out[45] = uint64(
		((in[45] - minv) >> 45) |

			((in[46] - minv) << 18))

	out[46] = uint64(
		((in[46] - minv) >> 46) |

			((in[47] - minv) << 17))

	out[47] = uint64(
		((in[47] - minv) >> 47) |

			((in[48] - minv) << 16))

	out[48] = uint64(
		((in[48] - minv) >> 48) |

			((in[49] - minv) << 15))

	out[49] = uint64(
		((in[49] - minv) >> 49) |

			((in[50] - minv) << 14))

	out[50] = uint64(
		((in[50] - minv) >> 50) |

			((in[51] - minv) << 13))

	out[51] = uint64(
		((in[51] - minv) >> 51) |

			((in[52] - minv) << 12))

	out[52] = uint64(
		((in[52] - minv) >> 52) |

			((in[53] - minv) << 11))

	out[53] = uint64(
		((in[53] - minv) >> 53) |

			((in[54] - minv) << 10))

	out[54] = uint64(
		((in[54] - minv) >> 54) |

			((in[55] - minv) << 9))

	out[55] = uint64(
		((in[55] - minv) >> 55) |

			((in[56] - minv) << 8))

	out[56] = uint64(
		((in[56] - minv) >> 56) |

			((in[57] - minv) << 7))

	out[57] = uint64(
		((in[57] - minv) >> 57) |

			((in[58] - minv) << 6))

	out[58] = uint64(
		((in[58] - minv) >> 58) |

			((in[59] - minv) << 5))

	out[59] = uint64(
		((in[59] - minv) >> 59) |

			((in[60] - minv) << 4))

	out[60] = uint64(
		((in[60] - minv) >> 60) |

			((in[61] - minv) << 3))

	out[61] = uint64(
		((in[61] - minv) >> 61) |

			((in[62] - minv) << 2))

	out[62] = uint64(
		((in[62] - minv) >> 62) |

			((in[63] - minv) << 1))

}
