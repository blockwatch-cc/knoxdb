// Copyright (c) 2025 Blockwatch Data Inc.
// Code automatically generated - DO NOT EDIT.
// Any manual changes will be lost.

package pack

func bitpack32[T uint32 | int32](minv T, in []T, out []uint32, log2 int) {
	switch log2 {
	case 0:
		bp32_0((*[32]T)(in), (*[0]uint32)(out), minv)
	case 1:
		bp32_1((*[32]T)(in), (*[1]uint32)(out), minv)
	case 2:
		bp32_2((*[32]T)(in), (*[2]uint32)(out), minv)
	case 3:
		bp32_3((*[32]T)(in), (*[3]uint32)(out), minv)
	case 4:
		bp32_4((*[32]T)(in), (*[4]uint32)(out), minv)
	case 5:
		bp32_5((*[32]T)(in), (*[5]uint32)(out), minv)
	case 6:
		bp32_6((*[32]T)(in), (*[6]uint32)(out), minv)
	case 7:
		bp32_7((*[32]T)(in), (*[7]uint32)(out), minv)
	case 8:
		bp32_8((*[32]T)(in), (*[8]uint32)(out), minv)
	case 9:
		bp32_9((*[32]T)(in), (*[9]uint32)(out), minv)
	case 10:
		bp32_10((*[32]T)(in), (*[10]uint32)(out), minv)
	case 11:
		bp32_11((*[32]T)(in), (*[11]uint32)(out), minv)
	case 12:
		bp32_12((*[32]T)(in), (*[12]uint32)(out), minv)
	case 13:
		bp32_13((*[32]T)(in), (*[13]uint32)(out), minv)
	case 14:
		bp32_14((*[32]T)(in), (*[14]uint32)(out), minv)
	case 15:
		bp32_15((*[32]T)(in), (*[15]uint32)(out), minv)
	case 16:
		bp32_16((*[32]T)(in), (*[16]uint32)(out), minv)
	case 17:
		bp32_17((*[32]T)(in), (*[17]uint32)(out), minv)
	case 18:
		bp32_18((*[32]T)(in), (*[18]uint32)(out), minv)
	case 19:
		bp32_19((*[32]T)(in), (*[19]uint32)(out), minv)
	case 20:
		bp32_20((*[32]T)(in), (*[20]uint32)(out), minv)
	case 21:
		bp32_21((*[32]T)(in), (*[21]uint32)(out), minv)
	case 22:
		bp32_22((*[32]T)(in), (*[22]uint32)(out), minv)
	case 23:
		bp32_23((*[32]T)(in), (*[23]uint32)(out), minv)
	case 24:
		bp32_24((*[32]T)(in), (*[24]uint32)(out), minv)
	case 25:
		bp32_25((*[32]T)(in), (*[25]uint32)(out), minv)
	case 26:
		bp32_26((*[32]T)(in), (*[26]uint32)(out), minv)
	case 27:
		bp32_27((*[32]T)(in), (*[27]uint32)(out), minv)
	case 28:
		bp32_28((*[32]T)(in), (*[28]uint32)(out), minv)
	case 29:
		bp32_29((*[32]T)(in), (*[29]uint32)(out), minv)
	case 30:
		bp32_30((*[32]T)(in), (*[30]uint32)(out), minv)
	case 31:
		bp32_31((*[32]T)(in), (*[31]uint32)(out), minv)
	}

}
func bp32_0[T uint32 | int32](in *[32]T, out *[0]uint32, minv T) {
}
func bp32_1[T uint32 | int32](in *[32]T, out *[1]uint32, minv T) {
	out[0] = uint32(
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
			((in[31] - minv) << 31))

}
func bp32_2[T uint32 | int32](in *[32]T, out *[2]uint32, minv T) {
	out[0] = uint32(
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
			((in[15] - minv) << 30))

	out[1] = uint32(
		((in[16] - minv) << 0) |
			((in[17] - minv) << 2) |
			((in[18] - minv) << 4) |
			((in[19] - minv) << 6) |
			((in[20] - minv) << 8) |
			((in[21] - minv) << 10) |
			((in[22] - minv) << 12) |
			((in[23] - minv) << 14) |
			((in[24] - minv) << 16) |
			((in[25] - minv) << 18) |
			((in[26] - minv) << 20) |
			((in[27] - minv) << 22) |
			((in[28] - minv) << 24) |
			((in[29] - minv) << 26) |
			((in[30] - minv) << 28) |
			((in[31] - minv) << 30))

}
func bp32_3[T uint32 | int32](in *[32]T, out *[3]uint32, minv T) {
	out[0] = uint32(
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
			((in[10] - minv) << 30))

	out[1] = uint32(
		((in[10] - minv) >> 2) |

			((in[11] - minv) << 1) |
			((in[12] - minv) << 4) |
			((in[13] - minv) << 7) |
			((in[14] - minv) << 10) |
			((in[15] - minv) << 13) |
			((in[16] - minv) << 16) |
			((in[17] - minv) << 19) |
			((in[18] - minv) << 22) |
			((in[19] - minv) << 25) |
			((in[20] - minv) << 28) |
			((in[21] - minv) << 31))

	out[2] = uint32(
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
			((in[31] - minv) << 29))

}
func bp32_4[T uint32 | int32](in *[32]T, out *[4]uint32, minv T) {
	out[0] = uint32(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 4) |
			((in[2] - minv) << 8) |
			((in[3] - minv) << 12) |
			((in[4] - minv) << 16) |
			((in[5] - minv) << 20) |
			((in[6] - minv) << 24) |
			((in[7] - minv) << 28))

	out[1] = uint32(
		((in[8] - minv) << 0) |
			((in[9] - minv) << 4) |
			((in[10] - minv) << 8) |
			((in[11] - minv) << 12) |
			((in[12] - minv) << 16) |
			((in[13] - minv) << 20) |
			((in[14] - minv) << 24) |
			((in[15] - minv) << 28))

	out[2] = uint32(
		((in[16] - minv) << 0) |
			((in[17] - minv) << 4) |
			((in[18] - minv) << 8) |
			((in[19] - minv) << 12) |
			((in[20] - minv) << 16) |
			((in[21] - minv) << 20) |
			((in[22] - minv) << 24) |
			((in[23] - minv) << 28))

	out[3] = uint32(
		((in[24] - minv) << 0) |
			((in[25] - minv) << 4) |
			((in[26] - minv) << 8) |
			((in[27] - minv) << 12) |
			((in[28] - minv) << 16) |
			((in[29] - minv) << 20) |
			((in[30] - minv) << 24) |
			((in[31] - minv) << 28))

}
func bp32_5[T uint32 | int32](in *[32]T, out *[5]uint32, minv T) {
	out[0] = uint32(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 5) |
			((in[2] - minv) << 10) |
			((in[3] - minv) << 15) |
			((in[4] - minv) << 20) |
			((in[5] - minv) << 25) |
			((in[6] - minv) << 30))

	out[1] = uint32(
		((in[6] - minv) >> 2) |

			((in[7] - minv) << 3) |
			((in[8] - minv) << 8) |
			((in[9] - minv) << 13) |
			((in[10] - minv) << 18) |
			((in[11] - minv) << 23) |
			((in[12] - minv) << 28))

	out[2] = uint32(
		((in[12] - minv) >> 4) |

			((in[13] - minv) << 1) |
			((in[14] - minv) << 6) |
			((in[15] - minv) << 11) |
			((in[16] - minv) << 16) |
			((in[17] - minv) << 21) |
			((in[18] - minv) << 26) |
			((in[19] - minv) << 31))

	out[3] = uint32(
		((in[19] - minv) >> 1) |

			((in[20] - minv) << 4) |
			((in[21] - minv) << 9) |
			((in[22] - minv) << 14) |
			((in[23] - minv) << 19) |
			((in[24] - minv) << 24) |
			((in[25] - minv) << 29))

	out[4] = uint32(
		((in[25] - minv) >> 3) |

			((in[26] - minv) << 2) |
			((in[27] - minv) << 7) |
			((in[28] - minv) << 12) |
			((in[29] - minv) << 17) |
			((in[30] - minv) << 22) |
			((in[31] - minv) << 27))

}
func bp32_6[T uint32 | int32](in *[32]T, out *[6]uint32, minv T) {
	out[0] = uint32(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 6) |
			((in[2] - minv) << 12) |
			((in[3] - minv) << 18) |
			((in[4] - minv) << 24) |
			((in[5] - minv) << 30))

	out[1] = uint32(
		((in[5] - minv) >> 2) |

			((in[6] - minv) << 4) |
			((in[7] - minv) << 10) |
			((in[8] - minv) << 16) |
			((in[9] - minv) << 22) |
			((in[10] - minv) << 28))

	out[2] = uint32(
		((in[10] - minv) >> 4) |

			((in[11] - minv) << 2) |
			((in[12] - minv) << 8) |
			((in[13] - minv) << 14) |
			((in[14] - minv) << 20) |
			((in[15] - minv) << 26))

	out[3] = uint32(
		((in[15] - minv) >> 6) |

			((in[16] - minv) << 0) |
			((in[17] - minv) << 6) |
			((in[18] - minv) << 12) |
			((in[19] - minv) << 18) |
			((in[20] - minv) << 24) |
			((in[21] - minv) << 30))

	out[4] = uint32(
		((in[21] - minv) >> 2) |

			((in[22] - minv) << 4) |
			((in[23] - minv) << 10) |
			((in[24] - minv) << 16) |
			((in[25] - minv) << 22) |
			((in[26] - minv) << 28))

	out[5] = uint32(
		((in[26] - minv) >> 4) |

			((in[27] - minv) << 2) |
			((in[28] - minv) << 8) |
			((in[29] - minv) << 14) |
			((in[30] - minv) << 20) |
			((in[31] - minv) << 26))

}
func bp32_7[T uint32 | int32](in *[32]T, out *[7]uint32, minv T) {
	out[0] = uint32(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 7) |
			((in[2] - minv) << 14) |
			((in[3] - minv) << 21) |
			((in[4] - minv) << 28))

	out[1] = uint32(
		((in[4] - minv) >> 4) |

			((in[5] - minv) << 3) |
			((in[6] - minv) << 10) |
			((in[7] - minv) << 17) |
			((in[8] - minv) << 24) |
			((in[9] - minv) << 31))

	out[2] = uint32(
		((in[9] - minv) >> 1) |

			((in[10] - minv) << 6) |
			((in[11] - minv) << 13) |
			((in[12] - minv) << 20) |
			((in[13] - minv) << 27))

	out[3] = uint32(
		((in[13] - minv) >> 5) |

			((in[14] - minv) << 2) |
			((in[15] - minv) << 9) |
			((in[16] - minv) << 16) |
			((in[17] - minv) << 23) |
			((in[18] - minv) << 30))

	out[4] = uint32(
		((in[18] - minv) >> 2) |

			((in[19] - minv) << 5) |
			((in[20] - minv) << 12) |
			((in[21] - minv) << 19) |
			((in[22] - minv) << 26))

	out[5] = uint32(
		((in[22] - minv) >> 6) |

			((in[23] - minv) << 1) |
			((in[24] - minv) << 8) |
			((in[25] - minv) << 15) |
			((in[26] - minv) << 22) |
			((in[27] - minv) << 29))

	out[6] = uint32(
		((in[27] - minv) >> 3) |

			((in[28] - minv) << 4) |
			((in[29] - minv) << 11) |
			((in[30] - minv) << 18) |
			((in[31] - minv) << 25))

}
func bp32_8[T uint32 | int32](in *[32]T, out *[8]uint32, minv T) {
	out[0] = uint32(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 8) |
			((in[2] - minv) << 16) |
			((in[3] - minv) << 24))

	out[1] = uint32(
		((in[4] - minv) << 0) |
			((in[5] - minv) << 8) |
			((in[6] - minv) << 16) |
			((in[7] - minv) << 24))

	out[2] = uint32(
		((in[8] - minv) << 0) |
			((in[9] - minv) << 8) |
			((in[10] - minv) << 16) |
			((in[11] - minv) << 24))

	out[3] = uint32(
		((in[12] - minv) << 0) |
			((in[13] - minv) << 8) |
			((in[14] - minv) << 16) |
			((in[15] - minv) << 24))

	out[4] = uint32(
		((in[16] - minv) << 0) |
			((in[17] - minv) << 8) |
			((in[18] - minv) << 16) |
			((in[19] - minv) << 24))

	out[5] = uint32(
		((in[20] - minv) << 0) |
			((in[21] - minv) << 8) |
			((in[22] - minv) << 16) |
			((in[23] - minv) << 24))

	out[6] = uint32(
		((in[24] - minv) << 0) |
			((in[25] - minv) << 8) |
			((in[26] - minv) << 16) |
			((in[27] - minv) << 24))

	out[7] = uint32(
		((in[28] - minv) << 0) |
			((in[29] - minv) << 8) |
			((in[30] - minv) << 16) |
			((in[31] - minv) << 24))

}
func bp32_9[T uint32 | int32](in *[32]T, out *[9]uint32, minv T) {
	out[0] = uint32(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 9) |
			((in[2] - minv) << 18) |
			((in[3] - minv) << 27))

	out[1] = uint32(
		((in[3] - minv) >> 5) |

			((in[4] - minv) << 4) |
			((in[5] - minv) << 13) |
			((in[6] - minv) << 22) |
			((in[7] - minv) << 31))

	out[2] = uint32(
		((in[7] - minv) >> 1) |

			((in[8] - minv) << 8) |
			((in[9] - minv) << 17) |
			((in[10] - minv) << 26))

	out[3] = uint32(
		((in[10] - minv) >> 6) |

			((in[11] - minv) << 3) |
			((in[12] - minv) << 12) |
			((in[13] - minv) << 21) |
			((in[14] - minv) << 30))

	out[4] = uint32(
		((in[14] - minv) >> 2) |

			((in[15] - minv) << 7) |
			((in[16] - minv) << 16) |
			((in[17] - minv) << 25))

	out[5] = uint32(
		((in[17] - minv) >> 7) |

			((in[18] - minv) << 2) |
			((in[19] - minv) << 11) |
			((in[20] - minv) << 20) |
			((in[21] - minv) << 29))

	out[6] = uint32(
		((in[21] - minv) >> 3) |

			((in[22] - minv) << 6) |
			((in[23] - minv) << 15) |
			((in[24] - minv) << 24))

	out[7] = uint32(
		((in[24] - minv) >> 8) |

			((in[25] - minv) << 1) |
			((in[26] - minv) << 10) |
			((in[27] - minv) << 19) |
			((in[28] - minv) << 28))

	out[8] = uint32(
		((in[28] - minv) >> 4) |

			((in[29] - minv) << 5) |
			((in[30] - minv) << 14) |
			((in[31] - minv) << 23))

}
func bp32_10[T uint32 | int32](in *[32]T, out *[10]uint32, minv T) {
	out[0] = uint32(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 10) |
			((in[2] - minv) << 20) |
			((in[3] - minv) << 30))

	out[1] = uint32(
		((in[3] - minv) >> 2) |

			((in[4] - minv) << 8) |
			((in[5] - minv) << 18) |
			((in[6] - minv) << 28))

	out[2] = uint32(
		((in[6] - minv) >> 4) |

			((in[7] - minv) << 6) |
			((in[8] - minv) << 16) |
			((in[9] - minv) << 26))

	out[3] = uint32(
		((in[9] - minv) >> 6) |

			((in[10] - minv) << 4) |
			((in[11] - minv) << 14) |
			((in[12] - minv) << 24))

	out[4] = uint32(
		((in[12] - minv) >> 8) |

			((in[13] - minv) << 2) |
			((in[14] - minv) << 12) |
			((in[15] - minv) << 22))

	out[5] = uint32(
		((in[15] - minv) >> 10) |

			((in[16] - minv) << 0) |
			((in[17] - minv) << 10) |
			((in[18] - minv) << 20) |
			((in[19] - minv) << 30))

	out[6] = uint32(
		((in[19] - minv) >> 2) |

			((in[20] - minv) << 8) |
			((in[21] - minv) << 18) |
			((in[22] - minv) << 28))

	out[7] = uint32(
		((in[22] - minv) >> 4) |

			((in[23] - minv) << 6) |
			((in[24] - minv) << 16) |
			((in[25] - minv) << 26))

	out[8] = uint32(
		((in[25] - minv) >> 6) |

			((in[26] - minv) << 4) |
			((in[27] - minv) << 14) |
			((in[28] - minv) << 24))

	out[9] = uint32(
		((in[28] - minv) >> 8) |

			((in[29] - minv) << 2) |
			((in[30] - minv) << 12) |
			((in[31] - minv) << 22))

}
func bp32_11[T uint32 | int32](in *[32]T, out *[11]uint32, minv T) {
	out[0] = uint32(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 11) |
			((in[2] - minv) << 22))

	out[1] = uint32(
		((in[2] - minv) >> 10) |

			((in[3] - minv) << 1) |
			((in[4] - minv) << 12) |
			((in[5] - minv) << 23))

	out[2] = uint32(
		((in[5] - minv) >> 9) |

			((in[6] - minv) << 2) |
			((in[7] - minv) << 13) |
			((in[8] - minv) << 24))

	out[3] = uint32(
		((in[8] - minv) >> 8) |

			((in[9] - minv) << 3) |
			((in[10] - minv) << 14) |
			((in[11] - minv) << 25))

	out[4] = uint32(
		((in[11] - minv) >> 7) |

			((in[12] - minv) << 4) |
			((in[13] - minv) << 15) |
			((in[14] - minv) << 26))

	out[5] = uint32(
		((in[14] - minv) >> 6) |

			((in[15] - minv) << 5) |
			((in[16] - minv) << 16) |
			((in[17] - minv) << 27))

	out[6] = uint32(
		((in[17] - minv) >> 5) |

			((in[18] - minv) << 6) |
			((in[19] - minv) << 17) |
			((in[20] - minv) << 28))

	out[7] = uint32(
		((in[20] - minv) >> 4) |

			((in[21] - minv) << 7) |
			((in[22] - minv) << 18) |
			((in[23] - minv) << 29))

	out[8] = uint32(
		((in[23] - minv) >> 3) |

			((in[24] - minv) << 8) |
			((in[25] - minv) << 19) |
			((in[26] - minv) << 30))

	out[9] = uint32(
		((in[26] - minv) >> 2) |

			((in[27] - minv) << 9) |
			((in[28] - minv) << 20) |
			((in[29] - minv) << 31))

	out[10] = uint32(
		((in[29] - minv) >> 1) |

			((in[30] - minv) << 10) |
			((in[31] - minv) << 21))

}
func bp32_12[T uint32 | int32](in *[32]T, out *[12]uint32, minv T) {
	out[0] = uint32(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 12) |
			((in[2] - minv) << 24))

	out[1] = uint32(
		((in[2] - minv) >> 8) |

			((in[3] - minv) << 4) |
			((in[4] - minv) << 16) |
			((in[5] - minv) << 28))

	out[2] = uint32(
		((in[5] - minv) >> 4) |

			((in[6] - minv) << 8) |
			((in[7] - minv) << 20))

	out[3] = uint32(
		((in[7] - minv) >> 12) |

			((in[8] - minv) << 0) |
			((in[9] - minv) << 12) |
			((in[10] - minv) << 24))

	out[4] = uint32(
		((in[10] - minv) >> 8) |

			((in[11] - minv) << 4) |
			((in[12] - minv) << 16) |
			((in[13] - minv) << 28))

	out[5] = uint32(
		((in[13] - minv) >> 4) |

			((in[14] - minv) << 8) |
			((in[15] - minv) << 20))

	out[6] = uint32(
		((in[15] - minv) >> 12) |

			((in[16] - minv) << 0) |
			((in[17] - minv) << 12) |
			((in[18] - minv) << 24))

	out[7] = uint32(
		((in[18] - minv) >> 8) |

			((in[19] - minv) << 4) |
			((in[20] - minv) << 16) |
			((in[21] - minv) << 28))

	out[8] = uint32(
		((in[21] - minv) >> 4) |

			((in[22] - minv) << 8) |
			((in[23] - minv) << 20))

	out[9] = uint32(
		((in[23] - minv) >> 12) |

			((in[24] - minv) << 0) |
			((in[25] - minv) << 12) |
			((in[26] - minv) << 24))

	out[10] = uint32(
		((in[26] - minv) >> 8) |

			((in[27] - minv) << 4) |
			((in[28] - minv) << 16) |
			((in[29] - minv) << 28))

	out[11] = uint32(
		((in[29] - minv) >> 4) |

			((in[30] - minv) << 8) |
			((in[31] - minv) << 20))

}
func bp32_13[T uint32 | int32](in *[32]T, out *[13]uint32, minv T) {
	out[0] = uint32(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 13) |
			((in[2] - minv) << 26))

	out[1] = uint32(
		((in[2] - minv) >> 6) |

			((in[3] - minv) << 7) |
			((in[4] - minv) << 20))

	out[2] = uint32(
		((in[4] - minv) >> 12) |

			((in[5] - minv) << 1) |
			((in[6] - minv) << 14) |
			((in[7] - minv) << 27))

	out[3] = uint32(
		((in[7] - minv) >> 5) |

			((in[8] - minv) << 8) |
			((in[9] - minv) << 21))

	out[4] = uint32(
		((in[9] - minv) >> 11) |

			((in[10] - minv) << 2) |
			((in[11] - minv) << 15) |
			((in[12] - minv) << 28))

	out[5] = uint32(
		((in[12] - minv) >> 4) |

			((in[13] - minv) << 9) |
			((in[14] - minv) << 22))

	out[6] = uint32(
		((in[14] - minv) >> 10) |

			((in[15] - minv) << 3) |
			((in[16] - minv) << 16) |
			((in[17] - minv) << 29))

	out[7] = uint32(
		((in[17] - minv) >> 3) |

			((in[18] - minv) << 10) |
			((in[19] - minv) << 23))

	out[8] = uint32(
		((in[19] - minv) >> 9) |

			((in[20] - minv) << 4) |
			((in[21] - minv) << 17) |
			((in[22] - minv) << 30))

	out[9] = uint32(
		((in[22] - minv) >> 2) |

			((in[23] - minv) << 11) |
			((in[24] - minv) << 24))

	out[10] = uint32(
		((in[24] - minv) >> 8) |

			((in[25] - minv) << 5) |
			((in[26] - minv) << 18) |
			((in[27] - minv) << 31))

	out[11] = uint32(
		((in[27] - minv) >> 1) |

			((in[28] - minv) << 12) |
			((in[29] - minv) << 25))

	out[12] = uint32(
		((in[29] - minv) >> 7) |

			((in[30] - minv) << 6) |
			((in[31] - minv) << 19))

}
func bp32_14[T uint32 | int32](in *[32]T, out *[14]uint32, minv T) {
	out[0] = uint32(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 14) |
			((in[2] - minv) << 28))

	out[1] = uint32(
		((in[2] - minv) >> 4) |

			((in[3] - minv) << 10) |
			((in[4] - minv) << 24))

	out[2] = uint32(
		((in[4] - minv) >> 8) |

			((in[5] - minv) << 6) |
			((in[6] - minv) << 20))

	out[3] = uint32(
		((in[6] - minv) >> 12) |

			((in[7] - minv) << 2) |
			((in[8] - minv) << 16) |
			((in[9] - minv) << 30))

	out[4] = uint32(
		((in[9] - minv) >> 2) |

			((in[10] - minv) << 12) |
			((in[11] - minv) << 26))

	out[5] = uint32(
		((in[11] - minv) >> 6) |

			((in[12] - minv) << 8) |
			((in[13] - minv) << 22))

	out[6] = uint32(
		((in[13] - minv) >> 10) |

			((in[14] - minv) << 4) |
			((in[15] - minv) << 18))

	out[7] = uint32(
		((in[15] - minv) >> 14) |

			((in[16] - minv) << 0) |
			((in[17] - minv) << 14) |
			((in[18] - minv) << 28))

	out[8] = uint32(
		((in[18] - minv) >> 4) |

			((in[19] - minv) << 10) |
			((in[20] - minv) << 24))

	out[9] = uint32(
		((in[20] - minv) >> 8) |

			((in[21] - minv) << 6) |
			((in[22] - minv) << 20))

	out[10] = uint32(
		((in[22] - minv) >> 12) |

			((in[23] - minv) << 2) |
			((in[24] - minv) << 16) |
			((in[25] - minv) << 30))

	out[11] = uint32(
		((in[25] - minv) >> 2) |

			((in[26] - minv) << 12) |
			((in[27] - minv) << 26))

	out[12] = uint32(
		((in[27] - minv) >> 6) |

			((in[28] - minv) << 8) |
			((in[29] - minv) << 22))

	out[13] = uint32(
		((in[29] - minv) >> 10) |

			((in[30] - minv) << 4) |
			((in[31] - minv) << 18))

}
func bp32_15[T uint32 | int32](in *[32]T, out *[15]uint32, minv T) {
	out[0] = uint32(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 15) |
			((in[2] - minv) << 30))

	out[1] = uint32(
		((in[2] - minv) >> 2) |

			((in[3] - minv) << 13) |
			((in[4] - minv) << 28))

	out[2] = uint32(
		((in[4] - minv) >> 4) |

			((in[5] - minv) << 11) |
			((in[6] - minv) << 26))

	out[3] = uint32(
		((in[6] - minv) >> 6) |

			((in[7] - minv) << 9) |
			((in[8] - minv) << 24))

	out[4] = uint32(
		((in[8] - minv) >> 8) |

			((in[9] - minv) << 7) |
			((in[10] - minv) << 22))

	out[5] = uint32(
		((in[10] - minv) >> 10) |

			((in[11] - minv) << 5) |
			((in[12] - minv) << 20))

	out[6] = uint32(
		((in[12] - minv) >> 12) |

			((in[13] - minv) << 3) |
			((in[14] - minv) << 18))

	out[7] = uint32(
		((in[14] - minv) >> 14) |

			((in[15] - minv) << 1) |
			((in[16] - minv) << 16) |
			((in[17] - minv) << 31))

	out[8] = uint32(
		((in[17] - minv) >> 1) |

			((in[18] - minv) << 14) |
			((in[19] - minv) << 29))

	out[9] = uint32(
		((in[19] - minv) >> 3) |

			((in[20] - minv) << 12) |
			((in[21] - minv) << 27))

	out[10] = uint32(
		((in[21] - minv) >> 5) |

			((in[22] - minv) << 10) |
			((in[23] - minv) << 25))

	out[11] = uint32(
		((in[23] - minv) >> 7) |

			((in[24] - minv) << 8) |
			((in[25] - minv) << 23))

	out[12] = uint32(
		((in[25] - minv) >> 9) |

			((in[26] - minv) << 6) |
			((in[27] - minv) << 21))

	out[13] = uint32(
		((in[27] - minv) >> 11) |

			((in[28] - minv) << 4) |
			((in[29] - minv) << 19))

	out[14] = uint32(
		((in[29] - minv) >> 13) |

			((in[30] - minv) << 2) |
			((in[31] - minv) << 17))

}
func bp32_16[T uint32 | int32](in *[32]T, out *[16]uint32, minv T) {
	out[0] = uint32(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 16))

	out[1] = uint32(
		((in[2] - minv) << 0) |
			((in[3] - minv) << 16))

	out[2] = uint32(
		((in[4] - minv) << 0) |
			((in[5] - minv) << 16))

	out[3] = uint32(
		((in[6] - minv) << 0) |
			((in[7] - minv) << 16))

	out[4] = uint32(
		((in[8] - minv) << 0) |
			((in[9] - minv) << 16))

	out[5] = uint32(
		((in[10] - minv) << 0) |
			((in[11] - minv) << 16))

	out[6] = uint32(
		((in[12] - minv) << 0) |
			((in[13] - minv) << 16))

	out[7] = uint32(
		((in[14] - minv) << 0) |
			((in[15] - minv) << 16))

	out[8] = uint32(
		((in[16] - minv) << 0) |
			((in[17] - minv) << 16))

	out[9] = uint32(
		((in[18] - minv) << 0) |
			((in[19] - minv) << 16))

	out[10] = uint32(
		((in[20] - minv) << 0) |
			((in[21] - minv) << 16))

	out[11] = uint32(
		((in[22] - minv) << 0) |
			((in[23] - minv) << 16))

	out[12] = uint32(
		((in[24] - minv) << 0) |
			((in[25] - minv) << 16))

	out[13] = uint32(
		((in[26] - minv) << 0) |
			((in[27] - minv) << 16))

	out[14] = uint32(
		((in[28] - minv) << 0) |
			((in[29] - minv) << 16))

	out[15] = uint32(
		((in[30] - minv) << 0) |
			((in[31] - minv) << 16))

}
func bp32_17[T uint32 | int32](in *[32]T, out *[17]uint32, minv T) {
	out[0] = uint32(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 17))

	out[1] = uint32(
		((in[1] - minv) >> 15) |

			((in[2] - minv) << 2) |
			((in[3] - minv) << 19))

	out[2] = uint32(
		((in[3] - minv) >> 13) |

			((in[4] - minv) << 4) |
			((in[5] - minv) << 21))

	out[3] = uint32(
		((in[5] - minv) >> 11) |

			((in[6] - minv) << 6) |
			((in[7] - minv) << 23))

	out[4] = uint32(
		((in[7] - minv) >> 9) |

			((in[8] - minv) << 8) |
			((in[9] - minv) << 25))

	out[5] = uint32(
		((in[9] - minv) >> 7) |

			((in[10] - minv) << 10) |
			((in[11] - minv) << 27))

	out[6] = uint32(
		((in[11] - minv) >> 5) |

			((in[12] - minv) << 12) |
			((in[13] - minv) << 29))

	out[7] = uint32(
		((in[13] - minv) >> 3) |

			((in[14] - minv) << 14) |
			((in[15] - minv) << 31))

	out[8] = uint32(
		((in[15] - minv) >> 1) |

			((in[16] - minv) << 16))

	out[9] = uint32(
		((in[16] - minv) >> 16) |

			((in[17] - minv) << 1) |
			((in[18] - minv) << 18))

	out[10] = uint32(
		((in[18] - minv) >> 14) |

			((in[19] - minv) << 3) |
			((in[20] - minv) << 20))

	out[11] = uint32(
		((in[20] - minv) >> 12) |

			((in[21] - minv) << 5) |
			((in[22] - minv) << 22))

	out[12] = uint32(
		((in[22] - minv) >> 10) |

			((in[23] - minv) << 7) |
			((in[24] - minv) << 24))

	out[13] = uint32(
		((in[24] - minv) >> 8) |

			((in[25] - minv) << 9) |
			((in[26] - minv) << 26))

	out[14] = uint32(
		((in[26] - minv) >> 6) |

			((in[27] - minv) << 11) |
			((in[28] - minv) << 28))

	out[15] = uint32(
		((in[28] - minv) >> 4) |

			((in[29] - minv) << 13) |
			((in[30] - minv) << 30))

	out[16] = uint32(
		((in[30] - minv) >> 2) |

			((in[31] - minv) << 15))

}
func bp32_18[T uint32 | int32](in *[32]T, out *[18]uint32, minv T) {
	out[0] = uint32(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 18))

	out[1] = uint32(
		((in[1] - minv) >> 14) |

			((in[2] - minv) << 4) |
			((in[3] - minv) << 22))

	out[2] = uint32(
		((in[3] - minv) >> 10) |

			((in[4] - minv) << 8) |
			((in[5] - minv) << 26))

	out[3] = uint32(
		((in[5] - minv) >> 6) |

			((in[6] - minv) << 12) |
			((in[7] - minv) << 30))

	out[4] = uint32(
		((in[7] - minv) >> 2) |

			((in[8] - minv) << 16))

	out[5] = uint32(
		((in[8] - minv) >> 16) |

			((in[9] - minv) << 2) |
			((in[10] - minv) << 20))

	out[6] = uint32(
		((in[10] - minv) >> 12) |

			((in[11] - minv) << 6) |
			((in[12] - minv) << 24))

	out[7] = uint32(
		((in[12] - minv) >> 8) |

			((in[13] - minv) << 10) |
			((in[14] - minv) << 28))

	out[8] = uint32(
		((in[14] - minv) >> 4) |

			((in[15] - minv) << 14))

	out[9] = uint32(
		((in[15] - minv) >> 18) |

			((in[16] - minv) << 0) |
			((in[17] - minv) << 18))

	out[10] = uint32(
		((in[17] - minv) >> 14) |

			((in[18] - minv) << 4) |
			((in[19] - minv) << 22))

	out[11] = uint32(
		((in[19] - minv) >> 10) |

			((in[20] - minv) << 8) |
			((in[21] - minv) << 26))

	out[12] = uint32(
		((in[21] - minv) >> 6) |

			((in[22] - minv) << 12) |
			((in[23] - minv) << 30))

	out[13] = uint32(
		((in[23] - minv) >> 2) |

			((in[24] - minv) << 16))

	out[14] = uint32(
		((in[24] - minv) >> 16) |

			((in[25] - minv) << 2) |
			((in[26] - minv) << 20))

	out[15] = uint32(
		((in[26] - minv) >> 12) |

			((in[27] - minv) << 6) |
			((in[28] - minv) << 24))

	out[16] = uint32(
		((in[28] - minv) >> 8) |

			((in[29] - minv) << 10) |
			((in[30] - minv) << 28))

	out[17] = uint32(
		((in[30] - minv) >> 4) |

			((in[31] - minv) << 14))

}
func bp32_19[T uint32 | int32](in *[32]T, out *[19]uint32, minv T) {
	out[0] = uint32(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 19))

	out[1] = uint32(
		((in[1] - minv) >> 13) |

			((in[2] - minv) << 6) |
			((in[3] - minv) << 25))

	out[2] = uint32(
		((in[3] - minv) >> 7) |

			((in[4] - minv) << 12) |
			((in[5] - minv) << 31))

	out[3] = uint32(
		((in[5] - minv) >> 1) |

			((in[6] - minv) << 18))

	out[4] = uint32(
		((in[6] - minv) >> 14) |

			((in[7] - minv) << 5) |
			((in[8] - minv) << 24))

	out[5] = uint32(
		((in[8] - minv) >> 8) |

			((in[9] - minv) << 11) |
			((in[10] - minv) << 30))

	out[6] = uint32(
		((in[10] - minv) >> 2) |

			((in[11] - minv) << 17))

	out[7] = uint32(
		((in[11] - minv) >> 15) |

			((in[12] - minv) << 4) |
			((in[13] - minv) << 23))

	out[8] = uint32(
		((in[13] - minv) >> 9) |

			((in[14] - minv) << 10) |
			((in[15] - minv) << 29))

	out[9] = uint32(
		((in[15] - minv) >> 3) |

			((in[16] - minv) << 16))

	out[10] = uint32(
		((in[16] - minv) >> 16) |

			((in[17] - minv) << 3) |
			((in[18] - minv) << 22))

	out[11] = uint32(
		((in[18] - minv) >> 10) |

			((in[19] - minv) << 9) |
			((in[20] - minv) << 28))

	out[12] = uint32(
		((in[20] - minv) >> 4) |

			((in[21] - minv) << 15))

	out[13] = uint32(
		((in[21] - minv) >> 17) |

			((in[22] - minv) << 2) |
			((in[23] - minv) << 21))

	out[14] = uint32(
		((in[23] - minv) >> 11) |

			((in[24] - minv) << 8) |
			((in[25] - minv) << 27))

	out[15] = uint32(
		((in[25] - minv) >> 5) |

			((in[26] - minv) << 14))

	out[16] = uint32(
		((in[26] - minv) >> 18) |

			((in[27] - minv) << 1) |
			((in[28] - minv) << 20))

	out[17] = uint32(
		((in[28] - minv) >> 12) |

			((in[29] - minv) << 7) |
			((in[30] - minv) << 26))

	out[18] = uint32(
		((in[30] - minv) >> 6) |

			((in[31] - minv) << 13))

}
func bp32_20[T uint32 | int32](in *[32]T, out *[20]uint32, minv T) {
	out[0] = uint32(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 20))

	out[1] = uint32(
		((in[1] - minv) >> 12) |

			((in[2] - minv) << 8) |
			((in[3] - minv) << 28))

	out[2] = uint32(
		((in[3] - minv) >> 4) |

			((in[4] - minv) << 16))

	out[3] = uint32(
		((in[4] - minv) >> 16) |

			((in[5] - minv) << 4) |
			((in[6] - minv) << 24))

	out[4] = uint32(
		((in[6] - minv) >> 8) |

			((in[7] - minv) << 12))

	out[5] = uint32(
		((in[7] - minv) >> 20) |

			((in[8] - minv) << 0) |
			((in[9] - minv) << 20))

	out[6] = uint32(
		((in[9] - minv) >> 12) |

			((in[10] - minv) << 8) |
			((in[11] - minv) << 28))

	out[7] = uint32(
		((in[11] - minv) >> 4) |

			((in[12] - minv) << 16))

	out[8] = uint32(
		((in[12] - minv) >> 16) |

			((in[13] - minv) << 4) |
			((in[14] - minv) << 24))

	out[9] = uint32(
		((in[14] - minv) >> 8) |

			((in[15] - minv) << 12))

	out[10] = uint32(
		((in[15] - minv) >> 20) |

			((in[16] - minv) << 0) |
			((in[17] - minv) << 20))

	out[11] = uint32(
		((in[17] - minv) >> 12) |

			((in[18] - minv) << 8) |
			((in[19] - minv) << 28))

	out[12] = uint32(
		((in[19] - minv) >> 4) |

			((in[20] - minv) << 16))

	out[13] = uint32(
		((in[20] - minv) >> 16) |

			((in[21] - minv) << 4) |
			((in[22] - minv) << 24))

	out[14] = uint32(
		((in[22] - minv) >> 8) |

			((in[23] - minv) << 12))

	out[15] = uint32(
		((in[23] - minv) >> 20) |

			((in[24] - minv) << 0) |
			((in[25] - minv) << 20))

	out[16] = uint32(
		((in[25] - minv) >> 12) |

			((in[26] - minv) << 8) |
			((in[27] - minv) << 28))

	out[17] = uint32(
		((in[27] - minv) >> 4) |

			((in[28] - minv) << 16))

	out[18] = uint32(
		((in[28] - minv) >> 16) |

			((in[29] - minv) << 4) |
			((in[30] - minv) << 24))

	out[19] = uint32(
		((in[30] - minv) >> 8) |

			((in[31] - minv) << 12))

}
func bp32_21[T uint32 | int32](in *[32]T, out *[21]uint32, minv T) {
	out[0] = uint32(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 21))

	out[1] = uint32(
		((in[1] - minv) >> 11) |

			((in[2] - minv) << 10) |
			((in[3] - minv) << 31))

	out[2] = uint32(
		((in[3] - minv) >> 1) |

			((in[4] - minv) << 20))

	out[3] = uint32(
		((in[4] - minv) >> 12) |

			((in[5] - minv) << 9) |
			((in[6] - minv) << 30))

	out[4] = uint32(
		((in[6] - minv) >> 2) |

			((in[7] - minv) << 19))

	out[5] = uint32(
		((in[7] - minv) >> 13) |

			((in[8] - minv) << 8) |
			((in[9] - minv) << 29))

	out[6] = uint32(
		((in[9] - minv) >> 3) |

			((in[10] - minv) << 18))

	out[7] = uint32(
		((in[10] - minv) >> 14) |

			((in[11] - minv) << 7) |
			((in[12] - minv) << 28))

	out[8] = uint32(
		((in[12] - minv) >> 4) |

			((in[13] - minv) << 17))

	out[9] = uint32(
		((in[13] - minv) >> 15) |

			((in[14] - minv) << 6) |
			((in[15] - minv) << 27))

	out[10] = uint32(
		((in[15] - minv) >> 5) |

			((in[16] - minv) << 16))

	out[11] = uint32(
		((in[16] - minv) >> 16) |

			((in[17] - minv) << 5) |
			((in[18] - minv) << 26))

	out[12] = uint32(
		((in[18] - minv) >> 6) |

			((in[19] - minv) << 15))

	out[13] = uint32(
		((in[19] - minv) >> 17) |

			((in[20] - minv) << 4) |
			((in[21] - minv) << 25))

	out[14] = uint32(
		((in[21] - minv) >> 7) |

			((in[22] - minv) << 14))

	out[15] = uint32(
		((in[22] - minv) >> 18) |

			((in[23] - minv) << 3) |
			((in[24] - minv) << 24))

	out[16] = uint32(
		((in[24] - minv) >> 8) |

			((in[25] - minv) << 13))

	out[17] = uint32(
		((in[25] - minv) >> 19) |

			((in[26] - minv) << 2) |
			((in[27] - minv) << 23))

	out[18] = uint32(
		((in[27] - minv) >> 9) |

			((in[28] - minv) << 12))

	out[19] = uint32(
		((in[28] - minv) >> 20) |

			((in[29] - minv) << 1) |
			((in[30] - minv) << 22))

	out[20] = uint32(
		((in[30] - minv) >> 10) |

			((in[31] - minv) << 11))

}
func bp32_22[T uint32 | int32](in *[32]T, out *[22]uint32, minv T) {
	out[0] = uint32(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 22))

	out[1] = uint32(
		((in[1] - minv) >> 10) |

			((in[2] - minv) << 12))

	out[2] = uint32(
		((in[2] - minv) >> 20) |

			((in[3] - minv) << 2) |
			((in[4] - minv) << 24))

	out[3] = uint32(
		((in[4] - minv) >> 8) |

			((in[5] - minv) << 14))

	out[4] = uint32(
		((in[5] - minv) >> 18) |

			((in[6] - minv) << 4) |
			((in[7] - minv) << 26))

	out[5] = uint32(
		((in[7] - minv) >> 6) |

			((in[8] - minv) << 16))

	out[6] = uint32(
		((in[8] - minv) >> 16) |

			((in[9] - minv) << 6) |
			((in[10] - minv) << 28))

	out[7] = uint32(
		((in[10] - minv) >> 4) |

			((in[11] - minv) << 18))

	out[8] = uint32(
		((in[11] - minv) >> 14) |

			((in[12] - minv) << 8) |
			((in[13] - minv) << 30))

	out[9] = uint32(
		((in[13] - minv) >> 2) |

			((in[14] - minv) << 20))

	out[10] = uint32(
		((in[14] - minv) >> 12) |

			((in[15] - minv) << 10))

	out[11] = uint32(
		((in[15] - minv) >> 22) |

			((in[16] - minv) << 0) |
			((in[17] - minv) << 22))

	out[12] = uint32(
		((in[17] - minv) >> 10) |

			((in[18] - minv) << 12))

	out[13] = uint32(
		((in[18] - minv) >> 20) |

			((in[19] - minv) << 2) |
			((in[20] - minv) << 24))

	out[14] = uint32(
		((in[20] - minv) >> 8) |

			((in[21] - minv) << 14))

	out[15] = uint32(
		((in[21] - minv) >> 18) |

			((in[22] - minv) << 4) |
			((in[23] - minv) << 26))

	out[16] = uint32(
		((in[23] - minv) >> 6) |

			((in[24] - minv) << 16))

	out[17] = uint32(
		((in[24] - minv) >> 16) |

			((in[25] - minv) << 6) |
			((in[26] - minv) << 28))

	out[18] = uint32(
		((in[26] - minv) >> 4) |

			((in[27] - minv) << 18))

	out[19] = uint32(
		((in[27] - minv) >> 14) |

			((in[28] - minv) << 8) |
			((in[29] - minv) << 30))

	out[20] = uint32(
		((in[29] - minv) >> 2) |

			((in[30] - minv) << 20))

	out[21] = uint32(
		((in[30] - minv) >> 12) |

			((in[31] - minv) << 10))

}
func bp32_23[T uint32 | int32](in *[32]T, out *[23]uint32, minv T) {
	out[0] = uint32(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 23))

	out[1] = uint32(
		((in[1] - minv) >> 9) |

			((in[2] - minv) << 14))

	out[2] = uint32(
		((in[2] - minv) >> 18) |

			((in[3] - minv) << 5) |
			((in[4] - minv) << 28))

	out[3] = uint32(
		((in[4] - minv) >> 4) |

			((in[5] - minv) << 19))

	out[4] = uint32(
		((in[5] - minv) >> 13) |

			((in[6] - minv) << 10))

	out[5] = uint32(
		((in[6] - minv) >> 22) |

			((in[7] - minv) << 1) |
			((in[8] - minv) << 24))

	out[6] = uint32(
		((in[8] - minv) >> 8) |

			((in[9] - minv) << 15))

	out[7] = uint32(
		((in[9] - minv) >> 17) |

			((in[10] - minv) << 6) |
			((in[11] - minv) << 29))

	out[8] = uint32(
		((in[11] - minv) >> 3) |

			((in[12] - minv) << 20))

	out[9] = uint32(
		((in[12] - minv) >> 12) |

			((in[13] - minv) << 11))

	out[10] = uint32(
		((in[13] - minv) >> 21) |

			((in[14] - minv) << 2) |
			((in[15] - minv) << 25))

	out[11] = uint32(
		((in[15] - minv) >> 7) |

			((in[16] - minv) << 16))

	out[12] = uint32(
		((in[16] - minv) >> 16) |

			((in[17] - minv) << 7) |
			((in[18] - minv) << 30))

	out[13] = uint32(
		((in[18] - minv) >> 2) |

			((in[19] - minv) << 21))

	out[14] = uint32(
		((in[19] - minv) >> 11) |

			((in[20] - minv) << 12))

	out[15] = uint32(
		((in[20] - minv) >> 20) |

			((in[21] - minv) << 3) |
			((in[22] - minv) << 26))

	out[16] = uint32(
		((in[22] - minv) >> 6) |

			((in[23] - minv) << 17))

	out[17] = uint32(
		((in[23] - minv) >> 15) |

			((in[24] - minv) << 8) |
			((in[25] - minv) << 31))

	out[18] = uint32(
		((in[25] - minv) >> 1) |

			((in[26] - minv) << 22))

	out[19] = uint32(
		((in[26] - minv) >> 10) |

			((in[27] - minv) << 13))

	out[20] = uint32(
		((in[27] - minv) >> 19) |

			((in[28] - minv) << 4) |
			((in[29] - minv) << 27))

	out[21] = uint32(
		((in[29] - minv) >> 5) |

			((in[30] - minv) << 18))

	out[22] = uint32(
		((in[30] - minv) >> 14) |

			((in[31] - minv) << 9))

}
func bp32_24[T uint32 | int32](in *[32]T, out *[24]uint32, minv T) {
	out[0] = uint32(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 24))

	out[1] = uint32(
		((in[1] - minv) >> 8) |

			((in[2] - minv) << 16))

	out[2] = uint32(
		((in[2] - minv) >> 16) |

			((in[3] - minv) << 8))

	out[3] = uint32(
		((in[3] - minv) >> 24) |

			((in[4] - minv) << 0) |
			((in[5] - minv) << 24))

	out[4] = uint32(
		((in[5] - minv) >> 8) |

			((in[6] - minv) << 16))

	out[5] = uint32(
		((in[6] - minv) >> 16) |

			((in[7] - minv) << 8))

	out[6] = uint32(
		((in[7] - minv) >> 24) |

			((in[8] - minv) << 0) |
			((in[9] - minv) << 24))

	out[7] = uint32(
		((in[9] - minv) >> 8) |

			((in[10] - minv) << 16))

	out[8] = uint32(
		((in[10] - minv) >> 16) |

			((in[11] - minv) << 8))

	out[9] = uint32(
		((in[11] - minv) >> 24) |

			((in[12] - minv) << 0) |
			((in[13] - minv) << 24))

	out[10] = uint32(
		((in[13] - minv) >> 8) |

			((in[14] - minv) << 16))

	out[11] = uint32(
		((in[14] - minv) >> 16) |

			((in[15] - minv) << 8))

	out[12] = uint32(
		((in[15] - minv) >> 24) |

			((in[16] - minv) << 0) |
			((in[17] - minv) << 24))

	out[13] = uint32(
		((in[17] - minv) >> 8) |

			((in[18] - minv) << 16))

	out[14] = uint32(
		((in[18] - minv) >> 16) |

			((in[19] - minv) << 8))

	out[15] = uint32(
		((in[19] - minv) >> 24) |

			((in[20] - minv) << 0) |
			((in[21] - minv) << 24))

	out[16] = uint32(
		((in[21] - minv) >> 8) |

			((in[22] - minv) << 16))

	out[17] = uint32(
		((in[22] - minv) >> 16) |

			((in[23] - minv) << 8))

	out[18] = uint32(
		((in[23] - minv) >> 24) |

			((in[24] - minv) << 0) |
			((in[25] - minv) << 24))

	out[19] = uint32(
		((in[25] - minv) >> 8) |

			((in[26] - minv) << 16))

	out[20] = uint32(
		((in[26] - minv) >> 16) |

			((in[27] - minv) << 8))

	out[21] = uint32(
		((in[27] - minv) >> 24) |

			((in[28] - minv) << 0) |
			((in[29] - minv) << 24))

	out[22] = uint32(
		((in[29] - minv) >> 8) |

			((in[30] - minv) << 16))

	out[23] = uint32(
		((in[30] - minv) >> 16) |

			((in[31] - minv) << 8))

}
func bp32_25[T uint32 | int32](in *[32]T, out *[25]uint32, minv T) {
	out[0] = uint32(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 25))

	out[1] = uint32(
		((in[1] - minv) >> 7) |

			((in[2] - minv) << 18))

	out[2] = uint32(
		((in[2] - minv) >> 14) |

			((in[3] - minv) << 11))

	out[3] = uint32(
		((in[3] - minv) >> 21) |

			((in[4] - minv) << 4) |
			((in[5] - minv) << 29))

	out[4] = uint32(
		((in[5] - minv) >> 3) |

			((in[6] - minv) << 22))

	out[5] = uint32(
		((in[6] - minv) >> 10) |

			((in[7] - minv) << 15))

	out[6] = uint32(
		((in[7] - minv) >> 17) |

			((in[8] - minv) << 8))

	out[7] = uint32(
		((in[8] - minv) >> 24) |

			((in[9] - minv) << 1) |
			((in[10] - minv) << 26))

	out[8] = uint32(
		((in[10] - minv) >> 6) |

			((in[11] - minv) << 19))

	out[9] = uint32(
		((in[11] - minv) >> 13) |

			((in[12] - minv) << 12))

	out[10] = uint32(
		((in[12] - minv) >> 20) |

			((in[13] - minv) << 5) |
			((in[14] - minv) << 30))

	out[11] = uint32(
		((in[14] - minv) >> 2) |

			((in[15] - minv) << 23))

	out[12] = uint32(
		((in[15] - minv) >> 9) |

			((in[16] - minv) << 16))

	out[13] = uint32(
		((in[16] - minv) >> 16) |

			((in[17] - minv) << 9))

	out[14] = uint32(
		((in[17] - minv) >> 23) |

			((in[18] - minv) << 2) |
			((in[19] - minv) << 27))

	out[15] = uint32(
		((in[19] - minv) >> 5) |

			((in[20] - minv) << 20))

	out[16] = uint32(
		((in[20] - minv) >> 12) |

			((in[21] - minv) << 13))

	out[17] = uint32(
		((in[21] - minv) >> 19) |

			((in[22] - minv) << 6) |
			((in[23] - minv) << 31))

	out[18] = uint32(
		((in[23] - minv) >> 1) |

			((in[24] - minv) << 24))

	out[19] = uint32(
		((in[24] - minv) >> 8) |

			((in[25] - minv) << 17))

	out[20] = uint32(
		((in[25] - minv) >> 15) |

			((in[26] - minv) << 10))

	out[21] = uint32(
		((in[26] - minv) >> 22) |

			((in[27] - minv) << 3) |
			((in[28] - minv) << 28))

	out[22] = uint32(
		((in[28] - minv) >> 4) |

			((in[29] - minv) << 21))

	out[23] = uint32(
		((in[29] - minv) >> 11) |

			((in[30] - minv) << 14))

	out[24] = uint32(
		((in[30] - minv) >> 18) |

			((in[31] - minv) << 7))

}
func bp32_26[T uint32 | int32](in *[32]T, out *[26]uint32, minv T) {
	out[0] = uint32(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 26))

	out[1] = uint32(
		((in[1] - minv) >> 6) |

			((in[2] - minv) << 20))

	out[2] = uint32(
		((in[2] - minv) >> 12) |

			((in[3] - minv) << 14))

	out[3] = uint32(
		((in[3] - minv) >> 18) |

			((in[4] - minv) << 8))

	out[4] = uint32(
		((in[4] - minv) >> 24) |

			((in[5] - minv) << 2) |
			((in[6] - minv) << 28))

	out[5] = uint32(
		((in[6] - minv) >> 4) |

			((in[7] - minv) << 22))

	out[6] = uint32(
		((in[7] - minv) >> 10) |

			((in[8] - minv) << 16))

	out[7] = uint32(
		((in[8] - minv) >> 16) |

			((in[9] - minv) << 10))

	out[8] = uint32(
		((in[9] - minv) >> 22) |

			((in[10] - minv) << 4) |
			((in[11] - minv) << 30))

	out[9] = uint32(
		((in[11] - minv) >> 2) |

			((in[12] - minv) << 24))

	out[10] = uint32(
		((in[12] - minv) >> 8) |

			((in[13] - minv) << 18))

	out[11] = uint32(
		((in[13] - minv) >> 14) |

			((in[14] - minv) << 12))

	out[12] = uint32(
		((in[14] - minv) >> 20) |

			((in[15] - minv) << 6))

	out[13] = uint32(
		((in[15] - minv) >> 26) |

			((in[16] - minv) << 0) |
			((in[17] - minv) << 26))

	out[14] = uint32(
		((in[17] - minv) >> 6) |

			((in[18] - minv) << 20))

	out[15] = uint32(
		((in[18] - minv) >> 12) |

			((in[19] - minv) << 14))

	out[16] = uint32(
		((in[19] - minv) >> 18) |

			((in[20] - minv) << 8))

	out[17] = uint32(
		((in[20] - minv) >> 24) |

			((in[21] - minv) << 2) |
			((in[22] - minv) << 28))

	out[18] = uint32(
		((in[22] - minv) >> 4) |

			((in[23] - minv) << 22))

	out[19] = uint32(
		((in[23] - minv) >> 10) |

			((in[24] - minv) << 16))

	out[20] = uint32(
		((in[24] - minv) >> 16) |

			((in[25] - minv) << 10))

	out[21] = uint32(
		((in[25] - minv) >> 22) |

			((in[26] - minv) << 4) |
			((in[27] - minv) << 30))

	out[22] = uint32(
		((in[27] - minv) >> 2) |

			((in[28] - minv) << 24))

	out[23] = uint32(
		((in[28] - minv) >> 8) |

			((in[29] - minv) << 18))

	out[24] = uint32(
		((in[29] - minv) >> 14) |

			((in[30] - minv) << 12))

	out[25] = uint32(
		((in[30] - minv) >> 20) |

			((in[31] - minv) << 6))

}
func bp32_27[T uint32 | int32](in *[32]T, out *[27]uint32, minv T) {
	out[0] = uint32(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 27))

	out[1] = uint32(
		((in[1] - minv) >> 5) |

			((in[2] - minv) << 22))

	out[2] = uint32(
		((in[2] - minv) >> 10) |

			((in[3] - minv) << 17))

	out[3] = uint32(
		((in[3] - minv) >> 15) |

			((in[4] - minv) << 12))

	out[4] = uint32(
		((in[4] - minv) >> 20) |

			((in[5] - minv) << 7))

	out[5] = uint32(
		((in[5] - minv) >> 25) |

			((in[6] - minv) << 2) |
			((in[7] - minv) << 29))

	out[6] = uint32(
		((in[7] - minv) >> 3) |

			((in[8] - minv) << 24))

	out[7] = uint32(
		((in[8] - minv) >> 8) |

			((in[9] - minv) << 19))

	out[8] = uint32(
		((in[9] - minv) >> 13) |

			((in[10] - minv) << 14))

	out[9] = uint32(
		((in[10] - minv) >> 18) |

			((in[11] - minv) << 9))

	out[10] = uint32(
		((in[11] - minv) >> 23) |

			((in[12] - minv) << 4) |
			((in[13] - minv) << 31))

	out[11] = uint32(
		((in[13] - minv) >> 1) |

			((in[14] - minv) << 26))

	out[12] = uint32(
		((in[14] - minv) >> 6) |

			((in[15] - minv) << 21))

	out[13] = uint32(
		((in[15] - minv) >> 11) |

			((in[16] - minv) << 16))

	out[14] = uint32(
		((in[16] - minv) >> 16) |

			((in[17] - minv) << 11))

	out[15] = uint32(
		((in[17] - minv) >> 21) |

			((in[18] - minv) << 6))

	out[16] = uint32(
		((in[18] - minv) >> 26) |

			((in[19] - minv) << 1) |
			((in[20] - minv) << 28))

	out[17] = uint32(
		((in[20] - minv) >> 4) |

			((in[21] - minv) << 23))

	out[18] = uint32(
		((in[21] - minv) >> 9) |

			((in[22] - minv) << 18))

	out[19] = uint32(
		((in[22] - minv) >> 14) |

			((in[23] - minv) << 13))

	out[20] = uint32(
		((in[23] - minv) >> 19) |

			((in[24] - minv) << 8))

	out[21] = uint32(
		((in[24] - minv) >> 24) |

			((in[25] - minv) << 3) |
			((in[26] - minv) << 30))

	out[22] = uint32(
		((in[26] - minv) >> 2) |

			((in[27] - minv) << 25))

	out[23] = uint32(
		((in[27] - minv) >> 7) |

			((in[28] - minv) << 20))

	out[24] = uint32(
		((in[28] - minv) >> 12) |

			((in[29] - minv) << 15))

	out[25] = uint32(
		((in[29] - minv) >> 17) |

			((in[30] - minv) << 10))

	out[26] = uint32(
		((in[30] - minv) >> 22) |

			((in[31] - minv) << 5))

}
func bp32_28[T uint32 | int32](in *[32]T, out *[28]uint32, minv T) {
	out[0] = uint32(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 28))

	out[1] = uint32(
		((in[1] - minv) >> 4) |

			((in[2] - minv) << 24))

	out[2] = uint32(
		((in[2] - minv) >> 8) |

			((in[3] - minv) << 20))

	out[3] = uint32(
		((in[3] - minv) >> 12) |

			((in[4] - minv) << 16))

	out[4] = uint32(
		((in[4] - minv) >> 16) |

			((in[5] - minv) << 12))

	out[5] = uint32(
		((in[5] - minv) >> 20) |

			((in[6] - minv) << 8))

	out[6] = uint32(
		((in[6] - minv) >> 24) |

			((in[7] - minv) << 4))

	out[7] = uint32(
		((in[7] - minv) >> 28) |

			((in[8] - minv) << 0) |
			((in[9] - minv) << 28))

	out[8] = uint32(
		((in[9] - minv) >> 4) |

			((in[10] - minv) << 24))

	out[9] = uint32(
		((in[10] - minv) >> 8) |

			((in[11] - minv) << 20))

	out[10] = uint32(
		((in[11] - minv) >> 12) |

			((in[12] - minv) << 16))

	out[11] = uint32(
		((in[12] - minv) >> 16) |

			((in[13] - minv) << 12))

	out[12] = uint32(
		((in[13] - minv) >> 20) |

			((in[14] - minv) << 8))

	out[13] = uint32(
		((in[14] - minv) >> 24) |

			((in[15] - minv) << 4))

	out[14] = uint32(
		((in[15] - minv) >> 28) |

			((in[16] - minv) << 0) |
			((in[17] - minv) << 28))

	out[15] = uint32(
		((in[17] - minv) >> 4) |

			((in[18] - minv) << 24))

	out[16] = uint32(
		((in[18] - minv) >> 8) |

			((in[19] - minv) << 20))

	out[17] = uint32(
		((in[19] - minv) >> 12) |

			((in[20] - minv) << 16))

	out[18] = uint32(
		((in[20] - minv) >> 16) |

			((in[21] - minv) << 12))

	out[19] = uint32(
		((in[21] - minv) >> 20) |

			((in[22] - minv) << 8))

	out[20] = uint32(
		((in[22] - minv) >> 24) |

			((in[23] - minv) << 4))

	out[21] = uint32(
		((in[23] - minv) >> 28) |

			((in[24] - minv) << 0) |
			((in[25] - minv) << 28))

	out[22] = uint32(
		((in[25] - minv) >> 4) |

			((in[26] - minv) << 24))

	out[23] = uint32(
		((in[26] - minv) >> 8) |

			((in[27] - minv) << 20))

	out[24] = uint32(
		((in[27] - minv) >> 12) |

			((in[28] - minv) << 16))

	out[25] = uint32(
		((in[28] - minv) >> 16) |

			((in[29] - minv) << 12))

	out[26] = uint32(
		((in[29] - minv) >> 20) |

			((in[30] - minv) << 8))

	out[27] = uint32(
		((in[30] - minv) >> 24) |

			((in[31] - minv) << 4))

}
func bp32_29[T uint32 | int32](in *[32]T, out *[29]uint32, minv T) {
	out[0] = uint32(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 29))

	out[1] = uint32(
		((in[1] - minv) >> 3) |

			((in[2] - minv) << 26))

	out[2] = uint32(
		((in[2] - minv) >> 6) |

			((in[3] - minv) << 23))

	out[3] = uint32(
		((in[3] - minv) >> 9) |

			((in[4] - minv) << 20))

	out[4] = uint32(
		((in[4] - minv) >> 12) |

			((in[5] - minv) << 17))

	out[5] = uint32(
		((in[5] - minv) >> 15) |

			((in[6] - minv) << 14))

	out[6] = uint32(
		((in[6] - minv) >> 18) |

			((in[7] - minv) << 11))

	out[7] = uint32(
		((in[7] - minv) >> 21) |

			((in[8] - minv) << 8))

	out[8] = uint32(
		((in[8] - minv) >> 24) |

			((in[9] - minv) << 5))

	out[9] = uint32(
		((in[9] - minv) >> 27) |

			((in[10] - minv) << 2) |
			((in[11] - minv) << 31))

	out[10] = uint32(
		((in[11] - minv) >> 1) |

			((in[12] - minv) << 28))

	out[11] = uint32(
		((in[12] - minv) >> 4) |

			((in[13] - minv) << 25))

	out[12] = uint32(
		((in[13] - minv) >> 7) |

			((in[14] - minv) << 22))

	out[13] = uint32(
		((in[14] - minv) >> 10) |

			((in[15] - minv) << 19))

	out[14] = uint32(
		((in[15] - minv) >> 13) |

			((in[16] - minv) << 16))

	out[15] = uint32(
		((in[16] - minv) >> 16) |

			((in[17] - minv) << 13))

	out[16] = uint32(
		((in[17] - minv) >> 19) |

			((in[18] - minv) << 10))

	out[17] = uint32(
		((in[18] - minv) >> 22) |

			((in[19] - minv) << 7))

	out[18] = uint32(
		((in[19] - minv) >> 25) |

			((in[20] - minv) << 4))

	out[19] = uint32(
		((in[20] - minv) >> 28) |

			((in[21] - minv) << 1) |
			((in[22] - minv) << 30))

	out[20] = uint32(
		((in[22] - minv) >> 2) |

			((in[23] - minv) << 27))

	out[21] = uint32(
		((in[23] - minv) >> 5) |

			((in[24] - minv) << 24))

	out[22] = uint32(
		((in[24] - minv) >> 8) |

			((in[25] - minv) << 21))

	out[23] = uint32(
		((in[25] - minv) >> 11) |

			((in[26] - minv) << 18))

	out[24] = uint32(
		((in[26] - minv) >> 14) |

			((in[27] - minv) << 15))

	out[25] = uint32(
		((in[27] - minv) >> 17) |

			((in[28] - minv) << 12))

	out[26] = uint32(
		((in[28] - minv) >> 20) |

			((in[29] - minv) << 9))

	out[27] = uint32(
		((in[29] - minv) >> 23) |

			((in[30] - minv) << 6))

	out[28] = uint32(
		((in[30] - minv) >> 26) |

			((in[31] - minv) << 3))

}
func bp32_30[T uint32 | int32](in *[32]T, out *[30]uint32, minv T) {
	out[0] = uint32(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 30))

	out[1] = uint32(
		((in[1] - minv) >> 2) |

			((in[2] - minv) << 28))

	out[2] = uint32(
		((in[2] - minv) >> 4) |

			((in[3] - minv) << 26))

	out[3] = uint32(
		((in[3] - minv) >> 6) |

			((in[4] - minv) << 24))

	out[4] = uint32(
		((in[4] - minv) >> 8) |

			((in[5] - minv) << 22))

	out[5] = uint32(
		((in[5] - minv) >> 10) |

			((in[6] - minv) << 20))

	out[6] = uint32(
		((in[6] - minv) >> 12) |

			((in[7] - minv) << 18))

	out[7] = uint32(
		((in[7] - minv) >> 14) |

			((in[8] - minv) << 16))

	out[8] = uint32(
		((in[8] - minv) >> 16) |

			((in[9] - minv) << 14))

	out[9] = uint32(
		((in[9] - minv) >> 18) |

			((in[10] - minv) << 12))

	out[10] = uint32(
		((in[10] - minv) >> 20) |

			((in[11] - minv) << 10))

	out[11] = uint32(
		((in[11] - minv) >> 22) |

			((in[12] - minv) << 8))

	out[12] = uint32(
		((in[12] - minv) >> 24) |

			((in[13] - minv) << 6))

	out[13] = uint32(
		((in[13] - minv) >> 26) |

			((in[14] - minv) << 4))

	out[14] = uint32(
		((in[14] - minv) >> 28) |

			((in[15] - minv) << 2))

	out[15] = uint32(
		((in[15] - minv) >> 30) |

			((in[16] - minv) << 0) |
			((in[17] - minv) << 30))

	out[16] = uint32(
		((in[17] - minv) >> 2) |

			((in[18] - minv) << 28))

	out[17] = uint32(
		((in[18] - minv) >> 4) |

			((in[19] - minv) << 26))

	out[18] = uint32(
		((in[19] - minv) >> 6) |

			((in[20] - minv) << 24))

	out[19] = uint32(
		((in[20] - minv) >> 8) |

			((in[21] - minv) << 22))

	out[20] = uint32(
		((in[21] - minv) >> 10) |

			((in[22] - minv) << 20))

	out[21] = uint32(
		((in[22] - minv) >> 12) |

			((in[23] - minv) << 18))

	out[22] = uint32(
		((in[23] - minv) >> 14) |

			((in[24] - minv) << 16))

	out[23] = uint32(
		((in[24] - minv) >> 16) |

			((in[25] - minv) << 14))

	out[24] = uint32(
		((in[25] - minv) >> 18) |

			((in[26] - minv) << 12))

	out[25] = uint32(
		((in[26] - minv) >> 20) |

			((in[27] - minv) << 10))

	out[26] = uint32(
		((in[27] - minv) >> 22) |

			((in[28] - minv) << 8))

	out[27] = uint32(
		((in[28] - minv) >> 24) |

			((in[29] - minv) << 6))

	out[28] = uint32(
		((in[29] - minv) >> 26) |

			((in[30] - minv) << 4))

	out[29] = uint32(
		((in[30] - minv) >> 28) |

			((in[31] - minv) << 2))

}
func bp32_31[T uint32 | int32](in *[32]T, out *[31]uint32, minv T) {
	out[0] = uint32(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 31))

	out[1] = uint32(
		((in[1] - minv) >> 1) |

			((in[2] - minv) << 30))

	out[2] = uint32(
		((in[2] - minv) >> 2) |

			((in[3] - minv) << 29))

	out[3] = uint32(
		((in[3] - minv) >> 3) |

			((in[4] - minv) << 28))

	out[4] = uint32(
		((in[4] - minv) >> 4) |

			((in[5] - minv) << 27))

	out[5] = uint32(
		((in[5] - minv) >> 5) |

			((in[6] - minv) << 26))

	out[6] = uint32(
		((in[6] - minv) >> 6) |

			((in[7] - minv) << 25))

	out[7] = uint32(
		((in[7] - minv) >> 7) |

			((in[8] - minv) << 24))

	out[8] = uint32(
		((in[8] - minv) >> 8) |

			((in[9] - minv) << 23))

	out[9] = uint32(
		((in[9] - minv) >> 9) |

			((in[10] - minv) << 22))

	out[10] = uint32(
		((in[10] - minv) >> 10) |

			((in[11] - minv) << 21))

	out[11] = uint32(
		((in[11] - minv) >> 11) |

			((in[12] - minv) << 20))

	out[12] = uint32(
		((in[12] - minv) >> 12) |

			((in[13] - minv) << 19))

	out[13] = uint32(
		((in[13] - minv) >> 13) |

			((in[14] - minv) << 18))

	out[14] = uint32(
		((in[14] - minv) >> 14) |

			((in[15] - minv) << 17))

	out[15] = uint32(
		((in[15] - minv) >> 15) |

			((in[16] - minv) << 16))

	out[16] = uint32(
		((in[16] - minv) >> 16) |

			((in[17] - minv) << 15))

	out[17] = uint32(
		((in[17] - minv) >> 17) |

			((in[18] - minv) << 14))

	out[18] = uint32(
		((in[18] - minv) >> 18) |

			((in[19] - minv) << 13))

	out[19] = uint32(
		((in[19] - minv) >> 19) |

			((in[20] - minv) << 12))

	out[20] = uint32(
		((in[20] - minv) >> 20) |

			((in[21] - minv) << 11))

	out[21] = uint32(
		((in[21] - minv) >> 21) |

			((in[22] - minv) << 10))

	out[22] = uint32(
		((in[22] - minv) >> 22) |

			((in[23] - minv) << 9))

	out[23] = uint32(
		((in[23] - minv) >> 23) |

			((in[24] - minv) << 8))

	out[24] = uint32(
		((in[24] - minv) >> 24) |

			((in[25] - minv) << 7))

	out[25] = uint32(
		((in[25] - minv) >> 25) |

			((in[26] - minv) << 6))

	out[26] = uint32(
		((in[26] - minv) >> 26) |

			((in[27] - minv) << 5))

	out[27] = uint32(
		((in[27] - minv) >> 27) |

			((in[28] - minv) << 4))

	out[28] = uint32(
		((in[28] - minv) >> 28) |

			((in[29] - minv) << 3))

	out[29] = uint32(
		((in[29] - minv) >> 29) |

			((in[30] - minv) << 2))

	out[30] = uint32(
		((in[30] - minv) >> 30) |

			((in[31] - minv) << 1))

}
