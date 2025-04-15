// Copyright (c) 2025 Blockwatch Data Inc.
// Code automatically generated - DO NOT EDIT.
// Any manual changes will be lost.

package pack

func bitpack16[T uint16 | int16](minv T, in []T, out []uint16, log2 int) {
	switch log2 {
	case 0:
		bp16_0((*[16]T)(in), (*[0]uint16)(out), minv)
	case 1:
		bp16_1((*[16]T)(in), (*[1]uint16)(out), minv)
	case 2:
		bp16_2((*[16]T)(in), (*[2]uint16)(out), minv)
	case 3:
		bp16_3((*[16]T)(in), (*[3]uint16)(out), minv)
	case 4:
		bp16_4((*[16]T)(in), (*[4]uint16)(out), minv)
	case 5:
		bp16_5((*[16]T)(in), (*[5]uint16)(out), minv)
	case 6:
		bp16_6((*[16]T)(in), (*[6]uint16)(out), minv)
	case 7:
		bp16_7((*[16]T)(in), (*[7]uint16)(out), minv)
	case 8:
		bp16_8((*[16]T)(in), (*[8]uint16)(out), minv)
	case 9:
		bp16_9((*[16]T)(in), (*[9]uint16)(out), minv)
	case 10:
		bp16_10((*[16]T)(in), (*[10]uint16)(out), minv)
	case 11:
		bp16_11((*[16]T)(in), (*[11]uint16)(out), minv)
	case 12:
		bp16_12((*[16]T)(in), (*[12]uint16)(out), minv)
	case 13:
		bp16_13((*[16]T)(in), (*[13]uint16)(out), minv)
	case 14:
		bp16_14((*[16]T)(in), (*[14]uint16)(out), minv)
	case 15:
		bp16_15((*[16]T)(in), (*[15]uint16)(out), minv)
	}

}
func bp16_0[T uint16 | int16](in *[16]T, out *[0]uint16, minv T) {
}
func bp16_1[T uint16 | int16](in *[16]T, out *[1]uint16, minv T) {
	out[0] = uint16(
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
			((in[15] - minv) << 15))

}
func bp16_2[T uint16 | int16](in *[16]T, out *[2]uint16, minv T) {
	out[0] = uint16(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 2) |
			((in[2] - minv) << 4) |
			((in[3] - minv) << 6) |
			((in[4] - minv) << 8) |
			((in[5] - minv) << 10) |
			((in[6] - minv) << 12) |
			((in[7] - minv) << 14))

	out[1] = uint16(
		((in[8] - minv) << 0) |
			((in[9] - minv) << 2) |
			((in[10] - minv) << 4) |
			((in[11] - minv) << 6) |
			((in[12] - minv) << 8) |
			((in[13] - minv) << 10) |
			((in[14] - minv) << 12) |
			((in[15] - minv) << 14))

}
func bp16_3[T uint16 | int16](in *[16]T, out *[3]uint16, minv T) {
	out[0] = uint16(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 3) |
			((in[2] - minv) << 6) |
			((in[3] - minv) << 9) |
			((in[4] - minv) << 12) |
			((in[5] - minv) << 15))

	out[1] = uint16(
		((in[5] - minv) >> 1) |

			((in[6] - minv) << 2) |
			((in[7] - minv) << 5) |
			((in[8] - minv) << 8) |
			((in[9] - minv) << 11) |
			((in[10] - minv) << 14))

	out[2] = uint16(
		((in[10] - minv) >> 2) |

			((in[11] - minv) << 1) |
			((in[12] - minv) << 4) |
			((in[13] - minv) << 7) |
			((in[14] - minv) << 10) |
			((in[15] - minv) << 13))

}
func bp16_4[T uint16 | int16](in *[16]T, out *[4]uint16, minv T) {
	out[0] = uint16(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 4) |
			((in[2] - minv) << 8) |
			((in[3] - minv) << 12))

	out[1] = uint16(
		((in[4] - minv) << 0) |
			((in[5] - minv) << 4) |
			((in[6] - minv) << 8) |
			((in[7] - minv) << 12))

	out[2] = uint16(
		((in[8] - minv) << 0) |
			((in[9] - minv) << 4) |
			((in[10] - minv) << 8) |
			((in[11] - minv) << 12))

	out[3] = uint16(
		((in[12] - minv) << 0) |
			((in[13] - minv) << 4) |
			((in[14] - minv) << 8) |
			((in[15] - minv) << 12))

}
func bp16_5[T uint16 | int16](in *[16]T, out *[5]uint16, minv T) {
	out[0] = uint16(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 5) |
			((in[2] - minv) << 10) |
			((in[3] - minv) << 15))

	out[1] = uint16(
		((in[3] - minv) >> 1) |

			((in[4] - minv) << 4) |
			((in[5] - minv) << 9) |
			((in[6] - minv) << 14))

	out[2] = uint16(
		((in[6] - minv) >> 2) |

			((in[7] - minv) << 3) |
			((in[8] - minv) << 8) |
			((in[9] - minv) << 13))

	out[3] = uint16(
		((in[9] - minv) >> 3) |

			((in[10] - minv) << 2) |
			((in[11] - minv) << 7) |
			((in[12] - minv) << 12))

	out[4] = uint16(
		((in[12] - minv) >> 4) |

			((in[13] - minv) << 1) |
			((in[14] - minv) << 6) |
			((in[15] - minv) << 11))

}
func bp16_6[T uint16 | int16](in *[16]T, out *[6]uint16, minv T) {
	out[0] = uint16(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 6) |
			((in[2] - minv) << 12))

	out[1] = uint16(
		((in[2] - minv) >> 4) |

			((in[3] - minv) << 2) |
			((in[4] - minv) << 8) |
			((in[5] - minv) << 14))

	out[2] = uint16(
		((in[5] - minv) >> 2) |

			((in[6] - minv) << 4) |
			((in[7] - minv) << 10))

	out[3] = uint16(
		((in[7] - minv) >> 6) |

			((in[8] - minv) << 0) |
			((in[9] - minv) << 6) |
			((in[10] - minv) << 12))

	out[4] = uint16(
		((in[10] - minv) >> 4) |

			((in[11] - minv) << 2) |
			((in[12] - minv) << 8) |
			((in[13] - minv) << 14))

	out[5] = uint16(
		((in[13] - minv) >> 2) |

			((in[14] - minv) << 4) |
			((in[15] - minv) << 10))

}
func bp16_7[T uint16 | int16](in *[16]T, out *[7]uint16, minv T) {
	out[0] = uint16(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 7) |
			((in[2] - minv) << 14))

	out[1] = uint16(
		((in[2] - minv) >> 2) |

			((in[3] - minv) << 5) |
			((in[4] - minv) << 12))

	out[2] = uint16(
		((in[4] - minv) >> 4) |

			((in[5] - minv) << 3) |
			((in[6] - minv) << 10))

	out[3] = uint16(
		((in[6] - minv) >> 6) |

			((in[7] - minv) << 1) |
			((in[8] - minv) << 8) |
			((in[9] - minv) << 15))

	out[4] = uint16(
		((in[9] - minv) >> 1) |

			((in[10] - minv) << 6) |
			((in[11] - minv) << 13))

	out[5] = uint16(
		((in[11] - minv) >> 3) |

			((in[12] - minv) << 4) |
			((in[13] - minv) << 11))

	out[6] = uint16(
		((in[13] - minv) >> 5) |

			((in[14] - minv) << 2) |
			((in[15] - minv) << 9))

}
func bp16_8[T uint16 | int16](in *[16]T, out *[8]uint16, minv T) {
	out[0] = uint16(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 8))

	out[1] = uint16(
		((in[2] - minv) << 0) |
			((in[3] - minv) << 8))

	out[2] = uint16(
		((in[4] - minv) << 0) |
			((in[5] - minv) << 8))

	out[3] = uint16(
		((in[6] - minv) << 0) |
			((in[7] - minv) << 8))

	out[4] = uint16(
		((in[8] - minv) << 0) |
			((in[9] - minv) << 8))

	out[5] = uint16(
		((in[10] - minv) << 0) |
			((in[11] - minv) << 8))

	out[6] = uint16(
		((in[12] - minv) << 0) |
			((in[13] - minv) << 8))

	out[7] = uint16(
		((in[14] - minv) << 0) |
			((in[15] - minv) << 8))

}
func bp16_9[T uint16 | int16](in *[16]T, out *[9]uint16, minv T) {
	out[0] = uint16(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 9))

	out[1] = uint16(
		((in[1] - minv) >> 7) |

			((in[2] - minv) << 2) |
			((in[3] - minv) << 11))

	out[2] = uint16(
		((in[3] - minv) >> 5) |

			((in[4] - minv) << 4) |
			((in[5] - minv) << 13))

	out[3] = uint16(
		((in[5] - minv) >> 3) |

			((in[6] - minv) << 6) |
			((in[7] - minv) << 15))

	out[4] = uint16(
		((in[7] - minv) >> 1) |

			((in[8] - minv) << 8))

	out[5] = uint16(
		((in[8] - minv) >> 8) |

			((in[9] - minv) << 1) |
			((in[10] - minv) << 10))

	out[6] = uint16(
		((in[10] - minv) >> 6) |

			((in[11] - minv) << 3) |
			((in[12] - minv) << 12))

	out[7] = uint16(
		((in[12] - minv) >> 4) |

			((in[13] - minv) << 5) |
			((in[14] - minv) << 14))

	out[8] = uint16(
		((in[14] - minv) >> 2) |

			((in[15] - minv) << 7))

}
func bp16_10[T uint16 | int16](in *[16]T, out *[10]uint16, minv T) {
	out[0] = uint16(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 10))

	out[1] = uint16(
		((in[1] - minv) >> 6) |

			((in[2] - minv) << 4) |
			((in[3] - minv) << 14))

	out[2] = uint16(
		((in[3] - minv) >> 2) |

			((in[4] - minv) << 8))

	out[3] = uint16(
		((in[4] - minv) >> 8) |

			((in[5] - minv) << 2) |
			((in[6] - minv) << 12))

	out[4] = uint16(
		((in[6] - minv) >> 4) |

			((in[7] - minv) << 6))

	out[5] = uint16(
		((in[7] - minv) >> 10) |

			((in[8] - minv) << 0) |
			((in[9] - minv) << 10))

	out[6] = uint16(
		((in[9] - minv) >> 6) |

			((in[10] - minv) << 4) |
			((in[11] - minv) << 14))

	out[7] = uint16(
		((in[11] - minv) >> 2) |

			((in[12] - minv) << 8))

	out[8] = uint16(
		((in[12] - minv) >> 8) |

			((in[13] - minv) << 2) |
			((in[14] - minv) << 12))

	out[9] = uint16(
		((in[14] - minv) >> 4) |

			((in[15] - minv) << 6))

}
func bp16_11[T uint16 | int16](in *[16]T, out *[11]uint16, minv T) {
	out[0] = uint16(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 11))

	out[1] = uint16(
		((in[1] - minv) >> 5) |

			((in[2] - minv) << 6))

	out[2] = uint16(
		((in[2] - minv) >> 10) |

			((in[3] - minv) << 1) |
			((in[4] - minv) << 12))

	out[3] = uint16(
		((in[4] - minv) >> 4) |

			((in[5] - minv) << 7))

	out[4] = uint16(
		((in[5] - minv) >> 9) |

			((in[6] - minv) << 2) |
			((in[7] - minv) << 13))

	out[5] = uint16(
		((in[7] - minv) >> 3) |

			((in[8] - minv) << 8))

	out[6] = uint16(
		((in[8] - minv) >> 8) |

			((in[9] - minv) << 3) |
			((in[10] - minv) << 14))

	out[7] = uint16(
		((in[10] - minv) >> 2) |

			((in[11] - minv) << 9))

	out[8] = uint16(
		((in[11] - minv) >> 7) |

			((in[12] - minv) << 4) |
			((in[13] - minv) << 15))

	out[9] = uint16(
		((in[13] - minv) >> 1) |

			((in[14] - minv) << 10))

	out[10] = uint16(
		((in[14] - minv) >> 6) |

			((in[15] - minv) << 5))

}
func bp16_12[T uint16 | int16](in *[16]T, out *[12]uint16, minv T) {
	out[0] = uint16(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 12))

	out[1] = uint16(
		((in[1] - minv) >> 4) |

			((in[2] - minv) << 8))

	out[2] = uint16(
		((in[2] - minv) >> 8) |

			((in[3] - minv) << 4))

	out[3] = uint16(
		((in[3] - minv) >> 12) |

			((in[4] - minv) << 0) |
			((in[5] - minv) << 12))

	out[4] = uint16(
		((in[5] - minv) >> 4) |

			((in[6] - minv) << 8))

	out[5] = uint16(
		((in[6] - minv) >> 8) |

			((in[7] - minv) << 4))

	out[6] = uint16(
		((in[7] - minv) >> 12) |

			((in[8] - minv) << 0) |
			((in[9] - minv) << 12))

	out[7] = uint16(
		((in[9] - minv) >> 4) |

			((in[10] - minv) << 8))

	out[8] = uint16(
		((in[10] - minv) >> 8) |

			((in[11] - minv) << 4))

	out[9] = uint16(
		((in[11] - minv) >> 12) |

			((in[12] - minv) << 0) |
			((in[13] - minv) << 12))

	out[10] = uint16(
		((in[13] - minv) >> 4) |

			((in[14] - minv) << 8))

	out[11] = uint16(
		((in[14] - minv) >> 8) |

			((in[15] - minv) << 4))

}
func bp16_13[T uint16 | int16](in *[16]T, out *[13]uint16, minv T) {
	out[0] = uint16(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 13))

	out[1] = uint16(
		((in[1] - minv) >> 3) |

			((in[2] - minv) << 10))

	out[2] = uint16(
		((in[2] - minv) >> 6) |

			((in[3] - minv) << 7))

	out[3] = uint16(
		((in[3] - minv) >> 9) |

			((in[4] - minv) << 4))

	out[4] = uint16(
		((in[4] - minv) >> 12) |

			((in[5] - minv) << 1) |
			((in[6] - minv) << 14))

	out[5] = uint16(
		((in[6] - minv) >> 2) |

			((in[7] - minv) << 11))

	out[6] = uint16(
		((in[7] - minv) >> 5) |

			((in[8] - minv) << 8))

	out[7] = uint16(
		((in[8] - minv) >> 8) |

			((in[9] - minv) << 5))

	out[8] = uint16(
		((in[9] - minv) >> 11) |

			((in[10] - minv) << 2) |
			((in[11] - minv) << 15))

	out[9] = uint16(
		((in[11] - minv) >> 1) |

			((in[12] - minv) << 12))

	out[10] = uint16(
		((in[12] - minv) >> 4) |

			((in[13] - minv) << 9))

	out[11] = uint16(
		((in[13] - minv) >> 7) |

			((in[14] - minv) << 6))

	out[12] = uint16(
		((in[14] - minv) >> 10) |

			((in[15] - minv) << 3))

}
func bp16_14[T uint16 | int16](in *[16]T, out *[14]uint16, minv T) {
	out[0] = uint16(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 14))

	out[1] = uint16(
		((in[1] - minv) >> 2) |

			((in[2] - minv) << 12))

	out[2] = uint16(
		((in[2] - minv) >> 4) |

			((in[3] - minv) << 10))

	out[3] = uint16(
		((in[3] - minv) >> 6) |

			((in[4] - minv) << 8))

	out[4] = uint16(
		((in[4] - minv) >> 8) |

			((in[5] - minv) << 6))

	out[5] = uint16(
		((in[5] - minv) >> 10) |

			((in[6] - minv) << 4))

	out[6] = uint16(
		((in[6] - minv) >> 12) |

			((in[7] - minv) << 2))

	out[7] = uint16(
		((in[7] - minv) >> 14) |

			((in[8] - minv) << 0) |
			((in[9] - minv) << 14))

	out[8] = uint16(
		((in[9] - minv) >> 2) |

			((in[10] - minv) << 12))

	out[9] = uint16(
		((in[10] - minv) >> 4) |

			((in[11] - minv) << 10))

	out[10] = uint16(
		((in[11] - minv) >> 6) |

			((in[12] - minv) << 8))

	out[11] = uint16(
		((in[12] - minv) >> 8) |

			((in[13] - minv) << 6))

	out[12] = uint16(
		((in[13] - minv) >> 10) |

			((in[14] - minv) << 4))

	out[13] = uint16(
		((in[14] - minv) >> 12) |

			((in[15] - minv) << 2))

}
func bp16_15[T uint16 | int16](in *[16]T, out *[15]uint16, minv T) {
	out[0] = uint16(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 15))

	out[1] = uint16(
		((in[1] - minv) >> 1) |

			((in[2] - minv) << 14))

	out[2] = uint16(
		((in[2] - minv) >> 2) |

			((in[3] - minv) << 13))

	out[3] = uint16(
		((in[3] - minv) >> 3) |

			((in[4] - minv) << 12))

	out[4] = uint16(
		((in[4] - minv) >> 4) |

			((in[5] - minv) << 11))

	out[5] = uint16(
		((in[5] - minv) >> 5) |

			((in[6] - minv) << 10))

	out[6] = uint16(
		((in[6] - minv) >> 6) |

			((in[7] - minv) << 9))

	out[7] = uint16(
		((in[7] - minv) >> 7) |

			((in[8] - minv) << 8))

	out[8] = uint16(
		((in[8] - minv) >> 8) |

			((in[9] - minv) << 7))

	out[9] = uint16(
		((in[9] - minv) >> 9) |

			((in[10] - minv) << 6))

	out[10] = uint16(
		((in[10] - minv) >> 10) |

			((in[11] - minv) << 5))

	out[11] = uint16(
		((in[11] - minv) >> 11) |

			((in[12] - minv) << 4))

	out[12] = uint16(
		((in[12] - minv) >> 12) |

			((in[13] - minv) << 3))

	out[13] = uint16(
		((in[13] - minv) >> 13) |

			((in[14] - minv) << 2))

	out[14] = uint16(
		((in[14] - minv) >> 14) |

			((in[15] - minv) << 1))

}
