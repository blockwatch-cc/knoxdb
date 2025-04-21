// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"unsafe"

	"blockwatch.cc/knoxdb/internal/types"
)

func unpack_zero[T types.Integer](_ uint64, p unsafe.Pointer) {
	clear((*[128]T)(p)[:])
}

func unpack_one[T types.Integer](_ uint64, p unsafe.Pointer) {
	dst := (*[128]T)(p)
	for i := range dst {
		dst[i] = 1
	}
}

func unpack_60[T types.Integer](v uint64, p unsafe.Pointer) {
	dst := (*[60]T)(p)
	dst[0] = T(v & 1)
	dst[1] = T((v >> 1) & 1)
	dst[2] = T((v >> 2) & 1)
	dst[3] = T((v >> 3) & 1)
	dst[4] = T((v >> 4) & 1)
	dst[5] = T((v >> 5) & 1)
	dst[6] = T((v >> 6) & 1)
	dst[7] = T((v >> 7) & 1)
	dst[8] = T((v >> 8) & 1)
	dst[9] = T((v >> 9) & 1)
	dst[10] = T((v >> 10) & 1)
	dst[11] = T((v >> 11) & 1)
	dst[12] = T((v >> 12) & 1)
	dst[13] = T((v >> 13) & 1)
	dst[14] = T((v >> 14) & 1)
	dst[15] = T((v >> 15) & 1)
	dst[16] = T((v >> 16) & 1)
	dst[17] = T((v >> 17) & 1)
	dst[18] = T((v >> 18) & 1)
	dst[19] = T((v >> 19) & 1)
	dst[20] = T((v >> 20) & 1)
	dst[21] = T((v >> 21) & 1)
	dst[22] = T((v >> 22) & 1)
	dst[23] = T((v >> 23) & 1)
	dst[24] = T((v >> 24) & 1)
	dst[25] = T((v >> 25) & 1)
	dst[26] = T((v >> 26) & 1)
	dst[27] = T((v >> 27) & 1)
	dst[28] = T((v >> 28) & 1)
	dst[29] = T((v >> 29) & 1)
	dst[30] = T((v >> 30) & 1)
	dst[31] = T((v >> 31) & 1)
	dst[32] = T((v >> 32) & 1)
	dst[33] = T((v >> 33) & 1)
	dst[34] = T((v >> 34) & 1)
	dst[35] = T((v >> 35) & 1)
	dst[36] = T((v >> 36) & 1)
	dst[37] = T((v >> 37) & 1)
	dst[38] = T((v >> 38) & 1)
	dst[39] = T((v >> 39) & 1)
	dst[40] = T((v >> 40) & 1)
	dst[41] = T((v >> 41) & 1)
	dst[42] = T((v >> 42) & 1)
	dst[43] = T((v >> 43) & 1)
	dst[44] = T((v >> 44) & 1)
	dst[45] = T((v >> 45) & 1)
	dst[46] = T((v >> 46) & 1)
	dst[47] = T((v >> 47) & 1)
	dst[48] = T((v >> 48) & 1)
	dst[49] = T((v >> 49) & 1)
	dst[50] = T((v >> 50) & 1)
	dst[51] = T((v >> 51) & 1)
	dst[52] = T((v >> 52) & 1)
	dst[53] = T((v >> 53) & 1)
	dst[54] = T((v >> 54) & 1)
	dst[55] = T((v >> 55) & 1)
	dst[56] = T((v >> 56) & 1)
	dst[57] = T((v >> 57) & 1)
	dst[58] = T((v >> 58) & 1)
	dst[59] = T((v >> 59) & 1)
}

func unpack_30[T types.Integer](v uint64, p unsafe.Pointer) {
	dst := (*[30]T)(p)
	dst[0] = T(v & 3)
	dst[1] = T((v >> 2) & 3)
	dst[2] = T((v >> 4) & 3)
	dst[3] = T((v >> 6) & 3)
	dst[4] = T((v >> 8) & 3)
	dst[5] = T((v >> 10) & 3)
	dst[6] = T((v >> 12) & 3)
	dst[7] = T((v >> 14) & 3)
	dst[8] = T((v >> 16) & 3)
	dst[9] = T((v >> 18) & 3)
	dst[10] = T((v >> 20) & 3)
	dst[11] = T((v >> 22) & 3)
	dst[12] = T((v >> 24) & 3)
	dst[13] = T((v >> 26) & 3)
	dst[14] = T((v >> 28) & 3)
	dst[15] = T((v >> 30) & 3)
	dst[16] = T((v >> 32) & 3)
	dst[17] = T((v >> 34) & 3)
	dst[18] = T((v >> 36) & 3)
	dst[19] = T((v >> 38) & 3)
	dst[20] = T((v >> 40) & 3)
	dst[21] = T((v >> 42) & 3)
	dst[22] = T((v >> 44) & 3)
	dst[23] = T((v >> 46) & 3)
	dst[24] = T((v >> 48) & 3)
	dst[25] = T((v >> 50) & 3)
	dst[26] = T((v >> 52) & 3)
	dst[27] = T((v >> 54) & 3)
	dst[28] = T((v >> 56) & 3)
	dst[29] = T((v >> 58) & 3)
}

func unpack_20[T types.Integer](v uint64, p unsafe.Pointer) {
	dst := (*[20]T)(p)
	dst[0] = T(v & 7)
	dst[1] = T((v >> 3) & 7)
	dst[2] = T((v >> 6) & 7)
	dst[3] = T((v >> 9) & 7)
	dst[4] = T((v >> 12) & 7)
	dst[5] = T((v >> 15) & 7)
	dst[6] = T((v >> 18) & 7)
	dst[7] = T((v >> 21) & 7)
	dst[8] = T((v >> 24) & 7)
	dst[9] = T((v >> 27) & 7)
	dst[10] = T((v >> 30) & 7)
	dst[11] = T((v >> 33) & 7)
	dst[12] = T((v >> 36) & 7)
	dst[13] = T((v >> 39) & 7)
	dst[14] = T((v >> 42) & 7)
	dst[15] = T((v >> 45) & 7)
	dst[16] = T((v >> 48) & 7)
	dst[17] = T((v >> 51) & 7)
	dst[18] = T((v >> 54) & 7)
	dst[19] = T((v >> 57) & 7)
}

func unpack_15[T types.Integer](v uint64, p unsafe.Pointer) {
	dst := (*[15]T)(p)
	dst[0] = T(v & 15)
	dst[1] = T((v >> 4) & 15)
	dst[2] = T((v >> 8) & 15)
	dst[3] = T((v >> 12) & 15)
	dst[4] = T((v >> 16) & 15)
	dst[5] = T((v >> 20) & 15)
	dst[6] = T((v >> 24) & 15)
	dst[7] = T((v >> 28) & 15)
	dst[8] = T((v >> 32) & 15)
	dst[9] = T((v >> 36) & 15)
	dst[10] = T((v >> 40) & 15)
	dst[11] = T((v >> 44) & 15)
	dst[12] = T((v >> 48) & 15)
	dst[13] = T((v >> 52) & 15)
	dst[14] = T((v >> 56) & 15)
}

func unpack_12[T types.Integer](v uint64, p unsafe.Pointer) {
	dst := (*[12]T)(p)
	dst[0] = T(v & 31)
	dst[1] = T((v >> 5) & 31)
	dst[2] = T((v >> 10) & 31)
	dst[3] = T((v >> 15) & 31)
	dst[4] = T((v >> 20) & 31)
	dst[5] = T((v >> 25) & 31)
	dst[6] = T((v >> 30) & 31)
	dst[7] = T((v >> 35) & 31)
	dst[8] = T((v >> 40) & 31)
	dst[9] = T((v >> 45) & 31)
	dst[10] = T((v >> 50) & 31)
	dst[11] = T((v >> 55) & 31)
}

func unpack_10[T types.Integer](v uint64, p unsafe.Pointer) {
	dst := (*[10]T)(p)
	dst[0] = T(v & 63)
	dst[1] = T((v >> 6) & 63)
	dst[2] = T((v >> 12) & 63)
	dst[3] = T((v >> 18) & 63)
	dst[4] = T((v >> 24) & 63)
	dst[5] = T((v >> 30) & 63)
	dst[6] = T((v >> 36) & 63)
	dst[7] = T((v >> 42) & 63)
	dst[8] = T((v >> 48) & 63)
	dst[9] = T((v >> 54) & 63)
}

func unpack_8[T types.Integer](v uint64, p unsafe.Pointer) {
	dst := (*[8]T)(p)
	dst[0] = T(v & 127)
	dst[1] = T((v >> 7) & 127)
	dst[2] = T((v >> 14) & 127)
	dst[3] = T((v >> 21) & 127)
	dst[4] = T((v >> 28) & 127)
	dst[5] = T((v >> 35) & 127)
	dst[6] = T((v >> 42) & 127)
	dst[7] = T((v >> 49) & 127)
}

func unpack_7[T types.Integer](v uint64, p unsafe.Pointer) {
	dst := (*[7]T)(p)
	dst[0] = T(v & 255)
	dst[1] = T((v >> 8) & 255)
	dst[2] = T((v >> 16) & 255)
	dst[3] = T((v >> 24) & 255)
	dst[4] = T((v >> 32) & 255)
	dst[5] = T((v >> 40) & 255)
	dst[6] = T((v >> 48) & 255)
}

func unpack_6[T types.Integer](v uint64, p unsafe.Pointer) {
	dst := (*[6]T)(p)
	dst[0] = T(v & 1023)
	dst[1] = T((v >> 10) & 1023)
	dst[2] = T((v >> 20) & 1023)
	dst[3] = T((v >> 30) & 1023)
	dst[4] = T((v >> 40) & 1023)
	dst[5] = T((v >> 50) & 1023)
}

func unpack_5[T types.Integer](v uint64, p unsafe.Pointer) {
	dst := (*[5]T)(p)
	dst[0] = T(v & 4095)
	dst[1] = T((v >> 12) & 4095)
	dst[2] = T((v >> 24) & 4095)
	dst[3] = T((v >> 36) & 4095)
	dst[4] = T((v >> 48) & 4095)
}

func unpack_4[T types.Integer](v uint64, p unsafe.Pointer) {
	dst := (*[4]T)(p)
	dst[0] = T(v & 32767)
	dst[1] = T((v >> 15) & 32767)
	dst[2] = T((v >> 30) & 32767)
	dst[3] = T((v >> 45) & 32767)
}

func unpack_3[T types.Integer](v uint64, p unsafe.Pointer) {
	dst := (*[3]T)(p)
	dst[0] = T(v & 1048575)
	dst[1] = T((v >> 20) & 1048575)
	dst[2] = T((v >> 40) & 1048575)
}

func unpack_2[T types.Integer](v uint64, p unsafe.Pointer) {
	dst := (*[2]T)(p)
	dst[0] = T(v & 1073741823)
	dst[1] = T((v >> 30) & 1073741823)
}

func unpack_1[T types.Integer](v uint64, p unsafe.Pointer) {
	dst := (*[1]T)(p)
	dst[0] = T(v & 1152921504606846975)
}
