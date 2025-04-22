// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"unsafe"

	"blockwatch.cc/knoxdb/internal/types"
)

func unpack_for_zero[T types.Integer](_ uint64, p unsafe.Pointer, minv uint64) {
	dst := (*[128]T)(p)
	for i := range dst {
		dst[i] = T(minv)
	}
}

func unpack_for_one[T types.Integer](_ uint64, p unsafe.Pointer, minv uint64) {
	dst := (*[128]T)(p)
	for i := range dst {
		dst[i] = T(minv + 1)
	}
}

func unpack_for_60[T types.Integer](v uint64, p unsafe.Pointer, minv uint64) {
	dst := (*[60]T)(p)
	dst[0] = T(v&1 + minv)
	dst[1] = T((v>>1)&1 + minv)
	dst[2] = T((v>>2)&1 + minv)
	dst[3] = T((v>>3)&1 + minv)
	dst[4] = T((v>>4)&1 + minv)
	dst[5] = T((v>>5)&1 + minv)
	dst[6] = T((v>>6)&1 + minv)
	dst[7] = T((v>>7)&1 + minv)
	dst[8] = T((v>>8)&1 + minv)
	dst[9] = T((v>>9)&1 + minv)
	dst[10] = T((v>>10)&1 + minv)
	dst[11] = T((v>>11)&1 + minv)
	dst[12] = T((v>>12)&1 + minv)
	dst[13] = T((v>>13)&1 + minv)
	dst[14] = T((v>>14)&1 + minv)
	dst[15] = T((v>>15)&1 + minv)
	dst[16] = T((v>>16)&1 + minv)
	dst[17] = T((v>>17)&1 + minv)
	dst[18] = T((v>>18)&1 + minv)
	dst[19] = T((v>>19)&1 + minv)
	dst[20] = T((v>>20)&1 + minv)
	dst[21] = T((v>>21)&1 + minv)
	dst[22] = T((v>>22)&1 + minv)
	dst[23] = T((v>>23)&1 + minv)
	dst[24] = T((v>>24)&1 + minv)
	dst[25] = T((v>>25)&1 + minv)
	dst[26] = T((v>>26)&1 + minv)
	dst[27] = T((v>>27)&1 + minv)
	dst[28] = T((v>>28)&1 + minv)
	dst[29] = T((v>>29)&1 + minv)
	dst[30] = T((v>>30)&1 + minv)
	dst[31] = T((v>>31)&1 + minv)
	dst[32] = T((v>>32)&1 + minv)
	dst[33] = T((v>>33)&1 + minv)
	dst[34] = T((v>>34)&1 + minv)
	dst[35] = T((v>>35)&1 + minv)
	dst[36] = T((v>>36)&1 + minv)
	dst[37] = T((v>>37)&1 + minv)
	dst[38] = T((v>>38)&1 + minv)
	dst[39] = T((v>>39)&1 + minv)
	dst[40] = T((v>>40)&1 + minv)
	dst[41] = T((v>>41)&1 + minv)
	dst[42] = T((v>>42)&1 + minv)
	dst[43] = T((v>>43)&1 + minv)
	dst[44] = T((v>>44)&1 + minv)
	dst[45] = T((v>>45)&1 + minv)
	dst[46] = T((v>>46)&1 + minv)
	dst[47] = T((v>>47)&1 + minv)
	dst[48] = T((v>>48)&1 + minv)
	dst[49] = T((v>>49)&1 + minv)
	dst[50] = T((v>>50)&1 + minv)
	dst[51] = T((v>>51)&1 + minv)
	dst[52] = T((v>>52)&1 + minv)
	dst[53] = T((v>>53)&1 + minv)
	dst[54] = T((v>>54)&1 + minv)
	dst[55] = T((v>>55)&1 + minv)
	dst[56] = T((v>>56)&1 + minv)
	dst[57] = T((v>>57)&1 + minv)
	dst[58] = T((v>>58)&1 + minv)
	dst[59] = T((v>>59)&1 + minv)
}

func unpack_for_30[T types.Integer](v uint64, p unsafe.Pointer, minv uint64) {
	dst := (*[30]T)(p)
	dst[0] = T(v&3 + minv)
	dst[1] = T((v>>2)&3 + minv)
	dst[2] = T((v>>4)&3 + minv)
	dst[3] = T((v>>6)&3 + minv)
	dst[4] = T((v>>8)&3 + minv)
	dst[5] = T((v>>10)&3 + minv)
	dst[6] = T((v>>12)&3 + minv)
	dst[7] = T((v>>14)&3 + minv)
	dst[8] = T((v>>16)&3 + minv)
	dst[9] = T((v>>18)&3 + minv)
	dst[10] = T((v>>20)&3 + minv)
	dst[11] = T((v>>22)&3 + minv)
	dst[12] = T((v>>24)&3 + minv)
	dst[13] = T((v>>26)&3 + minv)
	dst[14] = T((v>>28)&3 + minv)
	dst[15] = T((v>>30)&3 + minv)
	dst[16] = T((v>>32)&3 + minv)
	dst[17] = T((v>>34)&3 + minv)
	dst[18] = T((v>>36)&3 + minv)
	dst[19] = T((v>>38)&3 + minv)
	dst[20] = T((v>>40)&3 + minv)
	dst[21] = T((v>>42)&3 + minv)
	dst[22] = T((v>>44)&3 + minv)
	dst[23] = T((v>>46)&3 + minv)
	dst[24] = T((v>>48)&3 + minv)
	dst[25] = T((v>>50)&3 + minv)
	dst[26] = T((v>>52)&3 + minv)
	dst[27] = T((v>>54)&3 + minv)
	dst[28] = T((v>>56)&3 + minv)
	dst[29] = T((v>>58)&3 + minv)
}

func unpack_for_20[T types.Integer](v uint64, p unsafe.Pointer, minv uint64) {
	dst := (*[20]T)(p)
	dst[0] = T(v&7 + minv)
	dst[1] = T((v>>3)&7 + minv)
	dst[2] = T((v>>6)&7 + minv)
	dst[3] = T((v>>9)&7 + minv)
	dst[4] = T((v>>12)&7 + minv)
	dst[5] = T((v>>15)&7 + minv)
	dst[6] = T((v>>18)&7 + minv)
	dst[7] = T((v>>21)&7 + minv)
	dst[8] = T((v>>24)&7 + minv)
	dst[9] = T((v>>27)&7 + minv)
	dst[10] = T((v>>30)&7 + minv)
	dst[11] = T((v>>33)&7 + minv)
	dst[12] = T((v>>36)&7 + minv)
	dst[13] = T((v>>39)&7 + minv)
	dst[14] = T((v>>42)&7 + minv)
	dst[15] = T((v>>45)&7 + minv)
	dst[16] = T((v>>48)&7 + minv)
	dst[17] = T((v>>51)&7 + minv)
	dst[18] = T((v>>54)&7 + minv)
	dst[19] = T((v>>57)&7 + minv)
}

func unpack_for_15[T types.Integer](v uint64, p unsafe.Pointer, minv uint64) {
	dst := (*[15]T)(p)
	dst[0] = T(v&0xF + minv)
	dst[1] = T((v>>4)&0xF + minv)
	dst[2] = T((v>>8)&0xF + minv)
	dst[3] = T((v>>12)&0xF + minv)
	dst[4] = T((v>>16)&0xF + minv)
	dst[5] = T((v>>20)&0xF + minv)
	dst[6] = T((v>>24)&0xF + minv)
	dst[7] = T((v>>28)&0xF + minv)
	dst[8] = T((v>>32)&0xF + minv)
	dst[9] = T((v>>36)&0xF + minv)
	dst[10] = T((v>>40)&0xF + minv)
	dst[11] = T((v>>44)&0xF + minv)
	dst[12] = T((v>>48)&0xF + minv)
	dst[13] = T((v>>52)&0xF + minv)
	dst[14] = T((v>>56)&0xF + minv)
}

func unpack_for_12[T types.Integer](v uint64, p unsafe.Pointer, minv uint64) {
	dst := (*[12]T)(p)
	dst[0] = T(v&0x1F + minv)
	dst[1] = T((v>>5)&0x1F + minv)
	dst[2] = T((v>>10)&0x1F + minv)
	dst[3] = T((v>>15)&0x1F + minv)
	dst[4] = T((v>>20)&0x1F + minv)
	dst[5] = T((v>>25)&0x1F + minv)
	dst[6] = T((v>>30)&0x1F + minv)
	dst[7] = T((v>>35)&0x1F + minv)
	dst[8] = T((v>>40)&0x1F + minv)
	dst[9] = T((v>>45)&0x1F + minv)
	dst[10] = T((v>>50)&0x1F + minv)
	dst[11] = T((v>>55)&0x1F + minv)
}

func unpack_for_10[T types.Integer](v uint64, p unsafe.Pointer, minv uint64) {
	dst := (*[10]T)(p)
	dst[0] = T(v&0x3F + minv)
	dst[1] = T((v>>6)&0x3F + minv)
	dst[2] = T((v>>12)&0x3F + minv)
	dst[3] = T((v>>18)&0x3F + minv)
	dst[4] = T((v>>24)&0x3F + minv)
	dst[5] = T((v>>30)&0x3F + minv)
	dst[6] = T((v>>36)&0x3F + minv)
	dst[7] = T((v>>42)&0x3F + minv)
	dst[8] = T((v>>48)&0x3F + minv)
	dst[9] = T((v>>54)&0x3F + minv)
}

func unpack_for_8[T types.Integer](v uint64, p unsafe.Pointer, minv uint64) {
	dst := (*[8]T)(p)
	dst[0] = T(v&0x7F + minv)
	dst[1] = T((v>>7)&0x7F + minv)
	dst[2] = T((v>>14)&0x7F + minv)
	dst[3] = T((v>>21)&0x7F + minv)
	dst[4] = T((v>>28)&0x7F + minv)
	dst[5] = T((v>>35)&0x7F + minv)
	dst[6] = T((v>>42)&0x7F + minv)
	dst[7] = T((v>>49)&0x7F + minv)
}

func unpack_for_7[T types.Integer](v uint64, p unsafe.Pointer, minv uint64) {
	dst := (*[7]T)(p)
	dst[0] = T(v&0xFF + minv)
	dst[1] = T((v>>8)&0xFF + minv)
	dst[2] = T((v>>16)&0xFF + minv)
	dst[3] = T((v>>24)&0xFF + minv)
	dst[4] = T((v>>32)&0xFF + minv)
	dst[5] = T((v>>40)&0xFF + minv)
	dst[6] = T((v>>48)&0xFF + minv)
}

func unpack_for_6[T types.Integer](v uint64, p unsafe.Pointer, minv uint64) {
	dst := (*[6]T)(p)
	dst[0] = T(v&0x3FF + minv)
	dst[1] = T((v>>10)&0x3FF + minv)
	dst[2] = T((v>>20)&0x3FF + minv)
	dst[3] = T((v>>30)&0x3FF + minv)
	dst[4] = T((v>>40)&0x3FF + minv)
	dst[5] = T((v>>50)&0x3FF + minv)
}

func unpack_for_5[T types.Integer](v uint64, p unsafe.Pointer, minv uint64) {
	dst := (*[5]T)(p)
	dst[0] = T(v&0xFFF + minv)
	dst[1] = T((v>>12)&0xFFF + minv)
	dst[2] = T((v>>24)&0xFFF + minv)
	dst[3] = T((v>>36)&0xFFF + minv)
	dst[4] = T((v>>48)&0xFFF + minv)
}

func unpack_for_4[T types.Integer](v uint64, p unsafe.Pointer, minv uint64) {
	dst := (*[4]T)(p)
	dst[0] = T(v&0x7FFF + minv)
	dst[1] = T((v>>15)&0x7FFF + minv)
	dst[2] = T((v>>30)&0x7FFF + minv)
	dst[3] = T((v>>45)&0x7FFF + minv)
}

func unpack_for_3[T types.Integer](v uint64, p unsafe.Pointer, minv uint64) {
	dst := (*[3]T)(p)
	dst[0] = T(v&0xFFFFF + minv)
	dst[1] = T((v>>20)&0xFFFFF + minv)
	dst[2] = T((v>>40)&0xFFFFF + minv)
}

func unpack_for_2[T types.Integer](v uint64, p unsafe.Pointer, minv uint64) {
	dst := (*[2]T)(p)
	dst[0] = T(v&0x3FFFFFFF + minv)
	dst[1] = T((v>>30)&0x3FFFFFFF + minv)
}

func unpack_for_1[T types.Integer](v uint64, p unsafe.Pointer, minv uint64) {
	dst := (*[1]T)(p)
	dst[0] = T(v&0xFFFFFFFFFFFFFFF + minv)
}
