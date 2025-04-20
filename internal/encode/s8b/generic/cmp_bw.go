// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

// Comparing if a value x is between, i.e. in the range [a, b]. We use
// an unsigned integer wrap-around trick to translate two comparison ops
// into one subtraction and one compare:
//
// x ∈ [a, b] <-> x - a <= b - a
//
// This is not necessarily faster by shorter to write.

var cmp_bw [16]cmpFunc2 = [16]cmpFunc2{
	cmp_bw_0, cmp_bw_1, cmp_bw_2, cmp_bw_3, cmp_bw_4, cmp_bw_5, cmp_bw_6, cmp_bw_7,
	cmp_bw_8, cmp_bw_9, cmp_bw_10, cmp_bw_11, cmp_bw_12, cmp_bw_13, cmp_bw_14, cmp_bw_15,
}

func cmp_bw_0(_, val, val2 uint64) (int, uint64) {
	if val == 0 {
		return 1, 1
	}
	return 1, 0
}

func cmp_bw_1(_, val, val2 uint64) (int, uint64) {
	if val <= 1 && val2 >= 1 {
		return 1, 1
	}
	return 1, 0
}

func cmp_bw_2(word, val, val2 uint64) (int, uint64) {
	var bits uint64
	mask := uint64(0x1)
	val2 -= val
	if val <= mask {
		bits = b2u64(word&mask-val <= val2) |
			b2u64(word>>1&mask-val <= val2)<<1 |
			b2u64(word>>2&mask-val <= val2)<<2 |
			b2u64(word>>3&mask-val <= val2)<<3 |
			b2u64(word>>4&mask-val <= val2)<<4 |
			b2u64(word>>5&mask-val <= val2)<<5 |
			b2u64(word>>6&mask-val <= val2)<<6 |
			b2u64(word>>7&mask-val <= val2)<<7 |
			b2u64(word>>8&mask-val <= val2)<<8 |
			b2u64(word>>9&mask-val <= val2)<<9 |
			b2u64(word>>10&mask-val <= val2)<<10 |
			b2u64(word>>11&mask-val <= val2)<<11 |
			b2u64(word>>12&mask-val <= val2)<<12 |
			b2u64(word>>13&mask-val <= val2)<<13 |
			b2u64(word>>14&mask-val <= val2)<<14 |
			b2u64(word>>15&mask-val <= val2)<<15 |
			b2u64(word>>16&mask-val <= val2)<<16 |
			b2u64(word>>17&mask-val <= val2)<<17 |
			b2u64(word>>18&mask-val <= val2)<<18 |
			b2u64(word>>19&mask-val <= val2)<<19 |
			b2u64(word>>20&mask-val <= val2)<<20 |
			b2u64(word>>21&mask-val <= val2)<<21 |
			b2u64(word>>22&mask-val <= val2)<<22 |
			b2u64(word>>23&mask-val <= val2)<<23 |
			b2u64(word>>24&mask-val <= val2)<<24 |
			b2u64(word>>25&mask-val <= val2)<<25 |
			b2u64(word>>26&mask-val <= val2)<<26 |
			b2u64(word>>27&mask-val <= val2)<<27 |
			b2u64(word>>28&mask-val <= val2)<<28 |
			b2u64(word>>29&mask-val <= val2)<<29 |
			b2u64(word>>30&mask-val <= val2)<<30 |
			b2u64(word>>31&mask-val <= val2)<<31 |
			b2u64(word>>32&mask-val <= val2)<<32 |
			b2u64(word>>33&mask-val <= val2)<<33 |
			b2u64(word>>34&mask-val <= val2)<<34 |
			b2u64(word>>35&mask-val <= val2)<<35 |
			b2u64(word>>36&mask-val <= val2)<<36 |
			b2u64(word>>37&mask-val <= val2)<<37 |
			b2u64(word>>38&mask-val <= val2)<<38 |
			b2u64(word>>39&mask-val <= val2)<<39 |
			b2u64(word>>40&mask-val <= val2)<<40 |
			b2u64(word>>41&mask-val <= val2)<<41 |
			b2u64(word>>42&mask-val <= val2)<<42 |
			b2u64(word>>43&mask-val <= val2)<<43 |
			b2u64(word>>44&mask-val <= val2)<<44 |
			b2u64(word>>45&mask-val <= val2)<<45 |
			b2u64(word>>46&mask-val <= val2)<<46 |
			b2u64(word>>47&mask-val <= val2)<<47 |
			b2u64(word>>48&mask-val <= val2)<<48 |
			b2u64(word>>49&mask-val <= val2)<<49 |
			b2u64(word>>50&mask-val <= val2)<<50 |
			b2u64(word>>51&mask-val <= val2)<<51 |
			b2u64(word>>52&mask-val <= val2)<<52 |
			b2u64(word>>53&mask-val <= val2)<<53 |
			b2u64(word>>54&mask-val <= val2)<<54 |
			b2u64(word>>55&mask-val <= val2)<<55 |
			b2u64(word>>56&mask-val <= val2)<<56 |
			b2u64(word>>57&mask-val <= val2)<<57 |
			b2u64(word>>58&mask-val <= val2)<<58 |
			b2u64(word>>59&mask-val <= val2)<<59
	}
	return 60, bits
}

func cmp_bw_3(word, val, val2 uint64) (int, uint64) {
	var bits uint64
	mask := uint64(0x3)
	val2 -= val
	if val <= mask {
		bits = b2u64(word&mask-val <= val2) |
			b2u64(word>>2&mask-val <= val2)<<1 |
			b2u64(word>>4&mask-val <= val2)<<2 |
			b2u64(word>>6&mask-val <= val2)<<3 |
			b2u64(word>>8&mask-val <= val2)<<4 |
			b2u64(word>>10&mask-val <= val2)<<5 |
			b2u64(word>>12&mask-val <= val2)<<6 |
			b2u64(word>>14&mask-val <= val2)<<7 |
			b2u64(word>>16&mask-val <= val2)<<8 |
			b2u64(word>>18&mask-val <= val2)<<9 |
			b2u64(word>>20&mask-val <= val2)<<10 |
			b2u64(word>>22&mask-val <= val2)<<11 |
			b2u64(word>>24&mask-val <= val2)<<12 |
			b2u64(word>>26&mask-val <= val2)<<13 |
			b2u64(word>>28&mask-val <= val2)<<14 |
			b2u64(word>>30&mask-val <= val2)<<15 |
			b2u64(word>>32&mask-val <= val2)<<16 |
			b2u64(word>>34&mask-val <= val2)<<17 |
			b2u64(word>>36&mask-val <= val2)<<18 |
			b2u64(word>>38&mask-val <= val2)<<19 |
			b2u64(word>>40&mask-val <= val2)<<20 |
			b2u64(word>>42&mask-val <= val2)<<21 |
			b2u64(word>>44&mask-val <= val2)<<22 |
			b2u64(word>>46&mask-val <= val2)<<23 |
			b2u64(word>>48&mask-val <= val2)<<24 |
			b2u64(word>>50&mask-val <= val2)<<25 |
			b2u64(word>>52&mask-val <= val2)<<26 |
			b2u64(word>>54&mask-val <= val2)<<27 |
			b2u64(word>>56&mask-val <= val2)<<28 |
			b2u64(word>>58&mask-val <= val2)<<29
	}
	return 30, bits
}

func cmp_bw_4(word, val, val2 uint64) (int, uint64) {
	var bits uint64
	mask := uint64(0x7)
	val2 -= val
	if val <= mask {
		bits = b2u64(word&mask-val <= val2) |
			b2u64(word>>3&mask-val <= val2)<<1 |
			b2u64(word>>6&mask-val <= val2)<<2 |
			b2u64(word>>9&mask-val <= val2)<<3 |
			b2u64(word>>12&mask-val <= val2)<<4 |
			b2u64(word>>15&mask-val <= val2)<<5 |
			b2u64(word>>18&mask-val <= val2)<<6 |
			b2u64(word>>21&mask-val <= val2)<<7 |
			b2u64(word>>24&mask-val <= val2)<<8 |
			b2u64(word>>27&mask-val <= val2)<<9 |
			b2u64(word>>30&mask-val <= val2)<<10 |
			b2u64(word>>33&mask-val <= val2)<<11 |
			b2u64(word>>36&mask-val <= val2)<<12 |
			b2u64(word>>39&mask-val <= val2)<<13 |
			b2u64(word>>42&mask-val <= val2)<<14 |
			b2u64(word>>45&mask-val <= val2)<<15 |
			b2u64(word>>48&mask-val <= val2)<<16 |
			b2u64(word>>51&mask-val <= val2)<<17 |
			b2u64(word>>54&mask-val <= val2)<<18 |
			b2u64(word>>57&mask-val <= val2)<<19
	}
	return 20, bits
}

func cmp_bw_5(word, val, val2 uint64) (int, uint64) {
	var bits uint64
	mask := uint64(0xF)
	val2 -= val
	if val <= mask {
		bits = b2u64(word&mask-val <= val2) |
			b2u64(word>>4&mask-val <= val2)<<1 |
			b2u64(word>>8&mask-val <= val2)<<2 |
			b2u64(word>>12&mask-val <= val2)<<3 |
			b2u64(word>>16&mask-val <= val2)<<4 |
			b2u64(word>>20&mask-val <= val2)<<5 |
			b2u64(word>>24&mask-val <= val2)<<6 |
			b2u64(word>>28&mask-val <= val2)<<7 |
			b2u64(word>>32&mask-val <= val2)<<8 |
			b2u64(word>>36&mask-val <= val2)<<9 |
			b2u64(word>>40&mask-val <= val2)<<10 |
			b2u64(word>>44&mask-val <= val2)<<11 |
			b2u64(word>>48&mask-val <= val2)<<12 |
			b2u64(word>>52&mask-val <= val2)<<13 |
			b2u64(word>>56&mask-val <= val2)<<14
	}
	return 15, bits
}

func cmp_bw_6(word, val, val2 uint64) (int, uint64) {
	var bits uint64
	mask := uint64(0x1F)
	val2 -= val
	if val <= mask {
		bits = b2u64(word&mask-val <= val2) |
			b2u64(word>>5&mask-val <= val2)<<1 |
			b2u64(word>>10&mask-val <= val2)<<2 |
			b2u64(word>>15&mask-val <= val2)<<3 |
			b2u64(word>>20&mask-val <= val2)<<4 |
			b2u64(word>>25&mask-val <= val2)<<5 |
			b2u64(word>>30&mask-val <= val2)<<6 |
			b2u64(word>>35&mask-val <= val2)<<7 |
			b2u64(word>>40&mask-val <= val2)<<8 |
			b2u64(word>>45&mask-val <= val2)<<9 |
			b2u64(word>>50&mask-val <= val2)<<10 |
			b2u64(word>>55&mask-val <= val2)<<11
	}
	return 12, bits
}

func cmp_bw_7(word, val, val2 uint64) (int, uint64) {
	var bits uint64
	mask := uint64(0x3F)
	val2 -= val
	if val <= mask {
		bits = b2u64(word&mask-val <= val2) |
			b2u64(word>>6&mask-val <= val2)<<1 |
			b2u64(word>>12&mask-val <= val2)<<2 |
			b2u64(word>>18&mask-val <= val2)<<3 |
			b2u64(word>>24&mask-val <= val2)<<4 |
			b2u64(word>>30&mask-val <= val2)<<5 |
			b2u64(word>>36&mask-val <= val2)<<6 |
			b2u64(word>>42&mask-val <= val2)<<7 |
			b2u64(word>>48&mask-val <= val2)<<8 |
			b2u64(word>>54&mask-val <= val2)<<9
	}
	return 10, bits
}

func cmp_bw_8(word, val, val2 uint64) (int, uint64) {
	var bits uint64
	mask := uint64(0x7F)
	val2 -= val
	if val <= mask {
		bits = b2u64(word&mask-val <= val2) |
			b2u64(word>>7&mask-val <= val2)<<1 |
			b2u64(word>>14&mask-val <= val2)<<2 |
			b2u64(word>>21&mask-val <= val2)<<3 |
			b2u64(word>>28&mask-val <= val2)<<4 |
			b2u64(word>>35&mask-val <= val2)<<5 |
			b2u64(word>>42&mask-val <= val2)<<6 |
			b2u64(word>>49&mask-val <= val2)<<7
	}
	return 8, bits
}

func cmp_bw_9(word, val, val2 uint64) (int, uint64) {
	var bits uint64
	mask := uint64(0xFF)
	val2 -= val
	if val <= mask {
		bits = b2u64(word&mask-val <= val2) |
			b2u64(word>>8&mask-val <= val2)<<1 |
			b2u64(word>>16&mask-val <= val2)<<2 |
			b2u64(word>>24&mask-val <= val2)<<3 |
			b2u64(word>>32&mask-val <= val2)<<4 |
			b2u64(word>>40&mask-val <= val2)<<5 |
			b2u64(word>>48&mask-val <= val2)<<6
	}
	return 7, bits
}

func cmp_bw_10(word, val, val2 uint64) (int, uint64) {
	var bits uint64
	mask := uint64(0x3FF)
	val2 -= val
	if val <= mask {
		bits = b2u64(word&mask-val <= val2) |
			b2u64(word>>10&mask-val <= val2)<<1 |
			b2u64(word>>20&mask-val <= val2)<<2 |
			b2u64(word>>30&mask-val <= val2)<<3 |
			b2u64(word>>40&mask-val <= val2)<<4 |
			b2u64(word>>50&mask-val <= val2)<<5
	}
	return 6, bits
}

func cmp_bw_11(word, val, val2 uint64) (int, uint64) {
	var bits uint64
	mask := uint64(0xFFF)
	val2 -= val
	if val <= mask {
		bits = b2u64(word&mask-val <= val2) |
			b2u64(word>>12&mask-val <= val2)<<1 |
			b2u64(word>>24&mask-val <= val2)<<2 |
			b2u64(word>>36&mask-val <= val2)<<3 |
			b2u64(word>>48&mask-val <= val2)<<4
	}
	return 5, bits
}

func cmp_bw_12(word, val, val2 uint64) (int, uint64) {
	var bits uint64
	mask := uint64(0x7FFF)
	val2 -= val
	if val <= mask {
		bits = b2u64(word&mask-val <= val2) |
			b2u64(word>>15&mask-val <= val2)<<1 |
			b2u64(word>>30&mask-val <= val2)<<2 |
			b2u64(word>>45&mask-val <= val2)<<3
	}
	return 4, bits
}

func cmp_bw_13(word, val, val2 uint64) (int, uint64) {
	var bits uint64
	mask := uint64(0xFFFFF)
	val2 -= val
	if val <= mask {
		bits = b2u64(word&mask-val <= val2) |
			b2u64(word>>20&mask-val <= val2)<<1 |
			b2u64(word>>40&mask-val <= val2)<<2
	}
	return 3, bits
}

func cmp_bw_14(word, val, val2 uint64) (int, uint64) {
	var bits uint64
	mask := uint64(0x3FFFFFFF)
	val2 -= val
	if val <= mask {
		bits = b2u64(word&mask-val <= val2) |
			b2u64(word>>30&mask-val <= val2)<<1
	}
	return 2, bits
}

func cmp_bw_15(word, val, val2 uint64) (int, uint64) {
	val2 -= val
	return 1, b2u64(word&0x0FFFFFFFFFFFFFFF-val <= val2)
}
