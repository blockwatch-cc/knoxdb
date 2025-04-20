// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

var cmp_eq [16]cmpFunc = [16]cmpFunc{
	cmp_eq_0, cmp_eq_1, cmp_eq_2, cmp_eq_3, cmp_eq_4, cmp_eq_5, cmp_eq_6, cmp_eq_7,
	cmp_eq_8, cmp_eq_9, cmp_eq_10, cmp_eq_11, cmp_eq_12, cmp_eq_13, cmp_eq_14, cmp_eq_15,
}

func cmp_eq_0(_, val uint64) (int, uint64) {
	if val == 0 {
		return 1, 1
	}
	return 1, 0
}

func cmp_eq_1(_, val uint64) (int, uint64) {
	if val == 1 {
		return 1, 1
	}
	return 1, 0
}

func cmp_eq_2(word, val uint64) (int, uint64) {
	var bits uint64
	mask := uint64(0x1)
	if val <= mask {
		bits = b2u64(word&mask == val) |
			b2u64(word>>1&mask == val)<<1 |
			b2u64(word>>2&mask == val)<<2 |
			b2u64(word>>3&mask == val)<<3 |
			b2u64(word>>4&mask == val)<<4 |
			b2u64(word>>5&mask == val)<<5 |
			b2u64(word>>6&mask == val)<<6 |
			b2u64(word>>7&mask == val)<<7 |
			b2u64(word>>8&mask == val)<<8 |
			b2u64(word>>9&mask == val)<<9 |
			b2u64(word>>10&mask == val)<<10 |
			b2u64(word>>11&mask == val)<<11 |
			b2u64(word>>12&mask == val)<<12 |
			b2u64(word>>13&mask == val)<<13 |
			b2u64(word>>14&mask == val)<<14 |
			b2u64(word>>15&mask == val)<<15 |
			b2u64(word>>16&mask == val)<<16 |
			b2u64(word>>17&mask == val)<<17 |
			b2u64(word>>18&mask == val)<<18 |
			b2u64(word>>19&mask == val)<<19 |
			b2u64(word>>20&mask == val)<<20 |
			b2u64(word>>21&mask == val)<<21 |
			b2u64(word>>22&mask == val)<<22 |
			b2u64(word>>23&mask == val)<<23 |
			b2u64(word>>24&mask == val)<<24 |
			b2u64(word>>25&mask == val)<<25 |
			b2u64(word>>26&mask == val)<<26 |
			b2u64(word>>27&mask == val)<<27 |
			b2u64(word>>28&mask == val)<<28 |
			b2u64(word>>29&mask == val)<<29 |
			b2u64(word>>30&mask == val)<<30 |
			b2u64(word>>31&mask == val)<<31 |
			b2u64(word>>32&mask == val)<<32 |
			b2u64(word>>33&mask == val)<<33 |
			b2u64(word>>34&mask == val)<<34 |
			b2u64(word>>35&mask == val)<<35 |
			b2u64(word>>36&mask == val)<<36 |
			b2u64(word>>37&mask == val)<<37 |
			b2u64(word>>38&mask == val)<<38 |
			b2u64(word>>39&mask == val)<<39 |
			b2u64(word>>40&mask == val)<<40 |
			b2u64(word>>41&mask == val)<<41 |
			b2u64(word>>42&mask == val)<<42 |
			b2u64(word>>43&mask == val)<<43 |
			b2u64(word>>44&mask == val)<<44 |
			b2u64(word>>45&mask == val)<<45 |
			b2u64(word>>46&mask == val)<<46 |
			b2u64(word>>47&mask == val)<<47 |
			b2u64(word>>48&mask == val)<<48 |
			b2u64(word>>49&mask == val)<<49 |
			b2u64(word>>50&mask == val)<<50 |
			b2u64(word>>51&mask == val)<<51 |
			b2u64(word>>52&mask == val)<<52 |
			b2u64(word>>53&mask == val)<<53 |
			b2u64(word>>54&mask == val)<<54 |
			b2u64(word>>55&mask == val)<<55 |
			b2u64(word>>56&mask == val)<<56 |
			b2u64(word>>57&mask == val)<<57 |
			b2u64(word>>58&mask == val)<<58 |
			b2u64(word>>59&mask == val)<<59
	}
	return 60, bits
}

func cmp_eq_3(word, val uint64) (int, uint64) {
	var bits uint64
	mask := uint64(0x3)
	if val <= mask {
		bits = b2u64(word&mask == val) |
			b2u64(word>>2&mask == val)<<1 |
			b2u64(word>>4&mask == val)<<2 |
			b2u64(word>>6&mask == val)<<3 |
			b2u64(word>>8&mask == val)<<4 |
			b2u64(word>>10&mask == val)<<5 |
			b2u64(word>>12&mask == val)<<6 |
			b2u64(word>>14&mask == val)<<7 |
			b2u64(word>>16&mask == val)<<8 |
			b2u64(word>>18&mask == val)<<9 |
			b2u64(word>>20&mask == val)<<10 |
			b2u64(word>>22&mask == val)<<11 |
			b2u64(word>>24&mask == val)<<12 |
			b2u64(word>>26&mask == val)<<13 |
			b2u64(word>>28&mask == val)<<14 |
			b2u64(word>>30&mask == val)<<15 |
			b2u64(word>>32&mask == val)<<16 |
			b2u64(word>>34&mask == val)<<17 |
			b2u64(word>>36&mask == val)<<18 |
			b2u64(word>>38&mask == val)<<19 |
			b2u64(word>>40&mask == val)<<20 |
			b2u64(word>>42&mask == val)<<21 |
			b2u64(word>>44&mask == val)<<22 |
			b2u64(word>>46&mask == val)<<23 |
			b2u64(word>>48&mask == val)<<24 |
			b2u64(word>>50&mask == val)<<25 |
			b2u64(word>>52&mask == val)<<26 |
			b2u64(word>>54&mask == val)<<27 |
			b2u64(word>>56&mask == val)<<28 |
			b2u64(word>>58&mask == val)<<29
	}
	return 30, bits
}

func cmp_eq_4(word, val uint64) (int, uint64) {
	var bits uint64
	mask := uint64(0x7)
	if val <= mask {
		bits = b2u64(word&mask == val) |
			b2u64(word>>3&mask == val)<<1 |
			b2u64(word>>6&mask == val)<<2 |
			b2u64(word>>9&mask == val)<<3 |
			b2u64(word>>12&mask == val)<<4 |
			b2u64(word>>15&mask == val)<<5 |
			b2u64(word>>18&mask == val)<<6 |
			b2u64(word>>21&mask == val)<<7 |
			b2u64(word>>24&mask == val)<<8 |
			b2u64(word>>27&mask == val)<<9 |
			b2u64(word>>30&mask == val)<<10 |
			b2u64(word>>33&mask == val)<<11 |
			b2u64(word>>36&mask == val)<<12 |
			b2u64(word>>39&mask == val)<<13 |
			b2u64(word>>42&mask == val)<<14 |
			b2u64(word>>45&mask == val)<<15 |
			b2u64(word>>48&mask == val)<<16 |
			b2u64(word>>51&mask == val)<<17 |
			b2u64(word>>54&mask == val)<<18 |
			b2u64(word>>57&mask == val)<<19
	}
	return 20, bits
}

func cmp_eq_5(word, val uint64) (int, uint64) {
	var bits uint64
	mask := uint64(0xF)
	if val <= mask {
		bits = b2u64(word&mask == val) |
			b2u64(word>>4&mask == val)<<1 |
			b2u64(word>>8&mask == val)<<2 |
			b2u64(word>>12&mask == val)<<3 |
			b2u64(word>>16&mask == val)<<4 |
			b2u64(word>>20&mask == val)<<5 |
			b2u64(word>>24&mask == val)<<6 |
			b2u64(word>>28&mask == val)<<7 |
			b2u64(word>>32&mask == val)<<8 |
			b2u64(word>>36&mask == val)<<9 |
			b2u64(word>>40&mask == val)<<10 |
			b2u64(word>>44&mask == val)<<11 |
			b2u64(word>>48&mask == val)<<12 |
			b2u64(word>>52&mask == val)<<13 |
			b2u64(word>>56&mask == val)<<14
	}
	return 15, bits
}

func cmp_eq_6(word, val uint64) (int, uint64) {
	var bits uint64
	mask := uint64(0x1F)
	if val <= mask {
		bits = b2u64(word&mask == val) |
			b2u64(word>>5&mask == val)<<1 |
			b2u64(word>>10&mask == val)<<2 |
			b2u64(word>>15&mask == val)<<3 |
			b2u64(word>>20&mask == val)<<4 |
			b2u64(word>>25&mask == val)<<5 |
			b2u64(word>>30&mask == val)<<6 |
			b2u64(word>>35&mask == val)<<7 |
			b2u64(word>>40&mask == val)<<8 |
			b2u64(word>>45&mask == val)<<9 |
			b2u64(word>>50&mask == val)<<10 |
			b2u64(word>>55&mask == val)<<11
	}
	return 12, bits
}

func cmp_eq_7(word, val uint64) (int, uint64) {
	var bits uint64
	mask := uint64(0x3F)
	if val <= mask {
		bits = b2u64(word&mask == val) |
			b2u64(word>>6&mask == val)<<1 |
			b2u64(word>>12&mask == val)<<2 |
			b2u64(word>>18&mask == val)<<3 |
			b2u64(word>>24&mask == val)<<4 |
			b2u64(word>>30&mask == val)<<5 |
			b2u64(word>>36&mask == val)<<6 |
			b2u64(word>>42&mask == val)<<7 |
			b2u64(word>>48&mask == val)<<8 |
			b2u64(word>>54&mask == val)<<9
	}
	return 10, bits
}

func cmp_eq_8(word, val uint64) (int, uint64) {
	var bits uint64
	mask := uint64(0x7F)
	if val <= mask {
		bits = b2u64(word&mask == val) |
			b2u64(word>>7&mask == val)<<1 |
			b2u64(word>>14&mask == val)<<2 |
			b2u64(word>>21&mask == val)<<3 |
			b2u64(word>>28&mask == val)<<4 |
			b2u64(word>>35&mask == val)<<5 |
			b2u64(word>>42&mask == val)<<6 |
			b2u64(word>>49&mask == val)<<7
	}
	return 8, bits
}

func cmp_eq_9(word, val uint64) (int, uint64) {
	var bits uint64
	mask := uint64(0xFF)
	if val <= mask {
		bits = b2u64(word&mask == val) |
			b2u64(word>>8&mask == val)<<1 |
			b2u64(word>>16&mask == val)<<2 |
			b2u64(word>>24&mask == val)<<3 |
			b2u64(word>>32&mask == val)<<4 |
			b2u64(word>>40&mask == val)<<5 |
			b2u64(word>>48&mask == val)<<6
	}
	return 7, bits
}

func cmp_eq_10(word, val uint64) (int, uint64) {
	var bits uint64
	mask := uint64(0x3FF)
	if val <= mask {
		bits = b2u64(word&mask == val) |
			b2u64(word>>10&mask == val)<<1 |
			b2u64(word>>20&mask == val)<<2 |
			b2u64(word>>30&mask == val)<<3 |
			b2u64(word>>40&mask == val)<<4 |
			b2u64(word>>50&mask == val)<<5
	}
	return 6, bits
}

func cmp_eq_11(word, val uint64) (int, uint64) {
	var bits uint64
	mask := uint64(0xFFF)
	if val <= mask {
		bits = b2u64(word&mask == val) |
			b2u64(word>>12&mask == val)<<1 |
			b2u64(word>>24&mask == val)<<2 |
			b2u64(word>>36&mask == val)<<3 |
			b2u64(word>>48&mask == val)<<4
	}
	return 5, bits
}

func cmp_eq_12(word, val uint64) (int, uint64) {
	var bits uint64
	mask := uint64(0x7FFF)
	if val <= mask {
		bits = b2u64(word&mask == val) |
			b2u64(word>>15&mask == val)<<1 |
			b2u64(word>>30&mask == val)<<2 |
			b2u64(word>>45&mask == val)<<3
	}
	return 4, bits
}

func cmp_eq_13(word, val uint64) (int, uint64) {
	var bits uint64
	mask := uint64(0xFFFFF)
	if val <= mask {
		bits = b2u64(word&mask == val) |
			b2u64(word>>20&mask == val)<<1 |
			b2u64(word>>40&mask == val)<<2
	}
	return 3, bits
}

func cmp_eq_14(word, val uint64) (int, uint64) {
	var bits uint64
	mask := uint64(0x3FFFFFFF)
	if val <= mask {
		bits = b2u64(word&mask == val) |
			b2u64(word>>30&mask == val)<<1
	}
	return 2, bits
}

func cmp_eq_15(word, val uint64) (int, uint64) {
	return 1, b2u64(word&0x0FFFFFFFFFFFFFFF == val)
}
