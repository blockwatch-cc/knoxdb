// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

var cmp_gt [16]cmpFunc = [16]cmpFunc{
	cmp_gt_0, cmp_gt_1, cmp_gt_2, cmp_gt_3, cmp_gt_4, cmp_gt_5, cmp_gt_6, cmp_gt_7,
	cmp_gt_8, cmp_gt_9, cmp_gt_10, cmp_gt_11, cmp_gt_12, cmp_gt_13, cmp_gt_14, cmp_gt_15,
}

func cmp_gt_0(_, val uint64) (int, uint64) {
	if val == 0 {
		return 240, 1
	}
	return 240, 0
}

func cmp_gt_1(_, val uint64) (int, uint64) {
	if val == 0 {
		return 120, 1
	}
	return 120, 0
}

func cmp_gt_2(word, val uint64) (int, uint64) {
	var bits uint64
	mask := uint64(0x1)
	if val < mask {
		bits = b2u64(word&mask > val) << 59
		bits |= b2u64(word>>1&mask > val) << 58
		bits |= b2u64(word>>2&mask > val) << 57
		bits |= b2u64(word>>3&mask > val) << 56
		bits |= b2u64(word>>4&mask > val) << 55
		bits |= b2u64(word>>5&mask > val) << 54
		bits |= b2u64(word>>6&mask > val) << 53
		bits |= b2u64(word>>7&mask > val) << 52
		bits |= b2u64(word>>8&mask > val) << 51
		bits |= b2u64(word>>9&mask > val) << 50
		bits |= b2u64(word>>10&mask > val) << 49
		bits |= b2u64(word>>11&mask > val) << 48
		bits |= b2u64(word>>12&mask > val) << 47
		bits |= b2u64(word>>13&mask > val) << 46
		bits |= b2u64(word>>14&mask > val) << 45
		bits |= b2u64(word>>15&mask > val) << 44
		bits |= b2u64(word>>16&mask > val) << 43
		bits |= b2u64(word>>17&mask > val) << 42
		bits |= b2u64(word>>18&mask > val) << 41
		bits |= b2u64(word>>19&mask > val) << 40
		bits |= b2u64(word>>20&mask > val) << 39
		bits |= b2u64(word>>21&mask > val) << 38
		bits |= b2u64(word>>22&mask > val) << 37
		bits |= b2u64(word>>23&mask > val) << 36
		bits |= b2u64(word>>24&mask > val) << 35
		bits |= b2u64(word>>25&mask > val) << 34
		bits |= b2u64(word>>26&mask > val) << 33
		bits |= b2u64(word>>27&mask > val) << 32
		bits |= b2u64(word>>28&mask > val) << 31
		bits |= b2u64(word>>29&mask > val) << 30
		bits |= b2u64(word>>30&mask > val) << 29
		bits |= b2u64(word>>31&mask > val) << 28
		bits |= b2u64(word>>32&mask > val) << 27
		bits |= b2u64(word>>33&mask > val) << 26
		bits |= b2u64(word>>34&mask > val) << 25
		bits |= b2u64(word>>35&mask > val) << 24
		bits |= b2u64(word>>36&mask > val) << 23
		bits |= b2u64(word>>37&mask > val) << 22
		bits |= b2u64(word>>38&mask > val) << 21
		bits |= b2u64(word>>39&mask > val) << 20
		bits |= b2u64(word>>40&mask > val) << 19
		bits |= b2u64(word>>41&mask > val) << 18
		bits |= b2u64(word>>42&mask > val) << 17
		bits |= b2u64(word>>43&mask > val) << 16
		bits |= b2u64(word>>44&mask > val) << 15
		bits |= b2u64(word>>45&mask > val) << 14
		bits |= b2u64(word>>46&mask > val) << 13
		bits |= b2u64(word>>47&mask > val) << 12
		bits |= b2u64(word>>48&mask > val) << 11
		bits |= b2u64(word>>49&mask > val) << 10
		bits |= b2u64(word>>50&mask > val) << 9
		bits |= b2u64(word>>51&mask > val) << 8
		bits |= b2u64(word>>52&mask > val) << 7
		bits |= b2u64(word>>53&mask > val) << 6
		bits |= b2u64(word>>54&mask > val) << 5
		bits |= b2u64(word>>55&mask > val) << 4
		bits |= b2u64(word>>56&mask > val) << 3
		bits |= b2u64(word>>57&mask > val) << 2
		bits |= b2u64(word>>58&mask > val) << 1
		bits |= b2u64(word>>59&mask > val)
	}
	return 60, bits
}

func cmp_gt_3(word, val uint64) (int, uint64) {
	var bits uint64
	mask := uint64(0x3)
	if val < mask {
		bits = b2u64(word&mask > val) << 29
		bits |= b2u64(word>>2&mask > val) << 28
		bits |= b2u64(word>>4&mask > val) << 27
		bits |= b2u64(word>>6&mask > val) << 26
		bits |= b2u64(word>>8&mask > val) << 25
		bits |= b2u64(word>>10&mask > val) << 24
		bits |= b2u64(word>>12&mask > val) << 23
		bits |= b2u64(word>>14&mask > val) << 22
		bits |= b2u64(word>>16&mask > val) << 21
		bits |= b2u64(word>>18&mask > val) << 20
		bits |= b2u64(word>>20&mask > val) << 19
		bits |= b2u64(word>>22&mask > val) << 18
		bits |= b2u64(word>>24&mask > val) << 17
		bits |= b2u64(word>>26&mask > val) << 16
		bits |= b2u64(word>>28&mask > val) << 15
		bits |= b2u64(word>>30&mask > val) << 14
		bits |= b2u64(word>>32&mask > val) << 13
		bits |= b2u64(word>>34&mask > val) << 12
		bits |= b2u64(word>>36&mask > val) << 11
		bits |= b2u64(word>>38&mask > val) << 10
		bits |= b2u64(word>>40&mask > val) << 9
		bits |= b2u64(word>>42&mask > val) << 8
		bits |= b2u64(word>>44&mask > val) << 7
		bits |= b2u64(word>>46&mask > val) << 6
		bits |= b2u64(word>>48&mask > val) << 5
		bits |= b2u64(word>>50&mask > val) << 4
		bits |= b2u64(word>>52&mask > val) << 3
		bits |= b2u64(word>>54&mask > val) << 2
		bits |= b2u64(word>>56&mask > val) << 1
		bits |= b2u64(word>>58&mask > val)
	}
	return 30, bits
}

func cmp_gt_4(word, val uint64) (int, uint64) {
	var bits uint64
	mask := uint64(0x7)
	if val < mask {
		bits = b2u64(word&mask > val) << 19
		bits |= b2u64(word>>3&mask > val) << 18
		bits |= b2u64(word>>6&mask > val) << 17
		bits |= b2u64(word>>9&mask > val) << 16
		bits |= b2u64(word>>12&mask > val) << 15
		bits |= b2u64(word>>15&mask > val) << 14
		bits |= b2u64(word>>18&mask > val) << 13
		bits |= b2u64(word>>21&mask > val) << 12
		bits |= b2u64(word>>24&mask > val) << 11
		bits |= b2u64(word>>27&mask > val) << 10
		bits |= b2u64(word>>30&mask > val) << 9
		bits |= b2u64(word>>33&mask > val) << 8
		bits |= b2u64(word>>36&mask > val) << 7
		bits |= b2u64(word>>39&mask > val) << 6
		bits |= b2u64(word>>42&mask > val) << 5
		bits |= b2u64(word>>45&mask > val) << 4
		bits |= b2u64(word>>48&mask > val) << 3
		bits |= b2u64(word>>51&mask > val) << 2
		bits |= b2u64(word>>54&mask > val) << 1
		bits |= b2u64(word>>57&mask > val)
	}
	return 20, bits
}

func cmp_gt_5(word, val uint64) (int, uint64) {
	var bits uint64
	mask := uint64(0xF)
	if val < mask {
		bits = b2u64(word&mask > val) << 14
		bits |= b2u64(word>>4&mask > val) << 13
		bits |= b2u64(word>>8&mask > val) << 12
		bits |= b2u64(word>>12&mask > val) << 11
		bits |= b2u64(word>>16&mask > val) << 10
		bits |= b2u64(word>>20&mask > val) << 9
		bits |= b2u64(word>>24&mask > val) << 8
		bits |= b2u64(word>>28&mask > val) << 7
		bits |= b2u64(word>>32&mask > val) << 6
		bits |= b2u64(word>>36&mask > val) << 5
		bits |= b2u64(word>>40&mask > val) << 4
		bits |= b2u64(word>>44&mask > val) << 3
		bits |= b2u64(word>>48&mask > val) << 2
		bits |= b2u64(word>>52&mask > val) << 1
		bits |= b2u64(word>>56&mask > val)
	}
	return 15, bits
}

func cmp_gt_6(word, val uint64) (int, uint64) {
	var bits uint64
	mask := uint64(0x1F)
	if val < mask {
		bits = b2u64(word&mask > val) << 11
		bits |= b2u64(word>>5&mask > val) << 10
		bits |= b2u64(word>>10&mask > val) << 9
		bits |= b2u64(word>>15&mask > val) << 8
		bits |= b2u64(word>>20&mask > val) << 7
		bits |= b2u64(word>>25&mask > val) << 6
		bits |= b2u64(word>>30&mask > val) << 5
		bits |= b2u64(word>>35&mask > val) << 4
		bits |= b2u64(word>>40&mask > val) << 3
		bits |= b2u64(word>>45&mask > val) << 2
		bits |= b2u64(word>>50&mask > val) << 1
		bits |= b2u64(word>>55&mask > val)
	}
	return 12, bits
}

func cmp_gt_7(word, val uint64) (int, uint64) {
	var bits uint64
	mask := uint64(0x3F)
	if val < mask {
		bits = b2u64(word&mask > val) << 9
		bits |= b2u64(word>>6&mask > val) << 8
		bits |= b2u64(word>>12&mask > val) << 7
		bits |= b2u64(word>>18&mask > val) << 6
		bits |= b2u64(word>>24&mask > val) << 5
		bits |= b2u64(word>>30&mask > val) << 4
		bits |= b2u64(word>>36&mask > val) << 3
		bits |= b2u64(word>>42&mask > val) << 2
		bits |= b2u64(word>>48&mask > val) << 1
		bits |= b2u64(word>>54&mask > val)
	}
	return 10, bits
}

func cmp_gt_8(word, val uint64) (int, uint64) {
	var bits uint64
	mask := uint64(0x7F)
	if val < mask {
		bits = b2u64(word&mask > val) << 7
		bits |= b2u64(word>>7&mask > val) << 6
		bits |= b2u64(word>>14&mask > val) << 5
		bits |= b2u64(word>>21&mask > val) << 4
		bits |= b2u64(word>>28&mask > val) << 3
		bits |= b2u64(word>>35&mask > val) << 2
		bits |= b2u64(word>>42&mask > val) << 1
		bits |= b2u64(word>>49&mask > val)
	}
	return 8, bits
}

func cmp_gt_9(word, val uint64) (int, uint64) {
	var bits uint64
	mask := uint64(0xFF)
	if val < mask {
		bits = b2u64(word&mask > val) << 6
		bits |= b2u64(word>>8&mask > val) << 5
		bits |= b2u64(word>>16&mask > val) << 4
		bits |= b2u64(word>>24&mask > val) << 3
		bits |= b2u64(word>>32&mask > val) << 2
		bits |= b2u64(word>>40&mask > val) << 1
		bits |= b2u64(word>>48&mask > val)
	}
	return 7, bits
}

func cmp_gt_10(word, val uint64) (int, uint64) {
	var bits uint64
	mask := uint64(0x3FF)
	if val < mask {
		bits = b2u64(word&mask > val) << 5
		bits |= b2u64(word>>10&mask > val) << 4
		bits |= b2u64(word>>20&mask > val) << 3
		bits |= b2u64(word>>30&mask > val) << 2
		bits |= b2u64(word>>40&mask > val) << 1
		bits |= b2u64(word>>50&mask > val)
	}
	return 6, bits
}

func cmp_gt_11(word, val uint64) (int, uint64) {
	var bits uint64
	mask := uint64(0xFFF)
	if val < mask {
		bits = b2u64(word&mask > val) << 4
		bits |= b2u64(word>>12&mask > val) << 3
		bits |= b2u64(word>>24&mask > val) << 2
		bits |= b2u64(word>>36&mask > val) << 1
		bits |= b2u64(word>>48&mask > val)
	}
	return 5, bits
}

func cmp_gt_12(word, val uint64) (int, uint64) {
	var bits uint64
	mask := uint64(0x7FFF)
	if val < mask {
		bits = b2u64(word&mask > val) << 3
		bits |= b2u64(word>>15&mask > val) << 2
		bits |= b2u64(word>>30&mask > val) << 1
		bits |= b2u64(word>>45&mask > val)
	}
	return 4, bits
}

func cmp_gt_13(word, val uint64) (int, uint64) {
	var bits uint64
	mask := uint64(0xFFFFF)
	if val < mask {
		bits = b2u64(word&mask > val) << 2
		bits |= b2u64(word>>20&mask > val) << 1
		bits |= b2u64(word>>20&mask > val)
	}
	return 3, bits
}

func cmp_gt_14(word, val uint64) (int, uint64) {
	var bits uint64
	mask := uint64(0x3FFFFFFF)
	if val < mask {
		bits = b2u64(word&mask > val) << 1
		bits |= b2u64(word>>30&mask > val)
	}
	return 2, bits
}

func cmp_gt_15(word, val uint64) (int, uint64) {
	return 1, b2u64(word&0x0FFFFFFFFFFFFFFF > val)
}
