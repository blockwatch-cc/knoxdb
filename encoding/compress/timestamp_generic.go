// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package compress

// Compute the prefix sum and scale the deltas back up
func deltaScaleDecodeTimeGeneric(buf []uint64, mod uint64) {
	var last uint64
	if mod > 1 {
		for i := 0; i < len(buf); i++ {
			last += buf[i] * mod
			buf[i] = last
		}
	} else {
		for i := 0; i < len(buf); i++ {
			last += buf[i]
			buf[i] = last
		}
	}
}

// Compute the prefix sum and scale the timestamps back up
func deltaZzScaleDecodeTime(buf []uint64, mod uint64) {
	prev := uint64(0)
	if mod > 1 {
		for i := 0; i < len(buf); i++ {
			prev += uint64(ZigZagDecode(buf[i]))
			buf[i] = prev * mod
		}
	} else {
		for i := 0; i < len(buf); i++ {
			prev += uint64(ZigZagDecode(buf[i]))
			buf[i] = prev
		}
	}
}
