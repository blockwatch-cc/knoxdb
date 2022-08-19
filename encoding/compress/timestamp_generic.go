// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package compress

// Compute the prefix sum and scale the deltas back up
func deltaDecodeTimeGeneric(buf []uint64, mod uint64) {
	var prev uint64
	if mod > 1 {
		for i := 0; i < len(buf); i++ {
			prev += buf[i] * mod
			buf[i] = prev
		}
	} else {
		for i := 0; i < len(buf); i++ {
			prev += buf[i]
			buf[i] = prev
		}
	}
}

// Compute the prefix sum and scale the timestamps back up
func zzDeltaDecodeTimeGeneric(buf []uint64, mod uint64) {
	var prev uint64
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
