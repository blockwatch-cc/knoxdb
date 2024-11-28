// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package generic

// func deltaDecodeInt64(src []int64) {
// 	var prev int64
// 	for i := 0; i < len(src); i++ {
// 		prev += src[i]
// 		src[i] = prev
// 	}
// }

// func deltaDecodeInt32(src []int32) {
// 	var prev int32
// 	for i := 0; i < len(src); i++ {
// 		prev += src[i]
// 		src[i] = prev
// 	}
// }

// calculate prefix sum
func ZzDeltaDecodeInt64(src []int64) {
	var prev int64
	for i, v := range src {
		prev += ZigZagDecodeUint64(uint64(v))
		src[i] = prev
	}
}

// calculate prefix sum
func ZzDeltaDecodeInt32(src []int32) {
	var prev int32
	for i, v := range src {
		prev += ZigZagDecodeUint32(uint32(v))
		src[i] = prev
	}
}

// calculate prefix sum
func ZzDeltaDecodeInt16(src []int16) {
	var prev int16
	for i, v := range src {
		prev += ZigZagDecodeUint16(uint16(v))
		src[i] = prev
	}
}

// calculate prefix sum
func ZzDeltaDecodeInt8(src []int8) {
	var prev int8
	for i, v := range src {
		prev += ZigZagDecodeUint8(uint8(v))
		src[i] = prev
	}
}

// calculate prefix sum
func ZzDeltaDecodeUint64(src []uint64) {
	var prev uint64
	for i, v := range src {
		prev += uint64(ZigZagDecodeUint64(v))
		src[i] = prev
	}
}

// Compute the prefix sum and scale the deltas back up
func DeltaDecodeTime(buf []uint64, mod uint64) {
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
func ZzDeltaDecodeTime(buf []uint64, mod uint64) {
	var prev uint64
	if mod > 1 {
		for i := 0; i < len(buf); i++ {
			prev += uint64(ZigZagDecodeUint64(buf[i]))
			buf[i] = prev * mod
		}
	} else {
		for i := 0; i < len(buf); i++ {
			prev += uint64(ZigZagDecodeUint64(buf[i]))
			buf[i] = prev
		}
	}
}

// func zzDecodeUint64(src []uint64) {
// 	for i := range src {
// 		src[i] = uint64(ZigZagDecode(src[i]))
// 	}
// }

// func zzDecodeInt64(src []int64) {
// 	for i := range src {
// 		src[i] = ZigZagDecode(uint64(src[i]))
// 	}
// }

// func delta8DecodeUint64(src []uint64) {
// 	for i := 8; i < len(src); i++ {
// 		src[i] += src[i-8]
// 	}
// }

// func delta8EncodeUint64(src []uint64) uint64 {
// 	maxdelta := uint64(0)
// 	for i := len(src) - 1; i > 7; i-- {
// 		src[i] = src[i] - src[i-8]
// 		maxdelta |= src[i]
// 	}
// 	return maxdelta
// }
