// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package compress

// calculate prefix sum
func zzDeltaDecodeInt64Generic(data []int64) {
	data[0] = ZigZagDecode(uint64(data[0]))
	prev := data[0]
	for i := 1; i < len(data); i++ {
		prev += ZigZagDecode(uint64(data[i]))
		data[i] = prev
	}
}

// calculate prefix sum
func zzDeltaDecodeUint64Generic(data []uint64) {
	data[0] = uint64(ZigZagDecode(data[0]))
	prev := data[0]
	for i := 1; i < len(data); i++ {
		prev += uint64(ZigZagDecode(data[i]))
		data[i] = prev
	}
}

func zzDecodeUint64Generic(data []uint64) {
	for i := range data {
		data[i] = uint64(ZigZagDecode(data[i]))
	}
}

func zzDecodeInt64Generic(data []int64) {
	for i := range data {
		data[i] = ZigZagDecode(uint64(data[i]))
	}
}

func delta8DecodeUint64Generic(data []uint64) {
	for i := 8; i < len(data); i++ {
		data[i] += data[i-8]
	}
}

func delta8EncodeUint64Generic(data []uint64) uint64 {
	maxdelta := uint64(0)
	for i := len(data) - 1; i > 7; i-- {
		data[i] = data[i] - data[i-8]
		maxdelta |= data[i]
	}
	return maxdelta
}
