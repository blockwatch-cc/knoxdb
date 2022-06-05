// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package compress

func zzDeltaEncodeUint8Generic(data []uint8) uint8 {
	if len(data) == 0 {
		return 0
	}
	var maxdelta uint8
	for i := len(data) - 1; i > 0; i-- {
		data[i] = data[i] - data[i-1]
		data[i] = ZigZagEncodeInt8(int8(data[i]))
		if data[i] > maxdelta {
			maxdelta = data[i]
		}
	}
	data[0] = ZigZagEncodeInt8(int8(data[0]))
	return maxdelta
}

func zzDeltaEncodeUint16Generic(data []uint16) uint16 {
	if len(data) == 0 {
		return 0
	}
	var maxdelta uint16
	for i := len(data) - 1; i > 0; i-- {
		data[i] = data[i] - data[i-1]
		data[i] = ZigZagEncodeInt16(int16(data[i]))
		if data[i] > maxdelta {
			maxdelta = data[i]
		}
	}
	data[0] = ZigZagEncodeInt16(int16(data[0]))
	return maxdelta
}

func zzDeltaEncodeUint32Generic(data []uint32) uint32 {
	if len(data) == 0 {
		return 0
	}
	var maxdelta uint32
	for i := len(data) - 1; i > 0; i-- {
		data[i] = data[i] - data[i-1]
		data[i] = ZigZagEncodeInt32(int32(data[i]))
		if data[i] > maxdelta {
			maxdelta = data[i]
		}
	}
	data[0] = ZigZagEncodeInt32(int32(data[0]))
	return maxdelta
}

func zzDeltaEncodeUint64Generic(data []uint64) uint64 {
	if len(data) == 0 {
		return 0
	}
	var maxdelta uint64
	for i := len(data) - 1; i > 0; i-- {
		data[i] = data[i] - data[i-1]
		data[i] = ZigZagEncodeInt64(int64(data[i]))
		if data[i] > maxdelta {
			maxdelta = data[i]
		}
	}
	data[0] = ZigZagEncodeInt64(int64(data[0]))
	return maxdelta
}

// calculate prefix sum
func zzDeltaDecodeInt64Generic(data []int64) {
	if (len(data)) == 0 {
		return
	}
	data[0] = ZigZagDecode(uint64(data[0]))
	prev := data[0]
	for i := 1; i < len(data); i++ {
		prev += ZigZagDecode(uint64(data[i]))
		data[i] = prev
	}
}

func deltaDecodeInt64Generic(data []int64) {
	if len(data) == 0 {
		return
	}
	prev := data[0]
	for i := 1; i < len(data); i++ {
		prev += data[i]
		data[i] = prev
	}
}

// calculate prefix sum
func zzDeltaDecodeInt32Generic(data []int32) {
	if len(data) == 0 {
		return
	}
	data[0] = ZigZagDecodeUint32(uint32(data[0]))
	prev := data[0]
	for i := 1; i < len(data); i++ {
		prev += ZigZagDecodeUint32(uint32(data[i]))
		data[i] = prev
	}
}

// calculate prefix sum
func zzDeltaDecodeInt16Generic(data []int16) {
	if len(data) == 0 {
		return
	}
	data[0] = ZigZagDecodeUint16(uint16(data[0]))
	prev := data[0]
	for i := 1; i < len(data); i++ {
		prev += ZigZagDecodeUint16(uint16(data[i]))
		data[i] = prev
	}
}

// calculate prefix sum
func zzDeltaDecodeInt8Generic(data []int8) {
	if len(data) == 0 {
		return
	}
	data[0] = ZigZagDecodeUint8(uint8(data[0]))
	prev := data[0]
	for i := 1; i < len(data); i++ {
		prev += ZigZagDecodeUint8(uint8(data[i]))
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
