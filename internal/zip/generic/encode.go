// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package generic

func ZzDeltaEncodeUint64(dst, src []uint64) (max uint64) {
	if len(src) == 0 {
		return 0
	}
	if dst == nil {
		dst = src
	}
	for i := len(src) - 1; i > 0; i-- {
		dst[i] = ZigZagEncodeInt64(int64(src[i] - src[i-1]))
		if dst[i] > max {
			max = dst[i]
		}
	}
	dst[0] = ZigZagEncodeInt64(int64(src[0]))
	return
}

func ZzDeltaEncodeUint32(dst []uint64, src []uint32) (max uint64) {
	if len(src) == 0 {
		return 0
	}
	for i := len(src) - 1; i > 0; i-- {
		dst[i] = uint64(ZigZagEncodeInt32(int32(src[i] - src[i-1])))
		if dst[i] > max {
			max = dst[i]
		}
	}
	dst[0] = uint64(ZigZagEncodeInt32(int32(src[0])))
	return
}

func ZzDeltaEncodeUint16(dst []uint64, src []uint16) (max uint64) {
	if len(src) == 0 {
		return 0
	}
	for i := len(src) - 1; i > 0; i-- {
		dst[i] = uint64(ZigZagEncodeInt16(int16(src[i] - src[i-1])))
		if dst[i] > max {
			max = dst[i]
		}
	}
	dst[0] = uint64(ZigZagEncodeInt16(int16(src[0])))
	return
}

func ZzDeltaEncodeUint8(dst []uint64, src []uint8) (max uint64) {
	if len(src) == 0 {
		return 0
	}
	for i := len(src) - 1; i > 0; i-- {
		dst[i] = uint64(ZigZagEncodeInt8(int8(src[i] - src[i-1])))
		if dst[i] > max {
			max = dst[i]
		}
	}
	dst[0] = uint64(ZigZagEncodeInt8(int8(src[0])))
	return
}
