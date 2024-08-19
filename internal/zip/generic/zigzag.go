// Copyright (c) 2018-2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

// ZigZagEncodeInt64 converts a int64 to a uint64 by zig zagging negative and positive values
// across even and odd numbers.  Eg. [0,-1,1,-2] becomes [0, 1, 2, 3].
func ZigZagEncodeInt64(x int64) uint64 {
	return uint64(uint64(x<<1) ^ uint64((int64(x) >> 63)))
}

func ZigZagEncodeInt32(x int32) uint32 {
	return uint32(uint32(x<<1) ^ uint32((int32(x) >> 31)))
}

func ZigZagEncodeInt16(x int16) uint16 {
	return uint16(uint16(x<<1) ^ uint16((int16(x) >> 15)))
}

func ZigZagEncodeInt8(x int8) uint8 {
	return uint8(uint8(x<<1) ^ uint8((int8(x) >> 7)))
}

// ZigZagDecodeUint64 converts a previously zigzag encoded uint64 back to a int64.
func ZigZagDecodeUint64(v uint64) int64 {
	return int64((v >> 1) ^ uint64((int64(v&1)<<63)>>63))
}

// ZigZagDecode converts a previously zigzag encoded uint64 back to a int64.
func ZigZagDecodeUint32(v uint32) int32 {
	return int32((v >> 1) ^ uint32((int32(v&1)<<31)>>31))
}

// ZigZagDecode converts a previously zigzag encoded uint64 back to a int64.
func ZigZagDecodeUint16(v uint16) int16 {
	return int16((v >> 1) ^ uint16((int16(v&1)<<15)>>15))
}

// ZigZagDecode converts a previously zigzag encoded uint64 back to a int64.
func ZigZagDecodeUint8(v uint8) int8 {
	return int8((v >> 1) ^ uint8((int8(v&1)<<7)>>7))
}
