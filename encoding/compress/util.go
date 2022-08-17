// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package compress

import (
	"reflect"
	"unsafe"
)

const (
	// same as in block
	DefaultMaxPointsPerBlock = 1 << 16
)

// ZigZagEncode converts a int64 to a uint64 by zig zagging negative and positive values
// across even and odd numbers.  Eg. [0,-1,1,-2] becomes [0, 1, 2, 3].
func ZigZagEncode(x int64) uint64 {
	return ZigZagEncodeInt64(x)
}

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

func ZigZagDecode(v uint64) int64 {
	return ZigZagDecodeUint64(v)
}

// ZigZagDecode converts a previously zigzag encoded uint64 back to a int64.
func ZigZagDecodeUint64(v uint64) int64 {
	return int64((v >> 1) ^ uint64((int64(v&1)<<63)>>63))
	// return int64((v >> 1) ^ (-(v & 1)))
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

func uvarIntLen(n int) int {
	i := 0
	for n >= 0x80 {
		n >>= 7
		i++
	}
	return i + 1
}

func ReintepretInt64ToUint64Slice(src []int64) []uint64 {
	return *(*[]uint64)(unsafe.Pointer(&src))
}

func ReintepretInt32ToUint32Slice(src []int32) []uint32 {
	return *(*[]uint32)(unsafe.Pointer(&src))
}

func ReintepretInt16ToUint16Slice(src []int16) []uint16 {
	return *(*[]uint16)(unsafe.Pointer(&src))
}

func ReintepretInt8ToUint8Slice(src []int8) []uint8 {
	return *(*[]uint8)(unsafe.Pointer(&src))
}

func ReintepretUint64ToInt64Slice(src []uint64) []int64 {
	return *(*[]int64)(unsafe.Pointer(&src))
}

func ReintepretUint32ToInt32Slice(src []uint32) []int32 {
	return *(*[]int32)(unsafe.Pointer(&src))
}

func ReintepretUint16ToInt16Slice(src []uint16) []int16 {
	return *(*[]int16)(unsafe.Pointer(&src))
}

func ReintepretUint8ToInt8Slice(src []uint8) []int8 {
	return *(*[]int8)(unsafe.Pointer(&src))
}

func UnsafeGetBytes(s string) []byte {
	l := len(s)
	b := (*(*[]byte)(unsafe.Pointer((*reflect.StringHeader)(unsafe.Pointer(&s)))))
	if cap(b) < l {
		// copy
		return []byte(s)
	}
	return b[:l]
}

func UnsafeGetString(buf []byte) string {
	return *(*string)(unsafe.Pointer(&buf))
}

func MaxUint64(data []uint64) uint64 {
	var max uint64
	for _, v := range data {
		if v > max {
			max = v
		}
	}
	return max
}

func HasNegUint64(data []uint64) bool {
	for _, v := range data {
		if int64(v) < 0 {
			return true
		}
	}
	return false
}
