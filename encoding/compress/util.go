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
	return uint64(uint64(x<<1) ^ uint64((int64(x) >> 63)))
}

// ZigZagDecode converts a previously zigzag encoded uint64 back to a int64.
func ZigZagDecode(v uint64) int64 {
	return int64((v >> 1) ^ uint64((int64(v&1)<<63)>>63))
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

func ReintepretUint64ToInt64Slice(src []uint64) []int64 {
	return *(*[]int64)(unsafe.Pointer(&src))
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
