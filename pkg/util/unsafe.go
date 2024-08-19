// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package util

import (
	"unsafe"
)

// go 1.20 versions
func UnsafeGetBytes(s string) []byte {
	if s == "" {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

func UnsafeGetString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}

func Int64AsUint64Slice(src []int64) []uint64 {
	return *(*[]uint64)(unsafe.Pointer(&src))
}

func Int32AsUint32Slice(src []int32) []uint32 {
	return *(*[]uint32)(unsafe.Pointer(&src))
}

func Int16AsUint16Slice(src []int16) []uint16 {
	return *(*[]uint16)(unsafe.Pointer(&src))
}

func Int8AsUint8Slice(src []int8) []uint8 {
	return *(*[]uint8)(unsafe.Pointer(&src))
}

func Uint64AsInt64Slice(src []uint64) []int64 {
	return *(*[]int64)(unsafe.Pointer(&src))
}

func Uint32AsInt32Slice(src []uint32) []int32 {
	return *(*[]int32)(unsafe.Pointer(&src))
}

func Uint16AsInt16Slice(src []uint16) []int16 {
	return *(*[]int16)(unsafe.Pointer(&src))
}

func Uint8AsInt8Slice(src []uint8) []int8 {
	return *(*[]int8)(unsafe.Pointer(&src))
}

func Uint64SliceAsByteSlice(x []uint64) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(x))), len(x)*8)
}
