// Copyright (c) 2023-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package util

import (
	"errors"
	"reflect"
	"unsafe"
)

type Integer interface {
	int8 | int16 | int32 | int64 | uint8 | uint16 | uint32 | uint64
}

type Number interface {
	Integer | float32 | float64
}

func SizeOf[T Number]() int {
	return int(unsafe.Sizeof(T(0)))
}

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

func ToByteSlice[T Number](s []T) []byte {
	return unsafe.Slice(
		(*byte)(unsafe.Pointer(unsafe.SliceData(s))),
		len(s)*int(unsafe.Sizeof(T(0))),
	)
}

func FromByteSlice[T Number](s []byte) []T {
	return unsafe.Slice(
		(*T)(unsafe.Pointer(unsafe.SliceData(s))),
		len(s)/int(unsafe.Sizeof(T(0))),
	)
}

func ReinterpretSlice[T, S Number](t []T) []S {
	if unsafe.Sizeof(T(0)) == unsafe.Sizeof(S(0)) {
		return *(*[]S)(unsafe.Pointer(&t))
	}
	panic(errors.New(
		"cannot reinterprete []" +
			reflect.TypeOf(T(0)).String() +
			" to " +
			reflect.TypeOf(S(0)).String(),
	))
}

func ReinterpretValue[T Number, S Number](t T) S {
	if unsafe.Sizeof(T(0)) == unsafe.Sizeof(S(0)) {
		return *(*S)(unsafe.Pointer(&t))
	}
	return S(0)
}

func ConvertSlice[T, S Number](t []T) (s []S) {
	s = make([]S, len(t))
	for i, v := range t {
		s[i] = S(v)
	}
	return
}
