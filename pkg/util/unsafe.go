// Copyright (c) 2023-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package util

import (
	"unsafe"

	"golang.org/x/exp/constraints"
)

type Number interface {
	constraints.Integer | constraints.Float
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

func ReinterpretSlice[T, S constraints.Integer](t []T) []S {
	if unsafe.Sizeof(T(0)) == unsafe.Sizeof(S(0)) {
		return *(*[]S)(unsafe.Pointer(&t))
	}
	return nil
}

func ConvertSlice[T, S constraints.Integer](t []T) (s []S) {
	s = make([]S, len(t))
	for i, v := range t {
		s[i] = S(v)
	}
	return
}
