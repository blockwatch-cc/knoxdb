// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package arena

import (
	"errors"
	"reflect"
	"unsafe"
)

type FixedSizeType interface {
	int64 | int32 | int16 | int8 | uint64 | uint32 | uint16 | uint8 |
		float64 | float32
}

func SizeFor[T any]() int {
	var t T
	return int(unsafe.Sizeof(t))
}

func ToBytes[T FixedSizeType](s []T) []byte {
	return unsafe.Slice(
		(*byte)(unsafe.Pointer(unsafe.SliceData(s))),
		len(s)*SizeFor[T](),
	)
}

func FromBytes[T FixedSizeType](s []byte) []T {
	return unsafe.Slice(
		(*T)(unsafe.Pointer(unsafe.SliceData(s))),
		len(s)/SizeFor[T](),
	)
}

func ReinterpretSlice[T, S FixedSizeType](t []T) []S {
	if SizeFor[T]() == SizeFor[S]() {
		return *(*[]S)(unsafe.Pointer(&t))
	}
	panic(errors.New(
		"cannot reinterprete []" +
			reflect.TypeFor[T]().String() +
			" to []" +
			reflect.TypeFor[S]().String(),
	))
}

func ConvertSlice[T, S FixedSizeType](t []T) (s []S) {
	s = Alloc[S](len(t))[:len(t)]
	for i, v := range t {
		s[i] = S(v)
	}
	return
}
