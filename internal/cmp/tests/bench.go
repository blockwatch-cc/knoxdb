// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package tests

import (
	"strconv"
	"testing"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/types"
)

type BenchmarkSize struct {
	Name string
	N    int
}

var BenchmarkSizes = []BenchmarkSize{
	{"1K", 1 * 1024},
	{"16K", 16 * 1024},
	{"64K", 64 * 1024},
}

var BenchmarksMasks = [][]byte{
	maskAll,
	{0x00, 0x11},
}

type (
	CmpFunc[T types.Number]  func([]T, T, []byte) int64
	CmpFunc2[T types.Number] func([]T, T, T, []byte) int64

	CmpMaskFunc[T types.Number]  func([]T, T, []byte, []byte) int64
	CmpMaskFunc2[T types.Number] func([]T, T, T, []byte, []byte) int64
)

func BenchCases[T types.Number](b *testing.B, fn CmpFunc[T]) {
	b.Helper()
	for _, n := range BenchmarkSizes {
		a := randSlice[T](n.N)
		bits := MakeBitsPoison(n.N)
		b.Run(n.Name, func(b *testing.B) {
			b.SetBytes(int64(n.N * int(unsafe.Sizeof(T(0)))))
			for i := 0; i < b.N; i++ {
				fn(a, 127, bits)
			}
		})
	}
}

func BenchCases2[T types.Number](b *testing.B, fn CmpFunc2[T]) {
	b.Helper()
	for _, n := range BenchmarkSizes {
		a := randSlice[T](n.N)
		bits := MakeBitsPoison(n.N)
		b.Run(n.Name, func(b *testing.B) {
			b.SetBytes(int64(n.N * int(unsafe.Sizeof(T(0)))))
			for i := 0; i < b.N; i++ {
				fn(a, 5, 127, bits)
			}
		})
	}
}

func BenchMaskCases[T types.Number](b *testing.B, fn CmpMaskFunc[T]) {
	b.Helper()
	for _, n := range BenchmarkSizes {
		for i, m := range BenchmarksMasks {
			a := randSlice[T](n.N)
			bits, mask := MakeBitsAndMaskPoison(n.N, m)
			b.Run(n.Name+"_mask_"+strconv.Itoa(i), func(b *testing.B) {
				b.SetBytes(int64(n.N * int(unsafe.Sizeof(T(0)))))
				for i := 0; i < b.N; i++ {
					fn(a, 127, bits, mask)
				}
			})
		}
	}
}

func BenchMaskCases2[T types.Number](b *testing.B, fn CmpMaskFunc2[T]) {
	b.Helper()
	for _, n := range BenchmarkSizes {
		for i, m := range BenchmarksMasks {
			a := randSlice[T](n.N)
			bits, mask := MakeBitsAndMaskPoison(n.N, m)
			b.Run(n.Name+"_mask_"+strconv.Itoa(i), func(b *testing.B) {
				b.SetBytes(int64(n.N * int(unsafe.Sizeof(T(0)))))
				for i := 0; i < b.N; i++ {
					fn(a, 5, 127, bits, mask)
				}
			})
		}
	}
}
