// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package tests

import (
	"fmt"
	"testing"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/internal/types"
)

type BenchmarkMask struct {
	Name    string
	Pattern []byte
}

var BenchmarkMasks = []BenchmarkMask{
	{"0xFFFF", maskAll},
	{"0x0011", []byte{0x00, 0x11}},
}

type (
	CmpFunc[T types.Number]  func([]T, T, []byte) int64
	CmpFunc2[T types.Number] func([]T, T, T, []byte) int64

	CmpMaskFunc[T types.Number]  func([]T, T, []byte, []byte) int64
	CmpMaskFunc2[T types.Number] func([]T, T, T, []byte, []byte) int64
)

func BenchCases[T types.Number](b *testing.B, fn CmpFunc[T]) {
	for _, n := range tests.BenchmarkSizes {
		a := tests.GenRnd[T](n.N)
		bits := MakeBitsPoison(n.N)
		b.Run(fmt.Sprintf("%T/%s", T(0), n.Name), func(b *testing.B) {
			b.SetBytes(int64(n.N * int(unsafe.Sizeof(T(0)))))
			for range b.N {
				fn(a, 127, bits)
			}
		})
	}
}

func BenchCases2[T types.Number](b *testing.B, fn CmpFunc2[T]) {
	for _, n := range tests.BenchmarkSizes {
		a := tests.GenRnd[T](n.N)
		bits := MakeBitsPoison(n.N)
		b.Run(fmt.Sprintf("%T/%s", T(0), n.Name), func(b *testing.B) {
			b.SetBytes(int64(n.N * int(unsafe.Sizeof(T(0)))))
			for range b.N {
				fn(a, 5, 127, bits)
			}
		})
	}
}

func BenchMaskCases[T types.Number](b *testing.B, fn CmpMaskFunc[T]) {
	for _, n := range tests.BenchmarkSizes {
		for _, m := range BenchmarkMasks {
			a := tests.GenRnd[T](n.N)
			bits, mask := MakeBitsAndMaskPoison(n.N, m.Pattern)
			b.Run(fmt.Sprintf("%T/%s/mask_%s", T(0), n.Name, m.Name), func(b *testing.B) {
				b.SetBytes(int64(n.N * int(unsafe.Sizeof(T(0)))))
				for range b.N {
					fn(a, 127, bits, mask)
				}
			})
		}
	}
}

func BenchMaskCases2[T types.Number](b *testing.B, fn CmpMaskFunc2[T]) {
	for _, n := range tests.BenchmarkSizes {
		for _, m := range BenchmarkMasks {
			a := tests.GenRnd[T](n.N)
			bits, mask := MakeBitsAndMaskPoison(n.N, m.Pattern)
			b.Run(fmt.Sprintf("%T/%s/mask_%s", T(0), n.Name, m.Name), func(b *testing.B) {
				b.SetBytes(int64(n.N * int(unsafe.Sizeof(T(0)))))
				for range b.N {
					fn(a, 5, 127, bits, mask)
				}
			})
		}
	}
}
