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
	for _, c := range tests.BenchmarkSizes {
		a := tests.GenRnd[T](c.N)
		bits := MakeBitsPoison(c.N)
		b.Run(fmt.Sprintf("%T/%s", T(0), c.Name), func(b *testing.B) {
			b.SetBytes(int64(c.N * int(unsafe.Sizeof(T(0)))))
			for range b.N {
				fn(a, 127, bits)
			}
			b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
		})
	}
}

func BenchCases2[T types.Number](b *testing.B, fn CmpFunc2[T]) {
	for _, c := range tests.BenchmarkSizes {
		a := tests.GenRnd[T](c.N)
		bits := MakeBitsPoison(c.N)
		b.Run(fmt.Sprintf("%T/%s", T(0), c.Name), func(b *testing.B) {
			b.SetBytes(int64(c.N * int(unsafe.Sizeof(T(0)))))
			for range b.N {
				fn(a, 5, 127, bits)
			}
			b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
		})
	}
}

func BenchMaskCases[T types.Number](b *testing.B, fn CmpMaskFunc[T]) {
	for _, c := range tests.BenchmarkSizes {
		for _, m := range BenchmarkMasks {
			a := tests.GenRnd[T](c.N)
			bits, mask := MakeBitsAndMaskPoison(c.N, m.Pattern)
			b.Run(fmt.Sprintf("%T/%s/mask_%s", T(0), c.Name, m.Name), func(b *testing.B) {
				b.SetBytes(int64(c.N * int(unsafe.Sizeof(T(0)))))
				for range b.N {
					fn(a, 127, bits, mask)
				}
				b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
			})
		}
	}
}

func BenchMaskCases2[T types.Number](b *testing.B, fn CmpMaskFunc2[T]) {
	for _, c := range tests.BenchmarkSizes {
		for _, m := range BenchmarkMasks {
			a := tests.GenRnd[T](c.N)
			bits, mask := MakeBitsAndMaskPoison(c.N, m.Pattern)
			b.Run(fmt.Sprintf("%T/%s/mask_%s", T(0), c.Name, m.Name), func(b *testing.B) {
				b.SetBytes(int64(c.N * int(unsafe.Sizeof(T(0)))))
				for range b.N {
					fn(a, 5, 127, bits, mask)
				}
			})
			b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
		}
	}
}
