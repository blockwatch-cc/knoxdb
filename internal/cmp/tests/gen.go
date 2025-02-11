// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package tests

import (
	"strconv"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

type BenchmarkSize struct {
	Name string
	L    int
}

var BenchmarkSizes = []BenchmarkSize{
	// {"1K", 1 * 1024},
	// {"16K", 16 * 1024},
	// {"64K", 64 * 1024},
	{"128K", 128 * 1024},
}

var BenchmarksMasks = [][]byte{
	maskAll,
	{0x00, 0x11},
}

type Number interface {
	int64 | int32 | int16 | int8 | uint64 | uint32 | uint16 | uint8 | float64 | float32
}

type MatchTest[T Number] struct {
	Name   string
	Slice  []T
	Match  T
	Match2 T
	Result []byte
	Count  int64
}

// Test Drivers
func TestCases[T Number](t *testing.T, cases []MatchTest[T], fn func([]T, T, []byte) int64) {
	t.Helper()
	for _, c := range cases {
		bits, _ := MakeBitsAndMaskPoisonTail(len(c.Slice), 32, maskAll)
		cnt := fn(c.Slice, c.Match, bits)
		assert.Len(t, bits, len(c.Result), c.Name)
		assert.Equal(t, c.Count, cnt, "%s: unexpected result bit count", c.Name)
		assert.Equal(t, c.Result, bits, "%s: unexpected result", c.Name)
		assert.Equal(t, MakePoison(32), bits[len(bits):len(bits)+32], "%s: boundary violation", c.Name)
	}
}

func TestCases2[T Number](t *testing.T, cases []MatchTest[T], fn func([]T, T, T, []byte) int64) {
	t.Helper()
	for _, c := range cases {
		bits, _ := MakeBitsAndMaskPoisonTail(len(c.Slice), 32, maskAll)
		cnt := fn(c.Slice, c.Match, c.Match2, bits)
		assert.Len(t, bits, len(c.Result), c.Name)
		assert.Equal(t, c.Count, cnt, "%s: unexpected result bit count", c.Name)
		assert.Equal(t, c.Result, bits, "%s: unexpected result", c.Name)
		assert.Equal(t, MakePoison(32), bits[len(bits):len(bits)+32], "%s: boundary violation", c.Name)
	}
}

func BenchCases[T Number](b *testing.B, fn func([]T, T, []byte) int64) {
	b.Helper()
	for _, n := range BenchmarkSizes {
		for i, m := range BenchmarksMasks {
			a := randSlice[T](n.L)
			bits, _ := MakeBitsAndMaskPoison(n.L, m)
			b.Run(n.Name+"_mask_"+strconv.Itoa(i), func(b *testing.B) {
				var t T
				b.SetBytes(int64(n.L * int(unsafe.Sizeof(t))))
				for i := 0; i < b.N; i++ {
					fn(a, 127, bits)
				}
			})
		}
	}
}

func BenchCases2[T Number](b *testing.B, fn func([]T, T, T, []byte) int64) {
	b.Helper()
	for _, n := range BenchmarkSizes {
		for i, m := range BenchmarksMasks {
			a := randSlice[T](n.L)
			bits, _ := MakeBitsAndMaskPoison(n.L, m)
			b.Run(n.Name+"_mask_"+strconv.Itoa(i), func(b *testing.B) {
				var t T
				b.SetBytes(int64(n.L * int(unsafe.Sizeof(t))))
				for i := 0; i < b.N; i++ {
					fn(a, 5, 127, bits)
				}
			})
		}
	}
}
