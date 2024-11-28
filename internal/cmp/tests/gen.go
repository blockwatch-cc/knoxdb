// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package tests

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

type BenchmarkSize struct {
	Name string
	L    int
}

var BenchmarkSizes = []BenchmarkSize{
	{"1K", 1 * 1024},
	{"16K", 16 * 1024},
	{"64K", 64 * 1024},
	{"128K", 128 * 1024},
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
		t.Run(c.Name, func(t *testing.T) {
			bits, _ := MakeBitsAndMaskPoisonTail(len(c.Slice), 32)
			cnt := fn(c.Slice, c.Match, bits)
			assert.Len(t, bits, len(c.Result))
			assert.Equal(t, c.Count, cnt, "unexpected result bit count")
			assert.Equal(t, c.Result, bits, "unexpected result")
			assert.Equal(t, MakePoison(32), bits[len(bits):len(bits)+32], "boundary violation")
		})
	}
}

func TestCases2[T Number](t *testing.T, cases []MatchTest[T], fn func([]T, T, T, []byte) int64) {
	t.Helper()
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			bits, _ := MakeBitsAndMaskPoisonTail(len(c.Slice), 32)
			cnt := fn(c.Slice, c.Match, c.Match2, bits)
			assert.Len(t, bits, len(c.Result))
			assert.Equal(t, c.Count, cnt, "unexpected result bit count")
			assert.Equal(t, c.Result, bits, "unexpected result")
			assert.Equal(t, MakePoison(32), bits[len(bits):len(bits)+32], "boundary violation")
		})
	}
}

func BenchCases[T Number](b *testing.B, fn func([]T, T, []byte) int64) {
	b.Helper()
	for _, n := range BenchmarkSizes {
		a := randSlice[T](n.L)
		bits, _ := MakeBitsAndMaskPoison(n.L)
		b.Run(n.Name, func(b *testing.B) {
			var t T
			b.SetBytes(int64(n.L * int(unsafe.Sizeof(t))))
			for i := 0; i < b.N; i++ {
				fn(a, 127, bits)
			}
		})
	}
}

func BenchCases2[T Number](b *testing.B, fn func([]T, T, T, []byte) int64) {
	b.Helper()
	for _, n := range BenchmarkSizes {
		a := randSlice[T](n.L)
		bits, _ := MakeBitsAndMaskPoison(n.L)
		b.Run(n.Name, func(b *testing.B) {
			var t T
			b.SetBytes(int64(n.L * int(unsafe.Sizeof(t))))
			for i := 0; i < b.N; i++ {
				fn(a, 5, 127, bits)
			}
		})
	}
}
