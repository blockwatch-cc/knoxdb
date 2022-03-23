// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build go1.7 && amd64 && !gccgo && !appengine
// +build go1.7,amd64,!gccgo,!appengine

package s8bVec

import (
	"math/rand"
	"testing"

	"blockwatch.cc/knoxdb/util"
	"github.com/google/go-cmp/cmp"
)

// TestEncodeAll ensures 100% test coverage of EncodeAll and
// verifies all output by comparing the original input with the output of decodeAll
func TestEncodeAllAVX2(t *testing.T) {
	if !util.UseAVX2 {
		t.Skip()
	}

	rand.Seed(0)
	for _, test := range s8bTests {
		t.Run(test.name, func(t *testing.T) {
			if test.fn != nil {
				test.in = test.fn()
			}

			encoded, err := EncodeAll(append(make([]uint64, 0, len(test.in)), test.in...))
			if test.err != nil {
				if err != test.err {
					t.Fatalf("expected encode error, got\n%s", err)
				}
				return
			}

			decoded := make([]uint64, len(test.in))
			n, err := decodeAllAVX2(decoded, encoded)
			if err != nil {
				t.Fatalf("unexpected decode error\n%s", err)
			}

			if !cmp.Equal(decoded[:n], test.in) {
				t.Fatalf("unexpected values; +got/-exp\n%s", cmp.Diff(decoded, test.in))
			}
		})
	}
}

// TestEncodeAll ensures 100% test coverage of EncodeAll and
// verifies all output by comparing the original input with the output of decodeAll
func TestEncodeAllAVX2Call(t *testing.T) {
	if !util.UseAVX2 {
		t.Skip()
	}

	rand.Seed(0)
	for _, test := range s8bTests {
		t.Run(test.name, func(t *testing.T) {
			if test.fn != nil {
				test.in = test.fn()
			}

			encoded, err := EncodeAll(append(make([]uint64, 0, len(test.in)), test.in...))
			if test.err != nil {
				if err != test.err {
					t.Fatalf("expected encode error, got\n%s", err)
				}
				return
			}

			decoded := make([]uint64, len(test.in))
			n := decodeAllAVX2Call(decoded, encoded)

			if !cmp.Equal(decoded[:n], test.in) {
				t.Fatalf("unexpected values; +got/-exp\n%s", cmp.Diff(decoded, test.in))
			}
		})
	}
}

func TestEncodeAllAVX2Jmp(t *testing.T) {
	if !util.UseAVX2 {
		t.Skip()
	}

	rand.Seed(0)
	for _, test := range s8bTests {
		t.Run(test.name, func(t *testing.T) {
			if test.fn != nil {
				test.in = test.fn()
			}

			encoded, err := EncodeAll(append(make([]uint64, 0, len(test.in)), test.in...))
			if test.err != nil {
				if err != test.err {
					t.Fatalf("expected encode error, got\n%s", err)
				}
				return
			}

			decoded := make([]uint64, len(test.in))
			n := decodeAllAVX2Jmp(decoded, encoded)

			if !cmp.Equal(decoded[:n], test.in) {
				t.Fatalf("unexpected values; +got/-exp\n%s", cmp.Diff(decoded, test.in))
			}
		})
	}
}

func TestEncodeAllAVX2Opt(t *testing.T) {
	if !util.UseAVX2 {
		t.Skip()
	}

	rand.Seed(0)
	for _, test := range s8bTests {
		t.Run(test.name, func(t *testing.T) {
			if test.fn != nil {
				test.in = test.fn()
			}

			encoded, err := EncodeAll(append(make([]uint64, 0, len(test.in)), test.in...))
			if test.err != nil {
				if err != test.err {
					t.Fatalf("expected encode error, got\n%s", err)
				}
				return
			}

			decoded := make([]uint64, len(test.in))
			n := decodeAllAVX2Opt(decoded, encoded)

			if !cmp.Equal(decoded[:n], test.in) {
				t.Fatalf("unexpected values; +got/-exp\n%s", cmp.Diff(decoded, test.in))
			}
		})
	}
}

func BenchmarkDecodeAllAVX2(b *testing.B) {
	for _, bm := range s8bBenchmarks {
		in := bm.fn(s8bBenchmarkSize)()
		out := make([]uint64, len(in))
		comp, _ := EncodeAll(in)
		b.Run(bm.name, func(b *testing.B) {
			b.SetBytes(int64(8 * bm.size))
			for i := 0; i < b.N; i++ {
				decodeAllAVX2(out, comp)
			}
		})
	}
}

func BenchmarkDecodeAllAVX2Call(b *testing.B) {
	if !util.UseAVX2 {
		b.Skip()
	}

	for _, bm := range s8bBenchmarks {
		in := bm.fn(s8bBenchmarkSize)()
		out := make([]uint64, len(in))
		comp, _ := EncodeAll(in)
		b.Run(bm.name, func(b *testing.B) {
			b.SetBytes(int64(8 * bm.size))
			for i := 0; i < b.N; i++ {
				decodeAllAVX2Call(out, comp)
			}
		})
	}
}

func BenchmarkDecodeAllAVX2Jmp(b *testing.B) {
	if !util.UseAVX2 {
		b.Skip()
	}

	for _, bm := range s8bBenchmarks {
		in := bm.fn(s8bBenchmarkSize)()
		out := make([]uint64, len(in))
		comp, _ := EncodeAll(in)
		b.Run(bm.name, func(b *testing.B) {
			b.SetBytes(int64(8 * bm.size))
			for i := 0; i < b.N; i++ {
				decodeAllAVX2Call(out, comp)
			}
		})
	}
}

func BenchmarkDecodeAllAVX2Opt(b *testing.B) {
	if !util.UseAVX2 {
		b.Skip()
	}

	for _, bm := range s8bBenchmarks {
		in := bm.fn(s8bBenchmarkSize)()
		out := make([]uint64, len(in))
		comp, _ := EncodeAll(in)
		b.Run(bm.name, func(b *testing.B) {
			b.SetBytes(int64(8 * bm.size))
			for i := 0; i < b.N; i++ {
				decodeAllAVX2Call(out, comp)
			}
		})
	}
}
