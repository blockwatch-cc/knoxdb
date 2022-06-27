// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build go1.7 && amd64 && !gccgo && !appengine
// +build go1.7,amd64,!gccgo,!appengine

package s8bVec

import (
	"encoding/binary"
	"math/rand"
	"testing"

	"blockwatch.cc/knoxdb/util"
	"github.com/google/go-cmp/cmp"
)

// TestEncodeAll ensures 100% test coverage of EncodeAll and
// verifies all output by comparing the original input with the output of decodeAll
func TestEncodeAllUint64AVX512(t *testing.T) {
	if !util.UseAVX512_F {
		t.Skip()
	}

	rand.Seed(0)
	for _, test := range s8bTestsUint64 {
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

			buf := make([]byte, 8*len(encoded))
			b := buf
			for _, v := range encoded {
				binary.LittleEndian.PutUint64(b, v)
				b = b[8:]
			}
			count, err := countValuesAVX2(buf)
			if err != nil {
				t.Fatalf("unexpected count error\n%s", err)
			}

			if count != len(test.in) {
				t.Fatalf("unexpected count: got %d expected %d", count, len(test.in))
			}

			decoded := make([]uint64, len(test.in))
			n := decodeAllUint64AVX512(decoded, encoded)

			if !cmp.Equal(decoded[:n], test.in) {
				t.Fatalf("unexpected values; +got/-exp\n%s", cmp.Diff(decoded, test.in))
			}
		})
	}
}

func TestEncodeAllUint32AVX2(t *testing.T) {
	if !util.UseAVX2 {
		t.Skip()
	}
	rand.Seed(0)

	for _, test := range s8bTestsUint32 {
		t.Run(test.name, func(t *testing.T) {
			if test.fn != nil {
				test.in = test.fn()
			}

			tmp := make([]uint64, len(test.in))
			for i := 0; i < len(tmp); i++ {
				tmp[i] = uint64(test.in[i])
			}
			encoded, err := EncodeAll(append(make([]uint64, 0, len(test.in)), tmp...))
			if test.err != nil {
				if err != test.err {
					t.Fatalf("expected encode error, got\n%s", err)
				}
				return
			}
			buf := make([]byte, 8*len(encoded))
			b := buf
			for _, v := range encoded {
				binary.LittleEndian.PutUint64(b, v)
				b = b[8:]
			}

			count, err := countValuesAVX2(buf)
			if err != nil {
				t.Fatalf("unexpected count error\n%s", err)
			}
			if count != len(test.in) {
				t.Fatalf("unexpected count: got %d expected %d", count, len(test.in))
			}

			decoded := make([]uint32, len(test.in))
			n := decodeAllUint32AVX2(decoded, buf)
			if err != nil {
				t.Fatalf("unexpected decode error\n%s", err)
			}

			if !cmp.Equal(decoded[:n], test.in) {
				t.Fatalf("unexpected values; +got/-exp\n%s", cmp.Diff(decoded, test.in))
			}
		})
	}
}

func TestEncodeAllUint64AVX2(t *testing.T) {
	if !util.UseAVX2 {
		t.Skip()
	}

	rand.Seed(0)
	for _, test := range s8bTestsUint64 {
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

			buf := make([]byte, 8*len(encoded))
			b := buf
			for _, v := range encoded {
				binary.LittleEndian.PutUint64(b, v)
				b = b[8:]
			}

			count, err := countValuesAVX2(buf)
			if err != nil {
				t.Fatalf("unexpected count error\n%s", err)
			}
			if count != len(test.in) {
				t.Fatalf("unexpected count: got %d expected %d", count, len(test.in))
			}

			decoded := make([]uint64, len(test.in))
			n := decodeAllUint64AVX2(decoded, buf)

			if !cmp.Equal(decoded[:n], test.in) {
				t.Fatalf("unexpected values; +got/-exp\n%s", cmp.Diff(decoded, test.in))
			}
		})
	}
}

func BenchmarkCountBytesAVX2(b *testing.B) {
	if !util.UseAVX2 {
		b.Skip()
	}
	for _, bm := range s8bBenchmarksUint64 {
		in := bm.fn(s8bBenchmarkSize)()
		encoded, _ := EncodeAll(in)

		buf := make([]byte, 8*len(encoded))
		tmp := buf
		for _, v := range encoded {
			binary.BigEndian.PutUint64(tmp, v)
			tmp = tmp[8:]
		}

		b.Run(bm.name, func(b *testing.B) {
			b.SetBytes(int64(8 * bm.size))
			for i := 0; i < b.N; i++ {
				countValuesAVX2(buf)
			}
		})
	}
}

func BenchmarkDecodeAllUint64AVX512(b *testing.B) {
	if !util.UseAVX512_F {
		b.Skip()
	}

	for _, bm := range s8bBenchmarksUint64 {
		in := bm.fn(s8bBenchmarkSize)()
		out := make([]uint64, len(in))
		comp, _ := EncodeAll(in)
		b.Run(bm.name, func(b *testing.B) {
			b.SetBytes(int64(8 * bm.size))
			for i := 0; i < b.N; i++ {
				decodeAllUint64AVX512(out, comp)
			}
		})
	}
}

func BenchmarkDecodeAllUint32AVX2(b *testing.B) {
	if !util.UseAVX2 {
		b.Skip()
	}

	for _, bm := range s8bBenchmarksUint32 {
		in := bm.fn(s8bBenchmarkSize)()
		out := make([]uint32, len(in))
		comp, _ := EncodeAll(in)
		buf := make([]byte, 8*len(comp))
		for i, v := range comp {
			binary.LittleEndian.PutUint64(buf[8*i:], v)
		}
		b.Run(bm.name, func(b *testing.B) {
			b.SetBytes(int64(4 * bm.size))
			for i := 0; i < b.N; i++ {
				decodeAllUint32AVX2(out, buf)
			}
		})
	}
}

func BenchmarkDecodeAllUint64AVX2(b *testing.B) {
	if !util.UseAVX2 {
		b.Skip()
	}

	for _, bm := range s8bBenchmarksUint64 {
		in := bm.fn(s8bBenchmarkSize)()
		out := make([]uint64, len(in))
		comp, _ := EncodeAll(in)

		buf := make([]byte, 8*len(comp))
		tmp := buf
		for _, v := range comp {
			binary.LittleEndian.PutUint64(tmp, v)
			tmp = tmp[8:]
		}

		b.Run(bm.name, func(b *testing.B) {
			b.SetBytes(int64(8 * bm.size))
			for i := 0; i < b.N; i++ {
				decodeAllUint64AVX2(out, buf)
			}
		})
	}
}
