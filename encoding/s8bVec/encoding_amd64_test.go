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

			buf := make([]byte, 8*len(encoded))
			b := buf
			for _, v := range encoded {
				binary.LittleEndian.PutUint64(b, v)
				b = b[8:]
			}
			count, err := countBytesAVX2(buf)
			if err != nil {
				t.Fatalf("unexpected count error\n%s", err)
			}

			if count != len(test.in) {
				t.Fatalf("unexpected count: got %d expected %d", count, len(test.in))
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

			buf := make([]byte, 8*len(encoded))
			b := buf
			for _, v := range encoded {
				binary.LittleEndian.PutUint64(b, v)
				b = b[8:]
			}
			count, err := countBytesAVX2(buf)
			if err != nil {
				t.Fatalf("unexpected count error\n%s", err)
			}

			if count != len(test.in) {
				t.Fatalf("unexpected count: got %d expected %d", count, len(test.in))
			}

			decoded := make([]uint64, len(test.in))
			n := decodeAllAVX2Call(decoded, buf)

			if !cmp.Equal(decoded[:n], test.in) {
				t.Fatalf("unexpected values; +got/-exp\n%s", cmp.Diff(decoded, test.in))
			}
		})
	}
}

// TestEncodeAll ensures 100% test coverage of EncodeAll and
// verifies all output by comparing the original input with the output of decodeAll
func TestEncodeAllAVX512Call(t *testing.T) {
	if !util.UseAVX512_F {
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

			buf := make([]byte, 8*len(encoded))
			b := buf
			for _, v := range encoded {
				binary.BigEndian.PutUint64(b, v)
				b = b[8:]
			}
			count, err := countBytesAVX512(buf)
			if err != nil {
				t.Fatalf("unexpected count error\n%s", err)
			}

			if count != len(test.in) {
				t.Fatalf("unexpected count: got %d expected %d", count, len(test.in))
			}

			decoded := make([]uint64, len(test.in))
			n := decodeAllAVX512Call(decoded, encoded)

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

	for _, test := range s8bTests32bit {
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

			count, err := countBytesGeneric(buf)
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

func BenchmarkCountBytesAVX2(b *testing.B) {
	if !util.UseAVX2 {
		b.Skip()
	}
	for _, bm := range s8bBenchmarks {
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
				countBytesAVX2(buf)
			}
		})
	}
}

func BenchmarkDecodeAllAVX2(b *testing.B) {
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

		buf := make([]byte, 8*len(comp))
		tmp := buf
		for _, v := range comp {
			binary.LittleEndian.PutUint64(tmp, v)
			tmp = tmp[8:]
		}

		b.Run(bm.name, func(b *testing.B) {
			b.SetBytes(int64(8 * bm.size))
			for i := 0; i < b.N; i++ {
				decodeAllAVX2Call(out, buf)
			}
		})
	}
}

func BenchmarkDecodeAllAVX512Call(b *testing.B) {
	if !util.UseAVX512_F {
		b.Skip()
	}

	for _, bm := range s8bBenchmarks {
		in := bm.fn(s8bBenchmarkSize)()
		out := make([]uint64, len(in))
		comp, _ := EncodeAll(in)
		b.Run(bm.name, func(b *testing.B) {
			b.SetBytes(int64(8 * bm.size))
			for i := 0; i < b.N; i++ {
				decodeAllAVX512Call(out, comp)
			}
		})
	}
}

func BenchmarkDecodeAllUint32AVX2(b *testing.B) {
	if !util.UseAVX2 {
		b.Skip()
	}

	for _, bm := range s8bBenchmarks32bit {
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

func BenchmarkDecodeAll32bitAVX2Copy(b *testing.B) {
	if !util.UseAVX2 {
		b.Skip()
	}

	for _, bm := range s8bBenchmarks32bit {
		in := bm.fn(s8bBenchmarkSize)()
		out := make([]uint32, len(in))
		tmp := make([]uint64, len(in))
		comp, _ := EncodeAll(in)

		buf := make([]byte, 8*len(comp))
		b0 := buf
		for _, v := range comp {
			binary.LittleEndian.PutUint64(b0, v)
			b0 = b0[8:]
		}

		b.Run(bm.name, func(b *testing.B) {
			b.SetBytes(int64(4 * bm.size))
			for i := 0; i < b.N; i++ {
				decodeAllAVX2Call(tmp, buf)
				for i, v := range tmp {
					out[i] = uint32(v)
				}
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
				decodeAllAVX2Jmp(out, comp)
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
				decodeAllAVX2Opt(out, comp)
			}
		})
	}
}
