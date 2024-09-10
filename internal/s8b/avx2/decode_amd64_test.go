// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build go1.7 && amd64 && !gccgo && !appengine
// +build go1.7,amd64,!gccgo,!appengine

package avx2

import (
	"encoding/binary"
	"math/rand"
	"testing"

	"blockwatch.cc/knoxdb/internal/s8b/generic"
	"blockwatch.cc/knoxdb/internal/s8b/tests"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/google/go-cmp/cmp"
)

var (
	EncodeUint64   = generic.EncodeUint64
	s8bTestsUint64 = tests.S8bTestsUint64
	s8bTestsUint32 = tests.S8bTestsUint32
	s8bTestsUint16 = tests.S8bTestsUint16
	s8bTestsUint8  = tests.S8bTestsUint8

	s8bBenchmarkSize    = tests.S8bBenchmarkSize
	s8bBenchmarksUint64 = tests.S8bBenchmarksUint64
	s8bBenchmarksUint32 = tests.S8bBenchmarksUint32
	s8bBenchmarksUint16 = tests.S8bBenchmarksUint16
	s8bBenchmarksUint8  = tests.S8bBenchmarksUint8
)

func TestEncodeUint64AVX2(t *testing.T) {
	if !util.UseAVX2 {
		t.Skip()
	}

	rand.Seed(0)
	for _, test := range s8bTestsUint64 {
		t.Run(test.Name, func(t *testing.T) {
			if test.Fn != nil {
				test.In = test.Fn()
			}

			encoded, err := EncodeUint64(append(make([]uint64, 0, len(test.In)), test.In...))
			if err != nil {
				if !test.Err {
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

			count, err := CountValues(buf)
			if err != nil {
				t.Fatalf("unexpected count error\n%s", err)
			}
			if count != len(test.In) {
				t.Fatalf("unexpected count: got %d expected %d", count, len(test.In))
			}

			decoded := make([]uint64, len(test.In))
			n, _ := DecodeUint64(decoded, buf)

			if !cmp.Equal(decoded[:n], test.In) {
				t.Fatalf("unexpected values; +got/-exp\n%s", cmp.Diff(decoded, test.In))
			}
		})
	}
}

func TestEncodeUint32AVX2(t *testing.T) {
	if !util.UseAVX2 {
		t.Skip()
	}
	rand.Seed(0)

	for _, test := range s8bTestsUint32 {
		t.Run(test.Name, func(t *testing.T) {
			if test.Fn != nil {
				test.In = test.Fn()
			}

			tmp := make([]uint64, len(test.In))
			for i := 0; i < len(tmp); i++ {
				tmp[i] = uint64(test.In[i])
			}
			encoded, err := EncodeUint64(append(make([]uint64, 0, len(test.In)), tmp...))
			if err != nil {
				if !test.Err {
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

			count, err := CountValues(buf)
			if err != nil {
				t.Fatalf("unexpected count error\n%s", err)
			}
			if count != len(test.In) {
				t.Fatalf("unexpected count: got %d expected %d", count, len(test.In))
			}

			decoded := make([]uint32, len(test.In))
			n, err := DecodeUint32(decoded, buf)
			if err != nil {
				t.Fatalf("unexpected decode error\n%s", err)
			}

			if !cmp.Equal(decoded[:n], test.In) {
				t.Fatalf("unexpected values; +got/-exp\n%s", cmp.Diff(decoded, test.In))
			}
		})
	}
}

func TestEncodeUint16AVX2(t *testing.T) {
	if !util.UseAVX2 {
		t.Skip()
	}
	rand.Seed(0)

	for _, test := range s8bTestsUint16 {
		t.Run(test.Name, func(t *testing.T) {
			if test.Fn != nil {
				test.In = test.Fn()
			}

			tmp := make([]uint64, len(test.In))
			for i := 0; i < len(tmp); i++ {
				tmp[i] = uint64(test.In[i])
			}
			encoded, err := EncodeUint64(append(make([]uint64, 0, len(test.In)), tmp...))
			if err != nil {
				if !test.Err {
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

			count, err := CountValues(buf)
			if err != nil {
				t.Fatalf("unexpected count error\n%s", err)
			}
			if count != len(test.In) {
				t.Fatalf("unexpected count: got %d expected %d", count, len(test.In))
			}

			decoded := make([]uint16, len(test.In))
			n, err := DecodeUint16(decoded, buf)
			if err != nil {
				t.Fatalf("unexpected decode error\n%s", err)
			}

			if !cmp.Equal(decoded[:n], test.In) {
				t.Fatalf("unexpected values; +got/-exp\n%s", cmp.Diff(decoded, test.In))
			}
		})
	}
}

func TestEncodeUint8AVX2(t *testing.T) {
	if !util.UseAVX2 {
		t.Skip()
	}
	rand.Seed(0)

	for _, test := range s8bTestsUint8 {
		t.Run(test.Name, func(t *testing.T) {
			if test.Fn != nil {
				test.In = test.Fn()
			}

			tmp := make([]uint64, len(test.In))
			for i := 0; i < len(tmp); i++ {
				tmp[i] = uint64(test.In[i])
			}
			encoded, err := EncodeUint64(append(make([]uint64, 0, len(test.In)), tmp...))
			if err != nil {
				if !test.Err {
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

			count, err := CountValues(buf)
			if err != nil {
				t.Fatalf("unexpected count error\n%s", err)
			}
			if count != len(test.In) {
				t.Fatalf("unexpected count: got %d expected %d", count, len(test.In))
			}

			decoded := make([]uint8, len(test.In))
			n, err := DecodeUint8(decoded, buf)
			if err != nil {
				t.Fatalf("unexpected decode error\n%s", err)
			}

			if !cmp.Equal(decoded[:n], test.In) {
				t.Fatalf("unexpected values; +got/-exp\n%s", cmp.Diff(decoded, test.In))
			}
		})
	}
}

func BenchmarkCountBytesAVX2(b *testing.B) {
	if !util.UseAVX2 {
		b.Skip()
	}
	for _, bm := range s8bBenchmarksUint64 {
		in := bm.Fn(s8bBenchmarkSize)()
		encoded, _ := EncodeUint64(in)

		buf := make([]byte, 8*len(encoded))
		tmp := buf
		for _, v := range encoded {
			binary.BigEndian.PutUint64(tmp, v)
			tmp = tmp[8:]
		}

		b.Run(bm.Name, func(b *testing.B) {
			b.SetBytes(int64(8 * bm.Size))
			for i := 0; i < b.N; i++ {
				CountValues(buf)
			}
		})
	}
}

func BenchmarkDecodeUint8AVX2(b *testing.B) {
	if !util.UseAVX2 {
		b.Skip()
	}

	for _, bm := range s8bBenchmarksUint8 {
		in := bm.Fn(s8bBenchmarkSize)()
		out := make([]uint8, len(in))
		comp, _ := EncodeUint64(in)
		buf := make([]byte, 8*len(comp))
		for i, v := range comp {
			binary.LittleEndian.PutUint64(buf[8*i:], v)
		}
		b.Run(bm.Name, func(b *testing.B) {
			b.SetBytes(int64(bm.Size))
			for i := 0; i < b.N; i++ {
				DecodeUint8(out, buf)
			}
		})
	}
}

func BenchmarkDecodeUint16AVX2(b *testing.B) {
	if !util.UseAVX2 {
		b.Skip()
	}

	for _, bm := range s8bBenchmarksUint16 {
		in := bm.Fn(s8bBenchmarkSize)()
		out := make([]uint16, len(in))
		comp, _ := EncodeUint64(in)
		buf := make([]byte, 8*len(comp))
		for i, v := range comp {
			binary.LittleEndian.PutUint64(buf[8*i:], v)
		}
		b.Run(bm.Name, func(b *testing.B) {
			b.SetBytes(int64(2 * bm.Size))
			for i := 0; i < b.N; i++ {
				DecodeUint16(out, buf)
			}
		})
	}
}

func BenchmarkDecodeUint32AVX2(b *testing.B) {
	if !util.UseAVX2 {
		b.Skip()
	}

	for _, bm := range s8bBenchmarksUint32 {
		in := bm.Fn(s8bBenchmarkSize)()
		out := make([]uint32, len(in))
		comp, _ := EncodeUint64(in)
		buf := make([]byte, 8*len(comp))
		for i, v := range comp {
			binary.LittleEndian.PutUint64(buf[8*i:], v)
		}
		b.Run(bm.Name, func(b *testing.B) {
			b.SetBytes(int64(4 * bm.Size))
			for i := 0; i < b.N; i++ {
				DecodeUint32(out, buf)
			}
		})
	}
}

func BenchmarkDecodeUint64AVX2(b *testing.B) {
	if !util.UseAVX2 {
		b.Skip()
	}

	for _, bm := range s8bBenchmarksUint64 {
		in := bm.Fn(s8bBenchmarkSize)()
		out := make([]uint64, len(in))
		comp, _ := EncodeUint64(in)

		buf := make([]byte, 8*len(comp))
		tmp := buf
		for _, v := range comp {
			binary.LittleEndian.PutUint64(tmp, v)
			tmp = tmp[8:]
		}

		b.Run(bm.Name, func(b *testing.B) {
			b.SetBytes(int64(8 * bm.Size))
			for i := 0; i < b.N; i++ {
				DecodeUint64(out, buf)
			}
		})
	}
}
