// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build amd64 && !gccgo && !appengine
// +build amd64,!gccgo,!appengine

package avx512

import (
	"encoding/binary"
	"testing"

	"blockwatch.cc/knoxdb/internal/s8b/avx2"
	"blockwatch.cc/knoxdb/internal/s8b/generic"
	"blockwatch.cc/knoxdb/internal/s8b/tests"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/google/go-cmp/cmp"
)

var (
	EncodeUint64        = generic.EncodeUint64
	CountValues         = avx2.CountValues
	s8bTestsUint64      = tests.S8bTestsUint64
	s8bBenchmarkSize    = tests.S8bBenchmarkSize
	s8bBenchmarksUint64 = tests.S8bBenchmarksUint64
)

// TestEncode ensures 100% test coverage of Encode and
// verifies all output by comparing the original input with the output of decode
func TestEncodeUint64AVX512(t *testing.T) {
	if !util.UseAVX512_F {
		t.Skip()
	}

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
			n := decodeUint64AVX512(decoded, buf)

			if !cmp.Equal(decoded[:n], test.In) {
				t.Fatalf("unexpected values; +got/-exp\n%s", cmp.Diff(decoded, test.In))
			}
		})
	}
}

func BenchmarkDecodeUint64AVX512(b *testing.B) {
	if !util.UseAVX512_F {
		b.Skip()
	}

	for _, bm := range s8bBenchmarksUint64 {
		in := bm.Fn(s8bBenchmarkSize)()
		out := make([]uint64, len(in))
		comp, _ := EncodeUint64(in)
		buf := make([]byte, 8*len(comp))
		for i, v := range comp {
			binary.LittleEndian.PutUint64(buf[8*i:], v)
		}
		b.Run(bm.Name, func(b *testing.B) {
			b.SetBytes(int64(8 * bm.Size))
			for i := 0; i < b.N; i++ {
				DecodeUint64(out, buf)
			}
		})
	}
}
