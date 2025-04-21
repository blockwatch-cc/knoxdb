// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"slices"
	"testing"
	"unsafe"

	stests "blockwatch.cc/knoxdb/internal/encode/s8b/tests"
	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestEncode(t *testing.T) {
	stests.EncodeTest[uint64](t, Encode[uint64], Decode[uint64])
	stests.EncodeTest[uint32](t, Encode[uint32], Decode[uint32])
	stests.EncodeTest[uint16](t, Encode[uint16], Decode[uint16])
	stests.EncodeTest[uint8](t, Encode[uint8], Decode[uint8])
}

func TestDecode(t *testing.T) {
	stests.EncodeTest[uint64](t, Encode[uint64], Decode[uint64])
	stests.EncodeTest[uint32](t, Encode[uint32], Decode[uint32])
	stests.EncodeTest[uint16](t, Encode[uint16], Decode[uint16])
	stests.EncodeTest[uint8](t, Encode[uint8], Decode[uint8])
}

func DecodeLegacyWrapper[T types.Unsigned](dst []T, buf []byte) (int, error) {
	src := util.FromByteSlice[uint64](buf)
	switch any(T(0)).(type) {
	case uint64:
		return DecodeLegacy(util.ReinterpretSlice[T, uint64](dst), src)
	default:
		u64 := make([]uint64, len(dst))
		n, err := DecodeLegacy(u64, src)
		if err != nil {
			return 0, err
		}
		for i := 0; i < n; i++ {
			dst[i] = T(u64[i])
		}
		return n, nil
	}
}

func BenchmarkEncode(b *testing.B) {
	stests.EncodeBenchmark[uint64](b, Encode[uint64])
	stests.EncodeBenchmark[uint32](b, Encode[uint32])
	stests.EncodeBenchmark[uint16](b, Encode[uint16])
	stests.EncodeBenchmark[uint8](b, Encode[uint8])
}

func BenchmarkDecode(b *testing.B) {
	stests.DecodeBenchmark[uint64](b, Encode[uint64], Decode[uint64])
	stests.DecodeBenchmark[uint32](b, Encode[uint32], Decode[uint32])
	stests.DecodeBenchmark[uint16](b, Encode[uint16], Decode[uint16])
	stests.DecodeBenchmark[uint8](b, Encode[uint8], Decode[uint8])
}

func BenchmarkCount(b *testing.B) {
	for _, c := range tests.MakeBenchmarks[uint64]() {
		minv, maxv := slices.Min(c.Data), slices.Max(c.Data)
		buf, err := Encode[uint64](make([]byte, 8*len(c.Data)), c.Data, minv, maxv)
		require.NoError(b, err)
		b.Run("uint64/"+c.Name, func(b *testing.B) {
			b.SetBytes(int64(len(c.Data) * 8))
			for b.Loop() {
				_ = CountValues(buf)
			}
			b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
		})
	}
}

type zeroOneBenchmark struct {
	Name string
	Data []uint64
}

type zeroOneVersion struct {
	Name string
	Fn   func(p unsafe.Pointer, minv uint64) (bool, bool)
}

func TestZeroOrOnes(t *testing.T) {
	for i, c := range []zeroOneBenchmark{
		{"Zeros", tests.GenConst[uint64](128, 0)},
		{"Ones", tests.GenConst[uint64](128, 1)},
		{"Dups", tests.GenDups[uint64](128, 16, -1)},
		{"Runs", tests.GenRuns[uint64](128, 10, -1)},
		{"Seq", tests.GenSeq[uint64](128)},
	} {
		for _, f := range []zeroOneVersion{
			{"V9", zeroOrOne[uint64]},
		} {
			minv := slices.Min(c.Data)
			if i == 1 {
				minv = 0
			}
			t.Run(f.Name+"/"+c.Name, func(t *testing.T) {
				z, o := f.Fn(unsafe.Pointer(&c.Data[0]), minv)
				switch i {
				case 0:
					// zeros case
					require.True(t, z, "zeros not detected")
					require.False(t, o, "ones mistakenly detected")
				case 1:
					// ones case
					require.False(t, z, "zeros mistakenly detected")
					require.True(t, o, "ones not detected")
				default:
					// other cases
					require.False(t, z, "zeros mistakenly detected")
					require.False(t, o, "ones mistakenly detected")
				}
			})
		}
	}
}

func BenchmarkZeroOrOne(b *testing.B) {
	for i, c := range []zeroOneBenchmark{
		{"Zeros", tests.GenConst[uint64](128, 0)},
		{"Ones", tests.GenConst[uint64](128, 1)},
		{"Dups", tests.GenDups[uint64](128, 16, -1)},
		{"Runs", tests.GenRuns[uint64](128, 10, -1)},
		{"Seq", tests.GenSeq[uint64](128)},
	} {
		minv := slices.Min(c.Data)
		if i == 1 {
			minv = 0
		}
		b.Run(c.Name, func(b *testing.B) {
			for range b.N {
				_, _ = zeroOrOne(unsafe.Pointer(&c.Data[0]), minv)
			}
		})
	}
}

func TestSeek(t *testing.T) {
	for _, c := range stests.MakeTests[uint64]() {
		t.Run(c.Name, func(t *testing.T) {
			if c.Err {
				t.Skip()
			}
			in := c.Data
			if c.Gen != nil {
				in = c.Gen()
			}
			var minv, maxv uint64
			if len(in) > 0 {
				minv, maxv = slices.Min(in), slices.Max(in)
			}
			buf, err := Encode(make([]byte, len(in)*8), in, minv, maxv)
			require.NoError(t, err)
			dst := make([]uint64, 128)

			for i, v := range in {
				// t.Logf("Seek to %d, expecting %d", i, v)
				bpos, vpos := Seek(buf, i)
				require.NotEqual(t, -1, bpos, "illegal buffer pos")
				require.NotEqual(t, -1, vpos, "illegal code word pos")
				require.LessOrEqual(t, vpos, 128, "code word pos too large")
				require.Less(t, bpos, len(buf), "buf pos too large")
				// t.Logf("> found buf[%d] sel=%d vals=%d code[%d]",
				// 	bpos, buf[bpos+7]>>4, maxNPerSelector[buf[bpos+7]>>4], vpos)
				n := DecodeWord(dst, buf[bpos:])
				require.LessOrEqual(t, vpos, n, "vpos behind word contents")
				require.Equal(t, v, dst[vpos]+minv, "seek position mismatch")
			}
		})
	}
}
