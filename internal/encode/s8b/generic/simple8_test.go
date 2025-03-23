// from: github.com/jwilder/encoding
package generic

import (
	"slices"
	"testing"

	"blockwatch.cc/knoxdb/internal/encode/s8b/tests"
	etests "blockwatch.cc/knoxdb/internal/encode/tests"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestLegacy(t *testing.T) {
	for _, test := range tests.MakeTests[uint64]() {
		t.Run(test.Name, func(t *testing.T) {
			in := test.In
			if test.Fn != nil {
				in = test.Fn()
			}
			encoded, err := EncodeLegacy(slices.Clone(in))
			if test.Err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			decoded := make([]uint64, len(in))
			n, err := DecodeLegacy(decoded, encoded)
			require.NoError(t, err)
			if len(encoded) > 0 {
				require.Equal(t, in, decoded[:n])
			}
		})
	}
}

func TestEncodeUint64(t *testing.T) {
	tests.EncodeTest[uint64](t, Encode[uint64], DecodeLegacyWrapper[uint64])
}

func TestEncodeUint32(t *testing.T) {
	tests.EncodeTest[uint32](t, Encode[uint32], DecodeLegacyWrapper[uint32])
}

func TestEncodeUint16(t *testing.T) {
	tests.EncodeTest[uint16](t, Encode[uint16], DecodeLegacyWrapper[uint16])
}

func TestEncodeUint8(t *testing.T) {
	tests.EncodeTest[uint8](t, Encode[uint8], DecodeLegacyWrapper[uint8])
}

func TestDecodeUint64(t *testing.T) {
	tests.EncodeTest[uint64](t, Encode[uint64], Decode[uint64])
}

func TestDecodeUint32(t *testing.T) {
	tests.EncodeTest[uint32](t, Encode[uint32], Decode[uint32])
}

func TestDecodeUint16(t *testing.T) {
	tests.EncodeTest[uint16](t, Encode[uint16], Decode[uint16])
}

func TestDecodeUint8(t *testing.T) {
	tests.EncodeTest[uint8](t, Encode[uint8], Decode[uint8])
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

func BenchmarkEncodeLegacy(b *testing.B) {
	for _, bm := range etests.MakeBenchmarks[uint64]() {
		b.Run(bm.Name, func(b *testing.B) {
			b.SetBytes(int64(8 * len(bm.Data)))
			for i := 0; i < b.N; i++ {
				EncodeLegacy(slices.Clone(bm.Data))
			}
		})
	}
}

func BenchmarkDecodeLegacy(b *testing.B) {
	for _, c := range etests.MakeBenchmarks[uint64]() {
		enc, _ := EncodeLegacy(slices.Clone(c.Data))
		dec := make([]uint64, len(c.Data))
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(len(c.Data) * 8))
			for i := 0; i < b.N; i++ {
				_, _ = DecodeLegacy(dec, enc)
			}
		})
	}
}

func BenchmarkEncodeUint64(b *testing.B) {
	tests.EncodeBenchmark[uint64](b, Encode[uint64])
}

func BenchmarkEncodeUint32(b *testing.B) {
	tests.EncodeBenchmark[uint32](b, Encode[uint32])
}

func BenchmarkEncodeUint16(b *testing.B) {
	tests.EncodeBenchmark[uint16](b, Encode[uint16])
}

func BenchmarkEncodeUint8(b *testing.B) {
	tests.EncodeBenchmark[uint8](b, Encode[uint8])
}

func BenchmarkDecodeUint64(b *testing.B) {
	tests.DecodeBenchmark[uint64](b, Encode[uint64], Decode[uint64])
}

func BenchmarkDecodeUint32(b *testing.B) {
	tests.DecodeBenchmark[uint32](b, Encode[uint32], Decode[uint32])
}

func BenchmarkDecodeUint16(b *testing.B) {
	tests.DecodeBenchmark[uint16](b, Encode[uint16], Decode[uint16])
}

func BenchmarkDecodeUint8(b *testing.B) {
	tests.DecodeBenchmark[uint8](b, Encode[uint8], Decode[uint8])
}
