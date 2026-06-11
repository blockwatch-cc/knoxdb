// Copyright (c) 2024-2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
package encode

import (
	"encoding/hex"
	"strconv"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/internal/tests/testutil"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/schema/reflect"
	"github.com/stretchr/testify/require"
)

// func BenchmarkMemcopy(b *testing.B) {
// 	for _, n := range encodeBenchmarkSizes {
// 		b.Run(n.name, func(b *testing.B) {
// 			slice, sz := makeBenchData(n.num)
// 			enc := NewEncoderFor[encodeBenchStruct]()
// 			buf := enc.NewBuffer(n.num)
// 			_, err := enc.Encode(slice, buf)
// 			require.NoError(b, err)
// 			dst := make([]byte, buf.Len())
// 			b.ReportAllocs()
// 			b.SetBytes(sz)
// 			b.ResetTimer()
// 			for b.Loop() {
// 				copy(dst, buf.Bytes())
// 			}
// 			b.ReportMetric(float64(n.num*b.N)/b.Elapsed().Seconds(), "recs/s")
// 		})
// 	}
// }

var encodeBenchmarkSizes = []struct {
	name string
	num  int
}{
	{"1", 1},
	{"1K", 1 * 1024},
	{"16K", 16 * 1024},
	{"64K", 64 * 1024},
}

type encodeBenchStruct struct {
	Id       uint64         `knox:"id,pk"`
	Time     time.Time      `knox:"time"`
	Hash     [20]byte       `knox:"hash,filter=bloom3b"`
	String   string         `knox:"str"`
	Bool     bool           `knox:"bool"`
	Enum     MyEnum         `knox:"my_enum,enum"`
	Int64    int64          `knox:"i64"`
	Int32    int32          `knox:"i32"`
	Int16    int16          `knox:"i16"`
	Int8     int8           `knox:"i8"`
	Uint64   uint64         `knox:"u64,filter=bloom2b"`
	Uint32   uint32         `knox:"u32"`
	Uint16   uint16         `knox:"u16"`
	Uint8    uint8          `knox:"u8"`
	Float64  float64        `knox:"f64"`
	Float32  float32        `knox:"f32"`
	D32      num.Decimal32  `knox:"d32,scale=5"`
	D64      num.Decimal64  `knox:"d64,scale=15"`
	D128     num.Decimal128 `knox:"d128,scale=18"`
	D256     num.Decimal256 `knox:"d256,scale=24"`
	I128     num.Int128     `knox:"i128"`
	I256     num.Int256     `knox:"i256"`
	Big      num.Big        `knox:"big"`
	Duration time.Duration  `knox:"dur"`
}

func makeBenchData(sz int) (res []encodeBenchStruct, size int64) {
	for i := range sz {
		res = append(res, encodeBenchStruct{
			Id:       0,
			Time:     time.Now().UTC(),
			Hash:     [20]byte(testutil.RandBytes(20)),
			String:   hex.EncodeToString(testutil.RandBytes(4)),
			Bool:     true,
			Enum:     MyEnum(myEnum.MustValue(uint16(i%4 + 1))),
			Int64:    int64(i),
			Int32:    int32(i),
			Int16:    int16(i % (1<<16 - 1)),
			Int8:     int8(i % (1<<8 - 1)),
			Uint64:   uint64(i * 1000000),
			Uint32:   uint32(i * 1000000),
			Uint16:   uint16(i),
			Uint8:    uint8(i),
			Float32:  float32(i / 1000000),
			Float64:  float64(i / 1000000),
			D32:      num.NewDecimal32(int32(100123456789-i), 5),
			D64:      num.NewDecimal64(1123456789123456789-int64(i), 15),
			D128:     num.NewDecimal128(num.MustParseInt128(strconv.Itoa(i)+"00000000000000000000"), 18),
			D256:     num.NewDecimal256(num.MustParseInt256(strconv.Itoa(i)+"0000000000000000000000000000000000000000"), 24),
			I128:     num.MustParseInt128(strconv.Itoa(i) + "000000000000000000000000000000"),
			I256:     num.MustParseInt256(strconv.Itoa(i) + "000000000000000000000000000000000000000000000000000000000000"),
			Big:      num.NewBig(int64(i)),
			Duration: time.Minute * time.Duration(i),
		})
	}
	enc := NewEncoderFor[encodeBenchStruct]()
	buf := enc.NewBuffer(sz)
	enc.Encode(res, buf)
	return res, int64(buf.Len())
}

type encodeListBenchStruct struct {
	U64List     []uint64   `knox:"u64l"`
	U64ListList [][]uint64 `knox:"u64ll"`
	KVList      []KV       `knox:"kvl"`
}

func makeListBenchData() (res []encodeListBenchStruct, size int64) {
	u64 := uint64(1)
	res = append(res, encodeListBenchStruct{
		U64List: []uint64{u64, u64 + 1, u64 + 2},
		U64ListList: [][]uint64{
			{u64, u64 + 1, u64 + 2},
			{u64 + 3, u64 + 4, u64 + 5},
		},
		KVList: []KV{
			{Key: u64, Val: u64},
			{Key: u64 + 1, Val: u64 + 1},
		},
	})
	enc := NewEncoderFor[encodeListBenchStruct]()
	buf := enc.NewBuffer(1)
	enc.Encode(res, buf)
	return res, int64(buf.Len())
}

func BenchmarkEncodeVal(b *testing.B) {
	slice, sz := makeBenchData(1)
	enc := NewEncoderFor[encodeBenchStruct]()
	buf := enc.NewBuffer(1)
	b.ReportAllocs()
	b.SetBytes(sz)
	b.ResetTimer()
	for b.Loop() {
		enc.Encode(slice[0], buf)
		buf.Reset()
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "recs/s")
}

func BenchmarkEncodeValSkip(b *testing.B) {
	slice, sz := makeBenchData(1)
	enc := NewEncoderFor[encodeBenchStruct]()
	s, err := enc.schema.DeleteId(2)
	require.NoError(b, err)
	s, err = s.DeleteId(4)
	require.NoError(b, err)
	enc.schema = s
	buf := enc.NewBuffer(1)
	b.ReportAllocs()
	b.SetBytes(sz)
	b.ResetTimer()
	for b.Loop() {
		enc.Encode(slice[0], buf)
		buf.Reset()
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "recs/s")
}

func BenchmarkEncodePtr(b *testing.B) {
	slice, sz := makeBenchData(1)
	enc := NewEncoderFor[encodeBenchStruct]()
	buf := enc.NewBuffer(1)
	b.ReportAllocs()
	b.SetBytes(sz)
	b.ResetTimer()
	for b.Loop() {
		enc.Encode(&slice[0], buf)
		buf.Reset()
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "recs/s")
}

func BenchmarkEncodeBatch(b *testing.B) {
	enc := NewEncoderFor[encodeBenchStruct]()
	for _, n := range encodeBenchmarkSizes {
		b.Run(n.name, func(b *testing.B) {
			slice, sz := makeBenchData(n.num)
			buf := enc.NewBuffer(n.num)
			b.ReportAllocs()
			b.SetBytes(sz)
			b.ResetTimer()
			for b.Loop() {
				enc.Encode(slice, buf)
				buf.Reset()
			}
			b.ReportMetric(float64(n.num*b.N)/b.Elapsed().Seconds(), "recs/s")
		})
	}
}

func BenchmarkEncodePtrBatch(b *testing.B) {
	enc := NewEncoderFor[encodeBenchStruct]()
	for _, n := range encodeBenchmarkSizes {
		b.Run(n.name, func(b *testing.B) {
			slice, sz := makeBenchData(n.num)
			buf := enc.NewBuffer(n.num)
			ptrslice := make([]*encodeBenchStruct, len(slice))
			for i := range slice {
				ptrslice[i] = &slice[i]
			}
			b.SetBytes(sz)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				enc.Encode(ptrslice, buf)
				buf.Reset()
			}
			b.ReportMetric(float64(n.num*b.N)/b.Elapsed().Seconds(), "recs/s")
		})
	}
}

func BenchmarkEncodeList(b *testing.B) {
	slice, sz := makeListBenchData()
	enc := NewEncoderFor[encodeListBenchStruct]()
	buf := enc.NewBuffer(1)
	b.ReportAllocs()
	b.SetBytes(sz)
	b.ResetTimer()
	for b.Loop() {
		enc.Encode(slice[0], buf)
		buf.Reset()
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "recs/s")
}

func BenchmarkEncodeListNoAlloc(b *testing.B) {
	slice, sz := makeListBenchData()
	enc := NewEncoderFor[encodeListBenchStruct]()
	buf := enc.NewBuffer(1)
	b.ReportAllocs()
	b.SetBytes(sz)
	b.ResetTimer()
	for b.Loop() {
		enc.Encode(&slice[0], buf)
		buf.Reset()
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "recs/s")
}

func BenchmarkDecodeAlloc(b *testing.B) {
	slice, sz := makeBenchData(1)
	enc := NewEncoderFor[encodeBenchStruct]()
	dec := NewDecoderFor[encodeBenchStruct]()
	buf := enc.NewBuffer(1)
	_, err := enc.Encode(slice[0], buf)
	require.NoError(b, err)
	b.ReportAllocs()
	b.SetBytes(sz)
	b.ResetTimer()
	for b.Loop() {
		dec.Decode(buf.Bytes(), nil)
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "recs/s")
}

func BenchmarkDecodeNoAlloc(b *testing.B) {
	slice, sz := makeBenchData(1)
	enc := NewEncoderFor[encodeBenchStruct]()
	dec := NewDecoderFor[encodeBenchStruct]()
	buf := enc.NewBuffer(1)
	_, err := enc.Encode(slice[0], buf)
	require.NoError(b, err)
	var val encodeBenchStruct
	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(sz)
	for b.Loop() {
		dec.Decode(buf.Bytes(), &val)
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "recs/s")
}

func BenchmarkDecodeList(b *testing.B) {
	slice, sz := makeListBenchData()
	enc := NewEncoderFor[encodeListBenchStruct]()
	dec := NewDecoderFor[encodeListBenchStruct]()
	buf := enc.NewBuffer(1)
	_, err := enc.Encode(slice[0], buf)
	require.NoError(b, err)
	b.ReportAllocs()
	b.SetBytes(sz)
	b.ResetTimer()
	for b.Loop() {
		var val encodeListBenchStruct
		dec.Decode(buf.Bytes(), &val)
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "recs/s")
}

func BenchmarkDecodeListNoAlloc(b *testing.B) {
	slice, sz := makeListBenchData()
	enc := NewEncoderFor[encodeListBenchStruct]()
	dec := NewDecoderFor[encodeListBenchStruct]()
	buf := enc.NewBuffer(1)
	_, err := enc.Encode(slice[0], buf)
	require.NoError(b, err)
	b.ReportAllocs()
	b.SetBytes(sz)
	b.ResetTimer()
	for b.Loop() {
		dec.Decode(buf.Bytes(), &slice[0])
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "recs/s")
}

func BenchmarkDecodeBatch(b *testing.B) {
	enc := NewEncoderFor[encodeBenchStruct]()
	dec := NewDecoderFor[encodeBenchStruct]()
	for _, n := range encodeBenchmarkSizes {
		b.Run(n.name, func(b *testing.B) {
			slice, sz := makeBenchData(n.num)
			buf := enc.NewBuffer(n.num)
			enc.Encode(slice, buf)
			b.ReportAllocs()
			b.SetBytes(sz)
			b.ResetTimer()
			for b.Loop() {
				dec.DecodeBatch(buf.Bytes(), nil)
			}
			b.ReportMetric(float64(n.num*b.N)/b.Elapsed().Seconds(), "recs/s")
		})
	}
}

func BenchmarkDecodeBatchNoAlloc(b *testing.B) {
	enc := NewEncoderFor[encodeBenchStruct]()
	dec := NewDecoderFor[encodeBenchStruct]()
	for _, n := range encodeBenchmarkSizes {
		b.Run(n.name, func(b *testing.B) {
			slice, sz := makeBenchData(n.num)
			buf := enc.NewBuffer(n.num)
			enc.Encode(slice, buf)
			res := make([]encodeBenchStruct, n.num)
			b.ReportAllocs()
			b.SetBytes(sz)
			b.ResetTimer()
			for b.Loop() {
				dec.DecodeBatch(buf.Bytes(), res)
			}
			b.ReportMetric(float64(n.num*b.N)/b.Elapsed().Seconds(), "recs/s")
		})
	}
}

func BenchmarkDecodeViewNoAlloc(b *testing.B) {
	slice, sz := makeBenchData(1)
	enc := NewEncoderFor[encodeBenchStruct]()
	buf := enc.NewBuffer(1)
	_, err := enc.Encode(slice[0], buf)
	require.NoError(b, err)
	var val encodeBenchStruct
	view := schema.NewView(enc.Schema())
	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(sz)
	for b.Loop() {
		view.Reset(buf.Bytes())
		val.Id = view.Uint64(0)
		val.Time = view.Timestamp(1)
		copy(val.Hash[:], view.Bytes(2))
		val.String = view.String(3)
		val.Bool = view.Bool(4)
		val.Enum = MyEnum(view.Enum(5))
		val.Int64 = view.Int64(6)
		val.Int32 = view.Int32(7)
		val.Int16 = view.Int16(8)
		val.Int8 = view.Int8(9)
		val.Uint64 = view.Uint64(10)
		val.Uint32 = view.Uint32(11)
		val.Uint16 = view.Uint16(12)
		val.Uint8 = view.Uint8(13)
		val.Float64 = view.Float64(14)
		val.Float32 = view.Float32(15)
		val.D32 = view.Decimal32(16)
		val.D64 = view.Decimal64(17)
		val.D128 = view.Decimal128(18)
		val.D256 = view.Decimal256(19)
		val.I128 = view.Int128(20)
		val.I256 = view.Int256(21)
		val.Big = view.Bigint(22)
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "recs/s")
}

func BenchmarkEncodeMarshal(b *testing.B) {
	l, err := reflect.LayoutOf(MarshalRecord{}, MarshalRecordSchema)
	require.NoError(b, err)
	enc := NewEncoderWithLayout(MarshalRecordSchema, l)
	for _, n := range encodeBenchmarkSizes {
		b.Run(n.name, func(b *testing.B) {
			slice := makeMarsahlData(n.num)
			buf := enc.NewBuffer(n.num)
			res, err := enc.EncodeBatch(slice, buf)
			require.NoError(b, err)
			b.SetBytes(int64(len(res)))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				enc.EncodeBatch(slice, buf)
				buf.Reset()
			}
			b.ReportMetric(float64(n.num*b.N)/b.Elapsed().Seconds(), "recs/s")
		})
	}
}

func BenchmarkEncodeMarshalPtr(b *testing.B) {
	l, err := reflect.LayoutOf(MarshalRecord{}, MarshalRecordSchema)
	require.NoError(b, err)
	enc := NewEncoderWithLayout(MarshalRecordSchema, l)
	for _, n := range encodeBenchmarkSizes {
		b.Run(n.name, func(b *testing.B) {
			slice := makeMarsahlData(n.num)
			buf := enc.NewBuffer(n.num)
			ptrslice := make([]*MarshalRecord, len(slice))
			for i := range slice {
				ptrslice[i] = &slice[i]
			}
			res, err := enc.EncodeBatch(slice, buf)
			require.NoError(b, err)
			b.SetBytes(int64(len(res)))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				enc.EncodeBatch(ptrslice, buf)
				buf.Reset()
			}
			b.ReportMetric(float64(n.num*b.N)/b.Elapsed().Seconds(), "recs/s")
		})
	}
}

func BenchmarkDecodeMarshal(b *testing.B) {
	l, err := reflect.LayoutOf(MarshalRecord{}, MarshalRecordSchema)
	require.NoError(b, err)
	enc := NewEncoderWithLayout(MarshalRecordSchema, l)
	dec := NewDecoderWithLayout(MarshalRecordSchema, l)
	for _, n := range encodeBenchmarkSizes {
		slice := makeMarsahlData(n.num)
		buf, err := enc.EncodeBatch(slice, nil)
		require.NoError(b, err)
		dst := make([]MarshalRecord, n.num)
		b.Run(n.name, func(b *testing.B) {
			b.SetBytes(int64(len(buf)))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				dec.DecodeBatch(buf, dst)
			}
			b.ReportMetric(float64(n.num*b.N)/b.Elapsed().Seconds(), "recs/s")
		})
	}
}

func BenchmarkEncodeListMarshal(b *testing.B) {
	l, err := reflect.LayoutOf(LogRecord{}, LogRecordSchema)
	require.NoError(b, err)
	enc := NewEncoderWithLayout(LogRecordSchema, l)
	for _, n := range encodeBenchmarkSizes {
		b.Run(n.name, func(b *testing.B) {
			slice := makeLogRecords(n.num)
			buf := enc.NewBuffer(n.num)
			res, err := enc.EncodeBatch(slice, buf)
			require.NoError(b, err)
			b.SetBytes(int64(len(res)))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				enc.EncodeBatch(slice, buf)
				buf.Reset()
			}
			b.ReportMetric(float64(n.num*b.N)/b.Elapsed().Seconds(), "recs/s")
		})
	}
}

func BenchmarkDecodeListMarshal(b *testing.B) {
	l, err := reflect.LayoutOf(LogRecord{}, LogRecordSchema)
	require.NoError(b, err)
	enc := NewEncoderWithLayout(LogRecordSchema, l)
	dec := NewDecoderWithLayout(LogRecordSchema, l)
	for _, n := range encodeBenchmarkSizes {
		slice := makeLogRecords(n.num)
		buf, err := enc.EncodeBatch(slice, nil)
		require.NoError(b, err)
		dst := make([]LogRecord, n.num)
		b.Run(n.name, func(b *testing.B) {
			b.SetBytes(int64(len(buf)))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				dec.DecodeBatch(buf, dst)
			}
			b.ReportMetric(float64(n.num*b.N)/b.Elapsed().Seconds(), "recs/s")
		})
	}
}
