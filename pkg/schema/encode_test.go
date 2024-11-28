// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"bytes"
	"encoding/hex"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/pkg/num"
	"github.com/stretchr/testify/require"
)

var rnd = rand.New(rand.NewSource(mustParseI64(os.Getenv("GORANDSEED"))))

func mustParseI64(s string) int64 {
	if s == "" {
		return 0
	}
	n, err := strconv.ParseInt(s, 0, 64)
	if err != nil {
		panic(err)
	}
	return n
}

func randBytes(sz int) []byte {
	buf := make([]byte, sz)
	_, _ = rnd.Read(buf)
	return buf
}

func randFixed[T any]() T {
	var t T
	rval := reflect.ValueOf(&t).Elem()
	src := randBytes(rval.Len())
	reflect.Copy(rval, reflect.ValueOf(src))
	return t
}

type Hash [32]byte

type encodeTestStruct struct {
	Id        uint64         `knox:"id,pk"`
	Time      time.Time      `knox:"time"`
	Hash      []byte         `knox:"hash,fixed=20,index=bloom=3"`
	HashFixed Hash           `knox:"hash_fixed,index=bloom,zip=snappy"`
	String    string         `knox:"str"`
	Stringer  Stringer       `knox:"strlist"`
	Bool      bool           `knox:"bool"`
	Enum      MyEnum         `knox:"my_enum,enum"`
	Int64     int64          `knox:"i64"`
	Int32     int32          `knox:"i32"`
	Int16     int16          `knox:"i16"`
	Int8      int8           `knox:"i8"`
	Uint64    uint64         `knox:"u64,index=bloom"`
	Uint32    uint32         `knox:"u32"`
	Uint16    uint16         `knox:"u16"`
	Uint8     uint8          `knox:"u8"`
	Float64   float64        `knox:"f64"`
	Float32   float32        `knox:"f32"`
	D32       num.Decimal32  `knox:"d32,scale=5"`
	D64       num.Decimal64  `knox:"d64,scale=15"`
	D128      num.Decimal128 `knox:"d128,scale=18"`
	D256      num.Decimal256 `knox:"d256,scale=24"`
	I128      num.Int128     `knox:"i128"`
	I256      num.Int256     `knox:"i256"`
}

func makeTestData(sz int) (res []encodeTestStruct) {
	for i := 1; i <= sz; i++ {
		res = append(res, encodeTestStruct{
			Id:        0,
			Time:      time.Now().UTC(),
			Hash:      randBytes(20),
			HashFixed: randFixed[Hash](),
			String:    hex.EncodeToString(randBytes(4)),
			Stringer:  strings.SplitAfter(hex.EncodeToString(randBytes(32)), "a"),
			Bool:      true,
			Enum:      MyEnum(myEnum.MustValue(uint16(i%4 + 1))),
			Int64:     int64(i),
			Int32:     int32(i),
			Int16:     int16(i % (1<<16 - 1)),
			Int8:      int8(i % (1<<8 - 1)),
			Uint64:    uint64(i * 1000000),
			Uint32:    uint32(i * 1000000),
			Uint16:    uint16(i),
			Uint8:     uint8(i),
			Float32:   float32(i / 1000000),
			Float64:   float64(i / 1000000),
			D32:       num.NewDecimal32(int32(100123456789-i), 5),
			D64:       num.NewDecimal64(1123456789123456789-int64(i), 15),
			D128:      num.NewDecimal128(num.MustParseInt128(strconv.Itoa(i)+"00000000000000000000"), 18),
			D256:      num.NewDecimal256(num.MustParseInt256(strconv.Itoa(i)+"0000000000000000000000000000000000000000"), 24),
			I128:      num.MustParseInt128(strconv.Itoa(i) + "000000000000000000000000000000"),
			I256:      num.MustParseInt256(strconv.Itoa(i) + "000000000000000000000000000000000000000000000000000000000000"),
		})
	}
	return
}

type visibilityTestStruct struct {
	Id               uint64 `knox:"id,pk"`
	FDeleted         uint64 `knox:"f_deleted"`
	FInternal        uint64 `knox:"f_internal,internal"`
	FInternalDeleted uint64 `knox:"f_internal_deleted,internal"`
	HashFixed        Hash   `knox:"hash_fixed"`
}

func makeVisibilityTestData(sz int) (res []visibilityTestStruct) {
	for i := 1; i <= sz; i++ {
		res = append(res, visibilityTestStruct{
			Id:               0,
			FDeleted:         0xfafafafafafafafa,
			FInternal:        0xfbfbfbfbfbfbfbfb,
			FInternalDeleted: 0xfcfcfcfcfcfcfcfc,
			HashFixed:        randFixed[Hash](),
		})
	}
	return
}

func TestEncodeVal(t *testing.T) {
	vals := makeTestData(1)
	val := vals[0]
	enc := NewGenericEncoder[encodeTestStruct]()
	buf, err := enc.Encode(val, nil)
	require.NoError(t, err)
	require.NotNil(t, buf)
	require.NotEmpty(t, buf)
}

func TestEncodeValWithVisibility(t *testing.T) {
	// visibility tests (internal & deleted fields)
	s, err := SchemaOf(visibilityTestStruct{})
	val := makeVisibilityTestData(1)[0]
	require.NoError(t, err)
	s, err = s.DeleteField(2)
	require.NoError(t, err)
	s, err = s.DeleteField(3)
	require.NoError(t, err)
	enc := NewEncoder(s)
	buf, err := enc.Encode(&val, nil)
	require.NoError(t, err)
	require.NotNil(t, buf)
	require.NotEmpty(t, buf)
}

func TestEncodeRoundtrip(t *testing.T) {
	vals := makeTestData(1)
	val := vals[0]
	enc := NewGenericEncoder[encodeTestStruct]()
	buf, err := enc.Encode(val, nil)
	require.NoError(t, err)
	require.NotNil(t, buf)
	require.NotEmpty(t, buf)

	dec := NewGenericDecoder[encodeTestStruct]()
	val2, err := dec.Decode(buf, nil)
	require.NoError(t, err)
	require.IsType(t, val, *val2)
	require.Exactly(t, val, *val2)
}

func TestEncodeRoundtripWithVisibility(t *testing.T) {
	// visibility tests (internal & deleted fields)
	s, err := SchemaOf(visibilityTestStruct{})
	require.NoError(t, err)
	s, err = s.DeleteField(2)
	require.NoError(t, err)
	s, err = s.DeleteField(4)
	require.NoError(t, err)
	val := makeVisibilityTestData(1)[0]
	enc := NewEncoder(s)
	buf, err := enc.Encode(&val, nil)
	require.NoError(t, err)
	require.NotNil(t, buf)
	require.NotEmpty(t, buf)
	require.Len(t, buf, s.WireSize())

	dec := NewDecoder(s)
	var val2 visibilityTestStruct
	err = dec.Decode(buf, &val2)
	require.NoError(t, err)
	require.Equal(t, val.Id, val2.Id)
	require.Equal(t, val.HashFixed, val2.HashFixed, "hash_fixed")
	require.Equal(t, uint64(0), val2.FInternal, "internal")
	require.Equal(t, uint64(0), val2.FDeleted, "deleted")
	require.Equal(t, uint64(0), val2.FInternalDeleted, "internal_deleted")
}

func TestDecoderRead(t *testing.T) {
	enc := NewGenericEncoder[encodeTestStruct]()
	dec := NewGenericDecoder[encodeTestStruct]()
	// t.Log("E", enc.Schema())
	// t.Log("D", dec.Schema())
	vals := makeTestData(100)
	buf := enc.NewBuffer(100)
	for _, val := range vals {
		b, err := enc.Encode(val, buf)
		require.NoError(t, err)
		require.NotNil(t, b)
		require.NotEmpty(t, b)
	}
	for i := 0; i < 100; i++ {
		val, err := dec.Read(buf)
		require.NoError(t, err)
		if i < 99 {
			require.Greater(t, buf.Len(), 0, "no more bytes left to consume")
		}
		require.IsType(t, vals[i], *val)
		require.Exactly(t, vals[i], *val)
	}
	require.Equal(t, buf.Len(), 0, "not all bytes are consumed")
}

func TestEncodeSlice(t *testing.T) {
	vals := makeTestData(2)
	enc := NewGenericEncoder[encodeTestStruct]()
	buf, err := enc.EncodeSlice(vals, nil)
	require.NoError(t, err)
	require.NotNil(t, buf)
	require.NotEmpty(t, buf)
}

func TestEncodeValPtr(t *testing.T) {
	vals := makeTestData(1)
	val := &vals[0]
	enc := NewGenericEncoder[encodeTestStruct]()
	buf, err := enc.EncodePtr(val, nil)
	require.NoError(t, err)
	require.NotNil(t, buf)
	require.NotEmpty(t, buf)
}

func TestEncodePtrSlice(t *testing.T) {
	vals := makeTestData(2)
	ptrs := make([]*encodeTestStruct, 2)
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	enc := NewGenericEncoder[encodeTestStruct]()
	buf, err := enc.EncodePtrSlice(ptrs, nil)
	require.NoError(t, err)
	require.NotNil(t, buf)
	require.NotEmpty(t, buf)
}

var encodeBenchmarkSizes = []struct {
	name string
	num  int
}{
	{"1", 1},
	{"512", 512},
	{"1K", 1 * 1024},
	{"16K", 16 * 1024},
	{"32K", 32 * 1024},
	{"64K", 64 * 1024},
}

type encodeBenchStruct struct {
	Id      uint64         `knox:"id,pk"`
	Time    time.Time      `knox:"time"`
	Hash    []byte         `knox:"hash,fixed=20,index=bloom=3"`
	String  string         `knox:"str"`
	Bool    bool           `knox:"bool"`
	Enum    MyEnum         `knox:"my_enum,enum"`
	Int64   int64          `knox:"i64"`
	Int32   int32          `knox:"i32"`
	Int16   int16          `knox:"i16"`
	Int8    int8           `knox:"i8"`
	Uint64  uint64         `knox:"u64,index=bloom"`
	Uint32  uint32         `knox:"u32"`
	Uint16  uint16         `knox:"u16"`
	Uint8   uint8          `knox:"u8"`
	Float64 float64        `knox:"f64"`
	Float32 float32        `knox:"f32"`
	D32     num.Decimal32  `knox:"d32,scale=5"`
	D64     num.Decimal64  `knox:"d64,scale=15"`
	D128    num.Decimal128 `knox:"d128,scale=18"`
	D256    num.Decimal256 `knox:"d256,scale=24"`
	I128    num.Int128     `knox:"i128"`
	I256    num.Int256     `knox:"i256"`
}

func makeBenchData(sz int) (res []encodeBenchStruct, size int64) {
	for i := 0; i < sz; i++ {
		res = append(res, encodeBenchStruct{
			Id:      0,
			Time:    time.Now().UTC(),
			Hash:    randBytes(20),
			String:  hex.EncodeToString(randBytes(4)),
			Bool:    true,
			Enum:    MyEnum(myEnum.MustValue(uint16(i%4 + 1))),
			Int64:   int64(i),
			Int32:   int32(i),
			Int16:   int16(i % (1<<16 - 1)),
			Int8:    int8(i % (1<<8 - 1)),
			Uint64:  uint64(i * 1000000),
			Uint32:  uint32(i * 1000000),
			Uint16:  uint16(i),
			Uint8:   uint8(i),
			Float32: float32(i / 1000000),
			Float64: float64(i / 1000000),
			D32:     num.NewDecimal32(int32(100123456789-i), 5),
			D64:     num.NewDecimal64(1123456789123456789-int64(i), 15),
			D128:    num.NewDecimal128(num.MustParseInt128(strconv.Itoa(i)+"00000000000000000000"), 18),
			D256:    num.NewDecimal256(num.MustParseInt256(strconv.Itoa(i)+"0000000000000000000000000000000000000000"), 24),
			I128:    num.MustParseInt128(strconv.Itoa(i) + "000000000000000000000000000000"),
			I256:    num.MustParseInt256(strconv.Itoa(i) + "000000000000000000000000000000000000000000000000000000000000"),
		})
	}
	enc := NewGenericEncoder[encodeBenchStruct]()
	buf := enc.NewBuffer(sz)
	_, _ = enc.EncodeSlice(res, buf)
	return res, int64(buf.Len())
}

func BenchmarkEncodeVal(b *testing.B) {
	slice, sz := makeBenchData(1)
	enc := NewGenericEncoder[encodeBenchStruct]()
	buf := enc.NewBuffer(1)
	b.ReportAllocs()
	b.SetBytes(sz)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = enc.Encode(slice[0], buf)
		buf.Reset()
	}
}

func BenchmarkEncodeValSkip(b *testing.B) {
	slice, sz := makeBenchData(1)
	enc := NewGenericEncoder[encodeBenchStruct]()
	s, err := enc.enc.schema.DeleteField(2)
	require.NoError(b, err)
	s, err = s.DeleteField(4)
	require.NoError(b, err)
	enc.enc.schema = s
	buf := enc.NewBuffer(1)
	b.ReportAllocs()
	b.SetBytes(sz)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = enc.Encode(slice[0], buf)
		buf.Reset()
	}
}

func BenchmarkEncodePtr(b *testing.B) {
	slice, sz := makeBenchData(1)
	enc := NewGenericEncoder[encodeBenchStruct]()
	buf := enc.NewBuffer(1)
	b.ReportAllocs()
	b.SetBytes(sz)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = enc.EncodePtr(&slice[0], buf)
		buf.Reset()
	}
}

func BenchmarkEncodeSlice(b *testing.B) {
	enc := NewGenericEncoder[encodeBenchStruct]()
	for _, n := range encodeBenchmarkSizes {
		b.Run(n.name, func(b *testing.B) {
			slice, sz := makeBenchData(n.num)
			buf := enc.NewBuffer(n.num)
			b.ReportAllocs()
			b.SetBytes(sz)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = enc.EncodeSlice(slice, buf)
				buf.Reset()
			}
		})
	}
}

func BenchmarkEncodePtrSlice(b *testing.B) {
	enc := NewGenericEncoder[encodeBenchStruct]()
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
			for i := 0; i < b.N; i++ {
				_, _ = enc.EncodePtrSlice(ptrslice, buf)
				buf.Reset()
			}
		})
	}
}

func BenchmarkMemcopy(b *testing.B) {
	for _, n := range encodeBenchmarkSizes {
		b.Run(n.name, func(b *testing.B) {
			slice, sz := makeBenchData(n.num)
			enc := NewGenericEncoder[encodeBenchStruct]()
			buf := enc.NewBuffer(n.num)
			_, err := enc.EncodeSlice(slice, buf)
			require.NoError(b, err)
			dst := make([]byte, buf.Len())
			b.ReportAllocs()
			b.SetBytes(sz)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				copy(dst, buf.Bytes())
			}
		})
	}
}

func BenchmarkDecodeVal(b *testing.B) {
	slice, sz := makeBenchData(1)
	enc := NewGenericEncoder[encodeBenchStruct]()
	dec := NewGenericDecoder[encodeBenchStruct]()
	buf := enc.NewBuffer(1)
	_, err := enc.Encode(slice[0], buf)
	require.NoError(b, err)
	b.ReportAllocs()
	b.SetBytes(sz)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = dec.Decode(buf.Bytes(), nil)
	}
}

func BenchmarkDecodeTo(b *testing.B) {
	slice, sz := makeBenchData(1)
	enc := NewGenericEncoder[encodeBenchStruct]()
	dec := NewGenericDecoder[encodeBenchStruct]()
	buf := enc.NewBuffer(1)
	_, err := enc.Encode(slice[0], buf)
	require.NoError(b, err)
	var val encodeBenchStruct
	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(sz)
	for i := 0; i < b.N; i++ {
		_, _ = dec.Decode(buf.Bytes(), &val)
	}
}

func BenchmarkDecodeSlice(b *testing.B) {
	enc := NewGenericEncoder[encodeBenchStruct]()
	dec := NewGenericDecoder[encodeBenchStruct]()
	for _, n := range encodeBenchmarkSizes {
		b.Run(n.name, func(b *testing.B) {
			slice, sz := makeBenchData(n.num)
			buf := enc.NewBuffer(n.num)
			_, _ = enc.EncodeSlice(slice, buf)
			b.ReportAllocs()
			b.SetBytes(sz)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = dec.DecodeSlice(buf.Bytes(), nil)
			}
		})
	}
}

func BenchmarkDecodeSliceNoAlloc(b *testing.B) {
	enc := NewGenericEncoder[encodeBenchStruct]()
	dec := NewGenericDecoder[encodeBenchStruct]()
	for _, n := range encodeBenchmarkSizes {
		b.Run(n.name, func(b *testing.B) {
			slice, sz := makeBenchData(n.num)
			buf := enc.NewBuffer(n.num)
			_, _ = enc.EncodeSlice(slice, buf)
			res := make([]encodeBenchStruct, n.num)
			b.ReportAllocs()
			b.SetBytes(sz)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = dec.DecodeSlice(buf.Bytes(), res)
			}
		})
	}
}

func BenchmarkDecodeSliceRead(b *testing.B) {
	enc := NewGenericEncoder[encodeBenchStruct]()
	dec := NewGenericDecoder[encodeBenchStruct]()
	for _, n := range encodeBenchmarkSizes {
		b.Run(n.name, func(b *testing.B) {
			slice, sz := makeBenchData(n.num)
			buf := enc.NewBuffer(n.num)
			_, _ = enc.EncodeSlice(slice, buf)
			b.ReportAllocs()
			b.SetBytes(sz)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rd := bytes.NewBuffer(buf.Bytes())
				for {
					_, err := dec.Read(rd)
					if err != nil {
						break
					}
				}
			}
		})
	}
}
