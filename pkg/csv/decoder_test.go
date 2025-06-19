// Copyright (c) 025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package csv

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/require"
)

// CSV default types
type A struct {
	String string  `csv:"s" knox:"s"`
	Int    int64   `csv:"i" knox:"i"`
	Float  float64 `csv:"f" knox:"f"`
	Bool   bool    `csv:"b" knox:"b"`
}

// all supported types as schema compatible kinds
type SchemaB struct {
	Int64   int64          `knox:"i64"`
	Int32   int32          `knox:"i32"`
	Int16   int16          `knox:"i16"`
	Int8    int8           `knox:"i8"`
	Uint64  uint64         `knox:"u64"`
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
	Bool    bool           `knox:"bool"`
	Time    time.Time      `knox:"time,scale=s"`
	Hash    []byte         `knox:"bytes"`
	Array   [2]byte        `knox:"array2"`
	String  string         `knox:"string"`
	Big     num.Big        `knox:"big"`
}

// all supported types expressed by native Go types
type NativeB struct {
	Int64   int64    `knox:"i64"`
	Int32   int32    `knox:"i32"`
	Int16   int16    `knox:"i16"`
	Int8    int8     `knox:"i8"`
	Uint64  uint64   `knox:"u64"`
	Uint32  uint32   `knox:"u32"`
	Uint16  uint16   `knox:"u16"`
	Uint8   uint8    `knox:"u8"`
	Float64 float64  `knox:"f64"`
	Float32 float32  `knox:"f32"`
	D32     int32    `knox:"d32,scale=5"`
	D64     int64    `knox:"d64,scale=15"`
	D128    [16]byte `knox:"d128,scale=18"`
	D256    [32]byte `knox:"d256,scale=24"`
	I128    [16]byte `knox:"i128"`
	I256    [32]byte `knox:"i256"`
	Bool    bool     `knox:"bool"`
	Time    int64    `knox:"time,scale=s"`
	Hash    []byte   `knox:"bytes"`
	Array   [2]byte  `knox:"array2"`
	String  string   `knox:"string"`
	Big     []byte   `knox:"big"`
}

var (
	A1V = A{"Hello", 42, 23.45, true}
	A3V = A{"  Hello  ", 42, 23.45, true}

	CsvB = `-1,-1,-1,-1,1,1,1,1,1.1,1.1,1.00001,1.000000000000001,1.000000000000000001,1.000000000000000000000001,1,1,true,2026-06-07T02:00:01Z,787878,4141,sss,1234`
	BV   = NativeB{
		Int64:   -1,
		Int32:   -1,
		Int16:   -1,
		Int8:    -1,
		Uint64:  1,
		Uint32:  1,
		Uint16:  1,
		Uint8:   1,
		Float64: 1.1,
		Float32: 1.1,
		D32:     100001,
		D64:     1000000000000001,
		D128:    num.Int128FromInt64(1000000000000000001).Bytes16(),
		D256:    num.MustParseDecimal256("1.000000000000000000000001").Int256().Bytes32(),
		I128:    num.Int128FromInt64(1).Bytes16(),
		I256:    num.Int256FromInt64(1).Bytes32(),
		Bool:    true,
		Time:    util.MustParseTime("2026-06-07T02:00:01Z").Unix(),
		Hash:    []byte{0x78, 0x78, 0x78},
		Array:   [2]byte{0x41, 0x41},
		String:  "sss",
		Big:     num.NewBig(1234).Bytes(),
	}
)

type decoderTest struct {
	Name   string
	Csv    string
	Header bool
	Trim   bool
	Res    any
	Err    bool
}

var DecoderCases = []decoderTest{
	{"WithHeader", CsvWithHeader, true, false, &A1V, false},
	{"NoHeader", CsvWithoutHeader, false, false, &A1V, false},
	{"Comment", CsvComment, false, false, &A1V, false},
	{"Trim", CsvWhitespace, false, true, &A1V, false},
	{"Empty", CsvEmptyField, false, true, []*A{
		{"", 42, 23.45, true},
		{"Hello", 0, 23.45, true},
		{"Hello", 42, 0.0, true},
		{"Hello", 42, 23.45, false},
		{"", 0, 0.0, false},
	}, false},
	{"Null", CsvNullField, false, true, []*A{
		{"", 42, 23.45, true},
		{"Hello", 0, 23.45, true},
		{"Hello", 42, 0.0, true},
		{"Hello", 42, 23.45, false},
		{"", 0, 0.0, false},
	}, false},
}

func TestDecodeSimple(t *testing.T) {
	for _, c := range DecoderCases {
		t.Run(c.Name, func(t *testing.T) {
			s, err := schema.SchemaOfTag(A{}, "csv")
			require.NoError(t, err)
			dec := NewDecoder(s, strings.NewReader(c.Csv)).WithTrim(c.Trim).WithHeader(c.Header)
			switch v := c.Res.(type) {
			case *A:
				val, err := dec.Decode()
				require.NoError(t, err)
				require.Equal(t, fmt.Sprintf("%v", c.Res), fmt.Sprintf("%v", val))
			case []*A:
				for _, v := range v {
					val, err := dec.Decode()
					require.NoError(t, err)
					require.Equal(t, fmt.Sprintf("%v", v), fmt.Sprintf("%v", val))
				}
			}
		})
	}
}

func TestDecodeWithSchema(t *testing.T) {
	b := schema.NewBuilder().String("s").Int64("i").Float64("f").Bool("b").Finalize()
	require.NoError(t, b.Validate())
	for _, c := range DecoderCases {
		t.Run(c.Name, func(t *testing.T) {
			dec := NewDecoder(b.Schema(), strings.NewReader(c.Csv)).WithTrim(c.Trim).WithHeader(c.Header)
			switch v := c.Res.(type) {
			case *A:
				val, err := dec.Decode()
				require.NoError(t, err)
				require.Equal(t, fmt.Sprintf("%v", c.Res), fmt.Sprintf("%v", val))
			case []*A:
				for _, v := range v {
					val, err := dec.Decode()
					require.NoError(t, err)
					require.Equal(t, fmt.Sprintf("%v", v), fmt.Sprintf("%v", val))
				}
			}
		})
	}
}

func TestDecodeWithType(t *testing.T) {
	s, err := schema.SchemaOf(SchemaB{})
	require.NoError(t, err)
	dec := NewDecoder(s, strings.NewReader(CsvB)).WithHeader(false)
	val, err := dec.Decode()
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("%v", &BV), fmt.Sprintf("%v", val))
}

func BenchmarkDecoder(b *testing.B) {
	s := NewSniffer(strings.NewReader(netBench), 0)
	require.NoError(b, s.Sniff())
	dec := s.NewDecoder(strings.NewReader(netBench))
	dst := dec.MakeSlice(1024)
	var N int
	for b.Loop() {
		N = 0
		dec.Reset(strings.NewReader(netBench))
		for {
			n, err := dec.DecodeSlice(dst)
			require.NoError(b, err)
			N += n
			if n == 0 {
				break
			}
		}
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(netBench)))
	b.ReportMetric(float64(25*N*b.N)/float64(b.Elapsed().Nanoseconds()), "fields/ns")
}
