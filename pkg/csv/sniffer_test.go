// Copyright (c) 025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package csv

import (
	"strings"
	"testing"

	"blockwatch.cc/knoxdb/internal/types"
	"github.com/stretchr/testify/require"
)

type splitTest struct {
	name string
	src  string
	n    int
}

var splitTests = []splitTest{
	{"WithoutHeader", CsvWithoutHeader, 4},
	{"Whitespace", CsvWhitespace, 4},
	{"Unicode", CsvUnicode, 4},
	{"WithQuotes", CsvWithQuotes, 4},
	{"WithQuotesAndSep", CsvWithQuotesAndSep, 4},
	{"WithQuotesAndSepEnd", CsvWithQuotesAndSepEnd, 4},
	{"WithDoubleQuotes", CsvWithDoubleQuotes, 4},
	{"WithTailQuotes", CsvWithTailQuotes, 4},
	{"empty1", ``, 1},
	{"empty4", `,,,`, 4},
	{"broken1", `",,,`, 1},
	{"broken2", `,",,`, 2},
	{"broken3", `,""",,`, 2},
}

func TestSplit(t *testing.T) {
	for _, c := range splitTests {
		t.Run(c.name, func(t *testing.T) {
			var (
				i   int
				res []string
			)
			for _, v := range Split([]byte(c.src), ',') {
				res = append(res, string(v))
				i++
			}
			require.Equal(t, c.n, i)
			require.Equal(t, strings.Join(res, ","), c.src)
		})
	}
}

const (
	FT_TIME   = types.FieldTypeDatetime
	FT_I64    = types.FieldTypeInt64
	FT_U64    = types.FieldTypeUint64
	FT_F64    = types.FieldTypeFloat64
	FT_BOOL   = types.FieldTypeBoolean
	FT_STRING = types.FieldTypeString
	FT_BYTES  = types.FieldTypeBytes
	FT_I32    = types.FieldTypeInt32
	FT_I16    = types.FieldTypeInt16
	FT_I8     = types.FieldTypeInt8
	FT_U32    = types.FieldTypeUint32
	FT_U16    = types.FieldTypeUint16
	FT_U8     = types.FieldTypeUint8
	FT_F32    = types.FieldTypeFloat32
	FT_I256   = types.FieldTypeInt256
	FT_I128   = types.FieldTypeInt128
	FT_D256   = types.FieldTypeDecimal256
	FT_D128   = types.FieldTypeDecimal128
	FT_D64    = types.FieldTypeDecimal64
	FT_D32    = types.FieldTypeDecimal32
	FT_BIGINT = types.FieldTypeBigint
)

type fieldTest struct {
	name string
	src  []string
	flag fieldFlag
	len  int
	typ  types.FieldType
}

var fieldTests = []fieldTest{
	{"u8", []string{"1", "2", "12"}, fNum | fDecimal, 2, FT_U8},
	{"i8", []string{"-1", "2", "12"}, fSign | fNum | fDecimal, 2, FT_I8},
	{"u16", []string{"1", "2", "256"}, fNum | fDecimal, 3, FT_U16},
	{"i16", []string{"-1", "2", "256"}, fSign | fNum | fDecimal, 3, FT_I16},
	{"u32", []string{"1", "2", "65537"}, fNum | fDecimal, 5, FT_U32},
	{"i32", []string{"-1", "2", "65537"}, fSign | fNum | fDecimal, 5, FT_I32},
	{"u64", []string{"1", "2", "4294967296"}, fNum | fDecimal, 10, FT_U64},
	{"i64", []string{"-1", "2", "4294967296"}, fSign | fNum | fDecimal, 10, FT_I64},
	{"i128", []string{"-1", "2", "18446744073709551616"}, fSign | fNum | fDecimal, 20, FT_I128},
	{"i256", []string{"-1", "2", "340282366920938463463374607431768211455"}, fSign | fNum | fDecimal, 39, FT_I256},
	{"big", []string{"1", "2", "115792089237316195423570985008687907853269984665640564039457584007913129639935"}, fNum | fDecimal, 78, FT_BIGINT},
	{"bool", []string{"true", "false", "TRUE", "FALSE", "y", "Y", "n", "N", "null", ""}, fBool | fNull | fEmpty, 0, FT_BOOL},
	{"f64", []string{"NaN", "+Inf", "-Inf", "null", "", "1.2", "-1.2", "1e+1", "10E-1", "-1e+1"}, fSign | fNum | fNull | fDecimal | fExp | fDot | fEmpty, 5, FT_F64},
	{"d32", []string{"-1.0001", "2.0002", "65537.0003"}, fSign | fNum | fDecimal | fDot, 10, FT_D32},
	{"d64", []string{"-1.0001", "2.0002", "429496.0003"}, fSign | fNum | fDecimal | fDot, 11, FT_D64},
	{"d128", []string{"-1.0001", "2.0002", "1844674407370955.0003"}, fSign | fNum | fDecimal | fDot, 21, FT_D128},
	{"d256", []string{"-1.0001", "2.0002", "34028236692093846346337460743176821.0003"}, fSign | fNum | fDecimal | fDot, 40, FT_D256},
	{"string", []string{"Hello", `"quote me 1"`, "1up"}, fQuoted | fNum | fDecimal | fOther, 12, FT_STRING},
	{"byte", []string{"0xFF", "0123456789aAbBcCdDeEfF"}, fZerox | fNum | fDecimal | fHex, 22, FT_BYTES},
	// {"uuid", []string{"75fcf875-017d-4579-bfd9-791d3e6767f0"}, fNum | fDecimal | fHex | fDash, 36, FT_UUID},
}

func TestFieldDetect(t *testing.T) {
	for _, c := range fieldTests {
		t.Run(c.name, func(t *testing.T) {
			f := newField([]byte(c.src[0]))
			for _, v := range c.src[1:] {
				f.update([]byte(v))
			}
			require.Equal(t, c.len, f.len)
			require.Equal(t, c.flag, f.flag, "want=%s have=%s", c.flag, f.flag)
			require.Equal(t, c.typ, f.Type(), "want=%s have=%s", c.typ, f.Type())
		})
	}
}

type sniffTest struct {
	name  string
	src   string
	res   SnifferResult
	typs  []types.FieldType
	names []string
}

var sniffTests = []sniffTest{
	{
		"WithHeader",
		CsvWithHeader,
		SnifferResult{
			Sep:       ',',
			HasHeader: true,
			NumFields: 4,
		},
		[]types.FieldType{FT_STRING, FT_U8, FT_D32, FT_BOOL},
		[]string{"s", "i", "f", "b"},
	},
	{
		"WithoutHeader",
		CsvWithoutHeader,
		SnifferResult{
			Sep:       ',',
			NumFields: 4,
		},
		[]types.FieldType{FT_STRING, FT_U8, FT_D32, FT_BOOL},
		[]string{"f_0", "f_1", "f_2", "f_3"},
	},
	{
		"Whitespace",
		CsvWhitespace,
		SnifferResult{
			Sep:       ',',
			NeedsTrim: true,
			NumFields: 4,
		},
		[]types.FieldType{FT_STRING, FT_U8, FT_D32, FT_BOOL},
		[]string{"f_0", "f_1", "f_2", "f_3"},
	},
	{
		"Unicode",
		CsvUnicode,
		SnifferResult{
			Sep:       ',',
			NumFields: 4,
		},
		[]types.FieldType{FT_U8, FT_STRING, FT_STRING, FT_STRING},
		[]string{"f_0", "f_1", "f_2", "f_3"},
	},
	{
		"Semicolon",
		CsvSemicolon,
		SnifferResult{
			Sep:       ';',
			NumFields: 4,
		},
		[]types.FieldType{FT_STRING, FT_U8, FT_D32, FT_BOOL},
		[]string{"f_0", "f_1", "f_2", "f_3"},
	},
	{
		"Comment",
		CsvComment,
		SnifferResult{
			Sep:         ',',
			HasComments: true,
			NumFields:   4,
		},
		[]types.FieldType{FT_STRING, FT_U8, FT_D32, FT_BOOL},
		[]string{"f_0", "f_1", "f_2", "f_3"},
	},
	{
		"EmptyLine",
		CsvEmptyLine,
		SnifferResult{
			Sep:       ',',
			NumFields: 4,
		},
		[]types.FieldType{FT_STRING, FT_U8, FT_D32, FT_BOOL},
		[]string{"f_0", "f_1", "f_2", "f_3"},
	},
	{
		"EmptyField",
		CsvEmptyField,
		SnifferResult{
			Sep:       ',',
			NumFields: 4,
		},
		[]types.FieldType{FT_STRING, FT_U8, FT_D32, FT_BOOL},
		[]string{"f_0", "f_1", "f_2", "f_3"},
	},
	{
		"NullField",
		CsvNullField,
		SnifferResult{
			Sep:       ',',
			HasNull:   true,
			NumFields: 4,
		},
		[]types.FieldType{FT_STRING, FT_U8, FT_D32, FT_BOOL},
		[]string{"f_0", "f_1", "f_2", "f_3"},
	},
	{
		"WithQuotes",
		CsvWithQuotes,
		SnifferResult{
			Sep:       ',',
			HasQuotes: true,
			NumFields: 4,
		},
		[]types.FieldType{FT_STRING, FT_U8, FT_D32, FT_STRING},
		[]string{"f_0", "f_1", "f_2", "f_3"},
	},
	{
		"WithDoubleQuotes",
		CsvWithDoubleQuotes,
		SnifferResult{
			Sep:       ',',
			HasQuotes: true,
			HasHeader: true, // single line with strings treated as head
			NeedsTrim: true,
			HasEscape: true,
			NumFields: 4,
		},
		[]types.FieldType{FT_STRING, FT_STRING, FT_STRING, FT_STRING},
		[]string{"Hello_World", "Hello", "World", "World"},
	},
	{
		"dotnet",
		"a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y\n75fcf875-017d-4579-bfd9-791d3e6767f0,2020-11-28T01:50:41.2449947+00:00,Akinzekeel.BlazorGrid,0.9.1-preview,2020-11-27T22:42:54.3100000+00:00,AvailableAssets,RuntimeAssemblies,,,net5.0,,,,,,lib/net5.0/BlazorGrid.dll,BlazorGrid.dll,.dll,lib,net5.0,.NETCoreApp,5.0.0.0,,,0.0.0.0",
		SnifferResult{
			NumFields:  25,
			Sep:        ',',
			HasHeader:  true, // single line with strings treated as head
			HasTime:    true,
			TimeFormat: "2006-01-02T15:04:05Z07:00",
		},
		[]types.FieldType{
			FT_STRING, FT_TIME, FT_STRING, FT_STRING, FT_TIME,
			FT_STRING, FT_STRING, FT_STRING, FT_STRING, FT_STRING,
			FT_STRING, FT_STRING, FT_STRING, FT_STRING, FT_STRING,
			FT_STRING, FT_STRING, FT_STRING, FT_STRING, FT_STRING,
			FT_STRING, FT_STRING, FT_STRING, FT_STRING, FT_STRING,
		},
		[]string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y"},
	},
}

func TestSniffer(t *testing.T) {
	for _, c := range sniffTests {
		t.Run(c.name, func(t *testing.T) {
			s := NewSniffer(strings.NewReader(c.src), -1)
			require.NoError(t, s.Sniff())
			require.Equal(t, c.res, s.Result())
			sc := s.Schema()
			for i, f := range sc.Exported() {
				require.Equal(t, c.names[i], f.Name)
				require.Equal(t, c.typs[i], f.Type, "field=%s[%d] want=%s have=%s", f.Name, i, c.typs[i], f.Type)
			}
		})
	}
}

func BenchmarkSniffer(b *testing.B) {
	for b.Loop() {
		rd := NewInfReader(netBench)
		s := NewSniffer(rd, 0)
		require.NoError(b, s.Sniff())
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(netBench)))
	b.ReportMetric(float64(25000*b.N)/float64(b.Elapsed().Nanoseconds()), "fields/ns")
}
