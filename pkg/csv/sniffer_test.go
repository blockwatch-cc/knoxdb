// Copyright (c) 025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package csv

import (
	"strings"
	"testing"

	"blockwatch.cc/knoxdb/pkg/schema"
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

type fieldTest struct {
	name  string
	src   []string
	flag  fieldFlag
	len   int
	typ   schema.FieldType
	scale int
}

var fieldTests = []fieldTest{
	{"u8", []string{"1", "2", "12"}, fNum | fDecimal, 2, schema.Uint8, 0},
	{"i8", []string{"-1", "2", "12"}, fSign | fNum | fDecimal, 2, schema.Int8, 0},
	{"u16", []string{"1", "2", "256"}, fNum | fDecimal, 3, schema.Uint16, 0},
	{"i16", []string{"-1", "2", "256"}, fSign | fNum | fDecimal, 3, schema.Int16, 0},
	{"u32", []string{"1", "2", "65537"}, fNum | fDecimal, 5, schema.Uint32, 0},
	{"i32", []string{"-1", "2", "65537"}, fSign | fNum | fDecimal, 5, schema.Int32, 0},
	{"u64", []string{"1", "2", "4294967296"}, fNum | fDecimal, 10, schema.Uint64, 0},
	{"i64", []string{"-1", "2", "4294967296"}, fSign | fNum | fDecimal, 10, schema.Int64, 0},
	{"i128", []string{"-1", "2", "18446744073709551616"}, fSign | fNum | fDecimal, 20, schema.Int128, 0},
	{"i256", []string{"-1", "2", "340282366920938463463374607431768211455"}, fSign | fNum | fDecimal, 39, schema.Int256, 0},
	{"big", []string{"1", "2", "115792089237316195423570985008687907853269984665640564039457584007913129639935"}, fNum | fDecimal, 78, schema.Bigint, 0},
	{"bool", []string{"true", "false", "TRUE", "FALSE", "y", "Y", "n", "N", "null", ""}, fBool | fNull | fEmpty, 0, schema.Boolean, 0},
	{"f64", []string{"NaN", "+Inf", "-Inf", "null", "", "1.2", "-1.2", "1e+1", "10E-1", "-1e+1"}, fSign | fNum | fNull | fDecimal | fExp | fDot | fEmpty, 5, schema.Float64, 0},
	{"d32", []string{"-1.0001", "2.0002", "65537.0003"}, fSign | fNum | fDecimal | fDot, 10, schema.Decimal32, 0},
	{"d64", []string{"-1.0001", "2.0002", "429496.0003"}, fSign | fNum | fDecimal | fDot, 11, schema.Decimal64, 0},
	{"d128", []string{"-1.0001", "2.0002", "1844674407370955.0003"}, fSign | fNum | fDecimal | fDot, 21, schema.Decimal128, 0},
	{"d256", []string{"-1.0001", "2.0002", "34028236692093846346337460743176821.0003"}, fSign | fNum | fDecimal | fDot, 40, schema.Decimal256, 0},
	{"string", []string{"Hello", `"quote me 1"`, "1up"}, fQuoted | fNum | fDecimal | fOther, 12, schema.String, 0},
	{"byte", []string{"0xFF", "0123456789aAbBcCdDeEfF"}, fZerox | fNum | fDecimal | fHex, 22, schema.Bytes, 0},
	{"timestamp_s", []string{"2023-05-17 12:34:56 UTC"}, fNum | fDecimal | fDash | fOther | fTimestamp | fFixed, 23, schema.Timestamp, 3},
	{"timestamp_ms", []string{"2023-05-17 12:34:56.001 UTC"}, fNum | fDecimal | fDash | fOther | fDot | fTimestamp | fFixed, 27, schema.Timestamp, 2},
	{"timestamp_us", []string{"2023-05-17 12:34:56.000001 UTC"}, fNum | fDecimal | fDash | fOther | fDot | fTimestamp | fFixed, 30, schema.Timestamp, 1},
	{"timestamp_ns", []string{"2023-05-17 12:34:56.000000001 UTC"}, fNum | fDecimal | fDash | fOther | fDot | fTimestamp | fFixed, 33, schema.Timestamp, 0},
	{"time_s", []string{"12:34:56"}, fNum | fDecimal | fOther | fTime | fFixed, 8, schema.Time, 3},
	{"time_ms", []string{"12:34:56.001"}, fNum | fDecimal | fOther | fDot | fTime | fFixed, 12, schema.Time, 2},
	{"time_us", []string{"12:34:56.000001"}, fNum | fDecimal | fOther | fDot | fTime | fFixed, 15, schema.Time, 1},
	{"time_ns", []string{"12:34:56.000000001"}, fNum | fDecimal | fOther | fDot | fTime | fFixed, 18, schema.Time, 0},
	{"date", []string{"2023-05-17"}, fNum | fDecimal | fDash | fDate | fFixed, 10, schema.Date, 4},
	{"dur_s", []string{"1h1s"}, fNum | fDecimal | fOther | fFixed | fDuration, 4, schema.Duration, 3},
	{"dur_ms", []string{"1h10ms"}, fNum | fDecimal | fOther | fFixed | fDuration, 6, schema.Duration, 2},
	{"dur_us", []string{"1h1us"}, fNum | fDecimal | fOther | fFixed | fDuration, 5, schema.Duration, 1},
	{"dur_ns", []string{"1h123ns"}, fNum | fDecimal | fOther | fFixed | fDuration, 7, schema.Duration, 0},
	// {"uuid", []string{"75fcf875-017d-4579-bfd9-791d3e6767f0"}, fNum | fDecimal | fHex | fDash, 36, schema.UintUID},
}

func TestFieldDetect(t *testing.T) {
	for _, c := range fieldTests {
		t.Run(c.name, func(t *testing.T) {
			f := newField([]byte(c.src[0]), "", "")
			for _, v := range c.src[1:] {
				f.update([]byte(v), "", "")
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
	typs  []schema.FieldType
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
		[]schema.FieldType{schema.String, schema.Uint8, schema.Decimal32, schema.Boolean},
		[]string{"s", "i", "f", "b"},
	},
	{
		"WithoutHeader",
		CsvWithoutHeader,
		SnifferResult{
			Sep:       ',',
			NumFields: 4,
		},
		[]schema.FieldType{schema.String, schema.Uint8, schema.Decimal32, schema.Boolean},
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
		[]schema.FieldType{schema.String, schema.Uint8, schema.Decimal32, schema.Boolean},
		[]string{"f_0", "f_1", "f_2", "f_3"},
	},
	{
		"Unicode",
		CsvUnicode,
		SnifferResult{
			Sep:       ',',
			NumFields: 4,
		},
		[]schema.FieldType{schema.Uint8, schema.String, schema.String, schema.String},
		[]string{"f_0", "f_1", "f_2", "f_3"},
	},
	{
		"Semicolon",
		CsvSemicolon,
		SnifferResult{
			Sep:       ';',
			NumFields: 4,
		},
		[]schema.FieldType{schema.String, schema.Uint8, schema.Decimal32, schema.Boolean},
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
		[]schema.FieldType{schema.String, schema.Uint8, schema.Decimal32, schema.Boolean},
		[]string{"f_0", "f_1", "f_2", "f_3"},
	},
	{
		"EmptyLine",
		CsvEmptyLine,
		SnifferResult{
			Sep:       ',',
			NumFields: 4,
		},
		[]schema.FieldType{schema.String, schema.Uint8, schema.Decimal32, schema.Boolean},
		[]string{"f_0", "f_1", "f_2", "f_3"},
	},
	{
		"EmptyField",
		CsvEmptyField,
		SnifferResult{
			Sep:       ',',
			NumFields: 4,
		},
		[]schema.FieldType{schema.String, schema.Uint8, schema.Decimal32, schema.Boolean},
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
		[]schema.FieldType{schema.String, schema.Uint8, schema.Decimal32, schema.Boolean},
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
		[]schema.FieldType{schema.String, schema.Uint8, schema.Decimal32, schema.String},
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
		[]schema.FieldType{schema.String, schema.String, schema.String, schema.String},
		[]string{"Hello_World", "Hello", "World", "World2"},
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
		[]schema.FieldType{
			schema.String, schema.Timestamp, schema.String, schema.String, schema.Timestamp,
			schema.String, schema.String, schema.String, schema.String, schema.String,
			schema.String, schema.String, schema.String, schema.String, schema.String,
			schema.String, schema.String, schema.String, schema.String, schema.String,
			schema.String, schema.String, schema.String, schema.String, schema.String,
		},
		[]string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y"},
	},
	{
		"custome-time",
		"text,2024-01-01 00:00:00 UTC",
		SnifferResult{
			NumFields:  2,
			Sep:        ',',
			HasHeader:  false,
			HasTime:    true,
			TimeFormat: "2006-01-02 15:04:05 UTC",
		},
		[]schema.FieldType{schema.String, schema.Timestamp},
		[]string{"f_0", "f_1"},
	},
	{
		"sui",
		"object_id,type_,checkpoint,epoch,timestamp_ms,timestamp,owner_type,owner_address,object_status,previous_transaction,coin_balance,coin_type\n0x17779c308d62c710e11d1fa7001fed3f35a08a7dd513f70058e6038ed89fdcfe,0x2::coin::Coin<0x2::sui::SUI>,22438697,264,1704146400476,2024-01-01 22:00:00.476 UTC,AddressOwner,0xb9ae47efc627fc4ba4b873bc57f07a473ec78a527d311074b740c010d1f26387,Mutated,JEDDEA5j6ioZmHaST67XFWhrYnboXhQmSjCvc57dF1q8,52123668497,0x2::sui::SUI",
		SnifferResult{
			NumFields:  12,
			Sep:        ',',
			HasHeader:  true,
			HasTime:    true,
			TimeFormat: "2006-01-02 15:04:05.000 UTC",
		},
		[]schema.FieldType{schema.Bytes, schema.String, schema.Uint32, schema.Uint16, schema.Uint64, schema.Timestamp, schema.String, schema.Bytes, schema.String, schema.String, schema.Uint64, schema.String},
		[]string{"object_id", "type", "checkpoint", "epoch", "timestamp_ms", "timestamp", "owner_type", "owner_address", "object_status", "previous_transaction", "coin_balance", "coin_type"},
	},
	{
		"neon",
		"startDateTime,endDateTime,RHMean,RHMinimum,RHMaximum,RHVariance,RHNumPts,RHExpUncert,RHStdErMean,RHFinalQF,tempRHMean,tempRHMinimum,tempRHMaximum,tempRHVariance,tempRHNumPts,tempRHExpUncert,tempRHStdErMean,tempRHFinalQF,dewTempMean,dewTempMinimum,dewTempMaximum,dewTempVariance,dewTempNumPts,dewTempExpUncert,dewTempStdErMean,dewTempFinalQF\n\"2018-02-07T00:00:00Z\",\"2018-02-07T00:01:00Z\",83.66,83.66,83.66,0.00,1,0.19,,0,15.389,15.389,15.389,0.000,1,2.200,,0,12.592,12.592,12.592,0.000,1,0.885,,0",
		SnifferResult{
			NumFields:  26,
			Sep:        ',',
			HasQuotes:  true,
			HasHeader:  true,
			HasTime:    true,
			TimeFormat: "2006-01-02T15:04:05Z07:00",
		},
		[]schema.FieldType{
			schema.Timestamp, schema.Timestamp, schema.Decimal32, schema.Decimal32, schema.Decimal32, schema.Decimal32,
			schema.Uint8, schema.Decimal32, schema.String, schema.Uint8, schema.Decimal32, schema.Decimal32, schema.Decimal32, schema.Decimal32,
			schema.Uint8, schema.Decimal32, schema.String, schema.Uint8, schema.Decimal32, schema.Decimal32, schema.Decimal32, schema.Decimal32,
			schema.Uint8, schema.Decimal32, schema.String, schema.Uint8,
		},
		[]string{"startDateTime", "endDateTime", "RHMean", "RHMinimum", "RHMaximum", "RHVariance", "RHNumPts", "RHExpUncert", "RHStdErMean", "RHFinalQF", "tempRHMean", "tempRHMinimum", "tempRHMaximum", "tempRHVariance", "tempRHNumPts", "tempRHExpUncert", "tempRHStdErMean", "tempRHFinalQF", "dewTempMean", "dewTempMinimum", "dewTempMaximum", "dewTempVariance", "dewTempNumPts", "dewTempExpUncert", "dewTempStdErMean", "dewTempFinalQF"},
	},
}

func TestSniffer(t *testing.T) {
	for _, c := range sniffTests {
		t.Run(c.name, func(t *testing.T) {
			s := NewSniffer(strings.NewReader(c.src), -1)
			require.NoError(t, s.Sniff())
			require.Equal(t, c.res, s.Result())
			sc := s.Schema()
			for i, f := range sc.Fields {
				require.Equal(t, c.names[i], f.Name)
				require.Equal(t, c.typs[i], f.Type, "field=%s[%d] want=%s have=%s", f.Name, i, c.typs[i], f.Type)
			}
		})
	}
}

func BenchmarkSniffer(b *testing.B) {
	for b.Loop() {
		rd := NewInfReader(netBench)
		rd.Reset()
		s := NewSniffer(rd, 0)
		require.NoError(b, s.Sniff())
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(netBench)))
	b.ReportMetric(float64(25000*b.N)/float64(b.Elapsed().Nanoseconds()), "fields/ns")
}
