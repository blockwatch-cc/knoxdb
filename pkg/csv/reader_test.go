// Copyright (c) 2018-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package csv

import (
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/echa/log"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	if buf, err := os.ReadFile("bench.csv"); err == nil {
		netBench = string(buf)
	} else {
		log.Warn(err)
		log.Warn("cloning internal CSV line with less data variablity")
		netBench = strings.Repeat(netBenchLine, 1695)
	}
	m.Run()
}

const (
	CsvWithHeader = `s,i,f,b
Hello,42,23.45,true`
	CsvWithoutHeader    = `Hello,42,23.45,true`
	CsvWhitespace       = `  Hello  ,  42  ,  23.45  ,  true  `
	CsvWhitespaceString = `  Hello  ,42,23.45,true`
	CsvUnicode          = `1,🚀,🌱,😎`
	CsvSemicolon        = `Hello;42;23.45;true`
	CsvComment          = `# Comment line
Hello,42,23.45,true
#
# another comment
Hello World,43,24.56,false`
	CsvEmptyLine = `
Hello,42,23.45,true

Hello World,43,24.56,false`
	CsvEmptyField = `,42,23.45,true
Hello,,23.45,true
Hello,42,,true
Hello,42,23.45,
,,,`
	CsvNullField = `null,42,23.45,true
Hello,null,23.45,true
Hello,42,null,true
Hello,42,23.45,null
null,null,null,null`
	CsvWithCRLF            = "s,i,f,b\r\nHello,42,23.45,true\r\nHello World,43,24.56,false\r\n"
	CsvWithoutLF           = "s,i,f,b\nHello,42,23.45,true\nHello World,43,24.56,false"
	CsvWithQuotes          = `"Hello",42,23.45,"true"`
	CsvWithQuotesAndSep    = `"Hello,world","world, one",",two","three,"`         // + "\n"
	CsvWithQuotesAndSepEnd = `"Hello,world","world, one","world","world,three"`   // + "\n"
	CsvWithDoubleQuotes    = `"Hello ""World""","""Hello""" ,"""World","World"""` // + "\n"

	CsvWithUnquotedQuotes = `a"a"a,a""a,a"""a,a""a`                        // + "\n"
	CsvWithMultipleQuotes = `"a"a","a"a"a","a"""a",""a",""a"a"",""","""""` // + "\n"
	CsvWithSpaceAndQuotes = ` "a" , "" , "", "a","" ,"a" `                 // + "\n"

	CsvWithTailQuotes   = `"Hello,World","Hello,my","World","Hello" world`     // + "\n"
	CsvWithBrokenQuotes = `"Hello,World"","Hello,my",""World","Hello" "world"` // + "\n"
	CsvExtraField       = `s,i,f,b
Hello,42,23.45,true,Unknown`
	CsvMissingField = `s,i,f,b
Hello,42,23.45`
	Issue_1_early_close = `1|DEPARTMENT OF STATE                               |343753471|"ANTON" SONNENSCHUTZSYSTEME GESELLSCHAFT MIT BESCHRÂ¿NKTER HAFTUNG| |2012`
	Issue_2_early_close = `186473|null|""Quality is a|null|mix|null|""Mix is an|Mix|0`

	netBenchLine = "75fcf875-017d-4579-bfd9-791d3e6767f0,2020-11-28T01:50:41.2449947+00:00,Akinzekeel.BlazorGrid,0.9.1-preview,2020-11-27T22:42:54.3100000+00:00,AvailableAssets,RuntimeAssemblies,,,net5.0,,,,,,lib/net5.0/BlazorGrid.dll,BlazorGrid.dll,.dll,lib,net5.0,.NETCoreApp,5.0.0.0,,,0.0.0.0\n"
)

type readerTest struct {
	name     string
	src      string
	sep      rune
	n        int
	res      []string
	head     []string
	isHeader bool
	isTrim   bool
	isStrict bool
}

var (
	Head = []string{"s", "i", "f", "b"}
	A1   = []string{"Hello", "42", "23.45", "true"}
	A2   = []string{"Hello World", "43", "24.56", "false"}
	A3   = []string{"  Hello  ", "  42  ", "  23.45  ", "  true  "}
	A4   = []string{"1", "🚀", "🌱", "😎"}

	// empty fields
	E1 = []string{"", "42", "23.45", "true"}
	E2 = []string{"Hello", "", "23.45", "true"}
	E3 = []string{"Hello", "42", "", "true"}
	E4 = []string{"Hello", "42", "23.45", ""}
	E5 = []string{"", "", "", ""}

	// quote tests
	Q1 = []string{`Hello,world`, `world, one`, `,two`, `three,`}       // CsvWithQuotesAndSep
	Q2 = []string{`Hello,world`, `world, one`, `world`, `world,three`} // CsvWithQuotesAndSepEnd
	Q3 = []string{`Hello "World"`, `"Hello"`, `"World`, `World"`}      // CsvWithDoubleQuotes
	Q4 = []string{`Hello,World`, `Hello,my`, `World`, `"Hello" world`} // CsvWithTailQuotes

	Q5 = []string{`a"a"a`, `a""a`, `a"""a`, `a""a`}                 // CsvWithUnquotedQuotes
	Q6 = []string{`a"a`, `a"a"a`, `a""a`, `"a`, `"a"a"`, `"`, `""`} // CsvWithMultipleQuotes
	Q7 = []string{`a`, ``, ``, `a`, ``, `a`}                        // CsvWithSpaceAndQuotes

	I1 = []string{"1", "DEPARTMENT OF STATE", "343753471", `"ANTON" SONNENSCHUTZSYSTEME GESELLSCHAFT MIT BESCHRÂ¿NKTER HAFTUNG`, "", "2012"}
	I2 = []string{"186473", "null", `""Quality is a`, "null", "mix", "null", `""Mix is an`, "Mix", "0"}

	netBench string
)

var readerCases = []readerTest{
	{
		name:     "WithHeader",
		src:      CsvWithHeader,
		sep:      ',',
		n:        4,
		res:      A1,
		head:     Head,
		isHeader: true,
		isStrict: true,
		isTrim:   true,
	},
	{
		name:     "WithoutHeader",
		src:      CsvWithoutHeader,
		sep:      ',',
		n:        4,
		res:      A1,
		isStrict: true,
		isTrim:   true,
	},
	{
		name:     "Whitespace",
		src:      CsvWhitespace,
		sep:      ',',
		n:        4,
		res:      A1,
		isTrim:   true,
		isStrict: true,
	},
	{
		name:     "Whitespace",
		src:      CsvWhitespace,
		sep:      ',',
		n:        4,
		res:      A3,
		isStrict: true,
		isTrim:   false,
	},
	{
		name:     "Semicolon",
		src:      CsvSemicolon,
		n:        4,
		res:      A1,
		sep:      ';',
		isStrict: true,
		isTrim:   true,
	},
	{
		name:     "Unicode",
		src:      CsvUnicode,
		n:        4,
		res:      A4,
		sep:      ',',
		isStrict: true,
		isTrim:   true,
	},
	{
		name:     "Comment",
		src:      CsvComment,
		n:        4,
		res:      A2,
		head:     A1,
		sep:      ',',
		isStrict: true,
		isTrim:   true,
	},
	{
		name:     "EmptyLine",
		src:      CsvEmptyLine,
		n:        4,
		head:     A1,
		res:      A2,
		sep:      ',',
		isStrict: true,
		isTrim:   true,
	},
	{
		name:     "WithQuotes",
		src:      CsvWithQuotes,
		n:        4,
		res:      A1,
		sep:      ',',
		isStrict: true,
		isTrim:   true,
	},
	{
		name:     "QuotesAndSep",
		src:      CsvWithQuotesAndSep,
		n:        4,
		res:      Q1,
		sep:      ',',
		isStrict: true,
		isTrim:   true,
	},
	{
		name:     "QuotesAndSepEnd",
		src:      CsvWithQuotesAndSepEnd,
		n:        4,
		res:      Q2,
		sep:      ',',
		isStrict: true,
		isTrim:   true,
	},
	{
		name:     "DoubleQuotes",
		src:      CsvWithDoubleQuotes,
		n:        4,
		res:      Q3,
		sep:      ',',
		isStrict: true,
		isTrim:   true,
	},
	{
		name:     "TailQuotes",
		src:      CsvWithTailQuotes,
		n:        4,
		res:      Q4,
		sep:      ',',
		isTrim:   true,
		isStrict: true,
	},
	{
		name:     "UnquotedQuotes",
		src:      CsvWithUnquotedQuotes,
		n:        4,
		res:      Q5,
		sep:      ',',
		isTrim:   true,
		isStrict: true,
	},
	{
		name:     "MultipleQuotes",
		src:      CsvWithMultipleQuotes,
		n:        7,
		res:      Q6,
		sep:      ',',
		isTrim:   true,
		isStrict: true,
	},
	{
		name:     "SpaceAndQuotes",
		src:      CsvWithSpaceAndQuotes,
		n:        6,
		res:      Q7,
		sep:      ',',
		isTrim:   true,
		isStrict: true,
	},
	{
		name:     "Issue_1_early_close",
		src:      Issue_1_early_close,
		n:        6,
		res:      I1,
		sep:      '|',
		isTrim:   true,
		isStrict: false,
	},
	{
		name:     "Issue_2_early_close",
		src:      Issue_2_early_close,
		n:        9,
		res:      I2,
		sep:      '|',
		isTrim:   true,
		isStrict: false,
	},
}

func TestReader(t *testing.T) {
	for _, c := range readerCases {
		t.Run(c.name, func(t *testing.T) {
			r := strings.NewReader(c.src)
			rd := NewReader(r, c.n).
				WithSeparator(c.sep).
				WithTrim(c.isTrim).
				WithStrictQuotes(c.isStrict)
			res, err := rd.Read()
			require.NoError(t, err)
			if c.head != nil {
				require.Equal(t, c.head, res)
				res, err = rd.Read()
				require.NoError(t, err)
			}
			require.Equal(t, c.res, res)
			res, err = rd.Read()
			require.ErrorIs(t, err, io.EOF)
			require.Nil(t, res)

		})
	}
}

func TestReadEmptyFields(t *testing.T) {
	r := strings.NewReader(CsvEmptyField)
	rd := NewReader(r, 4)
	res, err := rd.Read()
	require.NoError(t, err)
	require.Equal(t, E1, res)

	res, err = rd.Read()
	require.NoError(t, err)
	require.Equal(t, E2, res)

	res, err = rd.Read()
	require.NoError(t, err)
	require.Equal(t, E3, res)

	res, err = rd.Read()
	require.NoError(t, err)
	require.Equal(t, E4, res)

	res, err = rd.Read()
	require.NoError(t, err)
	require.Equal(t, E5, res)

	res, err = rd.Read()
	require.ErrorIs(t, err, io.EOF)
	require.Nil(t, res)
}

func TestReadExtraField(t *testing.T) {
	r := strings.NewReader(CsvExtraField)
	rd := NewReader(r, 4)
	res, err := rd.Read()
	require.NoError(t, err)
	require.Equal(t, Head, res)
	res, err = rd.Read()
	require.Error(t, err)
	require.Nil(t, res)
}

func TestReadMissingField(t *testing.T) {
	r := strings.NewReader(CsvMissingField)
	rd := NewReader(r, 4)
	res, err := rd.Read()
	require.NoError(t, err)
	require.Equal(t, Head, res)
	res, err = rd.Read()
	require.Error(t, err)
	require.Nil(t, res)
}

func TestReadCRLF(t *testing.T) {
	r := strings.NewReader(CsvWithCRLF)
	rd := NewReader(r, 4)
	res, err := rd.Read()
	require.NoError(t, err)
	require.Equal(t, Head, res)
	res, err = rd.Read()
	require.NoError(t, err)
	require.Equal(t, A1, res)
	res, err = rd.Read()
	require.NoError(t, err)
	require.Equal(t, A2, res)
	res, err = rd.Read()
	require.ErrorIs(t, err, io.EOF)
	require.Nil(t, res)
}

func TestLastRecordWithoutLF(t *testing.T) {
	r := strings.NewReader(CsvWithoutLF)
	rd := NewReader(r, 4)
	res, err := rd.Read()
	require.NoError(t, err)
	require.Equal(t, Head, res)
	res, err = rd.Read()
	require.NoError(t, err)
	require.Equal(t, A1, res)
	res, err = rd.Read()
	require.NoError(t, err)
	require.Equal(t, A2, res)
	res, err = rd.Read()
	require.ErrorIs(t, err, io.EOF)
	require.Nil(t, res)
}

type InfReader struct {
	buf []byte
	n   int
}

func NewInfReader(s string) *InfReader {
	return &InfReader{
		buf: []byte(s),
	}
}

func (r *InfReader) Read(b []byte) (int, error) {
	n := copy(b, r.buf[r.n:])
	r.n = (r.n + n) % len(r.buf)
	return n, nil
}

var benchFields = []int{4, 16, 32, 64}

func BenchmarkReadSimple(b *testing.B) {
	for _, sz := range benchFields {
		var sb strings.Builder
		sb.WriteString("field")
		for range sz - 1 {
			sb.WriteRune(',')
			sb.WriteString("field")
		}
		sb.WriteRune('\n')
		s := strings.Repeat(sb.String(), 1000)
		rd := NewReader(NewInfReader(s), sz).WithTrim(false)
		b.Run(fmt.Sprintf("%d", sz), func(b *testing.B) {
			for b.Loop() {
				_, _ = rd.Read()
			}
			b.SetBytes(int64(rd.BytesProcessed() / rd.LinesProcessed()))
			b.ReportMetric(float64(sz*b.N)/float64(b.Elapsed().Nanoseconds()), "fields/ns")
		})
	}
}

func BenchmarkReadQuoted(b *testing.B) {
	for _, sz := range benchFields {
		var sb strings.Builder
		sb.WriteString("field")
		for range sz - 3 {
			sb.WriteRune(',')
			sb.WriteString("field")
		}
		// add two quoted fields with separator
		sb.WriteRune(',')
		sb.WriteString(`"field,sep"`)
		sb.WriteRune(',')
		sb.WriteString(`"field,sep"`)
		sb.WriteRune('\n')
		s := strings.Repeat(sb.String(), 1000)
		rd := NewReader(NewInfReader(s), sz).WithTrim(false)
		b.Run(fmt.Sprintf("%d", sz), func(b *testing.B) {
			for b.Loop() {
				_, _ = rd.Read()
			}
			b.SetBytes(int64(rd.BytesProcessed() / rd.LinesProcessed()))
			b.ReportMetric(float64(sz*b.N)/float64(b.Elapsed().Nanoseconds()), "fields/ns")
		})
	}
}

func BenchmarkReadNet(b *testing.B) {
	rd := NewReader(NewInfReader(netBench), 25).WithTrim(false)
	for b.Loop() {
		_, _ = rd.Read()
	}
	b.ReportAllocs()
	b.SetBytes(int64(rd.BytesProcessed() / rd.LinesProcessed()))
	b.ReportMetric(float64(25*b.N)/float64(b.Elapsed().Nanoseconds()), "fields/ns")
}

// BenchmarkGoReadSimple/4        227.0 ns/op   105.74 MB/s             0.01762 fields/ns
// BenchmarkGoReadSimple/16        456.4 ns/op   210.34 MB/s             0.03506 fields/ns
// BenchmarkGoReadSimple/32        798.6 ns/op   240.41 MB/s             0.04007 fields/ns
// BenchmarkGoReadSimple/64       1393 ns/op     275.60 MB/s             0.04593 fields/ns
// func BenchmarkGoReadSimple(b *testing.B) {
// 	for _, sz := range benchFields {
// 		var sb strings.Builder
// 		sb.WriteString("field")
// 		for range sz - 1 {
// 			sb.WriteRune(',')
// 			sb.WriteString("field")
// 		}
// 		sb.WriteRune('\n')
// 		s := sb.String()
// 		rd := csv.NewReader(NewInfReader(s))
// 		b.Run(fmt.Sprintf("%d", sz), func(b *testing.B) {
// 			for b.Loop() {
// 				_, err := rd.Read()
// 				require.NoError(b, err)
// 			}
// 			b.SetBytes(int64(sz * 6))
// 			b.ReportMetric(float64(sz*b.N)/float64(b.Elapsed().Nanoseconds()), "fields/ns")
// 		})
// 	}
// }

// BenchmarkEchaReadSimple/4       281.7 ns/op    85.20 MB/s             0.01420 fields/ns
// BenchmarkEchaReadSimple/16      560.5 ns/op   171.27 MB/s             0.02854 fields/ns
// BenchmarkEchaReadSimple/32      927.8 ns/op   206.95 MB/s             0.03449 fields/ns
// BenchmarkEchaReadSimple/64     1631 ns/op     235.46 MB/s             0.03924 fields/ns
// func BenchmarkEchaReadSimple(b *testing.B) {
// 	for _, sz := range benchFields {
// 		var sb strings.Builder
// 		sb.WriteString("field")
// 		for range sz - 1 {
// 			sb.WriteRune(',')
// 			sb.WriteString("field")
// 		}
// 		sb.WriteRune('\n')
// 		s := sb.String()
// 		rd := csv2.NewDecoder(NewInfReader(s))
// 		type M struct {
// 			M map[string]string `csv:",any"`
// 		}
// 		m := &M{}
// 		b.Run(fmt.Sprintf("%d", sz), func(b *testing.B) {
// 			for b.Loop() {
// 				l, err := rd.ReadLine()
// 				require.NoError(b, err)
// 				rd.DecodeRecord(m, l)
// 			}
// 			b.SetBytes(int64(sz * 6))
// 			b.ReportMetric(float64(sz*b.N)/float64(b.Elapsed().Nanoseconds()), "fields/ns")
// 		})
// 	}
// }
