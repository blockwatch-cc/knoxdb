// Copyright (c) 025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package csv

import (
	"bytes"
	"io"
	"testing"

	"blockwatch.cc/knoxdb/pkg/schema"
	"github.com/stretchr/testify/require"
)

type encoderTest struct {
	Name   string
	Src    any
	Schema *schema.Schema
	Sep    rune
	Csv    string
	Header bool
	Trim   bool
	Err    bool
}

var EncoderCases = []encoderTest{
	{
		Name:   "WithHeader",
		Src:    &A1V,
		Schema: schema.MustSchemaOf(A{}),
		Sep:    ',',
		Csv:    "s,i,f,b\nHello,42,23.45,true\n",
		Header: true,
		Trim:   false,
		Err:    false,
	},
	{
		Name:   "NoHeader",
		Src:    &A1V,
		Schema: schema.MustSchemaOf(A{}),
		Sep:    ',',
		Csv:    "Hello,42,23.45,true\n",
		Header: false,
		Trim:   false,
		Err:    false,
	},
	{
		Name:   "Trim",
		Src:    &A3V,
		Schema: schema.MustSchemaOf(A{}),
		Sep:    ',',
		Csv:    "Hello,42,23.45,true\n",
		Header: false,
		Trim:   true,
		Err:    false,
	},
	{
		Name:   "NoTrim",
		Src:    &A3V,
		Schema: schema.MustSchemaOf(A{}),
		Sep:    ',',
		Csv:    "  Hello  ,42,23.45,true\n",
		Header: false,
		Trim:   false,
		Err:    false,
	},
	{
		Name:   "Semicolon",
		Src:    &A1V,
		Schema: schema.MustSchemaOf(A{}),
		Sep:    ';',
		Csv:    "Hello;42;23.45;true\n",
		Header: false,
		Trim:   false,
		Err:    false,
	},
	{
		Name:   "Alltypes",
		Src:    &BV,
		Schema: schema.MustSchemaOf(SchemaB{}),
		Sep:    ',',
		Csv:    "-1,-1,-1,-1,1,1,1,1,1.1,1.1,1.00001,1.000000000000001,1.000000000000000001,1.000000000000000000000001,1,1,true,2026-06-07T02:00:01Z,787878,4141,sss,1234\n",
		Header: false,
		Trim:   false,
		Err:    false,
	},
	{
		Name:   "Quote",
		Src:    &A{`Hello,me`, 1, 1.1, true},
		Schema: schema.MustSchemaOf(A{}),
		Sep:    ',',
		Csv:    "\"Hello,me\",1,1.1,true\n",
		Header: false,
		Trim:   false,
		Err:    false,
	},
	{
		Name:   "DoubleQuote",
		Src:    &A{`Hello,"me"`, 1, 0.1, true},
		Schema: schema.MustSchemaOf(A{}),
		Sep:    ',',
		Csv:    "\"Hello,\"\"me\"\"\",1,0.1,true\n",
		Header: false,
		Trim:   false,
		Err:    false,
	},
}

func TestEncode(t *testing.T) {
	for _, c := range EncoderCases {
		t.Run(c.Name, func(t *testing.T) {
			w := new(bytes.Buffer)
			enc := NewEncoder(c.Schema, w).
				WithHeader(c.Header).
				WithTrim(c.Trim).
				WithSeparator(c.Sep)
			require.NoError(t, enc.Encode(c.Src))
			require.Equal(t, c.Csv, w.String())
		})
	}
}

func TestEncodeSlice(t *testing.T) {
	w := new(bytes.Buffer)
	enc := NewEncoder(schema.MustSchemaOf(A{}), w).WithHeader(true)
	require.NoError(t, enc.Encode([]A{A1V, A3V, A1V}))
}

func BenchmarkEncoder(b *testing.B) {
	s := schema.MustSchemaOf(SchemaB{})
	enc := NewEncoder(s, io.Discard)
	for b.Loop() {
		require.NoError(b, enc.Encode(&BV))
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(CsvB)))
	b.ReportMetric(float64(22*b.N)/float64(b.Elapsed().Nanoseconds()), "fields/ns")
}
