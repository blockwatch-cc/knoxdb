// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"bytes"
	"fmt"
	"testing"

	"blockwatch.cc/knoxdb/pkg/schema/reflect"
	"github.com/stretchr/testify/require"
)

func TestReadStruct(t *testing.T) {
	for _, v := range testStructs {
		t.Run(fmt.Sprintf("%T", v), func(t *testing.T) {
			pkg := makeTypedPackage(v, PACK_SIZE)
			s, err := reflect.SchemaOf(v)
			require.NoError(t, err)
			l, err := reflect.LayoutOf(v, s)
			require.NoError(t, err)
			maps, err := s.MapSchema(s)
			require.NoError(t, err)
			for i := range PACK_SIZE {
				err := pkg.ReadStruct(i, v, s, l, maps)
				require.NoError(t, err)
			}
		})
	}
}

func TestReadChildStruct(t *testing.T) {
	pkg := makeTypedPackage(&encodeTestStruct{}, PACK_SIZE)
	dst := &encodeTestSubStruct{}
	dstSchema, err := reflect.SchemaOf(dst)
	require.NoError(t, err)
	dstLayout, err := reflect.LayoutOf(dst, dstSchema)
	require.NoError(t, err)
	maps, err := pkg.schema.MapSchema(dstSchema)
	require.NoError(t, err)
	for i := range PACK_SIZE {
		err := pkg.ReadStruct(i, dst, dstSchema, dstLayout, maps)
		require.NoError(t, err)
	}
}

func BenchmarkReadStruct(b *testing.B) {
	for _, v := range testStructs {
		pkg := makeTypedPackage(v, PACK_SIZE)
		s, _ := reflect.SchemaOf(v)
		l, _ := reflect.LayoutOf(v, s)
		maps, _ := s.MapSchema(s)
		b.Run(fmt.Sprintf("%T/%d", v, PACK_SIZE), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				for k := range PACK_SIZE {
					_ = pkg.ReadStruct(k, v, s, l, maps)
				}
			}
			b.ReportMetric(float64(PACK_SIZE*b.N)/float64(b.Elapsed().Nanoseconds()), "rec/ns")
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(PACK_SIZE*b.N), "ns/rec")
		})
	}
}

func BenchmarkReadRow(b *testing.B) {
	for _, v := range testStructs {
		pkg := makeTypedPackage(v, PACK_SIZE)
		dst := make([]any, pkg.Cols())
		b.Run(fmt.Sprintf("%T/%d", v, pkg.Len()), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				for i := range PACK_SIZE {
					dst = pkg.ReadRow(i, dst)
				}
			}
			b.ReportMetric(float64(PACK_SIZE*b.N)/float64(b.Elapsed().Nanoseconds()), "rec/ns")
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(PACK_SIZE*b.N), "ns/rec")
		})
	}
}

func BenchmarkReadWire(b *testing.B) {
	for _, v := range testStructs {
		pkg := makeTypedPackage(v, PACK_SIZE)
		buf := bytes.NewBuffer(make([]byte, 0, pkg.schema.EstWireSize))
		b.Run(fmt.Sprintf("%T/%d", v, pkg.Len()), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				for i := range PACK_SIZE {
					buf.Reset()
					pkg.ReadWireBuffer(buf, i)
				}
			}
			b.ReportMetric(float64(PACK_SIZE*b.N)/float64(b.Elapsed().Nanoseconds()), "rec/ns")
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(PACK_SIZE*b.N), "ns/rec")
		})
	}
}

func BenchmarkReadWireE2E(b *testing.B) {
	for _, v := range testStructs {
		pkg := makeTypedPackage(v, PACK_SIZE)
		buf := bytes.NewBuffer(make([]byte, 0, pkg.schema.EstWireSize))
		b.Run(fmt.Sprintf("%T/%d", v, PACK_SIZE), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				for i := range PACK_SIZE {
					buf.Reset()
					pkg.ReadWireBuffer(buf, i)
					_ = v.Decode(buf.Bytes())
				}
			}
			b.ReportMetric(float64(PACK_SIZE*b.N)/float64(b.Elapsed().Nanoseconds()), "rec/ns")
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(PACK_SIZE*b.N), "ns/rec")
		})
	}
}
