// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"bytes"
	"fmt"
	"testing"

	"blockwatch.cc/knoxdb/pkg/schema"
	"github.com/stretchr/testify/require"
)

func TestReadStruct(t *testing.T) {
	for _, v := range testStructs {
		t.Run(fmt.Sprintf("%T", v), func(t *testing.T) {
			pkg := makeTypedPackage(v, PACK_SIZE)
			s, err := schema.SchemaOf(v)
			require.NoError(t, err)
			maps, err := s.MapTo(s)
			require.NoError(t, err)
			for i := range PACK_SIZE {
				err := pkg.ReadStruct(i, v, s, maps)
				require.NoError(t, err)
			}
		})
	}
}

func TestReadChildStruct(t *testing.T) {
	pkg := makeTypedPackage(&encodeTestStruct{}, PACK_SIZE)
	dst := &encodeTestSubStruct{}
	dstSchema, err := schema.SchemaOf(dst)
	require.NoError(t, err)
	maps, err := pkg.schema.MapTo(dstSchema)
	require.NoError(t, err)
	for i := range PACK_SIZE {
		err := pkg.ReadStruct(i, dst, dstSchema, maps)
		require.NoError(t, err)
	}
}

func BenchmarkReadStruct(b *testing.B) {
	for _, v := range testStructs {
		pkg := makeTypedPackage(v, PACK_SIZE)
		s, _ := schema.SchemaOf(v)
		maps, _ := s.MapTo(s)
		b.Run(fmt.Sprintf("%T/%d", v, PACK_SIZE), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				for k := range PACK_SIZE {
					_ = pkg.ReadStruct(k, v, s, maps)
				}
			}
			b.ReportMetric(float64(PACK_SIZE*b.N)/float64(b.Elapsed().Nanoseconds()), "rec/ns")
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
		})
	}
}

func BenchmarkReadWire(b *testing.B) {
	for _, v := range testStructs {
		pkg := makeTypedPackage(v, PACK_SIZE)
		buf := bytes.NewBuffer(make([]byte, 0, pkg.schema.WireSize()+128))
		b.Run(fmt.Sprintf("%T/%d", v, pkg.Len()), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				for i := range PACK_SIZE {
					buf.Reset()
					_ = pkg.ReadWireBuffer(buf, i)
				}
			}
			b.ReportMetric(float64(PACK_SIZE*b.N)/float64(b.Elapsed().Nanoseconds()), "rec/ns")
		})
	}
}

func BenchmarkReadWireE2E(b *testing.B) {
	for _, v := range testStructs {
		pkg := makeTypedPackage(v, PACK_SIZE)
		buf := bytes.NewBuffer(make([]byte, 0, pkg.schema.WireSize()+128))
		b.Run(fmt.Sprintf("%T/%d", v, PACK_SIZE), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				for i := range PACK_SIZE {
					buf.Reset()
					_ = pkg.ReadWireBuffer(buf, i)
					_ = v.Decode(buf.Bytes())
				}
			}
			b.ReportMetric(float64(PACK_SIZE*b.N)/float64(b.Elapsed().Nanoseconds()), "rec/ns")
		})
	}
}
