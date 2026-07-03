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
			b.ReportMetric(float64(PACK_SIZE*b.N)/b.Elapsed().Seconds(), "rec/s")
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
			b.ReportMetric(float64(PACK_SIZE*b.N)/b.Elapsed().Seconds(), "rec/s")
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
			b.ReportMetric(float64(PACK_SIZE*b.N)/b.Elapsed().Seconds(), "rec/s")
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
			b.ReportMetric(float64(PACK_SIZE*b.N)/b.Elapsed().Seconds(), "rec/s")
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(PACK_SIZE*b.N), "ns/rec")
		})
	}
}

func BenchmarkPackView(b *testing.B) {
	pkg := makeTypedPackage(&Transfer{}, 1)
	b.ReportAllocs()
	b.ResetTimer()
	var tf Transfer
	for b.Loop() {
		tf.ID = pkg.Uint64(0, 0)
		tf.DebitAccountID = pkg.Uint64(1, 0)
		tf.CreditAccountID = pkg.Uint64(2, 0)
		tf.Amount = pkg.Int128(3, 0)
		tf.PendingID = pkg.Uint64(4, 0)
		copy(tf.UserData256[:], pkg.Bytes(5, 0))
		tf.UserData64 = pkg.Uint64(6, 0)
		tf.UserData32 = pkg.Uint32(7, 0)
		tf.Timeout = pkg.Uint32(8, 0)
		tf.Ledger = pkg.Uint32(9, 0)
		tf.Code = pkg.Uint16(10, 0)
		tf.Flags = pkg.Uint16(11, 0)
		tf.Timestamp = pkg.Uint64(12, 0)
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "rec/s")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N), "ns/rec")
}

func BenchmarkPackViewPre(b *testing.B) {
	pkg := makeTypedPackage(&Transfer{}, 1)
	b.ReportAllocs()
	b.ResetTimer()
	var (
		tf               Transfer
		aID              = pkg.Block(0).Uint64()
		aDebitAccountID  = pkg.Block(1).Uint64()
		aCreditAccountID = pkg.Block(2).Uint64()
		aAmount          = pkg.Block(3).Int128()
		aPendingID       = pkg.Block(4).Uint64()
		aUserData256     = pkg.Block(5).Bytes()
		aUserData64      = pkg.Block(6).Uint64()
		aUserData32      = pkg.Block(7).Uint32()
		aTimeout         = pkg.Block(8).Uint32()
		aLedger          = pkg.Block(9).Uint32()
		aCode            = pkg.Block(10).Uint16()
		aFlags           = pkg.Block(11).Uint16()
		aTimestamp       = pkg.Block(12).Uint64()
	)
	for b.Loop() {
		tf.ID = aID.Get(0)
		tf.DebitAccountID = aDebitAccountID.Get(0)
		tf.CreditAccountID = aCreditAccountID.Get(0)
		tf.Amount = aAmount.Get(0)
		tf.PendingID = aPendingID.Get(0)
		copy(tf.UserData256[:], aUserData256.Get(0))
		tf.UserData64 = aUserData64.Get(0)
		tf.UserData32 = aUserData32.Get(0)
		tf.Timeout = aTimeout.Get(0)
		tf.Ledger = aLedger.Get(0)
		tf.Code = aCode.Get(0)
		tf.Flags = aFlags.Get(0)
		tf.Timestamp = aTimestamp.Get(0)
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "rec/s")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N), "ns/rec")
}
