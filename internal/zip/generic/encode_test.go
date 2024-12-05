// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package generic

import (
	"reflect"
	"testing"

	"blockwatch.cc/knoxdb/internal/zip/tests"
	"blockwatch.cc/knoxdb/pkg/util"
)

var (
	zzDeltaEncodeUint64Cases = tests.ZzDeltaEncodeUint64Cases
	zzDeltaEncodeUint32Cases = tests.ZzDeltaEncodeUint32Cases
	zzDeltaEncodeUint16Cases = tests.ZzDeltaEncodeUint16Cases
	zzDeltaEncodeUint8Cases  = tests.ZzDeltaEncodeUint8Cases

	benchmarkSizes = tests.BenchmarkSizes
	randInt64Slice = util.RandInts[int64]
	randInt32Slice = util.RandInts[int32]
	randInt16Slice = util.RandInts[int16]
	randInt8Slice  = util.RandInts[int8]
	Int64Size      = tests.Int64Size
	Int32Size      = tests.Int32Size
	Int16Size      = tests.Int16Size
	Int8Size       = tests.Int8Size
)

func makeUint64Result[T int8 | int16 | int32 | int64](s []T) []uint64 {
	r := make([]uint64, len(s))
	for i, v := range s {
		r[i] = uint64(v)
	}
	return r
}

// ---------------- zzDeltaEncodeUint64 -------------------------------------------------------------

func TestZzDeltaEncodeUint64Generic(t *testing.T) {
	for _, c := range zzDeltaEncodeUint64Cases {
		slice := make([]uint64, len(c.Slice))
		ZzDeltaEncodeUint64(slice, util.Int64AsUint64Slice(c.Slice))
		if got, want := len(slice), len(c.Result); got != want {
			t.Errorf("%s: unexpected result length %d, expected %d", c.Name, got, want)
		}
		if !reflect.DeepEqual(slice, makeUint64Result(c.Result)) {
			t.Errorf("%s: unexpected result %v, expected %v", c.Name, slice, c.Result)
		}
	}
}

// ---------------- zzDeltaEncodeUint32 -------------------------------------------------------------

func TestZzDeltaEncodeUint32Generic(t *testing.T) {
	for _, c := range zzDeltaEncodeUint32Cases {
		slice := make([]uint64, len(c.Slice))
		ZzDeltaEncodeUint32(slice, util.Int32AsUint32Slice(c.Slice))
		if got, want := len(slice), len(c.Result); got != want {
			t.Errorf("%s: unexpected result length %d, expected %d", c.Name, got, want)
		}
		if !reflect.DeepEqual(slice, makeUint64Result(c.Result)) {
			t.Errorf("%s: unexpected result %v, expected %v", c.Name, slice, c.Result)
		}
	}
}

// ---------------- zzDeltaEncodeUint16 -------------------------------------------------------------

func TestZzDeltaEncodeUint16Generic(t *testing.T) {
	for _, c := range zzDeltaEncodeUint16Cases {
		slice := make([]uint64, len(c.Slice))
		ZzDeltaEncodeUint16(slice, util.Int16AsUint16Slice(c.Slice))
		if got, want := len(slice), len(c.Result); got != want {
			t.Errorf("%s: unexpected result length %d, expected %d", c.Name, got, want)
		}
		// result is uint64
		if !reflect.DeepEqual(slice, makeUint64Result(c.Result)) {
			t.Errorf("%s: unexpected result %v, expected %v", c.Name, slice, c.Result)
		}
	}
}

// ---------------- zzDeltaEncodeUint8 -------------------------------------------------------------

func TestZzDeltaEncodeUint8Generic(t *testing.T) {
	for _, c := range zzDeltaEncodeUint8Cases {
		slice := make([]uint64, len(c.Slice))
		ZzDeltaEncodeUint8(slice, util.Int8AsUint8Slice(c.Slice))
		if got, want := len(slice), len(c.Result); got != want {
			t.Errorf("%s: unexpected result length %d, expected %d", c.Name, got, want)
		}
		if !reflect.DeepEqual(slice, makeUint64Result(c.Result)) {
			t.Errorf("%s: unexpected result %v, expected %v", c.Name, slice, c.Result)
		}
	}
}
