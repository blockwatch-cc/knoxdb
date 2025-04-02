// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package generic

import (
	"testing"

	ztests "blockwatch.cc/knoxdb/internal/zip/tests"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/require"
)

var (
	zzDeltaEncodeUint64Cases = ztests.ZzDeltaEncodeUint64Cases
	zzDeltaEncodeUint32Cases = ztests.ZzDeltaEncodeUint32Cases
	zzDeltaEncodeUint16Cases = ztests.ZzDeltaEncodeUint16Cases
	zzDeltaEncodeUint8Cases  = ztests.ZzDeltaEncodeUint8Cases
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
		ZzDeltaEncodeUint64(slice, util.ReinterpretSlice[int64, uint64](c.Slice))
		require.Len(t, slice, len(c.Result), "len")
		require.Equal(t, slice, makeUint64Result(c.Result))
	}
}

// ---------------- zzDeltaEncodeUint32 -------------------------------------------------------------

func TestZzDeltaEncodeUint32Generic(t *testing.T) {
	for _, c := range zzDeltaEncodeUint32Cases {
		slice := make([]uint64, len(c.Slice))
		ZzDeltaEncodeUint32(slice, util.ReinterpretSlice[int32, uint32](c.Slice))
		require.Len(t, slice, len(c.Result), "len")
		require.Equal(t, slice, makeUint64Result(c.Result)) // result is uint64
	}
}

// ---------------- zzDeltaEncodeUint16 -------------------------------------------------------------

func TestZzDeltaEncodeUint16Generic(t *testing.T) {
	for _, c := range zzDeltaEncodeUint16Cases {
		slice := make([]uint64, len(c.Slice))
		ZzDeltaEncodeUint16(slice, util.ReinterpretSlice[int16, uint16](c.Slice))
		require.Len(t, slice, len(c.Result), "len")
		require.Equal(t, slice, makeUint64Result(c.Result)) // result is uint64
	}
}

// ---------------- zzDeltaEncodeUint8 -------------------------------------------------------------

func TestZzDeltaEncodeUint8Generic(t *testing.T) {
	for _, c := range zzDeltaEncodeUint8Cases {
		slice := make([]uint64, len(c.Slice))
		ZzDeltaEncodeUint8(slice, util.ReinterpretSlice[int8, uint8](c.Slice))
		require.Len(t, slice, len(c.Result), "len")
		require.Equal(t, slice, makeUint64Result(c.Result)) // result is uint64
	}
}
