// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package s8b

import (
	"fmt"
	"slices"
	"testing"

	stests "blockwatch.cc/knoxdb/internal/encode/s8b/tests"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestIteratorNext(t *testing.T) {
	for _, c := range stests.MakeTests[uint64]() {
		t.Run(fmt.Sprintf("%s", c.Name), func(t *testing.T) {
			if c.Err {
				t.Skip()
			}
			src := c.Data
			if c.Gen != nil {
				src = c.Gen()
			}
			if len(src) == 0 {
				t.Skip()
			}
			minv, maxv := slices.Min(src), slices.Max(src)
			t.Logf("Encode len=%d minv=%d maxv=%d", len(src), minv, maxv)
			buf, err := EncodeUint64(make([]byte, len(src)*8), src, minv, maxv)
			require.NoError(t, err)

			it := NewIterator[uint64](buf, len(src), minv)
			require.Equal(t, len(src), it.Len(), "bad it len")
			for i, v := range src {
				val, ok := it.Next()
				require.True(t, ok, "short iterator at pos %d", i)
				require.Equal(t, val, v, "invalid val=%d pos=%d src=%d minv=%d",
					val, i, src[i], minv)
			}

			// reset and again
			it.Reset()

			require.Equal(t, len(src), it.Len(), "bad it len post reset")
			for i, v := range src {
				val, ok := it.Next()
				require.True(t, ok, "short iterator at pos %d post reset", i)
				require.Equal(t, val, v, "invalid val=%d pos=%d post reset", val, i)
			}
			it.Close()

			// init without len
			it = NewIterator[uint64](buf, 0, minv)
			require.Equal(t, len(src), it.Len(), "bad it len when detected")
			it.Close()
		})
	}
}

func TestIteratorNextChunk(t *testing.T) {
	for _, c := range stests.MakeTests[uint64]() {
		t.Run(fmt.Sprintf("%s", c.Name), func(t *testing.T) {
			if c.Err {
				t.Skip()
			}
			src := c.Data
			if c.Gen != nil {
				src = c.Gen()
			}
			if len(src) == 0 {
				t.Skip()
			}
			minv, maxv := slices.Min(src), slices.Max(src)
			buf, err := EncodeUint64(make([]byte, len(src)*8), src, minv, maxv)
			require.NoError(t, err)

			it := NewIterator[uint64](buf, len(src), minv)
			require.Equal(t, len(src), it.Len(), "bad it len")
			var seen int
			for {
				dst, n := it.NextChunk()
				if n == 0 {
					break
				}
				for i, v := range dst[:n] {
					require.Equal(t, v, src[seen+i], "invalid val=%d pos=%d src=%d", v, seen+i, src[seen+i])
				}
				seen += n
			}
			require.Equal(t, len(src), seen, "next chunk did not return all values")

			// reset and again
			it.Reset()

			// skip some chunks
			seen = it.SkipChunk()
			seen += it.SkipChunk()
			for {
				dst, n := it.NextChunk()
				if n == 0 {
					break
				}
				for i, v := range dst[:n] {
					require.Equal(t, v, src[seen+i], "invalid val=%d pos=%d src=%d after skip", v, seen+i, src[seen+i])
				}
				seen += n
			}
			require.Equal(t, len(src), seen, "skip&next chunk did not return all values")
			it.Close()
		})
	}
}

func TestIteratorSeek(t *testing.T) {
	for _, c := range stests.MakeTests[uint64]() {
		t.Run(fmt.Sprintf("%s", c.Name), func(t *testing.T) {
			if c.Err {
				t.Skip()
			}
			src := c.Data
			if c.Gen != nil {
				src = c.Gen()
			}
			if len(src) == 0 {
				t.Skip()
			}
			minv, maxv := slices.Min(src), slices.Max(src)
			buf, err := EncodeUint64(make([]byte, len(src)*8), src, minv, maxv)
			require.NoError(t, err)

			it := NewIterator[uint64](buf, len(src), minv)
			require.Equal(t, len(src), it.Len(), "bad it len")

			// seek random to existing values
			for range len(src) {
				i := util.RandIntn(len(src))
				ok := it.Seek(i)
				require.True(t, ok, "seek to existing pos %d/%d failed", i, len(src))
				val, ok := it.Next()
				require.True(t, ok, "next after seek to existing pos %d/%d failed", i, len(src))
				require.Equal(t, val, src[i], "invalid val=%d pos=%d after seek", val, i)
			}

			// seek to invalid values
			require.False(t, it.Seek(-1), "seek to negative")
			_, ok := it.Next()
			require.False(t, ok, "next after bad seek")

			require.False(t, it.Seek(len(src)), "seek to end")
			_, ok = it.Next()
			require.False(t, it.Seek(len(src)), "seek to end")

			require.False(t, it.Seek(len(src)+1), "seek beyond end")
			_, ok = it.Next()
			require.False(t, it.Seek(len(src)), "seek to end")
		})
	}
}
