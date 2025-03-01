// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package num

import (
	"bytes"
	"encoding/hex"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

var uvTestcases = []uint64{
	0,
	1,
	240,
	2287,
	67823,
	16777215,
	4294967295,
	1099511627775,
	281474976710655,
	72057594037927935,
	1<<64 - 1,
}

func TestUvarint(t *testing.T) {
	for _, v := range uvTestcases {
		t.Run(strconv.FormatUint(v, 16), func(t *testing.T) {
			var b [MaxVarintLen64]byte
			PutUvarint(b[:], v)
			t.Log("Enc", hex.EncodeToString(b[:]))
			x, n := Uvarint(b[:])
			assert.Equal(t, v, x)
			assert.GreaterOrEqual(t, n, 1)
			assert.LessOrEqual(t, n, MaxVarintLen64)
		})
	}
}

func TestAppendUvarint(t *testing.T) {
	for _, v := range uvTestcases {
		var b [MaxVarintLen64]byte
		buf := AppendUvarint(b[:0], v)
		assert.NotNil(t, buf)
		x, n := Uvarint(buf)
		assert.Equal(t, v, x)
		assert.Len(t, buf, n)
	}
}

func TestReadUvarint(t *testing.T) {
	for _, v := range uvTestcases {
		var b [MaxVarintLen64]byte
		buf := bytes.NewBuffer(AppendUvarint(b[:0], v))
		x, err := ReadUvarint(buf)
		assert.NoError(t, err)
		assert.Equal(t, v, x)
	}
}

func BenchmarkUvarint(b *testing.B) {
	for _, v := range uvTestcases {
		var buf [MaxVarintLen64]byte
		PutUvarint(buf[:], v)
		b.Run(strconv.FormatUint(v, 16), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				Uvarint(buf[:])
			}
		})
	}
}

func BenchmarkPutUvarint(b *testing.B) {
	for _, v := range uvTestcases {
		var buf [MaxVarintLen64]byte
		b.Run(strconv.FormatUint(v, 16), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				PutUvarint(buf[:], v)
			}
		})
	}
}

func BenchmarkAppendUvarint(b *testing.B) {
	for _, v := range uvTestcases {
		var buf [MaxVarintLen64]byte
		b.Run(strconv.FormatUint(v, 16), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				AppendUvarint(buf[:0], v)
			}
		})
	}
}
