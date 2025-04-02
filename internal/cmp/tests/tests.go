// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package tests

import (
	"fmt"
	"testing"

	"blockwatch.cc/knoxdb/internal/types"
	"github.com/stretchr/testify/assert"
)

type MatchTest[T types.Number] struct {
	Name   string
	Slice  []T
	Match  T
	Match2 T
	Result []byte
	Count  int64
}

// Test Drivers
func TestCases[T types.Number](t *testing.T, cases []MatchTest[T], fn func([]T, T, []byte) int64) {
	for _, c := range cases {
		t.Run(fmt.Sprintf("%T/%s", T(0), c.Name), func(t *testing.T) {
			bits, _ := MakeBitsAndMaskPoisonTail(len(c.Slice), 32, maskAll)
			cnt := fn(c.Slice, c.Match, bits)
			assert.Len(t, bits, len(c.Result), "len")
			assert.Equal(t, c.Count, cnt, "unexpected result bit count")
			assert.Equal(t, c.Result, bits, "unexpected result")
			assert.Equal(t, MakePoison(32), bits[len(bits):len(bits)+32], "boundary violation")
		})
	}
}

func TestCases2[T types.Number](t *testing.T, cases []MatchTest[T], fn func([]T, T, T, []byte) int64) {
	for _, c := range cases {
		t.Run(fmt.Sprintf("%T/%s", T(0), c.Name), func(t *testing.T) {
			bits, _ := MakeBitsAndMaskPoisonTail(len(c.Slice), 32, maskAll)
			cnt := fn(c.Slice, c.Match, c.Match2, bits)
			assert.Len(t, bits, len(c.Result), "len")
			assert.Equal(t, c.Count, cnt, "unexpected result bit count")
			assert.Equal(t, c.Result, bits, "unexpected result")
			assert.Equal(t, MakePoison(32), bits[len(bits):len(bits)+32], "boundary violation")
		})
	}
}
