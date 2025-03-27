// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package tests

import (
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
	t.Helper()
	for _, c := range cases {
		bits, _ := MakeBitsAndMaskPoisonTail(len(c.Slice), 32, maskAll)
		cnt := fn(c.Slice, c.Match, bits)
		assert.Len(t, bits, len(c.Result), c.Name)
		assert.Equal(t, c.Count, cnt, "%s: unexpected result bit count", c.Name)
		assert.Equal(t, c.Result, bits, "%s: unexpected result", c.Name)
		assert.Equal(t, MakePoison(32), bits[len(bits):len(bits)+32], "%s: boundary violation", c.Name)
	}
}

func TestCases2[T types.Number](t *testing.T, cases []MatchTest[T], fn func([]T, T, T, []byte) int64) {
	t.Helper()
	for _, c := range cases {
		bits, _ := MakeBitsAndMaskPoisonTail(len(c.Slice), 32, maskAll)
		cnt := fn(c.Slice, c.Match, c.Match2, bits)
		assert.Len(t, bits, len(c.Result), c.Name)
		assert.Equal(t, c.Count, cnt, "%s: unexpected result bit count", c.Name)
		assert.Equal(t, c.Result, bits, "%s: unexpected result", c.Name)
		assert.Equal(t, MakePoison(32), bits[len(bits):len(bits)+32], "%s: boundary violation", c.Name)
	}
}
