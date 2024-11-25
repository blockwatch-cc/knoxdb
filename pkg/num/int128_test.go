// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package num

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func i128s(n ...int) []Int128 {
	s := make([]Int128, len(n))
	for i, v := range n {
		s[i] = Int128FromInt64(int64(v))
	}
	return s

}

func TestInt128Unique(t *testing.T) {
	var tests = []struct {
		n string
		a []Int128
		b []Int128
		r []Int128
	}{
		{
			n: "empty",
			a: nil,
			b: nil,
			r: nil,
		},
		{
			n: "empty a",
			a: nil,
			b: i128s(1, 2),
			r: i128s(1, 2),
		},
		{
			n: "empty b",
			a: i128s(1, 2),
			b: nil,
			r: i128s(1, 2),
		},
		{
			n: "distinct unique",
			a: i128s(1, 2),
			b: i128s(3, 4),
			r: i128s(1, 2, 3, 4),
		},
		{
			n: "distinct unique gap",
			a: i128s(1, 2),
			b: i128s(4, 5),
			r: i128s(1, 2, 4, 5),
		},
		{
			n: "overlap duplicates",
			a: i128s(1, 2),
			b: i128s(2, 3),
			r: i128s(1, 2, 3),
		},
	}

	for _, c := range tests {
		t.Run(c.n, func(t *testing.T) {
			res := Int128Union(c.a, c.b)
			assert.Equal(t, c.r, res)
		})
	}
}
func TestInt128Intersect(t *testing.T) {
	var tests = []struct {
		n string
		a []Int128
		b []Int128
		r []Int128
	}{
		{
			n: "empty",
			a: nil,
			b: nil,
			r: nil,
		},
		{
			n: "empty a",
			a: nil,
			b: i128s(1, 2),
			r: nil,
		},
		{
			n: "empty b",
			a: i128s(1, 2),
			b: nil,
			r: nil,
		},
		{
			n: "distinct unique",
			a: i128s(1, 2),
			b: i128s(3, 4),
			r: i128s(),
		},
		{
			n: "distinct unique gap",
			a: i128s(1, 2),
			b: i128s(4, 5),
			r: i128s(),
		},
		{
			n: "overlap duplicates",
			a: i128s(1, 2),
			b: i128s(2, 3),
			r: i128s(2),
		},
		{
			n: "overlap duplicates not unique",
			a: i128s(1, 2),
			b: i128s(2, 3),
			r: i128s(2),
		},
	}

	for _, c := range tests {
		t.Run(c.n, func(t *testing.T) {
			res := Int128Intersect(c.a, c.b)
			assert.Equal(t, c.r, res)
		})
	}
}

func TestInt128Difference(t *testing.T) {
	var tests = []struct {
		n string
		a []Int128
		b []Int128
		r []Int128
	}{
		{
			n: "empty",
			a: nil,
			b: nil,
			r: nil,
		},
		{
			n: "empty a",
			a: nil,
			b: i128s(1, 2),
			r: nil,
		},
		{
			n: "empty b",
			a: i128s(1, 2),
			b: nil,
			r: i128s(1, 2),
		},
		{
			n: "distinct unique",
			a: i128s(1, 2),
			b: i128s(3, 4),
			r: i128s(1, 2),
		},
		{
			n: "distinct unique gap",
			a: i128s(1, 2),
			b: i128s(4, 5),
			r: i128s(1, 2),
		},
		{
			n: "overlap duplicates",
			a: i128s(1, 2),
			b: i128s(2, 3),
			r: i128s(1),
		},
		{
			n: "overlap duplicates not unique",
			a: i128s(1, 2),
			b: i128s(2, 3),
			r: i128s(1),
		},
	}

	for _, c := range tests {
		t.Run(c.n, func(t *testing.T) {
			res := Int128Difference(c.a, c.b)
			assert.Equal(t, c.r, res)
		})
	}
}
