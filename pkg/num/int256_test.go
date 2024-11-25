// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package num

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func i256s(n ...int) []Int256 {
	s := make([]Int256, len(n))
	for i, v := range n {
		s[i] = Int256FromInt64(int64(v))
	}
	return s

}

func TestInt256Unique(t *testing.T) {
	var tests = []struct {
		n string
		a []Int256
		b []Int256
		r []Int256
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
			b: i256s(1, 2),
			r: i256s(1, 2),
		},
		{
			n: "empty b",
			a: i256s(1, 2),
			b: nil,
			r: i256s(1, 2),
		},
		{
			n: "distinct unique",
			a: i256s(1, 2),
			b: i256s(3, 4),
			r: i256s(1, 2, 3, 4),
		},
		{
			n: "distinct unique gap",
			a: i256s(1, 2),
			b: i256s(4, 5),
			r: i256s(1, 2, 4, 5),
		},
		{
			n: "overlap duplicates",
			a: i256s(1, 2),
			b: i256s(2, 3),
			r: i256s(1, 2, 3),
		},
	}

	for _, c := range tests {
		t.Run(c.n, func(t *testing.T) {
			res := Int256Union(c.a, c.b)
			assert.Equal(t, c.r, res)
		})
	}
}
func TestInt256Intersect(t *testing.T) {
	var tests = []struct {
		n string
		a []Int256
		b []Int256
		r []Int256
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
			b: i256s(1, 2),
			r: nil,
		},
		{
			n: "empty b",
			a: i256s(1, 2),
			b: nil,
			r: nil,
		},
		{
			n: "distinct unique",
			a: i256s(1, 2),
			b: i256s(3, 4),
			r: i256s(),
		},
		{
			n: "distinct unique gap",
			a: i256s(1, 2),
			b: i256s(4, 5),
			r: i256s(),
		},
		{
			n: "overlap duplicates",
			a: i256s(1, 2),
			b: i256s(2, 3),
			r: i256s(2),
		},
		{
			n: "overlap duplicates not unique",
			a: i256s(1, 2),
			b: i256s(2, 3),
			r: i256s(2),
		},
	}

	for _, c := range tests {
		t.Run(c.n, func(t *testing.T) {
			res := Int256Intersect(c.a, c.b)
			assert.Equal(t, c.r, res)
		})
	}
}

func TestInt256Difference(t *testing.T) {
	var tests = []struct {
		n string
		a []Int256
		b []Int256
		r []Int256
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
			b: i256s(1, 2),
			r: nil,
		},
		{
			n: "empty b",
			a: i256s(1, 2),
			b: nil,
			r: i256s(1, 2),
		},
		{
			n: "distinct unique",
			a: i256s(1, 2),
			b: i256s(3, 4),
			r: i256s(1, 2),
		},
		{
			n: "distinct unique gap",
			a: i256s(1, 2),
			b: i256s(4, 5),
			r: i256s(1, 2),
		},
		{
			n: "overlap duplicates",
			a: i256s(1, 2),
			b: i256s(2, 3),
			r: i256s(1),
		},
		{
			n: "overlap duplicates not unique",
			a: i256s(1, 2),
			b: i256s(2, 3),
			r: i256s(1),
		},
	}

	for _, c := range tests {
		t.Run(c.n, func(t *testing.T) {
			res := Int256Difference(c.a, c.b)
			assert.Equal(t, c.r, res)
		})
	}
}
