// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package num

import (
	"bytes"
	"math/rand"
	"testing"
)

type BigintDecodeTest struct {
	name string
	buf  []byte
	val  string
	err  error
}

var bigintDecodeCases = []BigintDecodeTest{
	{
		name: "l0",
		buf:  []byte{},
		val:  "0",
	},
	{
		name: "l1",
		buf:  []byte{0x20},
		val:  "32",
	},
	{
		name: "l9",
		buf:  []byte{0x20, 0x40, 0x81, 0x02, 0x04, 0x08, 0x10, 0x20},
		val:  "2323999253380730912",
	},
	{
		name: "l10",
		buf:  []byte{0x10, 0x20, 0x40, 0x81, 0x02, 0x04, 0x08, 0x10, 0x20},
		val:  "297471904432733556768",
	},
	{
		name: "l18",
		buf:  []byte{0x10, 0x20, 0x40, 0x81, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x81, 0x02, 0x04, 0x08, 0x10, 0x20},
		val:  "21435109727303210296905487082316107808",
	},
	{
		name: "l19",
		buf:  []byte{0x08, 0x10, 0x20, 0x40, 0x81, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x81, 0x02, 0x04, 0x08, 0x10, 0x20},
		val:  "2743694045094810918003902346536461799456",
	},

	// negative bigints are not supported
	// {
	// 	name: "n1",
	// 	buf:  []byte{0x20},
	//
	// 	sign: 1,
	// 	val:  "-32",
	// },
	// {
	// 	name: "n9",
	// 	buf:  []byte{0xe0, 0xc0, 0xc0, 0xc0, 0xc0, 0xc0, 0xc0, 0xc0, 0x40},
	//
	// 	sign: 1,
	// 	val:  "-2323999253380730912",
	// },
	// {
	// 	name: "n10",
	// 	buf:  []byte{0xe0, 0xc0, 0xc0, 0xc0, 0xc0, 0xc0, 0xc0, 0xc0, 0xc0, 0x40},
	//
	// 	sign: 1,
	// 	val:  "-297471904432733556768",
	// },
	// {
	// 	name: "n18",
	// 	buf:  []byte{0xe0, 0xc0, 0xc0, 0xc0, 0xc0, 0xc0, 0xc0, 0xc0, 0xc0, 0xc0, 0xc0, 0xc0, 0xc0, 0xc0, 0xc0, 0xc0, 0xc0, 0x40},
	//
	// 	sign: 1,
	// 	val:  "-21435109727303210296905487082316107808",
	// },
	// {
	// 	name: "n19",
	// 	buf:  []byte{0xe0, 0xc0, 0xc0, 0xc0, 0xc0, 0xc0, 0xc0, 0xc0, 0xc0, 0xc0, 0xc0, 0xc0, 0xc0, 0xc0, 0xc0, 0xc0, 0xc0, 0xc0, 0x40},
	//
	// 	sign: 1,
	// 	val:  "-2743694045094810918003902346536461799456",
	// },
}

func TestBigCompare(t *testing.T) {
	for _, v := range [][2]int64{
		{1 << 8, 1<<8 + 1},
		{1 << 16, 1<<16 + 1},
		{1 << 32, 1<<32 + 1},
	} {
		var x, y Big
		x.SetInt64(v[0])
		y.SetInt64(v[1])
		if got, want := x.IsLess(y), v[0] < v[1]; got != want {
			t.Errorf("%d ? %d: unexpected cmp order", v[0], v[1])
		}

		cmp := bytes.Compare(x.Bytes(), y.Bytes())
		if got, want := cmp < 0, v[0] < v[1]; got != want {
			t.Errorf("%d ? %d: unexpected bytes cmp order", v[0], v[1])
		}
	}
}

func TestBigTextMarshal(t *testing.T) {
	for _, c := range bigintDecodeCases {
		var x Big
		err := x.UnmarshalBinary(c.buf)
		if err != nil {
			t.Errorf("%s: unexpected binary unmarshal error %v", c.name, err)
		}

		var z Big
		err = z.UnmarshalText([]byte(c.val))
		if got, want := err, c.err; got != want {
			t.Errorf("%s: unexpected error %v, expected %v", c.name, got, want)
		}
		if err != nil {
			continue
		}
		if got, want := z, x; got.Cmp(want) != 0 {
			t.Errorf("%s: unexpected result %v, expected %v", c.name, got, want)
		}

		buf, err := z.MarshalText()
		if err != nil {
			t.Errorf("%s: unexpected text marshal error %v", c.name, err)
		}
		if got, want := string(buf), c.val; got != want {
			t.Errorf("%s: unexpected text %s, expected %s", c.name, got, want)
		}
	}
}

func TestBigBinaryMarshal(t *testing.T) {
	for _, c := range bigintDecodeCases {
		var x Big
		err := x.UnmarshalText([]byte(c.val))
		if err != nil {
			t.Errorf("%s: unexpected text unmarshal error %v", c.name, err)
		}

		var z Big
		err = z.UnmarshalBinary(c.buf)
		if got, want := err, c.err; got != want {
			t.Errorf("%s: unexpected error %v, expected %v", c.name, got, want)
		}
		if err != nil {
			continue
		}

		if got, want := z, x; got.Cmp(want) != 0 {
			t.Errorf("%s: unexpected result %v, expected %v", c.name, got, want)
		}

		buf, err := z.MarshalBinary()
		if err != nil {
			t.Errorf("%s: unexpected binary marshal error %v", c.name, err)
		}
		if got, want := buf, c.buf; !bytes.Equal(got, want) {
			t.Errorf("%s: unexpected binary %v, expected %v", c.name, got, want)
		}
	}
}

type benchmarkSize struct {
	name string
	l    int
}

var benchmarkSizes = []benchmarkSize{
	{"6bit", 1},
	{"62bit", 9},
	{"125bit", 18},
	{"251bit", 36},
	{"510bit", 73},
}

func randBigintSlice(n int) []byte {
	s := make([]byte, n)
	if n == 1 {
		s[0] = byte(rand.Intn(0x40))
		return s
	}

	s[0] = byte(rand.Intn(0x40)) | 0x80
	for i := 1; i < n-1; i++ {
		s[i] = byte(rand.Intn(0x80)) | 0x80
	}
	s[n-1] = byte(rand.Intn(0x80))
	return s
}

func BenchmarkBigUnmarshalBinary(b *testing.B) {
	for _, bm := range benchmarkSizes {
		buf := randBigintSlice(bm.l)
		var z Big
		b.Run(bm.name, func(b *testing.B) {
			b.SetBytes(int64(bm.l))
			for i := 0; i < b.N; i++ {
				z.UnmarshalBinary(buf)
			}
		})
	}
}
