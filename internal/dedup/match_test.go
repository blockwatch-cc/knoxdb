package dedup

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/bitset"
	"github.com/stretchr/testify/require"
)

type TestMatchCase struct {
	Name          string
	ByteArray     ByteArray
	Value         []byte
	NValue        []byte
	Bits          *bitset.Bitset
	Mask          *bitset.Bitset
	ExpectedCount int
}

func TestMatchEqual(t *testing.T) {
	testCases := []TestMatchCase{
		{
			Name:          "Empty Data",
			ByteArray:     newFixedByteArray(0, 0),
			Value:         []byte{},
			Bits:          bitset.NewBitset(0),
			Mask:          bitset.NewBitset(0).One(),
			ExpectedCount: 0,
		},
		{
			Name:          "Has Unequal Data",
			ByteArray:     makeFixedByteArray(5, [][]byte{[]byte("abcde")}),
			Value:         []byte("a"),
			Bits:          bitset.NewBitset(5),
			Mask:          bitset.NewBitset(5),
			ExpectedCount: 0,
		},
		{
			Name:          "Has Equal Data",
			ByteArray:     makeFixedByteArray(5, [][]byte{[]byte("abcde")}),
			Value:         []byte("abcde"),
			Bits:          bitset.NewBitset(5).One(),
			Mask:          bitset.NewBitset(5).One(),
			ExpectedCount: 1,
		},
		{
			Name:          "Nil Mask",
			ByteArray:     makeFixedByteArray(5, [][]byte{[]byte("abcde")}),
			Value:         []byte("abcde"),
			Bits:          bitset.NewBitset(5),
			Mask:          nil,
			ExpectedCount: 1,
		},
	}
	for _, c := range testCases {
		t.Run(c.Name, func(t *testing.T) {
			matchEqual(c.ByteArray, c.Value, c.Bits, c.Mask)
			require.Equal(t, c.ExpectedCount, c.Bits.Count())
		})
	}
}

func TestMatchNotEqual(t *testing.T) {
	testCases := []TestMatchCase{
		{
			Name:          "Empty Data",
			ByteArray:     newFixedByteArray(0, 0),
			Value:         []byte{},
			Bits:          bitset.NewBitset(0),
			Mask:          bitset.NewBitset(0).One(),
			ExpectedCount: 0,
		},
		{
			Name:          "Has Unequal Data",
			ByteArray:     makeFixedByteArray(5, [][]byte{[]byte("abcde")}),
			Value:         []byte("a"),
			Bits:          bitset.NewBitset(5),
			Mask:          bitset.NewBitset(5),
			ExpectedCount: 0,
		},
		{
			Name:          "Has Equal Data",
			ByteArray:     makeFixedByteArray(5, [][]byte{[]byte("abcde")}),
			Value:         []byte("abcde"),
			Bits:          bitset.NewBitset(5).One(),
			Mask:          bitset.NewBitset(5).One(),
			ExpectedCount: 0,
		},
		{
			Name:          "Nil Mask",
			ByteArray:     makeFixedByteArray(5, [][]byte{[]byte("abcde")}),
			Value:         []byte("abcde"),
			Bits:          bitset.NewBitset(5),
			Mask:          nil,
			ExpectedCount: 0,
		},
	}
	for _, c := range testCases {
		t.Run(c.Name, func(t *testing.T) {
			matchNotEqual(c.ByteArray, c.Value, c.Bits, c.Mask)
			require.Equal(t, c.ExpectedCount, c.Bits.Count())
		})
	}
}

func TestMatchLess(t *testing.T) {
	testCases := []TestMatchCase{
		{
			Name:          "Matches all except 'e'",
			ByteArray:     makeFixedByteArray(5, [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}),
			Value:         []byte("e"),
			Bits:          bitset.NewBitset(5).One(),
			Mask:          bitset.NewBitset(5).One(),
			ExpectedCount: 4,
		},
		{
			Name:          "Matches 'a'",
			ByteArray:     makeFixedByteArray(5, [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}),
			Value:         []byte("b"),
			Bits:          bitset.NewBitset(5).One(),
			Mask:          bitset.NewBitset(5).One(),
			ExpectedCount: 1,
		},
		{
			Name:          "Matches 'a' and 'b'",
			ByteArray:     makeFixedByteArray(5, [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}),
			Value:         []byte("c"),
			Bits:          bitset.NewBitset(5).One(),
			Mask:          nil,
			ExpectedCount: 2,
		},
		{
			Name:          "Matches 'b'",
			ByteArray:     makeFixedByteArray(5, [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}),
			Value:         []byte("c"),
			Bits:          bitset.NewBitset(5).One(),
			Mask:          bitset.NewBitset(5).Set(1),
			ExpectedCount: 1,
		},
	}
	for _, c := range testCases {
		t.Run(c.Name, func(t *testing.T) {
			matchLess(c.ByteArray, c.Value, c.Bits, c.Mask)
			require.Equal(t, c.ExpectedCount, c.Bits.Count())
		})
	}
}

func TestMatchLessEqual(t *testing.T) {
	testCases := []TestMatchCase{
		{
			Name:          "Matches all",
			ByteArray:     makeFixedByteArray(1, [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}),
			Value:         []byte("e"),
			Bits:          bitset.NewBitset(5).One(),
			Mask:          bitset.NewBitset(5).One(),
			ExpectedCount: 5,
		},
		{
			Name:          "Matches 'a' and 'b'",
			ByteArray:     makeFixedByteArray(1, [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}),
			Value:         []byte("b"),
			Bits:          bitset.NewBitset(5).One(),
			Mask:          bitset.NewBitset(5).One(),
			ExpectedCount: 2,
		},
		{
			Name:          "Matches 'a', 'b' and 'c'",
			ByteArray:     makeFixedByteArray(1, [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}),
			Value:         []byte("c"),
			Bits:          bitset.NewBitset(5).One(),
			Mask:          nil,
			ExpectedCount: 3,
		},
		{
			Name:          "Matches 'b'",
			ByteArray:     makeFixedByteArray(1, [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}),
			Value:         []byte("c"),
			Bits:          bitset.NewBitset(5).One(),
			Mask:          bitset.NewBitset(5).Set(1),
			ExpectedCount: 1,
		},
	}
	for _, c := range testCases {
		t.Run(c.Name, func(t *testing.T) {
			matchLessEqual(c.ByteArray, c.Value, c.Bits, c.Mask)
			require.Equal(t, c.ExpectedCount, c.Bits.Count())
		})
	}
}

func TestMatchGreater(t *testing.T) {
	testCases := []TestMatchCase{
		{
			Name:          "Matches all except 'a'",
			ByteArray:     makeFixedByteArray(1, [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}),
			Value:         []byte("a"),
			Bits:          bitset.NewBitset(5).One(),
			Mask:          bitset.NewBitset(5).One(),
			ExpectedCount: 4,
		},
		{
			Name:          "Matches 'c', 'd' and 'e'",
			ByteArray:     makeFixedByteArray(1, [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}),
			Value:         []byte("b"),
			Bits:          bitset.NewBitset(5).One(),
			Mask:          bitset.NewBitset(5).One(),
			ExpectedCount: 3,
		},
		{
			Name:          "Matches 'd' and 'e'",
			ByteArray:     makeFixedByteArray(1, [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}),
			Value:         []byte("c"),
			Bits:          bitset.NewBitset(5).One(),
			Mask:          nil,
			ExpectedCount: 2,
		},
		{
			Name:          "Matches 'e'",
			ByteArray:     makeFixedByteArray(1, [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}),
			Value:         []byte("c"),
			Bits:          bitset.NewBitset(5).One(),
			Mask:          bitset.NewBitset(5).Set(4),
			ExpectedCount: 1,
		},
	}
	for _, c := range testCases {
		t.Run(c.Name, func(t *testing.T) {
			matchGreater(c.ByteArray, c.Value, c.Bits, c.Mask)
			require.Equal(t, c.ExpectedCount, c.Bits.Count())
		})
	}
}

func TestMatchGreaterEqual(t *testing.T) {
	testCases := []TestMatchCase{
		{
			Name:          "Matches all",
			ByteArray:     makeFixedByteArray(1, [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}),
			Value:         []byte("a"),
			Bits:          bitset.NewBitset(5).One(),
			Mask:          bitset.NewBitset(5).One(),
			ExpectedCount: 5,
		},
		{
			Name:          "Matches 'b', 'c', 'd' and 'e'",
			ByteArray:     makeFixedByteArray(1, [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}),
			Value:         []byte("b"),
			Bits:          bitset.NewBitset(5).One(),
			Mask:          bitset.NewBitset(5).One(),
			ExpectedCount: 4,
		},
		{
			Name:          "Matches 'c', 'd' and 'e'",
			ByteArray:     makeFixedByteArray(1, [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}),
			Value:         []byte("c"),
			Bits:          bitset.NewBitset(5).One(),
			Mask:          nil,
			ExpectedCount: 3,
		},
		{
			Name:          "Matches 'e'",
			ByteArray:     makeFixedByteArray(1, [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}),
			Value:         []byte("c"),
			Bits:          bitset.NewBitset(5).One(),
			Mask:          bitset.NewBitset(5).Set(4),
			ExpectedCount: 1,
		},
	}
	for _, c := range testCases {
		t.Run(c.Name, func(t *testing.T) {
			matchGreaterEqual(c.ByteArray, c.Value, c.Bits, c.Mask)
			require.Equal(t, c.ExpectedCount, c.Bits.Count())
		})
	}
}

func TestMatchBetween(t *testing.T) {
	testCases := []TestMatchCase{
		{
			Name:          "Matches 'a','b'",
			ByteArray:     makeFixedByteArray(1, [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}),
			Value:         []byte("a"),
			NValue:        []byte("b"),
			Bits:          bitset.NewBitset(5).One(),
			Mask:          bitset.NewBitset(5).One(),
			ExpectedCount: 2,
		},
		{
			Name:          "Matches 'b'",
			ByteArray:     makeFixedByteArray(1, [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}),
			Value:         []byte("b"),
			NValue:        []byte("b"),
			Bits:          bitset.NewBitset(5).One(),
			Mask:          bitset.NewBitset(5).One(),
			ExpectedCount: 1,
		},
		{
			Name:          "Matches 'c','d','e'",
			ByteArray:     makeFixedByteArray(1, [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}),
			Value:         []byte("c"),
			NValue:        []byte("e"),
			Bits:          bitset.NewBitset(5).One(),
			Mask:          bitset.NewBitset(5).One(),
			ExpectedCount: 3,
		},
		{
			Name:          "Matches 'a','b','c','d'",
			ByteArray:     makeFixedByteArray(1, [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}),
			Value:         []byte("a"),
			NValue:        []byte("d"),
			Bits:          bitset.NewBitset(5).One(),
			Mask:          bitset.NewBitset(5).One(),
			ExpectedCount: 4,
		},
		{
			Name:          "Matches 'a','b','c','d','e'",
			ByteArray:     makeFixedByteArray(1, [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}),
			Value:         []byte("a"),
			NValue:        []byte("e"),
			Bits:          bitset.NewBitset(5).One(),
			Mask:          bitset.NewBitset(5).One(),
			ExpectedCount: 5,
		},
	}
	for _, c := range testCases {
		t.Run(c.Name, func(t *testing.T) {
			matchBetween(c.ByteArray, c.Value, c.NValue, c.Bits, c.Mask)
			require.Equal(t, c.ExpectedCount, c.Bits.Count())
		})
	}
}

func TestMatchMinMax(t *testing.T) {
	testCases := []TestMatchCase{
		{
			Name:      "More than 1",
			ByteArray: makeFixedByteArray(1, [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}),
			Value:     []byte("a"),
			NValue:    []byte("e"),
		},
		{
			Name:      "1 element",
			ByteArray: makeFixedByteArray(1, [][]byte{[]byte("a")}),
			Value:     []byte("a"),
			NValue:    []byte("a"),
		},
		{
			Name:      "Zero element",
			ByteArray: newFixedByteArray(0, 0),
			Value:     nil,
			NValue:    nil,
		},
	}
	for _, c := range testCases {
		t.Run(c.Name, func(t *testing.T) {
			min, max := c.ByteArray.MinMax()
			require.Equal(t, c.Value, min)
			require.Equal(t, c.NValue, max)
		})
	}
}
