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
	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			v := matchEqual(testCase.ByteArray, testCase.Value, testCase.Bits, testCase.Mask)
			require.Equal(t, testCase.ExpectedCount, v.Count())
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
	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			v := matchNotEqual(testCase.ByteArray, testCase.Value, testCase.Bits, testCase.Mask)
			require.Equal(t, testCase.ExpectedCount, v.Count())
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
	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			v := matchLess(testCase.ByteArray, testCase.Value, testCase.Bits, testCase.Mask)
			require.Equal(t, testCase.ExpectedCount, v.Count())
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
	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			v := matchLessEqual(testCase.ByteArray, testCase.Value, testCase.Bits, testCase.Mask)
			require.Equal(t, testCase.ExpectedCount, v.Count())
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
	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			v := matchGreater(testCase.ByteArray, testCase.Value, testCase.Bits, testCase.Mask)
			require.Equal(t, testCase.ExpectedCount, v.Count())
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
	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			v := matchGreaterEqual(testCase.ByteArray, testCase.Value, testCase.Bits, testCase.Mask)
			require.Equal(t, testCase.ExpectedCount, v.Count())
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
	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			v := matchBetween(testCase.ByteArray, testCase.Value, testCase.NValue, testCase.Bits, testCase.Mask)
			require.Equal(t, testCase.ExpectedCount, v.Count())
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
	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			min, max := testCase.ByteArray.MinMax()
			require.Equal(t, testCase.Value, min)
			require.Equal(t, testCase.NValue, max)
		})
	}
}
