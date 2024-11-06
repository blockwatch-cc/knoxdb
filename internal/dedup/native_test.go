// Copyright (c) 2018-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package dedup

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"strconv"
	"testing"

	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/require"
)

const (
	nativeBufLen             = 4
	defaultMaxPointsPerBlock = 1 << 16
)

func makeNativeByteArrayReader(sz int) io.Reader {
	data := makeNumberedData(sz)
	arr := newNativeByteArrayFromBytes(data)
	buf := bytes.NewBuffer(nil)
	arr.WriteTo(buf)
	return buf
}

func makeNumberedData(n int) [][]byte {
	b := make([][]byte, n, n)
	for i := range b {
		b[i] = make([]byte, 8)
		binary.BigEndian.PutUint64(b[i][:], uint64(i))
	}
	return b
}

func cloneData(b [][]byte) [][]byte {
	c := make([][]byte, len(b))
	for i, v := range b {
		c[i] = make([]byte, len(v))
		copy(c[i][:], v)
	}
	return c
}

func TestNativeElem(t *testing.T) {
	data := makeNumberedData(defaultMaxPointsPerBlock)
	arr := newNativeByteArrayFromBytes(data)
	if got, want := arr.Len(), defaultMaxPointsPerBlock; got != want {
		t.Errorf("Len mismatch got=%d want=%d", got, want)
	}
	if got, want := arr.Cap(), defaultMaxPointsPerBlock; got != want {
		t.Errorf("Cap mismatch got=%d want=%d", got, want)
	}
	for i := range data {
		if got, want := arr.Elem(i), data[i]; !bytes.Equal(got, want) {
			t.Errorf("Elem %d mismatch got=%x want=%x", i, got, want)
		}
	}
}

func TestNativeAppend(t *testing.T) {
	for i := 0; i < 100; i++ {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			data := util.RandByteSlices(defaultMaxPointsPerBlock, nativeBufLen)
			arr := newNativeByteArray(defaultMaxPointsPerBlock)
			if got, want := arr.Len(), 0; got != want {
				t.Errorf("Len mismatch got=%d want=%d", got, want)
			}
			if got, want := arr.Cap(), defaultMaxPointsPerBlock; got != want {
				t.Errorf("Cap mismatch got=%d want=%d", got, want)
			}
			for i := range data {
				arr.Append(data[i])
				if got, want := arr.Elem(i), data[i]; !bytes.Equal(got, want) {
					t.Errorf("Elem %d mismatch got=%x want=%x", i, got, want)
				}
			}
		})
	}
}

func TestNativeAppendZero(t *testing.T) {
	for i := 0; i < 100; i++ {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			data := util.RandByteSlices(defaultMaxPointsPerBlock, nativeBufLen)
			arr := newNativeByteArray(defaultMaxPointsPerBlock)
			if got, want := arr.Len(), 0; got != want {
				t.Errorf("Len mismatch got=%d want=%d", got, want)
			}
			if got, want := arr.Cap(), defaultMaxPointsPerBlock; got != want {
				t.Errorf("Cap mismatch got=%d want=%d", got, want)
			}
			for i := range data {
				arr.AppendZeroCopy(data[i])
				if got, want := arr.Elem(i), data[i]; !bytes.Equal(got, want) {
					t.Errorf("Elem %d mismatch got=%x want=%x", i, got, want)
				}
			}
		})
	}
}

func TestNativeAppendFrom(t *testing.T) {
	data := util.RandByteSlices(defaultMaxPointsPerBlock, nativeBufLen)
	clone := cloneData(data)
	src := newNativeByteArrayFromBytes(data)
	dst := newNativeByteArray(defaultMaxPointsPerBlock)
	dst.AppendFrom(src)
	if got, want := dst.Len(), src.Len(); got != want {
		t.Errorf("Len mismatch got=%d want=%d", got, want)
	}
	src.Clear()
	for i := range clone {
		if got, want := dst.Elem(i), clone[i]; !bytes.Equal(got, want) {
			t.Errorf("Elem %d mismatch got=%x want=%x", i, got, want)
		}
	}
}

func TestNativeSet(t *testing.T) {
	data := [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}
	nativeByteArray := newNativeByteArrayFromBytes(data)
	testCases := []struct {
		Index  int
		CValue []byte
		Value  []byte
	}{
		{
			Index:  0,
			CValue: []byte("a"),
			Value:  []byte("f"),
		},
		{
			Index:  1,
			CValue: []byte("b"),
			Value:  []byte("g"),
		},
		{
			Index:  2,
			CValue: []byte("c"),
			Value:  []byte("h"),
		},
		{
			Index:  3,
			CValue: []byte("d"),
			Value:  []byte("j"),
		},
		{
			Index:  4,
			CValue: []byte("e"),
			Value:  []byte("k"),
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("Set Index=%d with %q", tc.Index, tc.Value), func(t *testing.T) {
			require.Equal(t, tc.CValue, nativeByteArray.Elem(tc.Index))
			nativeByteArray.Set(tc.Index, tc.Value)
			require.Equal(t, tc.Value, nativeByteArray.Elem(tc.Index))
		})
	}

	t.Run("Set larger value", func(t *testing.T) {
		idx := 0
		value := []byte("hello")
		nativeByteArray.Set(idx, value)
		require.Equal(t, value, nativeByteArray.Elem(idx))
	})
}

func TestNativeSetZeroCopy(t *testing.T) {
	data := [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}
	nativeByteArray := newNativeByteArrayFromBytes(data)
	testCases := []struct {
		Index  int
		CValue []byte
		Value  []byte
	}{
		{
			Index:  0,
			CValue: []byte("a"),
			Value:  []byte("f"),
		},
		{
			Index:  1,
			CValue: []byte("b"),
			Value:  []byte("g"),
		},
		{
			Index:  2,
			CValue: []byte("c"),
			Value:  []byte("h"),
		},
		{
			Index:  3,
			CValue: []byte("d"),
			Value:  []byte("j"),
		},
		{
			Index:  4,
			CValue: []byte("e"),
			Value:  []byte("k"),
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("Set Index=%d with %q", tc.Index, tc.Value), func(t *testing.T) {
			require.Equal(t, tc.CValue, nativeByteArray.Elem(tc.Index))
			nativeByteArray.SetZeroCopy(tc.Index, tc.Value)
			require.Equal(t, tc.Value, nativeByteArray.Elem(tc.Index))
		})
	}

	t.Run("Set larger value", func(t *testing.T) {
		idx := 0
		value := []byte("hello")
		nativeByteArray.SetZeroCopy(idx, value)
		require.Equal(t, value, nativeByteArray.Elem(idx))
	})
}

func TestNativeInsert(t *testing.T) {
	data := [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}
	nativeByteArray := newNativeByteArrayFromBytes(data)
	testCases := []struct {
		Index    int
		CValue   []byte
		Value    []byte
		Expected []byte
	}{
		{
			Index:    0,
			CValue:   []byte("a"),
			Value:    []byte("f"),
			Expected: []byte("f"),
		},
		{
			Index:    1,
			CValue:   []byte("b"),
			Value:    []byte("g"),
			Expected: []byte("g"),
		},
		{
			Index:    2,
			CValue:   []byte("c"),
			Value:    []byte("h"),
			Expected: []byte("h"),
		},
		{
			Index:    3,
			CValue:   []byte("d"),
			Value:    []byte("j"),
			Expected: []byte("j"),
		},
		{
			Index:    4,
			CValue:   []byte("e"),
			Value:    []byte("k"),
			Expected: []byte("k"),
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("Insert Index=%d with %q", tc.Index, tc.Value), func(t *testing.T) {
			require.Equal(t, tc.CValue, nativeByteArray.Elem(tc.Index))
			nativeByteArray.Insert(tc.Index, tc.Value)
			require.Equal(t, tc.Expected, nativeByteArray.Elem(tc.Index))
		})
	}
}

func TestNativeInsertFrom(t *testing.T) {
	data := [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}
	nativeByteArray := newNativeByteArrayFromBytes(data)
	testCases := []struct {
		Index    int
		CValue   []byte
		Value    ByteArray
		Expected []byte
	}{
		{
			Index:    0,
			CValue:   []byte("a"),
			Value:    newNativeByteArrayFromBytes([][]byte{[]byte("f")}),
			Expected: []byte("f"),
		},
		{
			Index:    1,
			CValue:   []byte("b"),
			Value:    newNativeByteArrayFromBytes([][]byte{[]byte("g")}),
			Expected: []byte("g"),
		},
		{
			Index:    2,
			CValue:   []byte("c"),
			Value:    newNativeByteArrayFromBytes([][]byte{[]byte("h")}),
			Expected: []byte("h"),
		},
		{
			Index:    3,
			CValue:   []byte("d"),
			Value:    newNativeByteArrayFromBytes([][]byte{[]byte("j")}),
			Expected: []byte("j"),
		},
		{
			Index:    4,
			CValue:   []byte("e"),
			Value:    newNativeByteArrayFromBytes([][]byte{[]byte("k")}),
			Expected: []byte("k"),
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("InsertFrom Index=%d with %v", tc.Index, tc.Value.Slice()), func(t *testing.T) {
			require.Equal(t, tc.CValue, nativeByteArray.Elem(tc.Index))
			nativeByteArray.InsertFrom(tc.Index, tc.Value)
			require.Equal(t, tc.Expected, nativeByteArray.Elem(tc.Index))
		})
	}
}

func TestNativeMinMax(t *testing.T) {
	testCases := []struct {
		Name      string
		ByteArray ByteArray
		Min       []byte
		Max       []byte
	}{
		{
			Name:      "More than 1",
			ByteArray: newNativeByteArrayFromBytes([][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}),
			Min:       []byte("a"),
			Max:       []byte("e"),
		},
		{
			Name:      "1 element",
			ByteArray: newNativeByteArrayFromBytes([][]byte{[]byte("a")}),
			Min:       []byte("a"),
			Max:       []byte("a"),
		},
		{
			Name:      "Zero elements",
			ByteArray: newNativeByteArrayFromBytes([][]byte{}),
			Min:       []byte{},
			Max:       []byte{},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			min, max := testCase.ByteArray.MinMax()
			require.Equal(t, testCase.Min, min)
			require.Equal(t, testCase.Max, max)
		})
	}
}

func TestNativeReadFrom(t *testing.T) {
	type TestCase struct {
		Name            string
		Reader          io.Reader
		ReadSize        int
		IsErrorExpected bool
	}

	testCases := []TestCase{
		{
			Name:            "Empty reader",
			Reader:          bytes.NewReader(nil),
			ReadSize:        0,
			IsErrorExpected: true,
		},
		{
			Name:            "Reader with only format",
			Reader:          bytes.NewReader([]byte{bytesCompactFormat << 4}),
			ReadSize:        0,
			IsErrorExpected: true,
		},
		{
			Name:            "Reader with data",
			Reader:          makeNativeByteArrayReader(0),
			ReadSize:        4,
			IsErrorExpected: false,
		},
		{
			Name:            "Reader with large data",
			Reader:          makeNativeByteArrayReader(defaultMaxPointsPerBlock),
			ReadSize:        4 + (defaultMaxPointsPerBlock * 12),
			IsErrorExpected: false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			c := newNativeByteArray(0)

			// read format off
			r := testCase.Reader
			var b [1]byte
			_, err := r.Read(b[:])
			if !testCase.IsErrorExpected && err != nil {
				t.Errorf("TestNativeReadFrom: %v", err)
			}

			n, err := c.ReadFrom(testCase.Reader)
			if testCase.IsErrorExpected {
				if err == nil {
					t.Errorf("TestNativeReadFrom: %v", err)
				}
			} else {
				if n != int64(testCase.ReadSize) {
					t.Errorf("TestNativeReadFrom: reader: %d expected: %d", n, testCase.ReadSize)
				}
			}
		})
	}
}

func TestNativeWriteTo(t *testing.T) {
	t.Run("With Empty Data", func(t *testing.T) {
		b := newNativeByteArrayFromBytes([][]byte{})

		buf := bytes.NewBuffer(nil)
		n, err := b.WriteTo(buf)
		if err != nil {
			t.Errorf("TestNativeWriteTo: writing to buffer should not fail")
		}
		// 1 format, 4 elements size, ** elements data
		expectedSize := 1 + 4
		if int64(expectedSize) != n {
			t.Errorf("TestNativeWriteTo: data expected to write %d but wrote %d", expectedSize, n)
		}
	})

	t.Run("With Data", func(t *testing.T) {
		data := makeNumberedData(defaultMaxPointsPerBlock)
		b := newNativeByteArrayFromBytes(data)

		buf := bytes.NewBuffer(nil)
		n, err := b.WriteTo(buf)
		if err != nil {
			t.Errorf("TestNativeWriteTo: writing to buffer should not fail")
		}

		expectedSize := 5 + len(data)*12 // 4 len, 8 item size
		if int64(expectedSize) != n {
			t.Errorf("TestNativeWriteTo: data expected to write %d but wrote %d", expectedSize, n)
		}
	})

	t.Run("With Large Data", func(t *testing.T) {
		data := makeNumberedData(1 << 20)
		b := newNativeByteArrayFromBytes(data)

		buf := bytes.NewBuffer(nil)
		n, err := b.WriteTo(buf)
		if err != nil {
			t.Errorf("TestNativeWriteTo: writing to buffer should not fail")
		}

		expectedSize := 5 + len(data)*12 // 4 len, 8 item size
		if int64(expectedSize) != n {
			t.Errorf("TestNativeWriteTo: data expected to write %d but wrote %d", expectedSize, n)
		}
	})

	t.Run("Faulty Writer", func(t *testing.T) {
		data := makeNumberedData(1 << 20)
		b := newNativeByteArrayFromBytes(data)

		failAfter := 5
		buf := &FaultyWriter{failAfter: failAfter}
		n, err := b.WriteTo(buf)
		if err == nil {
			t.Errorf("TestNativeWriteTo: writing to buffer should fail")
		}

		if int64(failAfter) != n {
			t.Errorf("TestNativeWriteTo: data expected to write less than %d but wrote %d", failAfter, n)
		}
	})
}

func TestNativeCopy(t *testing.T) {
	testCases := []struct {
		Name         string
		SrcByteArray ByteArray
		DstByteArray ByteArray
		DstPos       int
		SrcPos       int
		N            int
		Expected     ByteArray
	}{
		{
			Name:         "empty copy",
			SrcByteArray: newNativeByteArrayFromBytes([][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}),
			DstByteArray: newNativeByteArrayFromBytes([][]byte{[]byte("f"), []byte("g"), []byte("h"), []byte("i"), []byte("j")}),
			SrcPos:       4,
			DstPos:       4,
			N:            0,
			Expected:     newNativeByteArrayFromBytes([][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}),
		},
		{
			Name:         "bounded",
			SrcByteArray: newNativeByteArrayFromBytes([][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}),
			DstByteArray: newNativeByteArrayFromBytes([][]byte{[]byte("f"), []byte("g"), []byte("h"), []byte("i"), []byte("j")}),
			SrcPos:       0,
			DstPos:       3,
			N:            2,
			Expected:     newNativeByteArrayFromBytes([][]byte{[]byte("i"), []byte("j"), []byte("c"), []byte("d"), []byte("e")}),
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			b := testCase.SrcByteArray.Copy(testCase.DstByteArray, testCase.SrcPos, testCase.DstPos, testCase.N)
			require.Equal(t, testCase.Expected.Slice(), b.Slice())
		})
	}
}

func TestNativeDelete(t *testing.T) {
	testCases := []struct {
		Name      string
		ByteArray ByteArray
		Index     int
		N         int
		Expected  ByteArray
	}{
		{
			Name:      "empty delete",
			ByteArray: newNativeByteArrayFromBytes([][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}),
			Index:     0,
			N:         0,
			Expected:  newNativeByteArrayFromBytes([][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}),
		},
		{
			Name:      "valid delete 1 item",
			ByteArray: newNativeByteArrayFromBytes([][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}),
			Index:     3,
			N:         1,
			Expected:  newNativeByteArrayFromBytes([][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("e")}),
		},
		{
			Name:      "valid delete all item",
			ByteArray: newNativeByteArrayFromBytes([][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}),
			Index:     0,
			N:         5,
			Expected:  newNativeByteArrayFromBytes([][]byte{}),
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			b := testCase.ByteArray.Delete(testCase.Index, testCase.N)
			require.Equal(t, testCase.Expected.Slice(), b.Slice())
		})
	}
}
