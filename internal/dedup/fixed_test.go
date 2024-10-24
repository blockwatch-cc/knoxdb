package dedup

import (
	"bytes"
	"io"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFixedLen(t *testing.T) {
	type TestCase struct {
		Name        string
		Size        int
		Data        [][]byte
		ExpectedLen int
	}

	testCases := []TestCase{
		{
			Name:        "Empty Len",
			Size:        100,
			Data:        [][]byte{},
			ExpectedLen: 0,
		},
		{
			Name:        "Negative Size",
			Size:        -1,
			Data:        nil,
			ExpectedLen: 0,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			f := makeFixedByteArray(testCase.Size, testCase.Data)
			require.Equal(t, testCase.ExpectedLen, f.Len())
		})
	}
}

func TestFixedCap(t *testing.T) {
	type TestCase struct {
		Name        string
		Size        int
		Data        [][]byte
		ExpectedCap int
	}

	testCases := []TestCase{
		{
			Name:        "Empty Size",
			Size:        0,
			Data:        [][]byte{},
			ExpectedCap: 0,
		},
		{
			Name:        "Negative Size",
			Size:        -1,
			Data:        nil,
			ExpectedCap: 0,
		},
		{
			Name:        "100 Size",
			Size:        100,
			Data:        [][]byte{{0}},
			ExpectedCap: 1,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			f := makeFixedByteArray(testCase.Size, testCase.Data)
			require.Equal(t, testCase.ExpectedCap, f.Cap())
		})
	}
}

func TestFixedElem(t *testing.T) {
	rand.Seed(10)
	data := makeRandData(10, 10)
	f := makeFixedByteArray(10, data)
	for i := range data {
		if got, expected := f.Elem(i), data[i]; !bytes.Equal(got, expected) {
			t.Errorf("TestFixedElem: expected=%x to be same got=%x", expected, got)
		}
	}
}

func TestFixedUnsupported(t *testing.T) {
	handler := func(name string) {
		if err := recover(); err == nil {
			t.Errorf("TestFixedUnsupported: unsupported member function %q didn't panic", name)
		}
	}

	f := makeFixedByteArray(0, [][]byte{})

	t.Run("Grow", func(t *testing.T) {
		defer handler("Grow")
		f.Grow(0)
	})

	t.Run("Set", func(t *testing.T) {
		defer handler("Set")
		f.Set(0, []byte{})
	})

	t.Run("SetZero", func(t *testing.T) {
		defer handler("SetZero")
		f.SetZeroCopy(0, []byte{})
	})

	t.Run("Append", func(t *testing.T) {
		defer handler("Append")
		f.Append([]byte{})
	})

	t.Run("AppendZeroCopy", func(t *testing.T) {
		defer handler("AppendZeroCopy")
		f.AppendZeroCopy([]byte{})
	})

	t.Run("AppendFrom", func(t *testing.T) {
		defer handler("AppendFrom")
		nfixed := makeFixedByteArray(0, [][]byte{})
		f.AppendFrom(nfixed)
	})

	t.Run("Insert", func(t *testing.T) {
		defer handler("Insert")
		f.Insert(0, []byte{})
	})
	t.Run("InsertFrom", func(t *testing.T) {
		defer handler("InsertFrom")
		nfixed := makeFixedByteArray(0, [][]byte{})
		f.InsertFrom(0, nfixed)
	})

	t.Run("Copy", func(t *testing.T) {
		defer handler("Copy")
		nfixed := makeFixedByteArray(0, [][]byte{})
		f.Copy(nfixed, 0, 0, 0)
	})

	t.Run("Delete", func(t *testing.T) {
		defer handler("Delete")
		f.Delete(0, 0)
	})
}

func TestFixedWriteTo(t *testing.T) {
	sz := 10
	innerSz := 10
	data := makeRandData(sz, innerSz)
	f := makeFixedByteArray(sz, data)

	buf := bytes.NewBuffer(nil)
	n, err := f.WriteTo(buf)
	require.NoError(t, err)
	require.Greater(t, n, int64(0))

	headerSz := 9 // format 1, size 4, data size 4 (sz*innerSz)
	require.Equal(t, n, int64(sz*innerSz+headerSz))
}

func TestFixedReadFrom(t *testing.T) {
	type TestCase struct {
		Name            string
		Size            int
		N               int
		Reader          io.Reader
		IsErrorExpected bool
	}

	makeReaderData := func(n, l int) io.Reader {
		data := makeRandData(n, l)
		f := makeFixedByteArray(n, data)
		buf := bytes.NewBuffer(nil)
		_, err := f.WriteTo(buf)
		if err != nil {
			return nil
		}
		return buf
	}

	testCases := []TestCase{
		{
			Name:            "Empty Reader",
			Size:            10,
			N:               1,
			Reader:          bytes.NewBuffer(nil),
			IsErrorExpected: true,
		},
		{
			Name:            "Reader with valid data",
			Size:            10,
			N:               10,
			Reader:          makeReaderData(10, 10),
			IsErrorExpected: false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			f := newFixedByteArray(testCase.Size, testCase.N)
			n, err := f.ReadFrom(testCase.Reader)
			if testCase.IsErrorExpected {
				require.Error(t, err)
			} else {
				headerSz := 9 // format 1, size 4, data size 4 (sz*innerSz)
				require.Equal(t, n, int64(testCase.N*testCase.Size+headerSz))
			}
		})
	}
}
