package dedup

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ io.Writer = (*FaultyWriter)(nil)

type FaultyWriter struct {
	size      int
	failAfter int
}

func (f *FaultyWriter) Write(p []byte) (int, error) {
	if f.size >= f.failAfter {
		return 0, fmt.Errorf("FaultyWriter: failed to write data")
	}
	sz := len(p)
	f.size += sz
	return sz, nil
}

func makeDupmap(sz int) []int {
	dup := make([]int, sz)
	for i := range sz {
		dup[i] = -1
	}
	return dup
}

func makeCompactByteArrayReader(sz int) (io.Reader, ByteArray) {
	data := util.RandByteSlices(sz, sz)
	dup := makeDupmap(sz)
	c := makeCompactByteArray(sz, sz, data, dup)

	buf := bytes.NewBuffer(nil)
	c.WriteTo(buf)
	return buf, c
}

func TestCompactElem(t *testing.T) {
	data := util.RandByteSlices(10, 10)
	dup := makeDupmap(10)
	c := makeCompactByteArray(10, 10, data, dup)

	assert.Equalf(t, 10, c.Len(), "TestCompactElem: Len expected=%d but got=%d", 10, c.Len())
	assert.Equalf(t, 10, c.Cap(), "TestCompactElem: Cap expected=%d but got=%d", 10, c.Len())
	for i := range data {
		assert.Equalf(t, data[i], c.Elem(i), "TestCompactElem: expected=%x to be same as got=%x", data[i], c.Len())
		if got, expected := c.Elem(i), data[i]; !bytes.Equal(got, expected) {
		}
	}
}

func TestCompactWriteTo(t *testing.T) {
	t.Run("With Empty Data", func(t *testing.T) {
		data := util.RandByteSlices(0, 0)
		dup := makeDupmap(0)
		c := makeCompactByteArray(0, 0, data, dup)

		buf := bytes.NewBuffer(nil)
		n, err := c.WriteTo(buf)
		require.NoError(t, err, "TestCompactWriteTo: writing to buffer should not fail")
		// 1 format, 4 actual offset size, 4 compressed offset size, ** compressed offset data, 4 compressed size, ** compressed size data, 4 raw data size, 10 raw data
		var expectedSize int64 = 1 + 4 + 4 + 0 + 4 + 0 + 4
		assert.Equalf(t, expectedSize, n, "TestCompactWriteTo: data expected to write %d but wrote %d", expectedSize, n)
	})

	t.Run("With Data", func(t *testing.T) {
		data := util.RandByteSlices(10, 10)
		dup := makeDupmap(10)
		c := makeCompactByteArray(10, 10, data, dup)

		buf := bytes.NewBuffer(nil)
		n, err := c.WriteTo(buf)
		require.NoError(t, err, "TestCompactWriteTo: writing to buffer should not fail")

		var expectedSize int64 = 139
		assert.Equalf(t, expectedSize, n, "TestCompactWriteTo: data expected to write %d but wrote %d", expectedSize, n)
	})

	t.Run("With Large Data", func(t *testing.T) {
		sz := 10000
		data := util.RandByteSlices(sz, sz)
		dup := makeDupmap(sz)
		c := makeCompactByteArray(sz, sz, data, dup)

		buf := bytes.NewBuffer(nil)
		n, err := c.WriteTo(buf)
		require.NoError(t, err, "TestCompactWriteTo: writing to buffer should not fail")

		var expectedSize int64 = 100000043
		assert.Equalf(t, expectedSize, n, "TestCompactWriteTo: data expected to write %d but wrote %d", expectedSize, n)
	})

	t.Run("Faulty Writer", func(t *testing.T) {
		data := util.RandByteSlices(0, 0)
		dup := makeDupmap(0)
		c := makeCompactByteArray(0, 0, data, dup)

		failAfter := 5
		buf := &FaultyWriter{failAfter: failAfter}
		n, err := c.WriteTo(buf)
		require.Errorf(t, err, "TestCompactWriteTo: writing to buffer should fail")

		assert.Equal(t, int64(failAfter), n, "TestCompactWriteTo: data expected to write less than %d but wrote %d", failAfter, n)
	})
}

// TestCompactReadFrom
func TestCompactReadFrom(t *testing.T) {
	type TestCase struct {
		Name            string
		Reader          io.Reader
		ReadSize        int
		IsErrorExpected bool
	}

	c0Reader, _ := makeCompactByteArrayReader(0)
	c10000Reader, _ := makeCompactByteArrayReader(10_000)

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
			Reader:          c0Reader,
			ReadSize:        16,
			IsErrorExpected: false,
		},
		{
			Name:            "Reader with large data",
			Reader:          c10000Reader,
			ReadSize:        100000042,
			IsErrorExpected: false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			c := newCompactByteArray(0, 0)

			// read format off
			r := testCase.Reader
			var b [1]byte
			_, err := r.Read(b[:])
			if !testCase.IsErrorExpected {
				require.NoErrorf(t, err, "TestCompactReadFrom: %v", err)
			}

			n, err := c.ReadFrom(testCase.Reader)
			if testCase.IsErrorExpected {
				assert.Errorf(t, err, "TestCompactReadFrom: %v", err)
			} else {
				assert.Equalf(t, int64(testCase.ReadSize), n, "TestCompactReadFrom: reader: %d expected: %d", n, testCase.ReadSize)
			}
		})
	}
}

func TestCompactUnsupported(t *testing.T) {
	handler := func(name string) {
		err := recover()
		require.NotNilf(t, err, "TestCompactUnsupported: unsupported member function %q didn't panic", name)
	}

	c := makeCompactByteArray(0, 0, [][]byte{}, []int{})

	t.Run("Grow", func(t *testing.T) {
		defer handler("Grow")
		c.Grow(0)
	})

	t.Run("Set", func(t *testing.T) {
		defer handler("Set")
		c.Set(0, []byte{})
	})

	t.Run("SetZero", func(t *testing.T) {
		defer handler("SetZero")
		c.SetZeroCopy(0, []byte{})
	})

	t.Run("Append", func(t *testing.T) {
		defer handler("Append")
		c.Append([]byte{})
	})

	t.Run("AppendZeroCopy", func(t *testing.T) {
		defer handler("AppendZeroCopy")
		c.AppendZeroCopy([]byte{})
	})

	t.Run("AppendFrom", func(t *testing.T) {
		defer handler("AppendFrom")
		nfixed := makeFixedByteArray(0, [][]byte{})
		c.AppendFrom(nfixed)
	})

	t.Run("Insert", func(t *testing.T) {
		defer handler("Insert")
		c.Insert(0, []byte{})
	})
	t.Run("InsertFrom", func(t *testing.T) {
		defer handler("InsertFrom")
		nfixed := makeFixedByteArray(0, [][]byte{})
		c.InsertFrom(0, nfixed)
	})

	t.Run("Copy", func(t *testing.T) {
		defer handler("Copy")
		nfixed := makeFixedByteArray(0, [][]byte{})
		c.Copy(nfixed, 0, 0, 0)
	})

	t.Run("Delete", func(t *testing.T) {
		defer handler("Delete")
		c.Delete(0, 0)
	})
}
