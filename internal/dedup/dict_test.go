package dedup

import (
	"bytes"
	"io"
	"testing"

	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeDictByteArrayReader(sz int) (io.Reader, ByteArray) {
	data := util.RandByteSlices(sz, sz)
	dup := makeDupmap(sz)
	d := makeDictByteArray(sz, sz, data, dup)

	buf := bytes.NewBuffer(nil)
	d.WriteTo(buf)
	return buf, d
}

func TestDictElem(t *testing.T) {
	data := util.RandByteSlices(10, 10)
	dup := makeDupmap(10)
	d := makeDictByteArray(10, 10, data, dup)

	assert.Equalf(t, 10, d.Len(), "TestDictElem: Len expected=%d but got=%d", 10, d.Len())
	assert.Equalf(t, 12, d.Cap(), "TestDictElem: Cap expected=%d but got=%d", 12, d.Cap())

	for i := range data {
		assert.Equalf(t, data[i], d.Elem(i), "TestDictElem: expected=%x to be same as got=%x", data[i], d.Elem(i))
	}
}

func TestDictClear(t *testing.T) {
	data := util.RandByteSlices(10, 10)
	dup := makeDupmap(10)
	d := makeDictByteArray(10, 10, data, dup)

	assert.NotZerof(t, len(d.dict), "TestDictClear: dict parameter should not be %d", len(d.dict))
	assert.NotZerof(t, len(d.offs), "TestDictClear: offs parameter should not be %d", len(d.offs))
	assert.NotZerof(t, len(d.size), "TestDictClear: size parameter should not be %d", len(d.size))
	assert.NotZerof(t, len(d.ptr), "TestDictClear: ptr parameter should not be %d", len(d.ptr))
	assert.NotZerof(t, d.log2, "TestDictClear: log2 parameter should not be %d", d.log2)
	assert.NotZerof(t, d.n, "TestDictClear: n parameter should not be %d", d.n)

	// clear DictByteArray
	d.Clear()

	assert.Zerof(t, len(d.dict), "TestDictClear: dict parameter should not be %d", len(d.dict))
	assert.Zerof(t, len(d.offs), "TestDictClear: offs parameter should not be %d", len(d.offs))
	assert.Zerof(t, len(d.size), "TestDictClear: size parameter should not be %d", len(d.size))
	assert.Zerof(t, len(d.ptr), "TestDictClear: ptr parameter should not be %d", len(d.ptr))
	assert.Zerof(t, d.log2, "TestDictClear: log2 parameter should not be %d", d.log2)
	assert.Zerof(t, d.n, "TestDictClear: n parameter should not be %d", d.n)
}

func TestDictRelease(t *testing.T) {
	data := util.RandByteSlices(10, 10)
	dup := makeDupmap(10)
	d := makeDictByteArray(10, 10, data, dup)

	assert.NotZerof(t, len(d.dict), "TestDictRelease: dict parameter should not be %d", len(d.dict))
	assert.NotZerof(t, len(d.offs), "TestDictRelease: offs parameter should not be %d", len(d.offs))
	assert.NotZerof(t, len(d.size), "TestDictRelease: size parameter should not be %d", len(d.size))
	assert.NotZerof(t, len(d.ptr), "TestDictRelease: ptr parameter should not be %d", len(d.ptr))
	assert.NotZerof(t, d.log2, "TestDictRelease: log2 parameter should not be %d", d.log2)
	assert.NotZerof(t, d.n, "TestDictRelease: n parameter should not be %d", d.n)

	// release DictByteArray
	d.Release()

	assert.Zerof(t, len(d.dict), "TestDictRelease: dict parameter should not be %d", len(d.dict))
	assert.Zerof(t, len(d.offs), "TestDictRelease: offs parameter should not be %d", len(d.offs))
	assert.Zerof(t, len(d.size), "TestDictRelease: size parameter should not be %d", len(d.size))
	assert.Zerof(t, len(d.ptr), "TestDictRelease: ptr parameter should not be %d", len(d.ptr))
	assert.Zerof(t, d.log2, "TestDictRelease: log2 parameter should not be %d", d.log2)
	assert.Zerof(t, d.n, "TestDictRelease: n parameter should not be %d", d.n)
}

func TestDictUnsupported(t *testing.T) {
	handler := func(name string) {
		err := recover()
		require.NotNilf(t, err, "TestDictUnsupported: unsupported member function %q didn't panic", name)
	}

	d := makeDictByteArray(0, 0, [][]byte{}, []int{})

	t.Run("Grow", func(t *testing.T) {
		defer handler("Grow")
		d.Grow(0)
	})

	t.Run("Set", func(t *testing.T) {
		defer handler("Set")
		d.Set(0, []byte{})
	})

	t.Run("SetZeroCopy", func(t *testing.T) {
		defer handler("SetZeroCopy")
		d.SetZeroCopy(0, []byte{})
	})

	t.Run("Append", func(t *testing.T) {
		defer handler("Append")
		d.Append([]byte{})
	})

	t.Run("AppendZeroCopy", func(t *testing.T) {
		defer handler("AppendZeroCopy")
		d.AppendZeroCopy([]byte{})
	})

	t.Run("AppendFrom", func(t *testing.T) {
		defer handler("AppendFrom")
		nfixed := makeFixedByteArray(0, [][]byte{})
		d.AppendFrom(nfixed)
	})

	t.Run("Insert", func(t *testing.T) {
		defer handler("Insert")
		d.Insert(0, []byte{})
	})
	t.Run("InsertFrom", func(t *testing.T) {
		defer handler("InsertFrom")
		nfixed := makeFixedByteArray(0, [][]byte{})
		d.InsertFrom(0, nfixed)
	})

	t.Run("Copy", func(t *testing.T) {
		defer handler("Copy")
		nfixed := makeFixedByteArray(0, [][]byte{})
		d.Copy(nfixed, 0, 0, 0)
	})

	t.Run("Delete", func(t *testing.T) {
		defer handler("Delete")
		d.Delete(0, 0)
	})
}

func TestDictWriteTo(t *testing.T) {
	t.Run("With Empty Data", func(t *testing.T) {
		data := util.RandByteSlices(0, 0)
		dup := makeDupmap(0)
		d := makeDictByteArray(0, 0, data, dup)

		buf := bytes.NewBuffer(nil)
		n, err := d.WriteTo(buf)
		require.NoError(t, err, "TestDictWriteTo: writing to buffer should not fail")
		// 1 format, 4 len elements, 1 log2, 4 dict len elements offset size, 4 compressed offset size, ** compressed offset data, 4 dict size, ** dict data, 4 ptr size, ** ptr data (1)
		expectedSize := 1 + 4 + 1 + 4 + 4 + 0 + 4 + 0 + 4 + 1
		assert.Equal(t, int64(expectedSize), n, "TestDictWriteTo: data expected to write %d but wrote %d", expectedSize, n)
	})

	t.Run("With Data", func(t *testing.T) {
		data := util.RandByteSlices(10, 10)
		dup := makeDupmap(10)
		d := makeDictByteArray(10, 10, data, dup)

		buf := bytes.NewBuffer(nil)
		n, err := d.WriteTo(buf)
		require.NoError(t, err, "TestDictWriteTo: writing to buffer should not fail")

		var expectedSize int64 = 139
		assert.Equalf(t, expectedSize, n, "TestDictWriteTo: data expected to write %d but wrote %d", expectedSize, n)
	})

	t.Run("With Large Data", func(t *testing.T) {
		sz := 10000
		data := util.RandByteSlices(sz, sz)
		dup := makeDupmap(sz)
		d := makeDictByteArray(sz, sz, data, dup)

		buf := bytes.NewBuffer(nil)
		n, err := d.WriteTo(buf)
		require.NoError(t, err, "TestDictWriteTo: writing to buffer should not fail")

		var expectedSize int64 = 100017537
		assert.Equalf(t, expectedSize, n, "TestDictWriteTo: data expected to write %d but wrote %d", expectedSize, n)
	})

	t.Run("Faulty Writer", func(t *testing.T) {
		data := util.RandByteSlices(0, 0)
		dup := makeDupmap(0)
		d := makeDictByteArray(0, 0, data, dup)

		failAfter := 6
		buf := &FaultyWriter{failAfter: failAfter}
		z, err := d.WriteTo(buf)
		require.Error(t, err, "TestDictWriteTo: writing to buffer should not fail")
		assert.Equalf(t, int64(failAfter), z, "TestDictWriteTo: data expected to write greater than or equal to %d but wrote %d", failAfter, z)
	})
}

func TestDictReadFrom(t *testing.T) {
	type TestCase struct {
		Name            string
		Reader          io.Reader
		ReadSize        int
		IsErrorExpected bool
	}
	dictRead0, _ := makeDictByteArrayReader(0)
	dictRead10000, _ := makeDictByteArrayReader(10_000)

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
			Reader:          dictRead0,
			ReadSize:        22,
			IsErrorExpected: false,
		},
		{
			Name:            "Reader with large data",
			Reader:          dictRead10000,
			ReadSize:        100017536,
			IsErrorExpected: false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			d := newDictByteArray(0, 0, 0)

			// read format off
			r := testCase.Reader
			var b [1]byte
			_, err := r.Read(b[:])
			if !testCase.IsErrorExpected {
				require.NoErrorf(t, err, "TestDictReadFrom: %v", err)
			}

			n, err := d.ReadFrom(testCase.Reader)
			if testCase.IsErrorExpected {
				if err == nil {
					assert.Errorf(t, err, "TestDictReadFrom: %v", err)

				}
			} else {
				assert.Equalf(t, int64(testCase.ReadSize), n, "TestDictReadFrom: reader: %d expected: %d", n, testCase.ReadSize)
			}
		})
	}
}
