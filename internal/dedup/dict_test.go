package dedup

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"blockwatch.cc/knoxdb/pkg/util"
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

	if got, expected := d.Len(), 10; got != expected {
		t.Errorf("TestDictElem: Len expected=%d but got=%d", expected, got)
	}
	if got, expected := d.Cap(), 12; got != expected {
		t.Errorf("TestDictElem: Cap expected=%d but got=%d", expected, got)
	}
	for i := range data {
		if got, expected := d.Elem(i), data[i]; !bytes.Equal(got, expected) {
			t.Errorf("TestDictElem: expected=%x to be same got=%x", expected, got)
		}
	}
}

func TestDictClear(t *testing.T) {
	data := util.RandByteSlices(10, 10)
	dup := makeDupmap(10)
	d := makeDictByteArray(10, 10, data, dup)

	if len(d.dict) == 0 {
		t.Errorf("TestDictClear: dict parameter should not be %d", len(d.dict))
	}
	if len(d.offs) == 0 {
		t.Errorf("TestDictClear: offs parameter should not be %d", len(d.offs))
	}
	if len(d.size) == 0 {
		t.Errorf("TestDictClear: size parameter should not be %d", len(d.size))
	}
	if len(d.ptr) == 0 {
		t.Errorf("TestDictClear: ptr parameter should not be %d", len(d.ptr))
	}
	if d.log2 == 0 {
		t.Errorf("TestDictClear: log2 parameter should not be %d", d.log2)
	}
	if d.n == 0 {
		t.Errorf("TestDictClear: n parameter should not be %d", d.n)
	}

	// clear DictByteArray
	d.Clear()

	if len(d.dict) != 0 {
		t.Errorf("TestDictClear: dict parameter should be 0, got: %d", len(d.dict))
	}
	if len(d.offs) != 0 {
		t.Errorf("TestDictClear: offs parameter should be 0, got: %d", len(d.offs))
	}
	if len(d.size) != 0 {
		t.Errorf("TestDictClear: size parameter should be 0, got: %d", len(d.size))
	}
	if len(d.ptr) != 0 {
		t.Errorf("TestDictClear: ptr parameter should be 0, got: %d", len(d.ptr))
	}
	if d.log2 != 0 {
		t.Errorf("TestDictClear: log2 parameter should be 0, got: %d", d.log2)
	}
	if d.n != 0 {
		t.Errorf("TestDictClear: n parameter should be 0, got: %d", d.n)
	}
}

func TestDictRelease(t *testing.T) {
	data := util.RandByteSlices(10, 10)
	dup := makeDupmap(10)
	d := makeDictByteArray(10, 10, data, dup)

	if len(d.dict) == 0 {
		t.Errorf("TestDictRelease: dict parameter should not be %d", len(d.dict))
	}
	if len(d.offs) == 0 {
		t.Errorf("TestDictRelease: offs parameter should not be %d", len(d.offs))
	}
	if len(d.size) == 0 {
		t.Errorf("TestDictRelease: size parameter should not be %d", len(d.size))
	}
	if len(d.ptr) == 0 {
		t.Errorf("TestDictRelease: ptr parameter should not be %d", len(d.ptr))
	}
	if d.log2 == 0 {
		t.Errorf("TestDictRelease: log2 parameter should not be %d", d.log2)
	}
	if d.n == 0 {
		t.Errorf("TestDictRelease: n parameter should not be %d", d.n)
	}

	// release DictByteArray
	d.Release()

	if d.dict != nil {
		t.Errorf("TestDictRelease: dict parameter should be nil, got: %v", d.dict)
	}
	if d.offs != nil {
		t.Errorf("TestDictRelease: offs parameter should be nil, got: %v", d.offs)
	}
	if d.size != nil {
		t.Errorf("TestDictRelease: size parameter should be nil, got: %v", d.size)
	}
	if d.ptr != nil {
		t.Errorf("TestDictRelease: ptr parameter should be nil, got: %v", d.ptr)
	}
}

func TestDictUnsupported(t *testing.T) {
	handler := func(name string) {
		if err := recover(); err == nil {
			t.Errorf("TestDictUnsupported: unsupported member function %q didn't panic", name)
		}
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
		if err != nil {
			t.Errorf("TestDictWriteTo: writing to buffer should not fail")
		}
		// 1 format, 4 len elements, 1 log2, 4 dict len elements offset size, 4 compressed offset size, ** compressed offset data, 4 dict size, ** dict data, 4 ptr size, ** ptr data (1)
		expectedSize := 1 + 4 + 1 + 4 + 4 + 0 + 4 + 0 + 4 + 1
		if int64(expectedSize) != n {
			t.Errorf("TestDictWriteTo: data expected to write %d but wrote %d", expectedSize, n)
		}
	})

	t.Run("With Data", func(t *testing.T) {
		data := util.RandByteSlices(10, 10)
		dup := makeDupmap(10)
		d := makeDictByteArray(10, 10, data, dup)

		buf := bytes.NewBuffer(nil)
		n, err := d.WriteTo(buf)
		if err != nil {
			t.Errorf("TestDictWriteTo: writing to buffer should not fail")
		}

		expectedSize := 139
		if int64(expectedSize) != n {
			t.Errorf("TestDictWriteTo: data expected to write %d but wrote %d", expectedSize, n)
		}
	})

	t.Run("With Large Data", func(t *testing.T) {
		sz := 10000
		data := util.RandByteSlices(sz, sz)
		dup := makeDupmap(sz)
		d := makeDictByteArray(sz, sz, data, dup)

		buf := bytes.NewBuffer(nil)
		n, err := d.WriteTo(buf)
		if err != nil {
			t.Errorf("TestDictWriteTo: writing to buffer should not fail")
		}

		expectedSize := 100017537
		if int64(expectedSize) != n {
			t.Errorf("TestDictWriteTo: data expected to write %d but wrote %d", expectedSize, n)
		}
	})

	t.Run("Faulty Writer", func(t *testing.T) {
		data := util.RandByteSlices(0, 0)
		dup := makeDupmap(0)
		d := makeDictByteArray(0, 0, data, dup)

		failAfter := 6
		buf := &FaultyWriter{failAfter: failAfter}
		z, err := d.WriteTo(buf)
		if err == nil {
			t.Errorf("TestDictWriteTo: writing to buffer should fail")
		}

		fmt.Println("faultyWriteTo - ", z, err)
		if int64(failAfter) != z {
			t.Errorf("TestDictWriteTo: data expected to write greater than or equal to %d but wrote %d", failAfter, z)
		}
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
			if !testCase.IsErrorExpected && err != nil {
				t.Errorf("TestDictReadFrom: %v", err)
			}

			n, err := d.ReadFrom(testCase.Reader)
			if testCase.IsErrorExpected {
				if err == nil {
					t.Errorf("TestDictReadFrom: %v", err)
				}
			} else {
				if n != int64(testCase.ReadSize) {
					t.Errorf("TestDictReadFrom: reader: %d expected: %d", n, testCase.ReadSize)
				}
			}
		})
	}
}
