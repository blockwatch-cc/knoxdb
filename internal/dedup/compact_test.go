package dedup

import (
	"bytes"
	"math/rand"
	"testing"
)

func TestCompactElem(t *testing.T) {
	rand.Seed(99)
	data := makeRandData(10, 10)
	dup := make([]int, 10)
	for i := range 10 {
		dup[i] = -1
	}
	c := makeCompactByteArray(10, 10, data, dup)
	if got, expected := c.Len(), 10; got != expected {
		t.Errorf("TestCompactElem: Len expected=%d but got=%d", expected, got)
	}
	if got, expected := c.Cap(), 10; got != expected {
		t.Errorf("TestCompactElem: Cap expected=%d but got=%d", expected, got)
	}
	for i := range data {
		if got, expected := c.Elem(i), data[i]; !bytes.Equal(got, expected) {
			t.Errorf("TestCompactElem: expected=%x to be same got=%x", expected, got)
		}
	}
}

// TestCompactWriteTo
// TestCompactReadFrom

func TestCompactUnsupported(t *testing.T) {
	handler := func(name string) {
		if err := recover(); err == nil {
			t.Errorf("TestCompactUnsupported: unsupported member function %q didn't panic", name)
		}
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
