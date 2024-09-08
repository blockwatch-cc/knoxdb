// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func (e *EnumDictionary) dump() {
	fmt.Printf("Values\n%s", hex.Dump(e.values))
	fmt.Printf("Offsets %v\n", e.offsets)
	fmt.Printf("Sorted %v\n", e.sorted)
}

func TestEnumAdd(t *testing.T) {
	d := NewEnumDictionary("")
	d.AddValues("a", "b")
	t.Log("Added 2 values")
	d.dump()
	assert.Equal(t, d.Len(), 2)

	t.Log("Lookup values")
	v, ok := d.Value(0)
	assert.True(t, ok, "val a")
	assert.Equal(t, v, Enum("a"))
	v, ok = d.Value(1)
	assert.True(t, ok, "val b")
	assert.Equal(t, v, Enum("b"))

	t.Log("Lookup codes")
	c, ok := d.Code("a")
	assert.True(t, ok, "code a")
	assert.Equal(t, c, uint16(0), "code a")
	c, ok = d.Code("b")
	assert.True(t, ok, "code b")
	assert.Equal(t, c, uint16(1), "code b")

	t.Log("Lookup undefined")
	_, ok = d.Value(2)
	assert.False(t, ok, "overflow")
	_, ok = d.Code("c")
	assert.False(t, ok, "overflow")

	t.Log("Adding 1 more value")
	d.AddValues("c")
	d.dump()
	assert.Equal(t, d.Len(), 3)
	v, ok = d.Value(2)
	assert.True(t, ok, "val c")
	assert.Equal(t, v, Enum("c"))
	c, ok = d.Code("c")
	assert.True(t, ok, "code c")
	assert.Equal(t, c, uint16(2), "code c")
}

func TestEnumSort(t *testing.T) {
	d := NewEnumDictionary("")
	d.AddValues("b", "a")
	t.Log("Added 2 values")
	d.dump()
	assert.Equal(t, d.Len(), 2)

	t.Log("Lookup values")
	v, ok := d.Value(0)
	assert.True(t, ok, "val b")
	assert.Equal(t, v, Enum("b"))
	v, ok = d.Value(1)
	assert.True(t, ok, "val a")
	assert.Equal(t, v, Enum("a"))

	t.Log("Lookup codes")
	c, ok := d.Code("a")
	assert.True(t, ok, "code a")
	assert.Equal(t, c, uint16(1), "code a")
	c, ok = d.Code("b")
	assert.True(t, ok, "code b")
	assert.Equal(t, c, uint16(0), "code b")

	t.Log("Lookup undefined")
	_, ok = d.Value(2)
	assert.False(t, ok, "overflow")
	_, ok = d.Code("c")
	assert.False(t, ok, "overflow")
}
