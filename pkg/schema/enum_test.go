// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"encoding/hex"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Register a global enum and dictionary for all schema tests
type MyEnum string

var myEnum *EnumDictionary

func TestMain(m *testing.M) {
	myEnum = NewEnumDictionary("my_enum")
	myEnum.Append("a", "b", "c", "d", "e")
	RegisterEnum(0, myEnum)
	m.Run()
}

func (e *EnumDictionary) dump() {
	fmt.Printf("Values\n%s", hex.Dump(e.values))
	fmt.Printf("Offsets %v\n", e.offsets)
}

func TestEnumAdd(t *testing.T) {
	d := NewEnumDictionary("")
	d.Append("a", "b")
	t.Log("Added 2 values")
	// d.dump()
	assert.Equal(t, d.Len(), 2)

	t.Log("Lookup values")
	v, ok := d.Value(0)
	assert.True(t, ok, "val a")
	assert.Equal(t, v, "a")
	v, ok = d.Value(1)
	assert.True(t, ok, "val b")
	assert.Equal(t, v, "b")

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
	d.Append("c")
	// d.dump()
	assert.Equal(t, d.Len(), 3)
	v, ok = d.Value(2)
	assert.True(t, ok, "val c")
	assert.Equal(t, v, "c")
	c, ok = d.Code("c")
	assert.True(t, ok, "code c")
	assert.Equal(t, c, uint16(2), "code c")
}

func TestEnumSort(t *testing.T) {
	d := NewEnumDictionary("")
	d.Append("b", "a")
	t.Log("Added 2 values")
	// d.dump()
	assert.Equal(t, d.Len(), 2)

	t.Log("Lookup values")
	v, ok := d.Value(0)
	assert.True(t, ok, "val b")
	assert.Equal(t, v, "b")
	v, ok = d.Value(1)
	assert.True(t, ok, "val a")
	assert.Equal(t, v, "a")

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

func TestEnumMarshal(t *testing.T) {
	d := NewEnumDictionary("")
	d.Append("b", "a")
	t.Log("Added 2 values")
	assert.Equal(t, d.Len(), 2)

	t.Log("Marshal")
	buf, err := d.MarshalBinary()
	require.NoError(t, err)
	require.NotNil(t, buf)

	t.Log("Unmarshal")
	d2 := NewEnumDictionary("")
	err = d2.UnmarshalBinary(buf)
	require.NoError(t, err)

	t.Log("Lookup values")
	v, ok := d.Value(0)
	assert.True(t, ok, "val b")
	assert.Equal(t, v, "b")
	v, ok = d.Value(1)
	assert.True(t, ok, "val a")
	assert.Equal(t, v, "a")
}

var enumBenchSizes = []struct {
	name string
	num  int
}{
	{name: "1", num: 1},
	{name: "16", num: 16},
	{name: "256", num: 256},
	{name: "1k", num: 1024},
	{name: "4k", num: 4096},
	{name: "64k", num: 1 << 16},
}

func makeRandStrings(n int) []string {
	vals := []string{}
	for i := 0; i < n; i++ {
		vals = append(vals, hex.EncodeToString(Uint64Bytes(uint64(rand.Int63()))))
	}
	return vals
}

func makeEnum(name string, n int) *EnumDictionary {
	enum := NewEnumDictionary(name)
	err := enum.Append(makeRandStrings(n)...)
	if err != nil {
		panic(err)
	}
	return enum
}

func BenchmarkEnumAdd(b *testing.B) {
	for _, v := range enumBenchSizes {
		b.Run(v.name, func(b *testing.B) {
			vals := makeRandStrings(v.num)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				NewEnumDictionary(v.name).Append(vals...)
			}
		})
	}
}

func BenchmarkEnumValueLookup(b *testing.B) {
	for _, v := range enumBenchSizes {
		b.Run(v.name, func(b *testing.B) {
			enum := makeEnum(v.name, v.num)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = enum.Value(uint16(i % enum.Len()))
			}
		})
	}
}

func BenchmarkEnumCodeLookup(b *testing.B) {
	for _, v := range enumBenchSizes {
		b.Run(v.name, func(b *testing.B) {
			vals := makeRandStrings(v.num)
			enum := NewEnumDictionary(v.name)
			enum.Append(vals...)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = enum.Code(vals[i%enum.Len()])
			}
		})
	}
}

func TestEnumConversionScenarios(t *testing.T) {
	d := NewEnumDictionary("")
	d.Append("a", "b", "c")
	t.Log("Added 3 values")

	// Positive test: Convert string to uint16 using ParseValue
	t.Log("Positive test: ParseValue for string to uint16")
	val, err := d.ParseValue("b")
	assert.NoError(t, err, "ParseValue should succeed for 'b'")
	assert.Equal(t, uint16(1), val, "code for 'b' should be 1")

	// Negative test: ParseValue for unregistered string
	t.Log("Negative test: ParseValue for unregistered string")
	_, err = d.ParseValue("d")
	assert.Error(t, err, "ParseValue should fail for unregistered value 'd'")

	// Negative test: Attempt to cast non-uint16 type using CastValue
	t.Log("Negative test: CastValue for non-uint16 type")
	_, err = d.CastValue(123) // Pass an integer instead of a string
	assert.Error(t, err, "CastValue should fail for non-string type")
}
