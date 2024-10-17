// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type SubTypes struct {
	BaseModel           // 0
	Int64     int64     `knox:"i64"`    // 1
	Uint32    uint32    `knox:"u32"`    // 6
	Float64   float64   `knox:"f64"`    //10
	Bool      bool      `knox:"bool"`   // 18
	Time      time.Time `knox:"time"`   // 19
	Hash      []byte    `knox:"bytes"`  // 20
	String    string    `knox:"string"` // 22
}

type SubTypesReordered struct {
	Uint32  uint32    `knox:"u32"`    // 6
	Int64   int64     `knox:"i64"`    // 1
	Id      uint64    `knox:"id"`     // 0
	Float64 float64   `knox:"f64"`    // 10
	Time    time.Time `knox:"time"`   // 19
	String  string    `knox:"string"` // 22
	Hash    []byte    `knox:"bytes"`  // 20
	Bool    bool      `knox:"bool"`   // 18
}

func TestSchemaMapping(t *testing.T) {
	baseSchema, err := GenericSchema[AllTypes]()
	require.NoError(t, err)
	subSchema, err := GenericSchema[SubTypes]()
	require.NoError(t, err)
	reorderSchema, err := GenericSchema[SubTypesReordered]()
	require.NoError(t, err)

	b2s, err := subSchema.MapTo(baseSchema)
	require.NoError(t, err)
	require.Len(t, b2s, baseSchema.NumFields())
	require.Equal(t, []int{0, 1, -1, -1, -1, -1, 2, -1, -1, 3, -1, -1, -1, -1, -1, -1, -1, 4, 5, 6, -1, 7, -1}, b2s)

	b2r, err := reorderSchema.MapTo(baseSchema)
	require.NoError(t, err)
	require.Len(t, b2r, baseSchema.NumFields())
	require.Equal(t, []int{2, 1, -1, -1, -1, -1, 0, -1, -1, 3, -1, -1, -1, -1, -1, -1, -1, 7, 4, 6, -1, 5}, b2r)
}

func TestSchemaConvert(t *testing.T) {
	baseSchema := MustSchemaOf(AllTypes{})
	baseEnc := NewEncoder(baseSchema)

	// sub schema, same order
	subSchema := MustSchemaOf(SubTypes{})
	subDec := NewDecoder(subSchema)
	subConv := NewConverter(baseSchema, subSchema, binary.NativeEndian)
	t.Log(subSchema)

	for _, val := range []int64{
		int64(0x0faf0faf0faf0faf),
		int64(0x0eaddeaddeaddead),
		int64(0x0101010101010101),
		int64(0x0100010001000100),
		int64(0x0100000001000000),
		int64(0x0100000000000000),
	} {
		base := NewAllTypes(val)
		buf, err := baseEnc.Encode(&base, nil)
		require.NoError(t, err)
		require.NotNil(t, buf)
		buf2 := subConv.Extract(buf)
		require.NotNil(t, buf2)
		var sub SubTypes
		err = subDec.Decode(buf2, &sub)
		require.NoError(t, err)
		assert.Equal(t, base.Id, sub.Id)
		assert.Equal(t, base.Int64, sub.Int64)
		assert.Equal(t, base.Uint32, sub.Uint32)
		assert.Equal(t, base.Float64, sub.Float64)
		assert.Equal(t, base.Bool, sub.Bool)
		assert.Equal(t, base.Time, sub.Time)
		assert.Equal(t, base.Hash, sub.Hash)
		assert.Equal(t, base.String, sub.String)
	}

	// sub schema, different order
	reorderSchema := MustSchemaOf(SubTypesReordered{})
	reorderConv := NewConverter(baseSchema, reorderSchema, binary.NativeEndian)
	reorderDec := NewDecoder(reorderSchema)
	t.Log(reorderSchema)

	for _, val := range []int64{
		int64(0x0faf0faf0faf0faf),
		int64(0x0eaddeaddeaddead),
		int64(0x0101010101010101),
		int64(0x0100010001000100),
		int64(0x0100000001000000),
		int64(0x0100000000000000),
	} {
		base := NewAllTypes(val)
		buf, err := baseEnc.Encode(&base, nil)
		require.NoError(t, err)
		require.NotNil(t, buf)
		buf2 := reorderConv.Extract(buf)
		require.NotNil(t, buf2)
		var sub SubTypesReordered
		err = reorderDec.Decode(buf2, &sub)
		require.NoError(t, err)
		assert.Equal(t, base.Id, sub.Id)
		assert.Equal(t, base.Int64, sub.Int64)
		assert.Equal(t, base.Uint32, sub.Uint32)
		assert.Equal(t, base.Float64, sub.Float64)
		assert.Equal(t, base.Bool, sub.Bool)
		assert.Equal(t, base.Time, sub.Time)
		assert.Equal(t, base.Hash, sub.Hash)
		assert.Equal(t, base.String, sub.String)
	}
}

func BenchmarkSchemaConvertInorder(b *testing.B) {
	baseSchema := MustSchemaOf(AllTypes{})
	baseEnc := NewEncoder(baseSchema)
	base := NewAllTypes(int64(0x0faf0faf0faf0faf))
	buf, err := baseEnc.Encode(&base, nil)
	require.NoError(b, err)

	// sub schema, same order
	subSchema := MustSchemaOf(SubTypes{})
	subConv := NewConverter(baseSchema, subSchema, binary.NativeEndian)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = subConv.Extract(buf)
	}
}

func BenchmarkSchemaConvertReorder(b *testing.B) {
	baseSchema := MustSchemaOf(AllTypes{})
	baseEnc := NewEncoder(baseSchema)
	base := NewAllTypes(int64(0x0faf0faf0faf0faf))
	buf, err := baseEnc.Encode(&base, nil)
	require.NoError(b, err)

	// sub schema, different order
	subSchema := MustSchemaOf(SubTypesReordered{})
	subConv := NewConverter(baseSchema, subSchema, binary.NativeEndian)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = subConv.Extract(buf)
	}
}
