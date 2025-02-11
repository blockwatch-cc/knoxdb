// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"encoding/binary"
	"runtime"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type SubTypes struct {
	BaseModel           // 0
	Int64     int64     `knox:"i64"`    // 1
	Uint32    uint32    `knox:"u32"`    // 6
	Float64   float64   `knox:"f64"`    // 10
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

type FixedSubTypes struct {
	BaseModel            // 0
	FixedArray  [20]byte `knox:"fixed_array"`           // 1
	FixedString string   `knox:"fixed_string,fixed=20"` // 3
}

type FixedSubTypesReordered struct {
	FixedBytes []byte   `knox:"fixed_bytes,fixed=20"` // 2
	FixedArray [20]byte `knox:"fixed_array"`          // 1
}

func NewSubTypes(i int64) SubTypes {
	return SubTypes{
		BaseModel: BaseModel{
			Id: uint64(i),
		},
		Int64:   i,
		Uint32:  uint32(i),
		Float64: float64(i),
		Bool:    i%2 == 1,
		Time:    time.Unix(0, i).UTC(),
		Hash:    util.U64Bytes(uint64(i)),
		String:  util.U64Hex(uint64(i)),
	}
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
	require.Equal(t, []int{2, 1, -1, -1, -1, -1, 0, -1, -1, 3, -1, -1, -1, -1, -1, -1, -1, 7, 4, 6, -1, 5, -1}, b2r)
}

func TestSchemaConvert(t *testing.T) {
	baseSchema := MustSchemaOf(AllTypes{})
	baseEnc := NewEncoder(baseSchema)

	t.Run("var", func(t *testing.T) {
		// sub schema, same order
		subSchema := MustSchemaOf(SubTypes{})
		subDec := NewDecoder(subSchema)
		subConv := NewConverter(baseSchema, subSchema, binary.NativeEndian)

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
	})

	t.Run("var_reordered", func(t *testing.T) {
		// sub schema, different order
		reorderSchema := MustSchemaOf(SubTypesReordered{})
		reorderConv := NewConverter(baseSchema, reorderSchema, binary.NativeEndian)
		reorderDec := NewDecoder(reorderSchema)

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
	})

	// fixed schema
	fixSchema := MustSchemaOf(FixedTypes{})
	fixEnc := NewEncoder(fixSchema)

	t.Run("fix", func(t *testing.T) {
		fixSubSchema := MustSchemaOf(FixedSubTypes{})
		fixConv := NewConverter(fixSchema, fixSubSchema, binary.NativeEndian)
		fixDec := NewDecoder(fixSubSchema)

		for _, val := range []int64{
			int64(0x0faf0faf0faf0faf),
			int64(0x0eaddeaddeaddead),
			int64(0x0101010101010101),
			int64(0x0100010001000100),
			int64(0x0100000001000000),
			int64(0x0100000000000000),
		} {
			base := NewFixedTypes(val)
			buf, err := fixEnc.Encode(&base, nil)
			require.NoError(t, err)
			require.NotNil(t, buf)
			buf2 := fixConv.Extract(buf)
			require.NotNil(t, buf2)
			var sub FixedSubTypes
			err = fixDec.Decode(buf2, &sub)
			require.NoError(t, err)
			assert.Equal(t, base.Id, sub.Id)
			assert.Equal(t, base.FixedArray, sub.FixedArray)
			assert.Equal(t, base.FixedString, sub.FixedString)
		}
	})

	t.Run("fix_reordered", func(t *testing.T) {
		fixSubSchema := MustSchemaOf(FixedSubTypesReordered{})
		fixConv := NewConverter(fixSchema, fixSubSchema, binary.NativeEndian)
		fixDec := NewDecoder(fixSubSchema)

		for _, val := range []int64{
			int64(0x0faf0faf0faf0faf),
			int64(0x0eaddeaddeaddead),
			int64(0x0101010101010101),
			int64(0x0100010001000100),
			int64(0x0100000001000000),
			int64(0x0100000000000000),
		} {
			base := NewFixedTypes(val)
			buf, err := fixEnc.Encode(&base, nil)
			require.NoError(t, err)
			require.NotNil(t, buf)
			buf2 := fixConv.Extract(buf)
			require.NotNil(t, buf2)
			var sub FixedSubTypesReordered
			err = fixDec.Decode(buf2, &sub)
			require.NoError(t, err)
			assert.Equal(t, base.FixedArray, sub.FixedArray)
			assert.Equal(t, base.FixedBytes, sub.FixedBytes)
			runtime.KeepAlive(buf2)
		}
	})
}

func TestSchemaConvertWithVisibility(t *testing.T) {
	// convert data from parent to child after deleting & adding fields
	varSchema := MustSchemaOf(AllTypes{})
	varSchema, err := varSchema.DeleteField(2)
	require.NoError(t, err)
	penc := NewEncoder(varSchema)

	t.Run("var", func(t *testing.T) {
		child := MustSchemaOf(SubTypes{})
		cdec := NewDecoder(child)
		conv := NewConverter(varSchema, child, binary.NativeEndian)

		for _, val := range []int64{
			int64(0x0faf0faf0faf0faf),
			int64(0x0eaddeaddeaddead),
			int64(0x0101010101010101),
			int64(0x0100010001000100),
			int64(0x0100000001000000),
			int64(0x0100000000000000),
		} {
			base := NewAllTypes(val)
			buf, err := penc.Encode(&base, nil)
			require.NoError(t, err)
			require.NotNil(t, buf)
			buf2 := conv.Extract(buf)
			require.NotNil(t, buf2)
			var sub SubTypes
			err = cdec.Decode(buf2, &sub)
			require.NoError(t, err)
			assert.Equal(t, base.Id, sub.Id)
			assert.Equal(t, int64(0), sub.Int64) // deleted field
			assert.Equal(t, base.Uint32, sub.Uint32)
			assert.Equal(t, base.Float64, sub.Float64)
			assert.Equal(t, base.Bool, sub.Bool)
			assert.Equal(t, base.Time, sub.Time)
			assert.Equal(t, base.Hash, sub.Hash)
			assert.Equal(t, base.String, sub.String)
		}
	})

	t.Run("var_reorder", func(t *testing.T) {
		// sub schema, different order
		child2 := MustSchemaOf(SubTypesReordered{})
		conv2 := NewConverter(varSchema, child2, binary.NativeEndian)
		dec2 := NewDecoder(child2)

		for _, val := range []int64{
			int64(0x0faf0faf0faf0faf),
			int64(0x0eaddeaddeaddead),
			int64(0x0101010101010101),
			int64(0x0100010001000100),
			int64(0x0100000001000000),
			int64(0x0100000000000000),
		} {
			base := NewAllTypes(val)
			buf, err := penc.Encode(&base, nil)
			require.NoError(t, err)
			require.NotNil(t, buf)
			buf2 := conv2.Extract(buf)
			require.NotNil(t, buf2)
			var sub SubTypesReordered
			err = dec2.Decode(buf2, &sub)
			require.NoError(t, err)
			assert.Equal(t, base.Id, sub.Id)
			assert.Equal(t, int64(0), sub.Int64) // deleted field
			assert.Equal(t, base.Uint32, sub.Uint32)
			assert.Equal(t, base.Float64, sub.Float64)
			assert.Equal(t, base.Bool, sub.Bool)
			assert.Equal(t, base.Time, sub.Time)
			assert.Equal(t, base.Hash, sub.Hash)
			assert.Equal(t, base.String, sub.String)
		}
	})

	// fixed schema
	fixSchema := MustSchemaOf(FixedTypes{})
	fixSchema, err = fixSchema.DeleteField(2)
	require.NoError(t, err)
	fixEnc := NewEncoder(fixSchema)

	t.Run("fix", func(t *testing.T) {
		fixSubSchema := MustSchemaOf(FixedSubTypes{})
		fixConv := NewConverter(fixSchema, fixSubSchema, binary.NativeEndian)
		fixDec := NewDecoder(fixSubSchema)

		for _, val := range []int64{
			int64(0x0faf0faf0faf0faf),
			int64(0x0eaddeaddeaddead),
			int64(0x0101010101010101),
			int64(0x0100010001000100),
			int64(0x0100000001000000),
			int64(0x0100000000000000),
		} {
			base := NewFixedTypes(val)
			buf, err := fixEnc.Encode(&base, nil)
			require.NoError(t, err)
			require.NotNil(t, buf)
			buf2 := fixConv.Extract(buf)
			require.NotNil(t, buf2)
			var sub FixedSubTypes
			err = fixDec.Decode(buf2, &sub)
			require.NoError(t, err)
			assert.Equal(t, base.Id, sub.Id)
			assert.Equal(t, [20]byte{}, sub.FixedArray) // deleted field
			assert.Equal(t, base.FixedString, sub.FixedString)
		}
	})

	t.Run("fix_reordered", func(t *testing.T) {
		fixSubSchema := MustSchemaOf(FixedSubTypesReordered{})
		fixConv := NewConverter(fixSchema, fixSubSchema, binary.NativeEndian)
		fixDec := NewDecoder(fixSubSchema)
		t.Logf("%#v", fixConv)

		for _, val := range []int64{
			int64(0x0faf0faf0faf0faf),
			int64(0x0eaddeaddeaddead),
			int64(0x0101010101010101),
			int64(0x0100010001000100),
			int64(0x0100000001000000),
			int64(0x0100000000000000),
		} {
			base := NewFixedTypes(val)
			buf, err := fixEnc.Encode(&base, nil)
			require.NoError(t, err)
			require.NotNil(t, buf)
			buf2 := fixConv.Extract(buf)
			require.NotNil(t, buf2)
			var sub FixedSubTypesReordered
			err = fixDec.Decode(buf2, &sub)
			require.NoError(t, err)
			assert.Equal(t, [20]byte{}, sub.FixedArray) // deleted field
			assert.Equal(t, base.FixedBytes, sub.FixedBytes)
			runtime.KeepAlive(buf2)
		}
	})
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
