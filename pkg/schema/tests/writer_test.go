// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema_tests

import (
	"encoding/hex"
	"testing"

	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/schema/encode"
	"blockwatch.cc/knoxdb/pkg/schema/reflect"
	"github.com/stretchr/testify/require"
)

func TestWriterWrite(t *testing.T) {
	base := NewAllTypes(int64(0x0faf0faf0faf0faf))
	baseSchema := reflect.MustSchemaFor[AllTypes]()

	baseEnc := encode.NewEncoder(baseSchema)
	buf, err := baseEnc.Encode(&base, nil)
	require.NoError(t, err)

	w := schema.NewWriter(baseSchema, nil)
	require.NoError(t, w.Write(base.Id))
	require.NoError(t, w.Write(base.Int64))
	require.NoError(t, w.Write(base.Int32))
	require.NoError(t, w.Write(base.Int16))
	require.NoError(t, w.Write(base.Int8))
	require.NoError(t, w.Write(base.Uint64))
	require.NoError(t, w.Write(base.Uint32))
	require.NoError(t, w.Write(base.Uint16))
	require.NoError(t, w.Write(base.Uint8))
	require.NoError(t, w.Write(base.Float64))
	require.NoError(t, w.Write(base.Float32))
	require.NoError(t, w.Write(base.D32))
	require.NoError(t, w.Write(base.D64))
	require.NoError(t, w.Write(base.D128))
	require.NoError(t, w.Write(base.D256))
	require.NoError(t, w.Write(base.I128))
	require.NoError(t, w.Write(base.I256))
	require.NoError(t, w.Write(base.Bool))
	require.NoError(t, w.Write(base.Time))
	require.NoError(t, w.Write(base.Hash))
	require.NoError(t, w.Write(base.Array[:]))
	require.NoError(t, w.Write(base.String))
	require.NoError(t, w.Write(string(base.MyEnum)))
	require.NoError(t, w.Write(base.Big))
	require.NoError(t, w.Write(base.Duration))
	require.NoError(t, w.Write(base.Union))
	require.True(t, w.Done())

	require.Equal(t, buf, w.Bytes())
}

func TestWriterPrimitive(t *testing.T) {
	base := NewAllTypes(int64(0x0faf0faf0faf0faf))
	baseSchema := reflect.MustSchemaFor[AllTypes]()

	baseEnc := encode.NewEncoder(baseSchema)
	buf, err := baseEnc.Encode(&base, nil)
	require.NoError(t, err)

	w := schema.NewWriter(baseSchema, nil)
	require.NoError(t, w.WriteUint64(base.Id))
	require.NoError(t, w.WriteInt64(base.Int64))
	require.NoError(t, w.WriteInt32(base.Int32))
	require.NoError(t, w.WriteInt16(base.Int16))
	require.NoError(t, w.WriteInt8(base.Int8))
	require.NoError(t, w.WriteUint64(base.Uint64))
	require.NoError(t, w.WriteUint32(base.Uint32))
	require.NoError(t, w.WriteUint16(base.Uint16))
	require.NoError(t, w.WriteUint8(base.Uint8))
	require.NoError(t, w.WriteFloat64(base.Float64))
	require.NoError(t, w.WriteFloat32(base.Float32))
	require.NoError(t, w.WriteDecimal32(base.D32))
	require.NoError(t, w.WriteDecimal64(base.D64))
	require.NoError(t, w.WriteDecimal128(base.D128))
	require.NoError(t, w.WriteDecimal256(base.D256))
	require.NoError(t, w.WriteInt128(base.I128))
	require.NoError(t, w.WriteInt256(base.I256))
	require.NoError(t, w.WriteBool(base.Bool))
	require.NoError(t, w.WriteTimestamp(base.Time))
	require.NoError(t, w.WriteBytes(base.Hash))
	require.NoError(t, w.WriteBytes(base.Array[:]))
	require.NoError(t, w.WriteString(base.String))
	require.NoError(t, w.WriteEnum(string(base.MyEnum)))
	require.NoError(t, w.WriteBigint(base.Big))
	require.NoError(t, w.WriteDuration(base.Duration))
	require.NoError(t, w.WriteUnion(base.Union))
	require.True(t, w.Done())

	require.Equal(t, buf, w.Bytes())
}

func TestWriterListL1(t *testing.T) {
	// one level nested
	base := NewListFields()

	baseSchema := reflect.MustSchemaFor[ListFields]()
	baseEnc := encode.NewEncoder(baseSchema)

	// encode
	buf, err := baseEnc.Encode(&base, nil)
	require.NoError(t, err)
	require.LessOrEqual(t, baseSchema.MinWireSize, len(buf))

	// write
	w := schema.NewWriter(baseSchema, nil)
	require.NoError(t, w.WriteInt64(base.Int64a))

	// []uint64
	lw, err := w.ListWriter()
	require.NoError(t, err)
	require.NoError(t, lw.WriteUint64(base.U64List[0]))
	lw.Next()
	require.NoError(t, lw.WriteUint64(base.U64List[1]))
	lw.Close()

	// []time
	lw, err = w.ListWriter()
	require.NoError(t, err)
	require.NoError(t, lw.WriteDate(base.TimeList[0]))
	lw.Next()
	require.NoError(t, lw.WriteDate(base.TimeList[1]))
	lw.Close()

	// []Pair
	lw, err = w.ListWriter()
	require.NoError(t, err)
	require.NoError(t, lw.WriteInt64(base.PairList[0].Key))
	require.NoError(t, lw.WriteInt64(base.PairList[0].Val))
	lw.Next()
	require.NoError(t, lw.WriteInt64(base.PairList[1].Key))
	require.NoError(t, lw.WriteInt64(base.PairList[1].Val))
	lw.Close()

	// [][]byte
	lw, err = w.ListWriter()
	require.NoError(t, err)
	require.NoError(t, lw.WriteBytes(base.ByteList[0]))
	lw.Next()
	require.NoError(t, lw.WriteBytes(base.ByteList[1]))
	lw.Close()

	// [][2]byte
	lw, err = w.ListWriter()
	require.NoError(t, err)
	require.NoError(t, lw.WriteBytes(base.ArrList[0][:]))
	lw.Next()
	require.NoError(t, lw.WriteBytes(base.ArrList[1][:]))
	lw.Close()

	// []Decimal32
	lw, err = w.ListWriter()
	require.NoError(t, err)
	require.NoError(t, lw.WriteDecimal32(base.DecimalList[0]))
	lw.Next()
	require.NoError(t, lw.WriteDecimal32(base.DecimalList[1]))
	lw.Next()
	require.NoError(t, lw.WriteDecimal32(base.DecimalList[2]))
	lw.Close()

	require.NoError(t, w.WriteInt64(base.Int64b))
	require.True(t, w.Done())

	// check writer and encoder produce the same bytes
	require.Equal(t, buf, w.Bytes())

	// test decoder can read the bytes
	dec := encode.NewDecoder(baseSchema)
	var res ListFields
	require.NoError(t, dec.Decode(buf, &res))

	// check decoder produces the exact same struct values
	require.Equal(t, base, res)
}

func TestWriterListL2(t *testing.T) {
	// one level nested
	base := NewListInListFields()

	baseSchema := reflect.MustSchemaFor[ListInListFields]()
	baseEnc := encode.NewEncoder(baseSchema)

	// encode
	buf, err := baseEnc.Encode(&base, nil)
	require.NoError(t, err)
	require.LessOrEqual(t, baseSchema.MinWireSize, len(buf))

	// write
	w := schema.NewWriter(baseSchema, nil)
	require.NoError(t, w.WriteInt64(base.Int64a))

	// [][]uint64
	lw, err := w.ListWriter()
	require.NoError(t, err)
	lwi, err := w.ListWriter()
	require.NoError(t, err)
	require.NoError(t, lwi.WriteUint64(base.NestedUints[0][0]))
	lwi.Next()
	require.NoError(t, lwi.WriteUint64(base.NestedUints[0][1]))
	lwi.Close()
	lw.Next()
	lwi, err = w.ListWriter()
	require.NoError(t, err)
	require.NoError(t, lwi.WriteUint64(base.NestedUints[1][0]))
	lwi.Next()
	require.NoError(t, lwi.WriteUint64(base.NestedUints[1][1]))
	lwi.Close()
	lw.Close()

	// []Pair
	lw, err = w.ListWriter()
	require.NoError(t, err)
	lwi, err = w.ListWriter()
	require.NoError(t, err)
	require.NoError(t, lwi.WriteInt64(base.NestedPairs[0][0].Key))
	require.NoError(t, lwi.WriteInt64(base.NestedPairs[0][0].Val))
	lwi.Next()
	require.NoError(t, lw.WriteInt64(base.NestedPairs[0][1].Key))
	require.NoError(t, lw.WriteInt64(base.NestedPairs[0][1].Val))
	lwi.Close()
	lw.Next()
	lwi, err = w.ListWriter()
	require.NoError(t, err)
	require.NoError(t, lwi.WriteInt64(base.NestedPairs[1][0].Key))
	require.NoError(t, lwi.WriteInt64(base.NestedPairs[1][0].Val))
	lwi.Next()
	require.NoError(t, lw.WriteInt64(base.NestedPairs[1][1].Key))
	require.NoError(t, lw.WriteInt64(base.NestedPairs[1][1].Val))
	lwi.Close()
	lw.Close()

	require.NoError(t, w.WriteInt64(base.Int64b))
	require.True(t, w.Done())

	// check writer and encoder produce the same bytes
	require.Equal(t, buf, w.Bytes())

	// test decoder can read the bytes
	dec := encode.NewDecoder(baseSchema)
	var res ListInListFields
	require.NoError(t, dec.Decode(buf, &res))

	// check decoder produces the exact same struct values
	require.Equal(t, base, res)
}

func TestWriterListL3(t *testing.T) {
	// one level nested
	base := NewListInStructInListFields()

	baseSchema := reflect.MustSchemaFor[ListInStructInListFields]()
	baseEnc := encode.NewEncoder(baseSchema)

	// encode
	buf, err := baseEnc.Encode(&base, nil)
	require.NoError(t, err)
	require.LessOrEqual(t, baseSchema.MinWireSize, len(buf))

	// write
	w := schema.NewWriter(baseSchema, nil)
	require.NoError(t, w.WriteInt64(base.Int64a))

	// []OuterPairStruct
	lw, err := w.ListWriter()
	require.NoError(t, err)
	require.NoError(t, lw.WriteUint32(base.Pairs1[0].Val))
	lwi, err := w.ListWriter()
	require.NoError(t, err)
	require.NoError(t, lwi.WriteInt64(base.Pairs1[0].Pairs2[0].Key))
	require.NoError(t, lwi.WriteInt64(base.Pairs1[0].Pairs2[0].Val))
	lwi.Next()
	require.NoError(t, lw.WriteInt64(base.Pairs1[0].Pairs2[1].Key))
	require.NoError(t, lw.WriteInt64(base.Pairs1[0].Pairs2[1].Val))
	lwi.Close()
	lw.Next()
	require.NoError(t, lw.WriteUint32(base.Pairs1[1].Val))
	lwi, err = w.ListWriter()
	require.NoError(t, err)
	require.NoError(t, lwi.WriteInt64(base.Pairs1[1].Pairs2[0].Key))
	require.NoError(t, lwi.WriteInt64(base.Pairs1[1].Pairs2[0].Val))
	lwi.Next()
	require.NoError(t, lw.WriteInt64(base.Pairs1[1].Pairs2[1].Key))
	require.NoError(t, lw.WriteInt64(base.Pairs1[1].Pairs2[1].Val))
	lwi.Close()
	lw.Close()

	require.NoError(t, w.WriteInt64(base.Int64b))
	require.True(t, w.Done())

	// check writer and encoder produce the same bytes
	require.Equal(t, buf, w.Bytes())

	// test decoder can read the bytes
	dec := encode.NewDecoder(baseSchema)
	var res ListInStructInListFields
	require.NoError(t, dec.Decode(buf, &res))

	// check decoder produces the exact same struct values
	require.Equal(t, base, res)
}

func TestWriterUnion(t *testing.T) {
	s := schema.SchemaOf(
		[]*schema.Field{
			schema.FieldOf(schema.Union), // u64
			schema.FieldOf(schema.Union), // u32
			schema.FieldOf(schema.Union), // u16
			schema.FieldOf(schema.Union), // u8
			schema.FieldOf(schema.Union), // string
			schema.FieldOf(schema.Union), // []byte
			schema.FieldOf(schema.Union), // bool
			schema.FieldOf(schema.Union), // i128
		},
		schema.Name("test"),
	)

	require.Equal(t, 8, s.MinWireSize)

	testValues := []schema.UnionValue{
		schema.Uint64Union(2000),
		schema.Uint32Union(200),
		schema.Uint16Union(20),
		schema.Uint8Union(2),
		schema.StringUnion("hello"),
		schema.BytesUnion([]byte{42}),
		schema.BoolUnion(true),
		schema.Int128Union(num.Int128FromInt64(23)),
	}

	v := schema.NewView(s)
	w := schema.NewWriter(s, nil)

	// write
	w.Reset()
	for _, val := range testValues {
		n := w.Len()
		require.NoError(t, w.WriteUnion(val), "write val=%s", val)
		t.Logf("Union %s %q => %s", val.Type(), val, hex.Dump(w.Bytes()[n:]))
	}
	require.True(t, w.Done(), "done")
	require.LessOrEqual(t, s.MinWireSize, w.Len())

	// read back
	v.Reset(w.Bytes())
	for j, val := range testValues {
		require.Equal(t, val, v.Union(j), "val")
	}
}

func TestWriterSkip(t *testing.T) {
	s := schema.SchemaOf(
		[]*schema.Field{
			schema.FieldOf(schema.Int64),  // 8
			schema.FieldOf(schema.String), // 1
			schema.FieldOf(schema.Uint32), // 4
			schema.FieldOf(schema.Binary), // 4
			schema.FieldOf(schema.Union),  // 1, has 3 nested metadata fields!
			schema.FieldOf(schema.Uint16), // 2
		},
		schema.Name("test"),
	)

	require.Equal(t, 20, s.MinWireSize)

	testValues := []any{
		int64(5),
		"hello",
		uint32(42),
		[]byte("world"),
		schema.Uint64Union(2),
		uint16(23),
	}

	v := schema.NewView(s)
	w := schema.NewWriter(s, nil)

	for i := range testValues {
		// write and skip
		w.Reset()
		for j, val := range testValues {
			if j == i {
				require.NoError(t, w.Skip(), "skip i=%d j=%d", i, j)
			} else {
				require.NoError(t, w.Write(val), "write i=%d j=%d", i, j)
			}
		}
		require.True(t, w.Done(), "done")
		require.LessOrEqual(t, s.MinWireSize, w.Len())

		// read back
		v.Reset(w.Bytes())
		for j, val := range testValues {
			if j == i {
				// expect zero
				require.Equal(t, s.Field(j).Type.Zero(), v.Get(j), "skip=zero")
			} else {
				// expect original value
				require.Equal(t, val, v.Get(j), "val")
			}
		}
	}
}

func TestWriterMap(t *testing.T) {
	// primitives
	s, err := reflect.SchemaFor[PrimMapRecord]()
	require.NoError(t, err)
	t.Log(s)
	base := NewPrimMapRecord()
	w := schema.NewWriter(s, nil)
	require.NoError(t, w.Write(base))
	t.Log(hex.Dump(w.Bytes()))

	// marshaler only
	attr := NewUnionMapRecord()
	w = schema.NewWriter(UnionMapRecordSchema, nil)
	require.NoError(t, w.Write(attr))
	t.Log(hex.Dump(w.Bytes()))
}
