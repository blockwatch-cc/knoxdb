// Copyright (c) 2024-2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"encoding/hex"
	"strconv"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/internal/tests/testutil"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/schema/enum"
	"blockwatch.cc/knoxdb/pkg/schema/reflect"
	"github.com/stretchr/testify/require"
)

// Register a global enum and dictionary for all schema tests
type MyEnum string

var (
	enums  *enum.EnumRegistry
	myEnum *enum.EnumDictionary
)

func TestMain(m *testing.M) {
	// prepare enum
	myEnum = enum.NewEnumDictionary("my_enum")
	myEnum.Append("a", "b", "c", "d", "e")

	// create test registry and add enum to registry
	enums = enum.NewEnumRegistry()
	enums.Register(0, myEnum)

	// init schema and link enums (will lookup myEnum and link to field)
	reflect.MustSchemaFor[encodeTestStruct](schema.Enums(enums))
	reflect.MustSchemaFor[encodeBenchStruct](schema.Enums(enums))

	m.Run()
}

type KV struct {
	Key uint64
	Val uint64
}

type Hash [32]byte

type encodeTestStruct struct {
	Id          uint64         `knox:"id,pk"`
	Time        time.Time      `knox:"time"`
	HashArray   [20]byte       `knox:"hash_array,filter=bloom3b"`
	HashHash    Hash           `knox:"hash_hash,filter=bloom5b,zip=snappy"`
	String      string         `knox:"str"`
	Bool        bool           `knox:"bool"`
	Enum        MyEnum         `knox:"my_enum,enum"`
	Int64       int64          `knox:"i64"`
	Int32       int32          `knox:"i32"`
	Int16       int16          `knox:"i16"`
	Int8        int8           `knox:"i8"`
	Uint64      uint64         `knox:"u64,filter=bloom2b"`
	Uint32      uint32         `knox:"u32"`
	Uint16      uint16         `knox:"u16"`
	Uint8       uint8          `knox:"u8"`
	Float64     float64        `knox:"f64"`
	Float32     float32        `knox:"f32"`
	D32         num.Decimal32  `knox:"d32,scale=5"`
	D64         num.Decimal64  `knox:"d64,scale=15"`
	D128        num.Decimal128 `knox:"d128,scale=18"`
	D256        num.Decimal256 `knox:"d256,scale=24"`
	I128        num.Int128     `knox:"i128"`
	I256        num.Int256     `knox:"i256"`
	Big         num.Big        `knox:"big"`
	U64List     []uint64       `knox:"u64l"`
	U64ListList [][]uint64     `knox:"u64ll"`
	KVList      []KV           `knox:"kvl"`
}

func makeTestData(sz int) (res []encodeTestStruct) {
	u64 := uint64(sz)
	for i := 1; i <= sz; i++ {
		res = append(res, encodeTestStruct{
			Id:        0,
			Time:      time.Now().UTC(),
			HashArray: [20]byte(testutil.RandBytes(20)),
			HashHash:  Hash(testutil.RandBytes(32)),
			String:    hex.EncodeToString(testutil.RandBytes(4)),
			Bool:      true,
			Enum:      MyEnum(myEnum.MustValue(uint16(i%4 + 1))),
			Int64:     int64(i),
			Int32:     int32(i),
			Int16:     int16(i % (1<<16 - 1)),
			Int8:      int8(i % (1<<8 - 1)),
			Uint64:    uint64(i * 1000000),
			Uint32:    uint32(i * 1000000),
			Uint16:    uint16(i),
			Uint8:     uint8(i),
			Float32:   float32(i / 1000000),
			Float64:   float64(i / 1000000),
			D32:       num.NewDecimal32(int32(100123456789-i), 5),
			D64:       num.NewDecimal64(1123456789123456789-int64(i), 15),
			D128:      num.NewDecimal128(num.MustParseInt128(strconv.Itoa(i)+"00000000000000000000"), 18),
			D256:      num.NewDecimal256(num.MustParseInt256(strconv.Itoa(i)+"0000000000000000000000000000000000000000"), 24),
			I128:      num.MustParseInt128(strconv.Itoa(i) + "000000000000000000000000000000"),
			I256:      num.MustParseInt256(strconv.Itoa(i) + "000000000000000000000000000000000000000000000000000000000000"),
			Big:       num.NewBig(int64(i)),
			U64List:   []uint64{u64, u64 + 1, u64 + 2},
			U64ListList: [][]uint64{
				{u64, u64 + 1, u64 + 2},
				{u64 + 3, u64 + 4, u64 + 5},
			},
			KVList: []KV{
				{Key: u64, Val: u64},
				{Key: u64 + 1, Val: u64 + 1},
			},
		})
	}
	return
}

type visibilityTestStruct struct {
	Id           uint64 `knox:"id,pk"`
	FDeleted     uint64 `knox:"f_deleted"`
	FMeta        uint64 `knox:"f_meta,metadata"`
	FMetaDeleted uint64 `knox:"f_meta_deleted,metadata"`
	Hash         Hash   `knox:"hash"`
}

func makeVisibilityTestData(sz int) (res []visibilityTestStruct) {
	for i := 1; i <= sz; i++ {
		res = append(res, visibilityTestStruct{
			Id:           0,
			FDeleted:     0xfafafafafafafafa,
			FMeta:        0xfbfbfbfbfbfbfbfb,
			FMetaDeleted: 0xfcfcfcfcfcfcfcfc,
			Hash:         Hash(testutil.RandBytes(32)),
		})
	}
	return
}

func TestEncodeVal(t *testing.T) {
	vals := makeTestData(1)
	val := vals[0]
	enc := NewEncoderFor[encodeTestStruct]()
	buf, err := enc.Encode(val, nil)
	require.NoError(t, err)
	require.NotNil(t, buf)
	require.NotEmpty(t, buf)
	require.LessOrEqual(t, enc.Schema().MinWireSize, len(buf))
}

func TestEncodeValWithVisibility(t *testing.T) {
	// visibility tests (internal & deleted fields)
	s, err := reflect.SchemaFor[visibilityTestStruct]()
	require.NoError(t, err)
	val := makeVisibilityTestData(1)[0]
	s, err = s.DeleteId(2)
	require.NoError(t, err)
	s, err = s.DeleteId(4)
	require.NoError(t, err)
	enc := NewEncoder(s)
	buf, err := enc.Encode(&val, nil)
	require.NoError(t, err)
	require.NotNil(t, buf)
	require.NotEmpty(t, buf)
	require.Equal(t, s.MinWireSize, len(buf))
	require.Equal(t, 40, len(buf))
}

func TestEncodeRoundtrip(t *testing.T) {
	vals := makeTestData(1)
	val := vals[0]
	enc := NewEncoderFor[encodeTestStruct]()
	buf, err := enc.Encode(val, nil)
	require.NoError(t, err)
	require.NotNil(t, buf)
	require.NotEmpty(t, buf)
	require.LessOrEqual(t, enc.Schema().MinWireSize, len(buf))

	dec := NewDecoderFor[encodeTestStruct]()
	val2, err := dec.Decode(buf, nil)
	require.NoError(t, err)
	require.IsType(t, val, *val2)
	require.Exactly(t, val, *val2)
}

func TestEncodeRoundtripWithVisibility(t *testing.T) {
	// visibility tests (internal & deleted fields)
	s, err := reflect.SchemaFor[visibilityTestStruct]()
	require.NoError(t, err)
	s, err = s.DeleteId(2)
	require.NoError(t, err)
	s, err = s.DeleteId(4)
	require.NoError(t, err)
	val := makeVisibilityTestData(1)[0]
	enc := NewEncoder(s)
	buf, err := enc.Encode(&val, nil)
	require.NoError(t, err)
	require.NotNil(t, buf)
	require.NotEmpty(t, buf)
	require.Len(t, buf, s.MinWireSize)

	t.Log("Buf\n", hex.Dump(buf))

	dec := NewDecoder(s)
	var val2 visibilityTestStruct
	err = dec.Decode(buf, &val2)
	require.NoError(t, err)
	require.Equal(t, val.Id, val2.Id)
	require.Equal(t, val.Hash, val2.Hash, "hash")
	require.Equal(t, uint64(0), val2.FMeta, "meta")
	require.Equal(t, uint64(0), val2.FDeleted, "deleted")
	require.Equal(t, uint64(0), val2.FMetaDeleted, "meta_deleted")
}

func TestEncodeSlice(t *testing.T) {
	vals := makeTestData(2)
	enc := NewEncoderFor[encodeTestStruct]()
	buf, err := enc.EncodeSlice(vals, nil)
	require.NoError(t, err)
	require.NotNil(t, buf)
	require.NotEmpty(t, buf)
}

func TestEncodeValPtr(t *testing.T) {
	vals := makeTestData(1)
	val := &vals[0]
	enc := NewEncoderFor[encodeTestStruct]()
	buf, err := enc.EncodePtr(val, nil)
	require.NoError(t, err)
	require.NotNil(t, buf)
	require.NotEmpty(t, buf)
}

func TestEncodePtrSlice(t *testing.T) {
	vals := makeTestData(2)
	ptrs := make([]*encodeTestStruct, 2)
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	enc := NewEncoderFor[encodeTestStruct]()
	buf, err := enc.EncodePtrSlice(ptrs, nil)
	require.NoError(t, err)
	require.NotNil(t, buf)
	require.NotEmpty(t, buf)
}
