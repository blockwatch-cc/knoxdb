// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tests

import (
	"encoding/hex"
	"time"

	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/util"
)

type MyEnum string

// call this from TestMain() in any package that uses AllTypes
func RegisterEnum() {
	myEnum := schema.NewEnumDictionary("my_enum")
	myEnum.Append("a", "b", "c", "d", "e")
	schema.RegisterEnum(0, myEnum)
}

type AllTypes struct {
	Id      uint64         `knox:"id,pk"`
	Int64   int64          `knox:"i64"`
	Int32   int32          `knox:"i32"`
	Int16   int16          `knox:"i16"`
	Int8    int8           `knox:"i8"`
	Uint64  uint64         `knox:"u64"`
	Uint32  uint32         `knox:"u32"`
	Uint16  uint16         `knox:"u16"`
	Uint8   uint8          `knox:"u8"`
	Float64 float64        `knox:"f64"`
	Float32 float32        `knox:"f32"`
	D32     num.Decimal32  `knox:"d32,scale=5"`
	D64     num.Decimal64  `knox:"d64,scale=15"`
	D128    num.Decimal128 `knox:"d128,scale=18"`
	D256    num.Decimal256 `knox:"d256,scale=24"`
	I128    num.Int128     `knox:"i128"`
	I256    num.Int256     `knox:"i256"`
	Bool    bool           `knox:"bool"`
	Time    time.Time      `knox:"time"`
	Hash    []byte         `knox:"bytes"`
	Array   [2]byte        `knox:"array[2]"`
	String  string         `knox:"string"`
	MyEnum  MyEnum         `knox:"my_enum,enum"` // must register with schema
}

func NewAllTypes(i int64) AllTypes {
	return AllTypes{
		Id:      uint64(i),
		Int64:   int64(i),
		Int32:   int32(i),
		Int16:   int16(i),
		Int8:    int8(i),
		Uint64:  uint64(i),
		Uint32:  uint32(i),
		Uint16:  uint16(i),
		Uint8:   uint8(i),
		Float64: float64(i),
		Float32: float32(i),
		D32:     num.NewDecimal32(int32(i), 5),
		D64:     num.NewDecimal64(int64(i), 15),
		D128:    num.NewDecimal128(num.Int128FromInt64(int64(i)), 18),
		D256:    num.NewDecimal256(num.Int256FromInt64(int64(i)), 24),
		I128:    num.Int128FromInt64(int64(i)),
		I256:    num.Int256FromInt64(int64(i)),
		Bool:    i%2 == 1,
		Time:    time.Unix(0, int64(i)).UTC(),
		Hash:    util.Uint64Bytes(uint64(i)),
		Array:   [2]byte{byte(i >> 8 & 0xf), byte(i & 0xf)},
		String:  hex.EncodeToString(util.Uint64Bytes(uint64(i))),
		MyEnum:  MyEnum("a"),
	}
}

type Security struct {
	Id             uint64         `knox:"id,pk"`
	Ticker         []byte         `knox:"name"`
	LastClosePrice num.Decimal256 `knox:"last_close_price"`
	CreatedAt      time.Time      `knox:"created_at"`
	UpdatedAt      time.Time      `knox:"updated_at"`
}

func NewSecurity(i int64) Security {
	return Security{
		Id:             uint64(i),
		Ticker:         util.RandBytes(5),
		LastClosePrice: num.NewDecimal256(num.Int256FromInt64(int64(i)), 24),
		CreatedAt:      time.Unix(0, int64(i)).UTC(),
		UpdatedAt:      time.Unix(0, int64(i)).UTC(),
	}
}
