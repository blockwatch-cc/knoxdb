// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//

package pack

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/encoding/decimal"
	"blockwatch.cc/knoxdb/vec"
)

type fieldTestEnum uint

const (
	fieldTestEnumInvalid fieldTestEnum = iota // 0
	fieldTestEnumOne                          // 1 (success)
	fieldTestEnumTwo
	fieldTestEnumThree
	fieldTestEnumFour
)

type fieldTestStringer []string

func (s fieldTestStringer) MarshalText() ([]byte, error) {
	return []byte(strings.Join(s, ",")), nil
}

func (s *fieldTestStringer) UnmarshalText(b []byte) error {
	*s = strings.Split(string(b), ",")
	return nil
}

func randKey(length int) []byte {
	k := make([]byte, length)
	if _, err := io.ReadFull(rand.Reader, k); err != nil {
		return nil
	}
	return k
}

type fieldTestType struct {
	Id        uint64             `knox:"I,pk"                     json:"id"`
	Timestamp time.Time          `knox:"T,snappy"                 json:"time"`
	Hash      []byte             `knox:"H,bloom,snappy"           json:"hash"`
	String    string             `knox:"str,snappy"               json:"string"`
	Stringer  fieldTestStringer  `knox:"strlist,snappy"           json:"string_list"`
	Bool      bool               `knox:"bool,snappy"              json:"bool"`
	Enum      fieldTestEnum      `knox:"enum,u8,snappy"           json:"enum"`
	Int64     int64              `knox:"i64,snappy"               json:"int64"`
	Int32     int32              `knox:"i32,snappy"               json:"int32"`
	Int16     int16              `knox:"i16,snappy"               json:"int16"`
	Int8      int8               `knox:"i8,snappy"                json:"int8"`
	Int_8     int                `knox:"i_8,i8,snappy"            json:"int_as_int8"`
	Int_16    int                `knox:"i_16,i16,snappy"          json:"int_as_int16"`
	Int_32    int                `knox:"i_32,i32,snappy"          json:"int_as_int32"`
	Int_64    int                `knox:"i_64,i64,snappy"          json:"int_as_int64"`
	Uint64    uint64             `knox:"u64,snappy,bloom"         json:"uint64"`
	Uint32    uint32             `knox:"u32,snappy"               json:"uint32"`
	Uint16    uint16             `knox:"u16,snappy"               json:"uint16"`
	Uint8     uint8              `knox:"u8,snappy"                json:"uint8"`
	Uint_8    uint               `knox:"u_8,u8,snappy"            json:"uint_as_uint8"`
	Uint_16   uint               `knox:"u_16,u16,snappy"          json:"uint_as_uint16"`
	Uint_32   uint               `knox:"u_32,u32,snappy"          json:"uint_as_uint32"`
	Uint_64   uint               `knox:"u_64,u64,snappy"          json:"uint_as_uint64"`
	Float64   float64            `knox:"f64,snappy"               json:"float64"`
	Float32   float32            `knox:"f32,snappy"               json:"float32"`
	D32       decimal.Decimal32  `knox:"d32,scale=5,snappy"       json:"decimal32"`
	D64       decimal.Decimal64  `knox:"d64,scale=15,snappy"      json:"decimal64"`
	D128      decimal.Decimal128 `knox:"d128,scale=18,snappy"     json:"decimal128"`
	D256      decimal.Decimal256 `knox:"d256,scale=24,snappy"     json:"decimal256"`
	I128      vec.Int128         `knox:"i128,snappy"              json:"int128"`
	I256      vec.Int256         `knox:"i256,snappy"              json:"int256"`
}

func randFieldType(i int) *fieldTestType {
	return &fieldTestType{
		Id:        0, // empty, will be set by insert
		Timestamp: time.Now().UTC(),
		Hash:      randKey(20),
		String:    hex.EncodeToString(randKey(4)),
		Stringer:  strings.SplitAfter(hex.EncodeToString(randKey(4)), "a"),
		Bool:      true,
		Enum:      fieldTestEnum(i%4 + 1),
		// typed ints
		Int64: int64(i),
		Int32: int32(i),
		Int16: int16(i % (1<<16 - 1)),
		Int8:  int8(i % (1<<8 - 1)),
		// int to typed int
		Int_8:  i,
		Int_16: i,
		Int_32: i,
		Int_64: i,
		// typed uints
		Uint64: uint64(i * 1000000),
		Uint32: uint32(i * 1000000),
		Uint16: uint16(i),
		Uint8:  uint8(i),
		// uint to typed uint
		Uint_8:  uint(i),
		Uint_16: uint(i),
		Uint_32: uint(i),
		Uint_64: uint(i),
		Float32: float32(i / 1000000),
		Float64: float64(i / 1000000),
		// decimals
		D32:  decimal.NewDecimal32(int32(100123456789-i), 5),
		D64:  decimal.NewDecimal64(1123456789123456789-int64(i), 15),
		D128: decimal.NewDecimal128(vec.MustParseInt128(strconv.Itoa(i)+"00000000000000000000"), 18),
		D256: decimal.NewDecimal256(vec.MustParseInt256(strconv.Itoa(i)+"0000000000000000000000000000000000000000"), 24),
		I128: vec.MustParseInt128(strconv.Itoa(i) + "000000000000000000000000000000"),
		I256: vec.MustParseInt256(strconv.Itoa(i) + "000000000000000000000000000000000000000000000000000000000000"),
	}
}

func TestFieldEncodeRoundtrip(t *testing.T) {
	fields, err := Fields(fieldTestType{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 127; i++ {
		t.Run(fmt.Sprintf("run_%d", i), func(t *testing.T) {
			val := randFieldType(i)
			buf, err := fields.Encode(val)
			if err != nil {
				t.Fatal(err)
			}
			var val2 fieldTestType
			err = fields.Decode(buf, &val2)
			if err != nil {
				t.Fatal(err)
			}
			b1, _ := json.Marshal(val)
			b2, _ := json.Marshal(val2)
			if !bytes.Equal(b1, b2) {
				t.Fatalf("!equal:\nv1=%s\nv2=%s", string(b1), string(b2))
			}
		})
	}
}
