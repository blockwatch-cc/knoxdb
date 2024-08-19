// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
	"github.com/stretchr/testify/require"
)

const PACK_SIZE = 1 << 16

// const PACK_SIZE = 1

var testStructs = []Encodable{
	// &scalarStruct{},
	// &byteStruct{},
	// &byteUnmarshalStruct{},
	// &smallStruct{},
	// &largeStruct{},
	// &tradeStruct{},
	// &specialStruct{},
	&encodeTestStruct{},
}

func TestSetRow(T *testing.T) {
	for _, v := range testStructs {
		T.Run(fmt.Sprintf("%T", v), func(T *testing.T) {
			pkg := makeTypedPackage(v, 1, 1)
			if err := pkg.SetRow(0, v); err != nil {
				T.Error(err)
			}
		})
	}
}

func BenchmarkSetRow(B *testing.B) {
	for _, v := range testStructs {
		pkg := makeTypedPackage(v, PACK_SIZE, PACK_SIZE)
		B.Run(fmt.Sprintf("%T/%d", v, pkg.Len()), func(B *testing.B) {
			B.ReportAllocs()
			for b := 0; b < B.N; b++ {
				for i := 0; i < PACK_SIZE; i++ {
					pkg.SetRow(i, v)
				}
			}
		})
	}
}

func TestAppend(T *testing.T) {
	for _, v := range testStructs {
		T.Run(fmt.Sprintf("%T", v), func(T *testing.T) {
			pkg := makeTypedPackage(v, 1, 0)
			if err := pkg.AppendStruct(v); err != nil {
				T.Error(err)
			}
		})
	}
}

func BenchmarkAppend(B *testing.B) {
	for _, v := range testStructs {
		pkg := makeTypedPackage(v, PACK_SIZE, 0)
		B.Run(fmt.Sprintf("%T/%d", v, PACK_SIZE), func(B *testing.B) {
			B.ReportAllocs()
			for b := 0; b < B.N; b++ {
				for i := 0; i < PACK_SIZE; i++ {
					pkg.AppendStruct(v)
				}
				pkg.Clear()
			}
		})
	}
}

func BenchmarkAppendWire(B *testing.B) {
	for _, v := range testStructs {
		pkg := makeTypedPackage(v, PACK_SIZE, 0)
		s := makeZeroStruct(v)
		buf := s.(Encodable).Encode()
		B.Run(fmt.Sprintf("%T/%d", v, PACK_SIZE), func(B *testing.B) {
			B.ReportAllocs()
			for b := 0; b < B.N; b++ {
				for i := 0; i < PACK_SIZE; i++ {
					pkg.AppendWire(buf)
				}
				pkg.Clear()
			}
		})
	}
}

func BenchmarkAppendWireE2E(B *testing.B) {
	for _, v := range testStructs {
		pkg := makeTypedPackage(v, PACK_SIZE, 0)
		z := makeZeroStruct(v)
		B.Run(fmt.Sprintf("%T/%d", v, PACK_SIZE), func(B *testing.B) {
			B.ReportAllocs()
			for b := 0; b < B.N; b++ {
				for i := 0; i < PACK_SIZE; i++ {
					pkg.AppendWire(z.(Encodable).Encode())
				}
				pkg.Clear()
			}
		})
	}
}

func TestReadStruct(t *testing.T) {
	for _, v := range testStructs {
		t.Run(fmt.Sprintf("%T", v), func(t *testing.T) {
			pkg := makeTypedPackage(v, PACK_SIZE, PACK_SIZE)
			s, err := schema.SchemaOf(v)
			require.NoError(t, err)
			maps, err := s.MapTo(s)
			require.NoError(t, err)
			for i := 0; i < PACK_SIZE; i++ {
				err := pkg.ReadStruct(i, v, s, maps)
				require.NoError(t, err)
			}
		})
	}
}

func BenchmarkReadStruct(b *testing.B) {
	for _, v := range testStructs {
		pkg := makeTypedPackage(v, PACK_SIZE, PACK_SIZE)
		s, _ := schema.SchemaOf(v)
		maps, _ := s.MapTo(s)
		b.Run(fmt.Sprintf("%T/%d", v, PACK_SIZE), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				for k := 0; k < PACK_SIZE; k++ {
					_ = pkg.ReadStruct(k, v, s, maps)
				}
			}
		})
	}
}

func BenchmarkReadRow(B *testing.B) {
	for _, v := range testStructs {
		pkg := makeTypedPackage(v, PACK_SIZE, PACK_SIZE)
		dst := make([]any, pkg.Cols())
		B.Run(fmt.Sprintf("%T/%d", v, pkg.Len()), func(B *testing.B) {
			B.ReportAllocs()
			for b := 0; b < B.N; b++ {
				for i := 0; i < PACK_SIZE; i++ {
					dst = pkg.ReadRow(i, dst)
				}
			}
		})
	}
}

func BenchmarkReadWire(B *testing.B) {
	for _, v := range testStructs {
		pkg := makeTypedPackage(v, PACK_SIZE, PACK_SIZE)
		buf := bytes.NewBuffer(make([]byte, 0, pkg.schema.WireSize()+128))
		B.Run(fmt.Sprintf("%T/%d", v, pkg.Len()), func(B *testing.B) {
			B.ReportAllocs()
			for b := 0; b < B.N; b++ {
				for i := 0; i < PACK_SIZE; i++ {
					buf.Reset()
					_ = pkg.ReadWireBuffer(buf, i)
				}
			}
		})
	}
}

func BenchmarkReadWireE2E(B *testing.B) {
	for _, v := range testStructs {
		pkg := makeTypedPackage(v, PACK_SIZE, PACK_SIZE)
		buf := bytes.NewBuffer(make([]byte, 0, pkg.schema.WireSize()+128))
		B.Run(fmt.Sprintf("%T/%d", v, PACK_SIZE), func(B *testing.B) {
			B.ReportAllocs()
			for b := 0; b < B.N; b++ {
				for i := 0; i < PACK_SIZE; i++ {
					buf.Reset()
					_ = pkg.ReadWireBuffer(buf, i)
					_ = v.Decode(buf.Bytes())
				}
			}
		})
	}
}

func makeTypedPackage(typ any, sz, fill int) *Package {
	s, err := schema.SchemaOf(typ)
	if err != nil {
		panic(err)
	}
	pkg := New().WithMaxRows(sz).WithSchema(s)
	for i := 0; i < fill; i++ {
		if err := pkg.AppendStruct(makeZeroStruct(typ)); err != nil {
			panic(err)
		}
	}
	return pkg
}

func makeZeroStruct(v any) any {
	typ := reflect.TypeOf(v).Elem()
	ptr := reflect.New(typ)
	val := ptr.Elem()
	for i, l := 0, typ.NumField(); i < l; i++ {
		dst := val.Field(i)
		if dst.Kind() == reflect.Ptr {
			if dst.IsNil() && dst.CanSet() {
				dst.Set(reflect.New(dst.Type().Elem()))
			}
			dst = dst.Elem()
		}
		dst.Set(reflect.Zero(typ.Field(i).Type))

	}
	return ptr.Interface()
}

func makeScanners(val any) (cols []int, vals []any) {
	s, _ := schema.SchemaOf(val)
	v := reflect.Indirect(reflect.ValueOf(val))
	cols = make([]int, s.NumFields())
	vals = make([]any, s.NumFields())
	for i, field := range s.Fields() {
		cols[i] = i
		vals[i] = v.FieldByIndex(field.Path()).Addr().Interface()
	}
	return
}

type Enum uint16

const (
	EnumInvalid Enum = iota // 0
	EnumOne                 // 1 (success)
	EnumTwo
	EnumThree
	EnumFour
)

func (t Enum) IsValid() bool {
	return t != EnumInvalid
}

func (t *Enum) UnmarshalText(data []byte) error {
	v := ParseEnum(string(data))
	if !v.IsValid() {
		return fmt.Errorf("invalid enum '%s'", string(data))
	}
	*t = v
	return nil
}

func (t Enum) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func ParseEnum(s string) Enum {
	switch s {
	case "1", "one":
		return EnumOne
	case "2", "two":
		return EnumTwo
	case "3", "three":
		return EnumThree
	case "4", "four":
		return EnumFour
	default:
		return EnumInvalid
	}
}

func (t Enum) String() string {
	switch t {
	case EnumOne:
		return "one"
	case EnumTwo:
		return "two"
	case EnumThree:
		return "three"
	case EnumFour:
		return "four"
	default:
		return ""
	}
}

type Stringer []string

func (s Stringer) MarshalText() ([]byte, error) {
	return []byte(strings.Join(s, ",")), nil
}

func (s *Stringer) UnmarshalText(b []byte) error {
	*s = strings.Split(string(b), ",")
	return nil
}

type Encodable interface {
	Encode() []byte
	Decode([]byte) error
}

var (
	scalarStructEnc = schema.NewEncoder[scalarStruct]()
	scalarStructDec = schema.NewDecoder[scalarStruct]()
	scalarStructBuf = scalarStructEnc.NewBuffer(1)

	byteStructEnc = schema.NewEncoder[byteStruct]()
	byteStructDec = schema.NewDecoder[byteStruct]()
	byteStructBuf = byteStructEnc.NewBuffer(1)

	byteUnmarshalStructEnc = schema.NewEncoder[byteUnmarshalStruct]()
	byteUnmarshalStructDec = schema.NewDecoder[byteUnmarshalStruct]()
	byteUnmarshalStructBuf = byteUnmarshalStructEnc.NewBuffer(1)

	smallStructEnc = schema.NewEncoder[smallStruct]()
	smallStructDec = schema.NewDecoder[smallStruct]()
	smallStructBuf = smallStructEnc.NewBuffer(1)

	largeStructEnc = schema.NewEncoder[largeStruct]()
	largeStructDec = schema.NewDecoder[largeStruct]()
	largeStructBuf = largeStructEnc.NewBuffer(1)

	tradeStructEnc = schema.NewEncoder[tradeStruct]()
	tradeStructDec = schema.NewDecoder[tradeStruct]()
	tradeStructBuf = tradeStructEnc.NewBuffer(1)

	specialStructEnc = schema.NewEncoder[specialStruct]()
	specialStructDec = schema.NewDecoder[specialStruct]()
	specialStructBuf = specialStructEnc.NewBuffer(1)

	encodeTestStructEnc = schema.NewEncoder[encodeTestStruct]()
	encodeTestStructDec = schema.NewDecoder[encodeTestStruct]()
	encodeTestStructBuf = encodeTestStructEnc.NewBuffer(1)
)

type scalarStruct struct {
	One uint64 `knox:"one,pk"`
}

func (s scalarStruct) Encode() []byte {
	scalarStructBuf.Reset()
	scalarStructEnc.Encode(scalarStructBuf, s)
	return scalarStructBuf.Bytes()
}

func (s *scalarStruct) Decode(buf []byte) error {
	_, err := scalarStructDec.DecodeTo(buf, s)
	return err
}

type byteStruct struct {
	One   uint64 `knox:"one,pk"`
	Seven []byte `knox:"seven"`
}

func (s byteStruct) Encode() []byte {
	byteStructBuf.Reset()
	byteStructEnc.Encode(byteStructBuf, s)
	return byteStructBuf.Bytes()
}

func (s *byteStruct) Decode(buf []byte) error {
	_, err := byteStructDec.DecodeTo(buf, s)
	return err
}

type OpHash [32]byte

type byteUnmarshalStruct struct {
	One   uint64 `knox:"one,pk"`
	Seven OpHash `knox:"seven"`
}

func (s byteUnmarshalStruct) Encode() []byte {
	byteUnmarshalStructBuf.Reset()
	byteUnmarshalStructEnc.Encode(byteUnmarshalStructBuf, s)
	return byteUnmarshalStructBuf.Bytes()
}

func (s *byteUnmarshalStruct) Decode(buf []byte) error {
	_, err := byteUnmarshalStructDec.DecodeTo(buf, s)
	return err
}

type smallStruct struct {
	One   uint64  `knox:"one,pk"`
	Two   int64   `knox:"two"`
	Three float64 `knox:"three"`
	Four  uint8   `knox:"four"`
	Five  uint32  `knox:"five"`
	Six   int16   `knox:"six"`
}

func (s smallStruct) Encode() []byte {
	smallStructBuf.Reset()
	smallStructEnc.Encode(smallStructBuf, s)
	return smallStructBuf.Bytes()
}

func (s *smallStruct) Decode(buf []byte) error {
	_, err := smallStructDec.DecodeTo(buf, s)
	return err
}

type largeStruct struct {
	One            uint64    `knox:"one,pk"`
	Time           time.Time `knox:"time"`
	ID             int64     `knox:"id"`
	Index          int       `knox:"index"`
	UUID           uint64    `knox:"uuid"`
	IsActive       bool      `knox:"isActive"`
	Balance        int64     `knox:"balance"`
	Picture        int64     `knox:"picture"`
	Age            int       `knox:"age"`
	EyeColor       int8      `knox:"eyeColor"`
	Name           int64     `knox:"name"`
	Gender         int8      `knox:"gender"`
	Company        int16     `knox:"company"`
	Email          int64     `knox:"email"`
	Phone          uint64    `knox:"phone"`
	Address        int64     `knox:"address"`
	About          int64     `knox:"about"`
	Registered     int64     `knox:"registered"`
	Latitude       float64   `knox:"latitude"`
	Longitude      float64   `knox:"longitude"`
	Greeting       int64     `knox:"greeting"`
	FavoriteFruit  int64     `knox:"favoriteFruit"`
	AID            int64     `knox:"aid"`
	AIndex         int       `knox:"aindex"`
	AUUID          int64     `knox:"auuid"`
	AIsActive      bool      `knox:"aisActive"`
	ABalance       int64     `knox:"abalance"`
	APicture       int64     `knox:"apicture"`
	AAge           int       `knox:"aage"`
	AEyeColor      int64     `knox:"aeyeColor"`
	AName          int64     `knox:"aname"`
	AGender        int64     `knox:"agender"`
	ACompany       int64     `knox:"acompany"`
	AEmail         int64     `knox:"aemail"`
	APhone         int64     `knox:"aphone"`
	AAddress       int64     `knox:"aaddress"`
	AAbout         int64     `knox:"aabout"`
	ARegistered    int64     `knox:"aregistered"`
	ALatitude      float64   `knox:"alatitude"`
	ALongitude     float64   `knox:"alongitude"`
	AGreeting      int64     `knox:"agreeting"`
	AFavoriteFruit int64     `knox:"afavoriteFruit"`
}

func (s largeStruct) Encode() []byte {
	largeStructBuf.Reset()
	largeStructEnc.Encode(largeStructBuf, s)
	return largeStructBuf.Bytes()
}

func (s *largeStruct) Decode(buf []byte) error {
	_, err := largeStructDec.DecodeTo(buf, s)
	return err
}

type (
	TradeID   uint64
	PoolID    uint64
	TokenID   uint64
	AccountID uint64
	Entity    uint16
	Direction byte
)

type tradeStruct struct {
	Id          TradeID       `knox:"id,pk"`
	Pool        PoolID        `knox:"pool,index=bloom:3"`
	Entity      Entity        `knox:"entity,index=bloom:3"`
	Counter     int           `knox:"counter"`
	Side        Direction     `knox:"side"`
	VolumeA     num.Big       `knox:"volume_a,zip=snappy"`
	VolumeB     num.Big       `knox:"volume_b,zip=snappy"`
	FeeCy       TokenID       `knox:"fee_cy"`
	BurnCy      TokenID       `knox:"burn_cy"`
	PriceCy     TokenID       `knox:"price_cy"`
	LpFee       num.Big       `knox:"lp_fee,zip=snappy"`
	DevFee      num.Big       `knox:"dev_fee,zip=snappy"`
	RefFee      num.Big       `knox:"ref_fee,zip=snappy"`
	IncFee      num.Big       `knox:"inc_fee,zip=snappy"`
	Burn        num.Big       `knox:"burn,zip=snappy"`
	LpFeeBps    num.Decimal32 `knox:"lp_fee_bps,scale=2"`
	DevFeeBps   num.Decimal32 `knox:"dev_fee_bps,scale=2"`
	RefFeeBps   num.Decimal32 `knox:"ref_fee_bps,scale=2"`
	IncFeeBps   num.Decimal32 `knox:"inc_fee_bps,scale=2"`
	BurnBps     num.Decimal32 `knox:"burn_bps,scale=2"`
	PriceNet    num.Big       `knox:"price_net,zip=snappy"`
	PriceGross  num.Big       `knox:"price_gross,zip=snappy"`
	PriceBefore num.Big       `knox:"price_before,zip=snappy"`
	PriceAfter  num.Big       `knox:"price_after,zip=snappy"`
	Delta       num.Decimal32 `knox:"price_delta_bps,scale=2"`
	Impact      num.Decimal32 `knox:"price_impact_bps,scale=2"`
	PriceUSD    num.Big       `knox:"price_usd,zip=snappy"`
	FeesUSD     num.Big       `knox:"fees_usd,zip=snappy"`
	VolumeUSD   num.Big       `knox:"volume_usd,zip=snappy"`
	Signer      AccountID     `knox:"signer,index=bloom:3"`
	Sender      AccountID     `knox:"sender,index=bloom:3"`
	Receiver    AccountID     `knox:"receiver,index=bloom:3"`
	Router      AccountID     `knox:"router"`
	IsWash      bool          `knox:"is_wash_trade"`
	TxHash      OpHash        `knox:"tx_hash,index=bloom:3"`
	TxFee       int64         `knox:"tx_fee"`
	Block       int64         `knox:"block"`
	Time        time.Time     `knox:"time"`
}

func (s tradeStruct) Encode() []byte {
	tradeStructBuf.Reset()
	tradeStructEnc.Encode(tradeStructBuf, s)
	return tradeStructBuf.Bytes()
}

func (s *tradeStruct) Decode(buf []byte) error {
	_, err := tradeStructDec.DecodeTo(buf, s)
	return err
}

type specialStruct struct {
	Pk       uint64         `knox:"key,pk"`
	Enum     Enum           `knox:"enum"`
	Stringer Stringer       `knox:"strlist"`
	D32      num.Decimal32  `knox:"d32,scale=5"`
	D64      num.Decimal64  `knox:"d64,scale=15"`
	D128     num.Decimal128 `knox:"d128,scale=18"`
	D256     num.Decimal256 `knox:"d256,scale=24"`
	I128     num.Int128     `knox:"i128"`
	I256     num.Int256     `knox:"i256"`
	Z        num.Big        `knox:"Z"`
}

func (s specialStruct) Encode() []byte {
	specialStructBuf.Reset()
	specialStructEnc.Encode(specialStructBuf, s)
	return specialStructBuf.Bytes()
}

func (s *specialStruct) Decode(buf []byte) error {
	_, err := specialStructDec.DecodeTo(buf, s)
	return err
}

type encodeTestStruct struct {
	Id       uint64         `knox:"id,pk"`
	Time     time.Time      `knox:"time"`
	Hash     OpHash         `knox:"hash,fixed=32,index=bloom:3"`
	String   string         `knox:"str"`
	Stringer Stringer       `knox:"strlist"`
	Bool     bool           `knox:"bool"`
	Enum     Enum           `knox:"enum"`
	Int64    int64          `knox:"i64"`
	Int32    int32          `knox:"i32"`
	Int16    int16          `knox:"i16"`
	Int8     int8           `knox:"i8"`
	Uint64   uint64         `knox:"u64,index=bloom"`
	Uint32   uint32         `knox:"u32"`
	Uint16   uint16         `knox:"u16"`
	Uint8    uint8          `knox:"u8"`
	Float64  float64        `knox:"f64"`
	Float32  float32        `knox:"f32"`
	D32      num.Decimal32  `knox:"d32,scale=5"`
	D64      num.Decimal64  `knox:"d64,scale=15"`
	D128     num.Decimal128 `knox:"d128,scale=18"`
	D256     num.Decimal256 `knox:"d256,scale=24"`
	I128     num.Int128     `knox:"i128"`
	I256     num.Int256     `knox:"i256"`
}

func (s encodeTestStruct) Encode() []byte {
	encodeTestStructBuf.Reset()
	encodeTestStructEnc.Encode(encodeTestStructBuf, s)
	return encodeTestStructBuf.Bytes()
}

func (s *encodeTestStruct) Decode(buf []byte) error {
	_, err := encodeTestStructDec.DecodeTo(buf, s)
	return err
}
