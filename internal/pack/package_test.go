// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"bytes"
	"reflect"
	"time"

	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
)

const PACK_SIZE = 1 << 16

func init() {
	// register enum type with global schema registry (before first schema is created)
	myEnum := schema.NewEnumDictionary("my_enum")
	myEnum.Append([]string{"one", "two", "three", "four"}...)
	schema.RegisterEnum(0, myEnum)
}

var (
	testStructs = []Encodable{
		&scalarStruct{},
		&byteStruct{},
		&byteUnmarshalStruct{},
		&smallStruct{},
		&largeStruct{},
		&tradeStruct{},
		&specialStruct{},
		&encodeTestStruct{},
	}

	scalarStructEnc = schema.NewGenericEncoder[scalarStruct]()
	scalarStructDec = schema.NewGenericDecoder[scalarStruct]()
	scalarStructBuf = scalarStructEnc.NewBuffer(1)

	byteStructEnc = schema.NewGenericEncoder[byteStruct]()
	byteStructDec = schema.NewGenericDecoder[byteStruct]()
	byteStructBuf = byteStructEnc.NewBuffer(1)

	byteUnmarshalStructEnc = schema.NewGenericEncoder[byteUnmarshalStruct]()
	byteUnmarshalStructDec = schema.NewGenericDecoder[byteUnmarshalStruct]()
	byteUnmarshalStructBuf = byteUnmarshalStructEnc.NewBuffer(1)

	smallStructEnc = schema.NewGenericEncoder[smallStruct]()
	smallStructDec = schema.NewGenericDecoder[smallStruct]()
	smallStructBuf = smallStructEnc.NewBuffer(1)

	largeStructEnc = schema.NewGenericEncoder[largeStruct]()
	largeStructDec = schema.NewGenericDecoder[largeStruct]()
	largeStructBuf = largeStructEnc.NewBuffer(1)

	tradeStructEnc = schema.NewGenericEncoder[tradeStruct]()
	tradeStructDec = schema.NewGenericDecoder[tradeStruct]()
	tradeStructBuf = tradeStructEnc.NewBuffer(1)

	// lazy init to make sure enum is registered
	specialStructEnc *schema.GenericEncoder[specialStruct]
	specialStructDec *schema.GenericDecoder[specialStruct]
	specialStructBuf *bytes.Buffer

	encodeTestStructEnc *schema.GenericEncoder[encodeTestStruct]
	encodeTestStructDec *schema.GenericDecoder[encodeTestStruct]
	encodeTestStructBuf *bytes.Buffer
)

func makeTypedPackage(typ any, fill int) *Package {
	s, err := schema.SchemaOf(typ)
	if err != nil {
		panic(err)
	}
	s.WithEnums(&schema.GlobalRegistry)
	pkg := New().WithMaxRows(PACK_SIZE).WithSchema(s)
	enc := schema.NewEncoder(s)
	buf, err := enc.Encode(makeZeroStruct(typ), nil)
	if err != nil {
		panic(err)
	}
	for i := 0; i < fill; i++ {
		pkg.AppendWire(buf, nil)
	}
	return pkg
}

// func makeZeroSlice(v any, n int) reflect.Value {
// 	rtyp := reflect.TypeOf(v).Elem()
// 	rslice := reflect.MakeSlice(reflect.SliceOf(rtyp), 0, n)
// 	for i := 0; i < n; i++ {
// 		rslice = reflect.Append(rslice, reflect.Zero(rtyp))
// 	}
// 	return rslice
// }

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
		// fake enum
		if dst.Kind() == reflect.String {
			dst.SetString("one")
		}
	}
	return ptr.Interface()
}

type Enum uint16

type Encodable interface {
	Encode() []byte
	Decode([]byte) error
}

type scalarStruct struct {
	Id uint64 `knox:"id,pk"`
}

func (s *scalarStruct) Encode() []byte {
	scalarStructBuf.Reset()
	scalarStructEnc.EncodePtr(s, scalarStructBuf)
	return scalarStructBuf.Bytes()
}

func (s *scalarStruct) Decode(buf []byte) error {
	_, err := scalarStructDec.Decode(buf, s)
	return err
}

type byteStruct struct {
	Id    uint64 `knox:"id,pk"`
	Seven []byte `knox:"seven"`
}

func (s *byteStruct) Encode() []byte {
	byteStructBuf.Reset()
	byteStructEnc.EncodePtr(s, byteStructBuf)
	return byteStructBuf.Bytes()
}

func (s *byteStruct) Decode(buf []byte) error {
	_, err := byteStructDec.Decode(buf, s)
	return err
}

type OpHash [32]byte

type byteUnmarshalStruct struct {
	Id    uint64 `knox:"id,pk"`
	Seven OpHash `knox:"seven"`
}

func (s byteUnmarshalStruct) Encode() []byte {
	byteUnmarshalStructBuf.Reset()
	byteUnmarshalStructEnc.Encode(s, byteUnmarshalStructBuf)
	return byteUnmarshalStructBuf.Bytes()
}

func (s *byteUnmarshalStruct) Decode(buf []byte) error {
	_, err := byteUnmarshalStructDec.Decode(buf, s)
	return err
}

type smallStruct struct {
	Id    uint64  `knox:"id,pk"`
	Two   int64   `knox:"two"`
	Three float64 `knox:"three"`
	Four  uint8   `knox:"four"`
	Five  uint32  `knox:"five"`
	Six   int16   `knox:"six"`
}

func (s *smallStruct) Encode() []byte {
	smallStructBuf.Reset()
	smallStructEnc.EncodePtr(s, smallStructBuf)
	return smallStructBuf.Bytes()
}

func (s *smallStruct) Decode(buf []byte) error {
	_, err := smallStructDec.Decode(buf, s)
	return err
}

type largeStruct struct {
	Id             uint64    `knox:"id,pk"`
	Time           time.Time `knox:"time"`
	One            int64     `knox:"one"`
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

func (s *largeStruct) Encode() []byte {
	largeStructBuf.Reset()
	largeStructEnc.EncodePtr(s, largeStructBuf)
	return largeStructBuf.Bytes()
}

func (s *largeStruct) Decode(buf []byte) error {
	_, err := largeStructDec.Decode(buf, s)
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
	Pool        PoolID        `knox:"pool,filter=bloom3b"`
	Entity      Entity        `knox:"entity,filter=bloom3b"`
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
	Signer      AccountID     `knox:"signer,filter=bloom3b"`
	Sender      AccountID     `knox:"sender,filter=bloom3b"`
	Receiver    AccountID     `knox:"receiver,filter=bloom3b"`
	Router      AccountID     `knox:"router"`
	IsWash      bool          `knox:"is_wash_trade"`
	TxHash      OpHash        `knox:"tx_hash,filter=bloom3b"`
	TxFee       int64         `knox:"tx_fee"`
	Block       int64         `knox:"block"`
	Time        time.Time     `knox:"time"`
}

func (s *tradeStruct) Encode() []byte {
	tradeStructBuf.Reset()
	tradeStructEnc.EncodePtr(s, tradeStructBuf)
	return tradeStructBuf.Bytes()
}

func (s *tradeStruct) Decode(buf []byte) error {
	_, err := tradeStructDec.Decode(buf, s)
	return err
}

type specialStruct struct {
	Id   uint64         `knox:"id,pk"`
	Enum Enum           `knox:"enum"`
	D32  num.Decimal32  `knox:"d32,scale=5"`
	D64  num.Decimal64  `knox:"d64,scale=15"`
	D128 num.Decimal128 `knox:"d128,scale=18"`
	D256 num.Decimal256 `knox:"d256,scale=24"`
	I128 num.Int128     `knox:"i128"`
	I256 num.Int256     `knox:"i256"`
	Z    num.Big        `knox:"Z"`
}

func (s *specialStruct) Encode() []byte {
	s.init()
	specialStructBuf.Reset()
	specialStructEnc.EncodePtr(s, specialStructBuf)
	return specialStructBuf.Bytes()
}

func (s *specialStruct) Decode(buf []byte) error {
	s.init()
	_, err := specialStructDec.Decode(buf, s)
	return err
}

func (s specialStruct) init() {
	// lazy init on first use to ensure enum is in global registry
	if specialStructBuf == nil {
		specialStructEnc = schema.NewGenericEncoder[specialStruct]()
		specialStructDec = schema.NewGenericDecoder[specialStruct]()
		specialStructBuf = specialStructEnc.NewBuffer(1)
	}
}

type encodeTestStruct struct {
	Id      uint64         `knox:"id,pk"`
	Time    time.Time      `knox:"time"`
	Hash    OpHash         `knox:"hash,filter=bloom3b"`
	String  string         `knox:"str"`
	Bool    bool           `knox:"bool"`
	Enum    string         `knox:"my_enum,enum"`
	Int64   int64          `knox:"i64"`
	Int32   int32          `knox:"i32"`
	Int16   int16          `knox:"i16"`
	Int8    int8           `knox:"i8"`
	Uint64  uint64         `knox:"u64,filter=bloom2b"`
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
}

func (s *encodeTestStruct) Encode() []byte {
	s.init()
	encodeTestStructBuf.Reset()
	encodeTestStructEnc.EncodePtr(s, encodeTestStructBuf)
	return encodeTestStructBuf.Bytes()
}

func (s *encodeTestStruct) Decode(buf []byte) error {
	s.init()
	_, err := encodeTestStructDec.Decode(buf, s)
	return err
}

func (s encodeTestStruct) init() {
	// lazy init on first use to ensure enum is in global registry
	if encodeTestStructBuf == nil {
		encodeTestStructEnc = schema.NewGenericEncoder[encodeTestStruct]()
		encodeTestStructDec = schema.NewGenericDecoder[encodeTestStruct]()
		encodeTestStructBuf = encodeTestStructEnc.NewBuffer(1)
	}
}

type encodeTestSubStruct struct {
	Id    uint64    `knox:"id,pk"`
	Int64 int64     `knox:"i64"`
	Hash  OpHash    `knox:"hash,filter=bloom3b"`
	Time  time.Time `knox:"time"`
}
