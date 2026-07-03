// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine_tests

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"time"

	"blockwatch.cc/knoxdb/internal/operator/filter"
	"blockwatch.cc/knoxdb/internal/tests/testutil"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/schema/cast"
	"blockwatch.cc/knoxdb/pkg/schema/enum"
	"blockwatch.cc/knoxdb/pkg/schema/reflect"
)

var (
	EQ = types.FilterModeEqual
	IN = types.FilterModeIn
	NI = types.FilterModeNotIn
	LE = types.FilterModeLe
	LT = types.FilterModeLt
	GE = types.FilterModeGe
	GT = types.FilterModeGt
	RG = types.FilterModeRange
)

var myEnums = []string{"one", "two", "three", "four"}

var (
	enums          *enum.Registry
	allTypesSchema *schema.Schema
	transferSchema *schema.Schema
)

// call this from TestMain() in any package that uses AllTypes
func RegisterEnum() {
	if enums != nil {
		return
	}

	// create dictionary
	myEnum := enum.NewDictionary("my_enum")
	myEnum.Append(myEnums...)

	// create test registry and add enum to registry
	enums = enum.NewRegistry()
	enums.Register(0, myEnum)

	// init schema and link enums (will lookup myEnum and link to field)
	allTypesSchema = reflect.MustSchemaFor[AllTypes](schema.Enums(enums))
	transferSchema = reflect.MustSchemaFor[Transfer]()
}

// Types defines the schema for Workload1 and Workload2.
type Types struct {
	Id        uint64    `knox:"id,pk"`
	Timestamp time.Time `knox:"time"`
	String    string    `knox:"string"`
	Int64     int64     `knox:"int64"`
	MyEnum    string    `knox:"my_enum,enum"`
}

// NewRandomData generates random data for UnifiedRow and Types.
func NewRandomData() string {
	bytes := testutil.RandBytes(8) // Generates 8 random bytes
	return hex.EncodeToString(bytes)
}

// NewRandomTypes generates random instances of Types for workloads.
func NewRandomTypes(i int) *Types {
	return &Types{
		Id:        0, // Primary key will be assigned post-insertion
		Timestamp: time.Now().UTC(),
		String:    hex.EncodeToString(testutil.RandBytes(4)),
		Int64:     int64(i),
		MyEnum:    myEnums[i%len(myEnums)],
	}
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
	MyEnum  string         `knox:"my_enum,enum"` // must register with schema
}

func NewAllTypes(i int) *AllTypes {
	return &AllTypes{
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
		Hash:    binary.BigEndian.AppendUint64(nil, uint64(i)),
		Array:   [2]byte{byte(i >> 8 & 0xf), byte(i & 0xf)},
		String:  fmt.Sprintf("%016x", i),
		MyEnum:  myEnums[i%len(myEnums)],
	}
}

type Transfer struct {
	ID              uint64     `knox:"id,pk"`
	DebitAccountID  uint64     `knox:"debit_id"`
	CreditAccountID uint64     `knox:"credit_id"`
	Amount          num.Int128 `knox:"amount"`
	PendingID       uint64     `knox:"pending_id"`
	UserData256     [32]byte   `knox:"user_data_256"`
	UserData64      uint64     `knox:"user_data_64"`
	UserData32      uint32     `knox:"user_data_32"`
	Timeout         uint32     `knox:"timeout"`
	Ledger          uint32     `knox:"ledger"`
	Code            uint16     `knox:"code"`
	Flags           uint16     `knox:"flags"`
	Timestamp       uint64     `knox:"timestamp,timebase"`
}

func NewTransfer(i int) *Transfer {
	return &Transfer{
		ID:              uint64(i),
		DebitAccountID:  uint64(i),
		CreditAccountID: uint64(i),
		Amount:          num.Int128FromInt64(int64(i)),
		PendingID:       uint64(i),
		UserData256:     [32]byte{byte(i)},
		UserData64:      uint64(i),
		UserData32:      uint32(i),
		Timeout:         uint32(i),
		Ledger:          uint32(i),
		Code:            uint16(i),
		Flags:           uint16(i),
		Timestamp:       uint64(i),
	}
}

func makeFilter(s *schema.Schema, name string, mode types.FilterMode, val, val2 any) *filter.Node {
	field, ok := s.Find(name)
	if !ok {
		panic(fmt.Errorf("missing field %s in schema %s", name, s))
	}
	idx, _ := s.IndexId(field.Id)
	m := filter.NewFactory(field.Type).New(mode)
	c := cast.NewCaster(field.Type, field.Scale, nil)
	switch mode {
	case types.FilterModeRange:
		val, _ = c.CastValue(val)
		val2, _ = c.CastValue(val2)
		rg := filter.RangeValue{val, val2}
		val = rg
	case types.FilterModeIn, types.FilterModeNotIn:
		val, _ = c.CastSlice(val)
	default:
		val, _ = c.CastValue(val)
	}
	m.WithValue(val)
	return filter.NewNode().SetFilter(&filter.Filter{
		Name:    field.Name,
		Type:    filter.ToValueType(field.Type),
		Mode:    mode,
		Index:   idx,
		Id:      field.Id,
		Value:   val,
		Matcher: m,
	})
}

func makeTree(f ...*filter.Node) *filter.Node {
	n := filter.NewNode()
	n.Children = f
	return n
}
