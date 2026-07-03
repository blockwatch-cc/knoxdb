package schema_tests

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"time"

	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/schema/encode"
	"blockwatch.cc/knoxdb/pkg/schema/enum"
)

func RegisterEnums() *enum.Registry {
	// prepare enum
	myEnum = enum.NewDictionary("my_enum")
	myEnum.Append("a", "b", "c", "d", "e")

	// create test registry and add enum to registry
	enums = enum.NewRegistry()
	enums.Register(0, myEnum)

	return enums
}

func init() {
	RegisterEnums()
}

// Register a global enum and dictionary for all schema tests
type MyEnum string

var (
	enums  *enum.Registry
	myEnum *enum.Dictionary
)

type AllTypes struct {
	Id       uint64            `knox:"id,pk"`         // 0
	Int64    int64             `knox:"i64"`           // 1
	Int32    int32             `knox:"i32"`           // 2
	Int16    int16             `knox:"i16"`           // 3
	Int8     int8              `knox:"i8"`            // 4
	Uint64   uint64            `knox:"u64"`           // 5
	Uint32   uint32            `knox:"u32"`           // 6
	Uint16   uint16            `knox:"u16"`           // 7
	Uint8    uint8             `knox:"u8"`            // 8
	Float64  float64           `knox:"f64"`           // 9
	Float32  float32           `knox:"f32"`           // 10
	D32      num.Decimal32     `knox:"d32,scale=5"`   // 11
	D64      num.Decimal64     `knox:"d64,scale=15"`  // 12
	D128     num.Decimal128    `knox:"d128,scale=18"` // 13
	D256     num.Decimal256    `knox:"d256,scale=24"` // 14
	I128     num.Int128        `knox:"i128"`          // 15
	I256     num.Int256        `knox:"i256"`          // 16
	Bool     bool              `knox:"bool"`          // 17
	Time     time.Time         `knox:"time"`          // 18
	Hash     []byte            `knox:"bytes"`         // 19
	Array    [2]byte           `knox:"array[2]"`      // 20
	String   string            `knox:"string"`        // 21
	MyEnum   MyEnum            `knox:"my_enum,enum"`  // 22
	Big      num.Big           `knox:"big"`           // 23
	Duration time.Duration     `knox:"duration"`      // 24
	Union    schema.UnionValue `knox:"union"`         // 25
}

func NewAllTypes(i int64) *AllTypes {
	return &AllTypes{
		Id:       uint64(i),
		Int64:    i,
		Int32:    int32(i),
		Int16:    int16(i),
		Int8:     int8(i),
		Uint64:   uint64(i),
		Uint32:   uint32(i),
		Uint16:   uint16(i),
		Uint8:    uint8(i),
		Float64:  float64(i),
		Float32:  float32(i),
		D32:      num.NewDecimal32(int32(i), 5),
		D64:      num.NewDecimal64(i, 15),
		D128:     num.NewDecimal128(num.Int128FromInt64(i), 18),
		D256:     num.NewDecimal256(num.Int256FromInt64(i), 24),
		I128:     num.Int128FromInt64(i),
		I256:     num.Int256FromInt64(i),
		Bool:     i%2 == 1,
		Time:     time.Unix(0, i).UTC(),
		Hash:     binary.BigEndian.AppendUint64(nil, uint64(i)),
		Array:    [2]byte{byte(i >> 8 & 0xf), byte(i & 0xf)},
		String:   fmt.Sprintf("%016x", i),
		MyEnum:   MyEnum("a"),
		Big:      num.NewBig(i),
		Duration: time.Minute * time.Duration(i),
		Union:    schema.Int32Union(int32(i)),
	}
}

// func (s *AllTypes) Encode() []byte {
// 	allTypesBuf.Reset()
// 	allTypesEnc.Encode(allTypesBuf, s)
// 	return allTypesBuf.Bytes()
// }

// func (s *AllTypes) Decode(buf []byte) error {
// 	_, err := allTypesDec.Decode(buf, s)
// 	return err
// }

type TimeTypes struct {
	TimestampNs time.Time     `knox:"tsn,timestamp,scale=ns"`
	TimestampUs time.Time     `knox:"tsu,timestamp,scale=us"`
	TimestampMs time.Time     `knox:"tsm,timestamp,scale=ms"`
	TimestampS  time.Time     `knox:"tss,timestamp,scale=s"`
	TimeNs      time.Time     `knox:"tmn,time,scale=ns"`
	TimeUs      time.Time     `knox:"tmu,time,scale=us"`
	TimeMs      time.Time     `knox:"tmm,time,scale=ms"`
	TimeS       time.Time     `knox:"tms,time,scale=s"`
	Date        time.Time     `knox:"dt,date"`
	DurationN   time.Duration `knox:"dns,scale=ns"`
	DurationU   time.Duration `knox:"dus,scale=us"`
	DurationM   time.Duration `knox:"dms,scale=ms"`
	DurationS   time.Duration `knox:"ds,scale=s"`
}

type ListFields struct {
	Int64a      int64
	U64List     []uint64        `knox:"u64_list"`
	TimeList    []time.Time     `knox:"time_list,element=date"`
	PairList    []Pair          `knox:"pair_list"`
	ByteList    [][]byte        `knox:"byte_list,notnull"`
	ArrList     [][2]byte       `knox:"arr_list,notnull"`
	DecimalList []num.Decimal32 `knox:"dec_list,notnull,element=scale=4"`
	Int64b      int64
}

func NewListFields() *ListFields {
	return &ListFields{
		Int64a:  1,
		U64List: []uint64{2, 3},
		TimeList: []time.Time{
			time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2026, 4, 4, 0, 0, 0, 0, time.UTC),
		},
		PairList: []Pair{
			{Key: 1, Val: 2},
			{Key: 3, Val: 4},
		},
		ByteList: [][]byte{
			binary.BigEndian.AppendUint64(nil, 23),
			binary.BigEndian.AppendUint64(nil, 42),
		},
		ArrList: [][2]byte{
			{1, 2},
			{3, 4},
		},
		DecimalList: []num.Decimal32{
			num.NewDecimal32(1000, 4),
			num.NewDecimal32(2000, 4),
			num.NewDecimal32(3000, 4),
		},
		Int64b: 42,
	}
}

type Pair struct {
	Key int64 `knox:"k64"`
	Val int64 `knox:"v64"`
}

func (p Pair) MarshalSchema(w *schema.Writer) error {
	w.AppendInt64(p.Key)
	w.AppendInt64(p.Val)
	return w.Err()
}

type ListInListFields struct {
	Int64a      int64
	NestedUints [][]uint64
	NestedPairs [][]Pair
	Int64b      int64
}

func NewListInListFields() *ListInListFields {
	return &ListInListFields{
		Int64a: 1,
		NestedUints: [][]uint64{
			{2, 3},
			{4, 5},
		},
		NestedPairs: [][]Pair{
			{
				{Key: 1, Val: 2},
				{Key: 3, Val: 4},
			},
			{
				{Key: 5, Val: 6},
				{Key: 7, Val: 8},
			},
		},
		Int64b: 42,
	}
}

type OuterPairStruct struct {
	Val    uint32
	Pairs2 []Pair
}

type ListInStructInListFields struct {
	Int64a int64
	Pairs1 []OuterPairStruct
	Int64b int64
}

func NewListInStructInListFields() *ListInStructInListFields {
	return &ListInStructInListFields{
		Int64a: 1,
		Pairs1: []OuterPairStruct{
			{Val: 2, Pairs2: []Pair{
				{Key: 1, Val: 2},
				{Key: 3, Val: 4},
			}},
			{Val: 4, Pairs2: []Pair{
				{Key: 5, Val: 6},
				{Key: 7, Val: 8},
			}},
		},
		Int64b: 42,
	}
}

type MapFields struct {
	Int64a     int64
	U64Map     map[uint64]uint64        `knox:"u64_map"`
	DateMap    map[uint32]time.Time     `knox:"date_map,value=date"`
	PairMap    map[string]Pair          `knox:"pair_map"`
	ByteMap    map[string][]byte        `knox:"byte_map,notnull,value=notnull"`
	ArrMap     map[string][2]byte       `knox:"arr_map,notnull"`
	DecimalMap map[string]num.Decimal32 `knox:"dec_map,notnull,value=scale=4"`
	Int64b     int64
}

func NewMapFields() *MapFields {
	return &MapFields{
		Int64a: 1,
		U64Map: map[uint64]uint64{2: 2, 3: 3},
		DateMap: map[uint32]time.Time{
			1: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			2: time.Date(2026, 4, 4, 0, 0, 0, 0, time.UTC),
		},
		PairMap: map[string]Pair{
			"a": {Key: 1, Val: 2},
			"b": {Key: 3, Val: 4},
		},
		ByteMap: map[string][]byte{
			"one": binary.BigEndian.AppendUint64(nil, 23),
			"two": binary.BigEndian.AppendUint64(nil, 42),
		},
		ArrMap: map[string][2]byte{
			"one": {1, 2},
			"two": {3, 4},
		},
		DecimalMap: map[string]num.Decimal32{
			"usd": num.NewDecimal32(1000, 4),
			"eur": num.NewDecimal32(2000, 4),
			"jpy": num.NewDecimal32(3000, 4),
		},
		Int64b: 42,
	}
}

func (r MapFields) MarshalSchema(w *schema.Writer) error {
	w.AppendInt64(r.Int64a)
	w.AppendMap(func(mw *schema.MapWriter) error {
		return schema.AppendMap(mw, r.U64Map)
	})
	w.AppendMap(func(mw *schema.MapWriter) error {
		return schema.AppendMap(mw, r.DateMap)
	})
	w.AppendMap(func(mw *schema.MapWriter) error {
		return schema.MarshalMap(mw, r.PairMap)
	})
	w.AppendMap(func(mw *schema.MapWriter) error {
		return schema.AppendMap(mw, r.ByteMap)
	})
	w.AppendMap(func(mw *schema.MapWriter) error {
		for _, k := range schema.SortedKeys(r.ArrMap) {
			mw.AppendString(k)
			v := r.ArrMap[k]
			mw.AppendBytes(v[:])
			mw.Next()
		}
		return mw.Err()
	})
	w.AppendMap(func(mw *schema.MapWriter) error {
		return schema.AppendMap(mw, r.DecimalMap)
	})
	w.AppendInt64(r.Int64b)
	return w.Err()
}

type PrimMapRecord struct {
	U64     map[uint64]uint64
	Bools   map[uint64]bool
	Strings map[string]string
	Bigs    map[string]num.Big
	Dates   map[string]time.Time
	Times   map[time.Time]uint32
}

func (r PrimMapRecord) MarshalSchema(w *schema.Writer) error {
	w.AppendMap(func(mw *schema.MapWriter) error {
		return schema.AppendMap(mw, r.U64)
	})
	w.AppendMap(func(mw *schema.MapWriter) error {
		return schema.AppendMap(mw, r.Bools)
	})
	w.AppendMap(func(mw *schema.MapWriter) error {
		return schema.AppendMap(mw, r.Strings)
	})
	w.AppendMap(func(mw *schema.MapWriter) error {
		return schema.AppendMap(mw, r.Bigs)
	})
	w.AppendMap(func(mw *schema.MapWriter) error {
		return schema.AppendMap(mw, r.Dates)
	})
	w.AppendMap(func(mw *schema.MapWriter) error {
		return schema.AppendTimeMap(mw, r.Times)
	})
	return w.Err()
}

func NewPrimMapRecord() *PrimMapRecord {
	return &PrimMapRecord{
		U64:     map[uint64]uint64{1: 1, 2: 2},
		Bools:   map[uint64]bool{1: true, 2: false},
		Strings: map[string]string{"a": "b", "c": "d"},
		Bigs:    map[string]num.Big{"a": num.NewBig(1), "b": num.NewBig(2)},
		Dates:   map[string]time.Time{"now": time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
		Times: map[time.Time]uint32{
			time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC): 42,
		},
	}
}

type UnionMapRecord struct {
	Unions map[string]schema.UnionValue
}

func (r UnionMapRecord) MarshalSchema(w *schema.Writer) error {
	return w.AppendMap(func(mw *schema.MapWriter) error {
		return schema.MarshalMap(mw, r.Unions)
	})
}

var UnionMapRecordSchema = schema.SchemaOf([]*schema.Field{
	schema.MapOf(schema.String, schema.Union, schema.WithName("unions")),
})

func NewUnionMapRecord() *UnionMapRecord {
	return &UnionMapRecord{
		Unions: map[string]schema.UnionValue{
			"a": schema.Int64Union(1),
			"b": schema.Int32Union(2),
			"c": schema.BoolUnion(true),
			"d": schema.TimestampUnion(time.Date(2026, 1, 1, 1, 2, 3, 4, time.UTC)),
			"e": schema.Uint16Union(3),
		},
	}
}

var (
	// - k64 int64
	// - kv4 int64
	pairT = schema.SchemaOf([]*schema.Field{
		schema.FieldOf(schema.Int64, schema.WithName("k64")),
		schema.FieldOf(schema.Int64, schema.WithName("v64")),
	},
		schema.Name("pair"),
	)

	// different single nested list types with or without
	// special settings on the content type
	// - u64list []uint64
	// - time_list []Date
	// - pair_list []Pair
	// - byte_list [][]byte
	// - arr_list [][2]byte
	// - dec_list []Decimal32(4)
	listFieldsT = schema.SchemaOf([]*schema.Field{
		schema.FieldOf(schema.Int64, schema.WithName("int64a")),
		schema.ListOf(schema.Uint64, schema.WithName("u64_list")),
		schema.ListOf(schema.Date, schema.WithName("time_list")),
		schema.ListFor(pairT, schema.WithName("pair_list")),
		schema.ListOf(schema.Bytes, schema.WithName("byte_list"), schema.WithNullable(false)),
		schema.ListFor(
			schema.SchemaOf([]*schema.Field{
				schema.FieldOf(schema.Bytes, schema.WithArray(2)),
			}),
			schema.WithName("arr_list"),
			schema.WithNullable(false),
		),
		schema.ListFor(
			schema.SchemaOf([]*schema.Field{
				schema.FieldOf(schema.Decimal32, schema.WithScale(4)),
			}),
			schema.WithName("dec_list"),
			schema.WithNullable(false),
		),
		schema.FieldOf(schema.Int64, schema.WithName("int64b")),
	},
		schema.Name("list_fields"),
	)

	// double nested lists
	// - nested_uints [][]uint64
	// - nested_pairs [][]Pair
	listInListT = schema.SchemaOf([]*schema.Field{
		schema.FieldOf(schema.Int64, schema.WithName("int64a")),
		schema.ListFor(
			schema.SchemaOf([]*schema.Field{
				schema.ListOf(schema.Uint64),
			}),
			schema.WithName("nested_uints"),
		),
		schema.ListFor(
			schema.SchemaOf([]*schema.Field{
				schema.ListFor(pairT),
			}),
			schema.WithName("nested_pairs"),
		),
		schema.FieldOf(schema.Int64, schema.WithName("int64b")),
	},
		schema.Name("list_in_list_fields"),
	)

	// a list-in-struct-in-list-in-struct type
	//
	outerPairStructT = schema.SchemaOf([]*schema.Field{
		schema.FieldOf(schema.Uint32, schema.WithName("val")),
		schema.ListFor(pairT, schema.WithName("pairs2")),
	},
		schema.Name("outer_pair_struct"),
	)
	listInStructInListT = schema.SchemaOf([]*schema.Field{
		schema.FieldOf(schema.Int64, schema.WithName("int64a")),
		schema.ListFor(outerPairStructT, schema.WithName("pairs1")),
		schema.FieldOf(schema.Int64, schema.WithName("int64b")),
	},
		schema.Name("list_in_struct_in_list_fields"),
	)

	// variant fields
	addrT = []*schema.Schema{
		schema.SchemaOf([]*schema.Field{
			schema.FieldOf(schema.String, schema.WithName("street")),
			schema.FieldOf(schema.String, schema.WithName("city")),
			schema.FieldOf(schema.String, schema.WithName("postal_code")),
			schema.FieldOf(schema.String, schema.WithName("country")),
		}, schema.Name("residential")),
		schema.SchemaOf([]*schema.Field{
			schema.FieldOf(schema.String, schema.WithName("company_name")),
			schema.FieldOf(schema.String, schema.WithName("street")),
			schema.FieldOf(schema.String, schema.WithName("city")),
			schema.FieldOf(schema.String, schema.WithName("postal_code")),
			schema.FieldOf(schema.String, schema.WithName("country")),
			schema.FieldOf(schema.String, schema.WithName("tax_id")),
		}, schema.Name("business")),
		schema.SchemaOf([]*schema.Field{
			schema.FieldOf(schema.String, schema.WithName("po_box_number")),
			schema.FieldOf(schema.String, schema.WithName("city")),
			schema.FieldOf(schema.String, schema.WithName("postal_code")),
			schema.FieldOf(schema.String, schema.WithName("country")),
		}, schema.Name("po_box")),
		schema.SchemaOf([]*schema.Field{
			schema.FieldOf(schema.String, schema.WithName("street")),
			schema.FieldOf(schema.String, schema.WithName("city")),
			schema.FieldOf(schema.String, schema.WithName("postal_code")),
			schema.FieldOf(schema.String, schema.WithName("country")),
			schema.FieldOf(schema.String, schema.WithName("state_province")),
			schema.FieldOf(schema.String, schema.WithName("phone")),
		}, schema.Name("international")),
	}

	payT = []*schema.Schema{
		schema.SchemaOf([]*schema.Field{
			schema.FieldOf(schema.String, schema.WithName("last_four")),
			schema.FieldOf(schema.String, schema.WithName("brand")),
			schema.FieldOf(schema.Uint8, schema.WithName("expiry_month")),
			schema.FieldOf(schema.Uint16, schema.WithName("expiry_year")),
			schema.FieldOf(schema.String, schema.WithName("holder_name")),
		}, schema.Name("credit_card")),
		schema.SchemaOf([]*schema.Field{
			schema.FieldOf(schema.String, schema.WithName("email")),
			schema.FieldOf(schema.String, schema.WithName("paypal_account_id")),
			schema.FieldOf(schema.Boolean, schema.WithName("verified")),
		}, schema.Name("paypal")),
		schema.SchemaOf([]*schema.Field{
			schema.FieldOf(schema.String, schema.WithName("iban")),
			schema.FieldOf(schema.String, schema.WithName("bic")),
			schema.FieldOf(schema.String, schema.WithName("account_holder")),
			schema.FieldOf(schema.String, schema.WithName("bank_name")),
		}, schema.Name("bank_transfer")),
		schema.SchemaOf([]*schema.Field{
			schema.FieldOf(schema.String, schema.WithName("chain")),
			schema.FieldOf(schema.String, schema.WithName("wallet_address")),
			schema.FieldOf(schema.Decimal256, schema.WithName("amount_in_native"), schema.WithScale(18)),
			schema.FieldOf(schema.String, schema.WithName("tx_hash"), schema.WithNullable()),
		}, schema.Name("crypto")),
	}

	customerT = schema.SchemaOf([]*schema.Field{
		schema.FieldOf(schema.Uint64, schema.WithName("user_id")),
		schema.FieldOf(schema.String, schema.WithName("user_name")),
		schema.VariantFor(payT, schema.WithName("payment")),
		schema.VariantFor(addrT, schema.WithName("billing_address")),
		schema.VariantFor(addrT, schema.WithName("shipping_address")),
	}, schema.Name("consumer"))
)

var CustomerT = customerT

type CustomerVariant struct{}

func NewCustomer() *CustomerVariant {
	return &CustomerVariant{}
}

func (c CustomerVariant) MarshalSchema(w *schema.Writer) error {
	w.AppendUint64(1)
	w.AppendString("user")
	w.AppendVariant(3, // pay: bank transfer
		func(vw *schema.VariantWriter) error {
			vw.AppendString("iban")
			vw.AppendString("bic")
			vw.AppendString("holder")
			vw.AppendString("bank")
			return vw.Err()
		})
	w.AppendVariant(2, // billing: business
		func(vw *schema.VariantWriter) error {
			vw.AppendString("company")
			vw.AppendString("street")
			vw.AppendString("city")
			vw.AppendString("postcode")
			vw.AppendString("country")
			vw.AppendString("taxid")
			return vw.Err()
		})
	w.AppendVariant(1, // shipping: residential
		func(vw *schema.VariantWriter) error {
			vw.AppendString("street")
			vw.AppendString("city")
			vw.AppendString("postcode")
			vw.AppendString("country")
			return vw.Err()
		})
	return w.Err()
}

var (
	TestStructs = []Encodable{
		&Account{},
		&Transfer{},
		// &AllTypes{},
	}

	accountEnc = encode.NewEncoderFor[Account]()
	accountDec = encode.NewDecoderFor[Account]()
	accountBuf = accountEnc.NewBuffer(1)

	transferEnc = encode.NewEncoderFor[Transfer]()
	transferDec = encode.NewDecoderFor[Transfer]()
	transferBuf = transferEnc.NewBuffer(1)

// allTypesEnc = encode.NewEncoderFor[AllTypes](schema.Enums(enums))
// allTypesDec = encode.NewDecoderFor[AllTypes](schema.Enums(enums))
// allTypesBuf = allTypesEnc.NewBuffer(1)
)

func makeZeroStruct(v any) any {
	typ := reflect.TypeOf(v).Elem()
	ptr := reflect.New(typ)
	val := ptr.Elem()
	for i, l := 0, typ.NumField(); i < l; i++ {
		dst := val.Field(i)
		if dst.Kind() == reflect.Pointer {
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

type Encodable interface {
	Encode() []byte
	Decode([]byte) error
}

type Account struct {
	ID          uint64   `knox:"id,pk"`
	UserData256 [32]byte `knox:"user_data_256"`
	UserData64  uint64   `knox:"user_data_64"`
	UserData32  uint32   `knox:"user_data_32"`
	Reserved    uint32   `knox:"reserved"`
	Ledger      uint32   `knox:"ledger"`
	Code        uint16   `knox:"code"`
	Flags       uint16   `knox:"flags"`
	Timestamp   uint64   `knox:"timestamp,timebase"`
}

func (s *Account) Encode() []byte {
	accountBuf.Reset()
	accountEnc.Encode(accountBuf, s)
	return accountBuf.Bytes()
}

func (s *Account) Decode(buf []byte) error {
	_, err := accountDec.Decode(buf, s)
	return err
}

type Transfer struct {
	ID              uint64     `knox:"id,pk"`
	DebitAccountID  uint64     `knox:"debit_account_id"`
	CreditAccountID uint64     `knox:"credit_account_id"`
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

func (s *Transfer) Encode() []byte {
	transferBuf.Reset()
	transferEnc.Encode(transferBuf, s)
	return transferBuf.Bytes()
}

func (s *Transfer) Decode(buf []byte) error {
	_, err := transferDec.Decode(buf, s)
	return err
}
