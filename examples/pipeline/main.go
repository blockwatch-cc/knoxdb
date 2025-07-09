package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"time"

	"blockwatch.cc/knoxdb/internal/operator"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/util"
)

func main() {
	if err := run(); err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}

func run() error {
	pl := operator.NewPhysicalPipeline().
		WithSource(NewGenerator(128, 257)).
		WithOperator(operator.NewDescriber(os.Stdout)).
		WithSink(operator.NewLogger(os.Stdout, 10))
	defer pl.Close()

	return operator.NewExecutor().
		AddPipeline(pl).
		Run(context.Background())
}

// Generator produces packs with synthetic data
var _ operator.PullOperator = (*Generator)(nil)

type Generator struct {
	next  int
	limit int
	maxsz int
	pkg   *pack.Package
	enc   *schema.Encoder
	buf   *bytes.Buffer
	err   error
}

func NewGenerator(maxsz, limit int) *Generator {
	return &Generator{
		next:  1,
		limit: limit,
		maxsz: maxsz,
	}
}

func (gen *Generator) Next(context.Context) (*pack.Package, operator.Result) {
	if gen.pkg == nil {
		s, err := schema.SchemaOf(Record{})
		if err != nil {
			gen.err = err
			return nil, operator.ResultError
		}
		if enum, ok := s.Enums().Lookup("my_enum"); ok {
			enum.Append(myEnums...)
		}
		gen.enc = schema.NewEncoder(s)
		gen.buf = gen.enc.NewBuffer(1)
		gen.pkg = pack.New().
			WithKey(0).
			WithVersion(0).
			WithSchema(s).
			WithMaxRows(gen.maxsz).
			Alloc()
	} else {
		gen.pkg.WithKey(gen.pkg.Key() + 1)
		gen.pkg.Clear()
	}

	if gen.limit == 0 {
		return nil, operator.ResultDone
	}

	for gen.limit > 0 && !gen.pkg.IsFull() {
		buf, err := gen.enc.Encode(MakeRecord(gen.next), gen.buf)
		if err != nil {
			gen.err = err
			return nil, operator.ResultError
		}
		gen.buf.Reset()
		gen.pkg.AppendWire(buf, &schema.Meta{Rid: uint64(gen.next), Ref: uint64(gen.next), Xmin: 1})
		gen.limit--
		gen.next++
	}

	return gen.pkg, operator.ResultOK
}

func (gen *Generator) Err() error {
	return gen.err
}

func (gen *Generator) Close() {
	gen.next = 0
	gen.limit = 0
	gen.maxsz = 0
	if gen.pkg != nil {
		gen.pkg.Release()
		gen.pkg = nil
	}
	gen.enc = nil
	gen.buf = nil
	gen.err = nil
}

type MyEnum string

const (
	MyEnumOne   = "one"
	MyEnumTwo   = "two"
	MyEnumThree = "three"
	MyEnumFour  = "four"
)

var myEnums = []string{MyEnumOne, MyEnumTwo, MyEnumThree, MyEnumFour}

type Record struct {
	Id        uint64         `knox:"id,pk"`
	Timestamp time.Time      `knox:"time"`
	Date      time.Time      `knox:"date,date"`
	Hash      [32]byte       `knox:"hash,index=bloom:3"`
	String    string         `knox:"string"`
	Bool      bool           `knox:"bool"`
	MyEnum    MyEnum         `knox:"my_enum,enum"`
	Int64     int64          `knox:"int64"`
	Int32     int32          `knox:"int32"`
	Int16     int16          `knox:"int16"`
	Int8      int8           `knox:"int8"`
	Int_64    int            `knox:"int_as_int64"`
	Uint64    uint64         `knox:"uint64,index=bloom:2"`
	Uint32    uint32         `knox:"uint32"`
	Uint16    uint16         `knox:"uint16"`
	Uint8     uint8          `knox:"uint8"`
	Uint_64   uint           `knox:"uint_as_uint64"`
	Float64   float64        `knox:"float64"`
	Float32   float32        `knox:"float32"`
	D32       num.Decimal32  `knox:"decimal32,scale=5"`
	D64       num.Decimal64  `knox:"decimal64,scale=15"`
	D128      num.Decimal128 `knox:"decimal128,scale=18"`
	D256      num.Decimal256 `knox:"decimal256,scale=24"`
	I128      num.Int128     `knox:"int128"`
	I256      num.Int256     `knox:"int256"`
	Big       num.Big        `knox:"big"`
}

func MakeRecord(i int) *Record {
	return &Record{
		Id:        0, // empty, will be set by insert
		Timestamp: time.Now().UTC(),
		Date:      time.Now().UTC(),
		Hash:      [32]byte(util.RandBytes(32)),
		String:    hex.EncodeToString(util.RandBytes(4)),
		Bool:      true,
		MyEnum:    MyEnum(myEnums[i%4]),
		// typed ints
		Int64: int64(i),
		Int32: int32(i),
		Int16: int16(i % (1<<16 - 1)),
		Int8:  int8(i % (1<<8 - 1)),
		// int to typed int
		Int_64: i,
		// typed uints
		Uint64: uint64(i),
		Uint32: uint32(i),
		Uint16: uint16(i),
		Uint8:  uint8(i),
		// uint to typed uint
		Uint_64: uint(i),
		Float32: float32(i),
		Float64: float64(i),
		// decimals
		D32:  num.NewDecimal32(int32(i)*100000, 5),
		D64:  num.NewDecimal64(int64(i)*1000000000000, 15),
		D128: num.NewDecimal128(num.MustParseInt128(strconv.Itoa(i)+"00000000000000000000"), 18),
		D256: num.NewDecimal256(num.MustParseInt256(strconv.Itoa(i)+"0000000000000000000000000000000000000000"), 24),
		I128: num.MustParseInt128(strconv.Itoa(i) + "000000000000000000000000000000"),
		I256: num.MustParseInt256(strconv.Itoa(i) + "000000000000000000000000000000000000000000000000000000000000"),
		Big:  num.NewBig(int64(i)),
	}
}
