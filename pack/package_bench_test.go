package pack

import (
	"encoding/hex"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/encoding/decimal"
	"blockwatch.cc/knoxdb/vec"
)

var rnd = rand.New(rand.NewSource(mustParseI64(os.Getenv("GORANDSEED"))))

func mustParseI64(s string) int64 {
	if s == "" {
		return 0
	}
	n, err := strconv.ParseInt(s, 0, 64)
	if err != nil {
		panic(err)
	}
	return n
}

// func randBytes(sz int) []byte {
// 	buf := make([]byte, sz)
// 	_, _ = rnd.Read(buf)
// 	return buf
// }

var encodeBenchmarkSizes = []struct {
	name string
	num  int
}{
	{"1", 1},
	{"512", 512},
	{"1K", 1 * 1024},
	{"16K", 16 * 1024},
	{"32K", 32 * 1024},
	{"64K", 64 * 1024},
}

type encodeBenchStruct struct {
	Id      uint64             `knox:"id,pk"`
	Time    time.Time          `knox:"time"`
	Hash    []byte             `knox:"hash,bloom=3"`
	String  string             `knox:"str"`
	Bool    bool               `knox:"bool"`
	Enum    Enum               `knox:"enum"`
	Int64   int64              `knox:"i64"`
	Int32   int32              `knox:"i32"`
	Int16   int16              `knox:"i16"`
	Int8    int8               `knox:"i8"`
	Uint64  uint64             `knox:"u64,bloom"`
	Uint32  uint32             `knox:"u32"`
	Uint16  uint16             `knox:"u16"`
	Uint8   uint8              `knox:"u8"`
	Float64 float64            `knox:"f64"`
	Float32 float32            `knox:"f32"`
	D32     decimal.Decimal32  `knox:"d32,scale=5"`
	D64     decimal.Decimal64  `knox:"d64,scale=15"`
	D128    decimal.Decimal128 `knox:"d128,scale=18"`
	D256    decimal.Decimal256 `knox:"d256,scale=24"`
	I128    vec.Int128         `knox:"i128"`
	I256    vec.Int256         `knox:"i256"`
}

func makeBenchData(sz int) (res []encodeBenchStruct) {
	for i := 0; i < sz; i++ {
		res = append(res, encodeBenchStruct{
			Id:      0,
			Time:    time.Now().UTC(),
			Hash:    randBytes(20),
			String:  hex.EncodeToString(randBytes(4)),
			Bool:    true,
			Enum:    Enum(i%4 + 1),
			Int64:   int64(i),
			Int32:   int32(i),
			Int16:   int16(i % (1<<16 - 1)),
			Int8:    int8(i % (1<<8 - 1)),
			Uint64:  uint64(i * 1000000),
			Uint32:  uint32(i * 1000000),
			Uint16:  uint16(i),
			Uint8:   uint8(i),
			Float32: float32(i / 1000000),
			Float64: float64(i / 1000000),
			D32:     decimal.NewDecimal32(int32(100123456789-i), 5),
			D64:     decimal.NewDecimal64(1123456789123456789-int64(i), 15),
			D128:    decimal.NewDecimal128(vec.MustParseInt128(strconv.Itoa(i)+"00000000000000000000"), 18),
			D256:    decimal.NewDecimal256(vec.MustParseInt256(strconv.Itoa(i)+"0000000000000000000000000000000000000000"), 24),
			I128:    vec.MustParseInt128(strconv.Itoa(i) + "000000000000000000000000000000"),
			I256:    vec.MustParseInt256(strconv.Itoa(i) + "000000000000000000000000000000000000000000000000000000000000"),
		})
	}
	return
}

type Enum uint8

type Stringer []string

func (s Stringer) String() string {
	return strings.Join(s, ",")
}

func (s Stringer) MarshalText() ([]byte, error) {
	return []byte(strings.Join(s, ",")), nil
}

func (s *Stringer) UnmarshalText(b []byte) error {
	*s = strings.Split(string(b), ",")
	return nil
}

func makeTypedPackage(typ any, sz, fill int) *Package {
	fields, err := Fields(typ)
	if err != nil {
		panic(err)
	}
	pkg := NewPackage(sz, nil)
	pkg.InitFields(fields, nil)
	for i := 0; i < fill; i++ {
		if err := pkg.Push(makeZeroStruct(typ)); err != nil {
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
		val.Field(i).Set(reflect.Zero(typ.Field(i).Type))
	}
	return ptr.Interface()
}

func BenchmarkPush(b *testing.B) {
	for _, n := range packBenchmarkReadWriteSizes {
		b.Run(n.name, func(b *testing.B) {
			slice := makeBenchData(1)
			pkg := makeTypedPackage(encodeBenchStruct{}, n.l, 0)
			b.ResetTimer()
			b.ReportAllocs()
			// b.SetBytes(int64(s.minWireSize + 8))
			for i := 0; i < b.N; i++ {
				for i, l := 0, n.l; i < l; i++ {
					pkg.Push(slice[0])
				}
				pkg.Clear()
			}
		})
	}
}

func BenchmarkRead(b *testing.B) {
	for _, n := range packBenchmarkReadWriteSizes {
		b.Run(n.name, func(b *testing.B) {
			slice := makeBenchData(1)
			pkg := makeTypedPackage(encodeBenchStruct{}, n.l, 0)
			for i, l := 0, n.l; i < l; i++ {
				pkg.Push(slice[0])
			}
			tinfo, _ := getTypeInfo(slice[0])
			res := &Result{
				fields: pkg.fields,
				pkg:    pkg,
				tinfo:  tinfo,
			}
			b.ResetTimer()
			b.ReportAllocs()
			// b.SetBytes(int64(s.minWireSize + 8))
			var dst encodeBenchStruct
			for i := 0; i < b.N; i++ {
				for i, l := 0, n.l; i < l; i++ {
					_ = res.DecodeAt(i, &dst)
				}
			}
		})
	}
}
