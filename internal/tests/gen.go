// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tests

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"strconv"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/tests/testutil"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/stringx"
)

const BENCH_WIDTH = 60

var Generators = []Generator{
	NumGenerator[int8]{},
	NumGenerator[int16]{},
	NumGenerator[int32]{},
	NumGenerator[int64]{},
	NumGenerator[uint8]{},
	NumGenerator[uint16]{},
	NumGenerator[uint32]{},
	NumGenerator[uint64]{},
	FloatGenerator[float32]{},
	FloatGenerator[float64]{},
	BytesGenerator{},
	BoolsGenerator{},
	Int128Generator{},
	Int256Generator{},
}

type Generator interface {
	Name() string
	Type() types.BlockType
	MakeValue(int) any
	MakeSlice(...int) any
}

// int, uint
var _ Generator = (*NumGenerator[int32])(nil)

type NumGenerator[T types.Integer] struct{}

func (_ NumGenerator[T]) Type() types.BlockType {
	switch any(T(0)).(type) {
	case int64:
		return types.BlockInt64
	case int32:
		return types.BlockInt32
	case int16:
		return types.BlockInt16
	case int8:
		return types.BlockInt8
	case uint64:
		return types.BlockUint64
	case uint32:
		return types.BlockUint32
	case uint16:
		return types.BlockUint16
	case uint8:
		return types.BlockUint8
	default:
		return 0
	}
}

func (_ NumGenerator[T]) Name() string {
	var t T
	return reflect.ValueOf(t).Type().String()
}

func (_ NumGenerator[T]) MakeValue(n int) any {
	return T(n)
}

func (_ NumGenerator[T]) MakeSlice(n ...int) any {
	s := make([]T, len(n))
	for i := range n {
		s[i] = T(n[i])
	}
	return s
}

// float
var _ Generator = (*FloatGenerator[float64])(nil)

type FloatGenerator[T types.Float] struct{}

func (_ FloatGenerator[T]) Type() types.BlockType {
	switch any(T(0)).(type) {
	case float64:
		return types.BlockFloat64
	case float32:
		return types.BlockFloat32
	default:
		return 0
	}
}

func (_ FloatGenerator[T]) Name() string {
	var t T
	return reflect.ValueOf(t).Type().String()
}

func (_ FloatGenerator[T]) MakeValue(n int) any {
	return T(n) + T(0.5) // testutil.RandFloat64())
}

func (_ FloatGenerator[T]) MakeSlice(n ...int) any {
	s := make([]T, len(n))
	for i := range n {
		s[i] = T(n[i]) + T(0.5) // testutil.RandFloat64())
	}
	return s
}

// []byte
var _ Generator = (*BytesGenerator)(nil)

type BytesGenerator struct{}

func (BytesGenerator) Type() types.BlockType {
	return types.BlockBytes
}

func (BytesGenerator) Name() string {
	return "bytes"
}

func (BytesGenerator) MakeValue(n int) any {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(n))
	return b[:]
}

func (BytesGenerator) MakeSlice(n ...int) any {
	s := make([][]byte, len(n))
	for i := range n {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(n[i]))
		s[i] = bytes.Clone(b[:])
	}
	return s
}

// bool
var _ Generator = (*BoolsGenerator)(nil)

type BoolsGenerator struct{}

func (BoolsGenerator) Type() types.BlockType {
	return types.BlockBool
}

func (BoolsGenerator) Name() string {
	return "bool"
}

func (BoolsGenerator) MakeValue(n int) any {
	return n%2 == 0
}

func (BoolsGenerator) MakeSlice(n ...int) any {
	s := make([]bool, len(n))
	for i := range n {
		s[i] = n[i]%2 == 0
	}
	return s
}

// int128
var _ Generator = (*Int128Generator)(nil)

type Int128Generator struct{}

func (Int128Generator) Type() types.BlockType {
	return types.BlockInt128
}

func (Int128Generator) Name() string {
	return "i128"
}

func (Int128Generator) MakeValue(n int) any {
	return num.Int128FromInt64(int64(n))
}

func (Int128Generator) MakeSlice(n ...int) any {
	s := make([]num.Int128, len(n))
	for i := range n {
		s[i] = num.Int128FromInt64(int64(n[i]))
	}
	return s
}

// int256
var _ Generator = (*Int256Generator)(nil)

type Int256Generator struct{}

func (Int256Generator) Type() types.BlockType {
	return types.BlockInt256
}

func (Int256Generator) Name() string {
	return "i256"
}

func (Int256Generator) MakeValue(n int) any {
	return num.Int256FromInt64(int64(n))
}

func (Int256Generator) MakeSlice(n ...int) any {
	s := make([]num.Int256, len(n))
	for i := range n {
		s[i] = num.Int256FromInt64(int64(n[i]))
	}
	return s
}

// Generic Generator Functions

// creates n sequential values
func GenSeq[T types.Number](n, d int) []T {
	res := make([]T, n)
	if d == 0 {
		if types.IsSigned[T]() {
			d = -1
		} else {
			d = 1
		}
	}
	for i := range res {
		res[i] = T(i * d)
	}
	return res
}

func GenRange[T types.Number](start, end T) []T {
	result := make([]T, int(end-start))
	for i := range result {
		result[i] = start + T(i)
	}
	return result
}

// creates n constants of value v
func GenConst[T types.Number](n int, v T) []T {
	res := make([]T, n)
	for i := range res {
		res[i] = v
	}
	return res
}

// creates n random values
func GenRnd[T types.Number](n int) []T {
	var res []T
	switch any(T(0)).(type) {
	case int64:
		res = arena.ReinterpretSlice[int64, T](testutil.RandIntsn[int64](n, 1<<BENCH_WIDTH-1))
	case int32:
		res = arena.ReinterpretSlice[int32, T](testutil.RandIntsn[int32](n, 1<<(BENCH_WIDTH/2-1)))
	case int16:
		res = arena.ReinterpretSlice[int16, T](testutil.RandInts[int16](n))
	case int8:
		res = arena.ReinterpretSlice[int8, T](testutil.RandInts[int8](n))
	case uint64:
		res = arena.ReinterpretSlice[uint64, T](testutil.RandUintsn[uint64](n, 1<<BENCH_WIDTH-1))
	case uint32:
		res = arena.ReinterpretSlice[uint32, T](testutil.RandUintsn[uint32](n, 1<<(BENCH_WIDTH/2-1)))
	case uint16:
		res = arena.ReinterpretSlice[uint16, T](testutil.RandUints[uint16](n))
	case uint8:
		res = arena.ReinterpretSlice[uint8, T](testutil.RandUints[uint8](n))
	case float64:
		res = make([]T, n)
		for i, v := range testutil.RandUintsn[uint64](n, 1<<BENCH_WIDTH-1) {
			res[i] = T(v) / 100.0
		}
	case float32:
		res = make([]T, n)
		for i, v := range testutil.RandUintsn[uint32](n, 1<<(BENCH_WIDTH/2)-1) {
			res[i] = T(v) / 100.0
		}
	}
	return res
}

// creates n random values with bit width of up to w
func GenRndBits[T types.Number](n, w int) []T {
	if w == 0 {
		return make([]T, n)
	}
	var res []T
	switch any(T(0)).(type) {
	case int64:
		res = arena.ReinterpretSlice[int64, T](testutil.RandIntsn[int64](n, 1<<min(w, 63)-1))
	case int32:
		res = arena.ReinterpretSlice[int32, T](testutil.RandIntsn[int32](n, 1<<min(w, 31)-1))
	case int16:
		res = arena.ReinterpretSlice[int16, T](testutil.RandIntsn[int16](n, 1<<min(w, 15)-1))
	case int8:
		res = arena.ReinterpretSlice[int8, T](testutil.RandIntsn[int8](n, 1<<min(w, 7)-1))
	case uint64:
		res = arena.ReinterpretSlice[uint64, T](testutil.RandUintsn[uint64](n, 1<<w-1))
	case uint32:
		res = arena.ReinterpretSlice[uint32, T](testutil.RandUintsn[uint32](n, 1<<w-1))
	case uint16:
		res = arena.ReinterpretSlice[uint16, T](testutil.RandUintsn[uint16](n, 1<<w-1))
	case uint8:
		res = arena.ReinterpretSlice[uint8, T](testutil.RandUintsn[uint8](n, 1<<w-1))
	case float64:
		res = make([]T, n)
		for i, v := range testutil.RandUintsn[uint64](n, 1<<min(w, 49)-1) {
			res[i] = T(v) /// 100.0
		}
	case float32:
		res = make([]T, n)
		for i, v := range testutil.RandUintsn[uint32](n, 1<<min(w, 29)-1) {
			res[i] = T(v) / 100.0
		}
	}
	return res
}

// creates n values with cardinality c and max bit width w. W is only used
// for 64 and 32 bit types.
func GenDups[T types.Number](n, c, w int) []T {
	if w > BENCH_WIDTH {
		panic(fmt.Errorf("w=%d must be smaller than %d", w, BENCH_WIDTH))
	}
	if c > n {
		panic(fmt.Errorf("c=%d must be smaller than n=%d", c, n))
	}
	if w <= 0 {
		w = BENCH_WIDTH
	}
	if c <= 0 {
		c = 1
	}
	res := make([]T, n)
	switch any(T(0)).(type) {
	case int64:
		unique := testutil.RandIntsn[int64](c, 1<<min(w, 63)-1)
		for i := range res {
			res[i] = T(unique[testutil.RandIntn(c)])
		}
	case int32:
		unique := testutil.RandIntsn[int32](c, 1<<min(w, 31)-1)
		for i := range res {
			res[i] = T(unique[testutil.RandIntn(c)])
		}
	case int16:
		unique := testutil.RandIntsn[int16](c, 1<<min(w, 15)-1)
		for i := range res {
			res[i] = T(unique[testutil.RandIntn(c)])
		}
	case int8:
		unique := testutil.RandIntsn[int8](c, 1<<min(w, 7)-1)
		for i := range res {
			res[i] = T(unique[testutil.RandIntn(c)])
		}
	case uint64:
		unique := testutil.RandUintsn[uint64](c, 1<<w-1)
		for i := range res {
			res[i] = T(unique[testutil.RandIntn(c)])
		}
	case uint32:
		unique := testutil.RandUintsn[uint32](c, 1<<min(w, 32)-1)
		for i := range res {
			res[i] = T(unique[testutil.RandIntn(c)])
		}
	case uint16:
		unique := testutil.RandUintsn[uint16](c, 1<<min(w, 16)-1)
		for i := range res {
			res[i] = T(unique[testutil.RandIntn(c)])
		}
	case uint8:
		unique := testutil.RandUintsn[uint8](c, 1<<min(w, 8)-1)
		for i := range res {
			res[i] = T(unique[testutil.RandIntn(c)])
		}
	case float64:
		// produces not full random, but more realistic floats for tests
		unique := testutil.RandUintsn[uint64](c, 1<<min(w, 49)-1)
		for i := range res {
			res[i] = T(unique[testutil.RandIntn(c)]) / 100.0
		}
	case float32:
		// produces not full random, but more realistic floats for tests
		unique := testutil.RandUintsn[uint32](c, 1<<min(w/2, 29)-1)
		for i := range res {
			res[i] = T(unique[testutil.RandIntn(c)]) / 100.0
		}
	}
	return res
}

// creates n values with run length r at max bit width w
func GenRuns[T types.Number](n, r, w int) []T {
	if w > BENCH_WIDTH {
		panic(fmt.Errorf("w=%d must be smaller than %d", w, BENCH_WIDTH))
	}
	if r > n {
		panic(fmt.Errorf("r=%d must be smaller than n=%d", r, n))
	}
	if w <= 0 {
		w = BENCH_WIDTH
	}
	res := make([]T, 0, n)
	sz := (n + r - 1) / r
	switch any(T(0)).(type) {
	case int64:
		for _, v := range testutil.RandIntsn[int64](sz, 1<<w-1) {
			for range r {
				if len(res) == n {
					break
				}
				res = append(res, T(v))
			}
		}
	case int32:
		for _, v := range testutil.RandIntsn[int32](sz, 1<<min(w, 31)-1) {
			for range r {
				if len(res) == n {
					break
				}
				res = append(res, T(v))
			}
		}
	case int16:
		for _, v := range testutil.RandIntsn[int16](sz, 1<<min(w, 15)-1) {
			for range r {
				if len(res) == n {
					break
				}
				res = append(res, T(v))
			}
		}
	case int8:
		for _, v := range testutil.RandIntsn[int8](sz, 1<<min(w, 7)-1) {
			for range r {
				if len(res) == n {
					break
				}
				res = append(res, T(v))
			}
		}
	case uint64:
		for _, v := range testutil.RandUintsn[uint64](sz, 1<<w-1) {
			for range r {
				if len(res) == n {
					break
				}
				res = append(res, T(v))
			}
		}
	case uint32:
		for _, v := range testutil.RandUintsn[uint32](sz, 1<<min(w, 32)-1) {
			for range r {
				if len(res) == n {
					break
				}
				res = append(res, T(v))
			}
		}
	case uint16:
		for _, v := range testutil.RandUintsn[uint16](sz, 1<<min(w, 16)-1) {
			for range r {
				if len(res) == n {
					break
				}
				res = append(res, T(v))
			}
		}
	case uint8:
		for _, v := range testutil.RandUintsn[uint8](sz, 1<<min(w, 8)-1) {
			for range r {
				if len(res) == n {
					break
				}
				res = append(res, T(v))
			}
		}
	case float64:
		// produces not full random, but more realistic floats for tests
		for _, v := range testutil.RandUintsn[uint64](sz, 1<<min(w, 49)-1) {
			for range r {
				if len(res) == n {
					break
				}
				res = append(res, T(v)/100.0)
			}
		}
	case float32:
		for _, v := range testutil.RandUintsn[uint32](sz, 1<<min(w/2, 29)-1) {
			for range r {
				if len(res) == n {
					break
				}
				res = append(res, T(v)/100.0)
			}
		}
	}
	return res
}

func GenStringSeq(n, d int) *stringx.StringPool {
	var i int
	p := stringx.NewStringPool(n)
	for range n {
		p.AppendString(strconv.Itoa(i))
		i += d
	}
	return p
}

func GenStringConst(n int, v []byte) *stringx.StringPool {
	p := stringx.NewStringPool(n)
	for range n {
		p.Append(v)
	}
	return p
}

func GenStringRnd(n, l int) *stringx.StringPool {
	if l <= 0 {
		// random lengths up to 64
		p := stringx.NewStringPool(n)
		for range n {
			p.Append(testutil.RandBytes(testutil.RandIntn(l)))
		}
		return p
	} else {
		// fixed length strings
		p := stringx.NewStringPoolSize(n, l)
		p.AppendMany(testutil.RandByteSlices(n, l)...)
		return p
	}
}

func GenStringDups(n, c, l int) *stringx.StringPool {
	var mk func() []byte
	if l <= 0 {
		mk = func() []byte { return testutil.RandBytes(testutil.RandIntn(64)) }
	} else {
		mk = func() []byte { return testutil.RandBytes(l) }
	}
	if c > n {
		panic(fmt.Errorf("c=%d must be smaller than n=%d", c, n))
	}
	if c <= 0 {
		c = 1
	}
	unique := make([][]byte, 0, c)
	for range c {
		unique = append(unique, mk())
	}
	p := stringx.NewStringPool(n)
	for range n {
		p.Append(unique[testutil.RandIntn(c)])
	}
	return p
}

// creates n values with run length r at max string length l
func GenStringRuns(n, r, l int) *stringx.StringPool {
	if r > n {
		panic(fmt.Errorf("r=%d must be smaller than n=%d", r, n))
	}
	var mk func() []byte
	if l <= 0 {
		mk = func() []byte { return testutil.RandBytes(testutil.RandIntn(64)) }
	} else {
		mk = func() []byte { return testutil.RandBytes(l) }
	}

	p := stringx.NewStringPool(n)
	sz := (n + r - 1) / r

	for range sz {
		v := mk()
		for range r {
			if p.Len() == n {
				break
			}
			p.Append(v)
		}
	}
	return p
}
