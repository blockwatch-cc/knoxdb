// Copyright (c) 2021 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package xxhashVec

import (
	"encoding/binary"
	"reflect"
	"testing"

	"blockwatch.cc/knoxdb/pkg/util"
)

type hashBenchmarkSize struct {
	name string
	l    int
}

var hashBenchmarkSizes = []hashBenchmarkSize{
	{"8", 8},
	{"1K", 1 * 1024},
	{"16K", 16 * 1024},
	{"64K", 64 * 1024},
	{"128K", 128 * 1024},
	{"1M", 1024 * 1024},
	{"128M", 128 * 1024 * 1024},
}

type XXHash32Uint32Test struct {
	name   string
	slice  []uint32
	result []uint32
}

type XXHash32Int32Test struct {
	name   string
	slice  []int32
	result []uint32
}

type XXHash32Uint64Test struct {
	name   string
	slice  []uint64
	result []uint32
}

type XXHash32Int64Test struct {
	name   string
	slice  []int64
	result []uint32
}

type XXHash64Uint32Test struct {
	name   string
	slice  []uint32
	result []uint64
}

// type XXHash64Int32Test struct {
// 	name   string
// 	slice  []int32
// 	result []uint64
// }

type XXHash64Uint64Test struct {
	name   string
	slice  []uint64
	result []uint64
}

// type XXHash64Int64Test struct {
// 	name   string
// 	slice  []int64
// 	result []uint64
// }

var (
	xxhashInput = [][]byte{
		{0, 1, 2, 3, 4, 5, 6, 7},
		{1, 2, 3, 4, 5, 6, 7, 8},
		{2, 3, 4, 5, 6, 7, 8, 9},
		{3, 4, 5, 6, 7, 8, 9, 10},
		{4, 5, 6, 7, 8, 9, 10, 11},
		{5, 6, 7, 8, 9, 10, 11, 12},
		{6, 7, 8, 9, 10, 11, 12, 13},
		{7, 8, 9, 10, 11, 12, 13, 14},
	}
	/* reference values are calculatetd with xxhash library v0.8.0
	 * https://github.com/Cyan4973/xxHash */
	xxhash32Uint32Result = []uint32{2154372710, 4271296924, 2572881654, 3610179124,
		1767988938, 2757935525, 3225940163, 3594529143}
	xxhash32Uint64Result = []uint32{2746060985, 339348840, 1725762203, 1251338271,
		1114514114, 1889681329, 3683323844, 2797893054}
	xxhash64Uint32Result = []uint64{18432908232848821278, 6063570110359613137, 873772980599321746, 5856652436104769068,
		5752797560547662665, 16833853067498898772, 3015398042591893023, 11282460491355425862}
	xxhash64Uint64Result = []uint64{9820687458478070669, 9316896406413536788, 13085766782279498260, 1636669749266472520,
		7694617266880998282, 738958588033515616, 8444214855924868781, 5257069345255417428}
	xxh3Uint32Result = []uint64{6979084321315492338, 10992015174800262690, 9198932749014320068, 284606709437413655,
		9636445692175435800, 10506574136472534422, 15288656668032338727, 17931165511542358483}
	xxh3Uint64Result = []uint64{4187271766389786872, 1653410307359580823, 10968988069148854349, 18394629982161883682,
		7288085727936083465, 17701208102331325482, 17779176444116337920, 9817807099013809187}
)

// creates an XXHash32 test case for uint32 input date from the given slice
// Parameters:
//   - name: desired name of the test case
//   - slice: the slice for constructing the test case
//   - result: result for the given slice
//   - len: desired length of the test case
func CreateXXHash32Uint32TestCase(name string, input [][]byte, result []uint32, length int) XXHash32Uint32Test {
	if len(result) != len(input) {
		panic("CreateXXHash32Uint32TestCase: length of slice and length of result does not match")
	}

	// Create input slice from bytes
	slice := make([]uint32, len(input))
	for i, v := range input {
		slice[i] = binary.LittleEndian.Uint32(v[0:4])
	}

	// create new slice by concat of given slice
	// we make it a little bit longer check buffer overruns
	var new_slice []uint32
	var l int = length
	for l > 0 {
		new_slice = append(new_slice, slice...)
		l -= len(slice)
	}

	// create new result by concat of given result
	var new_result []uint32
	l = length
	for l > 0 {
		new_result = append(new_result, result...)
		l -= len(result)
	}

	return XXHash32Uint32Test{
		name:   name,
		slice:  new_slice[:length],
		result: new_result[:length],
	}
}

// creates an XXHash32 test case for int32 input date from the given slice
// Parameters:
//   - name: desired name of the test case
//   - slice: the slice for constructing the test case
//   - result: result for the given slice
//   - len: desired length of the test case
func CreateXXHash32Int32TestCase(name string, input [][]byte, result []uint32, length int) XXHash32Int32Test {
	if len(result) != len(input) {
		panic("CreateXXHash32Int32TestCase: length of slice and length of result does not match")
	}

	// Create input slice from bytes
	slice := make([]int32, len(input))
	for i, v := range input {
		slice[i] = int32(binary.LittleEndian.Uint32(v[0:4]))
	}

	// create new slice by concat of given slice
	// we make it a little bit longer check buffer overruns
	var new_slice []int32
	var l int = length
	for l > 0 {
		new_slice = append(new_slice, slice...)
		l -= len(slice)
	}

	// create new result by concat of given result
	var new_result []uint32
	l = length
	for l > 0 {
		new_result = append(new_result, result...)
		l -= len(result)
	}

	return XXHash32Int32Test{
		name:   name,
		slice:  new_slice[:length],
		result: new_result[:length],
	}
}

// creates an XXHash32 test case for uint64 input date from the given slice
// Parameters:
//   - name: desired name of the test case
//   - slice: the slice for constructing the test case
//   - result: result for the given slice
//   - len: desired length of the test case
func CreateXXHash32Uint64TestCase(name string, input [][]byte, result []uint32, length int) XXHash32Uint64Test {
	if len(result) != len(input) {
		panic("CreateXXHash32Uint64TestCase: length of slice and length of result does not match")
	}

	// Create input slice from bytes
	slice := make([]uint64, len(input))
	for i, v := range input {
		slice[i] = binary.LittleEndian.Uint64(v[0:8])
	}

	// create new slice by concat of given slice
	// we make it a little bit longer check buffer overruns
	var new_slice []uint64
	var l int = length
	for l > 0 {
		new_slice = append(new_slice, slice...)
		l -= len(slice)
	}

	// create new result by concat of given result
	var new_result []uint32
	l = length
	for l > 0 {
		new_result = append(new_result, result...)
		l -= len(result)
	}

	return XXHash32Uint64Test{
		name:   name,
		slice:  new_slice[:length],
		result: new_result[:length],
	}
}

// creates an XXHash32 test case for int64 input date from the given slice
// Parameters:
//   - name: desired name of the test case
//   - slice: the slice for constructing the test case
//   - result: result for the given slice
//   - len: desired length of the test case
func CreateXXHash32Int64TestCase(name string, input [][]byte, result []uint32, length int) XXHash32Int64Test {
	if len(result) != len(input) {
		panic("CreateXXHash32Int64TestCase: length of slice and length of result does not match")
	}

	// Create input slice from bytes
	slice := make([]int64, len(input))
	for i, v := range input {
		slice[i] = int64(binary.LittleEndian.Uint64(v[0:8]))
	}

	// create new slice by concat of given slice
	// we make it a little bit longer check buffer overruns
	var new_slice []int64
	var l int = length
	for l > 0 {
		new_slice = append(new_slice, slice...)
		l -= len(slice)
	}

	// create new result by concat of given result
	var new_result []uint32
	l = length
	for l > 0 {
		new_result = append(new_result, result...)
		l -= len(result)
	}

	return XXHash32Int64Test{
		name:   name,
		slice:  new_slice[:length],
		result: new_result[:length],
	}
}

// creates an XXHash64 test case for uint32 input date from the given slice
// Parameters:
//   - name: desired name of the test case
//   - slice: the slice for constructing the test case
//   - result: result for the given slice
//   - len: desired length of the test case
func CreateXXHash64Uint32TestCase(name string, input [][]byte, result []uint64, length int) XXHash64Uint32Test {
	if len(result) != len(input) {
		panic("CreateXXHash64Uint32TestCase: length of slice and length of result does not match")
	}

	// Create input slice from bytes
	slice := make([]uint32, len(input))
	for i, v := range input {
		slice[i] = binary.LittleEndian.Uint32(v[0:4])
	}

	// create new slice by concat of given slice
	// we make it a little bit longer check buffer overruns
	var new_slice []uint32
	var l int = length
	for l > 0 {
		new_slice = append(new_slice, slice...)
		l -= len(slice)
	}

	// create new result by concat of given result
	var new_result []uint64
	l = length
	for l > 0 {
		new_result = append(new_result, result...)
		l -= len(result)
	}

	return XXHash64Uint32Test{
		name:   name,
		slice:  new_slice[:length],
		result: new_result[:length],
	}
}

// creates an XXHash64 test case for uint64 input date from the given slice
// Parameters:
//   - name: desired name of the test case
//   - slice: the slice for constructing the test case
//   - result: result for the given slice
//   - len: desired length of the test case
func CreateXXHash64Uint64TestCase(name string, input [][]byte, result []uint64, length int) XXHash64Uint64Test {
	if len(result) != len(input) {
		panic("CreateXXHash64Uint64TestCase: length of slice and length of result does not match")
	}

	// Create input slice from bytes
	slice := make([]uint64, len(input))
	for i, v := range input {
		slice[i] = binary.LittleEndian.Uint64(v[0:8])
	}

	// create new slice by concat of given slice
	// we make it a little bit longer check buffer overruns
	var new_slice []uint64
	var l int = length
	for l > 0 {
		new_slice = append(new_slice, slice...)
		l -= len(slice)
	}

	// create new result by concat of given result
	var new_result []uint64
	l = length
	for l > 0 {
		new_result = append(new_result, result...)
		l -= len(result)
	}

	return XXHash64Uint64Test{
		name:   name,
		slice:  new_slice[:length],
		result: new_result[:length],
	}
}

/*************** xxhash32Uint32 *******************************************************/

func TestXXHash32Uint32(t *testing.T) {
	for i, c := range xxhashInput {
		if got, want := XXHash32Uint32(binary.LittleEndian.Uint32(c[0:4]), 0), xxhash32Uint32Result[i]; got != want {
			t.Errorf("%v: unexpected result %v expected %v", c[0:4], got, want)
		}
	}
}

var xxhash32Uint32Cases = []XXHash32Uint32Test{
	{
		name:   "l0",
		slice:  make([]uint32, 0),
		result: []uint32{},
	}, {
		name:   "nil",
		slice:  nil,
		result: []uint32{},
	},
	CreateXXHash32Uint32TestCase("l7", xxhashInput, xxhash32Uint32Result, 7),
	CreateXXHash32Uint32TestCase("l8", xxhashInput, xxhash32Uint32Result, 8),
	CreateXXHash32Uint32TestCase("l15", xxhashInput, xxhash32Uint32Result, 15),
	CreateXXHash32Uint32TestCase("l16", xxhashInput, xxhash32Uint32Result, 16),
	CreateXXHash32Uint32TestCase("l31", xxhashInput, xxhash32Uint32Result, 31),
	CreateXXHash32Uint32TestCase("l32", xxhashInput, xxhash32Uint32Result, 32),
}

func TestXXHash32Uint32SliceGeneric(t *testing.T) {
	for _, c := range xxhash32Uint32Cases {
		// pre-allocate the result slice
		res := make([]uint32, len(c.slice))
		xxhash32Uint32SliceGeneric(c.slice, res, 0)
		if got, want := len(res), len(c.result); got != want {
			t.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if !reflect.DeepEqual(res, c.result) {
			t.Errorf("%s: unexpected result %d, expected %d", c.name, res, c.result)
		}
	}
}

func BenchmarkXXHash32Uint32SliceGeneric(b *testing.B) {
	for _, n := range hashBenchmarkSizes {
		a := util.RandUints[uint32](n.l)
		res := make([]uint32, n.l)
		b.Run(n.name, func(b *testing.B) {
			b.SetBytes(int64(n.l * 4))
			for i := 0; i < b.N; i++ {
				xxhash32Uint32SliceGeneric(a, res, 0)
			}
		})
	}
}

/*************** xxhash32Int32 *******************************************************/

func TestXXHash32Int32(t *testing.T) {
	for i, c := range xxhashInput {
		if got, want := XXHash32Int32(int32(binary.LittleEndian.Uint32(c[0:4])), 0), xxhash32Uint32Result[i]; got != want {
			t.Errorf("%v: unexpected result %v expected %v", c[0:4], got, want)
		}
	}
}

var xxhash32Int32Cases = []XXHash32Int32Test{
	{
		name:   "l0",
		slice:  make([]int32, 0),
		result: []uint32{},
	}, {
		name:   "nil",
		slice:  nil,
		result: []uint32{},
	},
	CreateXXHash32Int32TestCase("l7", xxhashInput, xxhash32Uint32Result, 7),
	CreateXXHash32Int32TestCase("l8", xxhashInput, xxhash32Uint32Result, 8),
	CreateXXHash32Int32TestCase("l15", xxhashInput, xxhash32Uint32Result, 15),
	CreateXXHash32Int32TestCase("l16", xxhashInput, xxhash32Uint32Result, 16),
	CreateXXHash32Int32TestCase("l31", xxhashInput, xxhash32Uint32Result, 31),
	CreateXXHash32Int32TestCase("l32", xxhashInput, xxhash32Uint32Result, 32),
}

func TestXXHash32Int32SliceGeneric(t *testing.T) {
	for _, c := range xxhash32Int32Cases {
		// pre-allocate the result slice
		res := make([]uint32, len(c.slice))
		xxhash32Int32SliceGeneric(c.slice, res, 0)
		if got, want := len(res), len(c.result); got != want {
			t.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if !reflect.DeepEqual(res, c.result) {
			t.Errorf("%s: unexpected result %d, expected %d", c.name, res, c.result)
		}
	}
}

func BenchmarkXXHash32Int32SliceGeneric(b *testing.B) {
	for _, n := range hashBenchmarkSizes {
		a := util.RandInts[int32](n.l)
		res := make([]uint32, n.l)
		b.Run(n.name, func(b *testing.B) {
			b.SetBytes(int64(n.l * 4))
			for i := 0; i < b.N; i++ {
				xxhash32Int32SliceGeneric(a, res, 0)
			}
		})
	}
}

/*************** xxhash32Uint64 *******************************************************/

func TestXXHash32Uint64(t *testing.T) {
	for i, c := range xxhashInput {
		if got, want := XXHash32Uint64(binary.LittleEndian.Uint64(c[0:8]), 0), xxhash32Uint64Result[i]; got != want {
			t.Errorf("%v: unexpected result %v expected %v", c[0:8], got, want)
		}
	}
}

var xxhash32Uint64Cases = []XXHash32Uint64Test{
	{
		name:   "l0",
		slice:  make([]uint64, 0),
		result: []uint32{},
	}, {
		name:   "nil",
		slice:  nil,
		result: []uint32{},
	},
	CreateXXHash32Uint64TestCase("l7", xxhashInput, xxhash32Uint64Result, 7),
	CreateXXHash32Uint64TestCase("l8", xxhashInput, xxhash32Uint64Result, 8),
	CreateXXHash32Uint64TestCase("l15", xxhashInput, xxhash32Uint64Result, 15),
	CreateXXHash32Uint64TestCase("l16", xxhashInput, xxhash32Uint64Result, 16),
	CreateXXHash32Uint64TestCase("l31", xxhashInput, xxhash32Uint64Result, 31),
	CreateXXHash32Uint64TestCase("l32", xxhashInput, xxhash32Uint64Result, 32),
}

func TestXXHash32Uint64SliceGeneric(t *testing.T) {
	for _, c := range xxhash32Uint64Cases {
		// pre-allocate the result slice
		res := make([]uint32, len(c.slice))
		xxhash32Uint64SliceGeneric(c.slice, res, 0)
		if got, want := len(res), len(c.result); got != want {
			t.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if !reflect.DeepEqual(res, c.result) {
			t.Errorf("%s: unexpected result %d, expected %d", c.name, res, c.result)
		}
	}
}

func BenchmarkXXHash32Uint64SliceGeneric(b *testing.B) {
	for _, n := range hashBenchmarkSizes {
		a := util.RandUints[uint64](n.l)
		res := make([]uint32, n.l)
		b.Run(n.name, func(b *testing.B) {
			b.SetBytes(8 * int64(n.l))
			for i := 0; i < b.N; i++ {
				xxhash32Uint64SliceGeneric(a, res, 0)
			}
		})
	}
}

/*************** xxhash32Int64 *******************************************************/

func TestXXHash32Int64(t *testing.T) {
	for i, c := range xxhashInput {
		if got, want := XXHash32Int64(int64(binary.LittleEndian.Uint64(c[0:8])), 0), xxhash32Uint64Result[i]; got != want {
			t.Errorf("%v: unexpected result %v expected %v", c[0:4], got, want)
		}
	}
}

var xxhash32Int64Cases = []XXHash32Int64Test{
	{
		name:   "l0",
		slice:  make([]int64, 0),
		result: []uint32{},
	}, {
		name:   "nil",
		slice:  nil,
		result: []uint32{},
	},
	CreateXXHash32Int64TestCase("l7", xxhashInput, xxhash32Uint64Result, 7),
	CreateXXHash32Int64TestCase("l8", xxhashInput, xxhash32Uint64Result, 8),
	CreateXXHash32Int64TestCase("l15", xxhashInput, xxhash32Uint64Result, 15),
	CreateXXHash32Int64TestCase("l16", xxhashInput, xxhash32Uint64Result, 16),
	CreateXXHash32Int64TestCase("l31", xxhashInput, xxhash32Uint64Result, 31),
	CreateXXHash32Int64TestCase("l32", xxhashInput, xxhash32Uint64Result, 32),
}

func TestXXHash32Int64SliceGeneric(t *testing.T) {
	for _, c := range xxhash32Int64Cases {
		// pre-allocate the result slice
		res := make([]uint32, len(c.slice))
		xxhash32Int64SliceGeneric(c.slice, res, 0)
		if got, want := len(res), len(c.result); got != want {
			t.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if !reflect.DeepEqual(res, c.result) {
			t.Errorf("%s: unexpected result %d, expected %d", c.name, res, c.result)
		}
	}
}

func BenchmarkXXHash32Int64SliceGeneric(b *testing.B) {
	for _, n := range hashBenchmarkSizes {
		a := util.RandInts[int64](n.l)
		res := make([]uint32, n.l)
		b.Run(n.name, func(b *testing.B) {
			b.SetBytes(int64(n.l * 4))
			for i := 0; i < b.N; i++ {
				xxhash32Int64SliceGeneric(a, res, 0)
			}
		})
	}
}

/*************** xxhash64Uint32 *******************************************************/

func TestXXHash64Uint32(t *testing.T) {
	for i, c := range xxhashInput {
		if got, want := XXHash64Uint32(binary.LittleEndian.Uint32(c[0:4])), xxhash64Uint32Result[i]; got != want {
			t.Errorf("%v: unexpected result %v expected %v", c[0:4], got, want)
		}
	}
}

var xxhash64Uint32Cases = []XXHash64Uint32Test{
	{
		name:   "l0",
		slice:  make([]uint32, 0),
		result: []uint64{},
	}, {
		name:   "nil",
		slice:  nil,
		result: []uint64{},
	},
	CreateXXHash64Uint32TestCase("l3", xxhashInput, xxhash64Uint32Result, 3),
	CreateXXHash64Uint32TestCase("l4", xxhashInput, xxhash64Uint32Result, 4),
	CreateXXHash64Uint32TestCase("l7", xxhashInput, xxhash64Uint32Result, 7),
	CreateXXHash64Uint32TestCase("l8", xxhashInput, xxhash64Uint32Result, 8),
	CreateXXHash64Uint32TestCase("l15", xxhashInput, xxhash64Uint32Result, 15),
	CreateXXHash64Uint32TestCase("l16", xxhashInput, xxhash64Uint32Result, 16),
}

func TestXXHash64Uint32SliceGeneric(t *testing.T) {
	for _, c := range xxhash64Uint32Cases {
		// pre-allocate the result slice
		res := make([]uint64, len(c.slice))
		xxhash64Uint32SliceGeneric(c.slice, res)
		if got, want := len(res), len(c.result); got != want {
			t.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if !reflect.DeepEqual(res, c.result) {
			t.Errorf("%s: unexpected result %d, expected %d", c.name, res, c.result)
		}
	}
}

func BenchmarkXXHash64Uint32SliceGeneric(b *testing.B) {
	for _, n := range hashBenchmarkSizes {
		a := util.RandUints[uint32](n.l)
		res := make([]uint64, n.l)
		b.Run(n.name, func(b *testing.B) {
			b.SetBytes(4 * int64(n.l))
			for i := 0; i < b.N; i++ {
				xxhash64Uint32SliceGeneric(a, res)
			}
		})
	}
}

/*************** xxhash64Uint64 *******************************************************/

func TestXXHash64Uint64(t *testing.T) {
	for i, c := range xxhashInput {
		if got, want := XXHash64Uint64(binary.LittleEndian.Uint64(c[0:8])), xxhash64Uint64Result[i]; got != want {
			t.Errorf("%v: unexpected result %v expected %v", c[0:8], got, want)
		}
	}
}

var xxhash64Uint64Cases = []XXHash64Uint64Test{
	{
		name:   "l0",
		slice:  make([]uint64, 0),
		result: []uint64{},
	}, {
		name:   "nil",
		slice:  nil,
		result: []uint64{},
	},
	CreateXXHash64Uint64TestCase("l3", xxhashInput, xxhash64Uint64Result, 3),
	CreateXXHash64Uint64TestCase("l4", xxhashInput, xxhash64Uint64Result, 4),
	CreateXXHash64Uint64TestCase("l7", xxhashInput, xxhash64Uint64Result, 7),
	CreateXXHash64Uint64TestCase("l8", xxhashInput, xxhash64Uint64Result, 8),
	CreateXXHash64Uint64TestCase("l15", xxhashInput, xxhash64Uint64Result, 15),
	CreateXXHash64Uint64TestCase("l16", xxhashInput, xxhash64Uint64Result, 16),
}

func TestXXHash64Uint64SliceGeneric(t *testing.T) {
	for _, c := range xxhash64Uint64Cases {
		// pre-allocate the result slice
		res := make([]uint64, len(c.slice))
		xxhash64Uint64SliceGeneric(c.slice, res)
		if got, want := len(res), len(c.result); got != want {
			t.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if !reflect.DeepEqual(res, c.result) {
			t.Errorf("%s: unexpected result %d, expected %d", c.name, res, c.result)
		}
	}
}

func BenchmarkXXHash64Uint64SliceGeneric(b *testing.B) {
	for _, n := range hashBenchmarkSizes {
		a := util.RandUints[uint64](n.l)
		res := make([]uint64, n.l)
		b.Run(n.name, func(b *testing.B) {
			b.SetBytes(8 * int64(n.l))
			for i := 0; i < b.N; i++ {
				xxhash64Uint64SliceGeneric(a, res)
			}
		})
	}
}

/*************** xxh3Uint32 *******************************************************/

func TestXXH3Uint32(t *testing.T) {
	for i, c := range xxhashInput {
		if got, want := XXH3Uint32(binary.LittleEndian.Uint32(c[0:4])), xxh3Uint32Result[i]; got != want {
			t.Errorf("%v: unexpected result %v expected %v", c[0:4], got, want)
		}
	}
}

var xxh3Uint32Cases = []XXHash64Uint32Test{
	{
		name:   "l0",
		slice:  make([]uint32, 0),
		result: []uint64{},
	}, {
		name:   "nil",
		slice:  nil,
		result: []uint64{},
	},
	CreateXXHash64Uint32TestCase("l3", xxhashInput, xxh3Uint32Result, 3),
	CreateXXHash64Uint32TestCase("l4", xxhashInput, xxh3Uint32Result, 4),
	CreateXXHash64Uint32TestCase("l7", xxhashInput, xxh3Uint32Result, 7),
	CreateXXHash64Uint32TestCase("l8", xxhashInput, xxh3Uint32Result, 8),
	CreateXXHash64Uint32TestCase("l15", xxhashInput, xxh3Uint32Result, 15),
	CreateXXHash64Uint32TestCase("l16", xxhashInput, xxh3Uint32Result, 16),
}

func TestXXH3Uint32SliceGeneric(t *testing.T) {
	for _, c := range xxh3Uint32Cases {
		// pre-allocate the result slice
		res := make([]uint64, len(c.slice))
		xxh3Uint32SliceGeneric(c.slice, res)
		if got, want := len(res), len(c.result); got != want {
			t.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if !reflect.DeepEqual(res, c.result) {
			t.Errorf("%s: unexpected result %d, expected %d", c.name, res, c.result)
		}
	}
}

func BenchmarkXXH3Uint32SliceGeneric(b *testing.B) {
	for _, n := range hashBenchmarkSizes {
		a := util.RandUints[uint32](n.l)
		res := make([]uint64, n.l)
		b.Run(n.name, func(b *testing.B) {
			b.SetBytes(4 * int64(n.l))
			for i := 0; i < b.N; i++ {
				xxh3Uint32SliceGeneric(a, res)
			}
		})
	}
}

/*************** xxh3Uint64 *******************************************************/

func TestXXH3Uint64(t *testing.T) {
	for i, c := range xxhashInput {
		if got, want := XXH3Uint64(binary.LittleEndian.Uint64(c[0:8])), xxh3Uint64Result[i]; got != want {
			t.Errorf("%v: unexpected result %v expected %v", c[0:8], got, want)
		}
	}
}

var xxh3Uint64Cases = []XXHash64Uint64Test{
	{
		name:   "l0",
		slice:  make([]uint64, 0),
		result: []uint64{},
	}, {
		name:   "nil",
		slice:  nil,
		result: []uint64{},
	},
	CreateXXHash64Uint64TestCase("l3", xxhashInput, xxh3Uint64Result, 3),
	CreateXXHash64Uint64TestCase("l4", xxhashInput, xxh3Uint64Result, 4),
	CreateXXHash64Uint64TestCase("l7", xxhashInput, xxh3Uint64Result, 7),
	CreateXXHash64Uint64TestCase("l8", xxhashInput, xxh3Uint64Result, 8),
	CreateXXHash64Uint64TestCase("l15", xxhashInput, xxh3Uint64Result, 15),
	CreateXXHash64Uint64TestCase("l16", xxhashInput, xxh3Uint64Result, 16),
	CreateXXHash64Uint64TestCase("l31", xxhashInput, xxh3Uint64Result, 31),
	CreateXXHash64Uint64TestCase("l32", xxhashInput, xxh3Uint64Result, 32),
}

func TestXXH3Uint64SliceGeneric(t *testing.T) {
	for _, c := range xxh3Uint64Cases {
		// pre-allocate the result slice
		res := make([]uint64, len(c.slice))
		xxh3Uint64SliceGeneric(c.slice, res)
		if got, want := len(res), len(c.result); got != want {
			t.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if !reflect.DeepEqual(res, c.result) {
			t.Errorf("%s: unexpected result %d, expected %d", c.name, res, c.result)
		}
	}
}

func BenchmarkXXH3Uint64SliceGeneric(b *testing.B) {
	for _, n := range hashBenchmarkSizes {
		a := util.RandUints[uint64](n.l)
		res := make([]uint64, n.l)
		b.Run(n.name, func(b *testing.B) {
			b.SetBytes(8 * int64(n.l))
			for i := 0; i < b.N; i++ {
				xxh3Uint64SliceGeneric(a, res)
			}
		})
	}
}
