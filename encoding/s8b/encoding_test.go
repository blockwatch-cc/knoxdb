// from: github.com/jwilder/encoding
package s8b

import (
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func Test_Encode_NoValues(t *testing.T) {
	var in []uint64
	encoded, _ := EncodeAll(in)

	decoded := make([]uint64, len(in))
	n, _ := DecodeAll(decoded, encoded)

	if len(in) != len(decoded[:n]) {
		t.Fatalf("Len mismatch: got %v, exp %v", len(decoded), len(in))
	}
}

func ones(n int) func() []uint64 {
	return func() []uint64 {
		in := make([]uint64, n)
		for i := 0; i < n; i++ {
			in[i] = 1
		}
		return in
	}
}

func ones32(n int) func() []uint32 {
	return func() []uint32 {
		in := make([]uint32, n)
		for i := 0; i < n; i++ {
			in[i] = 1
		}
		return in
	}
}

func ones16(n int) func() []uint16 {
	return func() []uint16 {
		in := make([]uint16, n)
		for i := 0; i < n; i++ {
			in[i] = 1
		}
		return in
	}
}

func ones8(n int) func() []uint8 {
	return func() []uint8 {
		in := make([]uint8, n)
		for i := 0; i < n; i++ {
			in[i] = 1
		}
		return in
	}
}

func onesN() func(n int) func() []uint64 {
	return func(n int) func() []uint64 {
		return ones(n)
	}
}

func bitsN(b int) func(n int) func() []uint64 {
	return func(n int) func() []uint64 {
		return bits(n, b)
	}
}

func bitsN32(b int) func(n int) func() []uint32 {
	return func(n int) func() []uint32 {
		return bits32(n, b)
	}
}

func bitsN16(b int) func(n int) func() []uint16 {
	return func(n int) func() []uint16 {
		return bits16(n, b)
	}
}

func bitsN8(b int) func(n int) func() []uint8 {
	return func(n int) func() []uint8 {
		return bits8(n, b)
	}
}

func combineN(fns ...func(n int) func() []uint64) func(n int) func() []uint64 {
	return func(n int) func() []uint64 {
		var out []func() []uint64
		for _, fn := range fns {
			out = append(out, fn(n))
		}
		return combine(out...)
	}
}

func combineN32(fns ...func(n int) func() []uint32) func(n int) func() []uint32 {
	return func(n int) func() []uint32 {
		var out []func() []uint32
		for _, fn := range fns {
			out = append(out, fn(n))
		}
		return combine32(out...)
	}
}

func combineN16(fns ...func(n int) func() []uint16) func(n int) func() []uint16 {
	return func(n int) func() []uint16 {
		var out []func() []uint16
		for _, fn := range fns {
			out = append(out, fn(n))
		}
		return combine16(out...)
	}
}

func combineN8(fns ...func(n int) func() []uint8) func(n int) func() []uint8 {
	return func(n int) func() []uint8 {
		var out []func() []uint8
		for _, fn := range fns {
			out = append(out, fn(n))
		}
		return combine8(out...)
	}
}

// bits generates sequence of n numbers with max bits,
// ensuring max bit is set for 50% of the values.
func bits(n, bits int) func() []uint64 {
	return func() []uint64 {
		out := make([]uint64, n)
		maxVal := uint64(1 << uint8(bits))
		for i := range out {
			topBit := uint64((i & 1) << uint8(bits-1))
			out[i] = uint64(rand.Int63n(int64(maxVal))) | topBit
			if out[i] >= maxVal {
				panic("max")
			}
		}
		return out
	}
}

func bits32(n, bits int) func() []uint32 {
	return func() []uint32 {
		out := make([]uint32, n)
		maxVal := uint64(1 << uint8(bits))
		for i := range out {
			topBit := uint32((i & 1) << uint8(bits-1))
			out[i] = uint32(rand.Int63n(int64(maxVal))) | topBit
			if uint64(out[i]) >= maxVal {
				panic("max")
			}
		}
		return out
	}
}

func bits16(n, bits int) func() []uint16 {
	return func() []uint16 {
		out := make([]uint16, n)
		maxVal := uint64(1 << uint8(bits))
		for i := range out {
			topBit := uint16((i & 1) << uint8(bits-1))
			out[i] = uint16(rand.Int63n(int64(maxVal))) | topBit
			if uint64(out[i]) >= maxVal {
				panic("max")
			}
		}
		return out
	}
}

func bits8(n, bits int) func() []uint8 {
	return func() []uint8 {
		out := make([]uint8, n)
		maxVal := uint64(1 << uint8(bits))
		for i := range out {
			topBit := uint8((i & 1) << uint8(bits-1))
			out[i] = uint8(rand.Int63n(int64(maxVal))) | topBit
			if uint64(out[i]) >= maxVal {
				panic("max")
			}
		}
		return out
	}
}

func combine(fns ...func() []uint64) func() []uint64 {
	return func() []uint64 {
		var out []uint64
		for _, fn := range fns {
			out = append(out, fn()...)
		}
		return out
	}
}

func combine32(fns ...func() []uint32) func() []uint32 {
	return func() []uint32 {
		var out []uint32
		for _, fn := range fns {
			out = append(out, fn()...)
		}
		return out
	}
}

func combine16(fns ...func() []uint16) func() []uint16 {
	return func() []uint16 {
		var out []uint16
		for _, fn := range fns {
			out = append(out, fn()...)
		}
		return out
	}
}

func combine8(fns ...func() []uint8) func() []uint8 {
	return func() []uint8 {
		var out []uint8
		for _, fn := range fns {
			out = append(out, fn()...)
		}
		return out
	}
}

var s8bTestsUint64 = []struct {
	name string
	in   []uint64
	fn   func() []uint64
	err  error
}{
	{name: "no values", in: []uint64{}},
	{name: "mixed sizes", in: []uint64{7, 6, 256, 4, 3, 2, 1}},
	{name: "too big", in: []uint64{7, 6, 2<<61 - 1, 4, 3, 2, 1}, err: ErrValueOutOfBounds},
	{name: "1 bit", fn: bits(120, 1)},
	{name: "2 bits", fn: bits(120, 2)},
	{name: "3 bits", fn: bits(120, 3)},
	{name: "4 bits", fn: bits(120, 4)},
	{name: "5 bits", fn: bits(120, 5)},
	{name: "6 bits", fn: bits(120, 6)},
	{name: "7 bits", fn: bits(120, 7)},
	{name: "8 bits", fn: bits(120, 8)},
	{name: "10 bits", fn: bits(120, 10)},
	{name: "12 bits", fn: bits(120, 12)},
	{name: "15 bits", fn: bits(120, 15)},
	{name: "20 bits", fn: bits(120, 20)},
	{name: "30 bits", fn: bits(120, 30)},
	{name: "60 bits", fn: bits(120, 60)},
	{name: "combination", fn: combine(
		bits(120, 1),
		bits(120, 2),
		bits(120, 3),
		bits(120, 4),
		bits(120, 5),
		bits(120, 6),
		bits(120, 7),
		bits(120, 8),
		bits(120, 10),
		bits(120, 12),
		bits(120, 15),
		bits(120, 20),
		bits(120, 30),
		bits(120, 60),
	)},
	{name: "240 ones", fn: ones(240)},
	{name: "120 ones", fn: func() []uint64 {
		in := ones(240)()
		in[120] = 5
		return in
	}},
	{name: "119 ones", fn: func() []uint64 {
		in := ones(240)()
		in[119] = 5
		return in
	}},
	{name: "239 ones", fn: func() []uint64 {
		in := ones(241)()
		in[239] = 5
		return in
	}},
}

var s8bTestsUint32 = []struct {
	name string
	in   []uint32
	fn   func() []uint32
	err  error
}{
	{name: "no values", in: []uint32{}},
	{name: "mixed sizes", in: []uint32{7, 6, 256, 4, 3, 2, 1}},
	{name: "1 bit", fn: bits32(120, 1)},
	{name: "2 bits", fn: bits32(120, 2)},
	{name: "3 bits", fn: bits32(120, 3)},
	{name: "4 bits", fn: bits32(120, 4)},
	{name: "5 bits", fn: bits32(120, 5)},
	{name: "6 bits", fn: bits32(120, 6)},
	{name: "7 bits", fn: bits32(120, 7)},
	{name: "8 bits", fn: bits32(120, 8)},
	{name: "10 bits", fn: bits32(120, 10)},
	{name: "12 bits", fn: bits32(120, 12)},
	{name: "15 bits", fn: bits32(120, 15)},
	{name: "20 bits", fn: bits32(120, 20)},
	{name: "30 bits", fn: bits32(120, 30)},
	{name: "60 bits", fn: bits32(120, 32)},
	{name: "combination", fn: combine32(
		bits32(120, 1),
		bits32(120, 2),
		bits32(120, 3),
		bits32(120, 4),
		bits32(120, 5),
		bits32(120, 6),
		bits32(120, 7),
		bits32(120, 8),
		bits32(120, 10),
		bits32(120, 12),
		bits32(120, 15),
		bits32(120, 20),
		bits32(120, 30),
		bits32(120, 32),
	)},
	{name: "240 ones", fn: ones32(240)},
	{name: "120 ones", fn: func() []uint32 {
		in := ones32(240)()
		in[120] = 5
		return in
	}},
	{name: "119 ones", fn: func() []uint32 {
		in := ones32(240)()
		in[119] = 5
		return in
	}},
	{name: "239 ones", fn: func() []uint32 {
		in := ones32(241)()
		in[239] = 5
		return in
	}},
}

var s8bTestsUint16 = []struct {
	name string
	in   []uint16
	fn   func() []uint16
	err  error
}{
	{name: "no values", in: []uint16{}},
	{name: "mixed sizes", in: []uint16{7, 6, 256, 4, 3, 2, 1}},
	{name: "1 bit", fn: bits16(120, 1)},
	{name: "2 bits", fn: bits16(120, 2)},
	{name: "3 bits", fn: bits16(120, 3)},
	{name: "4 bits", fn: bits16(120, 4)},
	{name: "5 bits", fn: bits16(120, 5)},
	{name: "6 bits", fn: bits16(120, 6)},
	{name: "7 bits", fn: bits16(120, 7)},
	{name: "8 bits", fn: bits16(120, 8)},
	{name: "10 bits", fn: bits16(120, 10)},
	{name: "12 bits", fn: bits16(120, 12)},
	{name: "15 bits", fn: bits16(120, 15)},
	{name: "20 bits", fn: bits16(120, 16)},
	{name: "30 bits", fn: bits16(2, 16)},
	{name: "60 bits", fn: bits16(1, 16)},
	{name: "combination", fn: combine16(
		bits16(120, 1),
		bits16(120, 2),
		bits16(120, 3),
		bits16(120, 4),
		bits16(120, 5),
		bits16(120, 6),
		bits16(120, 7),
		bits16(120, 8),
		bits16(120, 10),
		bits16(120, 12),
		bits16(120, 15),
		bits16(120, 16),
	)},
	{name: "240 ones", fn: ones16(240)},
	{name: "120 ones", fn: func() []uint16 {
		in := ones16(240)()
		in[120] = 5
		return in
	}},
	{name: "119 ones", fn: func() []uint16 {
		in := ones16(240)()
		in[119] = 5
		return in
	}},
	{name: "239 ones", fn: func() []uint16 {
		in := ones16(241)()
		in[239] = 5
		return in
	}},
}

var s8bTestsUint8 = []struct {
	name string
	in   []uint8
	fn   func() []uint8
	err  error
}{
	{name: "no values", in: []uint8{}},
	{name: "mixed sizes", in: []uint8{7, 6, 255, 4, 3, 2, 1}},
	{name: "1 bit", fn: bits8(120, 1)},
	{name: "2 bits", fn: bits8(120, 2)},
	{name: "3 bits", fn: bits8(120, 3)},
	{name: "4 bits", fn: bits8(120, 4)},
	{name: "5 bits", fn: bits8(120, 5)},
	{name: "6 bits", fn: bits8(120, 6)},
	{name: "7 bits", fn: bits8(120, 7)},
	{name: "8 bits", fn: bits8(120, 8)},
	{name: "10 bits", fn: bits8(6, 8)},
	{name: "12 bits", fn: bits8(5, 8)},
	{name: "15 bits", fn: bits8(4, 8)},
	{name: "20 bits", fn: bits8(3, 8)},
	{name: "30 bits", fn: bits8(2, 8)},
	{name: "60 bits", fn: bits8(1, 8)},
	{name: "combination", fn: combine8(
		bits8(120, 1),
		bits8(120, 2),
		bits8(120, 3),
		bits8(120, 4),
		bits8(120, 5),
		bits8(120, 6),
		bits8(120, 7),
		bits8(120, 8),
	)},
	{name: "240 ones", fn: ones8(240)},
	{name: "120 ones", fn: func() []uint8 {
		in := ones8(240)()
		in[120] = 5
		return in
	}},
	{name: "119 ones", fn: func() []uint8 {
		in := ones8(240)()
		in[119] = 5
		return in
	}},
	{name: "239 ones", fn: func() []uint8 {
		in := ones8(241)()
		in[239] = 5
		return in
	}},
}

// TestEncodeAll ensures 100% test coverage of simple8b.EncodeAll and
// verifies all output by comparing the original input with the output of simple8b.DecodeAll
func TestEncodeAll(t *testing.T) {
	rand.Seed(0)

	for _, test := range s8bTestsUint64 {
		t.Run(test.name, func(t *testing.T) {
			if test.fn != nil {
				test.in = test.fn()
			}

			encoded, err := EncodeAll(append(make([]uint64, 0, len(test.in)), test.in...))
			if test.err != nil {
				if err != test.err {
					t.Fatalf("expected encode error, got\n%s", err)
				}
				return
			}

			decoded := make([]uint64, len(test.in))
			n, err := DecodeAll(decoded, encoded)
			if err != nil {
				t.Fatalf("unexpected decode error\n%s", err)
			}

			if !cmp.Equal(decoded[:n], test.in) {
				t.Fatalf("unexpected values; +got/-exp\n%s", cmp.Diff(decoded, test.in))
			}
		})
	}
}

// TestEncodeAll ensures 100% test coverage of EncodeAll and
// verifies all output by comparing the original input with the output of DecodeAll
func TestEncodeAllUint64Generic(t *testing.T) {
	rand.Seed(0)

	for _, test := range s8bTestsUint64 {
		t.Run(test.name, func(t *testing.T) {
			if test.fn != nil {
				test.in = test.fn()
			}

			encoded, err := EncodeAll(append(make([]uint64, 0, len(test.in)), test.in...))
			if test.err != nil {
				if err != test.err {
					t.Fatalf("expected encode error, got\n%s", err)
				}
				return
			}

			buf := make([]byte, 8*len(encoded))
			b := buf
			for _, v := range encoded {
				binary.LittleEndian.PutUint64(b, v)
				b = b[8:]
			}
			count, err := countValuesGeneric(buf)
			if err != nil {
				t.Fatalf("unexpected count error\n%s", err)
			}

			if count != len(test.in) {
				t.Fatalf("unexpected count: got %d expected %d", count, len(test.in))
			}

			decoded := make([]uint64, len(test.in))
			n, err := decodeAllUint64Generic(decoded, buf)
			if err != nil {
				t.Fatalf("unexpected decode error\n%s", err)
			}

			if !cmp.Equal(decoded[:n], test.in) {
				t.Fatalf("unexpected values; +got/-exp\n%s", cmp.Diff(decoded, test.in))
			}
		})
	}
}

func TestEncodeAllUint32Generic(t *testing.T) {
	rand.Seed(0)

	for _, test := range s8bTestsUint32 {
		t.Run(test.name, func(t *testing.T) {
			if test.fn != nil {
				test.in = test.fn()
			}

			tmp := make([]uint64, len(test.in))
			for i := 0; i < len(tmp); i++ {
				tmp[i] = uint64(test.in[i])
			}
			encoded, err := EncodeAll(append(make([]uint64, 0, len(test.in)), tmp...))
			if test.err != nil {
				if err != test.err {
					t.Fatalf("expected encode error, got\n%s", err)
				}
				return
			}

			buf := make([]byte, 8*len(encoded))
			b := buf
			for _, v := range encoded {
				binary.LittleEndian.PutUint64(b, v)
				b = b[8:]
			}
			count, err := countValuesGeneric(buf)
			if err != nil {
				t.Fatalf("unexpected count error\n%s", err)
			}

			if count != len(test.in) {
				t.Fatalf("unexpected count: got %d expected %d", count, len(test.in))
			}

			decoded := make([]uint32, len(test.in))
			n, err := decodeAllUint32Generic(decoded, buf)
			if err != nil {
				t.Fatalf("unexpected decode error\n%s", err)
			}

			if !cmp.Equal(decoded[:n], test.in) {
				t.Fatalf("unexpected values; +got/-exp\n%s", cmp.Diff(decoded, test.in))
			}
		})
	}
}

func TestEncodeAllUint16Generic(t *testing.T) {
	rand.Seed(0)

	for _, test := range s8bTestsUint16 {
		t.Run(test.name, func(t *testing.T) {
			if test.fn != nil {
				test.in = test.fn()
			}

			tmp := make([]uint64, len(test.in))
			for i := 0; i < len(tmp); i++ {
				tmp[i] = uint64(test.in[i])
			}
			encoded, err := EncodeAll(append(make([]uint64, 0, len(test.in)), tmp...))
			if test.err != nil {
				if err != test.err {
					t.Fatalf("expected encode error, got\n%s", err)
				}
				return
			}

			buf := make([]byte, 8*len(encoded))
			b := buf
			for _, v := range encoded {
				binary.LittleEndian.PutUint64(b, v)
				b = b[8:]
			}
			count, err := countValuesGeneric(buf)
			if err != nil {
				t.Fatalf("unexpected count error\n%s", err)
			}

			if count != len(test.in) {
				t.Fatalf("unexpected count: got %d expected %d", count, len(test.in))
			}

			decoded := make([]uint16, len(test.in))
			n, err := decodeAllUint16Generic(decoded, buf)
			if err != nil {
				t.Fatalf("unexpected decode error\n%s", err)
			}

			if !cmp.Equal(decoded[:n], test.in) {
				t.Fatalf("unexpected values; +got/-exp\n%s", cmp.Diff(decoded, test.in))
			}
		})
	}
}

func TestEncodeAllUint8Generic(t *testing.T) {
	rand.Seed(0)

	for _, test := range s8bTestsUint8 {
		t.Run(test.name, func(t *testing.T) {
			if test.fn != nil {
				test.in = test.fn()
			}

			tmp := make([]uint64, len(test.in))
			for i := 0; i < len(tmp); i++ {
				tmp[i] = uint64(test.in[i])
			}
			encoded, err := EncodeAll(append(make([]uint64, 0, len(test.in)), tmp...))
			if test.err != nil {
				if err != test.err {
					t.Fatalf("expected encode error, got\n%s", err)
				}
				return
			}

			buf := make([]byte, 8*len(encoded))
			b := buf
			for _, v := range encoded {
				binary.LittleEndian.PutUint64(b, v)
				b = b[8:]
			}
			count, err := countValuesGeneric(buf)
			if err != nil {
				t.Fatalf("unexpected count error\n%s", err)
			}

			if count != len(test.in) {
				t.Fatalf("unexpected count: got %d expected %d", count, len(test.in))
			}

			decoded := make([]uint8, len(test.in))
			n, err := decodeAllUint8Generic(decoded, buf)
			if err != nil {
				t.Fatalf("unexpected decode error\n%s", err)
			}

			if !cmp.Equal(decoded[:n], test.in) {
				t.Fatalf("unexpected values; +got/-exp\n%s", cmp.Diff(decoded, test.in))
			}
		})
	}
}

func Test_FewValues(t *testing.T) {
	testEncode(t, 20, 2)
}

func Test_Encode_Multiple_Zeros(t *testing.T) {
	testEncode(t, 250, 0)
}

func Test_Encode_Multiple_Ones(t *testing.T) {
	testEncode(t, 250, 1)
}

func Test_Encode_Multiple_Large(t *testing.T) {
	testEncode(t, 250, 134)
}

func Test_Encode_240Ones(t *testing.T) {
	testEncode(t, 240, 1)
}

func Test_Encode_120Ones(t *testing.T) {
	testEncode(t, 120, 1)
}

func Test_Encode_60(t *testing.T) {
	testEncode(t, 60, 1)
}

func Test_Encode_30(t *testing.T) {
	testEncode(t, 30, 3)
}

func Test_Encode_20(t *testing.T) {
	testEncode(t, 20, 7)
}

func Test_Encode_15(t *testing.T) {
	testEncode(t, 15, 15)
}

func Test_Encode_12(t *testing.T) {
	testEncode(t, 12, 31)
}

func Test_Encode_10(t *testing.T) {
	testEncode(t, 10, 63)
}

func Test_Encode_8(t *testing.T) {
	testEncode(t, 8, 127)
}

func Test_Encode_7(t *testing.T) {
	testEncode(t, 7, 255)
}

func Test_Encode_6(t *testing.T) {
	testEncode(t, 6, 1023)
}

func Test_Encode_5(t *testing.T) {
	testEncode(t, 5, 4095)
}

func Test_Encode_4(t *testing.T) {
	testEncode(t, 4, 32767)
}

func Test_Encode_3(t *testing.T) {
	testEncode(t, 3, 1048575)
}

func Test_Encode_2(t *testing.T) {
	testEncode(t, 2, 1073741823)
}

func Test_Encode_1(t *testing.T) {
	testEncode(t, 1, 1152921504606846975)
}

func testEncode(t *testing.T, n int, val uint64) {
	enc := NewEncoder()
	in := make([]uint64, n)
	for i := 0; i < n; i++ {
		in[i] = val
		enc.Write(in[i])
	}

	encoded, err := enc.Bytes()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	dec := NewDecoder(encoded)
	i := 0
	for dec.Next() {
		if i >= len(in) {
			t.Fatalf("Decoded too many values: got %v, exp %v", i, len(in))
		}

		if dec.Read() != in[i] {
			t.Fatalf("Decoded[%d] != %v, got %v", i, in[i], dec.Read())
		}
		i += 1
	}

	if exp, got := n, i; got != exp {
		t.Fatalf("Decode len mismatch: exp %v, got %v", exp, got)
	}

	got, err := CountValuesBigEndian(encoded)
	if err != nil {
		t.Fatalf("Unexpected error in Count: %v", err)
	}
	if got != n {
		t.Fatalf("Count mismatch: got %v, exp %v", got, n)
	}

}

func Test_Bytes(t *testing.T) {
	enc := NewEncoder()
	for i := 0; i < 30; i++ {
		enc.Write(uint64(i))
	}
	b, _ := enc.Bytes()

	dec := NewDecoder(b)
	x := uint64(0)
	for dec.Next() {
		if x != dec.Read() {
			t.Fatalf("mismatch: got %v, exp %v", dec.Read(), x)
		}
		x += 1
	}
}

func Test_Encode_ValueTooLarge(t *testing.T) {
	enc := NewEncoder()

	values := []uint64{
		1442369134000000000, 0,
	}

	for _, v := range values {
		enc.Write(v)
	}

	_, err := enc.Bytes()
	if err == nil {
		t.Fatalf("Expected error, got nil")

	}
}

func Test_Decode_NotEnoughBytes(t *testing.T) {
	dec := NewDecoder([]byte{0})
	if dec.Next() {
		t.Fatalf("Expected Next to return false but it returned true")
	}
}

func TestCountBytesBetween(t *testing.T) {
	enc := NewEncoder()
	in := make([]uint64, 8)
	for i := 0; i < len(in); i++ {
		in[i] = uint64(i)
		enc.Write(in[i])
	}

	encoded, err := enc.Bytes()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	dec := NewDecoder(encoded)
	i := 0
	for dec.Next() {
		if i >= len(in) {
			t.Fatalf("Decoded too many values: got %v, exp %v", i, len(in))
		}

		if dec.Read() != in[i] {
			t.Fatalf("Decoded[%d] != %v, got %v", i, in[i], dec.Read())
		}
		i += 1
	}

	if exp, got := len(in), i; got != exp {
		t.Fatalf("Decode len mismatch: exp %v, got %v", exp, got)
	}

	got, err := CountBytesBetween(encoded, 2, 6)
	if err != nil {
		t.Fatalf("Unexpected error in Count: %v", err)
	}
	if got != 4 {
		t.Fatalf("Count mismatch: got %v, exp %v", got, 4)
	}
}

func TestCountBytesBetween_SkipMin(t *testing.T) {
	enc := NewEncoder()
	in := make([]uint64, 8)
	for i := 0; i < len(in); i++ {
		in[i] = uint64(i)
		enc.Write(in[i])
	}
	in = append(in, 100000)
	enc.Write(100000)

	encoded, err := enc.Bytes()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	dec := NewDecoder(encoded)
	i := 0
	for dec.Next() {
		if i >= len(in) {
			t.Fatalf("Decoded too many values: got %v, exp %v", i, len(in))
		}

		if dec.Read() != in[i] {
			t.Fatalf("Decoded[%d] != %v, got %v", i, in[i], dec.Read())
		}
		i += 1
	}

	if exp, got := len(in), i; got != exp {
		t.Fatalf("Decode len mismatch: exp %v, got %v", exp, got)
	}

	got, err := CountBytesBetween(encoded, 100000, 100001)
	if err != nil {
		t.Fatalf("Unexpected error in Count: %v", err)
	}
	if got != 1 {
		t.Fatalf("Count mismatch: got %v, exp %v", got, 1)
	}
}

var s8bBenchmarkSize = 6000

var s8bBenchmarksUint64 = []struct {
	name string
	fn   func(n int) func() []uint64
	size int
}{
	{name: "0 bit", fn: onesN(), size: s8bBenchmarkSize},
	{name: "1 bit", fn: bitsN(1), size: s8bBenchmarkSize},
	{name: "2 bits", fn: bitsN(2), size: s8bBenchmarkSize},
	{name: "3 bits", fn: bitsN(3), size: s8bBenchmarkSize},
	{name: "4 bits", fn: bitsN(4), size: s8bBenchmarkSize},
	{name: "5 bits", fn: bitsN(5), size: s8bBenchmarkSize},
	{name: "6 bits", fn: bitsN(6), size: s8bBenchmarkSize},
	{name: "7 bits", fn: bitsN(7), size: s8bBenchmarkSize},
	{name: "8 bits", fn: bitsN(8), size: s8bBenchmarkSize},
	{name: "10 bits", fn: bitsN(10), size: s8bBenchmarkSize},
	{name: "12 bits", fn: bitsN(12), size: s8bBenchmarkSize},
	{name: "15 bits", fn: bitsN(15), size: s8bBenchmarkSize},
	{name: "20 bits", fn: bitsN(20), size: s8bBenchmarkSize},
	{name: "30 bits", fn: bitsN(30), size: s8bBenchmarkSize},
	{name: "60 bits", fn: bitsN(60), size: s8bBenchmarkSize},
	{name: "combination", fn: combineN(
		onesN(),
		bitsN(1),
		bitsN(2),
		bitsN(3),
		bitsN(4),
		bitsN(5),
		bitsN(6),
		bitsN(7),
		bitsN(8),
		bitsN(10),
		bitsN(12),
		bitsN(15),
		bitsN(20),
		bitsN(30),
		bitsN(60),
	), size: 15 * s8bBenchmarkSize},
}

var s8bBenchmarksUint32 = []struct {
	name string
	fn   func(n int) func() []uint64
	size int
}{
	{name: "0 bit", fn: onesN(), size: s8bBenchmarkSize},
	{name: "1 bit", fn: bitsN(1), size: s8bBenchmarkSize},
	{name: "2 bits", fn: bitsN(2), size: s8bBenchmarkSize},
	{name: "3 bits", fn: bitsN(3), size: s8bBenchmarkSize},
	{name: "4 bits", fn: bitsN(4), size: s8bBenchmarkSize},
	{name: "5 bits", fn: bitsN(5), size: s8bBenchmarkSize},
	{name: "6 bits", fn: bitsN(6), size: s8bBenchmarkSize},
	{name: "7 bits", fn: bitsN(7), size: s8bBenchmarkSize},
	{name: "8 bits", fn: bitsN(8), size: s8bBenchmarkSize},
	{name: "10 bits", fn: bitsN(10), size: s8bBenchmarkSize},
	{name: "12 bits", fn: bitsN(12), size: s8bBenchmarkSize},
	{name: "15 bits", fn: bitsN(15), size: s8bBenchmarkSize},
	{name: "20 bits", fn: bitsN(20), size: s8bBenchmarkSize},
	{name: "30 bits", fn: bitsN(30), size: s8bBenchmarkSize},
	{name: "60 bits", fn: bitsN(32), size: s8bBenchmarkSize},
	{name: "combination", fn: combineN(
		onesN(),
		bitsN(1),
		bitsN(2),
		bitsN(3),
		bitsN(4),
		bitsN(5),
		bitsN(6),
		bitsN(7),
		bitsN(8),
		bitsN(10),
		bitsN(12),
		bitsN(15),
		bitsN(20),
		bitsN(30),
		bitsN(32),
	), size: 15 * s8bBenchmarkSize},
}

var s8bBenchmarksUint16 = []struct {
	name string
	fn   func(n int) func() []uint64
	size int
}{
	{name: "0 bit", fn: onesN(), size: s8bBenchmarkSize},
	{name: "1 bit", fn: bitsN(1), size: s8bBenchmarkSize},
	{name: "2 bits", fn: bitsN(2), size: s8bBenchmarkSize},
	{name: "3 bits", fn: bitsN(3), size: s8bBenchmarkSize},
	{name: "4 bits", fn: bitsN(4), size: s8bBenchmarkSize},
	{name: "5 bits", fn: bitsN(5), size: s8bBenchmarkSize},
	{name: "6 bits", fn: bitsN(6), size: s8bBenchmarkSize},
	{name: "7 bits", fn: bitsN(7), size: s8bBenchmarkSize},
	{name: "8 bits", fn: bitsN(8), size: s8bBenchmarkSize},
	{name: "10 bits", fn: bitsN(10), size: s8bBenchmarkSize},
	{name: "12 bits", fn: bitsN(12), size: s8bBenchmarkSize},
	{name: "15 bits", fn: bitsN(15), size: s8bBenchmarkSize},
	{name: "20 bits", fn: bitsN(16), size: s8bBenchmarkSize},
	{name: "combination", fn: combineN(
		onesN(),
		bitsN(1),
		bitsN(2),
		bitsN(3),
		bitsN(4),
		bitsN(5),
		bitsN(6),
		bitsN(7),
		bitsN(8),
		bitsN(10),
		bitsN(12),
		bitsN(15),
		bitsN(16),
	), size: 15 * s8bBenchmarkSize},
}

var s8bBenchmarksUint8 = []struct {
	name string
	fn   func(n int) func() []uint64
	size int
}{
	{name: "0 bit", fn: onesN(), size: s8bBenchmarkSize},
	{name: "1 bit", fn: bitsN(1), size: s8bBenchmarkSize},
	{name: "2 bits", fn: bitsN(2), size: s8bBenchmarkSize},
	{name: "3 bits", fn: bitsN(3), size: s8bBenchmarkSize},
	{name: "4 bits", fn: bitsN(4), size: s8bBenchmarkSize},
	{name: "5 bits", fn: bitsN(5), size: s8bBenchmarkSize},
	{name: "6 bits", fn: bitsN(6), size: s8bBenchmarkSize},
	{name: "7 bits", fn: bitsN(7), size: s8bBenchmarkSize},
	{name: "8 bits", fn: bitsN(8), size: s8bBenchmarkSize},
	{name: "combination", fn: combineN(
		onesN(),
		bitsN(1),
		bitsN(2),
		bitsN(3),
		bitsN(4),
		bitsN(5),
		bitsN(6),
		bitsN(7),
		bitsN(8),
	), size: 15 * s8bBenchmarkSize},
}

func BenchmarkEncodeAll(b *testing.B) {
	for _, bm := range s8bBenchmarksUint64 {
		in := bm.fn(s8bBenchmarkSize)()
		b.Run(bm.name, func(b *testing.B) {
			b.SetBytes(int64(8 * bm.size))
			for i := 0; i < b.N; i++ {
				EncodeAll(append(make([]uint64, 0, len(in)), in...))
			}
		})
	}
}

func BenchmarkDecodeAllUint64Generic(b *testing.B) {
	for _, bm := range s8bBenchmarksUint64 {
		in := bm.fn(s8bBenchmarkSize)()
		out := make([]uint64, len(in))
		comp, _ := EncodeAll(in)
		buf := make([]byte, 8*len(comp))
		b0 := buf
		for _, v := range comp {
			binary.LittleEndian.PutUint64(b0, v)
			b0 = b0[8:]
		}
		b.Run(bm.name, func(b *testing.B) {
			b.SetBytes(int64(8 * bm.size))
			for i := 0; i < b.N; i++ {
				decodeAllUint64Generic(out, buf)
			}
		})
	}
}

func BenchmarkDecodeAllUint32Generic(b *testing.B) {
	for _, bm := range s8bBenchmarksUint32 {
		in := bm.fn(s8bBenchmarkSize)()
		out := make([]uint32, len(in))
		comp, _ := EncodeAll(in)
		buf := make([]byte, 8*len(comp))
		b0 := buf
		for _, v := range comp {
			binary.LittleEndian.PutUint64(b0, v)
			b0 = b0[8:]
		}
		b.Run(bm.name, func(b *testing.B) {
			b.SetBytes(int64(4 * bm.size))
			for i := 0; i < b.N; i++ {
				decodeAllUint32Generic(out, buf)
			}
		})
	}
}

func BenchmarkDecodeAllUint16Generic(b *testing.B) {
	for _, bm := range s8bBenchmarksUint16 {
		in := bm.fn(s8bBenchmarkSize)()
		out := make([]uint16, len(in))
		comp, _ := EncodeAll(in)
		buf := make([]byte, 8*len(comp))
		b0 := buf
		for _, v := range comp {
			binary.LittleEndian.PutUint64(b0, v)
			b0 = b0[8:]
		}
		b.Run(bm.name, func(b *testing.B) {
			b.SetBytes(int64(2 * bm.size))
			for i := 0; i < b.N; i++ {
				decodeAllUint16Generic(out, buf)
			}
		})
	}
}

func BenchmarkDecodeAllUint8Generic(b *testing.B) {
	for _, bm := range s8bBenchmarksUint8 {
		in := bm.fn(s8bBenchmarkSize)()
		out := make([]uint8, len(in))
		comp, _ := EncodeAll(in)
		buf := make([]byte, 8*len(comp))
		b0 := buf
		for _, v := range comp {
			binary.LittleEndian.PutUint64(b0, v)
			b0 = b0[8:]
		}
		b.Run(bm.name, func(b *testing.B) {
			b.SetBytes(int64(1 * bm.size))
			for i := 0; i < b.N; i++ {
				decodeAllUint8Generic(out, buf)
			}
		})
	}
}

func BenchmarkCountBytesGeneric(b *testing.B) {
	for _, bm := range s8bBenchmarksUint64 {
		in := bm.fn(s8bBenchmarkSize)()
		encoded, _ := EncodeAll(in)

		buf := make([]byte, 8*len(encoded))
		tmp := buf
		for _, v := range encoded {
			binary.BigEndian.PutUint64(tmp, v)
			tmp = tmp[8:]
		}

		b.Run(bm.name, func(b *testing.B) {
			b.SetBytes(int64(8 * bm.size))
			for i := 0; i < b.N; i++ {
				countValuesGeneric(buf)
			}
		})
	}
}

func BenchmarkEncode(b *testing.B) {
	x := make([]uint64, 1024)
	for i := 0; i < len(x); i++ {
		x[i] = uint64(15)
	}

	in := make([]uint64, 1024)

	b.SetBytes(int64(len(x) * 8))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		copy(in, x)
		EncodeAll(in)
	}
}

func BenchmarkEncoder(b *testing.B) {
	x := make([]uint64, 1024)
	for i := 0; i < len(x); i++ {
		x[i] = uint64(15)
	}

	enc := NewEncoder()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		enc.SetValues(x)
		enc.Bytes()
		b.SetBytes(int64(len(x)) * 8)
	}
}

func BenchmarkDecode(b *testing.B) {
	total := 0

	x := make([]uint64, 1024)
	for i := 0; i < len(x); i++ {
		x[i] = uint64(10)
	}
	y, _ := EncodeAll(x)

	decoded := make([]uint64, len(x))

	b.SetBytes(int64(len(decoded) * 8))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = DecodeAll(decoded, y)
		total += len(decoded)
	}
}

func BenchmarkDecoder(b *testing.B) {
	enc := NewEncoder()
	x := make([]uint64, 1024)
	for i := 0; i < len(x); i++ {
		x[i] = uint64(10)
		enc.Write(x[i])
	}
	y, _ := enc.Bytes()

	b.ResetTimer()

	dec := NewDecoder(y)
	for i := 0; i < b.N; i++ {
		dec.SetBytes(y)
		j := 0
		for dec.Next() {
			j += 1
		}
		b.SetBytes(int64(j * 8))
	}
}
