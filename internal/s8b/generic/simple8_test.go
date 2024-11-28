// from: github.com/jwilder/encoding
package generic

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/s8b/tests"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/google/go-cmp/cmp"
)

var (
	s8bTestsUint64      = tests.S8bTestsUint64
	s8bBenchmarksUint64 = tests.S8bBenchmarksUint64
)

func Test_Encode_NoValues(t *testing.T) {
	var in []uint64
	encoded, _ := EncodeUint64(in)

	decoded := make([]uint64, len(in))
	n, _ := DecodeUint64(decoded, util.Uint64SliceAsByteSlice(encoded))

	if len(in) != len(decoded[:n]) {
		t.Fatalf("Len mismatch: got %v, exp %v", len(decoded), len(in))
	}
}

// TestEncode ensures 100% test coverage of simple8b.Encode and
// verifies all output by comparing the original input with the output of simple8b.Decode
func TestEncode(t *testing.T) {
	for _, test := range s8bTestsUint64 {
		t.Run(test.Name, func(t *testing.T) {
			if test.Fn != nil {
				test.In = test.Fn()
			}

			encoded, err := EncodeUint64(append(make([]uint64, 0, len(test.In)), test.In...))
			if err != nil {
				if !test.Err {
					t.Fatalf("expected encode error, got\n%s", err)
				}
				return
			}

			decoded := make([]uint64, len(test.In))
			n, err := DecodeUint64(decoded, util.Uint64SliceAsByteSlice(encoded))
			if err != nil {
				t.Fatalf("unexpected decode error\n%s", err)
			}

			if !cmp.Equal(decoded[:n], test.In) {
				t.Fatalf("unexpected values; +got/-exp\n%s", cmp.Diff(decoded, test.In))
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
	// got, err := CountValues(encoded)
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

func BenchmarkEncode(b *testing.B) {
	for _, bm := range s8bBenchmarksUint64 {
		in := bm.Fn(s8bBenchmarkSize)()
		b.Run(bm.Name, func(b *testing.B) {
			b.SetBytes(int64(8 * bm.Size))
			for i := 0; i < b.N; i++ {
				Encode(append(make([]uint64, 0, len(in)), in...))
			}
		})
	}
}

// func BenchmarkEncode(b *testing.B) {
// 	x := make([]uint64, 1024)
// 	for i := 0; i < len(x); i++ {
// 		x[i] = uint64(15)
// 	}

// 	in := make([]uint64, 1024)

// 	b.SetBytes(int64(len(x) * 8))
// 	b.ResetTimer()
// 	for i := 0; i < b.N; i++ {
// 		copy(in, x)
// 		Encode(in)
// 	}
// }

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
	y, _ := EncodeUint64(x)

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
