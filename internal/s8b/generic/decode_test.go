package generic

import (
	"encoding/binary"
	"testing"

	"blockwatch.cc/knoxdb/internal/s8b/tests"
	"github.com/google/go-cmp/cmp"
)

var (
	// s8bTestsUint64 = tests.S8bTestsUint64
	s8bTestsUint32 = tests.S8bTestsUint32
	s8bTestsUint16 = tests.S8bTestsUint16
	s8bTestsUint8  = tests.S8bTestsUint8
)

// TestEncode ensures 100% test coverage of EncodeUint64 and
// verifies all output by comparing the original input with the output of Decode
func TestEncodeUint64Generic(t *testing.T) {
	for _, test := range s8bTestsUint64 {
		if test.Fn != nil {
			test.In = test.Fn()
		}

		encoded, err := EncodeUint64(append(make([]uint64, 0, len(test.In)), test.In...))
		if err != nil {
			if !test.Err {
				t.Fatalf("%s: expected encode error, got\n%s", test.Name, err)
			}
			return
		}

		buf := make([]byte, 8*len(encoded))
		b := buf
		for _, v := range encoded {
			binary.LittleEndian.PutUint64(b, v)
			b = b[8:]
		}
		count, err := CountValues(buf)
		if err != nil {
			t.Fatalf("%s: unexpected count error\n%s", test.Name, err)
		}

		if count != len(test.In) {
			t.Fatalf("%s: unexpected count: got %d expected %d", test.Name, count, len(test.In))
		}

		decoded := make([]uint64, len(test.In))
		n, err := DecodeUint64(decoded, buf)
		if err != nil {
			t.Fatalf("%s: unexpected decode error\n%s", test.Name, err)
		}

		if !cmp.Equal(decoded[:n], test.In) {
			t.Fatalf("%s: unexpected values; +got/-exp\n%s", test.Name, cmp.Diff(decoded, test.In))
		}
	}
}

func TestEncodeUint32Generic(t *testing.T) {
	for _, test := range s8bTestsUint32 {
		if test.Fn != nil {
			test.In = test.Fn()
		}

		tmp := make([]uint64, len(test.In))
		for i := 0; i < len(tmp); i++ {
			tmp[i] = uint64(test.In[i])
		}
		encoded, err := EncodeUint64(append(make([]uint64, 0, len(test.In)), tmp...))
		if err != nil {
			if !test.Err {
				t.Fatalf("%s: expected encode error, got\n%s", test.Name, err)
			}
			return
		}

		buf := make([]byte, 8*len(encoded))
		b := buf
		for _, v := range encoded {
			binary.LittleEndian.PutUint64(b, v)
			b = b[8:]
		}
		count, err := CountValues(buf)
		if err != nil {
			t.Fatalf("%s: unexpected count error\n%s", test.Name, err)
		}

		if count != len(test.In) {
			t.Fatalf("%s: unexpected count: got %d expected %d", test.Name, count, len(test.In))
		}

		decoded := make([]uint32, len(test.In))
		n, err := DecodeUint32(decoded, buf)
		if err != nil {
			t.Fatalf("%s: unexpected decode error\n%s", test.Name, err)
		}

		if !cmp.Equal(decoded[:n], test.In) {
			t.Fatalf("%s: unexpected values; +got/-exp\n%s", test.Name, cmp.Diff(decoded, test.In))
		}
	}
}

func TestEncodeUint16Generic(t *testing.T) {
	for _, test := range s8bTestsUint16 {
		if test.Fn != nil {
			test.In = test.Fn()
		}

		tmp := make([]uint64, len(test.In))
		for i := 0; i < len(tmp); i++ {
			tmp[i] = uint64(test.In[i])
		}
		encoded, err := EncodeUint64(append(make([]uint64, 0, len(test.In)), tmp...))
		if err != nil {
			if !test.Err {
				t.Fatalf("%s: expected encode error, got\n%s", test.Name, err)
			}
			return
		}

		buf := make([]byte, 8*len(encoded))
		b := buf
		for _, v := range encoded {
			binary.LittleEndian.PutUint64(b, v)
			b = b[8:]
		}
		count, err := CountValues(buf)
		if err != nil {
			t.Fatalf("%s: unexpected count error\n%s", test.Name, err)
		}

		if count != len(test.In) {
			t.Fatalf("%s: unexpected count: got %d expected %d", test.Name, count, len(test.In))
		}

		decoded := make([]uint16, len(test.In))
		n, err := DecodeUint16(decoded, buf)
		if err != nil {
			t.Fatalf("%s: unexpected decode error\n%s", test.Name, err)
		}

		if !cmp.Equal(decoded[:n], test.In) {
			t.Fatalf("%s: unexpected values; +got/-exp\n%s", test.Name, cmp.Diff(decoded, test.In))
		}
	}
}

func TestEncodeUint8Generic(t *testing.T) {
	for _, test := range s8bTestsUint8 {
		if test.Fn != nil {
			test.In = test.Fn()
		}

		tmp := make([]uint64, len(test.In))
		for i := 0; i < len(tmp); i++ {
			tmp[i] = uint64(test.In[i])
		}
		encoded, err := EncodeUint64(append(make([]uint64, 0, len(test.In)), tmp...))
		if err != nil {
			if !test.Err {
				t.Fatalf("%s: expected encode error, got\n%s", test.Name, err)
			}
			return
		}

		buf := make([]byte, 8*len(encoded))
		b := buf
		for _, v := range encoded {
			binary.LittleEndian.PutUint64(b, v)
			b = b[8:]
		}
		count, err := CountValues(buf)
		if err != nil {
			t.Fatalf("%s: unexpected count error\n%s", test.Name, err)
		}

		if count != len(test.In) {
			t.Fatalf("%s: unexpected count: got %d expected %d", test.Name, count, len(test.In))
		}

		decoded := make([]uint8, len(test.In))
		n, err := DecodeUint8(decoded, buf)
		if err != nil {
			t.Fatalf("%s: unexpected decode error\n%s", test.Name, err)
		}

		if !cmp.Equal(decoded[:n], test.In) {
			t.Fatalf("%s: unexpected values; +got/-exp\n%s", test.Name, cmp.Diff(decoded, test.In))
		}
	}
}
