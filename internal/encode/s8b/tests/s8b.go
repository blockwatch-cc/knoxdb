package tests

import (
	"slices"
	"testing"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/require"
)

type EncodeFunc[T types.Integer] func([]byte, []T, T, T) ([]byte, error)
type DecodeFunc[T types.Unsigned] func([]T, []byte) (int, error)

type S8bTests[T types.Unsigned] struct {
	Name string
	In   []T
	Fn   func() []T
	Err  bool
}

func MakeTests[T types.Unsigned]() []S8bTests[T] {
	width := unsafe.Sizeof(T(0))
	tests := []S8bTests[T]{
		{Name: "nil", In: nil},
		{Name: "empty", In: []T{}},
		{Name: "mixed sizes", In: []T{7, 6, 255, 4, 3, 2, 1}},
		{Name: "240 ones", Fn: ones[T](240)},
		{Name: "120 ones plus 5", Fn: func() []T {
			in := ones[T](240)()
			in[120] = 5
			return in
		}},
		{Name: "119 ones plus 5", Fn: func() []T {
			in := ones[T](240)()
			in[119] = 5
			return in
		}},
		{Name: "239 ones plus 5", Fn: func() []T {
			in := ones[T](241)()
			in[239] = 5
			return in
		}},
		{Name: "1 bit", Fn: bits[T](120, 1)},
		{Name: "2 bits", Fn: bits[T](120, 2)},
		{Name: "3 bits", Fn: bits[T](120, 3)},
		{Name: "4 bits", Fn: bits[T](120, 4)},
		{Name: "5 bits", Fn: bits[T](120, 5)},
		{Name: "6 bits", Fn: bits[T](120, 6)},
		{Name: "7 bits", Fn: bits[T](120, 7)},
		{Name: "8 bits", Fn: bits[T](120, 8)},
		{Name: "67", In: []T{
			67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
			67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
			67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
			67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
			67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
			67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
			67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
			67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
			67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
			67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
			67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
			67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
			67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
			67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
			67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
			67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
			67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
			67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
			67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
			67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
			67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
			67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
			67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
			67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
			67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
			67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
			67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
			67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
			67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
			67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
			67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
			67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
			67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
			67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
			67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
			67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
			67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
			67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
			67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
			67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
		}},
	}
	combi := S8bTests[T]{
		Name: "combination",
		Fn: combine[T](
			bits[T](120, 1),
			bits[T](120, 2),
			bits[T](120, 3),
			bits[T](120, 4),
			bits[T](120, 5),
			bits[T](120, 6),
			bits[T](120, 7),
			bits[T](120, 8),
		)}

	if width > 1 {
		tests = append(tests, []S8bTests[T]{
			{Name: "10 bits", Fn: bits[T](120, 10)},
			{Name: "12 bits", Fn: bits[T](120, 12)},
			{Name: "15 bits", Fn: bits[T](120, 15)},
		}...)
		combi.Fn = combine[T](
			bits[T](120, 1),
			bits[T](120, 2),
			bits[T](120, 3),
			bits[T](120, 4),
			bits[T](120, 5),
			bits[T](120, 6),
			bits[T](120, 7),
			bits[T](120, 8),
			bits[T](120, 10),
			bits[T](120, 12),
			bits[T](120, 15),
			bits[T](120, 16),
		)
	}

	if width > 2 {
		tests = append(tests, []S8bTests[T]{
			{Name: "20 bits", Fn: bits[T](120, 20)},
			{Name: "30 bits", Fn: bits[T](120, 30)},
			{Name: "32 bits", Fn: bits[T](120, 32)},
		}...)
		combi.Fn = combine[T](
			bits[T](120, 1),
			bits[T](120, 2),
			bits[T](120, 3),
			bits[T](120, 4),
			bits[T](120, 5),
			bits[T](120, 6),
			bits[T](120, 7),
			bits[T](120, 8),
			bits[T](120, 10),
			bits[T](120, 12),
			bits[T](120, 15),
			bits[T](120, 20),
			bits[T](120, 30),
			bits[T](120, 32),
		)
	}
	if width > 4 {
		tests = append(tests, []S8bTests[T]{
			{Name: "60 bits", Fn: bits[T](120, 60)},
			{
				Name: "too big",
				In:   util.ReinterpretSlice[uint64, T]([]uint64{7, 6, 2<<61 - 1, 4, 3, 2, 1}),
				Err:  true,
			},
		}...)

		combi.Fn = combine[T](
			bits[T](120, 1),
			bits[T](120, 2),
			bits[T](120, 3),
			bits[T](120, 4),
			bits[T](120, 5),
			bits[T](120, 6),
			bits[T](120, 7),
			bits[T](120, 8),
			bits[T](120, 10),
			bits[T](120, 12),
			bits[T](120, 15),
			bits[T](120, 20),
			bits[T](120, 30),
			bits[T](120, 60),
		)
	}

	return append(tests, combi)
}

func EncodeTest[T types.Unsigned](t *testing.T, enc EncodeFunc[T], dec DecodeFunc[T]) {
	for _, test := range MakeTests[T]() {
		t.Run(test.Name, func(t *testing.T) {
			in := test.In
			if test.Fn != nil {
				in = test.Fn()
			}
			var _, maxv T
			if len(in) > 0 {
				_, maxv = slices.Min(in), slices.Max(in)
			}
			buf := make([]byte, len(in)*8)

			// encode without min-FOR to be compatible with testcase data
			// testing all selectors
			buf, err := enc(buf, slices.Clone(in), 0, maxv)
			if test.Err {
				require.Error(t, err)
				return
			} else {
				require.NoError(t, err)
			}

			dst := make([]T, len(in))
			n, err := dec(dst, buf)
			require.NoError(t, err)

			if len(in) > 0 {
				require.Equal(t, in, dst[:n])
			}
		})
	}
}
