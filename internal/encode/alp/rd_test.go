package alp

import (
	"fmt"
	"testing"

	"blockwatch.cc/knoxdb/internal/types"
	"github.com/stretchr/testify/assert"
)

func TestAlpRD(t *testing.T) {
	AlpRDTest[float32, uint32](t)
	AlpRDTest[float64, uint64](t)
}

func AlpRDTest[T types.Float, U types.Unsigned](t *testing.T) {
	for _, c := range MakeTestcases[T]() {
		t.Run(fmt.Sprintf("%T/%s", T(0), c.Name), func(t *testing.T) {
			// e := NewEncoder[T]().Compress(c.Data)
			// s := e.State()
			// dec := NewDecoder[T](s.EncodingIndice.Factor, s.EncodingIndice.Exponent).
			// 	WithExceptions(s.Exceptions, s.ExceptionPositions)
			// res := make([]T, len(c.Data))
			// dec.Decompress(res, s.EncodedIntegers)
			// assert.Equal(t, c.Data, res)

			s := RDCompress[T, U](c.Data)
			res := RDDecompress[T, U](s)
			assert.Equal(t, c.Data, res)
		})
	}
}
