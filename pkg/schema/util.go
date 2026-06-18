package schema

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"time"

	"blockwatch.cc/knoxdb/pkg/num"
)

type Signed interface {
	~int8 | ~int16 | ~int32 | ~int64
}

type Unsigned interface {
	~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr
}

type Float interface {
	~float32 | ~float64
}

// MapValueTypes defines a type union for all primitive types
// that are accepted by the schema as map values.
type MapValueTypes interface {
	Signed | Unsigned | Float | ~string | ~bool | ~[]byte |
		time.Time | // timestamp, time, date
		num.Int256 | // large numeric types
		num.Int128 |
		num.Decimal256 |
		num.Decimal128 |
		num.Decimal64 |
		num.Decimal32 |
		num.Big
}

func basename(name string) string {
	idx := strings.LastIndex(name, ".")
	if idx < 0 {
		return name
	}
	return name[idx+1:]
}

func validateInt(name string, n, minVal, maxVal int) error {
	if n < minVal || (maxVal > 0 && n > maxVal) {
		return fmt.Errorf("%s %d out of bounds [%d..%d]", name, n, minVal, maxVal)
	}
	return nil
}

// CompareTime is a custom compare func for slices.SortedFunc
func CompareTime[K time.Time](a, b K) int {
	return time.Time(a).Compare(time.Time(b))
}

// SortedKeys is a generic helper to sort Go map keys. It allocates
// a new slice of type K which escapes to heap, so embedding this
// code at call location may be cheaper.
func SortedKeys[K cmp.Ordered, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}
