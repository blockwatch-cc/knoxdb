package schema

import (
	"strings"
	"time"

	"blockwatch.cc/knoxdb/pkg/num"
)

type Signed interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64
}

type Unsigned interface {
	~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr
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

// CompareTime is a custom compare func for slices.SortedFunc
func CompareTime[K time.Time](a, b K) int {
	return time.Time(a).Compare(time.Time(b))
}
