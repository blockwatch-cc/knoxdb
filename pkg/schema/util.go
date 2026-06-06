package schema

import "strings"

type Signed interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64
}

type Unsigned interface {
	~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr
}

type Float interface {
	~float32 | ~float64
}

func basename(name string) string {
	idx := strings.LastIndex(name, ".")
	if idx < 0 {
		return name
	}
	return name[idx+1:]
}
