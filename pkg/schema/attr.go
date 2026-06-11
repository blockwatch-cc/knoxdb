// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"time"

	"blockwatch.cc/knoxdb/pkg/num"
)

// Attr is an allocation free key/value type using a tagged union
// as value so it can represent any primitive schema type.
type Attr struct {
	Key   string     // attribute name
	Value UnionValue // tagged union of primitive type
}

func (a Attr) String() string {
	return a.Key + "=" + a.Value.String()
}

func Int64Attr(key string, value int64) Attr {
	return Attr{Key: key, Value: Int64Union(value)}
}

func Int32Attr(key string, value int32) Attr {
	return Attr{Key: key, Value: Int32Union(value)}
}

func Int16Attr(key string, value int16) Attr {
	return Attr{Key: key, Value: Int16Union(value)}
}

func Int8Attr(key string, value int8) Attr {
	return Attr{Key: key, Value: Int8Union(value)}
}

func Uint64Attr(key string, value uint64) Attr {
	return Attr{Key: key, Value: Uint64Union(value)}
}

func Uint32Attr(key string, value uint32) Attr {
	return Attr{Key: key, Value: Uint32Union(value)}
}

func Uint16Attr(key string, value uint16) Attr {
	return Attr{Key: key, Value: Uint16Union(value)}
}

func Uint8Attr(key string, value uint8) Attr {
	return Attr{Key: key, Value: Uint8Union(value)}
}

func Float64Attr(key string, value float64) Attr {
	return Attr{Key: key, Value: Float64Union(value)}
}

func Float32Attr(key string, value float32) Attr {
	return Attr{Key: key, Value: Float32Union(value)}
}

func TimestampAttr(key string, value time.Time) Attr {
	return Attr{Key: key, Value: TimestampUnion(value)}
}

func DurationAttr(key string, value time.Duration) Attr {
	return Attr{Key: key, Value: DurationUnion(value)}
}

func TimeAttr(key string, value time.Time) Attr {
	return Attr{Key: key, Value: TimeUnion(value)}
}

func DateAttr(key string, value time.Time) Attr {
	return Attr{Key: key, Value: DateUnion(value)}
}

func BoolAttr(key string, value bool) Attr {
	return Attr{Key: key, Value: BoolUnion(value)}
}

func StringAttr(key, value string) Attr {
	return Attr{Key: key, Value: StringUnion(value)}
}

func BytesAttr(key string, value []byte) Attr {
	return Attr{Key: key, Value: BytesUnion(value)}
}

func BigintAttr(key string, value num.Big) Attr {
	return Attr{Key: key, Value: BigintUnion(value)}
}

func Int128Attr(key string, value num.Int128) Attr {
	return Attr{Key: key, Value: Int128Union(value)}
}

func Int256Attr(key string, value num.Int256) Attr {
	return Attr{Key: key, Value: Int256Union(value)}
}
