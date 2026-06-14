// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cast

import (
	"fmt"

	"blockwatch.cc/knoxdb/pkg/schema"
)

// ValueCasters have the purpose of converting Go types used in programmatic
// queries (written in Go) to block schema. This is required since inputs for
// comparison functions accept interfaces and will perform unchecked type
// conversions. We use ValueCaster during query compilation to ensure these
// interface to type conversions don't panic.
//
// The type of a ValueCaster defines the output (target) type which must
// be equal to the underlying block type for a given field.

type ValueCaster interface {
	CastValue(any) (any, error)
	CastSlice(any) (any, error)
}

func CastError(val any, kind string) error {
	return fmt.Errorf("cast: unexpected value type %T for %s condition", val, kind)
}

func NewCaster(typ schema.FieldType, scale uint8, enum ValueCaster) ValueCaster {
	switch typ {
	case schema.Timestamp, schema.Time:
		return TimeCaster{scale: schema.TimeScale(scale)}
	case schema.Duration:
		return DurationCaster{scale: schema.TimeScale(scale)}
	case schema.Date:
		return DateCaster{}
	case schema.Boolean:
		return BoolCaster{}
	case schema.String:
		return StringCaster{} // MarshalText, stringer, ToString
	case schema.Bytes:
		return BytesCaster{} // MarshalBinary
	case schema.Int8:
		return IntCaster[int8]{}
	case schema.Int16:
		return IntCaster[int16]{}
	case schema.Int32:
		return IntCaster[int32]{}
	case schema.Int64:
		return IntCaster[int64]{}
	case schema.Uint8:
		return UintCaster[uint8]{}
	case schema.Uint16:
		return UintCaster[uint16]{}
	case schema.Uint32:
		return UintCaster[uint32]{}
	case schema.Uint64:
		return UintCaster[uint64]{}
	case schema.Float32:
		return FloatCaster[float32]{}
	case schema.Float64:
		return FloatCaster[float64]{}
	case schema.Int128:
		return I128Caster{}
	case schema.Int256:
		return I256Caster{}
	case schema.Decimal32:
		return IntCaster[int32]{}
	case schema.Decimal64:
		return IntCaster[int64]{}
	case schema.Decimal128:
		return I128Caster{}
	case schema.Decimal256:
		return I256Caster{}
	case schema.Bigint:
		return BigIntCaster{}
	case schema.Enum:
		return enum
	default:
		panic(fmt.Errorf("caster: unsupported field type %s %d", typ, typ))
	}
}
