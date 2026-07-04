// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package reducer

import (
	"bytes"
	"reflect"
	"time"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/types"
)

var null = []byte(`null`)

type Bucket interface {
	WithDimensions(TimeRange, TimeUnit) Bucket
	WithReducer(ReducerFunc) Bucket
	WithName(string) Bucket
	WithIndex(int) Bucket
	WithFill(FillMode) Bucket
	WithLimit(int) Bucket
	WithType(reflect.Type) Bucket
	WithTypeOf(Aggregatable) Bucket
	WithInit(Aggregatable) Bucket
	Len() int
	Push(time.Time, engine.QueryRow, bool) error
	Emit(*bytes.Buffer) error
}

func NewBucket(typ types.FieldType) Bucket {
	switch typ {
	case types.FT_TIMESTAMP, types.FT_DATE, types.FT_TIME:
		// required for time column
		return NewTimeBucket()

	case types.FT_DURATION:
		// TODO: must apply scale
		return NewDurationBucket()

	case types.FT_BYTES: // requires an aggregator type, use WithTypeOf(&MyType{})
		return NewTypedBucket()

	case types.FT_I64:
		b := NewNativeBucket[int64]()
		b.emit = emitIntegers[int64]
		return b
	case types.FT_I32:
		b := NewNativeBucket[int32]()
		b.emit = emitIntegers[int32]
		return b
	case types.FT_I16:
		b := NewNativeBucket[int16]()
		b.emit = emitIntegers[int16]
		return b
	case types.FT_I8:
		b := NewNativeBucket[int8]()
		b.emit = emitIntegers[int8]
		return b
	case types.FT_U64:
		b := NewNativeBucket[uint64]()
		b.emit = emitUnsigneds[uint64]
		return b
	case types.FT_U32:
		b := NewNativeBucket[uint32]()
		b.emit = emitUnsigneds[uint32]
		return b
	case types.FT_U16:
		b := NewNativeBucket[uint16]()
		b.emit = emitUnsigneds[uint16]
		return b
	case types.FT_U8:
		b := NewNativeBucket[uint8]()
		b.emit = emitUnsigneds[uint8]
		return b
	case types.FT_F64:
		b := NewNativeBucket[float64]()
		b.emit = emitFloats[float64]
		return b
	case types.FT_F32:
		b := NewNativeBucket[float32]()
		b.emit = emitFloats[float32]
		return b

	// TODO: maybe a DecimalBucket makes sense
	case types.FT_D256:
		b := NewNativeBucket[float64]()
		b.emit = emitFloats[float64]
		return b
	case types.FT_D128:
		b := NewNativeBucket[float64]()
		b.emit = emitFloats[float64]
		return b
	case types.FT_D64:
		b := NewNativeBucket[float64]()
		b.emit = emitFloats[float64]
		return b
	case types.FT_D32:
		b := NewNativeBucket[float64]()
		b.emit = emitFloats[float64]
		return b

	case types.FT_I256:
		b := NewTypedBucket()
		b.WithTypeOf(&Int256Aggregator{})
		b.read = b.readInt256
		return b

	case types.FT_I128:
		b := NewTypedBucket()
		b.WithTypeOf(&Int128Aggregator{})
		b.read = b.readInt128
		return b

		// unsupported for time-series output (can still use as filter)
		// case types.FieldTypeString:
		// case types.FieldTypeBoolean:
	}
	return nil
}
