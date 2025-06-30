// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package reducer

import (
	"bytes"
	"reflect"
	"time"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
)

var null = []byte(`null`)

type Bucket interface {
	WithDimensions(util.TimeRange, util.TimeUnit) Bucket
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
	case types.FieldTypeDatetime, types.FieldTypeDate, types.FieldTypeTime:
		// required for time column
		return NewTimeBucket()

	case types.FieldTypeBytes: // requires an aggregator type, use WithTypeOf(&MyType{})
		return NewTypedBucket()

	case types.FieldTypeInt64:
		b := NewNativeBucket[int64]()
		b.emit = emitIntegers[int64]
		return b
	case types.FieldTypeInt32:
		b := NewNativeBucket[int32]()
		b.emit = emitIntegers[int32]
		return b
	case types.FieldTypeInt16:
		b := NewNativeBucket[int16]()
		b.emit = emitIntegers[int16]
		return b
	case types.FieldTypeInt8:
		b := NewNativeBucket[int8]()
		b.emit = emitIntegers[int8]
		return b
	case types.FieldTypeUint64:
		b := NewNativeBucket[uint64]()
		b.emit = emitUnsigneds[uint64]
		return b
	case types.FieldTypeUint32:
		b := NewNativeBucket[uint32]()
		b.emit = emitUnsigneds[uint32]
		return b
	case types.FieldTypeUint16:
		b := NewNativeBucket[uint16]()
		b.emit = emitUnsigneds[uint16]
		return b
	case types.FieldTypeUint8:
		b := NewNativeBucket[uint8]()
		b.emit = emitUnsigneds[uint8]
		return b
	case types.FieldTypeFloat64:
		b := NewNativeBucket[float64]()
		b.emit = emitFloats[float64]
		return b
	case types.FieldTypeFloat32:
		b := NewNativeBucket[float32]()
		b.emit = emitFloats[float32]
		return b

	// TODO: maybe a DecimalBucket makes sense
	case types.FieldTypeDecimal256:
		b := NewNativeBucket[float64]()
		b.emit = emitFloats[float64]
		return b
	case types.FieldTypeDecimal128:
		b := NewNativeBucket[float64]()
		b.emit = emitFloats[float64]
		return b
	case types.FieldTypeDecimal64:
		b := NewNativeBucket[float64]()
		b.emit = emitFloats[float64]
		return b
	case types.FieldTypeDecimal32:
		b := NewNativeBucket[float64]()
		b.emit = emitFloats[float64]
		return b

	case types.FieldTypeInt256:
		b := NewTypedBucket()
		b.WithTypeOf(&Int256Aggregator{})
		b.read = b.readInt256
		return b

	case types.FieldTypeInt128:
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
