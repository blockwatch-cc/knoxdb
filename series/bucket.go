// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package series

import (
	"bytes"
	"reflect"
	"time"

	"blockwatch.cc/knoxdb/pack"
)

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
	Push(time.Time, pack.Row, bool) error
	Emit(*bytes.Buffer) error
}

func NewBucket(typ pack.FieldType) Bucket {
	switch typ {
	case pack.FieldTypeDatetime: // required for time column
		return NewTimeBucket()

	case pack.FieldTypeBytes: // requires an aggregator type, use WithTypeOf(&MyType{})
		return NewTypedBucket()

	case pack.FieldTypeInt64:
		b := NewNativeBucket[int64]()
		b.emit = emitIntegers[int64]
		b.read = b.readInt64
		return b
	case pack.FieldTypeInt32:
		b := NewNativeBucket[int32]()
		b.emit = emitIntegers[int32]
		b.read = b.readInt32
		return b
	case pack.FieldTypeInt16:
		b := NewNativeBucket[int16]()
		b.emit = emitIntegers[int16]
		b.read = b.readInt16
		return b
	case pack.FieldTypeInt8:
		b := NewNativeBucket[int8]()
		b.emit = emitIntegers[int8]
		b.read = b.readInt8
		return b
	case pack.FieldTypeUint64:
		b := NewNativeBucket[uint64]()
		b.emit = emitUnsigneds[uint64]
		b.read = b.readUint64
		return b
	case pack.FieldTypeUint32:
		b := NewNativeBucket[uint32]()
		b.emit = emitUnsigneds[uint32]
		b.read = b.readUint32
		return b
	case pack.FieldTypeUint16:
		b := NewNativeBucket[uint16]()
		b.emit = emitUnsigneds[uint16]
		b.read = b.readUint16
		return b
	case pack.FieldTypeUint8:
		b := NewNativeBucket[uint8]()
		b.emit = emitUnsigneds[uint8]
		b.read = b.readUint8
		return b
	case pack.FieldTypeFloat64:
		b := NewNativeBucket[float64]()
		b.emit = emitFloats[float64]
		b.read = b.readFloat64
		return b
	case pack.FieldTypeFloat32:
		b := NewNativeBucket[float32]()
		b.emit = emitFloats[float32]
		b.read = b.readFloat32
		return b

	// TODO: maybe a DecimalBucket makes sense
	case pack.FieldTypeDecimal256:
		b := NewNativeBucket[float64]()
		b.emit = emitFloats[float64]
		b.read = b.readDecimal256
		return b
	case pack.FieldTypeDecimal128:
		b := NewNativeBucket[float64]()
		b.emit = emitFloats[float64]
		b.read = b.readDecimal128
		return b
	case pack.FieldTypeDecimal64:
		b := NewNativeBucket[float64]()
		b.emit = emitFloats[float64]
		b.read = b.readDecimal64
		return b
	case pack.FieldTypeDecimal32:
		b := NewNativeBucket[float64]()
		b.emit = emitFloats[float64]
		b.read = b.readDecimal32
		return b

	case pack.FieldTypeInt256:
		b := NewTypedBucket()
		b.WithTypeOf(&Int256Aggregator{})
		b.read = b.readInt256
		return b

	case pack.FieldTypeInt128:
		b := NewTypedBucket()
		b.WithTypeOf(&Int128Aggregator{})
		b.read = b.readInt128
		return b

		// unsupported for time-series output (can still use as filter)
		// case pack.FieldTypeString:
		// case pack.FieldTypeBoolean:
	}
	return nil
}
