// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"strconv"

	"blockwatch.cc/knoxdb/internal/types"
)

type BuilderOption func(*Builder)

func Fixed[T int | uint16](n T) BuilderOption {
	return func(b *Builder) {
		b.currentField().fixed = uint16(n)
	}
}

func Scale[T int | uint8](n T) BuilderOption {
	return func(b *Builder) {
		b.currentField().scale = uint8(n)
	}
}

func Index(i types.IndexType) BuilderOption {
	return func(b *Builder) {
		b.currentField().index = i
	}
}

func Compression(c types.BlockCompression) BuilderOption {
	return func(b *Builder) {
		b.currentField().compress = c
	}
}

func Primary() BuilderOption {
	return func(b *Builder) {
		b.currentField().flags |= types.FieldFlagPrimary
	}
}

func Nullable() BuilderOption {
	return func(b *Builder) {
		b.currentField().flags |= types.FieldFlagNullable
	}
}

func Id(id uint16) BuilderOption {
	return func(b *Builder) {
		b.currentField().id = id
	}
}

type Builder struct {
	s *Schema
}

func NewBuilder() *Builder {
	return &Builder{
		s: NewSchema(),
	}
}

func (b *Builder) Validate() error {
	return b.s.Validate()
}

func (b *Builder) Finalize() *Builder {
	b.s.Finalize()
	return b
}

func (b *Builder) Schema() *Schema {
	return b.s
}

func (b *Builder) WithName(s string) *Builder {
	b.s.WithName(s)
	return b
}

func (b *Builder) WithVersion(v uint32) *Builder {
	b.s.WithVersion(v)
	return b
}

func (b *Builder) currentField() *Field {
	return &b.s.fields[len(b.s.fields)-1]
}

func (b *Builder) addField(typ types.FieldType, name string, opts ...BuilderOption) *Builder {
	if name == "" {
		name = "F" + strconv.Itoa(len(b.s.fields))
	}
	b.s.WithField(NewField(typ).WithName(name))
	for _, o := range opts {
		o(b)
	}
	return b
}

func (b *Builder) AddField(name string, typ types.FieldType, opts ...BuilderOption) *Builder {
	return b.addField(typ, name, opts...)
}

func (b *Builder) SetFieldOpts(opts ...BuilderOption) *Builder {
	for _, o := range opts {
		o(b)
	}
	return b
}

func (b *Builder) Int64(name string, opts ...BuilderOption) *Builder {
	return b.addField(types.FieldTypeInt64, name, opts...)
}

func (b *Builder) Int32(name string, opts ...BuilderOption) *Builder {
	return b.addField(types.FieldTypeInt32, name, opts...)
}

func (b *Builder) Int16(name string, opts ...BuilderOption) *Builder {
	return b.addField(types.FieldTypeInt16, name, opts...)
}

func (b *Builder) Int8(name string, opts ...BuilderOption) *Builder {
	return b.addField(types.FieldTypeInt8, name, opts...)
}

func (b *Builder) Uint64(name string, opts ...BuilderOption) *Builder {
	return b.addField(types.FieldTypeUint64, name, opts...)
}

func (b *Builder) Uint32(name string, opts ...BuilderOption) *Builder {
	return b.addField(types.FieldTypeUint32, name, opts...)
}

func (b *Builder) Uint16(name string, opts ...BuilderOption) *Builder {
	return b.addField(types.FieldTypeUint16, name, opts...)
}

func (b *Builder) Uint8(name string, opts ...BuilderOption) *Builder {
	return b.addField(types.FieldTypeUint8, name, opts...)
}

func (b *Builder) Datetime(name string, opts ...BuilderOption) *Builder {
	return b.addField(types.FieldTypeDatetime, name, opts...)
}

func (b *Builder) Float64(name string, opts ...BuilderOption) *Builder {
	return b.addField(types.FieldTypeFloat64, name, opts...)
}

func (b *Builder) Float32(name string, opts ...BuilderOption) *Builder {
	return b.addField(types.FieldTypeFloat32, name, opts...)
}

func (b *Builder) Bool(name string, opts ...BuilderOption) *Builder {
	return b.addField(types.FieldTypeBoolean, name, opts...)
}

func (b *Builder) String(name string, opts ...BuilderOption) *Builder {
	return b.addField(types.FieldTypeString, name, opts...)
}

func (b *Builder) Bytes(name string, opts ...BuilderOption) *Builder {
	return b.addField(types.FieldTypeBytes, name, opts...)
}

func (b *Builder) Int128(name string, opts ...BuilderOption) *Builder {
	return b.addField(types.FieldTypeInt128, name, opts...)
}

func (b *Builder) Int256(name string, opts ...BuilderOption) *Builder {
	return b.addField(types.FieldTypeInt256, name, opts...)
}

func (b *Builder) Decimal32(name string, opts ...BuilderOption) *Builder {
	return b.addField(types.FieldTypeDecimal32, name, opts...)
}

func (b *Builder) Decimal64(name string, opts ...BuilderOption) *Builder {
	return b.addField(types.FieldTypeDecimal64, name, opts...)
}

func (b *Builder) Decimal128(name string, opts ...BuilderOption) *Builder {
	return b.addField(types.FieldTypeDecimal128, name, opts...)
}

func (b *Builder) Decimal256(name string, opts ...BuilderOption) *Builder {
	return b.addField(types.FieldTypeDecimal256, name, opts...)
}

func (b *Builder) Bigint(name string, opts ...BuilderOption) *Builder {
	return b.addField(types.FieldTypeBigint, name, opts...)
}
