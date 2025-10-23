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
		b.currentField().Fixed = uint16(n)
	}
}

func Scale[T int | uint8](n T) BuilderOption {
	return func(b *Builder) {
		b.currentField().Scale = uint8(n)
	}
}

func Index(i types.IndexType) BuilderOption {
	return func(b *Builder) {
		b.currentField().Index = NewIndexInfo(b.currentField(), i)
	}
}

func Compression(c types.BlockCompression) BuilderOption {
	return func(b *Builder) {
		b.currentField().Compress = c
	}
}

func Primary() BuilderOption {
	return func(b *Builder) {
		b.currentField().Flags |= types.FieldFlagPrimary
	}
}

func Nullable() BuilderOption {
	return func(b *Builder) {
		b.currentField().Flags |= types.FieldFlagNullable
	}
}

func Id(id uint16) BuilderOption {
	return func(b *Builder) {
		b.currentField().Id = id
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
	return b.s.Fields[len(b.s.Fields)-1]
}

func (b *Builder) addField(typ types.FieldType, name string, opts ...BuilderOption) *Builder {
	if name == "" {
		name = "F" + strconv.Itoa(len(b.s.Fields))
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
	return b.addField(FT_I64, name, opts...)
}

func (b *Builder) Int32(name string, opts ...BuilderOption) *Builder {
	return b.addField(FT_I32, name, opts...)
}

func (b *Builder) Int16(name string, opts ...BuilderOption) *Builder {
	return b.addField(FT_I16, name, opts...)
}

func (b *Builder) Int8(name string, opts ...BuilderOption) *Builder {
	return b.addField(FT_I8, name, opts...)
}

func (b *Builder) Uint64(name string, opts ...BuilderOption) *Builder {
	return b.addField(FT_U64, name, opts...)
}

func (b *Builder) Uint32(name string, opts ...BuilderOption) *Builder {
	return b.addField(FT_U32, name, opts...)
}

func (b *Builder) Uint16(name string, opts ...BuilderOption) *Builder {
	return b.addField(FT_U16, name, opts...)
}

func (b *Builder) Uint8(name string, opts ...BuilderOption) *Builder {
	return b.addField(FT_U8, name, opts...)
}

func (b *Builder) Timestamp(name string, opts ...BuilderOption) *Builder {
	return b.addField(FT_TIMESTAMP, name, opts...)
}

func (b *Builder) Date(name string, opts ...BuilderOption) *Builder {
	return b.addField(FT_DATE, name, opts...)
}

func (b *Builder) Time(name string, opts ...BuilderOption) *Builder {
	return b.addField(FT_TIME, name, opts...)
}

func (b *Builder) Float64(name string, opts ...BuilderOption) *Builder {
	return b.addField(FT_F64, name, opts...)
}

func (b *Builder) Float32(name string, opts ...BuilderOption) *Builder {
	return b.addField(FT_F32, name, opts...)
}

func (b *Builder) Bool(name string, opts ...BuilderOption) *Builder {
	return b.addField(FT_BOOL, name, opts...)
}

func (b *Builder) String(name string, opts ...BuilderOption) *Builder {
	return b.addField(FT_STRING, name, opts...)
}

func (b *Builder) Bytes(name string, opts ...BuilderOption) *Builder {
	return b.addField(FT_BYTES, name, opts...)
}

func (b *Builder) Int128(name string, opts ...BuilderOption) *Builder {
	return b.addField(FT_I128, name, opts...)
}

func (b *Builder) Int256(name string, opts ...BuilderOption) *Builder {
	return b.addField(FT_I256, name, opts...)
}

func (b *Builder) Decimal32(name string, opts ...BuilderOption) *Builder {
	return b.addField(FT_D32, name, opts...)
}

func (b *Builder) Decimal64(name string, opts ...BuilderOption) *Builder {
	return b.addField(FT_D64, name, opts...)
}

func (b *Builder) Decimal128(name string, opts ...BuilderOption) *Builder {
	return b.addField(FT_D128, name, opts...)
}

func (b *Builder) Decimal256(name string, opts ...BuilderOption) *Builder {
	return b.addField(FT_D256, name, opts...)
}

func (b *Builder) Bigint(name string, opts ...BuilderOption) *Builder {
	return b.addField(FT_BIGINT, name, opts...)
}
