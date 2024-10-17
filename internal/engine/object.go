// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/wal"
	"blockwatch.cc/knoxdb/pkg/schema"
)

type Object interface {
	Type() types.ObjectTag
	Create(Context) error
	Drop(Context) error
	Update(Context) error
	Decode([]byte, wal.RecordType) error
	Encode(wal.RecordType) ([]byte, error)
}

// TableObject
type TableObject struct {
	id     uint64
	engine *Engine
	schema *schema.Schema
	opts   TableOptions
}

func (o *TableObject) Type() types.ObjectTag {
	return types.ObjectTagTable
}

func (o *TableObject) Create(ctx context.Context) error {
	_, ok := o.engine.GetTable(o.id)
	if ok {
		return nil
	}
	_, err := o.engine.CreateTable(ctx, o.schema, o.opts)
	return err
}

func (o *TableObject) Drop(ctx context.Context) error {
	t, ok := o.engine.GetTable(o.id)
	if !ok {
		return nil
	}
	return o.engine.DropTable(ctx, t.Schema().Name())
}

func (o *TableObject) Update(ctx context.Context) error {
	_, ok := o.engine.GetTable(o.id)
	if !ok {
		return ErrNoTable
	}
	return o.engine.AlterTable(ctx, o.schema.Name(), o.schema)
}

func (o *TableObject) Encode(typ wal.RecordType) ([]byte, error) {
	buf := bytes.NewBuffer(nil)

	// write tag
	buf.Write([]byte{byte(types.ObjectTagTable)})

	// delete records use a short encoding
	if typ == wal.RecordTypeDelete {
		binary.Write(buf, LE, o.id)
		return buf.Bytes(), nil
	}

	// write schema
	b, err := o.schema.MarshalBinary()
	if err != nil {
		return nil, err
	}
	binary.Write(buf, LE, uint32(len(b)))
	buf.Write(b)

	// write options
	b, err = schema.NewGenericEncoder[TableOptions]().Encode(o.opts, nil)
	if err != nil {
		return nil, err
	}
	binary.Write(buf, LE, uint32(len(b)))
	buf.Write(b)

	return buf.Bytes(), nil
}

func (o *TableObject) Decode(data []byte, typ wal.RecordType) error {
	if len(data) < 9 {
		return io.ErrShortBuffer
	}
	if data[0] != byte(types.ObjectTagTable) {
		return ErrInvalidObjectType
	}
	buf := bytes.NewBuffer(data[1:])

	// delete records use a short encoding
	if typ == wal.RecordTypeDelete {
		o.id = LE.Uint64(buf.Next(8))
		return nil
	}

	// read schema
	n := int(LE.Uint32(buf.Next(4)))
	o.schema = schema.NewSchema()
	if err := o.schema.UnmarshalBinary(buf.Next(n)); err != nil {
		return err
	}
	o.id = o.schema.TaggedHash(types.ObjectTagTable)

	// read options
	n = int(LE.Uint32(buf.Next(4)))
	_, err := schema.NewGenericDecoder[TableOptions]().Decode(buf.Next(n), &o.opts)
	if err != nil {
		return err
	}
	return nil
}

// Store object
type StoreObject struct {
	id     uint64
	engine *Engine
	schema *schema.Schema
	opts   StoreOptions
}

func (o *StoreObject) Type() types.ObjectTag {
	return types.ObjectTagStore
}

func (o *StoreObject) Create(ctx context.Context) error {
	_, ok := o.engine.GetStore(o.id)
	if ok {
		return nil
	}
	_, err := o.engine.CreateStore(ctx, o.schema, o.opts)
	return err
}

func (o *StoreObject) Drop(ctx context.Context) error {
	s, ok := o.engine.GetStore(o.id)
	if !ok {
		return nil
	}
	return o.engine.DropStore(ctx, s.Schema().Name())
}

func (o *StoreObject) Update(ctx context.Context) error {
	return ErrNotImplemented
}

func (o *StoreObject) Encode(typ wal.RecordType) ([]byte, error) {
	buf := bytes.NewBuffer(nil)

	// write tag
	buf.Write([]byte{byte(types.ObjectTagStore)})

	// delete records use a short encoding
	if typ == wal.RecordTypeDelete {
		binary.Write(buf, LE, o.id)
		return buf.Bytes(), nil
	}

	// write schema
	b, err := o.schema.MarshalBinary()
	if err != nil {
		return nil, err
	}
	binary.Write(buf, LE, uint32(len(b)))
	buf.Write(b)

	// write options
	b, err = schema.NewGenericEncoder[StoreOptions]().Encode(o.opts, nil)
	if err != nil {
		return nil, err
	}
	binary.Write(buf, LE, uint32(len(b)))
	buf.Write(b)

	return buf.Bytes(), nil
}

func (o *StoreObject) Decode(data []byte, typ wal.RecordType) error {
	if len(data) < 9 {
		return io.ErrShortBuffer
	}
	if data[0] != byte(types.ObjectTagStore) {
		return ErrInvalidObjectType
	}
	buf := bytes.NewBuffer(data[1:])

	// delete records use a short encoding
	if typ == wal.RecordTypeDelete {
		o.id = LE.Uint64(buf.Next(8))
		return nil
	}

	// read schema
	n := int(LE.Uint32(buf.Next(4)))
	o.schema = schema.NewSchema()
	if err := o.schema.UnmarshalBinary(buf.Next(n)); err != nil {
		return err
	}
	o.id = o.schema.TaggedHash(types.ObjectTagStore)

	// read options
	n = int(LE.Uint32(buf.Next(4)))
	_, err := schema.NewGenericDecoder[StoreOptions]().Decode(buf.Next(n), &o.opts)
	if err != nil {
		return err
	}
	return nil
}

// EnumObject
type EnumObject struct {
	id     uint64
	engine *Engine
	name   string
	vals   []string
}

func (o *EnumObject) Type() types.ObjectTag {
	return types.ObjectTagEnum
}

func (o *EnumObject) Create(ctx context.Context) error {
	e, ok := o.engine.GetEnum(o.id)
	if ok {
		return nil
	}
	_, err := o.engine.CreateEnum(ctx, e.Name())
	return err
}

func (o *EnumObject) Drop(ctx context.Context) error {
	_, ok := o.engine.GetEnum(o.id)
	if !ok {
		return nil
	}
	return o.engine.DropEnum(ctx, o.name)
}

func (o *EnumObject) Update(ctx context.Context) error {
	_, ok := o.engine.GetEnum(o.id)
	if !ok {
		return ErrNoEnum
	}
	return o.engine.ExtendEnum(ctx, o.name, o.vals...)
}

func (o *EnumObject) Encode(typ wal.RecordType) ([]byte, error) {
	buf := bytes.NewBuffer(nil)

	// write tag
	buf.Write([]byte{byte(types.ObjectTagEnum)})

	// write name
	binary.Write(buf, LE, uint16(len(o.name)))
	buf.WriteString(o.name)

	// delete records have shorter encoding
	if typ == wal.RecordTypeDelete {
		return buf.Bytes(), nil
	}

	// write values
	binary.Write(buf, LE, uint16(len(o.vals)))
	for _, v := range o.vals {
		binary.Write(buf, LE, uint16(len(v)))
		buf.WriteString(string(v))
	}

	return buf.Bytes(), nil
}

func (o *EnumObject) Decode(data []byte, typ wal.RecordType) error {
	if len(data) < 5 {
		return io.ErrShortBuffer
	}
	if data[0] != byte(types.ObjectTagEnum) {
		return ErrInvalidObjectType
	}
	buf := bytes.NewBuffer(data[1:])

	// read name
	n := int(LE.Uint16(buf.Next(2)))
	o.name = string(buf.Next(n))
	o.id = types.TaggedHash(types.ObjectTagEnum, o.name)

	// delete records have short encoding
	if typ == wal.RecordTypeDelete {
		return nil
	}

	// read values
	n = int(LE.Uint16(buf.Next(2)))
	o.vals = make([]string, n)
	for i := range o.vals {
		n = int(LE.Uint16(buf.Next(2)))
		o.vals[i] = string(buf.Next(n))
	}

	return nil
}

// IndexObject
type IndexObject struct {
	id     uint64
	engine *Engine
	table  string
	schema *schema.Schema
	opts   IndexOptions
}

func (o *IndexObject) Type() types.ObjectTag {
	return types.ObjectTagIndex
}

func (o *IndexObject) Create(ctx context.Context) error {
	_, ok := o.engine.GetIndex(o.id)
	if ok {
		return nil
	}
	_, err := o.engine.CreateIndex(ctx, o.table, o.schema, o.opts)
	return err
}

func (o *IndexObject) Drop(ctx context.Context) error {
	_, ok := o.engine.GetIndex(o.id)
	if !ok {
		return nil
	}
	return o.engine.DropIndex(ctx, o.schema.Name())
}

func (o *IndexObject) Update(ctx context.Context) error {
	return ErrNotImplemented
}

func (o *IndexObject) Encode(typ wal.RecordType) ([]byte, error) {
	buf := bytes.NewBuffer(nil)

	// write tag
	buf.Write([]byte{byte(types.ObjectTagIndex)})

	// delete records use a short encoding
	if typ == wal.RecordTypeDelete {
		binary.Write(buf, LE, o.id)
		return buf.Bytes(), nil
	}

	// write table name
	binary.Write(buf, LE, uint16(len(o.table)))
	buf.WriteString(o.table)

	// write schema
	b, err := o.schema.MarshalBinary()
	if err != nil {
		return nil, err
	}
	binary.Write(buf, LE, uint32(len(b)))
	buf.Write(b)

	// write options
	b, err = schema.NewGenericEncoder[IndexOptions]().Encode(o.opts, nil)
	if err != nil {
		return nil, err
	}
	binary.Write(buf, LE, uint32(len(b)))
	buf.Write(b)

	return buf.Bytes(), nil
}

func (o *IndexObject) Decode(data []byte, typ wal.RecordType) error {
	if len(data) < 11 {
		return io.ErrShortBuffer
	}
	if data[0] != byte(types.ObjectTagIndex) {
		return ErrInvalidObjectType
	}
	buf := bytes.NewBuffer(data[1:])

	// delete records use a short encoding
	if typ == wal.RecordTypeDelete {
		o.id = LE.Uint64(buf.Next(8))
		return nil
	}

	// read name
	n := int(LE.Uint16(buf.Next(2)))
	o.table = string(buf.Next(n))

	// read schema
	n = int(LE.Uint32(buf.Next(4)))
	o.schema = schema.NewSchema()
	if err := o.schema.UnmarshalBinary(buf.Next(n)); err != nil {
		return err
	}
	o.id = o.schema.TaggedHash(types.ObjectTagIndex)

	// read options
	n = int(LE.Uint32(buf.Next(4)))
	_, err := schema.NewGenericDecoder[IndexOptions]().Decode(buf.Next(n), &o.opts)
	if err != nil {
		return err
	}
	return nil
}
