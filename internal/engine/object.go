// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"path/filepath"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/wal"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/store"
)

type ActionType = wal.RecordType

const (
	CREATE = wal.RecordTypeInsert
	ALTER  = wal.RecordTypeUpdate
	DROP   = wal.RecordTypeDelete
)

type Object interface {
	Id() uint64
	Type() types.ObjectTag
	Action() wal.RecordType
	Create(Context) error
	Drop(Context) error
	Update(Context) error
	Encode() ([]byte, error)
	Decode(Context, *wal.Record) error
}

// TableObject
type TableObject struct {
	id     uint64
	action wal.RecordType
	cat    *Catalog
	schema *schema.Schema
	opts   TableOptions
}

func (c *Catalog) AppendTableCmd(ctx context.Context, act ActionType, s *schema.Schema, opts TableOptions) error {
	obj := &TableObject{
		cat:    c,
		id:     s.TaggedHash(types.ObjectTagTable),
		schema: s,
		opts:   opts,
		action: act,
	}
	return c.append(ctx, obj)
}

func (o *TableObject) Id() uint64 {
	return o.id
}

func (o *TableObject) Action() wal.RecordType {
	return o.action
}

func (o *TableObject) Type() types.ObjectTag {
	return types.ObjectTagTable
}

func (o *TableObject) Create(ctx context.Context) error {
	return o.cat.AddTable(ctx, o.id, o.schema, o.opts)
}

func (o *TableObject) Drop(ctx context.Context) error {
	// remove files (only if table data was found in catalog during decode)
	if o.schema != nil && !o.opts.ReadOnly {
		_ = store.Drop(o.opts.Driver, filepath.Join(o.cat.path, o.schema.Name))
	}
	return o.cat.DropTable(ctx, o.id)
}

func (o *TableObject) Update(ctx context.Context) error {
	return ErrNotImplemented
}

func (o *TableObject) Encode() ([]byte, error) {
	buf := bytes.NewBuffer(nil)

	// write tag
	buf.Write([]byte{byte(types.ObjectTagTable)})

	// delete records use a short encoding
	if o.action == wal.RecordTypeDelete {
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

func (o *TableObject) Decode(ctx context.Context, rec *wal.Record) error {
	buf := bytes.NewBuffer(rec.Data[0])
	if buf.Len() < 9 {
		return io.ErrShortBuffer
	}
	if buf.Next(1)[0] != byte(types.ObjectTagTable) {
		return ErrInvalidObjectType
	}
	o.action = rec.Type

	// delete records use a short encoding
	if rec.Type == wal.RecordTypeDelete {
		o.id = LE.Uint64(buf.Next(8))

		// load schema and opts from catalog if exist
		o.schema, o.opts, _ = o.cat.GetTable(ctx, o.id)

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
	action wal.RecordType
	cat    *Catalog
	schema *schema.Schema
	opts   StoreOptions
}

func (c *Catalog) AppendStoreCmd(ctx context.Context, act ActionType, s *schema.Schema, opts StoreOptions) error {
	obj := &StoreObject{
		cat:    c,
		id:     s.TaggedHash(types.ObjectTagStore),
		schema: s,
		opts:   opts,
		action: act,
	}
	return c.append(ctx, obj)
}

func (o *StoreObject) Id() uint64 {
	return o.id
}

func (o *StoreObject) Action() wal.RecordType {
	return o.action
}

func (o *StoreObject) Type() types.ObjectTag {
	return types.ObjectTagStore
}

func (o *StoreObject) Create(ctx context.Context) error {
	return o.cat.AddStore(ctx, o.id, o.schema, o.opts)
}

func (o *StoreObject) Drop(ctx context.Context) error {
	// remove files (only if table data was found in catalog during decode)
	if o.schema != nil && !o.opts.ReadOnly {
		_ = store.Drop(o.opts.Driver, filepath.Join(o.cat.path, o.schema.Name))
	}
	return o.cat.DropStore(ctx, o.id)
}

func (o *StoreObject) Update(ctx context.Context) error {
	return ErrNotImplemented
}

func (o *StoreObject) Encode() ([]byte, error) {
	buf := bytes.NewBuffer(nil)

	// write tag
	buf.Write([]byte{byte(types.ObjectTagStore)})

	// delete records use a short encoding
	if o.action == wal.RecordTypeDelete {
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

func (o *StoreObject) Decode(ctx context.Context, rec *wal.Record) error {
	buf := bytes.NewBuffer(rec.Data[0])
	if buf.Len() < 9 {
		return io.ErrShortBuffer
	}
	if buf.Next(1)[0] != byte(types.ObjectTagStore) {
		return ErrInvalidObjectType
	}
	o.action = rec.Type

	// delete records use a short encoding
	if rec.Type == wal.RecordTypeDelete {
		o.id = LE.Uint64(buf.Next(8))

		// load schema and opts from catalog if exist
		o.schema, o.opts, _ = o.cat.GetStore(ctx, o.id)

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
	action wal.RecordType
	cat    *Catalog
	name   string
	vals   []string
}

func (c *Catalog) AppendEnumCmd(ctx context.Context, act ActionType, e *schema.EnumDictionary) error {
	obj := &EnumObject{
		cat:    c,
		id:     e.Tag(),
		name:   e.Name(),
		vals:   e.Values(),
		action: act,
	}
	return c.append(ctx, obj)
}

func (o *EnumObject) Id() uint64 {
	return o.id
}

func (o *EnumObject) Action() wal.RecordType {
	return o.action
}

func (o *EnumObject) Type() types.ObjectTag {
	return types.ObjectTagEnum
}

func (o *EnumObject) Create(ctx context.Context) error {
	enum := schema.NewEnumDictionary(o.name)
	_ = enum.Append(o.vals...)
	return o.cat.AddEnum(ctx, enum)
}

func (o *EnumObject) Drop(ctx context.Context) error {
	return o.cat.DropEnum(ctx, o.id)
}

func (o *EnumObject) Update(ctx context.Context) error {
	enum := schema.NewEnumDictionary(o.name)
	_ = enum.Append(o.vals...)
	return o.cat.PutEnum(ctx, enum)
}

func (o *EnumObject) Encode() ([]byte, error) {
	buf := bytes.NewBuffer(nil)

	// write tag
	buf.Write([]byte{byte(types.ObjectTagEnum)})

	// write name
	binary.Write(buf, LE, uint16(len(o.name)))
	buf.WriteString(o.name)

	// delete records have shorter encoding
	if o.action == wal.RecordTypeDelete {
		return buf.Bytes(), nil
	}

	// write values
	binary.Write(buf, LE, uint16(len(o.vals)))
	for _, v := range o.vals {
		binary.Write(buf, LE, uint16(len(v)))
		buf.WriteString(v)
	}

	return buf.Bytes(), nil
}

func (o *EnumObject) Decode(ctx context.Context, rec *wal.Record) error {
	buf := bytes.NewBuffer(rec.Data[0])
	if buf.Len() < 5 {
		return io.ErrShortBuffer
	}
	if buf.Next(1)[0] != byte(types.ObjectTagEnum) {
		return ErrInvalidObjectType
	}
	o.action = rec.Type

	// read name
	n := int(LE.Uint16(buf.Next(2)))
	o.name = string(buf.Next(n))
	o.id = types.TaggedHash(types.ObjectTagEnum, o.name)

	// delete records have short encoding
	if rec.Type == wal.RecordTypeDelete {
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
	action wal.RecordType
	cat    *Catalog
	table  string
	schema *schema.IndexSchema
	opts   IndexOptions
}

func (c *Catalog) AppendIndexCmd(ctx context.Context, act ActionType, s *schema.IndexSchema, opts IndexOptions) error {
	obj := &IndexObject{
		cat:    c,
		id:     s.TaggedHash(types.ObjectTagIndex),
		schema: s,
		opts:   opts,
		table:  s.Base.Name,
		action: act,
	}
	return c.append(ctx, obj)
}

func (o *IndexObject) Id() uint64 {
	return o.id
}

func (o *IndexObject) Action() wal.RecordType {
	return o.action
}

func (o *IndexObject) Type() types.ObjectTag {
	return types.ObjectTagIndex
}

func (o *IndexObject) Create(ctx context.Context) error {
	tkey := types.TaggedHash(types.ObjectTagTable, o.table)
	return o.cat.AddIndex(ctx, o.id, tkey, o.schema, o.opts)
}

func (o *IndexObject) Drop(ctx context.Context) error {
	// remove files (only if table data was found in catalog during decode)
	if o.schema != nil && !o.opts.ReadOnly {
		_ = store.Drop(o.opts.Driver, filepath.Join(o.cat.path, o.schema.Name))
	}
	return o.cat.DropIndex(ctx, o.id)
}

func (o *IndexObject) Update(ctx context.Context) error {
	return ErrNotImplemented
}

func (o *IndexObject) Encode() ([]byte, error) {
	buf := bytes.NewBuffer(nil)

	// write tag
	buf.Write([]byte{byte(types.ObjectTagIndex)})

	// delete records use a short encoding
	if o.action == wal.RecordTypeDelete {
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

func (o *IndexObject) Decode(ctx context.Context, rec *wal.Record) error {
	buf := bytes.NewBuffer(rec.Data[0])
	if buf.Len() < 11 {
		return io.ErrShortBuffer
	}
	if buf.Next(1)[0] != byte(types.ObjectTagIndex) {
		return ErrInvalidObjectType
	}
	o.action = rec.Type

	// delete records use a short encoding
	if rec.Type == wal.RecordTypeDelete {
		o.id = LE.Uint64(buf.Next(8))

		// load schema and opts from catalog if exist
		o.schema, o.opts, _ = o.cat.GetIndex(ctx, o.id)

		return nil
	}

	// read name
	n := int(LE.Uint16(buf.Next(2)))
	o.table = string(buf.Next(n))

	// read schema
	n = int(LE.Uint32(buf.Next(4)))
	o.schema = &schema.IndexSchema{}
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
