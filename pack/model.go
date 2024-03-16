// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"context"

	"blockwatch.cc/knoxdb/encoding/bitmap"
)

// Marshaler interface, a more performant alternative to type based
// {Get|Put|Delete}Value methods, but incompatible with secondary indexes.
type KeyValueMarshaler interface {
	MarshalKey() ([]byte, error)
	MarshalValue() ([]byte, error)
}

type KeyValueUnmarshaler interface {
	UnmarshalKey([]byte) error
	UnmarshalValue([]byte) error
}

type TableEngine string

const (
	TableEnginePack = "pack"
	TableEngineKV   = "kv"
)

type Table interface {
	Name() string
	Engine() TableEngine
	DB() *DB
	Options() Options
	IsClosed() bool
	Fields() FieldList
	Stats() []TableStats
	PurgeCache()
	Close() error
	Drop() error
	Sync(context.Context) error
	Compact(context.Context) error

	Insert(context.Context, any) error
	Update(context.Context, any) error
	Query(context.Context, Query) (*Result, error)
	Stream(context.Context, Query, func(Row) error) error
	Count(context.Context, Query) (int64, error)
	Delete(context.Context, Query) (int64, error)
	LookupPks(context.Context, []uint64) (*Result, error)
	StreamPks(context.Context, []uint64, func(Row) error) error
	DeletePks(context.Context, []uint64) (int64, error)

	CreateIndex(IndexKind, FieldList, Options) error
	CreateIndexIfNotExists(IndexKind, FieldList, Options) error
	DropIndex(FieldList) error
	QueryIndexesTx(context.Context, *Tx, *ConditionTreeNode) (int, error)
}

type Store interface {
	Name() string
	Engine() string
	DB() *DB
	Options() Options
	IsClosed() bool
	Stats() []TableStats
	PurgeCache()
	Close() error
	Drop() error

	Get(key []byte) ([]byte, error)
	GetTx(tx *Tx, key []byte) []byte
	Get64(key uint64) ([]byte, error)
	GetValue64(key uint64, val any) error
	Put(key, val []byte) error
	PutTx(tx *Tx, key, val []byte) ([]byte, error)
	Put64(key uint64, val []byte) error
	PutValue64(key uint64, val any) error
	Del(key []byte) error
	DelTx(tx *Tx, key []byte) ([]byte, error)
	Del64(key uint64) error
	DeleteValue64(key uint64) error
	PrefixRange(prefix []byte, fn func(k, v []byte) error) error
	Range(from, to []byte, fn func(k, v []byte) error) error
}

type Index interface {
	Name() string
	Kind() IndexKind
	Drop() error
	PurgeCache()
	Fields() FieldList
	Stats() TableStats
	AddTx(tx *Tx, prev, val []byte) error
	DelTx(tx *Tx, prev []byte) error
	ScanTx(tx *Tx, prefix []byte) (bitmap.Bitmap, error)
	RangeTx(tx *Tx, from, to []byte) (bitmap.Bitmap, error)
	CloseTx(*Tx) error
}
