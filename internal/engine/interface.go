// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"context"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/wal"
	"blockwatch.cc/knoxdb/pkg/bitmap"
	"blockwatch.cc/knoxdb/pkg/schema"
)

type (
	Context    = context.Context
	Schema     = schema.Schema
	Bitmap     = bitmap.Bitmap
	OrderType  = types.OrderType
	FilterMode = types.FilterMode
)

type TableKind string

const (
	TableKindPack = "pack"
	TableKindLSM  = "lsm"
)

type TableFactory func() TableEngine

// internal interface required for all table engines
type TableEngine interface {
	Create(Context, *Schema, TableOptions) error
	Open(Context, *Schema, TableOptions) error
	Close(Context) error
	Schema() *Schema
	State() ObjectState
	Metrics() TableMetrics
	Drop(Context) error
	Sync(Context) error
	Compact(Context) error
	Truncate(Context) error

	// data ingress
	InsertRows(Context, []byte) (uint64, error) // wire encoded rows
	UpdateRows(Context, []byte) (uint64, error) // wire encoded rows
	ApplyWalRecord(Context, *wal.Record) error

	// data egress
	Query(Context, QueryPlan) (QueryResult, error)
	Count(Context, QueryPlan) (uint64, error)
	Delete(Context, QueryPlan) (uint64, error)
	Stream(Context, QueryPlan, func(QueryRow) error) error
	Lookup(Context, []uint64) (QueryResult, error)
	StreamLookup(Context, []uint64, func(QueryRow) error) error

	// index management
	UseIndex(IndexEngine)
	UnuseIndex(IndexEngine)
	Indexes() []IndexEngine
}

type QueryPlan interface {
	Schema() *Schema
	Validate() error
	Compile(ctx Context) error
	Close()
	Stream(ctx Context, fn func(r QueryRow) error) error
	Query(ctx Context) (QueryResult, error)
}

type QueryResult interface {
	Schema() *Schema
	Len() int
	Row(n int) QueryRow
	Record(n int) []byte
	Close()
	Bytes() []byte
	SortBy(name string, order OrderType)
	ForEach(fn func(r QueryRow) error) error
	Column(name string) (any, error)
}

type QueryRow interface {
	Schema() *Schema
	Bytes() []byte
	Decode(any) error
	Field(string) (any, error)
	Index(int) (any, error)
}

type IndexKind string

const (
	IndexKindPack = "pack"
	IndexKindLSM  = "lsm"
)

type IndexFactory func() IndexEngine

// internal interface required for all index engines
type IndexEngine interface {
	Create(Context, TableEngine, *Schema, IndexOptions) error
	Open(Context, TableEngine, *Schema, IndexOptions) error
	Close(Context) error
	Schema() *Schema
	Table() TableEngine
	Metrics() IndexMetrics
	Drop(Context) error
	Truncate(Context) error
	Rebuild(Context) error
	Sync(Context) error

	// data ingress
	Add(ctx Context, prev, val []byte) error // wire encoded rows
	Del(ctx Context, prev []byte) error      // wire encoded rows

	// data egress
	IsComposite() bool
	CanMatch(QueryCondition) bool // static: based to index engine type
	Query(Context, QueryCondition) (*Bitmap, bool, error)
	QueryComposite(Context, QueryCondition) (*Bitmap, bool, error)
}

type QueryCondition interface {
	IsLeaf() bool
	IsEmpty() bool
	IsEmptyMatch() bool
	IsProcessed() bool
	Fields() []string
}

type StoreKind string

const (
	StoreKindKV = "kv"
)

type StoreFactory func() StoreEngine

// internal interface required for all store engines
type StoreEngine interface {
	Create(Context, *Schema, StoreOptions) error
	Open(Context, *Schema, StoreOptions) error
	Close(Context) error
	Schema() *Schema
	State() ObjectState
	Metrics() StoreMetrics
	Drop(Context) error

	// data interface
	Get(ctx Context, key []byte) ([]byte, error)
	Put(ctx Context, key, val []byte) error
	Del(ctx Context, key []byte) error
	Range(ctx Context, prefix []byte, fn func(ctx Context, k, v []byte) error) error
	Scan(ctx Context, from, to []byte, fn func(ctx Context, k, v []byte) error) error
	ApplyWalRecord(Context, *wal.Record) error
}
