// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package knox

import (
	"context"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/schema"
)

// EXTERNAL user interface implemented by local and remote clients
type (
	DatabaseOptions = engine.DatabaseOptions
	TableOptions    = engine.TableOptions
	IndexOptions    = engine.IndexOptions
	StoreOptions    = engine.StoreOptions

	TableKind = engine.TableKind
	StoreKind = engine.StoreKind
	IndexKind = engine.IndexKind
	IndexType = types.IndexType

	StoreStats = engine.StoreStats
	TableStats = engine.TableStats

	QueryResult = engine.QueryResult
	QueryRow    = engine.QueryRow
)

const (
	TableKindPack = engine.TableKindPack
	TableKindLSM  = engine.TableKindLSM

	IndexKindPack = engine.IndexKindPack
	IndexKindLSM  = engine.IndexKindLSM
)

const (
	IndexTypeNone      = types.IndexTypeNone
	IndexTypeHash      = types.IndexTypeHash
	IndexTypeInt       = types.IndexTypeInt
	IndexTypeComposite = types.IndexTypeComposite
	IndexTypeBloom     = types.IndexTypeBloom
	IndexTypeBfuse     = types.IndexTypeBfuse
	IndexTypeBits      = types.IndexTypeBits
)

// type QueryResult interface {
// 	io.ReadCloser
// 	Bytes() []byte
// }

type QueryRequest interface {
	Encode() ([]byte, error)
	MakePlan() (engine.QueryPlan, error)
}

// external user interface
type Table interface {
	Schema() *schema.Schema
	Stats() TableStats
	Engine() engine.TableEngine
	Insert(context.Context, any) (uint64, error)
	Update(context.Context, any) (uint64, error)
	Delete(context.Context, QueryRequest) (uint64, error)
	Count(context.Context, QueryRequest) (uint64, error)
	Query(context.Context, QueryRequest) (QueryResult, error)
	Stream(context.Context, QueryRequest, func(QueryRow) error) error
}

// external user interface
type Store interface {
	Schema() *schema.Schema
	Stats() StoreStats
	Get(ctx context.Context, key []byte) ([]byte, error)
	Put(ctx context.Context, key, val []byte) error
	Del(ctx context.Context, key []byte) error
	Range(ctx context.Context, prefix []byte, fn func(ctx context.Context, k, v []byte) error) error
	Scan(ctx context.Context, from, to []byte, fn func(ctx context.Context, k, v []byte) error) error
}

// type Tx interface {
// 	Commit(ctx context.Context) error
// 	Abort(ctx context.Context) error
// }

type Database interface {
	// db global
	Sync(ctx context.Context) error
	Begin(ctx context.Context) (context.Context, func() error, func() error)
	Close(ctx context.Context) error

	// tables
	ListTables() []string
	CreateTable(ctx context.Context, s *schema.Schema, opts TableOptions) (Table, error)
	UseTable(name string) (Table, error)
	DropTable(ctx context.Context, name string) error
	AlterTable(ctx context.Context, name string, s *schema.Schema) error
	TruncateTable(ctx context.Context, name string) error
	CompactTable(ctx context.Context, name string) error

	// indexes
	ListIndexes(name string) []string
	CreateIndex(ctx context.Context, name string, table Table, s *schema.Schema, opts IndexOptions) error
	RebuildIndex(ctx context.Context, name string) error
	DropIndex(ctx context.Context, name string) error

	// stores
	ListStores() []string
	CreateStore(ctx context.Context, s *schema.Schema, opts StoreOptions) (Store, error)
	UseStore(name string) (Store, error)
	DropStore(ctx context.Context, name string) error

	// enums
	ListEnums() []string
	UseEnum(name string) (schema.EnumLUT, error)
	CreateEnum(ctx context.Context, name string) (schema.EnumLUT, error)
	ExtendEnum(ctx context.Context, name string, vals ...schema.Enum) error
	DropEnum(ctx context.Context, name string) error
}
