// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package knox

import (
	"context"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/schema/enum"
)

// EXTERNAL user interface implemented by local and remote clients
type (
	Options = engine.Options
	Option  = engine.Option

	TableKind = engine.TableKind
	IndexKind = engine.IndexKind

	TableMetrics = engine.TableMetrics
	IndexMetrics = engine.IndexMetrics

	QueryResult = engine.QueryResult
	QueryRow    = engine.QueryRow
)

var (
	WithNamespace       = engine.WithNamespace
	WithPath            = engine.WithPath
	WithCacheSize       = engine.WithCacheSize
	WithWalSegmentSize  = engine.WithWalSegmentSize
	WithWalRecoveryMode = engine.WithWalRecoveryMode
	WithLockTimeout     = engine.WithLockTimeout
	WithTxWaitTimeout   = engine.WithTxWaitTimeout
	WithMaxWorkers      = engine.WithMaxWorkers
	WithMaxTasks        = engine.WithMaxTasks
	WithLogger          = engine.WithLogger
	WithEngineType      = engine.WithEngineType
	WithPackSize        = engine.WithPackSize
	WithJournalSize     = engine.WithJournalSize
	WithJournalSegments = engine.WithJournalSegments
	WithDriverType      = engine.WithDriverType
	WithTxMaxSize       = engine.WithTxMaxSize
	WithPageSize        = engine.WithPageSize
	WithPageFill        = engine.WithPageFill
	WithNoSync          = engine.WithNoSync
	WithReadOnly        = engine.WithReadOnly
)

const (
	TableKindPack = engine.TableKindPack
	IndexKindPack = engine.IndexKindPack
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
	DB() Database
	Schema() *schema.Schema
	Metrics() TableMetrics
	Engine() engine.TableEngine
	Insert(context.Context, *schema.Batch) (uint64, int, error)
	Update(context.Context, *schema.Batch) (int, error)
	Delete(context.Context, QueryRequest) (int, error)
	Count(context.Context, QueryRequest) (int, error)
	Query(context.Context, QueryRequest) (QueryResult, error)
	Stream(context.Context, QueryRequest, func(QueryRow) error) error
}

type Index interface {
	DB() Database
	Schema() *schema.Schema
	IndexSchema() *schema.IndexSchema
	Metrics() IndexMetrics
	Engine() engine.IndexEngine
}

type Database interface {
	// db global
	Sync(ctx context.Context) error
	Begin(ctx context.Context, flags ...TxFlags) (context.Context, func() error, func() error, error)
	Close(ctx context.Context) error

	// tables
	ListTables() []string
	CreateTable(ctx context.Context, s *schema.Schema, opts ...Option) (Table, error)
	FindTable(name string) (Table, error)
	GetTable(tag uint64) (Table, bool)
	DropTable(ctx context.Context, name string) error
	AlterTable(ctx context.Context, name string, s *schema.Schema) error
	TruncateTable(ctx context.Context, name string) error
	CompactTable(ctx context.Context, name string) error

	// indexes
	ListIndexes(name string) []string
	FindIndex(name string) (Index, error)
	CreateIndex(ctx context.Context, s *schema.IndexSchema, opts ...Option) error
	RebuildIndex(ctx context.Context, name string) error
	DropIndex(ctx context.Context, name string) error

	// enums
	ListEnums() []string
	FindEnum(name string) (*enum.Dictionary, error)
	CreateEnum(ctx context.Context, name string) (*enum.Dictionary, error)
	ExtendEnum(ctx context.Context, name string, vals ...string) error
	DropEnum(ctx context.Context, name string) error
}
