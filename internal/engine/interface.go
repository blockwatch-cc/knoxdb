// Copyright (c) 2024-2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"context"
	"iter"
	"time"

	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/xroar"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
)

type (
	Context     = context.Context
	Schema      = schema.Schema
	Batch       = schema.Batch
	TableSchema = types.TableSchema
	IndexSchema = schema.IndexSchema
	View        = schema.View
	Bitmap      = xroar.Bitmap
	OrderType   = types.OrderType
	FilterMode  = types.FilterMode
	Package     = pack.Package
	WriteMode   = pack.WriteMode
	XID         = types.XID
)

type TableKind string

const (
	TableKindPack    = "pack"
	TableKindHistory = "history"
)

type TableFactory func() TableEngine

// backpressure channel to signal caller should block
type WaitCh <-chan struct{}

// internal interface required for all table engines
type TableEngine interface {
	Create(Context, *TableSchema, ...Option) error
	Open(Context, *TableSchema, ...Option) error
	Close(Context) error
	Schema() *TableSchema
	State() ObjectState
	Metrics() TableMetrics
	Drop(Context) error
	Sync(Context) error
	Compact(Context) error
	Truncate(Context) error
	Checkpoint(Context) error

	// data ingress
	InsertBatch(Context, *Batch) (uint64, int, error)
	InsertInto(Context, *Package) (uint64, int, error)
	ImportInto(Context, *Package) (uint64, int, error)
	UpdateBatch(Context, *Batch) (int, error)
	Update(Context, QueryPlan) (int, error)

	// data egress
	Query(Context, QueryPlan) (QueryResult, error)
	Count(Context, QueryPlan) (int, error)
	Delete(Context, QueryPlan) (int, error)
	Stream(Context, QueryPlan, func(QueryRow) error) error

	// index management
	ConnectIndex(QueryableIndex)
	DisconnectIndex(QueryableIndex)
	Indexes() []QueryableIndex
	PkIndex() (QueryableIndex, bool)

	// Tx Management
	CommitTx(ctx Context, xid XID) WaitCh
	AbortTx(ctx Context, xid XID)

	// data handling
	NewReader() TableReader
	NewWriter(uint32) TableWriter
}

type ReadMode byte

const (
	ReadModeAll = iota
	ReadModeIncludeMask
	ReadModeExcludeMask
)

type StatsReader interface {
	MinMax(int) (any, any)
}

type TableReader interface {
	WithQuery(QueryPlan) TableReader
	WithMask(*Bitmap, ReadMode) TableReader
	WithFields([]uint16) TableReader
	Next(Context) (*Package, error)
	Read(Context, uint32) (*Package, error)
	Reset()
	Close()
	Schema() *TableSchema
	Epoch() uint32
}

type TableWriter interface {
	Append(Context, *Package, WriteMode) error
	Replace(Context, *Package, WriteMode) error
	AppendIndexes(Context, *Package, WriteMode) error
	DeleteIndexes(Context, *Package, WriteMode) error
	Finalize(Context, ObjectState) error
	Close()
	Epoch() uint32
	GC() error
}

type QueryPlan interface {
	Schema() *Schema
	Validate() error
	Compile(ctx Context) error
	Close()
	// Stream(ctx Context, fn func(r QueryRow) error) error
	// Query(ctx Context) (QueryResult, error)
}

type QueryCondition interface {
	IsLeaf() bool
	IsProcessed() bool
	IsNoMatch() bool
	IsAnyMatch() bool
	Fields() []string
}

type QueryableIndex interface {
	IndexSchema() *IndexSchema
	Schema() *Schema
	IsComposite() bool
	IsPk() bool
	CanMatch(QueryCondition) bool
	Query(Context, QueryCondition) (*Bitmap, bool, error)
	QueryComposite(Context, QueryCondition) (*Bitmap, bool, error)
	Lookup(Context, map[uint64]uint64) error
}

type QueryableTable interface {
	Schema() *TableSchema
	Indexes() []QueryableIndex
	Query(Context, QueryPlan) (QueryResult, error)
	Stream(Context, QueryPlan, func(QueryRow) error) error
}

type QueryResultConsumer interface {
	Append(Context, *Package) error
	Len() int
}

type QueryResult interface {
	Schema() *Schema
	Pack() *Package
	Len() int
	Row(n int) QueryRow
	Record(n int) []byte
	Close()
	Encode() []byte
	SortBy(name string, order OrderType)
	Iterator() iter.Seq2[int, QueryRow]
	Value(int, int) any
	// Column(name string) (any, error)
	// TODO: Chunk and Vector access
}

type QueryRow interface {
	Schema() *Schema
	Record() []byte
	Decode(any) error
	Reset()

	Get(int) any
	Uint64(col int) uint64
	Uint32(col int) uint32
	Uint16(col int) uint16
	Uint8(col int) uint8
	Int256(col int) num.Int256
	Int128(col int) num.Int128
	Int64(col int) int64
	Int32(col int) int32
	Int16(col int) int16
	Int8(col int) int8
	Decimal256(col int) num.Decimal256
	Decimal128(col int) num.Decimal128
	Decimal64(col int) num.Decimal64
	Decimal32(col int) num.Decimal32
	Float64(col int) float64
	Float32(col int) float32
	String(col int) string
	Bytes(col int) []byte
	Bool(col int) bool
	Time(col int) time.Time
	Big(col int) num.Big
	Enum(col int) string
}

type IndexKind string

const (
	IndexKindPack = "pack"
)

type IndexFactory func() IndexEngine

// internal interface required for all index engines
type IndexEngine interface {
	Create(Context, TableEngine, *IndexSchema, ...Option) error
	Open(Context, TableEngine, *IndexSchema, ...Option) error
	Close(Context) error
	IndexSchema() *IndexSchema
	Schema() *Schema
	Table() TableEngine
	Metrics() IndexMetrics
	Drop(Context) error
	Truncate(Context) error
	Rebuild(Context) error
	Sync(Context) error

	// data ingress from table merge
	AddPack(Context, *Package, WriteMode) error
	DelPack(Context, *Package, WriteMode, uint32) error
	Finalize(Context, uint32) error
	GC(Context, uint32) error

	// data egress
	IsComposite() bool
	IsPk() bool
	CanMatch(QueryCondition) bool // static: based to index engine type
	Query(Context, QueryCondition) (*Bitmap, bool, error)
	QueryComposite(Context, QueryCondition) (*Bitmap, bool, error)
	Lookup(Context, map[uint64]uint64) error
}

type ConditionMatcher interface {
	// MatchView(*View) bool
	Overlaps(ConditionMatcher) bool
}

// all objects that support tracking tx info
type TxTracker interface {
	CommitTx(ctx Context, xid XID) WaitCh
	AbortTx(ctx Context, xid XID)
}
