// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"path/filepath"
	"time"

	"blockwatch.cc/knoxdb/internal/wal"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/store"
	"github.com/echa/log"
)

type Option func(o *Options)

type Options struct {
	// engine options
	Namespace       string           // unique db identifier
	Path            string           // local filesystem
	CacheSize       int              // in bytes
	WalSegmentSize  int              // wal file size
	WalRecoveryMode wal.RecoveryMode // howto recover from wal damage
	LockTimeout     time.Duration    // lock manager timeout
	TxWaitTimeout   time.Duration    // write tx timeout
	MaxWorkers      int              // max number of parallel worker goroutines
	MaxTasks        int              // max number of tasks waiting for execution
	Log             log.Logger       `knox:"-"`

	// table & index options
	Engine          string // pack (lsm, parquet, csv, remote maybe later)
	PackSize        int    // pack engine
	JournalSize     int    // pack engine
	JournalSegments int    // pack engine

	// store options
	Driver    string  // bolt, mem, ...
	TxMaxSize int     // maximum write size of low-level dbfile transactions
	PageSize  int     // boltdb
	PageFill  float64 // boltdb
	NoSync    bool    // boltdb, no fsync on transactions (dangerous)
	ReadOnly  bool    // read-only tx and no schema changes
	IsTemp    bool    // drop database on close
}

func (o Options) Apply(opts ...Option) Options {
	ocopy := o
	for _, opt := range opts {
		opt(&ocopy)
	}
	return ocopy
}

func (o Options) MarshalBinary() ([]byte, error) {
	enc := schema.NewGenericEncoder[Options]()
	return enc.Encode(o, nil)
}

func (o *Options) UnmarshalBinary(buf []byte) error {
	dec := schema.NewGenericDecoder[Options]()
	_, err := dec.Decode(buf, o)
	return err
}

func (o Options) DatabaseOptions() []Option {
	return []Option{
		WithNamespace(o.Namespace),
		WithPath(o.Path),
		WithCacheSize(o.CacheSize),
		WithWalSegmentSize(o.WalSegmentSize),
		WithWalRecoveryMode(o.WalRecoveryMode),
		WithLockTimeout(o.LockTimeout),
		WithTxWaitTimeout(o.TxWaitTimeout),
		WithMaxWorkers(o.MaxWorkers),
		WithMaxTasks(o.MaxTasks),
		WithLogger(o.Log),
		WithEngineType(o.Engine),
		WithPackSize(o.PackSize),
		WithJournalSize(o.JournalSize),
		WithJournalSegments(o.JournalSegments),
		WithDriverType(o.Driver),
		WithTxMaxSize(o.TxMaxSize),
		WithPageSize(o.PageSize),
		WithPageFill(o.PageFill),
		WithNoSync(o.NoSync),
		WithReadOnly(o.ReadOnly),
	}
}

func (o Options) FixedOptions() []Option {
	return []Option{
		WithNamespace(o.Namespace),
		WithWalSegmentSize(o.WalSegmentSize),
		WithEngineType(o.Engine),
		WithPackSize(o.PackSize),
		WithJournalSize(o.JournalSize),
		WithDriverType(o.Driver),
		WithPageSize(o.PageSize),
	}
}

func (o Options) TableOptions() []Option {
	return []Option{
		WithPackSize(o.PackSize),
		WithJournalSize(o.JournalSize),
		WithJournalSegments(o.JournalSegments),
		WithDriverType(o.Driver),
		WithTxMaxSize(o.TxMaxSize),
		WithPageSize(o.PageSize),
		WithPageFill(o.PageFill),
		WithNoSync(o.NoSync),
		WithReadOnly(o.ReadOnly),
		WithDropOnClose(o.IsTemp),
		WithLogger(o.Log),
	}
}

func (o Options) IndexOptions() []Option {
	return []Option{
		WithPackSize(o.PackSize),
		WithJournalSize(o.JournalSize),
		WithJournalSegments(o.JournalSegments),
		WithDriverType(o.Driver),
		WithTxMaxSize(o.TxMaxSize),
		WithPageSize(o.PageSize),
		WithPageFill(o.PageFill),
		WithNoSync(o.NoSync),
		WithReadOnly(o.ReadOnly),
		WithDropOnClose(o.IsTemp),
		WithLogger(o.Log),
	}
}

func (o Options) CatalogOptions(dbName string) []store.Option {
	return []store.Option{
		store.WithPath(filepath.Join(o.Path, dbName, CATALOG_NAME)),
		store.WithManifest(store.NewManifest(dbName, CATALOG_TYPE)),
		store.WithDriver(o.Driver),
		store.WithPageSize(o.PageSize),
		store.WithPageFill(o.PageFill),
		store.WithLogger(o.Log),
		store.WithNoSync(o.NoSync),
		store.WithDropOnClose(o.IsTemp),
		store.WithReadonly(o.ReadOnly),
	}
}

func (o Options) StoreOptions() []store.Option {
	return []store.Option{
		store.WithDriver(o.Driver),
		store.WithPageSize(o.PageSize),
		store.WithPageFill(o.PageFill),
		store.WithLogger(o.Log),
		store.WithNoSync(o.NoSync),
		store.WithDropOnClose(o.IsTemp),
		store.WithReadonly(o.ReadOnly),
	}
}

func WithNamespace(s string) Option {
	return func(o *Options) {
		if s != "" {
			o.Namespace = s
		}
	}
}

func WithPath(s string) Option {
	return func(o *Options) {
		if s != "" {
			o.Path = s
		}
	}
}

func WithCacheSize(n int) Option {
	return func(o *Options) {
		o.CacheSize = n
	}
}

func WithWalSegmentSize(sz int) Option {
	return func(o *Options) {
		if sz > 0 {
			o.WalSegmentSize = sz
		}
	}
}

func WithWalRecoveryMode(mode wal.RecoveryMode) Option {
	return func(o *Options) {
		o.WalRecoveryMode = mode
	}
}

func WithLockTimeout(to time.Duration) Option {
	return func(o *Options) {
		o.LockTimeout = to
	}
}

func WithTxWaitTimeout(to time.Duration) Option {
	return func(o *Options) {
		o.TxWaitTimeout = to
	}
}

func WithMaxWorkers(n int) Option {
	return func(o *Options) {
		if n > 0 {
			o.MaxWorkers = n
		}
	}
}

func WithMaxTasks(n int) Option {
	return func(o *Options) {
		if n > 0 {
			o.MaxTasks = n
		}
	}
}

func WithLogger(l log.Logger) Option {
	return func(o *Options) {
		if l != nil {
			o.Log = l
		}
	}
}

func WithEngineType(e string) Option {
	return func(o *Options) {
		if e != "" {
			o.Engine = e
		}
	}
}

func WithPackSize(n int) Option {
	return func(o *Options) {
		if n > 0 {
			o.PackSize = n
		}
	}
}

func WithJournalSize(n int) Option {
	return func(o *Options) {
		if n > 0 {
			o.JournalSize = n
		}
	}
}

func WithJournalSegments(n int) Option {
	return func(o *Options) {
		if n > 0 {
			o.JournalSegments = n
		}
	}
}

func WithTxMaxSize(n int) Option {
	return func(o *Options) {
		if n > 0 {
			o.TxMaxSize = n
		}
	}
}

func WithDriverType(s string) Option {
	return func(o *Options) {
		if s != "" {
			o.Driver = s
		}
	}
}

func WithPageSize(n int) Option {
	return func(o *Options) {
		if n > 0 {
			o.PageSize = n
		}
	}
}

func WithPageFill(n float64) Option {
	return func(o *Options) {
		if n > 0 {
			o.PageFill = n
		}
	}
}

func WithNoSync(b bool) Option {
	return func(o *Options) {
		o.NoSync = b
	}
}

func WithReadOnly(b bool) Option {
	return func(o *Options) {
		o.ReadOnly = b
	}
}

func WithDropOnClose(b bool) Option {
	return func(o *Options) {
		o.IsTemp = b
	}
}
