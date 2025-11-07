// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"path/filepath"
	"time"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/wal"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/store"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/echa/log"
)

type DatabaseOptions struct {
	// engine options
	Namespace       string           // unique db identifier
	CacheSize       int              // in bytes
	WalSegmentSize  int              // wal file size
	WalRecoveryMode wal.RecoveryMode // howto recover from wal damage
	LockTimeout     time.Duration    // lock manager timeout
	TxWaitTimeout   time.Duration    // write tx timeout
	MaxWorkers      int              // max number of parallel worker goroutines
	MaxTasks        int              // max number of tasks waiting for execution
	Logger          log.Logger       `knox:"-"`

	// catalog store options
	Path     string  // local filesystem
	Driver   string  // bolt, mem, ...
	PageSize int     // boltdb
	PageFill float64 // boltdb
	NoSync   bool    // boltdb, no fsync on transactions (dangerous)
	ReadOnly bool    // read-only tx and no schema changes
	IsTemp   bool    // drop database on close
}

func (o DatabaseOptions) Merge(o2 DatabaseOptions) DatabaseOptions {
	o.Namespace = util.NonZero(o2.Namespace, o.Namespace)
	o.CacheSize = util.NonZero(o2.CacheSize, o.CacheSize)
	o.WalSegmentSize = util.NonZero(o2.WalSegmentSize, o.WalSegmentSize)
	o.WalRecoveryMode = util.NonZero(o2.WalRecoveryMode, o.WalRecoveryMode)
	o.LockTimeout = util.NonZero(o2.LockTimeout, o.LockTimeout)
	o.TxWaitTimeout = util.NonZero(o2.TxWaitTimeout, o.TxWaitTimeout)
	o.MaxTasks = util.NonZero(o2.MaxTasks, o.MaxTasks)
	o.MaxWorkers = util.NonZero(o2.MaxWorkers, o.MaxWorkers)

	o.Path = util.NonZero(o2.Path, o.Path)
	o.Driver = util.NonZero(o2.Driver, o.Driver)
	o.PageSize = util.NonZero(o2.PageSize, o.PageSize)
	o.PageFill = util.NonZero(o2.PageFill, o.PageFill)
	o.NoSync = o2.NoSync
	o.ReadOnly = o2.ReadOnly
	o.IsTemp = o2.IsTemp
	if o2.Logger != nil {
		o.Logger = o2.Logger
	}
	return o
}

func (o DatabaseOptions) CatalogOptions(dbName string) []store.Option {
	opts := []store.Option{
		store.WithPath(filepath.Join(o.Path, dbName, CATALOG_NAME)),
		store.WithDriver(o.Driver),
		store.WithPageSize(o.PageSize),
		store.WithPageFill(o.PageFill),
		store.WithLogger(o.Logger),
		store.WithManifest(store.NewManifest(dbName, CATALOG_TYPE)),
	}
	if o.ReadOnly {
		opts = append(opts, store.WithReadonly())
	}
	if o.NoSync {
		opts = append(opts, store.WithNoSync())
	}
	if o.IsTemp {
		opts = append(opts, store.WithDeleteOnClose())
	}
	return opts
}

func (o DatabaseOptions) WithNamespace(n string) DatabaseOptions {
	o.Namespace = n
	return o
}

func (o DatabaseOptions) WithPath(n string) DatabaseOptions {
	o.Path = n
	return o
}

func (o DatabaseOptions) WithDriver(n string) DatabaseOptions {
	o.Driver = n
	return o
}

func (o DatabaseOptions) WithPageSize(n int) DatabaseOptions {
	o.PageSize = n
	return o
}

func (o DatabaseOptions) WithPageFill(n float64) DatabaseOptions {
	o.PageFill = n
	return o
}

func (o DatabaseOptions) WithCacheSize(n int) DatabaseOptions {
	o.CacheSize = n
	return o
}

func (o DatabaseOptions) WithReadOnly() DatabaseOptions {
	o.ReadOnly = true
	return o
}

func (o DatabaseOptions) WithDangerousNoSync() DatabaseOptions {
	o.NoSync = true
	return o
}

func (o DatabaseOptions) WithLogger(l log.Logger) DatabaseOptions {
	o.Logger = l
	return o
}

func (o DatabaseOptions) WithWalSegmentSize(sz int) DatabaseOptions {
	o.WalSegmentSize = sz
	return o
}

func (o DatabaseOptions) WithWalRecoveryMode(mode wal.RecoveryMode) DatabaseOptions {
	o.WalRecoveryMode = mode
	return o
}

func (o DatabaseOptions) WithLockTimeout(to time.Duration) DatabaseOptions {
	o.LockTimeout = to
	return o
}

func (o DatabaseOptions) WithMaxWorkers(n int) DatabaseOptions {
	o.MaxWorkers = n
	return o
}

func (o DatabaseOptions) WithMaxTasks(n int) DatabaseOptions {
	o.MaxTasks = n
	return o
}

func (o DatabaseOptions) WithDeleteOnClose() DatabaseOptions {
	o.IsTemp = true
	return o
}

func (o DatabaseOptions) MarshalBinary() ([]byte, error) {
	enc := schema.NewGenericEncoder[DatabaseOptions]()
	return enc.Encode(o, nil)
}

func (o *DatabaseOptions) UnmarshalBinary(buf []byte) error {
	dec := schema.NewGenericDecoder[DatabaseOptions]()
	_, err := dec.Decode(buf, o)
	return err
}

type TableOptions struct {
	// table options
	Engine          TableKind  // pack, lsm, parquet, csv, remote
	PackSize        int        // pack engine
	JournalSize     int        // pack engine
	JournalSegments int        // pack engine
	TxMaxSize       int        // maximum write size of low-level dbfile transactions
	Logger          log.Logger `knox:"-"` // custom logger

	// store backend options
	Driver   string  // bolt, mem, ...
	PageSize int     // boltdb
	PageFill float64 // boltdb
	ReadOnly bool    // read-only tx and no schema changes
	NoSync   bool    // boltdb, no fsync on transactions (dangerous)
	IsTemp   bool    // drop tables on close
}

func (o TableOptions) Merge(o2 TableOptions) TableOptions {
	o.Engine = util.NonZero(o2.Engine, o.Engine)
	o.PackSize = types.ToChunkSize(util.NonZero(o2.PackSize, o.PackSize))
	o.JournalSize = types.ToChunkSize(util.NonZero(o2.JournalSize, o.JournalSize))
	o.JournalSegments = util.NonZero(o2.JournalSegments, o.JournalSegments)
	o.TxMaxSize = util.NonZero(o2.TxMaxSize, o.TxMaxSize)
	if o2.Logger != nil {
		o.Logger = o2.Logger
	}

	o.Driver = util.NonZero(o2.Driver, o.Driver)
	o.PageSize = util.NonZero(o2.PageSize, o.PageSize)
	o.PageFill = util.NonZero(o2.PageFill, o.PageFill)
	o.ReadOnly = o2.ReadOnly
	o.NoSync = o2.NoSync
	o.IsTemp = o2.IsTemp
	return o
}

func (o TableOptions) StoreOptions() []store.Option {
	opts := []store.Option{
		store.WithDriver(o.Driver),
		store.WithPageSize(o.PageSize),
		store.WithPageFill(o.PageFill),
		store.WithLogger(o.Logger),
	}
	if o.ReadOnly {
		opts = append(opts, store.WithReadonly())
	}
	if o.NoSync {
		opts = append(opts, store.WithNoSync())
	}
	if o.IsTemp {
		opts = append(opts, store.WithDeleteOnClose())
	}
	return opts
}

func (o TableOptions) MarshalBinary() ([]byte, error) {
	enc := schema.NewGenericEncoder[TableOptions]()
	return enc.Encode(o, nil)
}

func (o *TableOptions) UnmarshalBinary(buf []byte) error {
	dec := schema.NewGenericDecoder[TableOptions]()
	_, err := dec.Decode(buf, o)
	return err
}

func (o TableOptions) WithEngine(e TableKind) TableOptions {
	o.Engine = e
	return o
}

func (o TableOptions) WithDriver(n string) TableOptions {
	o.Driver = n
	return o
}

func (o TableOptions) WithPackSize(n int) TableOptions {
	o.PackSize = n
	return o
}

func (o TableOptions) WithJournalSize(n int) TableOptions {
	o.JournalSize = n
	return o
}

func (o TableOptions) WithJournalSegments(n int) TableOptions {
	o.JournalSegments = n
	return o
}

func (o TableOptions) WithPageSize(n int) TableOptions {
	o.PageSize = n
	return o
}

func (o TableOptions) WithTxMaxSize(n int) TableOptions {
	o.TxMaxSize = n
	return o
}

func (o TableOptions) WithPageFill(n float64) TableOptions {
	o.PageFill = n
	return o
}

func (o TableOptions) WithReadOnly() TableOptions {
	o.ReadOnly = true
	return o
}

func (o TableOptions) WithDeleteOnClose() TableOptions {
	o.IsTemp = true
	return o
}

func (o TableOptions) WithDangerousNoSync() TableOptions {
	o.NoSync = true
	return o
}

func (o TableOptions) WithLogger(l log.Logger) TableOptions {
	o.Logger = l
	return o
}

type StoreOptions struct {
	Driver    string     // bolt, mem, ...
	PageSize  int        // boltdb
	PageFill  float64    // boltdb
	ReadOnly  bool       // read-only tx only
	NoSync    bool       // boltdb, no fsync on transactions (dangerous)
	TxMaxSize int        // maximum write size of low-level dbfile transactions
	Logger    log.Logger `knox:"-"` // custom logger
	IsTemp    bool       // drop store on close
}

func (o StoreOptions) Merge(o2 StoreOptions) StoreOptions {
	o.Driver = util.NonZero(o2.Driver, o.Driver)
	o.PageSize = util.NonZero(o2.PageSize, o.PageSize)
	o.PageFill = util.NonZero(o2.PageFill, o.PageFill)
	o.TxMaxSize = util.NonZero(o2.TxMaxSize, o.TxMaxSize)
	o.ReadOnly = o2.ReadOnly
	o.NoSync = o2.NoSync
	o.IsTemp = o2.IsTemp
	if o2.Logger != nil {
		o.Logger = o2.Logger
	}
	return o
}

func (o StoreOptions) StoreOptions() []store.Option {
	opts := []store.Option{
		store.WithDriver(o.Driver),
		store.WithPageSize(o.PageSize),
		store.WithPageFill(o.PageFill),
		store.WithLogger(o.Logger),
	}
	if o.ReadOnly {
		opts = append(opts, store.WithReadonly())
	}
	if o.NoSync {
		opts = append(opts, store.WithNoSync())
	}
	if o.IsTemp {
		opts = append(opts, store.WithDeleteOnClose())
	}
	return opts
}

func (o StoreOptions) MarshalBinary() ([]byte, error) {
	enc := schema.NewGenericEncoder[StoreOptions]()
	return enc.Encode(o, nil)
}

func (o *StoreOptions) UnmarshalBinary(buf []byte) error {
	dec := schema.NewGenericDecoder[StoreOptions]()
	_, err := dec.Decode(buf, o)
	return err
}

func (o StoreOptions) WithDriver(n string) StoreOptions {
	o.Driver = n
	return o
}

func (o StoreOptions) WithPageSize(n int) StoreOptions {
	o.PageSize = n
	return o
}

func (o StoreOptions) WithTxMaxSize(n int) StoreOptions {
	o.TxMaxSize = n
	return o
}

func (o StoreOptions) WithPageFill(n float64) StoreOptions {
	o.PageFill = n
	return o
}

func (o StoreOptions) WithReadOnly() StoreOptions {
	o.ReadOnly = true
	return o
}

func (o StoreOptions) WithDeleteOnClose() StoreOptions {
	o.IsTemp = true
	return o
}

func (o StoreOptions) WithDangerousNoSync() StoreOptions {
	o.NoSync = true
	return o
}

func (o StoreOptions) WithLogger(l log.Logger) StoreOptions {
	o.Logger = l
	return o
}

type IndexOptions struct {
	// table options
	Engine      IndexKind  // pack, lsm
	PackSize    int        // pack engine
	JournalSize int        // pack engine
	TxMaxSize   int        // maximum write size of low-level dbfile transactions
	Logger      log.Logger `knox:"-"` // custom logger

	// store backend options
	Driver   string  // bolt, mem, ...
	PageSize int     // boltdb
	PageFill float64 // boltdb
	ReadOnly bool    // read-only tx and no schema changes
	NoSync   bool    // boltdb, no fsync on transactions (dangerous)
	IsTemp   bool    // drop index on close
}

func (o IndexOptions) Merge(o2 IndexOptions) IndexOptions {
	o.Engine = util.NonZero(o2.Engine, o.Engine)
	o.PackSize = types.ToChunkSize(util.NonZero(o2.PackSize, o.PackSize))
	o.JournalSize = types.ToChunkSize(util.NonZero(o2.JournalSize, o.JournalSize))
	o.TxMaxSize = util.NonZero(o2.TxMaxSize, o.TxMaxSize)
	if o2.Logger != nil {
		o.Logger = o2.Logger
	}

	o.Driver = util.NonZero(o2.Driver, o.Driver)
	o.PageSize = util.NonZero(o2.PageSize, o.PageSize)
	o.PageFill = util.NonZero(o2.PageFill, o.PageFill)
	o.ReadOnly = o2.ReadOnly
	o.NoSync = o2.NoSync
	o.IsTemp = o2.IsTemp
	return o
}

func (o IndexOptions) StoreOptions() []store.Option {
	opts := []store.Option{
		store.WithDriver(o.Driver),
		store.WithPageSize(o.PageSize),
		store.WithPageFill(o.PageFill),
		store.WithLogger(o.Logger),
	}
	if o.ReadOnly {
		opts = append(opts, store.WithReadonly())
	}
	if o.NoSync {
		opts = append(opts, store.WithNoSync())
	}
	if o.IsTemp {
		opts = append(opts, store.WithDeleteOnClose())
	}
	return opts
}

func (o IndexOptions) MarshalBinary() ([]byte, error) {
	enc := schema.NewGenericEncoder[IndexOptions]()
	return enc.Encode(o, nil)
}

func (o *IndexOptions) UnmarshalBinary(buf []byte) error {
	dec := schema.NewGenericDecoder[IndexOptions]()
	_, err := dec.Decode(buf, o)
	return err
}

func (o IndexOptions) WithEngine(e IndexKind) IndexOptions {
	o.Engine = e
	return o
}

func (o IndexOptions) WithDriver(n string) IndexOptions {
	o.Driver = n
	return o
}

func (o IndexOptions) WithPackSize(n int) IndexOptions {
	o.PackSize = n
	return o
}

func (o IndexOptions) WithJournalSize(n int) IndexOptions {
	o.JournalSize = n
	return o
}

func (o IndexOptions) WithPageSize(n int) IndexOptions {
	o.PageSize = n
	return o
}

func (o IndexOptions) WithTxMaxSize(n int) IndexOptions {
	o.TxMaxSize = n
	return o
}

func (o IndexOptions) WithPageFill(n float64) IndexOptions {
	o.PageFill = n
	return o
}

func (o IndexOptions) WithReadOnly() IndexOptions {
	o.ReadOnly = true
	return o
}

func (o IndexOptions) WithDeleteOnClose() IndexOptions {
	o.IsTemp = true
	return o
}

func (o IndexOptions) WithDangerousNoSync() IndexOptions {
	o.NoSync = true
	return o
}

func (o IndexOptions) WithLogger(l log.Logger) IndexOptions {
	o.Logger = l
	return o
}
