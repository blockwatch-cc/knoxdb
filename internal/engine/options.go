// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"time"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/wal"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/echa/log"
)

type DatabaseOptions struct {
	Path            string           // local filesystem
	Namespace       string           // unique db identifier
	Driver          string           // bolt, mem, ...
	PageSize        int              // boltdb
	PageFill        float64          // boltdb
	CacheSize       int              // in bytes
	WalSegmentSize  int              // wal file size
	WalRecoveryMode wal.RecoveryMode // howto recover from wal damage
	LockTimeout     time.Duration    // lock manager timeout
	TxWaitTimeout   time.Duration    // write tx timeout
	NoSync          bool             // boltdb, no fsync on transactions (dangerous)
	NoGrowSync      bool             // boltdb, skip fsync+alloc on grow
	ReadOnly        bool             // read-only tx and no schema changes
	MaxWorkers      int              // max number of parallel worker goroutines
	MaxTasks        int              // max number of tasks waiting for execution
	Logger          log.Logger       `knox:"-"`
}

func (o DatabaseOptions) Merge(o2 DatabaseOptions) DatabaseOptions {
	o.Path = util.NonZero(o2.Path, o.Path)
	o.Namespace = util.NonZero(o2.Namespace, o.Namespace)
	o.Driver = util.NonZero(o2.Driver, o.Driver)
	o.PageSize = util.NonZero(o2.PageSize, o.PageSize)
	o.PageFill = util.NonZero(o2.PageFill, o.PageFill)
	o.CacheSize = util.NonZero(o2.CacheSize, o.CacheSize)
	o.WalSegmentSize = util.NonZero(o2.WalSegmentSize, o.WalSegmentSize)
	o.WalRecoveryMode = util.NonZero(o2.WalRecoveryMode, o.WalRecoveryMode)
	o.LockTimeout = util.NonZero(o2.LockTimeout, o.LockTimeout)
	o.TxWaitTimeout = util.NonZero(o2.TxWaitTimeout, o.TxWaitTimeout)
	o.ReadOnly = o2.ReadOnly
	o.NoSync = o2.NoSync
	o.NoGrowSync = o2.NoGrowSync
	o.MaxTasks = util.NonZero(o2.MaxTasks, o.MaxTasks)
	o.MaxWorkers = util.NonZero(o2.MaxWorkers, o.MaxWorkers)
	if o2.Logger != nil {
		o.Logger = o2.Logger
	}
	return o
}

func (o DatabaseOptions) WithPath(n string) DatabaseOptions {
	o.Path = n
	return o
}

func (o DatabaseOptions) WithNamespace(n string) DatabaseOptions {
	o.Namespace = n
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
	o.NoGrowSync = true
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
	Engine          TableKind  // pack, lsm, parquet, csv, remote
	Driver          string     // bolt, mem, ...
	PackSize        int        // pack engine
	JournalSize     int        // pack engine
	JournalSegments int        // pack engine
	PageSize        int        // boltdb
	PageFill        float64    // boltdb
	ReadOnly        bool       // read-only tx and no schema changes
	TxMaxSize       int        // maximum write size of low-level dbfile transactions
	NoSync          bool       // boltdb, no fsync on transactions (dangerous)
	NoGrowSync      bool       // boltdb, skip fsync+alloc on grow
	Logger          log.Logger `knox:"-"` // custom logger
}

func (o TableOptions) Merge(o2 TableOptions) TableOptions {
	o.Engine = util.NonZero(o2.Engine, o.Engine)
	o.Driver = util.NonZero(o2.Driver, o.Driver)
	o.PackSize = util.NonZero(o2.PackSize, o.PackSize)
	o.JournalSize = util.NonZero(o2.JournalSize, o.JournalSize)
	o.JournalSegments = util.NonZero(o2.JournalSegments, o.JournalSegments)
	o.PageSize = util.NonZero(o2.PageSize, o.PageSize)
	o.PageFill = util.NonZero(o2.PageFill, o.PageFill)
	o.TxMaxSize = util.NonZero(o2.TxMaxSize, o.TxMaxSize)
	o.ReadOnly = o2.ReadOnly
	o.NoSync = o2.NoSync
	o.NoGrowSync = o2.NoGrowSync
	if o2.Logger != nil {
		o.Logger = o2.Logger
	}
	return o
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

type StoreOptions struct {
	Driver     string     // bolt, mem, ...
	PageSize   int        // boltdb
	PageFill   float64    // boltdb
	ReadOnly   bool       // read-only tx only
	NoSync     bool       // boltdb, no fsync on transactions (dangerous)
	NoGrowSync bool       // boltdb, skip fsync+alloc on grow
	TxMaxSize  int        // maximum write size of low-level dbfile transactions
	Logger     log.Logger `knox:"-"` // custom logger
}

func (o StoreOptions) Merge(o2 StoreOptions) StoreOptions {
	o.Driver = util.NonZero(o2.Driver, o.Driver)
	o.PageSize = util.NonZero(o2.PageSize, o.PageSize)
	o.PageFill = util.NonZero(o2.PageFill, o.PageFill)
	o.TxMaxSize = util.NonZero(o2.TxMaxSize, o.TxMaxSize)
	o.ReadOnly = o2.ReadOnly
	o.NoSync = o2.NoSync
	o.NoGrowSync = o2.NoGrowSync
	if o2.Logger != nil {
		o.Logger = o2.Logger
	}
	return o
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

type IndexOptions struct {
	Engine      IndexKind       // pack, lsm
	Driver      string          // bolt, mem, ...
	Type        types.IndexType // hash, int, composite, bloom, bfuse, bits
	PackSize    int             // pack engine
	JournalSize int             // pack engine
	PageSize    int             // boltdb
	PageFill    float64         // boltdb
	ReadOnly    bool            // read-only tx and no schema changes
	TxMaxSize   int             // maximum write size of low-level dbfile transactions
	NoSync      bool            // boltdb, no fsync on transactions (dangerous)
	NoGrowSync  bool            // boltdb, skip fsync+alloc on grow
	Logger      log.Logger      `knox:"-"` // custom logger
}

func (o IndexOptions) Merge(o2 IndexOptions) IndexOptions {
	o.Engine = util.NonZero(o2.Engine, o.Engine)
	o.Driver = util.NonZero(o2.Driver, o.Driver)
	o.Type = util.NonZero(o2.Type, o.Type)
	o.PackSize = util.NonZero(o2.PackSize, o.PackSize)
	o.JournalSize = util.NonZero(o2.JournalSize, o.JournalSize)
	o.PageSize = util.NonZero(o2.PageSize, o.PageSize)
	o.PageFill = util.NonZero(o2.PageFill, o.PageFill)
	o.TxMaxSize = util.NonZero(o2.TxMaxSize, o.TxMaxSize)
	o.ReadOnly = o2.ReadOnly
	o.NoSync = o2.NoSync
	o.NoGrowSync = o2.NoGrowSync
	if o2.Logger != nil {
		o.Logger = o2.Logger
	}
	return o
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
