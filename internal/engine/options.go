// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	"blockwatch.cc/knoxdb/internal/wal"
	"blockwatch.cc/knoxdb/pkg/store"
	"github.com/echa/log"
)

type Option func(o *Options)

type Options struct {
	// engine options
	BaseContext     context.Context  // engine base context
	Namespace       string           // unique db identifier
	Path            string           // on local filesystem
	CacheSize       int              // engine block cache in bytes
	WalSegmentSize  int              // wal file size in bytes
	WalRecoveryMode wal.RecoveryMode // howto recover from wal damage
	LockTimeout     time.Duration    // lock manager timeout
	TxWaitTimeout   time.Duration    // write tx timeout, 0 = off
	MaxWorkers      int              // max number of parallel worker goroutines
	MaxTasks        int              // max number of tasks waiting for execution
	Log             log.Logger       `knox:"-"`

	// table & index options
	Engine          string // pack (future: lsm, parquet, csv, remote)
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
	IsTemp    bool    // drop table/index/database on close
}

var defaultDatabaseOptions = Options{
	BaseContext:     context.Background(),
	Path:            "./db",
	CacheSize:       16 << 20,
	WalSegmentSize:  128 << 20,
	WalRecoveryMode: wal.RecoveryModeTruncate,
	MaxWorkers:      runtime.NumCPU(),
	MaxTasks:        16,
	Engine:          TableKindPack,
	PackSize:        1 << 14, // 16k
	JournalSize:     1 << 15, // 32k
	JournalSegments: 16,
	Driver:          "bolt",
	TxMaxSize:       10 << 24, // 16 MB
	PageSize:        1 << 16,  // 64kB
	PageFill:        0.9,
	Log:             log.Disabled,
}

func (o Options) Apply(opts ...Option) Options {
	ocopy := o
	for _, opt := range opts {
		opt(&ocopy)
	}
	return ocopy
}

func (o Options) DatabaseOptions() []Option {
	return []Option{
		WithBaseContext(o.BaseContext),
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
		WithEngineType(o.Engine),
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
		WithEngineType(o.Engine),
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
		// store.WithLogger(o.Log),
		store.WithNoSync(o.NoSync),
		store.WithDropOnClose(o.IsTemp),
		store.WithReadonly(o.ReadOnly),
	}
}

func WithBaseContext(ctx context.Context) Option {
	return func(o *Options) {
		o.BaseContext = ctx
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

func WithNoSync(b ...bool) Option {
	return func(o *Options) {
		if len(b) == 0 {
			o.NoSync = true
		} else {
			o.NoSync = b[0]
		}
	}
}

func WithReadOnly(b ...bool) Option {
	return func(o *Options) {
		if len(b) == 0 {
			o.ReadOnly = true
		} else {
			o.ReadOnly = b[0]
		}
	}
}

func WithDropOnClose(b ...bool) Option {
	return func(o *Options) {
		if len(b) == 0 {
			o.IsTemp = true
		} else {
			o.IsTemp = b[0]
		}
	}
}

var (
	lineEnd  = []byte{'\n'}
	valueSep = []byte(": ")
)

type optionDecoder func(string) (Option, error)

// optionDecoders defines name to option decoder mapping
var optionDecoders = map[string]optionDecoder{
	"namespace":         asString(WithNamespace),
	"path":              asString(WithPath),
	"cache_size":        asInt(WithCacheSize),
	"wal_segment_size":  asInt(WithWalSegmentSize),
	"wal_recovery_mode": asIntT[wal.RecoveryMode](WithWalRecoveryMode),
	"lock_timeout":      asIntT[time.Duration](WithLockTimeout),
	"tx_wait_timeout":   asIntT[time.Duration](WithTxWaitTimeout),
	"max_workers":       asInt(WithMaxWorkers),
	"max_tasks":         asInt(WithMaxTasks),
	"engine":            asString(WithEngineType),
	"pack_size":         asInt(WithPackSize),
	"journal_size":      asInt(WithJournalSize),
	"journal_segments":  asInt(WithJournalSegments),
	"driver":            asString(WithDriverType),
	"tx_max_size":       asInt(WithTxMaxSize),
	"page_size":         asInt(WithPageSize),
	"page_fill":         asFloat(WithPageFill),
}

// Writes options as YAML compatible list
func (o Options) MarshalBinary() ([]byte, error) {
	buf := make([]byte, 0, 1024)
	buf = appendStringOpt(buf, "namespace", o.Namespace)
	buf = appendStringOpt(buf, "path", o.Path)
	buf = appendIntOpt(buf, "cache_size", o.CacheSize)
	buf = appendIntOpt(buf, "wal_segment_size", o.WalSegmentSize)
	buf = appendIntOpt(buf, "wal_recovery_mode", int(o.WalRecoveryMode))
	buf = appendIntOpt(buf, "lock_timeout", int(o.LockTimeout))
	buf = appendIntOpt(buf, "tx_wait_timeout", int(o.TxWaitTimeout))
	buf = appendIntOpt(buf, "max_workers", o.MaxWorkers)
	buf = appendIntOpt(buf, "max_tasks", o.MaxTasks)
	buf = appendStringOpt(buf, "engine", o.Engine)
	buf = appendIntOpt(buf, "pack_size", o.PackSize)
	buf = appendIntOpt(buf, "journal_size", o.JournalSize)
	buf = appendIntOpt(buf, "journal_segments", o.JournalSegments)
	buf = appendStringOpt(buf, "driver", o.Driver)
	buf = appendIntOpt(buf, "tx_max_size", o.TxMaxSize)
	buf = appendIntOpt(buf, "page_size", o.PageSize)
	buf = appendFloatOpt(buf, "page_fill", o.PageFill)
	return buf, nil
}

// Reads options from YAML compatible list
func (o *Options) UnmarshalBinary(buf []byte) error {
	// process all options line by line
	for line := range bytes.SplitSeq(buf, lineEnd) {
		// trim whitespace if any
		line = bytes.TrimSpace(line)

		// skip empty lines
		if len(line) == 0 {
			continue
		}

		// split into key/value
		k, v, ok := bytes.Cut(line, valueSep)
		if !ok {
			return fmt.Errorf("malformed option %q", string(line))
		}

		// trim whitespace if any
		k = bytes.TrimSpace(k)
		v = bytes.TrimSpace(v)

		// find decoder, ignore unknown options
		dec, ok := optionDecoders[string(k)]
		if !ok {
			continue
		}

		// decode the option value into an Option func
		fn, err := dec(string(v))
		if err != nil {
			return fmt.Errorf("%s: %v", string(k), err)
		}

		// apply the option func
		fn(o)
	}

	return nil
}

func asIntT[T ~byte | ~int | ~uint | ~int64](fn any) optionDecoder {
	return func(s string) (Option, error) {
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, err
		}
		return fn.(func(T) Option)(T(v)), nil
	}
}

func asInt(fn any) optionDecoder {
	return func(s string) (Option, error) {
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, err
		}
		return fn.(func(int) Option)(int(v)), nil
	}
}

func asFloat(fn any) optionDecoder {
	return func(s string) (Option, error) {
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, err
		}
		return fn.(func(float64) Option)(f), nil
	}
}

// func asBool(fn any) optionDecoder {
// 	return func(s string) (Option, error) {
// 		v, err := strconv.ParseBool(s)
// 		if err != nil {
// 			return nil, err
// 		}
// 		return fn.(func(bool) Option)(v), nil
// 	}
// }

func asString(fn any) optionDecoder {
	return func(s string) (Option, error) {
		return fn.(func(string) Option)(s), nil
	}
}

func appendIntOpt(buf []byte, k string, v int) []byte {
	if v == 0 {
		return buf
	}
	buf = append(buf, []byte(k)...)
	buf = append(buf, valueSep...)
	buf = strconv.AppendInt(buf, int64(v), 10)
	buf = append(buf, lineEnd...)
	return buf
}

func appendStringOpt(buf []byte, k, v string) []byte {
	if len(v) == 0 {
		return buf
	}
	buf = append(buf, []byte(k)...)
	buf = append(buf, valueSep...)
	buf = append(buf, []byte(v)...)
	buf = append(buf, lineEnd...)
	return buf
}

func appendFloatOpt(buf []byte, k string, v float64) []byte {
	if v == 0 || math.IsNaN(v) {
		return buf
	}
	buf = append(buf, []byte(k)...)
	buf = append(buf, valueSep...)
	buf = strconv.AppendFloat(buf, v, 'f', -1, 64)
	buf = append(buf, lineEnd...)
	return buf
}
