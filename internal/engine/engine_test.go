// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/wal"
	"blockwatch.cc/knoxdb/pkg/schema/enum"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/echa/log"
	"github.com/stretchr/testify/require"
)

const TEST_DB_NAME = "test"

func NewTestDatabaseOptions(t testing.TB, driver string) Options {
	return Options{
		BaseContext:   context.Background(),
		Path:          t.TempDir(),
		Driver:        driver,
		PageSize:      4096,
		PageFill:      1.0,
		CacheSize:     1 << 20,
		LockTimeout:   time.Second,
		TxWaitTimeout: 0,
		NoSync:        false,
		ReadOnly:      false,
		Log:           log.Log,
		IsTemp:        false,
	}
}

func NewTestEngine(t testing.TB, opts Options) *Engine {
	ectx, ecancel := context.WithCancelCause(opts.BaseContext)
	path := filepath.Join(opts.Path, TEST_DB_NAME)
	e := &Engine{
		ctx:     ectx,
		cancel:  ecancel,
		path:    path,
		tables:  util.NewLockFreeMap[uint64, TableEngine](),
		indexes: util.NewLockFreeMap[uint64, IndexEngine](),
		enums:   enum.NewRegistry(),
		xtoken:  make(chan struct{}, 1),
		dbId:    types.TaggedHash(types.ObjectTagDatabase, TEST_DB_NAME),
		opts:    opts,
		cat:     NewCatalog(TEST_DB_NAME),
		log:     opts.Log,
		lm:      NewLockManager(),
		cache: CacheManager{
			blocks:  block.NewCache(0),
			buffers: NewBufferCache(0),
		},
	}
	e.xmin.Store(1)
	e.xid.Store(0)
	e.vid.Store(ReadTxOffset)
	var err error
	e.wal, err = wal.Create(
		wal.WithSeed(0),
		wal.WithPath(path),
		wal.WithMaxSegmentSize(1024),
		wal.WithRecoveryMode(wal.RecoveryModeTruncate),
		wal.WithLogger(opts.Log),
	)
	require.NoError(t, err)
	e.cat.WithWal(e.wal)
	e.xtoken <- struct{}{}
	return e
}

func OpenTestEngine(t testing.TB, opts Options) *Engine {
	ectx, ecancel := context.WithCancelCause(opts.BaseContext)
	path := filepath.Join(opts.Path, TEST_DB_NAME)
	e := &Engine{
		ctx:     ectx,
		cancel:  ecancel,
		path:    path,
		tables:  util.NewLockFreeMap[uint64, TableEngine](),
		indexes: util.NewLockFreeMap[uint64, IndexEngine](),
		enums:   enum.NewRegistry(),
		xtoken:  make(chan struct{}, 1),
		dbId:    types.TaggedHash(types.ObjectTagDatabase, TEST_DB_NAME),
		opts:    opts,
		cat:     NewCatalog(TEST_DB_NAME),
		log:     opts.Log,
		lm:      NewLockManager(),
		cache: CacheManager{
			blocks:  block.NewCache(0),
			buffers: NewBufferCache(0),
		},
	}
	e.xmin.Store(1)
	e.xid.Store(0)
	e.vid.Store(ReadTxOffset)

	var err error
	e.wal, err = wal.Open(0,
		wal.WithSeed(0),
		wal.WithPath(path),
		wal.WithMaxSegmentSize(1024),
		wal.WithRecoveryMode(wal.RecoveryModeTruncate),
		wal.WithLogger(opts.Log),
	)
	require.NoError(t, err)
	e.xtoken <- struct{}{}
	return e
}
