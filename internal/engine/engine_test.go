// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"path/filepath"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/wal"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/echa/log"
	"github.com/stretchr/testify/require"
)

const TEST_DB_NAME = "test"

func NewTestDatabaseOptions(t testing.TB, driver string) DatabaseOptions {
	return DatabaseOptions{
		Path:          t.TempDir(),
		Namespace:     "cx.bwd.knoxdb.testdb",
		Driver:        driver,
		PageSize:      4096,
		PageFill:      1.0,
		CacheSize:     1 << 20,
		LockTimeout:   time.Second,
		TxWaitTimeout: 0,
		NoSync:        false,
		NoGrowSync:    false,
		ReadOnly:      false,
		Logger:        log.Log,
	}
}

func NewTestEngine(t testing.TB, opts DatabaseOptions) *Engine {
	path := filepath.Join(opts.Path, TEST_DB_NAME)
	e := &Engine{
		path: path,
		cache: CacheManager{
			blocks:  block.NewCache(0),
			buffers: NewBufferCache(0),
		},
		tables:  util.NewLockFreeMap[uint64, TableEngine](),
		stores:  util.NewLockFreeMap[uint64, StoreEngine](),
		indexes: util.NewLockFreeMap[uint64, IndexEngine](),
		enums:   schema.NewEnumRegistry(),
		txs:     make(TxList, 0),
		txchan:  make(chan struct{}, 1),
		xmin:    1,
		xnext:   1,
		vnext:   ReadTxOffset,
		dbId:    types.TaggedHash(types.ObjectTagDatabase, TEST_DB_NAME),
		opts:    opts,
		cat:     NewCatalog(TEST_DB_NAME),
		log:     opts.Logger,
		lm:      NewLockManager(),
	}
	var err error
	e.wal, err = wal.Create(wal.WalOptions{
		Seed:           0,
		Path:           path,
		MaxSegmentSize: 1024,
		RecoveryMode:   wal.RecoveryModeTruncate,
		Logger:         opts.Logger,
	})
	require.NoError(t, err)
	e.cat.WithWal(e.wal)
	e.txchan <- struct{}{}
	return e
}

func OpenTestEngine(t testing.TB, opts DatabaseOptions) *Engine {
	path := filepath.Join(opts.Path, TEST_DB_NAME)
	e := &Engine{
		path: path,
		cache: CacheManager{
			blocks:  block.NewCache(0),
			buffers: NewBufferCache(0),
		},
		tables:  util.NewLockFreeMap[uint64, TableEngine](),
		stores:  util.NewLockFreeMap[uint64, StoreEngine](),
		indexes: util.NewLockFreeMap[uint64, IndexEngine](),
		enums:   schema.NewEnumRegistry(),
		txs:     make(TxList, 0),
		txchan:  make(chan struct{}, 1),
		xmin:    1,
		xnext:   1,
		vnext:   ReadTxOffset,
		dbId:    types.TaggedHash(types.ObjectTagDatabase, TEST_DB_NAME),
		opts:    opts,
		cat:     NewCatalog(TEST_DB_NAME),
		log:     opts.Logger,
		lm:      NewLockManager(),
	}

	var err error
	e.wal, err = wal.Open(0, wal.WalOptions{
		Seed:           0,
		Path:           path,
		MaxSegmentSize: 1024,
		RecoveryMode:   wal.RecoveryModeTruncate,
		Logger:         opts.Logger,
	})
	require.NoError(t, err)
	e.txchan <- struct{}{}
	return e
}
