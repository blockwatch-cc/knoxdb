// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"path/filepath"
	"testing"

	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/wal"
	"blockwatch.cc/knoxdb/pkg/cache/rclru"
	"blockwatch.cc/knoxdb/pkg/schema"
	"github.com/echa/log"
)

const TEST_DB_NAME = "test"

func NewTestDatabaseOptions(t *testing.T, driver string) DatabaseOptions {
	return DatabaseOptions{
		Path:       t.TempDir(),
		Namespace:  "cx.bwd.knoxdb.testdb",
		Driver:     driver,
		PageSize:   4096,
		PageFill:   1.0,
		CacheSize:  1 << 20,
		NoSync:     false,
		NoGrowSync: false,
		ReadOnly:   false,
		Logger:     log.Log,
	}
}

func NewTestEngine(opts DatabaseOptions) *Engine {
	path := filepath.Join(opts.Path, TEST_DB_NAME)
	e := &Engine{
		path: path,
		cache: CacheManager{
			blocks:  rclru.NewNoCache[CacheKeyType, *block.Block](),
			buffers: rclru.NewNoCache[CacheKeyType, *Buffer](),
		},
		tables:  make(map[uint64]TableEngine),
		stores:  make(map[uint64]StoreEngine),
		indexes: make(map[uint64]IndexEngine),
		enums:   make(map[uint64]*schema.EnumDictionary),
		txs:     make(TxList, 0),
		xmin:    0,
		xnext:   1,
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
	if err != nil {
		panic(err)
	}
	e.cat.WithWal(e.wal)
	return e
}

func OpenTestEngine(opts DatabaseOptions) *Engine {
	path := filepath.Join(opts.Path, TEST_DB_NAME)
	e := &Engine{
		path: path,
		cache: CacheManager{
			blocks:  rclru.NewNoCache[CacheKeyType, *block.Block](),
			buffers: rclru.NewNoCache[CacheKeyType, *Buffer](),
		},
		tables:  make(map[uint64]TableEngine),
		stores:  make(map[uint64]StoreEngine),
		indexes: make(map[uint64]IndexEngine),
		enums:   make(map[uint64]*schema.EnumDictionary),
		txs:     make(TxList, 0),
		xmin:    0,
		xnext:   1,
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
	if err != nil {
		panic(err)
	}
	return e
}
