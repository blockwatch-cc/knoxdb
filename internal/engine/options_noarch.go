// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//go:build !wasm
// +build !wasm

package engine

import (
	"time"

	bolt "go.etcd.io/bbolt"
)

func (o DatabaseOptions) ToDriverOpts() any {
	switch o.Driver {
	case "bolt":
		drvOpts := bolt.Options{
			// open timeout when file is locked
			Timeout: time.Second,

			// faster for large databases
			FreelistType: bolt.FreelistMapType,

			// User-controlled options
			//
			// skip fsync (DANGEROUS on crashes, but better performance for bulk load)
			NoSync: o.NoSync,
			//
			// skip fsync+alloc on grow; don't use with ext3/4, good in Docker + XFS
			NoGrowSync: o.NoGrowSync,
			//
			// don't fsync freelist (improves write performance at the cost of full
			// database scan on start-up)
			NoFreelistSync: o.NoSync,
			//
			// PageSize overrides the default OS page size.
			PageSize: o.PageSize,
			//
			// v1.4 (currently in alpha)
			// Logger: o.Logger,
		}
		o.Logger.Debug("Bolt DB config")
		o.Logger.Debugf("  Readonly         %t", o.ReadOnly)
		o.Logger.Debugf("  No-Sync          %t", o.NoSync)
		o.Logger.Debugf("  No-Grow-Sync     %t", o.NoGrowSync)
		o.Logger.Debugf("  Pagesize         %d", o.PageSize)
		if o.NoSync {
			o.Logger.Warnf("Enabled NOSYNC mode. Database will not be safe on crashes!")
		}
		return &drvOpts
	default:
		return nil
	}
}

func (o TableOptions) ToDriverOpts() any {
	switch o.Driver {
	case "bolt":
		drvOpts := bolt.Options{
			// open timeout when file is locked
			Timeout: time.Second,

			// faster for large databases
			FreelistType: bolt.FreelistMapType,

			// User-controlled options
			//
			// skip fsync (DANGEROUS on crashes, but better performance for bulk load)
			NoSync: o.NoSync,
			//
			// skip fsync+alloc on grow; don't use with ext3/4, good in Docker + XFS
			NoGrowSync: o.NoGrowSync,
			//
			// don't fsync freelist (improves write performance at the cost of full
			// database scan on start-up)
			NoFreelistSync: o.NoSync,
			//
			// PageSize overrides the default OS page size.
			PageSize: o.PageSize,
		}
		o.Logger.Debug("Bolt DB config")
		o.Logger.Debugf("  Readonly         %t", o.ReadOnly)
		o.Logger.Debugf("  No-Sync          %t", o.NoSync)
		o.Logger.Debugf("  No-Grow-Sync     %t", o.NoGrowSync)
		o.Logger.Debugf("  Pagesize         %d", o.PageSize)
		if o.NoSync {
			o.Logger.Warnf("Enabled NOSYNC mode. Database will not be safe on crashes!")
		}
		return &drvOpts
	default:
		return nil
	}
}

func (o StoreOptions) ToDriverOpts() any {
	switch o.Driver {
	case "bolt":
		drvOpts := bolt.Options{
			// open timeout when file is locked
			Timeout: time.Second,

			// faster for large databases
			FreelistType: bolt.FreelistMapType,

			// User-controlled options
			//
			// skip fsync (DANGEROUS on crashes, but better performance for bulk load)
			NoSync: o.NoSync,
			//
			// skip fsync+alloc on grow; don't use with ext3/4, good in Docker + XFS
			NoGrowSync: o.NoGrowSync,
			//
			// don't fsync freelist (improves write performance at the cost of full
			// database scan on start-up)
			NoFreelistSync: o.NoSync,
			//
			// PageSize overrides the default OS page size.
			PageSize: o.PageSize,
		}
		o.Logger.Debug("Bolt DB config")
		o.Logger.Debugf("  Readonly         %t", o.ReadOnly)
		o.Logger.Debugf("  No-Sync          %t", o.NoSync)
		o.Logger.Debugf("  No-Grow-Sync     %t", o.NoGrowSync)
		o.Logger.Debugf("  Pagesize         %d", o.PageSize)
		if o.NoSync {
			o.Logger.Warnf("Enabled NOSYNC mode. Database will not be safe on crashes!")
		}
		return &drvOpts
	default:
		return nil
	}
}

func (o IndexOptions) ToDriverOpts() any {
	switch o.Driver {
	case "bolt":
		drvOpts := bolt.Options{
			// open timeout when file is locked
			Timeout: time.Second,

			// faster for large databases
			FreelistType: bolt.FreelistMapType,

			// User-controlled options
			//
			// skip fsync (DANGEROUS on crashes, but better performance for bulk load)
			NoSync: o.NoSync,
			//
			// skip fsync+alloc on grow; don't use with ext3/4, good in Docker + XFS
			NoGrowSync: o.NoGrowSync,
			//
			// don't fsync freelist (improves write performance at the cost of full
			// database scan on start-up)
			NoFreelistSync: o.NoSync,
			//
			// PageSize overrides the default OS page size.
			PageSize: o.PageSize,
		}
		o.Logger.Debug("Bolt DB config")
		o.Logger.Debugf("  Readonly         %t", o.ReadOnly)
		o.Logger.Debugf("  No-Sync          %t", o.NoSync)
		o.Logger.Debugf("  No-Grow-Sync     %t", o.NoGrowSync)
		o.Logger.Debugf("  Pagesize         %d", o.PageSize)
		if o.NoSync {
			o.Logger.Warnf("Enabled NOSYNC mode. Database will not be safe on crashes!")
		}
		return &drvOpts
	default:
		return nil
	}
}
