// Copyright (c) 2018-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"fmt"

	"blockwatch.cc/knoxdb/util"
)

const (
	defaultCacheSize        = 128 // use 128 MB memory (per table/index)
	defaultPackSizeLog2     = 16  // 64k entries per partition
	defaultJournalFillLevel = 50  // keep space for extension
)

var (
	DefaultOptions = Options{
		PackSizeLog2:    defaultPackSizeLog2, // 64k entries
		JournalSizeLog2: 17,                  // 128k entries
		CacheSize:       defaultCacheSize,    // in packs
		FillLevel:       90,                  // boltdb fill level to limit reallocations
	}
	NoOptions = Options{}
)

type Options struct {
	PackSizeLog2    int `json:"pack_size_log2,omitempty"`
	JournalSizeLog2 int `json:"journal_size_log2,omitempty"`
	CacheSize       int `json:"cache_size,omitempty"`
	FillLevel       int `json:"fill_level,omitempty"`
}

func (o Options) IsValid() bool {
	return o.PackSizeLog2 > 0
}

func (o Options) PackSize() int {
	return 1 << uint(o.PackSizeLog2)
}

func (o Options) JournalSize() int {
	return 1 << uint(o.JournalSizeLog2)
}

// Notes: allow cache size to be zero
func (o Options) Merge(o2 Options) Options {
	o.PackSizeLog2 = util.NonZero(o2.PackSizeLog2, o.PackSizeLog2)
	o.JournalSizeLog2 = util.NonZero(o2.JournalSizeLog2, o.JournalSizeLog2)
	o.FillLevel = util.NonZero(o2.FillLevel, o.FillLevel)
	o.CacheSize = o2.CacheSize
	return o
}

func (o Options) CacheSizeMBytes() int {
	return o.CacheSize * (1 << 20)
}

func (o Options) Check() error {
	// limit pack sizes to 256 .. 4M
	if o.PackSizeLog2 < 8 || o.PackSizeLog2 > 22 {
		return fmt.Errorf("PackSizeLog2 %d out of range [8, 22]", o.PackSizeLog2)
	}
	if o.JournalSizeLog2 < 8 || o.JournalSizeLog2 > 22 {
		return fmt.Errorf("JournalSizeLog2 %d out of range [8, 22]", o.JournalSizeLog2)
	}
	if o.CacheSize < 0 || o.CacheSize > 64*1024 {
		return fmt.Errorf("CacheSize %d out of range [0, 64k]", o.CacheSize)
	}
	if o.FillLevel < 10 || o.FillLevel > 100 {
		return fmt.Errorf("FillLevel %d out of range [10, 100]", o.FillLevel)
	}
	return nil
}
