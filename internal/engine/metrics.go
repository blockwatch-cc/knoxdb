// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"sync/atomic"
)

type DatabaseMetrics struct {
	CacheCapacity int64
	CacheSize     int64
}

type StoreMetrics struct {
	Name           string
	NumKeys        int64
	CacheHits      int64
	CacheMisses    int64
	CacheInserts   int64
	CacheEvictions int64 // cannot measure on shared caches
	CacheCount     int64 // cannot measure on shared caches
	InsertedKeys   int64
	UpdatedKeys    int64
	DeletedKeys    int64
	QueriedKeys    int64
	TotalSize      int64
	BytesWritten   int64
	BytesRead      int64
}

func NewStoreMetrics(name string) StoreMetrics {
	return StoreMetrics{
		Name: name,
	}
}

type TableMetrics struct {
	// global statistics
	Name              string `json:"name,omitempty"`
	LastMergeTime     int64  `json:"last_merge_time"`
	LastMergeDuration int64  `json:"last_merge_duration"`

	// tuple statistics
	TupleCount     int64 `json:"tuples_count"`
	InsertedTuples int64 `json:"tuples_inserted"`
	UpdatedTuples  int64 `json:"tuples_updated"`
	DeletedTuples  int64 `json:"tuples_deleted"`
	MergedTuples   int64 `json:"tuples_merged"`
	QueriedTuples  int64 `json:"tuples_queried"`
	StreamedTuples int64 `json:"tuples_streamed"`

	// call statistics
	InsertCalls int64 `json:"calls_insert"`
	UpdateCalls int64 `json:"calls_update"`
	DeleteCalls int64 `json:"calls_delete"`
	MergeCalls  int64 `json:"calls_merge"`
	QueryCalls  int64 `json:"calls_query"`
	StreamCalls int64 `json:"calls_stream"`

	// metadata statistics
	MetaBytesRead    int64 `json:"meta_bytes_read"`
	MetaBytesWritten int64 `json:"meta_bytes_written"`
	MetaSize         int64 `json:"meta_size"`

	// journal statistics
	JournalSize       int64 `json:"journal_size"`
	JournalSegments   int64 `json:"journal_segments"`
	JournalTuples     int64 `json:"journal_tuples"`
	JournalCapacity   int64 `json:"journal_capacity"`
	JournalTombstones int64 `json:"journal_tombstones"`

	// pack statistics
	PacksCount   int64 `json:"packs_count"`
	PacksLoaded  int64 `json:"packs_loaded"`
	PacksStored  int64 `json:"packs_stored"`
	BlocksLoaded int64 `json:"blocks_loaded"`
	BlocksStored int64 `json:"blocks_stored"`

	// I/O statistics
	BytesRead    int64 `json:"bytes_read"`
	BytesWritten int64 `json:"bytes_written"`
	TotalSize    int64 `json:"total_size"`

	// pack cache statistics
	CacheSize      int64 `json:"cache_size"`
	CacheCount     int64 `json:"cache_count"`
	CacheCapacity  int64 `json:"cache_capacity"`
	CacheHits      int64 `json:"cache_hits"`
	CacheMisses    int64 `json:"cache_misses"`
	CacheInserts   int64 `json:"cache_inserts"`
	CacheEvictions int64 `json:"cache_evictions"`
}

func NewTableMetrics(name string) TableMetrics {
	return TableMetrics{
		Name: name,
	}
}

func (s *TableMetrics) Clone() (c TableMetrics) {
	c.Name = s.Name
	c.LastMergeTime = atomic.LoadInt64(&s.LastMergeTime)
	c.LastMergeDuration = atomic.LoadInt64(&s.LastMergeDuration)

	// tuple statistics
	c.TupleCount = atomic.LoadInt64(&s.TupleCount)
	c.InsertedTuples = atomic.LoadInt64(&s.InsertedTuples)
	c.UpdatedTuples = atomic.LoadInt64(&s.UpdatedTuples)
	c.DeletedTuples = atomic.LoadInt64(&s.DeletedTuples)
	c.MergedTuples = atomic.LoadInt64(&s.MergedTuples)
	c.QueriedTuples = atomic.LoadInt64(&s.QueriedTuples)
	c.StreamedTuples = atomic.LoadInt64(&s.StreamedTuples)

	// call statistics
	c.InsertCalls = atomic.LoadInt64(&s.InsertCalls)
	c.UpdateCalls = atomic.LoadInt64(&s.UpdateCalls)
	c.DeleteCalls = atomic.LoadInt64(&s.DeleteCalls)
	c.MergeCalls = atomic.LoadInt64(&s.MergeCalls)
	c.QueryCalls = atomic.LoadInt64(&s.QueryCalls)
	c.StreamCalls = atomic.LoadInt64(&s.StreamCalls)

	// metadata statistics
	c.MetaBytesRead = atomic.LoadInt64(&s.MetaBytesRead)
	c.MetaBytesWritten = atomic.LoadInt64(&s.MetaBytesWritten)
	c.MetaSize = atomic.LoadInt64(&s.MetaSize)

	// journal statistics
	c.JournalSize = atomic.LoadInt64(&s.JournalSize)
	c.JournalSegments = atomic.LoadInt64(&s.JournalSegments)
	c.JournalTuples = atomic.LoadInt64(&s.JournalTuples)
	c.JournalCapacity = atomic.LoadInt64(&s.JournalCapacity)
	c.JournalTombstones = atomic.LoadInt64(&s.JournalTombstones)

	// pack statistics
	c.PacksCount = atomic.LoadInt64(&s.PacksCount)
	c.PacksLoaded = atomic.LoadInt64(&s.PacksLoaded)
	c.PacksStored = atomic.LoadInt64(&s.PacksStored)

	// I/O statistics
	c.BytesRead = atomic.LoadInt64(&s.BytesRead)
	c.BytesWritten = atomic.LoadInt64(&s.BytesWritten)
	c.TotalSize = atomic.LoadInt64(&s.TotalSize)

	// pack cache statistics
	c.CacheSize = atomic.LoadInt64(&s.CacheSize)
	c.CacheCount = atomic.LoadInt64(&s.CacheCount)
	c.CacheCapacity = atomic.LoadInt64(&s.CacheCapacity)
	c.CacheHits = atomic.LoadInt64(&s.CacheHits)
	c.CacheMisses = atomic.LoadInt64(&s.CacheMisses)
	c.CacheInserts = atomic.LoadInt64(&s.CacheInserts)
	c.CacheEvictions = atomic.LoadInt64(&s.CacheEvictions)

	return
}

type IndexMetrics struct {
	// global statistics
	Name              string `json:"name,omitempty"`
	LastMergeTime     int64  `json:"last_merge_time"`
	LastMergeDuration int64  `json:"last_merge_duration"`

	// tuple statistics
	TupleCount     int64 `json:"tuples_count"`
	InsertedTuples int64 `json:"tuples_inserted"`
	DeletedTuples  int64 `json:"tuples_deleted"`
	QueriedTuples  int64 `json:"tuples_queried"`

	// call statistics
	NumCalls int64 `json:"num_calls"`

	// pack statistics
	PacksCount   int64 `json:"packs_count"`
	PacksLoaded  int64 `json:"packs_loaded"`
	PacksStored  int64 `json:"packs_stored"`
	BlocksLoaded int64 `json:"blocks_loaded"`
	BlocksStored int64 `json:"blocks_stored"`

	// I/O statistics
	BytesRead    int64 `json:"bytes_read"`
	BytesWritten int64 `json:"bytes_written"`
	TotalSize    int64 `json:"total_size"`
}

func NewIndexMetrics(name string) IndexMetrics {
	return IndexMetrics{
		Name: name,
	}
}
