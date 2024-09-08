// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import "sync/atomic"

type DatabaseStats struct {
	CacheCapacity int64
	CacheSize     int64
}

type StoreStats struct {
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

type TableStats struct {
	// global statistics
	Name              string `json:"name,omitempty"`
	LastFlushTime     int64  `json:"last_flush_time"`
	LastFlushDuration int64  `json:"last_flush_duration"`

	// tuple statistics
	TupleCount     int64 `json:"tuples_count"`
	InsertedTuples int64 `json:"tuples_inserted"`
	UpdatedTuples  int64 `json:"tuples_updated"`
	DeletedTuples  int64 `json:"tuples_deleted"`
	FlushedTuples  int64 `json:"tuples_flushed"`
	QueriedTuples  int64 `json:"tuples_queried"`
	StreamedTuples int64 `json:"tuples_streamed"`

	// call statistics
	InsertCalls int64 `json:"calls_insert"`
	UpdateCalls int64 `json:"calls_update"`
	DeleteCalls int64 `json:"calls_delete"`
	FlushCalls  int64 `json:"calls_flush"`
	QueryCalls  int64 `json:"calls_query"`
	StreamCalls int64 `json:"calls_stream"`

	// metadata statistics
	MetaBytesRead    int64 `json:"meta_bytes_read"`
	MetaBytesWritten int64 `json:"meta_bytes_written"`
	MetaSize         int64 `json:"meta_size"`

	// journal statistics
	JournalSize            int64 `json:"journal_size"`
	JournalDiskSize        int64 `json:"journal_disk_size"`
	JournalTuplesCount     int64 `json:"journal_tuples_count"`
	JournalTuplesThreshold int64 `json:"journal_tuples_threshold"`
	JournalTuplesCapacity  int64 `json:"journal_tuples_capacity"`
	JournalPacksStored     int64 `json:"journal_packs_stored"`
	JournalTuplesFlushed   int64 `json:"journal_tuples_flushed"`
	JournalBytesWritten    int64 `json:"journal_bytes_written"`

	// tombstone statistics
	TombstoneSize            int64 `json:"tomb_size"`
	TombstoneDiskSize        int64 `json:"tomb_disk_size"`
	TombstoneTuplesCount     int64 `json:"tomb_tuples_count"`
	TombstoneTuplesThreshold int64 `json:"tomb_tuples_threshold"`
	TombstoneTuplesCapacity  int64 `json:"tomb_tuples_capacity"`
	TombstonePacksStored     int64 `json:"tomb_packs_stored"`
	TombstoneTuplesFlushed   int64 `json:"tomb_tuples_flushed"`
	TombstoneBytesWritten    int64 `json:"tomb_bytes_written"`

	// pack statistics
	PacksCount    int64 `json:"packs_count"`
	PacksAlloc    int64 `json:"packs_alloc"`
	PacksRecycled int64 `json:"packs_recycled"`
	PacksLoaded   int64 `json:"packs_loaded"`
	PacksStored   int64 `json:"packs_stored"`
	BlocksLoaded  int64 `json:"blocks_loaded"`
	BlocksStored  int64 `json:"blocks_stored"`

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

func (s *TableStats) Clone() (c TableStats) {
	c.Name = s.Name
	c.LastFlushTime = atomic.LoadInt64(&s.LastFlushTime)
	c.LastFlushDuration = atomic.LoadInt64(&s.LastFlushDuration)

	// tuple statistics
	c.TupleCount = atomic.LoadInt64(&s.TupleCount)
	c.InsertedTuples = atomic.LoadInt64(&s.InsertedTuples)
	c.UpdatedTuples = atomic.LoadInt64(&s.UpdatedTuples)
	c.DeletedTuples = atomic.LoadInt64(&s.DeletedTuples)
	c.FlushedTuples = atomic.LoadInt64(&s.FlushedTuples)
	c.QueriedTuples = atomic.LoadInt64(&s.QueriedTuples)
	c.StreamedTuples = atomic.LoadInt64(&s.StreamedTuples)

	// call statistics
	c.InsertCalls = atomic.LoadInt64(&s.InsertCalls)
	c.UpdateCalls = atomic.LoadInt64(&s.UpdateCalls)
	c.DeleteCalls = atomic.LoadInt64(&s.DeleteCalls)
	c.FlushCalls = atomic.LoadInt64(&s.FlushCalls)
	c.QueryCalls = atomic.LoadInt64(&s.QueryCalls)
	c.StreamCalls = atomic.LoadInt64(&s.StreamCalls)

	// metadata statistics
	c.MetaBytesRead = atomic.LoadInt64(&s.MetaBytesRead)
	c.MetaBytesWritten = atomic.LoadInt64(&s.MetaBytesWritten)
	c.MetaSize = atomic.LoadInt64(&s.MetaSize)

	// journal statistics
	c.JournalSize = atomic.LoadInt64(&s.JournalSize)
	c.JournalDiskSize = atomic.LoadInt64(&s.JournalDiskSize)
	c.JournalTuplesCount = atomic.LoadInt64(&s.JournalTuplesCount)
	c.JournalTuplesThreshold = atomic.LoadInt64(&s.JournalTuplesThreshold)
	c.JournalTuplesCapacity = atomic.LoadInt64(&s.JournalTuplesCapacity)
	c.JournalPacksStored = atomic.LoadInt64(&s.JournalPacksStored)
	c.JournalTuplesFlushed = atomic.LoadInt64(&s.JournalTuplesFlushed)
	c.JournalBytesWritten = atomic.LoadInt64(&s.JournalBytesWritten)

	// tombstone statistics
	c.TombstoneSize = atomic.LoadInt64(&s.TombstoneSize)
	c.TombstoneDiskSize = atomic.LoadInt64(&s.TombstoneDiskSize)
	c.TombstoneTuplesCount = atomic.LoadInt64(&s.TombstoneTuplesCount)
	c.TombstoneTuplesThreshold = atomic.LoadInt64(&s.TombstoneTuplesThreshold)
	c.TombstoneTuplesCapacity = atomic.LoadInt64(&s.TombstoneTuplesCapacity)
	c.TombstonePacksStored = atomic.LoadInt64(&s.TombstonePacksStored)
	c.TombstoneTuplesFlushed = atomic.LoadInt64(&s.TombstoneTuplesFlushed)
	c.TombstoneBytesWritten = atomic.LoadInt64(&s.TombstoneBytesWritten)

	// pack statistics
	c.PacksCount = atomic.LoadInt64(&s.PacksCount)
	c.PacksAlloc = atomic.LoadInt64(&s.PacksAlloc)
	c.PacksRecycled = atomic.LoadInt64(&s.PacksRecycled)
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

type IndexStats struct {
	// global statistics
	Name              string `json:"name,omitempty"`
	LastFlushTime     int64  `json:"last_flush_time"`
	LastFlushDuration int64  `json:"last_flush_duration"`

	// tuple statistics
	TupleCount     int64 `json:"tuples_count"`
	InsertedTuples int64 `json:"tuples_inserted"`
	DeletedTuples  int64 `json:"tuples_deleted"`
	QueriedTuples  int64 `json:"tuples_queried"`

	// call statistics
	InsertCalls int64 `json:"calls_insert"`
	DeleteCalls int64 `json:"calls_delete"`
	QueryCalls  int64 `json:"calls_query"`

	// metadata statistics
	MetaBytesRead    int64 `json:"meta_bytes_read"`
	MetaBytesWritten int64 `json:"meta_bytes_written"`
	MetaSize         int64 `json:"meta_size"`

	// pack statistics
	// PacksCount    int64 `json:"packs_count"`
	// PacksAlloc    int64 `json:"packs_alloc"`
	// PacksRecycled int64 `json:"packs_recycled"`
	// PacksLoaded   int64 `json:"packs_loaded"`
	// PacksStored   int64 `json:"packs_stored"`
	// BlocksLoaded  int64 `json:"blocks_loaded"`
	// BlocksStored  int64 `json:"blocks_stored"`

	// I/O statistics
	BytesRead    int64 `json:"bytes_read"`
	BytesWritten int64 `json:"bytes_written"`
	TotalSize    int64 `json:"total_size"`

	// pack cache statistics
	// CacheSize      int64 `json:"cache_size"`
	// CacheCount     int64 `json:"cache_count"`
	// CacheCapacity  int64 `json:"cache_capacity"`
	// CacheHits      int64 `json:"cache_hits"`
	// CacheMisses    int64 `json:"cache_misses"`
	// CacheInserts   int64 `json:"cache_inserts"`
	// CacheEvictions int64 `json:"cache_evictions"`
}

// func (t *Table) Stats() []TableStats {
// 	s := t.stats.Clone()

// 	// update from journal and tomb (reading here may be more efficient than
// 	// update on change, but creates a data race)
// 	s.JournalTuplesCount = int64(t.journal.data.Len())
// 	s.JournalTuplesCapacity = int64(t.journal.data.Cap())
// 	s.JournalSize = int64(t.journal.data.HeapSize())

// 	s.TombstoneTuplesCount = int64(len(t.journal.tomb))
// 	s.TombstoneTuplesCapacity = int64(cap(t.journal.tomb))
// 	s.TombstoneSize = s.TombstoneTuplesCount * 8

// 	// copy cache stats
// 	cs := t.bcache.Stats()
// 	s.CacheHits = cs.Hits
// 	s.CacheMisses = cs.Misses
// 	s.CacheInserts = cs.Inserts
// 	s.CacheEvictions = cs.Evictions
// 	s.CacheCount = cs.Count
// 	s.CacheSize = cs.Size

// 	resp := []TableStats{s}
// 	for _, idx := range t.indexes {
// 		resp = append(resp, idx.Stats())
// 	}
// 	return resp
// }
