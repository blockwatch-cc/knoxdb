// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"time"
)

type TableStats struct {
	// global statistics
	TableName         string        `json:"table_name,omitempty"`
	IndexName         string        `json:"index_name,omitempty"`
	LastFlushTime     time.Time     `json:"last_flush_time"`
	LastFlushDuration time.Duration `json:"last_flush_duration"`

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
	JournalTuplesCount     int64 `json:"journal_tuples_count"`
	JournalTuplesThreshold int64 `json:"journal_tuples_threshold"`
	JournalTuplesCapacity  int64 `json:"journal_tuples_capacity"`
	JournalPacksStored     int64 `json:"journal_packs_stored"`
	JournalTuplesFlushed   int64 `json:"journal_tuples_flushed"`
	JournalBytesWritten    int64 `json:"journal_bytes_written"`

	// tombstone statistics
	TombstoneSize            int64 `json:"tombstone_size"`
	TombstoneTuplesCount     int64 `json:"tomb_tuples_count"`
	TombstoneTuplesThreshold int64 `json:"tomb_tuples_threshold"`
	TombstoneTuplesCapacity  int64 `json:"tomb_tuples_capacity"`
	TombstonePacksStored     int64 `json:"tomb_packs_stored"`
	TombstoneTuplesFlushed   int64 `json:"tomb_tuples_flushed"`
	TombstoneBytesWritten    int64 `json:"tomb_bytes_written"`

	// pack statistics
	PacksCount        int64 `json:"packs_count"`
	PacksAlloc        int64 `json:"packs_alloc"`
	PacksRecycled     int64 `json:"packs_recycled"`
	PacksLoaded       int64 `json:"packs_loaded"`
	PacksStored       int64 `json:"packs_stored"`
	PacksBytesRead    int64 `json:"packs_bytes_read"`
	PacksBytesWritten int64 `json:"packs_bytes_written"`
	PacksSize         int64 `json:"packs_size"`

	// pack cache statistics
	PackCacheSize      int64 `json:"pack_cache_size"`
	PackCacheCount     int64 `json:"pack_cache_count"`
	PackCacheCapacity  int64 `json:"pack_cache_capacity"`
	PackCacheHits      int64 `json:"pack_cache_hits"`
	PackCacheMisses    int64 `json:"pack_cache_misses"`
	PackCacheInserts   int64 `json:"pack_cache_inserts"`
	PackCacheUpdates   int64 `json:"pack_cache_updates"`
	PackCacheEvictions int64 `json:"pack_cache_evictions"`
}
