// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

type TableStats struct {
	// tuple statistics
	TupleCount     int64 `json:"tuples_count"`
	InsertedTuples int64 `json:"tuples_inserted"`
	UpdatedTuples  int64 `json:"tuples_updated"`
	DeletedTuples  int64 `json:"tuples_deleted"`
	FlushedTuples  int64 `json:"tuples_flushed"`
	QueriedTuples  int64 `json:"tuples_queried"`
	StreamedTuples int64 `json:"tuples_streamed"`

	// metadata statistics
	MetaBytesRead    int64 `json:"meta_bytes_read"`
	MetaBytesWritten int64 `json:"meta_bytes_written"`

	// call statistics
	InsertCalls int64 `json:"calls_insert"`
	UpdateCalls int64 `json:"calls_update"`
	DeleteCalls int64 `json:"calls_delete"`
	FlushCalls  int64 `json:"calls_flush"`
	QueryCalls  int64 `json:"calls_query"`
	StreamCalls int64 `json:"calls_stream"`

	// journal statistics
	JournalPacksStored     int64 `json:"journal_packs_stored"`
	JournalFlushedTuples   int64 `json:"journal_tuples_flushed"`
	JournalBytesWritten    int64 `json:"journal_bytes_written"`
	TombstonePacksStored   int64 `json:"tomb_packs_stored"`
	TombstoneFlushedTuples int64 `json:"tomb_tuples_flushed"`
	TombstoneBytesWritten  int64 `json:"tomb_bytes_written"`

	// pack statistics
	PacksCount       int64 `json:"packs_count"`
	PacksCached      int64 `json:"packs_cached"`
	PacksAlloc       int64 `json:"packs_alloc"`
	PacksRecycled    int64 `json:"packs_recycled"`
	PacksLoaded      int64 `json:"packs_loaded"`
	PacksStored      int64 `json:"packs_stored"`
	PackBytesRead    int64 `json:"packs_bytes_read"`
	PackBytesWritten int64 `json:"packs_bytes_written"`

	// pack cache statistics
	PackCacheHits      int64 `json:"pack_cache_hits"`
	PackCacheMisses    int64 `json:"pack_cache_misses"`
	PackCacheInserts   int64 `json:"pack_cache_inserts"`
	PackCacheUpdates   int64 `json:"pack_cache_updates"`
	PackCacheEvictions int64 `json:"pack_cache_evictions"`

	// index tuple statistics
	IndexInsertedTuples int64 `json:"index_tuples_inserted"`
	IndexDeletedTuples  int64 `json:"index_tuples_deleted"`
	IndexFlushedTuples  int64 `json:"index_tuples_flushed"`
	IndexQueriedTuples  int64 `json:"index_tuples_queried"`

	// index call statistics
	IndexInsertCalls int64 `json:"index_calls_insert"`
	IndexDeleteCalls int64 `json:"index_calls_delete"`
	IndexFlushCalls  int64 `json:"index_calls_flush"`
	IndexQueryCalls  int64 `json:"index_calls_query"`

	// index statistics
	IndexPacksCount    int64 `json:"index_packs_count"`
	IndexPacksCached   int64 `json:"index_packs_cached"`
	IndexPacksAlloc    int64 `json:"index_packs_alloc"`
	IndexPacksRecycled int64 `json:"index_packs_recycle"`
	IndexPacksLoaded   int64 `json:"index_packs_loaded"`
	IndexPacksStored   int64 `json:"index_packs_stored"`
	IndexBytesRead     int64 `json:"index_packs_bytes_read"`
	IndexBytesWritten  int64 `json:"index_packs_bytes_written"`

	// index cache statistics
	IndexCacheHits      int64 `json:"index_cache_hits"`
	IndexCacheMisses    int64 `json:"index_cache_misses"`
	IndexCacheInserts   int64 `json:"index_cache_inserts"`
	IndexCacheUpdates   int64 `json:"index_cache_updates"`
	IndexCacheEvictions int64 `json:"index_cache_evictions"`
}

type TableSizeStats struct {
	IndexSize     int `json:"index_size"`
	CacheSize     int `json:"cache_size"`
	JournalSize   int `json:"journal_size"`
	TombstoneSize int `json:"tombstone_size"`
	TotalSize     int `json:"total_size"`
}

type IndexSizeStats struct {
	CacheSize     int `json:"cache_size"`
	JournalSize   int `json:"journal_size"`
	TombstoneSize int `json:"tombstone_size"`
	TotalSize     int `json:"total_size"`
}
