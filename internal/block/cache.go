// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package block

import "blockwatch.cc/knoxdb/pkg/cache"

type (
	BlockCache struct {
		*cache.PartitionedCache[*Block]
	}
	BlockCachePartition = *cache.CachePartition[*Block]
)

func NewCache(sz int) BlockCache {
	c := cache.NewPartitionedCache[*Block](sz)
	return BlockCache{c}
}

var NoCache BlockCachePartition = NewCache(0).Partition(0)
