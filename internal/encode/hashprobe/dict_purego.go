// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package hashprobe

import (
	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/cpu"
	"blockwatch.cc/knoxdb/pkg/util"
)

func BuildDict[T Integer](vals []T, numUnique int) ([]T, []uint16) {
	if cpu.UseAVX2 {
		return buildDictAVX2(vals, numUnique)
	}
	return buildDictGeneric(vals, numUnique)
}

// Note: requires we change the hash function to make it mix upper
// bits before masking the lower 16 bits. Not doing this results in
// excessive hash table collisions.
func BuildFloatDict[T float32 | float64](vals []T, numUnique int) ([]T, []uint16) {
	var (
		dict  []T
		codes []uint16
	)
	switch any(T(0)).(type) {
	case float32:
		u32 := util.ReinterpretSlice[T, uint32](vals)
		d, c := BuildDict(u32, numUnique)
		dict = util.ReinterpretSlice[uint32, T](d)
		codes = c
	case float64:
		u64 := util.ReinterpretSlice[T, uint64](vals)
		d, c := BuildDict(u64, numUnique)
		dict = util.ReinterpretSlice[uint64, T](d)
		codes = c
	}
	return dict, codes
}

func buildDictGeneric[T Integer](vals []T, numUnique int) ([]T, []uint16) {
	// alloc or reuse table
	table := allocHashTable[T]().Init()

	// Step 1: Deduplicate into hash table
	for _, v := range vals {
		h := table.hash16(uint64(v))
		var p uint16
		for table.codes[h] != HASH_MASK && table.keys[h] != v {
			p++
			h = (h + p*p) & HASH_MASK // Quadratic probe
		}
		if table.codes[h] == HASH_MASK { // New entry
			table.keys[h] = v
			table.codes[h] = 0
		}
	}

	// Step 2: Extract unique keys
	dict := arena.Alloc[T](safeDictLen(numUnique))
	for i, v := range table.codes {
		if v != HASH_MASK {
			dict = append(dict, table.keys[i])
		}
	}

	// Step 3: Sort keys
	util.Sort(dict, 0)

	// Step 4: Assign codes in sorted order
	for i, key := range dict {
		h := table.hash16(uint64(key))
		var p uint16
		for table.keys[h] != key {
			p++
			h = (h + p*p) & HASH_MASK
		}
		table.codes[h] = uint16(i)
	}

	// Step 4: encode values
	codes := arena.Alloc[uint16](len(vals))[:len(vals)]
	for i, v := range vals {
		h := table.hash16(uint64(v))
		var p uint16
		for table.keys[h] != v {
			p++
			h = (h + p*p) & HASH_MASK
		}
		codes[i] = table.codes[h]
	}

	freeHashTable(table)

	return dict, codes
}
