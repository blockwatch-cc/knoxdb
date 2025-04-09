// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build amd64 && !gccgo && !appengine
// +build amd64,!gccgo,!appengine

package hashprobe

import (
	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/pkg/util"
)

//go:noescape
func ht_build64(vals, dict, ht_keys *uint64, ht_values *uint16, len uint32, dict_size *uint32)

//go:noescape
func ht_build32(vals, dict, ht_keys *uint32, ht_values *uint16, len uint32, dict_size *uint32)

//go:noescape
func ht_encode64(vals, ht_keys *uint64, ht_values, codes *uint16, len uint32)

//go:noescape
func ht_encode32(vals, ht_keys *uint32, ht_values, codes *uint16, len uint32)

func buildDictAVX2[T Integer](vals []T, numUnique int) ([]T, []uint16) {
	switch util.SizeOf[T]() {
	case 8:
		u64 := util.ReinterpretSlice[T, uint64](vals)
		r64, codes := buildDict64AVX2(u64, numUnique)
		return util.ReinterpretSlice[uint64, T](r64), codes
	case 4:
		u32 := util.ReinterpretSlice[T, uint32](vals)
		r32, codes := buildDict32AVX2(u32, numUnique)
		return util.ReinterpretSlice[uint32, T](r32), codes
	default:
		return nil, nil
	}
}

func buildDict64AVX2(vals []uint64, numUnique int) ([]uint64, []uint16) {
	// alloc table or reuse
	table := allocHashTable[uint64]()

	// Step 1: Deduplicate into hash table and extract unique keys
	var retDictSize uint32
	dictLen := safeDictLen(numUnique)
	dict := arena.AllocT[uint64](dictLen)[:dictLen]
	ht_build64(
		&vals[0],
		&dict[0],
		&table.keys[0],
		&table.codes[0],
		uint32(len(vals)),
		&retDictSize,
	)
	dict = dict[:retDictSize] // Trim to actual size

	// Step 2: Sort keys
	util.Sort(dict, 0)

	// Step 3: Assign codes in sorted order
	for i, key := range dict {
		h := table.hash16(key)
		var p uint16
		for table.keys[h] != key {
			p++
			h = (h + p*p) & 0xFFFF
		}
		table.codes[h] = uint16(i)
	}

	// encode values
	codes := arena.AllocT[uint16](len(vals))[:len(vals)]
	ht_encode64(
		&vals[0],
		&table.keys[0],
		&table.codes[0],
		&codes[0],
		uint32(len(vals)),
	)

	// reclaim
	freeHashTable(table)

	return dict, codes
}

func buildDict32AVX2(vals []uint32, numUnique int) ([]uint32, []uint16) {
	// alloc table or reuse
	table := allocHashTable[uint32]()

	// Step 1: Deduplicate into hash table and extract unique keys
	var retDictSize uint32
	dictLen := safeDictLen(numUnique)
	dict := arena.AllocT[uint32](dictLen)[:dictLen]
	ht_build32(
		&vals[0],
		&dict[0],
		&table.keys[0],
		&table.codes[0],
		uint32(len(vals)),
		&retDictSize,
	)
	dict = dict[:retDictSize] // Trim to actual size

	// Step 2: Sort keys
	util.Sort(dict, 0)

	// Step 3: Assign codes in sorted order
	for i, key := range dict {
		h := table.hash16(uint64(key))
		var p uint16
		for table.keys[h] != key {
			p++
			h = (h + p*p) & 0xFFFF
		}
		table.codes[h] = uint16(i)
	}

	// encode values
	codes := arena.AllocT[uint16](len(vals))[:len(vals)]
	ht_encode32(
		&vals[0],
		&table.keys[0],
		&table.codes[0],
		&codes[0],
		uint32(len(vals)),
	)

	// reclaim
	freeHashTable(table)

	return dict, codes
}

// Debug version, kept for reference
// func buildDict64AVX2(vals []uint64, numUnique int) ([]uint64, []uint16) {
// 	// alloc table or reuse
// 	table := allocHashTable[uint64]()

// 	// Step 1: Deduplicate into hash table and extract unique keys
// 	var retDictSize uint32
// 	dictLen := safeDictLen(numUnique)
// 	dict := arena.AllocT[uint64](dictLen)[:dictLen]
// 	clear(dict)
// 	fmt.Printf("Build len=%d unique=%d dict-len=%d dict=%p\n", len(vals), numUnique, len(dict), &dict[0])
// 	for i, v := range vals {
// 		fmt.Printf("Val %-2d = %x\n", i, v)
// 	}
// 	ht_build64(
// 		&vals[0],
// 		&dict[0],
// 		&table.keys[0],
// 		&table.codes[0],
// 		uint32(len(vals)),
// 		&retDictSize,
// 	)

// 	// count how many slots are used
// 	var nSlots int
// 	for _, v := range table.codes {
// 		if v != 0xFFFF {
// 			nSlots++
// 		}
// 	}
// 	fmt.Printf("Card=%d dict-len=%d\n", nSlots, retDictSize)
// 	dict = dict[:retDictSize] // Trim to actual size

// 	// Step 2: Sort keys
// 	util.Sort(dict, 0)

// 	for i, v := range dict {
// 		fmt.Printf("Dict %-2d = %x\n", i, v)
// 	}

// 	// Step 3: Assign codes in sorted order
// 	for i, key := range dict {
// 		h := table.hash16(key)
// 		var p uint16
// 		for table.keys[h] != key {
// 			p++
// 			h = (h + p*p) & 0xFFFF
// 		}
// 		table.codes[h] = uint16(i)
// 	}

// 	// encode values
// 	fmt.Printf("Encode len=%d dict=%d\n", len(vals), len(dict))
// 	codes := arena.AllocT[uint16](len(vals))[:len(vals)]
// 	ht_encode64(
// 		&vals[0],
// 		&table.keys[0],
// 		&table.codes[0],
// 		&codes[0],
// 		uint32(len(vals)),
// 	)
// 	for i, v := range codes {
// 		fmt.Printf("Code %-2d = %d\n", i, v)
// 	}

// 	// reclaim
// 	freeHashTable(table)

// 	return dict, codes
// }
