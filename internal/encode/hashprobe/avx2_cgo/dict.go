package main

// #cgo CFLAGS: -O3 -mavx2
// #include "hashprobe.h"
import "C"

import (
	"unsafe"
)

type Signed interface {
	int64 | int32 | int16 | int8
}

type Unsigned interface {
	uint64 | uint32 | uint16 | uint8
}

type Integer interface {
	Signed | Unsigned
}

func IsSigned[T Integer]() bool {
	// Check if -1 is less than 0 in the type T
	// For signed types, this is true (e.g., -1 < 0)
	// For unsigned types, -1 wraps to MaxValue (e.g., 0xFF...FF), so it's false
	return T(0)-T(1) < T(0)
}

func EncodeDictHash64(vals []uint64, numUnique int) ([]uint64, []uint16) {
	// alloc table or reuse
	table := allocHashTable[uint64]()

	// Step 1: Deduplicate into hash table and extract unique keys
	var retDictSize C.size_t
	dictLen := safeDictLen(numUnique)
	dict := AllocT[uint64](dictLen)[:dictLen]
	C.ht_build64(
		(*C.uint64_t)(unsafe.Pointer(&vals[0])),
		(*C.uint64_t)(unsafe.Pointer(&table.keys[0])),
		(*C.uint16_t)(unsafe.Pointer(&table.codes[0])),
		(*C.uint64_t)(unsafe.Pointer(&dict[0])),
		C.size_t(len(vals)),
		(*C.size_t)(unsafe.Pointer(&retDictSize)),
	)
	dict = dict[:retDictSize] // Trim to actual size

	// Step 2: Sort keys
	Sort(dict, 0)

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
	codes := AllocT[uint16](len(vals))[:len(vals)]
	C.ht_encode64(
		(*C.uint64_t)(unsafe.Pointer(&vals[0])),
		(*C.uint64_t)(unsafe.Pointer(&table.keys[0])),
		(*C.uint16_t)(unsafe.Pointer(&table.codes[0])),
		(*C.uint16_t)(unsafe.Pointer(&codes[0])),
		C.size_t(len(vals)),
	)

	// reclaim
	freeHashTable(table)

	return dict, codes
}

func EncodeDictHash32(vals []uint32, numUnique int) ([]uint32, []uint16) {
	// alloc table or reuse
	table := allocHashTable[uint32]()

	// Step 1: Deduplicate into hash table and extract unique keys
	var retDictSize C.size_t
	dictLen := safeDictLen(numUnique)
	dict := AllocT[uint32](dictLen)[:dictLen]
	// fmt.Printf("v0=%x h0=%x\n", vals[0], table.hash16(uint64(vals[0])))
	// fmt.Printf("v1=%x h1=%x\n", vals[1], table.hash16(uint64(vals[1])))
	// fmt.Printf("v2=%x h2=%x\n", vals[2], table.hash16(uint64(vals[2])))
	// fmt.Printf("v3=%x h3=%x\n", vals[3], table.hash16(uint64(vals[3])))
	// fmt.Printf("v4=%x h4=%x\n", vals[4], table.hash16(uint64(vals[4])))
	// fmt.Printf("v5=%x h5=%x\n", vals[5], table.hash16(uint64(vals[5])))
	// fmt.Printf("v6=%x h6=%x\n", vals[6], table.hash16(uint64(vals[6])))
	// fmt.Printf("v7=%x h7=%x\n", vals[7], table.hash16(uint64(vals[7])))
	C.ht_build32(
		(*C.uint32_t)(unsafe.Pointer(&vals[0])),
		(*C.uint32_t)(unsafe.Pointer(&table.keys[0])),
		(*C.uint16_t)(unsafe.Pointer(&table.codes[0])),
		(*C.uint32_t)(unsafe.Pointer(&dict[0])),
		C.size_t(len(vals)),
		(*C.size_t)(unsafe.Pointer(&retDictSize)),
	)
	// return []uint32{}, []uint16{}
	dict = dict[:retDictSize] // Trim to actual size

	// Step 2: Sort keys
	Sort(dict, 0)

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
	codes := AllocT[uint16](len(vals))[:len(vals)]
	C.ht_encode32(
		(*C.uint32_t)(unsafe.Pointer(&vals[0])),
		(*C.uint32_t)(unsafe.Pointer(&table.keys[0])),
		(*C.uint16_t)(unsafe.Pointer(&table.codes[0])),
		(*C.uint16_t)(unsafe.Pointer(&codes[0])),
		C.size_t(len(vals)),
	)

	// reclaim
	freeHashTable(table)

	return dict, codes
}

func safeDictLen(n int) int {
	n <<= 1
	if d := n % 32; d > 0 {
		n += 32 - d
	}
	return n
}
