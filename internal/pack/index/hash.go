// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import "blockwatch.cc/knoxdb/internal/hash/fnv"

// func genHashKey32(buf []byte) []byte {
//  var u32 [4]byte
//  LE.PutUint32(u32[:], xxHash32.Checksum(buf, 0))
//  return u32[:]
// }

type hashFunc func([]byte) []byte

func genHashKey64(buf []byte) []byte {
	// reuse buffer when large enough and overwrite the first 8 bytes with hash
	res := buf
	if cap(res) < 16 {
		res = make([]byte, 16)
	}
	res = res[:16]

	// write hash
	var hash [8]byte
	LE.PutUint64(hash[:], fnv.Sum64a(buf[:len(buf)-8]))

	// copy pk from buffer tail
	copy(res[8:], buf[len(buf)-8:])

	// copy hash to buffer start
	copy(res, hash[:])

	return res
}

func genNoopKey(buf []byte) []byte {
	return buf
}

// expand byte, word, dword to quadword little endian keys
func makeKeyGen(sz int) func([]byte) []byte {
	switch sz {
	case 1, 2, 4:
		return func(buf []byte) []byte {
			// reuse buffer when large enough and overwrite the first 8 bytes with hash
			res := buf
			if cap(res) < 16 {
				res = make([]byte, 16)
			}
			res = res[:16]

			// copy pk to buffer end
			copy(res[8:], buf[len(buf)-8:])

			// expend integer to u64 at buffer start
			copy(res, buf[:len(buf)-8])

			return res
		}
	default:
		return genNoopKey
	}
}
