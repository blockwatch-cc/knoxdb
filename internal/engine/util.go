// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"encoding/binary"
)

var BE = binary.BigEndian

func Key64Bytes(u64 uint64) []byte {
	var key [8]byte
	BE.PutUint64(key[:], u64)
	return key[:]
}

func Key64(buf []byte) uint64 {
	return BE.Uint64(buf)
}
