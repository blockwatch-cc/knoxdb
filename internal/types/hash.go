// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package types

import "blockwatch.cc/knoxdb/internal/hash/fnv"

type HashTag byte

const (
	HashTagDatabase HashTag = 'D'
	HashTagTable    HashTag = 'T'
	HashTagIndex    HashTag = 'I'
	HashTagView     HashTag = 'V'
	HashTagStore    HashTag = 'S'
	HashTagEnum     HashTag = 'E'
	HashTagStream   HashTag = 'R'
	HashTagSnapshot HashTag = 'N'
)

func TaggedHash(tag HashTag, name string) uint64 {
	h := fnv.New64a()
	h.WriteByte(byte(tag))
	h.WriteString(name)
	return h.Sum64()
}
