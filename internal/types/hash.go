// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package types

import "blockwatch.cc/knoxdb/internal/hash/fnv"

type ObjectTag byte

const (
	ObjectTagDatabase ObjectTag = 'D'
	ObjectTagTable    ObjectTag = 'T'
	ObjectTagIndex    ObjectTag = 'I'
	ObjectTagView     ObjectTag = 'V'
	ObjectTagStore    ObjectTag = 'S'
	ObjectTagEnum     ObjectTag = 'E'
	ObjectTagStream   ObjectTag = 'R'
	ObjectTagSnapshot ObjectTag = 'N'
)

func TaggedHash(tag ObjectTag, name string) uint64 {
	h := fnv.New64a()
	h.WriteByte(byte(tag))
	h.WriteString(name)
	return h.Sum64()
}
