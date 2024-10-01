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

func (t ObjectTag) IsValid() bool {
	switch t {
	case ObjectTagDatabase,
		ObjectTagTable,
		ObjectTagIndex,
		ObjectTagView,
		ObjectTagStore,
		ObjectTagEnum,
		ObjectTagStream,
		ObjectTagSnapshot:
		return true
	default:
		return false
	}
}

func (t ObjectTag) String() string {
	switch t {
	case ObjectTagDatabase:
		return "database"
	case ObjectTagTable:
		return "table"
	case ObjectTagIndex:
		return "index"
	case ObjectTagView:
		return "view"
	case ObjectTagStore:
		return "store"
	case ObjectTagEnum:
		return "enum"
	case ObjectTagStream:
		return "stream"
	case ObjectTagSnapshot:
		return "snapshot"
	default:
		return "UNKNOWN_OBJECT"
	}
}

func TaggedHash(tag ObjectTag, name string) uint64 {
	h := fnv.New64a()
	h.WriteByte(byte(tag))
	h.WriteString(name)
	return h.Sum64()
}
