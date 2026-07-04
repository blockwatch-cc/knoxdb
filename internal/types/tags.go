// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package types

import "github.com/zeebo/xxh3"

type ObjectTag byte

const (
	ObjectTagDatabase ObjectTag = 'D'
	ObjectTagTable    ObjectTag = 'T'
	ObjectTagIndex    ObjectTag = 'I'
	ObjectTagView     ObjectTag = 'V'
	ObjectTagEnum     ObjectTag = 'E'
	ObjectTagStream   ObjectTag = 'R'
	ObjectTagSnapshot ObjectTag = 'S'
)

func (t ObjectTag) IsValid() bool {
	switch t {
	case ObjectTagDatabase,
		ObjectTagTable,
		ObjectTagIndex,
		ObjectTagView,
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
	var h xxh3.Hasher
	h.Write([]byte{byte(tag)})
	h.WriteString(name)
	return h.Sum64()
}
