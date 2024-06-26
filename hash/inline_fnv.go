// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
package hash

import (
	"encoding/binary"
)

// from stdlib hash/fnv/fnv.go
const (
	prime64  = 1099511628211
	offset64 = 14695981039346656037
)

// InlineFNV64a is an alloc-free port of the standard library's fnv64a.
// See https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function
type InlineFNV64a uint64

// NewInlineFNV64a returns a new instance of InlineFNV64a.
func NewInlineFNV64a() InlineFNV64a {
	return offset64
}

// Write adds data to the running hash.
func (s *InlineFNV64a) Write(data []byte) error {
	hash := uint64(*s)
	for _, c := range data {
		hash ^= uint64(c)
		hash *= prime64
	}
	*s = InlineFNV64a(hash)
	return nil
}

// Write adds data to the running hash.
func (s *InlineFNV64a) WriteString(data string) error {
	return s.Write([]byte(data))
}

// Write adds a single byte b to the running hash.
func (s *InlineFNV64a) WriteByte(b byte) error {
	return s.Write([]byte{b})
}

// Sum64 returns the uint64 of the current resulting hash.
func (s *InlineFNV64a) Sum64() uint64 {
	return uint64(*s)
}

func (s *InlineFNV64a) Sum() []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], s.Sum64())
	return buf[:]
}
