// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
package fnv

import (
	"encoding/binary"
	"hash"
	"unsafe"
)

// from stdlib hash/fnv/fnv.go
const (
	prime64  = 1099511628211
	offset64 = 14695981039346656037
)

// InlineFNV64a is an alloc-free port of the standard library's fnv64a.
// See https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function
type fnv64a uint64

var _ hash.Hash64 = (*fnv64a)(nil)

// New64a returns a new instance of FNV 64a hash algorithm.
func New64a() fnv64a {
	return offset64
}

func (s *fnv64a) Size() int {
	return 8
}

func (s *fnv64a) BlockSize() int {
	return 1
}

func (s *fnv64a) Reset() {
	*s = offset64
}

// Write adds data to the running hash.
func (s *fnv64a) Write(data []byte) (int, error) {
	hash := uint64(*s)
	for _, c := range data {
		hash ^= uint64(c)
		hash *= prime64
	}
	*s = fnv64a(hash)
	return len(data), nil
}

// Write adds data to the running hash.
func (s *fnv64a) WriteString(data string) (int, error) {
	return s.Write(unsafe.Slice(unsafe.StringData(data), len(data)))
}

// Write adds a single byte b to the running hash.
func (s *fnv64a) WriteByte(b byte) error {
	_, err := s.Write([]byte{b})
	return err
}

// Sum64 returns the uint64 of the current resulting hash.
func (s *fnv64a) Sum64() uint64 {
	return uint64(*s)
}

// Sum returns the uint64 of the current resulting hash.
func (s *fnv64a) Sum(b []byte) []byte {
	return binary.BigEndian.AppendUint64(b, uint64(*s))
}

// Sum64a returns the FNV64a hash of b.
func Sum64a(b []byte) uint64 {
	h := New64a()
	h.Write(b)
	return h.Sum64()
}
