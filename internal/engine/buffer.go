// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"sync/atomic"
)

type Buffer struct {
	ref int64
	buf []byte
}

func (b *Buffer) IncRef() int64 {
	return atomic.AddInt64(&b.ref, 1)
}

func (b *Buffer) DecRef() int64 {
	return atomic.AddInt64(&b.ref, -1)
}

func (b *Buffer) HeapSize() int {
	return 8 + 24 + len(b.buf)
}

func (b *Buffer) Bytes() []byte {
	return b.buf
}

func NewBuffer(b []byte) *Buffer {
	return &Buffer{buf: b, ref: 1}
}
