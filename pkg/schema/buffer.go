// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//go:build ignore
// +build ignore

package schema

import (
	"fmt"
	"unsafe"
)

type Buffer struct {
	buf     []byte
	fixedSz int
	wfpos   int
	wvpos   int
	rfpos   int
	rvpos   int
}

func NewBuffer(n, fixedSz, variableSz int) *Buffer {
	return &Buffer{
		buf:     make([]byte, n*(fixedSz+variableSz)),
		fixedSz: fixedSz,
		wvpos:   fixedSz,
		rvpos:   fixedSz,
	}
}

func NewBufferFromBytes(buf []byte, fixedSz int) *Buffer {
	return &Buffer{
		buf:   buf,
		wfpos: fixedSz,
		wvpos: -1,
		rvpos: fixedSz,
	}
}

func (b *Buffer) Reset() {
	b.wvpos = b.fixedSz
	b.rvpos = b.fixedSz
	b.wfpos = 0
	b.rfpos = 0
}

func (b *Buffer) Seal() {
	b.wfpos = b.wvpos
	b.wvpos = b.wfpos + b.fixedSz
	b.rfpos = b.rvpos
	b.rvpos = b.rfpos + b.fixedSz
}

// bytes in the unread portion (fixed + variable)
func (b *Buffer) Len() int {
	return b.fixedSz - b.rfpos + len(b.buf) - b.rvpos
}

func (b *Buffer) FixedLen() int {
	return b.fixedSz - b.rfpos
}

func (b *Buffer) Bytes() []byte {
	return b.buf[:min(len(b.buf), b.wvpos)]
}

func (b *Buffer) Next(n int) []byte {
	start := b.rfpos
	b.rfpos += n
	return b.buf[start:b.rfpos]
}

func (b *Buffer) NextTail(n int) []byte {
	start := b.rvpos
	b.rvpos += n
	return b.buf[start:b.rvpos]
}

func (b *Buffer) NextPtr(n int) unsafe.Pointer {
	start := b.rfpos
	b.rfpos += n
	_ = b.buf[b.rfpos+n-1]
	return unsafe.Pointer(&b.buf[start])
}

func (b *Buffer) NextTailPtr(n int) unsafe.Pointer {
	start := b.rvpos
	b.rvpos += n
	_ = b.buf[b.rvpos+n-1]
	return unsafe.Pointer(&b.buf[start])
}

func (b *Buffer) Write(buf []byte) (int, error) {
	if b.wfpos >= len(b.buf) {
		panic(fmt.Errorf("buffer is full"))
		// return 0, fmt.Errorf("buffer is full")
	}
	n := len(buf)
	copy(b.buf[b.wfpos:], buf)
	b.wfpos += n
	return n, nil
}

func (b *Buffer) WriteTail(buf []byte) (int, error) {
	if b.wvpos < 0 {
		panic(fmt.Errorf("buffer is read only"))
		// return 0, fmt.Errorf("buffer is read only")
	}
	n := len(buf)
	if cap(b.buf) < b.wvpos+n {
		cp := make([]byte, b.wvpos+n)
		copy(cp, b.buf[:b.wvpos])
		b.buf = cp
	}
	copy(b.buf[b.wvpos:b.wvpos+n], buf)
	b.wvpos += n
	return n, nil
}
