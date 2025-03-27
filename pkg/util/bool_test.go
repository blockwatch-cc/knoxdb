// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package util

import (
	"bytes"
	"testing"
)

// Test whether the Go compiler efficiently replaces calls to Bool2int with a
// single instruction.

func BenchmarkBoolIf(b *testing.B) {
	buf := bytes.Repeat([]byte{0xaa}, 256)
	for i := range b.N {
		workWithIf(buf, i)
	}
}

func workWithIf(buf []byte, i int) int {
	var work int
	if buf[i%256]&0x1 > 0 {
		work++
	}
	if buf[i%256]>>1&0x1 > 0 {
		work++
	}
	if buf[i%256]>>2&0x1 > 0 {
		work++
	}
	if buf[i%256]>>3&0x1 > 0 {
		work++
	}
	if buf[i%256]>>4&0x1 > 0 {
		work++
	}
	if buf[i%256]>>5&0x1 > 0 {
		work++
	}
	if buf[i%256]>>6&0x1 > 0 {
		work++
	}
	if buf[i%256]>>7&0x1 > 0 {
		work++
	}
	return work
}

func BenchmarkBoolPredicate(b *testing.B) {
	buf := bytes.Repeat([]byte{0xaa}, 256)
	for i := range b.N {
		workWithPredicate(buf, i)
	}
}

func workWithPredicate(buf []byte, i int) int {
	var work int
	work += Bool2int(buf[i%256]&0x1 > 0)
	work += Bool2int(buf[i%256]>>1&0x1 > 0)
	work += Bool2int(buf[i%256]>>2&0x1 > 0)
	work += Bool2int(buf[i%256]>>3&0x1 > 0)
	work += Bool2int(buf[i%256]>>4&0x1 > 0)
	work += Bool2int(buf[i%256]>>5&0x1 > 0)
	work += Bool2int(buf[i%256]>>6&0x1 > 0)
	work += Bool2int(buf[i%256]>>7&0x1 > 0)
	return work
}
