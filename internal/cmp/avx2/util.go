// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build amd64
// +build amd64

package avx2

func fillBits(buf []byte, n int) int64 {
	buf[0] = 0xff
	for i := 1; i < len(buf); i *= 2 {
		copy(buf[i:], buf[:i])
	}
	buf[len(buf)-1] = byte(0xff >> (7 - (n-1)&7))
	return int64(n)
}
