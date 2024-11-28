// Copyright (c) 2022-2024 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build amd64 && !gccgo && !appengine
// +build amd64,!gccgo,!appengine

package avx2

// ASM imports

//go:noescape
func packBytes32BitAVX2Core(src []uint64, dst []byte)

//go:noescape
func unpackBytes32BitAVX2Core(src []byte, dst []uint64)

//go:noescape
func packBytes16BitAVX2Core(src []uint64, dst []byte)

//go:noescape
func unpackBytes16BitAVX2Core(src []byte, dst []uint64)

// Go package exports
var (
	PackBytes16Bit   = packBytes16BitAVX2
	PackBytes32Bit   = packBytes32BitAVX2
	UnpackBytes16Bit = unpackBytes16BitAVX2
	UnpackBytes32Bit = unpackBytes32BitAVX2
)

func packBytes16BitAVX2(src []uint64, buf []byte) {
	len_head := len(src) & 0x7ffffffffffffff0
	packBytes16BitAVX2Core(src, buf)

	tmp := buf[len_head*2:]
	for i, v := range src[len_head:] {
		tmp[2*i] = byte((v >> 8) & 0xff)
		tmp[1+2*i] = byte(v & 0xff)
	}
}

func packBytes32BitAVX2(src []uint64, buf []byte) {
	len_head := len(src) & 0x7ffffffffffffff8
	packBytes32BitAVX2Core(src, buf)

	tmp := buf[len_head*4:]
	for i, v := range src[len_head:] {
		tmp[4*i] = byte((v >> 24) & 0xff)
		tmp[1+4*i] = byte((v >> 16) & 0xff)
		tmp[2+4*i] = byte((v >> 8) & 0xff)
		tmp[3+4*i] = byte(v & 0xff)
	}
}

func unpackBytes16BitAVX2(src []byte, res []uint64) {
	rlen := len(src) / 2
	len_head := rlen & 0x7ffffffffffffff0
	unpackBytes16BitAVX2Core(src, res)

	tmp := src[len_head*2:]

	for i, j := len_head, 0; i < rlen; i++ {
		res[i] = uint64(tmp[j])<<8 | uint64(tmp[1+j])
		j += 2
	}
}

func unpackBytes32BitAVX2(src []byte, res []uint64) {
	rlen := len(src) / 4
	len_head := rlen & 0x7ffffffffffffff8
	unpackBytes32BitAVX2Core(src, res)

	tmp := src[len_head*4:]

	for i, j := len_head, 0; i < rlen; i++ {
		res[i] = uint64(tmp[j])<<24 | uint64(tmp[1+j])<<16 | uint64(tmp[2+j])<<8 | uint64(tmp[3+j])
		j += 4
	}
}
