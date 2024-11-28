// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build !amd64 || appengine || gccgo
// +build !amd64 appengine gccgo

package compress

func packBytes8Bit(src []uint64, buf []byte) {
    packBytes8BitGeneric(src, buf)
}

func packBytes16Bit(src []uint64, buf []byte) {
    packBytes16BitGeneric(src, buf)
}

func packBytes24Bit(src []uint64, buf []byte) {
    packBytes24BitGeneric(src, buf)
}

func packBytes32Bit(src []uint64, buf []byte) {
    packBytes32BitGeneric(src, buf)
}

func unpackBytes8Bit(src []byte, dst []uint64) {
    unpackBytes8BitGeneric(src, dst)
}

func unpackBytes16Bit(src []byte, dst []uint64) {
    unpackBytes16BitGeneric(src, dst)
}

func unpackBytes24Bit(src []byte, dst []uint64) {
    unpackBytes24BitGeneric(src, dst)
}

func unpackBytes32Bit(src []byte, dst []uint64) {
    unpackBytes32BitGeneric(src, dst)
}
