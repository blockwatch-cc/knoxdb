// Copyright (c) 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

// +build go1.7,amd64,!gccgo,!appengine

package compress

//go:noescape
func Delta8AVX2(src []uint64) uint64

//go:noescape
func Undelta8AVX2(src []uint64)

//go:noescape
func PackIndex32BitAVX2(src []uint64, dst []byte)

//go:noescape
func UnpackIndex32BitAVX2(src []byte, dst []uint64)

//go:noescape
func PackIndex16BitAVX2(src []uint64, dst []byte)

//go:noescape
func UnpackIndex16BitAVX2(src []byte, dst []uint64)
