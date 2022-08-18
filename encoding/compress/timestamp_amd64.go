// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build go1.7 && amd64 && !gccgo && !appengine
// +build go1.7,amd64,!gccgo,!appengine

package compress

import "blockwatch.cc/knoxdb/util"

//go:noescape
func deltaScaleDecodeTimeAVX2Core(data []uint64, mod uint64)

func deltaScaleDecodeTime(data []uint64, mod uint64) {
	switch {
	case util.UseAVX2:
		deltaScaleDecodeTimeAVX2(data, mod)
	default:
		deltaScaleDecodeTimeGeneric(data, mod)
	}
}

func deltaScaleDecodeTimeAVX2(data []uint64, mod uint64) {
	len_head := len(data) & 0x7ffffffffffffffc
	deltaScaleDecodeTimeAVX2Core(data, mod)
	var prev uint64
	if len_head != 0 {
		prev = data[len_head-1]
	}
	for i := len_head; i < len(data); i++ {
		prev += data[i] * mod
		data[i] = prev
	}

}
