// Copyright (c) 2021 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build go1.7 && amd64 && !gccgo && !appengine
// +build go1.7,amd64,!gccgo,!appengine

package llbVec

import (
	"blockwatch.cc/knoxdb/util"
)

// go:noescape
func filterAddManyUint32AVX2Core(f LogLogBeta, data []uint32, seed uint32)

// // go:noescape
// func filterAddManyUint32AVX512Core(f LogLogBeta, data []uint32, seed uint32)

// // go:noescape
// func filterAddManyUint64AVX2Core(f LogLogBeta, data []uint64, seed uint32)

// // go:noescape
// func filterAddManyUint64AVX512Core(f LogLogBeta, data []uint64, seed uint32)

// // go:noescape
// func filterCardinalityAVX2(f LogLogBeta)

// // go:noescape
// func filterCardinalityAVX512(f LogLogBeta)

func filterAddManyUint32(f *LogLogBeta, data []uint32, seed uint32) {
	switch {
//	case util.UseAVX512_CD:
//        filterAddManyUint32AVX512(*f, data, seed)   
	case util.UseAVX2:
        filterAddManyUint32AVX2(*f, data, seed)	
    default:
        filterAddManyUint32Generic(*f, data, seed)
	}
}

func filterAddManyUint64(f *LogLogBeta, data []uint64, seed uint32) {
	switch {
//	case util.UseAVX512_CD:
//        filterAddManyUint64AVX512(*f, data, seed)   
//	case util.UseAVX2:
//        filterAddManyUint64AVX2(*f, data, seed)	
    default:
        filterAddManyUint64Generic(*f, data, seed)
	}
}

func filterCardinality(f *LogLogBeta) uint64 {
	switch {
//	case util.UseAVX512_F:
//        return filterCardinalityAVX512(*f)   
//	case util.UseAVX2:
//        return filterCardinalityAVX2(*f)	
    default:
        return filterCardinalityGeneric(*f)
	}
}

func filterAddManyUint32AVX2(f LogLogBeta, data []uint32, seed uint32) {
	len_head := len(data) & 0x7ffffffffffffff8
	filterAddManyUint32AVX2Core(f, data, seed)
	filterAddManyUint32Generic(f, data[len_head:], seed)
}
