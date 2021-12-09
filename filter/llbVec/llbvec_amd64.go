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

// go:noescape
func filterAddManyUint32AVX512Core(f LogLogBeta, data []uint32, seed uint32)

// go:noescape
func filterAddManyInt32AVX2Core(f LogLogBeta, data []int32, seed uint32)

// go:noescape
func filterAddManyInt32AVX512Core(f LogLogBeta, data []int32, seed uint32)

// go:noescape
func filterAddManyUint64AVX2Core(f LogLogBeta, data []uint64, seed uint32)

// go:noescape
func filterAddManyUint64AVX512Core(f LogLogBeta, data []uint64, seed uint32)

// go:noescape
func filterAddManyInt64AVX2Core(f LogLogBeta, data []int64, seed uint32)

// go:noescape
func filterAddManyInt64AVX512Core(f LogLogBeta, data []int64, seed uint32)

// // go:noescape
// func filterCardinalityAVX2(f LogLogBeta)

// // go:noescape
// func filterCardinalityAVX512(f LogLogBeta)

func filterAddManyUint32(f *LogLogBeta, data []uint32, seed uint32) {
	switch {
	case util.UseAVX512_CD:
        filterAddManyUint32AVX512(*f, data, seed)   
	case util.UseAVX2:
        filterAddManyUint32AVX2(*f, data, seed)	
    default:
        filterAddManyUint32Generic(*f, data, seed)
	}
}

func filterAddManyInt32(f *LogLogBeta, data []int32, seed uint32) {
	switch {
	case util.UseAVX512_CD:
        filterAddManyInt32AVX512(*f, data, seed)   
	case util.UseAVX2:
        filterAddManyInt32AVX2(*f, data, seed)	
    default:
        filterAddManyInt32Generic(*f, data, seed)
	}
}

func filterAddManyUint64(f *LogLogBeta, data []uint64, seed uint32) {
	switch {
	case util.UseAVX512_CD:
        filterAddManyUint64AVX512(*f, data, seed)   
	case util.UseAVX2:
        filterAddManyUint64AVX2(*f, data, seed)	
    default:
        filterAddManyUint64Generic(*f, data, seed)
	}
}

func filterAddManyInt64(f *LogLogBeta, data []int64, seed uint32) {
	switch {
	case util.UseAVX512_CD:
        filterAddManyInt64AVX512(*f, data, seed)   
	case util.UseAVX2:
        filterAddManyInt64AVX2(*f, data, seed)	
    default:
        filterAddManyInt64Generic(*f, data, seed)
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

func filterAddManyUint32AVX512(f LogLogBeta, data []uint32, seed uint32) {
	len_head := len(data) & 0x7ffffffffffffff0
	filterAddManyUint32AVX512Core(f, data, seed)
	filterAddManyUint32Generic(f, data[len_head:], seed)
}

func filterAddManyInt32AVX2(f LogLogBeta, data []int32, seed uint32) {
	len_head := len(data) & 0x7ffffffffffffff8
	filterAddManyInt32AVX2Core(f, data, seed)
	filterAddManyInt32Generic(f, data[len_head:], seed)
}

func filterAddManyInt32AVX512(f LogLogBeta, data []int32, seed uint32) {
	len_head := len(data) & 0x7ffffffffffffff0
	filterAddManyInt32AVX512Core(f, data, seed)
	filterAddManyInt32Generic(f, data[len_head:], seed)
}

func filterAddManyUint64AVX2(f LogLogBeta, data []uint64, seed uint32) {
	len_head := len(data) & 0x7ffffffffffffff8
	filterAddManyUint64AVX2Core(f, data, seed)
	filterAddManyUint64Generic(f, data[len_head:], seed)
}

func filterAddManyUint64AVX512(f LogLogBeta, data []uint64, seed uint32) {
	len_head := len(data) & 0x7ffffffffffffff0
	filterAddManyUint64AVX512Core(f, data, seed)
	filterAddManyUint64Generic(f, data[len_head:], seed)
}

func filterAddManyInt64AVX2(f LogLogBeta, data []int64, seed uint32) {
	len_head := len(data) & 0x7ffffffffffffff8
	filterAddManyInt64AVX2Core(f, data, seed)
	filterAddManyInt64Generic(f, data[len_head:], seed)
}

func filterAddManyInt64AVX512(f LogLogBeta, data []int64, seed uint32) {
	len_head := len(data) & 0x7ffffffffffffff0
	filterAddManyInt64AVX512Core(f, data, seed)
	filterAddManyInt64Generic(f, data[len_head:], seed)
}
