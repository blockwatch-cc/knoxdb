// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build !amd64 || appengine || gccgo
// +build !amd64 appengine gccgo

package compress

func zzDecodeInt64(data []int64) {
	zzDecodeInt64Generic(data)
}

func zzDecodeUint64(data []uint64) {
	zzDecodeUint64Generic(data)
}

func delta8DecodeUint64(data []uint64) {
	delta8DecodeUint64Generic(data)
}

func delta8EncodeUint64(data []uint64) uint64 {
	return delta8EncodeUint64Generic(data)
}

func zzDeltaEncodeUint64(data []uint64) uint64 {
	return zzDeltaEncodeUint64Generic(data)
}

func zzDeltaEncodeUint32(data []uint32) uint32 {
	return zzDeltaEncodeUint32Generic(data)
}

func zzDeltaEncodeUint16(data []uint16) uint16 {
	return zzDeltaEncodeUint16Generic(data)
}

func zzDeltaEncodeUint8(data []uint8) uint8 {
	return zzDeltaEncodeUint8Generic(data)
}

// func zzDeltaEncodeInt64(data []uint64) uint64 {
// 	return zzDeltaEncodeInt64Generic(data)
// }

// func zzDeltaEncodeInt32(data []uint32) uint32 {
// 	return zzDeltaEncodeInt32Generic(data)
// }

// func zzDeltaEncodeInt16(data []uint16) uint16 {
// 	return zzDeltaEncodeInt16Generic(data)
// }

// func zzDeltaEncodeInt8(data []uint8) uint8 {
// 	return zzDeltaEncodeInt8Generic(data)
// }

func zzDeltaDecodeUint64(data []uint64) {
	zzDeltaDecodeUint64Generic(data)
}

// func zzDeltaDecodeUint32(data []uint32) {
// 	zzDeltaDecodeUint32Generic(data)
// }

// func zzDeltaDecodeUint16(data []uint16) {
// 	zzDeltaDecodeUint16Generic(data)
// }

// func zzDeltaDecodeUint8(data []uint8) {
// 	zzDeltaDecodeUint8Generic(data)
// }

func zzDeltaDecodeInt64(data []int64) {
	zzDeltaDecodeInt64Generic(data)
}

func zzDeltaDecodeInt32(data []int32) {
	zzDeltaDecodeInt32Generic(data)
}

func zzDeltaDecodeInt16(data []int16) {
	zzDeltaDecodeInt16Generic(data)
}

func zzDeltaDecodeInt8(data []int8) {
	zzDeltaDecodeInt8Generic(data)
}
