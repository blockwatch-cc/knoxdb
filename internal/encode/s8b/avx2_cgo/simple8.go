package main

// #cgo CFLAGS: -mavx2 -O3
// #include "simple8.h"
import "C"
import (
	"fmt"
	"unsafe"
)

func EncodeUint64(dst []byte, src []uint64, minv, maxv uint64) ([]byte, error) {
	if len(src) == 0 {
		return dst[:0], nil
	}
	n := C.encode_u64_avx2(
		(*C.uint8_t)(unsafe.Pointer(&dst[0])),
		(*C.uint64_t)(unsafe.Pointer(&src[0])),
		C.size_t(len(src)),
		C.uint64_t(minv),
	)
	if int(n) < 0 {
		return nil, fmt.Errorf("error %d", -int(n))
	}
	return dst[:int(n)], nil
}

func EncodeUint32(dst []byte, src []uint32, minv, maxv uint32) ([]byte, error) {
	if len(src) == 0 {
		return dst[:0], nil
	}
	n := C.encode_u32_avx2(
		(*C.uint8_t)(unsafe.Pointer(&dst[0])),
		(*C.uint32_t)(unsafe.Pointer(&src[0])),
		C.size_t(len(src)),
		C.uint32_t(minv),
	)
	return dst[:int(n)], nil
}

func EncodeUint16(dst []byte, src []uint16, minv, maxv uint16) ([]byte, error) {
	if len(src) == 0 {
		return dst[:0], nil
	}
	n := C.encode_u16_avx2(
		(*C.uint8_t)(unsafe.Pointer(&dst[0])),
		(*C.uint16_t)(unsafe.Pointer(&src[0])),
		C.size_t(len(src)),
		C.uint16_t(minv),
	)
	return dst[:int(n)], nil
}

func EncodeUint8(dst []byte, src []uint8, minv, maxv uint8) ([]byte, error) {
	if len(src) == 0 {
		return dst[:0], nil
	}
	n := C.encode_u8_avx2(
		(*C.uint8_t)(unsafe.Pointer(&dst[0])),
		(*C.uint8_t)(unsafe.Pointer(&src[0])),
		C.size_t(len(src)),
		C.uint8_t(minv),
	)
	return dst[:int(n)], nil
}
