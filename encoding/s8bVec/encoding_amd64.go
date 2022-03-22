package s8bVec

import (
	"errors"
	"unsafe"
)

//go:noescape
func DecodeAllAVX2Opt(dst, src []uint64) (value int)

//go:noescape
func initAVX2Opt()

//go:noescape
func DecodeAllAVX2Jmp(dst, src []uint64) (value int)

//go:noescape
func DecodeAllAVX2JmpLoop()

//go:noescape
func DecodeAllAVX2JmpRet()

//go:noescape
func DecodeAllAVX2JmpExit()

//go:noescape
func DecodeAllAVX2OptExit()

//go:noescape
func initAVX2Jmp()

//go:noescape
func initAVX2Call()

//go:noescape
func DecodeAllAVX2Call(dst, src []uint64) (value int)

//go:noescape
func DecodeBytesBigEndianAVX2Core(dst []uint64, src []byte) (value int)

//go:noescape
func unpack1AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack2AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack3AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack4AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack5AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack6AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack7AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack8AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack10AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack12AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack15AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack20AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack30AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack60AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack120AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack240AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack1AVX2Jmp(v uint64, dst *[240]uint64)

//go:noescape
func unpack2AVX2Jmp(v uint64, dst *[240]uint64)

//go:noescape
func unpack3AVX2Jmp(v uint64, dst *[240]uint64)

//go:noescape
func unpack4AVX2Jmp(v uint64, dst *[240]uint64)

//go:noescape
func unpack5AVX2Jmp(v uint64, dst *[240]uint64)

//go:noescape
func unpack6AVX2Jmp(v uint64, dst *[240]uint64)

//go:noescape
func unpack7AVX2Jmp(v uint64, dst *[240]uint64)

//go:noescape
func unpack8AVX2Jmp(v uint64, dst *[240]uint64)

//go:noescape
func unpack10AVX2Jmp(v uint64, dst *[240]uint64)

//go:noescape
func unpack12AVX2Jmp(v uint64, dst *[240]uint64)

//go:noescape
func unpack15AVX2Jmp(v uint64, dst *[240]uint64)

//go:noescape
func unpack20AVX2Jmp(v uint64, dst *[240]uint64)

//go:noescape
func unpack30AVX2Jmp(v uint64, dst *[240]uint64)

//go:noescape
func unpack60AVX2Jmp(v uint64, dst *[240]uint64)

//go:noescape
func unpack120AVX2Jmp(v uint64, dst *[240]uint64)

//go:noescape
func unpack240AVX2Jmp(v uint64, dst *[240]uint64)

//go:noescape
func unpack1AVX2Opt(v uint64, dst *[240]uint64)

//go:noescape
func unpack2AVX2Opt(v uint64, dst *[240]uint64)

//go:noescape
func unpack3AVX2Opt(v uint64, dst *[240]uint64)

//go:noescape
func unpack4AVX2Opt(v uint64, dst *[240]uint64)

//go:noescape
func unpack5AVX2Opt(v uint64, dst *[240]uint64)

//go:noescape
func unpack6AVX2Opt(v uint64, dst *[240]uint64)

//go:noescape
func unpack7AVX2Opt(v uint64, dst *[240]uint64)

//go:noescape
func unpack8AVX2Opt(v uint64, dst *[240]uint64)

//go:noescape
func unpack10AVX2Opt(v uint64, dst *[240]uint64)

//go:noescape
func unpack12AVX2Opt(v uint64, dst *[240]uint64)

//go:noescape
func unpack15AVX2Opt(v uint64, dst *[240]uint64)

//go:noescape
func unpack20AVX2Opt(v uint64, dst *[240]uint64)

//go:noescape
func unpack30AVX2Opt(v uint64, dst *[240]uint64)

//go:noescape
func unpack60AVX2Opt(v uint64, dst *[240]uint64)

//go:noescape
func unpack120AVX2Opt(v uint64, dst *[240]uint64)

//go:noescape
func unpack240AVX2Opt(v uint64, dst *[240]uint64)

//go:noescape
func unpack1AVX2(v uint64, dst *[240]uint64)

//go:noescape
func unpack2AVX2(v uint64, dst *[240]uint64)

//go:noescape
func unpack3AVX2(v uint64, dst *[240]uint64)

//go:noescape
func unpack4AVX2(v uint64, dst *[240]uint64)

//go:noescape
func unpack5AVX2(v uint64, dst *[240]uint64)

//go:noescape
func unpack6AVX2(v uint64, dst *[240]uint64)

//go:noescape
func unpack7AVX2(v uint64, dst *[240]uint64)

//go:noescape
func unpack8AVX2(v uint64, dst *[240]uint64)

//go:noescape
func unpack10AVX2(v uint64, dst *[240]uint64)

//go:noescape
func unpack12AVX2(v uint64, dst *[240]uint64)

//go:noescape
func unpack15AVX2(v uint64, dst *[240]uint64)

//go:noescape
func unpack20AVX2(v uint64, dst *[240]uint64)

//go:noescape
func unpack30AVX2(v uint64, dst *[240]uint64)

//go:noescape
func unpack60AVX2(v uint64, dst *[240]uint64)

//go:noescape
func unpack120AVX2(v uint64, dst *[240]uint64)

//go:noescape
func unpack240AVX2(v uint64, dst *[240]uint64)

func init() {
	initAVX2Jmp()
	initAVX2Opt()
	initAVX2Call()
}

var selectorAVX2 [16]packing = [16]packing{
	packing{240, 0, unpack240AVX2, pack240},
	packing{120, 0, unpack120AVX2, pack120},
	packing{60, 1, unpack60AVX2, pack60},
	packing{30, 2, unpack30AVX2, pack30},
	packing{20, 3, unpack20AVX2, pack20},
	packing{15, 4, unpack15AVX2, pack15},
	packing{12, 5, unpack12AVX2, pack12},
	packing{10, 6, unpack10AVX2, pack10},
	packing{8, 7, unpack8AVX2, pack8},
	packing{7, 8, unpack7AVX2, pack7},
	packing{6, 10, unpack6AVX2, pack6},
	packing{5, 12, unpack5AVX2, pack5},
	packing{4, 15, unpack4AVX2, pack4},
	packing{3, 20, unpack3AVX2, pack3},
	packing{2, 30, unpack2AVX2, pack2},
	packing{1, 60, unpack1AVX2, pack1},
}

// Decode writes the uncompressed values from src to dst.  It returns the number
// of values written or an error.
//go:nocheckptr
// nocheckptr while the underlying struct layout doesn't change
func DecodeAllAVX2(dst, src []uint64) (value int, err error) {
	j := 0
	for _, v := range src {
		sel := (v >> 60) & 0xf
		selectorAVX2[sel].unpack(v, (*[240]uint64)(unsafe.Pointer(&dst[j])))
		j += selector[sel].n
	}
	return j, nil
}

func DecodeBytesBigEndianAVX2(dst []uint64, src []byte) (value int, err error) {
	if len(src)&7 != 0 {
		return 0, errors.New("src length is not multiple of 8")
	}
	return DecodeBytesBigEndianAVX2Core(dst, src), nil
}
