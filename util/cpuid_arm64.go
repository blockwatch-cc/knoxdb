// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build arm64 || appengine || gccgo
// +build arm64 appengine gccgo

package util

import (
    "log"
)

func DisableAVX2() {
    // noop
}

func DisableAVX512() {
    // noop
}

func EnableAVX() {
    // noop
}

var (
    UseAVX2 bool

    UseAVX512_F            bool // AVX-512 Foundation Instructions
    UseAVX512_DQ           bool // AVX-512 Doubleword & Quadword Instrs
    UseAVX512_IFMA         bool // AVX-512 Integer Fused Multiply Add
    UseAVX512_PF           bool // AVX-512 Prefetch Instructions
    UseAVX512_ER           bool // AVX-512 Exponent & Reciprocal Instrs
    UseAVX512_CD           bool // AVX-512 Conflict Detection Instrs
    UseAVX512_BW           bool // AVX-512 Byte and Word Instructions
    UseAVX512_VL           bool // AVX-512 Vector Length Extensions
    UseAVX512_VBMI         bool // AVX-512 Vector Byte Manipulation Instrs
    UseAVX512_BITALG       bool // Support for VPOPCNT[B,W] and VPSHUFBITQMB
    UseAVX512_VPOPCNTDQ    bool // POPCNT for vectors of DW/QW
    UseAVX512_4VNNIW       bool // AVX512 Neural Network Instructions
    UseAVX512_4FMAPS       bool // AVX512 Multiply Accumulation Single Precision
    UseAVX512_VP2INTERSECT bool // VP2INTERSECT{D,Q} insns
    UseAVX512_BF16         bool // AVX512 BFloat16 Instructions
)

func LogAVXFeatures(l *log.Logger) {
    l.Printf("AVX2 %t", UseAVX2)
    l.Printf("AVX512-F %t", UseAVX512_F)
    l.Printf("AVX512-DQ %t", UseAVX512_DQ)
    l.Printf("AVX512-IFMA %t", UseAVX512_IFMA)
    l.Printf("AVX512-PF %t", UseAVX512_PF)
    l.Printf("AVX512-ER %t", UseAVX512_ER)
    l.Printf("AVX512-CD %t", UseAVX512_CD)
    l.Printf("AVX512-BW %t", UseAVX512_BW)
    l.Printf("AVX512-VL %t", UseAVX512_VL)
    l.Printf("AVX512-VBMI %t", UseAVX512_VBMI)
    l.Printf("AVX512-BITALG %t", UseAVX512_BITALG)
    l.Printf("AVX512-VPOPCNTDQ %t", UseAVX512_VPOPCNTDQ)
    l.Printf("AVX512-4VNNIW %t", UseAVX512_4VNNIW)
    l.Printf("AVX512-4FMAPS %t", UseAVX512_4FMAPS)
    l.Printf("AVX512-VP2INTERSECT %t", UseAVX512_VP2INTERSECT)
    l.Printf("AVX512-BF16 %t", UseAVX512_BF16)
}
