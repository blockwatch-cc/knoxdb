// Copyright (c) 2020-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package util

import (
	"log"
	"os"
	"strconv"

	"golang.org/x/sys/cpu"
)

var (
	IsAMD64              bool
	UseAVX2              bool // AVX-2 available
	UseAVX512_F          bool // AVX-512 Foundation Instructions
	UseAVX512_DQ         bool // AVX-512 Doubleword & Quadword Instrs
	UseAVX512_IFMA       bool // AVX-512 Integer Fused Multiply Add
	UseAVX512_PF         bool // AVX-512 Prefetch Instructions
	UseAVX512_ER         bool // AVX-512 Exponent & Reciprocal Instrs
	UseAVX512_CD         bool // AVX-512 Conflict Detection Instrs
	UseAVX512_BW         bool // AVX-512 Byte and Word Instructions
	UseAVX512_VL         bool // AVX-512 Vector Length Extensions
	UseAVX512_VBMI       bool // AVX-512 Vector Byte Manipulation Instrs
	UseAVX512_BITALG     bool // AVX512 Support for VPOPCNT[B,W] and VPSHUFBITQMB
	UseAVX512_VPOPCNTDQ  bool // AVX512 POPCNT for vectors of DW/QW
	UseAVX512_4VNNIW     bool // AVX512 Neural Network Instructions
	UseAVX512_4FMAPS     bool // AVX512 Multiply Accumulation Single Precision
	UseAVX512_BF16       bool // AVX512 BFloat16 Instructions
	UseAVX512_VPCLMULQDQ bool // AVX512 carry-less multiply operations
	UseAVX512_VNNI       bool // Advanced vector extension 512 Vector Neural Network Instructions
	UseAVX512_GFNI       bool // Advanced vector extension 512 Galois field New Instructions
	UseAVX512_VAES       bool // Advanced vector extension 512 Vector AES instructions
	UseAVX512_VBMI2      bool // Advanced vector extension 512 Vector Byte Manipulation Instructions 2
)

func init() {
	EnableAVX()
}

func EnableAVX() {
	if !cpu.Initialized {
		return
	}
	IsAMD64 = cpu.X86.HasAVX || cpu.X86.HasAVX2

	if !env("NOAVX2") {
		UseAVX2 = cpu.X86.HasAVX2
	}

	if !env("NOAVX512") {
		UseAVX512_F = cpu.X86.HasAVX512F
		UseAVX512_DQ = cpu.X86.HasAVX512DQ
		UseAVX512_IFMA = cpu.X86.HasAVX512IFMA
		UseAVX512_PF = cpu.X86.HasAVX512PF
		UseAVX512_ER = cpu.X86.HasAVX512ER
		UseAVX512_CD = cpu.X86.HasAVX512CD
		UseAVX512_BW = cpu.X86.HasAVX512BW
		UseAVX512_VL = cpu.X86.HasAVX512VL
		UseAVX512_VBMI = cpu.X86.HasAVX512VBMI
		UseAVX512_BITALG = cpu.X86.HasAVX512BITALG
		UseAVX512_VPOPCNTDQ = cpu.X86.HasAVX512VPOPCNTDQ
		UseAVX512_4VNNIW = cpu.X86.HasAVX5124VNNIW
		UseAVX512_4FMAPS = cpu.X86.HasAVX5124FMAPS
		UseAVX512_BF16 = cpu.X86.HasAVX512BF16
		UseAVX512_VPCLMULQDQ = cpu.X86.HasAVX512VPCLMULQDQ
		UseAVX512_VNNI = cpu.X86.HasAVX512VNNI
		UseAVX512_GFNI = cpu.X86.HasAVX512GFNI
		UseAVX512_VAES = cpu.X86.HasAVX512VAES
		UseAVX512_VBMI2 = cpu.X86.HasAVX512VBMI2
	}
}

func env(s string) bool {
	val := os.Getenv(s)
	if val == "" {
		return false
	}
	ok, err := strconv.ParseBool(val)
	if err == nil {
		return ok
	}
	return false
}

func DisableAVX2() {
	UseAVX2 = false
}

func DisableAVX512() {
	UseAVX512_F = false
	UseAVX512_DQ = false
	UseAVX512_IFMA = false
	UseAVX512_PF = false
	UseAVX512_ER = false
	UseAVX512_CD = false
	UseAVX512_BW = false
	UseAVX512_VL = false
	UseAVX512_VBMI = false
	UseAVX512_BITALG = false
	UseAVX512_VPOPCNTDQ = false
	UseAVX512_4VNNIW = false
	UseAVX512_4FMAPS = false
	UseAVX512_BF16 = false
	UseAVX512_VPCLMULQDQ = false
	UseAVX512_VNNI = false
	UseAVX512_GFNI = false
	UseAVX512_VAES = false
	UseAVX512_VBMI2 = false
}

func LogCPUFeatures(l *log.Logger) {
	if IsAMD64 {
		l.Printf("No AMD64 CPU detected")
	} else {
		l.Printf("AMD64 CPU detected")
		l.Printf(" AVX2 %t", UseAVX2)
		l.Printf(" AVX512-F %t", UseAVX512_F)
		l.Printf(" AVX512-DQ %t", UseAVX512_DQ)
		l.Printf(" AVX512-IFMA %t", UseAVX512_IFMA)
		l.Printf(" AVX512-PF %t", UseAVX512_PF)
		l.Printf(" AVX512-ER %t", UseAVX512_ER)
		l.Printf(" AVX512-CD %t", UseAVX512_CD)
		l.Printf(" AVX512-BW %t", UseAVX512_BW)
		l.Printf(" AVX512-VL %t", UseAVX512_VL)
		l.Printf(" AVX512-VBMI %t", UseAVX512_VBMI)
		l.Printf(" AVX512-BITALG %t", UseAVX512_BITALG)
		l.Printf(" AVX512-VPOPCNTDQ %t", UseAVX512_VPOPCNTDQ)
		l.Printf(" AVX512-4VNNIW %t", UseAVX512_4VNNIW)
		l.Printf(" AVX512-4FMAPS %t", UseAVX512_4FMAPS)
		l.Printf(" AVX512-BF16 %t", UseAVX512_BF16)
		l.Printf(" AVX512-VPCLMULQDQ %t", UseAVX512_VPCLMULQDQ)
		l.Printf(" AVX512-VNNI %t", UseAVX512_VNNI)
		l.Printf(" AVX512-GFNI %t", UseAVX512_GFNI)
		l.Printf(" AVX512-VAES %t", UseAVX512_VAES)
		l.Printf(" AVX512-VBMI2 %t", UseAVX512_VBMI2)
	}
}
