// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package alp

import (
	"unsafe"
)

const (
	PATCH_POSITION_SIZE       = 16
	PATCH_POSITION_SIZE_BYTES = PATCH_POSITION_SIZE / 8
)

type Float interface {
	float32 | float64
}

type Int interface {
	int32 | int64
}

type Uint interface {
	uint32 | uint64
}

type constant[T Float] struct {
	WIDTH                   int
	RD_SIZE_THRESHOLD_LIMIT int
	MAGIC_NUMBER            T
	PATCH_SIZE              int
	MAX_EXPONENT            uint8
	F10                     []T
	IF10                    []T
}

var (
	c32 = constant[float32]{
		WIDTH:                   4,
		RD_SIZE_THRESHOLD_LIMIT: 22 * SAMPLE_SIZE, // 22 bits per value * 32
		MAGIC_NUMBER:            12582912.0,
		PATCH_SIZE:              32,
		MAX_EXPONENT:            10,

		F10: []float32{
			1.0,
			10.0,
			100.0,
			1000.0,
			10000.0,
			100000.0,
			1000000.0,
			10000000.0,
			100000000.0,
			1000000000.0,
			10000000000.0, // 10^10
		},
		IF10: []float32{
			1.0,
			0.1,
			0.01,
			0.001,
			0.0001,
			0.00001,
			0.000001,
			0.0000001,
			0.00000001,
			0.000000001,
			0.0000000001, // 10^-10
		},
	}

	c64 = constant[float64]{
		WIDTH:                   8,
		RD_SIZE_THRESHOLD_LIMIT: 48 * SAMPLE_SIZE, // 48 bits per value * 32
		MAGIC_NUMBER:            0x0018000000000000,
		PATCH_SIZE:              64,
		MAX_EXPONENT:            18,

		F10: []float64{
			1.0,
			10.0,
			100.0,
			1000.0,
			10000.0,
			100000.0,
			1000000.0,
			10000000.0,
			100000000.0,
			1000000000.0,
			10000000000.0,
			100000000000.0,
			1000000000000.0,
			10000000000000.0,
			100000000000000.0,
			1000000000000000.0,
			10000000000000000.0,
			100000000000000000.0,
			1000000000000000000.0,
			10000000000000000000.0,
			100000000000000000000.0,
			1000000000000000000000.0,
			10000000000000000000000.0,
			100000000000000000000000.0, // 10^10
		},
		IF10: []float64{
			1.0,
			0.1,
			0.01,
			0.001,
			0.0001,
			0.00001,
			0.000001,
			0.0000001,
			0.00000001,
			0.000000001,
			0.0000000001,
			0.00000000001,
			0.000000000001,
			0.0000000000001,
			0.00000000000001,
			0.000000000000001,
			0.0000000000000001,
			0.00000000000000001,
			0.000000000000000001,
			0.0000000000000000001,
			0.00000000000000000001, // 10^-10
		},
	}
)

func getConstantPtr[T Float]() *constant[T] {
	switch any(T(0)).(type) {
	case float32:
		return (*constant[T])(unsafe.Pointer(&c32))
	case float64:
		return (*constant[T])(unsafe.Pointer(&c64))
	}
	return nil
}

func getConstant[T Float]() constant[T] {
	switch any(T(0)).(type) {
	case float32:
		return *(*constant[T])(unsafe.Pointer(&c32))
	case float64:
		return *(*constant[T])(unsafe.Pointer(&c64))
	}
	return constant[T]{}
}
