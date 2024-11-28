// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//
// Test-usage only

package tests

import (
	"bytes"
)

type BitsetRunTestcase struct {
	// source data
	Name string
	Buf  []byte
	Size int
	// results for run algos
	Runs  [][2]int
	Rruns [][2]int // reverse
	// results for index algos
	Idx []uint32
}

func fillIndex(start, length int) []uint32 {
	result := make([]uint32, length)
	for i := range result {
		result[i] = uint32(start + i)
	}
	return result
}

var RunTestcases = []BitsetRunTestcase{
	{
		Name: "first_7",
		Buf:  []byte{0xff},
		Size: 7,
		Runs: [][2]int{
			{0, 7},
		},
		Rruns: [][2]int{
			{6, 7},
		},
		Idx: fillIndex(0, 7),
	},
	{
		Name: "first_9",
		Buf:  []byte{0xff, 0xff},
		Size: 9,
		Runs: [][2]int{
			{0, 9},
		},
		Rruns: [][2]int{
			{8, 9},
		},
		Idx: fillIndex(0, 9),
	},
	{
		Name: "first_15",
		Buf:  []byte{0xff, 0xff},
		Size: 15,
		Runs: [][2]int{
			{0, 15},
		},
		Rruns: [][2]int{
			{14, 15},
		},
		Idx: fillIndex(0, 15),
	},
	{
		Name: "first_17",
		Buf:  []byte{0xff, 0xff, 0xff},
		Size: 17,
		Runs: [][2]int{
			{0, 17},
		},
		Rruns: [][2]int{
			{16, 17},
		},
		Idx: fillIndex(0, 17),
	},
	{
		Name: "first_7_srl_1",
		Buf:  []byte{0xfe},
		Size: 7,
		Runs: [][2]int{
			{1, 6},
		},
		Rruns: [][2]int{
			{6, 6},
		},
		Idx: fillIndex(1, 6),
	},
	{
		Name: "first_15_srl_1",
		Buf:  []byte{0xfe, 0xff},
		Size: 15,
		Runs: [][2]int{
			{1, 14},
		},
		Rruns: [][2]int{
			{14, 14},
		},
		Idx: fillIndex(1, 14),
	},
	{
		Name: "first_ff_srl_4",
		Buf:  []byte{0xf0, 0x0f},
		Size: 16,
		Runs: [][2]int{
			{4, 8},
		},
		Rruns: [][2]int{
			{11, 8},
		},
		Idx: fillIndex(4, 8),
	},
	{
		Name: "first_33_srl_3",
		Buf:  []byte{0xf8, 0xff, 0xff, 0xff, 0x01},
		Size: 33,
		Runs: [][2]int{
			{3, 30},
		},
		Rruns: [][2]int{
			{32, 30},
		},
		Idx: fillIndex(3, 30),
	},
	{
		Name: "second_15",
		Buf:  []byte{0x0, 0xff},
		Size: 15,
		Runs: [][2]int{
			{8, 7},
		},
		Rruns: [][2]int{
			{14, 7},
		},
		Idx: fillIndex(8, 7),
	},
	{
		Name: "second_33_srl_3",
		Buf:  []byte{0x0, 0xf8, 0xff, 0xff, 0x01},
		Size: 33,
		Runs: [][2]int{
			{11, 22},
		},
		Rruns: [][2]int{
			{32, 22},
		},
		Idx: fillIndex(11, 22),
	},
	{
		Name: "two_fe_33",
		Buf:  []byte{0x7f, 0x00, 0x7f, 0x00, 0x00},
		Size: 33,
		Runs: [][2]int{
			{0, 7},
			{16, 7},
		},
		Rruns: [][2]int{
			{22, 7},
			{6, 7},
		},
		Idx: append(fillIndex(0, 7), fillIndex(16, 7)...),
	},
	{
		Name: "four_0e_31",
		Buf:  []byte{0x70, 0x70, 0x70, 0x70},
		Size: 31,
		Runs: [][2]int{
			{4, 3},
			{12, 3},
			{20, 3},
			{28, 3},
		},
		Rruns: [][2]int{
			{30, 3},
			{22, 3},
			{14, 3},
			{6, 3},
		},
		Idx: []uint32{4, 5, 6, 12, 13, 14, 20, 21, 22, 28, 29, 30},
	},
	{
		Name: "every_aa_15",
		Buf:  []byte{0x55, 0x55},
		Size: 15,
		Runs: [][2]int{
			{0, 1},
			{2, 1},
			{4, 1},
			{6, 1},
			{8, 1},
			{10, 1},
			{12, 1},
			{14, 1},
		},
		Rruns: [][2]int{
			{14, 1},
			{12, 1},
			{10, 1},
			{8, 1},
			{6, 1},
			{4, 1},
			{2, 1},
			{0, 1},
		},
		Idx: []uint32{0, 2, 4, 6, 8, 10, 12, 14},
	},
	{
		Name: "every_cc_15",
		Buf:  []byte{0x33, 0x33},
		Size: 15,
		Runs: [][2]int{
			{0, 2},
			{4, 2},
			{8, 2},
			{12, 2},
		},
		Rruns: [][2]int{
			{13, 2},
			{9, 2},
			{5, 2},
			{1, 2},
		},
		Idx: []uint32{0, 1, 4, 5, 8, 9, 12, 13},
	},
	{
		Name: "every_55_15",
		Buf:  []byte{0xaa, 0xaa},
		Size: 15,
		Runs: [][2]int{
			{1, 1},
			{3, 1},
			{5, 1},
			{7, 1},
			{9, 1},
			{11, 1},
			{13, 1},
		},
		Rruns: [][2]int{
			{13, 1},
			{11, 1},
			{9, 1},
			{7, 1},
			{5, 1},
			{3, 1},
			{1, 1},
		},
		Idx: []uint32{1, 3, 5, 7, 9, 11, 13},
	},
	{
		Name: "every_88_17",
		Buf:  []byte{0x11, 0x11, 0x11},
		Size: 17,
		Runs: [][2]int{
			{0, 1},
			{4, 1},
			{8, 1},
			{12, 1},
			{16, 1},
		},
		Rruns: [][2]int{
			{16, 1},
			{12, 1},
			{8, 1},
			{4, 1},
			{0, 1},
		},
		Idx: []uint32{0, 4, 8, 12, 16},
	},
	{
		Name: "last_0e_32",
		Buf:  []byte{0x0, 0x0, 0x0, 0x70},
		Size: 32,
		Runs: [][2]int{
			{28, 3},
		},
		Rruns: [][2]int{
			{30, 3},
		},
		Idx: []uint32{28, 29, 30},
	},
	{
		Name: "last_16",
		Buf:  []byte{0x0, 0x80},
		Size: 16,
		Runs: [][2]int{
			{15, 1},
		},
		Rruns: [][2]int{
			{15, 1},
		},
		Idx: []uint32{15},
	},
	{
		Name: "last_256",
		Buf:  append(FillBitset(nil, 256-8, 0), byte(0x80)),
		Size: 256,
		Runs: [][2]int{
			{255, 1},
		},
		Rruns: [][2]int{
			{255, 1},
		},
		Idx: []uint32{255},
	},
	{
		Name: "last_16k",
		Buf:  append(FillBitset(nil, 16*1024-8, 0), byte(0x80)),
		Size: 16 * 1024,
		Runs: [][2]int{
			{16*1024 - 1, 1},
		},
		Rruns: [][2]int{
			{16*1024 - 1, 1},
		},
		Idx: []uint32{16*1024 - 1},
	},
	{
		Name: "empty",
		Buf:  []byte{},
		Size: 0,
		Runs: [][2]int{
			{-1, 0},
		},
		Rruns: [][2]int{
			{-1, 0},
		},
		Idx: []uint32{},
	},
	{
		Name: "nil",
		Buf:  nil,
		Size: 0,
		Runs: [][2]int{
			{-1, 0},
		},
		Rruns: [][2]int{
			{-1, 0},
		},
		Idx: []uint32{},
	},
	{
		Name: "zeros_8",
		Buf:  FillBitset(nil, 8, 0),
		Size: 8,
		Runs: [][2]int{
			{-1, 0},
		},
		Rruns: [][2]int{
			{-1, 0},
		},
		Idx: []uint32{},
	},
	{
		Name: "zeros_32",
		Buf:  FillBitset(nil, 32, 0),
		Size: 32,
		Runs: [][2]int{
			{-1, 0},
		},
		Rruns: [][2]int{
			{-1, 0},
		},
		Idx: []uint32{},
	},
	{
		Name: "ones_32",
		Buf:  FillBitset(nil, 32, 0xff),
		Size: 32,
		Runs: [][2]int{
			{0, 32},
		},
		Rruns: [][2]int{
			{31, 32},
		},
		Idx: fillIndex(0, 32),
	},
	{
		Name: "ones_64",
		Buf:  FillBitset(nil, 64, 0xff),
		Size: 64,
		Runs: [][2]int{
			{0, 64},
		},
		Rruns: [][2]int{
			{63, 64},
		},
		Idx: fillIndex(0, 64),
	},
	{
		Name: "ones_32_zeros_32",
		Buf:  []byte{0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff},
		Size: 96,
		Runs: [][2]int{
			{0, 32},
			{64, 32},
		},
		Rruns: [][2]int{
			{95, 32},
			{31, 32},
		},
		Idx: append(fillIndex(0, 32), fillIndex(64, 32)...),
	},
	{
		Name: "ones_64_zeros_64",
		Buf: []byte{
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
			0, 0, 0, 0, 0, 0, 0, 0,
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
		Size: 192,
		Runs: [][2]int{
			{0, 64},
			{128, 64},
		},
		Rruns: [][2]int{
			{191, 64},
			{63, 64},
		},
		Idx: append(fillIndex(0, 64), fillIndex(128, 64)...),
	},
	{
		Name: "128_and_cd",
		Buf:  append(bytes.Repeat([]byte{0x0}, 15), byte(0xcd)),
		Size: 128,
		Runs: [][2]int{
			{128 - 8, 1},
			{128 - 6, 2},
			{128 - 2, 2},
		},
		Rruns: [][2]int{
			{128 - 1, 2},
			{128 - 5, 2},
			{128 - 8, 1},
		},
		Idx: []uint32{128 - 8, 128 - 6, 128 - 5, 128 - 2, 128 - 1},
	},
	{
		Name: "136_and_cd",
		Buf:  append(bytes.Repeat([]byte{0x0}, 16), byte(0xcd)),
		Size: 136,
		Runs: [][2]int{
			{136 - 8, 1},
			{136 - 6, 2},
			{136 - 2, 2},
		},
		Rruns: [][2]int{
			{136 - 1, 2},
			{136 - 5, 2},
			{136 - 8, 1},
		},
		Idx: []uint32{136 - 8, 136 - 6, 136 - 5, 136 - 2, 136 - 1},
	},
	{
		Name: "2048_and_cd",
		Buf:  append(bytes.Repeat([]byte{0x0}, 255), byte(0xcd)),
		Size: 2048,
		Runs: [][2]int{
			{2048 - 8, 1},
			{2048 - 6, 2},
			{2048 - 2, 2},
		},
		Rruns: [][2]int{
			{2048 - 1, 2},
			{2048 - 5, 2},
			{2048 - 8, 1},
		},
		Idx: []uint32{2048 - 8, 2048 - 6, 2048 - 5, 2048 - 2, 2048 - 1},
	},
	{
		Name: "2056_and_cd",
		Buf:  append(bytes.Repeat([]byte{0x0}, 256), byte(0xcd)),
		Size: 2056,
		Runs: [][2]int{
			{2056 - 8, 1},
			{2056 - 6, 2},
			{2056 - 2, 2},
		},
		Rruns: [][2]int{
			{2056 - 1, 2},
			{2056 - 5, 2},
			{2056 - 8, 1},
		},
		Idx: []uint32{2056 - 8, 2056 - 6, 2056 - 5, 2056 - 2, 2056 - 1},
	},
	{
		Name: "64k_and_cd",
		Buf:  append(bytes.Repeat([]byte{0x0}, 8*1024-1), byte(0xcd)),
		Size: 64 * 1024,
		Runs: [][2]int{
			{64*1024 - 8, 1},
			{64*1024 - 6, 2},
			{64*1024 - 2, 2},
		},
		Rruns: [][2]int{
			{64*1024 - 1, 2},
			{64*1024 - 5, 2},
			{64*1024 - 8, 1},
		},
		Idx: []uint32{64*1024 - 8, 64*1024 - 6, 64*1024 - 5, 64*1024 - 2, 64*1024 - 1},
	},
	{
		Name: "64k_and_8d",
		Buf:  append(bytes.Repeat([]byte{0x0}, 8*1024-1), byte(0x8d)),
		Size: 64 * 1024,
		Runs: [][2]int{
			{64*1024 - 8, 1},
			{64*1024 - 6, 2},
			{64*1024 - 1, 1},
		},
		Rruns: [][2]int{
			{64*1024 - 1, 1},
			{64*1024 - 5, 2},
			{64*1024 - 8, 1},
		},
		Idx: []uint32{64*1024 - 8, 64*1024 - 6, 64*1024 - 5, 64*1024 - 1},
	},
}
