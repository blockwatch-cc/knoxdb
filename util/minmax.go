// Copyright (c) 2013-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package util

import (
	"bytes"
	"math"
)

func MinString(a, b string) string {
	if a < b {
		return a
	}
	return b
}

func MaxString(a, b string) string {
	if a > b {
		return a
	}
	return b
}

func MinBytes(a, b []byte) []byte {
	if bytes.Compare(a, b) < 0 {
		return a
	}
	return b
}

func MaxBytes(a, b []byte) []byte {
	if bytes.Compare(a, b) > 0 {
		return a
	}
	return b
}

func Max(x, y int) int {
	if x < y {
		return y
	} else {
		return x
	}
}

func Min(x, y int) int {
	if x > y {
		return y
	}
	return x
}

func Abs(n int) int {
	y := n >> 63
	return (n ^ y) - y
}

func Clamp(val, min, max int) int {
	return Min(Max(val, min), max)
}

func MaxN(nums ...int) int {
	switch len(nums) {
	case 0:
		return 0
	case 1:
		return nums[0]
	default:
		n := nums[0]
		for _, v := range nums[1:] {
			if v > n {
				n = v
			}
		}
		return n
	}
}

func MinN(nums ...int) int {
	switch len(nums) {
	case 0:
		return 0
	case 1:
		return nums[0]
	default:
		n := nums[0]
		for _, v := range nums[1:] {
			if v < n {
				n = v
			}
		}
		return n
	}
}

func NonZero(x ...int) int {
	for _, v := range x {
		if v != 0 {
			return v
		}
	}
	return 0
}

func NonZeroMin(x ...int) int {
	var min int
	for _, v := range x {
		if v != 0 {
			if min == 0 {
				min = v
			} else {
				min = Min(min, v)
			}
		}
	}
	return min
}

func Max64(x, y int64) int64 {
	if x < y {
		return y
	}
	return x
}

func Min64(x, y int64) int64 {
	if x > y {
		return y
	}
	return x
}

func Abs64(n int64) int64 {
	y := n >> 63
	return (n ^ y) - y
}

func ClampInt64(val, min, max int64) int64 {
	return Min64(Max64(val, min), max)
}

func Max64n(nums ...int64) int64 {
	switch len(nums) {
	case 0:
		return 0
	case 1:
		return nums[0]
	default:
		n := nums[0]
		for _, v := range nums[1:] {
			if v > n {
				n = v
			}
		}
		return n
	}
}

func Min64n(nums ...int64) int64 {
	switch len(nums) {
	case 0:
		return 0
	case 1:
		return nums[0]
	default:
		n := nums[0]
		for _, v := range nums[1:] {
			if v < n {
				n = v
			}
		}
		return n
	}
}

func NonZero64(x ...int64) int64 {
	for _, v := range x {
		if v != 0 {
			return v
		}
	}
	return 0
}

func NonZeroMin64(x ...int64) int64 {
	var min int64
	for _, v := range x {
		if v != 0 {
			if min == 0 {
				min = v
			} else {
				min = Min64(min, v)
			}
		}
	}
	return min
}

func MaxU64(x, y uint64) uint64 {
	if x < y {
		return y
	}
	return x
}

func MinU64(x, y uint64) uint64 {
	if x > y {
		return y
	}
	return x
}

func ClampUint64(val, min, max uint64) uint64 {
	return MinU64(MaxU64(val, min), max)
}

func MaxU64n(nums ...uint64) uint64 {
	switch len(nums) {
	case 0:
		return 0
	case 1:
		return nums[0]
	default:
		n := nums[0]
		for _, v := range nums[1:] {
			if v > n {
				n = v
			}
		}
		return n
	}
}

func MinU64n(nums ...uint64) uint64 {
	switch len(nums) {
	case 0:
		return 0
	case 1:
		return nums[0]
	default:
		n := nums[0]
		for _, v := range nums[1:] {
			if v < n {
				n = v
			}
		}
		return n
	}
}

func NonZeroU64(x ...uint64) uint64 {
	for _, v := range x {
		if v != 0 {
			return v
		}
	}
	return 0
}

func NonZeroMinU64(x ...uint64) uint64 {
	var min uint64
	for _, v := range x {
		if v != 0 {
			if min == 0 {
				min = v
			} else {
				min = MinU64(min, v)
			}
		}
	}
	return min
}

func MinFloat64(x, y float64) float64 {
	return math.Min(x, y)
}

func MaxFloat64(x, y float64) float64 {
	return math.Max(x, y)
}

func ClampFloat64(val, min, max float64) float64 {
	return math.Min(math.Max(val, min), max)
}

func MinFloat64n(nums ...float64) float64 {
	switch len(nums) {
	case 0:
		return 0
	case 1:
		return nums[0]
	default:
		n := nums[0]
		for _, v := range nums[1:] {
			if v < n {
				n = v
			}
		}
		return n
	}
}

func MaxFloat64n(nums ...float64) float64 {
	switch len(nums) {
	case 0:
		return 0
	case 1:
		return nums[0]
	default:
		n := nums[0]
		for _, v := range nums[1:] {
			if v > n {
				n = v
			}
		}
		return n
	}
}

func NonZeroFloat64(x ...float64) float64 {
	for _, v := range x {
		if v != 0 {
			return v
		}
	}
	return 0
}

func NonZeroMinFloat64(x ...float64) float64 {
	var min float64
	for _, v := range x {
		if v != 0 {
			if min == 0 {
				min = v
			} else {
				min = MinFloat64(min, v)
			}
		}
	}
	return min
}

func MinSlice(x []int) int {
	var m int
	for _, v := range x {
		m = Min(m, v)
	}
	return m
}

func MaxSlice(x []int) int {
	var m int
	for _, v := range x {
		m = Max(m, v)
	}
	return m
}

func MinMaxSlice(x []int) (int, int) {
	var min, max int
	for _, v := range x {
		max = Max(max, v)
		min = Min(min, v)
	}
	return min, max
}

func MinSlice64(x []int64) int64 {
	var m int64
	for _, v := range x {
		m = Min64(m, v)
	}
	return m
}

func MaxSlice64(x []int64) int64 {
	var m int64
	for _, v := range x {
		m = Max64(m, v)
	}
	return m
}

func MinMaxSlice64(x []int64) (int64, int64) {
	var min, max int64
	for _, v := range x {
		max = Max64(max, v)
		min = Min64(min, v)
	}
	return min, max
}

func MinSliceFloat32(x []float32) float32 {
	var m float32
	for _, v := range x {
		m = float32(math.Min(float64(m), float64(v)))
	}
	return m
}

func MaxSliceFloat32(x []float32) float32 {
	var m float32
	for _, v := range x {
		m = float32(math.Max(float64(m), float64(v)))
	}
	return m
}

func MinMaxSliceFloat32(x []float32) (float32, float32) {
	var min, max float32
	for _, v := range x {
		max = float32(math.Max(float64(max), float64(v)))
		min = float32(math.Min(float64(min), float64(v)))
	}
	return min, max
}

func MinSliceFloat64(x []float64) float64 {
	var m float64
	for _, v := range x {
		m = math.Min(m, v)
	}
	return m
}

func MaxSliceFloat64(x []float64) float64 {
	var m float64
	for _, v := range x {
		m = math.Max(m, v)
	}
	return m
}

func MinMaxSliceFloat64(x []float64) (float64, float64) {
	var min, max float64
	for _, v := range x {
		max = math.Max(max, v)
		min = math.Min(min, v)
	}
	return min, max
}

func MaxU32(x, y uint32) uint32 {
	if x < y {
		return y
	}
	return x
}

func MinU32(x, y uint32) uint32 {
	if x > y {
		return y
	}
	return x
}

func NonZeroU32(x ...uint32) uint32 {
	for _, v := range x {
		if v != 0 {
			return v
		}
	}
	return 0
}

func NonZeroMinU32(x ...uint32) uint32 {
	var min uint32
	for _, v := range x {
		if v != 0 {
			if min == 0 {
				min = v
			} else {
				min = MinU32(min, v)
			}
		}
	}
	return min
}

func MaxU32n(nums ...uint32) uint32 {
	switch len(nums) {
	case 0:
		return 0
	case 1:
		return nums[0]
	default:
		n := nums[0]
		for _, v := range nums[1:] {
			if v > n {
				n = v
			}
		}
		return n
	}
}

func MinU32n(nums ...uint32) uint32 {
	switch len(nums) {
	case 0:
		return 0
	case 1:
		return nums[0]
	default:
		n := nums[0]
		for _, v := range nums[1:] {
			if v < n {
				n = v
			}
		}
		return n
	}
}

func Max32(x, y int32) int32 {
	if x < y {
		return y
	}
	return x
}

func Min32(x, y int32) int32 {
	if x > y {
		return y
	}
	return x
}

func NonZero32(x ...int32) int32 {
	for _, v := range x {
		if v != 0 {
			return v
		}
	}
	return 0
}

func NonZeroMin32(x ...int32) int32 {
	var min int32
	for _, v := range x {
		if v != 0 {
			if min == 0 {
				min = v
			} else {
				min = Min32(min, v)
			}
		}
	}
	return min
}

func Max32n(nums ...int32) int32 {
	switch len(nums) {
	case 0:
		return 0
	case 1:
		return nums[0]
	default:
		n := nums[0]
		for _, v := range nums[1:] {
			if v > n {
				n = v
			}
		}
		return n
	}
}

func Min32n(nums ...int32) int32 {
	switch len(nums) {
	case 0:
		return 0
	case 1:
		return nums[0]
	default:
		n := nums[0]
		for _, v := range nums[1:] {
			if v < n {
				n = v
			}
		}
		return n
	}
}

func MaxU16(x, y uint16) uint16 {
	if x < y {
		return y
	}
	return x
}

func MinU16(x, y uint16) uint16 {
	if x > y {
		return y
	}
	return x
}

func NonZeroU16(x ...uint16) uint16 {
	for _, v := range x {
		if v != 0 {
			return v
		}
	}
	return 0
}

func NonZeroMinU16(x ...uint16) uint16 {
	var min uint16
	for _, v := range x {
		if v != 0 {
			if min == 0 {
				min = v
			} else {
				min = MinU16(min, v)
			}
		}
	}
	return min
}

func MaxU16n(nums ...uint16) uint16 {
	switch len(nums) {
	case 0:
		return 0
	case 1:
		return nums[0]
	default:
		n := nums[0]
		for _, v := range nums[1:] {
			if v > n {
				n = v
			}
		}
		return n
	}
}

func MinU16n(nums ...uint16) uint16 {
	switch len(nums) {
	case 0:
		return 0
	case 1:
		return nums[0]
	default:
		n := nums[0]
		for _, v := range nums[1:] {
			if v < n {
				n = v
			}
		}
		return n
	}
}

func Max16(x, y int16) int16 {
	if x < y {
		return y
	}
	return x
}

func Min16(x, y int16) int16 {
	if x > y {
		return y
	}
	return x
}

func NonZero16(x ...int16) int16 {
	for _, v := range x {
		if v != 0 {
			return v
		}
	}
	return 0
}

func NonZeroMin16(x ...int16) int16 {
	var min int16
	for _, v := range x {
		if v != 0 {
			if min == 0 {
				min = v
			} else {
				min = Min16(min, v)
			}
		}
	}
	return min
}

func Max16n(nums ...int16) int16 {
	switch len(nums) {
	case 0:
		return 0
	case 1:
		return nums[0]
	default:
		n := nums[0]
		for _, v := range nums[1:] {
			if v > n {
				n = v
			}
		}
		return n
	}
}

func Min16n(nums ...int16) int16 {
	switch len(nums) {
	case 0:
		return 0
	case 1:
		return nums[0]
	default:
		n := nums[0]
		for _, v := range nums[1:] {
			if v < n {
				n = v
			}
		}
		return n
	}
}

func MaxU8(x, y uint8) uint8 {
	if x < y {
		return y
	}
	return x
}

func MinU8(x, y uint8) uint8 {
	if x > y {
		return y
	}
	return x
}

func NonZeroU8(x ...uint8) uint8 {
	for _, v := range x {
		if v != 0 {
			return v
		}
	}
	return 0
}

func NonZeroMinU8(x ...uint8) uint8 {
	var min uint8
	for _, v := range x {
		if v != 0 {
			if min == 0 {
				min = v
			} else {
				min = MinU8(min, v)
			}
		}
	}
	return min
}

func MaxU8n(nums ...uint8) uint8 {
	switch len(nums) {
	case 0:
		return 0
	case 1:
		return nums[0]
	default:
		n := nums[0]
		for _, v := range nums[1:] {
			if v > n {
				n = v
			}
		}
		return n
	}
}

func MinU8n(nums ...uint8) uint8 {
	switch len(nums) {
	case 0:
		return 0
	case 1:
		return nums[0]
	default:
		n := nums[0]
		for _, v := range nums[1:] {
			if v < n {
				n = v
			}
		}
		return n
	}
}

func Max8(x, y int8) int8 {
	if x < y {
		return y
	}
	return x
}

func Min8(x, y int8) int8 {
	if x > y {
		return y
	}
	return x
}

func NonZero8(x ...int8) int8 {
	for _, v := range x {
		if v != 0 {
			return v
		}
	}
	return 0
}

func NonZeroMin8(x ...int8) int8 {
	var min int8
	for _, v := range x {
		if v != 0 {
			if min == 0 {
				min = v
			} else {
				min = Min8(min, v)
			}
		}
	}
	return min
}

func Max8n(nums ...int8) int8 {
	switch len(nums) {
	case 0:
		return 0
	case 1:
		return nums[0]
	default:
		n := nums[0]
		for _, v := range nums[1:] {
			if v > n {
				n = v
			}
		}
		return n
	}
}

func Min8n(nums ...int8) int8 {
	switch len(nums) {
	case 0:
		return 0
	case 1:
		return nums[0]
	default:
		n := nums[0]
		for _, v := range nums[1:] {
			if v < n {
				n = v
			}
		}
		return n
	}
}
