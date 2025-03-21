// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tests

import (
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
)

const BENCH_WIDTH = 60

func GenForScheme[T types.Integer](scheme, n int) []T {
	switch scheme {
	case 0: // TIntegerConstant,
		return GenConst[T](n)
	case 1: // TIntegerDelta,
		return GenSequence[T](n)
	case 2: // TIntegerRunEnd,
		return GenRuns[T](n, 5)
	case 3: // TIntegerBitpacked,
		return GenRandom[T](n)
	case 4: // TIntegerDictionary,
		return GenDups[T](n, 10)
	case 5: // TIntegerSimple8,
		return GenRandom[T](n)
	case 6: // TIntegerRaw,
		return GenRandom[T](n)
	default:
		return GenRandom[T](n)
	}
}

// creates n sequential values
func GenSequence[T types.Integer](n int) []T {
	res := make([]T, n)
	for i := range res {
		res[i] = T(i)
	}
	return res
}

// creates n constants
func GenConst[T types.Integer](n int) []T {
	res := make([]T, n)
	for i := range res {
		res[i] = 42
	}
	return res
}

// creates n random values
func GenRandom[T types.Integer](n int) []T {
	var res []T
	var t T
	switch any(t).(type) {
	case int64:
		res = util.ReinterpretSlice[int64, T](util.RandIntsn[int64](n, 1<<BENCH_WIDTH-1))
	case int32:
		res = util.ReinterpretSlice[int32, T](util.RandInts[int32](n))
	case int16:
		res = util.ReinterpretSlice[int16, T](util.RandInts[int16](n))
	case int8:
		res = util.ReinterpretSlice[int8, T](util.RandInts[int8](n))
	case uint64:
		res = util.ReinterpretSlice[uint64, T](util.RandUintsn[uint64](n, 1<<BENCH_WIDTH-1))
	case uint32:
		res = util.ReinterpretSlice[uint32, T](util.RandUints[uint32](n))
	case uint16:
		res = util.ReinterpretSlice[uint16, T](util.RandUints[uint16](n))
	case uint8:
		res = util.ReinterpretSlice[uint8, T](util.RandUints[uint8](n))
	}
	return res
}

// creates n values with cardinality c (i.e. u unique values)
func GenDups[T types.Integer](n, u int) []T {
	c := n / u
	res := make([]T, n)
	var t T
	switch any(t).(type) {
	case int64:
		unique := util.RandIntsn[int64](c, 1<<BENCH_WIDTH-1)
		for i := range res {
			res[i] = T(unique[util.RandIntn(c)])
		}
	case int32:
		unique := util.RandInts[int32](c)
		for i := range res {
			res[i] = T(unique[util.RandIntn(c)])
		}
	case int16:
		unique := util.RandInts[int16](c)
		for i := range res {
			res[i] = T(unique[util.RandIntn(c)])
		}
	case int8:
		unique := util.RandInts[int8](c)
		for i := range res {
			res[i] = T(unique[util.RandIntn(c)])
		}
	case uint64:
		unique := util.RandUintsn[uint64](c, 1<<BENCH_WIDTH-1)
		for i := range res {
			res[i] = T(unique[util.RandIntn(c)])
		}
	case uint32:
		unique := util.RandUints[uint32](c)
		for i := range res {
			res[i] = T(unique[util.RandIntn(c)])
		}
	case uint16:
		unique := util.RandUints[uint16](c)
		for i := range res {
			res[i] = T(unique[util.RandIntn(c)])
		}
	case uint8:
		unique := util.RandUints[uint8](c)
		for i := range res {
			res[i] = T(unique[util.RandIntn(c)])
		}
	}
	return res
}

// creates n values with run length r
func GenRuns[T types.Integer](n, r int) []T {
	res := make([]T, 0, n)
	sz := (n + r - 1) / r
	var t T
	switch any(t).(type) {
	case int64:
		for _, v := range util.RandIntsn[int64](sz, 1<<BENCH_WIDTH-1) {
			for range r {
				if len(res) == n {
					break
				}
				res = append(res, T(v))
			}
		}
	case int32:
		for _, v := range util.RandInts[int32](sz) {
			for range r {
				if len(res) == n {
					break
				}
				res = append(res, T(v))
			}
		}
	case int16:
		for _, v := range util.RandInts[int16](sz) {
			for range r {
				if len(res) == n {
					break
				}
				res = append(res, T(v))
			}
		}
	case int8:
		for _, v := range util.RandInts[int8](sz) {
			for range r {
				if len(res) == n {
					break
				}
				res = append(res, T(v))
			}
		}
	case uint64:
		for _, v := range util.RandUintsn[uint64](sz, 1<<BENCH_WIDTH-1) {
			for range r {
				if len(res) == n {
					break
				}
				res = append(res, T(v))
			}
		}
	case uint32:
		for _, v := range util.RandUints[uint32](sz) {
			for range r {
				if len(res) == n {
					break
				}
				res = append(res, T(v))
			}
		}
	case uint16:
		for _, v := range util.RandUints[uint16](sz) {
			for range r {
				if len(res) == n {
					break
				}
				res = append(res, T(v))
			}
		}
	case uint8:
		for _, v := range util.RandUints[uint8](sz) {
			for range r {
				if len(res) == n {
					break
				}
				res = append(res, T(v))
			}
		}
	}
	return res
}

func Repeat[T types.Integer](val T, n int) []T {
	result := make([]T, n)
	for i := range result {
		result[i] = val
	}
	return result
}

func Sequence[T types.Integer](start, end T) []T {
	result := make([]T, int(end-start))
	for i := range result {
		result[i] = start + T(i)
	}
	return result
}
