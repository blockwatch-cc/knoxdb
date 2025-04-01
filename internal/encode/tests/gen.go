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
		return GenConst[T](n, 42)
	case 1: // TIntegerDelta,
		return GenSeq[T](n)
	case 2: // TIntegerRunEnd,
		return GenRuns[T](n, 5)
	case 3: // TIntegerBitpacked,
		return GenRnd[T](n)
	case 4: // TIntegerDictionary,
		return GenDups[T](n, 10)
	case 5: // TIntegerSimple8,
		return GenRnd[T](n)
	case 6: // TIntegerRaw,
		return GenRnd[T](n)
	default:
		return GenRnd[T](n)
	}
}

func GenForSchemeFloat[T types.Float](scheme, n int) []T {
	switch scheme {
	case 0: // TFloatConstant,
		return GenConst[T](n, 4.225)
	case 1: // TFloatRunEnd,
		return GenRuns[T](n, 5)
	case 2: // TFloatDictionary,
		return GenDups[T](n, 10)
	case 3: // TFloatAlp,
		return GenRnd[T](n)
	case 4: // TFloatAlpRd,
		return GenRnd[T](n)
	case 5: // TFloatRaw,
		return GenRnd[T](n)
	default:
		return GenRnd[T](n)
	}
}

// creates n sequential values
func GenSeq[T types.Number](n int) []T {
	res := make([]T, n)
	switch any(T(0)).(type) {
	case int64, int32, int16, int8, int, uint, uint64, uint32, uint16, uint8:
		for i := range res {
			res[i] = T(i)
		}
	case float64, float32:
		for i := range res {
			v := float64(i) + float64(0.5)
			res[i] = T(v)
		}
	}
	return res
}

func GenRange[T types.Integer](start, end T) []T {
	result := make([]T, int(end-start))
	for i := range result {
		result[i] = start + T(i)
	}
	return result
}

// creates n constants of value v
func GenConst[T types.Number](n int, v T) []T {
	res := make([]T, n)
	for i := range res {
		res[i] = v
	}
	return res
}

// creates n random values
func GenRnd[T types.Number](n int) []T {
	var res []T
	switch any(T(0)).(type) {
	case int64:
		res = util.ReinterpretSlice[int64, T](util.RandIntsn[int64](n, 1<<BENCH_WIDTH-1))
	case int32:
		res = util.ReinterpretSlice[int32, T](util.RandIntsn[int32](n, 1<<(BENCH_WIDTH/2-1)))
	case int16:
		res = util.ReinterpretSlice[int16, T](util.RandInts[int16](n))
	case int8:
		res = util.ReinterpretSlice[int8, T](util.RandInts[int8](n))
	case uint64:
		res = util.ReinterpretSlice[uint64, T](util.RandUintsn[uint64](n, 1<<BENCH_WIDTH-1))
	case uint32:
		res = util.ReinterpretSlice[uint32, T](util.RandUintsn[uint32](n, 1<<(BENCH_WIDTH/2-1)))
	case uint16:
		res = util.ReinterpretSlice[uint16, T](util.RandUints[uint16](n))
	case uint8:
		res = util.ReinterpretSlice[uint8, T](util.RandUints[uint8](n))
	case float64:
		v := util.RandFloatsn[float64](n, 1<<BENCH_WIDTH-1)
		for i := range v {
			res = append(res, T(v[i]))
		}
	case float32:
		v := util.RandFloatsn[float32](n, 1<<BENCH_WIDTH-1)
		for i := range v {
			res = append(res, T(v[i]))
		}
	}
	return res
}

// creates n random values with bit width of up to w
func GenRndBits[T types.Integer](n, w int) []T {
	var res []T
	switch any(T(0)).(type) {
	case int64:
		res = util.ReinterpretSlice[int64, T](util.RandIntsn[int64](n, 1<<w-1))
	case int32:
		res = util.ReinterpretSlice[int32, T](util.RandIntsn[int32](n, 1<<w-1))
	case int16:
		res = util.ReinterpretSlice[int16, T](util.RandIntsn[int16](n, 1<<w-1))
	case int8:
		res = util.ReinterpretSlice[int8, T](util.RandIntsn[int8](n, 1<<w-1))
	case uint64:
		res = util.ReinterpretSlice[uint64, T](util.RandUintsn[uint64](n, 1<<w-1))
	case uint32:
		res = util.ReinterpretSlice[uint32, T](util.RandUintsn[uint32](n, 1<<w-1))
	case uint16:
		res = util.ReinterpretSlice[uint16, T](util.RandUintsn[uint16](n, 1<<w-1))
	case uint8:
		res = util.ReinterpretSlice[uint8, T](util.RandUintsn[uint8](n, 1<<w-1))
	}
	return res
}

// creates n values with cardinality c (i.e. u unique values)
func GenDups[T types.Number](n, u int) []T {
	c := n / u
	res := make([]T, n)
	switch any(T(0)).(type) {
	case int64:
		unique := util.RandIntsn[int64](c, 1<<BENCH_WIDTH-1)
		for i := range res {
			res[i] = T(unique[util.RandIntn(c)])
		}
	case int32:
		unique := util.RandIntsn[int32](c, 1<<(BENCH_WIDTH/2-1))
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
		unique := util.RandUintsn[uint32](c, 1<<(BENCH_WIDTH/2-1))
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
	case float64:
		unique := util.RandFloatsn[float64](c, 1<<BENCH_WIDTH-1)
		for i := range res {
			res[i] = T(unique[util.RandIntn(c)])
		}
	case float32:
		unique := util.RandFloatsn[float32](c, 1<<BENCH_WIDTH-1)
		for i := range res {
			res[i] = T(unique[util.RandIntn(c)])
		}
	}
	return res
}

// creates n values with run length r
func GenRuns[T types.Number](n, r int) []T {
	res := make([]T, 0, n)
	sz := (n + r - 1) / r
	switch any(T(0)).(type) {
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
		for _, v := range util.RandIntsn[int32](sz, 1<<(BENCH_WIDTH/2-1)) {
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
		for _, v := range util.RandUintsn[uint32](sz, 1<<(BENCH_WIDTH/2-1)) {
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
	case float64:
		for _, v := range util.RandFloatsn[float64](sz, 1<<BENCH_WIDTH-1) {
			for range r {
				if len(res) == n {
					break
				}
				res = append(res, T(v))
			}
		}
	case float32:
		for _, v := range util.RandFloatsn[float32](sz, 1<<BENCH_WIDTH-1) {
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

// creates n values with u% values equal to x
func GenEqual[T types.Integer](n, u int) ([]T, T) {
	res := make([]T, n)
	var x T
	switch any(T(0)).(type) {
	case int64:
		x = T(util.RandInt64n(1<<BENCH_WIDTH - 1))
		for i := range res {
			if util.RandIntn(100) <= u {
				res[i] = x
			} else {
				res[i] = T(util.RandInt64n(1<<BENCH_WIDTH - 1))
			}
		}
	case int32:
		x = T(util.RandInt32n(1<<(BENCH_WIDTH/2) - 1))
		for i := range res {
			if util.RandIntn(100) <= u {
				res[i] = x
			} else {
				res[i] = T(util.RandInt32n(1<<(BENCH_WIDTH/2) - 1))
			}
		}
	case int16:
		x = T(util.RandInt64n(1<<16 - 1))
		for i := range res {
			if util.RandIntn(100) <= u {
				res[i] = x
			} else {
				res[i] = T(util.RandInt64n(1<<16 - 1))
			}
		}
	case int8:
		x = T(util.RandInt64n(1<<8 - 1))
		for i := range res {
			if util.RandIntn(100) <= u {
				res[i] = x
			} else {
				res[i] = T(util.RandInt64n(1<<8 - 1))
			}
		}
	case uint64:
		x = T(util.RandUint64n(1<<BENCH_WIDTH - 1))
		for i := range res {
			if util.RandIntn(100) <= u {
				res[i] = x
			} else {
				res[i] = T(util.RandUint64n(1<<BENCH_WIDTH - 1))
			}
		}
	case uint32:
		x = T(util.RandUint32n(1<<(BENCH_WIDTH/2) - 1))
		for i := range res {
			if util.RandIntn(100) <= u {
				res[i] = x
			} else {
				res[i] = T(util.RandUint32n(1<<(BENCH_WIDTH/2) - 1))
			}
		}
	case uint16:
		x = T(util.RandUint64n(1<<16 - 1))
		for i := range res {
			if util.RandIntn(100) <= u {
				res[i] = x
			} else {
				res[i] = T(util.RandUint64n(1<<16 - 1))
			}
		}
	case uint8:
		x = T(util.RandUint64n(1<<8 - 1))
		for i := range res {
			if util.RandIntn(100) <= u {
				res[i] = x
			} else {
				res[i] = T(util.RandUint64n(1<<8 - 1))
			}
		}
	}
	return res, x
}
