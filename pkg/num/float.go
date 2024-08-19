// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package num

import (
	"math"
)

func Round64(num float64) int64 {
	return int64(num + math.Copysign(0.5, num))
}

func Fixed64(num float64, precision int) float64 {
	mul := powf10(precision)
	return float64(Round64(num*mul)) / mul
}

var f10 [10]float64

func init() {
	for i := 0; i < len(f10); i++ {
		f10[i] = math.Pow(10, float64(i))
	}
}

func powf10(i int) float64 {
	if i >= 0 && i < len(f10) {
		return f10[i]
	}
	return math.Pow(10, float64(i))
}
