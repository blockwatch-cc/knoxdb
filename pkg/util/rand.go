// Copyright (c) 2024 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc

package util

import (
	"encoding/binary"
	"math/rand/v2"
	"os"
	"strconv"

	"golang.org/x/exp/constraints"
)

const randomSeedKey = "GORANDSEED"

var (
	RandInt    = rand.Int
	RandIntn   = rand.IntN
	RandInt32  = rand.Int32
	RandInt32n = rand.Int32N
	RandInt64  = rand.Int64
	RandInt64n = rand.Int64N

	RandUint    = rand.Uint
	RandUintn   = rand.UintN
	RandUint32  = rand.Uint32
	RandUint32n = rand.Uint32N
	RandUint64  = rand.Uint64
	RandUint64n = rand.Uint64N

	RandFloat32 = rand.Float32
	RandFloat64 = rand.Float64

	RandShuffle = rand.Shuffle
	RandPerm    = rand.Perm
)

// Initialize the random seed once for all random functions
func init() {
	if seed, err := strconv.ParseUint(os.Getenv(randomSeedKey), 0, 64); err == nil {
		rnd := rand.New(rand.NewPCG(seed, seed))
		RandInt = rnd.Int
		RandIntn = rnd.IntN
		RandInt32 = rnd.Int32
		RandInt32n = rnd.Int32N
		RandInt64 = rnd.Int64
		RandInt64n = rnd.Int64N
		RandUint = rnd.Uint
		RandUintn = rnd.UintN
		RandUint32 = rnd.Uint32
		RandUint32n = rnd.Uint32N
		RandUint64 = rnd.Uint64
		RandUint64n = rnd.Uint64N
		RandFloat32 = rnd.Float32
		RandFloat64 = rnd.Float64
		RandShuffle = rnd.Shuffle
		RandPerm = rnd.Perm
	}
}

func RandBytes(sz int) []byte {
	k := make([]byte, sz)
	for i := 0; i < sz/8; i++ {
		binary.BigEndian.PutUint64(k[i*8:], RandUint64())
	}
	for i := sz - sz%8; i < sz; i++ {
		k[i] = byte(RandInt())
	}
	return k
}

func RandByteSlices(n, u int) [][]byte {
	s := make([][]byte, n)
	for i := 0; i < n; i++ {
		s[i] = RandBytes(u)
	}
	return s
}

func RandString(sz int) string {
	letters := []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	str := make([]byte, sz)
	for i := range str {
		str[i] = letters[RandIntn(len(letters))]
	}
	return string(str)
}

func RandInts[T constraints.Signed](sz int) []T {
	s := make([]T, sz)
	for i := 0; i < sz; i++ {
		s[i] = T(RandInt64())
	}
	return s
}

func RandIntsn[T constraints.Signed](sz int, max T) []T {
	s := make([]T, sz)
	for i := 0; i < sz; i++ {
		s[i] = T(RandInt64n(int64(max)))
	}
	return s
}

func RandIntsRange[T constraints.Signed](sz int, min, max T) []T {
	s := RandIntsn[T](sz, max-min)
	for i := range s {
		s[i] += min
	}
	return s
}

func RandUints[T constraints.Unsigned](sz int) []T {
	s := make([]T, sz)
	for i := 0; i < sz; i++ {
		s[i] = T(RandUint64())
	}
	return s
}

func RandUintsn[T constraints.Unsigned](sz int, max T) []T {
	s := make([]T, sz)
	for i := 0; i < sz; i++ {
		s[i] = T(RandUint64n(uint64(max)))
	}
	return s
}

func RandUintsRange[T constraints.Unsigned](sz int, min, max T) []T {
	s := RandUintsn[T](sz, max-min)
	for i := range s {
		s[i] += min
	}
	return s
}

func RandFloats[T constraints.Float](sz int) []T {
	s := make([]T, sz)
	for i := 0; i < sz; i++ {
		s[i] = T(RandFloat64())
	}
	return s
}

func RandFloatsn[T constraints.Float](sz int, max T) []T {
	s := make([]T, sz)
	for i := 0; i < sz; i++ {
		s[i] = T(RandFloat64()) * max
	}
	return s
}

func RandFloatsRange[T constraints.Float](sz int, min, max T) []T {
	s := RandFloatsn[T](sz, max-min)
	for i := range s {
		s[i] += min
	}
	return s
}
