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

const GORANDSEED = "GORANDSEED"

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

	randSeed uint64
)

// Initialize the random seed once for all random functions
func init() {
	if seed, err := strconv.ParseUint(os.Getenv(GORANDSEED), 0, 64); err == nil {
		RandInit(seed)
	} else {
		RandInit(rand.Uint64())
	}
}

func RandInit(seed uint64) {
	randSeed = seed
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

func RandSeed() uint64 {
	return randSeed
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

const (
	letters       = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 64 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func RandString(sz int) string {
	b := make([]byte, sz)

	// A src.Uint64() generates 64 random bits, enough for letterIdxMax characters!
	for i, cache, remain := sz-1, RandUint64(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = RandUint64(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letters) {
			b[i] = letters[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}

func RandStringSlices(n, u int) []string {
	s := make([]string, n)
	for i := 0; i < n; i++ {
		s[i] = RandString(u)
	}
	return s
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

func RandIntRange[T constraints.Signed](min, max T) (T, T) {
	a, b := RandIntn(int(max-min)), RandIntn(int(max-min))
	if a > b {
		a, b = b, a
	}
	return T(a) + min, T(b) + min
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
