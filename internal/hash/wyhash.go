// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package hash

import (
	"math/bits"
	"unsafe"

	"golang.org/x/sys/cpu"
)

// wyhash
const (
	m1 = 0xa0761d6478bd642f
	m2 = 0xe7037ed1a0b428db
	m3 = 0x8ebc6af09c88c6e3
	m4 = 0x589965cc75374cc3
	m5 = 0x1d8e4e27c47d124f
)

var hashkey = [4]uintptr{m1, m2, m3, m4}

// func init() {
// 	for i := range hashkey {
// 		hashkey[i] = uintptr(rand.Uint64())
// 	}
// }

// func mix64(a, b uint64) uint64 {
// 	hi, lo := bits.Mul64(a, b)
// 	return hi ^ lo
// }

func mix(a, b uintptr) uintptr {
	hi, lo := bits.Mul64(uint64(a), uint64(b))
	return uintptr(hi ^ lo)
}

func WyHash32(value uint32, seed uint64) uintptr {
	return mix(m5^4, mix(uintptr(value)^m2, uintptr(value)^uintptr(seed)^m1))
}

func WyHash64(value uint64, seed uint64) uintptr {
	return mix(m5^8, mix(uintptr(value)^m2, uintptr(value)^uintptr(seed)^m1))
}

func WyHash(buf []byte, seed uint64) uint64 {
	return uint64(wyHash(unsafe.Pointer(&buf[0]), uintptr(seed), uintptr(len(buf))))
}

func wyHash(p unsafe.Pointer, seed, s uintptr) uintptr {
	var a, b uintptr
	seed ^= hashkey[0]
	switch {
	case s == 0:
		return seed
	case s < 4:
		a = uintptr(*(*byte)(p))
		a |= uintptr(*(*byte)(unsafe.Add(p, s>>1))) << 8
		a |= uintptr(*(*byte)(unsafe.Add(p, s-1))) << 16
	case s == 4:
		a = r4(p)
		b = a
	case s < 8:
		a = r4(p)
		b = r4(unsafe.Add(p, s-4))
	case s == 8:
		a = r8(p)
		b = a
	case s <= 16:
		a = r8(p)
		b = r8(unsafe.Add(p, s-8))
	default:
		l := s
		if l > 48 {
			seed1 := seed
			seed2 := seed
			for ; l > 48; l -= 48 {
				seed = mix(r8(p)^hashkey[1], r8(unsafe.Add(p, 8))^seed)
				seed1 = mix(r8(unsafe.Add(p, 16))^hashkey[2], r8(unsafe.Add(p, 24))^seed1)
				seed2 = mix(r8(unsafe.Add(p, 32))^hashkey[3], r8(unsafe.Add(p, 40))^seed2)
				p = unsafe.Add(p, 48)
			}
			seed ^= seed1 ^ seed2
		}
		for ; l > 16; l -= 16 {
			seed = mix(r8(p)^hashkey[1], r8(unsafe.Add(p, 8))^seed)
			p = unsafe.Add(p, 16)
		}
		a = r8(unsafe.Add(p, l-16))
		b = r8(unsafe.Add(p, l-8))
	}

	return mix(m5^s, mix(a^hashkey[1], b^seed))
}

func r4(p unsafe.Pointer) uintptr {
	q := (*[4]byte)(p)
	if cpu.IsBigEndian {
		return uintptr(uint32(q[3]) | uint32(q[2])<<8 | uint32(q[1])<<16 | uint32(q[0])<<24)
	} else {
		return uintptr(uint32(q[0]) | uint32(q[1])<<8 | uint32(q[2])<<16 | uint32(q[3])<<24)
	}
}

func r8(p unsafe.Pointer) uintptr {
	q := (*[8]byte)(p)
	if cpu.IsBigEndian {
		return uintptr(uint64(q[7]) | uint64(q[6])<<8 | uint64(q[5])<<16 | uint64(q[4])<<24 |
			uint64(q[3])<<32 | uint64(q[2])<<40 | uint64(q[1])<<48 | uint64(q[0])<<56)
	} else {
		return uintptr(uint64(q[0]) | uint64(q[1])<<8 | uint64(q[2])<<16 | uint64(q[3])<<24 |
			uint64(q[4])<<32 | uint64(q[5])<<40 | uint64(q[6])<<48 | uint64(q[7])<<56)
	}
}
