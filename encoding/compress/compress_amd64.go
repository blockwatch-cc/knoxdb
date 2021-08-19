// Copyright (c) 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

// +build go1.7,amd64,!gccgo,!appengine

package compress

import (
	"encoding/binary"
	"fmt"
	"math/bits"

	"blockwatch.cc/knoxdb/encoding/block"
)

//go:noescape
func Delta8AVX2(src []uint64) uint64

//go:noescape
func Undelta8AVX2(src []uint64)

//go:noescape
func PackIndex32BitAVX2(src []uint64, dst []byte)

//go:noescape
func UnpackIndex32BitAVX2(src []byte, dst []uint64)

//go:noescape
func PackIndex16BitAVX2(src []uint64, dst []byte)

//go:noescape
func UnpackIndex16BitAVX2(src []byte, dst []uint64)

func CompressHash(deltas []uint64) ([]byte, int, int, error) {

	// delta encoding

	/* maxdelta := uint64(0)
	for i := len(deltas) - 1; i > 7; i-- {
		deltas[i] = deltas[i] - deltas[i-8]
		maxdelta |= deltas[i]
	}*/

	maxdelta := Delta8AVX2(deltas)
	for i := len(deltas)%8 + 7; i > 7; i-- {
		deltas[i] = deltas[i] - deltas[i-8]
		maxdelta |= deltas[i]
	}

	var nbytes int
	if maxdelta == 0 {
		nbytes = 1 // all number zero -> use 1 byte
	} else {
		lz := bits.LeadingZeros64(maxdelta)
		nbytes = (71 - lz) >> 3 // = (64 - tz + 8 - 1) / 8 = ceil((64 - tz)/8)
	}

	buf := make([]byte, nbytes*(len(deltas)-8)+64)

	for i := 0; i < 8; i++ {
		binary.BigEndian.PutUint64(buf[8*i:], deltas[i])
	}

	tmp := buf[64:]

	switch nbytes {
	case 1:
		for i, v := range deltas[8:] {
			buf[64+i] = byte(v & 0xff)
		}
	case 2:
		/*		for i, v := range deltas[8:] {
				buf[64+2*i] = byte((v >> 8) & 0xff)
				buf[65+2*i] = byte(v & 0xff)
			}*/

		len_head := (len(deltas)-8)&0x7ffffffffffffff0 + 8
		PackIndex16BitAVX2(deltas[8:], buf[64:])

		tmp = buf[64+(len_head-8)*2:]

		for i, v := range deltas[len_head:] {
			tmp[2*i] = byte((v >> 8) & 0xff)
			tmp[1+2*i] = byte(v & 0xff)
		}

	case 3:
		for i, v := range deltas[8:] {
			tmp[3*i] = byte((v >> 16) & 0xff)
			tmp[1+3*i] = byte((v >> 8) & 0xff)
			tmp[2+3*i] = byte(v & 0xff)
		}
	case 4:

		len_head := len(deltas) & 0x7ffffffffffffff8
		PackIndex32BitAVX2(deltas[8:], buf[64:])

		tmp = buf[64+(len_head-8)*4:]

		for i, v := range deltas[len_head:] {
			tmp[4*i] = byte((v >> 24) & 0xff)
			tmp[1+4*i] = byte((v >> 16) & 0xff)
			tmp[2+4*i] = byte((v >> 8) & 0xff)
			tmp[3+4*i] = byte(v & 0xff)
		}
		/*for i, v := range deltas[8:] {
			tmp[4*i] = byte((v >> 24) & 0xff)
			tmp[1+4*i] = byte((v >> 16) & 0xff)
			tmp[2+4*i] = byte((v >> 8) & 0xff)
			tmp[3+4*i] = byte(v & 0xff)
		}*/
	default:
		return nil, -1, 0, fmt.Errorf("hash size (%d bytes) not yet implemented", nbytes)
	}

	return buf, len(buf), nbytes, nil
}

type CompressedHashBlock struct {
	hash_size int
	nbytes    int
	data      []byte
}

func CompressHashBlock(b block.Block, hash_size int) (CompressedHashBlock, error) {
	deltas := make([]uint64, len(b.Uint64))
	shift := 64 - hash_size
	for i := range b.Uint64 {
		deltas[i] = b.Uint64[i] >> shift
	}

	// delta encoding

	/* maxdelta := uint64(0)
	for i := len(deltas) - 1; i > 7; i-- {
		deltas[i] = deltas[i] - deltas[i-8]
		maxdelta |= deltas[i]
	}*/

	maxdelta := Delta8AVX2(deltas)
	for i := len(deltas)%8 + 7; i > 7; i-- {
		deltas[i] = deltas[i] - deltas[i-8]
		maxdelta |= deltas[i]
	}

	var nbytes int
	if maxdelta == 0 {
		nbytes = 1 // all number zero -> use 1 byte
	} else {
		lz := bits.LeadingZeros64(maxdelta)
		nbytes = (71 - lz) >> 3 // = (64 - tz + 8 - 1) / 8 = ceil((64 - tz)/8)
	}

	buf := make([]byte, nbytes*(len(deltas)-8)+64)

	for i := 0; i < 8; i++ {
		binary.BigEndian.PutUint64(buf[8*i:], deltas[i])
	}

	tmp := buf[64:]

	switch nbytes {
	case 1:
		for i, v := range deltas[8:] {
			buf[64+i] = byte(v & 0xff)
		}
	case 2:
		/*		for i, v := range deltas[8:] {
				buf[64+2*i] = byte((v >> 8) & 0xff)
				buf[65+2*i] = byte(v & 0xff)
			}*/

		len_head := (len(deltas)-8)&0x7ffffffffffffff0 + 8
		PackIndex16BitAVX2(deltas[8:], buf[64:])

		tmp = buf[64+(len_head-8)*2:]

		for i, v := range deltas[len_head:] {
			tmp[2*i] = byte((v >> 8) & 0xff)
			tmp[1+2*i] = byte(v & 0xff)
		}

	case 3:
		for i, v := range deltas[8:] {
			tmp[3*i] = byte((v >> 16) & 0xff)
			tmp[1+3*i] = byte((v >> 8) & 0xff)
			tmp[2+3*i] = byte(v & 0xff)
		}
	case 4:

		len_head := len(deltas) & 0x7ffffffffffffff8
		PackIndex32BitAVX2(deltas[8:], buf[64:])

		tmp = buf[64+(len_head-8)*4:]

		for i, v := range deltas[len_head:] {
			tmp[4*i] = byte((v >> 24) & 0xff)
			tmp[1+4*i] = byte((v >> 16) & 0xff)
			tmp[2+4*i] = byte((v >> 8) & 0xff)
			tmp[3+4*i] = byte(v & 0xff)
		}
		/*for i, v := range deltas[8:] {
			tmp[4*i] = byte((v >> 24) & 0xff)
			tmp[1+4*i] = byte((v >> 16) & 0xff)
			tmp[2+4*i] = byte((v >> 8) & 0xff)
			tmp[3+4*i] = byte(v & 0xff)
		}*/
	default:
		return CompressedHashBlock{0, 0, nil}, fmt.Errorf("hash size (%d bytes) not yet implemented", nbytes)
	}

	return CompressedHashBlock{hash_size, nbytes, buf}, nil
}
