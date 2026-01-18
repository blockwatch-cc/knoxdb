// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package fuse

import (
	"bytes"
	"encoding/binary"
	"io"
	"slices"
	"sync"
	"unsafe"

	"github.com/FastFilter/xorfilter"
)

var LE = binary.LittleEndian

type Unsigned interface {
	~uint8 | ~uint16 | ~uint32
}

var pool = &sync.Pool{
	New: func() any { return &xorfilter.BinaryFuseBuilder{} },
}

type BinaryFuse[T Unsigned] struct {
	xorfilter.BinaryFuse[T]
}

func Build[T Unsigned](keys []uint64) (*BinaryFuse[T], error) {
	b := pool.Get()
	defer pool.Put(b)
	f, err := xorfilter.BuildBinaryFuse[T](b.(*xorfilter.BinaryFuseBuilder), keys)
	return &BinaryFuse[T]{f}, err
}

// zero-copy read
func NewFromBytes[T Unsigned](buf []byte) (*BinaryFuse[T], error) {
	if len(buf) < 28 {
		return nil, io.ErrShortBuffer
	}
	f := xorfilter.BinaryFuse[T]{
		Seed:               LE.Uint64(buf),
		SegmentLength:      LE.Uint32(buf[8:]),
		SegmentLengthMask:  LE.Uint32(buf[12:]),
		SegmentCount:       LE.Uint32(buf[16:]),
		SegmentCountLength: LE.Uint32(buf[20:]),
	}

	fplen := LE.Uint32(buf[24:])
	f.Fingerprints = unsafe.Slice((*T)(unsafe.Pointer(unsafe.SliceData(buf[28:]))), int(fplen))

	return &BinaryFuse[T]{f}, nil
}

func (f *BinaryFuse[T]) MarshalBinary() ([]byte, error) {
	size := int(unsafe.Sizeof(T(0)))
	space := len(f.Fingerprints)*size + 28
	buf := bytes.NewBuffer(make([]byte, 0, space))
	err := f.Save(buf)
	return buf.Bytes(), err
}

func (f *BinaryFuse[T]) ContainsAny(keys []uint64) bool {
	return slices.ContainsFunc(keys, f.Contains)
}
