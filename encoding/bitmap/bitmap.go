// Copyright (c) 2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bitmap

import (
	"encoding/base64"
	"errors"

	// "sync"

	"blockwatch.cc/knoxdb/encoding/xroar"
	"github.com/klauspost/compress/snappy"
	"golang.org/x/exp/slices"
)

var ErrInvalidBuffer = errors.New("invalid buffer length")

// var pool = &sync.Pool{
//     New: func() interface{} { return xroar.NewBitmap() },
// }

type Bitmap struct {
	Bitmap *xroar.Bitmap
}

// func newBitmap() *Bitmap {
//     return &Bitmap{xroar.NewBitmap()}
// }

func New() Bitmap {
	return Bitmap{
		// Bitmap: pool.New().(*xroar.Bitmap),
		Bitmap: xroar.NewBitmap(),
	}
}

func NewFromBytes(src []byte) Bitmap {
	return Bitmap{
		Bitmap: xroar.FromBufferWithCopy(src),
	}
}

func NewFromArray(src []uint64) Bitmap {
	slices.Sort(src)
	return Bitmap{
		Bitmap: xroar.FromSortedList(src),
	}
}

func (b Bitmap) IsValid() bool {
	return b.Bitmap != nil
}

func (b *Bitmap) Free() {
	// b.Bitmap.Reset()
	// pool.Put(b.Bitmap)
	b.Bitmap = nil
}

func (b Bitmap) Clone() Bitmap {
	return Bitmap{
		Bitmap: b.Bitmap.Clone(),
	}
}

func (b Bitmap) Count() int {
	if b.Bitmap == nil {
		return 0
	}
	return b.Bitmap.GetCardinality()
}

func (b Bitmap) Size() int {
	if b.Bitmap == nil {
		return 0
	}
	return b.Bitmap.Size()
}

func (b Bitmap) Set(x uint64) bool {
	return b.Bitmap.Set(x)
}

func (b Bitmap) Remove(x uint64) bool {
	return b.Bitmap.Remove(x)
}

func (b Bitmap) Contains(x uint64) bool {
	return b.Bitmap.Contains(x)
}

func (b Bitmap) Bytes() []byte {
	return b.Bitmap.ToBufferWithCopy()
}

func (b *Bitmap) CloneFromBytes(src []byte) {
	// b.Bitmap.Reset()
	// pool.Put(b.Bitmap)
	// *b = NewFromBytes(src)
	b.Bitmap = xroar.FromBufferWithCopy(src)
}

func (b *Bitmap) CloneFrom(a Bitmap) {
	// b.Bitmap.Reset()
	// pool.Put(b.Bitmap)
	b.Bitmap = a.Bitmap.Clone()
}

func (b Bitmap) MarshalBinary() ([]byte, error) {
	src := b.Bitmap.ToBuffer()
	dst := make([]byte, 0, snappy.MaxEncodedLen(len(src)))
	dst = snappy.Encode(dst, src)
	return dst, nil
}

func (b *Bitmap) UnmarshalBinary(src []byte) error {
	l, err := snappy.DecodedLen(src)
	if err != nil {
		return err
	}
	dst, err := snappy.Decode(make([]byte, 0, l), src)
	if err != nil {
		return err
	}
	b.Bitmap = xroar.FromBuffer(dst)
	return nil
}

func (b Bitmap) MarshalText() ([]byte, error) {
	src := b.Bitmap.ToBuffer()
	dst := make([]byte, base64.StdEncoding.EncodedLen(len(src)))
	base64.StdEncoding.Encode(dst, src)
	return dst, nil
}

func (b *Bitmap) UnmarshalText(src []byte) error {
	dst := make([]byte, 0, base64.StdEncoding.DecodedLen(len(src)))
	_, err := base64.StdEncoding.Decode(dst, src)
	if err != nil {
		return err
	}
	b.Bitmap = xroar.FromBuffer(dst)
	return nil
}

func (b *Bitmap) Or(a Bitmap) Bitmap {
	b.Bitmap.Or(a.Bitmap)
	return *b
}

func (b *Bitmap) And(a Bitmap) Bitmap {
	b.Bitmap.And(a.Bitmap)
	b.Bitmap.Cleanup()
	return *b
}

func (b *Bitmap) AndNot(a Bitmap) Bitmap {
	b.Bitmap.AndNot(a.Bitmap)
	b.Bitmap.Cleanup()
	return *b
}

func Or(a, b Bitmap) Bitmap {
	return Bitmap{xroar.Or(a.Bitmap, b.Bitmap)}
}

func FastOr(bitmaps ...Bitmap) Bitmap {
	bm := make([]*xroar.Bitmap, len(bitmaps))
	for i, v := range bitmaps {
		bm[i] = v.Bitmap
	}
	return Bitmap{xroar.FastOr(bm...)}
}

func And(a, b Bitmap) Bitmap {
	bm := xroar.And(a.Bitmap, b.Bitmap)
	bm.Cleanup()
	return Bitmap{bm}
}

func FastAnd(bitmaps ...Bitmap) Bitmap {
	bm := make([]*xroar.Bitmap, len(bitmaps))
	for i, v := range bitmaps {
		bm[i] = v.Bitmap
	}
	return Bitmap{xroar.FastAnd(bm...)}
}

func AndNot(a, b Bitmap) Bitmap {
	bm := a.Bitmap.Clone()
	bm.AndNot(b.Bitmap)
	bm.Cleanup()
	return Bitmap{bm}
}
