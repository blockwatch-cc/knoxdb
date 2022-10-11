// Copyright (c) 2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bitmap

import (
    "errors"
    "io"
    "sync"

    "github.com/dgraph-io/sroar"
)

var ErrInvalidBuffer = errors.New("invalid buffer length")

var pool = &sync.Pool{
    New: func() interface{} { return sroar.NewBitmap() },
}

type Bitmap struct {
    *sroar.Bitmap
}

func newBitmap() *Bitmap {
    return &Bitmap{sroar.NewBitmap()}
}

func New() Bitmap {
    return Bitmap{
        Bitmap: pool.New().(*sroar.Bitmap),
    }
}

func (b *Bitmap) Free() {
    b.Bitmap.Reset()
    pool.Put(b.Bitmap)
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

func (b *Bitmap) CloneFrom(a Bitmap) {
    b.Bitmap.Reset()
    pool.Put(b.Bitmap)
    b.Bitmap = a.Bitmap.Clone()
}

func (b Bitmap) MarshalBinary() ([]byte, error) {
    return b.ToBuffer(), nil
}

func (b *Bitmap) UnmarshalBinary(src []byte) error {
    if len(src) > 0 && len(src) < 8 {
        return io.ErrShortBuffer
    }
    if len(src)%2 != 0 {
        return ErrInvalidBuffer
    }
    b.Bitmap = sroar.FromBufferWithCopy(src)
    return nil
}

func Or(a, b *Bitmap) Bitmap {
    return Bitmap{sroar.Or(a.Bitmap, b.Bitmap)}
}

func FastOr(bitmaps ...*Bitmap) Bitmap {
    bm := make([]*sroar.Bitmap, len(bitmaps))
    for i, v := range bitmaps {
        bm[i] = v.Bitmap
    }
    return Bitmap{sroar.FastOr(bm...)}
}
