// Copyright (c) 2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bitmap

import (
    "encoding/base64"
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

func (b Bitmap) MarshalText() ([]byte, error) {
    src := b.ToBuffer()
    dst := make([]byte, base64.RawStdEncoding.EncodedLen(len(src)))
    base64.RawStdEncoding.Encode(dst, src)
    return dst, nil
}

func (b *Bitmap) UnmarshalText(src []byte) error {
    dst := make([]byte, 0, base64.RawStdEncoding.DecodedLen(len(src)))
    _, err := base64.RawStdEncoding.Decode(dst, src)
    if err != nil {
        return err
    }
    b.Bitmap = sroar.FromBuffer(dst)
    return nil
}

func (b *Bitmap) Or(a Bitmap) Bitmap {
    b.Bitmap.Or(a.Bitmap)
    return *b
}

func (b *Bitmap) And(a Bitmap) Bitmap {
    b.Bitmap.And(a.Bitmap)
    return *b
}

func (b *Bitmap) AndNot(a Bitmap) Bitmap {
    b.Bitmap.AndNot(a.Bitmap)
    return *b
}

func Or(a, b Bitmap) Bitmap {
    return Bitmap{sroar.Or(a.Bitmap, b.Bitmap)}
}

func FastOr(bitmaps ...Bitmap) Bitmap {
    bm := make([]*sroar.Bitmap, len(bitmaps))
    for i, v := range bitmaps {
        bm[i] = v.Bitmap
    }
    return Bitmap{sroar.FastOr(bm...)}
}

func And(a, b Bitmap) Bitmap {
    return Bitmap{sroar.And(a.Bitmap, b.Bitmap)}
}

func FastAnd(bitmaps ...Bitmap) Bitmap {
    bm := make([]*sroar.Bitmap, len(bitmaps))
    for i, v := range bitmaps {
        bm[i] = v.Bitmap
    }
    return Bitmap{sroar.FastAnd(bm...)}
}

func AndNot(a, b Bitmap) Bitmap {
    bm := a.Bitmap.Clone()
    bm.AndNot(b.Bitmap)
    return Bitmap{bm}
}
