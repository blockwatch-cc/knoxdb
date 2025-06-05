// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package xroar

import (
	"encoding/base64"

	"github.com/klauspost/compress/snappy"
)

func (b Bitmap) MarshalBinary() ([]byte, error) {
	src := b.Bytes()
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
	b = NewFromBytes(dst)
	return nil
}

func (b Bitmap) MarshalText() ([]byte, error) {
	src := b.Bytes()
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
	b = NewFromBytes(dst)
	return nil
}
