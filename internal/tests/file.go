// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tests

import (
	"os"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
)

type RawFile[T types.Number] struct {
	f *os.File
}

func OpenRawFile[T types.Number](name string) (*RawFile[T], error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	return &RawFile[T]{f: f}, nil
}

func (f *RawFile[T]) Close() error {
	err := f.f.Close()
	if err == nil {
		f.f = nil
	}
	return err
}

func (f *RawFile[T]) Size() int {
	fi, err := f.f.Stat()
	if err != nil {
		return 0
	}
	return int(fi.Size())
}

func (f *RawFile[T]) Rewind() error {
	_, err := f.f.Seek(0, 0)
	return err
}

func (f *RawFile[T]) NextN(n int, dst []T) ([]T, int) {
	if cap(dst) < n {
		dst = make([]T, n)
	}
	dst = dst[:n]

	n, err := f.f.Read(util.ToByteSlice(dst))
	if err != nil {
		n = 0
	}
	n /= util.SizeOf[T]()

	return dst[:n], n
}
