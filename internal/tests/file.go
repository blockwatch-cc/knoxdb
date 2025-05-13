// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tests

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
)

type Test[T types.Number] struct {
	Name string
	Data []T
	N    int
	F    *File[T]
}

func (t *Test[T]) Next() ([]T, bool) {
	if t.F == nil {
		return nil, false
	}
	dst, n := t.F.NextN(t.N, t.Data)
	if n == 0 {
		return nil, false
	}
	t.Data = dst[:n]
	return t.Data, true
}

// ----------------------------------------
// File based benchmarks and tests
// ----------------------------------------

var GO_DATA_PATH = os.Getenv("GO_DATA_PATH")

func EnsureDataFiles(b testing.TB) {
	if GO_DATA_PATH == "" {
		b.Skip("no benchmark files, set GO_DATA_PATH env")
	}
}

func MakeFileTests[T types.Number](n int) []Test[T] {
	if GO_DATA_PATH == "" {
		return nil
	}
	files, err := filepath.Glob(filepath.Join(GO_DATA_PATH, "*.bin"))
	if err != nil {
		panic(err)
	}
	bench := make([]Test[T], len(files))
	for i, name := range files {
		f, err := OpenFile[T](name)
		if err != nil {
			panic(err)
		}
		bench[i].Name = strings.TrimSuffix(filepath.Base(name), filepath.Ext(name))
		bench[i].Data = make([]T, 0, n)
		bench[i].N = n
		bench[i].F = f
	}
	runtime.AddCleanup(&bench, func(_ *File[T]) {
		for _, b := range bench {
			b.F.Close()
		}
	}, nil)
	return bench
}

func MakeFileBenchmarks[T types.Number](n int) []Benchmark[T] {
	if GO_DATA_PATH == "" {
		return nil
	}
	files, err := filepath.Glob(filepath.Join(GO_DATA_PATH, "*.bin"))
	if err != nil {
		panic(err)
	}
	bench := make([]Benchmark[T], len(files))
	for i, name := range files {
		f, err := OpenFile[T](name)
		if err != nil {
			panic(err)
		}
		bench[i].Name = strings.TrimSuffix(filepath.Base(name), filepath.Ext(name))
		bench[i].Data = make([]T, 0, n)
		bench[i].N = n
		bench[i].F = f
	}
	runtime.AddCleanup(&bench, func(_ *File[T]) {
		for _, b := range bench {
			b.F.Close()
		}
	}, nil)
	return bench
}

// Raw Number File
type File[T types.Number] struct {
	f *os.File
}

func OpenFile[T types.Number](name string) (*File[T], error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	return &File[T]{f: f}, nil
}

func (f *File[T]) Close() error {
	err := f.f.Close()
	if err == nil {
		f.f = nil
	}
	return err
}

func (f *File[T]) Len() int {
	return f.Size() / util.SizeOf[T]()
}

func (f *File[T]) Size() int {
	fi, err := f.f.Stat()
	if err != nil {
		return 0
	}
	return int(fi.Size())
}

func (f *File[T]) Rewind() error {
	_, err := f.f.Seek(0, 0)
	return err
}

func (f *File[T]) NextN(n int, dst []T) ([]T, int) {
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
