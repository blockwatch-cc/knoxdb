// Copyright (c) 2018-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package dedup

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"strconv"
	"testing"
)

const (
	nativeBufLen             = 4
	defaultMaxPointsPerBlock = 1 << 16
)

func makeRandData(n, l int) [][]byte {
	b := make([][]byte, n, n)
	for i := range b {
		b[i] = make([]byte, l)
		rand.Read(b[i])
	}
	return b
}

func makeNumberedData(n int) [][]byte {
	b := make([][]byte, n, n)
	for i := range b {
		b[i] = make([]byte, 8)
		binary.BigEndian.PutUint64(b[i][:], uint64(i))
	}
	return b
}

func cloneData(b [][]byte) [][]byte {
	c := make([][]byte, len(b))
	for i, v := range b {
		c[i] = make([]byte, len(v))
		copy(c[i][:], v)
	}
	return c
}

func TestNativeElem(t *testing.T) {
	rand.Seed(1337)
	data := makeNumberedData(defaultMaxPointsPerBlock)
	arr := newNativeByteArrayFromBytes(data)
	if got, want := arr.Len(), defaultMaxPointsPerBlock; got != want {
		t.Errorf("Len mismatch got=%d want=%d", got, want)
	}
	if got, want := arr.Cap(), defaultMaxPointsPerBlock; got != want {
		t.Errorf("Cap mismatch got=%d want=%d", got, want)
	}
	for i := range data {
		if got, want := arr.Elem(i), data[i]; !bytes.Equal(got, want) {
			t.Errorf("Elem %d mismatch got=%x want=%x", i, got, want)
		}
	}
}

func TestNativeAppend(t *testing.T) {
	rand.Seed(1337)
	for i := 0; i < 100; i++ {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			data := makeRandData(defaultMaxPointsPerBlock, nativeBufLen)
			arr := newNativeByteArray(defaultMaxPointsPerBlock)
			if got, want := arr.Len(), 0; got != want {
				t.Errorf("Len mismatch got=%d want=%d", got, want)
			}
			if got, want := arr.Cap(), defaultMaxPointsPerBlock; got != want {
				t.Errorf("Cap mismatch got=%d want=%d", got, want)
			}
			for i := range data {
				arr.Append(data[i])
				if got, want := arr.Elem(i), data[i]; !bytes.Equal(got, want) {
					t.Errorf("Elem %d mismatch got=%x want=%x", i, got, want)
				}
			}
		})
	}
}

func TestNativeAppendFrom(t *testing.T) {
	rand.Seed(1337)
	data := makeRandData(defaultMaxPointsPerBlock, nativeBufLen)
	clone := cloneData(data)
	src := newNativeByteArrayFromBytes(data)
	dst := newNativeByteArray(defaultMaxPointsPerBlock)
	dst.AppendFrom(src)
	if got, want := dst.Len(), src.Len(); got != want {
		t.Errorf("Len mismatch got=%d want=%d", got, want)
	}
	src.Clear()
	for i := range clone {
		if got, want := dst.Elem(i), clone[i]; !bytes.Equal(got, want) {
			t.Errorf("Elem %d mismatch got=%x want=%x", i, got, want)
		}
	}
}
