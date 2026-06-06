// Copyright (c) 2024-2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"io"
	"unsafe"
)

// writeU16 writes a uint16 in machine native endianess to wire.
// used for writing enum codes.
func writeU16[T int | uint16](w io.Writer, v T) error {
	val := uint16(v)
	_, err := w.Write(unsafe.Slice((*byte)(unsafe.Pointer(&val)), 2))
	return err
}

// writeU32 writes a uint32 in machine native endianess to wire.
// used for writing string/byte lengths and float32 bits.
func writeU32[T int | uint32](w io.Writer, v T) error {
	val := uint32(v)
	_, err := w.Write(unsafe.Slice((*byte)(unsafe.Pointer(&val)), 4))
	return err
}

// writeU64 writes a uint64 in machine native endianess to wire.
// used for writing float64 bits, timestamps.
func writeU64[T int64 | uint64](w io.Writer, v T) error {
	val := uint64(v)
	_, err := w.Write(unsafe.Slice((*byte)(unsafe.Pointer(&val)), 8))
	return err
}
