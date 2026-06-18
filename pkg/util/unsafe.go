// Copyright (c) 2023-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package util

import (
	"unsafe"
)

func UnsafeGetBytes(s string) []byte {
	if s == "" {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

func UnsafeGetString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}

type eface struct {
	typ unsafe.Pointer
	val unsafe.Pointer
}

func UnboxAny(v any) unsafe.Pointer {
	if v == nil {
		return nil
	}
	ef := (*eface)(unsafe.Pointer(&v))
	return ef.val
}
