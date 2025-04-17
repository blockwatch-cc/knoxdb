// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//go:build wasm
// +build wasm

package wal

import (
	"os"
)

const (
	// wasm doesn't need any alignment
	alignSize = 0
)

func OpenFile(name string, flag int, perm os.FileMode) (file *os.File, err error) {
	return os.OpenFile(name, flag, perm)
}
