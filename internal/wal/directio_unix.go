// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//go:build !windows && !darwin && !openbsd && !plan9
// +build !windows,!darwin,!openbsd,!plan9

package wal

import (
	"os"
	"syscall"
)

const (
	// Size to align the buffer to
	alignSize = 4096
)

// OpenFile is a modified version of os.OpenFile which sets O_DIRECT
func OpenFile(name string, flag int, perm os.FileMode) (file *os.File, err error) {
	return os.OpenFile(name, syscall.O_DIRECT|flag, perm)
}
