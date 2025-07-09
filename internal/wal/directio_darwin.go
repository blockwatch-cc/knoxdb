// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package wal

import (
	"fmt"
	"os"
	"syscall"
)

const (
	// OSX doesn't need any alignment
	alignSize = 0
)

func OpenFile(name string, flag int, perm os.FileMode) (file *os.File, err error) {
	file, err = os.OpenFile(name, flag, perm)
	if err != nil {
		return
	}

	// Set F_NOCACHE to avoid caching
	// F_NOCACHE    Turns data caching off/on. A non-zero value in arg
	//              turns data caching off.  A value of zero in arg turns
	//              data caching on.
	_, _, errNo := syscall.Syscall(syscall.SYS_FCNTL, file.Fd(), syscall.F_NOCACHE, 1)
	if errNo != 0 {
		err = fmt.Errorf("failed to enable direct i/o: %v", err)
		file.Close()
		file = nil
	}

	return
}
