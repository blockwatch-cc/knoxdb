// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//go:build linux
// +build linux

package wal

import (
	"os"
	"syscall"
)

const (
	// Size to align the buffer to
	alignSize = 4096

	O_DIRECT = syscall.O_DIRECT
)

// OpenFile is a modified version of os.OpenFile which sets O_DIRECT
func OpenFile(name string, flag int, perm os.FileMode) (file *os.File, err error) {
	return os.OpenFile(name, flag, perm)
}

func fcntl(fd uintptr, cmd uintptr, arg uintptr) (uintptr, error) {
	r0, _, e1 := syscall.Syscall(syscall.SYS_FCNTL, fd, uintptr(cmd), uintptr(arg))
	if e1 != 0 {
		return 0, e1
	}

	return r0, nil
}

func setDirectIO(fd uintptr, on bool) error {
	flag, err := fcntl(fd, syscall.F_GETFL, 0)
	if err != nil {
		return err
	}

	if on {
		flag |= O_DIRECT
	} else {
		flag &^= O_DIRECT
	}

	_, err = fcntl(fd, syscall.F_SETFL, flag)
	return err
}
