// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package store

import (
	"errors"
	"fmt"
	"os"
)

var (
	ErrIsDir        = errors.New("is a directory")
	ErrNoDir        = errors.New("not a directory")
	ErrNotWriteable = errors.New("is not writeable")
	ErrNotReadable  = errors.New("is not readable")
)

// CheckFilesExists reports whether the named file exists, is not a
// directory and is at least readable.
func CheckFileExists(name string) (bool, error) {
	stat, err := os.Stat(name)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	if stat.IsDir() {
		return false, fmt.Errorf("%q: %w", name, ErrIsDir)
	}
	if stat.Mode().Perm()&0400 == 0 {
		return false, fmt.Errorf("%q: %w", name, ErrNotReadable)
	}
	return true, nil
}

// EnsureDirExists creates the directory if it does not exist
func EnsureDirExists(dir string) error {
	stat, err := os.Stat(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return os.MkdirAll(dir, 0o755)
		}
		return err
	}
	if !stat.IsDir() {
		return fmt.Errorf("%q: %w", dir, ErrNoDir)
	}
	return nil
}

// Fsyncs the directory to ensure new files and file metadata changes
// persist.
func SyncDir(name string) error {
	dir, err := os.Open(name)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
