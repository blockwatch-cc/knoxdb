// Copyright (c) 2024 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package wal

import "errors"

var (
	ErrInvalidRecord    = errors.New("invalid record")
	ErrInvalidWalOption = errors.New("invalid wal options ")
	ErrClosed           = errors.New("closed")
)
