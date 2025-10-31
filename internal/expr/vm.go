// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//go:build ignore
// +build ignore

package expr

import (
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/types"
)

type Vm struct {
	vRegs  [16]*Register // virtual (or [16]uint8, depends on src/dst vector mem abstraction)
	pRegs  [16]*Register // physical
	mRegs  [8][2]uint64  // masks
	rStore []byte        // physical register backing storage
	code   Bytecode
}

type Bytecode struct {
}

// [128]T
type Register struct {
	buf *byte
	typ types.BlockType
}

func (vm *Vm) Call(pkg *pack.Package) error {
	return nil
}
