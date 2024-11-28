// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//go:build wasm
// +build wasm

package engine

import "blockwatch.cc/knoxdb/internal/store/mem"

func (o DatabaseOptions) ToDriverOpts() any {
	switch o.Driver {
	case "bolt":
		return nil
	case "mem":
		return &mem.Options{
			ReadOnly: o.ReadOnly,
			Persist:  !o.NoSync,
		}
	default:
		return nil
	}
}

func (o TableOptions) ToDriverOpts() any {
	switch o.Driver {
	case "bolt":
		return nil
	case "mem":
		return &mem.Options{
			ReadOnly: o.ReadOnly,
			Persist:  !o.NoSync,
		}
	default:
		return nil
	}
}

func (o StoreOptions) ToDriverOpts() any {
	switch o.Driver {
	case "bolt":
		return nil
	case "mem":
		return &mem.Options{
			ReadOnly: o.ReadOnly,
			Persist:  !o.NoSync,
		}
	default:
		return nil
	}
}

func (o IndexOptions) ToDriverOpts() any {
	switch o.Driver {
	case "bolt":
		return nil
	case "mem":
		return &mem.Options{
			ReadOnly: o.ReadOnly,
			Persist:  !o.NoSync,
		}
	default:
		return nil
	}
}
