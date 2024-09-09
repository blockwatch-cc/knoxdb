// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//go:build wasm
// +build wasm

package engine

func (o DatabaseOptions) ToDriverOpts() any {
	switch o.Driver {
	case "bolt":
		return nil
	default:
		return nil
	}
}

func (o TableOptions) ToDriverOpts() any {
	switch o.Driver {
	case "bolt":
		return nil
	default:
		return nil
	}
}

func (o StoreOptions) ToDriverOpts() any {
	switch o.Driver {
	case "bolt":
		return nil
	default:
		return nil
	}
}

func (o IndexOptions) ToDriverOpts() any {
	switch o.Driver {
	case "bolt":
		return nil
	default:
		return nil
	}
}
