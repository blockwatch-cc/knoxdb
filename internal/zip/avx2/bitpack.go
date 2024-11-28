// Copyright (c) 2022-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build !amd64 || appengine || gccgo
// +build !amd64 appengine gccgo

package avx2

import "blockwatch.cc/knoxdb/internal/zip/generic"

// Go package exports
var (
	PackBytes16Bit   = generic.PackBytes16Bit
	PackBytes32Bit   = generic.PackBytes32Bit
	UnpackBytes16Bit = generic.UnpackBytes16Bit
	UnpackBytes32Bit = generic.UnpackBytes32Bit
)
