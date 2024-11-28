// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build !amd64 || appengine || gccgo
// +build !amd64 appengine gccgo

package avx512

import (
	"blockwatch.cc/knoxdb/internal/s8b/generic"
)

var (
	DecodeUint64 = generic.DecodeUint64
)
