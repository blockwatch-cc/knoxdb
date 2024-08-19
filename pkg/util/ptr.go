// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package util

func PtrTo[T any](v T) *T { return &v }