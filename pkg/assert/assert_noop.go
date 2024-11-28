// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//
//go:build !with_assert

package assert

func Always(condition bool, message string, details ...any)              {}
func AlwaysOrUnreachable(condition bool, message string, details ...any) {}
func Sometimes(condition bool, message string, details ...any)           {}
func Unreachable(message string, details ...any)                         {}
func Reachable(message string, details ...any)                           {}
