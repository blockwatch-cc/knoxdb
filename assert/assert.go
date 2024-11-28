// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//
//go:build with_assert

package assert

import (
	"fmt"
	"hash/fnv"
	"strings"
)

// Assertion package inspired by Anthisesis deterministic simulation tests.
// This package is in early development. Some condition semantics are not yet
// supported. Examples are the non-execution of Always and Reachable assertions
// which are supposed to result in failed assertions at program exit.

const (
	kindAlways              = "Always"
	kindAlwaysOrUnreachable = "AlwaysOrUnreachable"
	kindSometimes           = "Sometimes"
	kindReachable           = "Reachable"
	kindUnreachable         = "Unreachable"
)

const (
	failOnMiss     = true
	dontfailOnMiss = false
)

// Always asserts that cond is true every time this function is called,
// and that it is called at least once.
func Always(cond bool, msg string, details map[string]any) {
	assertImpl(cond, msg, details, failOnMiss, kindAlways)
}

// AlwaysOrUnreachable asserts that cond is true every time this function
// is called. The corresponding test property will pass if the assertion is never
// encountered (unlike Always assertion types).
func AlwaysOrUnreachable(cond bool, msg string, details map[string]any) {
	assertImpl(cond, msg, details, failOnMiss, kindAlwaysOrUnreachable)
}

// Sometimes asserts that cond is true at least one time that this function
// was called. (If the assertion is never encountered, the test property will
// therefore fail.)
func Sometimes(cond bool, msg string, details map[string]any) {
	assertImpl(cond, msg, details, dontfailOnMiss, kindSometimes)
}

// Unreachable asserts that a line of code is never reached. The corresponding
// test property will fail if this function is ever called. (If it is never
// called the test property will therefore pass.)
func Unreachable(msg string, details map[string]any) {
	assertImpl(false, msg, details, failOnMiss, kindUnreachable)
}

// Reachable asserts that a line of code is reached at least once.
// The corresponding test property will pass if this function is ever
// called. (If it is never called the test property will therefore fail.)
func Reachable(msg string, details map[string]any) {
	assertImpl(true, msg, details, failOnMiss, kindReachable)
}

type ErrAssert struct {
	kind    string
	msg     string
	loc     *locationInfo
	details map[string]any
}

func (e *ErrAssert) Error() string {
	id := makeKey(e.msg, e.loc)
	var b strings.Builder
	fmt.Fprintf(&b, "%s assertion failed [0x%016x, %s]: ", e.kind, id, e.loc)
	b.WriteString(e.msg)
	if len(e.details) > 0 {
		b.WriteByte(' ')
		fmt.Fprintf(&b, "%v", e.details)
	}
	return b.String()
}

func assertImpl(cond bool, msg string, details map[string]any, failOnMiss bool, kind string) {
	if cond {
		return
	}
	if !failOnMiss {
		return
	}
	loc := newLocationInfo(3)
	e := &ErrAssert{kind, msg, loc, details}
	panic(e)
}

func makeKey(msg string, loc *locationInfo) uint64 {
	h := fnv.New64a()
	h.Write([]byte(loc.PackageName))
	h.Write([]byte(loc.FuncName))
	h.Write([]byte(msg))
	return h.Sum64()
}
