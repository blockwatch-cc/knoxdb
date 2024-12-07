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
func Always(cond bool, msg string, details ...any) {
	assertImpl(cond, msg, failOnMiss, kindAlways, details)
}

// AlwaysOrUnreachable asserts that cond is true every time this function
// is called. The corresponding test property will pass if the assertion is never
// encountered (unlike Always assertion types).
func AlwaysOrUnreachable(cond bool, msg string, details ...any) {
	assertImpl(cond, msg, failOnMiss, kindAlwaysOrUnreachable, details)
}

// Sometimes asserts that cond is true at least one time that this function
// was called. (If the assertion is never encountered, the test property will
// therefore fail.)
func Sometimes(cond bool, msg string, details ...any) {
	assertImpl(cond, msg, dontfailOnMiss, kindSometimes, details)
}

// Unreachable asserts that a line of code is never reached. The corresponding
// test property will fail if this function is ever called. (If it is never
// called the test property will therefore pass.)
func Unreachable(msg string, details ...any) {
	assertImpl(false, msg, failOnMiss, kindUnreachable, details)
}

// Reachable asserts that a line of code is reached at least once.
// The corresponding test property will pass if this function is ever
// called. (If it is never called the test property will therefore fail.)
func Reachable(msg string, details ...any) {
	assertImpl(true, msg, failOnMiss, kindReachable, details)
}

type ErrAssert struct {
	kind    string
	msg     string
	loc     *locationInfo
	details []any
}

func (e *ErrAssert) Error() string {
	id := makeKey(e.msg, e.loc)
	var b strings.Builder
	fmt.Fprintf(&b, "%s assertion failed [0x%016x, %s]: ", e.kind, id, e.loc)
	b.WriteString(e.msg)
	if len(e.details) > 0 {
		b.WriteByte(' ')
		isEven := len(e.details)%2 == 0
		for i := 0; i < len(e.details); i += 2 {
			if i > 0 {
				b.WriteByte(' ')
			}
			if isEven {
				fmt.Fprintf(&b, "%v=%v", e.details[i], e.details[i+1])
			} else {
				fmt.Fprintf(&b, "%v", e.details[i])
			}
		}
	}
	return b.String()
}

func assertImpl(cond bool, msg string, failOnMiss bool, kind string, details ...any) {
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
