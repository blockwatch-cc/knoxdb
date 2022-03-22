// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build !amd64 || appengine || gccgo
// +build !amd64 appengine gccgo

package util

import (
	"log"
)

func LogCPUFeatures(l *log.Logger) {
	l.Printf("Non-Intel CPU architecture")
}
