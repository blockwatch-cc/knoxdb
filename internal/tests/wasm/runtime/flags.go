// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"

	"blockwatch.cc/knoxdb/pkg/util"
)

var (
	flags     = flag.NewFlagSet("runtime", flag.ContinueOnError)
	module    string
	seed      string
	cachedir  string
	tracefile string
	randomize bool
	runs      int
	verbose   bool
	vdebug    bool
	vtrace    bool
	random    *rand.Rand
)

func init() {
	flags.Usage = func() {}
	flags.BoolVar(&verbose, "v", true, "be verbose")
	flags.BoolVar(&vdebug, "vv", false, "enable debug mode")
	flags.BoolVar(&vtrace, "vvv", false, "enable trace mode")
	flags.StringVar(&module, "module", "dst.test", "WASM module to run")
	flags.StringVar(&cachedir, "cachedir", os.TempDir(), "WASM compiler cache directory")
	flags.StringVar(&tracefile, "tracefile", "", "file activity trace file")
	flags.StringVar(&seed, "seed", os.Getenv(util.GORANDSEED), "determinism seed")
	flags.BoolVar(&randomize, "randomize", false, "randomize seeds")
	flags.IntVar(&runs, "runs", 1, "execute test with `n` different seeds")
}

func printhelp() {
	fmt.Println("Usage: runtime -module=[name] [flags]")
	fmt.Println("Flags:")
	flags.PrintDefaults()
	fmt.Println()
}

// strip runtime-related flags from os.Args
func splitFlags(_ []string, flags *flag.FlagSet) ([]string, []string) {
	rtFlags := make([]string, 0)
	modFlags := []string{module}
	for i := 1; i < len(os.Args); i++ {
		flagName, _, _ := strings.Cut(os.Args[i][1:], "=")
		isKnown := flags.Lookup(flagName) != nil || os.Args[i] == "-h"
		isSingle := true
		if i+1 < len(os.Args) {
			if !strings.HasPrefix(os.Args[i+1], "-") {
				isSingle = false
			}
		}
		if isKnown {
			rtFlags = append(rtFlags, os.Args[i])
			if !isSingle {
				rtFlags = append(rtFlags, os.Args[i+1])
				i++
			}
		} else {
			modFlags = append(modFlags, os.Args[i])
			if !isSingle {
				modFlags = append(modFlags, os.Args[i+1])
				i++
			}
		}
	}
	return rtFlags, modFlags
}
