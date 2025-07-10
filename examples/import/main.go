// Copyright (c) 2024 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package main

import (
	"flag"
	"fmt"

	"github.com/echa/log"
)

var (
	in string // path to file
)

func main() {
	flag.StringVar(&in, "in", "", "file path")
	flag.Parse()

	printVersion()

	if err := run(); err != nil {
		log.Errorf("%s: %v", appName, err)
	}
}

func run() error {
	if in == "" {
		return fmt.Errorf("in file path should not be empty")
	}

	return Import(in)
}
