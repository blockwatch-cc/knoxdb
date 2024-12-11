// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package scenarios

import (
	"flag"
	"os"
	"testing"

	"github.com/echa/log"
)

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Verbose() {
		log.SetLevel(log.LevelDebug)
	} else {
		log.SetLevel(log.LevelInfo)
	}
	os.Exit(m.Run())
}
