// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package main

import (
	"github.com/echa/log"
)

var (
	commit  string = "dev"
	version string = "v0.0.1"
	appName string = "knox-importer"
)

func printVersion() {
	log.Infof("%s - %s/%s", appName, version, commit)
	log.Info("Copyright (c) 2025 Blockwatch Data Inc.")
}
