// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package scenarios

import (
	"flag"
	"os"
	"testing"

	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/echa/log"
)

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Verbose() {
		// log.SetLevel(log.LevelDebug)
	} else {
		log.SetLevel(log.LevelInfo)
	}
	os.Exit(m.Run())
}

func SetupDeterministicRand(t *testing.T) {
	// manage random seeds to drive the determinism for this test
	seed := util.RandSeed()

	// create a new random seed for multiple runs unless a user-defined seed is used
	testRun++
	if testRun > 1 && os.Getenv(util.GORANDSEED) == "" {
		seed = util.RandUint64()
	}

	// re-init random number generator (resets pseudo-randomness so that
	// rand usage in other testcases does not impact the random selection here)
	t.Logf("%s=0x%016x", util.GORANDSEED, seed)
	util.RandInit(seed)

}
