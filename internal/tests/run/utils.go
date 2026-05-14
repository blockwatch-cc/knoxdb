// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/echa/log"
)

var (
	timeFmt        = "2006-01-02_15-04-05"
	arch           string // wasm, native
	testrun        string
	numCpu         int
	enableRace     bool
	enableVerbose  bool
	maxRound       int
	maxErrors      int
	maxErrorRate   float64
	logPath        string
	skipRetentiona bool
	seedString     string
	seedList       []uint64
	timeout        time.Duration
)

func init() {
	// cli flags
	flag.StringVar(&arch, "arch", "native", "test with arch wasm/native")
	flag.StringVar(&testrun, "run", "", "regex to select workload to run")
	flag.BoolVar(&enableRace, "race", false, "enable race detector")
	flag.BoolVar(&enableVerbose, "v", false, "enable test log streaming")
	flag.IntVar(&maxRound, "count", 1, "number of iterations using different random seeds")
	flag.IntVar(&numCpu, "cpu", runtime.NumCPU(), "number of CPU cores to use for running tests")
	flag.IntVar(&maxErrors, "max-errors", 1, "stop the test runner after N total observed errors")
	flag.Float64Var(&maxErrorRate, "max-error-rate", 10, "stops the test runner when the rate of errors observed per second is greater than N (inclusive)")
	flag.StringVar(&seedString, "seed", "", "comma separated list of random seeds (uint64, each min 16 char long)")
	flag.DurationVar(&timeout, "timeout", time.Minute, "test run timeout (will abort and trace test run)")
	flag.StringVar(&logPath, "logs", "", "output path for test failure logs")
}

func initFlags() error {
	flag.Parse()

	if seedString != "" {
		for v := range strings.SplitSeq(seedString, ",") {
			if len(v) < 16 {
				return fmt.Errorf("seed %q too short, need at least 16 chars", v)
			}
			val, _ := strconv.ParseUint(v, 0, 64)
			seedList = append(seedList, val)
		}
	}

	if enableVerbose {
		log.SetLevel(log.LevelDebug)
	}
	return nil
}

func initStorage() error {
	if logPath == "" {
		log.Warn("Missing log path, disabling failure log retention. Use -logs to enable.")
		skipRetentiona = true
		return nil
	}
	// ensure logpath exists and is writable
	if stat, err := os.Stat(logPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return os.MkdirAll(logPath, 0755)
		}
		return err
	} else if !stat.IsDir() {
		return fmt.Errorf("log retention path %q: not a directory", logPath)
	} else if stat.Mode()&0200 == 0 {
		return fmt.Errorf("log retention path %q: not writable", logPath)
	}
	return nil
}

func logBuildInfo() {
	if os.Getenv("DRONE_BUILD_STARTED") != "" {
		logDroneBuild()
	} else {
		logLocalBuild()
	}
	if len(seedList) > 0 {
		log.Infof("Test Seeds    %s", seedString)
	} else {
		log.Infof("Test Seeds    randomized")
	}
	log.Infof("Test Rounds   %d", maxRound)
	log.Infof("Test Timeout  %s", timeout)
	log.Infof("Test Errors   max=%d max-rate=%f", maxErrors, maxErrorRate)
}

func logDroneBuild() {
	var hint string
	switch os.Getenv("DRONE_BUILD_EVENT") {
	case "pull_request":
		hint = fmt.Sprintf("%s/pull/%s", os.Getenv("DRONE_REPO_LINK"), os.Getenv("DRONE_PULL_REQUEST"))
	case "tag":
		hint = fmt.Sprintf("%s#%s", os.Getenv("DRONE_REPO_LINK"), os.Getenv("DRONE_TAG"))
	case "unknown":
		// non-drone execution
	default:
		hint = fmt.Sprintf("%s/commit/%s", os.Getenv("DRONE_REPO_LINK"), os.Getenv("DRONE_COMMIT"))
	}

	ts, _ := strconv.Atoi(os.Getenv("DRONE_BUILD_STARTED"))
	if ts == 0 {
		ts = int(time.Now().Unix())
	}

	log.Info("KnoxDB Test Scenario Runner")
	log.Infof("Build ID      #%s %s", os.Getenv("DRONE_BUILD_NUMBER"), os.Getenv("DRONE_BUILD_LINK"))
	log.Infof("Build Date    %s", time.Unix(int64(ts), 0).UTC().Format(time.DateTime))
	log.Infof("Build System  %s", os.Getenv("DRONE_SYSTEM_HOST"))
	log.Infof("Build Target  %s/%s", os.Getenv("DRONE_STAGE_OS"), os.Getenv("DRONE_STAGE_ARCH"))
	log.Infof("Build Repo    %s %s", os.Getenv("DRONE_REPO"), hint)
	log.Infof("Build Branch  %s", os.Getenv("DRONE_REPO_BRANCH"))
	log.Infof("Build Commit  %s", os.Getenv("DRONE_COMMIT"))
	log.Infof("Test Mode     drone ci")
	if !skipRetentiona {
		log.Infof("Failure logs are retained in %s", logPath)
	}
}

func logLocalBuild() {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return
	}

	var revision string
	var dirty bool
	for _, bs := range bi.Settings {
		switch bs.Key {
		case "vcs.revision":
			revision = bs.Value
			if len(revision) > 9 {
				revision = revision[:9]
			}
		case "vcs.modified":
			if bs.Value == "true" {
				dirty = true
			}
		}
	}
	if dirty {
		revision += "-dirty"
	}

	goos := os.Getenv("GOOS")
	goarch := os.Getenv("GOARCH")
	if arch == "wasm" {
		goos = "wasip1"
		goarch = "wasm"
	}
	if goos == "" {
		goos = runtime.GOOS
	}
	if goarch == "" {
		goarch = runtime.GOARCH
	}

	log.Info("KnoxDB Test Scenario Runner")
	log.Infof("Build Date    %s", time.Now().UTC().Format(time.DateTime))
	log.Infof("Build System  %s/%s", runtime.GOOS, runtime.GOARCH)
	log.Infof("Build Target  %s/%s", goos, goarch)
	log.Infof("Build Repo    %s %s", bi.Main.Path, bi.Main.Version)
	log.Infof("Build Commit  %s", revision)
	log.Infof("Build Version %s", bi.GoVersion)
	log.Infof("Test Mode     local")
}

func copyFile(srcPath, dstPath string) error {
	src, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer src.Close()

	dst, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	defer dst.Close()

	_, err = io.Copy(dst, src)
	if err != nil {
		return err
	}
	return dst.Sync()
}
