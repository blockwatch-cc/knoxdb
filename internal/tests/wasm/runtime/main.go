// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"

	"blockwatch.cc/knoxdb/internal/tests/wasm/vfs"
	"github.com/echa/log"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/experimental/sysfs"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
	"github.com/tetratelabs/wazero/sys"
)

const (
	randomSeedKey = "GORANDSEED"
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
	flags.StringVar(&seed, "seed", os.Getenv(randomSeedKey), "determinism seed")
	flags.BoolVar(&randomize, "randomize", false, "randomize seeds")
	flags.IntVar(&runs, "runs", 1, "execute test with `n` different seeds")
}

func printhelp() {
	fmt.Println("Usage: runtime -module=[name] [flags]")
	fmt.Println("Flags:")
	flags.PrintDefaults()
	fmt.Println()
}

func main() {
	if err := run(); err != nil {
		log.Error(err)
		os.Exit(1)
	}
}

func run() error {
	rtFlags, modFlags := splitFlags(os.Args[1:], flags)
	if err := flags.Parse(rtFlags); err != nil {
		if err == flag.ErrHelp {
			printhelp()
			return nil
		}
		return err
	}
	if !randomize && seed == "" {
		return fmt.Errorf("Missing random seed, set %s env var", randomSeedKey)
	}

	// setup logging
	switch {
	case vtrace:
		log.SetLevel(log.LevelTrace)
	case vdebug:
		log.SetLevel(log.LevelDebug)
	case verbose:
		log.SetLevel(log.LevelInfo)
	}
	plog := log.NewProgressLogger(log.Log).SetEvent("test")

	// init pseudo-random generator from main seed
	u64seed, _ := strconv.ParseUint(seed, 0, 64)
	random = rand.New(rand.NewSource(int64(u64seed)))
	seed = fmt.Sprintf("0x%016x", u64seed)

	log.Infof("WASM runtime wazero/%s %s %s/%s",
		getWazeroVersion(), runtime.Version(), runtime.GOOS, runtime.GOARCH)
	log.Infof("Using compile cache %s", cachedir)

	cc, err := wazero.NewCompilationCacheWithDir(cachedir)
	if err != nil {
		return fmt.Errorf("creating compilation cache: %w", err)
	}

	runtimeConfig := wazero.NewRuntimeConfig().
		// Enable debug info for better stack traces.
		WithDebugInfoEnabled(true).
		// Cache compilations to speed up subsequent runs.
		WithCompilationCache(cc)

	ctx := context.Background()
	r := wazero.NewRuntimeWithConfig(ctx, runtimeConfig)
	defer r.Close(ctx)

	config := wazero.NewModuleConfig().
		WithStdin(os.Stdin).
		WithStdout(os.Stdout).
		WithStderr(os.Stderr).
		// Time-related configuration options are to allow the module
		// to access "real" time on the host. We could use this as a source of
		// determinisme, but we currently compile the module with -faketime
		// which allows us to virtually speed up time with sleeping goroutines.
		// We could eventually revisit this, but this is fine for now.
		WithSysNanosleep().
		WithSysNanotime().
		WithSysWalltime().
		WithArgs(modFlags...).
		// Mount filesystem. This is taken from wazero's CLI implementation.
		WithFSConfig(wazero.NewFSConfig().(sysfs.FSConfig).
			WithSysFSMount(vfs.New("/", tracefile), "/"))

	log.Warnf("Exporting all env vars to wasm module, this is unsafe")
	env := os.Environ()
	slices.Sort(env)
	for _, e := range env {
		k, v, _ := strings.Cut(e, "=")
		if k == "PWD" {
			continue
		}
		config = config.WithEnv(k, v)
		log.Debugf("ENV %s=%s", k, sanitizeEnvVar(k, v))
	}

	log.Infof("Using module %s", module)
	buf, err := os.ReadFile(module)
	if err != nil {
		return fmt.Errorf("reading module: %w", err)
	}

	compiledModule, err := r.CompileModule(ctx, buf)
	if err != nil {
		return fmt.Errorf("compiling module: %w", err)
	}

	// Instantiate WASI, which implements host functions needed for TinyGo to
	// implement `panic`.
	wasi_snapshot_preview1.MustInstantiate(ctx, r)
	if len(modFlags) > 1 {
		log.Infof("Using module flags %s", strings.Join(modFlags[1:], " "))
	}

	// Instantiate the guest Wasm into the same runtime.
	for i := 0; i < runs; i++ {
		if randomize {
			seed = fmt.Sprintf("0x%016x", random.Uint64())
		}
		log.Debugf("Using random seed %s", seed)
		config = config.WithEnv(randomSeedKey, seed)

		mod, err := r.InstantiateModule(ctx, compiledModule, config)
		if err != nil {
			if exitErr, ok := err.(*sys.ExitError); ok {
				// exitCode := exitErr.ExitCode()
				// if exitCode == sys.ExitCodeDeadlineExceeded {
				// 	return fmt.Errorf("module failed: %v (timeout %v)", exitErr, timeout)
				// }
				return fmt.Errorf("module failed: %w", exitErr)
			}
			return fmt.Errorf("instantiating module: %w", err)
		}

		if err := mod.Close(ctx); err != nil {
			return fmt.Errorf("close module: %w", err)
		}
		plog.Log(1, fmt.Sprintf("%d total", i+1))
	}
	return nil
}

func getWazeroVersion() (ret string) {
	info, ok := debug.ReadBuildInfo()
	if ok {
		for _, dep := range info.Deps {
			// Note: we assume wazero is imported as github.com/tetratelabs/wazero.
			if strings.Contains(dep.Path, "github.com/tetratelabs/wazero") {
				ret = dep.Version
			}
		}
	} else {
		ret = "unknown"
	}
	return
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

func containsAny(s string, vals ...string) bool {
	for _, v := range vals {
		if strings.Contains(s, v) {
			return true
		}
	}
	return false
}

func sanitizeEnvVar(k, v string) string {
	if containsAny(strings.ToUpper(k), "SECRET", "KEY", "PASS") {
		v = strings.Repeat("*", len(v))
	}
	return v
}
