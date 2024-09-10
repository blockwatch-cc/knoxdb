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
	"strconv"
	"strings"

	"blockwatch.cc/knoxdb/internal/dst/vfs"
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
	u64seed, _ := strconv.ParseInt(os.Getenv(randomSeedKey), 0, 64)
	random = rand.New(rand.NewSource(u64seed))

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
		WithFSConfig(wazero.NewFSConfig().(sysfs.FSConfig).WithSysFSMount(vfs.New("/"), "/"))

	// vfs.MustInstantiate(ctx, r)

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
			seed = strconv.FormatUint(random.Uint64(), 16)
		}
		log.Debugf("Using random seed 0x%s", seed)
		config = config.WithEnv(randomSeedKey, "0x"+seed)
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

		// log.Infof("Exported functions")
		// for n, d := range mod.ExportedFunctionDefinitions() {
		// 	log.Infof("%s: %s(%v) %v", n, d.Name(), d.ParamNames(), d.ResultNames())
		// }
		// log.Infof("Exported memory")
		// for n, d := range mod.ExportedMemoryDefinitions() {
		// 	m, _ := d.Max()
		// 	log.Infof("%s: %d %v pages=%d/%d", n, d.Index(), d.ExportNames(), d.Min(), m)
		// }

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
			// Note: here's the assumption that wazero is imported as github.com/tetratelabs/wazero.
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
