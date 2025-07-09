// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/debug"
	"slices"
	"strings"

	"blockwatch.cc/knoxdb/internal/tests/wasm/vfs"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/echa/log"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/experimental/sysfs"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
	"github.com/tetratelabs/wazero/sys"
)

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
		return fmt.Errorf("missing random seed, set %s env var", util.GORANDSEED)
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
	seed = fmt.Sprintf("0x%016x", util.RandSeed())

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

	// intercept stdout, stderr and unwrap playback header from use of faketime
	stdout, stdoutWriter, _ := os.Pipe()
	stderr, stderrWriter, _ := os.Pipe()
	go skipPlaygroundOutputHeaders(os.Stdout, stdout)
	go skipPlaygroundOutputHeaders(os.Stderr, stderr)
	defer stdoutWriter.Close()
	defer stderrWriter.Close()

	config := wazero.NewModuleConfig().
		WithStdin(os.Stdin).
		WithStdout(stdoutWriter).
		WithStderr(stderrWriter).
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

	// forward log color flag if terminal supports color
	if log.Log.IsColor() {
		config = config.WithEnv("LOGCOLOR", "true")
	} else {
		config = config.WithEnv("LOGCOLOR", "false")
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
		config = config.WithEnv(util.GORANDSEED, seed)

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

func skipPlaygroundOutputHeaders(out io.Writer, in io.Reader) {
	bufin := bufio.NewReader(in)

	// Playback header: 0 0 P B <8-byte time> <4-byte data length>
	head := make([]byte, 4+8+4)
	for {
		if _, err := io.ReadFull(bufin, head); err != nil {
			if err != io.EOF {
				fmt.Fprintln(out, "read error:", err)
			}
			return
		}
		if !bytes.HasPrefix(head, []byte{0x00, 0x00, 'P', 'B'}) {
			// fmt.Fprintf(out, "expected playback header, got %q\n", head)
			io.Copy(out, bytes.NewBuffer(head))
			io.Copy(out, bufin)
			return
		}
		// Copy data until next header.
		size := binary.BigEndian.Uint32(head[12:])
		io.CopyN(out, bufin, int64(size))
	}
}
