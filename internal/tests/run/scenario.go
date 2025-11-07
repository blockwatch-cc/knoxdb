// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// run as
// go run -buildvcs=true ./internal/tests/run/ -v

package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/echa/log"
	"github.com/minio/minio-go/v7"
	"golang.org/x/time/rate"
)

func main() {
	if err := run(); err != nil {
		log.Error(err)
		os.Exit(1)
	}
}

func run() error {
	initFlags()

	// log repo and build identity info
	logBuildInfo()

	// use new random temp dir
	buildPath, err := os.MkdirTemp("", "knoxdb-scenario")
	if err != nil {
		return err
	}
	defer os.RemoveAll(buildPath) // clean up
	log.Debugf("Using temp dir %s", buildPath)

	logPath := util.NonEmptyString(os.Getenv("LOGS_PATH"), buildPath)

	// init s3
	s3, err := initStorage()
	if err != nil {
		return err
	}

	// build test
	build, run := setup()
	if err := build(buildPath, os.Stdout); err != nil {
		return err
	}

	var (
		f         *os.File
		rnd       uint64
		ctx       = context.Background()
		numErrors int
		errLimit  = rate.NewLimiter(rate.Limit(maxErrorRate), 60)
	)

	// cleanup on panic/exit
	defer func() {
		if f != nil {
			f.Close()
		}
		os.Unsetenv(util.GORANDSEED)
	}()

	for round := range maxRound {
		// cycle through seed list or generate random seed each round when empty
		if len(seedList) > 0 {
			rnd = seedList[round%len(seedList)]
		} else {
			rnd = util.RandUint64n(1<<64 - 1)
		}

		os.Setenv(util.GORANDSEED, strconv.FormatUint(rnd, 10))

		// create log file
		now := time.Now().UTC()
		logFileName := fmt.Sprintf("%s_0x%016x.log", now.Format(timeFmt), rnd)
		logFilePath := filepath.Join(logPath, logFileName)
		f, err = os.Create(logFilePath)
		if err != nil {
			return err
		}

		var (
			exit       chan bool
			r, w       *os.File
			sout, serr           = os.Stdout, os.Stderr
			mw         io.Writer = f // route test output to logfile as default
		)

		// split stream test output to terminal on request (-verbose)
		if enableVerbose {
			log.Debugf("Starting log streaming")

			// create writers
			mw = io.MultiWriter(f, os.Stdout)

			// get pipe reader and writer | writes to pipe writer come out pipe reader
			r, w, _ = os.Pipe()

			// replace stdout,stderr with pipe writer | all writes to stdout, stderr will go through pipe instead (fmt.print, log)
			os.Stdout = w
			os.Stderr = w

			// create channel to control exit | will block until all copies are finished
			exit = make(chan bool)

			go func() {
				// copy all reads from pipe to multiwriter, which writes to stdout and file
				_, _ = io.Copy(mw, r)
				// when r or w is closed copy will finish and true will be sent to channel
				exit <- true
			}()
		}

		log.Infof("Run scenario #%d/%d with %s=0x%016x", round+1, maxRound, util.GORANDSEED, rnd)

		// run test in child process
		if timeout > 0 {
			ctx2, cancel := context.WithTimeout(ctx, timeout)
			err = run(ctx2, buildPath, mw)
			cancel()
		} else {
			err = run(ctx, buildPath, mw)
		}

		if enableVerbose {
			// restore stdout
			os.Stdout = sout
			os.Stderr = serr

			// close writer then block on exit channel | this will let sout finish writing before we continue the loop
			_ = w.Close()
			<-exit
			log.Debugf("Stopped log streaming")
		}

		// close log file after all writes have finished
		_ = f.Close()

		// handle test run error
		if err != nil {
			log.Errorf("Fail scenario #%d/%d with %s=0x%016x err=%v", round+1, maxRound, util.GORANDSEED, rnd, err)

			if !errLimit.Allow() {
				return fmt.Errorf("stopping due to too high error frequency")
			}

			// upload
			if !skipUpload {
				logFileTarget := fmt.Sprintf("%s/%s", now.Format(time.DateOnly), logFileName)
				log.Infof("Uploading %s/%s/%s", s3endpoint, s3bucket, logFileTarget)
				_, err := s3.FPutObject(ctx, s3bucket, logFileTarget, logFilePath, minio.PutObjectOptions{})
				if err != nil {
					return err
				}
			} else {
				wd, err := os.Getwd()
				if err != nil {
					wd = filepath.FromSlash("./")
				}
				// copy log file to local directory
				target := filepath.Join(wd, logFileName)
				if err := os.Link(logFilePath, target); err != nil {
					return err
				}
				log.Infof("See %s for details", target)
			}

			// stop when max errors was reached
			numErrors++
			if maxErrors > 0 && numErrors >= maxErrors {
				return fmt.Errorf("max error limit reached, stopping")
			}

		} else {
			log.Infof("Done scenario #%d/%d with %s=0x%016x", round+1, maxRound, util.GORANDSEED, rnd)
		}

		// cleanup log file
		os.Remove(logFilePath)
	}

	return nil
}

func setup() (func(string, io.Writer) error, func(context.Context, string, io.Writer) error) {
	switch arch {
	case "wasm":
		log.Info("Running as WASM module")
		return buildTest, runTestInWasm
	default:
		log.Info("Running as native executable")
		return buildTest, runTestInNative
	}
}

func buildTest(out string, w io.Writer) error {
	goos := os.Getenv("GOOS")
	goarch := os.Getenv("GOARCH")
	defer func() {
		if goos != "" {
			os.Setenv("GOOS", goos)
		} else {
			os.Unsetenv("GOOS")
		}
		if goarch != "" {
			os.Setenv("GOARCH", goarch)
		} else {
			os.Unsetenv("GOARCH")
		}
	}()

	switch arch {
	case "wasm":
		os.Setenv("GOOS", "wasip1")
		os.Setenv("GOARCH", "wasm")
	case "native":
		os.Setenv("GOOS", runtime.GOOS)
		os.Setenv("GOARCH", runtime.GOARCH)
	}

	log.Infof("Building test for %s/%s", os.Getenv("GOOS"), os.Getenv("GOARCH"))
	args := []string{
		"test", "-c", "./internal/tests/scenarios",
		"-o", out,
		"-tags",
	}
	tags := "with_assert"
	if os.Getenv("GOARCH") == "wasm" {
		tags += ",faketime"
	}
	args = append(args, tags)
	if enableRace {
		args = append(args, "-race")
	}

	cmd := exec.Command("go", args...)
	log.Info(cmd)
	cmd.Stdout = w
	cmd.Stderr = w
	return cmd.Run()
}

func runTestInWasm(ctx context.Context, dir string, w io.Writer) error {
	args := []string{
		"run",
		"./internal/tests/wasm/runtime",
		"-vvv",
		"-module", filepath.Join(dir, "scenarios.test"),
		"-test.v",
		"-test.failfast",
		"-test.run=" + testrun,
	}

	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Stderr = w
	cmd.Stdout = w
	cmd.Env = append(cmd.Environ(), "KNOX_DRIVER=mem")
	log.Infof("Run %s", cmd)
	return cmd.Run()
}

func runTestInNative(ctx context.Context, dir string, w io.Writer) error {
	args := []string{
		"-test.v",
		"-test.failfast",
		"-test.cpu=" + strconv.Itoa(numCpu),
		"-test.count=1",
		"-test.run=" + testrun,
	}
	cmd := exec.Command(filepath.Join(dir, "scenarios.test"), args...)
	cmd.Stderr = w
	cmd.Stdout = w

	// set different process group to prevent forwarding signals sent to parent
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
		Pgid:    0,
	}

	log.Infof("Run %s", cmd)
	if err := cmd.Start(); err != nil {
		return err
	}

	// catch signals and send SIGABRT to child so we get a nice stack trace
	sigc := make(chan os.Signal, 1)
	errc := make(chan error, 1)

	go func() {
		errc <- cmd.Wait()
	}()

	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer signal.Stop(sigc)

	doAbort := false
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		log.Warn("Aborting child process after timeout")
		doAbort = true
	case s := <-sigc:
		log.Warnf("Aborting child process after %s", s)
		doAbort = true
	}

	if doAbort {
		err := cmd.Process.Signal(syscall.SIGABRT)
		if err != nil {
			log.Errorf("SIGABRT: %v", err)
		}
		return <-errc
	}
	return nil
}
