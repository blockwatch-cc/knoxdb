package run

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

var (
	timeFmt      = "2006-01-02_15-04-05"
	sys          string // wasm, native
	maxErrors    int
	maxErrorFreq float64
)

func init() {
	flag.StringVar(&sys, "system", "wasm", "set os. wasm/native")
	flag.IntVar(&maxErrors, "max-errors", 1, "stop the test runner after N total observed errors")
	flag.Float64Var(&maxErrorFreq, "max-error-freq", math.Inf(0), "stops the test runner when the rate of errors observed per second is greater than N (inclusive)")
}

func buildTest(t *testing.T, dirPath string) {
	t.Helper()
	t.Logf("Building test for %s/%s", os.Getenv("GOOS"), os.Getenv("GOARCH"))
	tags := "with_assert"
	if os.Getenv("GOARCH") == "wasm" {
		tags += ",faketime"
	}
	cmdScenarios := exec.Command("go", "test", "-c", "../scenarios", "-o", dirPath, "-tags", tags)
	t.Log(cmdScenarios)
	out, err := cmdScenarios.CombinedOutput()
	if err != nil {
		t.Logf("Build: %v", err)
		t.Fatalf("Reason: %s", out)
	}
}

// buildTestInWasm
func buildTestInWasm(t *testing.T, dirPath string) {
	t.Helper()

	// set wasm
	goos := os.Getenv("GOOS")
	goarch := os.Getenv("GOARCH")
	t.Setenv("GOOS", "wasip1")
	t.Setenv("GOARCH", "wasm")

	buildTest(t, dirPath)

	// reset
	os.Setenv("GOOS", goos)
	os.Setenv("GOARCH", goarch)
}

func runTestInWasm(t *testing.T, dirPath string, w io.Writer) error {
	t.Helper()
	cmdRuntime := exec.Command("go", "run", "../wasm/runtime", "-vvv", "-module", filepath.Join(dirPath, "scenarios.test"), "-test.v", "-test.failfast")
	cmdRuntime.Stderr = w
	cmdRuntime.Stdout = w
	t.Log(cmdRuntime)
	return cmdRuntime.Run()
}

func runTestInNative(t *testing.T, dirPath string, w io.Writer) error {
	t.Helper()
	cmdRuntime := exec.Command(filepath.Join(dirPath, "scenarios.test"), "-test.v", "-test.count=10", "-test.failfast")
	cmdRuntime.Stderr = w
	cmdRuntime.Stdout = w
	t.Log(cmdRuntime)
	return cmdRuntime.Run()
}

func setup(t *testing.T) (func(*testing.T, string), func(*testing.T, string, io.Writer) error) {
	switch sys {
	case "wasm":
		t.Log("Running as WASM module...")
		return buildTestInWasm, runTestInWasm
	default:
		t.Log("Running as native executable...")
		return buildTest, runTestInNative
	}
}

func getEnv(n string) string {
	s := os.Getenv(n)
	if s == "" {
		s = "none"
	}
	return s
}

func LogBuildInfo(t *testing.T) {
	t.Helper()
	var hint string
	switch getEnv("DRONE_BUILD_EVENT") {
	case "pull_request":
		hint = fmt.Sprintf("%s/pull/%s", getEnv("DRONE_REPO_LINK"), getEnv("DRONE_PULL_REQUEST"))
	case "tag":
		hint = fmt.Sprintf("%s#%s", getEnv("DRONE_REPO_LINK"), getEnv("DRONE_TAG"))
	case "unknown":
		// non-drone execution
	default:
		hint = fmt.Sprintf("%s/commit/%s", getEnv("DRONE_REPO_LINK"), getEnv("DRONE_COMMIT"))
	}

	ts, _ := strconv.Atoi(getEnv("DRONE_BUILD_STARTED"))
	if ts == 0 {
		ts = int(time.Now().Unix())
	}

	t.Logf("Build ID      #%s %s", getEnv("DRONE_BUILD_NUMBER"), getEnv("DRONE_BUILD_LINK"))
	t.Logf("Build Date    %s", time.Unix(int64(ts), 0).UTC().Format(time.DateTime))
	t.Logf("Build System  %s", getEnv("DRONE_SYSTEM_HOST"))
	t.Logf("Build Target  %s/%s", getEnv("DRONE_STAGE_OS"), getEnv("DRONE_STAGE_ARCH"))
	t.Logf("Build Repo    %s %s", getEnv("DRONE_REPO"), hint)
	t.Logf("Build Branch  %s", getEnv("DRONE_REPO_BRANCH"))
	t.Logf("Build Commit  %s", getEnv("DRONE_COMMIT"))
	if len(defaultSeeds) > 0 {
		t.Logf("Build Seeds   %s", getEnv("DST_SEEDS"))
	}
	if !skipUpload {
		ref := getEnv("DRONE_COMMIT")
		t.Logf("Build Upload  %s/%s/%s", s3endpoint, s3bucket, ref[:min(len(ref), 6)])
	}
	t.Logf("Build Errors  max=%d max-rate=%f", maxErrors, maxErrorFreq)
}

func TestScenarios(t *testing.T) {
	// log some repo and build identity info
	LogBuildInfo(t)

	buildPath := t.TempDir()
	logPath := util.NonEmptyString(os.Getenv("LOGS_PATH"), buildPath)

	ctx := context.Background()

	// init s3
	s3, err := InitStorage(t)
	require.NoError(t, err)

	// build test
	build, run := setup(t)
	build(t, buildPath)

	// cleanup on panic/exit
	var f *os.File
	defer func() {
		if f != nil {
			f.Close()
		}
	}()

	var (
		round, numErrors int
		errLimit         = rate.NewLimiter(rate.Limit(maxErrorFreq), 60)
	)
	for {
		// generate random seed and run multiple iterations from 0...n
		rnd := util.RandUint64n(1 << 20)
		if len(defaultSeeds) > 0 {
			rnd = defaultSeeds[0]
			defaultSeeds = defaultSeeds[1:]
		}
		round++

		err := os.Setenv(util.GORANDSEED, strconv.FormatUint(rnd, 10))
		require.NoError(t, err)

		// create file
		now := time.Now().UTC()
		logFileName := fmt.Sprintf("%s_0x%016x.log", now.Format(timeFmt), rnd)
		logFilePath := filepath.Join(logPath, logFileName)
		f, err = os.Create(logFilePath)
		require.NoError(t, err)

		_, err = fmt.Fprintf(f, "--- Scenario #%d with %s=0x%016x\n", round, util.GORANDSEED, rnd)
		require.NoError(t, err)

		// run test in child process
		err = run(t, buildPath, f)

		if err != nil {
			_, err := fmt.Fprintf(f, "--- FAILED Scenario #%d with %s=0x%016x err=%v\n", round, util.GORANDSEED, rnd, err)
			require.NoError(t, err)
		} else {
			_, err := fmt.Fprintf(f, "--- DONE Scenario #%d with %s=0x%016x\n", round, util.GORANDSEED, rnd)
			require.NoError(t, err)
		}

		// close output file
		require.NoError(t, f.Close())
		f = nil

		// handle error
		if err != nil {
			if !errLimit.Allow() {
				t.Log("Stopping due to too high error frequency")
				break
			}

			t.Logf("FAIL Scenario #%d %s=0x%016x with %v", round, util.GORANDSEED, rnd, err)

			// upload
			if !skipUpload {
				logFileTarget := fmt.Sprintf("%s/%s", now.Format(time.DateOnly), logFileName)
				t.Logf("Uploading %s/%s/%s", s3endpoint, s3bucket, logFileTarget)
				_, err := s3.FPutObject(ctx, s3bucket, logFileTarget, logFilePath, minio.PutObjectOptions{})
				require.NoError(t, err)
			} else {
				if f, err = os.Open(logFilePath); err == nil {
					io.Copy(os.Stdout, f)
					f.Close()
				}
			}

			// stop when max errors was reached
			numErrors++
			if maxErrors > 0 && numErrors >= maxErrors {
				t.Log("Max error limit reached, stopping.")
				break
			}
		}

		// cleanup
		require.NoError(t, os.Remove(logFilePath))
	}
	if numErrors > 0 {
		t.Fail()
	}
}
