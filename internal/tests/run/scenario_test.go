package run

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
)

var (
	randomEnvKey = "GORANDSEED"

	sys          string // wasm, native
	maxError     uint64
	maxErrorFreq float64
)

func init() {
	flag.StringVar(&sys, "system", "wasm", "set os. wasm/native")
	flag.Uint64Var(&maxError, "max-errors", 0, "stop the test runner after N total observed errors")
	flag.Float64Var(&maxErrorFreq, "max-error-freq", 0, "stops the test runner when the rate of errors observed per second is greater than N (inclusive)")
}

func buildTest(t *testing.T, dirPath string) {
	cmdScenarios := exec.Command("go", "test", "-c", "../scenarios", "-o", dirPath)
	out, err := cmdScenarios.CombinedOutput()
	t.Logf("build output\n%s", out)
	require.NoError(t, err)
	require.Equal(t, "", string(out))
}

// buildTestInWasm
func buildTestInWasm(t *testing.T, dirPath string) {
	// set wasm
	goos := os.Getenv("GOOS")
	goarch := os.Getenv("GOARCH")
	os.Setenv("GOOS", "wasip1")
	os.Setenv("GOARCH", "wasm")

	buildTest(t, dirPath)

	// reset
	os.Setenv("GOOS", goos)
	os.Setenv("GOARCH", goarch)
}

func runTestInWasm(dirPath string, w io.Writer) error {
	cmdRuntime := exec.Command("go", "run", "../dst/runtime", "-vvv", "-module", filepath.Join(dirPath, "scenarios.test"))
	cmdRuntime.Stderr = w
	cmdRuntime.Stdout = w
	return cmdRuntime.Run()
}

func runTestInNative(dirPath string, w io.Writer) error {
	cmdRuntime := exec.Command(filepath.Join(dirPath, "scenarios.test"))
	cmdRuntime.Stderr = w
	cmdRuntime.Stdout = w
	return cmdRuntime.Run()
}

func setup() (func(*testing.T, string), func(string, io.Writer) error) {
	switch sys {
	case "wasm":
		return buildTestInWasm, runTestInWasm
	default:
		return buildTest, runTestInNative
	}
}

func TestScenarios(t *testing.T) {
	dirPath := t.TempDir()
	path = util.NonEmptyString(os.Getenv("LOGS_PATH"), dirPath)
	require.NotEmpty(t, path, "environment vairable 'LOGS_PATH' should not be empty")

	ctx := context.Background()

	t.Log("Loading s3 client")
	s3, err := LoadStorage()
	require.NoError(t, err)

	build, run := setup()

	t.Log("Building scenarios test cases")
	build(t, dirPath)

	var f *os.File
	defer func() {
		if f != nil {
			f.Close()
		}
	}()

	i := 0
	errsNum := uint64(0)
	now := time.Now()
	for {
		// generate random seed and run multiple iterations from 0...n
		var rnd uint64
		if sz := len(defaultIter); sz > 0 && sz != i {
			rnd = defaultIter[i]
		} else {
			rnd = util.RandUint64n(1 << 20)
		}
		i++

		startInfo := fmt.Sprintf("starting scenario iteration i = %d, using %s=%d \n", i, randomEnvKey, rnd)
		endInfo := fmt.Sprintf("completed scenario iteration i = %d, using %s=%d \n", i, randomEnvKey, rnd)

		err := os.Setenv(randomEnvKey, strconv.FormatUint(rnd, 10))
		require.NoError(t, err)

		// create file
		errname := fmt.Sprintf("%d_%d.log", rnd, time.Now().Unix())
		errpath := filepath.Join(path, errname)
		f, err = os.Create(errpath)
		require.NoError(t, err)

		_, err = f.WriteString(startInfo)
		require.NoError(t, err)

		err = run(dirPath, f)
		if err != nil {
			errsNum++

			errInfo := fmt.Sprintf("failed to run scenario: %v", err)
			t.Log(errInfo)

			_, err = f.WriteString(errInfo)
			require.NoError(t, err)

			_, err = f.WriteString(endInfo)
			require.NoError(t, err)

			err = f.Close()
			require.NoError(t, err)
			f = nil

			// upload
			if !skipUpload {
				_, err = s3.FPutObject(ctx, s3bucket, errname, errpath, minio.PutObjectOptions{})
				require.NoError(t, err)
			}
		}

		if f != nil {
			err = f.Close()
			require.NoError(t, err)
			f = nil
		}

		err = os.Remove(errpath)
		require.NoError(t, err)

		if maxError > 0 && errsNum >= maxError {
			break
		}

		errFreq := float64(errsNum) / time.Since(now).Seconds()
		if maxErrorFreq > 0 && errFreq >= maxErrorFreq {
			break
		}
	}
}
