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

	sys string // wasm, native
)

func init() {
	flag.StringVar(&sys, "system", "wasm", "set os. wasm/native")
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

	i := 0
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
		t.Log(startInfo)

		err := os.Setenv(randomEnvKey, strconv.FormatUint(rnd, 10))
		require.NoError(t, err)

		// create file
		errname := fmt.Sprintf("%d_%d.log", rnd, time.Now().Unix())
		errpath := filepath.Join(path, errname)
		f, err := os.Create(errpath)
		require.NoError(t, err)

		err = run(dirPath, f)
		if err != nil {
			// upload
			if !skipUpload {
				_, err = s3.FPutObject(ctx, s3bucket, errname, errpath, minio.PutObjectOptions{})
				require.NoError(t, err)
			}
		}

		t.Log(endInfo)
	}
}
