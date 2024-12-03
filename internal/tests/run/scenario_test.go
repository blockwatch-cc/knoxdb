package run

import (
	"bytes"
	"context"
	"flag"
	"fmt"
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

func runTestInWasm(dirPath string) ([]byte, error) {
	cmdRuntime := exec.Command("go", "run", "../dst/runtime", "-vvv", "-module", filepath.Join(dirPath, "scenarios.test"))
	return cmdRuntime.CombinedOutput()
}

func runTestInNative(dirPath string) ([]byte, error) {
	cmdRuntime := exec.Command(filepath.Join(dirPath, "scenarios.test"))
	return cmdRuntime.CombinedOutput()
}

func setup() (func(*testing.T, string), func(string) ([]byte, error)) {
	switch sys {
	case "wasm":
		return buildTestInWasm, runTestInWasm
	default:
		return buildTest, runTestInNative
	}
}

func TestScenarios(t *testing.T) {
	require.NotEmpty(t, path, "environment vairable path 'LOGS_PATH' should not be empty")

	ctx := context.Background()
	dirPath := t.TempDir()

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

		shouldStop := false
		res, err := run(dirPath)
		if err != nil {
			shouldStop = true
		}
		t.Log(endInfo)

		if bytes.Contains(res, []byte("FAIL:")) {
			// buffer
			buf := bytes.NewBuffer(nil)
			_, err2 := buf.WriteString(startInfo)
			require.NoError(t, err2)
			_, err2 = buf.Write(res)
			require.NoError(t, err2)
			_, err2 = buf.WriteString(endInfo)
			require.NoError(t, err2)

			// store to file
			errname := fmt.Sprintf("%d_%d.log", rnd, time.Now().Unix())
			errpath := filepath.Join(path, errname)
			err2 = os.WriteFile(errpath, buf.Bytes(), 0644)
			require.NoError(t, err2)

			// upload
			if !skipUpload {
				_, err2 = s3.FPutObject(ctx, s3bucket, errname, errpath, minio.PutObjectOptions{})
				require.NoError(t, err2)
			}

			t.Log(string(res))
		}

		if shouldStop {
			require.NoError(t, err)
			break
		}
	}

	// reset
	os.Setenv(randomEnvKey, "")
}
