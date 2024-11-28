package run

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"

	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/require"
)

var (
	randomEnvKey = "GORANDSEED"
)

func buildScenariosTests(t *testing.T, dirPath string) {
	// build go scenarios package using wasm/wasipi1
	goos := os.Getenv("GOOS")
	goarch := os.Getenv("GOARCH")
	os.Setenv("GOOS", "wasip1")
	os.Setenv("GOARCH", "wasm")
	cmdScenarios := exec.Command("go", "test", "-c", "../scenarios", "-o", "path", dirPath)
	out, err := cmdScenarios.CombinedOutput()
	require.NoError(t, err)
	require.Equal(t, "", string(out))

	// reset
	os.Setenv("GOOS", goos)
	os.Setenv("GOARCH", goarch)
}

func runTest(dirPath string) ([]byte, error) {
	cmdRuntime := exec.Command("go", "run", "../dst/runtime", "-vvv", "-module", filepath.Join(dirPath, "scenarios.test"))
	return cmdRuntime.CombinedOutput()
}

func TestScenarios(t *testing.T) {
	dirPath := t.TempDir()

	buildScenariosTests(t, dirPath)
	// generate random seed and run multiple iterations from 0...n
	iterations := util.RandUint64n(1 << 12)
	for i := range iterations {
		t.Logf("starting scenario iteration i = %d, using %s=%d \n", i, randomEnvKey, i)
		err := os.Setenv(randomEnvKey, strconv.FormatUint(i, 10))
		require.NoError(t, err)

		res, err := runTest(dirPath)
		if err != nil {
			t.Log(string(res))
		}
		t.Logf("completed scenario iteration i = %d, using %s=%d \n", i, randomEnvKey, i)
	}

	// reset
	os.Setenv(randomEnvKey, "")
}
