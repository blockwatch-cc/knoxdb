package alp

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestCase struct {
	Name string
	File string
}

func TestAlpFloat64(t *testing.T) {
	for idx := range 40 {
		t.Run(fmt.Sprintf("float64_%d", idx), func(t *testing.T) {
			fd, err := os.ReadFile(path.Join("testdata", "float64", fmt.Sprintf("bw%d.csv", idx)))
			require.NoError(t, err)

			expectedNums := make([]float64, 0)
			lines := bytes.Split(fd, []byte("\n"))
			for _, line := range lines {
				l := string(line)
				if l == "" {
					continue
				}
				num, err := strconv.ParseFloat(l, 64)
				require.NoError(t, err)
				expectedNums = append(expectedNums, num)
			}

			e, err := Compress(expectedNums)
			require.NoError(t, err)

			actualValues, err := Decompress(e)
			require.NoError(t, err)

			assert.Equal(t, expectedNums, actualValues)
		})
	}
}

func TestAlpFloat32(t *testing.T) {
	for idx := range 27 {
		t.Run(fmt.Sprintf("float32_%d", idx), func(t *testing.T) {
			fd, err := os.ReadFile(path.Join("testdata", "float32", fmt.Sprintf("bw%d.csv", idx)))
			require.NoError(t, err)

			expectedNums := make([]float32, 0)
			lines := bytes.Split(fd, []byte("\n"))
			for _, line := range lines {
				l := string(line)
				if l == "" {
					continue
				}
				num, err := strconv.ParseFloat(l, 32)
				require.NoError(t, err)
				expectedNums = append(expectedNums, float32(num))
			}

			e, err := Compress(expectedNums)
			require.NoError(t, err)

			actualValues, err := Decompress(e)
			require.NoError(t, err)

			assert.Equal(t, expectedNums, actualValues)
		})
	}
}
