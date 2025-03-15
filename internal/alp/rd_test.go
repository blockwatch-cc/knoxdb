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

func TestAlpRDFloat64(t *testing.T) {
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

			s := RDCompress[float64, uint64](expectedNums)
			require.NoError(t, err)

			actualValues := RDDecompress[float64, uint64](s)
			require.NoError(t, err)

			assert.Equal(t, expectedNums, actualValues)
		})
	}
}

func TestAlpRDFloat32(t *testing.T) {
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

			s := RDCompress[float32, uint32](expectedNums)
			require.NoError(t, err)

			actualValues := RDDecompress[float32, uint32](s)
			require.NoError(t, err)

			assert.Equal(t, expectedNums, actualValues)
		})
	}
}
