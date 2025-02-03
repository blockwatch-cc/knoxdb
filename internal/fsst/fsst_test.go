// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package fsst

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type Test struct {
	Name           string
	Data           string
	CompressedFile string
}

var TestData = []Test{
	{
		Name:           "Small data",
		Data:           "data.txt",
		CompressedFile: "comp.txt",
	},
	{
		Name:           "Large Data",
		Data:           "data2.txt",
		CompressedFile: "comp2.txt",
	},
}

func TestFsst(t *testing.T) {
	for _, td := range TestData {
		t.Run(td.Name, func(t *testing.T) {
			data, err := os.ReadFile(path.Join("testdata", td.Data))
			require.NoError(t, err, "reading data file should not fail")

			compressedFile, err := os.ReadFile(path.Join("testdata", td.CompressedFile))

			t.Run("Compress", func(t *testing.T) {
				compressedRes := Compress([][]uint8{data})
				require.NoError(t, err, "reading compressed file should not fail")

				assert.Equal(t, compressedFile, compressedRes, "compressed response is not equal to expected data")
			})

			t.Run("Decompress", func(t *testing.T) {
				decRes, err := Decompress(compressedFile)
				require.NoError(t, err, "decompressing compressed file should not fail")

				assert.Equal(t, data, decRes, "decompressed response is not equal to expected data")
			})

		})
	}
}
