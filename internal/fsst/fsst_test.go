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
	Name     string
	FileName string
}

var TestData = []Test{
	{
		Name:     "Small data",
		FileName: "data",
	},
	{
		Name:     "Large Data",
		FileName: "data2",
	},
	{
		Name:     "Numbers",
		FileName: "c_name",
	},
	{
		Name:     "Unicodes (chinese)",
		FileName: "chinese",
	},
	{
		Name:     "Urls",
		FileName: "urls",
	},
}

func TestFsst(t *testing.T) {
	for _, td := range TestData {
		t.Run(td.Name, func(t *testing.T) {
			data, err := os.ReadFile(path.Join("testdata", td.FileName))
			require.NoError(t, err, "reading data file should not fail")
			compressedFile, err := os.ReadFile(path.Join("testdata", td.FileName+"_comp"))
			require.NoError(t, err, "reading data file should not fail")

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
