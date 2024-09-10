package wal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWal(t *testing.T) {
	dir := t.TempDir()
	f, err := os.OpenFile(filepath.Join(dir, "test.txt"), os.O_CREATE|os.O_RDWR, 0644)
	require.NoError(t, err)
	defer f.Close()
	_, err = f.Write([]byte("Hello"))
	require.NoError(t, err)
}
