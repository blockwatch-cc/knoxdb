package wal

import (
	"errors"
	"io"
	"testing"
)

type TestCase struct {
	Name     string
	DataSize uint64
	Fn       func(*testing.T, *bufferedReader)
}

var bufferReadTestCases = []TestCase{
	{
		Name:     "can read a segment",
		DataSize: 50,
	},
	{
		Name:     "can read across two segments",
		DataSize: 100,
	},
	{
		Name:     "can read across 10 segments",
		DataSize: 1000,
	},
	{
		Name:     "cannot read after reading to end of segment",
		DataSize: 70,
		Fn: func(t *testing.T, b *bufferedReader) {
			_, err := b.Read(100)
			if err != nil {
				t.Errorf("failed to read: %v", err)
			}
			_, err = b.Read(30)
			if !errors.Is(err, io.EOF) {
				t.Errorf("reading end of segment should return EOF: %v", err)
			}
		},
	},
	{
		Name:     "cannot read after buffered reader is closed",
		DataSize: 50,
		Fn: func(t *testing.T, b *bufferedReader) {
			err := b.Close()
			if err != nil {
				t.Errorf("failed to read: %v", err)
			}
			_, err = b.Read(30)
			if errors.Is(err, ErrClosed) {
				t.Errorf("reading closed buffered reader was successful: %v", err)
			}
		},
	},
}
