package wal

import (
	"bytes"
	"errors"
	"io"
	"math/rand"
	"testing"

	"blockwatch.cc/knoxdb/internal/types"
)

type TestCase struct {
	Name     string
	DataSize uint64
	Fn       func(*testing.T, *bufferedReader)
}

var testCases = []TestCase{
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
			if err == nil {
				t.Errorf("reading closed buffered reader was successful: %v", err)
			}
		},
	},
}

func generateRecord(data []byte) *Record {
	return &Record{
		Type:   RecordTypeCommit,
		Tag:    types.ObjectTagDatabase,
		Entity: 100,
		Data:   data,
		TxID:   rand.Uint64(),
	}
}

func TestBuffer(t *testing.T) {
	for _, tc := range testCases {
		var wal, err = Create(WalOptions{
			MaxSegmentSize: 100,
			Path:           t.TempDir(),
		})
		if err != nil {
			t.Errorf("failed to create wal: %v", err)
		}

		bufferReader := newBufferedReader(wal)
		data := make([]byte, tc.DataSize)
		rand.Read(data)

		record := generateRecord(data)
		_, err = wal.Write(record)
		if err != nil {
			t.Errorf("failed to write wal: %v", err)
		}

		t.Run(tc.Name, func(t *testing.T) {
			if tc.Fn != nil {
				tc.Fn(t, bufferReader)
				return
			}

			_, err = bufferReader.Read(30)
			if err != nil {
				t.Errorf("failed to read: %v", err)
			}
			d, err := bufferReader.Read(int(tc.DataSize))
			if err != nil {
				t.Errorf("failed to read: %v", err)
			}
			if !bytes.Equal(d, data) {
				if err != nil {
					t.Errorf("data is not equal: %v", err)
				}
			}
		})

		bufferReader.Close()
	}
}
