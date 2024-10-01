package wal

import (
	"bytes"
	"math/rand"
	"testing"

	"blockwatch.cc/knoxdb/internal/types"
)

func generateRecord(data []byte) *Record {
	return &Record{
		Type:   RecordTypeCommit,
		Tag:    types.ObjectTagDatabase,
		Entity: 100,
		Data:   data,
		TxID:   rand.Uint64(),
	}
}

func TestBufferRead(t *testing.T) {
	for _, tc := range bufferReadTestCases {
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
