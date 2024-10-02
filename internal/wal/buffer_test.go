package wal

import (
	"bytes"
	"errors"
	"math/rand"
	"testing"
)

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

func TestBufferHasNextSegment(t *testing.T) {
	var wal, err = Create(WalOptions{
		MaxSegmentSize: 100,
		Path:           t.TempDir(),
	})
	if err != nil {
		t.Errorf("failed to create wal: %v", err)
	}
	defer wal.Close()

	bufferReader := newBufferedReader(wal)
	defer bufferReader.Close()

	data := make([]byte, 50)
	rand.Read(data)

	record := generateRecord(data)
	lsn, err := wal.Write(record)
	if err != nil {
		t.Errorf("failed to write wal: %v", err)
	}

	err = bufferReader.Seek(lsn)
	if err != nil {
		t.Errorf("failed to seek to LSN %d: %v", lsn, err)
	}

	if bufferReader.hasNextSegment() {
		t.Error("buffered reader should not have more segment because the record wrote less than its max size")
	}

	data = make([]byte, 500)
	record = generateRecord(data)
	_, err = wal.Write(record)
	if err != nil {
		t.Errorf("failed to write wal: %v", err)
	}

	if !bufferReader.hasNextSegment() {
		t.Error("buffered reader should have more segment because the record wrote more than max segments")
	}
}

func TestBufferNextSegment(t *testing.T) {
	var wal, err = Create(WalOptions{
		MaxSegmentSize: 100,
		Path:           t.TempDir(),
	})
	if err != nil {
		t.Errorf("failed to create wal: %v", err)
	}
	defer wal.Close()

	bufferReader := newBufferedReader(wal)

	data := make([]byte, 50)
	rand.Read(data)

	record := generateRecord(data)

	var lsn LSN
	for i := 0; i < 3; i++ {
		lsn, err = wal.Write(record)
		if err != nil {
			t.Errorf("failed to write wal: %v", err)
		}
	}

	err = bufferReader.Seek(lsn)
	if err != nil {
		t.Errorf("failed to seek to LSN %d: %v", lsn, err)
	}

	err = bufferReader.nextSegment()
	if err != nil {
		t.Errorf("failed to load next segment: %v", err)
	}

	bufferReader.Close()
	err = bufferReader.nextSegment()
	if !errors.Is(err, ErrClosed) {
		t.Errorf("buffered reader should be closed: %v", err)
	}
}

func TestBufferSeek(t *testing.T) {
	var wal, err = Create(WalOptions{
		MaxSegmentSize: 100,
		Path:           t.TempDir(),
	})
	if err != nil {
		t.Errorf("failed to create wal: %v", err)
	}
	defer wal.Close()

	bufferReader := newBufferedReader(wal)
	defer bufferReader.Close()

	data := make([]byte, 100)
	rand.Read(data)

	record := generateRecord(data)

	lsns := make([]LSN, 3)
	for i := 0; i < 3; i++ {
		lsn, err := wal.Write(record)
		if err != nil {
			t.Errorf("failed to write wal: %v", err)
		}
		lsns[i] = lsn
	}

	for _, lsn := range lsns {
		err = bufferReader.Seek(lsn)
		if err != nil {
			t.Errorf("failed to seek to LSN %d: %v", lsn, err)
		}
	}
}

func TestBufferSeekClosed(t *testing.T) {
	var wal, err = Create(WalOptions{
		MaxSegmentSize: 100,
		Path:           t.TempDir(),
	})
	if err != nil {
		t.Errorf("failed to create wal: %v", err)
	}
	defer wal.Close()

	bufferReader := newBufferedReader(wal)

	data := make([]byte, 100)
	rand.Read(data)

	record := generateRecord(data)

	lsns := make([]LSN, 3)
	for i := 0; i < 3; i++ {
		lsn, err := wal.Write(record)
		if err != nil {
			t.Errorf("failed to write wal: %v", err)
		}
		lsns[i] = lsn
	}

	err = bufferReader.Close()
	if err != nil {
		t.Errorf("failed to close buffer reader: %v", err)
	}

	lsn := lsns[2]
	err = bufferReader.Seek(lsn)
	if !errors.Is(err, ErrClosed) {
		t.Errorf("seeked to LSN %d: %v", lsn, err)
	}
}
