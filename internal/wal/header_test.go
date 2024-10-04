// Copyright (c) 2024 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc

/*
The tests cover various aspects of WAL header validation, including:

1. Basic structure: Verifies the correct size and field placement within the header.

2. Record type validation: Tests for valid and invalid record types.

3. Object tag validation: Ensures only valid object tags are accepted.

4. TxID handling:
   - Checks for increasing TxID values
   - Verifies special cases like zero TxID for checkpoint records

5. Size limit enforcement:
   - Tests maximum allowed record sizes
   - Verifies behavior when record size exceeds WAL segment limits
   - Checks edge cases where records exactly fill or slightly exceed remaining space

6. LSN handling:
   - Tests various LSN values, including zero, maximum, and potential overflow scenarios

7. Checksum Verification: Ensures the checksum mechanism correctly identifies data integrity issues

8. Data Length Consistency: Verifies that the data length field in the header matches the actual data size
*/

package wal

import (
    "encoding/binary"
    "fmt"
    "hash/crc32"
    "math"
    "testing"

    "blockwatch.cc/knoxdb/internal/types"
)

// HeaderField represents the offset and size of each field in the header
type HeaderField struct {
    offset int
    size   int
}

// Header fields definition
var (
    TypeField     = HeaderField{0, 1}
    TagField      = HeaderField{1, 1}
    EntityField   = HeaderField{2, 8}
    TxIDField     = HeaderField{10, 8}
    DataLenField  = HeaderField{18, 4}
    ChecksumField = HeaderField{22, 8}
)

// extractField extracts a field from the header based on its definition.
func extractField(header []byte, field HeaderField) uint64 {
    switch field.size {
    case 1:
        return uint64(header[field.offset])
    case 4:
        return uint64(binary.LittleEndian.Uint32(header[field.offset : field.offset+field.size]))
    case 8:
        return binary.LittleEndian.Uint64(header[field.offset : field.offset+field.size])
    default:
        panic(fmt.Sprintf("Unsupported field size: %d", field.size))
    }
}

// calculateChecksum computes the checksum for the header and data.
func calculateChecksum(header []byte, data []byte) uint64 {
    h := crc32.NewIEEE()
    h.Write(header[:ChecksumField.offset])
    h.Write(data)
    return uint64(h.Sum32())
}

// verifyChecksum checks if the stored checksum matches the calculated one.
func verifyChecksum(header []byte, data []byte) bool {
    storedChecksum := extractField(header, ChecksumField)
    calculatedChecksum := calculateChecksum(header, data)
    return storedChecksum == calculatedChecksum
}

// checkHeader performs sanity checks on the WAL record header.
func checkHeader(header []byte, lastTxID uint64, currentLSN LSN, maxWalSize int64, data []byte) error {
    if len(header) != HeaderSize {
        return fmt.Errorf("invalid header size: got %d, want %d", len(header), HeaderSize)
    }

    recordType := RecordType(extractField(header, TypeField))
    if !recordType.IsValid() {
        return fmt.Errorf("invalid record type: %d", recordType)
    }

    tag := types.ObjectTag(extractField(header, TagField))
    if !tag.IsValid() {
        return fmt.Errorf("invalid object tag: %d", tag)
    }

    txID := extractField(header, TxIDField)
    if txID == 0 && recordType != RecordTypeCheckpoint {
        return fmt.Errorf("invalid TxID: 0 for non-checkpoint record")
    }
    if txID <= lastTxID && txID != 0 {
        return fmt.Errorf("TxID not increasing: current %d, last %d", txID, lastTxID)
    }

    dataLen := extractField(header, DataLenField)
    if int64(dataLen) != int64(len(data)) {
        return fmt.Errorf("data length mismatch: header claims %d, actual %d", dataLen, len(data))
    }
    totalRecordSize := int64(HeaderSize) + int64(dataLen)
    fmt.Printf("Debug: currentLSN=%d, totalRecordSize=%d, maxWalSize=%d, HeaderSize=%d\n", currentLSN, totalRecordSize, maxWalSize, HeaderSize)
    if currentLSN+LSN(totalRecordSize) > LSN(maxWalSize) {
        return fmt.Errorf("record would exceed max WAL size: currentLSN=%d, totalRecordSize=%d, maxWalSize=%d", currentLSN, totalRecordSize, maxWalSize)
    }

    if !verifyChecksum(header, data) {
        return fmt.Errorf("checksum mismatch")
    }

    return nil
}

// printHeader prints the contents of a WAL record header for debugging.
func printHeader(header []byte) {
    fmt.Printf("Type: %d\n", extractField(header, TypeField))
    fmt.Printf("Tag: %d\n", extractField(header, TagField))
    fmt.Printf("Entity: %d\n", extractField(header, EntityField))
    fmt.Printf("TxID: %d\n", extractField(header, TxIDField))
    fmt.Printf("Data Length: %d\n", extractField(header, DataLenField))
    fmt.Printf("Checksum: %d\n", extractField(header, ChecksumField))
}

// generateValidHeader creates a valid header for testing.
func generateValidHeader(recordType RecordType, tag types.ObjectTag, entity, txID uint64, dataLen uint32) [HeaderSize]byte {
    var header [HeaderSize]byte
    header[TypeField.offset] = byte(recordType)
    header[TagField.offset] = byte(tag)
    binary.LittleEndian.PutUint64(header[EntityField.offset:], entity)
    binary.LittleEndian.PutUint64(header[TxIDField.offset:], txID)
    binary.LittleEndian.PutUint32(header[DataLenField.offset:], dataLen)
    binary.LittleEndian.PutUint64(header[ChecksumField.offset:], 0)
    return header
}

func TestHeader(t *testing.T) {
    maxWalSize := int64(1024 * 1024)
    currentLSN := LSN(0)

    tests := []struct {
        name        string
        header      []byte
        data        []byte
        lastTxID    uint64
        currentLSN  LSN
        maxWalSize  int64
        expectError bool
    }{
        {
            name:        "Valid header",
            header:      generateValidHeader(RecordTypeInsert, types.ObjectTagDatabase, 1, 100, 1000, make([]byte, 1000)),
            data:        make([]byte, 1000),
            lastTxID:    50,
            currentLSN:  1000,
            maxWalSize:  maxWalSize,
            expectError: false,
        },
        {
            name: "Invalid record type",
            header: func() []byte {
                h := generateValidHeader(RecordTypeInsert, types.ObjectTagDatabase, 1, 100, 1000, make([]byte, 1000))
                h[TypeField.offset] = 255
                return h
            }(),
            data:        make([]byte, 1000),
            lastTxID:    50,
            currentLSN:  1000,
            maxWalSize:  maxWalSize,
            expectError: true,
        },
        {
            name: "Invalid object tag",
            header: func() []byte {
                h := generateValidHeader(RecordTypeInsert, types.ObjectTagDatabase, 1, 100, 1000, make([]byte, 1000))
                h[TagField.offset] = 255
                return h
            }(),
            data:        make([]byte, 1000),
            lastTxID:    50,
            currentLSN:  1000,
            maxWalSize:  maxWalSize,
            expectError: true,
        },
        {
            name:        "Zero TxID for non-checkpoint",
            header:      generateValidHeader(RecordTypeInsert, types.ObjectTagDatabase, 1, 0, 1000, make([]byte, 1000)),
            data:        make([]byte, 1000),
            lastTxID:    50,
            currentLSN:  1000,
            maxWalSize:  maxWalSize,
            expectError: true,
        },
        {
            name:        "Valid zero TxID for checkpoint",
            header:      generateValidHeader(RecordTypeCheckpoint, types.ObjectTagDatabase, 1, 0, 1000, make([]byte, 1000)),
            data:        make([]byte, 1000),
            lastTxID:    50,
            currentLSN:  1000,
            maxWalSize:  maxWalSize,
            expectError: false,
        },
        {
            name:        "TxID not increasing",
            header:      generateValidHeader(RecordTypeInsert, types.ObjectTagDatabase, 1, 25, 1000, make([]byte, 1000)),
            data:        make([]byte, 1000),
            lastTxID:    50,
            currentLSN:  1000,
            maxWalSize:  maxWalSize,
            expectError: true,
        },
        {
            name:        "Maximum allowed record size",
            header:      generateValidHeader(RecordTypeInsert, types.ObjectTagDatabase, 1, 100, uint32(maxWalSize-HeaderSize), make([]byte, maxWalSize-HeaderSize)),
            data:        make([]byte, maxWalSize-HeaderSize),
            lastTxID:    50,
            currentLSN:  0,
            maxWalSize:  maxWalSize,
            expectError: false,
        },
        {
            name:        "Record size exceeding maximum",
            header:      generateValidHeader(RecordTypeInsert, types.ObjectTagDatabase, 1, 100, uint32(maxWalSize-HeaderSize+1), make([]byte, maxWalSize-HeaderSize+1)),
            data:        make([]byte, maxWalSize-HeaderSize+1),
            lastTxID:    50,
            currentLSN:  0,
            maxWalSize:  maxWalSize,
            expectError: true,
        },
        {
            name:        "Record exactly fills remaining WAL space",
            header:      generateValidHeader(RecordTypeInsert, types.ObjectTagDatabase, 1, 100, uint32(maxWalSize-HeaderSize), make([]byte, maxWalSize-HeaderSize)),
            data:        make([]byte, maxWalSize-HeaderSize),
            lastTxID:    50,
            currentLSN:  LSN(0),
            maxWalSize:  maxWalSize,
            expectError: false,
        },
        {
            name:        "Record exceeds remaining WAL space by 1 byte",
            header:      generateValidHeader(RecordTypeInsert, types.ObjectTagDatabase, 1, 100, uint32(maxWalSize-HeaderSize-int64(currentLSN)+1), make([]byte, maxWalSize-HeaderSize-int64(currentLSN)+1)),
            data:        make([]byte, maxWalSize-HeaderSize-int64(currentLSN)+1),
            lastTxID:    50,
            currentLSN:  LSN(1000),
            maxWalSize:  maxWalSize,
            expectError: true,
        },
        {
            name:        "Maximum LSN value",
            header:      generateValidHeader(RecordTypeInsert, types.ObjectTagDatabase, 1, 100, 100, make([]byte, 100)),
            data:        make([]byte, 100),
            lastTxID:    50,
            currentLSN:  LSN(math.MaxInt64 - HeaderSize - 100),
            maxWalSize:  math.MaxInt64,
            expectError: false,
        },
        {
            name:        "LSN overflow",
            header:      generateValidHeader(RecordTypeInsert, types.ObjectTagDatabase, 1, 100, 101, make([]byte, 101)),
            data:        make([]byte, 101),
            lastTxID:    50,
            currentLSN:  LSN(math.MaxInt64 - HeaderSize - 100),
            maxWalSize:  math.MaxInt64,
            expectError: true,
        },
        {
            name: "Checksum mismatch",
            header: func() []byte {
                h := generateValidHeader(RecordTypeInsert, types.ObjectTagDatabase, 1, 100, 1000, make([]byte, 1000))
                // Directly modify the checksum field
                binary.LittleEndian.PutUint64(h[ChecksumField.offset:], 0)
                return h
            }(),
            data:        make([]byte, 1000),
            lastTxID:    50,
            currentLSN:  1000,
            maxWalSize:  maxWalSize,
            expectError: true,
        },
        {
            name:        "Zero LSN with maximum record size",
            header:      generateValidHeader(RecordTypeInsert, types.ObjectTagDatabase, 1, 100, uint32(maxWalSize-HeaderSize), make([]byte, maxWalSize-HeaderSize)),
            data:        make([]byte, maxWalSize-HeaderSize),
            lastTxID:    50,
            currentLSN:  0,
            maxWalSize:  maxWalSize,
            expectError: false,
        },
        {
            name:        "Data length mismatch",
            header:      generateValidHeader(RecordTypeInsert, types.ObjectTagDatabase, 1, 100, 1000, make([]byte, 1000)),
            data:        make([]byte, 999), // One byte short
            lastTxID:    50,
            currentLSN:  1000,
            maxWalSize:  maxWalSize,
            expectError: true,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            err := checkHeader(tt.header, tt.lastTxID, tt.currentLSN, tt.maxWalSize, tt.data)
            if (err != nil) != tt.expectError {
                t.Errorf("checkHeader() error = %v, expectError %v", err, tt.expectError)
            }

            // Print header for debugging
            fmt.Printf("Test case: %s\n", tt.name)
            printHeader(tt.header)
            if err != nil {
                fmt.Printf("Error: %v\n", err)
            }
            fmt.Println()
        })
    }
}

func FuzzHeaderCheck(f *testing.F) {
    // Add seed inputs based on existing tests
    f.Add(generateValidHeader(RecordTypeInsert, types.ObjectTagDatabase, 1, 100, 1000), make([]byte, 1000), uint64(50), uint64(1000), int64(1024*1024))
    f.Add(generateValidHeader(RecordTypeCheckpoint, types.ObjectTagDatabase, 1, 0, 1000), make([]byte, 1000), uint64(50), uint64(1000), int64(1024*1024))
    f.Add(generateValidHeader(RecordTypeInsert, types.ObjectTagDatabase, 1, 100, 1048546), make([]byte, 1048546), uint64(50), uint64(0), int64(1048576))

    f.Fuzz(func(t *testing.T, header [HeaderSize]byte, data []byte, lastTxID uint64, currentLSNValue uint64, maxWalSize int64) {
        currentLSN := LSN(currentLSNValue)
        err := checkHeader(header[:], lastTxID, currentLSN, maxWalSize, data)
        if err != nil {
            t.Logf("Error: %v", err)
            t.Fail()
        }
    })
}
