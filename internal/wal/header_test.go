// Copyright (c) 2024 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc

/*
This test covers various aspects of WAL header validation:

1. Header structure:
   - Defines a fixed-size header (HeaderSize constant)
   - Specifies offsets and sizes for each field in the header

2. Field extraction:
   - Implements extractField function to read values from the header

3. Checksum calculation and verification:
   - calculateChecksum computes an xxHash checksum of the header
   - verifyChecksum ensures the stored checksum matches the calculated one

4. Header Validation (checkHeader function):
   - Validates record type and object tag
   - Checks TxID (transaction ID) rules:
     * Non-zero for non-checkpoint records
     * Increasing values across records
   - Ensures the record fits within WAL size limits
   - Verifies the checksum

5. Test Cases (TestHeader function):
   - Covers various scenarios including:
     * Valid headers
     * Invalid record types and object tags
     * TxID edge cases (zero, non-increasing)
     * Size limit checks (maximum allowed, exceeding limits)
     * LSN (Log Sequence Number) handling (including overflow scenarios)
     * Checksum mismatches

6. Fuzzing (FuzzHeaderCheck function):
   - Generates random inputs to test the header validation
   - Includes checks for valid record types and object tags
   - Generates headers with calculated checksums
   - Logs error cases without failing the test

Usage of FuzzHeaderCheck:
The function uses go's built-in fuzzing framework. It can be run using the command:
go test -v -run=^$ -fuzz=FuzzHeaderCheck -fuzztime=1m

The fuzzer will generate random inputs for the header fields and test the checkHeader function with these inputs.
It logs any error cases found during the fuzzing process.
*/

package wal

import (
    "encoding/binary"
    "fmt"
    "math"
    "testing"

    "blockwatch.cc/knoxdb/internal/types"
    "github.com/cespare/xxhash/v2"
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
func extractField(header [HeaderSize]byte, field HeaderField) uint64 {
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

// calculateChecksum computes the checksum for the header.
func calculateChecksum(header [HeaderSize]byte) uint64 {
    h := xxhash.New()
    h.Write(header[:ChecksumField.offset])
    return h.Sum64()
}

// verifyChecksum checks if the stored checksum matches the calculated one.
func verifyChecksum(header [HeaderSize]byte) bool {
    storedChecksum := extractField(header, ChecksumField)
    calculatedChecksum := calculateChecksum(header)
    return storedChecksum == calculatedChecksum
}

// checkHeader performs sanity checks on the WAL record header.
func checkHeader(header [HeaderSize]byte, lastTxID uint64, currentLSN LSN, maxWalSize int64) error {
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
    totalRecordSize := int64(HeaderSize) + int64(dataLen)
    if currentLSN+LSN(totalRecordSize) > LSN(maxWalSize) {
        return fmt.Errorf("record would exceed max WAL size: currentLSN=%d, totalRecordSize=%d, maxWalSize=%d", currentLSN, totalRecordSize, maxWalSize)
    }

    if !verifyChecksum(header) {
        return fmt.Errorf("checksum mismatch")
    }

    return nil
}

// printHeader prints the contents of a WAL record header for debugging.
func printHeader(header [HeaderSize]byte) {
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
    return header
}

// Package-level variable for test cases
var headerTests = []struct {
    name        string
    header      [HeaderSize]byte
    lastTxID    uint64
    currentLSN  LSN
    maxWalSize  int64
    expectError bool
}{
    {
        name:        "Valid header",
        header:      generateValidHeader(RecordTypeInsert, types.ObjectTagDatabase, 1, 100, 1000),
        lastTxID:    50,
        currentLSN:  1000,
        maxWalSize:  1024 * 1024,
        expectError: false,
    },
    {
        name: "Invalid record type",
        header: func() [HeaderSize]byte {
            h := generateValidHeader(RecordTypeInsert, types.ObjectTagDatabase, 1, 100, 1000)
            h[TypeField.offset] = 255
            return h
        }(),
        lastTxID:    50,
        currentLSN:  1000,
        maxWalSize:  1024 * 1024,
        expectError: true,
    },
    {
        name: "Invalid object tag",
        header: func() [HeaderSize]byte {
            h := generateValidHeader(RecordTypeInsert, types.ObjectTagDatabase, 1, 100, 1000)
            h[TagField.offset] = 255
            return h
        }(),
        lastTxID:    50,
        currentLSN:  1000,
        maxWalSize:  1024 * 1024,
        expectError: true,
    },
    {
        name:        "Zero TxID for non-checkpoint",
        header:      generateValidHeader(RecordTypeInsert, types.ObjectTagDatabase, 1, 0, 1000),
        lastTxID:    50,
        currentLSN:  1000,
        maxWalSize:  1024 * 1024,
        expectError: true,
    },
    {
        name:        "Valid zero TxID for checkpoint",
        header:      generateValidHeader(RecordTypeCheckpoint, types.ObjectTagDatabase, 1, 0, 0),
        lastTxID:    50,
        currentLSN:  1000,
        maxWalSize:  1024 * 1024,
        expectError: false,
    },
    {
        name:        "TxID not increasing",
        header:      generateValidHeader(RecordTypeInsert, types.ObjectTagDatabase, 1, 25, 1000),
        lastTxID:    50,
        currentLSN:  1000,
        maxWalSize:  1024 * 1024,
        expectError: true,
    },
    {
        name:        "Maximum allowed record size",
        header:      generateValidHeader(RecordTypeInsert, types.ObjectTagDatabase, 1, 100, uint32(1024*1024-HeaderSize)),
        lastTxID:    50,
        currentLSN:  0,
        maxWalSize:  1024 * 1024,
        expectError: false,
    },
    {
        name:        "Record size exceeding maximum",
        header:      generateValidHeader(RecordTypeInsert, types.ObjectTagDatabase, 1, 100, uint32(1024*1024-HeaderSize+1)),
        lastTxID:    50,
        currentLSN:  0,
        maxWalSize:  1024 * 1024,
        expectError: true,
    },
    {
        name:        "Record exactly fills remaining WAL space",
        header:      generateValidHeader(RecordTypeInsert, types.ObjectTagDatabase, 1, 100, uint32(1024*1024-HeaderSize)),
        lastTxID:    50,
        currentLSN:  0,
        maxWalSize:  1024 * 1024,
        expectError: false,
    },
    {
        name:        "Record exceeds remaining WAL space by 1 byte",
        header:      generateValidHeader(RecordTypeInsert, types.ObjectTagDatabase, 1, 100, uint32(1024*1024-HeaderSize-1000+1)),
        lastTxID:    50,
        currentLSN:  1000,
        maxWalSize:  1024 * 1024,
        expectError: true,
    },
    {
        name:        "Maximum LSN value",
        header:      generateValidHeader(RecordTypeInsert, types.ObjectTagDatabase, 1, 100, 100),
        lastTxID:    50,
        currentLSN:  LSN(math.MaxInt64 - HeaderSize - 100),
        maxWalSize:  math.MaxInt64,
        expectError: false,
    },
    {
        name:        "LSN overflow",
        header:      generateValidHeader(RecordTypeInsert, types.ObjectTagDatabase, 1, 100, 101),
        lastTxID:    50,
        currentLSN:  LSN(math.MaxInt64 - HeaderSize - 100),
        maxWalSize:  math.MaxInt64,
        expectError: true,
    },
    {
        name: "Checksum mismatch",
        header: func() [HeaderSize]byte {
            h := generateValidHeader(RecordTypeInsert, types.ObjectTagDatabase, 1, 100, 1000)
            // Set an incorrect checksum
            binary.LittleEndian.PutUint64(h[ChecksumField.offset:], 12345) // Any value different from the correct checksum
            return h
        }(),
        lastTxID:    50,
        currentLSN:  1000,
        maxWalSize:  1024 * 1024,
        expectError: true,
    },
}

func TestHeader(t *testing.T) {
    for _, tt := range headerTests {
        t.Run(tt.name, func(t *testing.T) {
            if tt.name != "Checksum mismatch" {
                // Calculate and set checksum only for non-checksum mismatch tests
                checksum := calculateChecksum(tt.header)
                binary.LittleEndian.PutUint64(tt.header[ChecksumField.offset:], checksum)
            }

            err := checkHeader(tt.header, tt.lastTxID, tt.currentLSN, tt.maxWalSize)
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
    // Add seed inputs
    for _, tt := range headerTests {
        recordType := extractField(tt.header, TypeField)
        tag := extractField(tt.header, TagField)
        entityID := extractField(tt.header, EntityField)
        txID := extractField(tt.header, TxIDField)
        dataLen := extractField(tt.header, DataLenField)

        f.Add(byte(recordType), byte(tag), entityID, txID, uint32(dataLen), tt.lastTxID, uint64(tt.currentLSN), tt.maxWalSize)
    }

    var lastInterestingCase string
    f.Fuzz(func(t *testing.T, recordType byte, tag byte, entityID uint64, txID uint64, dataLen uint32, lastTxID uint64, currentLSNValue uint64, maxWalSize int64) {
        // Ensure recordType is valid
        if !RecordType(recordType).IsValid() {
            return // Invalid input, skip
        }

        // Ensure tag is valid
        if !types.ObjectTag(tag).IsValid() {
            return // Invalid input, skip
        }

        // Generate header
        header := generateValidHeader(RecordType(recordType), types.ObjectTag(tag), entityID, txID, dataLen)

        // Calculate and set checksum
        checksum := calculateChecksum(header)
        binary.LittleEndian.PutUint64(header[ChecksumField.offset:], checksum)

        currentLSN := LSN(currentLSNValue)

        // Perform the check
        err := checkHeader(header, lastTxID, currentLSN, maxWalSize)

        // Create a string representation of this case
        currentCase := fmt.Sprintf("RecordType=%d, Tag=%d, EntityID=%d, TxID=%d, DataLen=%d, LastTxID=%d, CurrentLSN=%d, MaxWalSize=%d",
            recordType, tag, entityID, txID, dataLen, lastTxID, currentLSN, maxWalSize)

        if err != nil && currentCase != lastInterestingCase {
            // Log the new interesting case
            fmt.Printf("\nNew interesting case found:\n")
            fmt.Printf("Input: %s\n", currentCase)
            fmt.Printf("Error: %v\n\n", err)

            lastInterestingCase = currentCase
        }
    })
}
