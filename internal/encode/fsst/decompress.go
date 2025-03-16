// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package fsst

import (
	"encoding/binary"
	"fmt"
	"math/bits"
)

const FSST_ESC = 255
const FSST_MEMBUF = 1 << 22
const FSST_VERSION = 20190218
const FSST_CORRUPT = 32774747032022883 /* 7-byte number in little endian containing "corrupt" */

type Decoder struct {
	zeroTerminated uint8       /* terminator is a single-byte code that does not appear in longer symbols */
	len            [255]uint8  /* len[x] is the byte-length of the symbol x (1 < len[x] <= 8). */
	symbol         [255]uint64 /* symbol[x] contains in LITTLE_ENDIAN the bytesequence that code x represents (0 <= x < 255). */
}

func NewDecoder(buf []uint8) (*Decoder, []byte, error) {
	dec := &Decoder{}
	var code, pos uint32 = 0, 17
	lenHisto := [8]uint8{}

	version := binary.LittleEndian.Uint64(buf[3:]) // first 3 bytes is discarded
	pos += 3
	if (version >> 32) != FSST_VERSION {
		return nil, nil, fmt.Errorf("unsupported version")
	}

	dec.zeroTerminated = buf[11] & 1 // version contains 8 bytes 3+8 = 11

	copy(lenHisto[:], buf[12:12+8])

	// in case of zero-terminated, first symbol is "" (zero always, may be overwritten)
	dec.len[0] = 1
	dec.symbol[0] = 0

	// we use lenHisto[0] as 1-byte symbol run length (at the end)
	code = uint32(dec.zeroTerminated)
	if dec.zeroTerminated > 0 {
		lenHisto[0]-- // if zeroTerminated, then symbol "" aka 1-byte code=0, is not stored at the end
	}

	// now get all symbols from the buffer
	for l := 1; l <= 8; l++ { /* l = 1,2,3,4,5,6,7,8 */
		for i := 0; i < int(lenHisto[(l&7)]); i++ { /* 1,2,3,4,5,6,7,0 */
			dec.len[code] = uint8((l & 7) + 1) /* len = 2,3,4,5,6,7,8,1  */
			dec.symbol[code] = 0
			data := make([]byte, 8)
			for j := 0; j < int(dec.len[code]); j++ {
				data[j] = buf[pos]
				pos++
			}
			dec.symbol[code] = binary.LittleEndian.Uint64(data) // note this enforces 'little endian' symbols
			code++
		}
	}

	if dec.zeroTerminated > 0 {
		lenHisto[0]++
	}

	// fill unused symbols with text "corrupt". Gives a chance to detect corrupted code sequences (if there are unused symbols).
	for code < 255 {
		dec.symbol[code] = FSST_CORRUPT
		dec.len[code] = 8
		code++
	}

	return dec, buf[pos:], nil
}

func Decompress(strIn []byte) ([]uint8, error) {
	size := _deserialize(strIn)

	decoder, strIn, err := NewDecoder(strIn)
	if err != nil {
		return nil, err
	}
	var posOut, posIn uint64 = 0, 0

	code := byte(0)
	length := decoder.len
	strOut := make([]byte, size)
	symbol := decoder.symbol
	lenIn := uint64(len(strIn))

	decode := func() {
		code = strIn[posIn]
		posIn++
		data := make([]byte, 8)
		binary.LittleEndian.PutUint64(data, symbol[code])
		copy(strOut[posOut:], data[:length[code]])
		posOut += uint64(length[code])
	}

	for posOut+32 <= size && posIn+4 <= lenIn {
		nextBlock := uint32(strIn[posIn]) |
			uint32(strIn[posIn+1])<<8 |
			uint32(strIn[posIn+2])<<16 |
			uint32(strIn[posIn+3])<<24

		// check for escape sequences
		escapeMask := (nextBlock & 0x80808080) & ((((^nextBlock) & 0x7F7F7F7F) + 0x7F7F7F7F) ^ 0x80808080)

		if escapeMask == 0 {
			for i := 0; i < 4; i++ {
				decode()
			}
		} else {
			firstEscapePos := uint64(bits.TrailingZeros32(escapeMask)) >> 3

			switch firstEscapePos {
			case 3:
				decode()
				fallthrough
			case 2:
				decode()
				fallthrough
			case 1:
				decode()
				fallthrough
			case 0:
				posIn += 2 /* decompress an escaped byte */
				strOut[posOut] = strIn[posIn-1]
				posOut++
			}
		}
	}

	if (posOut + 24) <= size { // handle the possibly 3 last bytes without a loop
		if (posIn + 2) <= lenIn {
			strOut[posOut] = strIn[posIn+1]
			if strIn[posIn] != FSST_ESC {
				decode()

				if strIn[posIn] != FSST_ESC {
					decode()
				} else {
					posIn += 2
					strOut[posOut] = strIn[posIn-1]
					posOut++
				}
			} else {
				posIn += 2
				posOut++
			}
		}
		if posIn < lenIn { // last code cannot be an escape
			decode()
		}
	}

	for posIn < lenIn {
		code = strIn[posIn]
		posIn++

		if code < FSST_ESC {
			symbolBytes := make([]byte, 8)
			binary.LittleEndian.PutUint64(symbolBytes, symbol[code])

			endWrite := posOut + uint64(length[code])
			if endWrite > size {
				endWrite = size
			}

			copy(strOut[posOut:endWrite], symbolBytes[:endWrite-posOut])
			posOut = endWrite
		} else {
			if posOut < size {
				strOut[posOut] = strIn[posIn]
			}
			posIn++
			posOut++
		}
	}

	if posOut >= size && (decoder.zeroTerminated&1) != 0 {
		strOut[size-1] = 0
	}

	return strOut[:posOut], nil
}
