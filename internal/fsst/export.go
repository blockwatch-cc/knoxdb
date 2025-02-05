// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package fsst

import (
	"encoding/binary"
)

const FSST_ENDIAN_MARKER = 1

func Export(e *Encoder, buf []byte) uint64 {
	// In ->version there is a versionnr, but we hide also suffixLim/terminator/nSymbols there.
	// This is sufficient in principle to *reconstruct* a fsst_encoder_t from a fsst_decoder_t
	// (such functionality could be useful to append compressed data to an existing block).
	//
	// However, the hash function in the encoder hash table is endian-sensitive, and given its
	// 'lossy perfect' hashing scheme is *unable* to contain other-endian-produced symbol tables.
	// Doing a endian-conversion during hashing will be slow and self-defeating.
	//
	// Overall, we could support reconstructing an encoder for incremental compression, but
	// should enforce equal-endianness. Bit of a bummer. Not going there now.
	//
	// The version field is now there just for future-proofness, but not used yet

	// version allows keeping track of fsst versions, track endianness, and encoder reconstruction
	var version uint64 = (FSST_VERSION << 32) | // version is 24 bits, most significant byte is 0
		(e.symbolTable.suffixLim << 24) |
		(uint64(e.symbolTable.terminator) << 16) |
		(uint64(e.symbolTable.nSymbols) << 8) |
		FSST_ENDIAN_MARKER // least significant byte is nonzero

	binary.LittleEndian.PutUint64(buf[3:11], version)
	buf[11] = byte(e.symbolTable.terminator)

	for i := 0; i < 8; i++ {
		buf[12+i] = byte(e.symbolTable.lenHisto[i])
	}

	// emit only the used bytes of the symbols
	var zero uint16 = 0
	if e.symbolTable.zeroTerminated {
		zero = 1
	}

	pos := uint32(20)
	for i := zero; i < e.symbolTable.nSymbols; i++ {
		symbol := e.symbolTable.symbols[i]
		copy(buf[pos:], symbol.val[:symbol.Len()])
		pos += symbol.Len()
	}

	return uint64(pos)
}
