// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package fsst

import (
	"encoding/binary"
)

// the main compression function (everything automatic)
func Compress(strIn [][]uint8) []uint8 {
	e := NewEncoder(strIn, false)
	// to be faster than scalar, simd needs 64 lines or more of length >=12; or fewer lines, but big ones (totLen > 32KB)
	buf := make([]uint8, 0)
	exported := Export(e)
	buf = append(buf, exported...)

	compressed := _compress(e, strIn)
	buf = append(buf, compressed...)

	return buf
}

func _compress(e *Encoder, strIn [][]uint8) []uint8 {
	avoidBranch := false
	noSuffixOpt := false

	if 100*e.symbolTable.lenHisto[1] > 65*e.symbolTable.nSymbols && 100*e.symbolTable.suffixLim > 96*uint64(e.symbolTable.lenHisto[1]) {
		noSuffixOpt = true
	} else if e.symbolTable.lenHisto[0] > 24 && e.symbolTable.lenHisto[0] < 92 && (e.symbolTable.lenHisto[0] < 43 ||
		(e.symbolTable.lenHisto[6]+e.symbolTable.lenHisto[7]) < uint16(29)) && (e.symbolTable.lenHisto[0] < 72 || e.symbolTable.lenHisto[2] < 72) {
		avoidBranch = true
	}

	return _compressImpl(e, strIn, noSuffixOpt, avoidBranch)
}

func _compressImpl(e *Encoder, strIn [][]uint8, noSuffixOpt, avoidBranch bool) []uint8 {
	return _compressGeneral(e.symbolTable, strIn, noSuffixOpt, avoidBranch)
}

// optimized adaptive *scalar* compression method
func _compressGeneral(sym *SymbolTable, strIn [][]uint8, noSuffixOpt, avoidBranch bool) []uint8 {
	end := 0
	compressed := make([]uint8, 0)
	suffixLim := sym.suffixLim
	var zero uint16 = 0
	if sym.zeroTerminated {
		zero = 1
	}
	byteLim := uint8(sym.nSymbols + zero - sym.lenHisto[0])

	const bufLen = 511

	// three variants are possible. dead code falls away since the bool arguments are constants
	compressVariant := func(buf []byte, out []byte, noSuffixOpt, avoidBranch bool) int {
		nout := out
		var start, end = 0, len(buf)
		for start < end {
			word := binary.LittleEndian.Uint64(buf[start:])
			if word == 0 {
				break
			}
			code := sym.shortCodes[word&0xFFFF]
			if noSuffixOpt && (uint8(code) < uint8(suffixLim)) {
				// 2 byte code without having to worry about longer matches
				nout[0] = uint8(code)
				nout = nout[1:]
				start += 2
			} else {
				pos := word & 0xFFFFFF
				idx := FSSTHash(pos) & (HashTabSize - 1)
				s := sym.hashTab[idx]
				nout[1] = uint8(word) // speculatively write out escaped byte
				word &= (0xFFFFFFFFFFFFFFFF >> uint8(s.icl))
				if (s.icl < uint64(FSST_ICL_FREE)) && s.val.Uint64() == word {
					nout[0] = uint8(s.Code())
					nout = nout[1:]
					start += int(s.Len())
				} else if avoidBranch {
					// could be a 2-byte or 1-byte code, or miss
					// handle everything with predication
					nout[0] = uint8(code)
					inc := 1 + ((code & FSST_CODE_BASE) >> 8)
					nout = nout[inc:]
					start += int(code >> FSST_LEN_BITS)
				} else if uint8(code) < byteLim {
					// 2 byte code after checking there is no longer pattern
					nout[0] = uint8(code)
					nout = nout[1:]
					start += 2
				} else {
					// 1 byte code or miss.
					nout[0] = uint8(code)
					inc := 1 + ((code & FSST_CODE_BASE) >> 8)
					nout = nout[inc:] // predicated - tested with a branch, that was always worse
					start++
				}
			}
		}

		return len(out) - len(nout)
	}

	for _, curLine := range strIn {
		var curOff int = 0

		for {
			chunk := min(len(curLine)-curOff, bufLen) // we need to compress in chunks of 511 in order to be byte-compatible with simd-compressed FSST

			// +7 sentinel is to avoid 8-byte unaligned-loads going beyond 511 out-of-bounds
			buf := make([]byte, chunk+8) /* and initialize the sentinal bytes */

			// copy the string to the 511-byte buffer

			copy(buf, curLine[curOff:curOff+chunk])
			buf[chunk] = uint8(sym.terminator)
			end += chunk

			// based on symboltable stats, choose a variant that is nice to the branch predictor
			out := make([]uint8, len(buf)*2)
			var pos int
			if noSuffixOpt {
				pos = compressVariant(buf, out, true, false)
			} else if avoidBranch {
				pos = compressVariant(buf, out, false, true)
			} else {
				pos = compressVariant(buf, out, false, false)
			}
			compressed = append(compressed, out[:pos]...)

			curOff += chunk
			if curOff >= len(curLine) {
				break
			}
		}
	}
	return compressed
}
