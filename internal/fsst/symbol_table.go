// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package fsst

import (
	"container/heap"
	"encoding/binary"

	"blockwatch.cc/knoxdb/pkg/assert"
	"github.com/echa/log"
)

// we construct FSST symbol tables using a random sample of about 16KB (1<<14)
const FSST_SAMPLETARGET = 1 << 14
const FSST_SAMPLEMAXSZ = 2 * FSST_SAMPLETARGET
const FSST_SAMPLELINE = 512

// two phases of compression, before and after optimize():
//
// (1) to encode values we probe (and maintain) three datastructures:
// - u16 byteCodes[256] array at the position of the next byte  (s.length==1)
// - u16 shortCodes[65536] array at the position of the next twobyte pattern (s.length==2)
// - Symbol hashtable[1024] (keyed by the next three bytes, ie for s.length>2),
// this search will yield a u16 code, it points into Symbol symbols[]. You always find a hit, because the first 256 codes are
// pseudo codes representing a single byte these will become escapes)
//
// (2) when we finished looking for the best symbol table we call optimize() to reshape it:
// - it renumbers the codes by length (first symbols of length 2,3,4,5,6,7,8; then 1 (starting from byteLim are symbols of length 1)
//   length 2 codes for which no longer suffix symbol exists (< suffixLim) come first among the 2-byte codes
//   (allows shortcut during compression)
// - for each two-byte combination, in all unused slots of shortCodes[], it enters the byteCode[] of the symbol corresponding
//   to the first byte (if such a single-byte symbol exists). This allows us to just probe the next two bytes (if there is only one
//   byte left in the string, there is still a terminator-byte added during compression) in shortCodes[]. That is, byteCodes[]
//   and its codepath is no longer required. This makes compression faster. The reason we use byteCodes[] during symbolTable construction
//   is that adding a new code/symbol is expensive (you have to touch shortCodes[] in 256 places). This optimization was
//   hence added to make symbolTable construction faster.
//
// this final layout allows for the fastest compression code, only currently present in compressBulk

// in the hash table, the icl field contains (low-to-high) ignoredBits:16,code:12,length:4
// high bits of icl (len=8,code=FSST_CODE_MASK) indicates free bucket
const FSST_ICL_FREE = (15 << 28) | (FSST_CODE_MASK << 16)

// ignoredBits is (8-length)*8, which is the amount of high bits to zero in the input word before comparing with the hashtable key
//             ..it could of course be computed from len during lookup, but storing it precomputed in some loose bits is faster
//
// the gain field is only used in the symbol queue that sorts symbols on gain

// HashTabSize represent smallest size that incurs no precision loss
const HashTabSize = 1 << FSST_HASH_LOG2SIZE

type SymbolTable struct {
	// lookup table using the next two bytes (65536 codes), or just the next single byte
	// contains code for 2-byte symbol, otherwise code for pseudo byte (escaped byte)
	shortCodes [65536]uint16

	// lookup table (only used during symbolTable construction, not during normal text compression)
	// contains code for every 1-byte symbol, otherwise code for pseudo byte (escaped byte)
	byteCodes [256]uint16

	// 'symbols' is the current symbol  table symbol[code].symbol is the max 8-byte 'symbol' for single-byte 'code'
	symbols [FSST_CODE_MAX]*Symbol // x in [0,255]: pseudo symbols representing escaped byte x; x in [FSST_CODE_BASE=256,256+nSymbols]: real symbols

	// replicate long symbols in hashTab (avoid indirection).
	hashTab [HashTabSize]*Symbol // used for all symbols of 3 and more bytes

	// amount of symbols in the map (max 255)
	nSymbols uint16

	// codes higher than this do not have a longer suffix
	suffixLim uint64

	// code of 1-byte symbol, that can be used as a terminator during compression
	terminator uint16

	// whether we are expecting zero-terminated strings (we then also produce zero-terminated compressed strings)
	zeroTerminated bool

	// lenHisto[x] is the amount of symbols of byte-length (x+1) in this SymbolTable
	lenHisto [FSST_CODE_BITS]uint16
}

func NewSymbolTable() *SymbolTable {
	// stuff done once at startup
	symbolTable := &SymbolTable{
		suffixLim:      uint64(FSST_CODE_MAX),
		nSymbols:       0,
		terminator:     0,
		zeroTerminated: false,
	}

	for i := 0; i < 256; i++ {
		symbolTable.symbols[i] = NewSymbol().
			WithCode(uint8(i), uint64(i|(1<<FSST_LEN_BITS))) // pseudo symbols
	}

	unused := NewSymbol().
		WithCode(0, uint64(FSST_CODE_MASK)) // single-char symbol, exception code
	for i := 256; i < FSST_CODE_MAX; i++ {
		symbolTable.symbols[i] = unused // we start with all symbols unused
	}

	// empty hash table
	var s Symbol
	s.val.SetUint64(0)
	s.icl = uint64(FSST_ICL_FREE) // marks empty in hashtab

	for i := 0; i < HashTabSize; i++ {
		symbolTable.hashTab[i] = &s
	}

	// fill byteCodes[] with the pseudo code all bytes (escaped bytes)
	for i := 0; i < 256; i++ {
		symbolTable.byteCodes[i] = uint16((1 << FSST_LEN_BITS) | i)
	}

	// fill shortCodes[] with the pseudo code for the first byte of each two-byte pattern
	for i := 0; i < 65536; i++ {
		symbolTable.shortCodes[i] = uint16((1 << FSST_LEN_BITS) | (i & 255))
	}

	return symbolTable
}

func (s *SymbolTable) Clear() {
	// clear a symbolTable with minimal effort (only erase the used positions in it)
	s.lenHisto = [FSST_CODE_BITS]uint16{} // all unused
	for i := uint32(FSST_CODE_BASE); i < uint32(FSST_CODE_BASE+s.nSymbols); i++ {
		if s.symbols[i].Len() == 1 {
			val := s.symbols[i].First()
			s.byteCodes[val] = (1 << FSST_LEN_BITS) | uint16(val)
		} else if s.symbols[i].Len() == 2 {
			val := s.symbols[i].First2()
			s.shortCodes[val] = (1 << FSST_LEN_BITS) | (val & 255)
		} else {
			idx := s.symbols[i].Hash() & (HashTabSize - 1)
			s.hashTab[idx].val.SetUint64(0)
			s.hashTab[idx].icl = FSST_ICL_FREE // marks empty in hashtab
		}
	}
	s.nSymbols = 0 // no need to clean symbols[] as no symbols are used
}

func (sym *SymbolTable) HashInsert(s *Symbol) bool {
	idx := s.Hash() & (HashTabSize - 1)
	if taken := (sym.hashTab[idx].icl < FSST_ICL_FREE); taken {
		return false // collision in hash table
	}
	sym.hashTab[idx].icl = s.icl
	sym.hashTab[idx].val.SetUint64(s.val.Uint64() & (0xFFFFFFFFFFFFFFFF >> s.icl))
	return true
}

func (sym *SymbolTable) Add(s *Symbol) bool {
	assert.Always(FSST_CODE_BASE+sym.nSymbols < FSST_CODE_MAX, "FSST_CODE_MAX should be greater than FSST_CODE_BASE+nSymbols")
	len := s.Len()
	s.SetCodeLen(uint32(FSST_CODE_BASE+sym.nSymbols), len)

	if len == 1 {
		sym.byteCodes[s.First()] = FSST_CODE_BASE + sym.nSymbols + (1 << FSST_LEN_BITS) // len=1 (<<FSST_LEN_BITS)
	} else if len == 2 {
		sym.shortCodes[s.First2()] = FSST_CODE_BASE + sym.nSymbols + (2 << FSST_LEN_BITS) // len=2 (<<FSST_LEN_BITS)
	} else if !sym.HashInsert(s) {
		return false
	}

	sym.symbols[FSST_CODE_BASE+sym.nSymbols] = s
	sym.nSymbols++
	sym.lenHisto[len-1]++
	return true
}

// / Find longest expansion, return code (= position in symbol table)
func (sym *SymbolTable) FindLongestSymbol(s *Symbol) uint64 {
	idx := s.Hash() & (HashTabSize - 1)
	if sym.hashTab[idx].icl <= s.icl && sym.hashTab[idx].val.Uint64() == (s.val.Uint64()&(0xFFFFFFFFFFFFFFFF>>sym.hashTab[idx].icl)) {
		return (sym.hashTab[idx].icl >> 16) & FSST_CODE_MASK // matched a long symbol
	}
	if s.Len() >= 2 {
		code := sym.shortCodes[s.First2()] & FSST_CODE_MASK
		if code >= FSST_CODE_BASE {
			return uint64(code)
		}
	}
	return uint64(sym.byteCodes[s.First()]) & FSST_CODE_MASK
}

// rationale for finalize:
// - during symbol table construction, we may create more than 256 codes, but bring it down to max 255 in the last makeTable()
//   consequently we needed more than 8 bits during symbol table contruction, but can simplify the codes to single bytes in finalize()
//   (this feature is in fact lo longer used, but could still be exploited: symbol construction creates no more than 255 symbols in each pass)
// - we not only reduce the amount of codes to <255, but also *reorder* the symbols and renumber their codes, for higher compression perf.
//   we renumber codes so they are grouped by length, to allow optimized scalar string compression (byteLim and suffixLim optimizations).
// - we make the use of byteCode[] no longer necessary by inserting single-byte codes in the free spots of shortCodes[]
//   Using shortCodes[] only makes compression faster. When creating the symbolTable, however, using shortCodes[] for the single-byte
//   symbols is slow, as each insert touches 256 positions in it. This optimization was added when optimizing symbolTable construction time.
//
// In all, we change the layout and coding, as follows..
//
// before finalize():
// - The real symbols are symbols[256..256+nSymbols>. As we may have nSymbols > 255
// - The first 256 codes are pseudo symbols (all escaped bytes)
//
// after finalize():
// - table layout is symbols[0..nSymbols>, with nSymbols < 256.
// - Real codes are [0,nSymbols>. 8-th bit not set.
// - Escapes in shortCodes have the 8th bit set (value: 256+255=511). 255 because the code to be emitted is the escape byte 255
// - symbols are grouped by length: 2,3,4,5,6,7,8, then 1 (single-byte codes last)
// the two-byte codes are split in two sections:
// - first section contains codes for symbols for which there is no longer symbol (no suffix). It allows an early-out during compression
//
// finally, shortCodes[] is modified to also encode all single-byte symbols (hence byteCodes[] is not required on a critical path anymore).

func (sym *SymbolTable) Finalize(zeroTerminated uint8) {
	assert.Always(sym.nSymbols <= uint16(255), "number of symbols should be less or equal to 255")
	var (
		newCode [256]uint8
		rsum    [8]uint8
	)
	byteLim := sym.nSymbols - (sym.lenHisto[0] - uint16(zeroTerminated))

	// compute running sum of code lengths (starting offsets for each length)
	rsum[0] = uint8(byteLim) // 1-byte codes are highest
	rsum[1] = uint8(zeroTerminated)
	for i := 1; i < 7; i++ {
		rsum[i+1] = rsum[i] + uint8(sym.lenHisto[i])
	}

	// determine the new code for each symbol, ordered by length (and splitting 2byte symbols into two classes around suffixLim)
	newCode[0] = 0
	sym.suffixLim = uint64(rsum[1])
	sym.symbols[newCode[0]] = sym.symbols[256] // keep symbol 0 in place (for zeroTerminated cases only)

	for i := uint32(zeroTerminated); i < uint32(sym.nSymbols); i++ {
		s1 := sym.symbols[FSST_CODE_BASE+i]
		len := s1.Len()
		opt := uint32(0)
		if len == 2 {
			opt = uint32(sym.nSymbols)
		}

		if opt > 0 {
			first2 := s1.First2()
			for k := 0; k < int(opt); k++ {
				s2 := sym.symbols[FSST_CODE_BASE+k]
				if k != int(i) && s2.Len() > 1 && first2 == s2.First2() { // test if symbol k is a suffix of s
					opt = 0
					break
				}
			}
			if opt > 0 {
				newCode[i] = uint8(sym.suffixLim)
				sym.suffixLim++
			} else {
				rsum[2]--
				newCode[i] = rsum[2]
			}
		} else {
			newCode[i] = rsum[len-1]
			rsum[len-1]++
		}
		s1.SetCodeLen(uint32(newCode[i]), len)
		sym.symbols[newCode[i]] = s1
	}

	// renumber the codes in byteCodes[]
	for i := 0; i < 256; i++ {
		if (sym.byteCodes[i] & FSST_CODE_MASK) >= FSST_CODE_BASE {
			sym.byteCodes[i] = uint16(newCode[uint8(sym.byteCodes[i])]) + (1 << FSST_LEN_BITS)
		} else {
			sym.byteCodes[i] = 511 + (1 << FSST_LEN_BITS)
		}
	}

	// renumber the codes in shortCodes[]
	for i := 0; i < 65536; i++ {
		if (sym.shortCodes[i] & FSST_CODE_MASK) >= FSST_CODE_BASE {
			sym.shortCodes[i] = uint16(newCode[uint8(sym.shortCodes[i])]) + (sym.shortCodes[i] & (15 << FSST_LEN_BITS))
		} else {
			sym.shortCodes[i] = sym.byteCodes[i&0xFF]
		}
	}

	// replace the symbols in the hash table
	for i := 0; i < HashTabSize; i++ {
		if sym.hashTab[i].icl < FSST_ICL_FREE {
			sym.hashTab[i] = sym.symbols[newCode[uint8(sym.hashTab[i].Code())]]
		}
	}
}

func buildSymbolTable(encoder *Encoder, sample [][]uint8, zeroTerminated bool) *SymbolTable {
	counters := encoder.counter
	bestTable, st := NewSymbolTable(), NewSymbolTable()

	bestGain := -FSST_SAMPLEMAXSZ // worst case (everything exception)
	sampleFrac := uint64(128)

	// start by determining the terminator. We use the (lowest) most infrequent byte as terminator
	st.zeroTerminated = zeroTerminated
	if zeroTerminated {
		st.terminator = 0 // except in case of zeroTerminated mode, then byte 0 is terminator regardless frequency
	} else {
		byteHisto := make(map[uint8]int)
		for _, line := range sample {
			for _, cur := range line {
				byteHisto[cur]++
			}
		}
		min := 0
		for k, v := range byteHisto {
			if v <= min {
				min = v
				st.terminator = uint16(k)
			}
		}
	}

	assert.Always(st.terminator != 256, "Terminator should not be '256'")

	// a random number between 0 and 128
	rnd128 := func(i uint64) uint64 {
		return 1 + (FSSTHash((i+1)*sampleFrac) & 127)
	}

	// compress sample, and compute (pair-)frequencies
	compressCount := func() int {
		gain := 0
		for lineIdx, line := range sample {
			if sampleFrac < 128 {
				// in earlier rounds (sampleFrac < 128) we skip data in the sample (reduces overall work ~2x)
				if rnd128(uint64(lineIdx)) > sampleFrac {
					continue
				}
			}
			cur := lineIdx
			end := len(line)
			start := cur
			if cur < end {
				start = cur
				code2 := 255
				code1 := st.FindLongestSymbol(NewSymbol().
					WithBuffer(line[cur:]))
				cur += int(st.symbols[code1].Len())
				code1Val := 0
				if isEscapeCode(code1) {
					code1Val = 1
				}
				gain += (int(st.symbols[code1].Len()) - (1 + code1Val))
				for {
					// count single symbol (i.e. an option is not extending it)
					counters.Count1Inc(uint32(code1))

					// as an alternative, consider just using the next byte..
					if st.symbols[code1].Len() != 1 {
						// .. but do not count single byte symbols doubly
						counters.Count1Inc(uint32(line[start]))
					}

					if cur == end {
						break
					}

					// now match a new symbol
					start = cur
					if cur < (end - 7) {
						word := binary.LittleEndian.Uint64(line[cur:])
						code := word & 0xFFFFFF
						idx := FSSTHash(code) & (HashTabSize - 1)
						var s *Symbol = st.hashTab[idx]
						code2 = int(st.shortCodes[(word&0xFFFF)] & FSST_CODE_MASK)
						word &= ((0xFFFFFFFFFFFFFFFF) >> uint8(s.icl))
						var c0Val, c1Val uint8 = 0, 0
						if s.icl < FSST_ICL_FREE {
							c0Val = 1
						}
						if s.val.Uint64() == word {
							c1Val = 1
						}
						if c0Val&c1Val > 0 {
							code2 = int(s.Code())
							cur += int(s.Len())
						} else if code2 >= FSST_CODE_BASE {
							cur += 2
						} else {
							code2 = int(st.byteCodes[word&0xFF] & FSST_CODE_MASK)
							cur += 1
						}
					} else {
						code2 = int(st.FindLongestSymbol(NewSymbol().WithBuffer(line[cur:])))
						cur += int(st.symbols[code2].Len())
					}

					// compute compressed output size
					code2Val := 0
					if isEscapeCode(uint64(code2)) {
						code2Val = 1
					}
					gain += int(cur-start) - (1 + code2Val)

					// now count the subsequent two symbols we encode as an extension codesibility
					if sampleFrac < 128 {
						// no need to count pairs in final round
						// consider the symbol that is the concatenation of the two last symbols
						counters.Count2Inc(uint32(code1), uint32(code2))

						// as an alternative, consider just extending with the next byte..
						if (cur - start) > 1 {
							// ..but do not count single byte extensions doubly
							counters.Count2Inc(uint32(code1), uint32(line[start]))
						}
						code1 = uint64(code2)
					}
				}
			}
		}
		return gain
	}

	makeTable := func(sym *SymbolTable, counter *Counter, isBest bool) {
		// hashmap of c (needed because we can generate duplicate candidates)
		cands := map[uint64]QSymbol{}

		// artificially make terminater the most frequent symbol so it gets included
		terminator := sym.terminator
		if sym.nSymbols > 0 {
			terminator = FSST_CODE_BASE
		}
		counter.Count1Set(uint32(terminator), 65535)

		addOrInc := func(s *Symbol, count uint64) {
			if count < (5*sampleFrac)/128 {
				return // improves both compression speed (less candidates), but also quality!!
			}
			var q QSymbol
			q.symbol = s
			q.gain = count * uint64(s.Len())
			qHash := q.Hash()
			if cand, ok := cands[qHash]; ok {
				q.gain += cand.gain
				delete(cands, qHash)
			}
			cands[q.Hash()] = q
		}

		// add candidate symbols based on counted frequency
		for pos1 := uint32(0); pos1 < uint32(FSST_CODE_BASE+sym.nSymbols); pos1++ {
			var cnt1 uint32
			cnt1, pos1 = counters.Count1GetNext(uint32(pos1))
			if cnt1 <= 0 {
				continue
			}

			// heuristic: promoting single-byte symbols (*8) helps reduce exception rates and increases [de]compression speed
			s1 := sym.symbols[pos1]
			cnt := 1
			if s1.Len() == 1 {
				cnt = 8
			}
			addOrInc(s1, uint64(cnt)*uint64(cnt1))

			// multi-byte symbols cannot contain the terminator byte
			if sampleFrac >= 128 || // last round we do not create new (combined) symbols
				s1.Len() == SYMBOL_MAX_LENGTH || // symbol cannot be extended
				uint16(s1.val[0]) == sym.terminator { // multi-byte symbols cannot contain the terminator byte
				continue
			}

			for pos2 := uint32(0); pos2 < uint32(FSST_CODE_BASE+sym.nSymbols); pos2++ {
				var cnt2 uint32
				cnt2, pos2 = counters.Count2GetNext(pos1, pos2) // may advance pos2!!
				if cnt2 == 0 {
					continue
				}

				// create a new symbol
				s2 := sym.symbols[pos2]
				s3 := s1.Concat(s2)
				if uint16(s2.val[0]) != sym.terminator { // multi-byte symbols cannot contain the terminator byte
					addOrInc(s3, uint64(cnt2))
				}
			}
		}

		// insert candidates into priority queue (by gain)
		pq := &QSymbolPriorityQueue{}
		heap.Init(pq)
		for _, cand := range cands {
			heap.Push(pq, cand)
		}

		// Create new symbol map using best candidates
		sym.Clear()
		for sym.nSymbols < 255 && pq.Len() != 0 {
			q := heap.Pop(pq)
			symbol := q.(QSymbol).symbol
			sym.Add(symbol)
			if isBest {
				encoder.stat.symbols = append(encoder.stat.symbols, q.(QSymbol).symbol)
				encoder.stat.symbolsSize += len(symbol.val)
			}
		}
		if len(encoder.stat.symbols) > 0 {
			encoder.stat.longestSymbol = encoder.stat.symbols[0]
		}
	}

	bestCounter := NewCounter()
	for sampleFrac = 8; true; sampleFrac += 30 {
		counters.Clear()
		gain := compressCount()
		if gain >= bestGain { // a new best solution!
			counters.Backup(bestCounter)
			*bestTable = *st
			bestGain = gain
		}
		if sampleFrac >= 128 {
			// we do 5 rounds (sampleFrac=8,38,68,98,128)
			break
		}
		makeTable(st, counters, false)
	}

	counters.Restore(bestCounter)
	makeTable(bestTable, counters, true)
	var zero uint8 = 0
	if zeroTerminated {
		zero = 1
	}
	bestTable.Finalize(zero) // renumber codes for more efficient compression
	return bestTable
}

// quickly select a uniformly random set of lines such that we have between [FSST_SAMPLETARGET,FSST_SAMPLEMAXSZ) string bytes
func makeSample(strIn [][]uint8) [][]uint8 {
	totSize := 0
	sample := make([][]uint8, 0)

	for _, v := range strIn {
		totSize += len(v)
	}

	if totSize < FSST_SAMPLETARGET {
		sample = append(sample, strIn...)
	} else {
		sampleRnd := FSSTHash(4637947)
		nBytes := 0

		for nBytes < FSST_SAMPLETARGET {
			// choose a non-empty line
			sampleRnd := FSSTHash(sampleRnd)
			nlines := uint64(len(strIn))
			linenr := sampleRnd % nlines
			for len(strIn[linenr]) == 0 {
				linenr++
				if linenr == uint64(nlines) {
					linenr = 0
				}
			}

			// choose a chunk
			chunks := 1 + (len(strIn[linenr]) / FSST_SAMPLELINE)
			sampleRnd = FSSTHash(sampleRnd)
			offset := FSST_SAMPLELINE * (int(sampleRnd) % chunks)

			// add the chunk to the sample
			len := min(len(strIn[linenr])-offset, FSST_SAMPLELINE)
			sample = append(sample, strIn[linenr][offset:offset+len])
			nBytes += len
		}
	}
	log.Tracef("Sample => %q", sample)
	return sample
}
