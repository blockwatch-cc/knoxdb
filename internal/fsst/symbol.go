// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package fsst

import (
	"encoding/binary"
	"fmt"

	"blockwatch.cc/knoxdb/pkg/assert"
	"github.com/echa/log"
)

// "symbols" are character sequences (up to 8 bytes)
// A symbol is compressed into a "code" of, in principle, one byte. But, we added an exception mechanism:
// byte 255 followed by byte X represents the single-byte symbol X. Its code is 256+X.

const (
	FSST_LEN_BITS  = 12
	FSST_CODE_BITS = 9
	FSST_CODE_BASE = 256                 // first 256 codes [0,255] are pseudo codes: escaped bytes
	FSST_CODE_MAX  = 1 << FSST_CODE_BITS // all bits set: indicating a symbol that has not been assigned a code yet
	FSST_CODE_MASK = FSST_CODE_MAX - 1   // all bits set: indicating a symbol that has not been assigned a code yet

	FSST_HASH_LOG2SIZE = 10
	FSST_HASH_PRIME    = 2971215073
	FSST_SHIFT         = 15

	SYMBOL_MAX_LENGTH = 8
)

type Val [SYMBOL_MAX_LENGTH]byte

func (v *Val) Uint64() uint64 {
	return binary.LittleEndian.Uint64(v[:])
}

func (v *Val) SetUint64(a uint64) {
	binary.LittleEndian.PutUint64(v[:], a)
}

type Symbol struct {
	// icl = u64 ignoredBits:16,code:12,length:4,unused:32 -- but we avoid exposing this bit-field notation
	icl uint64 // use a single u64 to be sure "code" is accessed with one load and can be compared with one comparison

	// the byte sequence that this symbol stands for
	val Val // usually we process it as a num(ber), as this is fast
}

// single-char symbol
func NewSymbol() *Symbol {
	return &Symbol{}
}

func (s *Symbol) WithCode(c uint8, code uint64) *Symbol {
	val := Val{}
	val.SetUint64(uint64(c))
	s.icl = uint64(1<<28) | uint64(code<<16) | 56
	s.val = val
	return s
}

func (s *Symbol) WithBuffer(buf []byte) *Symbol {
	val := Val{}
	le := len(buf)
	if le >= 8 {
		le = 8
		copy(val[:], buf[:8])
	} else {
		copy(val[:], buf)
	}
	s.val = val
	s.SetCodeLen(FSST_CODE_MAX, uint32(le))
	log.Debugf("sample -> %s", s)
	return s
}

func (s *Symbol) Len() uint32 {
	return uint32(s.icl >> 28)
}

func (s *Symbol) Code() uint16 {
	return uint16(s.icl >> 16 & FSST_CODE_MASK)
}

func (s *Symbol) IgnoreBits() uint32 {
	return uint32(s.icl)
}

func (s *Symbol) SetCodeLen(code uint32, len uint32) {
	s.icl = uint64((len << 28) | (code << 16) | ((8 - len) * 8))
}

func (s *Symbol) First() uint8 {
	assert.Always(s.Len() >= 1, "length should be greater than 1")
	return uint8(0xFF & s.val.Uint64())
}

func (s *Symbol) First2() uint16 {
	assert.Always(s.Len() >= 2, "length should be greater than 2")
	return uint16(0xFFFF & s.val.Uint64())
}

func (s *Symbol) Hash() uint64 {
	v := 0xFFFFFF & s.val.Uint64()
	return FSSTHash(v) // hash on the next 3 bytes
}

func (s *Symbol) Concat(s2 *Symbol) *Symbol {
	newS := &Symbol{}
	length := s.Len() + s2.Len()
	if length > SYMBOL_MAX_LENGTH {
		length = SYMBOL_MAX_LENGTH
	}
	newS.SetCodeLen(uint32(FSST_CODE_MASK), length)
	newS.val.SetUint64((s2.val.Uint64() << (8 * s.Len())) | s.val.Uint64())
	return newS
}

func (s *Symbol) String() string {
	return fmt.Sprintf("val => %q, hash => %x, icl => %d", s.val[:s.Len()], s.Hash(), s.icl)
}
