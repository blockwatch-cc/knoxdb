// Copyright (c) 2018 - 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package hash

import (
	"bytes"
	"encoding/hex"
	"fmt"
)

// HashSize of array used to store hashes.  See Hash.
const HashSize = 32

// MaxHashStringSize is the maximum length of a Hash hash string.
const MaxHashStringSize = HashSize * 2

// ErrHashStrSize describes an error that indicates the caller specified a hash
// string that has too many characters.
var ErrHashStrSize = fmt.Errorf("max hash string length is %v bytes", MaxHashStringSize)

// Hash is used in several of the bitcoin messages and common structures.  It
// typically represents the double sha256 of data.
type Hash [HashSize]byte

// String returns the Hash as the hexadecimal string of the byte-reversed
// hash.
func (hash Hash) String() string {
	for i := 0; i < HashSize/2; i++ {
		hash[i], hash[HashSize-1-i] = hash[HashSize-1-i], hash[i]
	}
	return hex.EncodeToString(hash[:])
}

func (hash Hash) MarshalText() ([]byte, error) {
	return []byte(hash.String()), nil
}

func (hash Hash) MarshalBinary() ([]byte, error) {
	return hash[:], nil
}

func (hash *Hash) UnmarshalBinary(buf []byte) error {
	if len(buf) != HashSize {
		return fmt.Errorf("invalid hash string length %d bytes", len(buf))
	}
	copy(hash[:], buf)
	return nil
}

// CloneBytes returns a copy of the bytes which represent the hash as a byte
// slice.
//
// NOTE: It is generally cheaper to just slice the hash directly thereby reusing
// the same bytes rather than calling this method.
func (hash Hash) CloneBytes() []byte {
	newHash := make([]byte, HashSize)
	copy(newHash, hash[:])

	return newHash
}

// SetBytes sets the bytes which represent the hash.  An error is returned if
// the number of bytes passed in is not HashSize.
func (hash *Hash) SetBytes(buf []byte) error {
	nhlen := len(buf)
	if nhlen != HashSize {
		return fmt.Errorf("invalid hash length of %v, want %v", nhlen,
			HashSize)
	}
	copy(hash[:], buf)

	return nil
}

func (hash Hash) IsValid() bool {
	return len(hash[:]) > 0
}

// IsEqual returns true if target is the same as hash.
func (hash Hash) IsEqual(target Hash) bool {
	return bytes.Compare(hash[:], target[:]) == 0
}

// NewHash returns a new Hash from a byte slice.  An error is returned if
// the number of bytes passed in is not HashSize.
func NewHash(buf []byte) (Hash, error) {
	var sh Hash
	err := sh.SetBytes(buf)
	if err != nil {
		return Hash{}, err
	}
	return sh, err
}

// NewHashFromStr creates a Hash from a hash string.  The string should be
// the hexadecimal string of a byte-reversed hash, but any missing characters
// result in zero padding at the end of the Hash.
func NewHashFromStr(hash string) (Hash, error) {
	var ret Hash
	err := ret.UnmarshalText([]byte(hash))
	if err != nil {
		return ret, err
	}
	return ret, nil
}

// Decode decodes the byte-reversed hexadecimal string encoding of a Hash to a
// destination.
func (h *Hash) UnmarshalText(buf []byte) error {
	// Return error if hash string is too long.
	if len(buf) > MaxHashStringSize {
		return ErrHashStrSize
	}

	// Hex decoder expects the hash to be a multiple of two.  When not, pad
	// with a leading zero.
	var srcBytes []byte
	if len(buf)%2 == 0 {
		srcBytes = buf
	} else {
		srcBytes = make([]byte, 1+len(buf))
		srcBytes[0] = '0'
		copy(srcBytes[1:], buf)
	}

	// Hex decode the source bytes to a temporary destination.
	var reversedHash Hash
	_, err := hex.Decode(reversedHash[HashSize-hex.DecodedLen(len(srcBytes)):], srcBytes)
	if err != nil {
		return err
	}

	// Reverse copy from the temporary hash to destination.  Because the
	// temporary was zeroed, the written result will be correctly padded.
	for i, b := range reversedHash[:HashSize/2] {
		h[i], h[HashSize-1-i] = reversedHash[HashSize-1-i], b
	}

	return nil
}
