// Copyright (c) 2018-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package dedup

import (
	"fmt"
)

const (
	bytesNativeFormat  = 0
	bytesFixedFormat   = 1
	bytesCompactFormat = 2
	bytesDictFormat    = 3
	bytesInvalidFormat = 4
)

var (
	errUnexpectedFormat = fmt.Errorf("ByteArray: Decode unexpected format")
	errInvalidLength    = fmt.Errorf("ByteArray: Decode invalid encoded length")
	errLengthOverflow   = fmt.Errorf("ByteArray: Decode length overflow")
	errShortBuffer      = fmt.Errorf("ByteArray: Decode short buffer")
)

var (
	bytesDecoderFunc = [...]func(b []byte, dst ByteArray, n int) (ByteArray, error){
		decodeNative,
		decodeFixed,
		decodeCompact,
		decodeDict,
		decodeInvalid,
	}
)

func Decode(buf []byte, dst ByteArray, n int) (ByteArray, error) {
	if len(buf) == 0 {
		// best for empty journals to use a native byte array here
		return NewByteArray(n), nil
	}

	encoding := buf[0] >> 4
	if encoding > bytesDictFormat {
		encoding = bytesInvalidFormat // invalid
	}

	return bytesDecoderFunc[encoding&3](buf, dst, n)
}

func decodeNative(buf []byte, dst ByteArray, n int) (ByteArray, error) {
	if _, ok := dst.(*NativeByteArray); !ok {
		dst = newNativeByteArray(n)
	} else {
		dst.Clear()
	}
	err := dst.Decode(buf)
	if err != nil {
		return nil, err
	}
	return dst, nil
}

func decodeFixed(buf []byte, dst ByteArray, n int) (ByteArray, error) {
	if _, ok := dst.(*FixedByteArray); !ok {
		dst = newFixedByteArray(0, n)
	} else {
		dst.Clear()
	}
	err := dst.Decode(buf)
	if err != nil {
		return nil, err
	}
	return dst, nil
}

func decodeCompact(buf []byte, dst ByteArray, n int) (ByteArray, error) {
	if _, ok := dst.(*CompactByteArray); !ok {
		dst = newCompactByteArray(0, n)
	} else {
		dst.Clear()
	}
	err := dst.Decode(buf)
	if err != nil {
		return nil, err
	}
	return dst, nil
}

func decodeDict(buf []byte, dst ByteArray, _ int) (ByteArray, error) {
	if _, ok := dst.(*DictByteArray); !ok {
		dst = &DictByteArray{}
	} else {
		dst.Clear()
	}
	err := dst.Decode(buf)
	if err != nil {
		return nil, err
	}
	return dst, nil
}

func decodeInvalid(b []byte, _ ByteArray, _ int) (ByteArray, error) {
	return newFixedByteArray(0, 0), fmt.Errorf("ByteArray: unknown encoding %v", b[0]>>4)
}
