// Copyright (c) 2018-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package dedup

import (
	"fmt"
	"io"
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

func ReadFrom(r io.Reader, arr ByteArray) (ByteArray, int64, error) {
	var b [1]byte
	_, err := r.Read(b[:])
	if err != nil {
		return nil, 0, err
	}
	if arr == nil {
		switch b[0] >> 4 {
		case bytesNativeFormat:
			arr = newNativeByteArray(0)
		case bytesCompactFormat:
			arr = newCompactByteArray(0, 0)
		case bytesFixedFormat:
			arr = newFixedByteArray(0, 0)
		case bytesDictFormat:
			arr = newDictByteArray(0, 0, 0)
		default:
			return nil, 0, fmt.Errorf("ByteArray: unknown encoding %d", b[0]>>4)
		}
	} else {
		switch b[0] >> 4 {
		case bytesNativeFormat:
			if _, ok := arr.(*NativeByteArray); !ok {
				arr = newNativeByteArray(0)
			} else {
				arr.Clear()
			}
		case bytesCompactFormat:
			if _, ok := arr.(*CompactByteArray); !ok {
				arr = newCompactByteArray(0, 0)
			} else {
				arr.Clear()
			}
		case bytesFixedFormat:
			if _, ok := arr.(*FixedByteArray); !ok {
				arr = newFixedByteArray(0, 0)
			} else {
				arr.Clear()
			}
		case bytesDictFormat:
			if _, ok := arr.(*CompactByteArray); !ok {
				arr = newDictByteArray(0, 0, 0)
			} else {
				arr.Clear()
			}
		default:
			return nil, 0, fmt.Errorf("ByteArray: unknown encoding %d", b[0]>>4)
		}
	}
	n, err := arr.ReadFrom(r)
	if err != nil {
		return nil, n + 1, err
	}
	return arr, n + 1, err
}

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
