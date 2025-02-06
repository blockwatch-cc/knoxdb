// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"bytes"
	"reflect"

	"blockwatch.cc/knoxdb/internal/hash/fnv"
	"blockwatch.cc/knoxdb/internal/query"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/assert"
)

type hashFunc func([]byte) []byte

func genHashKey64(buf []byte) []byte {
	// reuse buffer when large enough and overwrite the first 8 bytes with hash
	res := buf
	if cap(res) < 16 {
		res = make([]byte, 16)
	}
	res = res[:16]

	// generate hash
	hash := fnv.Sum64a(buf[:len(buf)-8])

	// copy pk from buffer tail
	copy(res[8:], buf[len(buf)-8:])

	// copy hash to buffer start
	LE.PutUint64(res, hash)

	return res
}

func genNoopKey(buf []byte) []byte {
	return buf
}

// expand byte, word, dword to quadword little endian keys
func makeKeyGen(sz int) func([]byte) []byte {
	switch sz {
	case 1, 2, 4:
		return func(buf []byte) []byte {
			// reuse buffer when large enough and overwrite the first 8 bytes with hash
			res := buf
			if cap(res) < 16 {
				res = make([]byte, 16)
			}
			res = res[:16]

			// copy pk to buffer end
			copy(res[8:], buf[len(buf)-8:])

			// expand integer to u64 at buffer start
			copy(res, buf[:len(buf)-8])

			return res
		}
	default:
		return genNoopKey
	}
}

func (idx *Index) hashFilterValue(f *query.Filter) []uint64 {
	// produce output hash (uint64) from field data encoded to wire format
	// use schema field encoding helper to translate Go types from query
	field := idx.convert.Schema().Fields()[0]
	buf := bytes.NewBuffer(nil)

	switch f.Mode {
	case types.FilterModeIn, types.FilterModeNotIn:
		// slice
		rval := reflect.ValueOf(f.Value)
		if rval.Kind() != reflect.Slice {
			return nil
		}
		res := make([]uint64, rval.Len())
		for i := range res {
			buf.Reset()
			_ = field.Encode(buf, rval.Index(i).Interface())
			res[i] = fnv.Sum64a(buf.Bytes())
		}
		return res
	case types.FilterModeEqual:
		// single
		_ = field.Encode(buf, f.Value)
		return []uint64{fnv.Sum64a(buf.Bytes())}
	default:
		// unreachable
		assert.Unreachable("invalid filter mode for pack hash query", "mode", f.Mode)
		return nil
	}
}
