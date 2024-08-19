// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package block

import (
	"fmt"
	"io"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/dedup"
	"blockwatch.cc/knoxdb/internal/zip"
	"blockwatch.cc/knoxdb/pkg/num"
)

// MaxStoredSize estimates the upper bound of space required to store
// a serialization of this block. The size hint is used to dimension
// encoder/decoder buffers which may help to avoid memcopy during processing.
func (b *Block) MaxStoredSize() int {
	l := b.len
	switch b.typ {
	case BlockInt64, BlockUint64:
		return zip.Int64EncodedSize(l)

	case BlockInt32, BlockUint32:
		return zip.Int32EncodedSize(l)

	case BlockInt16, BlockUint16:
		return zip.Int16EncodedSize(l)

	case BlockInt8, BlockUint8:
		return zip.Int8EncodedSize(l)

	case BlockTime:
		return zip.TimeEncodedSize(l)

	case BlockFloat64, BlockFloat32:
		return zip.FloatEncodedSize(l)

	case BlockBool:
		return zip.BitsetEncodedSize(b.Bool())

	case BlockString, BlockBytes:
		return b.Bytes().MaxEncodedSize()

	case BlockInt128:
		return zip.Int128EncodedSize(l)

	case BlockInt256:
		return zip.Int256EncodedSize(l)

	default:
		return 0
	}
}

// WriteTo writes a compressed version of the block's content to w.
// The compression method depends on data type and contents and is
// signalled in the first byte of the serialized data. No additional
// header or block type identifier is written. This is considered
// task of an outer framing protocol.
//
// The choice of io.Writer as target makes it possible to combine
// this method with different outer framing protocol, network transports,
// disk storage, etc. The outer protocol may also choose to transparently
// compress serialzed data with an entropy encoder.
func (b *Block) WriteTo(w io.Writer) (int64, error) {
	var (
		n   int
		err error
	)
	switch b.typ {
	case BlockInt64, BlockUint64:
		n, err = zip.EncodeUint64(b.Uint64().FullSlice(), w)

	case BlockInt32, BlockUint32:
		n, err = zip.EncodeUint32(b.Uint32().FullSlice(), w)

	case BlockInt16, BlockUint16:
		n, err = zip.EncodeUint16(b.Uint16().FullSlice(), w)

	case BlockInt8, BlockUint8:
		n, err = zip.EncodeUint8(b.Uint8().FullSlice(), w)

	case BlockTime:
		n, err = zip.EncodeTime(b.Int64().FullSlice(), w)

	case BlockBytes, BlockString:
		var n64 int64
		n64, err = (*(*dedup.ByteArray)(b.ptr)).WriteTo(w)
		n = int(n64)

	case BlockBool:
		n, err = zip.EncodeBitset((*bitset.Bitset)(b.ptr), w)

	case BlockFloat64:
		n, err = zip.EncodeFloat64(b.Float64().FullSlice(), w)

	case BlockFloat32:
		n, err = zip.EncodeFloat32(b.Float32().FullSlice(), w)

	case BlockInt128:
		n, err = zip.EncodeInt128(*(*num.Int128Stride)(b.ptr), w)

	case BlockInt256:
		n, err = zip.EncodeInt256(*(*num.Int256Stride)(b.ptr), w)

	default:
		err = fmt.Errorf("block: invalid data type %s (%[1]d)", b.typ)
	}

	if err == nil {
		b.dirty = false
	}
	return int64(n), err
}

// ReadFrom loads and decompresses block data from an io.Reader. It enables
// composition with stream decoders like snappy/lz4, but it requires scratch
// buffers because underlying decoders are block based.
func (b *Block) ReadFrom(r io.Reader) (n int64, err error) {
	switch b.typ {
	case BlockInt64, BlockUint64:
		b.len, n, err = zip.ReadUint64(b.Uint64().FullSlice(), r)

	case BlockInt32, BlockUint32:
		b.len, n, err = zip.ReadUint32(b.Uint32().FullSlice(), r)

	case BlockInt16, BlockUint16:
		b.len, n, err = zip.ReadUint16(b.Uint16().FullSlice(), r)

	case BlockInt8, BlockUint8:
		b.len, n, err = zip.ReadUint8(b.Uint8().FullSlice(), r)

	case BlockTime:
		b.len, n, err = zip.ReadTime(b.Int64().FullSlice(), r)

	case BlockString, BlockBytes:
		// can re-allocate a new dedup kind
		var arr dedup.ByteArray
		arr, n, err = dedup.ReadFrom(r, b.Bytes())
		if err != nil {
			return n, err
		}
		b.ptr = unsafe.Pointer(&arr)
		b.len = arr.Len()

	case BlockFloat64:
		b.len, n, err = zip.ReadFloat64(b.Float64().FullSlice(), r)

	case BlockFloat32:
		b.len, n, err = zip.ReadFloat32(b.Float32().FullSlice(), r)

	case BlockBool:
		n, err = zip.ReadBitset((*bitset.Bitset)(b.ptr), r)
		b.len = (*bitset.Bitset)(b.ptr).Len()

	case BlockInt128:
		n, err = zip.ReadInt128((*num.Int128Stride)(b.ptr), r)
		b.len = (*num.Int128Stride)(b.ptr).Len()

	case BlockInt256:
		n, err = zip.ReadInt256((*num.Int256Stride)(b.ptr), r)
		b.len = (*num.Int256Stride)(b.ptr).Len()

	default:
		err = fmt.Errorf("block: unsupported data type %s (%[1]d)", b.typ)
		b.len = 0
	}
	return
}

// Decode unpacks compressed data found in buf and replaces the block's content.
// This method is similar to ReadFrom(), however due to the block-based nature
// of most underlying decoders, Decode is faster because it can avoid allocating
// extra buffer space.
//
// Implemetation note: for performance reasons we should avoid passing pointer
// to slice *[]uint64 because the Go compiler will add extra checks to the
// generated ASM which makes slice value access 20-50% slower.
func (b *Block) Decode(buf []byte) (err error) {
	switch b.typ {
	case BlockInt64, BlockUint64:
		b.len, err = zip.DecodeUint64(b.Uint64().FullSlice(), buf)

	case BlockInt32, BlockUint32:
		b.len, err = zip.DecodeUint32(b.Uint32().FullSlice(), buf)

	case BlockInt16, BlockUint16:
		b.len, err = zip.DecodeUint16(b.Uint16().FullSlice(), buf)

	case BlockInt8, BlockUint8:
		b.len, err = zip.DecodeUint8(b.Uint8().FullSlice(), buf)

	case BlockTime:
		b.len, err = zip.DecodeTime(b.Int64().FullSlice(), buf)

	case BlockString, BlockBytes:
		// can re-allocate a new dedup kind
		var arr dedup.ByteArray
		arr, err = dedup.Decode(buf, b.Bytes(), b.Bytes().Len())
		if err != nil {
			return err
		}
		b.ptr = unsafe.Pointer(&arr)
		b.len = arr.Len()

	case BlockFloat64:
		b.len, err = zip.DecodeFloat64(b.Float64().FullSlice(), buf)

	case BlockFloat32:
		b.len, err = zip.DecodeFloat32(b.Float32().FullSlice(), buf)

	case BlockBool:
		err = zip.DecodeBitset((*bitset.Bitset)(b.ptr), buf)
		b.len = (*bitset.Bitset)(b.ptr).Len()

	case BlockInt128:
		err = zip.DecodeInt128((*num.Int128Stride)(b.ptr), buf)
		b.len = (*num.Int128Stride)(b.ptr).Len()

	case BlockInt256:
		err = zip.DecodeInt256((*num.Int256Stride)(b.ptr), buf)
		b.len = (*num.Int256Stride)(b.ptr).Len()

	default:
		err = fmt.Errorf("block: unsupported data type %s (%[1]d)", b.typ)
		b.len = 0
	}
	return
}
