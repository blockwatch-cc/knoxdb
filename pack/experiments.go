// Copyright (c) 2018-2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package pack

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/bits"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"blockwatch.cc/knoxdb/encoding/block"
	"blockwatch.cc/knoxdb/encoding/compress"
	"blockwatch.cc/knoxdb/encoding/s8b"
	"github.com/golang/snappy"
	"github.com/pierrec/lz4"
)

func ReintepretUint64ToByteSlice(src []uint64) []byte {
	header := *(*reflect.SliceHeader)(unsafe.Pointer(&src))
	header.Len *= 8
	header.Cap *= 8
	return *(*[]byte)(unsafe.Pointer(&header))
}

func ReintepretAnySliceToByteSlice(src interface{}) []byte {
	var header reflect.SliceHeader
	v := reflect.ValueOf(src)
	so := int(reflect.TypeOf(src).Elem().Size())
	header.Data = v.Pointer()
	header.Len = so * v.Len()
	header.Cap = so * v.Cap()
	return *(*[]byte)(unsafe.Pointer(&header))
}

func ReintepretByteSliceToAnySlice(src []byte, dst interface{}) interface{} {
	t := reflect.TypeOf(dst)
	so := int(t.Elem().Size())

	slice := reflect.NewAt(t, unsafe.Pointer(&src))
	slice2 := slice.Elem().Slice3(0, len(src)/so, cap(src)/so)

	return slice2.Interface()
}

func convertBlockToByteSlice(b *block.Block) []byte {
	var buf []byte
	switch b.Type() {
	case block.BlockBool:
		buf = b.Bits.Bytes()
	case block.BlockUint64:
		buf = ReintepretAnySliceToByteSlice(b.Uint64)
	case block.BlockUint32:
		buf = ReintepretAnySliceToByteSlice(b.Uint32)
	case block.BlockUint16:
		buf = ReintepretAnySliceToByteSlice(b.Uint16)
	case block.BlockUint8:
		buf = ReintepretAnySliceToByteSlice(b.Uint8)
	case block.BlockInt64, block.BlockTime:
		buf = ReintepretAnySliceToByteSlice(b.Int64)
	case block.BlockInt32:
		buf = ReintepretAnySliceToByteSlice(b.Int32)
	case block.BlockInt16:
		buf = ReintepretAnySliceToByteSlice(b.Int16)
	case block.BlockInt8:
		buf = ReintepretAnySliceToByteSlice(b.Int8)
	}
	return buf
}

func convertByteSliceToBlock(b *block.Block, src []byte) {
	switch b.Type() {
	case block.BlockBool:
		b.Bits = b.Bits.SetFromBytes(src, 8*len(src))
	case block.BlockUint64:
		b.Uint64 = ReintepretByteSliceToAnySlice(src, b.Uint64).([]uint64)
	case block.BlockUint32:
		b.Uint32 = ReintepretByteSliceToAnySlice(src, b.Uint32).([]uint32)
	case block.BlockUint16:
		b.Uint16 = ReintepretByteSliceToAnySlice(src, b.Uint16).([]uint16)
	case block.BlockUint8:
		b.Uint8 = ReintepretByteSliceToAnySlice(src, b.Uint8).([]uint8)
	case block.BlockInt64, block.BlockTime:
		b.Int64 = ReintepretByteSliceToAnySlice(src, b.Int64).([]int64)
	case block.BlockInt32:
		b.Int32 = ReintepretByteSliceToAnySlice(src, b.Int32).([]int32)
	case block.BlockInt16:
		b.Int16 = ReintepretByteSliceToAnySlice(src, b.Int16).([]int16)
	case block.BlockInt8:
		b.Int8 = ReintepretByteSliceToAnySlice(src, b.Int8).([]int8)
	}
}

func convertBlockToUint64(b *block.Block) []uint64 {
	buf := make([]uint64, b.Len())
	switch b.Type() {
	case block.BlockUint64:
		for i, v := range b.Uint64 {
			buf[i] = uint64(v)
		}
	case block.BlockUint32:
		for i, v := range b.Uint32 {
			buf[i] = uint64(v)
		}
	case block.BlockUint16:
		for i, v := range b.Uint16 {
			buf[i] = uint64(v)
		}
	case block.BlockUint8:
		for i, v := range b.Uint8 {
			buf[i] = uint64(v)
		}
	case block.BlockInt64, block.BlockTime:
		for i, v := range b.Int64 {
			buf[i] = uint64(v)
		}
	case block.BlockInt32:
		for i, v := range b.Int32 {
			buf[i] = uint64(v)
		}
	case block.BlockInt16:
		for i, v := range b.Int16 {
			buf[i] = uint64(v)
		}
	case block.BlockInt8:
		for i, v := range b.Int8 {
			buf[i] = uint64(v)
		}
	}
	return buf
}

func convertInt64ToBlock(b *block.Block, src []int64) {
	switch b.Type() {
	case block.BlockUint64:
		b.Uint64 = b.Uint64[:len(src)]
		for i, v := range src {
			b.Uint64[i] = uint64(v)
		}
	case block.BlockUint32:
		b.Uint32 = b.Uint32[:len(src)]
		for i, v := range src {
			b.Uint32[i] = uint32(v)
		}
	case block.BlockUint16:
		b.Uint16 = b.Uint16[:len(src)]
		for i, v := range src {
			b.Uint16[i] = uint16(v)
		}
	case block.BlockUint8:
		b.Uint8 = b.Uint8[:len(src)]
		for i, v := range src {
			b.Uint8[i] = uint8(v)
		}
	case block.BlockInt64, block.BlockTime:
		b.Int64 = b.Int64[:len(src)]
		for i, v := range src {
			b.Int64[i] = int64(v)
		}
	case block.BlockInt32:
		b.Int32 = b.Int32[:len(src)]
		for i, v := range src {
			b.Int32[i] = int32(v)
		}
	case block.BlockInt16:
		b.Int16 = b.Int16[:len(src)]
		for i, v := range src {
			b.Int16[i] = int16(v)
		}
	case block.BlockInt8:
		b.Int8 = b.Int8[:len(src)]
		for i, v := range src {
			b.Int8[i] = int8(v)
		}
	}
}

func convertUint64ToBlock(b *block.Block, src []uint64) {
	switch b.Type() {
	case block.BlockUint64:
		b.Uint64 = b.Uint64[:len(src)]
		for i, v := range src {
			b.Uint64[i] = uint64(v)
		}
	case block.BlockUint32:
		b.Uint32 = b.Uint32[:len(src)]
		for i, v := range src {
			b.Uint32[i] = uint32(v)
		}
	case block.BlockUint16:
		b.Uint16 = b.Uint16[:len(src)]
		for i, v := range src {
			b.Uint16[i] = uint16(v)
		}
	case block.BlockUint8:
		b.Uint8 = b.Uint8[:len(src)]
		for i, v := range src {
			b.Uint8[i] = uint8(v)
		}
	case block.BlockInt64, block.BlockTime:
		b.Int64 = b.Int64[:len(src)]
		for i, v := range src {
			b.Int64[i] = int64(v)
		}
	case block.BlockInt32:
		b.Int32 = b.Int32[:len(src)]
		for i, v := range src {
			b.Int32[i] = int32(v)
		}
	case block.BlockInt16:
		b.Int16 = b.Int16[:len(src)]
		for i, v := range src {
			b.Int16[i] = int16(v)
		}
	case block.BlockInt8:
		b.Int8 = b.Int8[:len(src)]
		for i, v := range src {
			b.Int8[i] = int8(v)
		}
	}
}

func compressSnappy(b *block.Block) ([]byte, int, error) {
	src := convertBlockToByteSlice(b)
	if src == nil {
		return nil, -1, nil
	}
	dst := snappy.Encode(nil, src)
	if dst != nil {
		return dst, len(dst), nil
	}
	return nil, -1, nil
}

func uncompressSnappy(b *block.Block, src []byte) (int, error) {
	if src == nil {
		return -1, nil
	}
	dst, err := snappy.Decode(nil, src)
	convertByteSliceToBlock(b, dst)
	if err != nil {
		return -1, err
	}
	return b.Len(), nil
}

func compressLz4(b *block.Block) ([]byte, int, error) {
	src := convertBlockToByteSlice(b)
	if src == nil {
		return nil, -1, nil
	}

	dst := make([]byte, len(src))
	ht := make([]int, 64<<10) // buffer for the compression table

	n, err := lz4.CompressBlock(src, dst, ht)
	if err != nil {
		return nil, -1, err
	}

	return dst[:n], n, nil
}

func uncompressLz4(b *block.Block, src []byte, size int) (int, error) {
	if src == nil {
		return -1, nil
	}
	dst := make([]byte, size)
	_, err := lz4.UncompressBlock(src, dst)
	if err != nil {
		return -1, err
	}
	convertByteSliceToBlock(b, dst)
	return b.Len(), nil
}

type CompressedHashBlock struct {
	hash_size   int
	hash_nbytes int
	hash_data   []byte
	pk_nbytes   int
	pk_data     []byte
}

func compressHashBlock(pkg Package, hash_size int) (CompressedHashBlock, error) {
	// compress hash block
	b := pkg.blocks[0]
	deltas := make([]uint64, len(b.Uint64))
	shift := 64 - hash_size
	for i := range b.Uint64 {
		deltas[i] = b.Uint64[i] >> shift
	}

	// delta encoding
	maxdelta := compress.Delta8EncodeUint64(deltas)

	var nbytes, nbytes2 int
	if maxdelta == 0 {
		nbytes = 1 // all number zero -> use 1 byte
	} else {
		lz := bits.LeadingZeros64(maxdelta)
		nbytes = (71 - lz) >> 3 // = (64 - tz + 8 - 1) / 8 = ceil((64 - tz)/8)
	}

	buf := make([]byte, nbytes*(len(deltas)-8)+64)

	for i := 0; i < 8; i++ {
		binary.BigEndian.PutUint64(buf[8*i:], deltas[i])
	}

	_, err := compress.PackBytes(deltas[8:], nbytes, buf[64:])
	if err != nil {
		return CompressedHashBlock{0, 0, nil, 0, nil}, err
	}

	// compress hash block
	b = pkg.blocks[1]

	src_max := uint64(0)
	for _, v := range b.Uint64 {
		src_max |= v
	}
	if src_max == 0 {
		nbytes2 = 1 // all number zero -> use 1 byte
	} else {
		lz := bits.LeadingZeros64(src_max)
		nbytes2 = (71 - lz) >> 3 // = (64 - tz + 8 - 1) / 8 = ceil((64 - tz)/8)
	}

	buf2 := make([]byte, nbytes2*len(b.Uint64))

	_, err = compress.PackBytes(b.Uint64, nbytes2, buf2)
	if err != nil {
		return CompressedHashBlock{0, 0, nil, 0, nil}, err
	}

	return CompressedHashBlock{hash_size, nbytes, buf, nbytes2, buf2}, nil
}

func uncompressHashBlock(chb CompressedHashBlock) ([]uint64, []uint64, error) {
	// uncompress hashes
	lenr := (len(chb.hash_data)-64)/chb.hash_nbytes + 8
	res1 := make([]uint64, lenr)
	for i := 0; i < 8; i++ {
		res1[i] = binary.BigEndian.Uint64(chb.hash_data[8*i:])
	}

	_, err := compress.UnpackBytes(chb.hash_data[64:], chb.hash_nbytes, res1[8:])

	if err != nil {
		return nil, nil, err
	}

	compress.Delta8DecodeUint64(res1)

	// uncompress pks
	lenr = len(chb.pk_data) / chb.pk_nbytes
	res2 := make([]uint64, lenr)

	_, err = compress.UnpackBytes(chb.pk_data, chb.pk_nbytes, res2)

	if err != nil {
		return nil, nil, err
	}

	return res1, res2, nil
}

func (p *Package) compressIdx(cmethod string) ([]float64, []float64, []float64, error) {
	cs := make([]float64, p.nFields)
	ct := make([]float64, p.nFields)
	dt := make([]float64, p.nFields)

	var tcomp float64 = -1
	var tdecomp float64 = -1
	var hashlen int
	var err error

	switch {
	case len(cmethod) > 10 && cmethod[:10] == "delta-hash":
		hashlen, err = strconv.Atoi(cmethod[10:])
	case len(cmethod) > 11 && cmethod[:11] == "linear-hash":
		hashlen, err = strconv.Atoi(cmethod[11:])
	default:
		return p.compress(cmethod)
	}

	if err != nil || hashlen < 1 || hashlen > 64 {
		return nil, nil, nil, fmt.Errorf("unknown compression method %s", cmethod)
	}

	// build new Hash
	data := make([]uint64, p.blocks[0].Len())
	for i, v := range p.blocks[0].Uint64 {
		data[i] = v >> (64 - hashlen)
	}

	var csize1, csize2 int
	var res1, res2 []uint64

	switch {
	case cmethod[:10] == "delta-hash":
		start := time.Now()
		chb, err := compressHashBlock(*p, hashlen)
		csize1 = len(chb.hash_data)
		csize2 = len(chb.pk_data)

		tcomp = time.Since(start).Seconds()
		if err == nil {
			start = time.Now()
			res1, _, err = uncompressHashBlock(chb)
			tdecomp = time.Since(start).Seconds()
		}

	default:
		return nil, nil, nil, fmt.Errorf("not yet implemented %s", cmethod)

	}

	if err != nil {
		return nil, nil, nil, err
	}
	for i := range res1 {
		if res1[i] != data[i] {
			fmt.Printf("hash compression: error at position %d\n", i)
		}
	}
	for i := range res2 {
		if res2[i] != p.blocks[1].Uint64[i] {
			fmt.Printf("pk compression: error at position %d\n", i)
		}
	}

	if csize1 < 0 {
		ct[0] = -1
		dt[0] = -1
	} else {
		ct[0] = tcomp
		dt[0] = tdecomp
	}
	cs[0] = float64(csize1)
	cs[1] = float64(csize2)

	return cs, ct, dt, nil
}

func (p *Package) compress(cmethod string) ([]float64, []float64, []float64, error) {
	cs := make([]float64, p.nFields)
	ct := make([]float64, p.nFields)
	dt := make([]float64, p.nFields)

	for j := 0; j < p.nFields; j++ {
		b := p.blocks[j]
		if !b.IsInt() && b.Type().String() != "time" {
			cs[j] = -1
			ct[j] = -1
			dt[j] = -1
			continue
		}
		b2 := block.NewBlock(b.Type(), b.Compression(), b.Len()+8)
		check := true
		var csize int = -1
		var tcomp float64 = -1
		var tdecomp float64 = -1
		var err error
		switch cmethod {
		case "legacy", "legacy-no", "legacy-lz4", "legacy-snappy":
			buf := bytes.NewBuffer(make([]byte, 0, b.MaxStoredSize()))
			switch cmethod {
			case "legacy-no":
				b.SetCompression(block.NoCompression)
			case "legacy-snappy":
				b.SetCompression(block.SnappyCompression)
			case "legacy-lz4":
				b.SetCompression(block.LZ4Compression)
			}
			start := time.Now()
			csize, err = b.Encode(buf)
			tcomp = time.Since(start).Seconds()
			if err == nil {
				switch b2.Type() {
				case block.BlockInt64:
					tmp := b2.Int64[len(b2.Int64):cap(b2.Int64)]
					for i := range tmp {
						tmp[i] = 12345
					}
				case block.BlockUint64:
					tmp := b2.Uint64[len(b2.Uint64):cap(b2.Uint64)]
					for i := range tmp {
						tmp[i] = 12345
					}
				case block.BlockInt32:
					tmp := b2.Int32[len(b2.Int32):cap(b2.Int32)]
					for i := range tmp {
						tmp[i] = 12345
					}
				case block.BlockUint32:
					tmp := b2.Uint32[len(b2.Uint32):cap(b2.Uint32)]
					for i := range tmp {
						tmp[i] = 12345
					}
				case block.BlockInt16:
					tmp := b2.Int16[len(b2.Int16):cap(b2.Int16)]
					for i := range tmp {
						tmp[i] = 12345
					}
				case block.BlockUint16:
					tmp := b2.Uint16[len(b2.Uint16):cap(b2.Uint16)]
					for i := range tmp {
						tmp[i] = 12345
					}
				case block.BlockInt8:
					tmp := b2.Int8[len(b2.Int8):cap(b2.Int8)]
					for i := range tmp {
						tmp[i] = 123
					}
				case block.BlockUint8:
					tmp := b2.Uint8[len(b2.Uint8):cap(b2.Uint8)]
					for i := range tmp {
						tmp[i] = 123
					}
				}

				t1 := b2.Type()
				c1 := b2.Compression()
				encoding := buf.Bytes()[1] >> 4
				start = time.Now()
				err = b2.Decode(buf.Bytes(), b.Len(), b2.MaxStoredSize())
				tdecomp = time.Since(start).Seconds()
				t2 := b2.Type()
				c2 := b2.Compression()
				if t1 != t2 {
					fmt.Printf("\nBlock %d: %s != %s %s != %s\n", j, t1.String(), t2.String(), c1.String(), c2.String())
				}
				l := b2.Len()
				c := b2.Cap()
				bo := false
				switch b2.Type() {
				case block.BlockInt64:
					tmp := b2.Int64[len(b2.Int64):cap(b2.Int64)]
					for _, v := range tmp {
						if v != 12345 {
							bo = true
						}
					}
				case block.BlockUint64:
					tmp := b2.Uint64[len(b2.Uint64):cap(b2.Uint64)]
					for _, v := range tmp {
						if v != 12345 {
							bo = true
						}
					}
				case block.BlockInt32:
					tmp := b2.Int32[len(b2.Int32):cap(b2.Int32)]
					for _, v := range tmp {
						if v != 12345 {
							bo = true
						}
					}
				case block.BlockUint32:
					tmp := b2.Uint32[len(b2.Uint32):cap(b2.Uint32)]
					for _, v := range tmp {
						if v != 12345 {
							bo = true
						}
					}
				case block.BlockInt16:
					tmp := b2.Int16[len(b2.Int16):cap(b2.Int16)]
					for _, v := range tmp {
						if v != 12345 {
							bo = true
						}
					}
				case block.BlockUint16:
					tmp := b2.Uint16[len(b2.Uint16):cap(b2.Uint16)]
					for _, v := range tmp {
						if v != 12345 {
							bo = true
						}
					}
				case block.BlockInt8:
					tmp := b2.Int8[len(b2.Int8):cap(b2.Int8)]
					for _, v := range tmp {
						if v != 123 {
							bo = true
						}
					}
				case block.BlockUint8:
					tmp := b2.Uint8[len(b2.Uint8):cap(b2.Uint8)]
					for _, v := range tmp {
						if v != 123 {
							bo = true
						}
					}
				}
				if bo {
					fmt.Printf("Pack %d, Block %d[%s,%d]: Overflow %v\n", p.key, j, t1.String(), encoding, b2.RangeSlice(l, c))
				}
			}
		case "delta-s8b":
			src := convertBlockToUint64(b)
			start := time.Now()
			if compress.ZzDeltaEncodeUint64(src) >= 1<<60 {
				fmt.Printf("\nCannot s8b compress pack %v block %v\n", p.key, j)
				continue
			}
			src, err = s8b.EncodeAll(src)
			csize = 8 * len(src)
			tcomp = time.Since(start).Seconds()
			if err == nil {
				dst := make([]uint64, b.Len())
				buf := ReintepretAnySliceToByteSlice(src)
				start := time.Now()
				s8b.DecodeAllUint64(dst, buf)
				compress.ZzDeltaDecodeUint64(dst)
				tdecomp = time.Since(start).Seconds()
				convertUint64ToBlock(b2, dst)
			}
		case "s8b":
			src := convertBlockToUint64(b)
			dst := make([]uint64, b.Len())
			zz := false
			if b.IsUint() {
				start := time.Now()
				if compress.MaxUint64(src) >= 1<<60 {
					fmt.Printf("\nCannot s8b compress pack %v block %v\n", p.key, j)
					continue
				}
				src, err = s8b.EncodeAll(src)
				csize = 8 * len(src)
				tcomp = time.Since(start).Seconds()
				if err == nil {
					buf := ReintepretAnySliceToByteSlice(src)
					start := time.Now()
					s8b.DecodeAllUint64(dst, buf)
					tdecomp = time.Since(start).Seconds()
				}
			} else {
				start := time.Now()
				if compress.HasNegUint64(src) {
					if compress.ZzEncodeUint64(src) >= 1<<60 {
						fmt.Printf("\nCannot s8b compress pack %v block %v\n", p.key, j)
						continue
					}
					zz = true
				} else {
					if compress.MaxUint64(src) >= 1<<60 {
						fmt.Printf("\nCannot s8b compress pack %v block %v\n", p.key, j)
						continue
					}
				}
				src, err = s8b.EncodeAll(src)
				csize = 8 * len(src)
				tcomp = time.Since(start).Seconds()
				if err == nil {
					buf := ReintepretAnySliceToByteSlice(src)
					start := time.Now()
					s8b.DecodeAllUint64(dst, buf)
					if zz {
						compress.ZzDecodeUint64(dst)
					}
					tdecomp = time.Since(start).Seconds()
				}
			}
			convertUint64ToBlock(b2, dst)
		case "snappy":
			var buf []byte
			start := time.Now()
			buf, csize, err = compressSnappy(p.blocks[j])
			tcomp = time.Since(start).Seconds()
			if err == nil {
				start = time.Now()
				_, err = uncompressSnappy(b2, buf)
				tdecomp = time.Since(start).Seconds()
			}
		case "lz4":
			var buf []byte
			start := time.Now()
			buf, csize, err = compressLz4(p.blocks[j])
			tcomp = time.Since(start).Seconds()
			if err == nil {
				start = time.Now()
				_, err = uncompressLz4(b2, buf, cap(buf))
				tdecomp = time.Since(start).Seconds()
			}
		default:
			return nil, nil, nil, fmt.Errorf("unknown compression method %s", cmethod)
		}
		if err != nil {
			return nil, nil, nil, err
		}
		if check && !reflect.DeepEqual(b.RawSlice(), b2.RawSlice()) {
			fmt.Printf("Compression/Decompression error pack %v block %v\n", p.key, j)
		}
		ct[j] = tcomp
		dt[j] = tdecomp
		cs[j] = float64(csize)
	}
	return cs, ct, dt, nil
}

func (t *Table) CompressPack(cmethod string, w io.Writer, i int, mode DumpMode) error {
	if i >= t.packidx.Len() || i < 0 {
		return ErrPackNotFound
	}
	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	pkg, err := t.loadSharedPack(tx, t.packidx.packs[i].Key, false, nil)
	if err != nil {
		return err
	}

	cratios := make([][]float64, 1)
	ctimes := make([][]float64, 1)
	dtimes := make([][]float64, 1)
	cs, ct, dt, err := pkg.compress(cmethod)
	if err != nil {
		return err
	}

	for j := 0; j < pkg.nFields; j++ {
		if cs[j] < 0 {
			ct[j] = -1
			dt[j] = -1
		} else {
			usize := float64(pkg.blocks[j].HeapSize())
			ct[j] = usize / ct[j] / 1000000
			dt[j] = usize / dt[j] / 1000000
			cs[j] = cs[j] / usize
		}
	}

	cratios[0] = cs
	ctimes[0] = ct
	dtimes[0] = dt

	t.releaseSharedPack(pkg)

	return DumpCompressResults(t.fields, cratios, ctimes, dtimes, w, mode, false)
}

func (t *Table) CompressIndexPack(cmethod string, w io.Writer, i, p int, mode DumpMode) error {
	if i >= len(t.indexes) || i < 0 {
		return ErrIndexNotFound
	}
	if p >= t.indexes[i].packidx.Len() || p < 0 {
		return ErrPackNotFound
	}
	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	pkg, err := t.indexes[i].loadSharedPack(tx, t.indexes[i].packidx.packs[p].Key, false)
	if err != nil {
		return err
	}

	cratios := make([][]float64, 1)
	ctimes := make([][]float64, 1)
	dtimes := make([][]float64, 1)
	cs, ct, dt, err := pkg.compressIdx(cmethod)
	if err != nil {
		return err
	}
	for j := 0; j < pkg.nFields; j++ {
		if cs[j] < 0 {
			ct[j] = -1
			dt[j] = -1
		} else {
			usize := float64(pkg.blocks[0].HeapSize() + pkg.blocks[1].HeapSize())
			ct[j] = usize / ct[j] / 1000000
			dt[j] = usize / dt[j] / 1000000
			cs[j] = cs[j] / usize
		}
	}

	cratios[0] = cs
	ctimes[0] = ct
	dtimes[0] = dt

	t.indexes[i].releaseSharedPack(pkg)

	fl := FieldList{{Name: "Hash", Type: FieldTypeUint64}, {Name: "PK", Type: FieldTypeUint64}}

	return DumpCompressResults(fl, cratios, ctimes, dtimes, w, mode, false)
}

func (t *Table) CompressIndexAll(cmethod string, i int, w io.Writer, mode DumpMode, verbose bool) error {
	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	nPacks := t.packidx.Len()
	cratios := make([][]float64, nPacks+1)
	ctimes := make([][]float64, nPacks+1)
	dtimes := make([][]float64, nPacks+1)

	colCSize := make([]float64, 2)
	colUSize := make([]float64, 2)
	colCTime := make([]float64, 2)
	colDTime := make([]float64, 2)

	for p := 0; p < nPacks; p++ {
		pkg, err := t.indexes[i].loadSharedPack(tx, t.indexes[i].packidx.packs[p].Key, false)
		if err != nil {
			return err
		}

		cs, ct, dt, err := pkg.compressIdx(cmethod)
		if err != nil {
			return err
		}

		for j := 0; j < pkg.nFields; j++ {
			if cs[j] < 0 {
				ct[j] = -1
				dt[j] = -1
			} else {
				usize := float64(pkg.blocks[0].HeapSize() + pkg.blocks[1].HeapSize())
				colUSize[j] += usize
				colCSize[j] += cs[j]
				colCTime[j] += ct[j]
				colDTime[j] += dt[j]
				ct[j] = usize / ct[j] / 1000000
				dt[j] = usize / dt[j] / 1000000
				cs[j] = cs[j] / usize
			}
		}
		cratios[p] = cs
		ctimes[p] = ct
		dtimes[p] = dt

		t.indexes[i].releaseSharedPack(pkg)

		fmt.Printf(".")
	}
	fmt.Printf("\nProcessed %d packs\n", t.indexes[i].packidx.Len())

	var totalUSize, totalCSize, totalCTime, totalDTime float64
	for j := 0; j < 2; j++ {
		usize := colUSize[j]
		totalUSize += usize
		totalCSize += colCSize[j]
		totalCTime += colCTime[j]
		totalDTime += colDTime[j]
		colCTime[j] = usize / colCTime[j] / 1000000
		colDTime[j] = usize / colDTime[j] / 1000000
		colCSize[j] = colCSize[j] / usize
	}
	cratios[nPacks] = colCSize
	ctimes[nPacks] = colCTime
	dtimes[nPacks] = colDTime

	fl := FieldList{{Name: "Hash", Type: FieldTypeUint64}, {Name: "PK", Type: FieldTypeUint64}}
	return DumpCompressResults(fl, cratios, ctimes, dtimes, w, mode, verbose)
}

func (t *Table) IndexCollisions(cmethod string, i int, w io.Writer, mode DumpMode, verbose bool) error {
	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if cmethod[:4] != "hash" {
		return fmt.Errorf("unknown compression method %s", cmethod)
	}

	hashlen, err := strconv.Atoi(cmethod[4:])

	if err != nil || hashlen < 1 || hashlen > 64 {
		return fmt.Errorf("unknown compression method %s", cmethod)
	}

	var collisions uint64

	for p := 0; p < t.indexes[i].packidx.Len(); p++ {
		pkg, err := t.indexes[i].loadSharedPack(tx, t.indexes[i].packidx.packs[p].Key, false)
		if err != nil {
			return err
		}

		data := pkg.blocks[0].Uint64
		shift := 64 - hashlen
		for i := 1; i < len(data); i++ {
			if data[i] != data[i-1] && (data[i]>>shift) == (data[i-1]>>shift) {
				collisions++
			}
		}
		t.indexes[i].releaseSharedPack(pkg)
	}

	fmt.Printf("Index contains %d additional collisions\n", collisions)

	return nil
}

func (t *Table) CompressAll(cmethod string, w io.Writer, mode DumpMode, verbose bool) error {
	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	nPacks := t.packidx.Len()
	cratios := make([][]float64, nPacks+1)
	ctimes := make([][]float64, nPacks+1)
	dtimes := make([][]float64, nPacks+1)

	colCSize := make([]float64, len(t.fields))
	colUSize := make([]float64, len(t.fields))
	colCTime := make([]float64, len(t.fields))
	colDTime := make([]float64, len(t.fields))

	for i := 0; i < nPacks; i++ {
		pkg, err := t.loadSharedPack(tx, t.packidx.packs[i].Key, false, nil)
		if err != nil {
			return err
		}

		cs, ct, dt, err := pkg.compress(cmethod)
		if err != nil {
			return err
		}

		for j := 0; j < pkg.nFields; j++ {
			if cs[j] < 0 {
				ct[j] = -1
				dt[j] = -1
			} else {
				usize := float64(pkg.blocks[j].HeapSize())
				colUSize[j] += usize
				colCSize[j] += cs[j]
				colCTime[j] += ct[j]
				colDTime[j] += dt[j]
				ct[j] = usize / ct[j] / 1000000
				dt[j] = usize / dt[j] / 1000000
				cs[j] = cs[j] / usize
			}
		}
		cratios[i] = cs
		ctimes[i] = ct
		dtimes[i] = dt

		t.releaseSharedPack(pkg)

		fmt.Printf(".")
	}
	fmt.Printf("\nProcessed %d packs\n", nPacks)
	var totalUSize, totalCSize, totalCTime, totalDTime float64
	for j := 0; j < len(t.fields); j++ {
		usize := colUSize[j]
		totalUSize += usize
		totalCSize += colCSize[j]
		totalCTime += colCTime[j]
		totalDTime += colDTime[j]
		colCTime[j] = usize / colCTime[j] / 1000000
		colDTime[j] = usize / colDTime[j] / 1000000
		colCSize[j] = colCSize[j] / usize
	}
	cratios[nPacks] = colCSize
	ctimes[nPacks] = colCTime
	dtimes[nPacks] = colDTime

	ret := DumpCompressResults(t.fields, cratios, ctimes, dtimes, w, mode, verbose)

	if ret != nil {
		return ret
	}
	fmt.Printf("\nUncompressed Size: %.2fGB\n", totalUSize/1000000000)
	fmt.Printf("Compressed Size: %.2fGB\n", totalCSize/1000000000)
	fmt.Printf("Compression Ratio: %.1f%%\n", (1-totalCSize/totalUSize)*100)
	fmt.Printf("Compression Time: %.0fs\n", totalCTime)
	fmt.Printf("Compression Throughput: %.0fMB/s\n", totalUSize/totalCTime/1000000)
	fmt.Printf("Decompression Time: %.0fs\n", totalDTime)
	fmt.Printf("Decompression Throughput: %.0fMB/s\n", totalUSize/totalDTime/1000000)

	return nil
}

func (t *Table) CacheTest() error {
	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	//nPacks := t.packidx.Len()

	var count int

	var list = []int{0, 0, 2, 3, 4, 5, 6, 2, -2}

	for _, i := range list {
		if i >= 0 {
			pkg, err := t.loadSharedPack(tx, t.packidx.packs[i].Key, true, nil)
			if err != nil {
				return err
			}
			t.releaseSharedPack(pkg)
		} else {
			t.cache.Remove(t.cachekey(encodePackKey(uint32(-i))))
		}

		r, f, e, b := t.cache.GetParams()
		fmt.Printf("size=%d recent=%d frequent=%d evicted=%d %dBytes\n", r+f, r, f, e, b)
		count++
	}

	fmt.Printf("\nProcessed %d packs\n", count)
	fmt.Printf("PackCacheSize %d\n", t.stats.PackCacheSize)
	fmt.Printf("PackCacheCount %d\n", t.stats.PackCacheCount)
	fmt.Printf("PackCacheCapacity %d\n", t.stats.PackCacheCapacity)
	fmt.Printf("PackCacheHits %d\n", t.stats.PackCacheHits)
	fmt.Printf("PackCacheMisses %d\n", t.stats.PackCacheMisses)
	fmt.Printf("PackCacheInserts %d\n", t.stats.PackCacheInserts)
	fmt.Printf("PackCacheEvictions %d\n", t.stats.PackCacheEvictions)

	return nil
}

func (t *Table) CacheBench() error {
	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// nPacks := t.packidx.Len()
	nPacks := 1000
	max_loop := 1000

	var count int

	// popuate the Cache
	for n := 0; n < nPacks; n++ {
		pkg, err := t.loadSharedPack(tx, t.packidx.packs[n].Key, true, nil)
		if err != nil {
			return err
		}
		t.releaseSharedPack(pkg)
	}

	// reset cache stats
	t.stats.PackCacheSize = 0
	t.stats.PackCacheCount = 0
	t.stats.PackCacheCapacity = 0
	t.stats.PackCacheHits = 0
	t.stats.PackCacheMisses = 0
	t.stats.PackCacheInserts = 0
	t.stats.PackCacheEvictions = 0

	tstart := time.Now()
	for n := 0; n < max_loop; n++ {
		i := rand.Intn(nPacks)
		pkg, err := t.loadSharedPack(tx, t.packidx.packs[i].Key, true, nil)
		if err != nil {
			return err
		}
		t.releaseSharedPack(pkg)

		count++
	}
	dur := time.Since(tstart)

	fmt.Printf("\nProcessed %d packs in %f seconds (%f s/Pack)\n", count, dur.Seconds(), dur.Seconds()/float64(count))
	fmt.Printf("PackCacheSize %d\n", t.stats.PackCacheSize)
	fmt.Printf("PackCacheCount %d\n", t.stats.PackCacheCount)
	fmt.Printf("PackCacheCapacity %d\n", t.stats.PackCacheCapacity)
	fmt.Printf("PackCacheHits %d (%.2f%%)\n", t.stats.PackCacheHits, 100*float64(t.stats.PackCacheHits)/float64(count))
	fmt.Printf("PackCacheMisses %d (%.2f%%)\n", t.stats.PackCacheMisses, 100*float64(t.stats.PackCacheMisses)/float64(count))
	fmt.Printf("PackCacheInserts %d\n", t.stats.PackCacheInserts)
	fmt.Printf("PackCacheEvictions %d\n", t.stats.PackCacheEvictions)

	return nil
}

func (t *Table) ShowCompression(cmethod string, w io.Writer, mode DumpMode, verbose bool) error {
	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	cratios := make([][]float64, t.packidx.Len())
	ctype := make([][]int8, t.packidx.Len())

	var csize int
	for i := 0; i < t.packidx.Len(); i++ {
		pkg, err := t.loadSharedPack(tx, t.packidx.packs[i].Key, false, nil)
		cr := make([]float64, pkg.nFields)
		ct := make([]int8, pkg.nFields)
		if err != nil {
			return err
		}
		for j := 0; j < pkg.nFields; j++ {
			b := pkg.blocks[j]
			if !b.IsInt() {
				cr[j] = -1
				ct[j] = -1
				continue
			}
			buf := bytes.NewBuffer(make([]byte, 0, b.MaxStoredSize()))

			b.SetCompression(block.NoCompression)
			csize, err = b.Encode(buf)
			cr[j] = float64(csize) / float64(b.HeapSize())
			ct[j] = int8(block.Compression(buf.Bytes()[1]) >> 4)
		}
		cratios[i] = cr
		ctype[i] = ct

		t.releaseSharedPack(pkg)

		fmt.Printf(".")
	}
	fmt.Printf("\nProcessed %d packs\n", t.packidx.Len())
	return DumpInfos(t.fields, ctype, w, mode, verbose)
}

func DumpCompressResults(fl FieldList, cratios, ctimes, dtimes [][]float64, w io.Writer, mode DumpMode, verbose bool) error {
	out := "Compression ratios\n"
	if _, err := w.Write([]byte(out)); err != nil {
		return err
	}
	if err := DumpRatios(fl, cratios, w, mode, verbose); err != nil {
		return err
	}

	out = "\nCompression troughput [MB/s]\n"
	if _, err := w.Write([]byte(out)); err != nil {
		return err
	}
	if err := DumpTimes(fl, ctimes, w, mode, verbose); err != nil {
		return err
	}

	out = "\nUncompression troughput [MB/s]\n"
	if _, err := w.Write([]byte(out)); err != nil {
		return err
	}
	if err := DumpTimes(fl, dtimes, w, mode, verbose); err != nil {
		return err
	}

	return nil
}

func DumpRatios(fl FieldList, cratios [][]float64, w io.Writer, mode DumpMode, verbose bool) error {
	names := fl.Names()
	nFields := len(names)
	if len(fl.Aliases()) == nFields && len(fl.Aliases()[0]) > 0 {
		names = fl.Aliases()
	}

	names = append([]string{"Pack"}, names...)
	nPacks := len(cratios) - 1 // last row is average
	if nPacks == 0 {
		nPacks = 1
	}

	// estimate sizes from the first 500 values
	switch mode {
	case DumpModeDec, DumpModeHex:
		sz := make([]int, nFields+1)
		row := make([]string, nFields+1)
		for j := 0; j < nFields+1; j++ {
			sz[j] = len(names[j])
		}
		for j := 0; j < nFields; j++ {
			if l := len(fl[j].Type.String()); l > sz[j+1] {
				sz[j+1] = l
			}
			if sz[j+1] < 6 {
				sz[j+1] = 6
			}
		}
		for j := 0; j < nFields+1; j++ {
			row[j] = fmt.Sprintf("%[2]*[1]s", names[j], -sz[j])
		}
		var out string
		out = "| " + strings.Join(row, " | ") + " |\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}
		row[0] = fmt.Sprintf("%[2]*[1]s", "", -sz[0])
		for j := 0; j < nFields; j++ {
			row[j+1] = fmt.Sprintf("%[2]*[1]s", fl[j].Type, -sz[j+1])
		}
		out = "| " + strings.Join(row, " | ") + " |\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}

		row[0] = fmt.Sprintf("%[2]*[1]s", "", -sz[0])
		for j := 0; j < nFields; j++ {
			row[j+1] = fmt.Sprintf("%[2]*[1]s", fl[j].Flags.Compression().String(), -sz[j+1])
		}
		out = "| " + strings.Join(row, " | ") + " |\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}

		for j := 0; j < nFields+1; j++ {
			row[j] = strings.Repeat("-", sz[j])
		}
		out = "|-" + strings.Join(row, "-|-") + "-|\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}
		for i := 0; i < nPacks; i++ {
			row[0] = fmt.Sprintf("%[2]*[1]d", i, sz[0])
			for j := 0; j < len(cratios[0]); j++ {
				if cratios[i][j] < 0 {
					row[j+1] = fmt.Sprintf("%[2]*[1]s", "", sz[j+1])
				} else {
					row[j+1] = fmt.Sprintf("%[2]*.[1]f%%", 100*(1-cratios[i][j]), sz[j+1]-1)
				}
			}
			if verbose || len(cratios) == 1 {
				out = "| " + strings.Join(row, " | ") + " |\n"
				if _, err := w.Write([]byte(out)); err != nil {
					return err
				}
			}
		}
		if len(cratios) == 1 {
			return nil
		}
		if verbose {
			for j := 0; j < nFields+1; j++ {
				row[j] = strings.Repeat("-", sz[j])
			}
			out = "|-" + strings.Join(row, "-|-") + "-|\n"
			if _, err := w.Write([]byte(out)); err != nil {
				return err
			}
			for j := 0; j < nFields+1; j++ {
				row[j] = fmt.Sprintf("%[2]*[1]s", names[j], -sz[j])
			}
			out = "| " + strings.Join(row, " | ") + " |\n"
			if _, err := w.Write([]byte(out)); err != nil {
				return err
			}
			row[0] = fmt.Sprintf("%[2]*[1]s", "", -sz[0])
			for j := 0; j < nFields; j++ {
				row[j+1] = fmt.Sprintf("%[2]*[1]s", fl[j].Type, -sz[j+1])
			}
			out = "| " + strings.Join(row, " | ") + " |\n"
			if _, err := w.Write([]byte(out)); err != nil {
				return err
			}
			for j := 0; j < nFields+1; j++ {
				row[j] = strings.Repeat("-", sz[j])
			}
			out = "|-" + strings.Join(row, "-|-") + "-|\n"
			if _, err := w.Write([]byte(out)); err != nil {
				return err
			}
		}
		row[0] = fmt.Sprintf(" AVG")
		avg := cratios[nPacks]
		for j := 0; j < len(avg); j++ {
			if avg[j] < 0 {
				row[j+1] = fmt.Sprintf("%[2]*[1]s", "", sz[j+1])
			} else {
				row[j+1] = fmt.Sprintf("%[2]*.[1]f%%", 100*(1-avg[j]), sz[j+1]-1)
			}
		}
		out = "| " + strings.Join(row, " | ") + " |\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}

		/*  case DumpModeCSV:
		    enc, ok := w.(*csv.Encoder)
		    if !ok {
		        enc = csv.NewEncoder(w)
		    }
		    if !enc.HeaderWritten() {
		        if err := enc.EncodeHeader(names, nil); err != nil {
		            return err
		        }
		    }
		    // csv encoder supports []interface{} records
		    for i := 0; i < p.nValues; i++ {
		        row, _ := p.RowAt(i)
		        if err := enc.EncodeRecord(row); err != nil {
		            return err
		        }
		    }*/
	}
	return nil
}

func DumpInfos(fl FieldList, cinfos [][]int8, w io.Writer, mode DumpMode, verbose bool) error {
	names := fl.Names()
	nFields := len(names)
	if len(fl.Aliases()) == nFields && len(fl.Aliases()[0]) > 0 {
		names = fl.Aliases()
	}

	names = append([]string{"Pack"}, names...)

	// estimate sizes from the first 500 values
	switch mode {
	case DumpModeDec, DumpModeHex:
		sz := make([]int, nFields+1)
		row := make([]string, nFields+1)
		for j := 0; j < nFields+1; j++ {
			sz[j] = len(names[j])
		}
		for j := 0; j < nFields; j++ {
			if l := len(fl[j].Type.String()); l > sz[j+1] {
				sz[j+1] = l
			}
			if sz[j+1] < 6 {
				sz[j+1] = 6
			}
		}
		for j := 0; j < nFields+1; j++ {
			row[j] = fmt.Sprintf("%[2]*[1]s", names[j], -sz[j])
		}
		var out string
		out = "| " + strings.Join(row, " | ") + " |\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}
		row[0] = fmt.Sprintf("%[2]*[1]s", "", -sz[0])
		for j := 0; j < nFields; j++ {
			row[j+1] = fmt.Sprintf("%[2]*[1]s", fl[j].Type, -sz[j+1])
		}
		out = "| " + strings.Join(row, " | ") + " |\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}

		for j := 0; j < nFields+1; j++ {
			row[j] = strings.Repeat("-", sz[j])
		}
		out = "|-" + strings.Join(row, "-|-") + "-|\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}

		s1 := make([]int, len(cinfos[0]))
		s2 := make([]int, len(cinfos[0]))
		s3 := make([]int, len(cinfos[0]))
		for i := 0; i < len(cinfos); i++ {
			row[0] = fmt.Sprintf("%[2]*[1]d", i, sz[0])
			for j := 0; j < len(cinfos[0]); j++ {
				switch cinfos[i][j] {
				case 0:
					s1[j]++
				case 1:
					s2[j]++
				case 2:
					s3[j]++
				}
				if cinfos[i][j] < 0 {
					row[j+1] = fmt.Sprintf("%[2]*[1]s", "", sz[j+1])
				} else {
					row[j+1] = fmt.Sprintf("%[2]*.[1]d", cinfos[i][j], sz[j+1])
				}
			}
			if verbose || len(cinfos) == 1 {
				out = "| " + strings.Join(row, " | ") + " |\n"
				if _, err := w.Write([]byte(out)); err != nil {
					return err
				}
			}
		}
		if len(cinfos) == 1 {
			return nil
		}
		if verbose {
			for j := 0; j < nFields+1; j++ {
				row[j] = strings.Repeat("-", sz[j])
			}
			out = "|-" + strings.Join(row, "-|-") + "-|\n"
			if _, err := w.Write([]byte(out)); err != nil {
				return err
			}
			for j := 0; j < nFields+1; j++ {
				row[j] = fmt.Sprintf("%[2]*[1]s", names[j], -sz[j])
			}
			out = "| " + strings.Join(row, " | ") + " |\n"
			if _, err := w.Write([]byte(out)); err != nil {
				return err
			}
			row[0] = fmt.Sprintf("%[2]*[1]s", "", -sz[0])
			for j := 0; j < nFields; j++ {
				row[j+1] = fmt.Sprintf("%[2]*[1]s", fl[j].Type, -sz[j+1])
			}
			out = "| " + strings.Join(row, " | ") + " |\n"
			if _, err := w.Write([]byte(out)); err != nil {
				return err
			}
			for j := 0; j < nFields+1; j++ {
				row[j] = strings.Repeat("-", sz[j])
			}
			out = "|-" + strings.Join(row, "-|-") + "-|\n"
			if _, err := w.Write([]byte(out)); err != nil {
				return err
			}
		}
		row[0] = fmt.Sprintf("%[2]*[1]s", "No", -sz[0])
		for j := 0; j < len(s1); j++ {
			row[j+1] = fmt.Sprintf("%[2]*[1]d", s1[j], sz[j+1])
		}
		out = "| " + strings.Join(row, " | ") + " |\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}
		row[0] = fmt.Sprintf("%[2]*[1]s", "S8B", -sz[0])
		for j := 0; j < len(s1); j++ {
			row[j+1] = fmt.Sprintf("%[2]*[1]d", s2[j], sz[j+1])
		}
		out = "| " + strings.Join(row, " | ") + " |\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}
		row[0] = fmt.Sprintf("%[2]*[1]s", "RLE", -sz[0])
		for j := 0; j < len(s1); j++ {
			row[j+1] = fmt.Sprintf("%[2]*[1]d", s3[j], sz[j+1])
		}
		out = "| " + strings.Join(row, " | ") + " |\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}

		/*  case DumpModeCSV:
		    enc, ok := w.(*csv.Encoder)
		    if !ok {
		        enc = csv.NewEncoder(w)
		    }
		    if !enc.HeaderWritten() {
		        if err := enc.EncodeHeader(names, nil); err != nil {
		            return err
		        }
		    }
		    // csv encoder supports []interface{} records
		    for i := 0; i < p.nValues; i++ {
		        row, _ := p.RowAt(i)
		        if err := enc.EncodeRecord(row); err != nil {
		            return err
		        }
		    }*/
	}
	return nil
}

func DumpTimes(fl FieldList, ctimes [][]float64, w io.Writer, mode DumpMode, verbose bool) error {
	names := fl.Names()
	nFields := len(names)
	if len(fl.Aliases()) == nFields && len(fl.Aliases()[0]) > 0 {
		names = fl.Aliases()
	}

	names = append([]string{"Pack"}, names...)
	nPacks := len(ctimes) - 1 // last row is average
	if nPacks == 0 {
		nPacks = 1
	}

	// estimate sizes from the first 500 values
	switch mode {
	case DumpModeDec, DumpModeHex:
		sz := make([]int, nFields+1)
		row := make([]string, nFields+1)
		for j := 0; j < nFields+1; j++ {
			sz[j] = len(names[j])
		}
		for j := 0; j < nFields; j++ {
			if l := len(fl[j].Type.String()); l > sz[j+1] {
				sz[j+1] = l
			}
			if sz[j+1] < 5 {
				sz[j+1] = 5
			}
		}
		for j := 0; j < nFields+1; j++ {
			row[j] = fmt.Sprintf("%[2]*[1]s", names[j], -sz[j])
		}
		var out string
		out = "| " + strings.Join(row, " | ") + " |\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}
		row[0] = fmt.Sprintf("%[2]*[1]s", "", -sz[0])
		for j := 0; j < nFields; j++ {
			row[j+1] = fmt.Sprintf("%[2]*[1]s", fl[j].Type, -sz[j+1])
		}
		out = "| " + strings.Join(row, " | ") + " |\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}
		for j := 0; j < nFields+1; j++ {
			row[j] = strings.Repeat("-", sz[j])
		}
		out = "|-" + strings.Join(row, "-|-") + "-|\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}
		for i := 0; i < nPacks; i++ {
			row[0] = fmt.Sprintf("%[2]*[1]d", i, sz[0])
			for j := 0; j < len(ctimes[0]); j++ {
				if ctimes[i][j] < 0 {
					row[j+1] = fmt.Sprintf("%[2]*[1]s", "", sz[j+1])
				} else {
					row[j+1] = fmt.Sprintf("%[2]*.[1]f", ctimes[i][j], sz[j+1])
				}
			}
			if verbose || len(ctimes) == 1 {
				out = "| " + strings.Join(row, " | ") + " |\n"
				if _, err := w.Write([]byte(out)); err != nil {
					return err
				}
			}
		}
		if len(ctimes) == 1 {
			return nil
		}
		if verbose {
			for j := 0; j < nFields+1; j++ {
				row[j] = strings.Repeat("-", sz[j])
			}
			out = "|-" + strings.Join(row, "-|-") + "-|\n"
			if _, err := w.Write([]byte(out)); err != nil {
				return err
			}
			for j := 0; j < nFields+1; j++ {
				row[j] = fmt.Sprintf("%[2]*[1]s", names[j], -sz[j])
			}
			out = "| " + strings.Join(row, " | ") + " |\n"
			if _, err := w.Write([]byte(out)); err != nil {
				return err
			}
			row[0] = fmt.Sprintf("%[2]*[1]s", "", -sz[0])
			for j := 0; j < nFields; j++ {
				row[j+1] = fmt.Sprintf("%[2]*[1]s", fl[j].Type, -sz[j+1])
			}
			out = "| " + strings.Join(row, " | ") + " |\n"
			if _, err := w.Write([]byte(out)); err != nil {
				return err
			}
			for j := 0; j < nFields+1; j++ {
				row[j] = strings.Repeat("-", sz[j])
			}
			out = "|-" + strings.Join(row, "-|-") + "-|\n"
			if _, err := w.Write([]byte(out)); err != nil {
				return err
			}
		}
		row[0] = fmt.Sprintf(" AVG")
		avg := ctimes[nPacks]
		for j := 0; j < len(avg); j++ {
			if avg[j] < 0 {
				row[j+1] = fmt.Sprintf("%[2]*[1]s", "", sz[j+1])
			} else {
				row[j+1] = fmt.Sprintf("%[2]*.[1]f", avg[j], sz[j+1])
			}
		}
		out = "| " + strings.Join(row, " | ") + " |\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}

		/*  case DumpModeCSV:
		    enc, ok := w.(*csv.Encoder)
		    if !ok {
		        enc = csv.NewEncoder(w)
		    }
		    if !enc.HeaderWritten() {
		        if err := enc.EncodeHeader(names, nil); err != nil {
		            return err
		        }
		    }
		    // csv encoder supports []interface{} records
		    for i := 0; i < p.nValues; i++ {
		        row, _ := p.RowAt(i)
		        if err := enc.EncodeRecord(row); err != nil {
		            return err
		        }
		    }*/
	}
	return nil
}
