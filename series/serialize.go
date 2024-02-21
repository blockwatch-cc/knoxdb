// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package series

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"strconv"
	"time"
)

func WriteInt64(b *bytes.Buffer, i int64, sep byte) {
	b.WriteString(strconv.FormatInt(i, 10))
	b.WriteByte(sep)
}

func WriteInt(b *bytes.Buffer, i int, sep byte) {
	b.WriteString(strconv.Itoa(i))
	b.WriteByte(sep)
}

func WriteUint64(b *bytes.Buffer, i uint64, sep byte) {
	b.WriteString(strconv.FormatUint(i, 10))
	b.WriteByte(sep)
}

func WriteFloat64(b *bytes.Buffer, f float64, prec int, sep byte) {
	b.WriteString(strconv.FormatFloat(f, 'f', prec, 64))
	b.WriteByte(sep)
}

func WriteString(b *bytes.Buffer, s string, sep byte) {
	b.WriteString(strconv.Quote(s))
	b.WriteByte(sep)
}

func WriteNullString(b *bytes.Buffer, s string, sep byte) {
	if len(s) == 0 {
		b.WriteString("null")
	} else {
		b.WriteString(strconv.Quote(s))
	}
	b.WriteByte(sep)
}

func WriteHexBytes(b *bytes.Buffer, data []byte, sep byte) {
	if len(data) > 0 {
		b.WriteByte('"')
		b.WriteString(hex.EncodeToString(data))
		b.WriteByte('"')
	} else {
		b.WriteString("null")
	}
	b.WriteByte(sep)
}

func WriteBase64Bytes(b *bytes.Buffer, data []byte, sep byte) {
	if len(data) > 0 {
		b.WriteByte('"')
		b.WriteString(base64.StdEncoding.EncodeToString(data))
		b.WriteByte('"')
	} else {
		b.WriteString("null")
	}
	b.WriteByte(sep)
}

func WriteRawBytes(b *bytes.Buffer, data []byte, sep byte) {
	if len(data) > 0 {
		b.Write(data)
	} else {
		b.WriteString("null")
	}
	b.WriteByte(sep)
}

func WriteBool(b *bytes.Buffer, i bool, sep byte) {
	if i {
		b.WriteByte('1')
	} else {
		b.WriteByte('0')
	}
	b.WriteByte(sep)
}

func WriteInt64Slice(b *bytes.Buffer, i []int64, sep byte) {
	if len(i) == 0 {
		return
	}
	b.WriteString(strconv.FormatInt(i[0], 10))
	for _, v := range i[1:] {
		b.WriteByte(sep)
		b.WriteString(strconv.FormatInt(v, 10))
	}
}

func WriteIntSlice(b *bytes.Buffer, i []int, sep byte) {
	if len(i) == 0 {
		return
	}
	b.WriteString(strconv.Itoa(i[0]))
	for _, v := range i[1:] {
		b.WriteByte(sep)
		b.WriteString(strconv.Itoa(v))
	}
}

func WriteUint64Slice(b *bytes.Buffer, i []uint64, sep byte) {
	if len(i) == 0 {
		return
	}
	b.WriteString(strconv.FormatUint(i[0], 10))
	for _, v := range i[1:] {
		b.WriteByte(sep)
		b.WriteString(strconv.FormatUint(v, 10))
	}
}

func WriteFloat64Slice(b *bytes.Buffer, f []float64, prec int, sep byte) {
	if len(f) == 0 {
		return
	}
	b.WriteString(strconv.FormatFloat(f[0], 'f', prec, 64))
	for _, v := range f[1:] {
		b.WriteByte(sep)
		b.WriteString(strconv.FormatFloat(v, 'f', prec, 64))
	}
}

func WriteRfc3339Slice(b *bytes.Buffer, t []time.Time, sep byte) {
	if len(t) == 0 {
		return
	}
	b.WriteString(strconv.Quote(t[0].Format(time.RFC3339)))
	for _, v := range t[1:] {
		b.WriteByte(sep)
		b.WriteString(strconv.Quote(v.Format(time.RFC3339)))
	}
}

func WriteStringSlice(b *bytes.Buffer, s []string, sep byte) {
	if len(s) == 0 {
		return
	}
	b.WriteString(strconv.Quote(s[0]))
	for _, v := range s[1:] {
		b.WriteByte(sep)
		b.WriteString(strconv.Quote(v))
	}
}
