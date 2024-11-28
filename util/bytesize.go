// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package util

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Note the difference between Kilobyte and Kibibyte.
// 1 Kilobyte = 1000 byte whereas 1 Kibibyte = 1024 byte.
//
// Kilobytes are often used to promote commercial products
// while Kibibytes are used in computer science, development, etc.
//
// See IEC 80000-13:2008, DIN EN 80000-13:2009-01

type ByteSize float64

const (
	_            = iota             // ignore first value by assigning to blank identifier
	KiB ByteSize = 1 << (10 * iota) // 1 << (10*1)
	MiB                             // 1 << (10*2)
	GiB                             // 1 << (10*3)
	TiB                             // 1 << (10*4)
	PiB                             // 1 << (10*5)
	EiB                             // 1 << (10*6)
	ZiB                             // 1 << (10*7)
	YiB                             // 1 << (10*8)
)

const (
	B  ByteSize = 1
	KB          = B * 1000  // 10^3
	MB          = KB * 1000 // 10^6
	GB          = MB * 1000 // 10^9
	TB          = GB * 1000 // 10^12
	PB          = TB * 1000 // 10^15
	EB          = PB * 1000 // 10^18
	ZB          = EB * 1000 // 10^21
	YB          = ZB * 1000 // 10^24
)

var EInvalidByteSize = errors.New("invalid byte size")

func (b ByteSize) BitRate(d time.Duration) BitRate {
	return BitRate(float64(b) * 8 * float64(time.Second) / float64(d))
}

func (b ByteSize) String() string {
	switch {
	case b >= YB:
		return fmt.Sprintf("%.2fYiB", b/YiB)
	case b >= ZB:
		return fmt.Sprintf("%.2fZiB", b/ZiB)
	case b >= EB:
		return fmt.Sprintf("%.2fEiB", b/EiB)
	case b >= PB:
		return fmt.Sprintf("%.2fPiB", b/PiB)
	case b >= TB:
		return fmt.Sprintf("%.2fTiB", b/TiB)
	case b >= GB:
		return fmt.Sprintf("%.2fGiB", b/GiB)
	case b >= MB:
		return fmt.Sprintf("%.2fMiB", b/MiB)
	case b >= KB:
		return fmt.Sprintf("%.2fkiB", b/KiB)
	}
	return fmt.Sprintf("%.2fB", b)
}

func ParseByteSize(r string) (ByteSize, error) {
	var f ByteSize
	var pos int
	switch {
	case strings.HasSuffix(r, "YB"):
		f, pos = YB, len(r)-2
	case strings.HasSuffix(r, "YiB"):
		f, pos = YiB, len(r)-3
	case strings.HasSuffix(r, "ZB"):
		f, pos = ZB, len(r)-2
	case strings.HasSuffix(r, "ZiB"):
		f, pos = ZiB, len(r)-3
	case strings.HasSuffix(r, "EB"):
		f, pos = EB, len(r)-2
	case strings.HasSuffix(r, "EiB"):
		f, pos = EiB, len(r)-3
	case strings.HasSuffix(r, "PB"):
		f, pos = PB, len(r)-2
	case strings.HasSuffix(r, "PiB"):
		f, pos = PiB, len(r)-3
	case strings.HasSuffix(r, "TB"):
		f, pos = TB, len(r)-2
	case strings.HasSuffix(r, "TiB"):
		f, pos = TiB, len(r)-3
	case strings.HasSuffix(r, "GB"):
		f, pos = GB, len(r)-2
	case strings.HasSuffix(r, "GiB"):
		f, pos = GiB, len(r)-3
	case strings.HasSuffix(r, "MB"):
		f, pos = MB, len(r)-2
	case strings.HasSuffix(r, "MiB"):
		f, pos = MiB, len(r)-3
	case strings.HasSuffix(r, "kB"):
		f, pos = KB, len(r)-2
	case strings.HasSuffix(r, "kiB"):
		f, pos = KiB, len(r)-3
	case strings.HasSuffix(r, "B"):
		f, pos = 1, len(r)-1
	default:
		f, pos = 1, len(r)
	}
	if v, err := strconv.ParseFloat(r[:pos], 64); err != nil {
		return 0, EInvalidByteSize
	} else {
		return ByteSize(v) * f, nil
	}
}

func (r ByteSize) Int64() int64 {
	return int64(r)
}

func (r ByteSize) MarshalText() ([]byte, error) {
	return []byte(r.String()), nil
}

func (r *ByteSize) UnmarshalText(data []byte) error {
	if rr, err := ParseByteSize(string(data)); err != nil {
		return err
	} else {
		*r = rr
	}
	return nil
}
