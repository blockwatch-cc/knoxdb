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

type BitRate float64

const (
	_             = iota             // ignore first value by assigning to blank identifier
	Kibit BitRate = 1 << (10 * iota) // 1 << (10*1)
	Mibit                            // 1 << (10*2)
	Gibit                            // 1 << (10*3)
	Tibit                            // 1 << (10*4)
	Pibit                            // 1 << (10*5)
	Eibit                            // 1 << (10*6)
	Zibit                            // 1 << (10*7)
	Yibit                            // 1 << (10*8)
)

const (
	bit  BitRate = 1
	Kbit         = bit * 1000  // 10^3
	Mbit         = Kbit * 1000 // 10^6
	Gbit         = Mbit * 1000 // 10^9
	Tbit         = Gbit * 1000 // 10^12
	Pbit         = Tbit * 1000 // 10^15
	Ebit         = Pbit * 1000 // 10^18
	Zbit         = Ebit * 1000 // 10^21
	Ybit         = Zbit * 1000 // 10^24
)

var EInvalidBitrate = errors.New("invalid bitrate")

func (r BitRate) Size(d time.Duration) ByteSize {
	return ByteSize(float64(r) * (float64(d) / float64(time.Second)) / 8)
}

func ParseBitRate(r string) (BitRate, error) {
	var f BitRate
	var pos int
	switch {
	case strings.HasSuffix(r, "Ybit/s"):
		f, pos = Ybit, len(r)-6
	case strings.HasSuffix(r, "Yibit/s"):
		f, pos = Yibit, len(r)-7
	case strings.HasSuffix(r, "Zbit/s"):
		f, pos = Zbit, len(r)-6
	case strings.HasSuffix(r, "Zibit/s"):
		f, pos = Zibit, len(r)-7
	case strings.HasSuffix(r, "Ebit/s"):
		f, pos = Ebit, len(r)-6
	case strings.HasSuffix(r, "Eibit/s"):
		f, pos = Eibit, len(r)-7
	case strings.HasSuffix(r, "Pbit/s"):
		f, pos = Pbit, len(r)-6
	case strings.HasSuffix(r, "Pibit/s"):
		f, pos = Pibit, len(r)-7
	case strings.HasSuffix(r, "Tbit/s"):
		f, pos = Tbit, len(r)-6
	case strings.HasSuffix(r, "Tibit/s"):
		f, pos = Tibit, len(r)-7
	case strings.HasSuffix(r, "Gbit/s"):
		f, pos = Gbit, len(r)-6
	case strings.HasSuffix(r, "Gibit/s"):
		f, pos = Gibit, len(r)-7
	case strings.HasSuffix(r, "Mbit/s"):
		f, pos = Mbit, len(r)-6
	case strings.HasSuffix(r, "Mibit/s"):
		f, pos = Mibit, len(r)-7
	case strings.HasSuffix(r, "kbit/s"):
		f, pos = Kbit, len(r)-6
	case strings.HasSuffix(r, "kibit/s"):
		f, pos = Kibit, len(r)-7
	case strings.HasSuffix(r, "bit/s"):
		f, pos = 1, len(r)-5
	default:
		f, pos = 1, len(r)
	}
	if v, err := strconv.ParseFloat(r[:pos], 64); err != nil {
		return 0, EInvalidBitrate
	} else {
		return BitRate(v) * f, nil
	}
}

// Text/JSON conversion
func (r BitRate) String() string {
	switch {
	case r > Ybit:
		return fmt.Sprintf("%.2fYbit/s", r/Ybit)
	case r > Zbit:
		return fmt.Sprintf("%.2fZbit/s", r/Zbit)
	case r > Ebit:
		return fmt.Sprintf("%.2fEbit/s", r/Ebit)
	case r > Pbit:
		return fmt.Sprintf("%.2fPbit/s", r/Pbit)
	case r > Tbit:
		return fmt.Sprintf("%.2fTbit/s", r/Tbit)
	case r > Gbit:
		return fmt.Sprintf("%.2fGbit/s", r/Gbit)
	case r > Mbit:
		return fmt.Sprintf("%.2fMbit/s", r/Mbit)
	case r > Kbit:
		return fmt.Sprintf("%.2fkbit/s", r/Kbit)
	default:
		return fmt.Sprintf("%.2fbit/s", r)
	}
}

func (r BitRate) Int64() int64 {
	return int64(r)
}

func (r BitRate) MarshalText() ([]byte, error) {
	return []byte(r.String()), nil
}

func (r *BitRate) UnmarshalText(data []byte) error {
	if rr, err := ParseBitRate(string(data)); err != nil {
		return err
	} else {
		*r = rr
	}
	return nil
}
