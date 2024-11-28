// Copyright (c) 2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package num

import (
	"fmt"
	"strconv"
	"strings"
)

var ZeroPercent = NewPercent(0, 1)

type Percent struct {
	Num int64 `json:"numerator,string"`
	Den int64 `json:"denominator,string"`
}

func NewPercent(n, d int64) Percent {
	return Percent{
		Num: n,
		Den: d,
	}
}

func (p Percent) IsZero() bool {
	return p.Den == 0 || p.Num == 0
}

func (p Percent) String() string {
	var v float64
	if p.Den > 0 {
		v = float64(p.Num) * 100.0 / float64(p.Den)
	}
	return strconv.FormatFloat(v, 'f', -1, 64) + "%"
}

func ParsePercent(s string) (Percent, error) {
	p := Percent{0, 1}
	a, b, ok := strings.Cut(s, "/")
	if !ok {
		return p, fmt.Errorf("invalid percentage %q", s)
	}
	num, err := strconv.ParseInt(a, 10, 64)
	if err != nil {
		return p, fmt.Errorf("invalid percentage %q", s)
	}
	den, err := strconv.ParseInt(b, 10, 64)
	if err != nil {
		return p, fmt.Errorf("invalid percentage %q", s)
	}
	return Percent{num, den}, nil
}

func (p *Percent) Set(s string) error {
	pp, err := ParsePercent(s)
	if err != nil {
		return err
	}
	*p = pp
	return nil
}

func (p Percent) Bps() Decimal32 {
	if p.Den == 0 {
		return NewDecimal32(0, 2)
	}
	return NewDecimal32(int32(1_000_000*p.Num/p.Den), 2)
}

func (p Percent) Half() Percent {
	return Percent{
		Num: p.Num * 5,
		Den: p.Den * 10,
	}
}

func (p Percent) Mul(z Big) Big {
	num, den := p.Num, p.Den
	if den <= 0 {
		den = 1
	}
	return z.Mul64(num).Div64(den)
}
