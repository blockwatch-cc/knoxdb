// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"time"
)

func MatchTimeEqual(src []time.Time, val time.Time, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i, v := range src {
			bit := bitmask(i)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if !v.Equal(val) {
				continue
			}
			bits[i>>3] |= bit
			cnt++
		}
	} else {
		for i, v := range src {
			if !v.Equal(val) {
				continue
			}
			bits[i>>3] |= bitmask(i)
			cnt++
		}
	}
	return cnt
}

func MatchTimeNotEqual(src []time.Time, val time.Time, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i, v := range src {
			bit := bitmask(i)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if v.Equal(val) {
				continue
			}
			bits[i>>3] |= bit
			cnt++
		}
	} else {
		for i, v := range src {
			if v.Equal(val) {
				continue
			}
			bits[i>>3] |= bitmask(i)
			cnt++
		}
	}
	return cnt
}

func MatchTimeLess(src []time.Time, val time.Time, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i, v := range src {
			bit := bitmask(i)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if !v.Before(val) {
				continue
			}
			bits[i>>3] |= bit
			cnt++
		}
	} else {
		for i, v := range src {
			if !v.Before(val) {
				continue
			}
			bits[i>>3] |= bitmask(i)
			cnt++
		}
	}
	return cnt
}

func MatchTimeLessEqual(src []time.Time, val time.Time, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i, v := range src {
			bit := bitmask(i)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if val.After(v) {
				continue
			}
			bits[i>>3] |= bit
			cnt++
		}
	} else {
		for i, v := range src {
			if val.After(v) {
				continue
			}
			bits[i>>3] |= bitmask(i)
			cnt++
		}
	}
	return cnt
}

func MatchTimeGreater(src []time.Time, val time.Time, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i, v := range src {
			bit := bitmask(i)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if !v.After(val) {
				continue
			}
			bits[i>>3] |= bit
			cnt++
		}
	} else {
		for i, v := range src {
			if !v.After(val) {
				continue
			}
			bits[i>>3] |= bitmask(i)
			cnt++
		}
	}
	return cnt
}

func MatchTimeGreaterEqual(src []time.Time, val time.Time, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i, v := range src {
			bit := bitmask(i)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if val.Before(v) {
				continue
			}
			bits[i>>3] |= bit
			cnt++
		}
	} else {
		for i, v := range src {
			if val.Before(v) {
				continue
			}
			bits[i>>3] |= bitmask(i)
			cnt++
		}
	}
	return cnt
}

func MatchTimeBetween(src []time.Time, a, b time.Time, bits, mask []byte) int64 {
	if a.Before(b) {
		b, a = a, b
	} else if a.Equal(b) {
		return MatchTimeEqual(src, a, bits, mask)
	}
	var cnt int64
	if mask != nil {
		for i, v := range src {
			bit := bitmask(i)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if v.Before(a) {
				continue
			}
			if v.After(b) {
				continue
			}
			bits[i>>3] |= bit
			cnt++
		}
	} else {
		for i, v := range src {
			if v.Before(a) {
				continue
			}
			if v.After(b) {
				continue
			}
			bits[i>>3] |= bitmask(i)
			cnt++
		}
	}
	return cnt
}
