// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package vec

import (
	"time"
)

func matchTimeEqualGeneric(src []time.Time, val time.Time, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i, v := range src {
			bit := byte(0x1) << uint(7-i&0x7)
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
			bits[i>>3] |= byte(0x1) << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchTimeNotEqualGeneric(src []time.Time, val time.Time, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i, v := range src {
			bit := byte(0x1) << uint(7-i&0x7)
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
			bits[i>>3] |= byte(0x1) << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchTimeLessThanGeneric(src []time.Time, val time.Time, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i, v := range src {
			bit := byte(0x1) << uint(7-i&0x7)
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
			bits[i>>3] |= byte(0x1) << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchTimeLessThanEqualGeneric(src []time.Time, val time.Time, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i, v := range src {
			bit := byte(0x1) << uint(7-i&0x7)
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
			bits[i>>3] |= byte(0x1) << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchTimeGreaterThanGeneric(src []time.Time, val time.Time, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i, v := range src {
			bit := byte(0x1) << uint(7-i&0x7)
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
			bits[i>>3] |= byte(0x1) << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchTimeGreaterThanEqualGeneric(src []time.Time, val time.Time, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i, v := range src {
			bit := byte(0x1) << uint(7-i&0x7)
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
			bits[i>>3] |= byte(0x1) << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchTimeBetweenGeneric(src []time.Time, a, b time.Time, bits, mask []byte) int64 {
	if a.Before(b) {
		b, a = a, b
	} else if a.Equal(b) {
		return matchTimeEqualGeneric(src, a, bits, mask)
	}
	var cnt int64
	if mask != nil {
		for i, v := range src {
			bit := byte(0x1) << uint(7-i&0x7)
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
			bits[i>>3] |= byte(0x1) << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}
