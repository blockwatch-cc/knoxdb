// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package vec

import (
	"strings"
)

func matchStringsEqualGeneric(src []string, val string, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i, v := range src {
			bit := byte(0x1) << uint(7-i&0x7)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if strings.Compare(v, val) != 0 {
				continue
			}
			bits[i>>3] |= bit
			cnt++
		}
	} else {
		for i, v := range src {
			if strings.Compare(v, val) != 0 {
				continue
			}
			bits[i>>3] |= byte(0x1) << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchStringsNotEqualGeneric(src []string, val string, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i, v := range src {
			bit := byte(0x1) << uint(7-i&0x7)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if strings.Compare(v, val) == 0 {
				continue
			}
			bits[i>>3] |= bit
			cnt++
		}
	} else {
		for i, v := range src {
			if strings.Compare(v, val) == 0 {
				continue
			}
			bits[i>>3] |= byte(0x1) << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchStringsLessThanGeneric(src []string, val string, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i, v := range src {
			bit := byte(0x1) << uint(7-i&0x7)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if strings.Compare(v, val) >= 0 {
				continue
			}
			bits[i>>3] |= bit
			cnt++
		}
	} else {
		for i, v := range src {
			if strings.Compare(v, val) >= 0 {
				continue
			}
			bits[i>>3] |= byte(0x1) << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchStringsLessThanEqualGeneric(src []string, val string, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i, v := range src {
			bit := byte(0x1) << uint(7-i&0x7)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if strings.Compare(v, val) > 0 {
				continue
			}
			bits[i>>3] |= bit
			cnt++
		}
	} else {
		for i, v := range src {
			if strings.Compare(v, val) > 0 {
				continue
			}
			bits[i>>3] |= byte(0x1) << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchStringsGreaterThanGeneric(src []string, val string, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i, v := range src {
			bit := byte(0x1) << uint(7-i&0x7)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if strings.Compare(v, val) <= 0 {
				continue
			}
			bits[i>>3] |= bit
			cnt++
		}
	} else {
		for i, v := range src {
			if strings.Compare(v, val) <= 0 {
				continue
			}
			bits[i>>3] |= byte(0x1) << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchStringsGreaterThanEqualGeneric(src []string, val string, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i, v := range src {
			bit := byte(0x1) << uint(7-i&0x7)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if strings.Compare(v, val) < 0 {
				continue
			}
			bits[i>>3] |= bit
			cnt++
		}
	} else {
		for i, v := range src {
			if strings.Compare(v, val) < 0 {
				continue
			}
			bits[i>>3] |= byte(0x1) << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchStringsBetweenGeneric(src []string, a, b string, bits, mask []byte) int64 {
	// short-cut for empty min
	if len(a) == 0 {
		if mask != nil {
			copy(bits, mask)
		} else {
			bits[0] = 0xff
			for bp := 1; bp < len(bits); bp *= 2 {
				copy(bits[bp:], bits[:bp])
			}
			bits[len(bits)-1] &= bytemask(len(src))
		}
		return int64(len(src))
	}

	// make sure min/max are in correct order
	if d := strings.Compare(a, b); d < 0 {
		b, a = a, b
	} else if d == 0 {
		return matchStringsEqualGeneric(src, a, bits, mask)
	}

	var cnt int64
	if mask != nil {
		for i, v := range src {
			bit := byte(0x1) << uint(7-i&0x7)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if strings.Compare(v, a) < 0 {
				continue
			}
			if strings.Compare(v, b) > 0 {
				continue
			}
			bits[i>>3] |= bit
			cnt++
		}
	} else {
		for i, v := range src {
			if strings.Compare(v, a) < 0 {
				continue
			}
			if strings.Compare(v, b) > 0 {
				continue
			}
			bits[i>>3] |= byte(0x1) << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}
