// Copyright 2016 by the roaring authors.
// Licensed under the Apache License, Version 2.0.
// Full version of the license is here:
// https://github.com/RoaringBitmap/roaring/blob/master/LICENSE

package xroar

// TODO: Add license from roaring bitmap library.

func difference(set1 []uint16, set2 []uint16, buffer []uint16) int {
	if len(set2) == 0 {
		buffer = buffer[:len(set1)]
		copy(buffer, set1)
		return len(set1)
	}
	if len(set1) == 0 {
		return 0
	}
	pos := 0
	k1 := 0
	k2 := 0
	buffer = buffer[:cap(buffer)]
	s1 := set1[k1]
	s2 := set2[k2]
forloop:
	for {
		switch {
		case s1 < s2:
			buffer[pos] = s1
			pos++
			k1++
			if k1 >= len(set1) {
				break forloop
			}
			s1 = set1[k1]
		case s1 == s2:
			k1++
			k2++
			if k1 >= len(set1) {
				break forloop
			}
			s1 = set1[k1]
			if k2 >= len(set2) {
				for ; k1 < len(set1); k1++ {
					buffer[pos] = set1[k1]
					pos++
				}
				break forloop
			}
			s2 = set2[k2]
		default: // if (val1>val2)
			k2++
			if k2 >= len(set2) {
				for ; k1 < len(set1); k1++ {
					buffer[pos] = set1[k1]
					pos++
				}
				break forloop
			}
			s2 = set2[k2]
		}
	}
	return pos
}

func intersection2by2(
	set1 []uint16,
	set2 []uint16,
	buffer []uint16) int {

	switch {
	case len(set1)*64 < len(set2):
		return onesidedgallopingintersect2by2(set1, set2, buffer)
	case len(set2)*64 < len(set1):
		return onesidedgallopingintersect2by2(set2, set1, buffer)
	default:
		return localintersect2by2(set1, set2, buffer)
	}
}

func localintersect2by2(
	set1 []uint16,
	set2 []uint16,
	buffer []uint16) int {

	if (len(set1) == 0) || (len(set2) == 0) {
		return 0
	}
	k1 := 0
	k2 := 0
	pos := 0
	buffer = buffer[:cap(buffer)]
	s1 := set1[k1]
	s2 := set2[k2]
mainwhile:
	for {
		if s2 < s1 {
			for {
				k2++
				if k2 == len(set2) {
					break mainwhile
				}
				s2 = set2[k2]
				if s2 >= s1 {
					break
				}
			}
		}
		if s1 < s2 {
			for {
				k1++
				if k1 == len(set1) {
					break mainwhile
				}
				s1 = set1[k1]
				if s1 >= s2 {
					break
				}
			}

		} else {
			// (set2[k2] == set1[k1])
			buffer[pos] = s1
			pos++
			k1++
			if k1 == len(set1) {
				break
			}
			s1 = set1[k1]
			k2++
			if k2 == len(set2) {
				break
			}
			s2 = set2[k2]
		}
	}
	return pos
}

func advanceUntil(
	array []uint16,
	pos int,
	length int,
	min uint16) int {
	lower := pos + 1

	if lower >= length || array[lower] >= min {
		return lower
	}

	spansize := 1

	for lower+spansize < length && array[lower+spansize] < min {
		spansize *= 2
	}
	var upper int
	if lower+spansize < length {
		upper = lower + spansize
	} else {
		upper = length - 1
	}

	if array[upper] == min {
		return upper
	}

	if array[upper] < min {
		// means
		// array
		// has no
		// item
		// >= min
		// pos = array.length;
		return length
	}

	// we know that the next-smallest span was too small
	lower += (spansize >> 1)

	mid := 0
	for lower+1 != upper {
		mid = (lower + upper) >> 1
		switch {
		case array[mid] == min:
			return mid
		case array[mid] < min:
			lower = mid
		default:
			upper = mid
		}
	}
	return upper

}

func onesidedgallopingintersect2by2(
	smallset []uint16,
	largeset []uint16,
	buffer []uint16) int {

	if len(smallset) == 0 {
		return 0
	}
	buffer = buffer[:cap(buffer)]
	k1 := 0
	k2 := 0
	pos := 0
	s1 := largeset[k1]
	s2 := smallset[k2]
mainwhile:

	for {
		if s1 < s2 {
			k1 = advanceUntil(largeset, k1, len(largeset), s2)
			if k1 == len(largeset) {
				break mainwhile
			}
			s1 = largeset[k1]
		}
		if s2 < s1 {
			k2++
			if k2 == len(smallset) {
				break mainwhile
			}
			s2 = smallset[k2]
		} else {

			buffer[pos] = s2
			pos++
			k2++
			if k2 == len(smallset) {
				break
			}
			s2 = smallset[k2]
			k1 = advanceUntil(largeset, k1, len(largeset), s2)
			if k1 == len(largeset) {
				break mainwhile
			}
			s1 = largeset[k1]
		}

	}
	return pos
}

func union2by2(set1 []uint16, set2 []uint16, buffer []uint16) int {
	pos := 0
	k1 := 0
	k2 := 0
	if len(set2) == 0 {
		buffer = buffer[:len(set1)]
		copy(buffer, set1)
		return len(set1)
	}
	if len(set1) == 0 {
		buffer = buffer[:len(set2)]
		copy(buffer, set2)
		return len(set2)
	}
	s1 := set1[k1]
	s2 := set2[k2]
	buffer = buffer[:cap(buffer)]
forloop:
	for {
		switch {
		case s1 < s2:
			buffer[pos] = s1
			pos++
			k1++
			if k1 >= len(set1) {
				copy(buffer[pos:], set2[k2:])
				pos += len(set2) - k2
				break forloop
			}
			s1 = set1[k1]
		case s1 == s2:
			buffer[pos] = s1
			pos++
			k1++
			k2++
			if k1 >= len(set1) {
				copy(buffer[pos:], set2[k2:])
				pos += len(set2) - k2
				break forloop
			}
			if k2 >= len(set2) {
				copy(buffer[pos:], set1[k1:])
				pos += len(set1) - k1
				break forloop
			}
			s1 = set1[k1]
			s2 = set2[k2]
		default: // if (set1[k1]>set2[k2])
			buffer[pos] = s2
			pos++
			k2++
			if k2 >= len(set2) {
				copy(buffer[pos:], set1[k1:])
				pos += len(set1) - k1
				break forloop
			}
			s2 = set2[k2]
		}
	}
	return pos
}
