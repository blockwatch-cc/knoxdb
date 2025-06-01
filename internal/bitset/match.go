// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bitset

func (s *Bitset) Matcher() BitmapMatcher {
	return s
}

func (s *Bitset) Writer() *Bitset {
	return s
}

func (s *Bitset) MatchEqual(val bool, bits, _ *Bitset) {
	bits.Copy(s)
	if !val {
		bits.Neg()
	}
}

func (s *Bitset) MatchNotEqual(val bool, bits, _ *Bitset) {
	s.MatchEqual(!val, bits, nil)
}

func (s *Bitset) MatchLess(val bool, bits, _ *Bitset) {
	if val {
		s.MatchEqual(false, bits, nil)
	}
}

func (s *Bitset) MatchLessEqual(val bool, bits, _ *Bitset) {
	if val {
		bits.One()
	} else {
		s.MatchEqual(false, bits, nil)
	}
}

func (s *Bitset) MatchGreater(val bool, bits, _ *Bitset) {
	if !val {
		s.MatchEqual(true, bits, nil)
	}
}

func (s *Bitset) MatchGreaterEqual(val bool, bits, _ *Bitset) {
	if !val {
		bits.One()
	} else {
		s.MatchEqual(true, bits, nil)
	}
}

func (s *Bitset) MatchBetween(a, b bool, bits, _ *Bitset) {
	switch {
	case a && b:
		s.MatchEqual(true, bits, nil)
	case !a && b:
		bits.One()
	case !a && !b:
		s.MatchEqual(false, bits, nil)
	}
}
