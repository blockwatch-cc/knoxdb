// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//
// adapted from https://github.com/achille-roussel/kway-go
// MIT License, Copyright (c) 2023 Achille Roussel
package btree

import (
	"iter"
)

// buffer collects up to 128 key/value pairs into a batch
// which is pushed along the merge pipeline to save iter.Pull
// overhead costs.
func buffer[T any](seq iter.Seq2[T, T]) iter.Seq[[][2]T] {
	var buf [128][2]T
	return func(yield func([][2]T) bool) {
		n := 0

		for buf[n][0], buf[n][1] = range seq {
			if n++; n == len(buf) {
				if !yield(buf[:]) {
					return
				}
				n = 0
			}
		}

		if n > 0 {
			yield(buf[:n])
		}
	}
}

// unbuffer unpacks batches of key/value pairs to individual pairs.
// it is the reverse operation to buffer.
func unbuffer[T any](seq iter.Seq[[][2]T]) iter.Seq2[T, T] {
	return func(yield func(T, T) bool) {
		seq(func(values [][2]T) bool {
			for _, value := range values {
				if !yield(value[0], value[1]) {
					return false
				}
			}
			return true
		})
	}
}

// merge2 is a prioritizing dual-sequence merge algorithm. It uses batching
// to amortize baseline costs of iter.Pull and considers tombstones in seq0
// when merging. Any key/value pair in seq0 takes precedence over pairs in
// seq1. When seq0 contains a tombstone, no pair is outpot. When both seq0
// and seq1 contain the same key, the pair from seq0 is output.
func merge2[T any](cmp func([2]T, [2]T) (int, bool), seq0, seq1 iter.Seq[[][2]T]) iter.Seq[[][2]T] {
	return func(yield func([][2]T) bool) {
		next0, stop0 := iter.Pull(seq0)
		defer stop0()

		next1, stop1 := iter.Pull(seq1)
		defer stop1()

		values0, ok0 := next0()
		values1, ok1 := next1()

		var buffer [128][2]T
		offset := 0
		i0 := 0
		i1 := 0
		for ok0 && ok1 {
			for i0 < len(values0) && i1 < len(values1) {
				v0 := values0[i0]
				v1 := values1[i1]

				if (offset + 1) >= len(buffer) {
					if !yield(buffer[:offset]) {
						return
					}
					offset = 0
				}

				diff, isDel := cmp(v0, v1)
				switch {
				case diff < 0:
					// skip tombstones for non matching keys
					if !isDel {
						buffer[offset] = v0
						offset++
					}
					i0++
				case diff > 0:
					buffer[offset] = v1
					offset++
					i1++
				default:
					if !isDel {
						// keep the update, drop both on tombstones
						buffer[offset] = v0
						offset++
					}
					i0++
					i1++
				}
			}

			if i0 == len(values0) {
				i0 = 0
				values0, ok0 = next0()
			}

			if i1 == len(values1) {
				i1 = 0
				values1, ok1 = next1()
			}
		}

		if offset > 0 && !yield(buffer[:offset]) {
			return
		}

		values0 = values0[i0:]
		values1 = values1[i1:]

		for ok0 {
			// skip non-matching tombstones
			var j int
			for i := range values0 {
				if _, isDel := cmp(values0[i], values0[i]); !isDel {
					values0[j] = values0[i]
					j++
				}
			}
			values0 = values0[:j]
			if !yield(values0) {
				return
			}
			values0, ok0 = next0()
		}

		for ok1 && yield(values1) {
			values1, ok1 = next1()
		}
	}
}

// merge2r performs reverse order merging. it works similar to merge2 but
// instead of using the minimum of keys it uses the maximom to decide on
// the next output.
func merge2r[T any](cmp func([2]T, [2]T) (int, bool), seq0, seq1 iter.Seq[[][2]T]) iter.Seq[[][2]T] {
	return func(yield func([][2]T) bool) {
		next0, stop0 := iter.Pull(seq0)
		defer stop0()

		next1, stop1 := iter.Pull(seq1)
		defer stop1()

		values0, ok0 := next0()
		values1, ok1 := next1()

		var buffer [128][2]T
		offset := 0
		i0 := 0
		i1 := 0
		for ok0 && ok1 {
			for i0 < len(values0) && i1 < len(values1) {
				v0 := values0[i0]
				v1 := values1[i1]

				if (offset + 1) >= len(buffer) {
					if !yield(buffer[:offset]) {
						return
					}
					offset = 0
				}

				diff, isDel := cmp(v0, v1)
				switch {
				case diff > 0:
					// skip tombstones for non matching keys
					if !isDel {
						buffer[offset] = v0
						offset++
					}
					i0++
				case diff < 0:
					buffer[offset] = v1
					offset++
					i1++
				default:
					if !isDel {
						// keep the update, drop both on tombstones
						buffer[offset] = v0
						offset++
					}
					i0++
					i1++
				}
			}

			if i0 == len(values0) {
				i0 = 0
				values0, ok0 = next0()
			}

			if i1 == len(values1) {
				i1 = 0
				values1, ok1 = next1()
			}
		}

		if offset > 0 && !yield(buffer[:offset]) {
			return
		}

		values0 = values0[i0:]
		values1 = values1[i1:]

		for ok0 {
			// skip non-matching tombstones
			var j int
			for i := range values0 {
				if _, isDel := cmp(values0[i], values0[i]); !isDel {
					values0[j] = values0[i]
					j++
				}
			}
			values0 = values0[:j]
			if !yield(values0) {
				return
			}
			values0, ok0 = next0()
		}

		for ok1 && yield(values1) {
			values1, ok1 = next1()
		}
	}
}
