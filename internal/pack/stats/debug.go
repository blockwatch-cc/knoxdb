// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package stats

import (
	"fmt"
)

func (s StatsIndex) Validate() []error {
	errs := make([]error, 0)
	for i := range s.packs {
		head := s.packs[i]
		if head.NValues == 0 {
			errs = append(errs, fmt.Errorf("%03d empty pack", head.Key))
		}
		// check min <= max
		min, max := s.minpks[i], s.maxpks[i]
		if min > max {
			errs = append(errs, fmt.Errorf("%03d min %d > max %d", head.Key, min, max))
		}
		// check invariant
		// - id's don't overlap between packs
		// - same key can span many packs, so min_a == max_b
		// - for long rows of same keys min_a == max_a
		for j := range s.packs {
			if i == j {
				continue
			}
			jmin, jmax := s.minpks[j], s.maxpks[j]
			dist := jmax - jmin + 1

			// single key packs are allowed
			if min == max {
				// check the signle key is not between any other pack (exclusing)
				if jmin < min && jmax > max {
					errs = append(errs, fmt.Errorf("%03d overlaps %03d - key %d E [%d:%d]",
						head.Key, s.packs[j].Key, min, jmin, jmax))
				}
			} else {
				// check min val is not contained in any other pack unless continued
				if min != jmin && min != jmax && min-jmin < dist {
					errs = append(errs, fmt.Errorf("%03d overlaps %03d - min %d E [%d:%d]",
						head.Key, s.packs[j].Key, min, jmin, jmax))
				}

				// check max val is not contained in any other pack unless continued
				if max != jmin && max-jmin < dist {
					errs = append(errs, fmt.Errorf("%03d overlaps %03d - max %d E [%d:%d]",
						head.Key, s.packs[j].Key, max, jmin, jmax))
				}
			}
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return errs
}
