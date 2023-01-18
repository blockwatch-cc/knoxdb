// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"fmt"
	"io"
)

func (p *Package) Validate() error {
	if a, b := len(p.fields), p.nFields; a != b {
		return fmt.Errorf("pack: mismatch len %d/%d", a, b)
	}
	if a, b := len(p.fields), len(p.blocks); a != b {
		return fmt.Errorf("pack: mismatch block len %d/%d", a, b)
	}

	for i, f := range p.fields {
		b := p.blocks[i]
		if a, b := b.Type(), f.Type.BlockType(); a != b {
			return fmt.Errorf("pack: mismatch block type %s/%s", a, b)
		}
		if a, b := p.nValues, b.Len(); a != b {
			return fmt.Errorf("pack: mismatch block len %d/%d", a, b)
		}
	}
	return nil
}

func (l PackIndex) Validate() []error {
	errs := make([]error, 0)
	for i := range l.packs {
		head := l.packs[i]
		if head.NValues == 0 {
			errs = append(errs, fmt.Errorf("%03d empty pack", head.Key))
		}
		// check min <= max
		min, max := l.minpks[i], l.maxpks[i]
		if min > max {
			errs = append(errs, fmt.Errorf("%03d min %d > max %d", head.Key, min, max))
		}
		// check invariant
		// - id's don't overlap between packs
		// - same key can span many packs, so min_a == max_b
		// - for long rows of same keys min_a == max_a
		for j := range l.packs {
			if i == j {
				continue
			}
			jmin, jmax := l.minpks[j], l.maxpks[j]
			dist := jmax - jmin + 1

			// single key packs are allowed
			if min == max {
				// check the signle key is not between any other pack (exclusing)
				if jmin < min && jmax > max {
					errs = append(errs, fmt.Errorf("%03d overlaps %03d - key %d E [%d:%d]",
						head.Key, l.packs[j].Key, min, jmin, jmax))
				}
			} else {
				// check min val is not contained in any other pack unless continued
				if min != jmin && min != jmax && min-jmin < dist {
					errs = append(errs, fmt.Errorf("%03d overlaps %03d - min %d E [%d:%d]",
						head.Key, l.packs[j].Key, min, jmin, jmax))
				}

				// check max val is not contained in any other pack unless continued
				if max != jmin && max-jmin < dist {
					errs = append(errs, fmt.Errorf("%03d overlaps %03d - max %d E [%d:%d]",
						head.Key, l.packs[j].Key, max, jmin, jmax))
				}
			}
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return errs
}

func (t *Table) ValidatePackIndex(w io.Writer) error {
	if errs := t.packidx.Validate(); errs != nil {
		for _, v := range errs {
			w.Write([]byte(v.Error() + "\n"))
		}
	}
	return nil
}

func (t *Table) ValidateIndexPackIndex(w io.Writer) error {
	for _, idx := range t.indexes {
		if errs := idx.packidx.Validate(); errs != nil {
			for _, v := range errs {
				w.Write([]byte(fmt.Sprintf("%s: %v\n", idx.Name, v.Error())))
			}
		}
	}
	return nil
}
