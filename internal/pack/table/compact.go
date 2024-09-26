// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package table

import (
	"context"
	"math"
	"time"

	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/pkg/util"
)

// merges non-full packs to minimize total pack count, also re-establishes a
// sequential/gapless pack key order when packs have been deleted or records
// been inserted out of order
//
// TODO: writes result into new file (thus garbage collecting previous db file)
// when finished reopens the new db file
// TODO: what happens when indexes are stored in the same file?
func (t *Table) Compact(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	start := time.Now()

	// merge journal first
	if err := t.mergeJournal(ctx); err != nil {
		return err
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	// check if compaction is possible
	if t.meta.Len() <= 1 {
		return nil
	}

	// check if compaction is required, either because packs are non-sequential
	// or not full (except the last)
	var (
		maxsz                 int = t.opts.PackSize
		srcSize               int64
		nextpack              uint32
		needCompact           bool
		total, moved, written int64
		pending               int
	)
	srcPacks := t.meta.AllPacks()
	nSrcPacks := len(srcPacks)
	for i, v := range srcPacks {
		needCompact = needCompact || v.Key > nextpack                       // sequence gap
		needCompact = needCompact || (i < nSrcPacks-1 && v.NValues < maxsz) // non-full pack (except the last)
		nextpack++
		total += int64(v.NValues)
		srcSize += int64(v.StoredSize)
	}
	if !needCompact {
		t.log.Debugf("pack: %s table %d packs / %d rows already compact",
			t.name(), nSrcPacks, total)
		return nil
	}

	// check if compaction precondition is satisfied
	// - no out-of-order min/max ranges across sorted pack keys exist

	// open write transaction (or reuse existing tx)
	// tx, err := engine.GetTransaction(ctx).StoreTx(t.db, true)
	// if err != nil {
	// 	return err
	// }

	var (
		dstPack, srcPack *pack.Package
		dstSize          int64
		dstIndex         int
		lastMaxPk        uint64
		isNewPack        bool
		err              error
	)

	t.log.Debugf("pack: %s table compacting %d packs / %d rows",
		t.name(), nSrcPacks, total)
	// t.DumpPackInfoDetail(os.Stdout, DumpModeDec, false)

	// This algorithm walks the table's pack list in pack key order and
	// collects/compacts contents in row id (pk) order. Note that pk order may
	// differ from pack order if out-of-order inserts ever happened. In such case
	// this algorithm may abort or skip such packs to preserve the invariant
	// of non-overlapping pk ranges between packs.
	//
	// Gaps in pack key sequence are filled with new packs created on the fly.
	// When source packs are emptied during the process, they are immediatly removed
	// from KV storage and header list, but may be re-added subsequently.
	//
	for {
		// stop when no more dst packs are found
		if dstIndex == t.meta.Len() {
			break
		}

		// load next dst pack
		if dstPack == nil {
			dstKey := uint32(dstIndex)

			// handle existing pack keys
			if dstKey == srcPacks[dstIndex].Key {
				// skip full packs
				if srcPacks[dstIndex].NValues == maxsz {
					// log.Debugf("pack: skipping full dst pack key=%x", dstKey)
					dstIndex++
					continue
				}
				// skip out of order packs
				pmin, pmax := t.meta.MinMax(dstIndex)
				if pmin < lastMaxPk {
					// log.Debugf("pack: skipping out-of-order dst pack key=%x", dstKey)
					dstIndex++
					continue
				}

				// log.Debugf("pack: loading dst pack %d key=%x", dstIndex, dstKey)
				dstPack, err = t.loadWritablePack(ctx, dstKey, 0)
				if err != nil {
					return err
				}
				lastMaxPk = pmax
				isNewPack = false
			} else {
				// handle gaps in key sequence
				// clone new pack from journal
				// log.Debugf("pack: creating new dst pack %d key=%x", dstIndex, dstKey)
				dstPack = pack.New().
					WithKey(dstKey).
					WithSchema(t.schema).
					WithMaxRows(t.opts.PackSize).
					Alloc()
				isNewPack = true
			}
		}

		// search for the next src pack that
		// - has a larger key than the current destination pack AND
		// - has the smallest min pk higher than the current destination's max pk
		if srcPack == nil {
			minSlice, _ := t.meta.MinMaxSlices()
			var startIndex, srcIndex int = dstIndex, -1
			var lastmin uint64 = math.MaxUint64
			if isNewPack && startIndex > 0 {
				startIndex--
			}
			for i := startIndex; i < len(minSlice); i++ {
				if srcPacks[i].Key < dstPack.Key() {
					continue
				}
				currmin := minSlice[i]
				if currmin <= lastMaxPk {
					continue
				}
				if lastmin > currmin {
					lastmin = currmin
					srcIndex = i
				}
			}

			// stop when no more source pack was found
			if srcIndex < 0 {
				break
			}

			ph := srcPacks[srcIndex]
			// log.Debugf("pack: loading src pack %d key=%x", srcIndex, ph.Key)
			srcPack, err = t.loadWritablePack(ctx, ph.Key, ph.NValues)
			if err != nil {
				return err
			}
		}

		// Guarantees at this point:
		// - dstPack has free space
		// - srcPack is not empty

		// determine free space in destination
		free := maxsz - dstPack.Len()
		cp := min(free, srcPack.Len())
		moved += int64(cp)

		// move data from src to dst
		// log.Debugf("pack: moving %d/%d rows from pack %x to %x", cp, srcPack.Len(),
		//  srcPack.key, dstPack.key)
		if err := dstPack.AppendPack(srcPack, 0, cp); err != nil {
			return err
		}
		if err := srcPack.Delete(0, cp); err != nil {
			return err
		}
		total += int64(cp)
		lastMaxPk = dstPack.Uint64(t.pkindex, dstPack.Len()-1)
		if err != nil {
			return err
		}

		// write dst when full
		if dstPack.Len() == maxsz {
			// this may extend the pack header list when dstPack is new
			// log.Debugf("pack: storing full dst pack %x", dstPack.key)
			n, err := t.storePack(ctx, dstPack)
			if err != nil {
				return err
			}
			dstSize += int64(n)
			dstIndex++
			written += int64(maxsz)
			pending += n

			// will load or create another output pack in next iteration
			dstPack.Release()
			dstPack = nil
		}

		// if srcPack.Len() == 0 {
		//  log.Debugf("pack: deleting empty src pack %x", srcPack.key)
		// }

		// store or delete source pack
		n, err := t.storePack(ctx, srcPack)
		if err != nil {
			return err
		}
		pending += n

		// load new src in next iteration (or stop there)
		srcPack.Release()
		srcPack = nil

		// commit tx after each N written bytes
		// if pending >= t.opts.TxMaxSize {
		// 	if tx, err = store.CommitAndContinue(tx); err != nil {
		// 		return err
		// 	}
		// 	pending = 0
		// 	if err := ctx.Err(); err != nil {
		// 		return err
		// 	}
		// }
	}

	// store the last dstPack
	if dstPack != nil {
		// log.Debugf("pack: storing last dst pack %x", dstPack.key)
		n, err := t.storePack(ctx, dstPack)
		if err != nil {
			return err
		}
		dstSize += int64(n)
		written += int64(dstPack.Len())
		dstPack.Release()
	}

	t.log.Debugf("pack: %s table compacted %d(+%d) rows into %d(%d) packs (%s ->> %s) in %s",
		t.name(), moved, written-moved,
		t.meta.Len(), nSrcPacks-t.meta.Len(),
		util.ByteSize(srcSize), util.ByteSize(dstSize),
		time.Since(start),
	)
	// t.DumpPackInfoDetail(os.Stdout, DumpModeDec, false)

	return nil
}
