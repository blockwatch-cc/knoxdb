// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// TODO
// - complex predicates "JOIN ON a.f = b.f AND a.id = b.id"
// - scalar predicates  "JOIN ON a.f = 42"
// - GetTotalRowCount() for all join types

package pack

import (
	"context"
	"fmt"
	"strings"

	"blockwatch.cc/knoxdb/util"
)

type JoinType int

const (
	InnerJoin  JoinType = iota // INNER JOIN (maybe EQUI JOIN)
	LeftJoin                   // LEFT OUTER JOIN
	RightJoin                  // RIGHT OUTER JOIN
	FullJoin                   // FULL OUTER JOIN
	CrossJoin                  // CROSS JOIN
	SelfJoin                   // unused
	AsOfJoin                   // see https://code.kx.com/q4m3/9_Queries_q-sql/#998-as-of-joins
	WindowJoin                 // see https://code.kx.com/q4m3/9_Queries_q-sql/#999-window-join
)

func (t JoinType) String() string {
	switch t {
	case InnerJoin:
		return "inner_join"
	case LeftJoin:
		return "left_join"
	case RightJoin:
		return "right_join"
	case FullJoin:
		return "full_join"
	case CrossJoin:
		return "cross_join"
	case SelfJoin:
		return "self_join"
	case AsOfJoin:
		return "as_of_join"
	case WindowJoin:
		return "window_join"
	default:
		return "invalid_join"
	}
}

type JoinTable struct {
	Table    *Table
	Where    ConditionList   // optional filters for table rows
	Fields   FieldList       // list of output fields, in order
	FieldsAs util.StringList // alias names of output fields, in order
	Limit    int             // individual table scan limit
}

type Join struct {
	Type      JoinType
	Predicate BinaryCondition
	Left      JoinTable
	Right     JoinTable

	// compiled data below
	limit   int
	fields  FieldList         // ordered list of output columns
	aliases map[string]string // query to output field translation
}

func (j Join) Fields() FieldList {
	return j.fields
}

func (j Join) IsEquiJoin() bool {
	return j.Predicate.Mode == FilterModeEqual
}

// check pre-conditions
func (j Join) Check() error {
	// join type is valid
	if j.Type < InnerJoin || j.Type > WindowJoin {
		return fmt.Errorf("pack: invalid join type %d", j.Type)
	}

	// predicate mode is allowed, fields are valid and have same type
	if err := j.Predicate.Check(); err != nil {
		return err
	}

	// tables are valid
	if j.Left.Table == nil {
		return fmt.Errorf("pack: left join table is nil")
	}
	if j.Right.Table == nil {
		return fmt.Errorf("pack: right join table is nil")
	}

	// table fields and alias list has same number of items
	if f, a := len(j.Left.Fields), len(j.Left.FieldsAs); f != a && a != 0 {
		return fmt.Errorf("pack: left join table has %d fields and %d alias names", f, a)
	}
	if f, a := len(j.Right.Fields), len(j.Right.FieldsAs); f != a && a != 0 {
		return fmt.Errorf("pack: right join table has %d fields and %d alias names", f, a)
	}

	// predicate uses fields from defined tables (i.e. fields exist)
	lfields := j.Left.Table.Fields()
	lname := j.Left.Table.Name()
	rfields := j.Right.Table.Fields()
	rname := j.Right.Table.Name()
	if !lfields.Contains(j.Predicate.Left.Name) {
		return fmt.Errorf("pack: missing predicate field '%s' in left table '%s'",
			j.Predicate.Left.Name, lname)
	}
	if !rfields.Contains(j.Predicate.Right.Name) {
		return fmt.Errorf("pack: missing predicate field '%s' in right table '%s'",
			j.Predicate.Right.Name, rname)
	}

	// output fields exist
	for _, v := range j.Left.Fields {
		// field must exist
		if !lfields.Contains(v.Name) {
			return fmt.Errorf("pack: undefined field '%s.%s' used in join output",
				lname, v.Name)
		}
		// field type must match
		if lfields.Find(v.Name).Type != v.Type {
			return fmt.Errorf("pack: mismatched type %s for field '%s.%s' used in join output",
				v.Type, lname, v.Name)
		}
		// field index must be valid
		if v.Index < 0 || v.Index >= len(lfields) {
			return fmt.Errorf("pack: illegal index %d for field '%s.%s' used in join output",
				v.Index, lname, v.Name)
		}
	}

	for _, v := range j.Right.Fields {
		// field must exist
		if !rfields.Contains(v.Name) {
			return fmt.Errorf("pack: undefined field '%s.%s' used in join output",
				rname, v.Name)
		}
		// field type must match
		if rfields.Find(v.Name).Type != v.Type {
			return fmt.Errorf("pack: mismatched type %s for field '%s.%s' used in join output",
				v.Type, rname, v.Name)
		}
		// field index must be valid
		if v.Index < 0 || v.Index >= len(lfields) {
			return fmt.Errorf("pack: illegal index %d for field '%s.%s' used in join output",
				v.Index, rname, v.Name)
		}
	}

	// where conditions are valid if set
	for i, c := range j.Left.Where {
		if err := j.Left.Where[i].Check(); err != nil {
			return fmt.Errorf("pack: invalid cond %d in join field '%s.%s': %v",
				i, lname, c.Field.Name, err)
		}
		// field must exist
		if !lfields.Contains(c.Field.Name) {
			return fmt.Errorf("pack: undefined field '%s.%s' used in join cond %d",
				lname, c.Field.Name, i)
		}
		// field type must match
		if lfields.Find(c.Field.Name).Type != c.Field.Type {
			return fmt.Errorf("pack: mismatched type %s for field '%s.%s' used in join cond %d",
				c.Field.Type, lname, c.Field.Name, i)
		}
		// field index must be valid
		if c.Field.Index < 0 || c.Field.Index >= len(lfields) {
			return fmt.Errorf("pack: illegal index %d for field '%s.%s' used in join cond %d",
				c.Field.Index, lname, c.Field.Name, i)
		}
	}

	for i, c := range j.Right.Where {
		if err := j.Right.Where[i].Check(); err != nil {
			return fmt.Errorf("pack: invalid cond %d in join field '%s.%s': %v",
				i, rname, c.Field.Name, err)
		}
		// field must exist
		if !rfields.Contains(c.Field.Name) {
			return fmt.Errorf("pack: undefined field '%s.%s' used in join cond %d",
				rname, c.Field.Name, i)
		}
		// field type must match
		if rfields.Find(c.Field.Name).Type != c.Field.Type {
			return fmt.Errorf("pack: mismatched type %s for field '%s.%s' used in join cond %d",
				c.Field.Type, rname, c.Field.Name, i)
		}
		// field index must be valid
		if c.Field.Index < 0 || c.Field.Index >= len(rfields) {
			return fmt.Errorf("pack: illegal index %d for field '%s.%s' used in join cond %d",
				c.Field.Index, rname, c.Field.Name, i)
		}
	}
	return nil
}

// compile output field list and where conditions
func (j *Join) Compile() error {
	// run only once
	if len(j.fields) > 0 {
		return nil
	}

	if err := j.Check(); err != nil {
		return err
	}

	j.aliases = make(map[string]string)

	// use all table fields when none are defined
	if len(j.Left.Fields) == 0 {
		j.Left.Fields = j.Left.Table.Fields()
	}
	if len(j.Right.Fields) == 0 {
		j.Right.Fields = j.Right.Table.Fields()
	}

	// assemble output field list from left and right table fields
	// default output names are of form {table_name}.{field_name}
	for i, v := range j.Left.Fields {
		joinname := j.Left.Table.Name() + "." + v.Name
		alias := joinname
		if len(j.Left.FieldsAs) > i+1 {
			alias = j.Left.FieldsAs[i]
		}
		// save alias mapping (original to output name conversion)
		j.aliases[joinname] = alias

		// save output field
		j.fields = append(j.fields, Field{
			Index: len(j.fields), // position in output list
			Name:  joinname,      // table name + original field name from source table
			Alias: alias,         // joinname or user-defined alias
			Type:  v.Type,        // original type from source table
			Flags: 0,             // strip all flags (note: packs will have no Pk!)
		})
	}

	for i, v := range j.Right.Fields {
		joinname := j.Right.Table.Name() + "." + v.Name
		alias := joinname
		if len(j.Right.FieldsAs) > i+1 {
			alias = j.Right.FieldsAs[i]
		}

		// save alias mapping (original to output name conversion)
		j.aliases[joinname] = alias

		// save output field
		j.fields = append(j.fields, Field{
			Index: len(j.fields), // position in output list
			Name:  joinname,      // table name + original field name from source table
			Alias: alias,         // joinname or user-defined alias
			Type:  v.Type,        // original type from source table
			Flags: 0,             // strip all flags (note: packs will have no Pk!)
		})
	}
	return nil
}

func (j Join) AppendResult(out, left *Package, l int, right *Package, r int) error {
	if err := out.Grow(1); err != nil {
		return err
	}
	ins := out.Len() - 1
	if left != nil {
		for i, v := range j.Left.Fields {
			f, err := left.FieldAt(v.Index, l)
			if err != nil {
				return err
			}
			if err := out.SetFieldAt(i, ins, f); err != nil {
				return err
			}
		}
	}
	offs := len(j.Left.Fields)
	if right != nil {
		for i, v := range j.Right.Fields {
			f, err := right.FieldAt(v.Index, r)
			if err != nil {
				return err
			}
			if err := out.SetFieldAt(i+offs, ins, f); err != nil {
				return err
			}
		}
	}
	return nil
}

// TODO
func (j Join) Stream(ctx context.Context, q Query, fn func(r Row) error) error {
	return nil
}

func (j Join) Query(ctx context.Context, q Query) (*Result, error) {
	// ------------------------------------------------------------
	// PREPARE
	// ------------------------------------------------------------

	// generate output field mapping (Note: ptr receiver, will change j)
	if err := j.Compile(); err != nil {
		return nil, err
	}

	// check and compile query using a temporary in-memory table without indexes
	if err := q.Compile(&Table{
		name: strings.Join([]string{
			j.Type.String(),
			j.Left.Table.Name(),
			j.Right.Table.Name(),
			"on",
			j.Predicate.Left.Name,
			j.Predicate.Mode.String(),
			j.Predicate.Right.Name,
		}, "."),
		fields: j.fields,
	}); err != nil {
		return nil, err
	}
	defer q.Close()

	// limit join to q.Limit when q has no extra conditions, otherwise the limit
	// is used in post-processing the joined table
	havePostFilter := len(q.Conditions) > 0
	if !havePostFilter {
		j.limit = q.Limit
	}

	// out is the final result to be returned, agg is an intermediate result
	// to collect potential candidate rows for post filter
	var out, agg *Result
	maxPackSize := 1 << defaultPackSizeLog2

	// Note: result is not owned by any table, so pkg will not be recycled
	out = &Result{
		fields: j.fields,
		pkg:    NewPackage(util.NonZero(q.Limit, maxPackSize)),
	}
	if err := out.pkg.InitFields(j.fields, nil); err != nil {
		return nil, err
	}

	if havePostFilter {
		agg = &Result{
			fields: j.fields,
		}
		pkg, err := out.pkg.Clone(false, util.NonZero(j.limit, maxPackSize))
		if err != nil {
			return nil, err
		}
		agg.pkg = pkg

		defer agg.Close()
	} else {
		// without post filter we can directly collect result rows into out
		agg = out
	}

	// ------------------------------------------------------------
	// PROCESS
	// ------------------------------------------------------------
	// Algorithm description
	//
	// Fetches join candidates from both tables, joins and post-processes them.
	// To handle very large tables this algo iterates in blocks, fetching one pack
	// of candidate rows at a time and stops when the requested output row limit
	// is reached or a join operation did not produce any results. This also means
	// that underlying tables are potentially queried multiple times for one join
	// query to complete.

	// determine query order (L-R or R-L) based on
	// - join type
	//   - RIGHT: R first, then add IN cond to L
	//   - FULL: needs different algo design !!!
	//   - otherwise: depends on limits
	// - limits
	//   - both have limits: L first, then add IN cond to R
	//   - only left limit: L first, then add IN cond to R
	//   - only right limit: R first, then add IN cond to L
	//   - no limit: expensive, but pick L first (FIXME: need query/join loop here)
	//
	queryOrderLR := !(j.Type == RightJoin || j.Type == FullJoin || j.Left.Limit == 0 && j.Right.Limit > 0)
	var (
		lRes, rRes *Result
		err        error
	)
	defer func() {
		if lRes != nil {
			lRes.Close()
		}
		if rRes != nil {
			rRes.Close()
		}
	}()

	// use row_id as an extra cursor to fetch a new block of matching rows
	var pkcursor uint64
	for {
		// ------------------------------------------------------------
		// QUERY
		// ------------------------------------------------------------
		if queryOrderLR {
			// query the left table first (ensure predicate column is returned)
			lQ := Query{
				Name:       q.Name + ".join_left",
				Fields:     j.Left.Fields.AddUnique(j.Predicate.Left),
				Conditions: j.Left.Where,
				Limit:      util.NonZeroMin(j.Left.Limit, maxPackSize),
			}
			if pkcursor > 0 {
				// FIXME: optimize/merge conditions (there may already exist one
				// or more conditions for the pk column)
				lQ.Conditions = append(lQ.Conditions, Condition{
					Field: j.Left.Fields.Pk(),
					Mode:  FilterModeGt,
					Value: pkcursor,
					Raw:   "left_join_cursor",
				})
			}
			// log.Debugf("join: left table query with %d cond, cursor=%d limit=%d",
			// 	len(lQ.Conditions), pkcursor, lQ.Limit)
			// for i, c := range lQ.Conditions {
			// 	log.Debugf("cond %d: %s", i, c.String())
			// }
			lRes, err = j.Left.Table.Query(ctx, lQ)
			if err != nil {
				return nil, err
			}
			// log.Debugf("join: left table result %d rows", lRes.Rows())

			// return result when no more rows are found
			if lRes.Rows() == 0 {
				// log.Debugf("join: final result contains %d rows", out.Rows())
				return out, nil
			}

			// set cursor to pk of last result row
			pkcursor, err = lRes.pkg.Uint64At(lRes.Fields().PkIndex(), lRes.Rows()-1)
			if err != nil {
				log.Errorf("join: no pk column in query result %s: %v", lQ.Name, err)
				return nil, err
			}

			// use right in-condition only for equi joins
			rConds := j.Right.Where
			if j.IsEquiJoin() && lRes.Rows() > 0 {
				// get a copy of the left predicate column
				lPredCol, err := lRes.Column(j.Predicate.Left.Name)
				if err != nil {
					return nil, err
				}
				lPredColCopy, err := j.Predicate.Left.Type.CopySliceType(lPredCol)
				if err != nil {
					return nil, err
				}
				// use left predicate field values as additional IN condition
				rConds = append(rConds, Condition{
					Field:    j.Predicate.Right,
					Mode:     FilterModeIn,
					Value:    lPredColCopy,
					IsSorted: j.Predicate.Left.Flags&FlagPrimary > 0, // slice will be sorted when not pk
					Raw:      "join_predicate." + j.Left.Table.Name() + "." + j.Predicate.Left.Name,
				})
			}

			// query the right table
			rQ := Query{
				Name:       q.Name + ".join_right",
				Fields:     j.Right.Fields.AddUnique(j.Predicate.Right),
				Conditions: rConds,
				// Limit:      j.Right.Limit, // no limit
			}
			// log.Debugf("join: right table query with %d cond and limit %d", len(rQ.Conditions), rQ.Limit)
			// for i, c := range rQ.Conditions {
			// 	log.Debugf("cond %d: %s", i, c.String())
			// }

			rRes, err = j.Right.Table.Query(ctx, rQ)
			if err != nil {
				return nil, err
			}
			// log.Debugf("join: right table result %d rows", rRes.Rows())

		} else {
			// query the right table first (ensure predicate column is returned)
			rQ := Query{
				Name:       q.Name + ".join_right",
				Fields:     j.Right.Fields.AddUnique(j.Predicate.Right),
				Conditions: j.Right.Where,
				Limit:      util.NonZeroMin(j.Right.Limit, maxPackSize),
			}
			if pkcursor > 0 {
				// FIXME: optimize/merge conditions (there may already exist one
				// or more conditions for the pk column)
				rQ.Conditions = append(rQ.Conditions, Condition{
					Field: j.Right.Fields.Pk(),
					Mode:  FilterModeGt,
					Value: pkcursor,
					Raw:   "right_join_cursor",
				})
			}
			// log.Debugf("join: right table query with %d cond, cursor=%d limit=%d",
			// 	len(rQ.Conditions), pkcursor, rQ.Limit)
			// for i, c := range rQ.Conditions {
			// 	log.Debugf("cond %d: %s", i, c.String())
			// }
			rRes, err = j.Right.Table.Query(ctx, rQ)
			if err != nil {
				return nil, err
			}
			// log.Debugf("join: right table result %d rows", rRes.Rows())

			// return result when no more rows are found
			if rRes.Rows() == 0 {
				// log.Debugf("join: final result contains %d rows", out.Rows())
				return out, nil
			}

			// set cursor to pk of last rsult row
			pkcursor, err = rRes.pkg.Uint64At(rRes.Fields().PkIndex(), rRes.Rows()-1)
			if err != nil {
				log.Errorf("join: no pk column in query result %s: %v", rQ.Name, err)
				return nil, err
			}

			lConds := j.Left.Where
			if j.IsEquiJoin() && rRes.Rows() > 0 {
				// get a copy of the right predicate column as slice (actually interface{} to slice)
				rPredCol, err := rRes.Column(j.Predicate.Right.Name)
				if err != nil {
					return nil, err
				}
				rPredColCopy, err := j.Predicate.Right.Type.CopySliceType(rPredCol)
				if err != nil {
					return nil, err
				}
				// use left predicate field values as additional IN condition
				lConds = append(lConds, Condition{
					Field:    j.Predicate.Left,
					Mode:     FilterModeIn,
					Value:    rPredColCopy,
					IsSorted: j.Predicate.Right.Flags&FlagPrimary > 0, // slice will be sorted when not pk
					Raw:      "join_predicate." + j.Right.Table.Name() + "." + j.Predicate.Right.Name,
				})
			}

			// query the left table
			lQ := Query{
				Name:       q.Name + ".join_left",
				Fields:     j.Left.Fields.AddUnique(j.Predicate.Left),
				Conditions: lConds,
				// Limit:      j.Left.Limit, // no limit
			}
			// log.Debugf("join: left table query with %d cond and limit %d", len(lQ.Conditions), lQ.Limit)
			// for i, c := range lQ.Conditions {
			// 	log.Debugf("cond %d: %s", i, c.String())
			// }
			lRes, err = j.Left.Table.Query(ctx, lQ)
			if err != nil {
				return nil, err
			}
			// log.Debugf("join: left table result %d rows", lRes.Rows())
		}

		// ------------------------------------------------------------
		// JOIN
		// ------------------------------------------------------------
		// merge result sets
		switch j.Type {
		case InnerJoin:
			if j.IsEquiJoin() {
				err = mergeJoinInner(j, lRes, rRes, agg)
			} else {
				err = loopJoinInner(j, lRes, rRes, agg)
			}
		case LeftJoin:
			if j.IsEquiJoin() {
				err = mergeJoinLeft(j, lRes, rRes, agg)
			} else {
				err = loopJoinLeft(j, lRes, rRes, agg)
			}
		case RightJoin:
			if j.IsEquiJoin() {
				err = mergeJoinRight(j, lRes, rRes, agg)
			} else {
				err = loopJoinRight(j, lRes, rRes, agg)
			}
		case CrossJoin:
			err = loopJoinCross(j, lRes, rRes, agg)
		// case FullJoin:
		//  // does not work with the loop algorithm above
		// 	if j.IsEquiJoin() {
		// 		n, err = mergeJoinFull(j, lRes, rRes, agg)
		// 	} else {
		// 		n, err = loopJoinFull(j, lRes, rRes, agg)
		// 	}
		// case SelfJoin:
		// case AsOfJoin:
		// case WindowJoin:
		default:
			return nil, fmt.Errorf("%s is not implemented yet", j.Type)
		}
		if err != nil {
			return nil, err
		}

		// close intermediate per-table results after join
		lRes.Close()
		rRes.Close()

		// ------------------------------------------------------------
		// POST-PROCESS
		// ------------------------------------------------------------
		if havePostFilter {
			// log.Debugf("join: filtering result with %d rows against %d conds", agg.Rows(), len(q.Conditions))

			// filter result by query
			bits := q.Conditions.MatchPack(agg.pkg, PackInfo{})
			for idx, length := bits.Run(0); idx >= 0; idx, length = bits.Run(idx + length) {
				n := length
				if q.Limit > 0 {
					// limit copy length to q.Limit
					n = util.Min(n, q.Limit-out.pkg.Len())
				}

				// append match to new output
				if err := out.pkg.AppendFrom(agg.pkg, idx, n, true); err != nil {
					return nil, err
				}

				// stop when limit is reached
				if q.Limit > 0 && out.pkg.Len() >= q.Limit {
					// log.Debugf("join: final result clipped at limit %d/%d", q.Limit, out.pkg.Len())
					return out, nil
				}

			}
			bits.Close()
			agg.pkg.Clear()
		}

		if q.Limit > 0 && out.pkg.Len() >= q.Limit {
			// log.Debugf("join: final result clipped at limit %d", q.Limit)
			return out, nil
		}
	}
	return out, nil
}

// non-equi joins
func loopJoinInner(join Join, left, right, out *Result) error {
	// log.Debugf("join: inner join on %d/%d rows using loop", left.Rows(), right.Rows())
	// build cartesian product (O(n^2)) with
	for i, il := 0, left.Rows(); i < il; i++ {
		for j, jl := 0, right.Rows(); j < jl; j++ {
			if join.Predicate.MatchPacksAt(left.pkg, i, right.pkg, j) {
				// merge result and append to out package
				if err := join.AppendResult(out.pkg, left.pkg, i, right.pkg, j); err != nil {
					return err
				}
				// stop on limit
				if join.limit > 0 && out.Rows() == join.limit {
					return nil
				}
			}
		}
	}
	return nil
}

// equi-joins only, |l| ~ |r| (close set sizes)
// TODO: never match NULL values (i.e. pkg.IsZeroAt(index,pos) == true)
func mergeJoinInner(join Join, left, right, out *Result) error {
	// log.Debugf("join: inner join on %d/%d rows using merge", left.Rows(), right.Rows())
	// The algorithm works as follows
	//
	// for every left-side row find all matching right-side rows
	// and output a merged and transformed row for each such pair.
	//
	// each side may contain duplicate values for the join predicate,
	// that is, there may be multiple matching rows both in the left
	// and the right set and we're supposed to output the cartesian product.
	//
	// To efficiently support this case we keep the start and end of equal
	// right-side matches and roll back when we advance the left side.
	//
	// To further safe comparisons when we know we're inside a block match
	// we only match the left side against the start of a block.
	var (
		currBlockStart, currBlockEnd int
		haveBlockMatch               bool
		forceMatch                   bool
	)

	// sorted input is required
	//
	// sort left result by predicate column unless it's the primary key
	if join.Predicate.Left.Flags&FlagPrimary == 0 {
		if err := left.SortByField(join.Predicate.Left.Name); err != nil {
			return err
		}
	}

	// sort right result by predicate column unless it's the primary key
	if join.Predicate.Right.Flags&FlagPrimary == 0 {
		if err := right.SortByField(join.Predicate.Right.Name); err != nil {
			return err
		}
	}

	// loop until one result set is exhausted
	i, j, il, jl := 0, 0, left.Rows(), right.Rows()
	for i < il && j < jl {
		// OPTIMIZATION
		// once we have found a right-side block match of size > 1, we
		// only have to compare the next left-side value once and not
		// again for this block.
		var cmp int
		if !haveBlockMatch || forceMatch || j > currBlockEnd {
			cmp = join.Predicate.ComparePacksAt(left.pkg, i, right.pkg, j)
			forceMatch = false
		}
		switch cmp {
		case -1:
			// l[i] < r[j]: no or no more matches for the left row
			// ->> advance left side (i)
			i++
			// reset right index to start of the right block in case the
			// next left row has the same join field value
			j = currBlockStart
			haveBlockMatch = currBlockEnd-currBlockStart > 1
			forceMatch = true
		case 1:
			// l[i] > r[j]: no or no more matches for the right value
			// ->> advance right side (j) behind end of block, update block start
			j = currBlockEnd + 1
			currBlockStart = j
			currBlockEnd = j
			haveBlockMatch = false
		case 0:
			// match, append merged result to out
			if err := join.AppendResult(out.pkg, left.pkg, i, right.pkg, j); err != nil {
				return err
			}

			// stop on limit
			if join.limit > 0 && out.Rows() == join.limit {
				return nil
			}

			// update indices
			if !haveBlockMatch {
				currBlockEnd = j
			}
			if j+1 < jl {
				// stay at current left pos and advance right pos if
				// we're not at the end of the right result set yet
				j++
			} else {
				// if we're at the end, try matching the next left side
				// result againts the current right-side block
				i++
				j = currBlockStart
				haveBlockMatch = currBlockEnd-currBlockStart > 1
				forceMatch = true
			}
		}
	}
	return nil
}

// equi-joins only, |l| << >> |r| (widely different set sizes)
func hashJoinInner(join Join, left, right, out *Result) error {
	log.Debugf("join: inner join on %d/%d rows using hash", left.Rows(), right.Rows())
	return nil
}

// TODO: never match NULL values (i.e. pkg.IsZeroAt(index,pos) == true)
func loopJoinLeft(join Join, left, right, out *Result) error {
	log.Debugf("join: left join on %d/%d rows using loop", left.Rows(), right.Rows())
	return nil
}

// TODO: never match NULL values (i.e. pkg.IsZeroAt(index,pos) == true)
func mergeJoinLeft(join Join, left, right, out *Result) error {
	// log.Debugf("join: left join on %d/%d rows using merge", left.Rows(), right.Rows())
	// The algorithm works as follows
	//
	// for every left-side row find all matching right-side rows
	// and output a merged and transformed row for each such pair.
	//
	// if no right side row matches (search stops when left[i] < right[j]),
	// output the left side with no matching right side (null or default values)
	//
	// each side may contain duplicate values for the join predicate,
	// that is, there may be multiple matching rows both in the left
	// and the right set and we're supposed to output the cartesian product.
	//
	// To efficiently support this case we keep the start and end of equal
	// right-side matches and roll back when we advance the left side.
	//
	// To further safe comparisons when we know we're inside a block match
	// we only match the left side against the start of a block.
	var (
		currBlockStart, currBlockEnd int
		wasMatch                     bool
	)

	// sorted input is required
	//
	// sort left result by predicate column unless it's the primary key
	if join.Predicate.Left.Flags&FlagPrimary == 0 {
		if err := left.SortByField(join.Predicate.Left.Name); err != nil {
			return err
		}
	}

	// sort right result by predicate column unless it's the primary key
	if join.Predicate.Right.Flags&FlagPrimary == 0 {
		if err := right.SortByField(join.Predicate.Right.Name); err != nil {
			return err
		}
	}

	// loop until one result set is exhausted
	i, j, il, jl := 0, 0, left.Rows(), right.Rows()
	for i < il {
		cmp := join.Predicate.ComparePacksAt(left.pkg, i, right.pkg, j)
		switch cmp {
		case -1:
			// l[i] < r[j]: no or no more matches for the left row
			// ->> output left and advance left side (i)
			if !wasMatch {
				if err := join.AppendResult(out.pkg, left.pkg, i, nil, -1); err != nil {
					return err
				}
				if join.limit > 0 && out.Rows() == join.limit {
					return nil
				}
			}
			i++
			j = currBlockStart
			wasMatch = false
		case 1:
			// l[i] > r[j]: no or no more matches for the right value
			// ->> advance right side (j) behind end of block, update block start
			if j+1 < jl {
				j = currBlockEnd + 1
				currBlockStart = j
				currBlockEnd = j
			} else {
				// without a match still output the left side
				if !wasMatch {
					if err := join.AppendResult(out.pkg, left.pkg, i, nil, -1); err != nil {
						return err
					}
					if join.limit > 0 && out.Rows() == join.limit {
						return nil
					}
				}
				i++
			}
			wasMatch = false
		case 0:
			// match, append merged result to out
			if err := join.AppendResult(out.pkg, left.pkg, i, right.pkg, j); err != nil {
				return err
			}
			if join.limit > 0 && out.Rows() == join.limit {
				return nil
			}
			if j+1 < jl {
				// stay at current left pos and advance right pos if
				// we're not at the end of the right result set yet
				j++
				wasMatch = true
			} else {
				i++
				j = currBlockStart
				wasMatch = false
			}
		}
	}
	return nil
}

// TODO: need hash table to remember whether a row was joined already
// process inner join first, then add missing left, then missing right rows
func loopJoinRight(join Join, left, right, out *Result) error {
	log.Debugf("join: right join on %d/%d rows using loop", left.Rows(), right.Rows())
	return nil
}

func mergeJoinRight(join Join, left, right, out *Result) error {
	log.Debugf("join: right join on %d/%d rows using merge", left.Rows(), right.Rows())
	return nil
}

func loopJoinFull(join Join, left, right, out *Result) error {
	log.Debugf("join: full loop join on %d/%d rows", left.Rows(), right.Rows())
	return nil
}

func mergeJoinFull(join Join, left, right, out *Result) error {
	log.Debugf("join: full join on %d/%d rows using merge", left.Rows(), right.Rows())
	return nil
}

func loopJoinCross(join Join, left, right, out *Result) error {
	// log.Debugf("join: cross join on %d/%d rows using loop", left.Rows(), right.Rows())
	// build cartesian product (O(n^2))
	for i, il := 0, left.Rows(); i < il; i++ {
		for j, jl := 0, right.Rows(); j < jl; j++ {
			// merge result and append to out package
			if err := join.AppendResult(out.pkg, left.pkg, i, right.pkg, j); err != nil {
				return err
			}
			if join.limit > 0 && out.Rows() == join.limit {
				return nil
			}
		}
	}
	return nil
}
