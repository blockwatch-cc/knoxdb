// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

type TableState struct {
	Sequence   uint64 // next free sequence
	NRows      uint64 // total non-deleted rows
	Checkpoint uint64 // latest wal checkpoint LSN
}

func NewTableState() TableState {
	return TableState{
		Sequence:   1,
		NRows:      0,
		Checkpoint: 0,
	}
}

func (s *TableState) Reset() {
	*s = NewTableState()
}

func (s *TableState) FromObjectState(o ObjectState) {
	s.Sequence = o[0]
	s.NRows = o[1]
	s.Checkpoint = o[2]
}

func (s TableState) ToObjectState() ObjectState {
	return ObjectState{s.Sequence, s.NRows, s.Checkpoint}
}
