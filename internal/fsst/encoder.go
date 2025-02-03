// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package fsst

type Encoder struct {
	symbolTable *SymbolTable
	counter     *Counter
}

func NewEncoder(strIn [][]uint8, zeroTerminated bool) *Encoder {
	encoder := &Encoder{
		symbolTable: NewSymbolTable(),
		counter:     &Counter{},
	}
	sample := makeSample(strIn)
	encoder.symbolTable = buildSymbolTable(encoder.counter, sample, zeroTerminated)
	return encoder
}
