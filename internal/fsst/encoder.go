// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package fsst

type Stat struct {
	longestSymbol Symbol
	symbolsSize   int
	symbols       []Symbol
}

type Encoder struct {
	symbolTable *SymbolTable
	counter     *Counter
	stat        Stat
}

func NewEncoder(strIn [][]uint8, zeroTerminated bool) *Encoder {
	encoder := &Encoder{
		symbolTable: &SymbolTable{},
		counter:     &Counter{},
	}
	sample := makeSample(strIn)
	encoder.symbolTable = buildSymbolTable(encoder, sample, zeroTerminated)
	return encoder
}
