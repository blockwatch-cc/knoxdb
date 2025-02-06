// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package fsst

import (
	"github.com/echa/log"
)

type Stat struct {
	longestSymbol *Symbol
	symbolsSize   int
	symbols       []*Symbol
}

type Encoder struct {
	symbolTable *SymbolTable
	counter     *Counter
	stat        Stat
}

func NewEncoder(strIn [][]uint8, zeroTerminated bool) *Encoder {
	encoder := &Encoder{
		symbolTable: NewSymbolTable(),
		counter:     &Counter{},
	}
	sample := makeSample(strIn)
	encoder.symbolTable = buildSymbolTable(encoder, sample, zeroTerminated)

	log.Debugf("Terminator => %x ", encoder.symbolTable.terminator)
	log.Debugf("logging %d symbols %d", encoder.symbolTable.nSymbols, len(encoder.symbolTable.symbols))
	for i := 0; i < int(encoder.symbolTable.nSymbols); i++ {
		sym := encoder.symbolTable.symbols[i]
		log.Debugf("symbol idx => %d symbol => [%s]", i, sym)
	}

	return encoder
}
