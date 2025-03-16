// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package alp

const (
	// Config
	// ALP Vector size (We recommend against changing this; it should be constant)
	VECTOR_SIZE = 1024
	// number of vectors per rowgroup
	N_VECTORS_PER_ROWGROUP = 100
	// Rowgroup size
	ROWGROUP_SIZE = N_VECTORS_PER_ROWGROUP * VECTOR_SIZE
	// Vectors from the rowgroup from which to take samples; this will be used to then calculate the jumps
	ROWGROUP_VECTOR_SAMPLES = 8
	// We calculate how many equidistant vector we must jump within a rowgroup
	ROWGROUP_SAMPLES_JUMP = (ROWGROUP_SIZE / ROWGROUP_VECTOR_SAMPLES) / VECTOR_SIZE
	// Values to sample per vector
	SAMPLES_PER_VECTOR = 32
	// Maximum number of combinations obtained from row group sampling
	MAX_K_COMBINATIONS           = 5
	CUTTING_LIMIT                = 16
	RD_MAX_DICTIONARY_BIT_WIDTH  = 3
	RD_MAX_DICTIONARY_SIZE       = (1 << RD_MAX_DICTIONARY_BIT_WIDTH)
	RD_DICTIONARY_ELEMENT_SIZE   = 16
	RD_MAX_DICTIONARY_SIZE_BYTES = RD_MAX_DICTIONARY_SIZE * RD_DICTIONARY_ELEMENT_SIZE
)
