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
	MAX_K_COMBINATIONS     = 5
	CUTTING_LIMIT          = 16
	MAX_RD_DICT_BIT_WIDTH  = 3
	MAX_RD_DICTIONARY_SIZE = (1 << MAX_RD_DICT_BIT_WIDTH)
)
