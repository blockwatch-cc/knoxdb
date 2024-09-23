// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package wal

type LinearReader struct {}

func (r *LinearReader) Seek(LSN) error {}
func (r *LinearReader) Next() (*Record, error) {}


type FilterReader struct {}

func (r *FilterReader) WithType(t RecordType) *FilterReader {}
func (r *FilterReader) WithTag(t types.ObjectTag) *FilterReader {}
func (r *FilterReader) WithEntity(v uint64) *FilterReader {}
func (r *FilterReader) WithTxID(v uint64) *FilterReader {}

func (r *FilterReader) Seek(LSN) error {}
func (r *FilterReader) Next() (*Record, error) {}
