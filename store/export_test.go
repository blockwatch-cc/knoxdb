// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

/*
This test file is part of the database package rather than the
database_test package so it can bridge access to the internals to properly test
cases which are either not possible or can't reliably be tested via the public
interface.  The functions, constants, and variables are only exported while the
tests are being run.
*/

package store

// TstNumErrorCodes makes the internal numErrorCodes parameter available to the
// test package.
const TstNumErrorCodes = numErrorCodes
