// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package tests

var (
	ZzDeltaEncodeUint64Cases = []Int64Test{
		// {
		// 	name:   "l0",
		// 	slice:  make([]int64, 0),
		// 	result: []int64{},
		// },
		CreateInt64TestCase("l0", nil, nil, 0),
		CreateInt64TestCase("l3", int64DecodedSlice, int64ZzDeltaEncoded, 3),
		CreateInt64TestCase("l4", int64DecodedSlice, int64ZzDeltaEncoded, 4),
		CreateInt64TestCase("l7", int64DecodedSlice, int64ZzDeltaEncoded, 7),
		CreateInt64TestCase("l8", int64DecodedSlice, int64ZzDeltaEncoded, 8),
		CreateInt64TestCase("l15", int64DecodedSlice, int64ZzDeltaEncoded, 15),
		CreateInt64TestCase("l16", int64DecodedSlice, int64ZzDeltaEncoded, 16),
	}

	ZzDeltaEncodeUint32Cases = []Int32Test{
		// {
		// 	name:   "l0",
		// 	slice:  make([]int32, 0),
		// 	result: []int32{},
		// },
		CreateInt32TestCase("l0", nil, nil, 0),
		CreateInt32TestCase("l3", int32DecodedSlice, int32ZzDeltaEncoded, 3),
		CreateInt32TestCase("l4", int32DecodedSlice, int32ZzDeltaEncoded, 4),
		CreateInt32TestCase("l7", int32DecodedSlice, int32ZzDeltaEncoded, 7),
		CreateInt32TestCase("l8", int32DecodedSlice, int32ZzDeltaEncoded, 8),
	}

	ZzDeltaEncodeUint16Cases = []Int16Test{
		// {
		// 	name:   "l0",
		// 	slice:  make([]int16, 0),
		// 	result: []int16{},
		// },
		CreateInt16TestCase("l0", nil, nil, 0),
		CreateInt16TestCase("l3", int16DecodedSlice, int16ZzDeltaEncoded, 3),
		CreateInt16TestCase("l4", int16DecodedSlice, int16ZzDeltaEncoded, 4),
		CreateInt16TestCase("l7", int16DecodedSlice, int16ZzDeltaEncoded, 7),
		CreateInt16TestCase("l8", int16DecodedSlice, int16ZzDeltaEncoded, 8),
	}

	ZzDeltaEncodeUint8Cases = []Int8Test{
		// {
		// 	name:   "l0",
		// 	slice:  make([]int8, 0),
		// 	result: []int8{},
		// },
		CreateInt8TestCase("l0", nil, nil, 0),
		CreateInt8TestCase("l3", int8DecodedSlice, int8ZzDeltaEncoded, 3),
		CreateInt8TestCase("l4", int8DecodedSlice, int8ZzDeltaEncoded, 4),
		CreateInt8TestCase("l7", int8DecodedSlice, int8ZzDeltaEncoded, 7),
		CreateInt8TestCase("l8", int8DecodedSlice, int8ZzDeltaEncoded, 8),
	}
)
