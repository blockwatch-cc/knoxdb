// Copyright (c) 2018-2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc
//
package compress

func ZzDeltaEncodeUint64(data []uint64) uint64 {
    var maxdelta uint64
    for i := len(data) - 1; i > 0; i-- {
        data[i] = data[i] - data[i-1]
        data[i] = ZigZagEncode(int64(data[i]))
        if data[i] > maxdelta {
            maxdelta = data[i]
        }
    }

    data[0] = ZigZagEncode(int64(data[0]))
    return maxdelta
}

func ZzEncodeUint64(data []uint64) uint64 {
    var max uint64
    for i := range data {
        data[i] = ZigZagEncode(int64(data[i]))
        if data[i] > max {
            max = data[i]
        }
    }
    return max
}

func ZzDeltaDecodeInt64(data []int64) {
    zzDeltaDecodeInt64(data)
}

func ZzDeltaDecodeUint64(data []uint64) {
    zzDeltaDecodeUint64(data)
}

func ZzDecodeInt64(data []int64) {
    zzDecodeInt64(data)
}

func ZzDecodeUint64(data []uint64) {
    zzDecodeUint64(data)
}

func Delta8DecodeUint64(data []uint64) {
    delta8DecodeUint64(data)
}

func Delta8EncodeUint64(data []uint64) uint64 {
    return delta8EncodeUint64(data)
}
