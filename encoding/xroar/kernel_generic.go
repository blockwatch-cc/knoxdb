package xroar

import (
    "math/bits"
)

var (
    bitmapOr     = bitmapOrGeneric
    bitmapAnd    = bitmapAndGeneric
    bitmapAndNot = bitmapAndNotGeneric
)

func bitmapOrGeneric(data []uint16, other []uint16) (num int) {
    for i := 0; i < len(data); i++ {
        data[i] |= other[i]
        // We are going to iterate over the entire container. So, we can
        // just recount the cardinality, starting from num=0.
        num += bits.OnesCount16(data[i])
    }
    return
}

func bitmapAndGeneric(data []uint16, other []uint16, buf []uint16) (num int) {
    for i := 0; i < len(data); i++ {
        buf[i] = data[i] & other[i]
        // We are going to iterate over the entire container. So, we can
        // just recount the cardinality, starting from num=0.
        num += bits.OnesCount16(buf[i])
    }
    return
}

func bitmapAndNotGeneric(data []uint16, other []uint16, buf []uint16) (num int) {
    for i := 0; i < len(data); i++ {
        //data[i] = data[i] ^ (data[i] & v)
        buf[i] = (data[i] &^ other[i]) // improved performance
        // We are going to iterate over the entire container. So, we can
        // just recount the cardinality, starting from num=0.
        num += bits.OnesCount16(buf[i])
    }
    return
}
