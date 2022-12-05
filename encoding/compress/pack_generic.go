// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package compress

func packBytes8BitGeneric(src []uint64, buf []byte) {
    for i, v := range src {
        buf[i] = byte(v & 0xff)
    }
}

func packBytes16BitGeneric(src []uint64, buf []byte) {
    for i, v := range src {
        buf[2*i] = byte((v >> 8) & 0xff)
        buf[1+2*i] = byte(v & 0xff)
    }
}

func packBytes24BitGeneric(src []uint64, buf []byte) {
    for i, v := range src {
        buf[3*i] = byte((v >> 16) & 0xff)
        buf[1+3*i] = byte((v >> 8) & 0xff)
        buf[2+3*i] = byte(v & 0xff)
    }
}

func packBytes32BitGeneric(src []uint64, buf []byte) {
    for i, v := range src {
        buf[4*i] = byte((v >> 24) & 0xff)
        buf[1+4*i] = byte((v >> 16) & 0xff)
        buf[2+4*i] = byte((v >> 8) & 0xff)
        buf[3+4*i] = byte(v & 0xff)
    }
}

func unpackBytes8BitGeneric(src []byte, res []uint64) {
    for i, j := 0, 0; i < len(src); i++ {
        res[i] = uint64(src[j])
        j++
    }
}

func unpackBytes16BitGeneric(src []byte, res []uint64) {
    for i, j := 0, 0; i < len(src)/2; i++ {
        res[i] = uint64(src[j])<<8 | uint64(src[1+j])
        j += 2
    }
}

func unpackBytes24BitGeneric(src []byte, res []uint64) {
    for i, j := 0, 0; i < len(src)/3; i++ {
        res[i] = uint64(src[j])<<16 | uint64(src[1+j])<<8 | uint64(src[2+j])
        j += 3
    }
}

func unpackBytes32BitGeneric(src []byte, res []uint64) {
    for i, j := 0, 0; i < len(src)/4; i++ {
        res[i] = uint64(src[j])<<24 | uint64(src[1+j])<<16 | uint64(src[2+j])<<8 | uint64(src[3+j])
        j += 4
    }
}
