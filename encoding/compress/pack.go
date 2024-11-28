// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package compress

import (
    "fmt"
)

func PackBytes(src []uint64, nbytes int, buf []byte) ([]byte, error) {
    if len(buf) < nbytes*len(src) {
        return nil, fmt.Errorf("compressBytes: write buffer to small")
    }

    switch nbytes {
    case 1:
        packBytes8Bit(src, buf)
    case 2:
        packBytes16Bit(src, buf)
    case 3:
        packBytes24Bit(src, buf)
    case 4:
        packBytes32Bit(src, buf)
    default:
        return nil, fmt.Errorf("UnpackBytes: size (%d bytes) not yet implemented", nbytes)
    }

    return buf, nil
}

func UnpackBytes(src []byte, nbytes int, res []uint64) ([]uint64, error) {
    rlen := len(src) / nbytes

    if len(res) < rlen {
        return nil, fmt.Errorf("uncompressBytes: write buffer to small")
    }

    switch nbytes {
    case 1:
        unpackBytes8Bit(src, res)
    case 2:
        unpackBytes16Bit(src, res)
    case 3:
        unpackBytes24Bit(src, res)
    case 4:
        unpackBytes32Bit(src, res)
    default:
        return nil, fmt.Errorf("UnpackBytes: size (%d bytes) not yet implemented", nbytes)
    }
    return res, nil
}
