package compress

// calculate prefix sum
func zzDeltaDecodeInt64Generic(data []int64) {
	data[0] = ZigZagDecode(uint64(data[0]))
	prev := data[0]
	for i := 1; i < len(data); i++ {
		prev += ZigZagDecode(uint64(data[i]))
		data[i] = prev
	}
}

// calculate prefix sum
func zzDeltaDecodeUint64Generic(data []uint64) {
	data[0] = uint64(ZigZagDecode(data[0]))
	prev := data[0]
	for i := 1; i < len(data); i++ {
		prev += uint64(ZigZagDecode(data[i]))
		data[i] = prev
	}
}

func zzDecodeUint64Generic(data []uint64) {
	for i := range data {
		data[i] = uint64(ZigZagDecode(data[i]))
	}
}

func zzDecodeInt64Generic(data []int64) {
	for i := range data {
		data[i] = ZigZagDecode(uint64(data[i]))
	}
}

func delta8DecodeUint64Generic(data []uint64) {
	for i := 8; i < len(data); i++ {
		data[i] += data[i-8]
	}
}

func delta8EncodeUint64Generic(data []uint64) uint64 {
	maxdelta := uint64(0)
	for i := len(data) - 1; i > 7; i-- {
		data[i] = data[i] - data[i-8]
		maxdelta |= data[i]
	}
	return maxdelta
}

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
