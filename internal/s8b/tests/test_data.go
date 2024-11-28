package tests

var S8bTestsUint64 = []struct {
	Name string
	In   []uint64
	Fn   func() []uint64
	Err  bool
}{
	{Name: "no values", In: []uint64{}},
	{Name: "mixed sizes", In: []uint64{7, 6, 256, 4, 3, 2, 1}},
	{Name: "too big", In: []uint64{7, 6, 2<<61 - 1, 4, 3, 2, 1}, Err: true},
	{Name: "1 bit", Fn: bits(120, 1)},
	{Name: "2 bits", Fn: bits(120, 2)},
	{Name: "3 bits", Fn: bits(120, 3)},
	{Name: "4 bits", Fn: bits(120, 4)},
	{Name: "5 bits", Fn: bits(120, 5)},
	{Name: "6 bits", Fn: bits(120, 6)},
	{Name: "7 bits", Fn: bits(120, 7)},
	{Name: "8 bits", Fn: bits(120, 8)},
	{Name: "10 bits", Fn: bits(120, 10)},
	{Name: "12 bits", Fn: bits(120, 12)},
	{Name: "15 bits", Fn: bits(120, 15)},
	{Name: "20 bits", Fn: bits(120, 20)},
	{Name: "30 bits", Fn: bits(120, 30)},
	{Name: "60 bits", Fn: bits(120, 60)},
	{Name: "combination", Fn: combine(
		bits(120, 1),
		bits(120, 2),
		bits(120, 3),
		bits(120, 4),
		bits(120, 5),
		bits(120, 6),
		bits(120, 7),
		bits(120, 8),
		bits(120, 10),
		bits(120, 12),
		bits(120, 15),
		bits(120, 20),
		bits(120, 30),
		bits(120, 60),
	)},
	{Name: "240 ones", Fn: ones(240)},
	{Name: "120 ones", Fn: func() []uint64 {
		in := ones(240)()
		in[120] = 5
		return in
	}},
	{Name: "119 ones", Fn: func() []uint64 {
		in := ones(240)()
		in[119] = 5
		return in
	}},
	{Name: "239 ones", Fn: func() []uint64 {
		in := ones(241)()
		in[239] = 5
		return in
	}},
}

var S8bTestsUint32 = []struct {
	Name string
	In   []uint32
	Fn   func() []uint32
	Err  bool
}{
	{Name: "no values", In: []uint32{}},
	{Name: "mixed sizes", In: []uint32{7, 6, 256, 4, 3, 2, 1}},
	{Name: "1 bit", Fn: bits32(120, 1)},
	{Name: "2 bits", Fn: bits32(120, 2)},
	{Name: "3 bits", Fn: bits32(120, 3)},
	{Name: "4 bits", Fn: bits32(120, 4)},
	{Name: "5 bits", Fn: bits32(120, 5)},
	{Name: "6 bits", Fn: bits32(120, 6)},
	{Name: "7 bits", Fn: bits32(120, 7)},
	{Name: "8 bits", Fn: bits32(120, 8)},
	{Name: "10 bits", Fn: bits32(120, 10)},
	{Name: "12 bits", Fn: bits32(120, 12)},
	{Name: "15 bits", Fn: bits32(120, 15)},
	{Name: "20 bits", Fn: bits32(120, 20)},
	{Name: "30 bits", Fn: bits32(120, 30)},
	{Name: "60 bits", Fn: bits32(120, 32)},
	{Name: "combination", Fn: combine32(
		bits32(120, 1),
		bits32(120, 2),
		bits32(120, 3),
		bits32(120, 4),
		bits32(120, 5),
		bits32(120, 6),
		bits32(120, 7),
		bits32(120, 8),
		bits32(120, 10),
		bits32(120, 12),
		bits32(120, 15),
		bits32(120, 20),
		bits32(120, 30),
		bits32(120, 32),
	)},
	{Name: "240 ones", Fn: ones32(240)},
	{Name: "120 ones", Fn: func() []uint32 {
		in := ones32(240)()
		in[120] = 5
		return in
	}},
	{Name: "119 ones", Fn: func() []uint32 {
		in := ones32(240)()
		in[119] = 5
		return in
	}},
	{Name: "239 ones", Fn: func() []uint32 {
		in := ones32(241)()
		in[239] = 5
		return in
	}},
}

var S8bTestsUint16 = []struct {
	Name string
	In   []uint16
	Fn   func() []uint16
	Err  bool
}{
	{Name: "no values", In: []uint16{}},
	{Name: "mixed sizes", In: []uint16{7, 6, 256, 4, 3, 2, 1}},
	{Name: "1 bit", Fn: bits16(120, 1)},
	{Name: "2 bits", Fn: bits16(120, 2)},
	{Name: "3 bits", Fn: bits16(120, 3)},
	{Name: "4 bits", Fn: bits16(120, 4)},
	{Name: "5 bits", Fn: bits16(120, 5)},
	{Name: "6 bits", Fn: bits16(120, 6)},
	{Name: "7 bits", Fn: bits16(120, 7)},
	{Name: "8 bits", Fn: bits16(120, 8)},
	{Name: "10 bits", Fn: bits16(120, 10)},
	{Name: "12 bits", Fn: bits16(120, 12)},
	{Name: "15 bits", Fn: bits16(120, 15)},
	{Name: "20 bits", Fn: bits16(120, 16)},
	{Name: "30 bits", Fn: bits16(2, 16)},
	{Name: "60 bits", Fn: bits16(1, 16)},
	{Name: "combination", Fn: combine16(
		bits16(120, 1),
		bits16(120, 2),
		bits16(120, 3),
		bits16(120, 4),
		bits16(120, 5),
		bits16(120, 6),
		bits16(120, 7),
		bits16(120, 8),
		bits16(120, 10),
		bits16(120, 12),
		bits16(120, 15),
		bits16(120, 16),
	)},
	{Name: "240 ones", Fn: ones16(240)},
	{Name: "120 ones", Fn: func() []uint16 {
		in := ones16(240)()
		in[120] = 5
		return in
	}},
	{Name: "119 ones", Fn: func() []uint16 {
		in := ones16(240)()
		in[119] = 5
		return in
	}},
	{Name: "239 ones", Fn: func() []uint16 {
		in := ones16(241)()
		in[239] = 5
		return in
	}},
}

var S8bTestsUint8 = []struct {
	Name string
	In   []uint8
	Fn   func() []uint8
	Err  bool
}{
	{Name: "no values", In: []uint8{}},
	{Name: "mixed sizes", In: []uint8{7, 6, 255, 4, 3, 2, 1}},
	{Name: "1 bit", Fn: bits8(120, 1)},
	{Name: "2 bits", Fn: bits8(120, 2)},
	{Name: "3 bits", Fn: bits8(120, 3)},
	{Name: "4 bits", Fn: bits8(120, 4)},
	{Name: "5 bits", Fn: bits8(120, 5)},
	{Name: "6 bits", Fn: bits8(120, 6)},
	{Name: "7 bits", Fn: bits8(120, 7)},
	{Name: "8 bits", Fn: bits8(120, 8)},
	{Name: "10 bits", Fn: bits8(6, 8)},
	{Name: "12 bits", Fn: bits8(5, 8)},
	{Name: "15 bits", Fn: bits8(4, 8)},
	{Name: "20 bits", Fn: bits8(3, 8)},
	{Name: "30 bits", Fn: bits8(2, 8)},
	{Name: "60 bits", Fn: bits8(1, 8)},
	{Name: "combination", Fn: combine8(
		bits8(120, 1),
		bits8(120, 2),
		bits8(120, 3),
		bits8(120, 4),
		bits8(120, 5),
		bits8(120, 6),
		bits8(120, 7),
		bits8(120, 8),
	)},
	{Name: "240 ones", Fn: ones8(240)},
	{Name: "120 ones", Fn: func() []uint8 {
		in := ones8(240)()
		in[120] = 5
		return in
	}},
	{Name: "119 ones", Fn: func() []uint8 {
		in := ones8(240)()
		in[119] = 5
		return in
	}},
	{Name: "239 ones", Fn: func() []uint8 {
		in := ones8(241)()
		in[239] = 5
		return in
	}},
}
