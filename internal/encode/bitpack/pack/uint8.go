// Copyright (c) 2025 Blockwatch Data Inc.
// Code automatically generated - DO NOT EDIT.
// Any manual changes will be lost.

package pack

func bitpack8[T uint8 | int8](minv T, in []T, out []uint8, log2 int) {
	switch log2 {
	case 0:
		bp8_0((*[8]T)(in), (*[0]uint8)(out), minv)
	case 1:
		bp8_1((*[8]T)(in), (*[1]uint8)(out), minv)
	case 2:
		bp8_2((*[8]T)(in), (*[2]uint8)(out), minv)
	case 3:
		bp8_3((*[8]T)(in), (*[3]uint8)(out), minv)
	case 4:
		bp8_4((*[8]T)(in), (*[4]uint8)(out), minv)
	case 5:
		bp8_5((*[8]T)(in), (*[5]uint8)(out), minv)
	case 6:
		bp8_6((*[8]T)(in), (*[6]uint8)(out), minv)
	case 7:
		bp8_7((*[8]T)(in), (*[7]uint8)(out), minv)
	}

}
func bp8_0[T uint8 | int8](in *[8]T, out *[0]uint8, minv T) {
}
func bp8_1[T uint8 | int8](in *[8]T, out *[1]uint8, minv T) {
	out[0] = uint8(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 1) |
			((in[2] - minv) << 2) |
			((in[3] - minv) << 3) |
			((in[4] - minv) << 4) |
			((in[5] - minv) << 5) |
			((in[6] - minv) << 6) |
			((in[7] - minv) << 7))

}
func bp8_2[T uint8 | int8](in *[8]T, out *[2]uint8, minv T) {
	out[0] = uint8(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 2) |
			((in[2] - minv) << 4) |
			((in[3] - minv) << 6))

	out[1] = uint8(
		((in[4] - minv) << 0) |
			((in[5] - minv) << 2) |
			((in[6] - minv) << 4) |
			((in[7] - minv) << 6))

}
func bp8_3[T uint8 | int8](in *[8]T, out *[3]uint8, minv T) {
	out[0] = uint8(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 3) |
			((in[2] - minv) << 6))

	out[1] = uint8(
		((in[2] - minv) >> 2) |

			((in[3] - minv) << 1) |
			((in[4] - minv) << 4) |
			((in[5] - minv) << 7))

	out[2] = uint8(
		((in[5] - minv) >> 1) |

			((in[6] - minv) << 2) |
			((in[7] - minv) << 5))

}
func bp8_4[T uint8 | int8](in *[8]T, out *[4]uint8, minv T) {
	out[0] = uint8(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 4))

	out[1] = uint8(
		((in[2] - minv) << 0) |
			((in[3] - minv) << 4))

	out[2] = uint8(
		((in[4] - minv) << 0) |
			((in[5] - minv) << 4))

	out[3] = uint8(
		((in[6] - minv) << 0) |
			((in[7] - minv) << 4))

}
func bp8_5[T uint8 | int8](in *[8]T, out *[5]uint8, minv T) {
	out[0] = uint8(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 5))

	out[1] = uint8(
		((in[1] - minv) >> 3) |

			((in[2] - minv) << 2) |
			((in[3] - minv) << 7))

	out[2] = uint8(
		((in[3] - minv) >> 1) |

			((in[4] - minv) << 4))

	out[3] = uint8(
		((in[4] - minv) >> 4) |

			((in[5] - minv) << 1) |
			((in[6] - minv) << 6))

	out[4] = uint8(
		((in[6] - minv) >> 2) |

			((in[7] - minv) << 3))

}
func bp8_6[T uint8 | int8](in *[8]T, out *[6]uint8, minv T) {
	out[0] = uint8(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 6))

	out[1] = uint8(
		((in[1] - minv) >> 2) |

			((in[2] - minv) << 4))

	out[2] = uint8(
		((in[2] - minv) >> 4) |

			((in[3] - minv) << 2))

	out[3] = uint8(
		((in[3] - minv) >> 6) |

			((in[4] - minv) << 0) |
			((in[5] - minv) << 6))

	out[4] = uint8(
		((in[5] - minv) >> 2) |

			((in[6] - minv) << 4))

	out[5] = uint8(
		((in[6] - minv) >> 4) |

			((in[7] - minv) << 2))

}
func bp8_7[T uint8 | int8](in *[8]T, out *[7]uint8, minv T) {
	out[0] = uint8(
		((in[0] - minv) << 0) |
			((in[1] - minv) << 7))

	out[1] = uint8(
		((in[1] - minv) >> 1) |

			((in[2] - minv) << 6))

	out[2] = uint8(
		((in[2] - minv) >> 2) |

			((in[3] - minv) << 5))

	out[3] = uint8(
		((in[3] - minv) >> 3) |

			((in[4] - minv) << 4))

	out[4] = uint8(
		((in[4] - minv) >> 4) |

			((in[5] - minv) << 3))

	out[5] = uint8(
		((in[5] - minv) >> 5) |

			((in[6] - minv) << 2))

	out[6] = uint8(
		((in[6] - minv) >> 6) |

			((in[7] - minv) << 1))

}
