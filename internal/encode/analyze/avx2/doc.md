# Howto Debug Go Assembly

Use a debug message area and write there from ASM. Then pass a buffer pointer.

```go
//go:noescape
func analyze_u32_avx2(vals *uint32, ret *Context[uint32], len int, buf *byte)

// ASM
//
// Debug: Store Y1 and Y2 to memory
VMOVDQU Y1, 0(CX)  // curr_vec (32 bytes)
VMOVDQU Y2, 32(CX) // shifted (32 bytes)

// Early exit after first iteration
VZEROUPPER
RET


// Print in stub function
func AnalyzeUint32(vals []uint32) (uint32, uint32, uint32, int) {
  if len(vals) == 0 {
    return 0, 0, 0, 0
  }
  var ctx Context[uint32]
  if len(vals) > 1 {
    ctx.Delta = vals[1] - vals[0]
  }
  analyze_u32_avx2(&vals[0], &ctx, len(vals), &debugBuffer[0])
  analyze_i16_avx2(&vals[0], &ctx, len(vals), &debugBuffer[0])
  fmt.Printf("curr_vec: %v\n", unpackUint16(debugBuffer[:32]))
  fmt.Printf("perm_vec: %v\n", unpackUint16(debugBuffer[32:64]))
  fmt.Printf("last_vec: %v\n", unpackUint16(debugBuffer[64:96]))
  fmt.Printf("shifted:  %v\n", unpackUint16(debugBuffer[96:128]))
  fmt.Printf("mask:  %016x\n", binary.LittleEndian.Uint64(debugBuffer[128:]))
  return ctx.Min, ctx.Max, ctx.Delta, int(ctx.NumRuns)
}

// fixed debug message buffer
var debugBuffer [128 + 8]byte

func unpackUint32(buf []byte) []uint32 {
    result := make([]uint32, 8)
    for i := 0; i < 8; i++ {
        result[i] = binary.LittleEndian.Uint32(buf[i*4 : (i+1)*4])
    }
    return result
}

func unpackUint16(buf []byte) []uint16 {
    result := make([]uint16, 16)
    for i := 0; i < 16; i++ {
        result[i] = binary.LittleEndian.Uint16(buf[i*2 : (i+1)*2])
    }
    return result
}
```
