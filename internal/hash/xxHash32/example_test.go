package xxHash32_test

import (
	"bytes"
	"fmt"

	"blockwatch.cc/knoxdb/internal/hash/xxHash32"
)

func ExampleNew() {
	buf := bytes.NewBufferString("this is a test")
	x := xxHash32.New(0xCAFE)
	x.Write(buf.Bytes())
	fmt.Printf("%x\n", x.Sum32())
	// Output: bb4f02bc
}

func ExampleChecksum() {
	buf := bytes.NewBufferString("this is a test")
	fmt.Printf("%x\n", xxHash32.Checksum(buf.Bytes(), 0xCAFE))
	// Output: bb4f02bc
}
