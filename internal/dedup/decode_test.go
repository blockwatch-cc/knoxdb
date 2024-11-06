package dedup

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

type TestDecodeCase struct {
	Name             string
	ByteArray        ByteArray
	Buf              []byte
	Size             int
	IsErrorExepected bool
	ExpectedLen      int
}

func TestDecode(t *testing.T) {
	// dict
	dictRead0, dictArray0 := makeDictByteArrayReader(0)
	dictRead10000, dictArray10000 := makeDictByteArrayReader(1)

	// fixed
	fixedReader10, fixedByteArray10 := makeFixedReaderData(10, 10)

	// native
	nativeBuf := bytes.NewBuffer(nil)
	nativeData := makeNumberedData(defaultMaxPointsPerBlock)
	nativeByteArray := newNativeByteArrayFromBytes(nativeData)
	nativeByteArray.WriteTo(nativeBuf)

	// compact
	compactByteReader, compactByteArray := makeCompactByteArrayReader(10000)

	// invalid format
	invalidBuf := bytes.Clone(compactByteReader.(*bytes.Buffer).Bytes())
	invalidBuf[0] = 1 << 6

	decodesCase := []TestDecodeCase{
		{
			Name:             "Decode DictByteArray with 0 data",
			ByteArray:        dictArray0,
			Buf:              dictRead0.(*bytes.Buffer).Bytes(),
			IsErrorExepected: false,
			ExpectedLen:      dictArray0.Len(),
		},
		{
			Name:             "Decode DictByteArray with 10000 data",
			ByteArray:        dictArray10000,
			Buf:              dictRead10000.(*bytes.Buffer).Bytes(),
			IsErrorExepected: false,
			ExpectedLen:      dictArray10000.Len(),
		},
		{
			Name:             "Decode FixedByteArray with 1000 data",
			ByteArray:        fixedByteArray10,
			Buf:              fixedReader10.(*bytes.Buffer).Bytes(),
			IsErrorExepected: false,
			ExpectedLen:      fixedByteArray10.Len(),
		},
		{
			Name:             "Decode NativeByteArray with valid data",
			ByteArray:        nativeByteArray,
			Buf:              nativeBuf.Bytes(),
			IsErrorExepected: false,
			ExpectedLen:      nativeByteArray.Len(),
		},
		{
			Name:             "Decode CompactByteArray with valid data",
			ByteArray:        compactByteArray,
			Size:             10,
			Buf:              compactByteReader.(*bytes.Buffer).Bytes(),
			IsErrorExepected: false,
			ExpectedLen:      compactByteArray.Len(),
		},
		{
			Name:             "Invalid Format",
			ByteArray:        compactByteArray,
			Buf:              invalidBuf,
			IsErrorExepected: true,
		},
	}

	for _, testCase := range decodesCase {
		t.Run(testCase.Name, func(t *testing.T) {
			b, err := Decode(testCase.Buf, testCase.ByteArray, testCase.Size)
			if testCase.IsErrorExepected {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, testCase.ExpectedLen, b.Len())
			}
		})
	}
}
