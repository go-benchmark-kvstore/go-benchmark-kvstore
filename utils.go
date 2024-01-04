package main

import (
	"bytes"
	"io"
	"unsafe"
)

func string2ByteSlice(str string) []byte {
	return unsafe.Slice(unsafe.StringData(str), len(str))
}

func byteSlice2String(bs []byte) string {
	if len(bs) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(bs), len(bs))
}

type readSeekCloser struct {
	io.ReadSeeker
	close func() error
}

func (r readSeekCloser) Close() error {
	return r.close()
}

func newReadSeekCloser(value []byte, close func() error) io.ReadSeekCloser {
	return readSeekCloser{
		ReadSeeker: bytes.NewReader(value),
		close:      close,
	}
}
