package main

import (
	"bytes"
	"io"
	"unsafe"

	"github.com/hashicorp/go-metrics"
	"github.com/rs/zerolog"
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

type readSeekCloseWriterTo struct {
	io.ReadSeeker
	io.WriterTo
	close func() error
}

func (r readSeekCloseWriterTo) Close() error {
	return r.close()
}

func bytesReadSeekCloser(value []byte, close func() error) io.ReadSeekCloser {
	return readSeekCloser{
		ReadSeeker: bytes.NewReader(value),
		close:      close,
	}
}

func newReadSeekCloser(readSeeker io.ReadSeeker, close func() error) io.ReadSeekCloser {
	if wt, ok := readSeeker.(io.WriterTo); ok {
		return readSeekCloseWriterTo{
			ReadSeeker: readSeeker,
			WriterTo:   wt,
			close:      close,
		}
	}
	return readSeekCloser{
		ReadSeeker: readSeeker,
		close:      close,
	}
}

type metricsEncoder struct {
	Logger zerolog.Logger
}

func (e metricsEncoder) Encode(value interface{}) error {
	if v, ok := value.(metrics.MetricsSummary); ok {
		e.Logger.Info().Str("timestamp", v.Timestamp).Any("counters", v.Counters).Any("samples", v.Samples).Any("gauges", v.Gauges).Send()
	}
	return nil
}
