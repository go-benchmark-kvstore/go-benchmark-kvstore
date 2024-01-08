package main

import (
	"encoding/base64"
	"io"
	"os"
	"path"

	"github.com/rs/zerolog"
	"gitlab.com/tozd/go/errors"
)

var _ Engine = (*FS)(nil)

type FS struct {
	dir string
}

func (e *FS) Close() errors.E {
	return nil
}

func (e *FS) Sync() errors.E {
	// We sync after every write, so there is nothing to sync here.
	return nil
}

func (e *FS) name(key []byte) string {
	return base64.RawURLEncoding.EncodeToString(key)
}

func (e *FS) Get(key []byte) (_ io.ReadSeekCloser, errE errors.E) {
	name := e.name(key)

	f, err := os.Open(path.Join(e.dir, name))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return newReadSeekCloser(f, f.Close), nil
}

func (e *FS) Init(benchmark *Benchmark, logger zerolog.Logger) errors.E {
	err := os.MkdirAll(benchmark.Data, 0700)
	if err != nil {
		return errors.WithStack(err)
	}
	if !isEmpty(benchmark.Data) {
		return errors.New("data directory is not empty")
	}
	e.dir = benchmark.Data
	return nil
}

func (*FS) Name() string {
	return "fs"
}

func (e *FS) Set(key []byte, value []byte) (errE errors.E) {
	name := e.name(key)

	f, err := os.Create(path.Join(e.dir, name))
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() {
		errE = errors.Join(errE, f.Close())
	}()

	_, err = f.Write(value)
	if err != nil {
		return errors.WithStack(err)
	}

	// To be able to compare between engines, we make all of them sync after every write.
	// This lowers throughput, but it makes relative differences between engines clearer.
	return errors.WithStack(f.Sync())
}
