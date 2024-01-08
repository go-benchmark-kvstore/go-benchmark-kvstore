package main

import (
	"io"

	"github.com/rs/zerolog"
	"gitlab.com/tozd/go/errors"
	"go.mills.io/bitcask/v2"
)

var _ Engine = (*Bitcask)(nil)

type Bitcask struct {
	db bitcask.DB
}

func (e *Bitcask) Close() errors.E {
	return errors.WithStack(e.db.Close())
}

func (e *Bitcask) Sync() errors.E {
	return errors.WithStack(e.db.Sync())
}

func (e *Bitcask) Get(key []byte) (io.ReadSeekCloser, errors.E) {
	tx := e.db.Transaction()

	value, err := tx.Get(key)
	if err != nil {
		tx.Discard()
		return nil, errors.WithStack(err)
	}
	return bytesReadSeekCloser(value, func() error {
		tx.Discard()
		return nil
	}), nil
}

func (e *Bitcask) Init(benchmark *Benchmark, logger zerolog.Logger) errors.E {
	// We set the max value to 6 GB so that we can test values larger than 2 GB.
	maxValueSize := 6 * 1024 * 1024 * 1024
	if !isEmpty(benchmark.Data) {
		return errors.New("data directory is not empty")
	}
	db, err := bitcask.Open(
		benchmark.Data,
		bitcask.WithMaxDatafileSize(2*maxValueSize),
		bitcask.WithMaxValueSize(uint64(maxValueSize)),
		// To be able to compare between engines, we make all of them sync after every write.
		// This lowers throughput, but it makes relative differences between engines clearer.
		bitcask.WithSync(true),
	)
	if err != nil {
		return errors.WithStack(err)
	}
	e.db = db
	return nil
}

func (*Bitcask) Name() string {
	return "bitcask"
}

func (e *Bitcask) Set(key []byte, value []byte) errors.E {
	tx := e.db.Transaction()
	defer tx.Discard()

	err := tx.Put(key, value)
	if err != nil {
		return errors.WithStack(err)
	}

	return errors.WithStack(tx.Commit())
}
