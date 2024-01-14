package main

import (
	"io"

	"github.com/cockroachdb/pebble"
	"github.com/rs/zerolog"
	"gitlab.com/tozd/go/errors"
)

var _ Engine = (*Pebble)(nil)

type Pebble struct {
	db *pebble.DB
}

func (e *Pebble) Close() errors.E {
	return errors.WithStack(e.db.Close())
}

func (e *Pebble) Sync() errors.E {
	return errors.WithStack(e.db.Flush())
}

func (e *Pebble) Get(key []byte) (io.ReadSeekCloser, errors.E) {
	// Snapshot is similar enough to a read-only transaction.
	tx := e.db.NewSnapshot()

	value, closer, err := tx.Get(key)
	if err != nil {
		return nil, errors.Join(err, tx.Close())
	}
	return bytesReadSeekCloser(value, func() error {
		return errors.Join(closer.Close(), tx.Close())
	}), nil
}

func (e *Pebble) Init(benchmark *Benchmark, logger zerolog.Logger) errors.E {
	if !isEmpty(benchmark.Data) {
		return errors.New("data directory is not empty")
	}
	db, err := pebble.Open(benchmark.Data, &pebble.Options{ //nolint:exhaustruct
		// The newest format for the current version of Pebble.
		FormatMajorVersion: pebble.FormatPrePebblev1MarkedCompacted,
		ErrorIfExists:      true,
		Logger:             loggerWrapper{logger},
		Levels: []pebble.LevelOptions{{ //nolint:exhaustruct
			// We disable compression so that measurements are comparable.
			Compression: pebble.NoCompression,
		}},
	})
	if err != nil {
		return errors.WithStack(err)
	}
	e.db = db
	return nil
}

func (*Pebble) Name() string {
	return "pebble"
}

func (e *Pebble) Set(key []byte, value []byte) (errE errors.E) { //nolint:nonamedreturns
	// Batch is not really a transaction, but close enough for our needs.
	// Maybe we should use instead e.db.NewSnapshot().NewIndexedBatch() once it is available.
	// See: https://github.com/cockroachdb/pebble/issues/1416
	tx := e.db.NewIndexedBatch()
	defer func() {
		if errE == nil {
			// We return tx to the pool when there is no error.
			// See: https://github.com/cockroachdb/pebble/issues/3190
			errE = errors.WithStack(tx.Close())
		}
	}()

	// To be able to compare between engines, we make all of them sync after every write.
	// This lowers throughput, but it makes relative differences between engines clearer.
	err := tx.Set(key, value, pebble.Sync)
	if err != nil {
		return errors.WithStack(err)
	}

	return errors.WithStack(tx.Commit(pebble.Sync))
}
