package main

import (
	"io"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
	"github.com/rs/zerolog"
	"gitlab.com/tozd/go/errors"
)

var _ Engine = (*Badger)(nil)

type Badger struct {
	db *badger.DB
}

func (e *Badger) Close() errors.E {
	return errors.WithStack(e.db.Close())
}

func (e *Badger) Sync() errors.E {
	return errors.WithStack(e.db.Sync())
}

func (e *Badger) Get(key []byte) (io.ReadSeekCloser, errors.E) {
	tx := e.db.NewTransaction(false)

	item, err := tx.Get(key)
	if err != nil {
		tx.Discard()
		return nil, errors.WithStack(err)
	}
	var value []byte
	err = item.Value(func(v []byte) error {
		value = v
		return nil
	})
	if err != nil {
		tx.Discard()
		return nil, errors.WithStack(err)
	}
	return bytesReadSeekCloser(value, func() error {
		tx.Discard()
		return nil
	}), nil
}

func (e *Badger) Init(benchmark *Benchmark, logger zerolog.Logger) errors.E {
	if !isEmpty(benchmark.Data) {
		return errors.New("data directory is not empty")
	}
	// Default options already have ValueLogFileSize at maximum value (2 GB).
	// See: https://github.com/dgraph-io/badger/issues/2040
	opts := badger.DefaultOptions(benchmark.Data)
	// We disable compression so that measurements are comparable.
	opts = opts.WithCompression(options.None)
	// When compression is disabled, cache size should be 0, says documentation.
	opts = opts.WithBlockCacheSize(0)
	// To be able to compare between engines, we make all of them sync after every write.
	// This lowers throughput, but it makes relative differences between engines clearer.
	opts = opts.WithSyncWrites(true)
	opts = opts.WithLogger(loggerWrapper{logger})
	db, err := badger.Open(opts)
	if err != nil {
		return errors.WithStack(err)
	}
	e.db = db
	return nil
}

func (*Badger) Name() string {
	return "badger"
}

func (e *Badger) Set(key, value []byte) errors.E {
	tx := e.db.NewTransaction(true)
	defer tx.Discard()

	err := tx.Set(key, value)
	if err != nil {
		return errors.WithStack(err)
	}

	return errors.WithStack(tx.Commit())
}
