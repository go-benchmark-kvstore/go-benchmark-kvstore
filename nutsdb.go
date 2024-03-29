package main

import (
	"io"
	"math"

	"github.com/nutsdb/nutsdb"
	"github.com/rs/zerolog"
	"gitlab.com/tozd/go/errors"
)

var nutsdbBucketName = "data" //nolint:gochecknoglobals

var _ Engine = (*Nutsdb)(nil)

type Nutsdb struct {
	db *nutsdb.DB
}

func (*Nutsdb) Version(_ *Benchmark) (string, errors.E) {
	return getModuleVersion("github.com/nutsdb/nutsdb")
}

func (e *Nutsdb) Close() errors.E {
	return errors.WithStack(e.db.Close())
}

func (e *Nutsdb) Sync() errors.E {
	return errors.WithStack(e.db.ActiveFile.Sync())
}

func (e *Nutsdb) Get(key []byte) (io.ReadSeekCloser, errors.E) {
	tx, err := e.db.Begin(false)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	value, err := tx.Get(nutsdbBucketName, key)
	if err != nil {
		return nil, errors.Join(err, tx.Rollback())
	}
	return bytesReadSeekCloser(value, func() error {
		return errors.WithStack(tx.Rollback())
	}), nil
}

func (e *Nutsdb) Init(benchmark *Benchmark, _ zerolog.Logger) errors.E {
	if !isEmpty(benchmark.Data) {
		return errors.New("data directory is not empty")
	}
	//
	db, err := nutsdb.Open(
		nutsdb.DefaultOptions,
		nutsdb.WithDir(benchmark.Data),
		nutsdb.WithRWMode(nutsdb.MMap),
		// Currently it is possible to store only math.MaxInt32 large values.
		// See: https://github.com/nutsdb/nutsdb/issues/574
		nutsdb.WithSegmentSize(2*math.MaxInt32), //nolint:gomnd
	)
	if err != nil {
		return errors.WithStack(err)
	}
	err = db.Update(func(tx *nutsdb.Tx) error {
		return errors.WithStack(tx.NewBucket(nutsdb.DataStructureBTree, nutsdbBucketName))
	})
	if err != nil {
		return errors.WithStack(err)
	}
	e.db = db
	return nil
}

func (*Nutsdb) Name() string {
	return "nutsdb"
}

func (e *Nutsdb) Set(key []byte, value []byte) (errE errors.E) { //nolint:nonamedreturns
	tx, err := e.db.Begin(true)
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() {
		err := tx.Rollback() //nolint:govet
		if errors.Is(err, nutsdb.ErrDBClosed) {
			err = nil
		}
		errE = errors.Join(errE, err)
	}()

	err = tx.Put(nutsdbBucketName, key, value, 0)
	if err != nil {
		return errors.WithStack(err)
	}

	return errors.WithStack(tx.Commit())
}
