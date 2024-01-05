package main

import (
	"io"
	"math"

	"github.com/nutsdb/nutsdb"
	"gitlab.com/tozd/go/errors"
)

var nutsdbBucketName = "data"

var _ Engine = (*Nutsdb)(nil)

type Nutsdb struct {
	db *nutsdb.DB
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
	return newReadSeekCloser(value, func() error {
		return errors.WithStack(tx.Rollback())
	}), nil
}

func (e *Nutsdb) Init(app *App) errors.E {
	if !isEmpty(app.Data) {
		return errors.New("data directory is not empty")
	}
	//
	db, err := nutsdb.Open(
		nutsdb.DefaultOptions,
		nutsdb.WithDir(app.Data),
		nutsdb.WithRWMode(nutsdb.MMap),
		// Currently it is possible to store only math.MaxInt32 large values.
		// See: https://github.com/nutsdb/nutsdb/issues/574
		nutsdb.WithSegmentSize(2*math.MaxInt32),
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
	return "Nutsdb"
}

func (e *Nutsdb) Put(key []byte, value []byte) (errE errors.E) {
	tx, err := e.db.Begin(true)
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() {
		err := tx.Rollback()
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
