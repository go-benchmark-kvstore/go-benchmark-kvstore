package main

import (
	"io"
	"os"
	"path"
	"time"

	"gitlab.com/tozd/go/errors"
	bolt "go.etcd.io/bbolt"
)

var bboltBucketName = []byte("data")

var _ Engine = (*Bbolt)(nil)

type Bbolt struct {
	db *bolt.DB
}

func (e *Bbolt) Close() errors.E {
	return errors.WithStack(e.db.Close())
}

func (e *Bbolt) Sync() errors.E {
	return errors.WithStack(e.db.Sync())
}

func (e *Bbolt) Get(key []byte) (io.ReadSeekCloser, errors.E) {
	tx, err := e.db.Begin(false)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	value := tx.Bucket(bboltBucketName).Get(key)
	if value == nil {
		return nil, errors.Join(errors.New("does not exist"), tx.Rollback())
	}
	return newReadSeekCloser(value, func() error {
		return errors.WithStack(tx.Rollback())
	}), nil
}

func (e *Bbolt) Init(app *App) errors.E {
	err := os.MkdirAll(app.Data, 0700)
	if err != nil {
		return errors.WithStack(err)
	}
	if !isEmpty(app.Data) {
		return errors.New("data directory is not empty")
	}
	db, err := bolt.Open(path.Join(app.Data, "data.db"), 0600, &bolt.Options{
		Timeout:      5 * time.Second,
		FreelistType: bolt.FreelistMapType,
	})
	if err != nil {
		return errors.WithStack(err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket(bboltBucketName)
		return errors.WithStack(err)
	})
	if err != nil {
		return errors.WithStack(err)
	}
	e.db = db
	return nil
}

func (*Bbolt) Name() string {
	return "Bbolt"
}

func (e *Bbolt) Put(key []byte, value []byte) (errE errors.E) {
	tx, err := e.db.Begin(true)
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() {
		err := tx.Rollback()
		if errors.Is(err, bolt.ErrTxClosed) {
			err = nil
		}
		errE = errors.Join(errE, err)
	}()

	err = tx.Bucket(bboltBucketName).Put(key, value)
	if err != nil {
		return errors.WithStack(err)
	}

	return errors.WithStack(tx.Commit())
}
