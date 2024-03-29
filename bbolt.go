package main

import (
	"io"
	"os"
	"path"
	"time"

	"github.com/rs/zerolog"
	"gitlab.com/tozd/go/errors"
	bolt "go.etcd.io/bbolt"
)

var bboltBucketName = []byte("data") //nolint:gochecknoglobals

var _ Engine = (*Bbolt)(nil)

type Bbolt struct {
	db *bolt.DB
}

func (*Bbolt) Version(_ *Benchmark) (string, errors.E) {
	return getModuleVersion("go.etcd.io/bbolt")
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
	return bytesReadSeekCloser(value, func() error {
		return errors.WithStack(tx.Rollback())
	}), nil
}

func (e *Bbolt) Init(benchmark *Benchmark, _ zerolog.Logger) errors.E {
	err := os.MkdirAll(benchmark.Data, 0o700) //nolint:gomnd
	if err != nil {
		return errors.WithStack(err)
	}
	if !isEmpty(benchmark.Data) {
		return errors.New("data directory is not empty")
	}
	db, err := bolt.Open(path.Join(benchmark.Data, "data.db"), 0o600, &bolt.Options{ //nolint:exhaustruct,gomnd
		Timeout:      5 * time.Second, //nolint:gomnd
		FreelistType: bolt.FreelistMapType,
	})
	if err != nil {
		return errors.WithStack(err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket(bboltBucketName) //nolint:govet
		return errors.WithStack(err)
	})
	if err != nil {
		return errors.WithStack(err)
	}
	e.db = db
	return nil
}

func (*Bbolt) Name() string {
	return "bbolt"
}

func (e *Bbolt) Set(key []byte, value []byte) (errE errors.E) { //nolint:nonamedreturns
	tx, err := e.db.Begin(true)
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() {
		err := tx.Rollback() //nolint:govet
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
