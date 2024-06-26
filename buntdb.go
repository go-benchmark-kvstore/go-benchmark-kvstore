package main

import (
	"io"
	"os"
	"path"
	"time"

	"github.com/rs/zerolog"
	"github.com/tidwall/buntdb"
	"gitlab.com/tozd/go/errors"
	"gitlab.com/tozd/go/x"
)

var _ Engine = (*Buntdb)(nil)

type Buntdb struct {
	db *buntdb.DB
}

func (*Buntdb) Version(_ *Benchmark) (string, errors.E) {
	return getModuleVersion("github.com/tidwall/buntdb")
}

func (e *Buntdb) Close() errors.E {
	return errors.WithStack(e.db.Close())
}

func (e *Buntdb) Sync() errors.E {
	// Buntdb syncs every second.
	time.Sleep(time.Second)
	return nil
}

func (e *Buntdb) Get(key []byte) (io.ReadSeekCloser, errors.E) {
	tx, err := e.db.Begin(false)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	value, err := tx.Get(x.ByteSlice2String(key))
	if err != nil {
		return nil, errors.Join(err, tx.Rollback())
	}
	return bytesReadSeekCloser(x.String2ByteSlice(value), func() error {
		return errors.WithStack(tx.Rollback())
	}), nil
}

func (e *Buntdb) Init(benchmark *Benchmark, _ zerolog.Logger) errors.E {
	err := os.MkdirAll(benchmark.Data, 0o700) //nolint:gomnd
	if err != nil {
		return errors.WithStack(err)
	}
	if !isEmpty(benchmark.Data) {
		return errors.New("data directory is not empty")
	}
	db, err := buntdb.Open(path.Join(benchmark.Data, "data.db"))
	if err != nil {
		return errors.WithStack(err)
	}
	var config buntdb.Config
	err = db.ReadConfig(&config)
	if err != nil {
		return errors.WithStack(err)
	}
	// To be able to compare between engines, we make all of them sync after every write.
	// This lowers throughput, but it makes relative differences between engines clearer.
	config.SyncPolicy = buntdb.Always
	err = db.SetConfig(config)
	if err != nil {
		return errors.WithStack(err)
	}
	e.db = db
	return nil
}

func (*Buntdb) Name() string {
	return "buntdb"
}

func (e *Buntdb) Set(key []byte, value []byte) (errE errors.E) { //nolint:nonamedreturns
	tx, err := e.db.Begin(true)
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() {
		err := tx.Rollback() //nolint:govet
		if errors.Is(err, buntdb.ErrTxClosed) {
			err = nil
		}
		errE = errors.Join(errE, err)
	}()

	_, _, err = tx.Set(x.ByteSlice2String(key), x.ByteSlice2String(value), nil)
	if err != nil {
		return errors.WithStack(err)
	}

	return errors.WithStack(tx.Commit())
}
