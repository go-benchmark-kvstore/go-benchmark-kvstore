package main

import (
	"bytes"
	"os"
	"path"
	"time"

	"github.com/tidwall/buntdb"
	"gitlab.com/tozd/go/errors"
)

var _ Engine = (*Buntdb)(nil)

type Buntdb struct {
	db *buntdb.DB
}

func (e *Buntdb) Close() errors.E {
	return errors.WithStack(e.db.Close())
}

func (e *Buntdb) Sync() errors.E {
	// Buntdb syncs every second.
	time.Sleep(time.Second)
	return nil
}

func (e *Buntdb) Get(key []byte) errors.E {
	tx, err := e.db.Begin(false)
	if err != nil {
		return errors.WithStack(err)
	}
	defer tx.Rollback()

	value, err := tx.Get(byteSlice2String(key))
	if err != nil {
		return errors.WithStack(err)
	}
	return consumerReader(bytes.NewReader(string2ByteSlice(value)))
}

func (e *Buntdb) Init(app *App) errors.E {
	err := os.MkdirAll(app.Data, 0700)
	if err != nil {
		return errors.WithStack(err)
	}
	if !isEmpty(app.Data) {
		return errors.New("data directory is not empty")
	}
	db, err := buntdb.Open(path.Join(app.Data, "data.db"))
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
	return "Buntdb"
}

func (e *Buntdb) Put(key []byte, value []byte) errors.E {
	tx, err := e.db.Begin(true)
	if err != nil {
		return errors.WithStack(err)
	}
	defer tx.Rollback()

	_, _, err = tx.Set(byteSlice2String(key), byteSlice2String(value), nil)
	if err != nil {
		return errors.WithStack(err)
	}

	return errors.WithStack(tx.Commit())
}
