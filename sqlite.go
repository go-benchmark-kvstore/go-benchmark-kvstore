package main

import (
	"bytes"
	"io"
	"os"
	"path"

	"crawshaw.io/sqlite"
	"crawshaw.io/sqlite/sqlitex"
	"github.com/rs/zerolog"
	"gitlab.com/tozd/go/errors"
)

var _ Engine = (*Sqlite)(nil)

type Sqlite struct {
	dbpool *sqlitex.Pool
}

func (e *Sqlite) Close() errors.E {
	return errors.WithStack(e.dbpool.Close())
}

func (e *Sqlite) Sync() errors.E {
	// Not supported. See: https://github.com/crawshaw/sqlite/issues/145
	return nil
}

func (e *Sqlite) Get(key []byte) (io.ReadSeekCloser, errors.E) {
	// We do not pass context so that tracer is not setup.
	conn := e.dbpool.Get(nil)

	tx := sqlitex.Save(conn)

	found := false
	var rowid int64
	err := sqlitex.Exec(conn, `SELECT rowid FROM kv WHERE key=?`, func(stmt *sqlite.Stmt) error {
		found = true
		rowid = stmt.ColumnInt64(0)
		return nil
	}, key)
	if err != nil {
		tx(&err)
		e.dbpool.Put(conn)
		return nil, errors.WithStack(err)
	}
	if !found {
		err := errors.Base("not found")
		tx(&err)
		e.dbpool.Put(conn)
		return nil, errors.WithStack(err)
	}
	valueBlob, err := conn.OpenBlob("main", "kv", "value", rowid, false)
	if err != nil {
		tx(&err)
		e.dbpool.Put(conn)
		return nil, errors.WithStack(err)
	}
	return newReadSeekCloser(valueBlob, func() error {
		err1 := valueBlob.Close()
		var err2 error
		tx(&err2)
		e.dbpool.Put(conn)
		return errors.Join(err1, err2)
	}), nil
}

func (e *Sqlite) Init(benchmark *Benchmark, logger zerolog.Logger) errors.E {
	err := os.MkdirAll(benchmark.Data, 0700)
	if err != nil {
		return errors.WithStack(err)
	}
	if !isEmpty(benchmark.Data) {
		return errors.New("data directory is not empty")
	}
	dbpool, err := sqlitex.Open(
		path.Join(benchmark.Data, "data.db"),
		sqlite.SQLITE_OPEN_READWRITE|
			sqlite.SQLITE_OPEN_CREATE|
			sqlite.SQLITE_OPEN_WAL|
			sqlite.SQLITE_OPEN_NOMUTEX|
			sqlite.SQLITE_OPEN_SHAREDCACHE,
		benchmark.Readers+benchmark.Writers+1, // We add 1 just in case.
	)
	if err != nil {
		return errors.WithStack(err)
	}
	// We do not pass context so that tracer is not setup.
	conn := dbpool.Get(nil)
	defer dbpool.Put(conn)
	err = sqlitex.Exec(conn, `CREATE TABLE kv (key BLOB PRIMARY KEY NOT NULL, value BLOB NOT NULL)`, nil)
	if err != nil {
		return errors.WithStack(err)
	}
	e.dbpool = dbpool
	return nil
}

func (*Sqlite) Name() string {
	return "sqlite"
}

func (e *Sqlite) Set(key []byte, value []byte) (errE errors.E) {
	// We do not pass context so that tracer is not setup.
	conn := e.dbpool.Get(nil)
	defer e.dbpool.Put(conn)

	tx := sqlitex.Save(conn)
	defer func() {
		// Calling "tx" already joins errors.
		var err error = errE
		tx(&err)
		errE = errors.WithStack(err)
	}()

	stmt := conn.Prep(`INSERT OR REPLACE INTO kv (key, value) VALUES ($key, $value)`)
	// Primary key cannot be written with blob I/O.
	stmt.SetBytes("$key", key)
	stmt.SetZeroBlob("$value", int64(len(value)))
	_, err := stmt.Step()
	if err != nil {
		return errors.WithStack(err)
	}
	rowid := conn.LastInsertRowID()
	valueBlob, err := conn.OpenBlob("main", "kv", "value", rowid, true)
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() {
		errE = errors.Join(errE, valueBlob.Close())
	}()
	_, err = io.Copy(valueBlob, bytes.NewReader(value))
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}
