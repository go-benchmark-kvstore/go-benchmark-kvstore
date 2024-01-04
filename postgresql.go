package main

import (
	"context"
	"io"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"gitlab.com/tozd/go/errors"
)

var _ Engine = (*Postgresql)(nil)

type Postgresql struct {
	dbpool *pgxpool.Pool
}

func (e *Postgresql) Close() errors.E {
	e.dbpool.Close()
	return nil
}

func (*Postgresql) Sync() errors.E {
	// PostgreSQL synces WAL after every transaction so this is not needed.
	return nil
}

func (e *Postgresql) Get(key []byte) (io.ReadSeekCloser, errors.E) {
	ctx := context.Background()

	tx, err := e.dbpool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.Serializable,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}

	var value []byte
	err = tx.QueryRow(ctx, `SELECT value FROM kv WHERE key=$1`, key).Scan(&value)
	if err != nil {
		tx.Rollback(ctx)
		return nil, errors.WithStack(err)
	}

	return newReadSeekCloser(value, func() error {
		return errors.WithStack(tx.Rollback(ctx))
	}), nil
}

func (e *Postgresql) Init(app *App) errors.E {
	ctx := context.Background()

	dbpool, err := pgxpool.New(ctx, app.Postgresql)
	if err != nil {
		return errors.WithStack(err)
	}
	var maxConnectionsStr string
	err = dbpool.QueryRow(ctx, `SHOW max_connections`).Scan(&maxConnectionsStr)
	if err != nil {
		return errors.WithStack(err)
	}
	maxConnections, err := strconv.Atoi(maxConnectionsStr)
	if err != nil {
		return errors.WithStack(err)
	}
	// We add 1 just in case.
	if maxConnections < app.Readers+app.Writers+1 {
		return errors.New("max_connections too low")
	}
	_, err = dbpool.Exec(ctx, `CREATE TABLE kv (key BYTEA PRIMARY KEY NOT NULL, value BYTEA NOT NULL)`)
	if err != nil {
		return errors.WithStack(err)
	}
	e.dbpool = dbpool
	return nil
}

func (*Postgresql) Name() string {
	return "Postgresql"
}

func (e *Postgresql) Put(key []byte, value []byte) (errE errors.E) {
	ctx := context.Background()

	tx, err := e.dbpool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.Serializable,
		AccessMode: pgx.ReadWrite,
	})
	if err != nil {
		return errors.WithStack(err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx,
		`INSERT INTO kv (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = $2`,
		key,
		value,
	)
	if err != nil {
		return errors.WithStack(err)
	}

	return errors.WithStack(tx.Commit(ctx))
}