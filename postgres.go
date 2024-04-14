package main

import (
	"context"
	"io"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
	"gitlab.com/tozd/go/errors"
)

var _ Engine = (*Postgres)(nil)

type Postgres struct {
	dbpool *pgxpool.Pool
}

func (*Postgres) Version(benchmark *Benchmark) (string, errors.E) {
	return postgresVersion(benchmark.Postgres)
}

func (e *Postgres) Close() errors.E {
	e.dbpool.Close()
	return nil
}

func (*Postgres) Sync() errors.E {
	// PostgreSQL synces WAL after every transaction so this is not needed.
	return nil
}

func (e *Postgres) Get(key []byte) (io.ReadSeekCloser, errors.E) {
	ctx := context.Background()

	tx, err := e.dbpool.BeginTx(ctx, pgx.TxOptions{ //nolint:exhaustruct
		IsoLevel:   pgx.Serializable,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}

	var value []byte
	err = tx.QueryRow(ctx, `SELECT value FROM kv WHERE key=$1`, key).Scan(&value)
	if err != nil {
		return nil, errors.Join(err, tx.Rollback(ctx))
	}

	return bytesReadSeekCloser(value, func() error {
		return errors.WithStack(tx.Rollback(ctx))
	}), nil
}

func (e *Postgres) Init(benchmark *Benchmark, _ zerolog.Logger) errors.E {
	ctx := context.Background()

	dbpool, err := pgxpool.New(ctx, benchmark.Postgres)
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
	var superuserReservedConnectionsStr string
	err = dbpool.QueryRow(ctx, `SHOW superuser_reserved_connections`).Scan(&superuserReservedConnectionsStr)
	if err != nil {
		return errors.WithStack(err)
	}
	superuserReservedConnections, err := strconv.Atoi(superuserReservedConnectionsStr)
	if err != nil {
		return errors.WithStack(err)
	}
	// We add 1 just in case.
	if maxConnections-superuserReservedConnections < benchmark.Readers+benchmark.Writers+1 {
		return errors.New("max_connections too low")
	}
	_, err = dbpool.Exec(ctx, `CREATE TABLE kv (key BYTEA PRIMARY KEY NOT NULL, value BYTEA NOT NULL)`)
	if err != nil {
		return errors.WithStack(err)
	}
	e.dbpool = dbpool
	return nil
}

func (*Postgres) Name() string {
	return "postgres"
}

func (e *Postgres) Set(key []byte, value []byte) (errE errors.E) { //nolint:nonamedreturns
	ctx := context.Background()

	tx, err := e.dbpool.BeginTx(ctx, pgx.TxOptions{ //nolint:exhaustruct
		IsoLevel:   pgx.Serializable,
		AccessMode: pgx.ReadWrite,
	})
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() {
		err := tx.Rollback(ctx) //nolint:govet
		if errors.Is(err, pgx.ErrTxClosed) {
			err = nil
		}
		errE = errors.Join(errE, err)
	}()

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
