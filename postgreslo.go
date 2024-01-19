package main

import (
	"bytes"
	"context"
	"io"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
	"gitlab.com/tozd/go/errors"
)

var _ Engine = (*PostgresLO)(nil)

type PostgresLO struct {
	dbpool *pgxpool.Pool
}

func (e *PostgresLO) Close() errors.E {
	e.dbpool.Close()
	return nil
}

func (*PostgresLO) Sync() errors.E {
	// PostgreSQL synces WAL after every transaction so this is not needed.
	return nil
}

func (e *PostgresLO) Get(key []byte) (io.ReadSeekCloser, errors.E) {
	ctx := context.Background()

	tx, err := e.dbpool.BeginTx(ctx, pgx.TxOptions{ //nolint:exhaustruct
		IsoLevel:   pgx.Serializable,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}

	var oid uint32
	err = tx.QueryRow(ctx, `SELECT value FROM kv WHERE key=$1`, key).Scan(&oid)
	if err != nil {
		return nil, errors.Join(err, tx.Rollback(ctx))
	}

	largeObjects := tx.LargeObjects()
	lo, err := largeObjects.Open(ctx, oid, pgx.LargeObjectModeRead)
	if err != nil {
		return nil, errors.Join(err, tx.Rollback(ctx))
	}

	return newReadSeekCloser(lo, func() error {
		return errors.Join(lo.Close(), tx.Rollback(ctx))
	}), nil
}

func (e *PostgresLO) Init(benchmark *Benchmark, _ zerolog.Logger) errors.E {
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
	// We add 1 just in case.
	if maxConnections < benchmark.Readers+benchmark.Writers+1 {
		return errors.New("max_connections too low")
	}
	_, err = dbpool.Exec(ctx, `CREATE SEQUENCE kv_value_seq`)
	if err != nil {
		return errors.WithStack(err)
	}
	_, err = dbpool.Exec(ctx, `CREATE TABLE kv (key BYTEA PRIMARY KEY NOT NULL, value OID NOT NULL DEFAULT nextval('kv_value_seq'))`)
	if err != nil {
		return errors.WithStack(err)
	}
	_, err = dbpool.Exec(ctx, `ALTER SEQUENCE kv_value_seq OWNED BY kv.value`)
	if err != nil {
		return errors.WithStack(err)
	}
	e.dbpool = dbpool
	return nil
}

func (*PostgresLO) Name() string {
	return "postgreslo"
}

func (e *PostgresLO) Set(key []byte, value []byte) (errE errors.E) { //nolint:nonamedreturns
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

	inserted := true
	var oid uint32
	err = tx.QueryRow(ctx,
		`INSERT INTO kv (key) VALUES ($1) ON CONFLICT (key) DO NOTHING RETURNING value`,
		key,
	).Scan(&oid)
	if errors.Is(err, pgx.ErrNoRows) {
		// Nothing was inserted which means key already exists.
		inserted = false
		err = tx.QueryRow(ctx,
			`SELECT value FROM kv WHERE key=$1`,
			key,
		).Scan(&oid)
	}
	if err != nil {
		return errors.WithStack(err)
	}

	largeObjects := tx.LargeObjects()
	if inserted {
		_, err := largeObjects.Create(ctx, oid) //nolint:govet
		if err != nil {
			return errors.WithStack(err)
		}
	}

	lo, err := largeObjects.Open(ctx, oid, pgx.LargeObjectModeWrite)
	if err != nil {
		return errors.WithStack(err)
	}
	// We do not need to defer lo.Close() because any large object descriptors
	// that remain open at the end of a transaction are closed automatically.

	_, err = io.Copy(lo, bytes.NewReader(value))
	if err != nil {
		return errors.WithStack(err)
	}

	return errors.WithStack(tx.Commit(ctx))
}
