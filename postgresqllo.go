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

var _ Engine = (*PostgresqlLO)(nil)

type PostgresqlLO struct {
	dbpool *pgxpool.Pool
}

func (e *PostgresqlLO) Close() errors.E {
	e.dbpool.Close()
	return nil
}

func (*PostgresqlLO) Sync() errors.E {
	// PostgreSQL synces WAL after every transaction so this is not needed.
	return nil
}

func (e *PostgresqlLO) Get(key []byte) (io.ReadSeekCloser, errors.E) {
	ctx := context.Background()

	tx, err := e.dbpool.BeginTx(ctx, pgx.TxOptions{
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

func (e *PostgresqlLO) Init(benchmark *Benchmark, logger zerolog.Logger) errors.E {
	ctx := context.Background()

	dbpool, err := pgxpool.New(ctx, benchmark.Postgresql)
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

func (*PostgresqlLO) Name() string {
	return "postgresqllo"
}

func (e *PostgresqlLO) Put(key []byte, value []byte) (errE errors.E) {
	ctx := context.Background()

	tx, err := e.dbpool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.Serializable,
		AccessMode: pgx.ReadWrite,
	})
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() {
		err := tx.Rollback(ctx)
		if errors.Is(err, pgx.ErrTxClosed) {
			err = nil
		}
		errE = errors.Join(errE, err)
	}()

	var oid uint32
	var inserted bool
	err = tx.QueryRow(ctx,
		`WITH
			existing AS (
				SELECT value FROM kv WHERE key=$1
			),
			inserted AS (
				INSERT INTO kv (key)
				SELECT $1 WHERE NOT EXISTS (SELECT FROM existing)
				RETURNING value
			)
		SELECT value, true FROM inserted
		UNION ALL
		SELECT value, false FROM existing`,
		key,
	).Scan(&oid, &inserted)
	if err != nil {
		return errors.WithStack(err)
	}

	largeObjects := tx.LargeObjects()
	if inserted {
		_, err := largeObjects.Create(ctx, oid)
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
