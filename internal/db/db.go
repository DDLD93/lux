package db

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schema string

// Connect parses the DSN, ensures the target database exists (creating it
// against the `postgres` maintenance DB if not), then returns a pool to it.
func Connect(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}

	if err := ensureDatabase(ctx, cfg.ConnConfig); err != nil {
		return nil, err
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	return pool, nil
}

func ensureDatabase(ctx context.Context, target *pgx.ConnConfig) error {
	dbName := target.Database
	if dbName == "" || dbName == "postgres" {
		return nil
	}

	admin := target.Copy()
	admin.Database = "postgres"

	conn, err := pgx.ConnectConfig(ctx, admin)
	if err != nil {
		// Fall back to letting the main connect surface the real error.
		return nil //nolint:nilerr
	}
	defer conn.Close(ctx)

	var exists bool
	if err := conn.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)`, dbName).Scan(&exists); err != nil {
		return fmt.Errorf("check database: %w", err)
	}
	if exists {
		return nil
	}
	// pg identifiers can't be parameterized; quote defensively.
	if _, err := conn.Exec(ctx, fmt.Sprintf(`CREATE DATABASE %s`, pgx.Identifier{dbName}.Sanitize())); err != nil {
		return fmt.Errorf("create database %q: %w", dbName, err)
	}
	return nil
}

func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, schema); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	return nil
}
