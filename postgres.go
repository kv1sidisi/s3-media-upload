package main

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const databaseStartupTimeout = 5 * time.Second

func openPostgres(parent context.Context, databaseURL string) (*pgxpool.Pool, error) {
	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, errors.New("postgres configuration is invalid")
	}
	poolConfig.ConnConfig.RuntimeParams["application_name"] = "s3-media-upload"

	ctx, cancel := context.WithTimeout(parent, databaseStartupTimeout)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, errors.New("postgres pool creation failed")
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, errors.New("postgres startup ping failed")
	}
	if err := checkSchemaV1(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

func checkSchemaV1(ctx context.Context, pool *pgxpool.Pool) error {
	var count int
	var latest int64
	err := pool.QueryRow(
		ctx,
		"SELECT count(*), COALESCE(max(version), 0) FROM schema_migrations",
	).Scan(&count, &latest)
	if err != nil {
		return errors.New("schema ledger check failed")
	}
	return validateSchemaV1(count, latest)
}

func validateSchemaV1(count int, latest int64) error {
	if count != 1 || latest != 1 {
		return errors.New("schema ledger must contain exactly version 1")
	}
	return nil
}
