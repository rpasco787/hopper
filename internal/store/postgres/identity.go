package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/rpasco787/hopper/internal/store"
)

// ShardMismatchError means a database already records itself as a different
// shard, or as part of a different count, than the broker was configured to
// use it as.
type ShardMismatchError struct {
	Shard, Shards         int // this broker's configuration
	HeldShard, HeldShards int // what the database recorded first
}

func (e *ShardMismatchError) Error() string {
	return fmt.Sprintf("store: this database is shard %d of %d, but the broker was configured to use it as shard %d of %d: "+
		"list the DSNs in their original order, and don't change the shard count (resharding is not supported)",
		e.HeldShard, e.HeldShards, e.Shard, e.Shards)
}

// ClaimShard records this database as shard of shards if it records nothing
// yet, and otherwise checks that it records the same. cmd/broker calls it for
// every database before serving.
//
// Two statements, for the reason Enqueue's lookup is a second statement: a
// select in the insert's own statement would share its snapshot, taken before
// a concurrent claimer committed, and find nothing.
func (s *Store) ClaimShard(ctx context.Context, shard, shards int) error {
	if shards < 1 || shards > store.MaxShards || shard < 0 || shard >= shards {
		return fmt.Errorf("store: claim shard: %d of %d is not a shard", shard, shards)
	}
	if _, err := s.pool.Exec(ctx,
		`insert into shard_identity (shard, shards) values ($1, $2) on conflict (singleton) do nothing`,
		shard, shards); err != nil {
		return wrap("claim shard", err)
	}
	var held, heldOf int
	if err := s.pool.QueryRow(ctx, `select shard, shards from shard_identity`).Scan(&held, &heldOf); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("store: claim shard: the row was deleted under us")
		}
		return wrap("claim shard", err)
	}
	if held != shard || heldOf != shards {
		return &ShardMismatchError{Shard: shard, Shards: shards, HeldShard: held, HeldShards: heldOf}
	}
	return nil
}

// AppliedMigrations lists the migrations recorded in schema_migrations, in
// order. cmd/broker compares the lists across shards after migrating them,
// so a schema that differs between shards stops the broker from serving.
func (s *Store) AppliedMigrations(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx, `select version from schema_migrations order by version`)
	if err != nil {
		return nil, wrap("applied migrations", err)
	}
	got, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, wrap("applied migrations", err)
	}
	return got, nil
}
