-- Which shard this database is, recorded by the first broker that starts on
-- it. A broker configured with the DSNs in another order, or with another
-- shard count, refuses to start instead of misrouting every job by ID
-- (docs/design/sharding.md). An unsharded deployment records shard 0 of 1,
-- which is what stops it being reused as one shard of several: resharding is
-- not supported. One row at most: the primary key is a constant.
create table shard_identity (
  singleton  boolean primary key default true check (singleton),
  shard      int not null check (shard >= 0),
  shards     int not null check (shards between 1 and 256 and shard < shards),
  claimed_at timestamptz not null default now()
);
