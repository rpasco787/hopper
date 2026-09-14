create table jobs (
  id              uuid primary key default gen_random_uuid(),  -- generate UUIDv7 client-side; default is a fallback
  queue           text not null,                 -- job type / handler name
  payload         jsonb not null,
  state           text not null check (state in ('scheduled','available','leased','completed','cancelled','dead')),
  priority        smallint not null default 0,   -- higher leases first
  run_at          timestamptz not null,          -- when the job becomes due (delay, or retry backoff)
  attempts        int not null default 0 check (attempts >= 0),        -- number of leases ever granted
  max_attempts    int not null default 5 check (max_attempts >= 1),
  lease_id        uuid,                          -- token for the current/last lease; overwritten on each lease, never cleared
  leased_by       text,                          -- worker id, observability only
  leased_at       timestamptz,
  lease_expires   timestamptz,                   -- visibility timeout
  idempotency_key text,                          -- unique per queue when present
  last_error      text,
  created_at      timestamptz not null default now(),
  updated_at      timestamptz not null default now(),   -- set explicitly in every update; no trigger
  finished_at     timestamptz,                   -- set when state becomes completed | cancelled | dead
  constraint jobs_leased_has_lease_check
    check (state <> 'leased' or (lease_id is not null and lease_expires is not null))
);

-- dedup: duplicate Enqueue returns the existing row
create unique index jobs_idem_uidx    on jobs (queue, idempotency_key) where idempotency_key is not null;
-- lease: hot set only; queue leading so `queue = any($1)` is an index scan per queue
create index        jobs_lease_idx    on jobs (queue, priority desc, run_at) where state = 'available';
-- promoter: scheduled -> available when due
create index        jobs_promote_idx  on jobs (run_at) where state = 'scheduled';
-- reaper: expired leases
create index        jobs_reap_idx     on jobs (lease_expires) where state = 'leased';
-- DLQ listing
create index        jobs_dead_idx     on jobs (queue, finished_at) where state = 'dead';

create table cron_schedules (
  name              text primary key,
  queue             text not null,
  payload           jsonb not null default '{}',
  cron_expr         text not null,
  timezone          text not null default 'UTC',
  enabled           boolean not null default true,
  priority          smallint not null default 0,
  max_attempts      int not null default 5,
  next_run_at       timestamptz not null,
  last_enqueued_at  timestamptz,
  created_at        timestamptz not null default now(),
  updated_at        timestamptz not null default now()
);
