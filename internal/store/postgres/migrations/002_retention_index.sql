-- retention: the janitor deletes completed and cancelled rows by age. Without
-- this index every sweep is a seq scan over the whole jobs table, which is the
-- table the janitor exists to keep small.
--
-- Partial, so it only ever holds rows the janitor can actually delete. Not
-- keyed on queue, because retention is global: the janitor asks "what finished
-- before T", never "what finished on queue q". That is why jobs_dead_idx
-- (queue, finished_at) cannot serve this — its leading column is one the
-- janitor never constrains, and its predicate is the one state retention must
-- not touch.
create index jobs_retention_idx on jobs (finished_at)
  where state in ('completed', 'cancelled');
