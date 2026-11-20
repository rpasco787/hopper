alter table jobs set (fillfactor = 80, autovacuum_vacuum_scale_factor = 0.01, autovacuum_vacuum_insert_scale_factor = 0.01, autovacuum_vacuum_cost_delay = 0)
