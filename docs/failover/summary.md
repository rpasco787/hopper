# Failover proofs on kind

Generated from each run's failover.json; see docs/semantics.md §15.

## sync (ref 12d598e)

| run | replication_paused_for | promoted_after_s | routed_after_s | first_acknowledged_enqueue_after_s | acknowledged | lost_acknowledged | lost_answered_only_as_deduplicated | acknowledged_while_paused | answers_that_found_their_job | repeated_starts_by_cause | broker_internal_by_rpc | old_primary_cancelled_sync_waits |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| f2-1795416348963758000 | 0s | 17.8 | 17.9 | 19.3 | 6050 | 0 | 0 | 0 | 35 | {} | {} | 0 |
| f2-1795416407657553000 | 0s | 18.0 | 18.3 | 19.6 | 6060 | 0 | 0 | 0 | 33 | {} | {} | 0 |
| f2-1795416466584335000 | 0s | 18.1 | 18.3 | 19.5 | 6122 | 0 | 0 | 0 | 22 | {} | {} | 0 |
| f2-1795416541303352000 | 0s | 18.2 | 18.6 | 20.3 | 6987 | 0 | 0 | 0 | 16 | {} | {} | 0 |
| f2-1795416600433820000 | 0s | 18.1 | 18.1 | 19.5 | 6136 | 0 | 0 | 0 | 43 | {} | {} | 0 |

5 runs; answered again after 19.3-20.3 s (median 19.5); 31355 acknowledged, 0 lost

## sync-lag (ref 12d598e)

| run | replication_paused_for | promoted_after_s | routed_after_s | first_acknowledged_enqueue_after_s | acknowledged | lost_acknowledged | lost_answered_only_as_deduplicated | acknowledged_while_paused | answers_that_found_their_job | repeated_starts_by_cause | broker_internal_by_rpc | old_primary_cancelled_sync_waits |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| f2-1795416679045154000 | 3s | 18.3 | 18.7 | 19.7 | 6065 | 0 | 0 | 0 | 34 | {} | {} | 12 |
| f2-1795416740958047000 | 3s | 19.2 | 19.3 | 21.9 | 6413 | 5 | 5 | 5 | 27 | {} | {} | 12 |
| f2-1795416815194577000 | 3s | 4.3 | 4.6 | 5.9 | 7000 | 9 | 9 | 9 | 10 | {} | {} | 13 |

3 runs; answered again after 5.9-21.9 s (median 19.7); 19478 acknowledged, 14 lost

## async (ref 12d598e)

| run | replication_paused_for | promoted_after_s | routed_after_s | first_acknowledged_enqueue_after_s | acknowledged | lost_acknowledged | lost_answered_only_as_deduplicated | acknowledged_while_paused | answers_that_found_their_job | repeated_starts_by_cause | broker_internal_by_rpc | old_primary_cancelled_sync_waits |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| f2-1795417076596053000 | 0s | 18.9 | 19.3 | 19.8 | 7003 | 0 | 0 | 0 | 0 | {} | {} | 0 |
| f2-1795417138833212000 | 0s | 18.8 | 18.9 | 19.1 | 7172 | 0 | 0 | 0 | 0 | {} | {} | 0 |
| f2-1795417196988349000 | 0s | 18.8 | 18.9 | 19.3 | 7000 | 0 | 0 | 0 | 0 | {} | {} | 0 |

3 runs; answered again after 19.1-19.8 s (median 19.3); 21175 acknowledged, 0 lost

## async-lag (ref ab24dec)

| run | replication_paused_for | promoted_after_s | routed_after_s | first_acknowledged_enqueue_after_s | acknowledged | lost_acknowledged | lost_answered_only_as_deduplicated | acknowledged_while_paused | answers_that_found_their_job | repeated_starts_by_cause | broker_internal_by_rpc | old_primary_cancelled_sync_waits |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| f2-1795417549343340000 | 3s | 18.8 | 18.8 | 18.9 | 7645 | 689 | 0 | 657 | 0 | {'acked': 10} | {} | 0 |
| f2-1795417612562662000 | 3s | 18.4 | 18.7 | 18.7 | 7769 | 652 | 0 | 633 | 0 | {'acked': 22} | {} | 0 |
| f2-1795417674719087000 | 3s | 18.2 | 18.6 | 18.8 | 7753 | 694 | 0 | 661 | 0 | {'acked': 11} | {} | 0 |

3 runs; answered again after 18.7-18.9 s (median 18.8); 23167 acknowledged, 2035 lost
