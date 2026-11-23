# The cancelled-wait fix on kind

Generated from each run's failover.json; see docs/design/cancelled-waits.md and docs/semantics.md §15.

## before-lag (release a3fcbda (before the fix); harness de94830)

| run | replication_paused_for | promoted_after_s | first_standby_after_s | first_acknowledged_enqueue_after_s | acknowledged | lost_acknowledged | acknowledged_while_paused | of_which_deduplicated | answers_that_found_their_job | new_primary_seen_without_standby | answered_without_standby | of_which_deduplicated_without_standby | confirmed_without_standby | old_primary_cancelled_sync_waits | new_primary_cancelled_sync_waits | pool_full_share_while_paused | pool_acquire_wait_s_while_paused | repeated_starts_by_cause | broker_internal_by_rpc |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| f2-1795427084993283000 | 3s | 17.9 | 23.5 | 21.2 | 6862 | 12 | 12 | 12 | 31 | True | 16 | 16 | 0 | 16 | 24 | hopper-broker-7d9ffc8cdb-j4bmc=0, hopper-broker-7d9ffc8cdb-kh729=1 | hopper-broker-7d9ffc8cdb-j4bmc=0, kh729=13.30 | {} | {} |
| f2-1795427148480296000 | 3s | 17.9 | 23.8 | 19.5 | 6487 | 9 | 9 | 9 | 42 | True | 17 | 17 | 0 | 16 | 33 | hopper-broker-7d9ffc8cdb-j4bmc=0, hopper-broker-7d9ffc8cdb-kh729=1 | hopper-broker-7d9ffc8cdb-j4bmc=0, kh729=13.70 | {} | {} |
| f2-1795427210948306000 | 3s | 18.1 | 24.8 | 19.8 | 6418 | 9 | 9 | 9 | 32 | True | 14 | 14 | 0 | 16 | 24 | hopper-broker-7d9ffc8cdb-j4bmc=0, hopper-broker-7d9ffc8cdb-kh729=1 | j4bmc=0.02, kh729=14.17 | {} | {} |

3 runs; answered again after 19.5-21.2 s (median 19.8); first synchronous standby after 23.5-24.8 s (median 23.8); 19767 acknowledged, 30 lost; 47 answered and 0 confirmed without a standby

## sync-lag (de94830)

| run | replication_paused_for | promoted_after_s | first_standby_after_s | first_acknowledged_enqueue_after_s | acknowledged | lost_acknowledged | acknowledged_while_paused | of_which_deduplicated | answers_that_found_their_job | new_primary_seen_without_standby | answered_without_standby | of_which_deduplicated_without_standby | confirmed_without_standby | old_primary_cancelled_sync_waits | new_primary_cancelled_sync_waits | pool_full_share_while_paused | pool_acquire_wait_s_while_paused | repeated_starts_by_cause | broker_internal_by_rpc |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| f2-1795427317954197000 | 3s | 17.9 | 24.2 | 24.2 | 7248 | 0 | 0 | 0 | 16 | True | 0 | 0 | 0 | 16 | 45 | hopper-broker-76b5b68cc9-dvbf8=0, j6f74=0.89 | dvbf8=0.03, j6f74=7.60 | {} | {} |
| f2-1795427384668228000 | 3s | 18.4 | 25.0 | 24.9 | 7302 | 0 | 0 | 0 | 16 | True | 0 | 0 | 0 | 14 | 34 | hopper-broker-76b5b68cc9-dvbf8=0, hopper-broker-76b5b68cc9-j6f74=1 | dvbf8=0.01, j6f74=13.48 | {} | {} |
| f2-1795427460141735000 | 3s | 18.3 | 24.4 | 24.6 | 7298 | 0 | 0 | 0 | 16 | True | 0 | 0 | 0 | 13 | 52 | hopper-broker-76b5b68cc9-dvbf8=0, hopper-broker-76b5b68cc9-j6f74=1 | hopper-broker-76b5b68cc9-dvbf8=0, j6f74=14.87 | {} | {} |

3 runs; answered again after 24.2-24.9 s (median 24.6); first synchronous standby after 24.2-25.0 s (median 24.4); 21848 acknowledged, 0 lost; 0 answered and 0 confirmed without a standby

## sync (de94830)

| run | replication_paused_for | promoted_after_s | first_standby_after_s | first_acknowledged_enqueue_after_s | acknowledged | lost_acknowledged | acknowledged_while_paused | of_which_deduplicated | answers_that_found_their_job | new_primary_seen_without_standby | answered_without_standby | of_which_deduplicated_without_standby | confirmed_without_standby | old_primary_cancelled_sync_waits | new_primary_cancelled_sync_waits | pool_full_share_while_paused | pool_acquire_wait_s_while_paused | repeated_starts_by_cause | broker_internal_by_rpc |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| f2-1795427550602830000 | 0s | 18.9 | 25.4 | 25.6 | 6988 | 0 | 0 | 0 | 16 | True | 0 | 0 | 0 | 0 | 40 |  |  | {} | {} |
| f2-1795427615388609000 | 0s | 19.0 | 26.0 | 25.8 | 7126 | 0 | 0 | 0 | 16 | True | 0 | 0 | 0 | 0 | 40 |  |  | {} | {} |
| f2-1795427681128853000 | 0s | 18.3 | 24.2 | 24.4 | 7152 | 0 | 0 | 0 | 16 | True | 0 | 0 | 0 | 0 | 46 |  |  | {} | {} |
| f2-1795427745047205000 | 0s | 18.4 | 20.6 | 20.8 | 7092 | 0 | 0 | 0 | 15 | True | 0 | 0 | 0 | 0 | 14 |  |  | {} | {} |
| f2-1795427805785699000 | 0s | 18.6 | 19.9 | 19.7 | 7062 | 0 | 0 | 0 | 15 | True | 0 | 0 | 0 | 0 | 15 |  |  | {} | {} |

5 runs; answered again after 19.7-25.8 s (median 24.4); first synchronous standby after 19.9-26.0 s (median 24.2); 35420 acknowledged, 0 lost; 0 answered and 0 confirmed without a standby

## drain (before=a3fcbda after=1f128c5 started=2026-11-23T09:17:19Z)

```
b1 throughput: after 5,685 (4,577-6,244) vs before 5,602 (4,692-6,399)  n=5/5  1.015x  no measurable effect
b1 commits_per_job: after 0.44 (0.44-0.45) vs before 0.45 (0.44-0.45)  n=5/5  1.023x  no measurable effect
drain-b1-w8-c8-p1-1f128c5-20261123T011725 
drain-b1-w8-c8-p1-1f128c5-20261123T012002 
drain-b1-w8-c8-p1-1f128c5-20261123T012052 
drain-b1-w8-c8-p1-1f128c5-20261123T012314 
drain-b1-w8-c8-p1-1f128c5-20261123T012403 
drain-b1-w8-c8-p1-a3fcbda-20261123T011821 
drain-b1-w8-c8-p1-a3fcbda-20261123T011912 
drain-b1-w8-c8-p1-a3fcbda-20261123T012139 
drain-b1-w8-c8-p1-a3fcbda-20261123T012226 
drain-b1-w8-c8-p1-a3fcbda-20261123T012449 
```
