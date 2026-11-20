# H2 comparison

Five-run medians; ranges in parentheses. Each variant uses code and harness `3aaf0c1`.
Confirmation statements/job includes Ack and Heartbeat batch and one-row statements. Fallback shares use exact counts.
RPC p99 values interpolate within fixed histogram buckets: 0.5, 1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000 ms.

## 1 broker(s)

| Variant | Jobs/s (range) | Commits/job | WAL syncs/job | Confirmation statements/job | WALWrite+WALSync % | Active backends | Ack/HB p99 ms | Submitted batch Ack/HB | Exact fallback % Ack/HB |
|---|---|---:|---:|---:|---:|---:|---|---|---|
| nobatch | 3,369 (2,039–4,725) | 2.24 | 0.425 | 2.000 | 81.7 | 9.89 | 49.59/48.19 | – | – |
| batch | 6,446 (5,304–8,170) | 0.45 | 0.142 | 0.227 | 82.3 | 6.00 | 28.25/33.72 | 11.0/10.6 | 0.003/4.027 |
| batch-flights1 | 7,251 (6,832–8,895) | 0.32 | 0.137 | 0.117 | 80.0 | 3.17 | 24.60/23.82 | 19.9/19.3 | 0.000/1.490 |
| async | 5,277 (4,281–6,701) | 2.24 | 0.002 | 2.000 | 0.0 | 1.88 | 37.83/35.25 | – | – |

## 2 broker(s)

| Variant | Jobs/s (range) | Commits/job | WAL syncs/job | Confirmation statements/job | WALWrite+WALSync % | Active backends | Ack/HB p99 ms | Submitted batch Ack/HB | Exact fallback % Ack/HB |
|---|---|---:|---:|---:|---:|---:|---|---|---|
| nobatch | 3,406 (2,753–4,793) | 2.23 | 0.259 | 2.000 | 90.3 | 20.33 | 59.17/54.32 | – | – |
| batch | 4,645 (4,168–6,599) | 0.62 | 0.131 | 0.386 | 79.3 | 8.44 | 47.21/46.81 | 6.3/6.2 | 0.058/6.415 |
| batch-flights1 | 4,232 (2,573–7,826) | 0.43 | 0.135 | 0.216 | 75.9 | 5.33 | 75.40/78.18 | 11.0/10.8 | 0.000/3.170 |
| async | 6,185 (3,272–6,372) | 2.23 | 0.002 | 2.000 | 0.0 | 6.57 | 23.24/23.98 | – | – |

## 4 broker(s)

| Variant | Jobs/s (range) | Commits/job | WAL syncs/job | Confirmation statements/job | WALWrite+WALSync % | Active backends | Ack/HB p99 ms | Submitted batch Ack/HB | Exact fallback % Ack/HB |
|---|---|---:|---:|---:|---:|---:|---|---|---|
| nobatch | 3,845 (1,457–4,147) | 2.23 | 0.180 | 2.000 | 75.8 | 37.50 | 48.84/48.57 | – | – |
| batch | 3,800 (2,094–5,087) | 0.93 | 0.128 | 0.657 | 71.9 | 15.38 | 61.79/69.52 | 3.6/3.5 | 0.340/9.068 |
| batch-flights1 | 3,546 (3,130–5,077) | 0.62 | 0.131 | 0.371 | 76.5 | 8.67 | 49.91/70.53 | 6.1/6.1 | 0.050/4.228 |
| async | 3,676 (3,010–5,227) | 2.21 | 0.002 | 2.000 | 0.0 | 13.20 | 48.24/50.00 | – | – |

