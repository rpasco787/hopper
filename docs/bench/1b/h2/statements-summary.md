# H2 statement diagnostics

Medians from the top-15 statement snapshots; shares are of all recorded statement execution time. Blocks are shared blocks hit + read per call.

| Variant | Brokers | Statement | Time share % | Mean ms/call | Blocks/call | Runs present |
|---|---:|---|---:|---:|---:|---:|
| async | 1 | Ack one | 30.1 | 0.135 | 12.2 | 5 |
| async | 1 | Heartbeat one | 35.8 | 0.163 | 12.0 | 5 |
| async | 1 | Lease | 29.2 | 0.571 | 178.3 | 5 |
| async | 2 | Ack one | 27.9 | 0.290 | 13.2 | 5 |
| async | 2 | Heartbeat one | 42.8 | 0.418 | 12.5 | 5 |
| async | 2 | Lease | 26.0 | 1.153 | 277.3 | 5 |
| async | 4 | Ack one | 29.5 | 1.058 | 14.0 | 5 |
| async | 4 | Heartbeat one | 50.6 | 1.700 | 12.9 | 5 |
| async | 4 | Lease | 18.3 | 3.023 | 460.2 | 5 |
| batch | 1 | Ack batch | 19.2 | 0.366 | 190.0 | 5 |
| batch | 1 | Ack one | 0.1 | 1.036 | 13.1 | 3 |
| batch | 1 | Heartbeat batch | 19.6 | 0.372 | 177.5 | 5 |
| batch | 1 | Heartbeat one | 8.3 | 0.339 | 11.8 | 5 |
| batch | 1 | Lease | 48.8 | 0.393 | 185.0 | 5 |
| batch | 2 | Ack batch | 17.2 | 0.406 | 110.4 | 5 |
| batch | 2 | Ack one | 0.1 | 0.535 | 14.9 | 5 |
| batch | 2 | Heartbeat batch | 18.3 | 0.411 | 100.9 | 5 |
| batch | 2 | Heartbeat one | 21.5 | 1.047 | 12.2 | 5 |
| batch | 2 | Lease | 37.7 | 0.566 | 190.9 | 5 |
| batch | 4 | Ack batch | 15.1 | 0.402 | 62.5 | 5 |
| batch | 4 | Ack one | 1.8 | 4.000 | 13.9 | 5 |
| batch | 4 | Heartbeat batch | 17.3 | 0.458 | 57.7 | 5 |
| batch | 4 | Heartbeat one | 35.5 | 3.014 | 12.5 | 5 |
| batch | 4 | Lease | 27.0 | 0.794 | 190.5 | 5 |
| batch-flights1 | 1 | Ack batch | 17.1 | 0.419 | 339.8 | 5 |
| batch-flights1 | 1 | Heartbeat batch | 18.4 | 0.467 | 328.6 | 5 |
| batch-flights1 | 1 | Heartbeat one | 2.4 | 0.248 | 11.6 | 5 |
| batch-flights1 | 1 | Lease | 56.5 | 0.354 | 188.9 | 5 |
| batch-flights1 | 2 | Ack batch | 16.9 | 0.545 | 189.8 | 5 |
| batch-flights1 | 2 | Ack one | 0.0 | 0.614 | 14.8 | 2 |
| batch-flights1 | 2 | Heartbeat batch | 17.2 | 0.544 | 181.0 | 5 |
| batch-flights1 | 2 | Heartbeat one | 12.4 | 1.343 | 11.9 | 5 |
| batch-flights1 | 2 | Lease | 44.3 | 0.600 | 190.4 | 5 |
| batch-flights1 | 4 | Ack batch | 15.7 | 0.532 | 105.7 | 5 |
| batch-flights1 | 4 | Ack one | 0.4 | 4.257 | 14.4 | 5 |
| batch-flights1 | 4 | Heartbeat batch | 17.3 | 0.522 | 100.1 | 5 |
| batch-flights1 | 4 | Heartbeat one | 25.5 | 2.446 | 12.4 | 5 |
| batch-flights1 | 4 | Lease | 33.9 | 0.757 | 190.1 | 5 |
| nobatch | 1 | Ack one | 35.1 | 0.117 | 11.9 | 5 |
| nobatch | 1 | Heartbeat one | 36.3 | 0.123 | 11.9 | 5 |
| nobatch | 1 | Lease | 24.5 | 0.335 | 142.9 | 5 |
| nobatch | 2 | Ack one | 29.2 | 0.213 | 12.5 | 5 |
| nobatch | 2 | Heartbeat one | 45.0 | 0.324 | 12.3 | 5 |
| nobatch | 2 | Lease | 23.1 | 0.737 | 183.3 | 5 |
| nobatch | 4 | Ack one | 22.8 | 0.428 | 13.8 | 5 |
| nobatch | 4 | Heartbeat one | 59.0 | 1.217 | 12.8 | 5 |
| nobatch | 4 | Lease | 16.2 | 1.474 | 221.6 | 5 |
