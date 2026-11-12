# Experiment protocol

## Workload

TPC-C is driven by BenchmarkSQL 5.0 in a locally modified `benchmarksql-5.0-epoch` build. The harness prepares the cluster; it never starts the client.

The database name comes from `EPOCH_TPCC_DB` (default `test`). The defaults follow the configuration used for the paper: 100 warehouses (`EPOCH_LOAD_WAREHOUSES`), 34 active per node (`EPOCH_RUN_WAREHOUSES_PER_NODE`), and 64 client connections split 21 / 22 / 21. The transaction mix is set in the client properties supplied through `EPOCH_TPCC_PROPS_NODE1..3`; the harness does not rewrite them.

There is no matrix runner, no warm-up control and no per-case isolation beyond restoring the data directory. Each case is a manual sequence: prepare, start, run the client, stop, collect.

## YCSB

YCSB runs through a bundled JDBC client (`run_ycsb.sh {load|run} {MC|HC|RO}`) that follows the same workload definition as the Calvin adapter: every transaction carries exactly ten operations, and keys are drawn from a Zipfian distribution over 1,000,000 rows, then scattered by a permutation so the hot rows are not the lowest ids.

| Mix | Writes per transaction | Reads per transaction | Key distribution |
| --- | --- | --- | --- |
| MC | 2 | 8 | Zipf, theta 0.8 |
| HC | 5 | 5 | Zipf, theta 0.9 |
| RO | 0 | 10 | uniform |

The table has ten columns: `ycsb_key` plus nine 100-byte fields. `load` creates the table if needed and inserts 1,000,000 rows. `run` opens 256 connections in total, split evenly across the active replicas (`EPOCH_YCSB_CONNECTIONS`, or `EPOCH_YCSB_THREADS` to fix the per-node count), and measures for 180 s after a 30 s warm-up (`EPOCH_YCSB_SECONDS`, `EPOCH_YCSB_WARMUP`). Each transaction is one SQL transaction, and the reported p50/p95/p99 are per transaction.

## Profiles

`configure_node.sh` selects a profile through `EPOCH_PROFILE`:

| Profile | Effect |
| --- | --- |
| `wait` | baseline: no Pacer mechanism |
| `dbr` | `wait` plus `epoch_dyn_net_catchup=on` |
| `pa` | `wait` plus `epoch_dyn_net_proxy=on` |
| `pacer` | `wait` plus both |

All four share the same node settings: terminal counts come from `env.sh` (21/22/21 by default), thread counts and diagnostics are fixed in `configure_node.sh`, and all four apply network shaping.

## Network

Two round-trip values are configured: `EPOCH_NETWORK_RTT_MS` (default 100 ms) for links incident to a slow node, and `EPOCH_RTT_HEALTHY_MS` (default 35 ms) between healthy nodes. `apply_netem.sh add` installs half of each value as an egress delay, on the slow node for all of its traffic and on healthy nodes per destination, so the achieved round trip includes native latency on top of the configured value.

Bandwidth caps apply only to nodes listed in `EPOCH_DYN_NET_SLOW_NODES`: `uplimit`, `downlimit` or `bilimit` at `EPOCH_DYN_NET_BW_MBPS` (default 10). The same cap is written to `epoch_dyn_net_upload_bps` / `epoch_dyn_net_download_bps` on the slow node so that progress monitoring can compute utilisation; healthy nodes get 0. Downlink shaping redirects traffic from the peer addresses to an `ifb0` device. Shaping needs root or passwordless sudo; `status` prints the current rules and `del` restores the previous state.

Verify with `ping` for the delay and `iperf3` for the rate before trusting a run.

## Outputs

The client writes its own run output per node; per-replica tpmC values are not additive across a full-replication deployment.

Each database instance writes into its data directory:

| File | Contents |
| --- | --- |
| `epoch_state_<stamp>.log` | one line per reporting interval: physical / receive / merge / apply / logical cursors, queue depth, snapshot timeouts, EOF request and resend counters, merge retries, stale messages, average epoch latency |
| `epoch_perf_<stamp>.log` | stage timing and stall attribution, when `epoch_perf_log=on` |
| `epoch_apply_profile_<stamp>.log`, `epoch_merge_profile_<stamp>.log` | per-stage profiles, when the corresponding switch is on |
| `epoch_diag_<stamp>.log`, `epoch_lifecycle_<stamp>.log`, `epoch_stuck_trace_<stamp>.log`, `epoch_sync_trace_<stamp>.log` | diagnostics, when enabled |

`prep_tpcc_cluster.sh` writes a run directory containing `summary.txt` (options, node state, start and finish times) and `postgresql.epoch.nodeN.conf`, a snapshot of the epoch settings actually in force.

## Cutoff and cleanup

The harness enforces no measurement window. The operator decides when to stop the client, and the reported interval is whatever the client measured. Stop the nodes with `stop_node.sh`, then remove the shaping with `apply_netem.sh del`. Keep the per-node logs outside the source tree; they contain host addresses.
