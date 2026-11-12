# Pacer

Research code for Pacer: progress monitoring (PM), dynamic batch reorganization (DBR), and proxy aggregation (PA) for geo-distributed deterministic databases.

This repository contains the **GeoGauss + Pacer** implementation, built on openGauss 6.0.0, together with the three-node TPC-C/YCSB harness used for the GeoGauss experiments. The tree is the complete openGauss source; the additions of this work live in `src/epoch/`, `src/occ/`, and `src/occ-opt/`.

## Key ideas

- **PM.** Each replica samples only its own state, in the send worker: uplink rate from `/proc/net/dev` on a congested sender, downlink rate on a lagging receiver, outgoing queue depth, receive-cursor lag, and current-epoch backlog. There is no global monitor and no additional consensus round.
- **DBR.** A write set is reassigned at the dequeue point of the per-destination send queue, before any replica has observed it. One decision per `(csn, server_id)` is shared by all destination queues, so a write set moves as a whole; the reassigned copy carries a later commit epoch while `csn`, `server_id`, and the read frontier captured at transaction start are unchanged.
- **PA.** A lagging replica announces a session (identifier, first unapplied epoch, proxy); one proxy serves any number of them. It decides per epoch whether to suppress direct delivery and broadcasts that decision to every peer, so all senders route the epoch identically. The proxy merges the epoch normally, then forwards to each lagging replica the survivors from the other sources: complete write sets for candidates, claim-only records for transactions the merge already aborted, and substitute EOFs for each suppressed source.
- **Deterministic path.** Pacer decides when a write set is merged and which replica collects it. Conflict resolution stays with the epoch CRDT: OCC execution and write-set exchange are unchanged, and the merge orders versions by `(csn, server_id)`.
- **Source closure.** An epoch is assembled from per-source contributions. A source's `declared` count is frozen when its EOF record arrives, so a receiver waits for exactly what the sender transmitted, not for what it originally intended to send. This is what lets DBR and PA move input between epochs without a second counting protocol.

## Layout

| Path | Contents |
| --- | --- |
| `src/epoch/e_pacer.{h,cpp}` | PM sampling, DBR decision table and target-epoch selection, PA session and forwarding |
| `src/epoch/e_xact.cpp` | Epoch assignment at transaction start, CSN allocation, pack into the per-destination queues |
| `src/epoch/e_pipeline.cpp` | Per-destination send queues, receive loop, merge and apply pipelines, Pacer hooks |
| `src/epoch/e_recv_ledger.cpp` | Per-epoch `received` / `declared` / `eof` accounting; `declared` freezes when a source's EOF arrives |
| `src/epoch/e_net.cpp` | Per-peer ZMQ PUSH/PULL transport, unicast path, EOF acknowledgement |
| `src/epoch/e_crdt_shmem.cpp` | Per-epoch CRDT; smaller CSN wins, ties broken by node id; claim-only records for PA shadows |
| `src/epoch/e_config.cpp` | Runtime switches (listed in the implementation notes) |
| `src/occ/`, `src/occ-opt/` | OCC engine and version staging used by the epoch pipeline |
| `src/include/epoch/` | Headers for the epoch module |
| `scripts/epoch_benchmark/` | Cluster configuration, TPC-C/YCSB preparation, network shaping |
| `docs/` | Dependencies, implementation details, experiment protocol |

## Requirements

openGauss 6.0.0 builds on CentOS 7.6 (x86_64) or openEuler 20.03 LTS (aarch64) with the packages listed in [dependency preparation](docs/DEPENDENCIES.md). This tree was developed on CentOS 7 with GCC 10.3 taken from the `binarylibs` bundle.

- openGauss 6.0.0 source tree and the matching `openGauss-third_party_binarylibs`, given as an absolute path to `-3rd`.
- CMake, for the `--cmake` build path used below.
- ZeroMQ 4.x runtime library (`libzmq.so`). The C++ binding header is bundled as `src/include/zmq.hpp`. Without the library the epoch module still builds, but the multi-node wire plane is compiled out.
- Three dedicated hosts, one full openGauss instance each; the original deployment used 16 vCPU and 32 GiB per host. Key-based SSH from the driver host, and `iproute2`/`tc` (netem, tbf) for wide-area emulation.
- BenchmarkSQL 5.0 (a locally modified `benchmarksql-5.0-epoch`) with a JDK, to drive TPC-C. It is not bundled; `EPOCH_BENCHMARKSQL` points at its `run` directory. The YCSB client is bundled and needs the same JDK plus a JDBC jar (`EPOCH_JDBC_JAR`).

## Build

```bash
sh build.sh -m release -3rd /path/to/openGauss-third_party_binarylibs --cmake
```

The build tree is `tmp_build` and the install tree is `mppdb_temp_install`; the server binary is `mppdb_temp_install/bin/gaussdb`, which is what `GAUSSHOME` points at. `-m` also accepts `debug` and `memcheck`, and a build without `--cmake` installs under `dest/`. The ZeroMQ prefix is the CMake cache variable `EPOCH_ZMQ_ROOT` (default `/data/geogauss/zmqroot`); it must contain `lib/libzmq.so`, and the configure step reports whether the wire plane is enabled.

## Cluster setup

`scripts/epoch_benchmark/env.sh` holds every deployment-specific value: node addresses, `GAUSSHOME`, the data root, the BenchmarkSQL directory, the database port (32000 by default), the slow-node list, and the profile. Install the tree at the same path on all hosts, or set `EPOCH_NODE2_BENCH_ROOT` and `EPOCH_NODE3_BENCH_ROOT` to the per-host paths, and edit `env.sh` before the first run.

Each node must already hold an initialized instance under `EPOCH_DATA_ROOT` together with a matching `<data dir>_bak` copy. `restore_tpcc_datadir.sh` restores that backup over the live directory before every preparation run, so create and load the copy once with `gs_initdb` and BenchmarkSQL; the harness never initializes a cluster itself.

The reference topology is three replicas. `EPOCH_SKIP_NODE` removes one node from both the membership and the harness steps, so the same tree covers a two-node configuration.

```bash
bash scripts/epoch_benchmark/configure_node.sh <NODE_ID>   # postgresql.conf, pg_hba.conf, tc rules
bash scripts/epoch_benchmark/start_cluster.sh
bash scripts/epoch_benchmark/prep_tpcc_cluster.sh         # restore, deploy, configure, restart, health check
```

`configure_node.sh` writes the epoch settings for the selected profile, appends `trust` entries for the peer addresses, and installs the traffic control rules. Nearly all of these settings are `PGC_POSTMASTER`, so the instance is restarted rather than reloaded. `prep_tpcc_cluster.sh` stops the nodes, restores the data directory, deploys binaries and scripts, applies the configuration, restarts, and prints a health check; it does not start the TPC-C client. `stop_node.sh` stops one node, and `apply_netem.sh del` removes the shaping.

Shaping uses the interface that carries the default route. `apply_netem.sh add` installs half of `EPOCH_NETWORK_RTT_MS` (100 ms by default) as the egress delay of a slow node and toward slow nodes from a healthy one, and half of `EPOCH_RTT_HEALTHY_MS` (35 ms) between healthy nodes. For `uplimit` / `downlimit` / `bilimit` it caps every node listed in `EPOCH_DYN_NET_SLOW_NODES` at `EPOCH_DYN_NET_BW_MBPS`, and downlink shaping redirects peer traffic to an `ifb0` device. `status` prints the current rules. Check the result with `ping` and `iperf3` before trusting a run.

## Running experiments

Profiles select the mechanism under test: `wait` (baseline), `dbr`, `pa`, and `pacer` (both). All four use the same node settings and the same shaping; `configure_node.sh` maps the profile onto `epoch_dyn_net_catchup` and `epoch_dyn_net_proxy`.

The clients run separately on each node:

```bash
cd "$EPOCH_BENCHMARKSQL" && ./runBenchmark.sh <props>   # TPC-C
bash scripts/epoch_benchmark/run_ycsb.sh run MC         # YCSB, after `load`
```

The YCSB driver is bundled: each transaction carries ten operations, keys follow the same Zipfian distribution as the Calvin adapter, and `load` creates the ten-column table. Mixes and parameters are listed in [experiment protocol](docs/EXPERIMENTS.md).

The harness leaves a prep directory with `summary.txt` and a snapshot of the epoch settings in force. Each instance writes `epoch_state_<stamp>.log` into its data directory, one line per reporting interval with the physical, receive, merge, apply and logical cursors, queue depth and EOF counters. A run is only meaningful if the cursors advance together and `apply` keeps up with `merge`.

See [experiment protocol](docs/EXPERIMENTS.md) for profile settings, shaping, metric meanings and output files.

## Implementation scope

This is an adapted three-replica GeoGauss build on openGauss 6.0.0, not an unmodified upstream deployment. Membership is static and read from `epoch_peer_hosts` at startup. DBR moves a contiguous range of unpublished epochs by one offset, bounded per epoch and per target, and an epoch is still closed by the ordinary seal; the reorganization state lives in memory and is not recovered after a crash. PA pins one proxy for the run, suppresses delivery for whole epochs, and forwards the post-merge survivors together with claim-only records for the transactions the merge aborted; it does not re-run conflict resolution.

Proxy takeover and replica-failure recovery are not implemented, and tests are end-to-end only. See [implementation details](docs/IMPLEMENTATION.md).

## Provenance

The base is openGauss 6.0.0. Upstream copyright notices remain in `License`, `COPYRIGHT`, `Third_Party_Open_Source_Software_Notice`, and the source headers. `src/epoch/`, `src/occ/`, and `src/occ-opt/` are additions of this work.

The release excludes host configuration beyond `env.sh`, generated binaries, database snapshots, and experiment logs. No historical performance numbers are asserted by this code-only release.
