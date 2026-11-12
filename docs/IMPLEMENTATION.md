# GeoGauss implementation notes

## Epoch assignment and sealing

A transaction is bound at start to the current physical epoch and records the logical epoch as its read frontier. It executes optimistically, and at pre-commit it receives a source-local CSN and is serialized into one wire record per destination, including a self slot.

An epoch is sealed when a node admits its 24th local write set: the sealer pushes a count-free EOF record into every destination queue and advances physical. The transport ring holds 256 epochs; admission blocks once physical comes within 100 epochs of the progress floor, so a ring slot is never reused while its epoch is still live.

The wire record carries `csn`, `epoch`, `server_id`, a commit/claim flag, and rows. Rows carry relid, a packed primary key, the read version CSN, and the serialized tuple.

## Send path and DBR

Each destination has its own bounded queue and its own send thread; the self slot has a dedicated thread so that local delivery never waits on a remote link. A write set is therefore present once per destination and is drained independently.

One decision per `(csn, server_id)` is recorded in a shared table the first time any thread pops a copy. Deferral rewrites only the record's `epoch`; `csn`, `server_id`, and the read frontier are untouched, and the record is pushed into the target epoch's queue for the same destination. A copy later popped from the target epoch is sent, so a write set moves at most once. If the target queue is full the sender waits rather than reverting to the original epoch, because a split assignment would leave replicas with different epoch membership.

The first deferral of an epoch `e` fixes a reorganization window: `delta = e_t - e`, where `e_t = max{e+1, e_hat + ceil(RTT / T_epoch) + K}` comes from the receive cursor `e_hat`, an EWMA of the physical advance interval, and `epoch_dyn_net_catchup_k`. Every write set from an epoch in `[e, e_t)` then moves to `epoch + delta`, so the spacing between epochs is preserved instead of collapsing several epochs into one target. The window is abandoned when its shifted epochs would fall at or below `physical+2` (already sealed) or more than 32 epochs ahead. A floor records the largest epoch assigned so far and keeps later decisions from moving backwards.

Admission is bounded twice: `epoch_dbr_admit_per_epoch` write sets per source epoch are sent directly and the rest may be deferred, and each target epoch accepts at most 32 deferred write sets. Both use reserve-or-send semantics, so the outcome never depends on queue population order.

## Receive, merge, apply

The receive thread drains one epoch at a time in order. A data record is first checked against an `(epoch, csn, server_id)` set, so a retransmitted or proxied copy cannot reach the merge and abort its own first copy; only the first copy increments that source's received count and is submitted to the merge pool. An EOF record freezes `declared` at the current received count. An epoch is merge-complete when every source's EOF has arrived and the merged count reaches the sum of declared counts.

Merge is a per-epoch map keyed by `(relid, primary key)`, falling back to the physical tuple id when no key is packed. Version order is `(csn, server_id)`, smaller first. A row written by an earlier version replaces the stored one, and the displaced transaction enters the epoch's abort set. A transaction is aborted if any of its rows meets an earlier version; the rows it does win are still recorded as claim-only entries, which take part in conflict resolution and never reach apply. This is the same claim representation PA uses for transactions aborted at the proxy.

Apply replays the surviving transactions of the epoch and skips any transaction present in the abort set.

## PA

Every replica listed in `epoch_dyn_net_slow_nodes` samples its own downlink rate, receive-cursor lag, and current-epoch backlog. When any of them exceeds its threshold that replica announces its state: session identifier and first unapplied epoch. The proxy is the lowest-id healthy and connected replica; every node derives it from the same rule, so no negotiation is needed and one proxy can serve several slow nodes. The receiver keeps one congestion bit and one first-epoch value per slow node: an epoch is assisted while any of them is congested, starting from the earliest declared epoch, and direct delivery resumes once the last one clears.

The proxy decides once per send epoch whether to suppress direct delivery to the slow set and broadcasts the decision to every peer, including the slow nodes themselves; a peer that reaches the epoch first asks for the decision and waits for it. The decision is frozen per epoch, so every sender routes that epoch identically, and a slow node's receive cursor cannot pass an assisted epoch before the proxy's bundle arrives.

During merge, the proxy buffers every wire record of that epoch. When the merge cursor advances, it forwards to each slow node all records except that node's own contribution: complete records for transactions that survived, and claim-only records for transactions the merge aborted. A claim-only record keeps the row keys, `csn`, and `server_id` but drops the tuple bytes; on the receiving side it takes part in conflict resolution and is never applied. The proxy then emits one EOF per suppressed source for that target, so the receiver's ledger closes the epoch.

Forwarding goes through an outbox drained by the send worker, so the merge thread never blocks on a slow link. The queue is ordered, retries on failure, and is capped; beyond the cap the oldest records are dropped and counted.

## Session record

The lagging replica writes a fixed-size record (magic, version, session, first epoch, exit epoch, proxy, congestion flag, checksum) to `$PGDATA/epoch_pa_session.state` with `fsync`, `rename`, and a directory `fsync` before announcing the session. On restart the record is validated and, if it says the session was active, the replica re-announces with a fresh session identifier.

## Transport

Each peer pair has a ZMQ PUSH/PULL channel with no send high-water mark, `ZMQ_IMMEDIATE`, and TCP keepalive; the broadcast PUB/SUB channel used by earlier versions was removed because a dropped EOF stalled epoch closure permanently. EOF records are acknowledged and resent from an eight-epoch ring when the acknowledgement stops advancing. PA control messages reuse the data wire format with reserved CSN values and are handled in the receive thread without entering the epoch queue.

## Settings

Cluster mode is off by default. These are restart-level (`PGC_POSTMASTER`), except `epoch_dbr_threshold_pct`, which is `PGC_SIGHUP`. `configure_node.sh` writes them from the profile selection.

| Setting | Meaning |
| --- | --- |
| `enable_epoch_multimaster` | Master switch; requires `enable_occ_disk=on` |
| `enable_occ_disk`, `enable_occ_staging` | OCC disk engine and version staging |
| `epoch_node_id`, `epoch_total_nodes`, `epoch_peer_hosts` | Static membership; `epoch_peer_hosts` is a comma-separated peer list |
| `epoch_data_port_base` | Base of the per-peer transport ports |
| `epoch_pack_threads`, `epoch_merge_threads`, `epoch_commit_threads` | Sender, merge, and apply worker counts |
| `epoch_dyn_net_catchup` | Enable DBR |
| `epoch_dbr_admit_per_epoch` | Write sets a congested sender still transmits per epoch; the rest are deferred |
| `epoch_dyn_net_rtt_ms`, `epoch_dyn_net_catchup_k` | RTT and headroom `K` in the target-epoch formula |
| `epoch_dyn_net_proxy` | Enable PA |
| `epoch_dyn_net_slow_nodes` | Comma-separated slow-node ids |
| `epoch_dyn_net_upload_bps`, `epoch_dyn_net_download_bps` | Bandwidth ceilings used by the congestion checks |
| `epoch_dbr_threshold_pct` | Fraction of a ceiling that counts as congestion |
| `epoch_dyn_net_lag_tolerance`, `epoch_dyn_net_queue_tolerance` | Receiver-pressure thresholds |

## Scope

The implementation does not include proxy takeover while a sender waits, replay of forwarded bundles after a disconnect, WAL logging of the session record, online membership changes, or replica-failure recovery. There are no unit tests; verification is end-to-end through the harness.
