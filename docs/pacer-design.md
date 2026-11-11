# Pacer 设计说明（GeoGauss / openGauss 6.0.0）

本文描述 `src/epoch/e_pacer.*` 及其挂点在代码中的实际形态，与论文 §V（DBR）、§VI（PA）对应。

## 1. 插入点

```
用户 SQL
  │
  ▼
事务开始（XACT_EVENT_START）
  ├─ commit epoch = physical epoch
  ├─ read frontier = 当前 logical epoch
  └─ executing++
  │
  ▼
OCC 本地执行（写集留在 backend）
  │
  ▼
PRE_COMMIT：分配 CSN → 序列化 wire → 压入每个目的端（含 self）的发送环
  │
  ▼
每条 peer 一条发送线程（self 单独一条）
  ├─ PA：该片若被判定“跳过慢节点”，本节点不发（由 proxy 统一补发）
  ├─ DBR：出队点整笔决策 —— 立即发送 / 改期到 e_t
  └─ 收尾包永远发送
  │
  ▼
接收线程（按 recv epoch 顺序）
  ├─ 写集：received++，提交 merge
  └─ 收尾包：declared := received（冻结）
  │
  ▼
merge worker：按 (relid, pk) 先写胜出；输者进 abort 集合
  │
  ▼
apply worker：只写仍获胜且未 abort 的写集
```

两个机制都插在“写集尚未被对端看到 / 尚未进入 merge”的位置：这是唯一可以整笔改变
epoch 归属而不破坏确定性的位置。

## 2. DBR（Dynamic Batch Reorganization）

### 2.1 整笔决策

一笔写集在每个目的端的发送环里各有一份。决策表按 `(csn, server_id)` 记录一次结论，
所有 peer 线程读同一份：

```
首次弹出：按压力决定 SEND / DEFER，并记录 decided_epoch、target_epoch
再次弹出：wire.epoch == decided_epoch → 沿用 DEFER（改期）
          wire.epoch == target_epoch   → SEND（改期后的副本不再后移）
```

一笔写集最多后移一次，避免在持续拥塞下被反复搬运。

因此同一笔写集在所有目的端落到同一个 epoch。收尾包不参与决策，永远发送。

### 2.2 目标片（Eq. 3）

```
e_t = max{ e + 1,  ê_peer + ceil(RTT / T_epoch) + K }
```

- `ê_peer`：接收游标（对端 EOF 推进到的位置）
- `T_epoch`：physical 推进间隔的 EWMA（下限 1 ms）
- `K`：`epoch_dyn_net_catchup_k`（默认 3）
- 约束：`e_t ≥ physical + 2`（目标片必须尚未封口），且 `e_t ≤ e + 32`（环深 256）

### 2.3 触发条件

发送端压力成立且持续超过一个片长：

- 上行速率达到 `epoch_dyn_net_upload_bps × epoch_dbr_threshold_pct%`，或
- 待发积压条数 ≥ `epoch_dyn_net_queue_tolerance × 目的端数`

每片还有发送预算 `epoch_dbr_admit_per_epoch`：预算内直发，超出的整笔改期；
每个目标片另有改期配额 32 条，配额耗尽时该笔按原片直发。两者都只在“首次决策”时
生效，因此不会出现同一笔在部分目的端改期、部分直发。

### 2.4 源端关窗

改期只改 wire 上的 `epoch`，`csn`、`server_id` 与事务开始时的 read frontier 不变。
源端随后仍按原片发出收尾包；接收端在收到收尾包时把 `declared` 冻结成实收条数，
所以被改期的写集不会在原片被期待，merge 关窗也不需要额外的计数消息。

## 3. PA（Proxy Aggregation）

### 3.1 会话

慢节点在接收侧持续压力下广播一个会话：

| 字段 | 含义 |
|------|------|
| `csn` | `PA_STATE_ON` / `PA_STATE_OFF` |
| `epoch` | 首个未应用 epoch（e_f） |
| `server_id` | 慢节点 id |
| `route_wid` | 选定的 proxy id |
| `origin_start_us` | session id |

接收压力判据：下行速率达到上限阈值、接收游标落后 physical 超过
`epoch_dyn_net_lag_tolerance`、或当前片接收队列积压超过阈值的任一成立。

proxy 选择：排除慢节点自身，在连通节点中取“已确认进度”（`EpochRecvAckUpto`）最大者，
同进度取小 id。选定后在整个运行期固定，只有该节点断开才重选——换代理会让同一片出现
两个决策源，重复投递会被判输。慢节点启动时先广播一次（不带会话），让所有节点对
proxy 达成一致。

### 3.2 逐片路由决策

proxy 对每个发送片做一次决定（直发 / 跳过），并把决定广播给所有健康发送端；
非代理节点在遇到该片时等待这条决定，不设超时。于是同一片在所有发送端的路由结论一致，
慢节点的接收游标在收到代理整包之前不会越过该片。

代理尚未走到该片时，等待方会发一条“决策请求”（控制消息，携带片号，每 5ms 重发一次）。
收包线程只登记请求，决策与广播由 proxy 的发送线程完成：那里可以安全重试，
也不会把收包线程拖进一次网络往返。

会话结束时（`PA_STATE_OFF`）记录 `exit_epoch = physical + 1`：此前的片继续由代理补发，
此后的片恢复直发。

### 3.3 转发：候选与影子

proxy 正常执行本片 merge；merge 游标推进时把缓冲的写集发给慢节点：

- **候选**：未进入 abort 集合的事务，转发原始 wire（完整写集）。
- **影子**：已判输的事务，转发 `claim_only` 版本 —— 保留 `csn`、`server_id`、
  行键（relid / pk / ctid），清空行数据。

影子在慢节点上只参与冲突裁决：它挡住 CSN 更大的本地事务（与直接同步一致），
但自身永不进入 apply。这样即使某个远端事务整笔判输，它在未被覆盖的行上的
冲突声明仍然生效，慢节点不会因此改判本地事务。

转发完成后，代理以每个被抑制源（含自己）的身份补发该片 EOF，慢节点的账本据此
把各源 `declared` 冻结成实收条数。

## 4. 正确性要点

1. **唯一归属**：决策表按 `(csn, server_id)` 只记一次结论，所有目的端副本共用；
   改期后的副本不再后移。
2. **源端完整**：收尾包不带计数，接收端在收尾包到达时冻结 `declared`；
   改期写集只会在目标片被期待。
3. **汇聚等价**：`merge` 对同一行按 CSN 比较、保留更早者，插入顺序无关；
   影子携带与原始事务相同的声明，因此 `A(R) ⊔ L` 与 `R ∪ L` 得到相同的裁决与状态。

## 5. 开关

| GUC | 作用 |
|-----|------|
| `epoch_dyn_net_catchup` | 打开 DBR |
| `epoch_dyn_net_proxy` | 打开 PA |
| `epoch_dyn_net_slow_nodes` | 慢节点 id 列表 |
| `epoch_dyn_net_rtt_ms` | Eq. 3 中的 RTT |
| `epoch_dyn_net_catchup_k` | Eq. 3 中的 K |
| `epoch_dyn_net_upload_bps` / `_download_bps` | 上/下行带宽上限 |
| `epoch_dbr_threshold_pct` | 拥塞判定阈值（百分比） |
| `epoch_dbr_admit_per_epoch` | 每片直发条数上限 |
| `epoch_dyn_net_lag_tolerance` / `_queue_tolerance` | 接收侧压力判据 |
