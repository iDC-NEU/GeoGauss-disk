# 设计草案：GeoGauss 式「自拉流水线」落在现有 epoch shmem

**状态**：2026-07-15 — **二维私有队列 + 共享 `merge_epoch` 游标**（对齐 `apply_epoch`）  
**队列**：Merge/Apply shmem 均为 `[epoch%slots][route_worker]`；无全局锁  
**merge 游标**：Claim / 关窗 / Feed 只针对 `merge_epoch`；complete 且 `merge_epoch+1 < physical` 时 `++`（**不**按 logical 做下限：logical 推进已蕴含 merge/apply 完成）  
**recv**：`g_remote_merge_queues[route_wid % n_merge]`；Drain 只入队 `[merge_epoch, physical)`  
**硬约束**：`apply(E)` 须 `merge_complete(E)`；`merge_epoch < physical`

---

## 0. 一句话

不是取消协调，而是把协调拆成：

| 以前混在一起 | 拆开之后 |
|---|---|
| coordinator 派 merge / 派 apply / 打勾完成 /（兼）推进 | **工人自拉干活** + **屏障线程只打勾 / 推进 logical** |

`merge_complete(E) → apply(E)` 的箭头保留，只是「谁把活放进 apply 队列、谁去领」变了。

---

## 1. 现状 vs 目标

### 1.1 旧状（派活型 + 全局环锁）——已废弃

```text
recv → queues[epoch%N]
Submit → 全局 MergeQueue（merge_lock）→ 任意 worker TryClaim（可 steal）
Apply 同构（apply_lock + ContainsWire）
```

### 1.2 当前（GeoGauss 二维私有队列）

```text
pack / recv
  └─ route = route_wid % N_merge
  └─ enqueue → g_remote_merge_queues[route]

merge worker w
  ├─ dequeue 仅 queues[w]（无 steal）
  ├─ Submit → MergeCell[epoch%slots][w]   // 仅本列 CAS，无全局锁
  ├─ TryClaim 仅扫 [*].[w]
  ├─ Finish → inflight-- + 同进程 InvokeDoneHook
  └─ PullTick：CAS merge_complete；Feed 仅 RouteWorker==w 的 Remembered wires

apply：同构 ApplyCell[epoch%slots][route_wid % N_apply]
  └─ TryClaim 仅 apply_epoch 行上本 worker 列；执行前须 merge_complete
```

容量：`depth = max(8, capacity/(slots*N))`，总 task ≈ `slots*N*depth`。

阶段依赖（不变）：

```text
seal(E) → … → merge_complete(E) → apply 可领 E → apply_complete(E) → logical 可过 E
```

---

## 2. shmem / 结构

| 组件 | 现状（改造后） |
|---|---|
| `g_remote_merge_queues` | 按 **route_wid % n_merge**；merge 工人自拉本下标 |
| `EpochMergeQueue` | `tasks[slot][worker][depth]`；Submit/Claim 按列；无全局锁；无 done 环 |
| `EpochApplyQueue` | 同构；无 `ContainsWire`；Finish 内联 hook |
| `EpochLogicalStageFlags` | 仍作屏障；CAS 关窗在 PullTick |
| LogicalAdvance | 仍推进 logical |

**必须全量重启**（shmem 布局变）；链接 `gaussdb` 后重启节点再压测。

---

## 3. 关键设计点（含你强调的门闩）

### 3.1 apply(E) 必须等 merge_complete(E)

强制实施（建议多层，防漏）：

1. **投喂层**：只有 `merge_complete(E)==true` 后，才允许 `ApplyQueueSubmit(..., epoch=E)`。  
2. **epoch 指针层**：`apply_epoch` 的 Ensure/Advance 满足 `apply_epoch` 对应 epoch 已 `merge_complete`（其实今日也有 merge_complete 才 TryApply；自拉后要把这检查沉到 **claim/feed**，不能只靠 coordinator）。  
3. **claim 层（兜底）**：`TryClaim` 若 `!merge_complete(task.epoch)` 则不领（或根本不该出现在 READY）。

流水线重叠仍允许：

```text
apply(E)  ‖  merge(E+1)  ‖  recv(E+2)   ← OK
apply(E)  早于  merge(E)                 ← 禁止
```

这与 GeoGauss「Commit 等 `IsRemoteExeced`（merge 屏障）」同类；你们用 per-epoch `merge_complete` 即可。

### 3.2 谁置 `merge_complete(E)`？

候选：

| 方案 | 做法 | 评价 |
|---|---|---|
| **M1（推荐）** | merge 工人 Finish 时 `IncMerged`；任一工人发现 `merged>=expected && peersReady` 时 CAS 置 `merge_complete` | 无站长；需 expected 准确（epoch-end） |
| M2 | Logical/屏障线程轮询条件后置位 | 简单，但 barrier 又兼「完成检测」，延迟多一轮 poll |
| M3 | 保留微型 feeder 线程只做「判完成 + snapshot + feed apply」 | 过渡期好切，最终可并进 M1 |

推荐 **M1 + 屏障线程只认 flag**（不替 merge 判条件，避免双写逻辑）。

### 3.3 谁把赢家送进 apply 环？

| 方案 | 做法 | 评价 |
|---|---|---|
| **F1** | 置 `merge_complete` 的那个 merge worker 顺带 snapshot + 批量 `ApplyQueueSubmit` | 少线程；merge 路径变长 |
| **F2（推荐过渡）** | 轻量 `ApplyFeeder` 线程：等待 `merge_complete(E)` → snapshot → submit apply | 与 merge/apply 工人解耦；易回滚 |
| F3 | apply 工人自己扫 wire 表构建任务 | 与现成「预构建 ready」差太远，不优先 |

过渡期建议 **F2**；稳定后再考虑 F1 省掉 feeder。

### 3.4 Logical 屏障线程职责（扩展现有线程）

只做：

```text
loop:
  ep = logical
  if merge_complete(ep) && apply_complete(ep) && 无残余 inflight:
      logical++
      允许 ApplyEpoch 推进（若未由 apply 侧 CAS）
  sleep/latch
```

业务上对齐 GeoGauss 的 LogicalTimer：**计数/旗门齐 → 推进**，不碰 recv 出队。

`apply_complete` 置位：建议 apply 工人 Finish 时 `IncApplied`，CAS 置 flag；屏障线程只读。

### 3.5 recv 分片，避免 merge 工人空转互抢

沿用今日 `g_remote_merge_queues` 按 `epoch % N` 分桶的思路，改为：

- `N = merge_threads`（或 slot_count）  
- worker `i` 优先 drain `queues[i]`，空再 steal 邻居（可选）

这样「merge 自拉」有明确归属，而不是所有人抢同一个全局锁队列。

---

## 4. 端到端时序（目标态）

```text
[Backend] pack 本地 txn → ZMQ 发出；seal → epoch-end
[Peer recv] EpochRemoteMergeEnqueue(txn) / 标记 epoch-end 计数

[Merge worker i]
  dequeue queues[i]
  merge / CRDT / abort_csn
  Finish → IncMerged(E)
  if ready: CAS merge_complete(E)=true

[ApplyFeeder]（或 merge 顺带）
  见 merge_complete(E)
  SnapshotExpectedApplyWinners(E)
  for winner: ApplyQueueSubmit(E, ...)   // 唯此处喂 apply(E)

[Apply worker j]
  TryClaim(apply_epoch)  // apply_epoch 仅在 merge_complete 后才指向 E
  heap apply
  Finish → IncApplied(E)
  if ready: CAS apply_complete(E)=true

[Logical barrier]
  apply_complete(E) && … → logical++
```

与「有 coordinator」的差异仅在虚线框：

```text
旧: recv → [coordinator drain+submit] → merge环 → [coord PopDone+submit] → apply环
新: recv → [merge自拉] → (merge_complete) → [feeder/submit] → [apply自拉]
```

---

## 5. 分阶段落地（降低炸裂风险）

### Phase 0 — 验证假设（可与架构并行，小补丁）

在 **仍保留 coordinator** 的前提下：`PollSendSeal` apply 排水环交错 `PollMerge`。  
验收：`seal→merge` 从 ~65ms 压回 ~30ms，logical≥32。  
**目的**：证明「时间片」判断正确，再上大改。

### Phase 1 — Merge 自拉 recv（coordinator 不再 DrainRemote）

- merge worker 循环：`try_dequeue` 本分片 recv 队 → `MergeQueueSubmit` 或就地 merge  
- coordinator 删除 `DrainRemoteMergeQueue`  
- 仍由某人（可暂时仍是 coord）置 `merge_complete` / feed apply  

验收：双节点 TPCC 正确；merge stall 中 `m_peer_txn`/`m_async` 不恶化；加 `merge_threads` 首次出现可见收益。

### Phase 2 — Apply 投喂脱离 coordinator + claim 门闩加固

- `ApplyFeeder`：`merge_complete` → snapshot → submit  
- apply claim / feed **硬约束** merge_complete（§3.1）  
- coordinator 不再 `TryApplyOneEpoch` 派单  

验收：apply 关窗不回退；**故意注入**「未 merge_complete 却 submit」应失败/断言。

### Phase 3 — Coordinator 空心化

- 主线程只留 seal / 本地发布 / 诊断  
- `merge_complete`/`apply_complete` 全改工人 CAS  
- Logical 屏障线程成为唯一 `logical++` 路径（今日已有线程，须关掉别处重复推进）

验收：砍掉 PollSendSeal 内 merge/apply 调度后，吞吐 ≥ Phase 2；`commit_threads`/`merge_threads` 5→10 出现接近线性的阶段耗时改善（有天花板但仍可见）。

### Phase 3b — Pack×N + Send×1（已落地主干）

对齐 GeoGauss：拆开「出队/seal」与「打网卡」。

```text
Backend → TxnQueue
            ↓ Pack×N（postmaster EPOCHPACKWORKER，GUC epoch_pack_threads=5）
       ├─ RemoteMergeEnqueue（本节点 merge 自拉）
       └─ send 队列（进程内 moodycamel）
            ↓ Send×1（EPOCHSENDWORKER）
              ZMQ PUB（原 PublishWire，现 EpochPipelineSendWire）
Pack#0 兼 TrySeal → epoch-end 入 send 队列；MarkSelfEpochEndSent 在 Send 成功后
```

- coordinator：`PackSendActive` 时不再 drain/seal/Publish  
- `configure_node.sh` / GUC 默认 `epoch_pack_threads=5`  

### Phase 4 —（可选）并掉 ApplyFeeder

merge 置 complete 时顺带 feed apply，少一跳延迟。（已在 P1–P2 顺带 feed 路径落地）

---

## 6. 风险与非目标

| 风险 | 说明 |
|---|---|
| expected 计数不准 | merge_complete CAS 永不等 → 卡 logical；epoch-end/`should_receive` 必须与今日一致且测满 |
| heap apply / SysCache | 工人已是 postmaster util 线程，保持「只在 apply worker 内碰堆表」；勿把 merge 扔进裸 `std::thread` |
| 有序性 / 亲和 | `route_wid` 亲和可保留；自拉后 steal 要避免同键乱序（若有） |
| done 环背压 | Finish 仍可能堵；容量与今日相同，需压测 |
| 与「加 commit_threads」的关系 | Phase 3 之后再谈加线程；Phase 0–2 前加线程预期仍不明显 |

**非目标（本草案不做）：**

- 取消 `merge_complete(E)` 对 `apply(E)` 的等待  
- 跨节点再引入中心 master  
- 一步改成纯 MOT 内存路径  

---

## 7. 成功判据（架构级）

1. 代码路径上：**无**「单线程 DrainRemote + SubmitApply」热循环。  
2. 语义：任意实现下 **`apply` 任务不可在 `merge_complete(E)` 前被 claim**。  
3. 性能：同 load 下 logical ≥ 历史健康水位（~32–35/s @ pack25/33t）；随后 `merge_threads`/`commit_threads` A/B 出现可测增益。  
4. 正确性：`verify_epoch_end_consistency` 通过。

---

## 8. 已落地代码映射（跳过 P0）

| 设计点 | 实现 |
|---|---|
| Merge 自拉 recv | `EpochPipelineMergeWorkerPullTick` + `EpochPipelineMergeWorkerDrainRecvShard`；`e_merge_worker_main` 每轮调用 |
| Coord 不再 DrainRemote（并行） | `EpochPipelineDrainRemoteMergeQueue` 并行模式下只 `PollCompletions` |
| merge_complete + 顺带喂 apply | `EpochPipelineTryMarkMergeCompleteAndFeed`（CAS → Snapshot → `TryApplyMergedEpoch`） |
| Apply claim 门闩 | `e_apply_worker_main`：`!EpochPipelineEpochMergeComplete` 则退回 READY |
| Coord 收缩 | `PollSendSeal` 去掉 16× 独占 apply 排水环；PollApply 以完成/续喂兜底为主 |
| Pack×5 + Send×1 | `e_pack_worker_main` / `e_send_worker_main` + `e_send_queue`；`EpochPipelinePackWorkerTick` / `SendWorkerTick`；postmaster 拉起 |
| Coord 不再 drain/seal | `EpochPipelinePackSendActive` 时 `AdvanceMainWorkerStages` 仅 DynNet GC |

**验证**：`pack=25`、33t、`wait`；看 seal→merge / merge→apply / logical；日志应有 `epoch pack worker started` ×5、`epoch send worker started`、`epoch merge worker started`。  
**用户全量链接**：`cd src/gausskernel && make -sj4 gaussdb` 后 `prep` + TPCC。
