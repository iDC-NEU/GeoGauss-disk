用户 SQL
  │
  ▼
事务开始（XACT_EVENT_START）
  │
  ├─ 分配 commit epoch = physical epoch
  │     dbr 且慢节点上行已 latch 时 = max(physical, 分配下界)
  ├─ 预留本物理周期事务槽（每周期 50 个）
  ├─ 把此时的 logical epoch 记成 read_frontier
  │     本事务只读 m_status ≤ 这个值的版本
  └─ executing++
        防止 worker 把还在执行的 epoch 当成空窗 seal
  │
  ▼
OCC 本地执行（写集留在 backend，不写 heap）
  │
  ├─ 读
  │     └─ 先查 EpochTipStaging（键 = relid + TID(pageid + offset)）
  │           ├─ 命中且未删除 → 用这份最新版本
  │           ├─ 命中且已删除 → 这行不存在
  │           └─ 未命中 → 回退 heap / 版本链，仍受 read_frontier 限制
  │
  ├─ 写（INS / WR / DEL）
  │     ├─ 记入本事务写集（sentinel = 表 + TID）
  │     ├─ 只留本地 draft，不锁 heap、不读 buffer
  │     └─ pack 前不做 ValidateOcc
  │
  └─ 只读事务
        └─ COMMIT 直接放行；对端已连通时等到本 epoch 的 logical 关窗
  │
  ▼
PRE_COMMIT（有写集、对端已连通）
  │
  ├─ 整理写集
  ├─ 分配 CSN（本节点单调递增，源节点 id 不变）
  ├─ 序列化后放入 EpochTxnQueue
  ├─ 该 epoch 的 change_set +1、local_pack +1
  ├─ 释放 executing 和 (todo OCC 锁)
  └─ backend 不再落 heap，交给后面的 apply
  │
  ▼
Pack 线程从 EpochTxnQueue 取出
  │
  ├─ dbr：仅慢节点，且上行持续堵住
  │     │
  │     ├─ 看 ZMQ 发布套接字里还没发出去的字节
  │     │     ├─ ≥ 16KB 记高，≤ 8KB 记低
  │     │     ├─ 连续高 5ms → latch
  │     │     └─ 新事务停约 5s 且未发送字节已回落 → 放开
  │     │
  │     ├─ latch 期间写集留在 g_pacer_pending，不进 merge、不发送
  │     │
  │     ├─ 关窗前整批改到后面的 epoch
  │     │     目标 = max(原 epoch, logical, physical, 对端最大 sealed)
  │     │            + ceil(RTT / T_epoch) + K
  │     │     T_epoch = max(10ms, RTT)，K 默认 3
  │     │     原窗计数减掉，加到目标窗；改 epoch，不改 CSN 和 server_id
  │     │     单次后移不超过 base+64
  │     │     同一写集移动超过 4 次后计数回到 1
  │     │
  │     └─ 目的 epoch 还没轮到 merge → 先放 g_pacer_scheduled
  │           轮到且已经不堵 → 进入原来的 merge 和发送
  │
  └─ wait，或 dbr 下未堵住 / 健康节点
        ├─ 进入本机 merge 队列（到一条就先 merge）
        └─ 进入发送队列
  │
  ▼
发送
  │
  ├─ wait，或副本少于 3 个
  │     └─ ZMQ PUB 广播
  │
  └─ pa，且至少两个健康副本（需要第三个节点）
        │
        ├─ 慢节点选定 sealed epoch 最大的健康节点做 proxy，并广播激活消息
        │     自己的写集仍按原来的广播发出
        ├─ 其他健康节点不再把写集直接发给慢节点，改为单播给其余健康副本
        └─ proxy 收齐后按先写胜出，只把赢家推给慢节点
              整笔输掉的不转发，被替换掉的行不转发
              proxy 自己的库仍用收到的全部写集做 merge
  │
  ▼
收到写集
  │
  ├─ pa 的激活消息：只记下谁是 proxy，不进 merge
  └─ 写集和 EOF：进入本机 merge 队列
  │
  ▼
关窗
  │
  ├─ 本机这个 epoch 不再接收新事务（executing 已归零）
  ├─ local_pack 与 change_set 对齐
  │     dbr 改记还没完成时，先不发 EOF
  └─ 发出 EOF
        告诉其他副本：这一窗的输入到此为止
        dbr 让出的写集不在这个 EOF 里，它们算后面那个 epoch 的
  │
  ▼
收齐各副本的写集和 EOF
  │
  ▼
Merge worker
  │
  └─ 按 (本机 relid, 主键) 做先写胜出
        ├─ 跳过 READ 行；抽不出主键则整笔拒绝
        ├─ 先整笔判断，任一行已有更优赢家则整笔不写入
        ├─ CSN 更小者胜；CSN 相同则 server_id 更小者胜
        ├─ 同主键的 INSERT 只留一个赢家
        └─ 被替换的事务记入 abort_csn
  │
  ▼
merge_complete
  │
  ▼
Apply（EpochTipStaging 开着）
  │
  ├─ 只取 CRDT 里仍然获胜的行
  ├─ 已在 abort_csn 里的事务不落盘
  │
  ├─ INSERT：写入 heap，并记下主键 → 本机 TID
  │
  └─ UPDATE / DELETE：不在热路径改 heap 页
        ├─ 本地用 wire 上的 root TID；staging 命中就不再追链
        ├─ 远端不信对端 TID，先查主键对应的本机 tip，没有再走索引
        └─ 写入 EpochTipStaging，再由 flush 按脏页刷回 heap
  │
  ▼
apply 完成之后，logical epoch 前进
        physical 最多领先 logical 5 个 epoch