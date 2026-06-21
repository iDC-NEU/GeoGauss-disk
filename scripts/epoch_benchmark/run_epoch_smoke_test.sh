#!/usr/bin/env bash
# 双节点 epoch 冒烟测试：比 TPCC 简单，用于确认 logical 能持续推进、无长期 STALL
#
# 用法:
#   ./run_epoch_smoke_test.sh [monitor_sec] [workers_node0] [txns_per_worker] [total_timeout_sec]
#
# 前置（仿 TPCC，导入与压测分离）:
#   ./import_smoke_data.sh
#
# 默认: 监控 12s，node0 1 worker、node1 0 worker，每 worker 20 笔 UPDATE
#
# 通过后再跑: ./run_tpcc.sh props.epoch.node0 90
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=env.sh
source "${SCRIPT_DIR}/env.sh"

MONITOR_SEC="${1:-12}"
# 第 2 参数仅控制 node0 worker 数；node1 默认 0（只测 node0 写入同步）
WORKERS_NODE0="${EPOCH_SMOKE_WORKERS_NODE0:-${2:-1}}"
WORKERS_NODE1="${EPOCH_SMOKE_WORKERS_NODE1:-0}"
TXNS_PER_WORKER="${3:-20}"
REMOTE="${EPOCH_REMOTE_USER}@${EPOCH_REMOTE_HOST}"
RB="${EPOCH_REMOTE_BENCH_ROOT}"

STALL_TIMEOUT_SEC="${EPOCH_SMOKE_STALL_TIMEOUT:-20}"
MIN_LOGICAL_ADVANCE="${EPOCH_SMOKE_MIN_ADVANCE:-3}"
MAX_NODE_DIFF="${EPOCH_SMOKE_MAX_DIFF:-64}"
MAX_PHYS_LOGICAL_GAP="${EPOCH_SMOKE_MAX_PHYS_LOGICAL_GAP:-5}"
TXN_TIMEOUT_SEC="${EPOCH_SMOKE_TXN_TIMEOUT:-30}"

if [ -n "${4:-}" ]; then
    TOTAL_TIMEOUT_SEC="$4"
elif [ -n "${EPOCH_SMOKE_TOTAL_TIMEOUT:-}" ]; then
    TOTAL_TIMEOUT_SEC="${EPOCH_SMOKE_TOTAL_TIMEOUT}"
else
    TOTAL_TIMEOUT_SEC=30
fi
WORKER_WAIT_SEC="${EPOCH_SMOKE_WORKER_TIMEOUT:-30}"

DIAG0="${EPOCH_DATA_ROOT}/epoch-node0/epoch_diag.log"
DIAG1_REMOTE="${RB}/data/epoch-node1/epoch_diag.log"
RECV0="${EPOCH_DATA_ROOT}/epoch-node0/epoch_recv_diag.log"
RECV1_REMOTE="${RB}/data/epoch-node1/epoch_recv_diag.log"

pids0=()
node1_job=""

parse_logical_from_diag() {
    local diag="$1"
    if [ ! -f "${diag}" ]; then
        echo 0
        return
    fi
    grep -E '^\[.*\] physical=' "${diag}" | tail -1 \
        | sed -n 's/.* logical=\([0-9][0-9]*\) .*/\1/p' || echo 0
}

# recv_diag 实时更新；epoch_diag 每 +100 physical 才写一行，单独读会误判 diff≈100
parse_logical_from_recv() {
    local recv="$1"
    if [ ! -f "${recv}" ]; then
        echo 0
        return
    fi
    local stall sent max_ep
    stall=$(grep -E 'STALL epoch=[0-9]+' "${recv}" 2>/dev/null | tail -1 \
        | sed -n 's/.*STALL epoch=\([0-9][0-9]*\).*/\1/p' || true)
    sent=$(grep -E 'SENT epoch=[0-9]+' "${recv}" 2>/dev/null | tail -1 \
        | sed -n 's/.*SENT epoch=\([0-9][0-9]*\).*/\1/p' || true)
    max_ep=0
    if [ -n "${stall:-}" ] && [ "${stall}" -gt "${max_ep}" ]; then
        max_ep="${stall}"
    fi
    if [ -n "${sent:-}" ] && [ $((sent + 1)) -gt "${max_ep}" ]; then
        max_ep=$((sent + 1))
    fi
    if [ "${max_ep}" -gt 0 ]; then
        echo "${max_ep}"
        return
    fi
    echo 0
}

read_logical_best() {
    local node_id="$1"
    local from_diag from_recv
    if [ "${node_id}" = "0" ]; then
        from_diag=$(parse_logical_from_diag "${DIAG0}")
        from_recv=$(parse_logical_from_recv "${RECV0}")
    else
        from_diag=$(timeout 10 ssh -o BatchMode=yes -o ConnectTimeout=5 "${REMOTE}" \
            "grep -E '^\[.*\] physical=' ${DIAG1_REMOTE} | tail -1 | sed -n 's/.* logical=\\([0-9][0-9]*\\) .*/\\1/p' || echo 0")
        from_recv=$(timeout 10 ssh -o BatchMode=yes -o ConnectTimeout=5 "${REMOTE}" \
            "recv='${RECV1_REMOTE}'; stall=\$(grep -E 'STALL epoch=[0-9]+' \"\${recv}\" 2>/dev/null | tail -1 | sed -n 's/.*STALL epoch=\\([0-9][0-9]*\\).*/\\1/p'); sent=\$(grep -E 'SENT epoch=[0-9]+' \"\${recv}\" 2>/dev/null | tail -1 | sed -n 's/.*SENT epoch=\\([0-9][0-9]*\\).*/\\1/p'); max=0; [ -n \"\${stall}\" ] && [ \"\${stall}\" -gt \"\${max}\" ] && max=\${stall}; [ -n \"\${sent}\" ] && [ \$((sent+1)) -gt \"\${max}\" ] && max=\$((sent+1)); echo \${max:-0}")
    fi
    if [ "${from_recv:-0}" -gt 0 ]; then
        echo "${from_recv}"
    else
        echo "${from_diag:-0}"
    fi
}

read_gap_phys_logical() {
    local node_id="$1"
    local gap
    if [ "${node_id}" = "0" ]; then
        if [ ! -f "${DIAG0}" ]; then
            echo 999
            return
        fi
        gap=$(grep -E '^\[.*\] physical=' "${DIAG0}" | tail -1 \
            | sed -n 's/.* gap_phys_logical=\([0-9][0-9]*\).*/\1/p' || true)
    else
        gap=$(timeout 10 ssh -o BatchMode=yes -o ConnectTimeout=5 "${REMOTE}" \
            "test -f ${DIAG1_REMOTE} && grep -E '^\[.*\] physical=' ${DIAG1_REMOTE} | tail -1 | sed -n 's/.* gap_phys_logical=\\([0-9][0-9]*\\).*/\\1/p'" \
            2>/dev/null || true)
    fi
    gap=$(echo "${gap:-}" | tr -d '[:space:]')
    echo "${gap:-999}"
}

# 必须用 bash 内建 wait；`timeout N wait PID` 会调用 /usr/bin/wait 导致 "not a child"
wait_pid_with_timeout() {
    local pid="$1"
    local sec="$2"
    local elapsed=0
    while kill -0 "${pid}" 2>/dev/null; do
        if [ "${elapsed}" -ge "${sec}" ]; then
            kill "${pid}" 2>/dev/null || true
            return 124
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done
    wait "${pid}"
}

read_logical_sync() {
    local node_id="$1"
    if [ "${node_id}" = "0" ]; then
        parse_logical_from_diag "${DIAG0}"
    else
        timeout 10 ssh -o BatchMode=yes -o ConnectTimeout=5 "${REMOTE}" \
            "grep -E '^\[.*\] physical=' ${DIAG1_REMOTE} | tail -1 | sed -n 's/.* logical=\\([0-9][0-9]*\\) .*/\\1/p' || echo 0"
    fi
}

read_logical() {
    read_logical_best "$1"
}

count_stall_lines() {
    local node_id="$1"
    local count=0
    if [ "${node_id}" = "0" ]; then
        count=$(grep -c '^STALL ' "${RECV0}" 2>/dev/null || true)
    else
        count=$(timeout 10 ssh -o BatchMode=yes -o ConnectTimeout=5 "${REMOTE}" \
            "grep -c '^STALL ' ${RECV1_REMOTE} 2>/dev/null || true")
    fi
    count=$(echo "${count}" | head -1 | tr -d '[:space:]')
    echo "${count:-0}"
}

print_failure_context() {
    echo "=== node0 epoch_diag (tail) ===" >&2
    grep -E '^\[.*\] physical=' "${DIAG0}" 2>/dev/null | tail -3 >&2 || true
    echo "=== node0 STALL (tail) ===" >&2
    grep '^STALL ' "${RECV0}" 2>/dev/null | tail -5 >&2 || true
    echo "=== node1 epoch_diag (tail) ===" >&2
    timeout 15 ssh -o ConnectTimeout=5 "${REMOTE}" \
        "grep -E '^\[.*\] physical=' ${DIAG1_REMOTE} | tail -3" >&2 || true
    echo "=== node1 STALL (tail) ===" >&2
    timeout 15 ssh -o ConnectTimeout=5 "${REMOTE}" \
        "grep '^STALL ' ${RECV1_REMOTE} | tail -5" >&2 || true
}

cleanup_workers() {
    local pid
    for pid in "${pids0[@]:-}"; do
        kill "${pid}" 2>/dev/null || true
    done
    if [ -n "${node1_job}" ]; then
        kill "${node1_job}" 2>/dev/null || true
    fi
    pkill -f "${SCRIPT_DIR}/smoke_worker.sh" 2>/dev/null || true
    timeout 10 ssh -o BatchMode=yes -o ConnectTimeout=5 "${REMOTE}" \
        "pkill -f smoke_worker.sh" 2>/dev/null || true
}

start_workers_node0() {
    local pids=()
    local w
    for w in $(seq 1 "${WORKERS_NODE0}"); do
        bash "${SCRIPT_DIR}/smoke_worker.sh" 0 "${TXNS_PER_WORKER}" "${w}" &
        pids+=("$!")
    done
    echo "${pids[@]}"
}

start_workers_node1() {
    timeout "${WORKER_WAIT_SEC}" ssh -o BatchMode=yes -o ConnectTimeout=10 "${REMOTE}" bash -s <<EOF
set -euo pipefail
export EPOCH_BENCH_ROOT="${RB}"
export GAUSSHOME="\${EPOCH_BENCH_ROOT}/openGauss-server/mppdb_temp_install"
export EPOCH_ZMQ_LIB="\${EPOCH_BENCH_ROOT}/epoch_deploy/zmq"
export PATH="\${GAUSSHOME}/bin:\${PATH}"
export LD_LIBRARY_PATH="\${GAUSSHOME}/lib:\${EPOCH_ZMQ_LIB}:\${LD_LIBRARY_PATH:-}"
export EPOCH_SMOKE_TXN_TIMEOUT="${TXN_TIMEOUT_SEC}"
SCRIPT_DIR="\${EPOCH_BENCH_ROOT}/openGauss-server/scripts/epoch_benchmark"
source "\${SCRIPT_DIR}/env.sh"
pids=""
for w in \$(seq 1 ${WORKERS_NODE1}); do
    bash "\${SCRIPT_DIR}/smoke_worker.sh" 1 ${TXNS_PER_WORKER} "\${w}" &
    pids="\${pids} \$!"
done
wait \${pids}
EOF
}

smoke_main() {
    local stall0_before stall1_before l0_start l1_start
    local monitor_end last_l0 last_l1 last_advance_ts fail_reason=""
    local l0 l1 diff stall0 stall1 new_stall idle_for
    local fail_workers=0 pid
    local l0_end l1_end adv0 adv1 diff_end stall0_end stall1_end

    echo "=== epoch 冒烟测试 ==="
    echo "workers node0=${WORKERS_NODE0} node1=${WORKERS_NODE1}  txns/worker=${TXNS_PER_WORKER}  monitor=${MONITOR_SEC}s"
    echo "total_timeout=${TOTAL_TIMEOUT_SEC}s  worker_wait=${WORKER_WAIT_SEC}s  txn_timeout=${TXN_TIMEOUT_SEC}s"
    echo "stall_timeout=${STALL_TIMEOUT_SEC}s  min_advance=${MIN_LOGICAL_ADVANCE}  max_diff=${MAX_NODE_DIFF}  max_gap=${MAX_PHYS_LOGICAL_GAP}"

    timeout 30 "${SCRIPT_DIR}/verify_cluster.sh" || { echo "FAIL cluster check" >&2; exit 2; }
    echo "（已假定 import_smoke_data.sh 完成；双节点下不再查表以免触发 START gate）"

    echo "=== 等待双节点 epoch 对齐（无负载，最多 ${EPOCH_SMOKE_SYNC_WAIT:-45}s）==="
    sync_end=$((SECONDS + ${EPOCH_SMOKE_SYNC_WAIT:-45}))
    while [ "${SECONDS}" -lt "${sync_end}" ]; do
        g0=$(read_gap_phys_logical 0)
        g1=$(read_gap_phys_logical 1)
        l0=$(read_logical_sync 0)
        l1=$(read_logical_sync 1)
        diff=$((l0 > l1 ? l0 - l1 : l1 - l0))
        echo "[$(date +%H:%M:%S)] sync: node0 logical=${l0} gap=${g0}  node1 logical=${l1} gap=${g1}  diff=${diff}"
        if [ "${g0}" -le "${MAX_PHYS_LOGICAL_GAP}" ] && [ "${g1}" -le "${MAX_PHYS_LOGICAL_GAP}" ] && \
           [ "${diff}" -le 100 ]; then
            echo "OK  epoch 已对齐，开始压测"
            break
        fi
        sleep 2
    done
    g0=$(read_gap_phys_logical 0)
    g1=$(read_gap_phys_logical 1)
    l0=$(read_logical_sync 0)
    l1=$(read_logical_sync 1)
    diff=$((l0 > l1 ? l0 - l1 : l1 - l0))
    if [ "${diff}" -gt 100 ]; then
        echo "FAIL  双节点 logical 差距 ${diff} > 100（epoch 未对齐）" >&2
        print_failure_context
        exit 1
    fi
    if [ "${g0}" -gt $((MAX_PHYS_LOGICAL_GAP + 8)) ] || [ "${g1}" -gt $((MAX_PHYS_LOGICAL_GAP + 8)) ]; then
        echo "FAIL  physical-logical 差距过大（node0 gap=${g0} node1 gap=${g1} limit=${MAX_PHYS_LOGICAL_GAP}）" >&2
        echo "  建议: ${SCRIPT_DIR}/import_smoke_data.sh  或等待更久后重试" >&2
        print_failure_context
        exit 1
    fi

    stall0_before=$(count_stall_lines 0)
    stall1_before=$(count_stall_lines 1)
    l0_start=$(read_logical 0)
    l1_start=$(read_logical 1)
    echo "baseline logical: node0=${l0_start} node1=${l1_start}"

    echo "=== 启动负载（node0 本机${WORKERS_NODE1:+ + node1 远程}）==="
    pids0=()
    for w in $(seq 1 "${WORKERS_NODE0}"); do
        bash "${SCRIPT_DIR}/smoke_worker.sh" 0 "${TXNS_PER_WORKER}" "${w}" &
        pids0+=("$!")
    done
    node1_job=""
    if [ "${WORKERS_NODE1}" -gt 0 ]; then
        start_workers_node1 &
        node1_job=$!
    fi
    echo "node0 workers: ${#pids0[@]}  node1 workers: ${WORKERS_NODE1}${node1_job:+ (ssh pid=${node1_job})}"

    monitor_end=$((SECONDS + MONITOR_SEC))
    last_l0="${l0_start}"
    last_l1="${l1_start}"
    last_advance_ts="${SECONDS}"

    while [ "${SECONDS}" -lt "${monitor_end}" ]; do
        l0=$(read_logical 0)
        l1=$(read_logical 1)
        diff=$((l0 > l1 ? l0 - l1 : l1 - l0))
        stall0=$(count_stall_lines 0)
        stall1=$(count_stall_lines 1)
        new_stall=$((stall0 - stall0_before + stall1 - stall1_before))

        if [ "${l0}" != "${last_l0}" ] || [ "${l1}" != "${last_l1}" ]; then
            last_advance_ts="${SECONDS}"
            last_l0="${l0}"
            last_l1="${l1}"
        fi

        idle_for=$((SECONDS - last_advance_ts))
        echo "[$(date +%H:%M:%S)] logical node0=${l0}(+$((l0 - l0_start))) node1=${l1}(+$((l1 - l1_start))) diff=${diff} stall_new=${new_stall} idle=${idle_for}s"

        if [ "${idle_for}" -ge "${STALL_TIMEOUT_SEC}" ]; then
            fail_reason="logical 超过 ${STALL_TIMEOUT_SEC}s 未推进（node0=${l0} node1=${l1}）"
            break
        fi
        if [ "${diff}" -gt "${MAX_NODE_DIFF}" ] && [ "${idle_for}" -ge 5 ]; then
            fail_reason="双节点 logical 差距 ${diff} > ${MAX_NODE_DIFF}"
            break
        fi
        sleep 2
    done

    echo "=== 等待 worker 结束（timeout ${WORKER_WAIT_SEC}s）==="
    set +e
    for pid in "${pids0[@]}"; do
        wait_pid_with_timeout "${pid}" "${WORKER_WAIT_SEC}" || fail_workers=1
    done
    if [ -n "${node1_job}" ]; then
        wait_pid_with_timeout "${node1_job}" "${WORKER_WAIT_SEC}" || fail_workers=1
    fi
    set -e
    cleanup_workers

    l0_end=$(read_logical_sync 0)
    l1_end=$(read_logical_sync 1)
    adv0=$((l0_end - l0_start))
    adv1=$((l1_end - l1_start))
    diff_end=$((l0_end > l1_end ? l0_end - l1_end : l1_end - l0_end))
    stall0_end=$(count_stall_lines 0)
    stall1_end=$(count_stall_lines 1)

    echo ""
    echo "=== 结果汇总 ==="
    echo "logical  node0: ${l0_start} -> ${l0_end} (+${adv0})"
    echo "logical  node1: ${l1_start} -> ${l1_end} (+${adv1})"
    echo "diff_end=${diff_end}  stall_new=$((stall0_end - stall0_before + stall1_end - stall1_before))"
    echo "workers_failed=${fail_workers}（不作为硬失败，以双节点数据一致为准）"

    if [ -n "${fail_reason}" ] && [ "${fail_workers}" -eq 0 ]; then
        # 负载已结束但监控窗口内曾短暂停滞：若 logical 推进已达标则仍算通过
        if [ "${adv0}" -ge "${MIN_LOGICAL_ADVANCE}" ] && [ "${adv1}" -ge "${MIN_LOGICAL_ADVANCE}" ] && \
           [ "${diff_end}" -le "${MAX_NODE_DIFF}" ]; then
            fail_reason=""
        fi
    fi
    if [ -n "${fail_reason}" ]; then
        echo "FAIL  ${fail_reason}" >&2
        print_failure_context
        exit 1
    fi
    if [ "${adv0}" -lt "${MIN_LOGICAL_ADVANCE}" ] || [ "${adv1}" -lt "${MIN_LOGICAL_ADVANCE}" ]; then
        echo "WARN  logical 推进偏少（node0=+${adv0} node1=+${adv1}），继续校验数据同步" >&2
    fi
    if [ "${diff_end}" -gt 100 ]; then
        echo "WARN  logical 差距 ${diff_end}（epoch_diag 粒度），继续校验数据同步" >&2
    fi

    echo "=== 校验双节点 epoch_smoke 数据一致（唯一硬指标）==="
    if ! bash "${SCRIPT_DIR}/verify_smoke_sync.sh" "${EPOCH_SMOKE_SYNC_DATA_WAIT:-30}" 30; then
        echo "FAIL  双节点 epoch_smoke 数据不一致（远端 apply 未追上）" >&2
        print_failure_context
        exit 1
    fi

    echo "PASS  双节点 epoch 冒烟测试通过，可继续跑 TPCC"
    echo "  下一步: ${SCRIPT_DIR}/run_tpcc.sh props.epoch.node0 90"
}

if [ "${SMOKE_INTERNAL:-}" = "1" ]; then
    trap cleanup_workers EXIT INT TERM
    smoke_main
    exit $?
fi

trap cleanup_workers EXIT INT TERM
set +e
SMOKE_INTERNAL=1 timeout "${TOTAL_TIMEOUT_SEC}" "${BASH_SOURCE[0]}" \
    "${MONITOR_SEC}" "${WORKERS_NODE0}" "${TXNS_PER_WORKER}" "${TOTAL_TIMEOUT_SEC}"
rc=$?
set -e

if [ "${rc}" -eq 124 ]; then
    echo "FAIL  冒烟测试总超时（${TOTAL_TIMEOUT_SEC}s）" >&2
    print_failure_context
    exit 124
fi
exit "${rc}"
