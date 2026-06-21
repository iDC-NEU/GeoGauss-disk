#!/usr/bin/env bash
# 双节点并行启动，尽量缩短启动时间差（避免单节点独自狂推 logical）
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=env.sh
source "${SCRIPT_DIR}/env.sh"

REMOTE="${EPOCH_REMOTE_USER}@${EPOCH_REMOTE_HOST}"

echo "=== 并行启动 node0 + node1 ==="
rm -f "${EPOCH_DATA_ROOT}/epoch-node0/epoch_diag.log" "${EPOCH_DATA_ROOT}/epoch-node0/epoch_recv_diag.log"
ssh -o BatchMode=yes -o ConnectTimeout=10 "${REMOTE}" \
    "rm -f ${EPOCH_REMOTE_BENCH_ROOT}/data/epoch-node1/epoch_diag.log ${EPOCH_REMOTE_BENCH_ROOT}/data/epoch-node1/epoch_recv_diag.log" \
    || true
ssh -o BatchMode=yes -o ConnectTimeout=10 "${REMOTE}" \
    "bash ${EPOCH_REMOTE_BENCH_ROOT}/openGauss-server/scripts/epoch_benchmark/start_node.sh 1" &
job1=$!
bash "${SCRIPT_DIR}/start_node.sh" 0 &
job0=$!

rc0=0 rc1=0
wait "${job0}" || rc0=$?
wait "${job1}" || rc1=$?
if [ "${rc0}" -ne 0 ] || [ "${rc1}" -ne 0 ]; then
    echo "ERROR: start failed rc0=${rc0} rc1=${rc1}" >&2
    exit 1
fi
sleep 3
bash "${SCRIPT_DIR}/verify_cluster.sh"
