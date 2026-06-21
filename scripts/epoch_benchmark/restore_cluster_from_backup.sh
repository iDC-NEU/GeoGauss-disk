#!/usr/bin/env bash
# 从 epoch-node{0,1}_bak 恢复双节点数据目录并启动（比 import_smoke_data 更快，适合 TPCC 基线）
#
# 用法: ./restore_cluster_from_backup.sh
#
# 前提:
#   node0: ${EPOCH_DATA_ROOT}/epoch-node0_bak
#   node1: ${EPOCH_REMOTE_BENCH_ROOT}/data/epoch-node1_bak
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=env.sh
source "${SCRIPT_DIR}/env.sh"

REMOTE="${EPOCH_REMOTE_USER}@${EPOCH_REMOTE_HOST}"
RB="${EPOCH_REMOTE_BENCH_ROOT}"
NODE0_BAK="${EPOCH_DATA_ROOT}/epoch-node0_bak"
NODE0_DIR="${EPOCH_DATA_ROOT}/epoch-node0"
NODE1_BAK_REMOTE="${RB}/data/epoch-node1_bak"
NODE1_DIR_REMOTE="${RB}/data/epoch-node1"

if [ ! -d "${NODE0_BAK}" ]; then
    echo "FAIL  缺少 node0 备份: ${NODE0_BAK}" >&2
    exit 1
fi

echo "=== 从备份恢复 epoch 双节点 ==="
echo "node0: ${NODE0_BAK} -> ${NODE0_DIR}"
echo "node1: ${NODE1_BAK_REMOTE} -> ${NODE1_DIR_REMOTE}"

bash "${SCRIPT_DIR}/stop_node.sh" 0
bash "${SCRIPT_DIR}/stop_node.sh" 1

if ! ssh -o BatchMode=yes -o ConnectTimeout=10 "${REMOTE}" "[ -d '${NODE1_BAK_REMOTE}' ]"; then
    echo "FAIL  缺少 node1 备份: ${NODE1_BAK_REMOTE}" >&2
    exit 1
fi

echo ">>> 恢复 node0 数据目录"
rm -rf "${NODE0_DIR}"
cp -a "${NODE0_BAK}" "${NODE0_DIR}"

echo ">>> 恢复 node1 数据目录"
ssh -o BatchMode=yes -o ConnectTimeout=10 "${REMOTE}" bash -s <<EOF
set -euo pipefail
rm -rf "${NODE1_DIR_REMOTE}"
cp -a "${NODE1_BAK_REMOTE}" "${NODE1_DIR_REMOTE}"
EOF

bash "${SCRIPT_DIR}/start_cluster.sh"
sleep 3
if ! bash "${SCRIPT_DIR}/verify_cluster.sh"; then
    echo "WARN  连通性检查未通过，尝试重启 node1 ..." >&2
    bash "${SCRIPT_DIR}/stop_node.sh" 1 || true
    ssh -o BatchMode=yes -o ConnectTimeout=10 "${REMOTE}" \
        "bash ${RB}/openGauss-server/scripts/epoch_benchmark/start_node.sh 1"
    sleep 3
    bash "${SCRIPT_DIR}/verify_cluster.sh"
fi
echo ""
echo "PASS  已从备份恢复并启动双节点"
echo "  冒烟: ${SCRIPT_DIR}/run_epoch_smoke_test.sh"
echo "  TPCC: ${SCRIPT_DIR}/run_tpcc.sh props.epoch.node0 90"
