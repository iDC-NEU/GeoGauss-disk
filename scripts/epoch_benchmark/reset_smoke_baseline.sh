#!/usr/bin/env bash
# 双节点 sync 下直接 UPDATE 会卡在 START gate；单节点依次重置 cnt 基线
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=env.sh
source "${SCRIPT_DIR}/env.sh"

REMOTE="${EPOCH_REMOTE_USER}@${EPOCH_REMOTE_HOST}"
RB="${EPOCH_REMOTE_BENCH_ROOT}"
RESET_SQL="UPDATE epoch_smoke SET cnt = 0, node_id = 0 WHERE id = 0; UPDATE epoch_smoke SET cnt = 0, node_id = 1 WHERE id = 1;"

reset_on_local() {
    timeout 30 gsql -h 127.0.0.1 -d "${EPOCH_TPCC_DB}" -p "${EPOCH_DB_PORT}" \
        -U "${EPOCH_TPCC_USER}" -W "${EPOCH_TPCC_PASSWORD}" -c "${RESET_SQL}"
}

echo "=== epoch_smoke 单节点依次重置基线 ==="
bash "${SCRIPT_DIR}/stop_node.sh" 1 || true
bash "${SCRIPT_DIR}/stop_node.sh" 0 || true
bash "${SCRIPT_DIR}/start_node.sh" 0
echo ">>> node0"
reset_on_local
bash "${SCRIPT_DIR}/stop_node.sh" 0

ssh -o BatchMode=yes -o ConnectTimeout=10 "${REMOTE}" bash -s <<EOF
set -euo pipefail
export EPOCH_BENCH_ROOT="${RB}"
export GAUSSHOME="\${EPOCH_BENCH_ROOT}/openGauss-server/mppdb_temp_install"
export PATH="\${GAUSSHOME}/bin:\${PATH}"
export LD_LIBRARY_PATH="\${GAUSSHOME}/lib:\${EPOCH_ZMQ_LIB:-}:\${LD_LIBRARY_PATH:-}"
source "\${EPOCH_BENCH_ROOT}/openGauss-server/scripts/epoch_benchmark/env.sh"
bash "\${EPOCH_BENCH_ROOT}/openGauss-server/scripts/epoch_benchmark/stop_node.sh" 0 || true
bash "\${EPOCH_BENCH_ROOT}/openGauss-server/scripts/epoch_benchmark/start_node.sh" 1
echo ">>> node1"
timeout 30 gsql -h 127.0.0.1 -d ${EPOCH_TPCC_DB} -p ${EPOCH_DB_PORT} \
    -U ${EPOCH_TPCC_USER} -W '${EPOCH_TPCC_PASSWORD}' -c "${RESET_SQL}"
bash "\${EPOCH_BENCH_ROOT}/openGauss-server/scripts/epoch_benchmark/stop_node.sh" 1
EOF

bash "${SCRIPT_DIR}/start_cluster.sh"
echo "OK  epoch_smoke 基线已重置（cnt=0）"
