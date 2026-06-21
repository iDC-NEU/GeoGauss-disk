#!/usr/bin/env bash
# 在双节点上创建 epoch_smoke（单节点依次执行，避免双节点 sync 下 INSERT 卡住）
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=env.sh
source "${SCRIPT_DIR}/env.sh"

REMOTE="${EPOCH_REMOTE_USER}@${EPOCH_REMOTE_HOST}"
RB="${EPOCH_REMOTE_BENCH_ROOT}"
SCHEMA_SQL="${SCRIPT_DIR}/sql/epoch_smoke_schema.sql"
DATA_SQL="${SCRIPT_DIR}/sql/epoch_smoke_data.sql"

import_on_local() {
    timeout 30 gsql -h 127.0.0.1 -d "${EPOCH_TPCC_DB}" -p "${EPOCH_DB_PORT}" \
        -U "${EPOCH_TPCC_USER}" -W "${EPOCH_TPCC_PASSWORD}" -f "${SCHEMA_SQL}"
    timeout 30 gsql -h 127.0.0.1 -d "${EPOCH_TPCC_DB}" -p "${EPOCH_DB_PORT}" \
        -U "${EPOCH_TPCC_USER}" -W "${EPOCH_TPCC_PASSWORD}" -f "${DATA_SQL}"
}

echo "=== epoch_smoke 单节点依次导入 ==="
bash "${SCRIPT_DIR}/stop_node.sh" 1 || true
bash "${SCRIPT_DIR}/stop_node.sh" 0 || true
bash "${SCRIPT_DIR}/start_node.sh" 0
echo ">>> node0"
import_on_local
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
    -U ${EPOCH_TPCC_USER} -W '${EPOCH_TPCC_PASSWORD}' -f ${RB}/openGauss-server/scripts/epoch_benchmark/sql/epoch_smoke_schema.sql
timeout 30 gsql -h 127.0.0.1 -d ${EPOCH_TPCC_DB} -p ${EPOCH_DB_PORT} \
    -U ${EPOCH_TPCC_USER} -W '${EPOCH_TPCC_PASSWORD}' -f ${RB}/openGauss-server/scripts/epoch_benchmark/sql/epoch_smoke_data.sql
bash "\${EPOCH_BENCH_ROOT}/openGauss-server/scripts/epoch_benchmark/stop_node.sh" 1
EOF

bash "${SCRIPT_DIR}/start_cluster.sh"
echo "OK  双节点 epoch_smoke 已就绪"
