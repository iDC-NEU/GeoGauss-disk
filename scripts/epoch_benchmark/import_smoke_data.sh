#!/usr/bin/env bash
# 仿 TPCC runDatabaseBuild：各节点单独起库、本地导入（避免双节点 epoch 同步阻塞 INSERT）
#
# 用法:
#   ./import_smoke_data.sh           # 默认：先 node0 单节点导入，再 node1 单节点导入，最后并行启动双节点
#   ./import_smoke_data.sh 0         # 仅 node0 单节点导入（会先停 node1）
#   ./import_smoke_data.sh 1         # 仅 node1 单节点导入（会先停 node0）
#   ./import_smoke_data.sh local     # 本机导入（不停/启库，供 ssh 远程调用）
#
# 通过后再跑: ./run_epoch_smoke_test.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=env.sh
source "${SCRIPT_DIR}/env.sh"

TARGET="${1:-both}"
REMOTE="${EPOCH_REMOTE_USER}@${EPOCH_REMOTE_HOST}"
RB="${EPOCH_REMOTE_BENCH_ROOT}"
SCHEMA_SQL="${SCRIPT_DIR}/sql/epoch_smoke_schema.sql"
DATA_SQL="${SCRIPT_DIR}/sql/epoch_smoke_data.sql"
DDL_TIMEOUT="${EPOCH_SMOKE_DDL_TIMEOUT:-10}"
DML_TIMEOUT="${EPOCH_SMOKE_DML_TIMEOUT:-10}"

remote_ssh() {
    ssh -o BatchMode=yes -o ConnectTimeout=10 "${REMOTE}" "$@"
}

stop_node1_remote() {
    echo ">>> 停止 node1"
    remote_ssh "bash ${RB}/openGauss-server/scripts/epoch_benchmark/stop_node.sh 1" || true
}

stop_node0_local() {
    echo ">>> 停止 node0"
    bash "${SCRIPT_DIR}/stop_node.sh" 0 || true
}

start_node0_local() {
    echo ">>> 启动 node0（单节点）"
    bash "${SCRIPT_DIR}/start_node.sh" 0
}

start_node1_remote() {
    echo ">>> 启动 node1（单节点）"
    remote_ssh "bash ${RB}/openGauss-server/scripts/epoch_benchmark/start_node.sh 1"
}

import_on_local_node() {
    local node_id="$1"
    local label="node${node_id}"

    smoke_gsql() {
        local t="${DDL_TIMEOUT}"
        if [ "${1:-}" = "--timeout" ]; then
            t="$2"
            shift 2
        fi
        timeout "${t}" gsql -h 127.0.0.1 -d "${EPOCH_TPCC_DB}" -p "${EPOCH_DB_PORT}" \
            -U "${EPOCH_TPCC_USER}" -W "${EPOCH_TPCC_PASSWORD}" "$@"
    }

    echo "=== ${label}: 建表 (timeout ${DDL_TIMEOUT}s) ==="
    smoke_gsql --timeout "${DDL_TIMEOUT}" -f "${SCHEMA_SQL}"

    echo "=== ${label}: 导入数据 (timeout ${DML_TIMEOUT}s) ==="
    smoke_gsql --timeout "${DML_TIMEOUT}" -f "${DATA_SQL}"

    bash "${SCRIPT_DIR}/verify_smoke_data.sh" "${node_id}" 10
}

import_node0_alone() {
    echo ""
    echo "========== node0 单节点导入 =========="
    stop_node1_remote
    stop_node0_local
    start_node0_local
    import_on_local_node 0
    stop_node0_local
    echo "OK  node0 导入完成"
}

import_node1_alone() {
    echo ""
    echo "========== node1 单节点导入 =========="
    stop_node0_local
    stop_node1_remote
    start_node1_remote
    remote_ssh "bash ${RB}/openGauss-server/scripts/epoch_benchmark/import_smoke_data.sh local"
    remote_ssh "bash ${RB}/openGauss-server/scripts/epoch_benchmark/stop_node.sh 1" || true
    echo "OK  node1 导入完成"
}

case "${TARGET}" in
    local)
        import_on_local_node 1
        ;;
    0)
        import_node0_alone
        ;;
    1)
        import_node1_alone
        ;;
    both)
        echo "=== epoch 冒烟数据导入（单节点依次导入，DDL=${DDL_TIMEOUT}s DML=${DML_TIMEOUT}s）==="
        import_node0_alone
        import_node1_alone
        echo ""
        echo ">>> 导入完成，并行启动双节点供压测"
        bash "${SCRIPT_DIR}/start_cluster.sh"
echo "等待 ZMQ 握手..."
sleep 5
        echo ""
        echo "PASS  双节点 epoch_smoke 数据已导入"
        echo "  下一步: ${SCRIPT_DIR}/run_epoch_smoke_test.sh"
        ;;
    *)
        echo "usage: $(basename "$0") [both|0|1|local]" >&2
        exit 2
        ;;
esac
