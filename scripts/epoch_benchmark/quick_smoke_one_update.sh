#!/usr/bin/env bash
# 单笔 UPDATE 冒烟：验证双节点 sync 下不卡死、cnt 能同步
# 用法: ./quick_smoke_one_update.sh [gsql_timeout_sec]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=env.sh
source "${SCRIPT_DIR}/env.sh"

GSQL_TIMEOUT="${1:-10}"
SYNC_WAIT="${2:-10}"
REMOTE="${EPOCH_REMOTE_USER}@${EPOCH_REMOTE_HOST}"

smoke_gsql_local() {
    timeout "${GSQL_TIMEOUT}" gsql -h 127.0.0.1 -d "${EPOCH_TPCC_DB}" -p "${EPOCH_DB_PORT}" \
        -U "${EPOCH_TPCC_USER}" -W "${EPOCH_TPCC_PASSWORD}" "$@"
}

echo "=== 单笔 UPDATE 冒烟（超时 ${GSQL_TIMEOUT}s）==="
echo "node0 基线:"
smoke_gsql_local -c "SELECT id, cnt, node_id FROM epoch_smoke ORDER BY id;"

t0=$(date +%s)
set +e
smoke_gsql_local -c "UPDATE epoch_smoke SET cnt = cnt + 1, node_id = 0 WHERE id = 0;"
rc=$?
set -e
elapsed=$(( $(date +%s) - t0 ))
if [ "${rc}" -ne 0 ]; then
    echo "FAIL  UPDATE 失败或超时 (${elapsed}s, rc=${rc})" >&2
    tail -5 "${EPOCH_DATA_ROOT}/epoch-node0/epoch_recv_diag.log" 2>/dev/null || true
    exit 1
fi
echo "OK  UPDATE 完成 (${elapsed}s)"

echo ""
echo "=== 等待双节点 cnt 一致（最多 ${SYNC_WAIT}s）==="
timeout $(( SYNC_WAIT + 5 )) bash "${SCRIPT_DIR}/verify_smoke_sync.sh" "${SYNC_WAIT}" "${GSQL_TIMEOUT}"
