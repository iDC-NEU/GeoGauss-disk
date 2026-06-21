#!/usr/bin/env bash
# 单 worker：循环 UPDATE（单笔失败不判死，最终以双节点数据一致为准）
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=env.sh
source "${SCRIPT_DIR}/env.sh"

NODE_ID="${1:?node_id}"
TXNS="${2:?txn_count}"
WORKER_ID="${3:-0}"
TXN_TIMEOUT_SEC="${EPOCH_SMOKE_TXN_TIMEOUT:-10}"

DB_USER="${EPOCH_TPCC_USER}"
DB_PASS="${EPOCH_TPCC_PASSWORD}"
DB_NAME="${EPOCH_TPCC_DB}"

for _ in $(seq 1 "${TXNS}"); do
    timeout "${TXN_TIMEOUT_SEC}" gsql -h 127.0.0.1 -d "${DB_NAME}" -p "${EPOCH_DB_PORT}" \
        -U "${DB_USER}" -W "${DB_PASS}" -q \
        -c "UPDATE epoch_smoke SET cnt = cnt + 1, node_id = ${NODE_ID} WHERE id = ${NODE_ID};" \
        >/dev/null 2>&1 || true
done
exit 0
