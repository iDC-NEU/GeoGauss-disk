#!/usr/bin/env bash
# 检查本机 test 库 epoch_smoke 表（单节点导入时使用；双节点压测前勿依赖此脚本）
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=env.sh
source "${SCRIPT_DIR}/env.sh"

NODE_ID="${1:-0}"
GSQL_TIMEOUT="${2:-10}"

smoke_gsql() {
    timeout "${GSQL_TIMEOUT}" gsql -h 127.0.0.1 -d "${EPOCH_TPCC_DB}" -p "${EPOCH_DB_PORT}" \
        -U "${EPOCH_TPCC_USER}" -W "${EPOCH_TPCC_PASSWORD}" -t -A "$@"
}

count="$(smoke_gsql -c "SELECT COUNT(*) FROM epoch_smoke;" 2>/dev/null | tr -d '[:space:]' || echo 0)"
if [ "${count}" != "2" ]; then
    echo "FAIL node${NODE_ID}: epoch_smoke 行数=${count}（期望 2）" >&2
    exit 1
fi

for id in 0 1; do
    if ! smoke_gsql -c "SELECT 1 FROM epoch_smoke WHERE id = ${id};" 2>/dev/null | grep -q 1; then
        echo "FAIL node${NODE_ID}: 缺少 epoch_smoke.id=${id}" >&2
        exit 1
    fi
done

echo "OK  node${NODE_ID}: epoch_smoke 已导入（2 行）"
