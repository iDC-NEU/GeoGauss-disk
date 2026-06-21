#!/usr/bin/env bash
# 校验双节点 epoch_smoke 表 cnt 一致（压测结束后调用）
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=env.sh
source "${SCRIPT_DIR}/env.sh"

WAIT_SEC="${1:-10}"
GSQL_TIMEOUT="${2:-10}"
# 至少一行被写过，避免双节点同为 0 误通过
MIN_WRITE="${EPOCH_SMOKE_MIN_WRITE:-1}"
REMOTE="${EPOCH_REMOTE_USER}@${EPOCH_REMOTE_HOST}"
RB="${EPOCH_REMOTE_BENCH_ROOT}"

smoke_gsql_local() {
    timeout "${GSQL_TIMEOUT}" gsql -h 127.0.0.1 -d "${EPOCH_TPCC_DB}" -p "${EPOCH_DB_PORT}" \
        -U "${EPOCH_TPCC_USER}" -W "${EPOCH_TPCC_PASSWORD}" -t -A "$@"
}

smoke_gsql_remote() {
    local sql="$1"
    timeout 10 ssh -o BatchMode=yes -o ConnectTimeout=5 "${REMOTE}" bash -s <<EOF
set -euo pipefail
export EPOCH_BENCH_ROOT="${RB}"
export GAUSSHOME="\${EPOCH_BENCH_ROOT}/openGauss-server/mppdb_temp_install"
export PATH="\${GAUSSHOME}/bin:\${PATH}"
export LD_LIBRARY_PATH="\${GAUSSHOME}/lib:\${EPOCH_ZMQ_LIB:-}:\${LD_LIBRARY_PATH:-}"
source "\${EPOCH_BENCH_ROOT}/openGauss-server/scripts/epoch_benchmark/env.sh"
timeout ${GSQL_TIMEOUT} gsql -h 127.0.0.1 -d ${EPOCH_TPCC_DB} -p ${EPOCH_DB_PORT} \
    -U ${EPOCH_TPCC_USER} -W '${EPOCH_TPCC_PASSWORD}' -t -A -c "${sql}"
EOF
}

read_cnt() {
    local node_id="$1"
    local row_id="$2"
    local sql="SELECT cnt FROM epoch_smoke WHERE id = ${row_id};"
    if [ "${node_id}" = "0" ]; then
        smoke_gsql_local -c "${sql}" 2>/dev/null | tr -d '[:space:]'
    else
        smoke_gsql_remote "${sql}" 2>/dev/null | tr -d '[:space:]'
    fi
}

echo "=== 等待双节点 epoch_smoke 数据一致（最多 ${WAIT_SEC}s）==="
end=$((SECONDS + WAIT_SEC))
last=""
while [ "${SECONDS}" -lt "${end}" ]; do
    ok=1
    details=""
    wrote=0
    for row_id in 0 1; do
        c0=$(read_cnt 0 "${row_id}" || echo "")
        c1=$(read_cnt 1 "${row_id}" || echo "")
        if [ -z "${c0}" ] || [ -z "${c1}" ]; then
            ok=0
            details="${details} id=${row_id}(query_fail)"
            continue
        fi
        if [ "${c0}" != "${c1}" ]; then
            ok=0
        fi
        if [ "${c0}" -ge "${MIN_WRITE}" ] 2>/dev/null || [ "${c1}" -ge "${MIN_WRITE}" ] 2>/dev/null; then
            wrote=1
        fi
        details="${details} id=${row_id}(n0=${c0},n1=${c1})"
    done
    if [ "${ok}" -eq 1 ] && [ "${wrote}" -eq 0 ]; then
        ok=0
        details="${details} (cnt 全为 0，无有效写入)"
    fi
    line="[$(date +%H:%M:%S)]${details}"
    if [ "${line}" != "${last}" ]; then
        echo "${line}"
        last="${line}"
    fi
    if [ "${ok}" -eq 1 ]; then
        echo "OK  双节点 epoch_smoke 数据一致${details}"
        exit 0
    fi
    sleep 2
done

echo "FAIL  ${WAIT_SEC}s 内 epoch_smoke 仍不一致${details}" >&2
exit 1
