#!/usr/bin/env bash
# 单节点 TPCC 准备：建库/用户 → 灌库 → hist_id 分片（不含压测）
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=env.sh
source "${SCRIPT_DIR}/env.sh"

NODE_ID="${1:-}"
ACTION="${2:-all}"
if [ -z "${NODE_ID}" ]; then
    echo "usage: $(basename "$0") NODE_ID [all|setup|load|shard]" >&2
    exit 2
fi
validate_node_id "${NODE_ID}"

BENCH_RUN="${EPOCH_BENCHMARKSQL}"
PROPS_LOAD="${BENCH_RUN}/props.epoch.load"
PROPS_RUN="${BENCH_RUN}/props.epoch.node${NODE_ID}"
JDK="${EPOCH_BENCH_ROOT}/epoch_deploy/jdk8"

if [ ! -x "${JDK}/bin/java" ]; then
    echo "ERROR: missing ${JDK}/bin/java" >&2
    echo "hint: rsync JRE from node0 to epoch_deploy/jdk8, or install openjdk-8" >&2
    exit 1
fi
export JAVA_HOME="${JDK}"
export PATH="${JAVA_HOME}/bin:${PATH}"

if [ ! -f "${PROPS_RUN}" ]; then
    echo "ERROR: missing ${PROPS_RUN}" >&2
    exit 1
fi

setup_tpcc_db() {
    echo "=== setup test database and bot user on node${NODE_ID} ==="
    if ! epoch_super_gsql -tAc "SELECT 1 FROM pg_roles WHERE rolname='bot'" | grep -q 1; then
        epoch_super_gsql -c "CREATE USER bot WITH PASSWORD '${EPOCH_TPCC_PASSWORD}';"
    fi
    if ! epoch_super_gsql -tAc "SELECT 1 FROM pg_database WHERE datname='${EPOCH_TPCC_DB}'" | grep -q 1; then
        epoch_super_gsql -c "CREATE DATABASE ${EPOCH_TPCC_DB} OWNER bot;"
    fi
    epoch_super_gsql -d "${EPOCH_TPCC_DB}" -c "GRANT ALL PRIVILEGES TO bot;"
    echo "database ${EPOCH_TPCC_DB} ready"
}

load_tpcc() {
    echo "=== TPCC load (100 warehouses) on node${NODE_ID} ==="
    cd "${BENCH_RUN}"
    if [ ! -f "${PROPS_LOAD}" ]; then
        cp props.epoch.node0 "${PROPS_LOAD}"
        sed -i 's/runWarehouses=33/runWarehouses=100/' "${PROPS_LOAD}"
        sed -i 's/loadWorkers=100/loadWorkers=50/' "${PROPS_LOAD}" 2>/dev/null || true
    fi
    ./runDatabaseBuild.sh props.epoch.load
}

shard_tpcc() {
    echo "=== shard bmsql_hist_id_seq on node${NODE_ID} ==="
    bash "${SCRIPT_DIR}/shard_hist_id_seq.sh" "${NODE_ID}"
}

case "${ACTION}" in
    all)
        setup_tpcc_db
        load_tpcc
        shard_tpcc
        ;;
    setup) setup_tpcc_db ;;
    load) load_tpcc ;;
    shard) shard_tpcc ;;
    *)
        echo "unknown action: ${ACTION}" >&2
        exit 2
        ;;
esac

echo "done: node${NODE_ID} action=${ACTION}"
