#!/usr/bin/env bash
# Transactional YCSB driver: ten operations per transaction, Zipfian key distribution.
# Needs a JDK and a PostgreSQL/openGauss JDBC jar (EPOCH_JDBC_JAR).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=env.sh
source "${SCRIPT_DIR}/env.sh"

MODE="${1:-run}"
WORKLOAD="${2:-MC}"

case "${MODE}" in
    load|run) ;;
    *)
        echo "usage: $(basename "$0") {load|run} [MC|HC|RO]" >&2
        exit 2
        ;;
esac

case "${WORKLOAD}" in
    MC|HC|RO) ;;
    *)
        echo "ERROR: workload must be MC, HC or RO, got: ${WORKLOAD}" >&2
        exit 2
        ;;
esac

if [ -z "${EPOCH_JDBC_JAR:-}" ]; then
    echo "ERROR: set EPOCH_JDBC_JAR to a JDBC jar (org.postgresql or org.opengauss)" >&2
    exit 1
fi

JAVA_CMD="${JAVA_HOME:+${JAVA_HOME}/bin/}java"
JAVAC_CMD="${JAVA_HOME:+${JAVA_HOME}/bin/}javac"
for tool in "${JAVA_CMD}" "${JAVAC_CMD}"; do
    if ! command -v "${tool}" >/dev/null 2>&1; then
        echo "ERROR: ${tool} not found; set JAVA_HOME" >&2
        exit 1
    fi
done

SRC="${SCRIPT_DIR}/ycsb/YcsbTxnClient.java"
CLASSES="${SCRIPT_DIR}/ycsb/classes"
mkdir -p "${CLASSES}"
if [ ! -f "${CLASSES}/YcsbTxnClient.class" ] || [ "${SRC}" -nt "${CLASSES}/YcsbTxnClient.class" ]; then
    echo "compiling ${SRC}" >&2
    "${JAVAC_CMD}" -encoding UTF-8 -d "${CLASSES}" "${SRC}"
fi

node_id="$(epoch_detect_local_node_id || echo 1)"
db_name="${EPOCH_YCSB_DB:-ycsb}"
table="${EPOCH_YCSB_TABLE:-usertable}"
url="jdbc:postgresql://$(epoch_node_ip "${node_id}"):${EPOCH_DB_PORT:-32000}/${db_name}"

# 论文里 256 个连接是总数；按活动节点均分，多余的依次给前面的节点
if [ -n "${EPOCH_YCSB_THREADS:-}" ]; then
    threads="${EPOCH_YCSB_THREADS}"
else
    total="${EPOCH_YCSB_CONNECTIONS:-256}"
    active_ids="$(epoch_active_node_ids)"
    # shellcheck disable=SC2086
    active_count=$(echo ${active_ids} | wc -w)
    base=$((total / active_count))
    extra=$((total % active_count))
    index=0
    for id in ${active_ids}; do
        index=$((index + 1))
        [ "${id}" = "${node_id}" ] && break
    done
    threads="${base}"
    if [ "${index}" -le "${extra}" ]; then
        threads=$((threads + 1))
    fi
fi

echo "ycsb ${MODE} ${WORKLOAD}: ${url} table=${table} threads=${threads}" >&2

exec "${JAVA_CMD}" -cp "${CLASSES}:${EPOCH_JDBC_JAR}" \
    -Dycsb.driver="${EPOCH_JDBC_DRIVER:-org.postgresql.Driver}" \
    YcsbTxnClient \
    mode="${MODE}" \
    url="${url}" \
    user="${EPOCH_TPCC_USER}" \
    password="${EPOCH_TPCC_PASSWORD}" \
    table="${table}" \
    mix="${WORKLOAD}" \
    threads="${threads}" \
    duration="${EPOCH_YCSB_SECONDS:-180}" \
    warmup="${EPOCH_YCSB_WARMUP:-30}"
