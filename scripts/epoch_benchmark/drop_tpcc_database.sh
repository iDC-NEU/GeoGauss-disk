#!/usr/bin/env bash
# 释放 epoch worker 对 TPCC 库的占用并 DROP DATABASE
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=env.sh
source "${SCRIPT_DIR}/env.sh"

NODE_ID="${1:-}"
if [ -z "${NODE_ID}" ]; then
    echo "usage: $(basename "$0") NODE_ID" >&2
    exit 2
fi
validate_node_id "${NODE_ID}"

DATA_DIR="$(data_dir_for_node "${NODE_ID}")"
CONF="${DATA_DIR}/postgresql.conf"
DB="${EPOCH_TPCC_DB}"

if [ ! -f "${CONF}" ]; then
    echo "missing ${CONF}" >&2
    exit 1
fi

set_conf_off() {
    local key="$1"
    if grep -qE "^[[:space:]]*#?${key}[[:space:]]*=" "${CONF}"; then
        sed -i "s/^[#[:space:]]*${key}[[:space:]]*=.*/${key} = off/" "${CONF}"
    else
        echo "${key} = off" >> "${CONF}"
    fi
}

echo "=== node ${NODE_ID}: disable epoch multimaster and restart ==="
set_conf_off enable_epoch_multimaster
bash "${SCRIPT_DIR}/stop_node.sh" "${NODE_ID}"
bash "${SCRIPT_DIR}/start_node.sh" "${NODE_ID}"

echo "=== drop database ${DB} ==="
epoch_super_gsql -c "clean connection to all force for database ${DB};" || true
epoch_super_gsql -c "DROP DATABASE IF EXISTS ${DB};"

echo "dropped ${DB} on node ${NODE_ID}"
echo "next: recreate/load TPCC, then: bash ${SCRIPT_DIR}/configure_node.sh ${NODE_ID} && bash ${SCRIPT_DIR}/stop_node.sh ${NODE_ID} && bash ${SCRIPT_DIR}/start_node.sh ${NODE_ID}"
