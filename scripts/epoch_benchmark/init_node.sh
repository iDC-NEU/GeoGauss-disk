#!/usr/bin/env bash
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
SUPER_USER="$(epoch_super_user_for_node "${NODE_ID}")"
SUPER_PASS="$(epoch_super_password_for_node "${NODE_ID}")"

if [ -f "${DATA_DIR}/postgresql.conf" ]; then
    echo "already initialized: ${DATA_DIR}"
    exit 0
fi

mkdir -p "${DATA_DIR}"
echo "initdb node ${NODE_ID} at ${DATA_DIR} (superuser=${SUPER_USER}) ..."
gs_initdb -D "${DATA_DIR}" --nodename="node${NODE_ID}" -w "${SUPER_PASS}"

echo "next: bash ${SCRIPT_DIR}/configure_node.sh ${NODE_ID}"
