#!/usr/bin/env bash
# runDatabaseBuild 之后执行：按 node_id 分片 bmsql_hist_id_seq，避免多主 apply 后 hist_id 撞号。
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

STRIDE="${EPOCH_HIST_ID_NODE_STRIDE}"
HIST_BASE=$((NODE_ID * STRIDE))
TOTAL_NODES="${EPOCH_TOTAL_NODES}"
# 最后一号节点本地发号上界约 TOTAL_NODES * STRIDE，须 < int4 max
if [ $((TOTAL_NODES * STRIDE)) -gt 2147483647 ]; then
    echo "ERROR: hist_id shard out of int4 range (nodes=${TOTAL_NODES} stride=${STRIDE})" >&2
    echo "hint: EPOCH_HIST_ID_NODE_STRIDE <= $((2147483647 / TOTAL_NODES))" >&2
    exit 1
fi

echo "node ${NODE_ID}: shard bmsql_hist_id_seq base=${HIST_BASE} stride=${STRIDE} ..."
epoch_tpcc_gsql -c "
SELECT setval(
    'bmsql_hist_id_seq',
    GREATEST(
        ${HIST_BASE}::integer,
        COALESCE((SELECT max(hist_id) FROM bmsql_history), 0)::integer
    ),
    true
);
"

echo "node ${NODE_ID}: next Payment hist_id will be > GREATEST(${HIST_BASE}, max(hist_id))"
