#!/usr/bin/env bash
# runDatabaseBuild / prep 后执行：按 node_id 分片 bmsql_hist_id_seq。
#
# 仅当 Payment 仍 INSERT bmsql_history（全局 hist_id）时需要。
# benchmarksql-5.0-epoch 已注释该 INSERT 时，prep 默认
# EPOCH_SKIP_HIST_ID_SHARD=1，不会调用本脚本；手动分片：
#   EPOCH_SKIP_HIST_ID_SHARD=0 bash prep_tpcc_cluster.sh
# 或直接: bash shard_hist_id_seq.sh NODE_ID
#
# 只用 setval + DEFAULT nextval（21:27 改挂前行为）。
# 保留 SSH 远程执行，避免从 node1 误把 peer 分片写到本机。
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

if ! epoch_is_local_node "${NODE_ID}"; then
    REMOTE="$(epoch_ssh_target "${NODE_ID}")"
    RB="$(epoch_bench_root_for_node "${NODE_ID}")"
    ssh -o BatchMode=yes -o ConnectTimeout=10 "${REMOTE}" \
        "export EPOCH_LOCAL_NODE_ID=${NODE_ID}; export EPOCH_BENCH_ROOT=${RB}; \
         export EPOCH_SKIP_NODE=${EPOCH_SKIP_NODE:-3}; \
         export EPOCH_SKIP_NODE2=$( [ \"${EPOCH_SKIP_NODE:-3}\" = 2 ] && echo 1 || echo 0 ); \
         export EPOCH_HIST_ID_NODE_STRIDE=${EPOCH_HIST_ID_NODE_STRIDE:-}; \
         export EPOCH_PROFILE=${EPOCH_PROFILE:-benchmark}; \
         bash ${RB}/openGauss-server/scripts/epoch_benchmark/shard_hist_id_seq.sh ${NODE_ID}"
    exit $?
fi

STRIDE="${EPOCH_HIST_ID_NODE_STRIDE}"
HIST_BASE=$(( $(epoch_guc_node_id "${NODE_ID}") * STRIDE ))
if [ "${EPOCH_PROFILE:-benchmark}" = "single-node-baseline" ]; then
    TOTAL_NODES="1"
else
    TOTAL_NODES="$(epoch_effective_total_nodes)"
fi
if [ $((TOTAL_NODES * STRIDE)) -gt 2147483647 ]; then
    echo "ERROR: hist_id shard out of int4 range (nodes=${TOTAL_NODES} stride=${STRIDE})" >&2
    echo "hint: EPOCH_HIST_ID_NODE_STRIDE <= $((2147483647 / TOTAL_NODES))" >&2
    exit 1
fi

HIST_END=$((HIST_BASE + STRIDE))
echo "node ${NODE_ID}: shard bmsql_hist_id_seq base=${HIST_BASE} end=${HIST_END} stride=${STRIDE} ..."
EXPECT_GUC="$(epoch_guc_node_id "${NODE_ID}")"
LIVE_GUC="$(epoch_tpcc_gsql -tAc 'SHOW epoch_node_id' | tr -d '[:space:]')"
if [ "${LIVE_GUC}" != "${EXPECT_GUC}" ]; then
    echo "ERROR: refusing shard on wrong DB: SHOW epoch_node_id=${LIVE_GUC}, expected ${EXPECT_GUC} for NODE_ID=${NODE_ID}" >&2
    exit 1
fi

epoch_tpcc_gsql -v ON_ERROR_STOP=1 -c "
DROP FUNCTION IF EXISTS bmsql_next_hist_id() CASCADE;
ALTER TABLE bmsql_history ALTER COLUMN hist_id DROP DEFAULT;
ALTER TABLE bmsql_history ALTER COLUMN hist_id SET DEFAULT nextval('bmsql_hist_id_seq');
SELECT setval(
    'bmsql_hist_id_seq',
    GREATEST(
        ${HIST_BASE}::integer,
        COALESCE(
            (SELECT max(hist_id) FROM bmsql_history
              WHERE hist_id >= ${HIST_BASE} AND hist_id < ${HIST_END}),
            ${HIST_BASE}
        )::integer
    ),
    true
);
"

echo "node ${NODE_ID}: Payment hist_id via nextval in [${HIST_BASE}, ${HIST_END})"
