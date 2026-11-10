#!/usr/bin/env bash
# 并行启动（默认三节点）。所有节点同时发起，避免 epoch 侧启动时间差。
# 用法: bash start_cluster.sh [NODE_ID ...]
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=env.sh
source "${SCRIPT_DIR}/env.sh"

NODES=("$@")
if [ ${#NODES[@]} -eq 0 ]; then
    NODES=(1 2 3)
fi

pids=()
names=()

start_one() {
    local node_id="$1"
    local sc="${SCRIPT_DIR}"
    if epoch_is_local_node "${node_id}"; then
        local ddir owner
        ddir="$(data_dir_for_node "${node_id}")"
        owner="$(stat -c %U "${ddir}" 2>/dev/null || echo "")"
        # openGauss 禁止 root 启动；本地节点若当前用户不是数据属主，切到属主执行
        if [ -n "${owner}" ] && [ "$(id -un)" != "${owner}" ]; then
            su -s /bin/bash "${owner}" -c "bash ${sc}/start_node.sh ${node_id}"
        else
            bash "${sc}/start_node.sh" "${node_id}"
        fi
    else
        bash "${sc}/start_node.sh" "${node_id}"
    fi
}

for n in "${NODES[@]}"; do
    start_one "${n}" &
    pids+=($!)
    names+=("${n}")
done

fail=0
for i in "${!pids[@]}"; do
    if ! wait "${pids[$i]}"; then
        echo "ERROR: node ${names[$i]} failed to start" >&2
        fail=1
    fi
done

if [ "${fail}" -ne 0 ]; then
    exit 1
fi
echo "cluster started: ${NODES[*]}"
