#!/usr/bin/env bash
# 用 tc 给副本之间加真实延迟和慢节点带宽。ping 能直接看到 RTT。
# 每台参与节点只在发往其他节点的出口上加 RTT 的一半，往返约等于 EPOCH_NETWORK_RTT_MS。
# 带宽只加在 EPOCH_DYN_NET_SLOW_NODES 上：uplimit 限出口，downlimit 限入口，bilimit 双向。
#
#   bash scripts/epoch_benchmark/apply_netem.sh add
#   ping -c 5 <对端 IP>
#   bash scripts/epoch_benchmark/apply_netem.sh del
#   bash scripts/epoch_benchmark/apply_netem.sh status
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

cleanup_tc() {
    local iface="$1"
    "${TC_CMD}" qdisc del dev "${iface}" root 2>/dev/null || true
    "${TC_CMD}" qdisc del dev "${iface}" ingress 2>/dev/null || true
    "${TC_CMD}" qdisc del dev ifb0 root 2>/dev/null || true
    "${IP_CMD}" link set dev ifb0 down 2>/dev/null || true
}

apply_local() {
    local cmd="$1"
    shift
    if [ "$(id -u)" -ne 0 ]; then
        echo "ERROR: --local 需要 root（由外层 sudo 调用）" >&2
        exit 1
    fi
    IP_CMD=$(command -v ip)
    TC_CMD=$(command -v tc)
    modprobe sch_netem 2>/dev/null || true

    local iface
    iface=$("${IP_CMD}" route show default | awk '/default/ {print $5; exit}')
    if [ -z "${iface}" ]; then
        echo "ERROR: 找不到默认路由网卡" >&2
        exit 1
    fi
    local local_ip
    local_ip=$("${IP_CMD}" -4 addr show dev "${iface}" | awk '/inet / {print $2; exit}' | cut -d/ -f1)

    if [ "${cmd}" = "del" ] || [ "${cmd}" = "status" ]; then
        if [ "${cmd}" = "del" ]; then
            cleanup_tc "${iface}"
            echo "Removed tc rules on ${local_ip} ${iface}"
        else
            echo "=== ${local_ip} ${iface} ==="
            "${TC_CMD}" qdisc show dev "${iface}" || true
            "${TC_CMD}" qdisc show dev ifb0 2>/dev/null || true
        fi
        return 0
    fi

    local delay_ms="$1"
    local up_mbit="$2"
    local down_mbit="$3"
    shift 3
    local peers=("$@")

    cleanup_tc "${iface}"
    if [ "${delay_ms}" -eq 0 ] && [ "${up_mbit}" -eq 0 ] && [ "${down_mbit}" -eq 0 ]; then
        echo "Node ${local_ip}: no delay and no rate limit"
        return 0
    fi
    if [ "${#peers[@]}" -eq 0 ]; then
        echo "Node ${local_ip}: no peers, skip" >&2
        return 0
    fi

    echo "Node ${local_ip} iface ${iface}: egress delay ${delay_ms}ms up ${up_mbit}Mbit down ${down_mbit}Mbit peers: ${peers[*]}"

    if [ "${delay_ms}" -gt 0 ] || [ "${up_mbit}" -gt 0 ]; then
        "${TC_CMD}" qdisc add dev "${iface}" root handle 1: prio bands 4 priomap 1 2 2 2 1 2 0 0 1 1 1 1 1 1 1 1
        if [ "${up_mbit}" -gt 0 ]; then
            "${TC_CMD}" qdisc add dev "${iface}" parent 1:4 handle 40: htb default 1
            "${TC_CMD}" class add dev "${iface}" parent 40: classid 40:1 htb rate "${up_mbit}mbit" ceil "${up_mbit}mbit" burst 256kb
            if [ "${delay_ms}" -gt 0 ]; then
                "${TC_CMD}" qdisc add dev "${iface}" parent 40:1 handle 41: netem delay "${delay_ms}ms"
            fi
        else
            "${TC_CMD}" qdisc add dev "${iface}" parent 1:4 handle 40: netem delay "${delay_ms}ms"
        fi
        local peer
        for peer in "${peers[@]}"; do
            "${TC_CMD}" filter add dev "${iface}" protocol ip parent 1:0 prio 1 u32 match ip dst "${peer}" flowid 1:4
        done
    fi

    if [ "${down_mbit}" -gt 0 ]; then
        modprobe ifb 2>/dev/null || true
        "${IP_CMD}" link add ifb0 type ifb 2>/dev/null || true
        "${IP_CMD}" link set dev ifb0 up
        "${TC_CMD}" qdisc add dev "${iface}" handle ffff: ingress
        local peer
        for peer in "${peers[@]}"; do
            "${TC_CMD}" filter add dev "${iface}" parent ffff: protocol ip u32 \
                match ip src "${peer}" action mirred egress redirect dev ifb0
        done
        "${TC_CMD}" qdisc add dev ifb0 root tbf rate "${down_mbit}mbit" burst 256kb latency 400ms
    fi
}

if [ "${1:-}" = "--local" ]; then
    shift
    apply_local "$@"
    exit 0
fi

# shellcheck source=env.sh
source "${SCRIPT_DIR}/env.sh"

CMD="${1:-}"
case "${CMD}" in
    add|del|status) ;;
    *)
        echo "Usage: bash $0 {add|del|status}" >&2
        exit 2
        ;;
esac

node_is_slow() {
    local node_id="$1"
    local guc id
    guc="$(epoch_guc_node_id "${node_id}")"
    local IFS=','
    for id in ${EPOCH_DYN_NET_SLOW_NODES:-}; do
        id="${id// /}"
        if [ -n "${id}" ] && [ "${id}" = "${guc}" ]; then
            return 0
        fi
    done
    return 1
}

run_sudo() {
    if sudo -n "$@"; then
        return 0
    fi
    if [ -n "${EPOCH_NODE2_SUDO_PASSWORD:-}" ]; then
        printf '%s\n' "${EPOCH_NODE2_SUDO_PASSWORD}" | sudo -S -p '' "$@"
        return
    fi
    return 1
}

run_on_node() {
    local node_id="$1"
    shift
    if epoch_is_local_node "${node_id}"; then
        run_sudo bash "${SCRIPT_DIR}/apply_netem.sh" --local "$@"
        return
    fi
    local rb target
    rb="$(epoch_bench_root_for_node "${node_id}")"
    target="$(epoch_ssh_target "${node_id}")"
    ssh -o BatchMode=yes -o ConnectTimeout=10 "${target}" \
        "mkdir -p '${rb}/tmp' && cat > '${rb}/tmp/apply_netem.sh'" \
        < "${SCRIPT_DIR}/apply_netem.sh"
    if ssh -o BatchMode=yes -o ConnectTimeout=10 "${target}" \
        "sudo -n bash '${rb}/tmp/apply_netem.sh' --local $*"; then
        return 0
    fi
    if [ -n "${EPOCH_NODE2_SUDO_PASSWORD:-}" ]; then
        printf '%s\n' "${EPOCH_NODE2_SUDO_PASSWORD}" | ssh -o BatchMode=yes -o ConnectTimeout=10 "${target}" \
            "sudo -S -p '' bash '${rb}/tmp/apply_netem.sh' --local $*"
        return
    fi
    echo "node${node_id} 没有免密 sudo，且未设置 EPOCH_NODE2_SUDO_PASSWORD。请在那台机器上执行：" >&2
    echo "  ssh ${target}" >&2
    echo "  sudo bash ${rb}/tmp/apply_netem.sh --local $*" >&2
    return 1
}

if [ "${CMD}" = "add" ]; then
    rtt="${EPOCH_NETWORK_RTT_MS:-100}"
    bw_mode="${EPOCH_DYN_NET_BW_MODE:-uplimit}"
    bw_mbps="${EPOCH_DYN_NET_BW_MBPS:-100}"
    if ! [[ "${rtt}" =~ ^[0-9]+$ ]]; then
        echo "ERROR: EPOCH_NETWORK_RTT_MS must be an integer, got: ${rtt}" >&2
        exit 1
    fi
    if ! [[ "${bw_mbps}" =~ ^[0-9]+$ ]]; then
        echo "ERROR: EPOCH_DYN_NET_BW_MBPS must be an integer, got: ${bw_mbps}" >&2
        exit 1
    fi
    case "${bw_mode}" in
        unlimit|uplimit|downlimit|bilimit) ;;
        *)
            echo "ERROR: EPOCH_DYN_NET_BW_MODE must be unlimit|uplimit|downlimit|bilimit, got: ${bw_mode}" >&2
            exit 1
            ;;
    esac
    delay_ms=$((rtt / 2))
fi

for n in $(epoch_active_node_ids); do
    if [ "${CMD}" != "add" ]; then
        run_on_node "${n}" "${CMD}"
        continue
    fi
    up=0
    down=0
    if node_is_slow "${n}"; then
        case "${bw_mode}" in
            uplimit) up="${bw_mbps}" ;;
            downlimit) down="${bw_mbps}" ;;
            bilimit) up="${bw_mbps}"; down="${bw_mbps}" ;;
        esac
    fi
    peers=""
    for p in $(epoch_active_node_ids); do
        [ "${p}" = "${n}" ] && continue
        peers="${peers} $(epoch_node_ip "${p}")"
    done
    # shellcheck disable=SC2086
    run_on_node "${n}" add "${delay_ms}" "${up}" "${down}" ${peers}
done

if [ "${CMD}" = "add" ]; then
    echo "tc 已加上。每端出口延迟 ${delay_ms}ms，ping 往返应约 ${EPOCH_NETWORK_RTT_MS}ms。"
    echo "核对: ping -c 5 ${EPOCH_NODE1_IP} ; ping -c 5 ${EPOCH_NODE2_IP}"
    echo "撤掉: bash ${SCRIPT_DIR}/apply_netem.sh del"
fi
