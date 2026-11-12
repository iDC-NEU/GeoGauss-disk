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

    # 参数：健康对端延迟、慢节点对端延迟、慢节点 IP 列表（逗号分隔）、上行、下行、健康对端 IP...
    local delay_ms="$1"
    local slow_delay_ms="$2"
    local slow_csv="$3"
    local up_mbit="$4"
    local down_mbit="$5"
    shift 5
    local peers=("$@")
    local -a slow_peers=()
    # 空列表用 "-" 占位：ssh 重放参数时空串会被吞掉
    if [ "${slow_csv}" = "-" ]; then
        slow_csv=""
    fi
    if [ -n "${slow_csv}" ]; then
        IFS=',' read -r -a slow_peers <<< "${slow_csv}"
    fi

    cleanup_tc "${iface}"
    if [ "${delay_ms}" -eq 0 ] && [ "${up_mbit}" -eq 0 ] && [ "${down_mbit}" -eq 0 ]; then
        echo "Node ${local_ip}: no delay and no rate limit"
        return 0
    fi
    if [ "${#peers[@]}" -eq 0 ]; then
        echo "Node ${local_ip}: no peers, skip" >&2
        return 0
    fi

    echo "Node ${local_ip} iface ${iface}: egress delay ${delay_ms}ms (slow peers ${slow_delay_ms}ms) up ${up_mbit}Mbit down ${down_mbit}Mbit peers: ${peers[*]}"

    if [ "${delay_ms}" -gt 0 ] || [ "${up_mbit}" -gt 0 ]; then
        "${TC_CMD}" qdisc add dev "${iface}" root handle 1: prio bands 4 priomap 1 2 2 2 1 2 0 0 1 1 1 1 1 1 1 1
        if [ "${slow_delay_ms}" -gt 0 ] && [ "${#slow_peers[@]}" -gt 0 ]; then
            "${TC_CMD}" qdisc add dev "${iface}" parent 1:3 handle 30: netem delay "${slow_delay_ms}ms"
            local slow
            for slow in "${slow_peers[@]}"; do
                "${TC_CMD}" filter add dev "${iface}" protocol ip parent 1:0 prio 1 u32 match ip dst "${slow}" flowid 1:3
            done
        fi
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
    slow_rtt="${EPOCH_NETWORK_RTT_MS:-100}"
    healthy_rtt="${EPOCH_RTT_HEALTHY_MS:-35}"
    bw_mode="${EPOCH_DYN_NET_BW_MODE:-uplimit}"
    bw_mbps="${EPOCH_DYN_NET_BW_MBPS:-10}"
    if ! [[ "${slow_rtt}" =~ ^[0-9]+$ ]] || ! [[ "${healthy_rtt}" =~ ^[0-9]+$ ]]; then
        echo "ERROR: EPOCH_NETWORK_RTT_MS / EPOCH_RTT_HEALTHY_MS must be integers" >&2
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
    slow_delay_ms=$((slow_rtt / 2))
    healthy_delay_ms=$((healthy_rtt / 2))
fi

for n in $(epoch_active_node_ids); do
    if [ "${CMD}" != "add" ]; then
        run_on_node "${n}" "${CMD}"
        continue
    fi
    up=0
    down=0
    if epoch_node_is_slow "${n}"; then
        case "${bw_mode}" in
            uplimit) up="${bw_mbps}" ;;
            downlimit) down="${bw_mbps}" ;;
            bilimit) up="${bw_mbps}"; down="${bw_mbps}" ;;
        esac
    fi
    # 慢节点：所有出口都按慢链路延迟；健康节点：到慢节点 1:3、到健康节点 1:4
    if epoch_node_is_slow "${n}"; then
        node_delay="${slow_delay_ms}"
        node_slow_delay=0
        slow_csv="-"
    else
        node_delay="${healthy_delay_ms}"
        node_slow_delay="${slow_delay_ms}"
        slow_csv=""
        for s in $(epoch_active_node_ids); do
            [ "${s}" = "${n}" ] && continue
            epoch_node_is_slow "${s}" || continue
            slow_csv="${slow_csv:+${slow_csv},}$(epoch_node_ip "${s}")"
        done
    fi
    peers=""
    for p in $(epoch_active_node_ids); do
        [ "${p}" = "${n}" ] && continue
        # 健康节点发往慢节点的流量已经分到 1:3，不再进 1:4
        if ! epoch_node_is_slow "${n}" && epoch_node_is_slow "${p}"; then
            continue
        fi
        peers="${peers} $(epoch_node_ip "${p}")"
    done
    # shellcheck disable=SC2086
    run_on_node "${n}" add "${node_delay}" "${node_slow_delay}" "${slow_csv}" "${up}" "${down}" ${peers}
done

if [ "${CMD}" = "add" ]; then
    echo "tc 已加上：慢节点出口 ${slow_delay_ms}ms，健康节点出口 ${healthy_delay_ms}ms（发往慢节点 ${slow_delay_ms}ms）。"
    echo "预期 RTT：慢节点链路约 ${EPOCH_NETWORK_RTT_MS:-100}ms，健康节点之间约 ${EPOCH_RTT_HEALTHY_MS:-35}ms。"
    echo "核对: ping -c 5 ${EPOCH_NODE1_IP} ; ping -c 5 ${EPOCH_NODE2_IP}"
    echo "撤掉: bash ${SCRIPT_DIR}/apply_netem.sh del"
fi
