#!/usr/bin/env bash
# HCJ ADDBY: epoch 多节点 BenchmarkSQL 测试环境变量

# 从 scripts/epoch_benchmark 向上三级推断 bench 根目录（node0/node1/node2 各自本机路径）
_epoch_env_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export EPOCH_BENCH_ROOT="${EPOCH_BENCH_ROOT:-$(cd "${_epoch_env_dir}/../../.." && pwd)}"
# 勿沿用 ~/.bashrc 里的 GAUSSHOME，epoch 必须用 2025-9 下与 node0 同步的二进制
export GAUSSHOME="${EPOCH_BENCH_ROOT}/openGauss-server/mppdb_temp_install"
export EPOCH_RUNTIME_LIBS="${EPOCH_BENCH_ROOT}/epoch_deploy/runtime_libs"
export BINARYLIBS="${BINARYLIBS:-/home/hcj2003/openGauss-third_party_binarylibs}"
export GCC_PATH="${GCC_PATH:-${BINARYLIBS}/buildtools/gcc10.3}"

epoch_prepend_ld_path() {
    local dir="$1"
    if [ -d "${dir}" ]; then
        if [ -z "${LD_LIBRARY_PATH:-}" ]; then
            LD_LIBRARY_PATH="${dir}"
        else
            case ":${LD_LIBRARY_PATH}:" in
                *:"${dir}":*) ;;
                *) LD_LIBRARY_PATH="${dir}:${LD_LIBRARY_PATH}" ;;
            esac
        fi
    fi
}

# 仅追加本机存在的目录，避免远程节点引用 hcj2003 路径；runtime_libs 优先于 /usr/local
LD_LIBRARY_PATH=""
epoch_prepend_ld_path "${GAUSSHOME}/lib"
epoch_prepend_ld_path "${EPOCH_RUNTIME_LIBS}"
if [ -f "${EPOCH_RUNTIME_LIBS}/libzmq.so.5" ] || [ -f "${EPOCH_RUNTIME_LIBS}/libzmq.so" ]; then
    :
elif [ -f "/lib64/libzmq.so.5" ]; then
    epoch_prepend_ld_path "/lib64"
elif [ -d "/home/hcj2003/GeoGauss/dest/lib" ] \
    && [ -f "/home/hcj2003/GeoGauss/dest/lib/libzmq.so.5" ]; then
    # 仅当无 runtime_libs/lib64 时回退；勿整目录加入 LD_LIBRARY_PATH（会污染 libstdc++）
    epoch_prepend_ld_path "/home/hcj2003/GeoGauss/dest/lib"
fi
if [ -d "${GCC_PATH}/lib64" ]; then
    epoch_prepend_ld_path "${GCC_PATH}/lib64"
    epoch_prepend_ld_path "${GCC_PATH}/isl/lib"
    epoch_prepend_ld_path "${GCC_PATH}/mpc/lib"
    epoch_prepend_ld_path "${GCC_PATH}/mpfr/lib"
    epoch_prepend_ld_path "${GCC_PATH}/gmp/lib"
fi
export LD_LIBRARY_PATH

PATH="${GAUSSHOME}/bin:/usr/bin:/bin"
if [ -d "${GCC_PATH}/gcc/bin" ]; then
    PATH="${GAUSSHOME}/bin:${GCC_PATH}/gcc/bin:/usr/bin:/bin"
fi
export PATH

export EPOCH_DATA_ROOT="${EPOCH_DATA_ROOT:-${EPOCH_BENCH_ROOT}/data}"
export EPOCH_BENCHMARKSQL="${EPOCH_BENCH_ROOT}/benchmarksql-5.0-epoch/run"

# 三节点拓扑
export EPOCH_NODE0_IP="${EPOCH_NODE0_IP:-202.199.13.149}"
export EPOCH_NODE1_IP="${EPOCH_NODE1_IP:-219.216.64.113}"
export EPOCH_NODE2_IP="${EPOCH_NODE2_IP:-202.199.13.61}"
export EPOCH_TOTAL_NODES=3
export EPOCH_PEER_HOSTS="${EPOCH_NODE0_IP},${EPOCH_NODE1_IP},${EPOCH_NODE2_IP}"
export EPOCH_DB_PORT="${EPOCH_DB_PORT:-32000}"
export EPOCH_ZMQ_PORT_BASE="${EPOCH_ZMQ_PORT_BASE:-25600}"

# node0 / node1：hcj2003；node2：zwx（gs_initdb 超级用户随 OS 用户）
# 以下密码仅用于 gs_initdb -w；日常 gsql 走 pg_hba trust，无需 -U/-W
export EPOCH_SUPER_USER="${EPOCH_SUPER_USER:-hcj2003}"
export EPOCH_SUPER_PASSWORD="${EPOCH_SUPER_PASSWORD:-Gauss@123.}"
export EPOCH_NODE2_SUPER_USER="${EPOCH_NODE2_SUPER_USER:-zwx}"
export EPOCH_NODE2_SUPER_PASSWORD="${EPOCH_NODE2_SUPER_PASSWORD:-Test@123}"
export EPOCH_TPCC_USER=bot
export EPOCH_TPCC_PASSWORD="${EPOCH_TPCC_PASSWORD:-Test@123}"
export EPOCH_TPCC_DB=test

# TPCC / epoch 性能调优（三节点各跑 33 warehouse，共 100；terminal：node0=33, node1=33, node2=34）
export EPOCH_LOAD_WAREHOUSES="${EPOCH_LOAD_WAREHOUSES:-100}"
export EPOCH_RUN_WAREHOUSES_PER_NODE="${EPOCH_RUN_WAREHOUSES_PER_NODE:-33}"
export EPOCH_TPCC_TERMINALS_NODE0="${EPOCH_TPCC_TERMINALS_NODE0:-33}"
export EPOCH_TPCC_TERMINALS_NODE1="${EPOCH_TPCC_TERMINALS_NODE1:-33}"
export EPOCH_TPCC_TERMINALS_NODE2="${EPOCH_TPCC_TERMINALS_NODE2:-34}"
# Payment hist_id 分片步长（bmsql_history.hist_id 为 int4）
# node N 从 N*stride+1 发号；默认 stride=2147483647/TOTAL_NODES
export EPOCH_HIST_ID_NODE_STRIDE="${EPOCH_HIST_ID_NODE_STRIDE:-$((2147483647 / EPOCH_TOTAL_NODES))}"
if ! [[ "${EPOCH_HIST_ID_NODE_STRIDE}" =~ ^[0-9]+$ ]] \
    || [ $((EPOCH_TOTAL_NODES * EPOCH_HIST_ID_NODE_STRIDE)) -gt 2147483647 ]; then
    export EPOCH_HIST_ID_NODE_STRIDE=$((2147483647 / EPOCH_TOTAL_NODES))
    echo "WARN: EPOCH_HIST_ID_NODE_STRIDE adjusted to ${EPOCH_HIST_ID_NODE_STRIDE} for ${EPOCH_TOTAL_NODES} nodes" >&2
fi

# 远程部署：node1
export EPOCH_REMOTE_USER="${EPOCH_REMOTE_USER:-hcj2003}"
export EPOCH_REMOTE_HOST="${EPOCH_REMOTE_HOST:-${EPOCH_NODE1_IP}}"
export EPOCH_REMOTE_HOME="${EPOCH_REMOTE_HOME:-/home/hcj2003}"
export EPOCH_REMOTE_BENCH_ROOT="${EPOCH_REMOTE_BENCH_ROOT:-${EPOCH_REMOTE_HOME}/2025-9}"

# 远程部署：node2
export EPOCH_NODE2_REMOTE_USER="${EPOCH_NODE2_REMOTE_USER:-zwx}"
export EPOCH_NODE2_REMOTE_HOME="${EPOCH_NODE2_REMOTE_HOME:-/home/zwx}"
export EPOCH_NODE2_BENCH_ROOT="${EPOCH_NODE2_BENCH_ROOT:-${EPOCH_NODE2_REMOTE_HOME}/2025-9}"

export BUILD_JOBS=40
export OPENGAUSS_SRC="${EPOCH_BENCH_ROOT}/openGauss-server"

epoch_tpcc_terminals_for_node() {
    local node_id="$1"
    case "${node_id}" in
        0) echo "${EPOCH_TPCC_TERMINALS_NODE0}" ;;
        1) echo "${EPOCH_TPCC_TERMINALS_NODE1}" ;;
        2) echo "${EPOCH_TPCC_TERMINALS_NODE2}" ;;
        *)
            echo "invalid node_id ${node_id} (expected 0..$((EPOCH_TOTAL_NODES - 1)))" >&2
            return 1
            ;;
    esac
}

# openGauss 编译环境（build.sh --cmake 依赖 gcc10.3 + cmake；与运行时 GAUSSHOME 分离）
epoch_find_cmake_bin_dir() {
    local d candidate
    if command -v cmake >/dev/null 2>&1; then
        dirname "$(command -v cmake)"
        return 0
    fi
    candidate="${EPOCH_BENCH_ROOT}/epoch_deploy/buildtools/cmake/bin"
    if [ -x "${candidate}/cmake" ]; then
        echo "${candidate}"
        return 0
    fi
    for d in \
        "${BINARYLIBS}"/buildtools/cmake*/bin \
        /home/hcj2003/.cache/JetBrains/RemoteDev/dist/*/bin/cmake/linux/x64/bin; do
        for d in ${d}; do
            if [ -x "${d}/cmake" ]; then
                echo "${d}"
                return 0
            fi
        done
    done
    return 1
}

epoch_ensure_cmake_installed() {
    local src_dir dest_dir
    dest_dir="${EPOCH_BENCH_ROOT}/epoch_deploy/buildtools/cmake/bin"
    if [ -x "${dest_dir}/cmake" ]; then
        echo "${dest_dir}"
        return 0
    fi
    src_dir="$(epoch_find_cmake_bin_dir)" || return 1
    if [ "${src_dir}" = "${dest_dir}" ]; then
        echo "${dest_dir}"
        return 0
    fi
    mkdir -p "${dest_dir}"
    ln -sf "${src_dir}/cmake" "${dest_dir}/cmake"
    echo "${dest_dir}"
}

epoch_ensure_build_java_home() {
    local plat="${BINARYLIBS}/kernel/platform"
    local arch
    arch="$(uname -p)"
    if [ ! -d "${plat}/huaweijdk8" ] && [ -d "${plat}/openjdk8" ]; then
        ln -sf openjdk8 "${plat}/huaweijdk8" 2>/dev/null || true
    fi
    if [ -d "${plat}/huaweijdk8/${arch}/jdk" ]; then
        echo "${plat}/huaweijdk8/${arch}/jdk"
    elif [ -d "${plat}/openjdk8/${arch}/jdk" ]; then
        echo "${plat}/openjdk8/${arch}/jdk"
    elif [ -d "${plat}/openjdk8/jdk" ]; then
        echo "${plat}/openjdk8/jdk"
    else
        return 1
    fi
}

epoch_setup_build_env() {
    local gcc_ver="10.3"
    local gcc_root="${BINARYLIBS}/buildtools/gcc${gcc_ver}"
    local cmake_dir dep_zstd
    if [ ! -x "${gcc_root}/gcc/bin/gcc" ]; then
        echo "ERROR: missing ${gcc_root}/gcc/bin/gcc (check BINARYLIBS=${BINARYLIBS})" >&2
        return 1
    fi
    cmake_dir="$(epoch_ensure_cmake_installed)" || {
        echo "ERROR: cmake not found; install cmake or keep CLion bundled cmake under ~/.cache/JetBrains" >&2
        return 1
    }
    dep_zstd="${BINARYLIBS}/kernel/dependency/zstd/lib"
    export CC="${gcc_root}/gcc/bin/gcc"
    export CXX="${gcc_root}/gcc/bin/g++"
    JAVA_HOME="$(epoch_ensure_build_java_home)" || {
        echo "ERROR: JDK not found under ${BINARYLIBS}/kernel/platform" >&2
        return 1
    }
    export JAVA_HOME
    export EPOCH_CMAKE_BIN_DIR="${cmake_dir}"
    export PATH="${cmake_dir}:${gcc_root}/gcc/bin:${PATH}"
    epoch_prepend_ld_path "${dep_zstd}"
    epoch_prepend_ld_path "${gcc_root}/gcc/lib64"
    epoch_prepend_ld_path "${gcc_root}/isl/lib"
    epoch_prepend_ld_path "${gcc_root}/mpc/lib"
    epoch_prepend_ld_path "${gcc_root}/mpfr/lib"
    epoch_prepend_ld_path "${gcc_root}/gmp/lib"
    export LD_LIBRARY_PATH
    echo "build env: CC=${CC}, CMAKE=${cmake_dir}/cmake, JAVA_HOME=${JAVA_HOME}"
}

epoch_build() {
    epoch_setup_build_env || return 1
    cd "${OPENGAUSS_SRC}"
    sh build.sh "$@"
}

epoch_detect_local_node_id() {
    local ip
    for ip in $(hostname -I 2>/dev/null); do
        if [ "${ip}" = "${EPOCH_NODE0_IP}" ]; then
            echo 0
            return
        fi
        if [ "${ip}" = "${EPOCH_NODE1_IP}" ]; then
            echo 1
            return
        fi
        if [ "${ip}" = "${EPOCH_NODE2_IP}" ]; then
            echo 2
            return
        fi
    done
    echo ""
}

if [ -z "${EPOCH_LOCAL_NODE_ID:-}" ]; then
    _detected_nid="$(epoch_detect_local_node_id)"
    if [ -n "${_detected_nid}" ]; then
        export EPOCH_LOCAL_NODE_ID="${_detected_nid}"
    fi
fi

data_dir_for_node() {
    local node_id="$1"
    echo "${EPOCH_DATA_ROOT}/epoch-node${node_id}"
}

epoch_node_ip() {
    local node_id="$1"
    case "${node_id}" in
        0) echo "${EPOCH_NODE0_IP}" ;;
        1) echo "${EPOCH_NODE1_IP}" ;;
        2) echo "${EPOCH_NODE2_IP}" ;;
        *)
            echo "invalid node_id ${node_id} (expected 0..$((EPOCH_TOTAL_NODES - 1)))" >&2
            return 1
            ;;
    esac
}

epoch_remote_user() {
    local node_id="$1"
    case "${node_id}" in
        0|1) echo "${EPOCH_REMOTE_USER}" ;;
        2) echo "${EPOCH_NODE2_REMOTE_USER}" ;;
        *)
            echo "invalid node_id ${node_id}" >&2
            return 1
            ;;
    esac
}

epoch_bench_root_for_node() {
    local node_id="$1"
    case "${node_id}" in
        0) echo "${EPOCH_BENCH_ROOT}" ;;
        1) echo "${EPOCH_REMOTE_BENCH_ROOT}" ;;
        2) echo "${EPOCH_NODE2_BENCH_ROOT}" ;;
        *)
            echo "invalid node_id ${node_id}" >&2
            return 1
            ;;
    esac
}

epoch_ssh_target() {
    local node_id="$1"
    echo "$(epoch_remote_user "${node_id}")@$(epoch_node_ip "${node_id}")"
}

# 远程 SSH 执行 gs_ctl/gaussdb 时的最小环境（GAUSSHOME/lib + runtime_libs）
epoch_remote_gauss_env() {
    local node_id="$1"
    local rb gh rt
    rb="$(epoch_bench_root_for_node "${node_id}")"
    gh="${rb}/openGauss-server/mppdb_temp_install"
    rt="${rb}/epoch_deploy/runtime_libs"
    echo "export GAUSSHOME=${gh}; export PATH=\${GAUSSHOME}/bin:/usr/bin:/bin; export LD_LIBRARY_PATH=\${GAUSSHOME}/lib:${rt}"
}

# 是否在目标节点本机执行（勿仅用 hostname -I 首地址，多网卡/hostname 别名会误判）
epoch_is_local_node() {
    local node_id="$1"
    if [ -n "${EPOCH_LOCAL_NODE_ID:-}" ] && [ "${EPOCH_LOCAL_NODE_ID}" = "${node_id}" ]; then
        return 0
    fi
    local target_ip ip
    target_ip="$(epoch_node_ip "${node_id}")"
    for ip in $(hostname -I 2>/dev/null); do
        if [ "${ip}" = "${target_ip}" ]; then
            return 0
        fi
    done
    return 1
}

epoch_super_user_for_node() {
    local node_id="$1"
    if [ "${node_id}" = "2" ]; then
        echo "${EPOCH_NODE2_SUPER_USER}"
    else
        echo "${EPOCH_SUPER_USER}"
    fi
}

epoch_super_password_for_node() {
    local node_id="$1"
    if [ "${node_id}" = "2" ]; then
        echo "${EPOCH_NODE2_SUPER_PASSWORD}"
    else
        echo "${EPOCH_SUPER_PASSWORD}"
    fi
}

validate_node_id() {
    local node_id="$1"
    if ! [[ "${node_id}" =~ ^[0-9]+$ ]] || [ "${node_id}" -ge "${EPOCH_TOTAL_NODES}" ]; then
        echo "invalid NODE_ID=${node_id}, expected 0..$((EPOCH_TOTAL_NODES - 1))" >&2
        return 1
    fi
}

epoch_check_gaussdb() {
    local gaussdb="${GAUSSHOME}/bin/gaussdb"
    if [ ! -x "${gaussdb}" ]; then
        echo "ERROR: missing ${gaussdb}, run deploy_to_remote.sh from node0" >&2
        return 1
    fi
    if ! "${gaussdb}" -V >/dev/null 2>&1; then
        echo "ERROR: gaussdb cannot start (shared library missing). ldd output:" >&2
        ldd "${gaussdb}" 2>&1 | grep 'not found' >&2 || ldd "${gaussdb}" >&2
        echo "hint: on node0 run deploy_to_remote.sh ${EPOCH_LOCAL_NODE_ID:-NODE_ID} to sync epoch_deploy/runtime_libs" >&2
        echo "hint: or install libncurses5: sudo apt install libncurses5" >&2
        return 1
    fi
}

# 连库：pg_hba 对本机/三节点 IP 为 trust，无需 -U/-W（以当前 OS 用户身份连入）
#   管理库：gsql -d postgres -p 32000
#   TPCC 库：gsql -d test -p 32000
epoch_gsql() {
    gsql -d "${EPOCH_TPCC_DB}" -p "${EPOCH_DB_PORT}" "$@"
}

epoch_tpcc_gsql() {
    epoch_gsql "$@"
}

epoch_super_gsql() {
    gsql -d postgres -p "${EPOCH_DB_PORT}" "$@"
}
