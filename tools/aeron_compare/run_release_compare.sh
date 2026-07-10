#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
AERON_TARBALL="${AERON_TARBALL:-${ROOT_DIR}/third_party/aeron-1.52.0.tar.gz}"
AERON_SRC="${AERON_SRC:-/tmp/salias-aeron-1.52.0}"
AERON_BUILD="${AERON_BUILD:-/tmp/salias-aeron-build}"
AERON_COMPARE="${AERON_BUILD}/aeron_ipc_compare"
PAYLOAD="${PAYLOAD:-64}"
POLL_LIMIT="${POLL_LIMIT:-64}"
FRAGMENT_LIMIT="${FRAGMENT_LIMIT:-64}"
ROUNDS="${ROUNDS:-5}"
WARMUP_ROUNDS="${WARMUP_ROUNDS:-1}"
SCENARIOS="${SCENARIOS:-fifo ordered}"
MPSC_MESSAGES="${MPSC_MESSAGES:-200000}"
MPMC_MESSAGES="${MPMC_MESSAGES:-200000}"
HYBRID_MESSAGES="${HYBRID_MESSAGES:-${MPSC_MESSAGES}}"
MPSC_PRODUCERS="${MPSC_PRODUCERS:-4}"
MPSC_CONSUMERS="${MPSC_CONSUMERS:-1}"
MPMC_PRODUCERS="${MPMC_PRODUCERS:-4}"
MPMC_CONSUMERS="${MPMC_CONSUMERS:-2}"
HYBRID_PRODUCERS="${HYBRID_PRODUCERS:-${MPSC_PRODUCERS}}"
HYBRID_CONSUMERS="${HYBRID_CONSUMERS:-1}"
SALIAS_PAGES="${SALIAS_PAGES:-normal huge2m huge1g}"
SALIAS_BATCH_SIZE="${SALIAS_BATCH_SIZE:-1}"
SALIAS_CAPACITY_NORMAL="${SALIAS_CAPACITY_NORMAL:-4194304}"
SALIAS_CAPACITY_HUGE2M="${SALIAS_CAPACITY_HUGE2M:-4194304}"
SALIAS_CAPACITY_HUGE1G="${SALIAS_CAPACITY_HUGE1G:-1073741824}"
PIN_CPUS="${PIN_CPUS:-0}"
CPU_BASE="${CPU_BASE:-0}"
CPU_STRIDE="${CPU_STRIDE:-1}"
AERON_DRIVER_CPU="${AERON_DRIVER_CPU:-}"
AERON_TERM_LENGTH="${AERON_TERM_LENGTH:-${SALIAS_CAPACITY_NORMAL}}"
LATENCY_SAMPLE_RATE="${LATENCY_SAMPLE_RATE:-0}"

if [[ ! -f "${AERON_TARBALL}" ]]; then
  echo "missing Aeron tarball: ${AERON_TARBALL}" >&2
  exit 2
fi

rm -rf "${AERON_SRC}" "${AERON_BUILD}"
mkdir -p "${AERON_SRC}"
tar -xzf "${AERON_TARBALL}" -C "${AERON_SRC}" --strip-components=1

# Aeron 1.52.0 要求 CMake 3.30；当前 benchmark 环境提供的是 3.28.3。
# 临时源码副本在关闭 tests/docs/archive 后可以用 3.28 正常配置。
sed -i \
  -e 's/cmake_minimum_required(VERSION 3.30 FATAL_ERROR)/cmake_minimum_required(VERSION 3.28 FATAL_ERROR)/' \
  -e 's/cmake_policy(VERSION 3.30)/cmake_policy(VERSION 3.28)/' \
  "${AERON_SRC}/CMakeLists.txt"

if ! cmake -S "${AERON_SRC}" -B "${AERON_BUILD}" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DAERON_TESTS=OFF \
    -DAERON_UNIT_TESTS=OFF \
    -DAERON_SYSTEM_TESTS=OFF \
    -DAERON_BUILD_DOCUMENTATION=OFF \
    -DBUILD_AERON_ARCHIVE_API=OFF \
    -DAERON_BUILD_SAMPLES=ON \
    -DBUILD_AERON_DRIVER=ON \
    -DAERON_INSTALL_TARGETS=OFF >/tmp/salias-aeron-cmake.log 2>&1; then
  cat /tmp/salias-aeron-cmake.log >&2
  exit 1
fi

cmake --build "${AERON_BUILD}" --target aeronmd Throughput -j"$(nproc)" >/dev/null

c++ -std=c++17 -O3 -DNDEBUG -DDISABLE_BOUNDS_CHECKS \
  "${ROOT_DIR}/tools/aeron_compare/aeron_ipc_compare.cpp" \
  -I"${AERON_SRC}/aeron-client/src/main/cpp_wrapper" \
  -I"${AERON_SRC}/aeron-client/src/main/c" \
  "${AERON_BUILD}/lib/libaeron_static.a" \
  -pthread -ldl -lrt -lm \
  -o "${AERON_COMPARE}"

cmake --build --preset release --target salias_ipc_compare >/dev/null

# 运行一个 Aeron IPC 对比场景，并在结束后清理 media driver 和共享目录。
run_aeron() {
  local scenario="$1"
  local messages="$2"
  local producers="$3"
  local consumers="$4"
  local dir="/dev/shm/salias-aeron-${scenario}-$$"
  rm -rf "${dir}"
  local driver_cmd=(
    "${AERON_BUILD}/binaries/aeronmd"
    -Daeron.dir="${dir}" \
    -Daeron.dir.delete.on.start=true \
    -Daeron.term.buffer.sparse.file=false \
    -Daeron.term.buffer.length="${AERON_TERM_LENGTH}"
  )
  local driver_cpu="${AERON_DRIVER_CPU}"
  if [[ -z "${driver_cpu}" ]]; then
    driver_cpu=$((CPU_BASE + (producers + consumers) * CPU_STRIDE))
  fi
  if [[ "${PIN_CPUS}" == "1" ]]; then
    taskset -c "${driver_cpu}" "${driver_cmd[@]}" >/tmp/salias-aeronmd-${scenario}.log 2>&1 &
  else
    "${driver_cmd[@]}" >/tmp/salias-aeronmd-${scenario}.log 2>&1 &
  fi
  local driver_pid=$!
  local ready=0
  for _ in $(seq 1 100); do
    if [[ -e "${dir}/cnc.dat" ]]; then
      ready=1
      break
    fi
    if ! kill -0 "${driver_pid}" 2>/dev/null; then
      cat /tmp/salias-aeronmd-${scenario}.log >&2
      wait "${driver_pid}" 2>/dev/null || true
      rm -rf "${dir}"
      return 1
    fi
    sleep 0.1
  done
  if [[ "${ready}" -ne 1 ]]; then
    echo "Aeron media driver did not create ${dir}/cnc.dat" >&2
    cat /tmp/salias-aeronmd-${scenario}.log >&2
    kill "${driver_pid}" 2>/dev/null || true
    wait "${driver_pid}" 2>/dev/null || true
    rm -rf "${dir}"
    return 1
  fi
  set +e
  local compare_cmd=(
    "${AERON_COMPARE}"
    --dir "${dir}" \
    --scenario "${scenario}" \
    --messages "${messages}" \
    --producers "${producers}" \
    --consumers "${consumers}" \
    --payload "${PAYLOAD}" \
    --fragment-limit "${FRAGMENT_LIMIT}" \
    --term-length "${AERON_TERM_LENGTH}" \
    --latency-sample-rate "${LATENCY_SAMPLE_RATE}"
  )
  if [[ "${PIN_CPUS}" == "1" ]]; then
    compare_cmd+=(--cpu-base "${CPU_BASE}" --cpu-stride "${CPU_STRIDE}")
  fi
  "${compare_cmd[@]}"
  local status=$?
  set -e
  kill "${driver_pid}" 2>/dev/null || true
  wait "${driver_pid}" 2>/dev/null || true
  rm -rf "${dir}"
  return "${status}"
}

# 运行一个 salias 具名共享内存 IPC benchmark 场景。
run_salias_ipc() {
  local scenario="$1"
  local messages="$2"
  local producers="$3"
  local consumers="$4"
  local page="$5"
  local capacity
  capacity="$(salias_capacity_for_page "${page}")"
  local name="compare-${scenario}-$$-${RANDOM}"
  local compare_cmd=(
    "${ROOT_DIR}/build/release/bench/salias_ipc_compare"
    --scenario "${scenario}" \
    --messages "${messages}" \
    --producers "${producers}" \
    --consumers "${consumers}" \
    --payload "${PAYLOAD}" \
    --capacity "${capacity}" \
    --batch-size "${SALIAS_BATCH_SIZE}" \
    --poll-limit "${POLL_LIMIT}" \
    --page "${page}" \
    --name "${name}" \
    --latency-sample-rate "${LATENCY_SAMPLE_RATE}"
  )
  if [[ "${PIN_CPUS}" == "1" ]]; then
    compare_cmd+=(--cpu-base "${CPU_BASE}" --cpu-stride "${CPU_STRIDE}")
  fi
  "${compare_cmd[@]}"
}

salias_capacity_for_page() {
  local page="$1"
  case "${page}" in
    normal)
      echo "${SALIAS_CAPACITY_NORMAL}"
      ;;
    huge2m)
      echo "${SALIAS_CAPACITY_HUGE2M}"
      ;;
    huge1g)
      echo "${SALIAS_CAPACITY_HUGE1G}"
      ;;
    *)
      echo "unknown salias page: ${page}" >&2
      return 2
      ;;
  esac
}

run_salias_ipc_attempt() {
  local phase="$1"
  local round="$2"
  local emit_result="$3"
  local scenario="$4"
  local messages="$5"
  local producers="$6"
  local consumers="$7"
  local page="$8"
  local error_log="/tmp/salias-ipc-${scenario}-${page}-${phase}-${round}-$$.err"
  set +e
  if [[ "${emit_result}" == "1" ]]; then
    run_salias_ipc \
      "${scenario}" "${messages}" "${producers}" "${consumers}" "${page}" \
      2>"${error_log}"
  else
    run_salias_ipc \
      "${scenario}" "${messages}" "${producers}" "${consumers}" "${page}" \
      >/dev/null 2>"${error_log}"
  fi
  local status=$?
  set -e
  if [[ "${status}" -eq 0 ]]; then
    rm -f "${error_log}"
    return 0
  fi
  if [[ "${page}" == "normal" ]]; then
    cat "${error_log}" >&2
    rm -f "${error_log}"
    return "${status}"
  fi
  rm -f "${error_log}"
  echo "SKIP library=salias-ipc scenario=${scenario} page=${page} phase=${phase}" \
    "round=${round} reason=status_${status}"
}

run_pair() {
  local scenario="$1"
  local aeron_scenario="$2"
  local messages="$3"
  local producers="$4"
  local consumers="$5"
  for round in $(seq 1 "${WARMUP_ROUNDS}"); do
    echo "# warmup round=${round} scenario=${scenario}"
    for page in ${SALIAS_PAGES}; do
      run_salias_ipc_attempt \
        warmup "${round}" 0 "${scenario}" "${messages}" "${producers}" "${consumers}" "${page}"
    done
    run_aeron "${aeron_scenario}" "${messages}" "${producers}" "${consumers}" >/dev/null
  done
  for round in $(seq 1 "${ROUNDS}"); do
    echo "# measured round=${round} scenario=${scenario}"
    for page in ${SALIAS_PAGES}; do
      run_salias_ipc_attempt \
        measured "${round}" 1 "${scenario}" "${messages}" "${producers}" "${consumers}" "${page}"
    done
    run_aeron "${aeron_scenario}" "${messages}" "${producers}" "${consumers}"
  done
}

aeron_scenario_for() {
  local producers="$1"
  local consumers="$2"
  if [[ "${producers}" -gt 1 && "${consumers}" -gt 1 ]]; then
    echo mpmc
  elif [[ "${producers}" -gt 1 ]]; then
    echo mpsc
  elif [[ "${consumers}" -gt 1 ]]; then
    echo spmc
  else
    echo spsc
  fi
}

for page in ${SALIAS_PAGES}; do
  salias_capacity_for_page "${page}" >/dev/null
done

echo "# salias named IPC vs Aeron C++ IPC release comparison"
echo "# payload=${PAYLOAD}B; salias_batch_size=${SALIAS_BATCH_SIZE};" \
  "poll_limit=${POLL_LIMIT}; fragment_limit=${FRAGMENT_LIMIT}; warmup=${WARMUP_ROUNDS};" \
  "rounds=${ROUNDS}; latency_sample_rate=${LATENCY_SAMPLE_RATE}"
echo "# salias_pages=${SALIAS_PAGES}; capacities normal=${SALIAS_CAPACITY_NORMAL}" \
  "huge2m=${SALIAS_CAPACITY_HUGE2M} huge1g=${SALIAS_CAPACITY_HUGE1G}"
echo "# pin_cpus=${PIN_CPUS}; cpu_base=${CPU_BASE}; cpu_stride=${CPU_STRIDE};" \
  "aeron_driver_cpu=${AERON_DRIVER_CPU:-auto}; aeron_term_length=${AERON_TERM_LENGTH}"
echo "# scenarios=${SCENARIOS}; FIFO=${HYBRID_PRODUCERS}P/${HYBRID_CONSUMERS}C;" \
  "ORDERED=${MPSC_PRODUCERS}P/${MPSC_CONSUMERS}C"

for scenario in ${SCENARIOS}; do
  case "${scenario}" in
    fifo)
      run_pair fifo "$(aeron_scenario_for "${HYBRID_PRODUCERS}" "${HYBRID_CONSUMERS}")" \
        "${HYBRID_MESSAGES}" "${HYBRID_PRODUCERS}" "${HYBRID_CONSUMERS}"
      ;;
    ordered)
      run_pair ordered "$(aeron_scenario_for "${MPSC_PRODUCERS}" "${MPSC_CONSUMERS}")" \
        "${MPSC_MESSAGES}" "${MPSC_PRODUCERS}" "${MPSC_CONSUMERS}"
      ;;
    *)
      echo "unknown scenario: ${scenario}" >&2
      exit 2
      ;;
  esac
done
