#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
AERON_TARBALL="${AERON_TARBALL:-${ROOT_DIR}/third_party/aeron-1.52.0.tar.gz}"
AERON_SRC="${AERON_SRC:-/tmp/salias-aeron-1.52.0}"
AERON_BUILD="${AERON_BUILD:-/tmp/salias-aeron-build}"
AERON_COMPARE="${AERON_BUILD}/aeron_ipc_compare"

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

cmake --build --preset release --target salias_bench_compare >/dev/null

# 运行一个 Aeron IPC 对比场景，并在结束后清理 media driver 和共享目录。
run_aeron() {
  local scenario="$1"
  local messages="$2"
  local producers="$3"
  local consumers="$4"
  local dir="/dev/shm/salias-aeron-${scenario}-$$"
  rm -rf "${dir}"
  "${AERON_BUILD}/binaries/aeronmd" \
    -Daeron.dir="${dir}" \
    -Daeron.dir.delete.on.start=true \
    -Daeron.term.buffer.sparse.file=false \
    >/tmp/salias-aeronmd-${scenario}.log 2>&1 &
  local driver_pid=$!
  sleep 1
  set +e
  "${AERON_COMPARE}" \
    --dir "${dir}" \
    --scenario "${scenario}" \
    --messages "${messages}" \
    --producers "${producers}" \
    --consumers "${consumers}" \
    --payload 64
  local status=$?
  set -e
  kill "${driver_pid}" 2>/dev/null || true
  wait "${driver_pid}" 2>/dev/null || true
  rm -rf "${dir}"
  return "${status}"
}

# 运行一个 salias release benchmark 场景。
run_salias() {
  local scenario="$1"
  local messages="$2"
  local producers="$3"
  local consumers="$4"
  "${ROOT_DIR}/build/release/bench/salias_bench_compare" \
    --scenario "${scenario}" \
    --messages "${messages}" \
    --producers "${producers}" \
    --consumers "${consumers}" \
    --payload 64
}

echo "# salias vs Aeron C++ IPC release comparison"
echo "# payload=64B; SPSC=1P/1C; SPMC=1P/2C; MPMC=4P/2C all-to-all pub/sub"

run_salias spsc 1000000 1 1
run_aeron spsc 1000000 1 1
run_salias spmc 500000 1 2
run_aeron spmc 500000 1 2
run_salias mpmc 200000 4 2
run_aeron mpmc 200000 4 2
