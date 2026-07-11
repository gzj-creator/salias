#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
AERON_SRC="${AERON_SRC:?set AERON_SRC to the Aeron source directory}"
AERON_BUILD="${AERON_BUILD:?set AERON_BUILD to the Aeron build directory}"
OUTPUT_DIR="${OUTPUT_DIR:-${ROOT_DIR}/build/release/tools/final_bench}"
mkdir -p "${OUTPUT_DIR}"

for role in publisher subscriber; do
  c++ -std=c++20 -O3 -DNDEBUG -DDISABLE_BOUNDS_CHECKS \
    "${ROOT_DIR}/tools/final_bench/aeron_${role}.cpp" \
    -I"${ROOT_DIR}" \
    -I"${AERON_SRC}/aeron-client/src/main/cpp_wrapper" \
    -I"${AERON_SRC}/aeron-client/src/main/c" \
    "${AERON_BUILD}/lib/libaeron_static.a" \
    -pthread -ldl -lrt -lm \
    -o "${OUTPUT_DIR}/aeron_final_${role}"
done
