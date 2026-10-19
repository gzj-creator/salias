#!/usr/bin/env bash
# Build each frozen revision from source with the same compiler and harness.
set -euo pipefail
cd "$(dirname "$0")/../.."
if (( $# != 2 )); then
  printf 'usage: bash %s SOURCE_ROOT OUTPUT_DIRECTORY\n' "$0" >&2
  exit 2
fi
source=$(realpath "$1")
output=$(realpath -m "$2")
compiler=$(command -v "${CXX:-c++}")
[[ -f "$source/CMakeLists.txt" && ! -e "$output" ]] || exit 2
mkdir -p "$output"
cmake -S "$source" -B "$output/cmake" -G Ninja \
  -DCMAKE_CXX_COMPILER="$compiler" -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTING=OFF -DSALIAS_BUILD_EXAMPLES=OFF -DSALIAS_BUILD_BENCHMARKS=OFF
cmake --build "$output/cmake" --target salias -j2
"$compiler" -std=c++23 -O3 -DNDEBUG -Wall -Wextra -Wpedantic -Werror \
  -I"$source/src/salias/include" -I"$source/src" tools/hft_acceptance/acceptance.cpp \
  "$output/cmake/src/salias/libsalias.a" "$output/cmake/src/core/libsalias_core.a" \
  -pthread -lrt -o "$output/acceptance"
"$compiler" --version > "$output/compiler.txt"
cp tools/hft_acceptance/acceptance.cpp "$output/acceptance.cpp"
sha256sum "$output/acceptance" "$output/acceptance.cpp" \
  "$output/cmake/src/salias/libsalias.a" "$output/cmake/src/core/libsalias_core.a" \
  "$source/src/core/channel/shared_hybrid_mpsc.hpp" > "$output/sha256.txt"
"$output/acceptance" --self-test > "$output/self-test.json"
