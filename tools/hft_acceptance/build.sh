#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
cmake --build build/release --target salias -j2
mkdir -p build/release/tools/hft_acceptance
c++ -std=c++23 -O3 -DNDEBUG -Wall -Wextra -Wpedantic -Werror \
  -Isrc/salias/include -Isrc tools/hft_acceptance/acceptance.cpp \
  build/release/src/salias/libsalias.a build/release/src/core/libsalias_core.a \
  -pthread -lrt -o build/release/tools/hft_acceptance/acceptance
