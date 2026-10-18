#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
printf '%s\n' '--- timestamp ---'
date --iso-8601=seconds
printf '%s\n' '--- revision and dirty paths ---'
git rev-parse HEAD
git status --short
printf '%s\n' '--- host ---'
uname -a
cat /etc/os-release
lscpu
printf '%s\n' '--- clocksource ---'
cat /sys/devices/system/clocksource/clocksource0/current_clocksource
printf '%s\n' '--- affinity / memory / disk ---'
taskset -pc $$
free -h
df -h /dev/shm .
printf '%s\n' '--- compiler / cmake ---'
c++ --version
cmake --version
rg '^(CMAKE_BUILD_TYPE|CMAKE_CXX_COMPILER:|CMAKE_CXX_FLAGS_RELEASE|SALIAS_ENABLE_|SALIAS_BUILD_BENCHMARKS)' build/release/CMakeCache.txt
printf '%s\n' '--- exact standalone build recipe ---'
cat tools/hft_acceptance/build.sh
printf '%s\n' '--- binary and source hashes ---'
sha256sum build/release/tools/hft_acceptance/acceptance build/release/src/salias/libsalias.a build/release/src/core/libsalias_core.a tools/hft_acceptance/acceptance.cpp tools/hft_acceptance/run_matrix.sh
printf '%s\n' '--- background process snapshot (not an idle-host assertion) ---'
ps -eo pid,comm,pcpu,args --sort=-pcpu | head -20 || true
printf '%s\n' '--- /proc/stat baseline ---'
cat /proc/stat
