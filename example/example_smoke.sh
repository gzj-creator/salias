#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: example_smoke.sh MPSC_SUB MPSC_PUB" >&2
  exit 2
fi

MPSC_SUBSCRIBER="$1"
MPSC_PUBLISHER="$2"

TMP_DIR="$(mktemp -d)"
PIDS=()

cleanup() {
  for pid in "${PIDS[@]:-}"; do
    kill "${pid}" 2>/dev/null || true
  done
  rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

wait_for_log() {
  local file="$1"
  local pattern="$2"
  local pid="$3"
  for _ in $(seq 1 500); do
    if grep -q "${pattern}" "${file}" 2>/dev/null; then
      return 0
    fi
    if ! kill -0 "${pid}" 2>/dev/null; then
      cat "${file}" >&2 || true
      return 1
    fi
    sleep 0.01
  done
  cat "${file}" >&2 || true
  return 1
}

run_mpsc() {
  local name="salias-example-mpsc-$$"
  local log="${TMP_DIR}/mpsc-subscriber.log"
  "${MPSC_SUBSCRIBER}" --name "${name}" --count 3 >"${log}" 2>&1 &
  local sub_pid=$!
  PIDS+=("${sub_pid}")
  wait_for_log "${log}" "ready" "${sub_pid}"
  "${MPSC_PUBLISHER}" --name "${name}" --count 3 --message mpsc-smoke
  wait "${sub_pid}"
  grep -q "mpsc-smoke #2" "${log}"
}

run_mpsc
