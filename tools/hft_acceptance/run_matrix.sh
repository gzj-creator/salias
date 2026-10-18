#!/usr/bin/env bash
set -uo pipefail
cd "$(dirname "$0")/../.."
out=${1:-build/benchmarks/hft-acceptance/$(date +%Y%m%d-%H%M%S)}
seconds=${2:-225}
mkdir -p "$out"
bin=build/release/tools/hft_acceptance/acceptance
status=0
printf 'matrix_start=%s\nmeasurement_seconds_per_phase=%s\n' "$(date --iso-8601=seconds)" "$seconds" > "$out/run-state.txt"
start=$(date +%s.%N)
for subscribers in 1 2; do
  for payload in 64 256; do
    for pattern in steady burst; do
      phase="2p${subscribers}s-${payload}b-${pattern}"
      printf 'phase_start=%s phase=%s\n' "$(date --iso-8601=seconds)" "$phase" | tee -a "$out/run-state.txt"
      command=("$bin" --subscribers "$subscribers" --payload "$payload" --seconds "$seconds" --pattern "$pattern" --rate 1000000 --peak-rate 4000000 --burst-ms 100 --cpu-base 0)
      printf '%q ' timeout --signal=TERM --kill-after=15s "$((seconds + 50))s" "${command[@]}" > "$out/$phase.command.txt"
      printf '\n' >> "$out/$phase.command.txt"
      timeout --signal=TERM --kill-after=15s "$((seconds + 50))s" "${command[@]}" > "$out/$phase.jsonl" 2> "$out/$phase.stderr.txt"
      rc=$?
      printf 'phase_end=%s phase=%s exit=%s\n' "$(date --iso-8601=seconds)" "$phase" "$rc" | tee -a "$out/run-state.txt"
      if (( rc != 0 )); then status=1; fi
    done
  done
done
end=$(date +%s.%N)
printf 'matrix_end=%s\nstart_epoch=%s\nend_epoch=%s\nexit=%s\n' "$(date --iso-8601=seconds)" "$start" "$end" "$status" >> "$out/run-state.txt"
exit "$status"
