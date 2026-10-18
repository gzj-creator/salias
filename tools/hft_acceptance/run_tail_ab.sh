#!/usr/bin/env bash
# 串行交错运行基线与候选，保持验收负载和计时实现不变。
set -euo pipefail
cd "$(dirname "$0")/../.."
if (( $# < 3 || $# > 4 )); then
  printf 'usage: bash %s OUTPUT BASELINE CANDIDATE [SECONDS=15]\n' "$0" >&2
  exit 2
fi
out=$1
baseline=$2
candidate=$3
seconds=${4:-15}
[[ -x "$baseline" && -x "$candidate" && "$seconds" =~ ^[1-9][0-9]*$ ]] || exit 2
if [[ -e "$out/run-state.txt" ]]; then
  printf 'refusing to overwrite existing run: %s\n' "$out" >&2
  exit 2
fi
mkdir -p "$out"
sha256sum "$baseline" "$candidate" tools/hft_acceptance/acceptance.cpp > "$out/input-sha256.txt"
printf 'start=%s\nseconds_per_phase=%s\norder=baseline,candidate,candidate,baseline\n' \
  "$(date --iso-8601=seconds)" "$seconds" > "$out/run-state.txt"
versions=(baseline candidate candidate baseline)
status=0
for subscribers in 1 2; do
  for payload in 64 256; do
    for pattern in steady burst; do
      phase="2p${subscribers}s-${payload}b-${pattern}"
      for round in 0 1 2 3; do
        version=${versions[$round]}
        bin=$baseline
        [[ "$version" == candidate ]] && bin=$candidate
        dir="$out/round-$round/$version"
        mkdir -p "$dir"
        command=(timeout --signal=TERM --kill-after=10s "$((seconds + 40))s" "$bin"
          --subscribers "$subscribers" --payload "$payload" --seconds "$seconds"
          --pattern "$pattern" --rate 1000000 --peak-rate 4000000 --burst-ms 100 --cpu-base 0)
        printf '%q ' "${command[@]}" > "$dir/$phase.command.txt"
        printf '\n' >> "$dir/$phase.command.txt"
        printf 'phase_start=%s phase=%s round=%s version=%s\n' \
          "$(date --iso-8601=seconds)" "$phase" "$round" "$version" | tee -a "$out/run-state.txt"
        rc=0
        "${command[@]}" > "$dir/$phase.jsonl" 2> "$dir/$phase.stderr.txt" || rc=$?
        printf 'phase_end=%s phase=%s round=%s version=%s exit=%s\n' \
          "$(date --iso-8601=seconds)" "$phase" "$round" "$version" "$rc" | tee -a "$out/run-state.txt"
        ((rc == 0)) || status=1
      done
    done
  done
done
sha256sum --check "$out/input-sha256.txt" > "$out/input-sha256-verification.txt"
printf 'end=%s\nexit=%s\n' "$(date --iso-8601=seconds)" "$status" >> "$out/run-state.txt"
exit "$status"
