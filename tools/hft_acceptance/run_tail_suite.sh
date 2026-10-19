#!/usr/bin/env bash
# Formal sequential ABBA repetitions; never rebuild or change the frozen binaries here.
set -euo pipefail
cd "$(dirname "$0")/../.."
if (( $# != 3 )); then
  printf 'usage: bash %s OUTPUT BASELINE CANDIDATE\n' "$0" >&2
  exit 2
fi
out=$1
baseline=$2
candidate=$3
[[ -x "$baseline" && -x "$candidate" && ! -e "$out" ]] || exit 2
mkdir -p "$out"
sha256sum "$baseline" "$candidate" tools/hft_acceptance/{acceptance.cpp,run_tail_ab.sh,run_tail_suite.sh,run_matrix.sh,build_tail_variant.sh,summarize.py,summarize_tail_ab.py} > "$out/frozen-sha256.txt"
printf 'start=%s\nformal_groups=3\nseconds_per_phase=30\n' "$(date --iso-8601=seconds)" > "$out/suite-state.txt"
status=0
trap 'printf "interrupted=%s\n" "$(date --iso-8601=seconds)" >> "$out/suite-state.txt"; exit 130' INT TERM
# Warm every configuration/version once. Preserve, but do not pool, warm-up output.
for version in baseline candidate; do
  bin=$baseline
  [[ "$version" == candidate ]] && bin=$candidate
  mkdir -p "$out/warmup/$version"
  for subscribers in 1 2; do
    for payload in 64 256; do
      for pattern in steady burst; do
        name="2p${subscribers}s-${payload}b-${pattern}"
        timeout --signal=TERM --kill-after=10s 42s "$bin" --subscribers "$subscribers" --payload "$payload" --seconds 2 --pattern "$pattern" > "$out/warmup/$version/$name.jsonl" 2> "$out/warmup/$version/$name.stderr.txt" || status=1
      done
    done
  done
done
for group in 1 2 3; do
  cat /proc/stat > "$out/group-$group.proc-stat.before"
  bash tools/hft_acceptance/run_tail_ab.sh "$out/group-$group" "$baseline" "$candidate" 30 || status=1
  cat /proc/stat > "$out/group-$group.proc-stat.after"
  python3 tools/hft_acceptance/summarize_tail_ab.py "$out/group-$group" > "$out/group-$group.summary.stdout" 2> "$out/group-$group.summary.stderr" || status=1
done
sha256sum --check "$out/frozen-sha256.txt" > "$out/frozen-sha256-verification.txt" || status=1
python3 tools/hft_acceptance/summarize_tail_ab.py "$out" --suite > "$out/relative-acceptance.stdout" 2> "$out/relative-acceptance.stderr" || status=1
printf 'end=%s\nexecution_exit=%s\n' "$(date --iso-8601=seconds)" "$status" >> "$out/suite-state.txt"
# Execution success is not the relative-performance or 50us verdict.
exit "$status"
