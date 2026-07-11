#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SALIAS_PUBLISHER="${SALIAS_PUBLISHER:-${ROOT_DIR}/build/release/tools/final_bench/salias_final_publisher}"
SALIAS_SUBSCRIBER="${SALIAS_SUBSCRIBER:-${ROOT_DIR}/build/release/tools/final_bench/salias_final_subscriber}"
AERON_PUBLISHER="${AERON_PUBLISHER:-${ROOT_DIR}/build/release/tools/final_bench/aeron_final_publisher}"
AERON_SUBSCRIBER="${AERON_SUBSCRIBER:-${ROOT_DIR}/build/release/tools/final_bench/aeron_final_subscriber}"
AERON_DRIVER="${AERON_DRIVER:-/tmp/salias-aeron-build/binaries/aeronmd}"
MESSAGES="${MESSAGES:-2000000}"
WARMUPS="${WARMUPS:-3}"
ROUNDS="${ROUNDS:-20}"
CAPACITIES="${CAPACITIES:-1048576 4194304 67108864}"
OUTPUT="${OUTPUT:-${ROOT_DIR}/doc/benchmarks/final-performance-raw.log}"

mkdir -p "$(dirname "${OUTPUT}")"
: >"${OUTPUT}"

wait_files() {
  local dir="$1" pattern="$2" count="$3" deadline=$((SECONDS + 60))
  while (( $(find "$dir" -maxdepth 1 -name "$pattern" | wc -l) < count )); do
    (( SECONDS < deadline )) || { echo "timeout waiting for $pattern" >&2; return 1; }
    sleep 0.01
  done
}

aggregate_run() {
  local library="$1" mode="$2" topology="$3" capacity="$4" batch="$5" phase="$6" round="$7" dir="$8" producers="$9" consumers="${10}"
  python3 - "$library" "$mode" "$topology" "$capacity" "$batch" "$phase" "$round" "$dir" "$producers" "$consumers" <<'PY'
import glob,re,sys
library,mode,topology,capacity,batch,phase,round_no,directory,producers,consumers=sys.argv[1:]
rows=[]
for path in glob.glob(directory+'/*.result'):
    text=open(path).read(); rows.append(dict(re.findall(r'(\w+)=([^ ]+)',text)))
starts=[int(r['start_ns']) for r in rows]; ends=[int(r['end_ns']) for r in rows]
seconds=(max(ends)-min(starts))/1e9
pub=[r for r in rows if r['role']=='publisher']; sub=[r for r in rows if r['role']=='subscriber']
published=sum(int(r['published']) for r in pub); delivered=sum(int(r['consumed']) for r in sub)
attempts=sum(int(r['attempts']) for r in pub); bp=sum(int(r['backpressured']) for r in pub)
invalid=sum(int(r.get('invalid','0')) for r in sub)
print(f"RUN library={library} mode={mode} topology={topology} capacity={capacity} batch={batch} phase={phase} round={round_no} producers={producers} consumers={consumers} published={published} delivered={delivered} seconds={seconds:.9f} publish_msg_per_sec={published/seconds:.6f} delivery_msg_per_sec={delivered/seconds:.6f} attempts={attempts} backpressured={bp} backpressure_rate={(bp/attempts if attempts else 0):.9f} invalid={invalid}")
PY
}

run_case() {
  local library="$1" mode="$2" producers="$3" consumers="$4" capacity="$5" batch="$6" phase="$7" round="$8"
  local topology="${producers}P${consumers}S"
  local dir="/tmp/salias-final-${library}-${mode}-${topology}-${capacity}-${batch}-$$-${RANDOM}"
  local name="final-${mode}-${topology}-$$-${RANDOM}"
  mkdir -p "$dir"
  local driver_pid=""; local -a pids=()
  cleanup_case() {
    for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done
    if [[ -n "$driver_pid" ]]; then kill "$driver_pid" 2>/dev/null || true; fi
    rm -rf "$dir"
  }

  if [[ "$library" == aeron ]]; then
    name="/dev/shm/salias-final-aeron-$$-${RANDOM}"
    rm -rf "$name"
    local driver_cpu=3
    (( consumers == 2 )) && driver_cpu=0
    taskset -c "$driver_cpu" "$AERON_DRIVER" -Daeron.dir="$name" -Daeron.dir.delete.on.start=true \
      -Daeron.term.buffer.sparse.file=false -Daeron.term.buffer.length="$capacity" >"$dir/driver.log" 2>&1 &
    driver_pid=$!
    local deadline=$((SECONDS + 20))
    while [[ ! -f "$name/cnc.dat" ]]; do (( SECONDS < deadline )) || return 1; sleep 0.02; done
  fi

  for ((i=0;i<consumers;i++)); do
    local cpu=$i
    if [[ "$library" == salias ]]; then
      local create=(); (( i == 0 )) && create=(--create)
      "$SALIAS_SUBSCRIBER" --name "$name" --mode "$mode" --coord-dir "$dir" --index "$i" \
        --producers "$producers" --consumers "$consumers" --messages "$MESSAGES" --capacity "$capacity" \
        --batch-size "$batch" --poll-limit 64 --cpu "$cpu" "${create[@]}" >"$dir/subscriber-$i.log" 2>&1 &
      pids+=("$!")
      if (( i == 0 )); then wait_files "$dir" 'subscriber-0.ready' 1; fi
    else
      "$AERON_SUBSCRIBER" --name "$name" --mode fifo --coord-dir "$dir" --index "$i" \
        --producers "$producers" --consumers "$consumers" --messages "$MESSAGES" --capacity "$capacity" \
        --batch-size 1 --poll-limit 64 --cpu "$cpu" >"$dir/subscriber-$i.log" 2>&1 &
      pids+=("$!")
    fi
  done

  for ((i=0;i<producers;i++)); do
    local cpu=$((consumers+i)); (( cpu > 3 )) && cpu=$((cpu%4))
    local binary="$SALIAS_PUBLISHER"; [[ "$library" == aeron ]] && binary="$AERON_PUBLISHER"
    "$binary" --name "$name" --mode "$mode" --coord-dir "$dir" --index "$i" \
      --producers "$producers" --consumers "$consumers" --messages "$MESSAGES" --capacity "$capacity" \
      --batch-size "$batch" --poll-limit 64 --cpu "$cpu" >"$dir/publisher-$i.log" 2>&1 &
    pids+=("$!")
  done

  wait_files "$dir" 'subscriber-*.ready' "$consumers"
  wait_files "$dir" 'publisher-*.ready' "$producers"
  touch "$dir/start"
  local status=0
  for pid in "${pids[@]}"; do wait "$pid" || status=1; done
  (( status == 0 )) || { cat "$dir"/*.log >&2; return 1; }
  local line
  line="$(aggregate_run "$library" "$mode" "$topology" "$capacity" "$batch" "$phase" "$round" "$dir" "$producers" "$consumers")"
  [[ "$phase" == measured ]] && echo "$line" | tee -a "$OUTPUT" || true
  cleanup_case
  if [[ "$library" == aeron ]]; then rm -rf "$name"; fi
}

run_group() {
  local library="$1" mode="$2" producers="$3" consumers="$4" batch="$5"
  local -a caps=($CAPACITIES)
  for ((warm=1;warm<=WARMUPS;warm++)); do
    for cap in "${caps[@]}"; do run_case "$library" "$mode" "$producers" "$consumers" "$cap" "$batch" warmup "$warm"; done
  done
  for ((round=1;round<=ROUNDS;round++)); do
    local shift=$((round % ${#caps[@]}))
    for ((j=0;j<${#caps[@]};j++)); do
      local cap="${caps[$(((j+shift)%${#caps[@]}))]}"
      run_case "$library" "$mode" "$producers" "$consumers" "$cap" "$batch" measured "$round"
    done
  done
}

for topology in '2 1' '2 2'; do
  read -r producers consumers <<<"$topology"
  run_group salias fifo "$producers" "$consumers" 1
  run_group aeron fifo "$producers" "$consumers" 1
  run_group salias ordered "$producers" "$consumers" 1
  run_group salias ordered "$producers" "$consumers" 8
done

python3 "${ROOT_DIR}/tools/final_bench/summarize.py" "$OUTPUT"
