#!/usr/bin/env bash
# 在后台采集系统状态；stdout 只输出监控 PID，诊断信息写入 monitor.stderr。
set -euo pipefail

usage() {
    cat >&2 <<'EOF'
Usage: capture_runtime_state.sh OUTPUT_DIR [--interval SECONDS] [--cpus LIST]

The monitor writes mpstat.log, pidstat.log, interrupts.log, loadavg.log,
timeline.log, monitor-meta.json and monitor.pid under OUTPUT_DIR.
Stop it with: kill -TERM PID; wait PID
EOF
}

if [[ $# -lt 1 ]]; then
    usage
    exit 2
fi

OUTPUT=$1
shift
INTERVAL=1
CPU_LIST=1,2,3
while [[ $# -gt 0 ]]; do
    case "$1" in
        --interval)
            [[ $# -ge 2 ]] || { usage; exit 2; }
            INTERVAL=$2
            shift 2
            ;;
        --cpus)
            [[ $# -ge 2 ]] || { usage; exit 2; }
            CPU_LIST=$2
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage
            exit 2
            ;;
    esac
done

command -v mpstat >/dev/null || { echo "mpstat is required (sysstat)" >&2; exit 1; }
command -v pidstat >/dev/null || { echo "pidstat is required (sysstat)" >&2; exit 1; }

mkdir -p "$OUTPUT"
OUTPUT=$(realpath "$OUTPUT")

# Detach with all stdio redirected before returning the PID. This is required when
# callers use command substitution, which otherwise waits for inherited stdout.
log_file="/tmp/salias-runtime-monitor.$$"
setsid bash -c '
    set -euo pipefail
    output=$1
    interval=$2
    cpus=$3
    pid_file="$output/monitor.pid"
    timeline="$output/timeline.log"
    error_log="$output/monitor.stderr"

    # The mpstat JSON writer does not close its root object on TERM. The text writer
    # is line-oriented and flushes every sample, so it is safe for long-running capture.
    LC_ALL=C mpstat -P "$cpus" -u -I SUM "$interval" \
        >"$output/mpstat.log" 2>>"$error_log" &
    mpstat_pid=$!
    LC_ALL=C pidstat -u -p ALL "$interval" \
        >"$output/pidstat.log" 2>>"$error_log" &
    pidstat_pid=$!

    {
        echo "timestamp_ns monotonic_ns load1 load5 load15 running_tasks process_count"
        while [[ ! -s "$output/mpstat.log" ]]; do
            sleep 0.02
        done
        while true; do
            wall_ns=$(date +%s%N)
            read -r uptime _rest < /proc/uptime
            mono_ns=$(awk -v value="$uptime" "BEGIN {printf \"%.0f\", value * 1000000000}")
            read -r load1 load5 load15 tasks _rest < /proc/loadavg
            running=${tasks%%/*}
            total=${tasks##*/}
            printf "%s %s %s %s %s %s %s\\n" \
                "$wall_ns" "$mono_ns" "$load1" "$load5" "$load15" "$running" "$total"
            cp /proc/interrupts "$output/interrupts.log.tmp"
            mv "$output/interrupts.log.tmp" "$output/interrupts.log"
            cp /proc/loadavg "$output/loadavg.txt.tmp"
            mv "$output/loadavg.txt.tmp" "$output/loadavg.txt"
            sleep "$interval"
        done
    } >>"$timeline" 2>>"$error_log" &
    timeline_pid=$!

    cleanup() {
        trap - EXIT INT TERM HUP
        kill -TERM "$mpstat_pid" "$pidstat_pid" "$timeline_pid" 2>/dev/null || true
        for _ in {1..50}; do
            alive=0
            for child in "$mpstat_pid" "$pidstat_pid" "$timeline_pid"; do
                kill -0 "$child" 2>/dev/null && alive=1
            done
            [[ "$alive" -eq 0 ]] && break
            sleep 0.02
        done
        kill -KILL "$mpstat_pid" "$pidstat_pid" "$timeline_pid" 2>/dev/null || true
        wait "$mpstat_pid" 2>/dev/null || true
        wait "$pidstat_pid" 2>/dev/null || true
        wait "$timeline_pid" 2>/dev/null || true
        rm -f "$pid_file"
        exit 0
    }
    trap cleanup EXIT INT TERM HUP

    cat >"$output/monitor-meta.json" <<EOF
{
  "pid": $$,
  "started_at_unix_ns": $(date +%s%N),
  "started_at": "$(date -Iseconds)",
  "interval_seconds": $interval,
  "cpu_list": "$cpus",
  "pidstat_scope": "all",
  "pidstat_signals": false,
  "tools": {
    "mpstat": "$(mpstat -V 2>&1 | head -1)",
    "pidstat": "$(pidstat -V 2>&1 | head -1)"
  }
}
EOF
    echo $$ >"$pid_file"
    echo $$
    wait
' salias-runtime-monitor "$OUTPUT" "$INTERVAL" "$CPU_LIST" \
    </dev/null >"$log_file" 2>&1 &
launcher_pid=$!

for _ in {1..250}; do
    if [[ -s "$OUTPUT/monitor.pid" ]]; then
        monitor_pid=$(<"$OUTPUT/monitor.pid")
        if kill -0 "$monitor_pid" 2>/dev/null; then
            echo "$monitor_pid"
            exit 0
        fi
    fi
    if ! kill -0 "$launcher_pid" 2>/dev/null; then
        cat "$log_file" >&2 || true
        rm -f "$log_file"
        echo "Runtime monitor exited before publishing its PID" >&2
        exit 1
    fi
    sleep 0.02
done

cat "$log_file" >&2 || true
rm -f "$log_file"
echo "Timed out waiting for runtime monitor PID" >&2
exit 1
