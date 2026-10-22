#!/usr/bin/env python3
"""Bounded local 1P1S/1P2S comparison, using the unmodified release binary."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import signal
from statistics import median
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools/aeron_compare"))
from run_latency_compare import available_kib, check_resource_limits, resource_counters, stop


def fields(line):
    return dict(item.split("=", 1) for item in line.split()[1:])


def interrupted(signum, frame):
    raise KeyboardInterrupt(f"signal {signum}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    check_resource_limits()
    args.output.mkdir(parents=True, exist_ok=False)
    binary = ROOT / "build/release/bench/salias_ipc_compare"
    metadata = {
        "date": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT).decode().strip(),
        "tag": subprocess.check_output(["git", "describe", "--exact-match", "--tags"], cwd=ROOT).decode().strip(),
        "binary": str(binary), "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "kernel": platform.release(),
        "cpu_affinity": sorted(os.sched_getaffinity(0)),
        "cpu_roles": "publisher CPU3; subscriber0 CPU1; subscriber1 CPU2 when present; CPU0 unused",
        "limits": resource_counters(),
        "semantics": "Reliable FIFO broadcast: every subscriber receives every published message",
        "latency": "2 seconds paced at 1M messages/s; 1/16 sampling; 1 warmup + 5 measured rounds",
        "throughput": "10M messages, unpaced, no latency sampling; 1 warmup + 5 measured rounds",
        "payload_limit": "logical size 64/256B; latency writes timestamp+marker (16B), throughput marker (8B); not full payload verification",
    }
    (args.output / "environment.json").write_text(json.dumps(metadata, indent=2) + "\n")
    rows = []
    number = 0
    for kind in ("latency", "throughput"):
        cells = [(consumers, payload, origin)
                 for payload in (64, 256)
                 for origin in (("attempt", "planned") if kind == "latency" else ("claim",))
                 for consumers in (1, 2)]
        for round_number in range(6):
            offset = round_number % len(cells)
            for consumers, payload, origin in cells[offset:] + cells[:offset]:
                number += 1
                if available_kib() < 1024 * 1024:
                    raise RuntimeError("MemAvailable below 1GiB before case")
                name = f"spmc-{os.getpid()}-{number}"
                prefix = args.output / f"{number:03d}-{kind}-{consumers}s-{payload}b-{origin}"
                messages = 2_000_000 if kind == "latency" else 10_000_000
                samples = (messages + 15) // 16 if kind == "latency" else 0
                command = [str(binary), "--scenario", "fifo", "--name", name,
                           "--page", "normal", "--capacity", "1048576",
                           "--batch-size", "1", "--poll-limit", "64",
                           "--producers", "1", "--consumers", str(consumers),
                           "--payload", str(payload), "--messages", str(messages),
                           "--cpu-base", "1", "--cpu-stride", "2" if consumers == 1 else "1",
                           "--latency-origin", origin, "--rate", "1000000" if kind == "latency" else "0",
                           "--latency-sample-rate", "16" if kind == "latency" else "0"]
                process = None
                before = resource_counters()
                lowest = available_kib()
                try:
                    with prefix.with_suffix(".stdout").open("w") as out, prefix.with_suffix(".stderr").open("w") as err:
                        process = subprocess.Popen(command, stdout=out, stderr=err, start_new_session=True)
                        deadline = time.monotonic() + 30
                        while process.poll() is None:
                            lowest = min(lowest, available_kib())
                            if lowest < 600 * 1024 or time.monotonic() > deadline:
                                raise RuntimeError(f"Resource floor or timeout: {prefix}")
                            time.sleep(0.05)
                        if process.returncode:
                            raise RuntimeError(f"Worker failed: {prefix} exit={process.returncode}")
                    stdout = prefix.with_suffix(".stdout").read_text()
                    result = fields(next(line for line in stdout.splitlines() if line.startswith("RESULT ")))
                    endpoints = [fields(line) for line in stdout.splitlines() if line.startswith("LATENCY ")]
                    assert int(result["published"]) == messages
                    assert int(result["delivered"]) == messages * consumers
                    assert len(endpoints) == consumers
                    assert sorted(int(e["consumer"]) for e in endpoints) == list(range(consumers))
                    assert all(int(e["samples"]) == samples for e in endpoints)
                    assert int(result["latency_samples"]) == samples * consumers
                    row = {"kind": kind, "consumers": consumers, "payload": payload, "origin": origin,
                           "round": round_number, "phase": "warmup" if round_number == 0 else "measured",
                           "command": command, "result": result, "consumers_detail": endpoints,
                           "worst_subscriber_ns": {p: max(int(e[p + "_ns"]) for e in endpoints)
                                                   for p in ("p50", "p99", "p999", "max")},
                           "resources_before": before, "resources_after": resource_counters(),
                           "min_mem_available_kib": lowest,
                           "stdout_sha256": hashlib.sha256(stdout.encode()).hexdigest()}
                    rows.append(row)
                    with (args.output / "results.jsonl").open("a") as stream:
                        stream.write(json.dumps(row) + "\n")
                    print(f"{number:02d} {row['phase']} {kind} 1P{consumers}S {payload}B {origin} "
                          f"pub={float(result['publish_msg_per_sec'])/1e6:.3f}M/s "
                          f"worst_p999={row['worst_subscriber_ns']['p999']/1000:.3f}us", flush=True)
                finally:
                    stop(process)
                    for path in Path("/dev/shm").glob(f"salias-{name}-*"):
                        path.unlink(missing_ok=True)
                time.sleep(0.25)
    groups = []
    measured = [r for r in rows if r["phase"] == "measured"]
    for kind in ("latency", "throughput"):
        keys = sorted({(r["consumers"], r["payload"], r["origin"]) for r in measured if r["kind"] == kind})
        for consumers, payload, origin in keys:
            cell = [r for r in measured if (r["kind"], r["consumers"], r["payload"], r["origin"]) == (kind, consumers, payload, origin)]
            assert len(cell) == 5
            summary = {"kind": kind, "consumers": consumers, "payload": payload, "origin": origin, "rounds": 5,
                       "publish_mmsg_s": median(float(r["result"]["publish_msg_per_sec"]) / 1e6 for r in cell),
                       "delivery_mmsg_s": median(float(r["result"]["delivery_msg_per_sec"]) / 1e6 for r in cell),
                       "duration_s_range": [min(float(r["result"]["seconds"]) for r in cell), max(float(r["result"]["seconds"]) for r in cell)],
                       "latency_ns": {p: {"median": median(r["worst_subscriber_ns"][p] for r in cell),
                                          "min": min(r["worst_subscriber_ns"][p] for r in cell),
                                          "max": max(r["worst_subscriber_ns"][p] for r in cell)}
                                      for p in ("p50", "p99", "p999", "max")},
                       "per_subscriber_median_ns": [{p: median(int(r["consumers_detail"][i][p + "_ns"]) for r in cell)
                                                      for p in ("p50", "p99", "p999", "max")}
                                                     for i in range(consumers)]}
            groups.append(summary)
    output = {"environment": metadata, "aggregation": "median of per-round WORST-subscriber percentile; not pooled quantiles",
              "measured_runs": len(measured), "warmup_runs": len(rows) - len(measured),
              "minimum_mem_available_kib": min(r["min_mem_available_kib"] for r in rows), "groups": groups}
    (args.output / "summary.json").write_text(json.dumps(output, indent=2) + "\n")
    print("SUMMARY " + json.dumps(output), flush=True)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGHUP, interrupted)
    main()
