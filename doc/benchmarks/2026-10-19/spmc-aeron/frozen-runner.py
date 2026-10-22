#!/usr/bin/env python3
"""Paired 1P2S broadcast comparison with a separate CPU for the Aeron driver."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import signal
from statistics import median
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools/aeron_compare"))
from run_latency_compare import available_kib, cgroup_directory, fields, resource_counters, stop

SALIAS = ROOT / "build/release/bench/salias_ipc_compare"
AERON = ROOT / "build/release-check-v3.0.0-20261019/aeron_ipc_compare"
DRIVER_BUILD = ROOT / "build/benchmarks/aeron-latency-20261018/deps/aeron-build"
DRIVER = DRIVER_BUILD / "binaries/aeronmd"


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def interrupted(signum, frame):
    raise KeyboardInterrupt(f"Interrupted by signal {signum}")


def verify_limits():
    directory = cgroup_directory()
    limit = (directory / "memory.max").read_text().strip()
    if (os.environ.get("SALIAS_RESOURCE_CAPPED") != "1" or
            os.sched_getaffinity(0) != {0, 1, 2, 3} or limit == "max" or
            int(limit) > 704 * 1024 * 1024 or
            (directory / "memory.swap.max").read_text().strip() != "0"):
        raise RuntimeError("Require CPUs 0-3, MemoryMax<=704M, zero swap and explicit capped-run flag")


def run_case(output, number, kind, payload, origin, engine, round_number):
    if available_kib() < 1024 * 1024:
        raise RuntimeError("Less than 1GiB MemAvailable before case")
    name = f"spmc-ab-{os.getpid()}-{number}"
    directory = Path("/dev/shm") / name
    prefix = output / f"{number:03d}-{engine}-{kind}-{payload}b-{origin}-r{round_number}"
    messages = 2_000_000 if kind == "latency" else 10_000_000
    samples = (messages + 15) // 16 if kind == "latency" else 0
    common = ["--messages", str(messages), "--producers", "1", "--consumers", "2",
              "--payload", str(payload), "--cpu-base", "1", "--cpu-stride", "1",
              "--latency-origin", origin, "--rate", "1000000" if kind == "latency" else "0",
              "--latency-sample-rate", "16" if kind == "latency" else "0"]
    driver = worker = driver_log = None
    driver_command = None
    lowest = available_kib()
    before = resource_counters()
    started = time.monotonic()
    try:
        if engine == "aeron":
            driver_command = ["taskset", "-c", "0", str(DRIVER), f"-Daeron.dir={directory}",
                              "-Daeron.dir.delete.on.start=true", "-Daeron.threading.mode=SHARED",
                              "-Daeron.shared.idle.strategy=backoff", "-Daeron.term.buffer.sparse.file=false",
                              "-Daeron.term.buffer.length=1048576", "-Daeron.ipc.term.buffer.length=1048576"]
            driver_log = prefix.with_suffix(".driver.log").open("w")
            driver = subprocess.Popen(driver_command, stdout=driver_log, stderr=subprocess.STDOUT,
                                      start_new_session=True)
            deadline = time.monotonic() + 10
            while not (directory / "cnc.dat").exists():
                lowest = min(lowest, available_kib())
                if driver.poll() is not None or time.monotonic() > deadline or lowest < 600 * 1024:
                    raise RuntimeError(f"Driver startup failed: {prefix}")
                time.sleep(0.02)
            command = [str(AERON), "--dir", str(directory), "--scenario", "spmc",
                       "--term-length", "1048576", "--fragment-limit", "64", *common]
        else:
            command = [str(SALIAS), "--scenario", "fifo", "--name", name, "--page", "normal",
                       "--capacity", "1048576", "--batch-size", "1", "--poll-limit", "64", *common]
        with prefix.with_suffix(".stdout").open("w") as out, prefix.with_suffix(".stderr").open("w") as err:
            worker = subprocess.Popen(command, stdout=out, stderr=err, start_new_session=True)
            deadline = time.monotonic() + 30
            while worker.poll() is None:
                lowest = min(lowest, available_kib())
                if lowest < 600 * 1024 or time.monotonic() > deadline:
                    raise RuntimeError(f"Memory floor or timeout: {prefix}")
                if driver is not None and driver.poll() is not None:
                    raise RuntimeError(f"Driver exited during case: {prefix}")
                time.sleep(0.05)
            if worker.returncode:
                raise RuntimeError(f"Benchmark failed ({worker.returncode}): {prefix}")
        raw = prefix.with_suffix(".stdout").read_text()
        result = fields(next(line for line in raw.splitlines() if line.startswith("RESULT ")))
        consumers = sorted([fields(line) for line in raw.splitlines() if line.startswith("LATENCY ")],
                           key=lambda item: int(item["consumer"]))
        assert int(result["producers"]) == 1 and int(result["consumers"]) == 2
        assert int(result["published"]) == messages
        assert int(result["delivered"]) == messages * 2
        assert [int(item["consumer"]) for item in consumers] == [0, 1]
        assert all(int(item["samples"]) == samples for item in consumers)
        assert int(result["latency_samples"]) == samples * 2
        assert int(result["rate_per_producer"]) == (1_000_000 if kind == "latency" else 0)
        assert result["latency_origin"] == origin
        after = resource_counters()
        events = dict(line.split() for line in after["memory.events"].splitlines())
        assert all(int(events[key]) == 0 for key in ("high", "max", "oom", "oom_kill"))
        record = {"number": number, "engine": engine, "kind": kind, "payload": payload,
                  "origin": origin, "round": round_number, "producers": 1, "consumers": 2,
                  "phase": "warmup" if round_number == 0 else "measured",
                  "command": command, "driver_command": driver_command, "result": result,
                  "consumers_detail": consumers,
                  "worst_subscriber_ns": {key: max(int(item[key + "_ns"]) for item in consumers)
                                          for key in ("p50", "p99", "p999", "max")},
                  "stdout": str(prefix.with_suffix(".stdout")),
                  "stdout_sha256": digest(prefix.with_suffix(".stdout")),
                  "resources_before": before, "resources_after": after,
                  "min_mem_available_kib": lowest, "wall_seconds": time.monotonic() - started}
        with (output / "results.jsonl").open("a") as stream:
            stream.write(json.dumps(record) + "\n")
        print(f"{number:02d} {record['phase']} {engine} 1P2S {payload}B {kind}/{origin} "
              f"pub={float(result['publish_msg_per_sec'])/1e6:.3f}M/s "
              f"worst_p50={record['worst_subscriber_ns']['p50']}ns "
              f"worst_p999={record['worst_subscriber_ns']['p999']/1000:.3f}us", flush=True)
        return record
    finally:
        stop(worker)
        stop(driver)
        if driver_log is not None:
            driver_log.close()
        if directory.exists():
            shutil.rmtree(directory)
        for path in Path("/dev/shm").glob(f"salias-{name}-*"):
            path.unlink(missing_ok=True)
        time.sleep(0.25)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    verify_limits()
    args.output.mkdir(parents=True, exist_ok=False)
    # Keep the coordinator off all application-worker CPUs in both engines.
    os.sched_setaffinity(0, {0})
    files = [SALIAS, AERON, DRIVER, DRIVER_BUILD / "lib/libaeron_driver.so",
             DRIVER_BUILD / "lib/libaeron.so", Path(__file__)]
    metadata = {
        "date": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT).decode().strip(),
        "tag": subprocess.check_output(["git", "describe", "--exact-match", "--tags"], cwd=ROOT).decode().strip(),
        "kernel": platform.release(), "aeron_version": "1.52.0",
        "files": {str(path): digest(path) for path in files},
        "cpu_roles": "subscriber0 CPU1; subscriber1 CPU2; publisher CPU3; coordinator CPU0; Aeron driver CPU0",
        "cpu_budget": "4-vCPU host; 3 fixed application-worker CPUs for both; Aeron has an additional driver on CPU0, not CPU-exclusive or equal total CPU cost",
        "difference_from_prior": "Earlier Salias-only run excluded CPU0 and ran its coordinator on CPUs1-3; do not splice the runs",
        "semantics": "1P2S Reliable broadcast: every subscriber receives every message; Salias FifoMpmc versus Aeron IPC exclusive publication",
        "capacity": "Salias 1MiB ring versus Aeron 1MiB term (triple-term log); not equal total mapped memory",
        "latency": "1M/s planned input; 2M messages/case; sample 1/16; attempt and planned measured independently",
        "throughput": "10M messages/case, no pacing, no latency samples, batch1",
        "aggregation": "Median across five rounds of each round's worst-subscriber percentile; never pooled quantiles",
        "payload_limit": "Logical 64/256B; latency writes 16B timestamp+marker, throughput 8B marker; count/sample checks, no full content/sequence validation",
        "limits": resource_counters(),
    }
    (args.output / "environment.json").write_text(json.dumps(metadata, indent=2) + "\n")
    rows = []
    number = 0
    for kind in ("latency", "throughput"):
        cells = [(payload, origin) for payload in (64, 256)
                 for origin in (("attempt", "planned") if kind == "latency" else ("claim",))]
        for round_number in range(6):
            offset = round_number % len(cells)
            for cell_index, (payload, origin) in enumerate(cells[offset:] + cells[:offset]):
                engines = ("salias", "aeron") if (round_number + cell_index) % 2 == 0 else ("aeron", "salias")
                for engine in engines:
                    number += 1
                    rows.append(run_case(args.output, number, kind, payload, origin, engine, round_number))
    measured = [row for row in rows if row["phase"] == "measured"]
    groups = []
    for kind, payload, origin, engine in sorted({(r["kind"], r["payload"], r["origin"], r["engine"]) for r in measured}):
        cell = [r for r in measured if (r["kind"], r["payload"], r["origin"], r["engine"]) == (kind, payload, origin, engine)]
        assert len(cell) == 5
        group = {"kind": kind, "payload": payload, "origin": origin, "engine": engine, "rounds": 5,
                 "publish_mmsg_s": median(float(r["result"]["publish_msg_per_sec"]) / 1e6 for r in cell),
                 "delivery_mmsg_s": median(float(r["result"]["delivery_msg_per_sec"]) / 1e6 for r in cell),
                 "publish_mmsg_s_range": [min(float(r["result"]["publish_msg_per_sec"]) / 1e6 for r in cell),
                                           max(float(r["result"]["publish_msg_per_sec"]) / 1e6 for r in cell)],
                 "duration_s_range": [min(float(r["result"]["seconds"]) for r in cell),
                                       max(float(r["result"]["seconds"]) for r in cell)],
                 "latency_ns": {key: {"median": median(r["worst_subscriber_ns"][key] for r in cell),
                                      "min": min(r["worst_subscriber_ns"][key] for r in cell),
                                      "max": max(r["worst_subscriber_ns"][key] for r in cell)}
                                for key in ("p50", "p99", "p999", "max")},
                 "per_subscriber_median_ns": [{key: median(int(r["consumers_detail"][i][key + "_ns"]) for r in cell)
                                                for key in ("p50", "p99", "p999", "max")} for i in range(2)]}
        groups.append(group)
    summary = {"environment": metadata, "groups": groups,
               "measured_runs": len(measured), "warmup_runs": len(rows) - len(measured),
               "measured_published": sum(int(r["result"]["published"]) for r in measured),
               "measured_delivered": sum(int(r["result"]["delivered"]) for r in measured),
               "measured_latency_samples": sum(int(r["result"]["latency_samples"]) for r in measured),
               "min_mem_available_kib": min(r["min_mem_available_kib"] for r in rows)}
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print("Completed paired comparison: " + str(args.output / "summary.json"), flush=True)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGHUP, interrupted)
    main()
