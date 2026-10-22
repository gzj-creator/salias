#!/usr/bin/env python3
"""Run the Salias-only Phase 1 topology sweep inside the documented cgroup."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import signal
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "tools/aeron_compare"))
from run_latency_compare import available_kib, cgroup_directory, fields, resource_counters, stop

SALIAS_CLIENT = (
    ROOT
    / "build/benchmarks/baseline-extended-20261019-183822"
    / "salias_ipc_compare.uninstrumented"
)
TOPOLOGIES = (
    {"producers": 1, "consumers": 1, "mode": "FifoMpsc"},
    {"producers": 1, "consumers": 2, "mode": "FifoMpmc"},
    {"producers": 1, "consumers": 3, "mode": "FifoMpmc", "required_cpus": 5},
)
PAYLOADS = (64, 256)
MESSAGES = 50_000_000
ROUNDS = 10


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
        raise RuntimeError(
            "Require CPUs 0-3, MemoryMax<=704M, zero swap and explicit capped-run flag"
        )


def process_cpu_count():
    """Return CPUs available to this process before the coordinator is pinned."""
    return len(os.sched_getaffinity(0))


def selected_topologies(cpu_count):
    return [topology for topology in TOPOLOGIES
            if cpu_count >= topology.get("required_cpus", 4)]


def worker_cpu(topology, producer_index=0):
    # Consumers occupy 1..N, then published workers follow; the coordinator stays on CPU0.
    return topology["consumers"] + producer_index + 1


def benchmark_command(client, topology, payload, round_number, messages, name):
    return [
        str(client), "--scenario", "fifo", "--name", name, "--page", "normal",
        "--capacity", "1048576", "--batch-size", "1", "--poll-limit", "64",
        "--messages", str(messages), "--producers", str(topology["producers"]),
        "--consumers", str(topology["consumers"]), "--payload", str(payload),
        "--cpu-base", "1", "--cpu-stride", "1", "--rate", "0",
        "--latency-sample-rate", "0", "--latency-origin", "claim",
    ]


def validate_result(result, topology, payload, messages):
    expected_delivered = messages * topology["consumers"]
    checks = {
        "producers": topology["producers"],
        "consumers": topology["consumers"],
        "payload": payload,
        "published": messages,
        "delivered": expected_delivered,
        "latency_samples": 0,
        "latency_sample_rate": 0,
        "rate_per_producer": 0,
        "scenario": "fifo",
    }
    for key, expected in checks.items():
        actual = result[key]
        if (actual != expected if key == "scenario" else int(actual) != expected):
            raise RuntimeError(f"Result mismatch for {key}: {result[key]} != {expected}")


def run_case(args, topology, payload, round_number, number):
    if available_kib() < 1024 * 1024:
        raise RuntimeError("Less than 1GiB MemAvailable before case")
    label = f"1P{topology['consumers']}S"
    name = f"topology-sweep-{os.getpid()}-{number}"
    prefix = args.output / f"{number:03d}-{label}-{payload}b-r{round_number}"
    command = benchmark_command(args.salias_client, topology, payload, round_number,
                                args.messages, name)
    worker = None
    lowest = available_kib()
    before = resource_counters()
    started = time.monotonic()
    try:
        with prefix.with_suffix(".stdout").open("w") as out, \
                prefix.with_suffix(".stderr").open("w") as err:
            worker = subprocess.Popen(command, stdout=out, stderr=err, start_new_session=True)
            deadline = time.monotonic() + args.timeout
            while worker.poll() is None:
                lowest = min(lowest, available_kib())
                if lowest < 600 * 1024 or time.monotonic() > deadline:
                    raise RuntimeError(f"Memory floor or timeout: {prefix}")
                time.sleep(0.05)
            if worker.returncode:
                raise RuntimeError(f"Benchmark failed ({worker.returncode}): {prefix}")
        raw = prefix.with_suffix(".stdout").read_text()
        result_lines = [line for line in raw.splitlines() if line.startswith("RESULT ")]
        if len(result_lines) != 1:
            raise RuntimeError(f"Expected exactly one RESULT: {prefix}")
        result = fields(result_lines[0])
        validate_result(result, topology, payload, args.messages)
        consumers = [fields(line) for line in raw.splitlines() if line.startswith("LATENCY ")]
        if [int(item["consumer"]) for item in consumers] != list(range(topology["consumers"])):
            raise RuntimeError(f"Consumer output mismatch: {prefix}")
        after = resource_counters()
        events = dict(line.split() for line in after["memory.events"].splitlines())
        if any(int(events[key]) for key in ("high", "max", "oom", "oom_kill")):
            raise RuntimeError(f"Memory events: {events}")
        record = {
            "number": number,
            "engine": "salias",
            "kind": "throughput",
            "topology": label,
            "mode": topology["mode"],
            "payload": payload,
            "producers": topology["producers"],
            "consumers": topology["consumers"],
            "round": round_number,
            "phase": "warmup" if round_number == 0 else "measured",
            "messages": args.messages,
            "command": command,
            "result": result,
            "consumers_detail": consumers,
            "stdout": prefix.with_suffix(".stdout").name,
            "stdout_sha256": digest(prefix.with_suffix(".stdout")),
            "resources_before": before,
            "resources_after": after,
            "min_mem_available_kib": lowest,
            "wall_seconds": time.monotonic() - started,
        }
        with (args.output / "results.jsonl").open("a") as stream:
            stream.write(json.dumps(record) + "\n")
        print(f"{number:02d} {record['phase']} {label} {payload}B "
              f"pub={float(result['publish_msg_per_sec']) / 1e6:.3f}M/s", flush=True)
    finally:
        stop(worker)
        # Never remove another run's channel; only this case's generated name is touched.
        for path in Path("/dev/shm").glob(f"salias-{name}-*"):
            path.unlink(missing_ok=True)
        time.sleep(args.cooldown)


def write_checksums(output):
    files = sorted(path for path in output.iterdir()
                   if path.is_file() and path.name != "SHA256SUMS")
    lines = [f"{digest(path)}  {path.name}\n" for path in files]
    (output / "SHA256SUMS").write_text("".join(lines))


def write_summary(output, environment, records):
    measured = [record for record in records if record["phase"] == "measured"]
    groups = []
    for topology in environment["tested_topologies"]:
        for payload in environment["payloads"]:
            cell = [record for record in measured
                    if record["topology"] == topology and record["payload"] == payload]
            rates = [float(record["result"]["publish_msg_per_sec"]) / 1e6 for record in cell]
            mean_rate = sum(rates) / len(rates)
            groups.append({
                "topology": topology,
                "payload": payload,
                "rounds": len(cell),
                "publish_mmsg_s": sorted(rates)[len(rates) // 2],
                "publish_mmsg_s_mean": mean_rate,
                "publish_mmsg_s_range": [min(rates), max(rates)] if rates else None,
            })
    summary = {
        "measured_runs": len(measured),
        "warmup_runs": len(records) - len(measured),
        "measured_published": sum(int(record["result"]["published"]) for record in measured),
        "measured_delivered": sum(int(record["result"]["delivered"]) for record in measured),
        "min_mem_available_kib": min(record["min_mem_available_kib"] for record in records),
        "groups": groups,
    }
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--salias-client", type=Path, default=SALIAS_CLIENT)
    parser.add_argument("--messages", type=int, default=MESSAGES)
    parser.add_argument("--rounds", type=int, default=ROUNDS)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--cooldown", type=float, default=0.25)
    args = parser.parse_args()
    if (not 1 <= args.messages <= 100_000_000 or not 1 <= args.rounds <= 20 or
            not 10 <= args.timeout <= 600 or args.cooldown < 0):
        parser.error("Require 1..100M messages, 1..20 rounds, timeout 10..600s")
    args.salias_client = args.salias_client.resolve()
    if not args.salias_client.is_file() or not os.access(args.salias_client, os.X_OK):
        parser.error(f"Benchmark client is not executable: {args.salias_client}")
    verify_limits()
    args.output.mkdir(parents=True, exist_ok=False)
    host_cpu_count = process_cpu_count()
    topologies = selected_topologies(host_cpu_count)
    if len(topologies) < 2:
        raise RuntimeError("At least 1P1S and 1P2S require four CPUs")
    # The parent is only the coordinator; workers pin themselves from cpu-base=1.
    os.sched_setaffinity(0, {0})
    files = [args.salias_client, Path(__file__),
             ROOT / "tools/aeron_compare/salias_ipc_compare.cpp",
             ROOT / "tools/aeron_compare/run_latency_compare.py"]
    metadata = {
        "date": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "kernel": platform.release(),
        "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT).decode().strip(),
        "host_cpu_count": host_cpu_count,
        "tested_topologies": [f"1P{t['consumers']}S" for t in topologies],
        "skipped_topologies": [f"1P{t['consumers']}S" for t in TOPOLOGIES
                               if t not in topologies],
        "messages": args.messages,
        "rounds": args.rounds,
        "payloads": list(PAYLOADS),
        "files": {str(path): digest(path) for path in files},
        "cpu_roles": "coordinator CPU0; consumer0 CPU1; consumer1 CPU2; consumer2 CPU3; "
                     "publisher CPU(1+N)",
        "capacity_bytes": 1048576,
        "batch_size": 1,
        "poll_limit": 64,
        "latency_sample_rate": 0,
        "rate_per_producer": 0,
        "limits": resource_counters(),
    }
    snapshots = {
        "frozen-runner.py": Path(__file__),
        "salias_ipc_compare.uninstrumented": args.salias_client,
    }
    for name, source in snapshots.items():
        shutil.copy2(source, args.output / name)
    metadata["snapshots"] = {name: digest(args.output / name) for name in snapshots}
    (args.output / "environment.json").write_text(json.dumps(metadata, indent=2) + "\n")
    number = 0
    for round_number in range(args.rounds + 1):
        payloads = PAYLOADS if round_number % 2 == 0 else tuple(reversed(PAYLOADS))
        topologies_ordered = (topologies if round_number % 2 == 0
                              else list(reversed(topologies)))
        for payload in payloads:
            for topology in topologies_ordered:
                number += 1
                run_case(args, topology, payload, round_number, number)
    records = [json.loads(line) for line in (args.output / "results.jsonl").read_text().splitlines()]
    write_summary(args.output, metadata, records)
    write_checksums(args.output)
    print(f"Completed {number} runs: {args.output}", flush=True)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGHUP, interrupted)
    main()
