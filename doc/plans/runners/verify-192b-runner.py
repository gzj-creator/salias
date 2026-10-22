#!/usr/bin/env python3
"""Verify the P1.2 192-byte high-throughput result with 20 measured rounds."""

import argparse
import importlib.util
import json
import os
from pathlib import Path
import platform
import shutil
import signal
import subprocess
import time

ROOT = Path(__file__).resolve().parents[3]
spec = importlib.util.spec_from_file_location(
    "payload_runner", Path(__file__).with_name("payload-sweep-runner.py")
)
payload_runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(payload_runner)
topology_runner = payload_runner.topology_runner
from run_latency_compare import available_kib, resource_counters, stop

PAYLOAD = 192
TOPOLOGY = payload_runner.TOPOLOGY
MESSAGES = 50_000_000
ROUNDS = 20


def run_case(args, round_number, number):
    if available_kib() < 1024 * 1024:
        raise RuntimeError("Less than 1GiB MemAvailable before case")
    name = f"verify-192b-{os.getpid()}-{number}"
    prefix = args.output / f"{number:03d}-1p2s-192b-r{round_number}"
    command = payload_runner.benchmark_command(
        args.salias_client, PAYLOAD, args.messages, name
    )
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
        result = topology_runner.fields(result_lines[0])
        topology_runner.validate_result(result, TOPOLOGY, PAYLOAD, args.messages)
        consumers = [topology_runner.fields(line) for line in raw.splitlines()
                     if line.startswith("LATENCY ")]
        if [int(item["consumer"]) for item in consumers] != [0, 1]:
            raise RuntimeError(f"Consumer output mismatch: {prefix}")
        after = resource_counters()
        events = dict(line.split() for line in after["memory.events"].splitlines())
        if any(int(events[key]) for key in ("high", "max", "oom", "oom_kill")):
            raise RuntimeError(f"Memory events: {events}")
        record = {
            "number": number,
            "engine": "salias",
            "kind": "throughput",
            "topology": "1P2S",
            "mode": TOPOLOGY["mode"],
            "payload": PAYLOAD,
            "producers": 1,
            "consumers": 2,
            "round": round_number,
            "phase": "warmup" if round_number == 0 else "measured",
            "messages": args.messages,
            "command": command,
            "result": result,
            "consumers_detail": consumers,
            "stdout": prefix.with_suffix(".stdout").name,
            "stdout_sha256": have_digest(prefix.with_suffix(".stdout")),
            "resources_before": before,
            "resources_after": after,
            "min_mem_available_kib": lowest,
            "wall_seconds": time.monotonic() - started,
        }
        with (args.output / "results.jsonl").open("a") as stream:
            stream.write(json.dumps(record) + "\n")
        print(f"{number:02d} {record['phase']} 192B "
              f"pub={float(result['publish_msg_per_sec']) / 1e6:.3f}M/s", flush=True)
    finally:
        stop(worker)
        for path in Path("/dev/shm").glob(f"salias-{name}-*"):
            path.unlink(missing_ok=True)
        time.sleep(args.cooldown)


def have_digest(path):
    return topology_runner.digest(path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--salias-client", type=Path, default=payload_runner.SALIAS_CLIENT)
    parser.add_argument("--messages", type=int, default=MESSAGES)
    parser.add_argument("--rounds", type=int, default=ROUNDS)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--cooldown", type=float, default=0.25)
    args = parser.parse_args()
    if (not 1 <= args.messages <= 100_000_000 or not 1 <= args.rounds <= 30 or
            not 10 <= args.timeout <= 600 or args.cooldown < 0):
        parser.error("Require 1..100M messages, 1..30 rounds, timeout 10..600s")
    args.salias_client = args.salias_client.resolve()
    if not args.salias_client.is_file() or not os.access(args.salias_client, os.X_OK):
        parser.error(f"Benchmark client is not executable: {args.salias_client}")
    topology_runner.verify_limits()
    args.output.mkdir(parents=True, exist_ok=False)
    os.sched_setaffinity(0, {0})
    files = [args.salias_client, Path(__file__), Path(payload_runner.__file__),
             Path(topology_runner.__file__), ROOT / "tools/aeron_compare/salias_ipc_compare.cpp",
             ROOT / "src/core/frame/codec.hpp", ROOT / "src/core/frame/header.hpp",
             ROOT / "tools/aeron_compare/run_latency_compare.py"]
    metadata = {
        "date": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "kernel": platform.release(),
        "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT).decode().strip(),
        "topology": "1P2S",
        "payload": PAYLOAD,
        "messages": args.messages,
        "rounds": args.rounds,
        "files": {str(path): topology_runner.digest(path) for path in files},
        "cpu_roles": "coordinator CPU0; consumer0 CPU1; consumer1 CPU2; publisher CPU3",
        "capacity_bytes": 1048576,
        "batch_size": 1,
        "poll_limit": 64,
        "limits": resource_counters(),
    }
    snapshots = {
        "frozen-runner.py": Path(__file__),
        "imported-payload-runner.py": Path(payload_runner.__file__),
        "imported-topology-runner.py": Path(topology_runner.__file__),
        "salias_ipc_compare.uninstrumented": args.salias_client,
    }
    for name, source in snapshots.items():
        shutil.copy2(source, args.output / name)
    metadata["snapshots"] = {
        name: topology_runner.digest(args.output / name) for name in snapshots
    }
    (args.output / "environment.json").write_text(json.dumps(metadata, indent=2) + "\n")
    number = 0
    for round_number in range(args.rounds + 1):
        number += 1
        run_case(args, round_number, number)
    records = [json.loads(line) for line in (args.output / "results.jsonl").read_text().splitlines()]
    topology_runner.write_summary(args.output, {
        "tested_topologies": ["1P2S"],
        "payloads": [PAYLOAD],
    }, records)
    topology_runner.write_checksums(args.output)
    print(f"Completed {number} runs: {args.output}", flush=True)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, topology_runner.interrupted)
    signal.signal(signal.SIGHUP, topology_runner.interrupted)
    main()
