#!/usr/bin/env python3
"""Run isolated 1P2S throughput diagnostics and paired instrumentation controls."""

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
    "extended_baseline", Path(__file__).with_name("baseline-extended-runner.py"))
harness = importlib.util.module_from_spec(spec)
spec.loader.exec_module(harness)


def run_case(args, number, payload, mode, round_number):
    """Retain every stdout and reject incomplete delivery or resource failures."""
    if harness.available_kib() < 1024 * 1024:
        raise RuntimeError("Less than 1GiB MemAvailable before case")
    name = f"phase1-diag-{os.getpid()}-{number}"
    prefix = args.output / f"{number:03d}-salias-throughput-{payload}b-{mode}-r{round_number}"
    client = args.control_client if mode == "off" else args.salias_client
    command = [str(client), "--scenario", "fifo", "--name", name,
               "--page", "normal", "--capacity", "1048576", "--batch-size", "1",
               "--poll-limit", "64", "--messages", str(args.messages),
               "--producers", "1", "--consumers", "2", "--payload", str(payload),
               "--cpu-base", "1", "--cpu-stride", "1", "--rate", "0",
               "--latency-sample-rate", "0", "--latency-origin", "claim"]
    if mode != "off":
        command += ["--diagnostic", mode]
    worker = None
    before = harness.resource_counters()
    lowest = harness.available_kib()
    started = time.monotonic()
    try:
        with prefix.with_suffix(".stdout").open("w") as out, \
                prefix.with_suffix(".stderr").open("w") as err:
            worker = subprocess.Popen(command, stdout=out, stderr=err, start_new_session=True)
            deadline = time.monotonic() + 30
            while worker.poll() is None:
                lowest = min(lowest, harness.available_kib())
                if lowest < 600 * 1024 or time.monotonic() > deadline:
                    raise RuntimeError(f"Memory floor or timeout: {prefix}")
                time.sleep(0.05)
            if worker.returncode:
                raise RuntimeError(f"Benchmark failed ({worker.returncode}): {prefix}")
        raw = prefix.with_suffix(".stdout").read_text()
        results = [harness.fields(line) for line in raw.splitlines() if line.startswith("RESULT ")]
        if len(results) != 1:
            raise RuntimeError(f"Expected exactly one RESULT: {prefix}")
        result = results[0]
        if (int(result["published"]) != args.messages or
                int(result["delivered"]) != args.messages * 2 or
                int(result["producers"]) != 1 or int(result["consumers"]) != 2 or
                int(result["payload"]) != payload or int(result["latency_samples"]) != 0):
            raise RuntimeError(f"Count/topology mismatch: {prefix}")
        after = harness.resource_counters()
        events = dict(line.split() for line in after["memory.events"].splitlines())
        if any(int(events[key]) for key in ("high", "max", "oom", "oom_kill")):
            raise RuntimeError(f"Memory events: {events}")
        record = dict(number=number, engine="salias", kind="throughput", payload=payload,
                      mode=mode, round=round_number,
                      phase="warmup" if round_number == 0 else "measured",
                      messages=args.messages, command=command, result=result,
                      stdout=prefix.with_suffix(".stdout").name,
                      stdout_sha256=harness.digest(prefix.with_suffix(".stdout")),
                      resources_before=before, resources_after=after,
                      min_mem_available_kib=lowest, wall_seconds=time.monotonic() - started)
        with (args.output / "results.jsonl").open("a") as stream:
            stream.write(json.dumps(record) + "\n")
        print(f"{number:02d} {record['phase']} {payload}B {mode} "
              f"pub={float(result['publish_msg_per_sec']) / 1e6:.3f}M/s", flush=True)
    finally:
        harness.stop(worker)
        for path in Path("/dev/shm").glob(f"salias-{name}-*"):
            path.unlink(missing_ok=True)
        time.sleep(0.25)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--messages", type=int, default=10_000_000)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--modes", nargs="+", choices=("off", "counters", "timed"),
                        default=["off", "counters", "timed"])
    parser.add_argument("--salias-client", type=Path, default=harness.SALIAS)
    parser.add_argument("--control-client", type=Path)
    args = parser.parse_args()
    if not 1 <= args.messages <= 100_000_000 or not 1 <= args.rounds <= 10:
        parser.error("Require 1..100M messages and 1..10 rounds")
    if len(set(args.modes)) != len(args.modes):
        parser.error("Duplicate diagnostic modes")
    args.salias_client = args.salias_client.resolve()
    args.control_client = (args.control_client or args.salias_client).resolve()
    for client in {args.salias_client, args.control_client}:
        if not client.is_file() or not os.access(client, os.X_OK):
            parser.error(f"Benchmark client is not executable: {client}")
    harness.verify_limits()
    args.output.mkdir(parents=True, exist_ok=False)
    os.sched_setaffinity(0, {0})
    files = {args.salias_client, args.control_client, Path(__file__),
             Path(harness.__file__), ROOT / "tools/aeron_compare/salias_ipc_compare.cpp"}
    metadata = dict(date=time.strftime("%Y-%m-%dT%H:%M:%S%z"), kernel=platform.release(),
                    commit=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT).decode().strip(),
                    files={str(path): harness.digest(path) for path in sorted(files)},
                    cpu_roles="coordinator CPU0; consumer0 CPU1; consumer1 CPU2; publisher CPU3",
                    capacity_bytes=1048576, batch_size=1, poll_limit=64,
                    messages=args.messages, rounds=args.rounds, modes=args.modes,
                    payloads=[64, 256], producers=1, consumers=2,
                    latency_sample_rate=0, rate_per_producer=0,
                    salias_client=str(args.salias_client), control_client=str(args.control_client),
                    claim_timing="timed: two steady_clock reads per try_claim; successful calls only; not Guard-exclusive",
                    limits=harness.resource_counters())
    # Freeze the executed harness and binaries; later rebuilds must not change the evidence.
    snapshots = {
        "executed-runner.py": Path(__file__),
        "imported-baseline-runner.py": Path(harness.__file__),
        "salias_ipc_compare.instrumented.cpp": ROOT / "tools/aeron_compare/salias_ipc_compare.cpp",
        "salias_ipc_compare.instrumented": args.salias_client,
        "salias_ipc_compare.control": args.control_client,
    }
    for name, source in snapshots.items():
        shutil.copy2(source, args.output / name)
    metadata["snapshots"] = {name: harness.digest(args.output / name) for name in snapshots}
    (args.output / "environment.json").write_text(json.dumps(metadata, indent=2) + "\n")
    number = 0
    for round_number in range(args.rounds + 1):
        payloads = (64, 256) if round_number % 2 == 0 else (256, 64)
        offset = round_number % len(args.modes)
        modes = args.modes[offset:] + args.modes[:offset]
        for payload in payloads:
            for mode in modes:
                number += 1
                run_case(args, number, payload, mode, round_number)
    print(f"Completed {number} runs: {args.output}", flush=True)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, harness.interrupted)
    signal.signal(signal.SIGHUP, harness.interrupted)
    main()
