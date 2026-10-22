#!/usr/bin/env python3
"""Run measured 1P2S rounds with a selectable payload and ring capacity."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import statistics
import subprocess
import time

ROOT = Path(__file__).resolve().parents[3]
INSTRUMENTED = (
    ROOT
    / "build/benchmarks/diagnostic-stable-20261019-205458"
    / "salias_ipc_compare.instrumented"
)
PAYLOAD = 192
CAPACITY = 1048576
MESSAGES = 50_000_000
MAX_ROUNDS = 30
CPU_ROLES = "coordinator CPU0; consumer0 CPU1; consumer1 CPU2; publisher CPU3"


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def fields(line):
    """把 RESULT 行解析成键值，保留原始文本以便逐项校验。"""
    return dict(item.split("=", 1) for item in line.split()[1:])


def cgroup_directory():
    relative = next(
        line.split(":", 2)[2]
        for line in Path("/proc/self/cgroup").read_text().splitlines()
        if line.startswith("0::")
    )
    return Path("/sys/fs/cgroup") / relative.lstrip("/")


def resource_counters():
    directory = cgroup_directory()
    return {
        name: (directory / name).read_text().strip()
        for name in ("memory.current", "memory.peak", "memory.events", "cpu.stat")
    }


def verify_limits():
    """拒绝在缺少 CPU、内存和 swap 上限的环境中采集不可比数据。"""
    directory = cgroup_directory()
    memory_max = (directory / "memory.max").read_text().strip()
    if (
        os.environ.get("SALIAS_RESOURCE_CAPPED") != "1"
        or os.sched_getaffinity(0) != {0, 1, 2, 3}
        or memory_max == "max"
        or int(memory_max) > 704 * 1024 * 1024
        or (directory / "memory.swap.max").read_text().strip() != "0"
    ):
        raise RuntimeError(
            "Require CPUs 0-3, MemoryMax<=704M, MemorySwapMax=0 "
            "and SALIAS_RESOURCE_CAPPED=1"
        )


def available_kib():
    return int(
        next(
            line.split()[1]
            for line in Path("/proc/meminfo").read_text().splitlines()
            if line.startswith("MemAvailable:")
        )
    )


def stop(process):
    """终止整个 worker 进程组，避免 consumer/publisher 子进程遗留。"""
    if process is None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=3)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait()


def interrupted(signum, frame):
    raise KeyboardInterrupt(f"Interrupted by signal {signum}")


def infer_instrumentation_level(client):
    """从二进制文件名推断 L0-L4；调用方可用显式参数覆盖。"""
    match = re.search(r"_L([0-4])(?:\.|$)", Path(client).name)
    return int(match.group(1)) if match else 0


def diagnostic_mode_for_level(level):
    """L0 保持完全未插桩，L4 才启用计时模板。"""
    if level == 0:
        return "off"
    return "timed" if level == 4 else "counters"


def benchmark_command(client, messages, name, diagnostic_mode, payload, capacity):
    """保持与 Phase 1.5 相同的 1P2S、ring、batch 和 poll 配置。"""
    command = [
        str(client),
        "--scenario",
        "fifo",
        "--name",
        name,
        "--page",
        "normal",
        "--capacity",
        str(capacity),
        "--batch-size",
        "1",
        "--poll-limit",
        "64",
        "--messages",
        str(messages),
        "--producers",
        "1",
        "--consumers",
        "2",
        "--payload",
        str(payload),
        "--cpu-base",
        "1",
        "--cpu-stride",
        "1",
        "--rate",
        "0",
        "--latency-sample-rate",
        "0",
        "--latency-origin",
        "claim",
    ]
    if diagnostic_mode != "off":
        command.extend(["--diagnostic", diagnostic_mode])
    return command


def validate_diagnostic(level, mode, diagnostic, messages):
    """按编译时级别验证实际存在的诊断字段，不把未编译字段当证据。"""
    if level == 0:
        if diagnostic:
            raise RuntimeError("L0 must not emit diagnostic fields")
        return
    if diagnostic.get("diagnostic_mode") != mode:
        raise RuntimeError(f"Diagnostic mode mismatch: expected {mode}")

    if level >= 1:
        success = int(diagnostic["diagnostic_claim_success"])
        if success != messages:
            raise RuntimeError(f"Claim count mismatch: {success} != {messages}")
        for consumer in range(2):
            prefix = f"diagnostic_consumer{consumer}_"
            if int(diagnostic[prefix + "poll_total_messages"]) != messages:
                raise RuntimeError(f"Consumer message mismatch: {consumer}")
            if int(diagnostic[prefix + "poll_success"]) <= 0:
                raise RuntimeError(f"Consumer poll success is zero: {consumer}")
    if level >= 2:
        backpressure = int(diagnostic["diagnostic_claim_backpressure"])
        if int(diagnostic["diagnostic_claim_yield_cycles"]) != backpressure:
            raise RuntimeError("Yield count mismatch")
    if level >= 3:
        for consumer in range(2):
            if f"diagnostic_consumer{consumer}_poll_empty" not in diagnostic:
                raise RuntimeError(f"Missing poll_empty for consumer {consumer}")
    if level >= 4:
        samples = int(
            diagnostic.get(
                "diagnostic_claim_timing_samples",
                diagnostic.get("diagnostic_claim_timed_samples", 0),
            )
        )
        if samples <= 0:
            raise RuntimeError("Timing sample count is zero")


def run_case(args, round_number, number):
    # The benchmark itself runs in a 704 MiB unit; these checks belong to the
    # coordinator, not to a unit that also contains the benchmark payload.
    if available_kib() < 600 * 1024:
        raise RuntimeError("Less than 600MiB MemAvailable before case")
    name = f"instrumented-{args.payload}b-{os.getpid()}-{number}"
    group = (number - 1) // args.rounds
    round_in_group = (number - 1) % args.rounds
    prefix = args.output / (
        f"group-{group:02d}/round-{round_in_group:02d}"
    )
    prefix.parent.mkdir(parents=True, exist_ok=True)
    command = benchmark_command(
        args.salias_client,
        args.messages,
        name,
        args.diagnostic_mode,
        args.payload,
        args.capacity,
    )
    worker = None
    lowest = available_kib()
    before = resource_counters()
    started = time.monotonic()
    try:
        with prefix.with_suffix(".stdout").open("w") as out, prefix.with_suffix(
            ".stderr"
        ).open("w") as err:
            systemd_command = [
                "systemd-run",
                "--user",
                "--wait",
                "--pipe",
                "--collect",
                "-p",
                f"WorkingDirectory={ROOT}",
                "-p",
                "AllowedCPUs=0-3",
                "-p",
                "Nice=5",
                "-p",
                "MemoryHigh=640M",
                "-p",
                "MemoryMax=704M",
                "-p",
                "MemorySwapMax=0",
                f"-p",
                f"RuntimeMaxSec={args.timeout}s",
                "--setenv=SALIAS_RESOURCE_CAPPED=1",
                "taskset",
                "-c",
                "0",
            ] + command
            worker = subprocess.Popen(
                systemd_command, stdout=out, stderr=err, start_new_session=True
            )
            deadline = time.monotonic() + args.timeout
            while worker.poll() is None:
                lowest = min(lowest, available_kib())
                if lowest < 600 * 1024 or time.monotonic() > deadline:
                    raise RuntimeError(f"Memory floor or timeout: {prefix}")
                time.sleep(0.05)
            if worker.returncode:
                raise RuntimeError(
                    f"Benchmark failed ({worker.returncode}): {prefix}"
                )

        raw = prefix.with_suffix(".stdout").read_text()
        result_lines = [
            line for line in raw.splitlines() if line.startswith("RESULT ")
        ]
        if len(result_lines) != 1:
            raise RuntimeError(f"Expected exactly one RESULT: {prefix}")
        result = fields(result_lines[0])
        expected = {
            "scenario": "fifo",
            "producers": "1",
            "consumers": "2",
            "payload": str(args.payload),
            "capacity": str(args.capacity),
            "published": str(args.messages),
            "delivered": str(args.messages * 2),
            "latency_samples": "0",
            "latency_sample_rate": "0",
            "rate_per_producer": "0",
            "batch_size": "1",
            "poll_limit": "64",
        }
        mismatches = {
            key: (result.get(key), value)
            for key, value in expected.items()
            if result.get(key) != value
        }
        if mismatches:
            raise RuntimeError(f"Result metadata mismatch {mismatches}: {prefix}")

        diagnostic = {}
        for line in raw.splitlines():
            if line.startswith("diagnostic_"):
                key, value = line.split("=", 1)
                if key in diagnostic:
                    raise RuntimeError(f"Duplicate {key}: {prefix}")
                diagnostic[key] = value
        try:
            validate_diagnostic(
                args.instrumentation_level,
                args.diagnostic_mode,
                diagnostic,
                args.messages,
            )
        except RuntimeError as error:
            raise RuntimeError(f"{error}: {prefix}") from error

        after = resource_counters()
        events = dict(line.split() for line in after["memory.events"].splitlines())
        if any(int(events[key]) for key in ("high", "max", "oom", "oom_kill")):
            raise RuntimeError(f"Memory events: {events}")
        record = {
            "number": number,
            "engine": "salias",
            "kind": "throughput",
            "topology": "1P2S",
            "mode": "FifoMpmc",
            "instrumentation_level": args.instrumentation_level,
            "diagnostic_mode": args.diagnostic_mode,
            "payload": args.payload,
            "capacity": args.capacity,
            "producers": 1,
            "consumers": 2,
            "group": group,
            "round": round_in_group,
            "phase": "measured",
            "messages": args.messages,
            "command": command,
            "result": result,
            "diagnostic": diagnostic,
            "stdout": prefix.with_suffix(".stdout").name,
            "stdout_sha256": digest(prefix.with_suffix(".stdout")),
            "resources_before": before,
            "resources_after": after,
            "min_mem_available_kib": lowest,
            "wall_seconds": time.monotonic() - started,
        }
        with (args.output / "results.jsonl").open("a") as stream:
            stream.write(json.dumps(record) + "\n")
        group_results = prefix.with_suffix(".json")
        group_results.write_text(json.dumps(record, indent=2) + "\n")
        with (prefix.parent / "results.jsonl").open("a") as stream:
            stream.write(json.dumps(record) + "\n")
        print(
            f"{number:02d} {record['phase']} L{args.instrumentation_level} "
            f"{args.diagnostic_mode} "
            f"pub={float(result['publish_msg_per_sec']) / 1e6:.3f}M/s",
            flush=True,
        )
    finally:
        stop(worker)
        for path in Path("/dev/shm").glob(f"salias-{name}-*"):
            path.unlink(missing_ok=True)
        time.sleep(args.cooldown)


def write_group_summary(output, args):
    """按高频比较器约定输出每组吞吐量与高模式计数。"""
    groups = []
    for group_dir in sorted(output.glob("group-*")):
        results = group_dir / "results.jsonl"
        if not results.is_file():
            continue
        records = [
            json.loads(line)
            for line in results.read_text().splitlines()
            if line.strip()
        ]
        if not records:
            continue
        rates = [
            float(record["result"]["publish_msg_per_sec"]) / 1e6
            for record in records
        ]
        groups.append({
            "group": int(group_dir.name.split("-", 1)[1]),
            "interval": "fixed_0s",
            "sleep_duration": 0.0,
            "throughputs": rates,
            "high_mode_count": sum(rate >= 15.0 for rate in rates),
            "median": statistics.median(rates),
            "mean": statistics.mean(rates),
            "min": min(rates),
            "max": max(rates),
            "rounds": len(rates),
        })
    groups.sort(key=lambda row: row["group"])
    (output / "summary.json").write_text(json.dumps(groups, indent=2) + "\n")
    return groups


def write_checksums(output):
    files = sorted(
        path for path in output.rglob("*")
        if path.is_file() and path.name != "SHA256SUMS"
    )
    (output / "SHA256SUMS").write_text(
        "".join(f"{digest(path)}  {path.relative_to(output)}\n" for path in files)
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--client",
        "--salias-client",
        dest="salias_client",
        type=Path,
        default=INSTRUMENTED,
    )
    parser.add_argument(
        "--instrumentation-level",
        type=int,
        default=None,
        help="Override the L0-L4 level inferred from the client name",
    )
    parser.add_argument("--messages", type=int, default=MESSAGES)
    parser.add_argument("--payload", type=int, default=PAYLOAD)
    parser.add_argument("--capacity", type=int, default=CAPACITY)
    parser.add_argument("--rounds", type=int, default=20)
    parser.add_argument("--groups", type=int, default=5, help=argparse.SUPPRESS)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--cooldown", type=float, default=0.25)
    args = parser.parse_args()
    if (
        not 1 <= args.messages <= 100_000_000
        or not 1 <= args.rounds <= MAX_ROUNDS
        or not 1 <= args.groups <= 20
        or not 1 <= args.payload <= 1_000_000
        or not 4096 <= args.capacity <= 1 << 30
        or args.capacity & (args.capacity - 1)
        or not 10 <= args.timeout <= 600
        or args.cooldown < 0
    ):
        parser.error(
            "Require 1..100M messages, 1..20 groups x 1..30 rounds, payload 1..1M bytes, "
            "power-of-two capacity 4KiB..1GiB, timeout 10..600s"
        )
    args.salias_client = args.salias_client.resolve()
    if args.instrumentation_level is None:
        args.instrumentation_level = infer_instrumentation_level(args.salias_client)
    if not 0 <= args.instrumentation_level <= 4:
        parser.error("instrumentation level must be between 0 and 4")
    args.diagnostic_mode = diagnostic_mode_for_level(args.instrumentation_level)
    if not args.salias_client.is_file() or not os.access(
        args.salias_client, os.X_OK
    ):
        parser.error(f"Benchmark client is not executable: {args.salias_client}")
    args.output.mkdir(parents=True, exist_ok=False)

    files = [
        args.salias_client,
        Path(__file__),
        ROOT / "tools/aeron_compare/analyze_instrumented_192b.py",
        ROOT / "tools/aeron_compare/parse_diagnostic.py",
        ROOT / "tools/aeron_compare/run_latency_compare.py",
        ROOT / "tools/aeron_compare/salias_ipc_compare.cpp",
    ]
    metadata = {
        "date": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "kernel": platform.release(),
        "commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT
        ).decode().strip(),
        "topology": "1P2S",
        "payload": args.payload,
        "messages": args.messages,
        "groups": args.groups,
        "rounds_per_group": args.rounds,
        "instrumentation_level": args.instrumentation_level,
        "diagnostic_mode": args.diagnostic_mode,
        "diagnostic_timing": args.diagnostic_mode == "timed",
        "cpu_roles": CPU_ROLES,
        "capacity_bytes": args.capacity,
        "batch_size": 1,
        "poll_limit": 64,
        "rate_per_producer": 0,
        "latency_sample_rate": 0,
        "salias_client": str(args.salias_client),
        "files": {str(path): digest(path) for path in sorted(files)},
        "limits": resource_counters(),
    }
    snapshots = {
        "frozen-runner.py": Path(__file__),
        "salias_ipc_compare.instrumented": args.salias_client,
        "parse_diagnostic.py": ROOT / "tools/aeron_compare/parse_diagnostic.py",
        "analyze_instrumented_192b.py": (
            ROOT / "tools/aeron_compare/analyze_instrumented_192b.py"
        ),
    }
    for name, source in snapshots.items():
        shutil.copy2(source, args.output / name)
    metadata["snapshots"] = {
        name: digest(args.output / name) for name in snapshots
    }
    (args.output / "environment.json").write_text(
        json.dumps(metadata, indent=2) + "\n"
    )

    number = 0
    for group in range(args.groups):
        group_dir = args.output / f"group-{group:02d}"
        group_dir.mkdir()
        for round_number in range(args.rounds):
            number += 1
            run_case(args, round_number, number)
        write_group_summary(args.output, args)
        print(f"Group {group}: complete", flush=True)
    groups = write_group_summary(args.output, args)
    if len(groups) != args.groups or any(
        group["rounds"] != args.rounds for group in groups
    ):
        raise RuntimeError("Incomplete group matrix")
    write_checksums(args.output)
    print(f"Completed {number} runs: {args.output}", flush=True)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGHUP, interrupted)
    main()
