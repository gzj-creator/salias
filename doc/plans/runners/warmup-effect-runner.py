#!/usr/bin/env python3
"""Test whether 192 B and 128 B preloads change subsequent 64 B throughput."""

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

TOPOLOGY = payload_runner.TOPOLOGY
MESSAGES = 50_000_000
MEASURED_ROUNDS = 10
PRELOAD_ROUNDS = 5
GROUP_GAP_SECONDS = 5.0
MONITOR_SCRIPT = ROOT / "tools/system/capture_runtime_state.sh"
SYSTEM_MONITOR_DIR = "system-monitor"
SEQUENCE = (
    ("group1-direct-64b", (64,) * MEASURED_ROUNDS),
    ("group2-192b-preload", (192,) * PRELOAD_ROUNDS + (64,) * MEASURED_ROUNDS),
    ("group3-192b-128b-preload",
     (192,) * PRELOAD_ROUNDS + (128,) * PRELOAD_ROUNDS + (64,) * MEASURED_ROUNDS),
)


def digest(path):
    return topology_runner.digest(path)


def cpu_frequency_snapshot():
    values = {}
    for cpu in range(4):
        path = Path(f"/sys/devices/system/cpu/cpu{cpu}/cpufreq/scaling_cur_freq")
        try:
            values[str(cpu)] = int(path.read_text().strip()) * 1000
        except (FileNotFoundError, PermissionError, ValueError):
            values[str(cpu)] = None
    return values


def system_snapshot():
    load = Path("/proc/loadavg").read_text().split()
    return {
        "timestamp": time.time(),
        "monotonic": time.monotonic(),
        "loadavg": [float(value) for value in load[:3]],
        "running_tasks": int(load[3].split("/")[0]),
        "process_count": int(load[3].split("/")[1]),
        "mem_available_kib": available_kib(),
        "cpu_frequency_hz": cpu_frequency_snapshot(),
        "cgroup": resource_counters(),
    }


def idle_for(seconds):
    # Sleep without consuming CPU; the coordinator remains pinned to CPU0.
    time.sleep(seconds)


def start_system_monitor(output):
    """启动独立系统监控，并返回其 PID 与输出目录。"""
    directory = output / SYSTEM_MONITOR_DIR
    directory.mkdir()
    completed = subprocess.run(
        [str(MONITOR_SCRIPT), str(directory)],
        check=True,
        capture_output=True,
        text=True,
    )
    try:
        pid = int(completed.stdout.strip())
    except ValueError as exc:
        raise RuntimeError(f"Unexpected monitor PID: {completed.stdout!r}") from exc
    if pid <= 0:
        raise RuntimeError(f"Invalid monitor PID: {pid}")
    return pid, directory


def stop_system_monitor(pid):
    """结束监控并等待其退出，确保采样进程已经回收。"""
    if pid is None:
        return
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        time.sleep(0.05)
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        return
    raise RuntimeError(f"System monitor {pid} did not stop after SIGTERM")


def remove_monitor_pid(directory):
    """监控正常退出后会移除 PID 文件；保留目录内容供分析。"""
    if directory is not None:
        (directory / "monitor.pid").unlink(missing_ok=True)


def write_checksums(output):
    """校验顶层证据和 system-monitor 原始日志。"""
    files = sorted(
        path for path in output.rglob("*")
        if path.is_file() and path.name != "SHA256SUMS"
    )
    lines = [
        f"{digest(path)}  {path.relative_to(output)}\n"
        for path in files
    ]
    (output / "SHA256SUMS").write_text("".join(lines))


def run_case(args, group, payload, phase, round_number, number):
    if available_kib() < 1024 * 1024:
        raise RuntimeError("Less than 1GiB MemAvailable before case")
    before = system_snapshot()
    name = f"warmup-effect-{os.getpid()}-{number}"
    prefix = args.output / f"{number:03d}-{group}-{payload}b-{phase}-r{round_number}"
    command = payload_runner.benchmark_command(args.salias_client, payload, args.messages, name)
    worker = None
    lowest = before["mem_available_kib"]
    started_monotonic_ns = time.monotonic_ns()
    started_at_ns = time.time_ns()
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
        topology_runner.validate_result(result, TOPOLOGY, payload, args.messages)
        consumers = [topology_runner.fields(line) for line in raw.splitlines()
                     if line.startswith("LATENCY ")]
        if [int(item["consumer"]) for item in consumers] != [0, 1]:
            raise RuntimeError(f"Consumer output mismatch: {prefix}")
        after = resource_counters()
        events = dict(line.split() for line in after["memory.events"].splitlines())
        if any(int(events[key]) for key in ("high", "max", "oom", "oom_kill")):
            raise RuntimeError(f"Memory events: {events}")
        finished_monotonic_ns = time.monotonic_ns()
        finished_at_ns = time.time_ns()
        record = {
            "number": number,
            "engine": "salias",
            "kind": "throughput",
            "group": group,
            "topology": "1P2S",
            "mode": TOPOLOGY["mode"],
            "payload": payload,
            "producers": 1,
            "consumers": 2,
            "round": round_number,
            "phase": phase,
            "messages": args.messages,
            "command": command,
            "result": result,
            "consumers_detail": consumers,
            "stdout": prefix.with_suffix(".stdout").name,
            "stdout_sha256": digest(prefix.with_suffix(".stdout")),
            "system_before": before,
            "resources_after": after,
            "min_mem_available_kib": lowest,
            "wall_seconds": (finished_monotonic_ns - started_monotonic_ns) / 1e9,
            "started_at_ns": started_at_ns,
            "finished_at_ns": finished_at_ns,
            "started_monotonic_ns": started_monotonic_ns,
            "finished_monotonic_ns": finished_monotonic_ns,
        }
        with (args.output / "results.jsonl").open("a") as stream:
            stream.write(json.dumps(record) + "\n")
        print(f"{number:03d} {group} {payload}B {phase} r{round_number} "
              f"pub={float(result['publish_msg_per_sec']) / 1e6:.3f}M/s", flush=True)
    finally:
        stop(worker)
        for path in Path("/dev/shm").glob(f"salias-{name}-*"):
            path.unlink(missing_ok=True)
        time.sleep(args.cooldown)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--salias-client", type=Path, default=payload_runner.SALIAS_CLIENT)
    parser.add_argument("--messages", type=int, default=MESSAGES)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--cooldown", type=float, default=0.25)
    parser.add_argument("--smoke", action="store_true",
                        help="Run one measured round per group and no preload repeats")
    args = parser.parse_args()
    if (not 1 <= args.messages <= 100_000_000 or
            not 10 <= args.timeout <= 600 or args.cooldown < 0):
        parser.error("Require 1..100M messages, timeout 10..600s")
    args.salias_client = args.salias_client.resolve()
    if not args.salias_client.is_file() or not os.access(args.salias_client, os.X_OK):
        parser.error(f"Benchmark client is not executable: {args.salias_client}")
    topology_runner.verify_limits()
    args.output.mkdir(parents=True, exist_ok=False)
    os.sched_setaffinity(0, {0})
    files = [args.salias_client, Path(__file__), Path(payload_runner.__file__),
             Path(topology_runner.__file__), ROOT / "tools/aeron_compare/salias_ipc_compare.cpp",
             ROOT / "src/core/frame/codec.hpp", ROOT / "src/core/frame/header.hpp",
             ROOT / "tools/aeron_compare/run_latency_compare.py",
             MONITOR_SCRIPT,
             ROOT / "tools/system/analyze_system_state.py"]
    measured_rounds = 1 if args.smoke else MEASURED_ROUNDS
    preload_rounds = 1 if args.smoke else PRELOAD_ROUNDS
    metadata = {
        "date": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "kernel": platform.release(),
        "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT).decode().strip(),
        "topology": "1P2S",
        "messages": args.messages,
        "measured_rounds": measured_rounds,
        "preload_rounds": preload_rounds,
        "group_gap_seconds": GROUP_GAP_SECONDS,
        "sequence": (
            [["group1-direct-64b", [64] * measured_rounds],
             ["group2-192b-preload", [192] * preload_rounds + [64] * measured_rounds],
             ["group3-192b-128b-preload",
              [192] * preload_rounds + [128] * preload_rounds + [64] * measured_rounds]]
            if not args.smoke else
            [["group1-direct-64b", [64]],
             ["group2-192b-preload", [192, 64]],
             ["group3-192b-128b-preload", [192, 128, 64]]]
        ),
        "smoke": args.smoke,
        "files": {str(path): digest(path) for path in files},
        "cpu_roles": "coordinator CPU0; consumer0 CPU1; consumer1 CPU2; publisher CPU3",
        "capacity_bytes": 1048576,
        "batch_size": 1,
        "poll_limit": 64,
        "frequency_source": "/sys/devices/system/cpu/cpuN/cpufreq/scaling_cur_freq",
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
    metadata["snapshots"] = {name: digest(args.output / name) for name in snapshots}
    (args.output / "environment.json").write_text(json.dumps(metadata, indent=2) + "\n")
    monitor_pid = None
    monitor_dir = None
    number = 0
    try:
        monitor_pid, monitor_dir = start_system_monitor(args.output)
        metadata["system_monitor"] = {
            "pid": monitor_pid,
            "directory": monitor_dir.name,
            "interval_seconds": 1,
            "cpu_list": "1,2,3",
        }
        (args.output / "environment.json").write_text(
            json.dumps(metadata, indent=2) + "\n"
        )
        for group, group_sequence in (("group1-direct-64b", SEQUENCE[0][1]),
                                      ("group2-192b-preload", SEQUENCE[1][1]),
                                      ("group3-192b-128b-preload", SEQUENCE[2][1])):
            if args.smoke:
                if group == "group2-192b-preload":
                    group_sequence = (192, 64)
                elif group == "group3-192b-128b-preload":
                    group_sequence = (192, 128, 64)
                else:
                    group_sequence = (64,)
            payload_round = {}
            for payload in group_sequence:
                payload_round[payload] = payload_round.get(payload, 0) + 1
                round_number = payload_round[payload]
                phase = "preload" if payload != 64 else "measured"
                number += 1
                run_case(args, group, payload, phase, round_number, number)
            if group != "group3-192b-128b-preload":
                idle_for(GROUP_GAP_SECONDS)
    finally:
        stop_system_monitor(monitor_pid)
        remove_monitor_pid(monitor_dir)
        if monitor_dir is not None:
            metadata["system_monitor"]["stopped_at_ns"] = time.time_ns()
            (args.output / "environment.json").write_text(
                json.dumps(metadata, indent=2) + "\n"
            )
    records = [json.loads(line) for line in (args.output / "results.jsonl").read_text().splitlines()]
    topology_runner.write_summary(args.output, {
        "tested_topologies": ["1P2S"],
        "payloads": [64],
    }, [dict(record, topology="1P2S")
        for record in records if record["phase"] == "measured"])
    write_checksums(args.output)
    print(f"Completed {number} runs: {args.output}", flush=True)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, topology_runner.interrupted)
    signal.signal(signal.SIGHUP, topology_runner.interrupted)
    main()
