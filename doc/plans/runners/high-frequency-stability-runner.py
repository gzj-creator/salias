#!/usr/bin/env python3
"""High-frequency stability test: 20 groups x 20 measured rounds.

Each round starts a fresh benchmark process inside a resource-capped systemd
unit. That preserves the cold-process boundary while allowing the scheduler
and CPU caches to retain state across rounds.
"""

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import statistics
import subprocess
import time


ROOT = Path(__file__).resolve().parents[3]
GROUPS = 20
ROUNDS = 20
MESSAGES = 50_000_000
PAYLOAD = 192
CAPACITY = 1048576
HIGH_MODE_THRESHOLD_MMSG_S = 15.0
RESULT_RATE_PATTERN = re.compile(r"(?:^|\s)publish_msg_per_sec=([0-9.eE+-]+)")
SLEEP_SCHEDULE = (
    ("no_sleep", 0, 5, 0.0),
    ("light_1s", 5, 10, 1.0),
    ("medium_10s", 10, 15, 10.0),
    ("heavy_60s", 15, 20, 60.0),
)


def load_systemd_recorder():
    """复用仓库已有的 cgroup 进程记录与回收逻辑。"""
    path = ROOT / "tools/aeron_compare/run_latency_compare.py"
    spec = importlib.util.spec_from_file_location("run_latency_compare", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


recorder = load_systemd_recorder()


def digest(path):
    """返回文件 SHA-256，便于把摘要与原始输出关联起来。"""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def cgroup_accounting():
    """返回当前 unit 的 CPU 与内存事件计数。"""
    directory = recorder.cgroup_directory()
    return {
        "cpu_stat": (directory / "cpu.stat").read_text().strip(),
        "memory_events": (directory / "memory.events").read_text().strip(),
        "memory_peak": (directory / "memory.peak").read_text().strip(),
    }


def interval_for_group(group_idx, override=None):
    """返回组所属的间隔名称和该组启动前的等待秒数。"""
    if override is not None:
        return f"fixed_{override:g}s", float(override)
    for name, start, end, sleep_duration in SLEEP_SCHEDULE:
        if start <= group_idx < end:
            return name, sleep_duration
    raise ValueError(f"Group index out of range: {group_idx}")


def parse_throughput(stdout):
    """从唯一的 RESULT 行提取发布吞吐量，单位 Mmsg/s。"""
    matches = RESULT_RATE_PATTERN.findall(stdout)
    if len(matches) != 1:
        raise RuntimeError(f"Expected exactly one publish rate, found {len(matches)}")
    return float(matches[0]) / 1e6


def benchmark_command(args, group_idx, round_idx):
    """构造单轮基准测试命令。"""
    name = f"high-frequency-{os.getpid()}-g{group_idx:02d}-r{round_idx:02d}"
    command = [
        str(args.client),
        "--scenario", "fifo",
        "--producers", "1",
        "--consumers", "2",
        "--payload", str(args.payload),
        "--messages", str(args.messages),
        # Keep the P0 ring size explicit; the client default is 4 MiB.
        "--page", "normal",
        "--capacity", str(args.capacity),
        "--cpu-base", "1",
        "--cpu-stride", "1",
        "--name", name,
        "--batch-size", "1",
        "--poll-limit", "64",
        "--rate", "0",
        "--latency-sample-rate", "0",
        "--latency-origin", "claim",
    ]
    if getattr(args, "publish_path", "claim") != "claim":
        command.extend(["--publish-path", args.publish_path])
    if getattr(args, "diagnostic", "off") != "off":
        command.extend(["--diagnostic", args.diagnostic])
    # Historical P0 runners pin the coordinator to CPU0; workers pin themselves
    # to CPU1-3 from --cpu-base/--cpu-stride. Without this, the coordinator can
    # steal a worker CPU and collapse the high mode.
    command = ["taskset", "-c", "0"] + command
    systemd_command = [
        "systemd-run",
        "--user", "--wait", "--pipe", "--collect",
        "-p", f"WorkingDirectory={ROOT}",
        "-p", "AllowedCPUs=0-3",
        "-p", "Nice=5",
        "-p", "MemoryHigh=640M",
        "-p", "MemoryMax=704M",
        "-p", "MemorySwapMax=0",
        f"-p", f"RuntimeMaxSec={args.timeout}s",
        "--setenv=SALIAS_RESOURCE_CAPPED=1",
    ]
    # 追加调用方指定的环境变量（例如 SALIAS_DISABLE_ASYMMETRIC_FENCE=1）：
    # systemd-run 的 --setenv 必须在被执行的命令之前，因此拼在命令前。
    for assignment in getattr(args, "setenv", None) or []:
        systemd_command.append(f"--setenv={assignment}")
    systemd_command += command
    return name, command, systemd_command


def admit_round(args, group_idx, round_idx, throughput):
    """P0 回归门禁：开头必须看到高模式，之后允许脆弱的模式切换。"""
    if throughput < HIGH_MODE_THRESHOLD_MMSG_S:
        args.consecutive_low += 1
    else:
        args.consecutive_low = 0
        args.admission_highs += 1
    if (
        args.max_consecutive_low > 0
        and args.consecutive_low >= args.max_consecutive_low
        and args.admission_highs == 0
    ):
        raise RuntimeError(
            "Baseline admission failed: "
            f"{args.consecutive_low} consecutive rounds below "
            f"{HIGH_MODE_THRESHOLD_MMSG_S:g} Mmsg/s; latest group/round "
            f"{group_idx}/{round_idx}. No high-mode round was observed, so P0 "
            "admission failed before the stability experiment began."
        )


def run_round(args, group_idx, round_idx, output_dir):
    """在独立 cgroup 中运行一轮，并保存原始输出与结构化结果。"""
    name, command, systemd_command = benchmark_command(args, group_idx, round_idx)
    prefix = output_dir / f"round-{round_idx:02d}"
    stdout_path = prefix.with_suffix(".stdout")
    stderr_path = prefix.with_suffix(".stderr")
    started_at_ns = time.time_ns()
    started_monotonic_ns = time.monotonic_ns()
    try:
        with stdout_path.open("w") as stdout, stderr_path.open("w") as stderr:
            process = subprocess.Popen(
                systemd_command,
                cwd=ROOT,
                stdout=stdout,
                stderr=stderr,
                text=True,
                start_new_session=True,
            )
            try:
                returncode = process.wait(timeout=args.timeout + 15)
            except subprocess.TimeoutExpired:
                recorder.stop(process)
                raise RuntimeError(
                    f"Round {group_idx}/{round_idx} timed out after {args.timeout}s"
                )
        if returncode:
            raise RuntimeError(
                f"Round {group_idx}/{round_idx} failed ({returncode}); "
                f"see {stderr_path}"
            )
        raw = stdout_path.read_text()
        throughput = parse_throughput(raw)
        record = {
            "group": group_idx,
            "round": round_idx,
            "name": name,
            "publish_mmsg_s": throughput,
            "high_mode": throughput >= HIGH_MODE_THRESHOLD_MMSG_S,
            "command": command,
            "started_at_ns": started_at_ns,
            "finished_at_ns": time.time_ns(),
            "started_monotonic_ns": started_monotonic_ns,
            "finished_monotonic_ns": time.monotonic_ns(),
            "stdout": stdout_path.name,
            "stdout_sha256": digest(stdout_path),
            "stderr": stderr_path.name,
            "stderr_sha256": digest(stderr_path),
            "cgroup_after": cgroup_accounting(),
        }
        (prefix.with_suffix(".json")).write_text(json.dumps(record, indent=2) + "\n")
        with (output_dir / "results.jsonl").open("a") as stream:
            stream.write(json.dumps(record) + "\n")
        (output_dir / "latest.json").write_text(json.dumps(record) + "\n")
        admit_round(args, group_idx, round_idx, throughput)
        return record
    finally:
        process = locals().get("process")
        if process is not None and process.poll() is None:
            recorder.stop(process)
        for path in Path("/dev/shm").glob(f"salias-{name}-*"):
            path.unlink(missing_ok=True)


def write_group_summary(output_dir, group_idx, interval, sleep_duration, records):
    """写入组摘要；文件名保持与验收分析脚本约定一致。"""
    rates = [record["publish_mmsg_s"] for record in records]
    ordered = sorted(rates)
    midpoint = len(ordered) // 2
    group_median = (ordered[midpoint - 1] + ordered[midpoint]) / 2
    summary = {
        "group": group_idx,
        "interval": interval,
        "sleep_duration": sleep_duration,
        "throughputs": rates,
        "high_mode_count": sum(
            rate >= HIGH_MODE_THRESHOLD_MMSG_S for rate in rates
        ),
        "median": group_median,
        "mean": sum(rates) / len(rates),
        "min": min(rates),
        "max": max(rates),
        "rounds": len(records),
    }
    (output_dir / "throughputs.json").write_text(json.dumps(summary, indent=2) + "\n")
    return summary


def existing_throughputs(output):
    """读取同一连续实验的既有分块，避免状态序列在续跑时重置。"""
    rates = []
    for results in sorted(output.glob("group-*/results.jsonl")):
        for line in results.read_text().splitlines():
            if line.strip():
                rates.append(float(json.loads(line)["publish_mmsg_s"]))
    if not rates:
        return {
            "throughputs": [], "high_mode_count": 0,
            "median": 0.0, "mean": 0.0, "min": 0.0, "max": 0.0, "rounds": 0,
        }
    ordered = sorted(rates)
    midpoint = len(ordered) // 2
    median = (
        (ordered[midpoint - 1] + ordered[midpoint]) / 2
        if len(ordered) % 2 == 0 else ordered[midpoint]
    )
    return {
        "throughputs": rates,
        "high_mode_count": sum(rate >= HIGH_MODE_THRESHOLD_MMSG_S for rate in rates),
        "median": median,
        "mean": statistics.mean(rates),
        "min": min(rates),
        "max": max(rates),
        "rounds": len(rates),
    }


def group_throughputs(directory):
    """读取单个组目录中的吞吐量并计算摘要。"""
    path = directory / "results.jsonl"
    if not path.is_file():
        return None
    rates = [
        float(json.loads(line)["publish_mmsg_s"])
        for line in path.read_text().splitlines()
        if line.strip()
    ]
    if not rates:
        return None
    ordered = sorted(rates)
    midpoint = len(ordered) // 2
    return {
        "throughputs": rates,
        "high_mode_count": sum(rate >= HIGH_MODE_THRESHOLD_MMSG_S for rate in rates),
        "median": (
            (ordered[midpoint - 1] + ordered[midpoint]) / 2
            if len(ordered) % 2 == 0 else ordered[midpoint]
        ),
        "mean": statistics.mean(rates),
        "min": min(rates),
        "max": max(rates),
        "rounds": len(rates),
    }


def write_combined_summary(output, args):
    """把顺序分块合并成分析器需要的完整组矩阵。"""
    groups = []
    for group_idx in range(args.groups):
        directory = output / f"group-{group_idx:02d}"
        if not directory.is_dir():
            break
        values = group_throughputs(directory)
        if values is None:
            continue
        interval, sleep_duration = interval_for_group(
            group_idx, getattr(args, "interval_seconds", None)
        )
        groups.append({
            "group": group_idx, "interval": interval,
            "sleep_duration": sleep_duration, **values,
        })
    (output / "summary.json").write_text(json.dumps(groups, indent=2) + "\n")
    return groups


def verify_client(client):
    """拒绝不存在的客户端，避免等到第一轮才失败。"""
    if not client.is_file() or not os.access(client, os.X_OK):
        raise ValueError(f"Client is not executable: {client}")


def supports_publish_path(client):
    """检查客户端是否声明了仅用于诊断的 publish-path 参数。"""
    try:
        help_text = subprocess.run(
            [str(client), "--help"], text=True, capture_output=True, check=False
        ).stdout
    except OSError:
        return False
    return "--publish-path" in help_text


def supports_diagnostic(client):
    """检查客户端是否声明了编译期诊断开关。"""
    try:
        help_text = subprocess.run(
            [str(client), "--help"], text=True, capture_output=True, check=False
        ).stdout
    except OSError:
        return False
    return "--diagnostic" in help_text


def write_environment(args, output):
    """记录测试矩阵和客户端身份，确保结果可追溯。"""
    publish_path_supported = supports_publish_path(args.client)
    diagnostic_supported = supports_diagnostic(args.client)
    files = {
        "client": args.client,
        "runner": Path(__file__),
    }
    environment = {
        "date": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "groups": args.groups,
        "rounds_per_group": args.rounds,
        "messages_per_round": args.messages,
        "payload_bytes": args.payload,
        "capacity_bytes": args.capacity,
        "rate_per_producer": 0,
        "latency_sample_rate": 0,
        "publish_path": getattr(args, "publish_path", "claim"),
        "diagnostic": getattr(args, "diagnostic", "off"),
        "diagnostic_supported": diagnostic_supported,
        "publish_path_supported": publish_path_supported,
        "cpu_roles": "coordinator CPU0; consumer0 CPU1; consumer1 CPU2; publisher CPU3",
        "high_mode_threshold_mmsg_s": HIGH_MODE_THRESHOLD_MMSG_S,
        "sleep_schedule": (
            [{
                "name": f"fixed_{args.interval_seconds:g}s",
                "start_group": 0,
                "end_group_exclusive": args.groups,
                "sleep_seconds": args.interval_seconds,
            }]
            if args.interval_seconds is not None
            else [
                {
                    "name": name,
                    "start_group": start,
                    "end_group_exclusive": end,
                    "sleep_seconds": sleep_duration,
                }
                for name, start, end, sleep_duration in SLEEP_SCHEDULE
            ]
        ),
        "client": str(args.client),
        "client_sha256": digest(args.client),
        "client_size": args.client.stat().st_size,
        "client_mtime_ns": args.client.stat().st_mtime_ns,
        "environment_variables": list(getattr(args, "setenv", None) or []),
        "runner_sha256": digest(Path(__file__)),
        "files": {name: digest(path) for name, path in files.items()},
    }
    (output / "environment.json").write_text(json.dumps(environment, indent=2) + "\n")


def verify_environment(args, output):
    """拒绝在续跑分块中改变影响比较结果的参数。"""
    path = output / "environment.json"
    if not path.is_file():
        raise ValueError(f"Missing environment.json for resumed experiment: {output}")
    environment = json.loads(path.read_text())
    expected = {
        "groups": args.groups,
        "rounds_per_group": args.rounds,
        "messages_per_round": args.messages,
        "payload_bytes": args.payload,
        "capacity_bytes": args.capacity,
        "client_sha256": digest(args.client),
        "publish_path": getattr(args, "publish_path", "claim"),
        "diagnostic": getattr(args, "diagnostic", "off"),
        "environment_variables": list(getattr(args, "setenv", None) or []),
        "high_mode_threshold_mmsg_s": HIGH_MODE_THRESHOLD_MMSG_S,
    }
    mismatched = {
        key: (environment.get(key), value)
        for key, value in expected.items()
        if environment.get(key) != value
    }
    if args.interval_seconds is not None:
        schedule = environment.get("sleep_schedule", [])
        expected_schedule = [{
            "name": f"fixed_{args.interval_seconds:g}s",
            "start_group": 0,
            "end_group_exclusive": args.groups,
            "sleep_seconds": args.interval_seconds,
        }]
        if schedule != expected_schedule:
            mismatched["sleep_schedule"] = (schedule, expected_schedule)
    else:
        expected_schedule = [
            {
                "name": name,
                "start_group": start,
                "end_group_exclusive": end,
                "sleep_seconds": sleep_duration,
            }
            for name, start, end, sleep_duration in SLEEP_SCHEDULE
        ]
        if environment.get("sleep_schedule") != expected_schedule:
            mismatched["sleep_schedule"] = (
                environment.get("sleep_schedule"), expected_schedule
            )
    if environment.get("client") != str(args.client):
        mismatched["client_path"] = (environment.get("client"), str(args.client))
    if mismatched:
        raise ValueError(f"Resumed experiment environment differs: {mismatched}")


def write_failure(output, args, error):
    """把门禁中止写成机器可读状态，便于下一会话准确接续。"""
    records = [
        json.loads(line)
        for path in sorted(output.glob("group-*/results.jsonl"))
        for line in path.read_text().splitlines()
        if line.strip()
    ]
    failure = {
        "date": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "status": "aborted_baseline_admission",
        "reason": str(error),
        "completed_rounds": len(records),
        "expected_rounds": args.groups * args.rounds,
        "completed_groups": len(list(output.glob("group-*/throughputs.json"))),
        "expected_groups": args.groups,
        "max_consecutive_low": args.max_consecutive_low,
        "throughputs_mmsg_s": [record["publish_mmsg_s"] for record in records],
        "complete": False,
    }
    (output / "failure.json").write_text(json.dumps(failure, indent=2) + "\n")
    print(
        f"ABORTED: {failure['completed_rounds']}/{failure['expected_rounds']} rounds; "
        f"{failure['reason']}",
        flush=True,
    )


def write_checksums(output):
    """为已完成目录写入可验证的 SHA-256 清单。"""
    files = sorted(
        path for path in output.rglob("*")
        if path.is_file() and path.name != "SHA256SUMS"
        and path != output / "high-frequency-analysis.json"
    )
    (output / "SHA256SUMS").write_text(
        "".join(f"{digest(path)}  {path.relative_to(output)}\n" for path in files)
    )


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--messages", type=int, default=MESSAGES)
    parser.add_argument("--payload", type=int, default=PAYLOAD)
    parser.add_argument("--capacity", type=int, default=CAPACITY)
    parser.add_argument("--publish-path", choices=("claim", "offer"), default="claim")
    parser.add_argument("--diagnostic", choices=("off", "counters", "timed"), default="off")
    parser.add_argument(
        "--interval-seconds", type=float, default=None,
        help="use one fixed wait before every group instead of the historical schedule",
    )
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument(
        "--setenv", action="append", default=[], metavar="KEY=VALUE",
        help="extra environment variable for every benchmark round (repeatable)",
    )
    parser.add_argument("--groups", type=int, default=GROUPS, help=argparse.SUPPRESS)
    parser.add_argument("--rounds", type=int, default=ROUNDS, help=argparse.SUPPRESS)
    parser.add_argument(
        "--max-consecutive-low", type=int, default=3,
        help="abort if N leading rounds have no high mode (0 disables)",
    )
    parser.add_argument("--start-group", type=int, default=0,
                        help="first group in this chunk")
    parser.add_argument("--end-group", type=int, default=None,
                        help="exclusive last group in this chunk")
    parser.add_argument("--allow-existing", action="store_true",
                        help="append to a partial experiment")
    args = parser.parse_args()
    if args.end_group is None:
        args.end_group = args.groups
    if not 1 <= args.messages <= 100_000_000:
        parser.error("--messages must be in 1..100000000")
    if not 1 <= args.payload <= 1_048_576:
        parser.error("--payload must be in 1..1048576")
    if not 64 <= args.capacity <= 1 << 24 or args.capacity & (args.capacity - 1):
        parser.error("--capacity must be a power of two in 64..16777216")
    if args.interval_seconds is not None and not 0.0 <= args.interval_seconds <= 3600.0:
        parser.error("--interval-seconds must be in 0..3600")
    if not 10 <= args.timeout <= 600:
        parser.error("--timeout must be in 10..600 seconds")
    if not 1 <= args.groups <= GROUPS or not 1 <= args.rounds <= ROUNDS:
        parser.error(f"Use at most {GROUPS} groups and {ROUNDS} rounds")
    if args.max_consecutive_low < 0:
        parser.error("--max-consecutive-low must be non-negative")
    for assignment in args.setenv:
        key, separator, value = assignment.partition("=")
        if not separator or not key:
            parser.error(f"--setenv expects KEY=VALUE, got {assignment!r}")
        if any(character.isspace() for character in key):
            parser.error(f"--setenv key must not contain whitespace: {assignment!r}")
    if not 0 <= args.start_group < args.end_group <= args.groups:
        parser.error("Require 0 <= --start-group < --end-group <= --groups")
    args.consecutive_low = 0
    args.admission_highs = 0
    args.client = args.client.resolve()
    args.output = args.output.resolve()
    try:
        verify_client(args.client)
    except ValueError as exc:
        parser.error(str(exc))
    if args.publish_path != "claim" and not supports_publish_path(args.client):
        parser.error("--publish-path requires a client built with offer support")
    if args.diagnostic != "off" and not supports_diagnostic(args.client):
        parser.error("--diagnostic requires a client built with diagnostic support")
    if args.start_group == 0:
        if args.output.exists() and any(args.output.iterdir()):
            parser.error(f"Output directory is not empty: {args.output}")
    else:
        if not args.allow_existing:
            parser.error("--start-group > 0 requires --allow-existing")
        for group_idx in range(args.start_group):
            results = args.output / f"group-{group_idx:02d}" / "results.jsonl"
            count = len(results.read_text().splitlines()) if results.is_file() else 0
            if count != args.rounds:
                parser.error(f"Earlier group {group_idx} is incomplete: {count}/{args.rounds}")
        if (args.output / f"group-{args.start_group:02d}").exists():
            parser.error(f"Chunk start group already exists: {args.start_group}")
    return args


def main():
    args = parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    if args.start_group == 0:
        write_environment(args, args.output)
    else:
        if not args.allow_existing:
            raise ValueError("Resumed experiments require --allow-existing")
        try:
            verify_environment(args, args.output)
        except ValueError as error:
            raise SystemExit(str(error))

    print("=== High-Frequency Stability Test ===")
    print(f"Client: {args.client}")
    print(f"Output: {args.output}")
    chunk_size = args.end_group - args.start_group
    print(f"groups {args.start_group}..{args.end_group - 1}: {chunk_size} groups x "
          f"{args.rounds} rounds = {chunk_size * args.rounds} rounds in this chunk")
    print(flush=True)

    results_summary = []
    try:
        for group_idx in range(args.start_group, args.end_group):
            interval, sleep_duration = interval_for_group(
                group_idx, args.interval_seconds
            )
            if group_idx > 0 and sleep_duration:
                print(f"[Group {group_idx}] Sleeping {sleep_duration:g}s...", flush=True)
                time.sleep(sleep_duration)

            print(f"[Group {group_idx}] Running {args.rounds} rounds...", flush=True)
            group_output = args.output / f"group-{group_idx:02d}"
            group_output.mkdir(parents=True, exist_ok=True)
            records = []
            try:
                for round_idx in range(args.rounds):
                    record = run_round(args, group_idx, round_idx, group_output)
                    records.append(record)
                    print(
                        f"  r{round_idx:02d}: {record['publish_mmsg_s']:.3f} Mmsg/s "
                        f"({'H' if record['high_mode'] else 'L'})",
                        flush=True,
                    )
            except RuntimeError:
                if records:
                    group_summary = write_group_summary(
                        group_output, group_idx, interval, sleep_duration, records
                    )
                    results_summary.append(group_summary)
                    write_combined_summary(args.output, args)
                raise

            group_summary = write_group_summary(
                group_output, group_idx, interval, sleep_duration, records
            )
            results_summary.append(group_summary)
            print(
                f"Group {group_idx} ({interval}): median={group_summary['median']:.3f} Mmsg/s, "
                f"high={group_summary['high_mode_count']}/{args.rounds}",
                flush=True,
            )

            # Checkpoint after every group so an interrupted long run remains useful.
            write_combined_summary(args.output, args)
    except RuntimeError as error:
        write_failure(args.output, args, error)
        write_checksums(args.output)
        raise

    combined = write_combined_summary(args.output, args)
    if args.end_group != args.groups:
        write_checksums(args.output)
        print(f"Completed chunk: {args.output}", flush=True)
        return
    if len(combined) != args.groups:
        raise RuntimeError(f"Incomplete final matrix: {len(combined)}/{args.groups} groups")
    total_high = sum(group["high_mode_count"] for group in combined)
    total_rounds = args.groups * args.rounds
    print("\n=== Summary ===")
    print(f"Total high-mode rounds: {total_high}/{total_rounds} "
          f"({total_high * 100 / total_rounds:.1f}%)")
    if args.interval_seconds is not None:
        high = sum(group["high_mode_count"] for group in combined)
        total = len(combined) * args.rounds
        print(f"Fixed {args.interval_seconds:g}s: {high}/{total} "
              f"({high * 100 / total:.1f}%)")
    else:
        for name, start, end, sleep_duration in SLEEP_SCHEDULE:
            groups = combined[start:min(end, args.groups)]
            if not groups:
                continue
            high = sum(group["high_mode_count"] for group in groups)
            total = len(groups) * args.rounds
            print(f"Sleep {sleep_duration:2g}s ({name}): {high}/{total} "
                  f"({high * 100 / total:.1f}%)")

    write_checksums(args.output)

    analysis = subprocess.run(
        [
            "python3",
            str(ROOT / "tools/aeron_compare/analyze_high_frequency_stability.py"),
            str(args.output),
        ],
        cwd=ROOT,
        text=True,
    )
    if analysis.returncode:
        raise RuntimeError(f"Analysis failed ({analysis.returncode})")
    print(f"Completed: {args.output}", flush=True)


if __name__ == "__main__":
    main()
