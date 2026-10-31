#!/usr/bin/env python3
"""Run the Channel API convergence ABBA matrix and preserve raw evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import platform
import statistics
import subprocess
import sys
from datetime import datetime, timezone


EVIDENCE = pathlib.Path(__file__).resolve().parent
FIXED_BENCH = pathlib.Path("build/release/bench/salias_fixed_channel_poll_compare")
GENERIC_BENCH = pathlib.Path("build/release/bench/salias_ipc_compare")


def sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def command_output(command: list[str], cwd: pathlib.Path | None = None) -> str:
    result = subprocess.run(
        command,
        cwd=cwd,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    return result.stdout.strip()


def parse_result(stdout: str) -> dict[str, str]:
    lines = [line for line in stdout.splitlines() if line.startswith("RESULT ")]
    if len(lines) != 1:
        raise RuntimeError(f"expected one RESULT line, found {len(lines)}")
    fields: dict[str, str] = {}
    for field in lines[0].split()[1:]:
        name, value = field.split("=", 1)
        fields[name] = value
    return fields


def configure_cases(before_root: pathlib.Path, after_root: pathlib.Path) -> dict[str, dict]:
    fixed_before = before_root / FIXED_BENCH
    fixed_after = after_root / FIXED_BENCH
    generic_before = before_root / GENERIC_BENCH
    generic_after = after_root / GENERIC_BENCH
    for path in (fixed_before, fixed_after, generic_before, generic_after):
        if not path.is_file() or not os.access(path, os.X_OK):
            raise RuntimeError(f"benchmark executable is missing: {path}")

    generic_common = [
        "--scenario", "fifo",
        "--messages", "2000000",
        "--producers", "1",
        "--consumers", "1",
        "--payload", "64",
        "--capacity", "1048576",
        "--batch-size", "1",
        "--poll-limit", "64",
        "--verify", "full",
        "--cpu-base", "1",
        "--cpu-stride", "1",
    ]
    return {
        "fixed-throughput": {
            "description": "FixedChannel SPSC 64 B saturated poll throughput",
            "executables": {"before": fixed_before, "after": fixed_after},
            "arguments": [
                "--mode", "poll",
                "--workload", "throughput",
                "--messages", "20000000",
                "--producer-cpu", "2",
                "--consumer-cpu", "3",
                "--poll-batch", "64",
            ],
            "metrics": {"mmsg_s": "Mmsg/s"},
            "validate": lambda row: (
                int(row["verified"]) == 20_000_000 and float(row["mmsg_s"]) > 0
            ),
        },
        "fixed-planned-latency": {
            "description": "FixedChannel SPSC 64 B planned 1 Mmsg/s latency",
            "executables": {"before": fixed_before, "after": fixed_after},
            "arguments": [
                "--mode", "poll",
                "--workload", "latency",
                "--origin", "planned",
                "--rate", "1000000",
                "--seconds", "2",
                "--sample-rate", "16",
                "--producer-cpu", "2",
                "--consumer-cpu", "3",
                "--poll-batch", "64",
            ],
            "metrics": {"mmsg_s": "Mmsg/s", "p50_ns": "ns", "p99_ns": "ns"},
            "validate": lambda row: (
                int(row["verified"]) == 2_000_000
                and int(row["samples"]) == 125_000
                and float(row["mmsg_s"]) > 0
            ),
        },
        "generic-throughput": {
            "description": "Channel<FifoMpsc> 1P1C 64 B saturated full-verify throughput",
            "executables": {"before": generic_before, "after": generic_after},
            "arguments": [*generic_common, "--rate", "0", "--latency-sample-rate", "0"],
            "metrics": {"delivery_msg_per_sec": "msg/s"},
            "validate": lambda row: (
                int(row["delivered"]) == 2_000_000
                and int(row["verified"]) == 2_000_000
                and int(row["latency_samples"]) == 0
            ),
        },
        "generic-planned-latency": {
            "description": "Channel<FifoMpsc> 1P1C 64 B planned 1 Mmsg/s latency",
            "executables": {"before": generic_before, "after": generic_after},
            "arguments": [
                *generic_common,
                "--rate", "1000000",
                "--latency-sample-rate", "16",
                "--latency-origin", "planned",
            ],
            "metrics": {
                "delivery_msg_per_sec": "msg/s",
                "latency_p50_ns": "ns",
                "latency_p99_ns": "ns",
            },
            "validate": lambda row: (
                int(row["delivered"]) == 2_000_000
                and int(row["verified"]) == 2_000_000
                and int(row["latency_samples"]) == 125_000
            ),
        },
    }


def load_average() -> str:
    try:
        return pathlib.Path("/proc/loadavg").read_text(encoding="utf-8").strip()
    except OSError:
        return "unavailable"


def mem_available_kib() -> str:
    try:
        for line in pathlib.Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
            if line.startswith("MemAvailable:"):
                return line.split()[1]
    except OSError:
        pass
    return "unavailable"


def run_once(
    case_name: str,
    arm: str,
    executable: pathlib.Path,
    arguments: list[str],
    output: pathlib.Path,
) -> dict:
    stdout_path = output
    stderr_path = output.with_suffix(".stderr")
    relative = stdout_path.relative_to(EVIDENCE)
    command = [str(executable), *arguments]
    before = {"loadavg": load_average(), "mem_available_kib": mem_available_kib()}
    result = subprocess.run(
        command,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=180,
        env={**os.environ, "LC_ALL": "C", "MALLOC_ARENA_MAX": "2"},
    )
    stdout_path.parent.mkdir(parents=True, exist_ok=True)
    stdout_path.write_text(result.stdout, encoding="utf-8")
    stderr_path.write_text(result.stderr, encoding="utf-8")
    record = {
        "case": case_name,
        "arm": arm,
        "command": command,
        "returncode": result.returncode,
        "environment": {
            "before": before,
            "after": {"loadavg": load_average(), "mem_available_kib": mem_available_kib()},
        },
        "stdout_path": str(relative),
        "stderr_path": str(relative.with_suffix(".stderr")),
    }
    if result.returncode != 0:
        raise RuntimeError(f"{case_name} {arm} failed: {result.stderr}")
    record["result"] = parse_result(result.stdout)
    return record


def summarize(values: list[float]) -> dict[str, float | int]:
    return {
        "samples": len(values),
        "median": statistics.median(values),
        "min": min(values),
        "max": max(values),
    }


def build_summary(records: list[dict], cases: dict[str, dict], groups: int) -> dict:
    summary: dict = {
        "order": "ABBA",
        "groups": groups,
        "runs_per_arm_per_case": groups * 2,
        "cases": {},
    }
    for case_name, case in cases.items():
        case_records = [record for record in records if record["case"] == case_name]
        case_summary: dict = {"description": case["description"], "metrics": {}}
        for metric, unit in case["metrics"].items():
            arm_values = {
                arm: [float(record["result"][metric]) for record in case_records if record["arm"] == arm]
                for arm in ("before", "after")
            }
            if len(arm_values["before"]) != groups * 2 or len(arm_values["after"]) != groups * 2:
                raise RuntimeError(f"incomplete samples for {case_name} {metric}")
            paired_ratios = []
            for group in range(1, groups + 1):
                pair = [
                    float(record["result"][metric])
                    for record in case_records
                    if int(record["group"]) == group
                ]
                if len(pair) != 4:
                    raise RuntimeError(f"incomplete ABBA group {group} for {case_name} {metric}")
                before = statistics.median([pair[0], pair[3]])
                after = statistics.median([pair[1], pair[2]])
                paired_ratios.append(after / before)
            case_summary["metrics"][metric] = {
                "unit": unit,
                "before": summarize(arm_values["before"]),
                "after": summarize(arm_values["after"]),
                "paired_after_over_before": summarize(paired_ratios),
            }
        summary["cases"][case_name] = case_summary
    return summary


def write_environment(before_root: pathlib.Path, after_root: pathlib.Path) -> None:
    lscpu = command_output(["lscpu"])
    environment = "\n".join(
        [
            f"captured_utc={datetime.now(timezone.utc).isoformat()}",
            f"hostname={platform.node()}",
            f"kernel={platform.release()}",
            f"python={platform.python_version()}",
            f"compiler={command_output(['c++', '--version']).splitlines()[0]}",
            f"cmake={command_output(['cmake', '--version']).splitlines()[0]}",
            f"ninja={command_output(['ninja', '--version'])}",
            f"loadavg={load_average()}",
            f"memtotal_kib={pathlib.Path('/proc/meminfo').read_text().splitlines()[0].split()[1]}",
            "",
            "## lscpu",
            lscpu,
            "",
            "## free -h",
            command_output(["free", "-h"]),
            "",
        ]
    )
    (EVIDENCE / "environment.txt").write_text(environment, encoding="utf-8")

    binaries = [FIXED_BENCH, GENERIC_BENCH]
    lines = [
        f"before_root={before_root}",
        "before_revision=" + command_output(["git", "rev-parse", "HEAD"], before_root),
        "before_describe=" + command_output(["git", "describe", "--always", "--tags", "--dirty"], before_root),
        f"after_root={after_root}",
        "after_revision=" + command_output(["git", "rev-parse", "HEAD"], after_root),
        "after_describe=" + command_output(["git", "describe", "--always", "--tags", "--dirty"], after_root),
        "wire_protocol_version=" + command_output(
            ["rg", "-o", "kNamedVersion = [0-9]+", "src/salias/src/channel.cpp"], after_root
        ).split("= ")[-1],
        "",
        "## source and binary sha256",
    ]
    for root_label, root in (("before", before_root), ("after", after_root)):
        for relative in (
            pathlib.Path("bench/fixed_channel_poll_compare.cpp"),
            pathlib.Path("bench/hotpath_ab.cpp"),
            pathlib.Path("tools/aeron_compare/salias_ipc_compare.cpp"),
            pathlib.Path("tools/aeron_compare/latency_histogram.hpp"),
            pathlib.Path("tools/aeron_compare/latency_pacing.hpp"),
            FIXED_BENCH,
            GENERIC_BENCH,
        ):
            lines.append(f"{sha256(root / relative)}  {root_label}/{relative}")
    (EVIDENCE / "source-state.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_sha256sums() -> None:
    paths = sorted(
        path
        for path in EVIDENCE.rglob("*")
        if path.is_file() and path.name != "SHA256SUMS"
    )
    lines = [f"{sha256(path)}  {path.relative_to(EVIDENCE)}" for path in paths]
    (EVIDENCE / "SHA256SUMS").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before-root", type=pathlib.Path, required=True)
    parser.add_argument("--after-root", type=pathlib.Path, required=True)
    parser.add_argument("--groups", type=int, default=5)
    args = parser.parse_args()
    if args.groups < 1:
        parser.error("--groups must be positive")

    before_root = args.before_root.resolve()
    after_root = args.after_root.resolve()
    cases = configure_cases(before_root, after_root)
    records: list[dict] = []
    (EVIDENCE / "results.jsonl").unlink(missing_ok=True)

    for case_name, case in cases.items():
        for arm in ("before", "after"):
            warmup_output = EVIDENCE / "raw" / case_name / f"warmup-{arm}.stdout"
            run_once(
                case_name,
                arm,
                case["executables"][arm],
                case["arguments"],
                warmup_output,
            )
        for group in range(1, args.groups + 1):
            for position, arm in enumerate(("before", "after", "after", "before"), start=1):
                output = EVIDENCE / "raw" / case_name / f"group-{group:02d}-{position}-{arm}.stdout"
                record = run_once(
                    case_name,
                    arm,
                    case["executables"][arm],
                    case["arguments"],
                    output,
                )
                if not case["validate"](record["result"]):
                    raise RuntimeError(f"result invariant failed: {record['stdout_path']}")
                record["group"] = group
                record["position"] = position
                records.append(record)
                print(
                    f"{case_name} group={group} position={position} arm={arm} "
                    f"{json.dumps(record['result'], sort_keys=True)}",
                    flush=True,
                )
                with (EVIDENCE / "results.jsonl").open("a", encoding="utf-8") as stream:
                    stream.write(json.dumps(record, sort_keys=True) + "\n")

    summary = build_summary(records, cases, args.groups)
    (EVIDENCE / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    write_environment(before_root, after_root)
    write_sha256sums()
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    sys.exit(main())
