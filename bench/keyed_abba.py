#!/usr/bin/env python3
"""Run the fixed Keyed 4W workload with A/A and ABBA checks.

The executable is invoked once per sample and writes one Google Benchmark JSON result.  The
benchmark itself owns the 8000-update load; this driver only orders runs and records evidence.  A/A
is checked before a baseline/candidate comparison so a noisy machine cannot be reported as a
regression or improvement.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import statistics
import subprocess
import tempfile
import time
from typing import Any


def coefficient_of_variation(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = statistics.fmean(values)
    return statistics.stdev(values) / mean if mean else math.inf


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    if path.is_dir():
        for child in sorted(p for p in path.rglob("*") if p.is_file()):
            digest.update(str(child.relative_to(path)).encode())
            with child.open("rb") as stream:
                for block in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(block)
    else:
        with path.open("rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(block)
    return digest.hexdigest()


def run_once(executable: Path, benchmark_filter: str, cpus: str, timeout: float) -> dict[str, Any]:
    with tempfile.NamedTemporaryFile(prefix="salias-keyed-", suffix=".json", delete=False) as output:
        output_path = Path(output.name)
    command = [
        str(executable),
        f"--benchmark_filter={benchmark_filter}",
        "--benchmark_min_time=0.001s",
        "--benchmark_repetitions=1",
        f"--benchmark_out={output_path}",
        "--benchmark_out_format=json",
        "--benchmark_report_aggregates_only=true",
    ]
    environment = os.environ.copy()
    if cpus:
        environment["SALIAS_BENCH_CPUS"] = cpus
    started = time.monotonic()
    try:
        result = subprocess.run(
            command,
            text=True,
            capture_output=True,
            env=environment,
            timeout=timeout,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"{executable} exited {result.returncode}: {result.stdout}\n{result.stderr}"
            )
        report = json.loads(output_path.read_text(encoding="utf-8"))
        rows = [row for row in report.get("benchmarks", []) if row.get("run_type") == "iteration"]
        if len(rows) != 1:
            raise RuntimeError(f"expected one keyed benchmark row, got {len(rows)}")
        row = dict(rows[0])
        row["driver_command"] = command
        row["driver_elapsed_s"] = time.monotonic() - started
        return row
    finally:
        output_path.unlink(missing_ok=True)


def check_aa(samples: list[float], label: str) -> dict[str, Any]:
    if len(samples) < 2:
        return {"label": label, "samples": samples, "cv": 0.0, "median": samples[0] if samples else 0.0}
    first = samples[::2]
    second = samples[1::2]
    cv = coefficient_of_variation(samples)
    ratio = statistics.median(second) / statistics.median(first) if statistics.median(first) else math.inf
    return {
        "label": label,
        "samples": samples,
        "cv": cv,
        "median": statistics.median(samples),
        "paired_median_ratio": ratio,
        "pass": cv <= 0.10 and 0.95 <= ratio <= 1.05,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", type=Path, required=True, help="frozen baseline benchmark binary")
    parser.add_argument("--after", type=Path, required=True, help="candidate benchmark binary")
    parser.add_argument("--output", type=Path, required=True, help="raw JSON evidence path")
    parser.add_argument("--readers", type=int, choices=(1, 4), default=4)
    parser.add_argument("--retry", choices=("LegacyRetry", "AdaptiveRetry"), default="LegacyRetry")
    parser.add_argument(
        "--topology",
        action="append",
        default=[],
        metavar="NAME=CPU_LIST",
        help="repeat for each topology, e.g. 2core=0,1 and 4core=0,1,2,3",
    )
    parser.add_argument("--aa-samples", type=int, default=5)
    parser.add_argument("--samples", type=int, default=20)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--source", type=Path, help="source tree to hash into the evidence")
    args = parser.parse_args()
    if args.aa_samples < 2 or args.samples < 2 or args.timeout <= 0:
        parser.error("aa-samples and samples must be at least two, and timeout must be positive")
    for executable in (args.before, args.after):
        if not executable.is_file() or not os.access(executable, os.X_OK):
            parser.error(f"benchmark is not executable: {executable}")
    if args.source and not args.source.exists():
        parser.error(f"source path does not exist: {args.source}")

    topologies = []
    for item in args.topology or ["unbound="]:
        if "=" not in item:
            parser.error(f"topology must be NAME=CPU_LIST: {item}")
        name, cpus = item.split("=", 1)
        if not name:
            parser.error("topology name cannot be empty")
        topologies.append((name, cpus))

    benchmark_filter = f"BM_keyed_concurrent_64b<{args.readers}, RetryMode::{args.retry}>"
    evidence: dict[str, Any] = {
        "benchmark_filter": benchmark_filter,
        "readers": args.readers,
        "writers": 4,
        "updates_per_writer": 2000,
        "retry": args.retry,
        "before": {"path": str(args.before.resolve()), "sha256": sha256(args.before)},
        "after": {"path": str(args.after.resolve()), "sha256": sha256(args.after)},
        "topologies": {},
    }
    if args.source:
        evidence["source"] = {"path": str(args.source.resolve()), "sha256": sha256(args.source)}

    overall_ok = True
    for topology, cpus in topologies:
        # A/A noise gate uses the same binary and the same CPU assignment.  Samples are interleaved
        # into two groups to expose drift as well as ordinary run-to-run variation.
        aa_rows = [run_once(args.after, benchmark_filter, cpus, args.timeout) for _ in range(args.aa_samples * 2)]
        aa_times = [float(row["real_time"]) for row in aa_rows]
        aa = check_aa(aa_times, f"{topology}/after")
        if not aa.get("pass", False):
            overall_ok = False

        order: list[str] = []
        rows: list[dict[str, Any]] = []
        # ABBA blocks keep thermal and scheduler drift visible while yielding exactly `samples`
        # observations per version when samples is divisible by two.
        blocks = (args.samples + 1) // 2
        for _ in range(blocks):
            for version, executable in (("before", args.before), ("after", args.after),
                                         ("after", args.after), ("before", args.before)):
                order.append(version)
                row = run_once(executable, benchmark_filter, cpus, args.timeout)
                row["version"] = version
                rows.append(row)
        before = [float(row["real_time"]) for row in rows if row["version"] == "before"][: args.samples]
        after = [float(row["real_time"]) for row in rows if row["version"] == "after"][: args.samples]
        before_median = statistics.median(before)
        after_median = statistics.median(after)
        comparison = {
            "order": order,
            "before_samples": before,
            "after_samples": after,
            "before_median": before_median,
            "after_median": after_median,
            "after_over_before": after_median / before_median if before_median else math.inf,
            "before_cv": coefficient_of_variation(before),
            "after_cv": coefficient_of_variation(after),
            "aa": aa,
            "status": "indeterminate_aa_noise" if not aa.get("pass", False) else "measured",
            "rows": rows,
        }
        evidence["topologies"][topology] = comparison

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
    for topology, result in evidence["topologies"].items():
        print(
            f"{topology}: AA={'PASS' if result['aa'].get('pass') else 'FAIL'} "
            f"before={result['before_median']:.3f} after={result['after_median']:.3f} "
            f"ratio={result['after_over_before']:.4f} status={result['status']}"
        )
    return 0 if overall_ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
