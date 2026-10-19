#!/usr/bin/env python3
"""Validate and preserve compact diagnostic evidence separately from build artifacts."""

import argparse
import hashlib
import json
from pathlib import Path
import shutil
import statistics
import subprocess

from diagnose_design import summarize
from run_latency_compare import fields


def export(source, output):
    output.mkdir(parents=True, exist_ok=True)
    inputs = []
    groups = {}
    run_count = messages = samples = 0
    for dataset in ("clean-final", "polling-formal", "confirm", "stage-measurements", "trace-clean"):
        path = source / dataset / "results.jsonl"
        rows = [json.loads(line) for line in path.read_text().splitlines()]
        for row in rows:
            result = row["result"]
            if (int(result["published"]) != row["rate"] * 2 and dataset != "trace-clean"):
                raise ValueError(f"unexpected count in {path}")
            if int(result["delivered"]) != int(result["published"]):
                raise ValueError(f"delivery mismatch in {path}")
            if int(result["latency_samples"]) != (int(result["published"]) + 15) // 16:
                raise ValueError(f"sample mismatch in {path}")
            counters = dict(line.split() for line in row["resources"]["memory.events"].splitlines())
            if any(int(counters.get(key, "0")) for key in ("high", "max", "oom", "oom_kill")):
                raise ValueError(f"memory pressure in {path}")
            if row["phase"] == "measured":
                run_count += 1
                messages += int(result["published"])
                samples += int(result["latency_samples"])
        shutil.copyfile(path, output / f"{dataset}.jsonl")
        inputs.append(dict(dataset=dataset, sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                           rows=len(rows), measured_rows=sum(row["phase"] == "measured" for row in rows)))
        groups[dataset] = summarize(rows)
    stage_rows = []
    for row in map(json.loads, (source / "stage-measurements/results.jsonl").read_text().splitlines()):
        if row["phase"] != "measured":
            continue
        for line in Path(row["stdout"]).read_text().splitlines():
            if line.startswith("STAGE "):
                stage_rows.append(dict(variant=row["variant"], round=row["round"], **fields(line)))
    stage_summary = []
    for variant in ("baseline", "aeron"):
        for stage in ("claim", "precommit", "handoff"):
            rows = [row for row in stage_rows if row["variant"] == variant and row["stage"] == stage]
            stage_summary.append(dict(variant=variant, stage=stage,
                p50_ns=statistics.median(int(row["p50_ns"]) for row in rows),
                mean_ns=sum(int(row["sum_ns"]) for row in rows) / sum(int(row["samples"]) for row in rows),
                rounds=rows))
    (output / "stages.json").write_text(json.dumps(stage_summary, indent=2) + "\n")
    shutil.copyfile(source / "trace-clean/switch-analysis.json", output / "switch-analysis.json")
    profiles = output / "profiles"
    profiles.mkdir(exist_ok=True)
    for pattern in ("*.svg", "*.top.json", "*.report.txt", "*.stderr", "*.stdout", "*.folded"):
        for path in (source / "profiles-final").glob(pattern):
            shutil.copyfile(path, profiles / path.name)
    flamegraph_commit = subprocess.check_output(["git", "-C", str(source / "FlameGraph"), "rev-parse", "HEAD"], text=True).strip()
    manifest = dict(schema_version=1, aggregation="Median/range of independent run quantiles, NOT pooled percentiles",
                    source_root=str(source), inputs=inputs, measured_runs=run_count,
                    measured_messages=messages, measured_latency_samples=samples,
                    minimum_observed_available_mib=min(
                        row["min_mem_available_kib"] / 1024 for dataset in groups
                        for row in map(json.loads, (source / dataset / "results.jsonl").read_text().splitlines())),
                    flamegraph_commit=flamegraph_commit, groups=groups)
    (output / "summary.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps({key: value for key, value in manifest.items() if key != "groups"}, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    export(args.source, args.output)


if __name__ == "__main__":
    main()
