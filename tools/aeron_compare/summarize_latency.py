#!/usr/bin/env python3
"""Validate latency runs and summarize per-run quantiles without pooling them."""

import argparse
from collections import defaultdict
import hashlib
import json
from pathlib import Path
from statistics import median


METRICS = ("p50", "p99", "p999")


def summarize(paths):
    groups = defaultdict(list)
    records = []
    inputs = []
    for path in paths:
        data = path.read_bytes()
        inputs.append({"path": str(path), "sha256": hashlib.sha256(data).hexdigest()})
        for line_number, line in enumerate(data.decode().splitlines(), 1):
            if not line.strip():
                continue
            row = json.loads(line)
            result = row["result"]
            count = row["rate"] * row["duration_s"]
            producers = row["producers"]
            expected_samples = producers * ((count // producers + 15) // 16)
            if (count % producers or int(result["published"]) != count or
                    int(result["delivered"]) != count or
                    int(result["latency_samples"]) != expected_samples or
                    int(result["consumers"]) != 1 or len(row["consumers"]) != 1 or
                    int(row["consumers"][0]["samples"]) != expected_samples or
                    row["origin"] != result["latency_origin"] or
                    row["origin"] not in ("claim", "attempt", "planned")):
                raise ValueError(f"Count or configuration mismatch: {path}:{line_number}")
            events = dict(item.split() for item in row["resources"]["memory.events"].splitlines())
            if any(int(events.get(name, 0)) for name in ("high", "max", "oom", "oom_kill")):
                raise ValueError(f"Resource pressure invalidates this run: {path}:{line_number}")
            records.append(row)
            if row["phase"] == "warmup" and row["round"] == 0:
                continue
            if row["phase"] != "measured" or row["round"] <= 0:
                raise ValueError(f"Invalid phase or round: {path}:{line_number}")
            key = (row["origin"], producers, row["payload"], row["rate"],
                   row["engine"], row["duration_s"])
            groups[key].append(row)

    summaries = []
    for key, rows in sorted(groups.items()):
        origin, producers, payload, rate, engine, seconds = key
        rows.sort(key=lambda row: row["round"])
        if [row["round"] for row in rows] != list(range(1, len(rows) + 1)):
            raise ValueError(f"Missing or duplicate rounds: {key}")
        values = {metric: [int(row["result"][f"latency_{metric}_ns"]) for row in rows]
                  for metric in METRICS}
        summaries.append({
            "origin": origin, "producers": producers, "consumers": 1,
            "payload": payload, "total_rate": rate, "engine": engine,
            "duration_s": seconds, "round_count": len(rows),
            "round_median_ns": {metric: median(values[metric]) for metric in METRICS},
            "round_min_ns": {metric: min(values[metric]) for metric in METRICS},
            "round_max_ns": {metric: max(values[metric]) for metric in METRICS},
            "sampled_max_ns": max(int(row["result"]["latency_max_ns"]) for row in rows),
            "sample_count": sum(int(row["result"]["latency_samples"]) for row in rows),
            "rounds": [{"round": row["round"],
                        "quantiles_ns": {metric: int(row["result"][f"latency_{metric}_ns"])
                                         for metric in METRICS},
                        "sampled_max_ns": int(row["result"]["latency_max_ns"])} for row in rows],
        })
    if not summaries:
        raise ValueError("No measured runs")
    measured = [row for row in records if row["phase"] == "measured"]
    return {
        "schema_version": 1, "inputs": inputs,
        "aggregation": "Median and range of independent run quantiles, NOT pooled quantiles",
        "all_run_count": len(records), "measured_run_count": len(measured),
        "measured_message_count": sum(int(row["result"]["published"]) for row in measured),
        "measured_sample_count": sum(int(row["result"]["latency_samples"]) for row in measured),
        "min_observed_mem_available_mib": min(row["min_mem_available_kib"] for row in records) / 1024,
        "max_observed_cgroup_peak_mib": max(int(row["resources"]["memory.peak"]) for row in records) / 1048576,
        "memory_pressure_events_zero": True, "groups": summaries,
    }


def markdown(summary):
    lines = ["# Latency Comparison", "",
             "All quantiles below are medians of per-run quantiles, not merged percentiles.",
             "Ranges span every measured run; maxima are sampled maxima. Units: microseconds.", ""]
    for origin in sorted({group["origin"] for group in summary["groups"]}):
        lines.extend([f"## {origin}", "",
                      "| P/S | Bytes | Total msg/s | Engine | Runs | p50 | p99 | p99.9 | p99.9 Range | Sampled Max |",
                      "|---|---:|---:|---|---:|---:|---:|---:|---:|---:|"])
        for group in summary["groups"]:
            if group["origin"] != origin:
                continue
            quantiles = group["round_median_ns"]
            low = group["round_min_ns"]["p999"] / 1000
            high = group["round_max_ns"]["p999"] / 1000
            lines.append(f"| {group['producers']}P1S | {group['payload']} | {group['total_rate']} | "
                         f"{group['engine']} | {group['round_count']} | "
                         + " | ".join(f"{quantiles[metric] / 1000:.3f}" for metric in METRICS)
                         + f" | {low:.3f}-{high:.3f} | {group['sampled_max_ns'] / 1000:.3f} |")
        lines.append("")
    lines.extend([f"Measured runs: {summary['measured_run_count']}; "
                  f"messages: {summary['measured_message_count']}; samples: {summary['measured_sample_count']}.",
                  f"Minimum observed MemAvailable: {summary['min_observed_mem_available_mib']:.1f} MiB; "
                  f"maximum observed cgroup peak: {summary['max_observed_cgroup_peak_mib']:.1f} MiB."])
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", type=Path, nargs="+")
    parser.add_argument("--json-output", type=Path)
    args = parser.parse_args()
    summary = summarize(args.inputs)
    if args.json_output:
        args.json_output.write_text(json.dumps(summary, indent=2) + "\n")
    print(markdown(summary), end="")


if __name__ == "__main__":
    main()
