#!/usr/bin/env python3
"""Validate and summarize the Phase 1 1P2S payload sweep."""

import argparse
import hashlib
import json
import math
from pathlib import Path
import statistics
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools/aeron_compare"))
from analyze_frame_layout import analyze as analyze_layout
from analyze_frame_layout import source_constants
from analyze_topology_sweep import refresh_checksums


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def stats(values):
    if not values or any(not math.isfinite(value) for value in values):
        raise ValueError("Require finite throughput observations")
    sample_mean = statistics.mean(values)
    sample_stddev = statistics.stdev(values) if len(values) > 1 else None
    quartiles = (statistics.quantiles(values, n=4, method="inclusive")
                 if len(values) > 1 else [values[0]] * 3)
    return {
        "rounds": len(values),
        "median_mmsg_s": statistics.median(values),
        "mean_mmsg_s": sample_mean,
        "q1_mmsg_s": quartiles[0],
        "q3_mmsg_s": quartiles[2],
        "iqr_mmsg_s": quartiles[2] - quartiles[0],
        "min_mmsg_s": min(values),
        "max_mmsg_s": max(values),
        "sample_stddev_mmsg_s": sample_stddev,
        "cv": sample_stddev / sample_mean if sample_stddev is not None and sample_mean else None,
    }


def load_records(output):
    manifest = {}
    for line in (output / "SHA256SUMS").read_text().splitlines():
        checksum, separator, name = line.partition("  ")
        if not separator or len(checksum) != 64:
            raise ValueError(f"Invalid checksum line: {line}")
        manifest[name] = checksum
    records = []
    for line in (output / "results.jsonl").read_text().splitlines():
        record = json.loads(line)
        name = Path(record["stdout"]).name
        path = output / name
        if manifest.get(name) != digest(path) or record.get("stdout_sha256") != manifest[name]:
            raise ValueError(f"stdout checksum mismatch: {path}")
        records.append(record)
    if not records:
        raise ValueError("No results")
    return records


def validate_matrix(records, environment):
    payloads = tuple(int(value) for value in environment["payloads"])
    rounds = int(environment["rounds"])
    messages = int(environment["messages"])
    expected = {(payload, round_number) for payload in payloads for round_number in range(rounds + 1)}
    actual = {(int(record["payload"]), int(record["round"])) for record in records}
    if actual != expected or len(actual) != len(records):
        raise ValueError("Expected payload matrix from environment.json; missing or duplicate rounds")
    measured = []
    for record in records:
        result = record["result"]
        expected_fields = {
            "published": messages, "delivered": 2 * messages, "producers": 1,
            "consumers": 2, "payload": record["payload"], "latency_samples": 0,
            "latency_sample_rate": 0, "rate_per_producer": 0,
        }
        mismatched = any(int(result[key]) != int(value) for key, value in expected_fields.items())
        if result["scenario"] != "fifo" or mismatched:
            raise ValueError(f"Result metadata mismatch: {record['number']}")
        if record["phase"] == "warmup" and record["round"] == 0:
            continue
        if record["phase"] != "measured" or record["round"] == 0:
            raise ValueError(f"Invalid phase/round: {record['number']}")
        measured.append(record)
    if len(measured) != len(payloads) * rounds:
        raise ValueError("Unexpected warmup count")
    return measured


def summarize(output):
    environment = json.loads((output / "environment.json").read_text())
    records = load_records(output)
    measured = validate_matrix(records, environment)
    constants = source_constants()
    groups = {}
    for payload in (int(value) for value in environment["payloads"]):
        cell = [record for record in measured if int(record["payload"]) == payload]
        rates = [float(record["result"]["publish_msg_per_sec"]) / 1e6 for record in cell]
        layout = analyze_layout(payload, int(environment["messages"]), [1, 1],
                                constants["kHeaderSize"], constants["kFrameAlign"],
                                capacity=int(environment["capacity_bytes"]))
        groups[payload] = {
            **stats(rates),
            "frame_stride_bytes": layout["frame_stride_bytes"],
            "frame_stride_mod64": layout["frame_stride_mod64"],
            "frame_stride_cacheline_aligned": layout["frame_stride_cacheline_aligned"],
        }
    transitions = []
    payloads = [int(value) for value in environment["payloads"]]
    for previous, current in zip(payloads, payloads[1:]):
        old = groups[previous]["median_mmsg_s"]
        new = groups[current]["median_mmsg_s"]
        change = (new - old) / old if old else None
        transitions.append({
            "from_payload": previous, "to_payload": current,
            "relative_change": change,
            "relative_change_percent": change * 100 if change is not None else None,
            "breakpoint_over_50_percent": abs(change) > 0.50 if change is not None else False,
        })
    difference_56_64 = ((groups[56]["median_mmsg_s"] - groups[64]["median_mmsg_s"]) /
                        groups[64]["median_mmsg_s"] if 56 in groups and 64 in groups else None)
    breakpoints = [row for row in transitions if row["breakpoint_over_50_percent"]]
    if difference_56_64 is not None and difference_56_64 > 0.20:
        conclusion = "H2 supported: 56 B is more than 20% faster than 64 B"
    elif breakpoints:
        conclusion = "payload breakpoint detected"
    else:
        conclusion = "no clear payload breakpoint; alignment signal absent"
    return {
        "schema_version": 1,
        "source": str(output.resolve()),
        "results_sha256": digest(output / "results.jsonl"),
        "environment_sha256": digest(output / "environment.json"),
        "measured_runs": len(measured),
        "warmup_runs": len(records) - len(measured),
        "groups": {str(key): value for key, value in groups.items()},
        "transitions": transitions,
        "decision": {
            "breakpoints": breakpoints,
            "difference_56_vs_64_percent": difference_56_64 * 100
            if difference_56_64 is not None else None,
            "h2_supported": difference_56_64 is not None and difference_56_64 > 0.20,
            "conclusion": conclusion,
        },
    }


def print_table(result):
    print(f"{'payload':>8} {'stride':>7} {'aligned':>8} {'rounds':>7} {'median':>12} "
          f"{'Q1':>10} {'Q3':>10} {'CV':>8}")
    for payload, group in result["groups"].items():
        cv = "--" if group["cv"] is None else f"{group['cv']:.2%}"
        print(f"{payload + ' B':>8} {group['frame_stride_bytes']:>7} "
              f"{'yes' if group['frame_stride_cacheline_aligned'] else 'no':>8} "
              f"{group['rounds']:>7} {group['median_mmsg_s']:>12.3f} "
              f"{group['q1_mmsg_s']:>10.3f} {group['q3_mmsg_s']:>10.3f} {cv:>8}")
    print("\ntransitions:")
    for row in result["transitions"]:
        print(f"{row['from_payload']} -> {row['to_payload']} B: "
              f"{row['relative_change_percent']:+.2f}%"
              f"{' (breakpoint >50%)' if row['breakpoint_over_50_percent'] else ''}")
    print(f"\n56 B vs 64 B: {result['decision']['difference_56_vs_64_percent']:+.2f}%")
    print(f"conclusion: {result['decision']['conclusion']}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    result = summarize(args.output.resolve())
    path = args.output / "payload-analysis.json"
    path.write_text(json.dumps(result, indent=2) + "\n")
    refresh_checksums(args.output)
    print_table(result)
    print(f"\nwrote {path}")


if __name__ == "__main__":
    main()
