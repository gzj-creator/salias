#!/usr/bin/env python3
"""Summarize the A.2 direct/192-B/192+128-B preload experiment."""

import argparse
import hashlib
import json
import math
from pathlib import Path
import statistics
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools/aeron_compare"))
from analyze_topology_sweep import refresh_checksums

MEASURED_PAYLOAD = 64
HIGH_MODE_THRESHOLD = 10.0


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_records(output):
    manifest = {}
    for line in (output / "SHA256SUMS").read_text().splitlines():
        checksum, separator, name = line.partition("  ")
        if not separator or len(checksum) != 64:
            raise ValueError(f"Invalid checksum line: {line}")
        if name != "SHA256SUMS":
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


def stats(values, threshold=None):
    if not values or any(not math.isfinite(value) for value in values):
        raise ValueError("Require finite throughput observations")
    mean = statistics.mean(values)
    stddev = statistics.stdev(values) if len(values) > 1 else None
    quartiles = (statistics.quantiles(values, n=4, method="inclusive")
                 if len(values) > 1 else [values[0]] * 3)
    result = {
        "rounds": len(values),
        "median_mmsg_s": statistics.median(values),
        "mean_mmsg_s": mean,
        "q1_mmsg_s": quartiles[0],
        "q3_mmsg_s": quartiles[2],
        "min_mmsg_s": min(values),
        "max_mmsg_s": max(values),
        "sample_stddev_mmsg_s": stddev,
        "cv": stddev / mean if stddev is not None and mean else None,
    }
    if threshold is not None:
        result["high_mode_threshold_mmsg_s"] = threshold
        result["high_mode_rounds"] = sum(value >= threshold for value in values)
        result["low_mode_rounds"] = sum(value < threshold for value in values)
    return result


def validate_records(records, environment):
    measured_rounds = int(environment["measured_rounds"])
    preload_rounds = int(environment["preload_rounds"])
    messages = int(environment["messages"])
    expected_groups = (
        ("group1-direct-64b", 0, measured_rounds),
        ("group2-192b-preload", preload_rounds, measured_rounds),
        ("group3-192b-128b-preload", 2 * preload_rounds, measured_rounds),
    )
    actual = {}
    for record in records:
        key = (record["group"], record["phase"], int(record["payload"]))
        actual[key] = actual.get(key, 0) + 1
        result = record["result"]
        checks = {
            "published": messages, "delivered": 2 * messages, "producers": 1,
            "consumers": 2, "payload": int(record["payload"]),
            "latency_samples": 0, "latency_sample_rate": 0, "rate_per_producer": 0,
        }
        if (result["scenario"] != "fifo" or
                any(int(result[key]) != int(value) for key, value in checks.items())):
            raise ValueError(f"Result metadata mismatch: {record['number']}")
    expected = {
        ("group1-direct-64b", "measured", 64): measured_rounds,
        ("group2-192b-preload", "preload", 192): preload_rounds,
        ("group2-192b-preload", "measured", 64): measured_rounds,
        ("group3-192b-128b-preload", "preload", 192): preload_rounds,
        ("group3-192b-128b-preload", "preload", 128): preload_rounds,
        ("group3-192b-128b-preload", "measured", 64): measured_rounds,
    }
    if actual != expected:
        raise ValueError(f"Unexpected group matrix: {actual}")
    return records


def summarize(output):
    environment = json.loads((output / "environment.json").read_text())
    records = validate_records(load_records(output), environment)
    groups = {}
    for group, _, _ in (("group1-direct-64b", 0, 0),
                        ("group2-192b-preload", 0, 0),
                        ("group3-192b-128b-preload", 0, 0)):
        group_records = [record for record in records if record["group"] == group]
        measured = [record for record in group_records if record["phase"] == "measured"]
        rates = [float(record["result"]["publish_msg_per_sec"]) / 1e6 for record in measured]
        preload = [record for record in group_records if record["phase"] == "preload"]
        groups[group] = {
            **stats(rates, HIGH_MODE_THRESHOLD),
            "preload_payloads": [int(record["payload"]) for record in preload],
            "preload_mmsg_s": [float(record["result"]["publish_msg_per_sec"]) / 1e6
                               for record in preload],
            "system_before_measured": [
                record["system_before"] for record in measured
            ],
            "measured_rates_mmsg_s": rates,
        }
    baseline = groups["group1-direct-64b"]["median_mmsg_s"]
    changes = {
        name: (group["median_mmsg_s"] - baseline) / baseline * 100
        for name, group in groups.items()
    }
    high_after_preload = any(group["high_mode_rounds"] > 0
                             for name, group in groups.items()
                             if name != "group1-direct-64b")
    conclusion = (
        "preload changes 64 B median by at least 10%" if any(
            abs(change) >= 10.0 for name, change in changes.items()
            if name != "group1-direct-64b")
        else "no clear preload effect on 64 B"
    )
    return {
        "schema_version": 1,
        "source": str(output.resolve()),
        "results_sha256": digest(output / "results.jsonl"),
        "environment_sha256": digest(output / "environment.json"),
        "measured_rounds_per_group": int(environment["measured_rounds"]),
        "preload_rounds": int(environment["preload_rounds"]),
        "groups": groups,
        "changes_from_direct_percent": changes,
        "decision": {
            "high_mode_threshold_mmsg_s": HIGH_MODE_THRESHOLD,
            "any_high_mode_after_preload": high_after_preload,
            "conclusion": conclusion,
        },
    }


def print_result(result):
    print(f"{'group':<30} {'rounds':>7} {'median':>12} {'Q1':>10} {'Q3':>10} "
          f"{'CV':>8} {'high':>5} {'low':>4}")
    for name, group in result["groups"].items():
        cv = "--" if group["cv"] is None else f"{group['cv']:.2%}"
        print(f"{name:<30} {group['rounds']:>7} {group['median_mmsg_s']:>12.3f} "
              f"{group['q1_mmsg_s']:>10.3f} {group['q3_mmsg_s']:>10.3f} {cv:>8} "
              f"{group['high_mode_rounds']:>5} {group['low_mode_rounds']:>4}")
    print("\n64 B median changes from direct baseline:")
    for name, change in result["changes_from_direct_percent"].items():
        if name != "group1-direct-64b":
            print(f"  {name}: {change:+.2f}%")
    print(f"conclusion: {result['decision']['conclusion']}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    output = args.output.resolve()
    result = summarize(output)
    path = output / "warmup-effect-analysis.json"
    path.write_text(json.dumps(result, indent=2) + "\n")
    refresh_checksums(output)
    print_result(result)
    print(f"\nwrote {path}")


if __name__ == "__main__":
    main()
