#!/usr/bin/env python3
"""Analyze the independent 192-byte replication against the P1.2 result."""

import argparse
import hashlib
import json
import math
from pathlib import Path
import statistics

PAYLOAD = 192


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


def validate_matrix(records, environment):
    rounds = int(environment["rounds"])
    messages = int(environment["messages"])
    expected = set(range(rounds + 1))
    actual = {int(record["round"]) for record in records}
    if actual != expected or len(actual) != len(records):
        raise ValueError("Expected complete round sequence; missing or duplicate rounds")
    measured = []
    for record in records:
        if int(record["payload"]) != PAYLOAD:
            raise ValueError(f"Unexpected payload: {record['number']}")
        result = record["result"]
        checks = {
            "published": messages, "delivered": 2 * messages, "producers": 1,
            "consumers": 2, "payload": PAYLOAD, "latency_samples": 0,
            "latency_sample_rate": 0, "rate_per_producer": 0,
        }
        if (result["scenario"] != "fifo" or
                any(int(result[key]) != int(value) for key, value in checks.items())):
            raise ValueError(f"Result metadata mismatch: {record['number']}")
        if record["phase"] == "warmup" and record["round"] == 0:
            continue
        if record["phase"] != "measured" or record["round"] == 0:
            raise ValueError(f"Invalid phase/round: {record['number']}")
        measured.append(record)
    if len(measured) != rounds:
        raise ValueError("Unexpected warmup count")
    return measured


def stats(values):
    mean = statistics.mean(values)
    stddev = statistics.stdev(values) if len(values) > 1 else None
    quartiles = (statistics.quantiles(values, n=4, method="inclusive")
                 if len(values) > 1 else [values[0]] * 3)
    return {
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


def comparison(previous, current):
    return {
        "p1_2_median_mmsg_s": previous["median_mmsg_s"],
        "verification_median_mmsg_s": current["median_mmsg_s"],
        "median_change_percent": (current["median_mmsg_s"] - previous["median_mmsg_s"]) /
        previous["median_mmsg_s"] * 100,
        "p1_2_cv": previous["cv"],
        "verification_cv": current["cv"],
    }


def summarize(output, p1_output=None):
    environment = json.loads((output / "environment.json").read_text())
    records = load_records(output)
    measured = validate_matrix(records, environment)
    rates = [float(record["result"]["publish_msg_per_sec"]) / 1e6 for record in measured]
    current = stats(rates)
    median = current["median_mmsg_s"]
    outliers = [{
        "round": record["round"],
        "mmsg_s": rate,
        "absolute_deviation_percent": abs(rate - median) / median * 100,
    } for record, rate in zip(measured, rates)
        if abs(rate - median) / median > 0.50]
    previous = None
    if p1_output is not None:
        payload_analysis = json.loads((p1_output / "payload-analysis.json").read_text())
        previous = payload_analysis["groups"]["192"]
    reproducible = (16.0 <= median <= 18.0 and current["cv"] is not None and
                    current["cv"] < 0.05 and not outliers)
    return {
        "schema_version": 1,
        "source": str(output.resolve()),
        "results_sha256": digest(output / "results.jsonl"),
        "environment_sha256": digest(output / "environment.json"),
        "measured_runs": len(measured),
        "warmup_runs": len(records) - len(measured),
        "statistics": current,
        "outliers": outliers,
        "p1_2_comparison": comparison(previous, current) if previous is not None else None,
        "decision": {
            "reproduced_16_to_18_mmsg_s": reproducible,
            "stable_cv_below_5_percent": current["cv"] is not None and current["cv"] < 0.05,
            "outlier_rounds": len(outliers),
            "conclusion": ("192 B high-throughput region reproduced" if reproducible else
                           "192 B did not reproduce as a stable high-throughput region"),
        },
    }


def print_result(result):
    current = result["statistics"]
    cv = "--" if current["cv"] is None else f"{current['cv']:.2%}"
    print(f"192 B measured rounds: {current['rounds']}")
    print(f"median={current['median_mmsg_s']:.3f} Mmsg/s; "
          f"Q1={current['q1_mmsg_s']:.3f}; Q3={current['q3_mmsg_s']:.3f}; "
          f"min={current['min_mmsg_s']:.3f}; max={current['max_mmsg_s']:.3f}; "
          f"CV={cv}")
    print(f"outliers (>50% from median): {len(result['outliers'])}")
    for row in result["outliers"]:
        print(f"  round {row['round']}: {row['mmsg_s']:.3f} Mmsg/s "
              f"({row['absolute_deviation_percent']:.2f}% deviation)")
    if result["p1_2_comparison"]:
        row = result["p1_2_comparison"]
        print(f"P1.2 median={row['p1_2_median_mmsg_s']:.3f}; verification median="
              f"{row['verification_median_mmsg_s']:.3f}; change="
              f"{row['median_change_percent']:+.2f}%")
    print(f"conclusion: {result['decision']['conclusion']}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--p1-output", type=Path)
    args = parser.parse_args()
    result = summarize(args.output.resolve(), args.p1_output.resolve() if args.p1_output else None)
    path = args.output / "verify-192b-analysis.json"
    path.write_text(json.dumps(result, indent=2) + "\n")
    from analyze_topology_sweep import refresh_checksums
    refresh_checksums(args.output)
    print_result(result)
    print(f"\nwrote {path}")


if __name__ == "__main__":
    main()
