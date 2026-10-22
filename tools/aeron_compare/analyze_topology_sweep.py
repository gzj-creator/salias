#!/usr/bin/env python3
"""Validate and summarize a completed Phase 1 topology sweep."""

import argparse
import hashlib
import json
import math
from pathlib import Path
import statistics

ROOT = Path(__file__).resolve().parents[2]


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
    try:
        manifest = {}
        for line in (output / "SHA256SUMS").read_text().splitlines():
            checksum, separator, name = line.partition("  ")
            if not separator or len(checksum) != 64:
                raise ValueError(f"Invalid checksum line: {line}")
            manifest[name] = checksum
    except (FileNotFoundError, ValueError) as exc:
        raise ValueError(f"Unreadable SHA256SUMS: {output}") from exc
    records = []
    for line in (output / "results.jsonl").read_text().splitlines():
        record = json.loads(line)
        name = Path(record["stdout"]).name
        stdout = output / name
        if manifest.get(name) != digest(stdout) or record.get("stdout_sha256") != manifest[name]:
            raise ValueError(f"stdout checksum mismatch: {stdout}")
        if not stdout.read_text().splitlines():
            raise ValueError(f"Empty stdout: {stdout}")
        records.append(record)
    if not records:
        raise ValueError("No results")
    return records


def validate_matrix(records, environment):
    rounds = int(environment["rounds"])
    payloads = tuple(int(value) for value in environment["payloads"])
    topologies = tuple(environment["tested_topologies"])
    messages = int(environment["messages"])
    cells = {(topology, payload, round_number)
             for topology in topologies for payload in payloads
             for round_number in range(rounds + 1)}
    actual = {(record["topology"], int(record["payload"]), int(record["round"]))
              for record in records}
    if actual != cells or len(actual) != len(records):
        raise ValueError("Expected matrix from environment.json; missing or duplicate rounds")
    measured = []
    for record in records:
        result = record["result"]
        expected = {
            "published": messages,
            "delivered": messages * record["consumers"],
            "producers": 1,
            "consumers": record["consumers"],
            "payload": record["payload"],
            "latency_samples": 0,
            "latency_sample_rate": 0,
            "rate_per_producer": 0,
            "scenario": "fifo",
        }
        mismatched = any(
            result[key] != value if key == "scenario" else int(result[key]) != int(value)
            for key, value in expected.items()
        )
        if mismatched:
            raise ValueError(f"Result metadata mismatch: {record['number']}")
        if record["phase"] == "warmup" and record["round"] == 0:
            continue
        if record["phase"] != "measured" or record["round"] == 0:
            raise ValueError(f"Invalid phase/round: {record['number']}")
        measured.append(record)
    if len(measured) != len(records) - len(topologies) * len(payloads):
        raise ValueError("Unexpected warmup count")
    return measured


def relative_change(new, old):
    if not old:
        raise ValueError("Cannot calculate a relative change from zero throughput")
    return (new - old) / old


def summarize(output):
    environment = json.loads((output / "environment.json").read_text())
    records = load_records(output)
    measured = validate_matrix(records, environment)
    groups = {}
    for topology in environment["tested_topologies"]:
        for payload in (int(value) for value in environment["payloads"]):
            cell = [record for record in measured
                    if record["topology"] == topology and int(record["payload"]) == payload]
            rates = [float(record["result"]["publish_msg_per_sec"]) / 1e6
                     for record in cell]
            groups[f"{topology}:{payload}"] = stats(rates)
    transitions = []
    topologies = environment["tested_topologies"]
    for payload in (int(value) for value in environment["payloads"]):
        for previous, current in zip(topologies, topologies[1:]):
            old = groups[f"{previous}:{payload}"]
            new = groups[f"{current}:{payload}"]
            change = relative_change(new["median_mmsg_s"], old["median_mmsg_s"])
            transitions.append({
                "from": previous,
                "to": current,
                "payload": payload,
                "relative_change": change,
                "relative_change_percent": change * 100,
                "exceeds_30_percent_degradation": change < -0.30,
            })
    one_by_two = {row["payload"]: row for row in transitions
                  if row["from"] == "1P1S" and row["to"] == "1P2S"}
    p64 = one_by_two.get(64)
    if p64 is None:
        raise ValueError("A 1P1S to 1P2S transition at 64 B is required")
    rate64 = groups["1P1S:64"]["median_mmsg_s"]
    degradation64 = -p64["relative_change"]
    if rate64 <= 8.0:
        conclusion = "publisher-side bottleneck (1P1S itself is slow)"
    elif rate64 < 10.0:
        conclusion = "unclear: 64 B degradation crossed 30%, but 1P1S is below 10 Mmsg/s"
    elif degradation64 > 0.30:
        conclusion = "subscriber-scanner bottleneck candidate"
    else:
        conclusion = "unclear"
    return {
        "schema_version": 1,
        "source": str(output.resolve()),
        "results_sha256": digest(output / "results.jsonl"),
        "environment_sha256": digest(output / "environment.json"),
        "measured_runs": len(measured),
        "warmup_runs": len(records) - len(measured),
        "groups": groups,
        "transitions": transitions,
        "decision": {
            "p1_1s_64_median_mmsg_s": rate64,
            "p1_2s_64_median_mmsg_s": groups["1P2S:64"]["median_mmsg_s"],
            "one_to_two_64_relative_change_percent": p64["relative_change_percent"],
            "one_to_two_64_degradation_over_30_percent":
                p64["exceeds_30_percent_degradation"],
            "conclusion": conclusion,
        },
    }


def print_table(result):
    print(f"{'topology':<9} {'payload':>8} {'rounds':>7} {'median':>12} "
          f"{'Q1':>10} {'Q3':>10} {'CV':>8}")
    for key, group in result["groups"].items():
        topology, payload = key.split(":")
        cv = "--" if group["cv"] is None else f"{group['cv']:.2%}"
        print(f"{topology:<9} {payload + ' B':>8} {group['rounds']:>7} "
              f"{group['median_mmsg_s']:>12.3f} {group['q1_mmsg_s']:>10.3f} "
              f"{group['q3_mmsg_s']:>10.3f} {cv:>8}")
    print("\ntransitions:")
    for row in result["transitions"]:
        print(f"{row['from']} -> {row['to']} {row['payload']} B: "
              f"{row['relative_change_percent']:+.2f}%"
              f"{' (degradation >30%)' if row['exceeds_30_percent_degradation'] else ''}")
    decision = result["decision"]
    print(f"\nconclusion: {decision['conclusion']}")
    print(f"1P1S 64 B: {decision['p1_1s_64_median_mmsg_s']:.3f} Mmsg/s; "
          f"1P2S 64 B: {decision['p1_2s_64_median_mmsg_s']:.3f} Mmsg/s; "
          f"delta: {decision['one_to_two_64_relative_change_percent']:+.2f}%")


def refresh_checksums(output):
    manifest = output / "SHA256SUMS"
    entries = {}
    for line in manifest.read_text().splitlines():
        checksum, separator, name = line.partition("  ")
        if not separator:
            raise ValueError(f"Invalid checksum line: {line}")
        if name == manifest.name:
            continue
        entries[name] = checksum
    for path in sorted(output.iterdir()):
        if path.is_file() and path.name != manifest.name:
            entries[path.name] = digest(path)
    manifest.write_text("".join(f"{entries[name]}  {name}\n" for name in entries))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    result = summarize(args.output.resolve())
    path = args.output / "topology-analysis.json"
    path.write_text(json.dumps(result, indent=2) + "\n")
    refresh_checksums(args.output)
    print_table(result)
    print(f"\nwrote {path}")


if __name__ == "__main__":
    main()
