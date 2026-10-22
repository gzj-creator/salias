#!/usr/bin/env python3
"""Split alternating E1 latency runs by measured client identity.

The standard latency summarizer groups both clients together. E1 needs the
same per-run medians, but separated by the client hash recorded in each row so
an alternating B/C run cannot be mistaken for one arm.
"""

import argparse
import hashlib
import json
from pathlib import Path
import statistics


METRICS = ("p50", "p99", "p999")


class LatencyABError(RuntimeError):
    """Raised when an alternating latency record set is inconsistent."""


def load(path, expected):
    """Read one results file and verify every row against its client hash."""
    path = Path(path)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    rows = []
    for line_number, line in enumerate(path.read_text().splitlines(), 1):
        if not line.strip():
            continue
        row = json.loads(line)
        if row.get("engine") != "fifo":
            raise LatencyABError(f"{path}:{line_number}: engine is not fifo")
        if row.get("arm") not in expected:
            raise LatencyABError(f"{path}:{line_number}: unknown arm {row.get('arm')}")
        if row.get("client_sha256") != expected[row["arm"]]:
            raise LatencyABError(f"{path}:{line_number}: client hash does not match arm")
        events = dict(item.split() for item in row["resources"]["memory.events"].splitlines())
        if any(int(events.get(name, 0)) for name in ("high", "max", "oom", "oom_kill")):
            raise LatencyABError(f"{path}:{line_number}: memory pressure event")
        rows.append(row)
    return digest, rows


def summarize(rows, baseline_sha, candidate_sha, rounds=10):
    """Group measured rows by origin, payload and arm using run medians."""
    required_rounds = rounds
    groups = []
    for origin in sorted({row["origin"] for row in rows}):
        for payload in sorted({row["payload"] for row in rows
                               if row["origin"] == origin}):
            for arm, sha in (("B", baseline_sha), ("C", candidate_sha)):
                arm_rows = sorted(
                    (row for row in rows
                     if row["origin"] == origin and row["payload"] == payload
                     and row["arm"] == arm and row["phase"] == "measured"),
                    key=lambda row: row["round"],
                )
                observed_rounds = [row["round"] for row in arm_rows]
                if observed_rounds != list(range(1, required_rounds + 1)):
                    raise LatencyABError(
                        f"{origin}/{payload}/{arm} does not contain measured rounds "
                        f"1..{required_rounds}"
                    )
                quantiles = {
                    metric: statistics.median(
                        int(row["result"][f"latency_{metric}_ns"]) for row in arm_rows
                    )
                    for metric in METRICS
                }
                groups.append({
                    "origin": origin,
                    "payload": payload,
                    "arm": arm,
                    "client_sha256": sha,
                    "round_count": len(arm_rows),
                    "published_messages": sum(int(row["result"]["published"])
                                              for row in arm_rows),
                    "throughput_median_mmsg_s": statistics.median(
                        float(row["result"]["publish_msg_per_sec"]) / 1e6
                        for row in arm_rows
                    ),
                    "quantiles_ns": quantiles,
                    "quantile_range_ns": {
                        metric: [
                            min(int(row["result"][f"latency_{metric}_ns"])
                                for row in arm_rows),
                            max(int(row["result"][f"latency_{metric}_ns"])
                                for row in arm_rows),
                        ]
                        for metric in METRICS
                    },
                })
    return groups


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", type=Path, nargs="+")
    parser.add_argument("--baseline-sha256", required=True)
    parser.add_argument("--candidate-sha256", required=True)
    parser.add_argument("--rounds", type=int, default=10,
                        help="required measured rounds per arm (1..10)")
    parser.add_argument("--output", type=Path, required=True)
    if not 1 <= args.rounds <= 10:
        parser.error("--rounds must be in 1..10")
    args = parser.parse_args()
    expected = {"B": args.baseline_sha256, "C": args.candidate_sha256}
    rows = []
    inputs = []
    for path in args.inputs:
        sha, values = load(path, expected)
        inputs.append({"path": str(path), "sha256": sha})
        rows.extend(values)
    result = {
        "inputs": inputs,
        "clients": expected,
        "aggregation": "Median of independent run quantiles, not pooled quantiles",
        "groups": summarize(rows, args.baseline_sha256, args.candidate_sha256,
                            args.rounds),
    }
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print("| Origin | Bytes | Arm | Client SHA-256 | Runs | p50 ns | p99 ns | p99.9 ns |")
    print("|---|---:|---|---|---:|---:|---:|---:|")
    for group in result["groups"]:
        q = group["quantiles_ns"]
        print(f"| {group['origin']} | {group['payload']} | {group['arm']} | "
              f"{group['client_sha256'][:12]} | {group['round_count']} | "
              f"{q['p50']} | {q['p99']} | {q['p999']} |")


if __name__ == "__main__":
    main()
