#!/usr/bin/env python3
"""Verify fixed-ring throughput and attempt-latency gates from suite summaries."""

import argparse
import json
from pathlib import Path
import sys


TOPOLOGIES = ("spsc", "mpsc", "spmc", "mpmc")
PAYLOADS = (64, 256)
MIN_THROUGHPUT_RATIO = 1.10
MIN_BASELINE_THROUGHPUT_GAIN = 1.25
MAX_ATTEMPT_P99_RATIO = 1.10


def load_summary(path):
    return json.loads(Path(path).read_text())


def group_index(summary):
    return {
        (group["topology"], group["kind"], int(group["payload"]), group["origin"],
         group["engine"]): group
        for group in summary.get("groups", [])
    }


def median_range_median(group, path):
    value = group
    for key in path:
        value = value[key]
    return value["median"]


def protocol(summary):
    workload = summary.get("environment", {}).get("workload", {})
    cell_rounds = [int(group.get("rounds", 0)) for group in summary.get("groups", [])]
    cell_groups = [int(group.get("groups", 1)) for group in summary.get("groups", [])]
    return {
        "groups": int(workload.get("groups", max(cell_groups, default=1))),
        "rounds_per_group": int(workload.get(
            "measured_rounds_per_engine_per_group",
            workload.get("measured_rounds", max(cell_rounds, default=0)))),
        "engine_order": workload.get("engine_order", "legacy paired rounds"),
        "verification": workload.get("verification", summary.get("verification", "none")),
    }


def verify_gates(fast_summary, baseline_summary=None, minimum_rounds=5,
                 minimum_groups=1, require_dedicated_driver=False,
                 require_full_verification=False, require_abba=False,
                 require_matched_protocol=False):
    fast = group_index(fast_summary)
    baseline = group_index(baseline_summary) if baseline_summary is not None else None
    cells = []
    failures = []
    fast_protocol = protocol(fast_summary)
    baseline_protocol = protocol(baseline_summary) if baseline_summary is not None else None

    if fast_protocol["groups"] < minimum_groups:
        failures.append(
            f"suite has {fast_protocol['groups']} groups, need at least {minimum_groups}"
        )
    if fast_protocol["rounds_per_group"] < minimum_rounds:
        failures.append(
            f"fast suite has {fast_protocol['rounds_per_group']} rounds per group, "
            f"need at least {minimum_rounds}"
        )
    if require_abba and fast_protocol["engine_order"] != "ABBA paired rounds":
        failures.append("suite was not run with ABBA paired-round ordering")
    if require_full_verification and fast_protocol["verification"] != "full":
        failures.append("fast suite was not run with --verify full")

    if baseline_summary is not None:
        if baseline_protocol["rounds_per_group"] < minimum_rounds:
            failures.append(
                f"baseline suite has {baseline_protocol['rounds_per_group']} rounds per "
                f"group, need at least {minimum_rounds}"
            )
        if require_full_verification and baseline_protocol["verification"] != "full":
            failures.append("baseline suite was not run with --verify full")
        if require_matched_protocol:
            for key in ("groups", "rounds_per_group", "engine_order", "verification"):
                if baseline_protocol[key] != fast_protocol[key]:
                    failures.append(
                        f"baseline protocol {key}={baseline_protocol[key]!r} does not match "
                        f"fast {fast_protocol[key]!r}"
                    )

    if require_dedicated_driver:
        cpu_capacity = fast_summary.get("environment", {}).get("cpu_capacity", {})
        if not cpu_capacity.get("require_dedicated_driver"):
            failures.append("suite was not run with --require-dedicated-driver")
        elif not cpu_capacity.get("dedicated_driver_available"):
            failures.append("Aeron driver did not have a dedicated application CPU")

    def require(key, label):
        try:
            return fast[key]
        except KeyError:
            failures.append(f"missing fast group: {label}")
            return None

    for topology in TOPOLOGIES:
        for payload in PAYLOADS:
            label = f"{topology}/{payload}B"
            fast_throughput = require(
                (topology, "throughput", payload, "claim", "salias"), f"{label} throughput")
            aeron_throughput = require(
                (topology, "throughput", payload, "claim", "aeron"), f"{label} Aeron throughput")
            fast_p50 = require(
                (topology, "latency", payload, "attempt", "salias"), f"{label} attempt p50")
            aeron_p50 = require(
                (topology, "latency", payload, "attempt", "aeron"), f"{label} Aeron attempt p50")
            fast_p99 = require(
                (topology, "latency", payload, "attempt", "salias"), f"{label} attempt p99")
            aeron_p99 = require(
                (topology, "latency", payload, "attempt", "aeron"), f"{label} Aeron attempt p99")
            if any(group is None for group in (
                    fast_throughput, aeron_throughput, fast_p50, aeron_p50, fast_p99, aeron_p99)):
                continue

            rounds = min(fast_throughput["rounds"], aeron_throughput["rounds"],
                         fast_p50["rounds"], aeron_p50["rounds"])
            groups = min(fast_throughput.get("groups", 1), aeron_throughput.get("groups", 1),
                         fast_p50.get("groups", 1), aeron_p50.get("groups", 1))
            throughput_ratio = (median_range_median(fast_throughput, ("publish_mmsg_s",)) /
                                median_range_median(aeron_throughput, ("publish_mmsg_s",)))
            p50_fast = median_range_median(fast_p50, ("latency_ns", "p50"))
            p50_aeron = median_range_median(aeron_p50, ("latency_ns", "p50"))
            p99_fast = median_range_median(fast_p99, ("latency_ns", "p99"))
            p99_aeron = median_range_median(aeron_p99, ("latency_ns", "p99"))
            p99_ratio = p99_fast / p99_aeron

            baseline_group = None
            baseline_ratio = None
            if baseline is not None:
                baseline_group = baseline.get(
                    (topology, "throughput", payload, "claim", "salias"))
                if baseline_group is None:
                    failures.append(f"missing FIFO baseline group: {label} throughput")
                else:
                    baseline_ratio = (median_range_median(fast_throughput, ("publish_mmsg_s",)) /
                                      median_range_median(baseline_group, ("publish_mmsg_s",)))

            cell_failures = []
            required_rounds = minimum_rounds * minimum_groups
            if groups < minimum_groups or rounds < required_rounds:
                cell_failures.append(
                    f"only {rounds} measured rounds across {groups} groups; "
                    f"need {required_rounds} across {minimum_groups}"
                )
            if throughput_ratio < MIN_THROUGHPUT_RATIO:
                cell_failures.append("Aeron throughput ratio below 1.10x")
            if p50_fast > p50_aeron:
                cell_failures.append("attempt p50 exceeds Aeron")
            if p99_ratio > MAX_ATTEMPT_P99_RATIO:
                cell_failures.append("attempt p99 exceeds 1.10x Aeron")
            if baseline_ratio is not None and baseline_ratio < MIN_BASELINE_THROUGHPUT_GAIN:
                cell_failures.append("FIFO throughput gain below 1.25x")
            failures.extend(f"{label}: {failure}" for failure in cell_failures)
            cells.append({
                "topology": topology,
                "payload": payload,
                "rounds": rounds,
                "groups": groups,
                "throughput_mmsg_s": {
                    "fast": median_range_median(fast_throughput, ("publish_mmsg_s",)),
                    "aeron": median_range_median(aeron_throughput, ("publish_mmsg_s",)),
                    "fifo": (median_range_median(baseline_group, ("publish_mmsg_s",))
                             if baseline_group is not None else None),
                },
                "fast_over_aeron_throughput": throughput_ratio,
                "fast_over_fifo_throughput": baseline_ratio,
                "attempt_p50_ns": {"fast": p50_fast, "aeron": p50_aeron},
                "attempt_p99_ns": {"fast": p99_fast, "aeron": p99_aeron},
                "fast_over_aeron_p99": p99_ratio,
                "passed": not cell_failures,
                "failures": cell_failures,
            })

    return {
        "passed": not failures and len(cells) == len(TOPOLOGIES) * len(PAYLOADS),
        "minimum_rounds": minimum_rounds,
        "minimum_groups": minimum_groups,
        "baseline_evaluated": baseline is not None,
        "require_dedicated_driver": require_dedicated_driver,
        "require_full_verification": require_full_verification,
        "require_abba": require_abba,
        "protocol": {"fast": fast_protocol, "baseline": baseline_protocol},
        "criteria": {
            "minimum_aeron_throughput_ratio": MIN_THROUGHPUT_RATIO,
            "minimum_fifo_throughput_gain": MIN_BASELINE_THROUGHPUT_GAIN,
            "maximum_attempt_p50_ns": "Aeron",
            "maximum_attempt_p99_ratio": MAX_ATTEMPT_P99_RATIO,
            "rounds": "minimum_rounds x minimum_groups",
        },
        "cells": cells,
        "failures": failures,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fast-summary", type=Path, required=True)
    parser.add_argument("--baseline-summary", type=Path)
    parser.add_argument("--minimum-rounds", type=int, default=5)
    parser.add_argument("--minimum-groups", type=int, default=1)
    parser.add_argument("--require-dedicated-driver", action="store_true")
    parser.add_argument("--require-full-verification", action="store_true")
    parser.add_argument("--require-abba", action="store_true")
    parser.add_argument("--require-matched-protocol", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.minimum_rounds < 1:
        parser.error("--minimum-rounds must be positive")
    if args.minimum_groups < 1:
        parser.error("--minimum-groups must be positive")
    baseline = load_summary(args.baseline_summary) if args.baseline_summary else None
    report = verify_gates(
        load_summary(args.fast_summary), baseline, args.minimum_rounds,
        args.minimum_groups, args.require_dedicated_driver,
        args.require_full_verification, args.require_abba,
        args.require_matched_protocol,
    )
    rendered = json.dumps(report, indent=2) + "\n"
    if args.output:
        args.output.write_text(rendered)
    sys.stdout.write(rendered)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
