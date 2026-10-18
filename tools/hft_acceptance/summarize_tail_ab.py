#!/usr/bin/env python3
"""Independently audit interleaved A/B runs without changing latency semantics."""

import argparse
import hashlib
import json
from pathlib import Path

import summarize


def aggregate(paths):
    """Merge raw buckets by subscriber, never across different configurations."""
    phases = [summarize.load_phase(path) for path in paths]
    starts = [phase["start"] for phase in phases]
    keys = ("subscribers", "payload_bytes", "pattern", "duration_s", "steady_total_rate",
            "peak_total_rate", "burst_ms_per_second", "capacity_per_publisher_bytes",
            "publication_window_bytes", "batch", "poll_limit", "sample_hash_mask", "cpus")
    assert all(tuple(start[key] for key in keys) == tuple(starts[0][key] for key in keys)
               for start in starts), "configuration mismatch"
    assert all((start["steady_total_rate"], start["peak_total_rate"]) == (1000000, 4000000)
               for start in starts), "offered rate changed"
    assert all((start["capacity_per_publisher_bytes"], start["publication_window_bytes"],
                start["batch"], start["poll_limit"], start["sample_hash_mask"]) ==
               (4194304, 131072, 1, 64, 63) for start in starts), "measurement settings changed"
    for phase in phases:
        start, result = phase["start"], phase["result"]
        burst_ms = start["burst_ms_per_second"]
        assert burst_ms == (100 if start["pattern"] == "burst" else 0)
        planned = start["duration_s"] * (4000000 * burst_ms + 1000000 * (1000 - burst_ms)) // 1000
        assert start["planned_published"] == result["planned_published"] == planned
    subs = []
    for subscriber in range(starts[0]["subscribers"]):
        rows = [phase["result"]["subscribers"][subscriber] for phase in phases]
        assert all(row["subscriber"] == subscriber for row in rows)
        subs.append({"subscriber": subscriber,
                     "from_planned_arrival": summarize.merge([r["from_planned_arrival"] for r in rows]),
                     "from_first_attempt": summarize.merge([r["from_first_attempt"] for r in rows])})
    valid = all(phase["acceptance"]["valid_and_complete"] for phase in phases)
    return {
        "runs": [{"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                  "acceptance": phase["acceptance"]} for path, phase in zip(paths, phases)],
        "measurement_seconds": sum(p["result"]["measurement_seconds"] for p in phases),
        "published": sum(p["result"]["published"] for p in phases),
        "delivered": sum(p["result"]["delivered"] for p in phases),
        "valid_and_complete": valid,
        "all_individual_runs_p999_pass": all(p["acceptance"]["p999_acceptance_pass"] for p in phases),
        "backpressure_retries": sum(p["backpressure_retries"] for phase in phases
                                    for p in phase["result"]["publishers"]),
        "subscribers": subs,
        "worst_planned_p99_ns": max(s["from_planned_arrival"]["p99_ns"] for s in subs),
        "worst_planned_p999_ns": max(s["from_planned_arrival"]["p999_ns"] for s in subs),
        "worst_attempt_p999_ns": max(s["from_first_attempt"]["p999_ns"] for s in subs),
        "planned_p999_budget_pass": valid and all(s["from_planned_arrival"]["p999_pass_50us"] for s in subs),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    expected = {f"2p{s}s-{payload}b-{pattern}" for s in (1, 2)
                for payload in (64, 256) for pattern in ("steady", "burst")}
    phases = {}
    all_paths = []
    for name in sorted(expected):
        versions = {}
        for version, rounds in (("baseline", (0, 3)), ("candidate", (1, 2))):
            paths = [args.directory / f"round-{r}" / version / f"{name}.jsonl" for r in rounds]
            versions[version] = aggregate(paths)
            all_paths.extend(paths)
        assert versions["baseline"]["measurement_seconds"] == versions["candidate"]["measurement_seconds"]
        # Preserve failed/incomplete runs in the comparison instead of hiding them
        # behind a summary exception. Completion is an explicit acceptance gate.
        phases[name] = versions
    assert set(args.directory.glob("round-*/*/2p*.jsonl")) == set(all_paths), "unexpected raw files"
    document = {"order": ["baseline", "candidate", "candidate", "baseline"], "phases": phases,
                "measurement_seconds": sum(v["measurement_seconds"] for p in phases.values() for v in p.values()),
                "all_valid_and_complete": all(v["valid_and_complete"] for p in phases.values() for v in p.values())}
    lines = ["# Interleaved Tail Latency A/B", "",
             "Order per configuration: baseline, candidate, candidate, baseline; two runs per binary.",
             "Primary latency starts at the unchanged planned arrival. Each subscriber is merged separately across runs.",
             "These short runs screen effects; they do not replace the 30-minute candidate acceptance.", "",
             "| Configuration | Baseline p99 us | Candidate p99 us | Baseline p99.9 us | Candidate p99.9 us | p99.9 change | Valid/complete |",
             "|---|---:|---:|---:|---:|---:|---|"]
    for name, versions in phases.items():
        a, b = versions["baseline"], versions["candidate"]
        change = (b["worst_planned_p999_ns"] / a["worst_planned_p999_ns"] - 1) * 100
        lines.append(f"| {name} | {a['worst_planned_p99_ns']/1000:.3f} | {b['worst_planned_p99_ns']/1000:.3f} | "
                     f"{a['worst_planned_p999_ns']/1000:.3f} | {b['worst_planned_p999_ns']/1000:.3f} | {change:+.1f}% | "
                     f"{a['valid_and_complete']}/{b['valid_and_complete']} |")
    lines += ["", "## Individual Runs", "",
              "| Configuration | Version | Round | Worst p99 us | Worst p99.9 us | Valid/complete |",
              "|---|---|---|---:|---:|---|"]
    for name, versions in phases.items():
        for version, result in versions.items():
            for run in result["runs"]:
                a = run["acceptance"]
                lines.append(f"| {name} | {version} | {Path(run['path']).parents[1].name} | "
                             f"{a['worst_planned_p99_ns']/1000:.3f} | {a['worst_planned_p999_ns']/1000:.3f} | {a['valid_and_complete']} |")
    lines += ["", "Negative change means lower p99.9. No causal claim about guest scheduling or host contention is made.",
              "Do not subtract independent quantiles to attribute component time. No cross-configuration percentiles are reported.", ""]
    (args.directory / "ab-summary.json").write_text(json.dumps(document, indent=2) + "\n")
    (args.directory / "ab-summary.md").write_text("\n".join(lines))
    print(json.dumps({k: document[k] for k in ("measurement_seconds", "all_valid_and_complete")}))


if __name__ == "__main__":
    main()
