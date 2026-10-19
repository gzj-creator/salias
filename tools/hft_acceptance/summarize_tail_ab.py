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
    for start in starts:
        summarize.check_formal_start(start)
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
    publishers = []
    for publisher in range(2):
        rows = [phase["result"]["publishers"][publisher] for phase in phases]
        assert all(row["publisher"] == publisher for row in rows)
        publishers.append({"publisher": publisher, **{
            key: summarize.merge([r[key] for r in rows])
            for key in ("first_attempt_to_commit", "planned_to_first_attempt")}})
    return {
        "configuration": {key: starts[0][key] for key in keys},
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
        "publishers": publishers,
        "run_count": len(phases),
        "seconds_per_run": starts[0]["duration_s"],
        "worst_planned_p99_ns": max(s["from_planned_arrival"]["p99_ns"] for s in subs),
        "worst_planned_p999_ns": max(s["from_planned_arrival"]["p999_ns"] for s in subs),
        "worst_attempt_p999_ns": max(s["from_first_attempt"]["p999_ns"] for s in subs),
        "planned_p999_budget_pass": valid and all(s["from_planned_arrival"]["p999_pass_50us"] for s in subs),
    }


def relative_gate(name, baseline, candidate):
    """Integer comparisons include the exact 20% improvement and 5% regression boundaries."""
    targeted = name in ("2p2s-64b-burst", "2p2s-256b-burst")
    a, b = baseline["worst_planned_p999_ns"], candidate["worst_planned_p999_ns"]
    valid = baseline["valid_and_complete"] and candidate["valid_and_complete"] and a > 0
    passed = b * 100 <= a * (80 if targeted else 105)
    return {"targeted": targeted, "change_percent": (b / a - 1) * 100 if a else None,
            "valid_and_complete": valid, "pass": bool(valid and passed)}


def assess_groups(groups):
    """Every independent group must pass; pooled quantiles cannot rescue a failed group."""
    expected = {f"2p{s}s-{p}b-{pattern}" for s in (1, 2) for p in (64, 256)
                for pattern in ("steady", "burst")}
    assert len(groups) == 3, "exactly three independent ABBA groups required"
    assessments = []
    for group in groups:
        assert set(group["phases"]) == expected, "missing or unexpected configuration"
        assert group["order"] == ["baseline", "candidate", "candidate", "baseline"]
        assert all(v["measurement_seconds"] == 60 for phase in group["phases"].values()
                   for v in phase.values()), "formal ABBA requires two 30-second runs per version"
        assert all(v["run_count"] == 2 and v["seconds_per_run"] == 30
                   for phase in group["phases"].values() for v in phase.values()), "formal run count or duration mismatch"
        assessments.append({name: relative_gate(name, versions["baseline"], versions["candidate"])
                            for name, versions in group["phases"].items()})
    return {"groups": assessments, "relative_acceptance_pass": all(
        result["pass"] for group in assessments for result in group.values()),
        "candidate_50us_pass": all(v["candidate"]["planned_p999_budget_pass"]
                                   for group in groups for v in group["phases"].values())}


def audit_saved_group(group, directory=None):
    """Recompute each version from its raw files; a stale summary cannot authorize a pass."""
    all_paths = []
    for name, versions in group["phases"].items():
        assert set(versions) == {"baseline", "candidate"}, "unexpected A/B version"
        for version, result in versions.items():
            paths = [Path(run["path"]) for run in result["runs"]]
            all_paths.extend(paths)
            if directory is not None:
                assert all(p.parents[2].resolve() == directory.resolve() for p in paths), "raw run belongs to another group"
            assert len(set(paths)) == len(paths), "duplicated raw run"
            assert all(p.stem == name and p.parent.name == version for p in paths), "wrong raw run identity"
            rounds = [p.parents[1].name for p in paths]
            assert rounds == (["round-0", "round-3"] if version == "baseline" else ["round-1", "round-2"]), "wrong ABBA run order"
            assert aggregate(paths) == result, "saved summary differs from raw audit"
        assert versions["baseline"]["configuration"] == versions["candidate"]["configuration"], "A/B configuration mismatch"
    if directory is not None:
        assert set(directory.glob("round-*/*/2p*.jsonl")) == set(all_paths), "unexpected raw files"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--suite", action="store_true", help="audit three group-N/ab-summary.json files")
    args = parser.parse_args()
    if args.suite:
        paths = [args.directory / f"group-{i}" / "ab-summary.json" for i in range(1, 4)]
        groups = [json.loads(path.read_text()) for path in paths]
        for group, path in zip(groups, paths):
            audit_saved_group(group, path.parent)
        result = assess_groups(groups)
        result["inputs"] = [{"path": str(p), "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
                            for p in paths]
        (args.directory / "relative-acceptance.json").write_text(json.dumps(result, indent=2) + "\n")
        lines = ["# Relative Publisher Tail Acceptance", "",
                 f"Relative gate: **{'PASS' if result['relative_acceptance_pass'] else 'FAIL'}**.",
                 f"Candidate 50us gate: **{'PASS' if result['candidate_50us_pass'] else 'FAIL'}**.", "",
                 "Every group must pass independently: both 2P2S bursts improve >=20%; other configurations regress <=5%.",
                 "All baseline/candidate input and delivery checks must pass. Negative change means improvement.", "",
                 "| Configuration | Group 1 change | Group 2 change | Group 3 change | All groups pass |",
                 "|---|---:|---:|---:|---|"]
        for name in sorted(result["groups"][0]):
            values = [group[name] for group in result["groups"]]
            changes = [f"{v['change_percent']:+.1f}%" if v['change_percent'] is not None else "N/A" for v in values]
            lines.append(f"| {name} | {' | '.join(changes)} | {all(v['pass'] for v in values)} |")
        (args.directory / "relative-acceptance.md").write_text("\n".join(lines) + "\n")
        print(json.dumps(result))
        return
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
        assert versions["baseline"]["configuration"] == versions["candidate"]["configuration"], "A/B configuration mismatch"
        config = versions["baseline"]["configuration"]
        assert name == f"2p{config['subscribers']}s-{config['payload_bytes']}b-{config['pattern']}", "filename/configuration mismatch"
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
    lines += ["", "## Publisher Components", "",
              "Each row retains one publisher; quantiles are merged from that publisher's raw buckets only.", "",
              "| Configuration | Version | Publisher | Attempt to commit p50 / p99 / p99.9 us | Planned to attempt p50 / p99 / p99.9 us |",
              "|---|---|---|---|---|"]
    for name, versions in phases.items():
        for version, result in versions.items():
            for publisher in result["publishers"]:
                values = [" / ".join(f"{publisher[key][q]/1000:.3f}" for q in ("p50_ns", "p99_ns", "p999_ns"))
                          for key in ("first_attempt_to_commit", "planned_to_first_attempt")]
                lines.append(f"| {name} | {version} | {publisher['publisher']} | {' | '.join(values)} |")
    lines += ["", "## Backpressure", "",
              "| Configuration | Version | Failed claim retries | Published | Delivered |",
              "|---|---|---:|---:|---:|"]
    for name, versions in phases.items():
        for version, result in versions.items():
            lines.append(f"| {name} | {version} | {result['backpressure_retries']} | {result['published']} | {result['delivered']} |")
    lines += ["", "Negative change means lower p99.9. No causal claim about guest scheduling or host contention is made.",
              "Do not subtract independent quantiles to attribute component time. No cross-configuration percentiles are reported.", ""]
    (args.directory / "ab-summary.json").write_text(json.dumps(document, indent=2) + "\n")
    (args.directory / "ab-summary.md").write_text("\n".join(lines))
    print(json.dumps({k: document[k] for k in ("measurement_seconds", "all_valid_and_complete")}))


if __name__ == "__main__":
    main()
