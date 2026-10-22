#!/usr/bin/env python3
"""Audit P3-1 HFT non-inferiority using the existing raw-bucket validator.

This is not the historical publisher-tail improvement gate. Both p99 and p99.9
must be at most C-seq * 1.05 in every configuration and independent ABBA group.
All input must complete, and every delivery must pass the HFT integrity checks.
The 50 us budget is reported separately and cannot authorize a relative pass.
"""

import argparse
import hashlib
import json
from pathlib import Path

from summarize_tail_ab import aggregate


ARMS = {"S": {"SALIAS_DISABLE_ASYMMETRIC_FENCE": "1"}, "A": {}}
ORDER = ("S", "A", "A", "S")
CONFIGURATIONS = tuple(f"2p{s}s-{p}b-{pattern}" for s in (1, 2)
                       for p in (64, 256) for pattern in ("steady", "burst"))
METRICS = ("worst_planned_p99_ns", "worst_planned_p999_ns")
PROTOCOL = {
    "name": "P3-1 G4 HFT non-inferiority",
    "groups": 3,
    "seconds_per_run": 30,
    "order": list(ORDER),
    "configurations": list(CONFIGURATIONS),
    "environment": ARMS,
    "metrics": list(METRICS),
    "maximum_candidate_percent_of_seq": 105,
    "every_group_and_configuration_required": True,
    "integrity_and_original_input_completion_required": True,
    "independent_50us_budget_is_not_the_relative_gate": True,
}


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def assess_pair(seq, asym):
    """Apply integer boundaries to independently merged subscriber histograms."""
    valid = seq["valid_and_complete"] and asym["valid_and_complete"]
    metrics = {}
    for metric in METRICS:
        before, after = seq[metric], asym[metric]
        metrics[metric] = {
            "seq_ns": before, "asym_ns": after,
            "change_percent": (after / before - 1) * 100 if before else None,
            "passed": bool(valid and before > 0 and after * 100 <= before * 105),
        }
    return {"valid_and_complete": bool(valid), "metrics": metrics,
            "passed": all(item["passed"] for item in metrics.values()),
            "candidate_50us_pass": bool(asym["planned_p999_budget_pass"])}


def audit(directory):
    """Verify identities and re-read every raw HFT file before judging G4."""
    directory = Path(directory).resolve()
    manifest = json.loads((directory / "experiment.json").read_text())
    if manifest["protocol"] != PROTOCOL:
        raise ValueError("HFT protocol differs from the pre-measurement specification")
    if digest(manifest["client"]) != manifest["client_sha256"]:
        raise ValueError("HFT client hash changed")
    for path, sha in manifest["inputs"].items():
        if digest(path) != sha:
            raise ValueError(f"frozen input changed: {path}")
    expected = [(group, name, round_number, arm)
                for group in range(1, 4) for name in CONFIGURATIONS
                for round_number, arm in enumerate(ORDER)]
    runs = manifest["runs"]
    if len(runs) != len(expected):
        raise ValueError(f"expected {len(expected)} formal HFT runs, got {len(runs)}")
    paths = set()
    groups = []
    for run, identity in zip(runs, expected):
        if tuple(run[key] for key in ("group", "configuration", "round", "arm")) != identity:
            raise ValueError("HFT run order/identity mismatch")
        group, name, round_number, arm = identity
        relative = f"group-{group}/round-{round_number}/{arm}/{name}.jsonl"
        if (run["raw"] != relative or run["environment"] != ARMS[arm] or
                run["client_sha256"] != manifest["client_sha256"]):
            raise ValueError("HFT run arm, environment or raw path mismatch")
        command = run["command"]
        subscribers, payload, pattern = name.split("-")
        expected_command = ["timeout", "--signal=TERM", "--kill-after=10s", "70s",
                            manifest["client"], "--subscribers", subscribers[2],
                            "--payload", payload[:-1], "--seconds", "30", "--pattern", pattern,
                            "--rate", "1000000", "--peak-rate", "4000000", "--burst-ms", "100",
                            "--cpu-base", "0"]
        if command != expected_command:
            raise ValueError("HFT run command differs from protocol")
        for key in ("raw", "stderr"):
            path = directory / run[key]
            if directory not in path.resolve().parents or digest(path) != run[f"{key}_sha256"]:
                raise ValueError(f"HFT {key} hash/path mismatch")
        if run["returncode"] != 0:
            raise ValueError(f"HFT run failed: {relative}, exit={run['returncode']}")
        paths.add(directory / relative)
    if set(directory.glob("group-*/round-*/*/*.jsonl")) != paths:
        raise ValueError("unexpected HFT raw files")
    for group in range(1, 4):
        phases = {}
        for name in CONFIGURATIONS:
            versions = {
                arm: aggregate([directory / f"group-{group}/round-{r}/{arm}/{name}.jsonl"
                                for r, selected in enumerate(ORDER) if selected == arm])
                for arm in ARMS
            }
            seq, asym = versions["S"], versions["A"]
            if seq["configuration"] != asym["configuration"]:
                raise ValueError("HFT A/S configuration mismatch")
            config = seq["configuration"]
            if (name != f"2p{config['subscribers']}s-{config['payload_bytes']}b-{config['pattern']}" or
                    config["cpus"] != list(range(config["subscribers"] + 2))):
                raise ValueError("HFT filename/configuration/CPU mismatch")
            if any(v["run_count"] != 2 or v["seconds_per_run"] != 30
                   for v in versions.values()):
                raise ValueError("formal HFT needs two 30-second runs per arm/group")
            phases[name] = {"arms": versions, **assess_pair(seq, asym)}
        groups.append({"group": group, "phases": phases,
                       "passed": all(p["passed"] for p in phases.values())})
    return {
        "directory": str(directory), "client_sha256": manifest["client_sha256"],
        "protocol": PROTOCOL, "groups": groups,
        "formal_runs": len(runs), "measurement_seconds": len(runs) * 30,
        "all_valid_and_complete": all(p["valid_and_complete"] for g in groups
                                      for p in g["phases"].values()),
        "passed": all(group["passed"] for group in groups),
        "candidate_50us_pass": all(p["candidate_50us_pass"] for g in groups
                                   for p in g["phases"].values()),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    result = audit(args.directory)
    (args.directory / "hft-noninferiority.json").write_text(json.dumps(result, indent=2) + "\n")
    lines = ["# P3-1 HFT Non-Inferiority", "",
             f"G4 HFT tail: **{'PASS' if result['passed'] else 'FAIL'}**.",
             f"All input and integrity checks: **{result['all_valid_and_complete']}**.",
             "Each independent group and configuration must satisfy A <= S * 1.05 for both quantiles.",
             "Raw buckets merge separately for each subscriber; the worst subscriber determines each quantile.", "",
             "| Configuration | Group | S p99 us | A p99 us | S p99.9 us | A p99.9 us | Pass |",
             "|---|---:|---:|---:|---:|---:|---|"]
    for group in result["groups"]:
        for name, phase in group["phases"].items():
            q99, q999 = (phase["metrics"][key] for key in METRICS)
            lines.append(f"| {name} | {group['group']} | {q99['seq_ns']/1000:.3f} | "
                         f"{q99['asym_ns']/1000:.3f} | {q999['seq_ns']/1000:.3f} | "
                         f"{q999['asym_ns']/1000:.3f} | {phase['passed']} |")
    (args.directory / "hft-noninferiority.md").write_text("\n".join(lines) + "\n")
    print(json.dumps({key: result[key] for key in
                      ("formal_runs", "all_valid_and_complete", "passed", "candidate_50us_pass")}))


if __name__ == "__main__":
    main()
