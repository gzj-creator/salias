#!/usr/bin/env python3
"""Preserve follow-up A/B evidence without treating unsafe protocols as accepted."""

import argparse
import hashlib
import json
from pathlib import Path
import shutil

from diagnose_design import summarize


def export(source, output):
    output.mkdir(parents=True, exist_ok=True)
    manifest = {"schema_version": 1, "aggregation": "Median of independent run quantiles; not pooled",
                "unsafe_variants": ["frame"], "groups": {}, "inputs": []}
    count = messages = samples = 0
    minimum_available = None
    for dataset in ("screen", "confirm", "scheduler-matrix", "scheduler-trace", "checked-timing", "planned-confirm", "profiles"):
        path = source / dataset / "results.jsonl"
        if not path.exists():
            continue
        rows = [json.loads(line) for line in path.read_text().splitlines()]
        for row in rows:
            result = row["result"]
            expected = row["rate"] * row["duration_s"]
            if (int(result["published"]) != expected or int(result["delivered"]) != expected or
                    int(result["latency_samples"]) != (expected + 15) // 16):
                raise ValueError(f"counts differ: {row['stdout']}")
            raw = Path(row["stdout"])
            if hashlib.sha256(raw.read_bytes()).hexdigest() != row["stdout_sha256"]:
                raise ValueError(f"stdout hash differs: {raw}")
            events = dict(line.split() for line in row["resources"]["memory.events"].splitlines())
            if any(int(events.get(key, 0)) for key in ("high", "max", "oom", "oom_kill")):
                raise ValueError(f"memory pressure in {path}")
            available = row["min_mem_available_kib"]
            minimum_available = available if minimum_available is None else min(minimum_available, available)
            if row["phase"] == "measured" and not row["profiled"]:
                count += 1
                messages += expected
                samples += (expected + 15) // 16
        shutil.copyfile(path, output / f"{dataset}.jsonl")
        manifest["inputs"].append({"dataset": dataset, "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                                   "rows": len(rows)})
        manifest["groups"][dataset] = summarize(rows)
        for pattern in ("*.stdout", "*.stderr", "*.svg", "*.folded", "*.top.json", "*.report.txt",
                        "switch-analysis.json", "policy-snapshot.txt"):
            for item in path.parent.glob(pattern):
                target = output / dataset / item.name
                target.parent.mkdir(exist_ok=True)
                shutil.copyfile(item, target)
    for item in source.glob("*-poll-memset.txt"):
        shutil.copyfile(item, output / item.name)
    manifest.update(unprofiled_measured_runs=count, unprofiled_measured_messages=messages,
                    unprofiled_measured_samples=samples, minimum_available_mib=minimum_available / 1024)
    (output / "summary.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps({key: value for key, value in manifest.items() if key != "groups"}, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    export(args.source, args.output)


if __name__ == "__main__":
    main()
