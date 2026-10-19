#!/usr/bin/env python3
"""核验计数、哈希与资源门禁，保存可独立检查的轮询实验结果。"""

import argparse
import hashlib
import json
from pathlib import Path
import shutil

from diagnose_design import summarize
from run_latency_compare import fields


def export(source, output):
    output.mkdir(parents=True, exist_ok=True)
    manifest = dict(schema_version=1, aggregation="Median of independent run quantiles, NOT pooled",
                    source_root=str(source.resolve()), groups={}, inputs=[], variants={}, verification={})
    count = messages = samples = 0
    lowest = None
    for path in sorted(source.glob("*/results.jsonl")):
        rows = [json.loads(line) for line in path.read_text().splitlines()]
        for row in rows:
            result = row["result"]
            expected = row["rate"] * row["duration_s"]
            if (int(result["published"]) != expected or int(result["delivered"]) != expected or
                    int(result["latency_samples"]) != (expected + 15) // 16):
                raise ValueError(f"count mismatch: {row['stdout']}")
            raw = Path(row["stdout"])
            if hashlib.sha256(raw.read_bytes()).hexdigest() != row["stdout_sha256"]:
                raise ValueError(f"stdout hash mismatch: {raw}")
            printed = [fields(line) for line in raw.read_text().splitlines() if line.startswith("RESULT ")]
            if printed != [result]:
                raise ValueError(f"record differs from stdout: {raw}")
            command = row["command"]
            binary = Path(command[command.index("--") + 1] if command[:3] == ["sudo", "-n", "perf"]
                          else command[0])
            if hashlib.sha256(binary.read_bytes()).hexdigest() != row["binary_sha256"]:
                raise ValueError(f"binary hash mismatch: {binary}")
            events = dict(line.split() for line in row["resources"]["memory.events"].splitlines())
            if any(int(events.get(key, 0)) for key in ("high", "max", "oom", "oom_kill")):
                raise ValueError(f"memory pressure: {path}")
            lowest = row["min_mem_available_kib"] if lowest is None else min(lowest, row["min_mem_available_kib"])
            if row["phase"] == "measured" and not row["profiled"]:
                count += 1
                messages += expected
                samples += (expected + 15) // 16
        dataset = path.parent.name
        target = output / dataset
        target.mkdir(exist_ok=True)
        shutil.copyfile(path, target / path.name)
        for pattern in ("*.stdout", "*.stderr", "*.svg", "*.folded", "*.top.json", "*.report.txt",
                        "*.annotate.txt"):
            for item in path.parent.glob(pattern):
                shutil.copyfile(item, target / item.name)
        manifest["groups"][dataset] = summarize(rows)
        manifest["inputs"].append(dict(dataset=dataset, rows=len(rows),
                                       sha256=hashlib.sha256(path.read_bytes()).hexdigest()))
    for header in sorted(source.glob("*/include/core/channel/shared_hybrid_mpsc.hpp")):
        variant = header.parents[3].name
        directory = source / variant
        manifest["variants"][variant] = dict(
            header_sha256=hashlib.sha256(header.read_bytes()).hexdigest(),
            channel_sha256=hashlib.sha256((directory / "channel.cpp").read_bytes()).hexdigest(),
            benchmark_sha256=hashlib.sha256((directory / "compare.cpp").read_bytes()).hexdigest(),
            binary_sha256=hashlib.sha256((source / "bin" / variant).read_bytes()).hexdigest())
        snapshot = output / "variants" / variant
        snapshot.mkdir(parents=True, exist_ok=True)
        shutil.copytree(directory / "include", snapshot / "include", dirs_exist_ok=True)
        for name in ("channel.cpp", "compare.cpp"):
            shutil.copyfile(directory / name, snapshot / name)
    for name in ("latency_histogram.hpp", "latency_pacing.hpp"):
        if (source / name).exists():
            shutil.copyfile(source / name, output / "variants" / name)
    if (source / "verification").exists():
        shutil.copytree(source / "verification", output / "verification", dirs_exist_ok=True)
        for path in sorted((source / "verification").glob("*.verification.json")):
            verification = json.loads(path.read_text())
            result = json.loads(path.with_name(f"{verification['variant']}.api.json").read_text())
            if result["failures"] or result.get("errors", 0) or result["tests"] != verification["tests"]:
                raise ValueError(f"API result mismatch: {path}")
            binary = source / "bin" / f"{verification['variant']}-api"
            if hashlib.sha256(binary.read_bytes()).hexdigest() != verification["binary_sha256"]:
                raise ValueError(f"API binary hash mismatch: {path}")
            manifest["verification"][verification["variant"]] = verification
    manifest.update(unprofiled_measured_runs=count, unprofiled_measured_messages=messages,
                    unprofiled_measured_samples=samples,
                    minimum_available_mib=None if lowest is None else lowest / 1024)
    (output / "summary.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps({key: value for key, value in manifest.items()
                     if key not in ("groups", "variants", "verification")}, indent=2))
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    export(args.source, args.output)


if __name__ == "__main__":
    main()
