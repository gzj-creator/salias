#!/usr/bin/env python3
"""Build an isolated acquire_consumer probe and retain G6 raw timings."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess

ROOT = Path(__file__).resolve().parents[3]


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-source", type=Path, required=True)
    parser.add_argument("--baseline-core", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    source = ROOT / "tools/hft_acceptance/registration_cost.cpp"
    manifest = {"probe_sha256": digest(source), "builds": {}, "runs": []}
    binaries = {}
    for version, root, core in (
            ("baseline", args.baseline_source.resolve(), args.baseline_core.resolve()),
            ("candidate", ROOT, ROOT / "build/release/src/core/libsalias_core.a")):
        binary = output / version
        command = ["c++", "-std=c++23", "-O3", "-DNDEBUG", "-Wall", "-Wextra", "-Wpedantic",
                   "-I", str(root / "src/salias/include"), "-I", str(root / "src"),
                   str(source), str(core), "-pthread", "-lrt", "-o", str(binary)]
        with (output / f"{version}.build.log").open("w") as log:
            subprocess.run(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, check=True)
        manifest["builds"][version] = {
            "command": command, "binary_sha256": digest(binary),
            "core_sha256": digest(core),
            "source_inputs": {str(path): digest(path) for path in
                              sorted((root / "src").rglob("*.cpp")) +
                              sorted((root / "src").rglob("*.hpp"))},
        }
        binaries[version] = binary
    manifest["compiler"] = subprocess.check_output(["c++", "--version"], text=True)
    # Alternate direction between groups; keep setup and release outside timing.
    for group in range(6):
        for arm in ("BSA" if group % 2 == 0 else "ASB"):
            binary = binaries["baseline" if arm == "B" else "candidate"]
            env = dict(os.environ)
            env.pop("SALIAS_DISABLE_ASYMMETRIC_FENCE", None)
            if arm == "S":
                env["SALIAS_DISABLE_ASYMMETRIC_FENCE"] = "1"
            raw = output / f"group-{group}-{arm}.json"
            with raw.open("w") as stream:
                subprocess.run([str(binary), "5000"], env=env, cwd=ROOT, stdout=stream,
                               timeout=60, check=True)
            result = json.loads(raw.read_text())
            if len(result["elapsed_ns"]) != 5000 or result["publisher_light_path"] != (arm == "A"):
                raise RuntimeError("registration samples or publisher fence mode mismatch")
            values = sorted(result["elapsed_ns"])
            row = {"group": group, "arm": arm, "raw": raw.name, "raw_sha256": digest(raw),
                   "median_ns": statistics.median(values), "mean_ns": statistics.mean(values),
                   "p99_ns": values[(len(values) * 99 + 99) // 100 - 1]}
            manifest["runs"].append(row)
            print(f"group={group} arm={arm} median={row['median_ns']} ns "
                  f"p99={row['p99_ns']} ns", flush=True)
    manifest["summary"] = {}
    for arm in "BSA":
        medians = [row["median_ns"] for row in manifest["runs"] if row["arm"] == arm]
        manifest["summary"][arm] = {"median_of_run_medians_ns": statistics.median(medians),
                                    "run_medians_ns": medians, "samples": 30000}
    (output / "registration.json").write_text(json.dumps(manifest, indent=2) + "\n")


if __name__ == "__main__":
    main()
