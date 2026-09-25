#!/usr/bin/env python3
"""Compare identical benchmark workloads in ABBA order; preserve raw results."""

import argparse
import json
import os
from pathlib import Path
import statistics
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("before", type=Path)
    parser.add_argument("after", type=Path)
    parser.add_argument("--kind", choices=["receiver", "hotpath"], default="receiver")
    parser.add_argument("--reps", type=int, default=11)
    parser.add_argument("--messages", type=int, default=2000000)
    parser.add_argument("--case")
    parser.add_argument("--cpu", type=int, help="Pin receiver benchmark to this Linux CPU")
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not 1 <= args.reps <= 100 or args.messages < 4 or args.timeout <= 0:
        parser.error("require 1..100 repetitions, at least 4 messages and positive timeout")
    if args.cpu is not None:
        if args.kind != "receiver":
            parser.error("hotpath benchmark sets its own thread affinities")
        os.sched_setaffinity(0, {args.cpu})
    command_args = [str(args.reps)] if args.kind == "receiver" else [
        "--reps", str(args.reps), "--messages", str(args.messages)]
    if args.case:
        if args.kind != "hotpath":
            parser.error("--case requires --kind hotpath")
        command_args += ["--case", args.case]
    report = dict(kind=args.kind, cpu=args.cpu, reps=args.reps, order="ABBA", runs=[])
    for version in ["before", "after", "after", "before"]:
        executable = (args.before if version == "before" else args.after).resolve()
        command = [str(executable), *command_args]
        result = subprocess.run(command, text=True, capture_output=True,
                                timeout=args.timeout, check=True)
        rows = [dict(field.split("=", 1) for field in line.split()[1:])
                for line in result.stdout.splitlines() if line.startswith("RESULT ")]
        if not rows:
            raise RuntimeError("benchmark returned no results; check case selection")
        report["runs"].append(dict(version=version, command=command, rows=rows,
                                   stdout=result.stdout, stderr=result.stderr))
        args.output.write_text(json.dumps(report, indent=2) + chr(10))
        print(version, result.stdout, flush=True)
    def key(row):
        return tuple((name, row[name]) for name in ["case", "mode", "producers",
                                                  "sparse", "batch"] if name in row)
    keys = [key(row) for row in report["runs"][0]["rows"]]
    if any([key(row) for row in run["rows"]] != keys for run in report["runs"]):
        raise RuntimeError("benchmark workloads differ between runs")
    for index, identity in enumerate(keys):
        medians = {version: statistics.median(
            float(run["rows"][index]["median_ns"]) for run in report["runs"]
            if run["version"] == version) for version in ["before", "after"]}
        before, after = medians["before"], medians["after"]
        print(dict(identity), f"{before:.3f} -> {after:.3f} ns/msg; "
                              f"time reduction {(1 - after / before) * 100:.1f}%")


if __name__ == "__main__":
    main()
