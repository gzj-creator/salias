#!/usr/bin/env python3
"""Export perf evidence using the upstream FlameGraph tools, never synthetic stacks."""

import argparse
from collections import Counter
import json
from pathlib import Path
import re
import subprocess


def export(data, tools):
    prefix = data.with_suffix("")
    script_path = prefix.with_suffix(".stacks.txt")
    with script_path.open("w") as out:
        subprocess.run(["sudo", "-n", "perf", "script", "--no-inline", "-i", str(data)], stdout=out, check=True)
    counters = Counter()
    totals = Counter()
    roles = {}
    for block in script_path.read_text().split("\n\n"):
        lines = block.splitlines()
        if not lines:
            continue
        match = re.match(r"\S+\s+(\d+)\s+", lines[0])
        frames = [line.strip() for line in lines[1:] if line.startswith("\t")]
        if not match or not frames:
            continue
        pid = match[1]
        totals[pid] += 1
        if any("run_consumer_child" in frame for frame in frames):
            roles[pid] = "consumer"
        if any("Rx::try_recv" in frame or "aeron_image_poll" in frame for frame in frames):
            roles[pid] = "consumer"
        if any("run_producer_child" in frame for frame in frames):
            roles[pid] = "producer"
        if any("Tx::claim" in frame for frame in frames):
            roles[pid] = "producer"
        top = frames[0].split(" ", 1)[-1].rsplit(" (", 1)[0]
        counters[(pid, re.sub(r"\+0x[0-9a-f]+$", "", top))] += 1
    top = [{"pid": pid, "role": roles.get(pid, "unclassified"), "samples": total,
            "top": [{"symbol": symbol, "samples": count, "percent": 100 * count / total}
                    for (thread, symbol), count in counters.most_common() if thread == pid][:12]}
           for pid, total in totals.most_common()]
    prefix.with_suffix(".top.json").write_text(json.dumps(top, indent=2) + "\n")
    folded = prefix.with_suffix(".folded")
    with folded.open("w") as out:
        subprocess.run(["perl", str(tools / "stackcollapse-perf.pl"), str(script_path)], stdout=out, check=True)
    with prefix.with_suffix(".svg").open("w") as out:
        subprocess.run(["perl", str(tools / "flamegraph.pl"), "--title", data.name,
                        "--countname", "cpu-clock samples", str(folded)], stdout=out, check=True)
    report = prefix.with_suffix(".report.txt")
    with report.open("w") as out:
        subprocess.run(["sudo", "-n", "perf", "report", "--stdio", "--no-children", "--no-inline",
                        "-g", "none", "--sort", "pid,dso,symbol", "--percent-limit", "1", "-i", str(data)], stdout=out, check=True)
    print(json.dumps(dict(profile=str(data), threads=top[:4])), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--flamegraph", type=Path, required=True)
    args = parser.parse_args()
    for data in sorted(args.directory.glob("*.perf.data")):
        export(data, args.flamegraph)


if __name__ == "__main__":
    main()
