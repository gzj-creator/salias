#!/usr/bin/env python3
"""Correlate CLOCK_MONOTONIC tail windows with actual perf switch-out intervals."""

import argparse
import json
from pathlib import Path
import re
import subprocess


def parse_switches(text):
    pending = {}
    spans = []
    cpus = {}
    pattern = re.compile(r"\s(\d+) \[(\d+)\]\s+(\d+)\.(\d+): PERF_RECORD_SWITCH (OUT|IN)(.*)")
    for line in text.splitlines():
        match = pattern.search(line)
        if not match:
            continue
        pid, cpu, seconds, fraction, direction, extra = match.groups()
        now = int(seconds) * 10**9 + int(fraction.ljust(9, "0"))
        if direction == "OUT":
            pending[pid] = (now, "preempt" in extra, cpu)
        else:
            cpus.setdefault(pid, set()).add(int(cpu))
            if pid in pending:
                start, preempted, old_cpu = pending.pop(pid)
                spans.append(dict(pid=int(pid), cpu=int(old_cpu), start_ns=start, end_ns=now,
                                  off_cpu_ns=now - start, preempted=preempted))
    return spans, cpus


def correlate(events, spans):
    result = []
    for event in events:
        overlaps = []
        for span in spans:
            if not span["preempted"]:
                continue
            overlap = min(event["receive_ns"], span["end_ns"]) - max(event["send_ns"], span["start_ns"])
            if overlap > 0:
                overlaps.append({**span, "overlap_ns": overlap})
        result.append({**event, "overlaps": sorted(overlaps, key=lambda item: item["overlap_ns"], reverse=True)[:4]})
    return sorted(result, key=lambda item: item["latency_ns"], reverse=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    args = parser.parse_args()
    output = []
    for path in sorted(args.directory.glob("*.switch.data")):
        text = subprocess.check_output(["sudo", "-n", "perf", "script", "--show-switch-events", "-i", str(path)], text=True)
        prefix = path.with_suffix("").with_suffix("")
        prefix.with_suffix(".switches.txt").write_text(text)
        spans, cpus = parse_switches(text)
        events = []
        for line in prefix.with_suffix(".stdout").read_text().splitlines():
            if line.startswith("TAIL "):
                events.append({key: int(value) for key, value in (item.split("=", 1) for item in line.split()[1:])})
        row = dict(trace=str(path), pid_cpus={pid: sorted(value) for pid, value in cpus.items()},
                   longest_preemptions=sorted([span for span in spans if span["preempted"]],
                                             key=lambda item: item["off_cpu_ns"], reverse=True)[:10],
                   tail_events=correlate(events, spans))
        output.append(row)
        print(path.name, json.dumps(row["tail_events"][:1]), flush=True)
    (args.directory / "switch-analysis.json").write_text(json.dumps(output, indent=2) + "\n")


if __name__ == "__main__":
    main()
