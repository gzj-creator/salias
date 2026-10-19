#!/usr/bin/env python3
"""Validate diagnostic evidence and summarize measured rounds, never warmups."""

import argparse
import hashlib
import json
import math
from pathlib import Path
from statistics import mean, median, stdev


def parse_stdout(path):
    metrics = {}
    for line in path.read_text().splitlines():
        if not line.startswith("diagnostic_"):
            continue
        key, value = line.split("=", 1)
        if key in metrics:
            raise ValueError(f"Duplicate {key}: {path}")
        if key == "diagnostic_mode":
            metrics[key] = value
            continue
        number = float(value) if any(c in value for c in ".eE") else int(value)
        if not math.isfinite(number) or number < 0:
            raise ValueError(f"Invalid {key}: {path}")
        metrics[key] = number
    return metrics


def ratio(numerator, denominator, label):
    if denominator <= 0:
        raise ValueError(f"No observations for {label}")
    return numerator / denominator


def statistics(values):
    return dict(median=median(values), mean=mean(values), min=min(values), max=max(values),
                sample_stddev=stdev(values) if len(values) > 1 else None)


def analyze_record(record, directory):
    path = directory / Path(record["stdout"]).name
    if hashlib.sha256(path.read_bytes()).hexdigest() != record["stdout_sha256"]:
        raise ValueError(f"stdout checksum mismatch: {path}")
    metrics = parse_stdout(path)
    mode = record["mode"]
    row = {key: record[key] for key in ("number", "payload", "round", "phase", "mode", "stdout")}
    result = record["result"]
    expected = record["messages"]
    if (int(result["published"]) != expected or int(result["delivered"]) != 2 * expected or
            int(result["producers"]) != 1 or int(result["consumers"]) != 2):
        raise ValueError(f"Delivery/topology mismatch: {path}")
    row["publish_mmsg_s"] = float(result["publish_msg_per_sec"]) / 1e6
    row["duration_s"] = float(result["seconds"])
    if mode == "off":
        if metrics:
            raise ValueError(f"Unexpected diagnostics in control: {path}")
        return row
    if metrics.get("diagnostic_mode") != mode:
        raise ValueError(f"Diagnostic mode mismatch: {path}")
    success = metrics["diagnostic_claim_success"]
    backpressure = metrics["diagnostic_claim_backpressure"]
    if success != expected or metrics["diagnostic_claim_yield_cycles"] != backpressure:
        raise ValueError(f"Claim count mismatch: {path}")
    row["backpressure_rate"] = ratio(backpressure, success + backpressure, "claims")
    row["backpressure_per_success"] = ratio(backpressure, success, "successful claims")
    row["claim_avg_ns"] = None
    if mode == "timed":
        # Recompute from integer totals rather than rounded console output.
        row["claim_avg_ns"] = ratio(metrics["diagnostic_claim_total_ns"], success, "claim time")
    consumers = []
    for i in range(2):
        prefix = f"diagnostic_consumer{i}_"
        empty, polls, messages = (metrics[prefix + key]
                                  for key in ("poll_empty", "poll_success", "poll_total_messages"))
        if messages != expected:
            raise ValueError(f"Consumer {i} message mismatch: {path}")
        consumers.append(dict(consumer=i, poll_empty=empty, poll_success=polls, messages=messages,
                              empty_rate=ratio(empty, empty + polls, "polls"),
                              avg_batch=ratio(messages, polls, "successful polls")))
    row["consumers"] = consumers
    row["metrics"] = metrics
    return row


def summarize(directory):
    environment = json.loads((directory / "environment.json").read_text())
    records = [json.loads(line) for line in (directory / "results.jsonl").read_text().splitlines()]
    modes = environment["modes"]
    expected = {(payload, mode, round_number) for payload in (64, 256) for mode in modes
                for round_number in range(environment["rounds"] + 1)}
    actual = [(r["payload"], r["mode"], r["round"]) for r in records]
    if len(actual) != len(set(actual)) or set(actual) != expected:
        raise ValueError("Incomplete or duplicate diagnostic matrix")
    if any(r["phase"] != ("warmup" if r["round"] == 0 else "measured") for r in records):
        raise ValueError("Incorrect warmup classification")
    rows = [analyze_record(record, directory) for record in records]
    groups = []
    for payload in (64, 256):
        for mode in modes:
            cell = [r for r in rows if r["payload"] == payload and r["mode"] == mode
                    and r["phase"] == "measured"]
            group = dict(payload=payload, mode=mode, rounds=len(cell),
                         publish_mmsg_s=statistics([r["publish_mmsg_s"] for r in cell]),
                         duration_s=statistics([r["duration_s"] for r in cell]))
            if mode != "off":
                group["backpressure_rate"] = statistics([r["backpressure_rate"] for r in cell])
                group["claim_avg_ns"] = (statistics([r["claim_avg_ns"] for r in cell])
                                          if mode == "timed" else None)
                group["consumers"] = [dict(consumer=i, **{
                    key: statistics([r["consumers"][i][key] for r in cell])
                    for key in ("empty_rate", "avg_batch")}) for i in range(2)]
                controls = {r["round"]: r["publish_mmsg_s"] for r in rows
                            if r["payload"] == payload and r["mode"] == "off"
                            and r["phase"] == "measured"}
                group["paired_throughput_change_fraction"] = (statistics([
                    r["publish_mmsg_s"] / controls[r["round"]] - 1 for r in cell])
                    if controls else None)
            groups.append(group)
    return dict(schema_version=1, input_directory=str(directory.resolve()), environment=environment,
                measured_runs=sum(r["phase"] == "measured" for r in rows),
                warmup_runs=sum(r["phase"] == "warmup" for r in rows), groups=groups, runs=rows,
                interpretation=[
                    "Empty poll rate above 50% does NOT prove consumers cannot keep up; it can indicate starvation.",
                    "Timed claim covers the entire successful try_claim, not ClaimReservationGuard alone.",
                    "Timing and counters perturb scheduling; compare paired controls before causal attribution."])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    summary = summarize(args.directory)
    output = args.directory / "diagnostic_summary.json"
    output.write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps({"output": str(output), "groups": summary["groups"]}, indent=2))


if __name__ == "__main__":
    main()
