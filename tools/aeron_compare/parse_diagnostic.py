#!/usr/bin/env python3
"""Validate diagnostic evidence and summarize measured rounds, never warmups."""

import argparse
import hashlib
import json
import math
from pathlib import Path
from statistics import mean, median, quantiles, stdev


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
    sample_mean = mean(values)
    sample_stddev = stdev(values) if len(values) > 1 else None
    quartiles = quantiles(values, n=4, method="inclusive") if len(values) > 1 else [values[0]] * 3
    return dict(median=median(values), mean=sample_mean, min=min(values), max=max(values),
                q1=quartiles[0], q3=quartiles[2], iqr=quartiles[2] - quartiles[0],
                sample_stddev=sample_stddev,
                cv=(sample_stddev / sample_mean
                    if sample_stddev is not None and sample_mean != 0 else None))


def signed_statistics(values):
    """Signed ratio statistics must not expose a misleading standard-deviation CV."""
    result = statistics(values)
    result["cv"] = None
    return result


def measure_consistency(group):
    """Report throughput drift even when no zero-instrumentation control exists."""
    throughput = group["publish_mmsg_s"]
    relative_iqr = throughput["iqr"] / throughput["median"] if throughput["median"] else None
    return dict(cv=throughput["cv"], relative_iqr=relative_iqr,
                cv_below_5_percent=(throughput["cv"] is not None and
                                    throughput["cv"] < 0.05),
                cv_below_8_percent=(throughput["cv"] is not None and
                                    throughput["cv"] < 0.08))


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
    row["claim_timed_samples"] = None
    if mode == "timed":
        # 每次采样覆盖一条成功消息，因此分母是采样数而不是全部成功消息。
        row["claim_timed_samples"] = metrics["diagnostic_claim_timed_samples"]
        row["claim_avg_ns"] = ratio(metrics["diagnostic_claim_total_ns"],
                                    row["claim_timed_samples"], "claim time")
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
    empty = sum(consumer["poll_empty"] for consumer in consumers)
    polls = sum(consumer["poll_success"] for consumer in consumers)
    messages = sum(consumer["messages"] for consumer in consumers)
    row["consumer_empty_rate"] = ratio(empty, empty + polls, "all consumer polls")
    row["avg_batch"] = ratio(messages, polls, "all successful consumer polls")
    row["metrics"] = metrics
    return row


def assess_hypotheses(groups):
    """Separate the requested threshold checks from unsupported causal claims."""
    summaries = []
    for payload in (64, 256):
        available = {group["mode"]: group for group in groups if group["payload"] == payload}
        counters = available.get("counters", available.get("timed"))
        timed = available.get("timed")
        summaries.append(dict(
            payload=payload,
            counter_mode=counters["mode"] if counters else None,
            rounds=counters["rounds"] if counters else 0,
            backpressure_rate=counters["backpressure_rate"]["median"] if counters else None,
            consumer_empty_rate=counters["consumer_empty_rate"]["median"] if counters else None,
            avg_batch=counters["avg_batch"]["median"] if counters else None,
            claim_avg_ns=timed["claim_avg_ns"]["median"] if timed else None,
            claim_timed_samples=timed["claim_timed_samples"]["median"] if timed else None,
            claim_timing_mode="timed" if timed else None,
            aggregation="median of measured rounds; consumer ratios pool both consumers per round",
        ))
    small = summaries[0]
    hypotheses = {}
    for name, metric, threshold, meaning in (
            ("H1", "backpressure_rate", 0.1, "frequent backpressure"),
            ("H2", "consumer_empty_rate", 0.5, "high empty-poll fraction, not proof of consumer lag"),
            ("H3", "claim_avg_ns", 200, "slow successful try_claim, not Guard-exclusive timing")):
        value = small[metric]
        hypotheses[name] = dict(
            metric=metric, payload=64, value=value, threshold=threshold, comparison=">",
            threshold_exceeded=value > threshold if value is not None else None,
            meaning=meaning,
        )
    candidates = []
    if hypotheses["H1"]["threshold_exceeded"]:
        candidates.append("Phase 2A: validate batched/delayed progress writeback without instrumentation")
    if hypotheses["H3"]["threshold_exceeded"]:
        candidates.append("Phase 2B: isolate the single-publisher fast path and Guard cost")
    if hypotheses["H2"]["threshold_exceeded"]:
        candidates.append("Inspect poll batching and subscriber scans; empty polls can indicate publisher starvation")
    if not candidates:
        candidates.append("Expand 1P1S/1P3S and neighboring-payload scans before selecting Phase 2")
    perturbation = [dict(payload=g["payload"], mode=g["mode"],
                         paired_throughput_change_fraction=g["paired_throughput_change_fraction"]["median"])
                    for g in groups if g["mode"] != "off"
                    and g["paired_throughput_change_fraction"] is not None]
    measurement_controls = {}
    for payload in (64, 256):
        available = {group["mode"]: group for group in groups if group["payload"] == payload}
        off = available.get("off")
        counters = available.get("counters")
        timed = available.get("timed")
        measurement_controls[str(payload)] = dict(
            off_measure_consistency=measure_consistency(off) if off else None,
            counters_measure_consistency=measure_consistency(counters) if counters else None,
            timed_measure_consistency=measure_consistency(timed) if timed else None,
            counters_median_change=counters["paired_throughput_change_fraction"]["median"]
            if counters and counters.get("paired_throughput_change_fraction") else None,
            counters_change_below_5_percent=(
                abs(counters["paired_throughput_change_fraction"]["median"]) < 0.05
                if counters and counters.get("paired_throughput_change_fraction") else None),
            timed_median_change=timed["paired_throughput_change_fraction"]["median"]
            if timed and timed.get("paired_throughput_change_fraction") else None,
            timed_change_below_5_percent=(
                abs(timed["paired_throughput_change_fraction"]["median"]) < 0.05
                if timed and timed.get("paired_throughput_change_fraction") else None))
    return dict(payload_summary=summaries, hypotheses=hypotheses,
                phase2_candidates=candidates, instrumentation_controls=perturbation,
                instrumentation_exceeds_5_percent=any(
                    abs(g["paired_throughput_change_fraction"]) > 0.05 for g in perturbation)
                    if perturbation else None,
                measurement_controls=measurement_controls,
                hypothesis_naming="H2 follows the session prompt (consumer empty rate); the plan uses H2 for frame alignment",
                causal_confirmation="Threshold crossings alone do not establish consumer lag or Guard overhead")


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
                group["consumer_empty_rate"] = statistics([r["consumer_empty_rate"] for r in cell])
                group["avg_batch"] = statistics([r["avg_batch"] for r in cell])
                group["claim_avg_ns"] = (statistics([r["claim_avg_ns"] for r in cell])
                                          if mode == "timed" else None)
                group["claim_timed_samples"] = (statistics([r["claim_timed_samples"] for r in cell])
                                                 if mode == "timed" else None)
                group["consumers"] = [dict(consumer=i, **{
                    key: statistics([r["consumers"][i][key] for r in cell])
                    for key in ("empty_rate", "avg_batch")}) for i in range(2)]
                controls = {r["round"]: r["publish_mmsg_s"] for r in rows
                            if r["payload"] == payload and r["mode"] == "off"
                            and r["phase"] == "measured"}
                group["paired_throughput_change_fraction"] = (signed_statistics([
                    r["publish_mmsg_s"] / controls[r["round"]] - 1 for r in cell])
                    if controls else None)
            groups.append(group)
    return dict(schema_version=3, input_directory=str(directory.resolve()), environment=environment,
                measured_runs=sum(r["phase"] == "measured" for r in rows),
                warmup_runs=sum(r["phase"] == "warmup" for r in rows), groups=groups, runs=rows,
                **assess_hypotheses(groups),
                interpretation=[
                    "Empty poll rate above 50% does NOT prove consumers cannot keep up; it can indicate starvation.",
                    "Timed claim samples every 1000th successful try_claim; each sample covers the entire call, not ClaimReservationGuard alone.",
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
