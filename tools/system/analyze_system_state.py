#!/usr/bin/env python3
"""Correlate warmup-effect throughput modes with runtime system state."""

import argparse
import json
import math
from pathlib import Path
import re
import statistics
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools/aeron_compare"))

HIGH_MODE_THRESHOLD_MMSG_S = 10.0
SIGNIFICANT_CHANGE_PERCENT = 20.0
INT_RE = re.compile(r"^(?P<irq>\d+):\s+(?P<counts>(?:\d+\s+)+)")


def load_results(output):
    records = []
    for line in (output / "results.jsonl").read_text().splitlines():
        if line.strip():
            records.append(json.loads(line))
    if not records:
        raise ValueError(f"No records: {output / 'results.jsonl'}")
    return records


def complete_timeline(path):
    rows = []
    if not path.exists():
        return rows
    for line in path.read_text().splitlines()[1:]:
        values = line.split()
        if len(values) != 7:
            continue
        try:
            row = {
                "timestamp_ns": int(values[0]),
                "monotonic_ns": int(values[1]),
                "load1": float(values[2]),
                "load5": float(values[3]),
                "load15": float(values[4]),
                "running_tasks": int(values[5]),
                "process_count": int(values[6]),
            }
            row["sample_monotonic_ns"] = row["monotonic_ns"]
            rows.append(row)
        except ValueError:
            continue
    return rows


def complete_samples(records):
    """Only retain records with an explicit closed time interval."""
    required = ("started_monotonic_ns", "finished_monotonic_ns", "started_at_ns",
                "finished_at_ns")
    complete = []
    for record in records:
        if not all(key in record for key in required):
            raise ValueError(f"Round {record.get('number')} lacks time boundaries")
        if int(record["finished_monotonic_ns"]) <= int(record["started_monotonic_ns"]):
            raise ValueError(f"Round {record.get('number')} has an empty interval")
        complete.append(record)
    return complete


def parse_mpstat(path):
    """Parse alternating mpstat CPU and interrupt sections, keyed by sample end time."""
    if not path.exists():
        return []
    samples = {}
    section = None
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("Average:"):
            break
        if re.match(r"^\d\d:\d\d:\d\d\s+CPU\s+%usr", line):
            section = "cpu"
            continue
        if re.match(r"^\d\d:\d\d:\d\d\s+CPU\s+intr/s", line):
            section = "irq"
            continue
        values = line.split()
        if section == "cpu" and len(values) == 12 and values[1].isdigit():
            timestamp, cpu = values[:2]
            try:
                sample = samples.setdefault(timestamp, {
                    "timestamp": timestamp, "cpu": {}, "irqs": {},
                })
                sample["cpu"][cpu] = {
                    "%usr": float(values[2]), "%nice": float(values[3]),
                    "%sys": float(values[4]),
                    "%irq": float(values[5]), "%soft": float(values[6]),
                    "%steal": float(values[7]), "%idle": float(values[11]),
                }
            except ValueError:
                continue
        elif section == "irq" and len(values) == 3 and values[1].isdigit():
            timestamp, cpu = values[:2]
            try:
                samples.setdefault(timestamp, {
                    "timestamp": timestamp, "cpu": {}, "irqs": {},
                })["irqs"][cpu] = float(values[2])
            except ValueError:
                continue
    return [samples[key] for key in sorted(samples)]


def parse_timeline(path):
    rows = complete_timeline(path)
    if not rows:
        raise ValueError(f"No complete timeline samples: {path}")
    return rows


def align_interval(start_mono, finish_mono, samples):
    """Select samples whose 1-second measurement interval overlaps the run."""
    interval_ns = 1_000_000_000
    return [sample for sample in samples
            if sample["sample_monotonic_ns"] <= finish_mono
            and sample["sample_monotonic_ns"] + interval_ns >= start_mono]


def parse_interrupts(path):
    """Return the last per-IRQ and per-CPU interrupt counters."""
    if not path.exists():
        return {"per_cpu": {str(cpu): 0 for cpu in range(4)}, "per_irq": {}}
    total = {str(cpu): 0 for cpu in range(4)}
    per_irq = {}
    for line in path.read_text().splitlines():
        match = INT_RE.match(line.strip())
        if not match:
            continue
        try:
            counts = [int(value) for value in match.group("counts").split()]
        except ValueError:
            continue
        irq = match.group("irq")
        per_irq[irq] = counts
        for cpu, count in enumerate(counts[:4]):
            total[str(cpu)] += count
    return {"per_cpu": total, "per_irq": per_irq}


def interval_loadavg(start, finish, timeline):
    rows = align_interval(start, finish, timeline)
    if not rows:
        raise ValueError("No loadavg samples overlap a benchmark interval")
    return {
        "samples": len(rows),
        "load1": statistics.mean(row["load1"] for row in rows),
        "load5": statistics.mean(row["load5"] for row in rows),
        "load15": statistics.mean(row["load15"] for row in rows),
        "running_tasks": statistics.mean(row["running_tasks"] for row in rows),
    }


def interval_cpu(start, finish, samples):
    selected = align_interval(start, finish, samples)
    if not selected:
        raise ValueError("No mpstat samples overlap a benchmark interval")
    by_cpu = {}
    for cpu in ("1", "2", "3"):
        rows = [sample["cpu"][cpu] for sample in selected if cpu in sample["cpu"]]
        if not rows:
            raise ValueError(f"No mpstat samples for CPU{cpu}")
        busy = statistics.mean(
            row["%usr"] + row["%nice"] + row["%sys"] + row["%irq"]
            + row["%soft"] + row["%steal"]
            for row in rows
        )
        by_cpu[cpu] = {
            "busy_percent": busy,
            "usr_percent": statistics.mean(row["%usr"] for row in rows),
            "nice_percent": statistics.mean(row["%nice"] for row in rows),
            "sys_percent": statistics.mean(row["%sys"] for row in rows),
            "irq_percent": statistics.mean(row["%irq"] for row in rows),
            "soft_percent": statistics.mean(row["%soft"] for row in rows),
            "idle_percent": statistics.mean(row["%idle"] for row in rows),
        }
    return {"samples": len(selected), "cpus": by_cpu}


def attach_clock(records, mpstat_samples, timeline):
    """Map mpstat's wall-clock timestamps onto the monitor's monotonic timeline."""
    if not mpstat_samples:
        raise ValueError("No mpstat samples")
    anchor_second = records[0]["started_at_ns"] // 1_000_000_000
    # mpstat timestamps are local wall-clock values. Reconstruct their epoch second
    # from the benchmark's local calendar day, then match the nearest monitor sample.
    local = time.localtime(anchor_second)
    offset = local.tm_gmtoff
    local_day = (anchor_second + offset) // 86400
    clock_delta_ns = (records[0]["started_at_ns"]
                      - records[0]["started_monotonic_ns"])
    for sample in mpstat_samples:
        hour, minute, second = (int(value) for value in sample["timestamp"].split(":"))
        second_of_day = hour * 3600 + minute * 60 + second
        wall_second = local_day * 86400 + second_of_day - offset
        anchor_time = (anchor_second + offset) % 86400
        if second_of_day < anchor_time - 12 * 3600:
            wall_second += 86400
        elif second_of_day > anchor_time + 12 * 3600:
            wall_second -= 86400
        sample_monotonic_ns = (
            wall_second * 1_000_000_000 - clock_delta_ns + 500_000_000
        )
        candidates = [row for row in timeline
                      if abs(row["monotonic_ns"] - sample_monotonic_ns)
                      <= 1_500_000_000]
        row = min(candidates, key=lambda item: abs(
            item["monotonic_ns"] - sample_monotonic_ns
        )) if candidates else None
        if row is None:
            raise ValueError(
                f"No timeline sample for mpstat {sample['timestamp']}: "
                f"wall_second={wall_second} anchor_second={anchor_second}"
            )
        sample["sample_monotonic_ns"] = row["monotonic_ns"]
        sample["anchored_timestamp_ns"] = row["timestamp_ns"]
    return mpstat_samples


def interval_irq(start, finish, samples):
    selected = align_interval(start, finish, samples)
    if not selected:
        raise ValueError("No IRQ samples overlap a benchmark interval")
    by_cpu = {}
    for cpu in ("1", "2", "3"):
        rows = [sample["irqs"][cpu] for sample in selected if cpu in sample["irqs"]]
        if not rows:
            raise ValueError(f"No IRQ samples for CPU{cpu}")
        by_cpu[cpu] = {
            "irq_per_second": statistics.mean(rows),
            "irq_count": sum(rows),
            "samples": len(rows),
        }
    return by_cpu


def summarize_round(record, timeline, cpu_samples, irq_samples):
    start = int(record["started_monotonic_ns"])
    finish = int(record["finished_monotonic_ns"])
    rate = float(record["result"]["publish_msg_per_sec"]) / 1e6
    return {
        "number": record["number"],
        "group": record["group"],
        "phase": record["phase"],
        "payload": int(record["payload"]),
        "throughput_mmsg_s": rate,
        "mode": "high" if rate >= HIGH_MODE_THRESHOLD_MMSG_S else "low",
        "duration_seconds": (finish - start) / 1e9,
        "cpu": interval_cpu(start, finish, cpu_samples),
        "irq": interval_irq(start, finish, irq_samples),
        "loadavg": interval_loadavg(start, finish, timeline),
    }


def compare_modes(rounds, expose_span=True):
    high = [item for item in rounds if item["mode"] == "high"]
    low = [item for item in rounds if item["mode"] == "low"]
    if not high or not low:
        raise ValueError("Both high and low mode rounds are required for comparison")
    result = {"high_mode_rounds": len(high), "low_mode_rounds": len(low), "cpu": {}, "irq": {}}
    if expose_span:
        result["payloads"] = sorted({item["payload"] for item in rounds})
        result["duration_seconds"] = {
            "high": statistics.mean(item["duration_seconds"] for item in high),
            "low": statistics.mean(item["duration_seconds"] for item in low),
        }
    for cpu in ("1", "2", "3"):
        high_busy = [item["cpu"]["cpus"][cpu]["busy_percent"] for item in high]
        low_busy = [item["cpu"]["cpus"][cpu]["busy_percent"] for item in low]
        high_irq = [item["irq"][cpu]["irq_per_second"] for item in high]
        low_irq = [item["irq"][cpu]["irq_per_second"] for item in low]
        high_cpu_mean = statistics.mean(high_busy)
        low_cpu_mean = statistics.mean(low_busy)
        high_irq_mean = statistics.mean(high_irq)
        low_irq_mean = statistics.mean(low_irq)
        result["cpu"][cpu] = {
            "high_percent": high_cpu_mean,
            "low_percent": low_cpu_mean,
            "high_minus_low_percentage_points": high_cpu_mean - low_cpu_mean,
            "high_over_low_percent": percent_change(high_cpu_mean, low_cpu_mean),
        }
        result["irq"][cpu] = {
            "high_per_second": high_irq_mean,
            "low_per_second": low_irq_mean,
            "high_minus_low_percent": percent_change(high_irq_mean, low_irq_mean),
        }
    result["loadavg"] = {}
    for key in ("load1", "load5", "load15", "running_tasks"):
        high_mean = statistics.mean(item["loadavg"][key] for item in high)
        low_mean = statistics.mean(item["loadavg"][key] for item in low)
        result["loadavg"][key] = {
            "high": high_mean,
            "low": low_mean,
            "high_minus_low_percent": percent_change(high_mean, low_mean),
        }
    return result


def mode_breakdown(rounds):
    """Provide the primary measured-only view plus all-round mode attribution."""
    all_comparison = compare_modes(rounds)
    result = {"all_rounds": all_comparison}
    measured = [item for item in rounds if item["phase"] == "measured"]
    high = [item for item in measured if item["mode"] == "high"]
    low = [item for item in measured if item["mode"] == "low"]
    result["measured_only"] = {
        "high_mode_rounds": len(high),
        "low_mode_rounds": len(low),
    }
    if high and low:
        result["measured_only"].update(compare_modes(measured, expose_span=False))
    result["same_payload"] = same_payload_comparison(rounds)
    return result


def same_payload_comparison(rounds):
    result = []
    for payload in sorted({item["payload"] for item in rounds}):
        rows = [item for item in rounds if item["payload"] == payload]
        high = [item for item in rows if item["mode"] == "high"]
        low = [item for item in rows if item["mode"] == "low"]
        if not high or not low:
            continue
        row = {
            "payload": payload,
            "high_mode_rounds": len(high),
            "low_mode_rounds": len(low),
            "cpu": {},
            "irq": {},
        }
        for cpu in ("1", "2", "3"):
            high_cpu = statistics.mean(
                item["cpu"]["cpus"][cpu]["busy_percent"] for item in high
            )
            low_cpu = statistics.mean(
                item["cpu"]["cpus"][cpu]["busy_percent"] for item in low
            )
            high_irq = statistics.mean(
                item["irq"][cpu]["irq_per_second"] for item in high
            )
            low_irq = statistics.mean(
                item["irq"][cpu]["irq_per_second"] for item in low
            )
            row["cpu"][cpu] = {
                "high_percent": high_cpu,
                "low_percent": low_cpu,
                "high_over_low_percent": percent_change(high_cpu, low_cpu),
            }
            row["irq"][cpu] = {
                "high_per_second": high_irq,
                "low_per_second": low_irq,
                "high_minus_low_percent": percent_change(high_irq, low_irq),
            }
        result.append(row)
    return result


def percent_change(new, old):
    if old == 0:
        return None if new == 0 else math.inf
    return (new - old) / old * 100.0


def classify(comparison):
    cpu_changes = [row["high_over_low_percent"] for row in comparison["cpu"].values()]
    irq_changes = [row["high_minus_low_percent"] for row in comparison["irq"].values()]
    significant_cpu = [value for value in cpu_changes
                       if value is not None and abs(value) > SIGNIFICANT_CHANGE_PERCENT]
    significant_irq = [value for value in irq_changes
                       if value is not None and abs(value) > SIGNIFICANT_CHANGE_PERCENT]
    cpu_high = any(value is not None and value > 10.0 for value in cpu_changes)
    irq_low = any(value is not None and value < -30.0 for value in irq_changes)
    if cpu_high:
        conclusion = "high mode may use more CPU; inspect yield/sleep and backpressure retry"
    elif irq_low:
        conclusion = "high mode has lower IRQ rate; inspect IRQ interference and binding"
    elif significant_cpu or significant_irq:
        conclusion = "system-state metrics differ, but not in a decision threshold"
    else:
        conclusion = "no material CPU/IRQ difference; state switch is likely lower-level"
    return {
        "significant_change_threshold_percent": SIGNIFICANT_CHANGE_PERCENT,
        "significant_cpu_cpus": [
            cpu for cpu, row in comparison["cpu"].items()
            if row["high_over_low_percent"] is not None
            and abs(row["high_over_low_percent"]) > SIGNIFICANT_CHANGE_PERCENT
        ],
        "significant_irq_cpus": [
            cpu for cpu, row in comparison["irq"].items()
            if row["high_minus_low_percent"] is not None
            and abs(row["high_minus_low_percent"]) > SIGNIFICANT_CHANGE_PERCENT
        ],
        "cpu_high_over_low_gt_10_percent": cpu_high,
        "irq_high_less_than_low_30_percent": irq_low,
        "conclusion": conclusion,
    }


def not_applicable(reason):
    return {
        "available": False,
        "reason": reason,
        "significant_cpu_cpus": [],
        "significant_irq_cpus": [],
        "cpu_high_over_low_gt_10_percent": False,
        "irq_high_less_than_low_30_percent": False,
        "conclusion": "insufficient samples for measured-only comparison",
    }


def summarize(output):
    monitor = output / "system-monitor"
    records = complete_samples(load_results(output))
    timeline = parse_timeline(monitor / "timeline.log")
    cpu_samples = attach_clock(records, parse_mpstat(monitor / "mpstat.log"), timeline)
    irq_samples = cpu_samples
    rounds = [summarize_round(record, timeline, cpu_samples, irq_samples)
              for record in records]
    measured_rounds = [item for item in rounds if item["phase"] == "measured"]
    if not measured_rounds:
        raise ValueError("No measured rounds")
    breakdown = mode_breakdown(rounds)
    comparison = breakdown["all_rounds"]
    snapshot = parse_interrupts(monitor / "interrupts.log")
    return {
        "schema_version": 1,
        "source": str(output.resolve()),
        "high_mode_threshold_mmsg_s": HIGH_MODE_THRESHOLD_MMSG_S,
        "measured_rounds": len(measured_rounds),
        "all_rounds": len(rounds),
        "monitor": {
            "path": str(monitor),
            "timeline_samples": len(timeline),
            "mpstat_samples": len(cpu_samples),
            "interrupt_counters": snapshot["per_cpu"],
        },
        "rounds": rounds,
        "comparison": comparison,
        "mode_breakdown": breakdown,
        "decision": {
            "all_rounds": classify(comparison),
            "measured_only": (
                classify(breakdown["measured_only"])
                if "cpu" in breakdown["measured_only"]
                else not_applicable("measured rounds contain only one throughput mode")
            ),
        },
    }


def print_summary(result):
    comparison = result["comparison"]
    print(f"high mode: {comparison['high_mode_rounds']} rounds; "
          f"low mode: {comparison['low_mode_rounds']} rounds")
    print(f"{'CPU':<5} {'busy high':>10} {'busy low':>10} {'delta':>9} "
          f"{'IRQ/s high':>12} {'IRQ/s low':>12} {'delta':>9}")
    for cpu in ("1", "2", "3"):
        cpu_row = comparison["cpu"][cpu]
        irq_row = comparison["irq"][cpu]
        print(f"{cpu:<5} {cpu_row['high_percent']:>9.2f}% "
              f"{cpu_row['low_percent']:>9.2f}% "
              f"{cpu_row['high_over_low_percent']:>+8.2f}% "
              f"{irq_row['high_per_second']:>12.2f} "
              f"{irq_row['low_per_second']:>12.2f} "
              f"{irq_row['high_minus_low_percent']:>+8.2f}%")
    load = comparison["loadavg"]
    print(f"loadavg(1m): high={load['load1']['high']:.3f} "
          f"low={load['load1']['low']:.3f} "
          f"delta={load['load1']['high_minus_low_percent']:+.2f}%")
    print(f"conclusion (all rounds): {result['decision']['all_rounds']['conclusion']}")
    measured = result["decision"]["measured_only"]
    print(f"conclusion (measured only): {measured['conclusion']}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    output = args.output.resolve()
    result = summarize(output)
    path = output / "system-correlation.json"
    path.write_text(json.dumps(result, indent=2) + "\n")
    print_summary(result)
    print(f"\nwrote {path}")


if __name__ == "__main__":
    main()
