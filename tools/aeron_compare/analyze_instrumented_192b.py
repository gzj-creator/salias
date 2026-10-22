#!/usr/bin/env python3
"""Compare backpressure and empty-poll counters across 192 B runtime modes."""

import argparse
import hashlib
import json
import math
from pathlib import Path
from statistics import mean, median, stdev

HIGH_MIN_MMSG_S = 15.0
LOW_MAX_MMSG_S = 10.0


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_manifest(output):
    manifest = {}
    for line in (output / "SHA256SUMS").read_text().splitlines():
        checksum, separator, name = line.partition("  ")
        if not separator or len(checksum) != 64:
            raise ValueError(f"Invalid checksum line: {line}")
        if name != "SHA256SUMS":
            manifest[name] = checksum
    return manifest


def load_records(output):
    manifest = load_manifest(output)
    records = []
    for line in (output / "results.jsonl").read_text().splitlines():
        record = json.loads(line)
        name = Path(record["stdout"]).name
        path = output / name
        if (
            manifest.get(name) != digest(path)
            or record.get("stdout_sha256") != manifest[name]
        ):
            raise ValueError(f"stdout checksum mismatch: {path}")
        records.append(record)
    if not records:
        raise ValueError("No results")
    return records


def ratio(numerator, denominator, label):
    if denominator <= 0:
        raise ValueError(f"No observations for {label}")
    return numerator / denominator


def stats(values, throughput=False):
    if not values or any(not math.isfinite(value) for value in values):
        raise ValueError("Require finite observations")
    sample_mean = mean(values)
    sample_stddev = stdev(values) if len(values) > 1 else None
    if throughput:
        return {
            "rounds": len(values),
            "mean_mmsg_s": sample_mean,
            "median_mmsg_s": median(values),
            "min_mmsg_s": min(values),
            "max_mmsg_s": max(values),
            "sample_stddev_mmsg_s": sample_stddev,
            "cv": sample_stddev / sample_mean if sample_stddev and sample_mean else None,
        }
    return {
        "rounds": len(values),
        "mean": sample_mean,
        "median": median(values),
        "min": min(values),
        "max": max(values),
        "sample_stddev": sample_stddev,
        "cv": sample_stddev / sample_mean if sample_stddev and sample_mean else None,
    }


def parse_round(record):
    result = record["result"]
    diagnostic = record["diagnostic"]
    expected = record["messages"]
    if (
        int(result["published"]) != expected
        or int(result["delivered"]) != 2 * expected
        or int(result["producers"]) != 1
        or int(result["consumers"]) != 2
        or int(result["payload"]) != 192
        or result["scenario"] != "fifo"
    ):
        raise ValueError(f"Delivery/topology mismatch: {record['number']}")
    if diagnostic.get("diagnostic_mode") != "counters":
        raise ValueError(f"Diagnostic mode mismatch: {record['number']}")
    success = int(diagnostic["diagnostic_claim_success"])
    backpressure = int(diagnostic["diagnostic_claim_backpressure"])
    if success != expected:
        raise ValueError(f"Claim count mismatch: {record['number']}")
    if int(diagnostic["diagnostic_claim_yield_cycles"]) != backpressure:
        raise ValueError(f"Yield count mismatch: {record['number']}")

    consumers = []
    for consumer in range(2):
        prefix = f"diagnostic_consumer{consumer}_"
        empty = int(diagnostic[prefix + "poll_empty"])
        polls = int(diagnostic[prefix + "poll_success"])
        messages = int(diagnostic[prefix + "poll_total_messages"])
        if messages != expected:
            raise ValueError(
                f"Consumer {consumer} message mismatch: {record['number']}"
            )
        consumers.append(
            {
                "consumer": consumer,
                "poll_empty": empty,
                "poll_success": polls,
                "messages": messages,
                "empty_rate": ratio(empty, empty + polls, "polls"),
                "avg_batch": ratio(messages, polls, "successful polls"),
            }
        )
    total_empty = sum(row["poll_empty"] for row in consumers)
    total_success = sum(row["poll_success"] for row in consumers)
    total_messages = sum(row["messages"] for row in consumers)
    return {
        "number": record["number"],
        "round": record["round"],
        "phase": record["phase"],
        "throughput_mmsg_s": float(result["publish_msg_per_sec"]) / 1e6,
        "claim_success": success,
        "claim_backpressure": backpressure,
        "backpressure_rate": ratio(backpressure, success + backpressure, "claims"),
        "consumer_empty_rate": ratio(
            total_empty, total_empty + total_success, "consumer polls"
        ),
        "avg_batch": ratio(total_messages, total_success, "successful polls"),
        "consumers": consumers,
    }


def classify(rows):
    high = [row for row in rows if row["throughput_mmsg_s"] >= HIGH_MIN_MMSG_S]
    low = [row for row in rows if row["throughput_mmsg_s"] < LOW_MAX_MMSG_S]
    intermediate = [
        row
        for row in rows
        if LOW_MAX_MMSG_S <= row["throughput_mmsg_s"] < HIGH_MIN_MMSG_S
    ]
    return high, low, intermediate


def group_stats(rows, key, throughput=False):
    return (
        stats([row[key] for row in rows], throughput=throughput)
        if rows
        else None
    )


def relative_change(low, high, metric):
    denominator = low[metric]["mean"] if low and low[metric]["mean"] else None
    return high[metric]["mean"] / denominator - 1 if high and denominator else None


def summarize(directory, require_both_modes=True):
    environment = json.loads((directory / "environment.json").read_text())
    records = load_records(directory)
    measured = [
        parse_round(record) for record in records if record["phase"] == "measured"
    ]
    expected_rounds = int(environment["rounds"])
    if len(measured) != expected_rounds:
        raise ValueError("Unexpected measured round count")
    high_rows, low_rows, intermediate_rows = classify(measured)
    if require_both_modes and (not high_rows or not low_rows):
        raise ValueError(
            f"Need both runtime modes; high={len(high_rows)}, low={len(low_rows)}"
        )
    high = {
        "throughput": group_stats(
            high_rows, "throughput_mmsg_s", throughput=True
        ),
        "backpressure_rate": group_stats(high_rows, "backpressure_rate"),
        "consumer_empty_rate": group_stats(high_rows, "consumer_empty_rate"),
        "avg_batch": group_stats(high_rows, "avg_batch"),
    }
    low = {
        "throughput": group_stats(low_rows, "throughput_mmsg_s", throughput=True),
        "backpressure_rate": group_stats(low_rows, "backpressure_rate"),
        "consumer_empty_rate": group_stats(low_rows, "consumer_empty_rate"),
        "avg_batch": group_stats(low_rows, "avg_batch"),
    }
    backpressure_ratio = (
        low["backpressure_rate"]["mean"] / high["backpressure_rate"]["mean"]
        if high["backpressure_rate"] is not None
        and low["backpressure_rate"] is not None
        and high["backpressure_rate"]["mean"] > 0
        else None
    )
    empty_ratio = (
        low["consumer_empty_rate"]["mean"] / high["consumer_empty_rate"]["mean"]
        if high["consumer_empty_rate"] is not None
        and low["consumer_empty_rate"] is not None
        else None
    )
    comparison = None
    if high_rows and low_rows:
        comparison = {
            "throughput_mmsg_s": {
                "high_mean": high["throughput"]["mean_mmsg_s"],
                "low_mean": low["throughput"]["mean_mmsg_s"],
                "low_to_high_ratio": (
                    low["throughput"]["mean_mmsg_s"]
                    / high["throughput"]["mean_mmsg_s"]
                ),
            },
            "backpressure_rate": {
                "high_mean": high["backpressure_rate"]["mean"],
                "low_mean": low["backpressure_rate"]["mean"],
                "low_to_high_ratio": backpressure_ratio,
                "relative_change": relative_change(low, high, "backpressure_rate"),
            },
            "consumer_empty_rate": {
                "high_mean": high["consumer_empty_rate"]["mean"],
                "low_mean": low["consumer_empty_rate"]["mean"],
                "low_to_high_ratio": empty_ratio,
                "relative_change": relative_change(low, high, "consumer_empty_rate"),
            },
            "avg_batch": {
                "high_mean": high["avg_batch"]["mean"],
                "low_mean": low["avg_batch"]["mean"],
                "low_to_high_ratio": (
                    low["avg_batch"]["mean"] / high["avg_batch"]["mean"]
                ),
                "relative_change": relative_change(low, high, "avg_batch"),
            },
        }
    candidates = [
        (name, abs(row["relative_change"]))
        for name, row in (comparison or {}).items()
        if row.get("relative_change") is not None
    ]
    largest = max(candidates, key=lambda item: item[1]) if candidates else None
    return {
        "schema_version": 1,
        "source": str(directory.resolve()),
        "results_sha256": digest(directory / "results.jsonl"),
        "environment_sha256": digest(directory / "environment.json"),
        "thresholds_mmsg_s": {
            "high_min": HIGH_MIN_MMSG_S,
            "low_max": LOW_MAX_MMSG_S,
            "intermediate": "excluded from both groups",
        },
        "measured_rounds": len(measured),
        "warmup_rounds": len(records) - len(measured),
        "mode_rounds": {
            "high": len(high_rows),
            "low": len(low_rows),
            "intermediate": len(intermediate_rows),
        },
        "high": high,
        "low": low,
        "comparison": comparison,
        "rounds": measured,
        "intermediate_rounds": intermediate_rows,
        "answers": {
            "backpressure_low_to_high_ratio": backpressure_ratio,
            "backpressure_difference_over_100x": (
                backpressure_ratio > 100 if backpressure_ratio is not None else None
            ),
            "consumer_empty_low_to_high_ratio": empty_ratio,
            "consumer_empty_difference_over_5x": (
                empty_ratio > 5 if empty_ratio is not None else None
            ),
            "largest_absolute_relative_change": largest[0] if largest else None,
            "largest_absolute_relative_change_value": largest[1] if largest else None,
            "backpressure_threshold_high_lt_0_001_low_gt_1_percent": (
                high["backpressure_rate"] is not None
                and low["backpressure_rate"] is not None
                and high["backpressure_rate"]["mean"] < 0.00001
                and low["backpressure_rate"]["mean"] > 0.01
            ),
            "poll_threshold_high_lt_10_low_gt_50_percent": (
                high["consumer_empty_rate"] is not None
                and low["consumer_empty_rate"] is not None
                and high["consumer_empty_rate"]["mean"] < 0.10
                and low["consumer_empty_rate"]["mean"] > 0.50
            ),
        },
    }


def print_result(result):
    high = result["high"]
    low = result["low"]
    comparison = result["comparison"]

    def percent(value):
        return value * 100

    if high["throughput"] is not None:
        print(
            f"High mode: {result['mode_rounds']['high']} rounds; "
            f"median={high['throughput']['median_mmsg_s']:.3f} Mmsg/s"
        )
    else:
        print("High mode: 0 rounds (insufficient comparison)")
    if low["throughput"] is not None:
        print(
            f"Low mode: {result['mode_rounds']['low']} rounds; "
            f"median={low['throughput']['median_mmsg_s']:.3f} Mmsg/s"
        )
    else:
        print("Low mode: 0 rounds (insufficient comparison)")
    print(
        f"Intermediate: {result['mode_rounds']['intermediate']} rounds "
        "(excluded)"
    )
    print()
    if comparison is None:
        print("| metric | high | low | difference |")
        print("|---|---:|---:|---:|")
        low_throughput = low["throughput"]
        low_backpressure = low["backpressure_rate"]
        low_empty = low["consumer_empty_rate"]
        low_batch = low["avg_batch"]
        if low_throughput is None:
            print("| comparison unavailable | -- | -- | no high or low sample |")
            return
        print(
            f"| median throughput | N/A | "
            f"{low_throughput['median_mmsg_s']:.3f} Mmsg/s | undefined |"
        )
        print(
            f"| backpressure rate | N/A | "
            f"{percent(low_backpressure['mean']):.6f}% | undefined |"
        )
        print(
            f"| poll empty rate | N/A | "
            f"{percent(low_empty['mean']):.3f}% | undefined |"
        )
        print(f"| average batch | N/A | {low_batch['mean']:.3f} | undefined |")
        return
    print("| metric | high | low | difference |")
    print("|---|---:|---:|---:|")
    print(
        f"| median throughput | {high['throughput']['median_mmsg_s']:.3f} Mmsg/s "
        f"| {low['throughput']['median_mmsg_s']:.3f} Mmsg/s "
        f"| {comparison['throughput_mmsg_s']['low_to_high_ratio']:.3f}x |"
    )
    print(
        f"| backpressure rate | {percent(high['backpressure_rate']['mean']):.6f}% "
        f"| {percent(low['backpressure_rate']['mean']):.6f}% "
        f"| {comparison['backpressure_rate']['relative_change'] * 100:+.2f}% |"
    )
    print(
        f"| poll empty rate | {percent(high['consumer_empty_rate']['mean']):.3f}% "
        f"| {percent(low['consumer_empty_rate']['mean']):.3f}% "
        f"| {comparison['consumer_empty_rate']['relative_change'] * 100:+.2f}% |"
    )
    print(
        f"| average batch | {high['avg_batch']['mean']:.3f} "
        f"| {low['avg_batch']['mean']:.3f} "
        f"| {comparison['avg_batch']['relative_change'] * 100:+.2f}% |"
    )
    print()
    answers = result["answers"]
    if answers["backpressure_low_to_high_ratio"] is None:
        print("backpressure low/high ratio: undefined (high group is zero)")
    else:
        print(
            "backpressure low/high ratio: "
            f"{answers['backpressure_low_to_high_ratio']:.3f}x"
        )
    if answers["consumer_empty_low_to_high_ratio"] is None:
        print("poll empty low/high ratio: undefined (high group is empty)")
    else:
        print(
            f"poll empty low/high ratio: "
            f"{answers['consumer_empty_low_to_high_ratio']:.3f}x"
        )
    if answers["largest_absolute_relative_change"] is not None:
        print(
            f"largest relative difference: "
            f"{answers['largest_absolute_relative_change']} "
            f"({answers['largest_absolute_relative_change_value'] * 100:.2f}%)"
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument(
        "--allow-single-mode",
        action="store_true",
        help="Report observed counters even when no high-mode comparison exists",
    )
    args = parser.parse_args()
    result = summarize(
        args.directory.resolve(), require_both_modes=not args.allow_single_mode
    )
    output = args.directory / "instrumented-192b-comparison.json"
    output.write_text(json.dumps(result, indent=2) + "\n")
    from analyze_topology_sweep import refresh_checksums

    refresh_checksums(args.directory)
    print_result(result)
    print(f"\nwrote {output}")


if __name__ == "__main__":
    main()
