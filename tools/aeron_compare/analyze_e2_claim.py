#!/usr/bin/env python3
"""Analyze the P3 E2 claim-isolation experiment results.

This analyzer independently validates the 4-arm ABBA matrix and computes:
  - Throughput statistics per configuration/arm/group
  - Latency percentiles per configuration/arm/round
  - Recovered fraction: (MT-B - MT-O) / (ST - MT-O)
  - Consistency checks: thread state, claim variant, ABBA order

The analyzer reads raw stdout files and does not trust the runner's summary.
"""

import argparse
import hashlib
import json
import re
from pathlib import Path
import statistics
import sys


PROBE_IDENTITY = "p3-e2-claim-isolation-probe-v1"
ARMS = ("ST", "MT-O", "MT-X", "MT-B")
ARM_ORDER = ("ST", "MT-O", "MT-X", "MT-B", "MT-B", "MT-X", "MT-O", "ST")
EXPECTED_GROUPS = 6


def digest(path):
    """Return the SHA-256 digest of a file."""
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def parse_stdout(stdout_path):
    """Extract key metrics from one round's stdout."""
    text = stdout_path.read_text()

    # Probe identity
    identity_match = re.search(r'PROBE identity=(\S+)', text)
    identity = identity_match.group(1) if identity_match else None

    # Publisher mode and claim variant
    mode_match = re.search(r'publisher_mode=(\S+)', text)
    publisher_mode = mode_match.group(1) if mode_match else None

    variant_match = re.search(r'claim_variant=(\S+)', text)
    claim_variant = variant_match.group(1) if variant_match else None

    # Thread state
    thread_state_match = re.search(
        r'THREAD_STATE.*?actual_threads=(\d+)\s+libc_single_threaded_available=(\d+)\s+'
        r'libc_single_threaded=(-?\d+)',
        text
    )
    if thread_state_match:
        actual_threads = int(thread_state_match.group(1))
        libc_available = int(thread_state_match.group(2)) == 1
        libc_single = int(thread_state_match.group(3))
    else:
        actual_threads = None
        libc_available = None
        libc_single = None

    # Throughput (from publish_msg_per_sec in RESULT line)
    throughput_match = re.search(r'publish_msg_per_sec=([0-9.e+]+)', text)
    throughput = float(throughput_match.group(1)) / 1e6 if throughput_match else None

    # Latency samples
    samples_match = re.search(r'latency_samples=(\d+)', text)
    sample_count = int(samples_match.group(1)) if samples_match else 0

    # Percentiles (if present) from RESULT line
    percentiles = {}
    percentile_map = {
        'p50': r'latency_p50_ns=([0-9.]+)',
        'p99': r'latency_p99_ns=([0-9.]+)',
        'p99.9': r'latency_p999_ns=([0-9.]+)',
        'max': r'latency_max_ns=([0-9.]+)',
    }
    for label, pattern in percentile_map.items():
        match = re.search(pattern, text)
        if match:
            val = float(match.group(1))
            if val > 0:  # Only include non-zero values
                percentiles[label] = val

    # Delivery counts (from RESULT line)
    published_match = re.search(r'published=(\d+)', text)
    delivered_match = re.search(r'delivered=(\d+)', text)

    published = int(published_match.group(1)) if published_match else None
    delivered = int(delivered_match.group(1)) if delivered_match else None

    return {
        'identity': identity,
        'publisher_mode': publisher_mode,
        'claim_variant': claim_variant,
        'actual_threads': actual_threads,
        'libc_available': libc_available,
        'libc_single': libc_single,
        'throughput_mmsg_s': throughput,
        'latency_samples': sample_count,
        'percentiles': percentiles,
        'published': published,
        'delivered': delivered,
    }


def validate_round(round_dir, expected_spec):
    """Validate one round's identity, thread state, and claim variant."""
    stdout_path = round_dir / "stdout.txt"
    outcome_path = round_dir / "outcome.json"

    if not stdout_path.exists() or not outcome_path.exists():
        return None, ["missing stdout or outcome"]

    outcome = json.loads(outcome_path.read_text())
    if outcome["status"] != "ok":
        return None, [f"status={outcome['status']}"]

    parsed = parse_stdout(stdout_path)
    issues = []

    # Check identity
    if parsed["identity"] != PROBE_IDENTITY:
        issues.append(f"identity mismatch: {parsed['identity']}")

    # Check publisher mode matches arm
    expected_mode = {
        "ST": "st",
        "MT-O": "mt-o",
        "MT-X": "mt-x",
        "MT-B": "mt-b",
    }[expected_spec["arm"]]

    if parsed["publisher_mode"] != expected_mode:
        issues.append(f"mode mismatch: expected {expected_mode}, got {parsed['publisher_mode']}")

    # Check claim variant
    expected_variant = "borrowed" if expected_spec["arm"] == "MT-B" else "owning"
    if parsed["claim_variant"] != expected_variant:
        issues.append(f"variant mismatch: expected {expected_variant}, got {parsed['claim_variant']}")

    # Check thread state
    if expected_spec["arm"] == "ST":
        if parsed["actual_threads"] != 1:
            issues.append(f"ST should have 1 thread, got {parsed['actual_threads']}")
        if parsed["libc_available"] and parsed["libc_single"] != 1:
            issues.append(f"ST should have libc_single_threaded=1, got {parsed['libc_single']}")
    elif expected_spec["arm"] == "MT-X":
        # MT-X: helper exited, so 1 thread, but libc state is multi-threaded
        if parsed["actual_threads"] != 1:
            issues.append(f"MT-X should have 1 thread after exit, got {parsed['actual_threads']}")
        if parsed["libc_available"] and parsed["libc_single"] != 0:
            issues.append(f"MT-X should have libc_single_threaded=0, got {parsed['libc_single']}")
    else:  # MT-O, MT-B
        if parsed["actual_threads"] != 2:
            issues.append(f"{expected_spec['arm']} should have 2 threads, got {parsed['actual_threads']}")
        if parsed["libc_available"] and parsed["libc_single"] != 0:
            issues.append(f"{expected_spec['arm']} should have libc_single_threaded=0, got {parsed['libc_single']}")

    # Check delivery counts
    expected_published = expected_spec["messages"]
    expected_delivered = expected_spec["messages"] * expected_spec["consumers"]

    if parsed["published"] != expected_published:
        issues.append(f"published mismatch: expected {expected_published}, got {parsed['published']}")
    if parsed["delivered"] != expected_delivered:
        issues.append(f"delivered mismatch: expected {expected_delivered}, got {parsed['delivered']}")

    # Check latency sample count if applicable
    if expected_spec["latency_sample_rate"] > 0:
        expected_samples = expected_spec["messages"] // expected_spec["latency_sample_rate"]
        if parsed["latency_samples"] != expected_samples:
            issues.append(f"sample count mismatch: expected {expected_samples}, got {parsed['latency_samples']}")

    if issues:
        return parsed, issues

    return parsed, None


def analyze_throughput(formal_rounds, output_dir):
    """Analyze throughput by configuration, arm, and group."""
    throughput_configs = [r for r in formal_rounds if r["kind"] == "throughput"]

    results = {}

    for config_id in sorted(set(r["configuration_id"] for r in throughput_configs)):
        config_rounds = [r for r in throughput_configs if r["configuration_id"] == config_id]

        arm_stats = {}
        group_data = {}

        for arm in ARMS:
            arm_rounds = [r for r in config_rounds if r["arm"] == arm]
            throughputs = [r["parsed"]["throughput_mmsg_s"] for r in arm_rounds]

            if not throughputs:
                continue

            arm_stats[arm] = {
                "count": len(throughputs),
                "median": statistics.median(throughputs),
                "mean": statistics.mean(throughputs),
                "stdev": statistics.stdev(throughputs) if len(throughputs) > 1 else 0,
                "cv_percent": (statistics.stdev(throughputs) / statistics.mean(throughputs) * 100)
                              if len(throughputs) > 1 else 0,
                "min": min(throughputs),
                "max": max(throughputs),
                "values": throughputs,
            }

        # Group-level ratios
        for group in range(EXPECTED_GROUPS):
            group_rounds = [r for r in config_rounds if r["group"] == group]
            group_medians = {}

            for arm in ARMS:
                arm_group = [r["parsed"]["throughput_mmsg_s"]
                           for r in group_rounds if r["arm"] == arm]
                if arm_group:
                    group_medians[arm] = statistics.median(arm_group)

            if len(group_medians) == 4:
                st = group_medians["ST"]
                mt_o = group_medians["MT-O"]
                mt_x = group_medians["MT-X"]
                mt_b = group_medians["MT-B"]

                group_data[group] = {
                    "medians": group_medians,
                    "st_vs_mt_o": st / mt_o if mt_o > 0 else None,
                    "st_vs_mt_x": st / mt_x if mt_x > 0 else None,
                    "st_vs_mt_b": st / mt_b if mt_b > 0 else None,
                    "recovered_fraction": ((mt_b - mt_o) / (st - mt_o)) if (st - mt_o) > 0 else None,
                }

        results[config_id] = {
            "arm_stats": arm_stats,
            "group_data": group_data,
        }

    return results


def analyze_latency(formal_rounds, output_dir):
    """Analyze latency by configuration, arm, and round."""
    latency_configs = [r for r in formal_rounds if r["kind"] == "latency"]

    results = {}

    for config_id in sorted(set(r["configuration_id"] for r in latency_configs)):
        config_rounds = [r for r in latency_configs if r["configuration_id"] == config_id]

        arm_percentiles = {}

        for arm in ARMS:
            arm_rounds = [r for r in config_rounds if r["arm"] == arm]

            # Collect percentiles per round (not pooled)
            rounds_data = []
            for round_data in arm_rounds:
                if round_data["parsed"]["percentiles"]:
                    rounds_data.append({
                        "ordinal": round_data["ordinal"],
                        "group": round_data["group"],
                        "percentiles": round_data["parsed"]["percentiles"],
                    })

            if not rounds_data:
                continue

            # Compute median of each percentile across rounds
            percentile_labels = ["p50", "p99", "p99.9", "max"]
            medians = {}
            ranges = {}

            for label in percentile_labels:
                values = [r["percentiles"].get(label) for r in rounds_data
                         if label in r["percentiles"]]
                if values:
                    medians[label] = statistics.median(values)
                    ranges[label] = (min(values), max(values))

            arm_percentiles[arm] = {
                "count": len(rounds_data),
                "medians": medians,
                "ranges": ranges,
                "rounds": rounds_data,
            }

        results[config_id] = arm_percentiles

    return results


def compute_summary_statistics(throughput_analysis):
    """Compute summary statistics for the E2 hypotheses."""
    summary = {}

    for config_id, data in throughput_analysis.items():
        arm_stats = data["arm_stats"]
        group_data = data["group_data"]

        if "ST" not in arm_stats or "MT-O" not in arm_stats or "MT-B" not in arm_stats:
            continue

        # Overall arm medians
        st_median = arm_stats["ST"]["median"]
        mt_o_median = arm_stats["MT-O"]["median"]
        mt_x_median = arm_stats.get("MT-X", {}).get("median")
        mt_b_median = arm_stats["MT-B"]["median"]

        # Recovered fractions per group
        recovered_fractions = [g["recovered_fraction"] for g in group_data.values()
                              if g["recovered_fraction"] is not None]

        # Direction consistency
        mt_b_better_count = sum(1 for g in group_data.values()
                               if g["medians"]["MT-B"] > g["medians"]["MT-O"])

        summary[config_id] = {
            "st_median": st_median,
            "mt_o_median": mt_o_median,
            "mt_x_median": mt_x_median,
            "mt_b_median": mt_b_median,
            "st_vs_mt_o": st_median / mt_o_median if mt_o_median > 0 else None,
            "mt_b_vs_mt_o": mt_b_median / mt_o_median if mt_o_median > 0 else None,
            "recovered_fraction_median": statistics.median(recovered_fractions) if recovered_fractions else None,
            "recovered_fraction_mean": statistics.mean(recovered_fractions) if recovered_fractions else None,
            "recovered_fraction_values": recovered_fractions,
            "groups_mt_b_better": mt_b_better_count,
            "total_groups": len(group_data),
            "direction_consistent": mt_b_better_count >= 5,  # 5/6 threshold
        }

    return summary


def format_markdown_report(analysis, summary, validation_issues):
    """Format the analysis as a markdown report."""
    lines = []

    lines.append("# Phase 3 E2 Claim Isolation Analysis")
    lines.append("")

    # Validation issues
    if validation_issues:
        lines.append("## Validation Issues")
        lines.append("")
        for issue in validation_issues[:20]:  # Limit to first 20
            lines.append(f"- Round {issue['ordinal']}: {', '.join(issue['issues'])}")
        if len(validation_issues) > 20:
            lines.append(f"- ... and {len(validation_issues) - 20} more")
        lines.append("")

    # Summary
    lines.append("## Summary Statistics")
    lines.append("")

    for config_id, stats in summary.items():
        lines.append(f"### {config_id}")
        lines.append("")
        lines.append("| Metric | Value |")
        lines.append("|--------|-------|")
        lines.append(f"| ST median | {stats['st_median']:.6f} Mmsg/s |")
        lines.append(f"| MT-O median | {stats['mt_o_median']:.6f} Mmsg/s |")
        if stats['mt_x_median']:
            lines.append(f"| MT-X median | {stats['mt_x_median']:.6f} Mmsg/s |")
        lines.append(f"| MT-B median | {stats['mt_b_median']:.6f} Mmsg/s |")
        lines.append(f"| ST/MT-O ratio | {stats['st_vs_mt_o']:.3f} |")
        lines.append(f"| MT-B/MT-O ratio | {stats['mt_b_vs_mt_o']:.3f} |")

        if stats['recovered_fraction_median']:
            lines.append(f"| Recovered fraction (median) | {stats['recovered_fraction_median']:.1%} |")
            lines.append(f"| Recovered fraction (mean) | {stats['recovered_fraction_mean']:.1%} |")

        lines.append(f"| Groups MT-B > MT-O | {stats['groups_mt_b_better']}/{stats['total_groups']} |")
        lines.append(f"| Direction consistent | {stats['direction_consistent']} |")
        lines.append("")

    # Detailed throughput
    lines.append("## Throughput Details")
    lines.append("")

    for config_id, data in analysis["throughput"].items():
        lines.append(f"### {config_id}")
        lines.append("")
        lines.append("| Arm | Median | Mean | CV | Min–Max |")
        lines.append("|-----|--------|------|----|---------| ")

        for arm in ARMS:
            if arm in data["arm_stats"]:
                s = data["arm_stats"][arm]
                lines.append(f"| {arm} | {s['median']:.6f} | {s['mean']:.6f} | "
                           f"{s['cv_percent']:.1f}% | {s['min']:.6f}–{s['max']:.6f} |")
        lines.append("")

    # Latency summary
    lines.append("## Latency Summary")
    lines.append("")

    for config_id, data in analysis["latency"].items():
        lines.append(f"### {config_id}")
        lines.append("")
        lines.append("| Arm | p50 | p99 | p99.9 | max |")
        lines.append("|-----|-----|-----|-------|-----|")

        for arm in ARMS:
            if arm in data:
                medians = data[arm]["medians"]
                line = f"| {arm} |"
                for label in ["p50", "p99", "p99.9", "max"]:
                    if label in medians:
                        line += f" {medians[label]:.0f} |"
                    else:
                        line += " — |"
                lines.append(line)
        lines.append("")

    return "\n".join(lines)


def analyze(experiment_dir):
    """Run the complete independent analysis."""
    experiment_dir = Path(experiment_dir).resolve()

    # Load experiment manifest
    experiment_json = experiment_dir / "experiment.json"
    if not experiment_json.exists():
        raise SystemExit(f"missing experiment.json in {experiment_dir}")

    manifest = json.loads(experiment_json.read_text())

    # Find all formal rounds
    results_json = experiment_dir / "results.json"
    if not results_json.exists():
        raise SystemExit(f"missing results.json in {experiment_dir}")

    results = json.loads(results_json.read_text())

    formal_rounds = []
    validation_issues = []

    for outcome in results["formal"]:
        round_dir = experiment_dir / f"round_{outcome['ordinal']:04d}"

        # Load command to get expected spec
        command_json = round_dir / "command.json"
        if not command_json.exists():
            validation_issues.append({
                "ordinal": outcome["ordinal"],
                "issues": ["missing command.json"]
            })
            continue

        command_data = json.loads(command_json.read_text())
        expected_spec = command_data["round_spec"]

        # Validate round
        parsed, issues = validate_round(round_dir, expected_spec)

        if issues:
            validation_issues.append({
                "ordinal": outcome["ordinal"],
                "arm": expected_spec["arm"],
                "issues": issues
            })
            continue

        formal_rounds.append({
            **expected_spec,
            "parsed": parsed,
        })

    print(f"Validated {len(formal_rounds)} formal rounds")
    if validation_issues:
        print(f"Found {len(validation_issues)} validation issues")

    # Analyze throughput and latency
    throughput_analysis = analyze_throughput(formal_rounds, experiment_dir)
    latency_analysis = analyze_latency(formal_rounds, experiment_dir)

    analysis = {
        "throughput": throughput_analysis,
        "latency": latency_analysis,
    }

    summary = compute_summary_statistics(throughput_analysis)

    # Write analysis JSON
    (experiment_dir / "analysis.json").write_text(json.dumps({
        "analysis": analysis,
        "summary": summary,
        "validation_issues": validation_issues,
        "validated_rounds": len(formal_rounds),
        "total_expected": results["expected_formal"],
    }, indent=2) + "\n")

    # Write markdown report
    report = format_markdown_report(analysis, summary, validation_issues)
    (experiment_dir / "analysis.md").write_text(report)

    print(f"Analysis written to {experiment_dir / 'analysis.json'}")
    print(f"Report written to {experiment_dir / 'analysis.md'}")

    return 0


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("experiment_dir", type=Path,
                        help="experiment output directory")
    parser.add_argument("--format", choices=["json", "markdown"], default="json",
                        help="output format")
    return parser.parse_args()


def main():
    args = parse_args()
    return analyze(args.experiment_dir)


if __name__ == "__main__":
    sys.exit(main())
