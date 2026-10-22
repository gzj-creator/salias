#!/usr/bin/env python3
"""Compare high-mode rates from two high-frequency stability experiments."""

import argparse
import hashlib
import json
import math
from pathlib import Path
import random
import statistics
from statistics import NormalDist


DEFAULT_THRESHOLD_MMSG_S = 15.0
DEFAULT_MIN_INCREASE_PP = 10.0
DEFAULT_ALPHA = 0.05
DEFAULT_POWER_TARGET = 0.80
CONDITIONS = ("messages_per_round", "payload_bytes", "capacity_bytes")


def digest(path):
    """Return a file SHA-256 for evidence identity checks."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def normal_cdf(value):
    """Return the standard normal CDF using the standard library."""
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def wilson_interval(successes, total, z=1.959963984540054):
    """Return a two-sided Wilson score interval for a binomial proportion."""
    if total <= 0:
        raise ValueError("Wilson interval requires a positive sample count")
    proportion = successes / total
    denominator = 1.0 + z * z / total
    centre = (proportion + z * z / (2.0 * total)) / denominator
    radius = (
        z
        * math.sqrt(
            proportion * (1.0 - proportion) / total
            + z * z / (4.0 * total * total)
        )
        / denominator
    )
    return max(0.0, centre - radius), min(1.0, centre + radius)


def newcombe_difference_interval(candidate, baseline):
    """Return Newcombe's hybrid score interval for candidate - baseline."""
    p1 = baseline["high_count"] / baseline["rounds"]
    p2 = candidate["high_count"] / candidate["rounds"]
    baseline_low, baseline_high = wilson_interval(
        baseline["high_count"], baseline["rounds"]
    )
    candidate_low, candidate_high = wilson_interval(
        candidate["high_count"], candidate["rounds"]
    )
    difference = p2 - p1
    lower = difference - math.sqrt(
        (p1 - baseline_low) ** 2 + (candidate_high - p2) ** 2
    )
    upper = difference + math.sqrt(
        (baseline_high - p1) ** 2 + (p2 - candidate_low) ** 2
    )
    return max(-1.0, lower), min(1.0, upper)


def two_proportion_test(baseline, candidate):
    """Run pooled two-proportion z tests for candidate > baseline."""
    n1 = baseline["rounds"]
    n2 = candidate["rounds"]
    x1 = baseline["high_count"]
    x2 = candidate["high_count"]
    p1 = x1 / n1
    p2 = x2 / n2
    observed_difference = p2 - p1
    pooled = (x1 + x2) / (n1 + n2)
    standard_error = math.sqrt(pooled * (1.0 - pooled) * (1.0 / n1 + 1.0 / n2))
    if standard_error == 0.0:
        z_score = 0.0 if observed_difference == 0.0 else math.copysign(math.inf, observed_difference)
        p_two_sided = 1.0 if observed_difference == 0.0 else 0.0
        p_one_sided = 1.0 if observed_difference <= 0.0 else 0.0
    else:
        z_score = observed_difference / standard_error
        p_two_sided = 2.0 * (1.0 - normal_cdf(abs(z_score)))
        p_one_sided = 1.0 - normal_cdf(z_score)
    return {
        "baseline_rate": p1,
        "candidate_rate": p2,
        "absolute_increase": observed_difference,
        "relative_increase": observed_difference / p1 if p1 else None,
        "risk_ratio": p2 / p1 if p1 else None,
        "pooled_rate": pooled,
        "standard_error": standard_error,
        "z_score": z_score,
        "p_value_one_sided": p_one_sided,
        "p_value_two_sided": p_two_sided,
    }


def log_choose(n, k):
    """Return log(n choose k) without constructing large integers."""
    return math.lgamma(n + 1) - math.lgamma(k + 1) - math.lgamma(n - k + 1)


def fisher_exact_greater(baseline, candidate):
    """Return the one-sided Fisher exact p-value for candidate > baseline."""
    n1 = baseline["rounds"]
    n2 = candidate["rounds"]
    x1 = baseline["high_count"]
    x2 = candidate["high_count"]
    successes = x1 + x2
    total = n1 + n2
    observed = x2
    minimum = max(0, successes - n1)
    maximum = min(n2, successes)
    denominator = log_choose(total, successes)
    if minimum > maximum:
        return 1.0

    # Start from the lower support endpoint and advance the hypergeometric PMF
    # recursively. This avoids thousands of lgamma calls on long experiments.
    def mass(value):
        return math.exp(
            log_choose(n2, value)
            + log_choose(n1, successes - value)
            - denominator
        )

    current = mass(minimum)
    probability = 0.0
    for value in range(minimum, maximum + 1):
        if value >= observed:
            probability += current
        if value < maximum:
            current *= (
                (n2 - value)
                * (successes - value)
                / ((value + 1) * (n1 - successes + value + 1))
            )
    return min(1.0, max(0.0, probability))


def cluster_permutation_test(baseline_groups, candidate_groups, trials=20000, seed=20261020):
    """Test the group-level rate difference while preserving cluster sizes."""
    baseline_rates = [group["high_rate"] for group in baseline_groups]
    candidate_rates = [group["high_rate"] for group in candidate_groups]
    observed = statistics.mean(candidate_rates) - statistics.mean(baseline_rates)
    pooled = baseline_rates + candidate_rates
    candidate_size = len(candidate_rates)
    generator = random.Random(seed)
    at_least_observed = 0
    for _ in range(trials):
        generator.shuffle(pooled)
        candidate_mean = statistics.mean(pooled[:candidate_size])
        baseline_mean = statistics.mean(pooled[candidate_size:])
        if candidate_mean - baseline_mean >= observed - 1e-15:
            at_least_observed += 1
    return {
        "observed_difference": observed,
        "p_value_one_sided": (at_least_observed + 1) / (trials + 1),
        "trials": trials,
        "seed": seed,
    }


def required_sample_size(
    baseline_rate, candidate_rate, alpha, power, two_sided=True
):
    """Estimate rounds per arm for an equal-size two-proportion z test."""
    if not 0.0 < baseline_rate < 1.0 or not 0.0 < candidate_rate < 1.0:
        return None
    difference = abs(candidate_rate - baseline_rate)
    if difference == 0.0:
        return None
    pooled = (baseline_rate + candidate_rate) / 2.0
    z_alpha = NormalDist().inv_cdf(
        1.0 - alpha / 2.0 if two_sided else 1.0 - alpha
    )
    z_power = NormalDist().inv_cdf(power)
    numerator = (
        z_alpha * math.sqrt(2.0 * pooled * (1.0 - pooled))
        + z_power
        * math.sqrt(
            baseline_rate * (1.0 - baseline_rate)
            + candidate_rate * (1.0 - candidate_rate)
        )
    )
    return math.ceil(numerator * numerator / (difference * difference))


def load_experiment(directory, threshold_override=None, allow_incomplete=False):
    """Load and validate one completed high-frequency experiment."""
    environment_path = directory / "environment.json"
    summary_path = directory / "summary.json"
    analysis_path = directory / "high-frequency-analysis.json"
    for path in (environment_path, summary_path):
        if not path.is_file():
            raise ValueError(f"Missing required evidence: {path}")
    environment = json.loads(environment_path.read_text())
    groups = json.loads(summary_path.read_text())
    if not groups:
        raise ValueError(f"No groups in {summary_path}")

    expected_groups = int(environment.get("groups", len(groups)))
    if expected_groups != len(groups) and not allow_incomplete:
        raise ValueError(
            f"Incomplete experiment {directory}: {len(groups)}/{expected_groups} groups"
        )
    expected_rounds = environment.get("rounds_per_group")
    high_threshold = environment.get(
        "high_mode_threshold_mmsg_s", DEFAULT_THRESHOLD_MMSG_S
    )
    if threshold_override is not None:
        high_threshold = threshold_override
    high_threshold = float(high_threshold)
    if not 0.0 < high_threshold <= 1000.0:
        raise ValueError(f"Invalid high-mode threshold in {environment_path}")

    group_rows = []
    for expected_index, group in enumerate(groups):
        if int(group.get("group", expected_index)) != expected_index:
            raise ValueError(f"Non-contiguous group indices in {summary_path}")
        rates = [float(rate) for rate in group.get("throughputs", [])]
        if not rates or any(not math.isfinite(rate) or rate < 0.0 for rate in rates):
            raise ValueError(f"Invalid throughput in group {expected_index}")
        if expected_rounds is not None and len(rates) != int(expected_rounds):
            raise ValueError(
                f"Group {expected_index} has {len(rates)}/{expected_rounds} rounds"
            )
        high_count = sum(rate >= high_threshold for rate in rates)
        stored_count = group.get("high_mode_count")
        if stored_count is not None and int(stored_count) != high_count:
            raise ValueError(
                f"Stored high-mode count disagrees with threshold in group {expected_index}"
            )
        high_rates = [rate for rate in rates if rate >= high_threshold]
        low_rates = [rate for rate in rates if rate < high_threshold]
        group_rows.append(
            {
                "group": expected_index,
                "rates": rates,
                "high_count": high_count,
                "high_rate": high_count / len(rates),
                "high_median": statistics.median(high_rates) if high_rates else None,
                "low_median": statistics.median(low_rates) if low_rates else None,
            }
        )

    rates = [rate for group in group_rows for rate in group["rates"]]
    high_rates = [rate for rate in rates if rate >= high_threshold]
    low_rates = [rate for rate in rates if rate < high_threshold]
    analysis_threshold = None
    if analysis_path.is_file():
        analysis = json.loads(analysis_path.read_text())
        analysis_threshold = analysis.get("threshold_mmsg_s")
        if analysis_threshold is not None and float(analysis_threshold) != high_threshold:
            raise ValueError(
                f"Analysis threshold disagrees with environment in {directory}"
            )
    return {
        "directory": str(directory.resolve()),
        "environment_sha256": digest(environment_path),
        "summary_sha256": digest(summary_path),
        "expected_groups": expected_groups,
        "complete": expected_groups == len(groups),
        "client_sha256": environment.get("client_sha256"),
        "client": environment.get("client"),
        "groups": group_rows,
        "rounds": len(rates),
        "high_count": len(high_rates),
        "high_rate": len(high_rates) / len(rates),
        "high_median": statistics.median(high_rates) if high_rates else None,
        "low_median": statistics.median(low_rates) if low_rates else None,
        "all_median": statistics.median(rates),
        "threshold": high_threshold,
        "messages_per_round": environment.get("messages_per_round"),
        "payload_bytes": environment.get("payload_bytes"),
        "capacity_bytes": environment.get("capacity_bytes"),
    }


def compare(
    baseline,
    candidate,
    min_increase_pp,
    alpha,
    min_high_median,
    allow_missing_conditions=False,
):
    """Compare two experiments and apply the relative-improvement gates."""
    if baseline["threshold"] != candidate["threshold"]:
        raise ValueError(
            "High-mode thresholds differ: "
            f"{baseline['threshold']} vs {candidate['threshold']}"
        )
    mismatched = {
        key: (baseline[key], candidate[key])
        for key in CONDITIONS
        if baseline[key] is None
        or candidate[key] is None
        or baseline[key] != candidate[key]
    }
    missing = {
        key: values for key, values in mismatched.items() if None in values
    }
    actual_mismatches = {
        key: values for key, values in mismatched.items() if None not in values
    }
    if actual_mismatches or (missing and not allow_missing_conditions):
        raise ValueError(f"Experiment conditions differ: {mismatched}")
    test = two_proportion_test(baseline, candidate)
    fishers_p = fisher_exact_greater(baseline, candidate)
    difference_low, difference_high = newcombe_difference_interval(candidate, baseline)
    clustered = cluster_permutation_test(baseline["groups"], candidate["groups"])
    increase_pp = test["absolute_increase"] * 100.0
    threshold_met = increase_pp + 1e-12 >= min_increase_pp
    significant = test["p_value_two_sided"] < alpha
    high_median_ok = (
        candidate["high_median"] is not None
        and candidate["high_median"] >= min_high_median
    )
    target_rate = test["baseline_rate"] + min_increase_pp / 100.0
    required_n = required_sample_size(
        test["baseline_rate"], target_rate, alpha, DEFAULT_POWER_TARGET,
        two_sided=False,
    )
    return {
        "baseline": {
            key: baseline[key]
            for key in (
                "directory",
                "environment_sha256",
                "summary_sha256",
                "client_sha256",
                "expected_groups",
                "complete",
                "groups",
                "rounds",
                "high_count",
                "high_rate",
                "high_median",
                "low_median",
                "all_median",
                "threshold",
                "messages_per_round",
                "payload_bytes",
                "capacity_bytes",
            )
        },
        "candidate": {
            key: candidate[key]
            for key in (
                "directory",
                "environment_sha256",
                "summary_sha256",
                "client_sha256",
                "expected_groups",
                "complete",
                "groups",
                "rounds",
                "high_count",
                "high_rate",
                "high_median",
                "low_median",
                "all_median",
                "threshold",
                "messages_per_round",
                "payload_bytes",
                "capacity_bytes",
            )
        },
        "test": {
            **test,
            "difference_95ci": [difference_low, difference_high],
            "fisher_exact_one_sided_p": fishers_p,
            "cluster_permutation": clustered,
        },
        "acceptance": {
            "alpha": alpha,
            "min_increase_pp": min_increase_pp,
            "min_high_mode_median_mmsg_s": min_high_median,
            "increase_pp": increase_pp,
            "increase_threshold_met": threshold_met,
            "z_test_significant": significant,
            "candidate_high_median_ok": high_median_ok,
            "passed": (
                threshold_met
                and significant
                and test["absolute_increase"] > 0.0
                and high_median_ok
                and baseline["complete"]
                and candidate["complete"]
                and not missing
            ),
            "evidence_complete": (
                baseline["complete"] and candidate["complete"] and not missing
            ),
            "direction_positive": test["absolute_increase"] > 0.0,
            "missing_conditions": sorted(missing),
            "target_candidate_rate": target_rate,
            "estimated_rounds_per_arm_80pct_power_one_sided": required_n,
            "estimated_rounds_per_arm_80pct_power_two_sided": required_sample_size(
                test["baseline_rate"], target_rate, alpha, DEFAULT_POWER_TARGET
            ),
            "observed_rounds_per_arm": [baseline["rounds"], candidate["rounds"]],
        },
    }


def format_rate(rate):
    return "n/a" if rate is None else f"{rate * 100.0:.1f}%"


def format_throughput(value):
    return "n/a" if value is None else f"{value:.3f}"


def print_result(result):
    baseline = result["baseline"]
    candidate = result["candidate"]
    test = result["test"]
    acceptance = result["acceptance"]
    print("=== High-Mode Relative Comparison ===\n")
    if not result["acceptance"]["evidence_complete"]:
        print("INCOMPLETE: evidence is diagnostic only and cannot pass.\n")
    print(
        f"Baseline:  {baseline['high_count']}/{baseline['rounds']} "
        f"({format_rate(baseline['high_rate'])})"
    )
    print(
        f"Candidate: {candidate['high_count']}/{candidate['rounds']} "
        f"({format_rate(candidate['high_rate'])})"
    )
    print(f"Increase:  {acceptance['increase_pp']:+.1f} pp")
    print(
        "95% CI:    "
        f"{test['difference_95ci'][0] * 100.0:+.1f}.."
        f"{test['difference_95ci'][1] * 100.0:+.1f} pp"
    )
    print(f"Pooled z:  p={test['p_value_two_sided']:.6g} (two-sided)")
    print(f"Fisher:    p={test['fisher_exact_one_sided_p']:.6g} (one-sided)")
    print(
        "Cluster:   "
        f"p={test['cluster_permutation']['p_value_one_sided']:.6g} (one-sided)"
    )
    print()
    print(f"High median: baseline {format_throughput(baseline['high_median'])} Mmsg/s; "
          f"candidate {format_throughput(candidate['high_median'])} Mmsg/s")
    print(f"Low median:  baseline {format_throughput(baseline['low_median'])} Mmsg/s; "
          f"candidate {format_throughput(candidate['low_median'])} Mmsg/s")
    print()
    print(
        "Acceptance: "
        f"{'PASS' if acceptance['passed'] else 'FAIL'} "
        f"(increase={acceptance['increase_threshold_met']}, "
        f"z={acceptance['z_test_significant']}, "
        f"positive={acceptance['direction_positive']}, "
        f"high_median={acceptance['candidate_high_median_ok']})"
    )
    print(
        "Power note: "
        f"estimated {acceptance['estimated_rounds_per_arm_80pct_power_one_sided']} rounds/arm "
        f"needed for {acceptance['min_increase_pp']:.0f} pp at one-sided 80% power "
        f"({acceptance['estimated_rounds_per_arm_80pct_power_two_sided']} two-sided); "
        f"observed {acceptance['observed_rounds_per_arm'][0]}/"
        f"{acceptance['observed_rounds_per_arm'][1]}"
    )


def print_markdown(result):
    """Print a compact table suitable for pasting into a benchmark report."""
    baseline = result["baseline"]
    candidate = result["candidate"]
    test = result["test"]
    acceptance = result["acceptance"]
    print("| Metric | Baseline | Candidate |")
    print("|---|---:|---:|")
    print(f"| High mode | {baseline['high_count']}/{baseline['rounds']} "
          f"({format_rate(baseline['high_rate'])}) | "
          f"{candidate['high_count']}/{candidate['rounds']} "
          f"({format_rate(candidate['high_rate'])}) |")
    print(f"| High median (Mmsg/s) | {format_throughput(baseline['high_median'])} | "
          f"{format_throughput(candidate['high_median'])} |")
    print(f"| Low median (Mmsg/s) | {format_throughput(baseline['low_median'])} | "
          f"{format_throughput(candidate['low_median'])} |")
    print(f"| All median (Mmsg/s) | {format_throughput(baseline['all_median'])} | "
          f"{format_throughput(candidate['all_median'])} |")
    print()
    print(f"Increase: {acceptance['increase_pp']:+.1f} pp; "
          f"95% CI {test['difference_95ci'][0] * 100.0:+.1f}.."
          f"{test['difference_95ci'][1] * 100.0:+.1f} pp; "
          f"z p={test['p_value_two_sided']:.6g}; "
          f"Fisher p={test['fisher_exact_one_sided_p']:.6g}; "
          f"cluster p={test['cluster_permutation']['p_value_one_sided']:.6g}.  ")
    print(f"Acceptance: {'PASS' if acceptance['passed'] else 'FAIL'}.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--threshold", type=float, default=None,
                        help="override the recorded high-mode threshold")
    parser.add_argument("--allow-incomplete", action="store_true",
                        help="compare partial experiments for diagnostics only")
    parser.add_argument("--min-increase-pp", type=float, default=DEFAULT_MIN_INCREASE_PP)
    parser.add_argument("--alpha", type=float, default=DEFAULT_ALPHA)
    parser.add_argument("--min-high-median", type=float, default=15.0)
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--markdown", action="store_true",
                        help="print a compact report table instead of the full summary")
    parser.add_argument("--require-complete", action="store_true",
                        help="exit nonzero unless every acceptance gate passes")
    args = parser.parse_args()
    if args.min_increase_pp < 0.0:
        parser.error("--min-increase-pp must be non-negative")
    if not 0.0 < args.alpha < 1.0:
        parser.error("--alpha must be in (0, 1)")
    if args.min_high_median < 0.0:
        parser.error("--min-high-median must be non-negative")

    baseline = load_experiment(
        args.baseline, args.threshold, allow_incomplete=args.allow_incomplete
    )
    candidate = load_experiment(
        args.candidate, args.threshold, allow_incomplete=args.allow_incomplete
    )
    result = compare(
        baseline,
        candidate,
        args.min_increase_pp,
        args.alpha,
        args.min_high_median,
        allow_missing_conditions=args.allow_incomplete,
    )
    if args.markdown:
        print_markdown(result)
    else:
        print_result(result)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2) + "\n")
        print(f"\nwrote {args.output}")
    if args.require_complete and not result["acceptance"]["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
