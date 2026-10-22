#!/usr/bin/env python3
"""Analyze the pre-registered E1 zero-layout throughput comparison.

Both arms must contain six complete groups of 20 rounds produced by the
high-frequency runner. The tool verifies SHA256SUMS and every RESULT line before
computing medians, an exact group-level permutation test, ABBA directions and a
cluster-bootstrap confidence interval.
"""

import argparse
import hashlib
import json
import math
from pathlib import Path
import random
import re
import statistics


GROUPS = 6
ROUNDS = 20
MESSAGES = 50_000_000
GO_RATIO = 1.25
NO_GO_RATIO = 1.10
GO_P_VALUE = 0.01
NO_GO_P_VALUE = 0.05
GO_DIRECTIONS = 5
HIGH_THROUGHPUT_MMSG_S = 10.0
BOOTSTRAP_SAMPLES = 20_000
BOOTSTRAP_SEED = 20261021
RESULT_PATTERN = re.compile(r"^RESULT\s+(.*)$")


class AnalysisError(RuntimeError):
    """Raised when an arm is incomplete or its integrity cannot be proven."""


def digest(path):
    """Return the SHA-256 digest of one file."""
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def parse_result(line):
    """Parse a RESULT key=value line with an explicit structural check."""
    match = RESULT_PATTERN.match(line)
    if not match:
        raise AnalysisError(f"not a RESULT line: {line}")
    fields = {}
    for item in match.group(1).split():
        if "=" not in item:
            raise AnalysisError(f"malformed RESULT field: {item}")
        key, value = item.split("=", 1)
        if key in fields:
            raise AnalysisError(f"duplicate RESULT field: {key}")
        fields[key] = value
    return fields


def verify_checksums(directory):
    """Verify every file named by SHA256SUMS and require the manifest itself."""
    path = directory / "SHA256SUMS"
    if not path.is_file():
        raise AnalysisError(f"missing SHA256SUMS: {directory}")
    verified = 0
    for line in path.read_text().splitlines():
        if not line.strip():
            continue
        if len(line) < 67 or line[64:66] != "  ":
            raise AnalysisError(f"malformed SHA256SUMS line: {line}")
        expected, relative = line[:64], line[66:]
        target = directory / relative
        if not target.is_file():
            raise AnalysisError(f"SHA256SUMS names a missing file: {relative}")
        actual = digest(target)
        if actual != expected:
            raise AnalysisError(f"SHA256 mismatch for {relative}")
        verified += 1
    if not verified:
        raise AnalysisError(f"empty SHA256SUMS: {directory}")
    return verified


def read_environment(directory):
    """Load runner environment metadata and enforce the E1 matrix invariants."""
    path = directory / "environment.json"
    if not path.is_file():
        raise AnalysisError(f"missing environment.json: {directory}")
    environment = json.loads(path.read_text())
    expected = {
        "messages_per_round": MESSAGES,
        "rate_per_producer": 0,
        "latency_sample_rate": 0,
    }
    mismatched = {
        key: (environment.get(key), value)
        for key, value in expected.items()
        if environment.get(key) != value
    }
    if mismatched:
        raise AnalysisError(f"environment differs from E1: {mismatched}")
    return environment


def verify_round(record, stdout_path, payload):
    """Cross-check one JSON record against its raw RESULT line."""
    if record.get("payload") is None and "command" in record:
        command = record["command"]
        try:
            payload_arg = command[command.index("--payload") + 1]
        except (ValueError, IndexError) as error:
            raise AnalysisError(f"round command has no payload: {record}") from error
        if int(payload_arg) != payload:
            raise AnalysisError(f"round payload is {payload_arg}, expected {payload}")
    if not stdout_path.is_file():
        raise AnalysisError(f"missing round stdout: {stdout_path}")
    lines = [line for line in stdout_path.read_text().splitlines()
             if line.startswith("RESULT ")]
    if len(lines) != 1:
        raise AnalysisError(f"expected one RESULT line in {stdout_path}")
    fields = parse_result(lines[0])
    expected = {
        "library": "salias-ipc",
        "scenario": "fifo",
        "producers": "1",
        "consumers": "2",
        "payload": str(payload),
        "capacity": str(1 << 20),
        "batch_size": "1",
        "poll_limit": "64",
        "published": str(MESSAGES),
        "delivered": str(2 * MESSAGES),
    }
    mismatched = {
        key: (fields.get(key), value)
        for key, value in expected.items()
        if fields.get(key) != value
    }
    if mismatched:
        raise AnalysisError(f"RESULT mismatch in {stdout_path}: {mismatched}")
    rate = float(fields["publish_msg_per_sec"]) / 1e6
    recorded = float(record["publish_mmsg_s"])
    if not math.isclose(rate, recorded, rel_tol=0, abs_tol=1e-6):
        raise AnalysisError(
            f"RESULT/JSON throughput mismatch in {stdout_path}: {rate} vs {recorded}"
        )
    return rate


def load_arm(directory, expected_payload=None):
    """Load six complete groups after proving file and result integrity."""
    directory = Path(directory).resolve()
    environment = read_environment(directory)
    payload = int(environment["payload_bytes"])
    if expected_payload is not None and payload != expected_payload:
        raise AnalysisError(f"payload is {payload}, expected {expected_payload}")
    if int(environment.get("groups", -1)) != GROUPS:
        raise AnalysisError(f"groups is {environment.get('groups')}, expected {GROUPS}")
    if int(environment.get("rounds_per_group", -1)) != ROUNDS:
        raise AnalysisError(
            f"rounds_per_group is {environment.get('rounds_per_group')}, expected {ROUNDS}"
        )
    if int(environment.get("capacity_bytes", -1)) != 1 << 20:
        raise AnalysisError("capacity is not 1 MiB")
    verified_files = verify_checksums(directory)
    groups = []
    all_rates = []
    for group_idx in range(GROUPS):
        group_dir = directory / f"group-{group_idx:02d}"
        results = group_dir / "results.jsonl"
        if not results.is_file():
            raise AnalysisError(f"missing group results: {results}")
        records = [json.loads(line) for line in results.read_text().splitlines()
                   if line.strip()]
        if len(records) != ROUNDS:
            raise AnalysisError(
                f"group {group_idx} has {len(records)} rounds, expected {ROUNDS}"
            )
        if [record.get("round") for record in records] != list(range(ROUNDS)):
            raise AnalysisError(f"group {group_idx} round order is invalid")
        rates = []
        for record in records:
            if record.get("group") != group_idx:
                raise AnalysisError(f"group index mismatch in {results}")
            stdout_path = group_dir / str(record["stdout"])
            rates.append(verify_round(record, stdout_path, payload))
        median = statistics.median(rates)
        groups.append({
            "group": group_idx,
            "rates": rates,
            "median": median,
            "mean": statistics.mean(rates),
            "cv": statistics.pstdev(rates) / statistics.mean(rates),
            "high_count": sum(rate >= HIGH_THROUGHPUT_MMSG_S for rate in rates),
        })
        all_rates.extend(rates)
    return {
        "directory": str(directory),
        "client_sha256": environment.get("client_sha256"),
        "payload": payload,
        "verified_files": verified_files,
        "groups": groups,
        "rates": all_rates,
        "median": statistics.median(all_rates),
        "mean": statistics.mean(all_rates),
        "cv": statistics.pstdev(all_rates) / statistics.mean(all_rates),
        "high_count": sum(rate >= HIGH_THROUGHPUT_MMSG_S for rate in all_rates),
    }


def permutation_test(baseline_groups, candidate_groups):
    """Run the exact two-sided 6-vs-6 permutation test on group medians."""
    values = list(baseline_groups) + list(candidate_groups)
    if len(values) != 2 * GROUPS:
        raise AnalysisError("permutation test requires 6 + 6 groups")
    observed = statistics.mean(candidate_groups) - statistics.mean(baseline_groups)
    tolerance = 1e-12
    total = 0
    extreme = 0
    n = len(values)
    for mask in range(1 << n):
        if mask.bit_count() != GROUPS:
            continue
        candidate = [values[index] for index in range(n) if mask & (1 << index)]
        baseline = [values[index] for index in range(n) if not mask & (1 << index)]
        difference = statistics.mean(candidate) - statistics.mean(baseline)
        total += 1
        if abs(difference) + tolerance >= abs(observed):
            extreme += 1
    return {
        "observed_difference_mmsg_s": observed,
        "extreme_partitions": extreme,
        "partitions": total,
        "p_value": extreme / total,
    }


def bootstrap_interval(baseline_groups, candidate_groups,
                       samples=BOOTSTRAP_SAMPLES, seed=BOOTSTRAP_SEED):
    """Cluster-bootstrap the median difference by resampling whole groups."""
    rng = random.Random(seed)
    differences = []
    for _ in range(samples):
        baseline = [
            rate
            for group in (rng.choice(baseline_groups) for _ in range(GROUPS))
            for rate in group
        ]
        candidate = [
            rate
            for group in (rng.choice(candidate_groups) for _ in range(GROUPS))
            for rate in group
        ]
        differences.append(statistics.median(candidate) - statistics.median(baseline))
    differences.sort()
    low_index = max(0, math.floor(0.025 * (len(differences) - 1)))
    high_index = min(len(differences) - 1, math.ceil(0.975 * (len(differences) - 1)))
    return {
        "samples": samples,
        "seed": seed,
        "median_difference_mmsg_s": statistics.median(differences),
        "ci95_low_mmsg_s": differences[low_index],
        "ci95_high_mmsg_s": differences[high_index],
    }


def decide(ratio, p_value, directions):
    """Apply the fixed section 4.4 decision rule without post-hoc changes."""
    if ratio >= GO_RATIO and p_value < GO_P_VALUE and directions >= GO_DIRECTIONS:
        return "GO"
    if ratio < NO_GO_RATIO or p_value >= NO_GO_P_VALUE:
        return "NO-GO"
    return "INCONCLUSIVE"


def compare(baseline_dir, candidate_dir, expected_payload=None):
    """Build the complete E1 comparison record for two arm directories."""
    baseline = load_arm(baseline_dir, expected_payload)
    candidate = load_arm(candidate_dir, expected_payload)
    if baseline["payload"] != candidate["payload"]:
        raise AnalysisError("arms use different payloads")
    if baseline["client_sha256"] == candidate["client_sha256"]:
        raise AnalysisError("baseline and candidate client hashes are identical")
    baseline_group_medians = [group["median"] for group in baseline["groups"]]
    candidate_group_medians = [group["median"] for group in candidate["groups"]]
    ratio = candidate["median"] / baseline["median"]
    permutation = permutation_test(baseline_group_medians, candidate_group_medians)
    directions = sum(
        candidate_median > baseline_median
        for baseline_median, candidate_median in zip(
            baseline_group_medians, candidate_group_medians
        )
    )
    decision = decide(ratio, permutation["p_value"], directions)
    return {
        "protocol": "E1 zero-layout, FIFO 1P2S, 1 MiB, batch 1, poll-limit 64",
        "payload_bytes": baseline["payload"],
        "groups": GROUPS,
        "rounds_per_group": ROUNDS,
        "baseline": {
            key: baseline[key]
            for key in ("directory", "client_sha256", "verified_files", "median",
                        "mean", "cv", "high_count")
        },
        "candidate": {
            key: candidate[key]
            for key in ("directory", "client_sha256", "verified_files", "median",
                        "mean", "cv", "high_count")
        },
        "group_medians_mmsg_s": {
            "baseline": baseline_group_medians,
            "candidate": candidate_group_medians,
        },
        "median_ratio_c_over_b": ratio,
        "median_difference_mmsg_s": candidate["median"] - baseline["median"],
        "permutation": permutation,
        "abba_directions_c_above_b": directions,
        "bootstrap": bootstrap_interval(
            [group["rates"] for group in baseline["groups"]],
            [group["rates"] for group in candidate["groups"]],
        ),
        "decision_eligible": baseline["payload"] == 64,
        "decision": decision if baseline["payload"] == 64 else "DESCRIPTIVE",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--payload", type=int, choices=(64, 192), default=None)
    args = parser.parse_args()
    try:
        result = compare(args.baseline, args.candidate, args.payload)
    except AnalysisError as error:
        raise SystemExit(f"analysis failed: {error}")
    result["decision_rule"] = {
        "go": f"ratio >= {GO_RATIO}, p < {GO_P_VALUE}, directions >= {GO_DIRECTIONS}",
        "no_go": f"ratio < {NO_GO_RATIO} or p >= {NO_GO_P_VALUE}",
        "otherwise": "INCONCLUSIVE",
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    baseline = result["baseline"]
    candidate = result["candidate"]
    print(f"Baseline:  median={baseline['median']:.6f} mean={baseline['mean']:.6f} "
          f"CV={baseline['cv']:.4%} >=10={baseline['high_count']}/120")
    print(f"Candidate: median={candidate['median']:.6f} mean={candidate['mean']:.6f} "
          f"CV={candidate['cv']:.4%} >=10={candidate['high_count']}/120")
    print(f"Median ratio C/B: {result['median_ratio_c_over_b']:.6f}")
    print(f"Exact permutation p: {result['permutation']['p_value']:.6f} "
          f"({result['permutation']['extreme_partitions']}/"
          f"{result['permutation']['partitions']})")
    print(f"ABBA directions C>B: {result['abba_directions_c_above_b']}/{GROUPS}")
    print(f"Bootstrap 95% CI: [{result['bootstrap']['ci95_low_mmsg_s']:.6f}, "
          f"{result['bootstrap']['ci95_high_mmsg_s']:.6f}] Mmsg/s")
    if result["decision_eligible"]:
        print(f"DECISION {result['decision']}")
    else:
        print("DECISION DESCRIPTIVE (192 B is not part of the E1 gate)")


if __name__ == "__main__":
    main()
