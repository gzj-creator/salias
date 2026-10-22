#!/usr/bin/env python3
"""Analyze the claim API v4 three-arm product A/B without pooling rounds."""

import argparse
import hashlib
import json
import math
from pathlib import Path
import random
import statistics


ROOT = Path(__file__).resolve().parents[2]
ARMS = ("B", "S", "A")
ARM_NAMES = {
    "B": "v3",
    "S": "v4-sequential-fence",
    "A": "v4-default-fence",
}
ARM_DIRECTORIES = {
    "B": "baseline",
    "S": "candidate-sequential-fence",
    "A": "candidate-default-fence",
}
PAYLOADS = (64, 256)
GROUPS = 6
ROUNDS = 20
MESSAGES = 50_000_000
LATENCY_ROUNDS = 10
LATENCY_ORIGINS = ("attempt", "planned")
METRICS = ("p50", "p99", "p999")
GO_RATIO = 1.25
GO_P_VALUE = 0.01
GO_DIRECTIONS = 5
BOOTSTRAP_SAMPLES = 20_000
BOOTSTRAP_SEED = 20261022
COMPARISONS = {
    "claim_api_S_vs_B": ("S", "B"),
    "fence_A_vs_S": ("A", "S"),
    "combined_A_vs_B": ("A", "B"),
}


class AnalysisError(RuntimeError):
    """Raised when evidence is incomplete, inconsistent or has the wrong identity."""


def digest(path):
    """Return a file SHA-256 digest."""
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def load_experiment(path):
    """Read the experiment manifest and enforce the formal matrix shape."""
    path = Path(path)
    manifest_path = path / "experiment.json" if path.is_dir() else path
    data = json.loads(manifest_path.read_text())
    if data.get("protocol") != "p3-claim-api-v4-formal-ab-v1":
        raise AnalysisError("experiment is not the claim API v4 formal protocol")
    if data.get("mode") != "formal":
        raise AnalysisError("smoke results are not acceptance evidence")
    throughput = data.get("throughput", {})
    latency = data.get("latency", {})
    expected = {
        "groups": GROUPS,
        "rounds_per_group": ROUNDS,
        "messages_per_round": MESSAGES,
        "capacity_bytes": 1 << 20,
    }
    mismatched = {
        key: (throughput.get(key), value)
        for key, value in expected.items()
        if throughput.get(key) != value
    }
    if mismatched:
        raise AnalysisError(f"throughput matrix differs from the frozen protocol: {mismatched}")
    if sorted(throughput.get("payloads", [])) != list(PAYLOADS):
        raise AnalysisError("throughput payloads must be exactly 64 B and 256 B")
    expected_sequence = [
        [f"{arm}{group}" for arm in (ARMS if group % 2 == 0 else tuple(reversed(ARMS)))]
        for group in range(GROUPS)
    ]
    if throughput.get("sequence") != expected_sequence:
        raise AnalysisError("throughput arm order is not the pre-registered rotation")
    if (latency.get("payloads") != list(PAYLOADS) or
            latency.get("origins") != list(LATENCY_ORIGINS) or
            latency.get("rounds") != LATENCY_ROUNDS or
            latency.get("rate_per_producer") != 1_000_000):
        raise AnalysisError("latency matrix differs from the frozen protocol")
    clients = data.get("clients", {})
    if set(clients) != set(ARMS):
        raise AnalysisError("experiment must contain B, S and A clients")
    for arm in ARMS:
        client = clients[arm]
        expected_environment = (["SALIAS_DISABLE_ASYMMETRIC_FENCE=1"]
                                if arm == "S" else [])
        if client.get("environment") != expected_environment:
            raise AnalysisError(f"arm {arm} has unexpected environment")
        if not isinstance(client.get("path"), str) or not client["path"]:
            raise AnalysisError(f"arm {arm} has no client path")
    if clients["S"]["path"] != clients["A"]["path"]:
        raise AnalysisError("sequential and default fence arms must use the same v4 client")
    if Path(clients["B"]["path"]).resolve() == Path(clients["S"]["path"]).resolve():
        raise AnalysisError("baseline and candidate executable paths are identical")
    verify_blocks(data)
    return data


def verify_blocks(data):
    """Require the complete 36-throughput/4-latency block ledger to have succeeded."""
    blocks = data.get("blocks")
    if not isinstance(blocks, list) or not data.get("finished"):
        raise AnalysisError("experiment is not finished or has no block ledger")
    expected_throughput = {
        ("throughput", payload, arm, group)
        for payload in PAYLOADS for arm in ARMS for group in range(GROUPS)
    }
    expected_latency = {
        ("latency", payload, origin, None)
        for payload in PAYLOADS for origin in LATENCY_ORIGINS
    }
    observed_throughput = set()
    observed_latency = set()
    for block in blocks:
        if block.get("returncode") != 0:
            raise AnalysisError(f"experiment block failed: {block.get('command')}")
        if block.get("kind") == "throughput":
            observed_throughput.add((block.get("kind"), block.get("payload"),
                                     block.get("arm"), block.get("group")))
        elif block.get("kind") == "latency":
            observed_latency.add((block.get("kind"), block.get("payload"),
                                  block.get("origin"), None))
        else:
            raise AnalysisError(f"unknown experiment block kind: {block.get('kind')!r}")
    if observed_throughput != expected_throughput or len(observed_throughput) != len(blocks) - len(expected_latency):
        raise AnalysisError("throughput block ledger is incomplete or duplicated")
    if observed_latency != expected_latency:
        raise AnalysisError("latency block ledger is incomplete or duplicated")


def parse_round_line(line):
    """Parse one benchmark RESULT line."""
    if not line.startswith("RESULT "):
        raise AnalysisError(f"not a RESULT line: {line}")
    fields = {}
    for item in line.split()[1:]:
        key, separator, value = item.partition("=")
        if not separator or key in fields:
            raise AnalysisError(f"malformed RESULT field: {item}")
        fields[key] = value
    return fields


def verify_round(record, stdout_path, payload):
    """Cross-check one throughput JSON record against its raw stdout."""
    stdout_path = Path(stdout_path)
    lines = [line for line in stdout_path.read_text().splitlines()
             if line.startswith("RESULT ")]
    if len(lines) != 1:
        raise AnalysisError(f"expected one RESULT line in {stdout_path}")
    fields = parse_round_line(lines[0])
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
        "latency_sample_rate": "0",
        "rate_per_producer": "0",
    }
    mismatched = {
        key: (fields.get(key), value)
        for key, value in expected.items()
        if fields.get(key) != value
    }
    if mismatched:
        raise AnalysisError(f"RESULT mismatch in {stdout_path}: {mismatched}")
    rate = float(fields["publish_msg_per_sec"]) / 1e6
    if not math.isclose(rate, float(record["publish_mmsg_s"]), abs_tol=1e-6):
        raise AnalysisError(f"RESULT/JSON throughput mismatch in {stdout_path}")
    return rate


def verify_arm_environment(directory, arm):
    """Prove that a throughput arm was run with the declared claim/fence shape."""
    environment = json.loads((Path(directory) / "environment.json").read_text())
    expected = ["SALIAS_DISABLE_ASYMMETRIC_FENCE=1"] if arm == "S" else []
    if environment.get("environment_variables") != expected:
        raise AnalysisError(f"arm {arm} environment does not match its declared shape")
    expected_values = {
        "capacity_bytes": 1 << 20,
        "groups": GROUPS,
        "rounds_per_group": ROUNDS,
        "messages_per_round": MESSAGES,
        "rate_per_producer": 0,
        "latency_sample_rate": 0,
    }
    for key, value in expected_values.items():
        if environment.get(key) != value:
            raise AnalysisError(f"arm {arm} environment {key} is incorrect")
    return environment


def load_throughput_arm(directory, arm, payload):
    """Load one complete arm and return group medians and raw rates."""
    directory = Path(directory)
    environment = verify_arm_environment(directory, arm)
    if environment.get("payload_bytes") != payload:
        raise AnalysisError(f"arm {arm} payload is not {payload} B")
    checksum_path = directory / "SHA256SUMS"
    if not checksum_path.is_file():
        raise AnalysisError(f"missing SHA256SUMS in {directory}")
    for line in checksum_path.read_text().splitlines():
        if not line.strip():
            continue
        if len(line) < 67 or line[64:66] != "  ":
            raise AnalysisError(f"malformed SHA256SUMS in {directory}")
        expected, relative = line[:64], line[66:]
        target = directory / relative
        if not target.is_file() or digest(target) != expected:
            raise AnalysisError(f"SHA256SUMS verification failed for {target}")
    groups = []
    for group in range(GROUPS):
        results = directory / f"group-{group:02d}" / "results.jsonl"
        records = [json.loads(line) for line in results.read_text().splitlines()
                   if line.strip()]
        if len(records) != ROUNDS:
            raise AnalysisError(f"{directory} group {group} is incomplete")
        if [record.get("round") for record in records] != list(range(ROUNDS)):
            raise AnalysisError(f"{directory} group {group} round order is invalid")
        rates = []
        for record in records:
            if record.get("group") != group or record.get("arm"):
                raise AnalysisError(f"{directory} group {group} metadata is invalid")
            if record.get("payload") not in (None, payload):
                raise AnalysisError(f"{directory} group {group} payload is invalid")
            events = dict(item.split() for item in
                          record.get("cgroup_after", {}).get("memory_events", "").splitlines())
            if any(int(events.get(name, 0)) for name in ("high", "max", "oom", "oom_kill")):
                raise AnalysisError(f"memory pressure invalidates {directory} group {group}")
            rates.append(verify_round(record, directory / f"group-{group:02d}"
                                      / record["stdout"], payload))
        groups.append({
            "group": group,
            "median": statistics.median(rates),
            "mean": statistics.mean(rates),
            "rate": rates,
        })
    all_rates = [rate for group in groups for rate in group["rate"]]
    return {
        "client_sha256": environment.get("client_sha256"),
        "groups": groups,
        "median": statistics.median(all_rates),
        "mean": statistics.mean(all_rates),
        "rates": all_rates,
    }


def load_latency_arm(records, path, arm):
    """Load one arm from a shared case file, never pooling histograms."""
    path = Path(path)
    records = [record for record in records if record.get("arm") == arm]
    if not records:
        raise AnalysisError(f"latency arm {arm} is empty: {path}")
    measured = [record for record in records if record.get("phase") == "measured"]
    client_hashes = {record.get("client_sha256") for record in records}
    if len(client_hashes) != 1 or None in client_hashes:
        raise AnalysisError(f"latency arm {arm} mixes client identities: {path}")
    for record in records:
        events = dict(item.split() for item in
                      record["resources"]["memory.events"].splitlines())
        if any(int(events.get(name, 0)) for name in ("high", "max", "oom", "oom_kill")):
            raise AnalysisError(f"memory pressure invalidates latency arm {arm}: {path}")
        result = record["result"]
        expected = int(record["rate"]) * int(record["duration_s"])
        producers = int(record["producers"])
        expected_samples = producers * ((expected // producers + 15) // 16)
        if (int(result.get("published", -1)) != expected or
                int(result.get("delivered", -1)) != expected or
                int(result.get("latency_samples", -1)) != expected_samples):
            raise AnalysisError(f"latency counts are invalid in arm {arm}: {path}")
        stdout = path.parent / record["stdout"]
        if not stdout.is_file() or digest(stdout) != record.get("stdout_sha256"):
            raise AnalysisError(f"latency stdout identity is invalid in arm {arm}: {stdout}")
        lines = [line for line in stdout.read_text().splitlines()
                 if line.startswith("RESULT ")]
        if len(lines) != 1:
            raise AnalysisError(f"expected one RESULT line in {stdout}")
        fields = parse_round_line(lines[0])
        for key, value in result.items():
            if fields.get(key) != value:
                raise AnalysisError(f"latency RESULT mismatch in {stdout}: {key}")
    result = {}
    for origin in LATENCY_ORIGINS:
        source = sorted((record for record in measured if record["origin"] == origin),
                        key=lambda record: record["round"])
        if not source:
            continue
        if [record["round"] for record in source] != list(range(1, LATENCY_ROUNDS + 1)):
            raise AnalysisError(f"latency {origin} arm {arm} is incomplete")
        result[origin] = {
            metric: statistics.median(int(record["result"][f"latency_{metric}_ns"])
                                      for record in source)
            for metric in METRICS
        }
        result[origin]["range_ns"] = {
            metric: [
                min(int(record["result"][f"latency_{metric}_ns"]) for record in source),
                max(int(record["result"][f"latency_{metric}_ns"]) for record in source),
            ]
            for metric in METRICS
        }
    return result


def load_latency_case(directory):
    """Verify all three latency arms and return their separated medians."""
    directory = Path(directory)
    expected_origin = directory.name.split("-", 1)[1]
    payload = int(directory.name.split("b-", 1)[0])
    path = directory / "results.jsonl"
    if not path.is_file():
        raise AnalysisError(f"missing latency case: {path}")
    rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    if {row.get("arm") for row in rows} != set(ARMS):
        raise AnalysisError(f"latency case {directory.name} does not contain exactly B, S and A")
    if any(row.get("origin") != expected_origin for row in rows):
        raise AnalysisError(f"latency case {directory.name} mixes origins")
    if any(row.get("engine") != "fifo" for row in rows):
        raise AnalysisError(f"latency case {directory.name} is not FIFO")
    if any(row.get("payload") != payload for row in rows):
        raise AnalysisError(f"latency case {directory.name} mixes payload sizes")
    arms = {}
    for arm in ARMS:
        arm_rows = [row for row in rows if row.get("arm") == arm]
        values = load_latency_arm(arm_rows, path, arm)
        if set(values) != {expected_origin}:
            raise AnalysisError(f"latency case {directory.name} mixes origins")
        expected_environment = ({"SALIAS_DISABLE_ASYMMETRIC_FENCE": "1"}
                                if arm == "S" else {})
        if any(row.get("arm_environment") != expected_environment for row in arm_rows):
            raise AnalysisError(f"latency arm {arm} has the wrong fence environment")
        arms[arm] = values
    rows_by_arm = {arm: [row for row in rows if row.get("arm") == arm] for arm in ARMS}
    identity_sets = {arm: {row.get("client_sha256") for row in rows_by_arm[arm]}
                     for arm in ARMS}
    if identity_sets["S"] != identity_sets["A"]:
        raise AnalysisError("latency S and A must use the same v4 executable")
    if identity_sets["B"] & identity_sets["S"]:
        raise AnalysisError("latency baseline and candidate identities overlap")
    return arms


def direction_count(candidate, baseline):
    """Count groups in which the candidate median exceeds the baseline median."""
    return sum(c["median"] > b["median"] for c, b in zip(candidate, baseline))


def permutation_test(baseline_groups, candidate_groups):
    """Run the exact two-sided 6-vs-6 permutation test on group medians."""
    values = list(baseline_groups) + list(candidate_groups)
    if len(values) != 2 * GROUPS:
        raise AnalysisError("permutation test requires 6 + 6 groups")
    observed = statistics.mean(candidate_groups) - statistics.mean(baseline_groups)
    total = 0
    extreme = 0
    for mask in range(1 << len(values)):
        if mask.bit_count() != GROUPS:
            continue
        candidate = [values[index] for index in range(len(values)) if mask & (1 << index)]
        baseline = [values[index] for index in range(len(values)) if not mask & (1 << index)]
        difference = statistics.mean(candidate) - statistics.mean(baseline)
        total += 1
        if abs(difference) + 1e-12 >= abs(observed):
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
        baseline = [rate for group in (rng.choice(baseline_groups) for _ in range(GROUPS))
                    for rate in group]
        candidate = [rate for group in (rng.choice(candidate_groups) for _ in range(GROUPS))
                     for rate in group]
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


def compare_throughput(arm_data):
    """Compute claim, fence and combined comparisons from group medians."""
    comparisons = {}
    for label, (candidate_arm, baseline_arm) in COMPARISONS.items():
        candidate = arm_data[candidate_arm]
        baseline = arm_data[baseline_arm]
        ratio = candidate["median"] / baseline["median"]
        directions = direction_count(candidate["groups"], baseline["groups"])
        permutation = permutation_test(
            [group["median"] for group in baseline["groups"]],
            [group["median"] for group in candidate["groups"]],
        )
        comparisons[label] = {
            "candidate": candidate_arm,
            "baseline": baseline_arm,
            "candidate_median_mmsg_s": candidate["median"],
            "baseline_median_mmsg_s": baseline["median"],
            "ratio": ratio,
            "relative_percent": (ratio - 1.0) * 100.0,
            "group_directions_candidate_above_baseline": directions,
            "permutation": permutation,
            "bootstrap": bootstrap_interval(
                [group["rate"] for group in baseline["groups"]],
                [group["rate"] for group in candidate["groups"]],
            ),
            "group_medians_mmsg_s": {
                "candidate": [group["median"] for group in candidate["groups"]],
                "baseline": [group["median"] for group in baseline["groups"]],
            },
        }
    return comparisons


def throughput_decision(payload, comparison):
    """Apply the pre-registered 64 B E1 rule; other payloads stay descriptive."""
    if payload != 64:
        return "DESCRIPTIVE"
    ratio = comparison["ratio"]
    p_value = comparison["permutation"]["p_value"]
    directions = comparison["group_directions_candidate_above_baseline"]
    if ratio >= GO_RATIO and p_value < GO_P_VALUE and directions >= GO_DIRECTIONS:
        return "GO"
    if ratio < 1.10 or p_value >= 0.05:
        return "NO-GO"
    return "INCONCLUSIVE"


def compare_latency(latency_data):
    """Compute per-origin latency ratios for every arm comparison."""
    comparisons = {}
    for label, (candidate_arm, baseline_arm) in COMPARISONS.items():
        comparisons[label] = {}
        for origin in LATENCY_ORIGINS:
            comparisons[label][origin] = {
                metric: {
                    "candidate_ns": latency_data[candidate_arm][origin][metric],
                    "baseline_ns": latency_data[baseline_arm][origin][metric],
                    "ratio": (latency_data[candidate_arm][origin][metric] /
                              latency_data[baseline_arm][origin][metric]),
                    "candidate_range_ns": latency_data[candidate_arm][origin]["range_ns"][metric],
                    "baseline_range_ns": latency_data[baseline_arm][origin]["range_ns"][metric],
                }
                for metric in METRICS
            }
    return comparisons


def markdown(payload, throughput, latency, decision):
    """Render a compact report; performance gates remain pending until reviewed."""
    lines = [
        "# Claim API v4 三臂 A/B 分析",
        "",
        f"负载：{payload} B，FIFO 1P2S，1 MiB，batch=1，poll-limit=64。",
        "B=v3，S=v4+顺序屏障，A=v4+默认非对称屏障。",
        "延迟为各臂 10 轮独立分位数的中位数，不做直方图合并。",
        "",
        "## 吞吐",
        "",
        "| 对比 | 候选 Mmsg/s | 基线 Mmsg/s | 相对变化 | 组方向 | 精确置换 p |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for label, row in throughput.items():
        lines.append(
            f"| {label} | {row['candidate_median_mmsg_s']:.3f} | "
            f"{row['baseline_median_mmsg_s']:.3f} | {row['relative_percent']:+.2f}% | "
            f"{row['group_directions_candidate_above_baseline']}/6 | "
            f"{row['permutation']['p_value']:.6f} |"
        )
    lines.extend([
        "",
        f"claim API 64 B 预注册吞吐判定：**{decision}**"
        if payload == 64 else "256 B 结果仅作描述性报告，不套用 64 B GO 门槛。",
    ])
    lines.extend(["", "## 延迟", "",
                  "| 对比 | 口径 | p50 候选/基线 (ns) | p99 候选/基线 (ns) | "
                  "p99.9 候选/基线 (ns) |",
                  "|---|---|---:|---:|---:|"])
    for label, origins in latency.items():
        for origin, metrics in origins.items():
            lines.append(
                f"| {label} | {origin} | "
                + " | ".join(
                    f"{metrics[metric]['candidate_ns']}/{metrics[metric]['baseline_ns']} "
                    f"({metrics[metric]['ratio']:.3f}x)"
                    for metric in METRICS
                )
                + " |"
            )
    lines.extend(["", "### 延迟轮间范围", "",
                  "| 对比 | 口径 | 指标 | 候选 min–max (ns) | 基线 min–max (ns) |",
                  "|---|---|---|---:|---:|"])
    for label, origins in latency.items():
        for origin, metrics in origins.items():
            for metric in METRICS:
                candidate_range = metrics[metric]["candidate_range_ns"]
                baseline_range = metrics[metric]["baseline_range_ns"]
                lines.append(
                    f"| {label} | {origin} | {metric} | "
                    f"{candidate_range[0]}–{candidate_range[1]} | "
                    f"{baseline_range[0]}–{baseline_range[1]} |"
                )
    lines.extend([
        "",
        "## 判定边界",
        "",
        "本报告不自动宣告发布验收通过。64 B 的 GO 只适用于 claim API 吞吐门槛，"
        "不包含延迟尾部或发布验收。claim API 结论以 S/B 为准；"
        "A/S 单独归因屏障改动；A/B 是组合效果。需要结合预注册阈值、"
        "方向一致性、资源事件与完整正式矩阵评审后才能写入 CHANGELOG 或性能报告。",
    ])
    return "\n".join(lines) + "\n"


def analyze(experiment, output=None):
    """Run the complete analysis and optionally write JSON plus Markdown."""
    experiment = Path(experiment)
    data = load_experiment(experiment)
    payload_reports = []
    for payload in PAYLOADS:
        throughput = {}
        for arm in ARMS:
            directory = experiment / f"throughput-{payload}b" / ARM_DIRECTORIES[arm]
            throughput[arm] = load_throughput_arm(directory, arm, payload)
            if throughput[arm]["client_sha256"] != data["clients"][arm]["sha256"]:
                raise AnalysisError(f"throughput arm {arm} used a different executable")
        latency = {}
        for origin in LATENCY_ORIGINS:
            case = load_latency_case(experiment / "latency" / f"{payload}b-{origin}")
            for arm in ARMS:
                rows = [json.loads(line) for line in
                        (experiment / "latency" / f"{payload}b-{origin}"
                         / "results.jsonl").read_text().splitlines() if line.strip()]
                rows = [row for row in rows if row.get("arm") == arm]
                if any(row["client_sha256"] != data["clients"][arm]["sha256"]
                       for row in rows):
                    raise AnalysisError(f"latency arm {arm} used a different executable")
                latency.setdefault(arm, {})[origin] = case[arm][origin]
        tcompare = compare_throughput(throughput)
        decision = throughput_decision(payload, tcompare["claim_api_S_vs_B"])
        raw_latency = compare_latency(latency)
        lcompare = {
            label: {
                origin: {
                    metric: raw_latency[label][origin][metric]["ratio"]
                    for metric in METRICS
                }
                for origin in LATENCY_ORIGINS
            }
            for label in COMPARISONS
        }
        payload_reports.append({
            "payload": payload,
            "throughput": tcompare,
            "claim_api_throughput_decision": decision,
            "latency": lcompare,
            "latency_detail": raw_latency,
        })
        if output:
            out = Path(output)
            out.mkdir(parents=True, exist_ok=True)
            (out / f"claim-api-v4-{payload}b.md").write_text(
                markdown(payload, tcompare, raw_latency, decision)
            )
    report = {"protocol": "p3-claim-api-v4-analysis-v1", "payloads": payload_reports}
    if output:
        (Path(output) / "claim-api-v4-analysis.json").write_text(
            json.dumps(report, indent=2) + "\n"
        )
    return report


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--experiment", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True,
                        help="build manifest used to verify executable identity")
    parser.add_argument("--output", type=Path, default=None)
    return parser.parse_args()


def main():
    args = parse_args()
    try:
        manifest = json.loads(args.manifest.read_text())
        experiment = load_experiment(args.experiment)
        for arm in ARMS:
            expected = manifest["variants"]["v3" if arm == "B" else "v4"]["client_sha256"]
            if experiment["clients"][arm]["sha256"] != expected:
                raise AnalysisError(f"arm {arm} executable differs from build manifest")
        report = analyze(args.experiment, args.output or args.experiment)
    except (OSError, KeyError, ValueError, AnalysisError) as error:
        raise SystemExit(f"analysis failed: {error}")
    for payload in report["payloads"]:
        print(f"\n{payload['payload']} B")
        print(f"  claim API throughput decision: {payload['claim_api_throughput_decision']}")
        for label, row in payload["throughput"].items():
            print(f"  {label}: {row['relative_percent']:+.2f}% "
                  f"({row['group_directions_candidate_above_baseline']}/6 groups)")


if __name__ == "__main__":
    main()
