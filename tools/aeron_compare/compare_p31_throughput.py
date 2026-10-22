#!/usr/bin/env python3
"""Apply the pre-registered P3-1 three-arm throughput gates (G1-G3).

The experiment directory must contain ``experiment.json`` plus the three arm
directories written by ``doc/plans/runners/p3-p31-throughput.py``.  Every arm is
re-verified through the E1 loader (SHA256SUMS, RESULT lines, group/round
structure) before any statistic is computed.  Arm identity is checked as well:

* B is the frozen v3.0.0 client (pin below);
* S and A are the *same* candidate binary, distinguished only by
  ``SALIAS_DISABLE_ASYMMETRIC_FENCE=1`` being present (S) or absent (A).

Gates (plan §6.3, pre-registered):

* G1 (64 B): C-asym / C-seq median ratio >= 1.25, exact group-level
  permutation p < 0.01, at least 5/6 groups in the same direction.
* G2 (64 B): C-asym / B median ratio >= 1.20.
* G3 (192 B): C-asym median >= C-seq * 0.97 and the high-mode rate
  (>= 15 Mmsg/s, the runner threshold) must not drop by more than 10 pp.
"""

import argparse
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from compare_throughput_ab import (  # noqa: E402
    AnalysisError,
    GROUPS,
    ROUNDS,
    load_arm,
    permutation_test,
    bootstrap_interval,
    read_environment,
)


BASELINE_SHA256 = "3f8fc6957ce56483d7051dd6625f44fd8f755d57a66d04e1e5efa48e65b9c7da"
ARM_DIRECTORIES = {"B": "baseline", "S": "candidate-seq", "A": "candidate-asym"}
ARM_SETENV = {"B": [], "S": ["SALIAS_DISABLE_ASYMMETRIC_FENCE=1"], "A": []}
GO_RATIO = 1.25
GO_P_VALUE = 0.01
GO_DIRECTIONS = 5
G2_RATIO = 1.20
G3_NONINFERIOR_RATIO = 0.97
G3_HIGH_MODE_DROP_LIMIT = 0.10
HIGH_MODE_THRESHOLD_MMSG_S = 15.0


def expected_sequence(groups=GROUPS):
    """Return the registered alternating block order."""
    return [
        [f"{arm}{group}" for arm in (("B", "S", "A") if group % 2 == 0 else ("A", "S", "B"))]
        for group in range(groups)
    ]


def load_manifest(directory, baseline_sha256=BASELINE_SHA256):
    """Load and validate the three-arm experiment manifest."""
    path = Path(directory) / "experiment.json"
    if not path.is_file():
        raise AnalysisError(f"missing experiment.json: {directory}")
    manifest = json.loads(path.read_text())
    if manifest.get("groups") != GROUPS or manifest.get("rounds_per_group") != ROUNDS:
        raise AnalysisError("experiment is not 6 groups x 20 rounds")
    if int(manifest.get("payload_bytes", -1)) not in (64, 192):
        raise AnalysisError("experiment payload is not 64 B or 192 B")
    if int(manifest.get("capacity_bytes", -1)) != 1 << 20:
        raise AnalysisError("experiment capacity is not 1 MiB")
    if int(manifest.get("messages_per_round", -1)) != 50_000_000:
        raise AnalysisError("experiment messages are not 50,000,000")
    sequence = expected_sequence()
    if manifest.get("sequence") != sequence:
        raise AnalysisError(f"block sequence is not the registered rotation: {manifest.get('sequence')}")
    blocks = list(manifest.get("blocks", []))
    if len(blocks) != GROUPS * 3:
        raise AnalysisError(f"manifest has {len(blocks)} blocks, expected {GROUPS * 3}")
    expected_blocks = [
        (arm, group)
        for group, arms_in_group in enumerate(sequence)
        for arm in (item[0] for item in arms_in_group)
    ]
    for index, (block, (expected_arm, expected_group)) in enumerate(
            zip(blocks, expected_blocks)):
        if (block.get("block"), block.get("arm"), block.get("group")) != (
                index, expected_arm, expected_group):
            raise AnalysisError(f"block {index} is not the registered arm/group")
        if block.get("returncode") != 0:
            raise AnalysisError(f"block {block.get('block')} did not exit cleanly")
    arms = manifest.get("arms") or {}
    if set(arms) != set(ARM_DIRECTORIES):
        raise AnalysisError(f"manifest arms are {sorted(arms)}, expected B/S/A")
    for arm, directory_name in ARM_DIRECTORIES.items():
        entry = arms[arm]
        if Path(entry.get("output", "")).name != directory_name:
            raise AnalysisError(f"arm {arm} output directory is not {directory_name}")
        if entry.get("setenv") != ARM_SETENV[arm]:
            raise AnalysisError(f"arm {arm} setenv is {entry.get('setenv')}, expected {ARM_SETENV[arm]}")
        client = Path(entry.get("client", ""))
        if not client.is_file():
            raise AnalysisError(f"arm {arm} client file is missing: {client}")
        actual_sha256 = hashlib.sha256(client.read_bytes()).hexdigest()
        if entry.get("sha256") != actual_sha256:
            raise AnalysisError(f"arm {arm} client hash differs from experiment manifest")
    if arms["B"]["sha256"] != baseline_sha256:
        raise AnalysisError("arm B is not the frozen v3.0.0 client")
    if (arms["S"]["sha256"] != arms["A"]["sha256"] or
            Path(arms["S"]["client"]).resolve() != Path(arms["A"]["client"]).resolve()):
        raise AnalysisError("C-seq and C-asym must be the same binary")
    return manifest


def load_experiment_arm(directory, arm, payload, manifest):
    """Load one arm and require the recorded environment to match its identity."""
    arm_directory = Path(directory) / ARM_DIRECTORIES[arm]
    environment = read_environment(arm_directory)
    recorded = environment.get("environment_variables")
    if recorded != ARM_SETENV[arm]:
        raise AnalysisError(
            f"arm {arm} environment_variables are {recorded}, expected {ARM_SETENV[arm]}"
        )
    client = Path(manifest["arms"][arm]["client"]).resolve()
    if (environment.get("client_sha256") != manifest["arms"][arm]["sha256"] or
            Path(environment.get("client", "")).resolve() != client):
        raise AnalysisError(f"arm {arm} environment client identity differs from experiment manifest")
    loaded = load_arm(arm_directory, payload)
    if loaded["client_sha256"] != manifest["arms"][arm]["sha256"]:
        raise AnalysisError(f"arm {arm} loader identity differs from experiment manifest")
    return loaded


def high_mode_count(rates, threshold=HIGH_MODE_THRESHOLD_MMSG_S):
    """Count rounds at or above the runner high-mode threshold."""
    return sum(rate >= threshold for rate in rates)


def gate_g1(seq, asym, eligible=True):
    """G1: barrier effect measured on the identical binary."""
    seq_medians = [group["median"] for group in seq["groups"]]
    asym_medians = [group["median"] for group in asym["groups"]]
    ratio = asym["median"] / seq["median"]
    permutation = permutation_test(seq_medians, asym_medians)
    directions = sum(a > s for s, a in zip(seq_medians, asym_medians))
    passed = (ratio >= GO_RATIO and permutation["p_value"] < GO_P_VALUE
              and directions >= GO_DIRECTIONS) if eligible else None
    return {
        "ratio_a_over_s": ratio,
        "permutation": permutation,
        "directions_a_above_s": directions,
        "requirements": {
            "ratio": f">= {GO_RATIO}",
            "p_value": f"< {GO_P_VALUE}",
            "directions": f">= {GO_DIRECTIONS}",
        },
        "eligible": eligible,
        "decision": ("PASS" if passed else "FAIL") if eligible else "DESCRIPTIVE",
        "passed": passed,
    }


def gate_g3(seq, asym):
    """G3: 192 B non-inferiority of the asymmetric path."""
    ratio = asym["median"] / seq["median"]
    seq_high = high_mode_count(seq["rates"]) / len(seq["rates"])
    asym_high = high_mode_count(asym["rates"]) / len(asym["rates"])
    drop = seq_high - asym_high
    passed = ratio >= G3_NONINFERIOR_RATIO and drop <= G3_HIGH_MODE_DROP_LIMIT
    return {
        "ratio_a_over_s": ratio,
        "high_mode_rate_seq": seq_high,
        "high_mode_rate_asym": asym_high,
        "high_mode_drop_pp": drop * 100,
        "high_mode_threshold_mmsg_s": HIGH_MODE_THRESHOLD_MMSG_S,
        "requirements": {
            "ratio": f">= {G3_NONINFERIOR_RATIO}",
            "high_mode_drop_pp": f"<= {G3_HIGH_MODE_DROP_LIMIT * 100:g}",
        },
        "passed": passed,
    }


def summarize(arm):
    """Return the reporting fields shared by all arms."""
    return {
        "directory": arm["directory"],
        "client_sha256": arm["client_sha256"],
        "verified_files": arm["verified_files"],
        "median_mmsg_s": arm["median"],
        "mean_mmsg_s": arm["mean"],
        "cv": arm["cv"],
        "high_mode_count_at_15": high_mode_count(arm["rates"]),
        "group_medians_mmsg_s": [group["median"] for group in arm["groups"]],
    }


def compare(directory, payload=None, baseline_sha256=BASELINE_SHA256):
    """Run all registered gates for one payload experiment directory."""
    directory = Path(directory).resolve()
    manifest = load_manifest(directory, baseline_sha256)
    experiment_payload = int(manifest["payload_bytes"])
    if payload is not None and payload != experiment_payload:
        raise AnalysisError(f"payload is {experiment_payload}, expected {payload}")
    arms = {arm: load_experiment_arm(directory, arm, experiment_payload, manifest)
            for arm in ARM_DIRECTORIES}
    if arms["S"]["client_sha256"] != arms["A"]["client_sha256"]:
        raise AnalysisError("C-seq and C-asym hashes differ")
    result = {
        "protocol": "P3-1 pre-registered three-arm interleaved throughput",
        "payload_bytes": experiment_payload,
        "groups": GROUPS,
        "rounds_per_group": ROUNDS,
        "arms": {arm: summarize(arms[arm]) for arm in ARM_DIRECTORIES},
        "g1_barrier_effect": gate_g1(
            arms["S"], arms["A"], eligible=experiment_payload == 64),
        "bootstrap_a_minus_s": bootstrap_interval(
            [group["rates"] for group in arms["S"]["groups"]],
            [group["rates"] for group in arms["A"]["groups"]],
        ),
    }
    if experiment_payload == 64:
        result["g2_product_ratio"] = {
            "ratio_a_over_b": arms["A"]["median"] / arms["B"]["median"],
            "requirement": f">= {G2_RATIO}",
            "passed": arms["A"]["median"] / arms["B"]["median"] >= G2_RATIO,
        }
    if experiment_payload == 192:
        result["g3_192b_non_inferior"] = gate_g3(arms["S"], arms["A"])
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("experiment", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--payload", type=int, choices=(64, 192), default=None)
    args = parser.parse_args()
    try:
        result = compare(args.experiment, args.payload)
    except AnalysisError as error:
        raise SystemExit(f"analysis failed: {error}")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(f"payload={result['payload_bytes']} B (frozen)")
    for arm in ("B", "S", "A"):
        summary = result["arms"][arm]
        print(f"  {arm}: median={summary['median_mmsg_s']:.6f} "
              f"means={summary['mean_mmsg_s']:.6f} CV={summary['cv']:.4%} "
              f"high15={summary['high_mode_count_at_15']}/120")
    g1 = result["g1_barrier_effect"]
    print(f"G1 ratio A/S={g1['ratio_a_over_s']:.6f} "
          f"p={g1['permutation']['p_value']:.6f} "
          f"directions={g1['directions_a_above_s']}/6 -> {g1['decision']}")
    if "g2_product_ratio" in result:
        g2 = result["g2_product_ratio"]
        print(f"G2 ratio A/B={g2['ratio_a_over_b']:.6f} -> {'PASS' if g2['passed'] else 'FAIL'}")
    if "g3_192b_non_inferior" in result:
        g3 = result["g3_192b_non_inferior"]
        print(f"G3 ratio A/S={g3['ratio_a_over_s']:.6f} "
              f"high-mode drop={g3['high_mode_drop_pp']:+.1f}pp -> "
              f"{'PASS' if g3['passed'] else 'FAIL'}")


if __name__ == "__main__":
    main()
