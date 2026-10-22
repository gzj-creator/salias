#!/usr/bin/env python3
"""Run the pre-registered P3 E2 claim-isolation 4-arm ABBA matrix.

The runner executes the fixed 4-arm matrix:
  - ST: single-threaded publisher, owning claim
  - MT-O: multi-threaded publisher (sleeping helper), owning claim
  - MT-X: helper exits before publish, owning claim (isolates libc state)
  - MT-B: multi-threaded publisher (sleeping helper), borrowed claim

Each group runs ST, MT-O, MT-X, MT-B, MT-B, MT-X, MT-O, ST (ABBA order).
Every round gets a fresh transient systemd unit. A failed round terminates the run.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parents[3]
BUILD_TOOL = ROOT / "tools/aeron_compare/build_e2_claim_probe.py"
PROBE_IDENTITY = "p3-e2-claim-isolation-probe-v1"
V3_SOURCE_SHA256 = "6a23a77598c6e980f7b6625eb2471cc42a26917469dc77fc51ab9d5b767bab1e"
V3_SOURCE_SPEC = "v3.0.0:tools/aeron_compare/salias_ipc_compare.cpp"
GROUPS = 6
ARM_ORDER = ("ST", "MT-O", "MT-X", "MT-B", "MT-B", "MT-X", "MT-O", "ST")
ARMS = ("ST", "MT-O", "MT-X", "MT-B")
CAPACITY = 1 << 20

# The complete formal matrix is frozen before smoke or warm-up starts.
CONFIGURATIONS = (
    {
        "id": "throughput-64b",
        "kind": "throughput",
        "producers": 1,
        "consumers": 2,
        "payload": 64,
        "messages": 50_000_000,
        "rate": 0,
        "latency_sample_rate": 0,
        "latency_origin": "claim",
        "capacity": CAPACITY,
        "batch_size": 1,
        "poll_limit": 64,
        "runtime_seconds": 240,
    },
    {
        "id": "throughput-256b",
        "kind": "throughput",
        "producers": 1,
        "consumers": 2,
        "payload": 256,
        "messages": 50_000_000,
        "rate": 0,
        "latency_sample_rate": 0,
        "latency_origin": "claim",
        "capacity": CAPACITY,
        "batch_size": 1,
        "poll_limit": 64,
        "runtime_seconds": 240,
    },
    {
        "id": "latency-64b-attempt",
        "kind": "latency",
        "producers": 1,
        "consumers": 1,
        "payload": 64,
        "messages": 2_000_000,
        "rate": 1_000_000,
        "latency_sample_rate": 16,
        "latency_origin": "attempt",
        "capacity": CAPACITY,
        "batch_size": 1,
        "poll_limit": 64,
        "runtime_seconds": 120,
    },
    {
        "id": "latency-64b-planned",
        "kind": "latency",
        "producers": 1,
        "consumers": 1,
        "payload": 64,
        "messages": 2_000_000,
        "rate": 1_000_000,
        "latency_sample_rate": 16,
        "latency_origin": "planned",
        "capacity": CAPACITY,
        "batch_size": 1,
        "poll_limit": 64,
        "runtime_seconds": 120,
    },
    {
        "id": "latency-256b-attempt",
        "kind": "latency",
        "producers": 1,
        "consumers": 1,
        "payload": 256,
        "messages": 2_000_000,
        "rate": 1_000_000,
        "latency_sample_rate": 16,
        "latency_origin": "attempt",
        "capacity": CAPACITY,
        "batch_size": 1,
        "poll_limit": 64,
        "runtime_seconds": 120,
    },
    {
        "id": "latency-256b-planned",
        "kind": "latency",
        "producers": 1,
        "consumers": 1,
        "payload": 256,
        "messages": 2_000_000,
        "rate": 1_000_000,
        "latency_sample_rate": 16,
        "latency_origin": "planned",
        "capacity": CAPACITY,
        "batch_size": 1,
        "poll_limit": 64,
        "runtime_seconds": 120,
    },
)


def digest(path):
    """Return the SHA-256 digest of a file."""
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def explicit_environment(arm):
    """Return the complete environment override for one arm."""
    if arm not in ARMS:
        raise ValueError(f"unknown arm: {arm}")

    mode_map = {
        "ST": "st",
        "MT-O": "mt-o",
        "MT-X": "mt-x",
        "MT-B": "mt-b",
    }

    # MT-B uses borrowed claim; all others use owning claim
    claim_variant = "borrowed" if arm == "MT-B" else "owning"

    return {
        "SALIAS_E2_PUBLISHER_MODE": mode_map[arm],
        "SALIAS_E2_CLAIM_VARIANT": claim_variant,
        "SALIAS_DISABLE_ASYMMETRIC_FENCE": "0",
        "SALIAS_RESOURCE_CAPPED": "1",
    }


def arm_sequence():
    """Return the fixed ABBA order for every independent group."""
    return list(ARM_ORDER)


def formal_schedule():
    """Expand the fixed 6-configuration, 6-group, ABBA matrix."""
    schedule = []
    ordinal = 0
    for configuration in CONFIGURATIONS:
        for group in range(GROUPS):
            for arm in arm_sequence():
                schedule.append({
                    "ordinal": ordinal,
                    "configuration_id": configuration["id"],
                    "kind": configuration["kind"],
                    "group": group,
                    "arm": arm,
                    "producers": configuration["producers"],
                    "consumers": configuration["consumers"],
                    "payload": configuration["payload"],
                    "messages": configuration["messages"],
                    "rate": configuration["rate"],
                    "latency_sample_rate": configuration["latency_sample_rate"],
                    "latency_origin": configuration["latency_origin"],
                    "capacity": configuration["capacity"],
                    "batch_size": configuration["batch_size"],
                    "poll_limit": configuration["poll_limit"],
                    "runtime_seconds": configuration["runtime_seconds"],
                })
                ordinal += 1
    return schedule


def warmup_schedule():
    """Generate independent warm-up rounds: one per configuration per arm."""
    schedule = []
    ordinal = 0
    for configuration in CONFIGURATIONS:
        for arm in ARMS:
            schedule.append({
                "ordinal": ordinal,
                "configuration_id": configuration["id"],
                "kind": configuration["kind"],
                "group": -1,  # Not part of formal groups
                "arm": arm,
                "producers": configuration["producers"],
                "consumers": configuration["consumers"],
                "payload": configuration["payload"],
                "messages": configuration["messages"],
                "rate": configuration["rate"],
                "latency_sample_rate": configuration["latency_sample_rate"],
                "latency_origin": configuration["latency_origin"],
                "capacity": configuration["capacity"],
                "batch_size": configuration["batch_size"],
                "poll_limit": configuration["poll_limit"],
                "runtime_seconds": configuration["runtime_seconds"],
            })
            ordinal += 1
    return schedule


def smoke_schedule():
    """Generate functional smoke tests: 64B throughput, one per arm."""
    config = CONFIGURATIONS[0]  # throughput-64b
    schedule = []
    ordinal = 0
    for arm in ARMS:
        schedule.append({
            "ordinal": ordinal,
            "configuration_id": f"smoke-{config['id']}",
            "kind": config["kind"],
            "group": -1,
            "arm": arm,
            "producers": config["producers"],
            "consumers": config["consumers"],
            "payload": config["payload"],
            "messages": 1_000_000,  # Shorter smoke test
            "rate": 0,
            "latency_sample_rate": 0,
            "latency_origin": "claim",
            "capacity": CAPACITY,
            "batch_size": 1,
            "poll_limit": 64,
            "runtime_seconds": 60,
        })
        ordinal += 1
    return schedule


def systemd_run_command(round_spec, client_binary, output_dir):
    """Build the systemd-run command with cgroup limits and CPU affinity."""
    round_dir = output_dir / f"round_{round_spec['ordinal']:04d}"
    round_dir.mkdir(parents=True, exist_ok=True)

    # CPU affinity: coordinator on CPU0, workers on CPU1-3
    allowed_cpus = "0-3"

    client_args = [
        str(client_binary),
        "--producers", str(round_spec["producers"]),
        "--consumers", str(round_spec["consumers"]),
        "--payload", str(round_spec["payload"]),
        "--messages", str(round_spec["messages"]),
        "--capacity", str(round_spec["capacity"]),
        "--batch-size", str(round_spec["batch_size"]),
        "--poll-limit", str(round_spec["poll_limit"]),
    ]

    if round_spec["rate"] > 0:
        client_args.extend(["--rate", str(round_spec["rate"])])
    if round_spec["latency_sample_rate"] > 0:
        client_args.extend([
            "--latency-sample-rate", str(round_spec["latency_sample_rate"]),
            "--latency-origin", round_spec["latency_origin"],
        ])

    env = explicit_environment(round_spec["arm"])

    systemd_cmd = [
        "systemd-run",
        "--user",
        "--wait",
        "--pipe",
        "--collect",
        f"--property=AllowedCPUs={allowed_cpus}",
        "--property=Nice=5",
        "--property=MemoryHigh=640M",
        "--property=MemoryMax=704M",
        "--property=MemorySwapMax=0",
    ]

    for key, value in env.items():
        systemd_cmd.append(f"--setenv={key}={value}")

    systemd_cmd.extend(client_args)

    return systemd_cmd, round_dir


def run_round(round_spec, client_binary, output_dir, stage):
    """Execute one round and save all evidence."""
    systemd_cmd, round_dir = systemd_run_command(round_spec, client_binary, output_dir)

    stdout_path = round_dir / "stdout.txt"
    stderr_path = round_dir / "stderr.txt"
    command_path = round_dir / "command.json"

    # Save command and spec
    command_path.write_text(json.dumps({
        "stage": stage,
        "round_spec": round_spec,
        "systemd_command": systemd_cmd,
        "environment": explicit_environment(round_spec["arm"]),
    }, indent=2) + "\n")

    print(f"[{stage}] round {round_spec['ordinal']:04d} "
          f"config={round_spec['configuration_id']} group={round_spec['group']} "
          f"arm={round_spec['arm']}", flush=True)

    start_time = time.time()

    try:
        with stdout_path.open("wb") as stdout_file, stderr_path.open("wb") as stderr_file:
            result = subprocess.run(
                systemd_cmd,
                stdout=stdout_file,
                stderr=stderr_file,
                timeout=round_spec["runtime_seconds"] + 30,
            )

        elapsed = time.time() - start_time

        outcome = {
            "ordinal": round_spec["ordinal"],
            "stage": stage,
            "configuration_id": round_spec["configuration_id"],
            "group": round_spec["group"],
            "arm": round_spec["arm"],
            "exit_code": result.returncode,
            "elapsed_seconds": elapsed,
            "stdout_sha256": digest(stdout_path),
            "stderr_sha256": digest(stderr_path),
            "command_sha256": digest(command_path),
            "status": "ok" if result.returncode == 0 else "failed",
        }

        (round_dir / "outcome.json").write_text(json.dumps(outcome, indent=2) + "\n")

        if result.returncode != 0:
            print(f"  FAILED: exit code {result.returncode}", flush=True)
            return None

        print(f"  OK: {elapsed:.1f}s", flush=True)
        return outcome

    except subprocess.TimeoutExpired:
        elapsed = time.time() - start_time
        print(f"  TIMEOUT after {elapsed:.1f}s", flush=True)

        outcome = {
            "ordinal": round_spec["ordinal"],
            "stage": stage,
            "configuration_id": round_spec["configuration_id"],
            "group": round_spec["group"],
            "arm": round_spec["arm"],
            "exit_code": -1,
            "elapsed_seconds": elapsed,
            "stdout_sha256": digest(stdout_path) if stdout_path.exists() else None,
            "stderr_sha256": digest(stderr_path) if stderr_path.exists() else None,
            "command_sha256": digest(command_path),
            "status": "timeout",
        }

        (round_dir / "outcome.json").write_text(json.dumps(outcome, indent=2) + "\n")
        return None


def select_client_binary(probe_dir, arm):
    """Select the correct client binary for the arm."""
    # MT-B uses borrowed variant; all others use owning variant
    variant = "borrowed" if arm == "MT-B" else "owning"
    return probe_dir / variant / "probe"


def run_experiment(args):
    """Execute the complete E2 experiment: smoke, warmup, formal rounds."""
    probe_dir = args.client.parent.parent if args.client else (
        ROOT / "build/p3-e2-claim-probe"
    )

    # Verify both variants exist
    owning_binary = probe_dir / "owning" / "probe"
    borrowed_binary = probe_dir / "borrowed" / "probe"

    if not owning_binary.is_file():
        raise SystemExit(f"missing owning probe binary: {owning_binary}")
    if not borrowed_binary.is_file():
        raise SystemExit(f"missing borrowed probe binary: {borrowed_binary}")

    output_dir = args.output.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    # Save experiment manifest
    manifest = {
        "protocol": PROBE_IDENTITY,
        "v3_source_spec": V3_SOURCE_SPEC,
        "v3_source_sha256": V3_SOURCE_SHA256,
        "probe_dir": str(probe_dir),
        "owning_binary": str(owning_binary),
        "owning_binary_sha256": digest(owning_binary),
        "borrowed_binary": str(borrowed_binary),
        "borrowed_binary_sha256": digest(borrowed_binary),
        "hostname": platform.node(),
        "start_time": time.strftime("%Y-%m-%d %H:%M:%S %Z"),
        "groups": GROUPS,
        "arm_order": ARM_ORDER,
        "configurations": list(CONFIGURATIONS),
    }

    (output_dir / "experiment.json").write_text(json.dumps(manifest, indent=2) + "\n")

    print(f"P3 E2 claim-isolation experiment")
    print(f"Output: {output_dir}")
    print(f"Owning binary: {digest(owning_binary)[:16]}...")
    print(f"Borrowed binary: {digest(borrowed_binary)[:16]}...")
    print()

    # Run stages
    results = {
        "smoke": [],
        "warmup": [],
        "formal": [],
    }

    # Smoke tests
    print("=== Smoke tests ===")
    for round_spec in smoke_schedule():
        client_binary = select_client_binary(probe_dir, round_spec["arm"])
        outcome = run_round(round_spec, client_binary, output_dir, "smoke")
        if outcome:
            results["smoke"].append(outcome)

    print()

    # Warmup
    print("=== Warmup rounds ===")
    for round_spec in warmup_schedule():
        client_binary = select_client_binary(probe_dir, round_spec["arm"])
        outcome = run_round(round_spec, client_binary, output_dir, "warmup")
        if outcome:
            results["warmup"].append(outcome)

    print()

    # Formal rounds
    print("=== Formal rounds ===")
    formal = formal_schedule()
    print(f"Total: {len(formal)} rounds")
    print()

    for round_spec in formal:
        client_binary = select_client_binary(probe_dir, round_spec["arm"])
        outcome = run_round(round_spec, client_binary, output_dir, "formal")

        if outcome is None:
            print("\nFormal round failed. Stopping.")
            results["stopped_at_ordinal"] = round_spec["ordinal"]
            break

        results["formal"].append(outcome)

    # Save results
    results["completed_formal"] = len(results["formal"])
    results["expected_formal"] = len(formal)
    results["complete"] = results["completed_formal"] == results["expected_formal"]

    (output_dir / "results.json").write_text(json.dumps(results, indent=2) + "\n")

    print()
    print(f"Completed: {results['completed_formal']}/{results['expected_formal']} formal rounds")

    if results["complete"]:
        print("Experiment COMPLETE")
        return 0
    else:
        print("Experiment INCOMPLETE")
        return 1


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client", type=Path, default=None,
                        help="path to one probe binary (its parent.parent is the probe_dir)")
    parser.add_argument("--output", type=Path, required=True,
                        help="output directory for all evidence")
    return parser.parse_args()


def main():
    args = parse_args()
    return run_experiment(args)


if __name__ == "__main__":
    sys.exit(main())
