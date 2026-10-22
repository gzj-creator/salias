#!/usr/bin/env python3
"""Run the pre-registered P3-1 three-arm throughput matrix.

Arms: B (frozen v3.0.0 client), C-seq (candidate with
SALIAS_DISABLE_ASYMMETRIC_FENCE=1) and C-asym (candidate, default protocol).
Every block is one group of 20 rounds in its own systemd unit. Blocks are
interleaved as B,C-seq,C-asym / C-asym,C-seq,B alternating per group so that
each arm is measured adjacent in time with the order flipped across groups.
"""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parents[3]
RUNNER = ROOT / "doc/plans/runners/high-frequency-stability-runner.py"
BASELINE_SHA256 = "3f8fc6957ce56483d7051dd6625f44fd8f755d57a66d04e1e5efa48e65b9c7da"
ARMS = ("B", "S", "A")
ARM_DIRECTORY = {
    "B": "baseline",
    "S": "candidate-seq",
    "A": "candidate-asym",
}
ARM_SETENV = {
    "B": [],
    "S": ["SALIAS_DISABLE_ASYMMETRIC_FENCE=1"],
    "A": [],
}


def digest(path):
    """Return the SHA-256 digest used to bind an arm to its client."""
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def block_sequence(groups):
    """Return [[(arm, group), ...], ...] following the registered rotation."""
    sequence = []
    for group in range(groups):
        order = ("B", "S", "A") if group % 2 == 0 else ("A", "S", "B")
        sequence.append([(arm, group) for arm in order])
    return sequence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-client", type=Path, required=True)
    parser.add_argument("--candidate-client", type=Path, required=True)
    parser.add_argument("--candidate-sha256", required=True,
                        help="pinned SHA-256 of the candidate client for this matrix")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--payload", type=int, choices=(64, 192), required=True)
    parser.add_argument("--messages", type=int, default=50_000_000)
    parser.add_argument("--capacity", type=int, default=1 << 20)
    parser.add_argument("--groups", type=int, default=6)
    parser.add_argument("--rounds", type=int, default=20)
    args = parser.parse_args()
    if args.groups != 6 or args.rounds != 20:
        parser.error("P3-1 acceptance is pre-registered as 6 groups x 20 rounds")
    clients = {
        "B": args.baseline_client.resolve(),
        "S": args.candidate_client.resolve(),
        "A": args.candidate_client.resolve(),
    }
    expected = {"B": BASELINE_SHA256, "S": args.candidate_sha256, "A": args.candidate_sha256}
    for arm, client in clients.items():
        if not client.is_file():
            parser.error(f"missing {arm} client: {client}")
        actual = digest(client)
        if actual != expected[arm]:
            parser.error(f"{arm} client SHA-256 is {actual}, expected {expected[arm]}")
    output = args.output.resolve()
    if output.exists() and any(output.iterdir()):
        parser.error(f"output directory is not empty: {output}")
    output.mkdir(parents=True, exist_ok=True)
    logs = output / "orchestration-logs"
    logs.mkdir()
    sequence = block_sequence(args.groups)
    manifest = {
        "protocol": "P3-1 pre-registered three-arm interleaved throughput",
        "payload_bytes": args.payload,
        "messages_per_round": args.messages,
        "capacity_bytes": args.capacity,
        "groups": args.groups,
        "rounds_per_group": args.rounds,
        "sequence": [[f"{arm}{group}" for arm, group in block] for block in sequence],
        "arms": {
            arm: {
                "client": str(clients[arm]),
                "sha256": digest(clients[arm]),
                "setenv": ARM_SETENV[arm],
                "output": ARM_DIRECTORY[arm],
            }
            for arm in ARMS
        },
        "started": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "blocks": [],
    }
    manifest_path = output / "experiment.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    total = sum(len(block) for block in sequence)
    block_index = 0
    for block in sequence:
        for arm, group in block:
            arm_dir = output / ARM_DIRECTORY[arm]
            command = [
                "taskset", "-c", "0", sys.executable, str(RUNNER),
                "--client", str(clients[arm]),
                "--output", str(arm_dir),
                "--messages", str(args.messages),
                "--payload", str(args.payload),
                "--capacity", str(args.capacity),
                "--groups", str(args.groups),
                "--rounds", str(args.rounds),
                "--interval-seconds", "0",
                "--max-consecutive-low", "0",
                "--start-group", str(group),
                "--end-group", str(group + 1),
            ]
            for assignment in ARM_SETENV[arm]:
                command.extend(["--setenv", assignment])
            if group > 0:
                command.append("--allow-existing")
            prefix = f"block-{block_index:02d}-{arm}{group}"
            started = time.time()
            print(f"[{block_index + 1}/{total}] arm={arm} group={group} "
                  f"payload={args.payload}", flush=True)
            with (logs / f"{prefix}.stdout").open("w") as stdout, \
                    (logs / f"{prefix}.stderr").open("w") as stderr:
                result = subprocess.run(command, cwd=ROOT, stdout=stdout, stderr=stderr,
                                        text=True, check=False)
            manifest["blocks"].append({
                "block": block_index,
                "arm": arm,
                "group": group,
                "returncode": result.returncode,
                "wall_seconds": time.time() - started,
                "command": command,
                "stdout": str((logs / f"{prefix}.stdout").relative_to(output)),
                "stderr": str((logs / f"{prefix}.stderr").relative_to(output)),
            })
            manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
            if result.returncode:
                raise SystemExit(
                    f"block {prefix} failed ({result.returncode}); "
                    f"see {logs / prefix}.stderr"
                )
            block_index += 1
    manifest["finished"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"Completed P3-1 throughput payload={args.payload}: {output}", flush=True)


if __name__ == "__main__":
    main()
