#!/usr/bin/env python3
"""Run the pre-registered E1 ABBA throughput matrix with independent blocks."""

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
CANDIDATE_SHA256 = "5efae0b41ed42f9a38d69997ed8a1fafc69d78ae69eea948c79abbdec0f433b6"
SEQUENCE = (
    ("B", 0), ("C", 0), ("C", 1), ("B", 1),
    ("B", 2), ("C", 2), ("C", 3), ("B", 3),
    ("B", 4), ("C", 4), ("C", 5), ("B", 5),
)


def digest(path):
    """Return the SHA-256 digest used to bind an arm to its client."""
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-client", type=Path, required=True)
    parser.add_argument("--candidate-client", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--payload", type=int, choices=(64, 192), required=True)
    parser.add_argument("--messages", type=int, default=50_000_000)
    parser.add_argument("--groups", type=int, default=6)
    parser.add_argument("--rounds", type=int, default=20)
    args = parser.parse_args()
    if args.groups != 6 or args.rounds != 20:
        parser.error("E1 is pre-registered as 6 groups x 20 rounds")
    clients = {
        "B": args.baseline_client.resolve(),
        "C": args.candidate_client.resolve(),
    }
    expected = {"B": BASELINE_SHA256, "C": CANDIDATE_SHA256}
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
    manifest = {
        "protocol": "E1 pre-registered ABBA throughput",
        "payload_bytes": args.payload,
        "messages_per_round": args.messages,
        "groups": args.groups,
        "rounds_per_group": args.rounds,
        "sequence": [f"{arm}{group}" for arm, group in SEQUENCE],
        "clients": {
            "B": {"path": str(clients["B"]), "sha256": digest(clients["B"])},
            "C": {"path": str(clients["C"]), "sha256": digest(clients["C"])},
        },
        "started": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "blocks": [],
    }
    manifest_path = output / "experiment.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    for block_idx, (arm, group) in enumerate(SEQUENCE):
        arm_dir = output / ("baseline" if arm == "B" else "candidate")
        command = [
            "taskset", "-c", "0", sys.executable, str(RUNNER),
            "--client", str(clients[arm]),
            "--output", str(arm_dir),
            "--messages", str(args.messages),
            "--payload", str(args.payload),
            "--capacity", str(1 << 20),
            "--groups", str(args.groups),
            "--rounds", str(args.rounds),
            "--interval-seconds", "0",
            "--max-consecutive-low", "0",
            "--start-group", str(group),
            "--end-group", str(group + 1),
        ]
        if group > 0:
            command.append("--allow-existing")
        prefix = f"block-{block_idx:02d}-{arm}{group}"
        started = time.time()
        print(f"[{block_idx + 1}/{len(SEQUENCE)}] {arm}{group} payload={args.payload}",
              flush=True)
        with (logs / f"{prefix}.stdout").open("w") as stdout, \
                (logs / f"{prefix}.stderr").open("w") as stderr:
            result = subprocess.run(command, cwd=ROOT, stdout=stdout, stderr=stderr,
                                    text=True, check=False)
        record = {
            "block": block_idx,
            "arm": arm,
            "group": group,
            "returncode": result.returncode,
            "wall_seconds": time.time() - started,
            "command": command,
            "stdout": str((logs / f"{prefix}.stdout").relative_to(output)),
            "stderr": str((logs / f"{prefix}.stderr").relative_to(output)),
        }
        manifest["blocks"].append(record)
        manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
        if result.returncode:
            raise SystemExit(
                f"block {prefix} failed ({result.returncode}); see {logs / prefix}.stderr"
            )
    manifest["finished"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"Completed E1 throughput payload={args.payload}: {output}", flush=True)


if __name__ == "__main__":
    main()
