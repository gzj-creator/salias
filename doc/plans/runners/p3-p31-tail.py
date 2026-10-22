#!/usr/bin/env python3
"""Run the P3-1 HFT A/S experiment serially on one frozen candidate binary."""

import argparse
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "tools/hft_acceptance"))
from compare_p31_tail import ARMS, CONFIGURATIONS, ORDER, PROTOCOL  # noqa: E402


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def command(client, name, seconds):
    subscribers, payload, pattern = name.split("-")
    return ["timeout", "--signal=TERM", "--kill-after=10s", f"{seconds + 40}s", str(client),
            "--subscribers", subscribers[2], "--payload", payload[:-1],
            "--seconds", str(seconds), "--pattern", pattern, "--rate", "1000000",
            "--peak-rate", "4000000", "--burst-ms", "100", "--cpu-base", "0"]


def save(path, data):
    path.write_text(json.dumps(data, indent=2) + "\n")


def run(args):
    args.output.mkdir(parents=True, exist_ok=False)
    frozen = [Path(__file__).resolve(), ROOT / "tools/hft_acceptance/compare_p31_tail.py",
              ROOT / "tools/hft_acceptance/summarize.py",
              ROOT / "tools/hft_acceptance/summarize_tail_ab.py",
              ROOT / "tools/hft_acceptance/acceptance.cpp",
              *sorted((ROOT / "src").rglob("*.hpp")),
              *sorted((ROOT / "src").rglob("*.cpp")),
              ROOT / "build/release/src/salias/libsalias.a",
              ROOT / "build/release/src/core/libsalias_core.a"]
    manifest = {
        "protocol": PROTOCOL, "started_at": datetime.now().astimezone().isoformat(),
        "client": str(args.client), "client_sha256": digest(args.client),
        "inputs": {str(path): digest(path) for path in frozen}, "runs": [], "warmup": [],
    }
    # Persist the complete rule before the first warm-up or measurement.
    save(args.output / "experiment.json", manifest)
    environment_capture = subprocess.run(
        ["bash", "tools/hft_acceptance/capture_environment.sh"], cwd=ROOT,
        text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False)
    (args.output / "environment.txt").write_text(environment_capture.stdout)
    if environment_capture.returncode:
        raise RuntimeError(f"environment capture failed: exit={environment_capture.returncode}")
    for group in range(4):
        if group:
            (args.output / f"group-{group}.proc-stat.before").write_text(Path("/proc/stat").read_text())
        for name in CONFIGURATIONS:
            for round_number, arm in enumerate(ORDER if group else ("S", "A")):
                relative = (f"group-{group}/round-{round_number}/{arm}/{name}" if group else
                            f"warmup/{arm}/{name}")
                raw = args.output / f"{relative}.jsonl"
                stderr = args.output / f"{relative}.stderr.txt"
                raw.parent.mkdir(parents=True, exist_ok=True)
                env = dict(os.environ)
                env.pop("SALIAS_DISABLE_ASYMMETRIC_FENCE", None)
                env.update(ARMS[arm])
                cmd = command(args.client, name, 30 if group else 2)
                print(f"{datetime.now().astimezone().isoformat()} group={group} "
                      f"configuration={name} round={round_number} arm={arm}", flush=True)
                with raw.open("w") as stdout, stderr.open("w") as errors:
                    result = subprocess.run(cmd, cwd=ROOT, env=env, stdout=stdout, stderr=errors,
                                            timeout=85 if group else 55, check=False)
                record = {"group": group, "configuration": name, "round": round_number,
                          "arm": arm, "command": cmd, "environment": ARMS[arm],
                          "client_sha256": manifest["client_sha256"],
                          "returncode": result.returncode,
                          "raw": str(raw.relative_to(args.output)), "raw_sha256": digest(raw),
                          "stderr": str(stderr.relative_to(args.output)),
                          "stderr_sha256": digest(stderr)}
                manifest["runs" if group else "warmup"].append(record)
                save(args.output / "experiment.json", manifest)
                if result.returncode:
                    raise RuntimeError(f"HFT worker failure retained: {relative}, exit={result.returncode}")
        if group:
            (args.output / f"group-{group}.proc-stat.after").write_text(Path("/proc/stat").read_text())
    if any(digest(path) != sha for path, sha in manifest["inputs"].items()):
        raise RuntimeError("frozen input changed during HFT measurement")
    manifest["completed_at"] = datetime.now().astimezone().isoformat()
    save(args.output / "experiment.json", manifest)
    subprocess.run([sys.executable, str(ROOT / "tools/hft_acceptance/compare_p31_tail.py"),
                    str(args.output)], cwd=ROOT, check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.client = args.client.resolve(strict=True)
    args.output = args.output.resolve()
    run(args)


if __name__ == "__main__":
    main()
