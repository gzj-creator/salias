#!/usr/bin/env python3
"""Run the pre-registered claim API v4 throughput and latency comparison.

Arms:
  B  v3.0.0 claim implementation
  S  v4 with SALIAS_DISABLE_ASYMMETRIC_FENCE=1
  A  v4 with the default asymmetric claim fence

The S-versus-B comparison isolates the claim API change. A-versus-S isolates
the fence change, while A-versus-B reports the combined product effect.
"""

import argparse
import hashlib
import json
from pathlib import Path
import shlex
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parents[3]
THROUGHPUT_RUNNER = ROOT / "doc/plans/runners/high-frequency-stability-runner.py"
LATENCY_RUNNER = ROOT / "tools/aeron_compare/run_latency_compare.py"
ANALYZER = ROOT / "tools/aeron_compare/analyze_claim_v4_ab.py"
PROTOCOL = "p3-claim-api-v4-product-ab-v1"
CLIENT_PATH = "tools/aeron_compare/salias_ipc_compare.cpp"
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
ARM_ENVIRONMENT = {
    "B": [],
    "S": ["SALIAS_DISABLE_ASYMMETRIC_FENCE=1"],
    "A": [],
}
PAYLOADS = (64, 256)
GROUPS = 6
ROUNDS = 20
MESSAGES = 50_000_000
CAPACITY = 1 << 20
LATENCY_ROUNDS = 10
LATENCY_SECONDS = 2
LATENCY_RATE = 1_000_000
LATENCY_ORIGINS = ("attempt", "planned")


def digest(path):
    """Return a file SHA-256 digest."""
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def block_sequence(groups):
    """Return the three-arm rotation, reversing arm order on odd groups."""
    sequence = []
    for group in range(groups):
        order = ARMS if group % 2 == 0 else tuple(reversed(ARMS))
        sequence.append([(arm, group) for arm in order])
    return sequence


def load_manifest(path):
    """Validate the A/B build manifest and return its frozen client paths."""
    path = Path(path).resolve()
    data = json.loads(path.read_text())
    if data.get("protocol") != PROTOCOL:
        raise ValueError(
            f"build protocol is {data.get('protocol')!r}, expected {PROTOCOL!r}"
        )
    # New manifests distinguish the working-tree client template from the
    # pinned v3.0.0 baseline source. The historical manifest used one ambiguous
    # client_source_sha256 field for the pinned source; keep reading it so the
    # completed formal evidence remains reproducible.
    baseline_sha = data.get(
        "baseline_client_source_sha256", data.get("client_source_sha256")
    )
    baseline_identity = data.get(
        "baseline_client_source_identity", data.get("client_source_identity")
    )
    if not isinstance(baseline_sha, str) or len(baseline_sha) != 64:
        raise ValueError("build manifest has no baseline client source SHA-256")
    if baseline_identity != f"git:v3.0.0:{CLIENT_PATH}":
        raise ValueError("build manifest client is not the pinned v3.0.0 benchmark source")
    template_sha = data.get("client_template_sha256")
    if template_sha is not None:
        if data.get("client_template_identity") != CLIENT_PATH:
            raise ValueError("build manifest has no working-tree client template identity")
        if not isinstance(template_sha, str) or len(template_sha) != 64:
            raise ValueError("build manifest has no client template SHA-256")
    variants = data.get("variants", {})
    if set(variants) != {"v3", "v4"}:
        raise ValueError("build manifest must contain exactly v3 and v4 variants")
    clients = {
        "B": Path(variants["v3"]["client"]).resolve(),
        "S": Path(variants["v4"]["client"]).resolve(),
        "A": Path(variants["v4"]["client"]).resolve(),
    }
    for arm, client in clients.items():
        if not client.is_file():
            raise ValueError(f"missing {arm} client: {client}")
        expected = variants["v3" if arm == "B" else "v4"].get("client_sha256")
        actual = digest(client)
        if not expected or actual != expected:
            raise ValueError(f"{arm} client SHA-256 does not match its build manifest")
    if clients["B"] == clients["S"]:
        raise ValueError("baseline and candidate client paths are identical")
    if variants["v3"].get("submission_sites") != {"claim_commit": 2, "publisher_commit": 0}:
        raise ValueError("v3 client does not contain exactly two claim.commit() sites")
    if variants["v4"].get("submission_sites") != {"claim_commit": 0, "publisher_commit": 2}:
        raise ValueError("v4 client does not contain exactly two publisher.commit() sites")
    if variants["v3"].get("client_source_sha256") == variants["v4"].get("client_source_sha256"):
        raise ValueError("v3 and v4 client sources are identical")
    data["baseline_client_source_sha256"] = baseline_sha
    data["client_template_sha256"] = template_sha or baseline_sha
    data["clients"] = {arm: str(client) for arm, client in clients.items()}
    return data


def throughput_command(client, output, payload, args, group, setenv):
    """Build one interleaved throughput block command."""
    command = [
        "taskset", "-c", "0", sys.executable, str(THROUGHPUT_RUNNER),
        "--client", str(client),
        "--output", str(output),
        "--messages", str(args.messages),
        "--payload", str(payload),
        "--capacity", str(CAPACITY),
        "--groups", str(args.groups),
        "--rounds", str(args.rounds),
        "--interval-seconds", "0",
        "--max-consecutive-low", "0",
        "--timeout", "120",
        "--start-group", str(group),
        "--end-group", str(group + 1),
    ]
    for assignment in setenv:
        command.extend(["--setenv", assignment])
    if group > 0:
        command.append("--allow-existing")
    return command


def latency_command(client_map, output, payload, origin, args):
    """Build a systemd-wrapped three-arm latency matrix command."""
    command = [
        "systemd-run", "--user", "--wait", "--pipe", "--collect",
        "-p", f"WorkingDirectory={ROOT}",
        "-p", "AllowedCPUs=0-3",
        "-p", "Nice=5",
        "-p", "MemoryHigh=640M",
        "-p", "MemoryMax=704M",
        "-p", "MemorySwapMax=0",
        "-p", "TasksMax=48",
        "-p", "RuntimeMaxSec=1800s",
        "-p", "KillMode=control-group",
        "-p", "OOMPolicy=stop",
        "--setenv=SALIAS_RESOURCE_CAPPED=1",
        sys.executable, str(LATENCY_RUNNER),
        "--output", str(output),
        "--engines", "fifo",
        "--seconds", str(args.latency_seconds),
        "--rounds", str(args.latency_rounds),
        "--rates", str(LATENCY_RATE),
        "--payloads", str(payload),
        "--producers", "1",
        "--origin", origin,
    ]
    for arm in ARMS:
        command.extend(["--salias-arm", f"{arm}={client_map[arm]}"])
    for assignment in ARM_ENVIRONMENT["S"]:
        command.extend(["--salias-arm-env", f"S={assignment}"])
    return command


def write_json(path, value):
    """Write stable, human-readable experiment metadata."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n")


def run_logged(command, log_dir, prefix):
    """Run one block, preserving stdout, stderr and the exact command."""
    log_dir.mkdir(parents=True, exist_ok=True)
    stdout = log_dir / f"{prefix}.stdout"
    stderr = log_dir / f"{prefix}.stderr"
    started = time.time()
    with stdout.open("w") as out, stderr.open("w") as err:
        result = subprocess.run(command, cwd=ROOT, stdout=out, stderr=err,
                                text=True, check=False)
    return {
        "command": shlex.join(command),
        "returncode": result.returncode,
        "wall_seconds": time.time() - started,
        "stdout": str(stdout),
        "stderr": str(stderr),
        "stdout_sha256": digest(stdout),
        "stderr_sha256": digest(stderr),
    }


def run_throughput(manifest, output, args, state):
    """Run both payload matrices and checkpoint after every block."""
    sequence = block_sequence(args.groups)
    total_blocks = len(sequence) * len(args.payloads)
    completed = 0
    for payload in args.payloads:
        payload_root = output / f"throughput-{payload}b"
        for block in sequence:
            for arm, group in block:
                client = manifest["clients"][arm]
                arm_output = payload_root / ARM_DIRECTORIES[arm]
                command = throughput_command(
                    client, arm_output, payload, args, group,
                    ARM_ENVIRONMENT[arm],
                )
                completed += 1
                prefix = f"payload-{payload:03d}-block-{completed:03d}-{arm}{group}"
                print(f"[throughput {completed}/{total_blocks}] {payload}B {arm}{group}",
                      flush=True)
                record = run_logged(command, output / "orchestration-logs", prefix)
                record.update({
                    "kind": "throughput",
                    "payload": payload,
                    "arm": arm,
                    "group": group,
                })
                state["blocks"].append(record)
                write_json(output / "experiment.json", state)
                if record["returncode"]:
                    raise RuntimeError(
                        f"throughput block {prefix} failed; see {record['stderr']}"
                    )


def run_latency(manifest, output, args, state):
    """Run attempt/planned latency matrices for both payload sizes."""
    cases = [(payload, origin) for payload in args.payloads
             for origin in args.origins]
    for index, (payload, origin) in enumerate(cases, 1):
        case_output = output / "latency" / f"{payload}b-{origin}"
        command = latency_command(manifest["clients"], case_output, payload,
                                  origin, args)
        prefix = f"latency-{payload:03d}-{origin}"
        print(f"[latency {index}/{len(cases)}] {payload}B {origin}", flush=True)
        record = run_logged(command, output / "orchestration-logs", prefix)
        record.update({
            "kind": "latency",
            "payload": payload,
            "origin": origin,
        })
        state["blocks"].append(record)
        write_json(output / "experiment.json", state)
        if record["returncode"]:
            raise RuntimeError(
                f"latency case {prefix} failed; see {record['stderr']}"
            )


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--payloads", type=int, nargs="+", default=list(PAYLOADS))
    parser.add_argument("--origins", nargs="+", choices=LATENCY_ORIGINS,
                        default=list(LATENCY_ORIGINS))
    parser.add_argument("--skip-throughput", action="store_true")
    parser.add_argument("--skip-latency", action="store_true")
    parser.add_argument("--smoke", action="store_true",
                        help="use a small non-acceptance matrix")
    parser.add_argument("--groups", type=int, default=GROUPS, help=argparse.SUPPRESS)
    parser.add_argument("--rounds", type=int, default=ROUNDS, help=argparse.SUPPRESS)
    parser.add_argument("--messages", type=int, default=MESSAGES, help=argparse.SUPPRESS)
    parser.add_argument("--latency-rounds", type=int, default=LATENCY_ROUNDS,
                        help=argparse.SUPPRESS)
    parser.add_argument("--latency-seconds", type=int, default=LATENCY_SECONDS,
                        help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.skip_throughput and args.skip_latency:
        parser.error("at least one of throughput or latency must run")
    if any(payload not in PAYLOADS for payload in args.payloads):
        parser.error("payloads must be 64 and/or 256 bytes")
    if not args.origins:
        parser.error("at least one latency origin is required")
    if args.smoke:
        if not (1 <= args.groups <= GROUPS and 1 <= args.rounds <= ROUNDS and
                1 <= args.messages <= MESSAGES and 1 <= args.latency_rounds <= LATENCY_ROUNDS
                and 1 <= args.latency_seconds <= 2):
            parser.error("smoke matrix parameters are out of range")
    elif (args.groups != GROUPS or args.rounds != ROUNDS or args.messages != MESSAGES or
          args.latency_rounds != LATENCY_ROUNDS or args.latency_seconds != LATENCY_SECONDS):
        parser.error("formal claim API v4 matrix is frozen at 6 groups x 20 rounds, "
                     "50M messages, 10 latency rounds x 2 seconds")
    if not args.skip_latency and not args.smoke and args.origins != list(LATENCY_ORIGINS):
        parser.error("formal latency matrix requires attempt and planned origins")
    args.output = args.output.resolve()
    if args.output.exists() and any(args.output.iterdir()):
        parser.error(f"output directory is not empty: {args.output}")
    return args


def main():
    args = parse_args()
    try:
        manifest = load_manifest(args.build_manifest)
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as error:
        raise SystemExit(f"invalid build manifest: {error}")
    args.output.mkdir(parents=True, exist_ok=True)
    state = {
        "protocol": "p3-claim-api-v4-formal-ab-v1",
        "mode": "smoke" if args.smoke else "formal",
        "started": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "build_manifest": str(args.build_manifest.resolve()),
        "build_manifest_sha256": digest(args.build_manifest),
        "client_template_sha256": manifest["client_template_sha256"],
        "baseline_client_source_sha256": manifest["baseline_client_source_sha256"],
        "clients": {
            arm: {"path": manifest["clients"][arm], "sha256": digest(manifest["clients"][arm]),
                  "environment": ARM_ENVIRONMENT[arm]}
            for arm in ARMS
        },
        "throughput": {
            "payloads": args.payloads,
            "groups": args.groups,
            "rounds_per_group": args.rounds,
            "messages_per_round": args.messages,
            "capacity_bytes": CAPACITY,
            "sequence": [[f"{arm}{group}" for arm, group in block]
                         for block in block_sequence(args.groups)],
        },
        "latency": {
            "payloads": args.payloads,
            "origins": args.origins,
            "rounds": args.latency_rounds,
            "seconds": args.latency_seconds,
            "rate_per_producer": LATENCY_RATE,
        },
        "blocks": [],
    }
    write_json(args.output / "experiment.json", state)
    if not args.skip_throughput:
        run_throughput(manifest, args.output, args, state)
    if not args.skip_latency:
        run_latency(manifest, args.output, args, state)
    state["finished"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    write_json(args.output / "experiment.json", state)
    if args.smoke:
        print(f"Completed smoke matrix: {args.output}", flush=True)
        return
    analysis_command = [
        sys.executable, str(ANALYZER), "--experiment", str(args.output),
        "--manifest", str(args.build_manifest),
    ]
    analysis = subprocess.run(analysis_command, cwd=ROOT, check=False)
    if analysis.returncode:
        raise SystemExit(f"analysis failed ({analysis.returncode})")
    print(f"Completed formal claim API v4 A/B: {args.output}", flush=True)


if __name__ == "__main__":
    main()
