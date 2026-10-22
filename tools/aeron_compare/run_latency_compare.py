#!/usr/bin/env python3
"""Sequential, memory-capped latency comparison; launch inside the documented cgroup."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import shutil
import subprocess
import time


ROOT = Path(__file__).resolve().parents[2]


def fields(line):
    return dict(item.split("=", 1) for item in line.split()[1:])


def available_kib():
    return int(next(line.split()[1] for line in Path("/proc/meminfo").read_text().splitlines()
                    if line.startswith("MemAvailable:")))


def cgroup_directory():
    path = next(line.split(":", 2)[2] for line in Path("/proc/self/cgroup").read_text().splitlines()
                if line.startswith("0::"))
    return Path("/sys/fs/cgroup") / path.lstrip("/")


def resource_counters():
    directory = cgroup_directory()
    return {name: (directory / name).read_text().strip()
            for name in ("memory.current", "memory.peak", "memory.events", "cpu.stat")}


def check_resource_limits():
    directory = cgroup_directory()
    limit = (directory / "memory.max").read_text().strip()
    affinity = os.sched_getaffinity(0)
    if (os.environ.get("SALIAS_RESOURCE_CAPPED") != "1" or
            (0 in affinity and not affinity <= {0, 1, 2, 3}) or
            (0 not in affinity and not affinity <= {1, 2, 3}) or limit == "max" or
            int(limit) > 704 * 1024 * 1024 or
            (directory / "memory.swap.max").read_text().strip() != "0"):
        raise RuntimeError(
            "Require CPUs 1-3 (coordinator may use CPU0), MemoryMax<=704M, "
            "MemorySwapMax=0 and SALIAS_RESOURCE_CAPPED=1"
        )


def stop(process):
    if process is None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=3)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait()


def parse_arm_specs(arm_specs, env_specs):
    """把 LABEL=PATH 与 LABEL=KEY=VALUE 规格解析成 (label, path, env) 列表。"""
    environment = {}
    for assignment in env_specs:
        label, separator, variable = assignment.partition("=")
        if not separator or not label or "=" not in variable:
            raise ValueError(f"--salias-arm-env expects LABEL=KEY=VALUE, got {assignment!r}")
        key, _, value = variable.partition("=")
        if not key or any(character.isspace() for character in key):
            raise ValueError(f"invalid environment key in {assignment!r}")
        environment.setdefault(label, {})[key] = value
    arms = []
    labels = set()
    for spec in arm_specs:
        label, separator, path = spec.partition("=")
        if not separator or not label or label in labels:
            raise ValueError(f"--salias-arm expects unique LABEL=PATH, got {spec!r}")
        labels.add(label)
        client = Path(path)
        if not client.is_file():
            raise ValueError(f"--salias-arm client is not a file: {path}")
        arms.append((label, client, environment.get(label, {})))
    unknown = set(environment) - labels
    if unknown:
        raise ValueError(f"--salias-arm-env names unknown arms: {sorted(unknown)}")
    return arms


def arm_process_environment(overrides):
    """Keep an inherited fence switch from silently changing an explicit arm."""
    environment = dict(os.environ)
    if overrides is not None:
        environment.pop("SALIAS_DISABLE_ASYMMETRIC_FENCE", None)
        environment.update(overrides)
    return environment


def run_case(args, case, number, salias_client=None, salias_env=None):
    if available_kib() < 1024 * 1024:
        raise RuntimeError("Less than 1 GiB MemAvailable; refusing the next case")
    prefix = args.output / f"{number:03d}-{case['engine']}-{case['producers']}p-{case['payload']}b-{case['rate']}"
    driver = worker = None
    driver_dir = Path("/dev/shm") / f"salias-latency-{os.getpid()}-{number}"
    name = f"latency-{os.getpid()}-{number}"
    driver_log = None
    before = available_kib()
    lowest = before
    started = time.monotonic()
    common = ["--messages", str(case["rate"] * args.seconds // case["producers"]),
              "--producers", str(case["producers"]), "--consumers", "1",
              "--payload", str(case["payload"]), "--cpu-base", "1", "--cpu-stride", "1",
              "--latency-sample-rate", "16", "--rate", str(case["rate"] // case["producers"]),
              "--latency-origin", args.origin]
    try:
        if case["engine"] == "aeron":
            driver_log = prefix.with_suffix(".driver.log").open("w")
            driver = subprocess.Popen([
                "taskset", "-c", "3", str(args.aeron_build / "binaries/aeronmd"),
                f"-Daeron.dir={driver_dir}", "-Daeron.dir.delete.on.start=true",
                "-Daeron.threading.mode=SHARED", "-Daeron.shared.idle.strategy=backoff",
                "-Daeron.term.buffer.sparse.file=false", "-Daeron.term.buffer.length=1048576",
                "-Daeron.ipc.term.buffer.length=1048576"], stdout=driver_log,
                stderr=subprocess.STDOUT, start_new_session=True)
            deadline = time.monotonic() + 10
            while not (driver_dir / "cnc.dat").exists():
                if driver.poll() is not None or time.monotonic() > deadline:
                    raise RuntimeError(f"Aeron driver did not start (exit={driver.poll()}): {prefix}")
                lowest = min(lowest, available_kib())
                if lowest < 600 * 1024:
                    raise RuntimeError("Available memory fell below 600 MiB during driver startup")
                time.sleep(0.02)
            command = [str(args.aeron_client), "--dir", str(driver_dir), "--scenario", "mpsc",
                       "--term-length", "1048576", "--fragment-limit", "64", *common]
        else:
            client = salias_client or args.salias_client
            command = [str(client), "--scenario", case["engine"], "--name", name,
                       "--page", "normal", "--capacity", "1048576", "--batch-size", "1",
                       "--poll-limit", "64", *common]
        with prefix.with_suffix(".stdout").open("w") as stdout, prefix.with_suffix(".stderr").open("w") as stderr:
            environment = arm_process_environment(salias_env)
            worker = subprocess.Popen(command, stdout=stdout, stderr=stderr,
                                     env=environment, start_new_session=True)
            deadline = time.monotonic() + args.seconds + 20
            while worker.poll() is None:
                lowest = min(lowest, available_kib())
                if lowest < 600 * 1024 or time.monotonic() > deadline:
                    raise RuntimeError(f"Resource floor or timeout reached: {prefix}")
                time.sleep(0.1)
            if worker.returncode:
                raise RuntimeError(f"Benchmark failed ({worker.returncode}): {prefix}")
        output = prefix.with_suffix(".stdout").read_text().splitlines()
        results = [fields(line) for line in output if line.startswith("RESULT ")]
        if len(results) != 1:
            raise RuntimeError(f"Expected exactly one RESULT: {prefix}")
        result = results[0]
        consumers = [fields(line) for line in output if line.startswith("LATENCY ")]
        expected = case["rate"] * args.seconds
        samples = case["producers"] * ((expected // case["producers"] + 15) // 16)
        if int(result["published"]) != expected or int(result["delivered"]) != expected:
            raise RuntimeError(f"Delivery count mismatch: {prefix}")
        if int(result["latency_samples"]) != samples or len(consumers) != 1:
            raise RuntimeError(f"Sample count mismatch: {prefix}")
        record = {**case, "origin": args.origin, "duration_s": args.seconds,
                  "result": result, "consumers": consumers, "command": command,
                  "client_sha256": hashlib.sha256(Path(client).read_bytes()).hexdigest()
                  if case["engine"] != "aeron" else None,
                  "arm_environment": dict(sorted((salias_env or {}).items())),
                  "stdout": prefix.with_suffix(".stdout").name,
                  "stdout_sha256": hashlib.sha256(prefix.with_suffix(".stdout").read_bytes()).hexdigest(),
                  "stderr": prefix.with_suffix(".stderr").name,
                  "stderr_sha256": hashlib.sha256(prefix.with_suffix(".stderr").read_bytes()).hexdigest(),
                  "min_mem_available_kib": lowest, "wall_seconds": time.monotonic() - started,
                  "resources": resource_counters()}
        with (args.output / "results.jsonl").open("a") as stream:
            stream.write(json.dumps(record) + "\n")
        print(f"case={number} phase={case['phase']} round={case['round']} {case['engine']} "
              f"{case['producers']}P1S {case['payload']}B {case['rate']}/s "
              f"p50={int(result['latency_p50_ns'])/1000:.3f}us "
              f"p99={int(result['latency_p99_ns'])/1000:.3f}us "
              f"p999={int(result['latency_p999_ns'])/1000:.3f}us "
              f"mem_available={lowest//1024}MiB", flush=True)
    finally:
        stop(worker)
        stop(driver)
        if driver_log:
            driver_log.close()
        # Clean only this run's exact shared-memory names, never other benchmark data.
        if driver_dir.exists():
            shutil.rmtree(driver_dir)
        for path in Path("/dev/shm").glob(f"salias-{name}-*"):
            path.unlink(missing_ok=True)
    time.sleep(args.cooldown)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--aeron-build", type=Path, default=None)
    parser.add_argument("--aeron-client", type=Path, default=None)
    parser.add_argument("--salias-client", type=Path, default=ROOT / "build/release/bench/salias_ipc_compare")
    parser.add_argument("--alternate-salias-client", type=Path, default=None,
                        help="interleave this client with --salias-client on successive measured rounds")
    parser.add_argument("--salias-arm", action="append", default=[], metavar="LABEL=PATH",
                        help="explicit salias arm (repeatable); replaces the B/C shorthand")
    parser.add_argument("--salias-arm-env", action="append", default=[],
                        metavar="LABEL=KEY=VALUE",
                        help="environment variable applied only to one arm (repeatable)")
    parser.add_argument("--seconds", type=int, default=2)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--rates", type=int, nargs="+", default=[100000, 1000000])
    parser.add_argument("--payloads", type=int, nargs="+", default=[64, 256])
    parser.add_argument("--producers", type=int, nargs="+", default=[1, 2])
    parser.add_argument("--engines", nargs="+", choices=["fifo", "ordered", "aeron"], default=["fifo", "aeron", "ordered"])
    parser.add_argument("--origin", choices=["claim", "attempt", "planned"], default="planned")
    parser.add_argument("--cooldown", type=float, default=0.5)
    args = parser.parse_args()
    if (not 1 <= args.seconds <= 10 or not 1 <= args.rounds <= 10 or
            any(p not in (1, 2) for p in args.producers) or
            any(r < 2000 or r > 1000000 or r % 2000 for r in args.rates) or
            any(p < 16 or p > 1024 for p in args.payloads) or args.cooldown < 0.25):
        parser.error("unsafe or unsupported matrix parameters")
    check_resource_limits()
    if "aeron" in args.engines and (args.aeron_build is None or args.aeron_client is None):
        parser.error("--aeron-build and --aeron-client are required for Aeron cases")
    if args.aeron_build is not None and not (args.aeron_build / "binaries/aeronmd").is_file():
        parser.error("--aeron-build must contain binaries/aeronmd")
    if args.aeron_client is not None and not args.aeron_client.is_file():
        parser.error("--aeron-client must be a file")
    args.output.mkdir(parents=True, exist_ok=True)
    if (args.output / "results.jsonl").exists():
        parser.error("output already contains results; choose a new output directory")
    if (args.alternate_salias_client is not None and
            not args.alternate_salias_client.is_file()):
        parser.error("--alternate-salias-client must be an executable file")
    if args.salias_arm_env and not args.salias_arm:
        parser.error("--salias-arm-env requires --salias-arm")
    arms = []
    if args.salias_arm:
        if args.alternate_salias_client is not None:
            parser.error("--salias-arm replaces --alternate-salias-client")
        try:
            arms = parse_arm_specs(args.salias_arm, args.salias_arm_env)
        except ValueError as error:
            parser.error(str(error))
    elif args.alternate_salias_client is not None:
        arms = [("B", args.salias_client, {}),
                ("C", args.alternate_salias_client, {})]
    if arms:
        (args.output / "arms.json").write_text(json.dumps([
            {"arm": label, "client": str(Path(client).resolve()),
             "client_sha256": hashlib.sha256(Path(client).read_bytes()).hexdigest(),
             "environment": dict(sorted(environment.items()))}
            for label, client, environment in arms
        ], indent=2) + "\n")
    number = 0
    for producers in args.producers:
        for payload in args.payloads:
            for rate in args.rates:
                for round_number in range(args.rounds + 1):
                    engines = args.engines[round_number % len(args.engines):] + args.engines[:round_number % len(args.engines)]
                    for engine in engines:
                        if engine != "aeron" and arms:
                            order = arms
                            if round_number % len(order):
                                shift = round_number % len(order)
                                order = order[shift:] + order[:shift]
                            for arm, salias_client, arm_env in order:
                                number += 1
                                run_case(args, {
                                    "producers": producers, "payload": payload,
                                    "rate": rate, "engine": engine,
                                    "phase": "warmup" if round_number == 0 else "measured",
                                    "round": round_number, "arm": arm,
                                }, number, salias_client, arm_env)
                        else:
                            number += 1
                            run_case(args, {
                                "producers": producers, "payload": payload,
                                "rate": rate, "engine": engine,
                                "phase": "warmup" if round_number == 0 else "measured",
                                "round": round_number,
                            }, number, args.salias_client)


if __name__ == "__main__":
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f"Interrupted by signal {signum}")

    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGHUP, interrupted)
    main()
