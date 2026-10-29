#!/usr/bin/env python3
"""Run a paired Salias/Aeron SPSC, MPSC, SPMC, and MPMC comparison."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import signal
import statistics
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools/aeron_compare"))
from run_latency_compare import (  # noqa: E402
    available_kib,
    cgroup_directory,
    fields,
    resource_counters,
    stop,
)


TOPOLOGIES = {
    "spsc": {"producers": 1, "consumers": 1},
    "mpsc": {"producers": 2, "consumers": 1},
    "spmc": {"producers": 1, "consumers": 2},
    "mpmc": {"producers": 2, "consumers": 2},
}
SALIAS_SCENARIO = "fifo"
LATENCY_PERCENTILES = ("p50", "p99", "p999", "max")


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def interrupted(signum, frame):
    raise KeyboardInterrupt(f"Interrupted by signal {signum}")


def verify_limits():
    directory = cgroup_directory()
    limit = (directory / "memory.max").read_text().strip()
    affinity = os.sched_getaffinity(0)
    if (os.environ.get("SALIAS_RESOURCE_CAPPED") != "1" or affinity != {0, 1, 2, 3} or
            limit == "max" or int(limit) > 704 * 1024 * 1024 or
            (directory / "memory.swap.max").read_text().strip() != "0"):
        raise RuntimeError(
            "Require CPUs 0-3, MemoryMax<=704M, MemorySwapMax=0, and "
            "SALIAS_RESOURCE_CAPPED=1"
        )


def cpu_layout(producers, consumers):
    worker_count = producers + consumers
    if worker_count <= 3:
        return {
            "worker_cpu_base": 1,
            "driver_cpu": 0,
            "coordinator_cpu": 0,
            "layout": "workers on CPUs 1-3; coordinator and Aeron driver on CPU0",
        }
    return {
        "worker_cpu_base": 0,
        "driver_cpu": 0,
        "coordinator_cpu": 0,
        "layout": (
            "workers on CPUs 0-3; coordinator and Aeron driver share CPU0 "
            "with subscriber0"
        ),
    }


def worker_roles(producers, consumers, cpu_base):
    roles = []
    for consumer in range(consumers):
        roles.append({"role": "subscriber", "index": consumer, "cpu": cpu_base + consumer})
    for producer in range(producers):
        roles.append({
            "role": "publisher",
            "index": producer,
            "cpu": cpu_base + consumers + producer,
        })
    return roles


def expected_counts(kind, producers, consumers, messages, sample_rate):
    published = producers * messages
    delivered = published * consumers
    samples_per_producer = (
        (messages + sample_rate - 1) // sample_rate if kind == "latency" else 0
    )
    return published, delivered, producers * samples_per_producer * consumers


def run_case(args, case, number, output):
    if available_kib() < 1024 * 1024:
        raise RuntimeError("Less than 1 GiB MemAvailable; refusing the next case")

    producers = case["producers"]
    consumers = case["consumers"]
    layout = cpu_layout(producers, consumers)
    name = f"topology-{os.getpid()}-{number}"
    driver_dir = Path("/dev/shm") / name
    prefix = output / (
        f"{number:03d}-{case['engine']}-{case['topology']}-{case['kind']}-"
        f"{case['payload']}b-{case['origin']}-r{case['round']}"
    )
    if case["kind"] == "latency":
        messages_per_producer = args.latency_total_rate * args.latency_seconds // producers
        rate_per_producer = args.latency_total_rate // producers
        sample_rate = args.latency_sample_rate
    else:
        messages_per_producer = args.throughput_messages_per_producer
        rate_per_producer = 0
        sample_rate = 0

    common = [
        "--messages", str(messages_per_producer),
        "--producers", str(producers),
        "--consumers", str(consumers),
        "--payload", str(case["payload"]),
        "--cpu-base", str(layout["worker_cpu_base"]),
        "--cpu-stride", "1",
        "--rate", str(rate_per_producer),
        "--latency-origin", case["origin"],
        "--latency-sample-rate", str(sample_rate),
    ]
    driver = worker = driver_log = None
    driver_log_path = None
    driver_command = None
    lowest = available_kib()
    resources_before = resource_counters()
    started = time.monotonic()
    timeout_s = args.latency_seconds + 40 if case["kind"] == "latency" else 90
    try:
        if case["engine"] == "aeron":
            driver_command = [
                "taskset", "-c", str(layout["driver_cpu"]), str(args.aeron_driver),
                f"-Daeron.dir={driver_dir}",
                "-Daeron.dir.delete.on.start=true",
                "-Daeron.threading.mode=SHARED",
                "-Daeron.shared.idle.strategy=backoff",
                "-Daeron.term.buffer.sparse.file=false",
                "-Daeron.term.buffer.length=1048576",
                "-Daeron.ipc.term.buffer.length=1048576",
            ]
            driver_log_path = prefix.with_suffix(".driver.log")
            driver_log = driver_log_path.open("w")
            driver = subprocess.Popen(
                driver_command,
                stdout=driver_log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            deadline = time.monotonic() + 15
            while not (driver_dir / "cnc.dat").exists():
                lowest = min(lowest, available_kib())
                if driver.poll() is not None or time.monotonic() > deadline:
                    raise RuntimeError(f"Aeron driver startup failed: {prefix}")
                if lowest < 600 * 1024:
                    raise RuntimeError("MemAvailable fell below 600 MiB during driver startup")
                time.sleep(0.02)
            command = [
                str(args.aeron_client),
                "--dir", str(driver_dir),
                "--scenario", case["topology"],
                "--term-length", "1048576",
                "--fragment-limit", "64",
                *common,
            ]
        else:
            command = [
                str(args.salias_client),
                "--scenario", SALIAS_SCENARIO,
                "--name", name,
                "--page", "normal",
                "--capacity", "1048576",
                "--batch-size", "1",
                "--poll-limit", "64",
                *common,
            ]

        with prefix.with_suffix(".stdout").open("w") as stdout, \
                prefix.with_suffix(".stderr").open("w") as stderr:
            worker = subprocess.Popen(
                command,
                stdout=stdout,
                stderr=stderr,
                start_new_session=True,
            )
            deadline = time.monotonic() + timeout_s
            while worker.poll() is None:
                lowest = min(lowest, available_kib())
                if lowest < 600 * 1024:
                    raise RuntimeError(f"MemAvailable fell below 600 MiB: {prefix}")
                if time.monotonic() > deadline:
                    raise RuntimeError(f"Benchmark timeout: {prefix}")
                if driver is not None and driver.poll() is not None:
                    raise RuntimeError(f"Aeron driver exited during case: {prefix}")
                time.sleep(0.05)
            if worker.returncode:
                raise RuntimeError(f"Benchmark failed ({worker.returncode}): {prefix}")

        # Flush the driver log before hashing it; the driver writes during shutdown.
        stop(driver)
        driver = None
        if driver_log:
            driver_log.close()
            driver_log = None

        raw_lines = prefix.with_suffix(".stdout").read_text().splitlines()
        results = [fields(line) for line in raw_lines if line.startswith("RESULT ")]
        consumers_detail = sorted(
            [fields(line) for line in raw_lines if line.startswith("LATENCY ")],
            key=lambda item: int(item["consumer"]),
        )
        if len(results) != 1:
            raise RuntimeError(f"Expected one RESULT line: {prefix}")
        result = results[0]
        expected_published, expected_delivered, expected_samples = expected_counts(
            case["kind"], producers, consumers, messages_per_producer, sample_rate
        )
        if int(result["published"]) != expected_published:
            raise RuntimeError(f"Published count mismatch: {prefix}")
        if int(result["delivered"]) != expected_delivered:
            raise RuntimeError(f"Delivered count mismatch: {prefix}")
        if int(result["latency_samples"]) != expected_samples:
            raise RuntimeError(f"Latency sample count mismatch: {prefix}")
        if [int(item["consumer"]) for item in consumers_detail] != list(range(consumers)):
            raise RuntimeError(f"Subscriber result mismatch: {prefix}")
        if int(result["producers"]) != producers or int(result["consumers"]) != consumers:
            raise RuntimeError(f"Topology result mismatch: {prefix}")
        if int(result["rate_per_producer"]) != rate_per_producer:
            raise RuntimeError(f"Rate mismatch: {prefix}")
        if result["latency_origin"] != case["origin"]:
            raise RuntimeError(f"Latency origin mismatch: {prefix}")

        resources_after = resource_counters()
        events = dict(line.split() for line in resources_after["memory.events"].splitlines())
        if any(int(events[key]) != 0 for key in ("max", "oom", "oom_kill")):
            raise RuntimeError(f"Memory max/OOM event observed: {prefix}")

        worst = {
            key: max(int(item[key + "_ns"]) for item in consumers_detail)
            for key in LATENCY_PERCENTILES
        }
        record = {
            "number": number,
            "phase": "warmup" if case["round"] == 0 else "measured",
            "round": case["round"],
            "engine": case["engine"],
            "topology": case["topology"],
            "kind": case["kind"],
            "payload": case["payload"],
            "origin": case["origin"],
            "producers": producers,
            "consumers": consumers,
            "messages_per_producer": messages_per_producer,
            "rate_per_producer": rate_per_producer,
            "latency_sample_rate": sample_rate,
            "cpu_layout": {**layout, "roles": worker_roles(producers, consumers,
                                                            layout["worker_cpu_base"])},
            "command": command,
            "driver_command": driver_command,
            "result": result,
            "consumers_detail": consumers_detail,
            "worst_subscriber_ns": worst,
            "stdout": prefix.with_suffix(".stdout").name,
            "stdout_sha256": digest(prefix.with_suffix(".stdout")),
            "stderr": prefix.with_suffix(".stderr").name,
            "stderr_sha256": digest(prefix.with_suffix(".stderr")),
            "driver_log": driver_log_path.name if driver_log_path else None,
            "driver_log_sha256": digest(driver_log_path) if driver_log_path else None,
            "min_mem_available_kib": lowest,
            "wall_seconds": time.monotonic() - started,
            "resources_before": resources_before,
            "resources_after": resources_after,
        }
        with (output / "results.jsonl").open("a") as stream:
            stream.write(json.dumps(record) + "\n")
        if case["kind"] == "latency":
            print(
                f"case={number:03d} {case['engine']:5s} {case['topology']} "
                f"{case['kind']} {case['payload']}B {case['origin']} "
                f"p50={int(result['latency_p50_ns'])/1000:.3f}us "
                f"p99={int(result['latency_p99_ns'])/1000:.3f}us "
                f"p999={int(result['latency_p999_ns'])/1000:.3f}us",
                flush=True,
            )
        else:
            print(
                f"case={number:03d} {case['engine']:5s} {case['topology']} "
                f"throughput {case['payload']}B "
                f"publish={float(result['publish_msg_per_sec'])/1e6:.3f}Mmsg/s",
                flush=True,
            )
        return record
    finally:
        stop(worker)
        stop(driver)
        if driver_log:
            driver_log.close()
        if driver_dir.exists():
            shutil.rmtree(driver_dir)
        for path in Path("/dev/shm").glob(f"salias-{name}-*"):
            path.unlink(missing_ok=True)
        time.sleep(args.cooldown)


def median_range(values):
    return {"median": statistics.median(values), "min": min(values), "max": max(values)}


def summarize(rows, environment):
    measured = [row for row in rows if row["phase"] == "measured"]
    groups = []
    group_keys = sorted({
        (row["topology"], row["kind"], row["payload"], row["origin"], row["engine"])
        for row in measured
    })
    for topology, kind, payload, origin, engine in group_keys:
        cell = [
            row for row in measured
            if (row["topology"], row["kind"], row["payload"], row["origin"], row["engine"]) ==
            (topology, kind, payload, origin, engine)
        ]
        group = {
            "topology": topology,
            "kind": kind,
            "payload": payload,
            "origin": origin,
            "engine": engine,
            "rounds": len(cell),
            "publish_mmsg_s": median_range(
                [float(row["result"]["publish_msg_per_sec"]) / 1e6 for row in cell]
            ),
            "delivery_mmsg_s": median_range(
                [float(row["result"]["delivery_msg_per_sec"]) / 1e6 for row in cell]
            ),
            "duration_s": median_range([float(row["result"]["seconds"]) for row in cell]),
        }
        if kind == "latency":
            group["latency_ns"] = {
                key: median_range([row["worst_subscriber_ns"][key] for row in cell])
                for key in LATENCY_PERCENTILES
            }
            group["per_subscriber_median_ns"] = [
                {
                    key: statistics.median(
                        int(row["consumers_detail"][index][key + "_ns"]) for row in cell
                    )
                    for key in LATENCY_PERCENTILES
                }
                for index in range(len(cell[0]["consumers_detail"]))
            ]
        groups.append(group)

    paired = []
    for topology, kind, payload, origin in sorted({
        (group["topology"], group["kind"], group["payload"], group["origin"])
        for group in groups
    }):
        pair = {
            group["engine"]: group for group in groups
            if (group["topology"], group["kind"], group["payload"], group["origin"]) ==
            (topology, kind, payload, origin)
        }
        if set(pair) != {"salias", "aeron"}:
            continue
        comparison = {
            "topology": topology,
            "kind": kind,
            "payload": payload,
            "origin": origin,
            "publish_mmsg_s": {
                "salias": pair["salias"]["publish_mmsg_s"],
                "aeron": pair["aeron"]["publish_mmsg_s"],
                "salias_over_aeron": (
                    pair["salias"]["publish_mmsg_s"]["median"] /
                    pair["aeron"]["publish_mmsg_s"]["median"]
                ),
            },
        }
        if kind == "latency":
            comparison["latency_ns"] = {
                key: {
                    "salias": pair["salias"]["latency_ns"][key],
                    "aeron": pair["aeron"]["latency_ns"][key],
                    "salias_minus_aeron_median": (
                        pair["salias"]["latency_ns"][key]["median"] -
                        pair["aeron"]["latency_ns"][key]["median"]
                    ),
                }
                for key in LATENCY_PERCENTILES
            }
        paired.append(comparison)

    return {
        "environment": environment,
        "groups": groups,
        "paired": paired,
        "measured_runs": len(measured),
        "warmup_runs": len(rows) - len(measured),
        "measured_published": sum(int(row["result"]["published"]) for row in measured),
        "measured_delivered": sum(int(row["result"]["delivered"]) for row in measured),
        "measured_latency_samples": sum(
            int(row["result"]["latency_samples"]) for row in measured
        ),
        "min_mem_available_kib": min(row["min_mem_available_kib"] for row in rows),
    }


def write_environment(args, output):
    directory = cgroup_directory()
    files = [
        args.salias_client,
        args.aeron_client,
        args.aeron_driver,
        Path(__file__),
        ROOT / "third_party/aeron-1.52.0.tar.gz",
    ]
    environment = {
        "date": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
        "tag": subprocess.check_output(
            ["git", "describe", "--tags", "--always", "--dirty"], cwd=ROOT, text=True
        ).strip(),
        "hostname": platform.node(),
        "kernel": platform.release(),
        "machine": platform.machine(),
        "cpu_model": next(
            line.split(":", 1)[1].strip()
            for line in Path("/proc/cpuinfo").read_text().splitlines()
            if line.startswith("model name")
        ),
        "online_cpus": sorted(os.sched_getaffinity(0)),
        "aeron_version": "1.52.0",
        "workload": {
            "topologies": [name for name in args.topologies],
            "payloads": list(args.payloads),
            "throughput_messages_per_producer": args.throughput_messages_per_producer,
            "latency_total_rate_msg_s": args.latency_total_rate,
            "latency_seconds": args.latency_seconds,
            "latency_origins": list(args.origins),
            "latency_sample_rate": args.latency_sample_rate,
            "warmup_rounds": 1,
            "measured_rounds": args.rounds,
        },
        "semantics": (
            "Salias FIFO per-publisher rings and Aeron exclusive publications; "
            "multiple subscribers receive every published message (fanout), not "
            "competing consumers"
        ),
        "capacity": (
            "Salias 1 MiB per-producer ring versus Aeron 1 MiB term in a multi-term "
            "log; total mapped capacities differ"
        ),
        "aggregation": (
            "Median and range across measured run values; latency uses each run's "
            "worst subscriber, never pooled histograms"
        ),
        "payload_limit": (
            "Logical payloads are 64/256 B; latency writes 16 B and throughput 8 B. "
            "Counts and samples are checked, not full content, sequence, or duplicate logic"
        ),
        "cgroup": {
            "path": str(directory),
            **{
                name: (directory / name).read_text().strip()
                if (directory / name).exists() else None
                for name in ("memory.high", "memory.max", "memory.swap.max", "cpu.max")
            },
        },
        "files": {str(path): digest(path) for path in files},
        "limits_start": resource_counters(),
    }
    (output / "environment.json").write_text(json.dumps(environment, indent=2) + "\n")
    return environment


def validate_args(parser, args):
    if not 1 <= args.rounds <= 10:
        parser.error("--rounds must be in [1, 10]")
    if not 1 <= args.latency_seconds <= 10:
        parser.error("--latency-seconds must be in [1, 10]")
    if not 2000 <= args.latency_total_rate <= 1_000_000:
        parser.error("--latency-total-rate must be in [2000, 1000000]")
    if any(args.latency_total_rate % producers for producers in (1, 2)):
        parser.error("--latency-total-rate must divide evenly across one and two publishers")
    if args.latency_sample_rate < 1 or args.latency_sample_rate > 1024:
        parser.error("--latency-sample-rate must be in [1, 1024]")
    if args.throughput_messages_per_producer < 100_000:
        parser.error("--throughput-messages-per-producer must be at least 100000")
    if any(payload < 16 or payload > 1024 for payload in args.payloads):
        parser.error("payloads must be in [16, 1024]")
    if args.cooldown < 0.25:
        parser.error("--cooldown must be at least 0.25 seconds")
    for path in (args.salias_client, args.aeron_client, args.aeron_driver):
        if not path.is_file():
            parser.error(f"missing executable: {path}")
    if not os.access(args.salias_client, os.X_OK) or not os.access(args.aeron_client, os.X_OK):
        parser.error("comparison clients must be executable")
    if args.output.exists() and any(args.output.iterdir()):
        parser.error("--output must be absent or empty")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--salias-client", type=Path,
        default=ROOT / "build/release/bench/salias_ipc_compare",
    )
    parser.add_argument("--aeron-client", type=Path, required=True)
    parser.add_argument("--aeron-driver", type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--cooldown", type=float, default=0.5)
    parser.add_argument("--topologies", nargs="+", choices=TOPOLOGIES.keys(),
                        default=list(TOPOLOGIES))
    parser.add_argument("--payloads", nargs="+", type=int, default=[64, 256])
    parser.add_argument("--throughput-messages-per-producer", type=int, default=10_000_000)
    parser.add_argument("--latency-total-rate", type=int, default=1_000_000)
    parser.add_argument("--latency-seconds", type=int, default=2)
    parser.add_argument("--latency-sample-rate", type=int, default=16)
    parser.add_argument("--origins", nargs="+", choices=["attempt", "planned"],
                        default=["attempt", "planned"])
    args = parser.parse_args()
    validate_args(parser, args)
    verify_limits()

    args.output.mkdir(parents=True, exist_ok=True)
    environment = write_environment(args, args.output)
    rows = []
    number = 0
    cells = []
    for topology in args.topologies:
        for payload in args.payloads:
            for origin in args.origins:
                cells.append((topology, "latency", payload, origin))
    for topology in args.topologies:
        for payload in args.payloads:
            cells.append((topology, "throughput", payload, "claim"))

    for round_number in range(args.rounds + 1):
        offset = round_number % len(cells)
        for cell_index, (topology, kind, payload, origin) in enumerate(
                cells[offset:] + cells[:offset]):
            engines = ("salias", "aeron") if (round_number + cell_index) % 2 == 0 else (
                "aeron", "salias"
            )
            for engine in engines:
                number += 1
                case = {
                    "engine": engine,
                    "topology": topology,
                    "kind": kind,
                    "payload": payload,
                    "origin": origin,
                    "round": round_number,
                    **TOPOLOGIES[topology],
                }
                rows.append(run_case(args, case, number, args.output))

    environment["limits_end"] = resource_counters()
    summary = summarize(rows, environment)
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    (args.output / "environment.json").write_text(json.dumps(environment, indent=2) + "\n")
    shutil.copy2(Path(__file__), args.output / "frozen-runner.py")
    files = sorted(
        path for path in args.output.iterdir()
        if path.is_file() and path.name != "SHA256SUMS"
    )
    (args.output / "SHA256SUMS").write_text(
        "".join(f"{digest(path)}  {path.name}\n" for path in files)
    )
    print(f"Completed four-topology comparison: {args.output / 'summary.json'}", flush=True)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGHUP, interrupted)
    main()
