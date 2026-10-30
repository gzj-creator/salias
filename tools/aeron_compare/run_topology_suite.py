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
ABBA_ORDER = ("salias", "aeron", "aeron", "salias")


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def interrupted(signum, frame):
    raise KeyboardInterrupt(f"Interrupted by signal {signum}")


def verify_limits(required_workers=1, require_dedicated_driver=False):
    directory = cgroup_directory()
    limit = (directory / "memory.max").read_text().strip()
    affinity = sorted(os.sched_getaffinity(0))
    required_cpus = required_workers + (1 if require_dedicated_driver else 0)
    if (os.environ.get("SALIAS_RESOURCE_CAPPED") != "1" or len(affinity) < required_cpus or
            limit == "max" or int(limit) > 704 * 1024 * 1024 or
            (directory / "memory.swap.max").read_text().strip() != "0"):
        raise RuntimeError(
            f"Require at least {required_cpus} allowed CPUs, MemoryMax<=704M, "
            "MemorySwapMax=0, and SALIAS_RESOURCE_CAPPED=1"
        )


def cpu_layout(producers, consumers, allowed_cpus=None, require_dedicated_driver=False):
    cpus = sorted(os.sched_getaffinity(0) if allowed_cpus is None else allowed_cpus)
    worker_count = producers + consumers
    if len(cpus) < worker_count:
        raise RuntimeError(f"Need {worker_count} application CPUs, found {len(cpus)}")
    driver_cpu = cpus[0]
    can_reserve_driver = len(cpus) >= worker_count + 1
    if require_dedicated_driver and not can_reserve_driver:
        raise RuntimeError(
            f"Dedicated Aeron driver needs {worker_count + 1} CPUs, found {len(cpus)}"
        )
    worker_cpus = (cpus[1:worker_count + 1] if can_reserve_driver else cpus[:worker_count])
    if any(right != left + 1 for left, right in zip(worker_cpus, worker_cpus[1:])):
        raise RuntimeError(f"Application CPUs must be contiguous, got {worker_cpus}")
    dedicated = driver_cpu not in worker_cpus
    return {
        "worker_cpu_base": worker_cpus[0],
        "worker_cpus": worker_cpus,
        "driver_cpu": driver_cpu,
        "coordinator_cpu": driver_cpu,
        "dedicated_driver": dedicated,
        "layout": (
            f"workers on CPUs {worker_cpus[0]}-{worker_cpus[-1]}; coordinator and Aeron "
            f"driver on CPU{driver_cpu}"
            + ("" if dedicated else "; driver shares an application CPU")
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


def engine_order(round_number, cell_index, group_index=0):
    """Return the paired engine order for one measured ABBA round."""
    if round_number == 0:
        first = "salias" if (cell_index + group_index) % 2 == 0 else "aeron"
    else:
        offset = (round_number - 1 + cell_index + group_index) % len(ABBA_ORDER)
        first = ABBA_ORDER[offset]
    second = "aeron" if first == "salias" else "salias"
    return first, second


def run_case(args, case, number, output):
    if available_kib() < 1024 * 1024:
        raise RuntimeError("Less than 1 GiB MemAvailable; refusing the next case")

    producers = case["producers"]
    consumers = case["consumers"]
    layout = cpu_layout(producers, consumers, require_dedicated_driver=args.require_dedicated_driver)
    name = f"topology-{os.getpid()}-{number}"
    driver_dir = Path("/dev/shm") / name
    prefix = output / (
        f"{number:03d}-{case['engine']}-{case['topology']}-{case['kind']}-"
        f"{case['payload']}b-{case['origin']}-g{case['group']}-r{case['round']}"
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
        "--verify", args.verify,
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
        elif args.salias_engine == "fast":
            if case["kind"] == "latency":
                command = [
                    str(args.fast_latency_client),
                    "--topology", case["topology"],
                    "--origin", case["origin"],
                    "--rate", str(args.latency_total_rate),
                    "--seconds", str(args.latency_seconds),
                    "--sample-rate", str(sample_rate),
                    "--payload", str(case["payload"]),
                    "--verify", args.verify,
                    "--cpu-base", str(layout["worker_cpu_base"]),
                    "--cpu-stride", "1",
                ]
            else:
                command = [
                    str(args.fast_throughput_client),
                    "--topology", case["topology"],
                    "--messages", str(messages_per_producer),
                    "--payload", str(case["payload"]),
                    "--verify", args.verify,
                    "--cpu-base", str(layout["worker_cpu_base"]),
                    "--cpu-stride", "1",
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
        if case["kind"] == "latency":
            if [int(item["consumer"]) for item in consumers_detail] != list(range(consumers)):
                raise RuntimeError(f"Subscriber result mismatch: {prefix}")
        elif consumers_detail and [int(item["consumer"]) for item in consumers_detail] != list(
                range(consumers)):
            raise RuntimeError(f"Subscriber result mismatch: {prefix}")
        if int(result["producers"]) != producers or int(result["consumers"]) != consumers:
            raise RuntimeError(f"Topology result mismatch: {prefix}")
        if int(result["rate_per_producer"]) != rate_per_producer:
            raise RuntimeError(f"Rate mismatch: {prefix}")
        if result["latency_origin"] != case["origin"]:
            raise RuntimeError(f"Latency origin mismatch: {prefix}")
        if result.get("verification") != args.verify:
            raise RuntimeError(f"Verification mode mismatch: {prefix}")
        if args.verify == "full" and int(result.get("verified", -1)) != int(result["delivered"]):
            raise RuntimeError(f"Full verification count mismatch: {prefix}")

        resources_after = resource_counters()
        events = dict(line.split() for line in resources_after["memory.events"].splitlines())
        if any(int(events[key]) != 0 for key in ("max", "oom", "oom_kill")):
            raise RuntimeError(f"Memory max/OOM event observed: {prefix}")

        worst = ({
            key: max(int(item[key + "_ns"]) for item in consumers_detail)
            for key in LATENCY_PERCENTILES
        } if case["kind"] == "latency" else None)
        record = {
            "number": number,
            "phase": "warmup" if case["round"] == 0 else "measured",
            "group": case["group"],
            "order_index": case["order_index"],
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
            "verification": args.verify,
            "remote_payload_bytes": messages_per_producer * case["payload"] * producers,
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
            latency_p50 = result.get("latency_p50_ns") or result["p50_ns"]
            latency_p99 = result.get("latency_p99_ns") or result["p99_ns"]
            latency_p999 = result.get("latency_p999_ns") or result["p999_ns"]
            print(
                f"case={number:03d} {case['engine']:5s} {case['topology']} "
                f"{case['kind']} {case['payload']}B {case['origin']} "
                f"p50={int(latency_p50)/1000:.3f}us "
                f"p99={int(latency_p99)/1000:.3f}us "
                f"p999={int(latency_p999)/1000:.3f}us",
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
        group_count = len({row.get("group", 0) for row in cell})
        group = {
            "topology": topology,
            "kind": kind,
            "payload": payload,
            "origin": origin,
            "engine": engine,
            "rounds": len(cell),
            "groups": group_count,
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
        "verification": environment.get("workload", {}).get("verification", "none"),
        "measured_published": sum(int(row["result"]["published"]) for row in measured),
        "measured_delivered": sum(int(row["result"]["delivered"]) for row in measured),
        "measured_latency_samples": sum(
            int(row["result"]["latency_samples"]) for row in measured
        ),
        "verified_messages": sum(int(row["result"].get("verified", 0)) for row in measured),
        "min_mem_available_kib": min(row["min_mem_available_kib"] for row in rows),
    }


def write_environment(args, output):
    directory = cgroup_directory()
    allowed_cpus = sorted(os.sched_getaffinity(0))
    max_workers = max(
        TOPOLOGIES[topology]["producers"] + TOPOLOGIES[topology]["consumers"]
        for topology in args.topologies
    )
    files = [
        args.salias_client,
        *([args.fast_throughput_client, args.fast_latency_client]
          if args.salias_engine == "fast" else []),
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
        "online_cpus": allowed_cpus,
        "cpu_capacity": {
            "allowed_cpus": allowed_cpus,
            "max_application_workers": max_workers,
            "dedicated_driver_available": len(allowed_cpus) >= max_workers + 1,
            "require_dedicated_driver": args.require_dedicated_driver,
        },
        "aeron_version": "1.52.0",
        "workload": {
            "topologies": [name for name in args.topologies],
            "payloads": list(args.payloads),
            "kinds": list(args.kinds),
            "throughput_messages_per_producer": args.throughput_messages_per_producer,
            "latency_total_rate_msg_s": args.latency_total_rate,
            "latency_seconds": args.latency_seconds,
            "latency_origins": list(args.origins),
            "latency_sample_rate": args.latency_sample_rate,
            "warmup_rounds": 1,
            "warmup_rounds_per_group": 1,
            "groups": args.groups,
            "measured_rounds": args.rounds,
            "measured_rounds_per_engine_per_group": args.rounds,
            "engine_order": "ABBA paired rounds",
            "verification": args.verify,
        },
        "semantics": (
            f"Salias {args.salias_engine} engine and Aeron exclusive publications; "
            "multiple subscribers receive every published message (fanout), not "
            "competing consumers"
        ),
        "salias_engine": args.salias_engine,
        "capacity": (
            "Salias fast ring stores 1 MiB of payload data per producer for SPSC/SPMC and "
            "256 KiB for MPSC/MPMC producers, plus fixed-slot metadata; Aeron uses 1 MiB "
            "terms in a multi-term log, so total mapped capacities differ"
            if args.salias_engine == "fast" else
            "Salias 1 MiB per-producer ring versus Aeron 1 MiB term in a multi-term "
            "log; total mapped capacities differ"
        ),
        "aggregation": (
            "Median and range across measured run values; latency uses each run's "
            "worst subscriber, never pooled histograms"
        ),
        "payload_limit": (
            "Full verification checks every payload byte, producer identity, per-producer "
            "sequence, ordering, and duplicates"
            if args.verify == "full" else
            "Counts and samples are checked; payload content, sequence, and duplicate "
            "logic are not verified"
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
    if not 1 <= args.rounds <= 20:
        parser.error("--rounds must be in [1, 20]")
    if not 1 <= args.groups <= 6:
        parser.error("--groups must be in [1, 6]")
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
    paths = [args.salias_client, args.aeron_client, args.aeron_driver]
    if args.salias_engine == "fast":
        paths.extend([args.fast_throughput_client, args.fast_latency_client])
    for path in paths:
        if not path.is_file():
            parser.error(f"missing executable: {path}")
    if any(not os.access(path, os.X_OK) for path in paths):
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
    parser.add_argument("--salias-engine", choices=["fifo", "fast"], default="fifo")
    parser.add_argument(
        "--fast-throughput-client", type=Path,
        default=ROOT / "build/release/bench/salias_fast_ring_compare",
    )
    parser.add_argument(
        "--fast-latency-client", type=Path,
        default=ROOT / "build/release/bench/salias_fast_ring_latency",
    )
    parser.add_argument("--aeron-client", type=Path, required=True)
    parser.add_argument("--aeron-driver", type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--groups", type=int, default=1)
    parser.add_argument("--cooldown", type=float, default=0.5)
    parser.add_argument("--verify", choices=["none", "full"], default="none")
    parser.add_argument("--require-dedicated-driver", action="store_true")
    parser.add_argument("--topologies", nargs="+", choices=TOPOLOGIES.keys(),
                        default=list(TOPOLOGIES))
    parser.add_argument("--kinds", nargs="+", choices=["latency", "throughput"],
                        default=["latency", "throughput"])
    parser.add_argument("--payloads", nargs="+", type=int, default=[64, 256])
    parser.add_argument("--throughput-messages-per-producer", type=int, default=10_000_000)
    parser.add_argument("--latency-total-rate", type=int, default=1_000_000)
    parser.add_argument("--latency-seconds", type=int, default=2)
    parser.add_argument("--latency-sample-rate", type=int, default=16)
    parser.add_argument("--origins", nargs="+", choices=["attempt", "planned"],
                        default=["attempt", "planned"])
    args = parser.parse_args()
    validate_args(parser, args)
    max_workers = max(
        TOPOLOGIES[topology]["producers"] + TOPOLOGIES[topology]["consumers"]
        for topology in args.topologies
    )
    verify_limits(max_workers, args.require_dedicated_driver)

    args.output.mkdir(parents=True, exist_ok=True)
    environment = write_environment(args, args.output)
    rows = []
    number = 0
    cells = []
    for topology in args.topologies:
        for payload in args.payloads:
            if "latency" in args.kinds:
                for origin in args.origins:
                    cells.append((topology, "latency", payload, origin))
    for topology in args.topologies:
        if "throughput" in args.kinds:
            for payload in args.payloads:
                cells.append((topology, "throughput", payload, "claim"))

    for group_number in range(args.groups):
        for round_number in range(args.rounds + 1):
            offset = round_number % len(cells)
            for cell_index, (topology, kind, payload, origin) in enumerate(
                    cells[offset:] + cells[:offset]):
                engines = engine_order(round_number, cell_index, group_number)
                for order_index, engine in enumerate(engines):
                    number += 1
                    case = {
                        "engine": engine,
                        "topology": topology,
                        "kind": kind,
                        "payload": payload,
                        "origin": origin,
                        "group": group_number,
                        "order_index": order_index,
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
