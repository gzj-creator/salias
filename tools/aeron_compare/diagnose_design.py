#!/usr/bin/env python3
"""Bounded, interleaved design experiments; profiling and timing run separately."""

import argparse
import copy
import fcntl
import hashlib
import json
import os
from pathlib import Path
import signal
import statistics
import subprocess
import time

import run_latency_compare as harness

ROOT = Path(__file__).resolve().parents[2]


def stop_worker(process, privileged=False):
    if not privileged:
        harness.stop(process)
        return
    if process is None:
        return

    def signal_group(name):
        # A user-owned sudo parent does not grant permission to signal root perf.
        result = subprocess.run(["sudo", "-n", "kill", f"-{name}", "--", f"-{process.pid}"],
                                capture_output=True, text=True, timeout=5)
        if result.returncode and process.poll() is None:
            raise RuntimeError(f"Cannot stop profiling process group {process.pid}: {result.stderr}")

    signal_group("TERM")
    try:
        process.wait(timeout=3)
    except subprocess.TimeoutExpired:
        signal_group("KILL")
        process.wait(timeout=5)


def configurations(args):
    cases = {
        "stock": (ROOT / "build/release/bench/salias_ipc_compare", []),
        "baseline": (args.baseline, []),
        "poll1": (args.baseline, ["--poll-limit", "1"]),
        "aeron": (args.aeron_client, []),
    }
    for item in args.variant:
        name, path = item.split("=", 1)
        if name in cases:
            raise ValueError(f"duplicate variant: {name}")
        cases[name] = (Path(path).resolve(), [])
    return cases


def scheduled_command(command, fifo_priority):
    if not fifo_priority:
        return command
    return ["sudo", "-n", "chrt", "--reset-on-fork", "--fifo", str(fifo_priority), *command]


def execute(args, label, binary, extra, origin, rate, payload, round_number, index):
    if harness.available_kib() < 1024 * 1024:
        raise RuntimeError("Require at least 1 GiB available before each run")
    scheduling = f"-fifo{args.fifo_priority}" if args.fifo_priority else "-normal"
    prefix = args.output / f"{index:03d}-{label}{scheduling}-{origin}-{rate}-{payload}-r{round_number}"
    name = f"design-{os.getpid()}-{index}"
    driver_dir = Path("/dev/shm") / name
    driver = worker = None
    driver_log = None
    common = ["--messages", str(rate * args.seconds), "--producers", "1", "--consumers", "1",
              "--payload", str(payload), "--cpu-base", "1", "--cpu-stride", "1",
              "--rate", str(rate), "--latency-sample-rate", "16", "--latency-origin", origin]
    try:
        if label == "aeron":
            driver_log = prefix.with_suffix(".driver.log").open("w")
            driver = subprocess.Popen([
                "taskset", "-c", "3", str(args.aeron_build / "binaries/aeronmd"),
                f"-Daeron.dir={driver_dir}", "-Daeron.dir.delete.on.start=true",
                "-Daeron.threading.mode=SHARED", "-Daeron.shared.idle.strategy=backoff",
                "-Daeron.term.buffer.sparse.file=false", "-Daeron.ipc.term.buffer.length=1048576"],
                stdout=driver_log, stderr=subprocess.STDOUT, start_new_session=True)
            deadline = time.monotonic() + 10
            while not (driver_dir / "cnc.dat").exists():
                if driver.poll() is not None or time.monotonic() > deadline:
                    raise RuntimeError("Aeron startup failed")
                time.sleep(0.02)
            command = [str(binary), "--dir", str(driver_dir), "--scenario", "mpsc",
                       "--term-length", "1048576", "--fragment-limit", "64", *common, *extra]
        else:
            command = [str(binary), "--scenario", "fifo", "--name", name,
                       "--capacity", "1048576", "--poll-limit", "64", *common, *extra]
        command = scheduled_command(command, args.fifo_priority)
        profiling = args.profile
        if profiling:
            command = ["sudo", "-n", "perf", "record", "-e", "cpu-clock", "-F", "199", "-m", "1024",
                       "--timestamp", "--sample-cpu", "--switch-events",
                       "-g", "--call-graph", "dwarf,4096", "-o", str(prefix.with_suffix(".perf.data")),
                       "--", *command]
        if args.trace and label != "aeron":
            command += ["--tail-threshold-ns", "10000"]
            command = ["sudo", "-n", "perf", "record", "-e", "dummy", "--clockid", "mono", "--timestamp", "--sample-cpu",
                       "--switch-events", "-o", str(prefix.with_suffix(".switch.data")), "--", *command]
        started = time.monotonic()
        with prefix.with_suffix(".stdout").open("w") as out, prefix.with_suffix(".stderr").open("w") as err:
            worker = subprocess.Popen(command, stdout=out, stderr=err, start_new_session=True)
            lowest = harness.available_kib()
            while worker.poll() is None:
                lowest = min(lowest, harness.available_kib())
                if lowest < 600 * 1024 or time.monotonic() - started > args.seconds + 25:
                    raise RuntimeError("Resource floor or timeout")
                time.sleep(0.05)
            if worker.returncode:
                raise RuntimeError(f"Run failed: {prefix}")
        lines = prefix.with_suffix(".stdout").read_text().splitlines()
        result = harness.fields(next(line for line in lines if line.startswith("RESULT ")))
        expected = rate * args.seconds
        if (int(result["published"]) != expected or int(result["delivered"]) != expected or
                int(result["latency_samples"]) != (expected + 15) // 16):
            raise RuntimeError(f"Count mismatch: {prefix}")
        record = dict(variant=label, origin=origin, rate=rate, payload=payload, round=round_number,
                      duration_s=args.seconds,
                      scheduler="fifo-workers" if args.fifo_priority else "normal",
                      fifo_priority=args.fifo_priority,
                      phase="warmup" if round_number == 0 else "measured", profiled=profiling or args.trace,
                      result=result, command=command, stdout=str(prefix.with_suffix(".stdout")),
                      binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                      stdout_sha256=hashlib.sha256(prefix.with_suffix(".stdout").read_bytes()).hexdigest(),
                      min_mem_available_kib=lowest, resources=harness.resource_counters())
        with (args.output / "results.jsonl").open("a") as out:
            out.write(json.dumps(record) + "\n")
        print(f"{index} {label} {origin} {rate}/s r{round_number} " + " ".join(
            f"{key}={result['latency_' + key + '_ns']}ns" for key in ("p50", "p99", "p999", "max")), flush=True)
    finally:
        try:
            stop_worker(worker, args.profile or args.fifo_priority != 0 or
                        (args.trace and label != "aeron"))
        finally:
            harness.stop(driver)
            if driver_log:
                driver_log.close()
            if driver_dir.exists():
                import shutil
                shutil.rmtree(driver_dir)
            for path in Path("/dev/shm").glob(f"salias-{name}-*"):
                path.unlink(missing_ok=True)
    time.sleep(0.25)


def summarize(records):
    groups = {}
    for record in records:
        if record["phase"] != "measured" or record["profiled"]:
            continue
        key = (record["variant"], record["origin"], record["rate"], record["payload"],
               record.get("scheduler", "normal"), record.get("fifo_priority", 0))
        groups.setdefault(key, []).append(record)
    summary = []
    for (variant, origin, rate, payload, scheduler, priority), rows in sorted(groups.items()):
        entry = dict(variant=variant, origin=origin, rate=rate, payload=payload, rounds=len(rows),
                     scheduler=scheduler, fifo_priority=priority)
        for metric in ("p50", "p99", "p999", "max"):
            values = [int(row["result"][f"latency_{metric}_ns"]) for row in rows]
            entry[metric] = dict(median_ns=statistics.median(values), min_ns=min(values), max_ns=max(values))
        summary.append(entry)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--aeron-client", type=Path, required=True)
    parser.add_argument("--aeron-build", type=Path, required=True)
    parser.add_argument("--variant", action="append", default=[])
    parser.add_argument("--cases", nargs="+", default=["baseline", "aeron"])
    parser.add_argument("--origins", nargs="+", choices=["attempt", "planned"], default=["attempt", "planned"])
    parser.add_argument("--rates", nargs="+", type=int, default=[1000000])
    parser.add_argument("--payloads", nargs="+", type=int, default=[64])
    parser.add_argument("--seconds", type=int, default=2)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--profile", action="store_true")
    parser.add_argument("--trace", action="store_true")
    parser.add_argument("--fifo-priority", type=int, default=0,
                        help="experimental worker-only FIFO 1-10; requires reset-on-fork binaries")
    parser.add_argument("--compare-normal", action="store_true",
                        help="interleave normal and FIFO runs of the same scheduler binaries")
    args = parser.parse_args()
    lock = open(f"/tmp/salias-design-diagnosis-{os.getuid()}.lock", "a")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as error:
        raise RuntimeError("Another design benchmark is running; never overlap measurement matrices") from error
    if (not 1 <= args.seconds <= 10 or not 1 <= args.rounds <= 10 or
            not 0 <= args.fifo_priority <= 10 or
            any(not 2000 <= rate <= 1000000 for rate in args.rates) or
            any(not 16 <= payload <= 1024 for payload in args.payloads)):
        parser.error("unsafe matrix parameters")
    if args.compare_normal and not args.fifo_priority:
        parser.error("--compare-normal requires --fifo-priority")
    if args.fifo_priority:
        if args.seconds > 2:
            parser.error("FIFO experiments must not exceed two seconds per case")
        if Path("/proc/sys/kernel/sched_rt_runtime_us").read_text().strip() == "-1":
            parser.error("FIFO requires the system RT runtime guard to remain enabled")
    harness.check_resource_limits()
    if args.profile and args.trace:
        parser.error("profiling and scheduler tracing are separate runs")
    cases = configurations(args)
    if any(case not in cases for case in args.cases):
        parser.error("unknown case")
    if args.fifo_priority and any(b"SALIAS_WORKER_FIFO_V1" not in cases[case][0].read_bytes()
                                  for case in args.cases):
        parser.error("FIFO requires binaries generated by build_worker_scheduler.py")
    args.output.mkdir(parents=True, exist_ok=True)
    if (args.output / "results.jsonl").exists():
        parser.error("choose a fresh output directory")
    index = 0
    for payload in args.payloads:
        for rate in args.rates:
            for origin in args.origins:
                for round_number in range(args.rounds + 1):
                    labels = args.cases[round_number % len(args.cases):] + args.cases[:round_number % len(args.cases)]
                    priorities = [0, args.fifo_priority] if args.compare_normal else [args.fifo_priority]
                    if round_number % 2:
                        priorities.reverse()
                    for priority in priorities:
                        scheduled = copy.copy(args)
                        scheduled.fifo_priority = priority
                        for label in labels:
                            index += 1
                            execute(scheduled, label, *cases[label], origin, rate, payload, round_number, index)
    records = [json.loads(line) for line in (args.output / "results.jsonl").read_text().splitlines()]
    (args.output / "summary.json").write_text(json.dumps(summarize(records), indent=2) + "\n")


if __name__ == "__main__":
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f"signal {signum}")

    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGHUP, interrupted)
    main()
