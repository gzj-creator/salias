#!/usr/bin/env python3
"""Run the pre-registered P3-2 single-thread versus sleeping-helper matrix.

The runner is intentionally strict: the matrix and ABBA order are constants,
every round gets a fresh transient systemd unit, and a failed round is recorded
and terminates the run.  It never retries a round.
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
BUILD_TOOL = ROOT / "tools/aeron_compare/build_multithread_probe.py"
PROBE_IDENTITY = "p3-p32-publisher-thread-probe-v1"
V3_SOURCE_SHA256 = "6a23a77598c6e980f7b6625eb2471cc42a26917469dc77fc51ab9d5b767bab1e"
V3_SOURCE_SPEC = "v3.0.0:tools/aeron_compare/salias_ipc_compare.cpp"
GROUPS = 6
ARM_ORDER = ("ST", "MT", "MT", "ST")
ARMS = ("ST", "MT")
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
    return {
        "SALIAS_P32_PUBLISHER_MODE": arm.lower(),
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
            for position, arm in enumerate(ARM_ORDER):
                schedule.append({
                    "ordinal": ordinal,
                    "configuration": configuration,
                    "group": group,
                    "position": position,
                    "arm": arm,
                })
                ordinal += 1
    return schedule


def warmup_schedule():
    """Return one independent warm-up round per configuration and arm."""
    return [
        {"configuration": configuration, "arm": arm}
        for configuration in CONFIGURATIONS
        for arm in ARMS
    ]


def client_command(configuration, arm, client, name):
    """Build the exact v3-compatible probe command for one round."""
    return [
        "taskset",
        "-c",
        "0",
        str(client),
        "--scenario",
        "fifo",
        "--producers",
        str(configuration["producers"]),
        "--consumers",
        str(configuration["consumers"]),
        "--messages",
        str(configuration["messages"]),
        "--payload",
        str(configuration["payload"]),
        "--page",
        "normal",
        "--capacity",
        str(configuration["capacity"]),
        "--batch-size",
        str(configuration["batch_size"]),
        "--poll-limit",
        str(configuration["poll_limit"]),
        "--cpu-base",
        "1",
        "--cpu-stride",
        "1",
        "--name",
        name,
        "--rate",
        str(configuration["rate"]),
        "--latency-sample-rate",
        str(configuration["latency_sample_rate"]),
        "--latency-origin",
        configuration["latency_origin"],
    ]


def systemd_round_command(configuration, arm, client, name, unit):
    """Wrap one round in the fixed CPU, nice, memory, and swap cgroup policy."""
    command = [
        "systemd-run",
        "--user",
        "--wait",
        "--pipe",
        "--collect",
        f"--unit={unit}",
        f"--property=WorkingDirectory={ROOT}",
        "--property=AllowedCPUs=0-3",
        "--property=Nice=5",
        "--property=MemoryHigh=640M",
        "--property=MemoryMax=704M",
        "--property=MemorySwapMax=0",
        f"--property=RuntimeMaxSec={configuration['runtime_seconds']}",
    ]
    for key, value in explicit_environment(arm).items():
        command.append(f"--setenv={key}={value}")
    command.extend(client_command(configuration, arm, client, name))
    return command


def fields(line):
    """Parse one key=value control line."""
    return dict(item.split("=", 1) for item in line.split()[1:])


def control_lines(lines, prefix, reasons):
    """Parse matching control lines while retaining malformed output as failure."""
    parsed = []
    for line in lines:
        if not line.startswith(prefix):
            continue
        try:
            parsed.append(fields(line))
        except ValueError:
            reasons.append(f"malformed {prefix.strip()} line: {line}")
    return parsed


def cgroup_file_value(evidence, name):
    """Return one value block from raw cgroup evidence, or None when absent."""
    marker = f"[CGROUP_FILE {name}]\n"
    start = evidence.find(marker)
    if start < 0:
        return None
    start += len(marker)
    next_file = evidence.find("\n[CGROUP_FILE ", start)
    end_marker = evidence.find("\nCGROUP_EVIDENCE_END", start)
    ends = [value for value in (next_file, end_marker) if value >= 0]
    end = min(ends) if ends else len(evidence)
    return evidence[start:end].strip()


def parse_cpu_list(value):
    """Expand a Linux CPU list such as ``0-3,8`` into integer CPU ids."""
    cpus = set()
    for part in value.split(","):
        if not part:
            raise ValueError("empty CPU-list item")
        if "-" in part:
            start, end = (int(item) for item in part.split("-", 1))
            if end < start:
                raise ValueError("descending CPU-list range")
            cpus.update(range(start, end + 1))
        else:
            cpus.add(int(part))
    return cpus


def validate_cgroup_evidence(evidence):
    """Validate the actual cgroup limits and memory-pressure event counters."""
    reasons = []
    required = (
        "cpu.stat", "cpu.pressure", "memory.events", "memory.peak",
        "memory.high", "memory.max", "memory.swap.max", "memory.swap.current",
        "memory.pressure",
    )
    values = {}
    for name in required:
        value = cgroup_file_value(evidence, name)
        if value is None or value == "<unavailable>":
            reasons.append(f"cgroup evidence unavailable for {name}")
        else:
            values[name] = value
    try:
        if values.get("memory.high") != str(640 * 1024 * 1024):
            reasons.append("cgroup MemoryHigh is not 640 MiB")
        if values.get("memory.max") != str(704 * 1024 * 1024):
            reasons.append("cgroup MemoryMax is not 704 MiB")
        if values.get("memory.swap.max") != "0":
            reasons.append("cgroup swap is not zero")
        if values.get("memory.swap.current") != "0":
            reasons.append("cgroup swap usage is nonzero")
        events = dict(
            line.split(maxsplit=1) for line in values.get("memory.events", "").splitlines()
        )
        for event in ("high", "max", "oom", "oom_kill", "oom_group_kill"):
            if int(events.get(event, "0")) > 0:
                reasons.append(f"cgroup memory event {event} is nonzero")
        if "cpuset.cpus.effective" in values:
            cpus = parse_cpu_list(values["cpuset.cpus.effective"])
            if not cpus or not cpus <= {0, 1, 2, 3}:
                reasons.append("cgroup effective CPUs are outside 0-3")
    except (TypeError, ValueError) as error:
        reasons.append(f"malformed cgroup evidence: {error}")
    return reasons


def extract_cgroup_evidence(stdout):
    """Extract the raw cgroup-v2 evidence block emitted by the probe."""
    lines = stdout.splitlines(keepends=True)
    starts = [index for index, line in enumerate(lines)
              if line.startswith("CGROUP_EVIDENCE_BEGIN ")]
    ends = [index for index, line in enumerate(lines)
            if line.strip() == "CGROUP_EVIDENCE_END"]
    if len(starts) != 1 or len(ends) != 1 or ends[0] <= starts[0]:
        return None, ["expected exactly one complete CGROUP_EVIDENCE block"]
    evidence = "".join(lines[starts[0]:ends[0] + 1])
    missing = [name for name in (
        "cpu.stat", "cpu.pressure", "memory.events", "memory.peak", "memory.high",
        "memory.max", "memory.swap.max", "memory.swap.current", "memory.pressure",
    )
               if f"[CGROUP_FILE {name}]" not in evidence]
    reasons = [f"cgroup evidence missing {name}" for name in missing]
    if not missing:
        reasons.extend(validate_cgroup_evidence(evidence))
    return evidence, reasons


def parse_probe_output(stdout, configuration, arm):
    """Validate identity, explicit mode, thread state, and delivery counters."""
    reasons = []
    lines = stdout.splitlines()
    probe = control_lines(lines, "PROBE ", reasons)
    threads = control_lines(lines, "THREAD_STATE ", reasons)
    results = control_lines(lines, "RESULT ", reasons)
    if len(probe) != 1:
        reasons.append(f"expected one PROBE line, found {len(probe)}")
    else:
        if probe[0].get("identity") != PROBE_IDENTITY:
            reasons.append("probe identity mismatch")
        if probe[0].get("source_sha256") != V3_SOURCE_SHA256:
            reasons.append("probe source digest mismatch")
        if probe[0].get("barrier") != "A":
            reasons.append("probe did not report barrier A")
        if probe[0].get("publisher_mode") != arm.lower():
            reasons.append("probe publisher mode does not match arm")
    if len(threads) != 1:
        reasons.append(f"expected one THREAD_STATE line, found {len(threads)}")
    else:
        state = threads[0]
        expected_threads = 1 if arm == "ST" else 2
        if state.get("role") != "publisher":
            reasons.append("thread state was not emitted by publisher")
        if state.get("mode") != arm.lower():
            reasons.append("thread-state mode does not match arm")
        if state.get("expected_threads") != str(expected_threads):
            reasons.append("thread-state expected count does not match fixed mode")
        if state.get("actual_threads") != str(expected_threads):
            reasons.append("publisher actual thread count does not match fixed mode")
        if state.get("before_publish") != "1":
            reasons.append("thread state was not emitted before publish")
        if state.get("libc_single_threaded_available") == "1":
            expected_libc = "1" if expected_threads == 1 else "0"
            if state.get("libc_single_threaded") != expected_libc:
                reasons.append("libc single-threaded state does not match fixed mode")
    metrics = {}
    if len(results) != 1:
        reasons.append(f"expected one RESULT line, found {len(results)}")
    else:
        result = results[0]
        try:
            expected_published = configuration["producers"] * configuration["messages"]
            expected_delivered = expected_published * configuration["consumers"]
            metrics = {
                "published": int(result["published"]),
                "delivered": int(result["delivered"]),
                "seconds": float(result["seconds"]),
                "publish_msg_per_sec": float(result["publish_msg_per_sec"]),
                "delivery_msg_per_sec": float(result["delivery_msg_per_sec"]),
                "latency_samples": int(result["latency_samples"]),
                "latency_p50_ns": int(result["latency_p50_ns"]),
                "latency_p99_ns": int(result["latency_p99_ns"]),
                "latency_p999_ns": int(result["latency_p999_ns"]),
                "latency_max_ns": int(result["latency_max_ns"]),
            }
            if metrics["published"] != expected_published:
                reasons.append("published counter mismatch")
            if metrics["delivered"] != expected_delivered:
                reasons.append("delivered counter mismatch")
        except (KeyError, ValueError) as error:
            reasons.append(f"malformed RESULT fields: {error}")
            metrics = {}
    return metrics, reasons


def write_json(path, value):
    """Atomically replace a JSON evidence file."""
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    os.replace(temporary, path)


def execute_round(output, phase, configuration, arm, ordinal, client,
                  group=None, position=None,
                  invoke=subprocess.run):
    """Execute exactly one round and retain all evidence, including failure."""
    label = f"{ordinal:03d}-{phase}-{configuration['id']}-{arm.lower()}"
    round_dir = output / "rounds" / label
    round_dir.mkdir(parents=True, exist_ok=False)
    stdout_path = round_dir / "stdout.txt"
    stderr_path = round_dir / "stderr.txt"
    round_path = round_dir / "round.json"
    name = f"p3p32-{os.getpid()}-{ordinal:03d}"
    unit = f"p3-p32-{os.getpid()}-{ordinal:03d}"
    command = systemd_round_command(configuration, arm, client, name, unit)
    environment = explicit_environment(arm)
    process_environment = dict(os.environ)
    process_environment.update(environment)
    started_at = time.time()
    failure_reasons = []
    returncode = None
    try:
        with stdout_path.open("w") as stdout, stderr_path.open("w") as stderr:
            completed = invoke(
                command,
                cwd=ROOT,
                env=process_environment,
                stdout=stdout,
                stderr=stderr,
                check=False,
                timeout=configuration["runtime_seconds"] + 30,
            )
            returncode = completed.returncode
    except subprocess.TimeoutExpired:
        returncode = 124
        failure_reasons.append("runner timeout; systemd RuntimeMax was retained")
    except OSError as error:
        returncode = 127
        failure_reasons.append(f"could not launch systemd-run: {error}")
    finished_at = time.time()
    stdout = stdout_path.read_text(errors="replace")
    cgroup, cgroup_reasons = extract_cgroup_evidence(stdout)
    failure_reasons.extend(cgroup_reasons)
    cgroup_path = None
    cgroup_sha256 = None
    if cgroup is not None:
        cgroup_path = round_dir / "cgroup.txt"
        cgroup_path.write_text(cgroup)
        cgroup_sha256 = digest(cgroup_path)
    metrics, parse_reasons = parse_probe_output(stdout, configuration, arm)
    failure_reasons.extend(parse_reasons)
    if returncode != 0:
        failure_reasons.append(f"non-zero return code: {returncode}")
    record = {
        "status": "ok" if not failure_reasons else "failed",
        "phase": phase,
        "ordinal": ordinal,
        "configuration": configuration["id"],
        "kind": configuration["kind"],
        "group": group,
        "position": position,
        "arm": arm,
        "mode": arm.lower(),
        "command": command,
        "environment": environment,
        "client": str(client),
        "client_sha256": digest(client),
        "input_source": V3_SOURCE_SPEC,
        "input_sha256": V3_SOURCE_SHA256,
        "returncode": returncode,
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(started_at)),
        "finished_at": time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(finished_at)),
        "wall_seconds": finished_at - started_at,
        "stdout": str(stdout_path.relative_to(output)),
        "stdout_sha256": digest(stdout_path),
        "stderr": str(stderr_path.relative_to(output)),
        "stderr_sha256": digest(stderr_path),
        "cgroup_evidence": (str(cgroup_path.relative_to(output))
                            if cgroup_path is not None else None),
        "cgroup_sha256": cgroup_sha256,
        "metrics": metrics,
        "failure_reasons": failure_reasons,
    }
    write_json(round_path, record)
    record["record"] = str(round_path.relative_to(output))
    return record


def matrix_description():
    """Return the immutable matrix description used in manifests and self-test."""
    return {
        "protocol": PROBE_IDENTITY,
        "source": V3_SOURCE_SPEC,
        "source_sha256": V3_SOURCE_SHA256,
        "configurations": [dict(configuration) for configuration in CONFIGURATIONS],
        "groups_per_configuration": GROUPS,
        "arm_order_per_group": list(ARM_ORDER),
        "measured_rounds_per_configuration": GROUPS * len(ARM_ORDER),
        "warmups_per_configuration_per_arm": 1,
        "barrier_mode": "A",
        "cgroup": {
            "manager": "systemd-run --user --wait --pipe --collect",
            "allowed_cpus": "0-3",
            "coordinator_cpu": 0,
            "nice": 5,
            "memory_high": "640M",
            "memory_max": "704M",
            "memory_swap_max": 0,
        },
        "retry_policy": "none; any failure is retained and terminates the run",
    }


def validate_matrix():
    """Fail if the frozen matrix or ABBA order was accidentally changed."""
    if len(CONFIGURATIONS) != 6:
        raise ValueError("P3-2 requires exactly six configurations")
    if GROUPS != 6 or tuple(ARM_ORDER) != ("ST", "MT", "MT", "ST"):
        raise ValueError("P3-2 requires six groups with per-group ABBA order")
    for configuration in CONFIGURATIONS:
        if configuration["capacity"] != 1 << 20:
            raise ValueError("P3-2 requires a 1 MiB ring")
        if configuration["batch_size"] != 1 or configuration["poll_limit"] != 64:
            raise ValueError("P3-2 requires batch=1 and poll-limit=64")
        if configuration["producers"] != 1:
            raise ValueError("P3-2 requires one publisher")
        if configuration["kind"] == "throughput":
            if (configuration["consumers"] != 2 or configuration["messages"] != 50_000_000
                    or configuration["rate"] != 0):
                raise ValueError("throughput configuration drifted from P3-2")
        elif configuration["kind"] == "latency":
            if (configuration["consumers"] != 1 or configuration["messages"] != 2_000_000
                    or configuration["rate"] != 1_000_000
                    or configuration["latency_sample_rate"] != 16
                    or configuration["latency_origin"] not in ("attempt", "planned")):
                raise ValueError("latency configuration drifted from P3-2")
        else:
            raise ValueError(f"unknown configuration kind: {configuration['kind']}")
    schedule = formal_schedule()
    if len(schedule) != 6 * GROUPS * len(ARM_ORDER):
        raise ValueError("formal P3-2 schedule is incomplete")


def verify_v3_source():
    """Verify that the registered v3 source remains available and unchanged."""
    result = subprocess.run(
        ["git", "show", V3_SOURCE_SPEC],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode:
        raise RuntimeError(result.stderr.decode("utf-8", "replace").strip())
    actual = hashlib.sha256(result.stdout).hexdigest()
    if actual != V3_SOURCE_SHA256:
        raise RuntimeError(f"v3 source SHA-256 changed: {actual}")


def preparation_environment(client):
    """Capture the immutable build and host context before any run."""
    systemd = subprocess.run(
        ["systemd-run", "--version"], text=True, capture_output=True, check=False
    )
    return {
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "hostname": platform.node(),
        "platform": platform.platform(),
        "python": sys.version,
        "client": str(client),
        "client_sha256": digest(client),
        "runner": str(Path(__file__).resolve()),
        "runner_sha256": digest(Path(__file__).resolve()),
        "build_tool": str(BUILD_TOOL),
        "build_tool_sha256": digest(BUILD_TOOL),
        "source": V3_SOURCE_SPEC,
        "source_sha256": V3_SOURCE_SHA256,
        "systemd_run_version": (systemd.stdout.splitlines() or [""])[0],
        "cgroup_v2": Path("/sys/fs/cgroup/cgroup.controllers").is_file(),
    }


def record_failure(output, manifest, record):
    """Persist the first failed round and stop without retrying it."""
    failure = {
        "status": "failed",
        "retry_policy": "none",
        "failed_round": record,
        "failed_phase": record["phase"],
        "completed_measured_rounds": manifest["progress"]["measured_rounds"],
        "planned_measured_rounds": 6 * GROUPS * len(ARM_ORDER),
    }
    write_json(output / "failure.json", failure)
    manifest["status"] = "failed"
    manifest["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    write_json(output / "experiment.json", manifest)


def run_records(output, manifest, phase, records, invoke):
    """Run a fixed schedule once, stopping at the first retained failure."""
    for item in records:
        record = execute_round(
            output,
            phase,
            item["configuration"],
            item["arm"],
            item["ordinal"],
            manifest["client"],
            group=item.get("group"),
            position=item.get("position"),
            invoke=invoke,
        )
        manifest["records"].append(record)
        if phase == "measured":
            manifest["progress"]["measured_rounds"] += 1
        write_json(output / "experiment.json", manifest)
        if record["status"] != "ok":
            record_failure(output, manifest, record)
            raise RuntimeError(
                f"{phase} round failed: {record['record']} ({record['failure_reasons']})"
            )
    return manifest


def make_manifest(client, output, smoke=False):
    """Write the complete frozen protocol before the first workload starts."""
    manifest = matrix_description()
    manifest.update({
        "status": "running",
        "mode": "smoke" if smoke else "formal",
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "client": str(client),
        "output": str(output),
        "progress": {"measured_rounds": 0},
        "planned_measured_order": [
            {
                "ordinal": item["ordinal"],
                "configuration": item["configuration"]["id"],
                "group": item["group"],
                "position": item["position"],
                "arm": item["arm"],
            }
            for item in formal_schedule()
        ],
        "planned_warmups": [
            {"configuration": item["configuration"]["id"], "arm": item["arm"]}
            for item in warmup_schedule()
        ],
        "records": [],
    })
    write_json(output / "experiment.json", manifest)
    write_json(output / "environment.json", preparation_environment(client))
    return manifest


def run_smoke(client, output, invoke=subprocess.run):
    """Run one tiny 1P1S round per mode without entering the formal matrix."""
    output.mkdir(parents=True, exist_ok=False)
    configuration = {
        "id": "smoke-64b",
        "kind": "smoke",
        "producers": 1,
        "consumers": 1,
        "payload": 64,
        "messages": 10_000,
        "rate": 0,
        "latency_sample_rate": 0,
        "latency_origin": "claim",
        "capacity": CAPACITY,
        "batch_size": 1,
        "poll_limit": 64,
        "runtime_seconds": 30,
    }
    manifest = make_manifest(client, output, smoke=True)
    records = [{"configuration": configuration, "arm": arm, "ordinal": ordinal}
               for ordinal, arm in enumerate(ARMS)]
    run_records(output, manifest, "smoke", records, invoke)
    manifest["status"] = "complete"
    manifest["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    write_json(output / "experiment.json", manifest)
    return manifest


def run_formal(client, output, invoke=subprocess.run):
    """Run smoke, per-arm warm-ups, and the fixed ABBA matrix exactly once."""
    output.mkdir(parents=True, exist_ok=False)
    manifest = make_manifest(client, output)
    smoke_configuration = {
        "id": "smoke-64b",
        "kind": "smoke",
        "producers": 1,
        "consumers": 1,
        "payload": 64,
        "messages": 10_000,
        "rate": 0,
        "latency_sample_rate": 0,
        "latency_origin": "claim",
        "capacity": CAPACITY,
        "batch_size": 1,
        "poll_limit": 64,
        "runtime_seconds": 30,
    }
    smoke = [{"configuration": smoke_configuration, "arm": arm, "ordinal": ordinal}
             for ordinal, arm in enumerate(ARMS)]
    run_records(output, manifest, "smoke", smoke, invoke)
    warmups = [
        {**item, "ordinal": len(smoke) + ordinal}
        for ordinal, item in enumerate(warmup_schedule())
    ]
    run_records(output, manifest, "warmup", warmups, invoke)
    measured = []
    offset = len(smoke) + len(warmups)
    for item in formal_schedule():
        measured.append({**item, "ordinal": offset + item["ordinal"]})
    run_records(output, manifest, "measured", measured, invoke)
    manifest["status"] = "complete"
    manifest["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    write_json(output / "experiment.json", manifest)
    return manifest


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client", type=Path)
    parser.add_argument("--output", type=Path)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--self-test", action="store_true",
                       help="validate the fixed matrix and identity without running workloads")
    modes.add_argument("--smoke", action="store_true",
                       help="run only the two tiny cgroup smoke rounds")
    args = parser.parse_args()
    if args.self_test:
        return args
    if args.client is None or args.output is None:
        parser.error("--client and --output are required unless --self-test is used")
    args.client = args.client.resolve()
    args.output = args.output.resolve()
    if not args.client.is_file() or not os.access(args.client, os.X_OK):
        parser.error(f"client is not executable: {args.client}")
    if args.output.exists() and any(args.output.iterdir()):
        parser.error(f"output directory is not empty: {args.output}")
    return args


def main():
    args = parse_args()
    validate_matrix()
    if args.self_test:
        print(json.dumps(matrix_description(), indent=2))
        return
    verify_v3_source()
    try:
        if args.smoke:
            run_smoke(args.client, args.output)
        else:
            run_formal(args.client, args.output)
    except Exception as error:
        print(f"FAILED: {error}", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
