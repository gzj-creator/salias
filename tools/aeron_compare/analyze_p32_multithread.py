#!/usr/bin/env python3
"""Independently audit and summarize the P3-2 ST/MT diagnostic matrix.

The formal result is descriptive: it measures a single-threaded publisher
process against the same binary with one sleeping helper thread.  It does not
separate std::shared_ptr atomic reference counting from other libc/runtime
costs of a multithreaded process.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
import statistics
from typing import Any


PROTOCOL = "p3-p32-publisher-thread-probe-v1"
V3_SOURCE_SHA256 = "6a23a77598c6e980f7b6625eb2471cc42a26917469dc77fc51ab9d5b767bab1e"
ARMS = ("ST", "MT")
ARM_ORDER = ("ST", "MT", "MT", "ST")
GROUPS = 6
CONFIGURATIONS = {
    "throughput-64b": {"kind": "throughput", "producers": 1, "consumers": 2,
                       "payload": 64, "messages": 50_000_000},
    "throughput-256b": {"kind": "throughput", "producers": 1, "consumers": 2,
                        "payload": 256, "messages": 50_000_000},
    "latency-64b-attempt": {"kind": "latency", "producers": 1, "consumers": 1,
                            "payload": 64, "messages": 2_000_000},
    "latency-64b-planned": {"kind": "latency", "producers": 1, "consumers": 1,
                            "payload": 64, "messages": 2_000_000},
    "latency-256b-attempt": {"kind": "latency", "producers": 1, "consumers": 1,
                             "payload": 256, "messages": 2_000_000},
    "latency-256b-planned": {"kind": "latency", "producers": 1, "consumers": 1,
                             "payload": 256, "messages": 2_000_000},
}
LATENCY_KEYS = ("latency_p50_ns", "latency_p99_ns", "latency_p999_ns", "latency_max_ns")


class AuditError(ValueError):
    """The evidence cannot be accepted as the frozen P3-2 matrix."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise AuditError(message)


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _safe_file(directory: Path, relative: Any) -> Path:
    _require(isinstance(relative, str) and relative, "artifact path is missing")
    path = (directory / relative).resolve()
    _require(directory in path.parents, f"artifact escapes evidence directory: {relative}")
    _require(path.is_file(), f"artifact is missing: {relative}")
    return path


def _verify_artifact(directory: Path, record: dict[str, Any], key: str,
                     hash_key: str | None = None) -> Path:
    path = _safe_file(directory, record.get(key))
    expected = record.get(hash_key or f"{key}_sha256")
    _require(digest(path) == expected, f"{key} hash mismatch: {path}")
    return path


def _control_lines(lines: list[str], prefix: str) -> list[dict[str, str]]:
    parsed = []
    for line in lines:
        if not line.startswith(prefix):
            continue
        try:
            parsed.append(dict(item.split("=", 1) for item in line.split()[1:]))
        except ValueError as error:
            raise AuditError(f"malformed {prefix.strip()} line: {line}") from error
    return parsed


def cgroup_values(evidence: str) -> dict[str, str]:
    """Parse cgroup-v2 files from the probe's retained evidence block."""
    values: dict[str, str] = {}
    for line in evidence.splitlines():
        if not line.startswith("[CGROUP_FILE ") or not line.endswith("]"):
            continue
        name = line[len("[CGROUP_FILE "):-1]
        marker = line + "\n"
        start = evidence.find(marker)
        _require(start >= 0, f"duplicate or missing cgroup marker: {name}")
        start += len(marker)
        next_file = evidence.find("\n[CGROUP_FILE ", start)
        end_marker = evidence.find("\nCGROUP_EVIDENCE_END", start)
        ends = [value for value in (next_file, end_marker) if value >= 0]
        _require(bool(ends), f"unterminated cgroup block: {name}")
        values[name] = evidence[start:min(ends)].strip()
    return values


def validate_cgroup(evidence: str) -> dict[str, str]:
    _require(evidence.startswith("CGROUP_EVIDENCE_BEGIN "), "cgroup evidence header missing")
    _require(evidence.rstrip().endswith("CGROUP_EVIDENCE_END"), "cgroup evidence footer missing")
    values = cgroup_values(evidence)
    for name in ("cpu.stat", "memory.events", "memory.peak", "memory.high",
                 "memory.max", "memory.swap.max", "memory.swap.current"):
        _require(values.get(name) not in (None, "", "<unavailable>"),
                 f"cgroup evidence unavailable: {name}")
    _require(values["memory.high"] == str(640 * 1024 * 1024), "MemoryHigh is not 640 MiB")
    _require(values["memory.max"] == str(704 * 1024 * 1024), "MemoryMax is not 704 MiB")
    _require(values["memory.swap.max"] == "0", "cgroup swap limit is not zero")
    _require(values["memory.swap.current"] == "0", "cgroup swap usage is nonzero")
    events = dict(line.split(maxsplit=1) for line in values["memory.events"].splitlines())
    for event in ("high", "max", "oom", "oom_kill", "oom_group_kill"):
        _require(int(events.get(event, "0")) == 0, f"cgroup memory event {event} is nonzero")
    return values


def parse_probe(stdout: str, configuration: str, arm: str) -> dict[str, Any]:
    lines = stdout.splitlines()
    probe = _control_lines(lines, "PROBE ")
    states = _control_lines(lines, "THREAD_STATE ")
    results = _control_lines(lines, "RESULT ")
    _require(len(probe) == 1, f"expected one PROBE line for {configuration}/{arm}")
    _require(len(states) == 1, f"expected one THREAD_STATE line for {configuration}/{arm}")
    _require(len(results) == 1, f"expected one RESULT line for {configuration}/{arm}")
    expected_threads = "1" if arm == "ST" else "2"
    _require(probe[0].get("identity") == PROTOCOL, "probe identity mismatch")
    _require(probe[0].get("source_sha256") == V3_SOURCE_SHA256,
             "frozen source digest mismatch")
    _require(probe[0].get("barrier") == "A", "product barrier mode is not A")
    _require(probe[0].get("publisher_mode") == arm.lower(),
             "PROBE publisher mode differs from record arm")
    _require(states[0].get("role") == "publisher", "thread state is not from publisher")
    _require(states[0].get("mode") == arm.lower(), "thread-state mode differs from arm")
    _require(states[0].get("expected_threads") == expected_threads,
             "publisher expected thread count differs from fixed arm")
    _require(states[0].get("actual_threads") == expected_threads,
             "publisher thread count differs from fixed arm")
    _require(states[0].get("before_publish") == "1",
             "publisher thread state was not captured before publish")
    if states[0].get("libc_single_threaded_available") == "1":
        expected_libc = "1" if arm == "ST" else "0"
        _require(states[0].get("libc_single_threaded") == expected_libc,
                 "libc single-threaded state differs from fixed arm")
    result = results[0]
    metrics = {key: float(result[key]) for key in (
        "seconds", "publish_msg_per_sec", "delivery_msg_per_sec")}
    metrics.update({key: int(result[key]) for key in (
        "published", "delivered", "latency_samples", *LATENCY_KEYS)})
    _require(metrics["seconds"] > 0 and metrics["publish_msg_per_sec"] > 0,
             "non-positive throughput measurement")
    _require(metrics["delivery_msg_per_sec"] > 0, "non-positive delivery rate")
    latency_values = [metrics[key] for key in LATENCY_KEYS]
    _require(all(value >= 0 and math.isfinite(value) for value in latency_values),
             "invalid latency metric")
    _require(latency_values == sorted(latency_values),
             "latency quantiles are not monotonic")
    return {"metrics": metrics, "thread_state": states[0]}


def summarize(values: list[float]) -> dict[str, float | int]:
    _require(bool(values), "cannot summarize an empty sample")
    mean = statistics.mean(values)
    stddev = statistics.stdev(values) if len(values) > 1 else 0.0
    return {
        "n": len(values), "median": statistics.median(values), "mean": mean,
        "stddev": stddev, "cv_percent": 100 * stddev / mean if mean else 0.0,
        "min": min(values), "max": max(values),
    }


def analyze(directory: Path) -> dict[str, Any]:
    directory = directory.resolve()
    manifest_path = directory / "experiment.json"
    _require(manifest_path.is_file(), f"manifest is missing: {manifest_path}")
    manifest = json.loads(manifest_path.read_text())
    _require(manifest.get("protocol") == PROTOCOL, "manifest protocol mismatch")
    _require(manifest.get("mode") == "formal", "formal matrix manifest required")
    _require(manifest.get("status") == "complete", "formal matrix is not complete")
    _require(manifest.get("progress", {}).get("measured_rounds") == 144,
             "manifest measured-round count is not 144")
    manifest_configurations = manifest.get("configurations")
    _require(isinstance(manifest_configurations, list), "manifest configurations are missing")
    by_id = {item.get("id"): item for item in manifest_configurations
             if isinstance(item, dict)}
    _require(set(by_id) == set(CONFIGURATIONS), "manifest configuration set mismatch")
    for name, expected in CONFIGURATIONS.items():
        actual = by_id[name]
        for key, value in expected.items():
            _require(actual.get(key) == value,
                     f"manifest configuration drift: {name}.{key}")
    client = Path(manifest.get("client", ""))
    _require(client.is_file(), "probe client is missing")
    environment_path = directory / "environment.json"
    _require(environment_path.is_file(), "environment.json is missing")
    environment = json.loads(environment_path.read_text())
    _require(digest(client) == environment.get("client_sha256"), "probe client hash changed")
    _require(manifest.get("source_sha256") == V3_SOURCE_SHA256,
             "manifest source digest mismatch")

    planned = manifest.get("planned_measured_order")
    _require(isinstance(planned, list) and len(planned) == 144,
             "planned measured order must contain 144 rounds")
    expected_ordinals = list(range(144))
    _require([item.get("ordinal") for item in planned] == expected_ordinals,
             "planned ordinals are not contiguous")
    for item in planned:
        configuration = item.get("configuration")
        _require(configuration in CONFIGURATIONS, f"unknown planned configuration: {configuration}")
        _require(item.get("group") in range(GROUPS), "planned group is invalid")
        _require(item.get("arm") in ARMS, "planned arm is invalid")
        _require(item.get("position") in range(len(ARM_ORDER)), "planned position is invalid")

    all_records = manifest.get("records", [])
    measured = [record for record in all_records if record.get("phase") == "measured"]
    _require(len(measured) == 144, f"expected 144 measured records, got {len(measured)}")
    _require(not any(record.get("phase") in ("smoke", "warmup") and
                     record.get("status") != "ok" for record in all_records),
             "a smoke or warm-up round failed")
    measured_offset = next(index for index, record in enumerate(all_records)
                           if record.get("phase") == "measured")
    _require(measured_offset == 14, "formal run must have two smoke and twelve warm-up rounds")
    rows = []
    for index, (record, expected) in enumerate(zip(measured, planned)):
        _require(record.get("ordinal") == measured_offset + index,
                 f"record ordinal mismatch at measured index {index}")
        for field in ("ordinal", "configuration", "group", "position", "arm"):
            actual = record.get(field)
            wanted = (measured_offset + expected.get(field)
                      if field == "ordinal" else expected.get(field))
            _require(actual == wanted,
                     f"record identity mismatch at measured index {index}: {field}")
        _require(record.get("status") == "ok" and record.get("returncode") == 0,
                 f"measured round failed: {record.get('ordinal')}")
        _require(record.get("client_sha256") == digest(client), "record client hash mismatch")
        _require(record.get("input_sha256") == V3_SOURCE_SHA256,
                 "record input digest mismatch")
        _require(record.get("environment") == {
            "SALIAS_P32_PUBLISHER_MODE": record["arm"].lower(),
            "SALIAS_DISABLE_ASYMMETRIC_FENCE": "0",
            "SALIAS_RESOURCE_CAPPED": "1",
        }, "record environment differs from the explicit arm")
        stdout_path = _verify_artifact(directory, record, "stdout")
        _verify_artifact(directory, record, "stderr")
        cgroup_path = _verify_artifact(directory, record, "cgroup_evidence", "cgroup_sha256")
        parsed = parse_probe(stdout_path.read_text(), record["configuration"], record["arm"])
        cgroup = validate_cgroup(cgroup_path.read_text())
        saved = json.loads(_safe_file(directory, record.get("record")).read_text())
        _require(saved.get("status") == "ok" and saved.get("failure_reasons") == [],
                 "round record is not clean")
        configuration = CONFIGURATIONS[record["configuration"]]
        expected_published = configuration["producers"] * configuration["messages"]
        expected_delivered = expected_published * configuration["consumers"]
        _require(parsed["metrics"]["published"] == expected_published,
                 "published count differs from plan")
        _require(parsed["metrics"]["delivered"] == expected_delivered,
                 "delivered count differs from plan")
        _require(parsed["metrics"]["latency_samples"] ==
                 (configuration["messages"] // 16 if configuration["kind"] == "latency" else 0),
                 "latency sample count differs from plan")
        rows.append({**record, **parsed["metrics"], "cgroup": cgroup,
                     "stdout_path": stdout_path})

    for configuration in CONFIGURATIONS:
        for group in range(GROUPS):
            arms = [row["arm"] for row in rows
                    if row["configuration"] == configuration and row["group"] == group]
            _require(tuple(arms) == ARM_ORDER,
                     f"ABBA order mismatch for {configuration}/group-{group}")

    throughput = {}
    for configuration in ("throughput-64b", "throughput-256b"):
        arms = {}
        for arm in ARMS:
            values = [row["publish_msg_per_sec"] / 1e6 for row in rows
                      if row["configuration"] == configuration and row["arm"] == arm]
            arms[arm] = {"summary": summarize(values), "values": values}
        groups = []
        for group in range(GROUPS):
            group_values = {}
            for arm in ARMS:
                values = [row["publish_msg_per_sec"] / 1e6 for row in rows
                          if row["configuration"] == configuration and row["group"] == group
                          and row["arm"] == arm]
                group_values[arm] = {"values": values, "median": statistics.median(values)}
            ratio = group_values["ST"]["median"] / group_values["MT"]["median"]
            groups.append({"group": group, **group_values, "st_over_mt_median": ratio})
        ratios = [group["st_over_mt_median"] for group in groups]
        arms["ST_over_MT"] = {
            "group_median_ratios": ratios,
            "median": statistics.median(ratios),
            "min": min(ratios), "max": max(ratios),
            "st_faster_groups": sum(ratio > 1 for ratio in ratios),
            "arm_median_ratio": (arms["ST"]["summary"]["median"] /
                                 arms["MT"]["summary"]["median"]),
            "arm_mean_ratio": (arms["ST"]["summary"]["mean"] /
                               arms["MT"]["summary"]["mean"]),
        }
        arms["group_summaries"] = groups
        throughput[configuration] = arms

    latency = {}
    for configuration in (name for name, item in CONFIGURATIONS.items()
                          if item["kind"] == "latency"):
        latency[configuration] = {}
        for arm in ARMS:
            selected = [row for row in rows
                        if row["configuration"] == configuration and row["arm"] == arm]
            latency[configuration][arm] = {
                key: summarize([row[key] for row in selected]) for key in LATENCY_KEYS
            }
    return {
        "schema_version": 1,
        "protocol": PROTOCOL,
        "evidence": {
            "directory": str(directory),
            "manifest_sha256": digest(manifest_path),
            "client_sha256": digest(client),
            "source_sha256": V3_SOURCE_SHA256,
            "measured_rounds": len(rows),
            "smoke_rounds": sum(record.get("phase") == "smoke"
                                for record in manifest["records"]),
            "warmup_rounds": sum(record.get("phase") == "warmup"
                                 for record in manifest["records"]),
            "all_rounds_complete": True,
            "all_thread_states_valid": True,
        },
        "throughput": throughput,
        "latency": latency,
        "interpretation_boundary": (
            "ST and MT differ by one sleeping helper thread in the publisher process. "
            "The matrix measures the cost of a multithreaded publisher process, not the "
            "isolated std::shared_ptr reference-count cost. Other libc/runtime state and "
            "host variance remain confounded."
        ),
    }


def markdown(document: dict[str, Any]) -> str:
    lines = ["# P3-2 single-thread versus multithread publisher", "",
             f"Protocol: `{document['protocol']}`.", "",
             "| Configuration | Arm | Median Mmsg/s | Mean | CV | Range |",
             "|---|---|---:|---:|---:|---:|"]
    for configuration, arms in document["throughput"].items():
        for arm in ARMS:
            item = arms[arm]["summary"]
            lines.append(f"| {configuration} | {arm} | {item['median']:.6f} | "
                         f"{item['mean']:.6f} | {item['cv_percent']:.2f}% | "
                         f"{item['min']:.6f}-{item['max']:.6f} |")
    lines += ["", "## Group ratios", "",
              "| Configuration | Group | ST median | MT median | ST/MT |",
              "|---|---:|---:|---:|---:|"]
    for configuration, arms in document["throughput"].items():
        for group in range(GROUPS):
            pass
    # Re-read grouped details from the document without changing the JSON contract.
    for configuration, arms in document["throughput"].items():
        ratios = arms["ST_over_MT"]["group_median_ratios"]
        for group, ratio in enumerate(ratios):
            st_median = arms["group_summaries"][group]["ST"]["median"]
            mt_median = arms["group_summaries"][group]["MT"]["median"]
            lines.append(f"| {configuration} | {group} | {st_median:.6f} | "
                         f"{mt_median:.6f} | {ratio:.6f} |")
        lines.append(f"| {configuration} | **all** | - | - | median "
                     f"{arms['ST_over_MT']['median']:.6f}; "
                     f"{arms['ST_over_MT']['st_faster_groups']}/{GROUPS} groups |")
    lines += ["", "## Latency", "",
              "| Configuration | Arm | Metric | Median ns | Min ns | Max ns |",
              "|---|---|---|---:|---:|---:|"]
    for configuration, arms in document["latency"].items():
        for arm in ARMS:
            for key in LATENCY_KEYS:
                item = arms[arm][key]
                lines.append(f"| {configuration} | {arm} | {key} | {item['median']:.0f} | "
                             f"{item['min']:.0f} | {item['max']:.0f} |")
    lines += ["", document["interpretation_boundary"], ""]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--format", choices=("json", "markdown"), default="json")
    args = parser.parse_args()
    document = analyze(args.directory)
    print(json.dumps(document, indent=2) if args.format == "json" else markdown(document))


if __name__ == "__main__":
    main()
