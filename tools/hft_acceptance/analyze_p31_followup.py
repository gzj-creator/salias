#!/usr/bin/env python3
"""Audit and summarize the preserved P3-1 HFT evidence without re-measuring.

The analyzer verifies the frozen manifest, client, inputs, formal raw logs and
worker roles. It then exposes publisher and subscriber evidence per group,
configuration, arm and round. Progress snapshots, measurement-cutoff counters,
and the later drain/reap completion are kept as separate observations.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
from typing import Any


ARMS = {"S": {"SALIAS_DISABLE_ASYMMETRIC_FENCE": "1"}, "A": {}}
ORDER = ("S", "A", "A", "S")
CONFIGURATIONS = tuple(
    f"2p{s}s-{payload}b-{pattern}"
    for s in (1, 2)
    for payload in (64, 256)
    for pattern in ("steady", "burst")
)
METRICS = ("worst_planned_p99_ns", "worst_planned_p999_ns")
PROTOCOL = {
    "name": "P3-1 G4 HFT non-inferiority",
    "groups": 3,
    "seconds_per_run": 30,
    "order": list(ORDER),
    "configurations": list(CONFIGURATIONS),
    "environment": ARMS,
    "metrics": list(METRICS),
    "maximum_candidate_percent_of_seq": 105,
    "every_group_and_configuration_required": True,
    "integrity_and_original_input_completion_required": True,
    "independent_50us_budget_is_not_the_relative_gate": True,
}
FORMAL_RUNS = 96
EXPECTED_INPUTS = 50
EXPECTED_WARMUPS = 16
RESOURCE_KEYS = (
    "user_us",
    "system_us",
    "voluntary_context_switches",
    "involuntary_context_switches",
    "minor_faults",
    "major_faults",
)
HISTOGRAM_KEYS = ("samples", "p50_ns", "p99_ns", "p999_ns", "max_sampled_ns")


class AuditError(ValueError):
    """The evidence differs from the frozen identity or expected shape."""


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise AuditError(message)


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _require_nonnegative_mapping(mapping: dict[str, Any], keys: tuple[str, ...], label: str) -> None:
    _require(isinstance(mapping, dict), f"{label} must be an object")
    for key in keys:
        _require(key in mapping, f"{label}.{key} is missing")
        _require(_is_number(mapping[key]) and mapping[key] >= 0, f"{label}.{key} is invalid")


def _configuration_identity(name: str) -> tuple[int, int, str]:
    subscribers, payload, pattern = name.split("-")
    _require(subscribers in ("2p1s", "2p2s"), f"invalid subscriber configuration: {name}")
    _require(payload in ("64b", "256b"), f"invalid payload configuration: {name}")
    _require(pattern in ("steady", "burst"), f"invalid traffic configuration: {name}")
    return int(subscribers[2]), int(payload[:-1]), pattern


def _expected_command(client: str, name: str, seconds: int) -> list[str]:
    subscribers, payload, pattern = _configuration_identity(name)
    return [
        "timeout",
        "--signal=TERM",
        "--kill-after=10s",
        f"{seconds + 40}s",
        client,
        "--subscribers",
        str(subscribers),
        "--payload",
        str(payload),
        "--seconds",
        str(seconds),
        "--pattern",
        pattern,
        "--rate",
        "1000000",
        "--peak-rate",
        "4000000",
        "--burst-ms",
        "100",
        "--cpu-base",
        "0",
    ]


def _safe_artifact(directory: Path, relative: str, label: str) -> Path:
    _require(isinstance(relative, str) and relative, f"{label} path is missing")
    candidate = (directory / relative).resolve()
    _require(directory in candidate.parents, f"{label} escapes the evidence directory: {relative}")
    _require(candidate.is_file(), f"{label} is missing: {relative}")
    return candidate


def _verify_artifact(directory: Path, record: dict[str, Any], stem: str) -> Path:
    path = _safe_artifact(directory, record.get(stem), stem)
    _require(digest(path) == record.get(f"{stem}_sha256"), f"{stem} hash mismatch: {path}")
    return path


def _load_manifest(directory: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    manifest_path = directory / "experiment.json"
    _require(manifest_path.is_file(), f"manifest is missing: {manifest_path}")
    try:
        manifest = json.loads(manifest_path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise AuditError(f"manifest is not valid JSON: {error}") from error
    _require(isinstance(manifest, dict), "manifest root must be an object")
    _require(manifest.get("protocol") == PROTOCOL, "protocol differs from the frozen specification")

    client = Path(manifest.get("client", ""))
    _require(client.is_file(), f"client is missing: {client}")
    _require(digest(client) == manifest.get("client_sha256"), "client hash changed")

    inputs = manifest.get("inputs")
    _require(isinstance(inputs, dict), "manifest.inputs must be an object")
    _require(len(inputs) == EXPECTED_INPUTS, f"expected {EXPECTED_INPUTS} frozen inputs")
    input_paths: set[Path] = set()
    for raw_path, expected_sha in inputs.items():
        path = Path(raw_path).resolve()
        _require(path not in input_paths, f"duplicate frozen input: {raw_path}")
        _require(path.is_file(), f"frozen input is missing: {raw_path}")
        _require(digest(path) == expected_sha, f"frozen input changed: {raw_path}")
        input_paths.add(path)

    runs = manifest.get("runs")
    _require(isinstance(runs, list) and len(runs) == FORMAL_RUNS,
             f"expected {FORMAL_RUNS} formal runs")
    expected_identities = [
        (group, name, round_number, arm)
        for group in range(1, 4)
        for name in CONFIGURATIONS
        for round_number, arm in enumerate(ORDER)
    ]
    raw_paths: set[Path] = set()
    stderr_paths: set[Path] = set()
    for run, identity in zip(runs, expected_identities):
        actual_identity = tuple(run.get(key) for key in ("group", "configuration", "round", "arm"))
        _require(actual_identity == identity, "formal run order or identity mismatch")
        group, name, round_number, arm = identity
        relative = f"group-{group}/round-{round_number}/{arm}/{name}.jsonl"
        _require(run.get("raw") == relative, f"formal raw path mismatch: {relative}")
        _require(run.get("environment") == ARMS[arm], f"formal environment mismatch: {relative}")
        _require(run.get("client_sha256") == manifest["client_sha256"],
                 f"formal client hash mismatch: {relative}")
        _require(run.get("command") == _expected_command(manifest["client"], name, 30),
                 f"formal command mismatch: {relative}")
        _require(run.get("returncode") == 0, f"formal process failed: {relative}")
        raw_paths.add(_verify_artifact(directory, run, "raw"))
        stderr_paths.add(_verify_artifact(directory, run, "stderr"))

    observed_raw = set(directory.glob("group-*/round-*/*/*.jsonl"))
    observed_stderr = set(directory.glob("group-*/round-*/*/*.stderr.txt"))
    _require(observed_raw == raw_paths, "unexpected or missing formal JSONL files")
    _require(observed_stderr == stderr_paths, "unexpected or missing formal stderr files")

    warmups = manifest.get("warmup")
    _require(isinstance(warmups, list) and len(warmups) == EXPECTED_WARMUPS,
             f"expected {EXPECTED_WARMUPS} warmup runs")
    expected_warmups = [
        (name, round_number, arm)
        for name in CONFIGURATIONS
        for round_number, arm in enumerate(("S", "A"))
    ]
    for run, identity in zip(warmups, expected_warmups):
        name, round_number, arm = identity
        actual_identity = (run.get("configuration"), run.get("round"), run.get("arm"))
        _require(run.get("group") == 0 and actual_identity == identity,
                 "warmup identity mismatch")
        relative = f"warmup/{arm}/{name}.jsonl"
        _require(run.get("raw") == relative, f"warmup raw path mismatch: {relative}")
        _require(run.get("environment") == ARMS[arm], f"warmup environment mismatch: {relative}")
        _require(run.get("client_sha256") == manifest["client_sha256"],
                 f"warmup client hash mismatch: {relative}")
        _require(run.get("command") == _expected_command(manifest["client"], name, 2),
                 f"warmup command mismatch: {relative}")
        _require(run.get("returncode") == 0, f"warmup process failed: {relative}")
        _verify_artifact(directory, run, "raw")
        _verify_artifact(directory, run, "stderr")

    identity = {
        "manifest": str(manifest_path.resolve()),
        "manifest_sha256": digest(manifest_path),
        "client": str(client.resolve()),
        "client_sha256": manifest["client_sha256"],
        "frozen_inputs": len(inputs),
        "formal_runs": len(runs),
        "formal_raw_sha256": len(raw_paths),
        "formal_stderr_sha256": len(stderr_paths),
        "warmup_runs": len(warmups),
        "status": "verified",
    }
    return manifest, identity


def _load_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        text = path.read_text()
    except OSError as error:
        raise AuditError(f"cannot read run log {path}: {error}") from error
    for line_number, line in enumerate(text.splitlines(), 1):
        _require(line.strip(), f"blank JSONL line {path}:{line_number}")
        try:
            row = json.loads(line)
        except json.JSONDecodeError as error:
            raise AuditError(f"invalid JSONL at {path}:{line_number}: {error}") from error
        _require(isinstance(row, dict), f"non-object JSONL row at {path}:{line_number}")
        rows.append(row)
    return rows


def _validate_start(start: dict[str, Any], name: str) -> None:
    subscribers, payload, pattern = _configuration_identity(name)
    expected_planned = 30 * (1_300_000 if pattern == "burst" else 1_000_000)
    expected = {
        "duration_s": 30,
        "subscribers": subscribers,
        "payload_bytes": payload,
        "pattern": pattern,
        "steady_total_rate": 1_000_000,
        "peak_total_rate": 4_000_000,
        "burst_ms_per_second": 100 if pattern == "burst" else 0,
        "planned_published": expected_planned,
        "capacity_per_publisher_bytes": 4_194_304,
        "publication_window_bytes": 131_072,
        "batch": 1,
        "poll_limit": 64,
        "sample_hash_mask": 63,
        "cpus": list(range(subscribers + 2)),
        "diagnostics": False,
        "stall_subscriber_ms": 0,
    }
    for key, value in expected.items():
        _require(start.get(key) == value, f"start.{key} differs from protocol")
    _require(_is_int(start.get("start_monotonic_ns")) and start["start_monotonic_ns"] > 0,
             "start.start_monotonic_ns is invalid")


def _validate_histogram(value: Any, label: str) -> None:
    _require(isinstance(value, dict), f"{label} must be an object")
    for key in HISTOGRAM_KEYS:
        _require(key in value and _is_number(value[key]) and value[key] >= 0,
                 f"{label}.{key} is invalid")


def _validate_result(result: dict[str, Any], name: str, subscriber_count: int) -> None:
    planned = result.get("planned_published")
    published = result.get("published")
    delivered = result.get("delivered")
    unsent = result.get("unsent_at_cutoff")
    _require(result.get("measurement_seconds") == 30, "result.measurement_seconds mismatch")
    _require(_is_int(planned) and planned == 30 * (1_300_000 if name.endswith("burst") else 1_000_000),
             "result.planned_published mismatch")
    _require(_is_int(published) and 0 <= published <= planned, "result.published is invalid")
    _require(_is_int(unsent) and unsent == planned - published,
             "result.unsent_at_cutoff is inconsistent")
    _require(_is_number(result.get("wall_seconds_including_drain_and_reap")) and
             result["wall_seconds_including_drain_and_reap"] >= 30,
             "result.wall_seconds_including_drain_and_reap is invalid")
    expected_rate = published / 30
    # The C++ writer uses default six-significant-digit formatting for this field.
    _require(_is_number(result.get("actual_published_msg_s")) and
             math.isclose(result["actual_published_msg_s"], expected_rate, rel_tol=1e-5, abs_tol=1e-6),
             "result.actual_published_msg_s is inconsistent")
    _require(isinstance(result.get("integrity_pass"), bool), "result.integrity_pass is invalid")
    _require(isinstance(result.get("worker_failure"), bool), "result.worker_failure is invalid")

    publishers = result.get("publishers")
    _require(isinstance(publishers, list) and len(publishers) == 2,
             "result must contain exactly two publishers")
    _require([publisher.get("publisher") for publisher in publishers] == [0, 1],
             "publisher role ids must be [0, 1]")
    per_publisher_planned = planned // 2
    sent_total = 0
    unsent_total = 0
    for publisher in publishers:
        label = f"publisher[{publisher['publisher']}]"
        numeric = (
            "sent", "unsent", "backpressure_retries", "backpressured_messages",
            "backpressure_episode_ns", "max_backpressure_episode_ns",
            "unsent_backlog_highwater_observed",
        )
        _require_nonnegative_mapping(publisher, numeric, label)
        _require(publisher["sent"] <= per_publisher_planned, f"{label}.sent exceeds plan")
        _require(publisher["unsent"] == per_publisher_planned - publisher["sent"],
                 f"{label}.unsent is inconsistent")
        _require(publisher["max_backpressure_episode_ns"] <= publisher["backpressure_episode_ns"],
                 f"{label} backpressure maximum exceeds its sum")
        _require(publisher["backpressured_messages"] <= publisher["sent"] + unsent,
                 f"{label}.backpressured_messages is invalid")
        _validate_histogram(publisher.get("first_attempt_to_commit"),
                            f"{label}.first_attempt_to_commit")
        _validate_histogram(publisher.get("planned_to_first_attempt"),
                            f"{label}.planned_to_first_attempt")
        _require_nonnegative_mapping(publisher.get("resources"), RESOURCE_KEYS,
                                     f"{label}.resources")
        sent_total += publisher["sent"]
        unsent_total += publisher["unsent"]
    _require(sent_total == published, "publisher sent sum differs from result.published")
    _require(unsent_total == unsent, "publisher unsent sum differs from result.unsent_at_cutoff")

    subscribers = result.get("subscribers")
    _require(isinstance(subscribers, list) and len(subscribers) == subscriber_count,
             "result subscriber count differs from the configuration")
    _require([subscriber.get("subscriber") for subscriber in subscribers] == list(range(subscriber_count)),
             "subscriber role ids differ from 0..N-1")
    received_total = 0
    for subscriber in subscribers:
        label = f"subscriber[{subscriber['subscriber']}]"
        for key in ("received", "unreceived_at_measurement_cutoff_observed", "unreceived_after_drain"):
            _require(_is_int(subscriber.get(key)) and subscriber[key] >= 0,
                     f"{label}.{key} is invalid")
        _require(subscriber["received"] <= planned, f"{label}.received exceeds plan")
        _require(subscriber["unreceived_after_drain"] == planned - subscriber["received"],
                 f"{label}.unreceived_after_drain is inconsistent")
        _require(isinstance(subscriber.get("integrity_pass"), bool), f"{label}.integrity_pass is invalid")
        _validate_histogram(subscriber.get("from_first_attempt"), f"{label}.from_first_attempt")
        _validate_histogram(subscriber.get("from_planned_arrival"), f"{label}.from_planned_arrival")
        _require_nonnegative_mapping(subscriber.get("resources"), RESOURCE_KEYS,
                                     f"{label}.resources")
        sequence = subscriber.get("sequence")
        _require(isinstance(sequence, list) and len(sequence) == 2,
                 f"{label} must contain two publisher sequences")
        _require([item.get("publisher") for item in sequence] == [0, 1],
                 f"{label} publisher sequence ids must be [0, 1]")
        for item in sequence:
            sequence_label = f"{label}.sequence[{item['publisher']}]"
            for key in ("received", "unique", "missing_published", "duplicates", "out_of_order",
                        "gap_events", "gap_positions_observed", "corrupt", "invalid"):
                _require(_is_int(item.get(key)) and item[key] >= 0,
                         f"{sequence_label}.{key} is invalid")
            _require(_is_int(item.get("channel_producer_id")),
                     f"{sequence_label}.channel_producer_id is invalid")
            publisher_sent = publishers[item["publisher"]]["sent"]
            _require(item["unique"] <= publisher_sent, f"{sequence_label}.unique exceeds sent")
            _require(item["missing_published"] == max(0, publisher_sent - item["unique"]),
                     f"{sequence_label}.missing_published is inconsistent")
        _require(sequence[0]["channel_producer_id"] != sequence[1]["channel_producer_id"],
                 f"{label} channel producer ids are not distinct")
        received_total += subscriber["received"]
    _require(delivered == received_total, "result.delivered differs from subscriber received sum")
    expected_completed = unsent == 0 and delivered == published * subscriber_count
    _require(result.get("offered_load_completed") == expected_completed,
             "result.offered_load_completed is inconsistent")


def _progress_observation(progress: list[dict[str, Any]]) -> dict[str, Any]:
    _require(progress, "run has no progress observations")
    previous_elapsed = -1.0
    previous_due = -1
    for index, row in enumerate(progress):
        elapsed = row.get("elapsed_s")
        due = row.get("due")
        sent = row.get("sent")
        received = row.get("received")
        _require(_is_number(elapsed) and elapsed >= previous_elapsed,
                 f"progress[{index}].elapsed_s is not monotonic")
        _require(_is_int(due) and due >= previous_due,
                 f"progress[{index}].due is not monotonic")
        _require(isinstance(sent, list) and len(sent) == 2 and
                 all(_is_int(value) and value >= 0 for value in sent),
                 f"progress[{index}].sent shape is invalid")
        _require(isinstance(received, list) and received and
                 all(_is_int(value) and value >= 0 for value in received),
                 f"progress[{index}].received shape is invalid")
        previous_elapsed = elapsed
        previous_due = due
    last = progress[-1]
    sent_total = sum(last["sent"])
    return {
        "count": len(progress),
        "last": {
            "elapsed_s": last["elapsed_s"],
            "due": last["due"],
            "sent": last["sent"],
            "received": last["received"],
            "due_minus_sent_total_lower_bound": max(0, last["due"] - sent_total),
        },
        "is_measurement_cutoff": False,
        "counter_semantics": "independent periodic atomic snapshots; sent advances every 256 messages",
    }


def _run_analysis(directory: Path, run: dict[str, Any], rows: list[dict[str, Any]]) -> dict[str, Any]:
    name = run["configuration"]
    subscriber_count, _, _ = _configuration_identity(name)
    starts = [index for index, row in enumerate(rows) if row.get("type") == "start"]
    results = [index for index, row in enumerate(rows) if row.get("type") == "result"]
    _require(starts == [0], f"run must have exactly one leading start record: {run['raw']}")
    _require(results == [len(rows) - 1], f"run must have exactly one trailing result record: {run['raw']}")
    _require(all(row.get("type") == "progress" for row in rows[1:-1]),
             f"run contains an unknown record type: {run['raw']}")
    start, result = rows[0], rows[-1]
    _validate_start(start, name)
    _validate_result(result, name, subscriber_count)
    progress = rows[1:-1]
    progress_summary = _progress_observation(progress)
    _require(len(progress_summary["last"]["received"]) == subscriber_count,
             "last progress subscriber shape differs from the configuration")
    _require(result["planned_published"] == start["planned_published"],
             "start and result planned_published differ")

    publishers = []
    for publisher in result["publishers"]:
        publishers.append({
            "publisher": publisher["publisher"],
            "sent": publisher["sent"],
            "unsent": publisher["unsent"],
            "backpressure_retries": publisher["backpressure_retries"],
            "backpressured_messages": publisher["backpressured_messages"],
            "backpressure_episode_ns": publisher["backpressure_episode_ns"],
            "max_backpressure_episode_ns": publisher["max_backpressure_episode_ns"],
            "unsent_backlog_highwater_observed": publisher["unsent_backlog_highwater_observed"],
            "backlog_sampling_interval_messages": 256,
            "planned_to_first_attempt": {
                key: publisher["planned_to_first_attempt"][key]
                for key in HISTOGRAM_KEYS
            },
            "resources": {key: publisher["resources"][key] for key in RESOURCE_KEYS},
        })
    subscribers = []
    for subscriber in result["subscribers"]:
        subscribers.append({
            "subscriber": subscriber["subscriber"],
            "received": subscriber["received"],
            "unreceived_at_measurement_cutoff_observed":
                subscriber["unreceived_at_measurement_cutoff_observed"],
            "unreceived_after_drain": subscriber["unreceived_after_drain"],
            "integrity_pass": subscriber["integrity_pass"],
            "resources": {key: subscriber["resources"][key] for key in RESOURCE_KEYS},
        })
    return {
        "group": run["group"],
        "configuration": name,
        "round": run["round"],
        "arm": run["arm"],
        "raw": run["raw"],
        "raw_sha256": run["raw_sha256"],
        "identity_verified": True,
        "complete": result["offered_load_completed"],
        "integrity_pass": result["integrity_pass"],
        "worker_failure": result["worker_failure"],
        "progress_observation": progress_summary,
        "measurement_cutoff": {
            "planned_published": result["planned_published"],
            "published": result["published"],
            "unsent": result["unsent_at_cutoff"],
            "subscriber_unreceived_observed": [
                {
                    "subscriber": subscriber["subscriber"],
                    "unreceived": subscriber["unreceived_at_measurement_cutoff_observed"],
                }
                for subscriber in result["subscribers"]
            ],
            "source": "result counters; consumer cutoff is detected on the first poll after the 30 s boundary",
        },
        "drain_reap_end": {
            "wall_seconds_including_drain_and_reap": result["wall_seconds_including_drain_and_reap"],
            "published": result["published"],
            "delivered": result["delivered"],
            "subscriber_unreceived_after_drain": [
                {
                    "subscriber": subscriber["subscriber"],
                    "unreceived": subscriber["unreceived_after_drain"],
                }
                for subscriber in result["subscribers"]
            ],
            "cutoff_to_drain_elapsed_available": False,
            "reason": "result records one combined wall duration, not cutoff and drain/reap timestamps",
        },
        "publishers": publishers,
        "subscribers": subscribers,
        "role_structure": {
            "publisher_count": len(publishers),
            "subscriber_count": len(subscribers),
            "configuration_subscribers": subscriber_count,
            "status": "verified",
        },
    }


def _candidate_assessment(runs: list[dict[str, Any]]) -> dict[str, Any]:
    incomplete = [run for run in runs if not run["complete"]]
    backpressured = [
        run for run in incomplete
        if any(publisher["backpressure_retries"] or publisher["backpressure_episode_ns"]
               for publisher in run["publishers"])
    ]
    tiny_shortfalls = [run for run in incomplete if run["measurement_cutoff"]["unsent"] <= 100]
    arms = sorted({run["arm"] for run in incomplete})
    candidates = []
    if len(arms) == 1:
        candidates.append({
            "rank": len(candidates) + 1,
            "candidate": f"{arms[0]}-arm-specific publication progress",
            "evidence": f"all {len(incomplete)} incomplete formal runs are arm {arms[0]}",
            "proven": False,
            "boundary": "this is an observed association; the evidence does not establish a product-code cause",
        })
    if backpressured:
        candidates.append({
            "rank": len(candidates) + 1,
            "candidate": "publisher backpressure and publication backlog",
            "evidence": f"{len(backpressured)}/{len(incomplete)} incomplete runs contain nonzero publisher retry/backpressure counters",
            "proven": False,
            "boundary": "backpressure counters can be a symptom of downstream or scheduling delay; they do not identify its source",
        })
    if tiny_shortfalls:
        candidates.append({
            "rank": len(candidates) + 1,
            "candidate": "end-of-window arrival or cutoff boundary",
            "evidence": f"{len(tiny_shortfalls)} incomplete run(s) have at most 100 unsent messages",
            "proven": False,
            "boundary": "the preserved logs have no synchronized trace at the final arrival and stop transition",
        })
    candidates.append({
        "rank": len(candidates) + 1,
        "candidate": "host scheduling interference",
        "evidence": "incomplete publisher roles record large planned-to-attempt tails and involuntary context switches",
        "proven": False,
        "boundary": "there is no synchronized off-CPU trace or host-wide causal marker in the preserved input",
    })
    return {
        "ranking": candidates,
        "explicitly_not_proven": [
            "host scheduler as root cause",
            "CPU cache or cache-line contention as root cause",
            "a specific product-code defect as root cause",
        ],
    }


def analyze(directory: Path) -> dict[str, Any]:
    directory = Path(directory).resolve()
    _require(directory.is_dir(), f"evidence directory is missing: {directory}")
    manifest, identity = _load_manifest(directory)
    runs = []
    for run in manifest["runs"]:
        rows = _load_rows(directory / run["raw"])
        runs.append(_run_analysis(directory, run, rows))

    incomplete = [
        {
            "group": run["group"],
            "configuration": run["configuration"],
            "round": run["round"],
            "arm": run["arm"],
            "unsent": run["measurement_cutoff"]["unsent"],
        }
        for run in runs if not run["complete"]
    ]
    _require(len(incomplete) == 4, f"expected the four preserved incomplete S runs, found {len(incomplete)}")
    _require(all(run["arm"] == "S" for run in incomplete),
             "preserved incomplete-run arm identity changed")
    return {
        "schema_version": 1,
        "evidence": identity,
        "p3_1_g4": {
            "conclusion": "FAIL",
            "status": "preserved; this analysis does not re-adjudicate or replace the formal gate",
        },
        "run_inventory": {
            "formal_runs": len(runs),
            "complete": sum(run["complete"] for run in runs),
            "incomplete": incomplete,
            "integrity_pass": sum(run["integrity_pass"] for run in runs),
            "worker_failures": sum(run["worker_failure"] for run in runs),
        },
        "observation_boundaries": {
            "progress": "periodic 1 s snapshot; not the measurement cutoff",
            "measurement_cutoff": "result publisher totals and first observed subscriber cutoff counters",
            "drain_reap": "later completion captured by delivered and unreceived_after_drain plus one combined wall duration",
        },
        "runs": runs,
        "root_cause_assessment": _candidate_assessment(runs),
    }


def _print_markdown(document: dict[str, Any]) -> None:
    evidence = document["evidence"]
    inventory = document["run_inventory"]
    print("# P3-1 HFT historical evidence analysis\n")
    print("P3-1 G4 remains **FAIL**. This diagnostic does not re-adjudicate or replace the formal gate.\n")
    print(f"- Manifest: `{evidence['manifest']}` (`{evidence['manifest_sha256']}`)")
    print(f"- Client: `{evidence['client_sha256']}`")
    print(f"- Verified: {evidence['frozen_inputs']} frozen inputs, "
          f"{evidence['formal_runs']} formal JSONL/stderr pairs, {evidence['warmup_runs']} warmups")
    print(f"- Formal inventory: {inventory['formal_runs']} runs, {inventory['complete']} complete, "
          f"{len(inventory['incomplete'])} incomplete, {inventory['worker_failures']} worker failures\n")
    print("## Incomplete Runs\n")
    print("| Group | Configuration | Round | Arm | Unsent |")
    print("|---:|---|---:|---|---:|")
    for run in inventory["incomplete"]:
        print(f"| {run['group']} | {run['configuration']} | {run['round']} | {run['arm']} | {run['unsent']:,} |")
    print("\n## Per Publisher\n")
    print("| G/R | Configuration | Arm | P | Unsent | Retries | BP msgs | BP ms | Max BP ms | Backlog HW | Schedule p999 us | Invol. ctx |")
    print("|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for run in document["runs"]:
        for publisher in run["publishers"]:
            schedule = publisher["planned_to_first_attempt"]
            resources = publisher["resources"]
            print(
                f"| {run['group']}/{run['round']} | {run['configuration']} | {run['arm']} | "
                f"{publisher['publisher']} | {publisher['unsent']:,} | "
                f"{publisher['backpressure_retries']:,} | {publisher['backpressured_messages']:,} | "
                f"{publisher['backpressure_episode_ns']/1e6:.3f} | "
                f"{publisher['max_backpressure_episode_ns']/1e6:.3f} | "
                f"{publisher['unsent_backlog_highwater_observed']:,} | "
                f"{schedule['p999_ns']/1e3:.3f} | "
                f"{resources['involuntary_context_switches']:,} |"
            )
    print("\n## Candidate Ranking\n")
    for item in document["root_cause_assessment"]["ranking"]:
        print(f"{item['rank']}. **{item['candidate']}** (unproven): {item['evidence']}. "
              f"Boundary: {item['boundary']}.")
    print("\nExplicitly unproven: " + "; ".join(document["root_cause_assessment"]["explicitly_not_proven"]) + ".")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "directory",
        nargs="?",
        default="build/benchmarks/p3-p31-acceptance-20261021-155122/hft-tail-formal",
        type=Path,
    )
    parser.add_argument("--format", choices=("json", "markdown"), default="json")
    args = parser.parse_args()
    try:
        document = analyze(args.directory)
    except AuditError as error:
        parser.exit(2, f"evidence audit failed: {error}\n")
    if args.format == "markdown":
        _print_markdown(document)
    else:
        print(json.dumps(document, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
