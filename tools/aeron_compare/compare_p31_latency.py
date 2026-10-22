#!/usr/bin/env python3
"""Audit the P3-1 latency arms (G4) written by run_latency_compare.py.

The runner records one row per (arm, origin, payload, round) together with the
arm label, the client hash and the exact environment override.  This tool
verifies those identities, then reports median run quantiles per arm so the
pre-registered G4 rule can be applied without pooling different configurations.

G4 (plan §6.3):
  * worst-subscriber *planned* p50 of C-asym must not exceed C-seq + 10 ns;
  * p99/p99.9 are judged non-inferior by the HFT tail tool (reported separately);
  * published-message completion must not decrease.
"""

import argparse
import hashlib
import json
from pathlib import Path
import statistics


BASELINE_SHA256 = "3f8fc6957ce56483d7051dd6625f44fd8f755d57a66d04e1e5efa48e65b9c7da"
ARM_ENVIRONMENT = {"B": {}, "S": {"SALIAS_DISABLE_ASYMMETRIC_FENCE": "1"}, "A": {}}
METRICS = ("p50", "p99", "p999")
MEASURED_ROUNDS = tuple(range(1, 11))
G4_P50_ALLOWANCE_NS = 10


class LatencyAnalysisError(RuntimeError):
    """Raised when an arm record set is inconsistent or incomplete."""


def load_arms(directory, baseline_sha256=BASELINE_SHA256):
    """Return the verified arm identity map for one runner output directory."""
    path = Path(directory) / "arms.json"
    if not path.is_file():
        raise LatencyAnalysisError(f"missing arms.json: {directory}")
    entries = json.loads(path.read_text())
    arms = {entry["arm"]: entry for entry in entries}
    if len(entries) != 3 or set(arms) != set(ARM_ENVIRONMENT):
        raise LatencyAnalysisError(f"arms are {sorted(arms)}, expected B/S/A")
    for arm, entry in arms.items():
        actual = hashlib.sha256(Path(entry["client"]).read_bytes()).hexdigest()
        if actual != entry["client_sha256"]:
            raise LatencyAnalysisError(f"arm {arm} client file does not match its hash")
    if arms["B"]["client_sha256"] != baseline_sha256:
        raise LatencyAnalysisError("arm B is not the frozen v3.0.0 client")
    if (arms["S"]["client_sha256"] != arms["A"]["client_sha256"] or
            Path(arms["S"]["client"]).resolve() != Path(arms["A"]["client"]).resolve()):
        raise LatencyAnalysisError("C-seq and C-asym must be the same binary")
    for arm, expected in ARM_ENVIRONMENT.items():
        if arms[arm]["environment"] != expected:
            raise LatencyAnalysisError(
                f"arm {arm} environment is {arms[arm]['environment']}, expected {expected}"
            )
    return arms


def verify_raw_row(directory, row):
    """Cross-check recorded fields against the hashed original stdout/stderr."""
    for kind in ("stdout", "stderr"):
        name = row.get(kind, "")
        if not name or Path(name).name != name:
            raise LatencyAnalysisError(f"missing or invalid {kind} filename")
        path = Path(directory) / name
        if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != row.get(
                f"{kind}_sha256"):
            raise LatencyAnalysisError(f"{kind} missing or hash mismatch: {path}")
    lines = (Path(directory) / row["stdout"]).read_text().splitlines()
    results = [dict(item.split("=", 1) for item in line.split()[1:])
               for line in lines if line.startswith("RESULT ")]
    consumers = [dict(item.split("=", 1) for item in line.split()[1:])
                 for line in lines if line.startswith("LATENCY ")]
    if results != [row["result"]] or consumers != row.get("consumers"):
        raise LatencyAnalysisError("raw RESULT/LATENCY differs from JSON row")


def load_rows(directory, arms):
    """Read every row and verify arm identity, memory pressure and structure."""
    path = Path(directory) / "results.jsonl"
    if not path.is_file():
        raise LatencyAnalysisError(f"missing results.jsonl: {directory}")
    rows = []
    for line_number, line in enumerate(path.read_text().splitlines(), 1):
        if not line.strip():
            continue
        row = json.loads(line)
        if row.get("engine") != "fifo":
            raise LatencyAnalysisError(f"{path}:{line_number}: engine is not fifo")
        arm = row.get("arm")
        if arm not in arms:
            raise LatencyAnalysisError(f"{path}:{line_number}: unknown arm {arm}")
        if row.get("client_sha256") != arms[arm]["client_sha256"]:
            raise LatencyAnalysisError(f"{path}:{line_number}: client hash does not match arm")
        if row.get("arm_environment") != ARM_ENVIRONMENT[arm]:
            raise LatencyAnalysisError(f"{path}:{line_number}: arm environment mismatch")
        if (row.get("origin") not in ("attempt", "planned") or
                row.get("payload") not in (64, 256) or row.get("rate") != 1_000_000 or
                row.get("producers") != 1 or row.get("duration_s") != 2):
            raise LatencyAnalysisError(f"{path}:{line_number}: not the registered latency matrix")
        if (row.get("phase"), row.get("round")) not in (
                [("warmup", 0)] + [("measured", number) for number in MEASURED_ROUNDS]):
            raise LatencyAnalysisError(f"{path}:{line_number}: invalid phase or round")
        verify_raw_row(directory, row)
        result = row["result"]
        expected = {
            "library": "salias-ipc", "scenario": "fifo", "producers": "1", "consumers": "1",
            "payload": str(row["payload"]), "capacity": "1048576", "batch_size": "1",
            "poll_limit": "64", "published": "2000000", "delivered": "2000000",
            "latency_samples": "125000", "latency_sample_rate": "16",
            "rate_per_producer": "1000000", "latency_origin": row["origin"],
        }
        if any(result.get(key) != value for key, value in expected.items()):
            raise LatencyAnalysisError(f"{path}:{line_number}: result settings or completion mismatch")
        if (len(row["consumers"]) != 1 or row["consumers"][0].get("samples") != "125000" or
                any(row["consumers"][0].get(f"{metric}_ns") != result.get(f"latency_{metric}_ns")
                    for metric in METRICS)):
            raise LatencyAnalysisError(f"{path}:{line_number}: subscriber counts or quantiles mismatch")
        events = dict(item.split() for item in row["resources"]["memory.events"].splitlines())
        if any(int(events.get(name, 0)) for name in ("high", "max", "oom", "oom_kill")):
            raise LatencyAnalysisError(f"{path}:{line_number}: memory pressure event")
        rows.append(row)
    if not rows:
        raise LatencyAnalysisError(f"empty latency results: {path}")
    return rows


def summarize(rows):
    """Median of independent run quantiles per (origin, payload, arm)."""
    if not rows:
        raise LatencyAnalysisError("no latency rows")
    origins = {row["origin"] for row in rows}
    if any({row["payload"] for row in rows if row["origin"] == origin} != {64, 256}
           for origin in origins):
        raise LatencyAnalysisError("every origin requires both payloads 64 and 256 B")
    groups = []
    for origin in sorted({row["origin"] for row in rows}):
        for payload in sorted({row["payload"] for row in rows if row["origin"] == origin}):
            for arm in "BSA":
                arm_rows = sorted(
                    (row for row in rows
                     if row["origin"] == origin and row["payload"] == payload
                     and row["arm"] == arm and row["phase"] == "measured"),
                    key=lambda row: row["round"],
                )
                if tuple(row["round"] for row in arm_rows) != MEASURED_ROUNDS:
                    raise LatencyAnalysisError(
                        f"{origin}/{payload}/{arm} does not contain measured rounds 1..10"
                    )
                published = {int(row["result"]["published"]) for row in arm_rows}
                if len(published) != 1:
                    raise LatencyAnalysisError(
                        f"{origin}/{payload}/{arm} has inconsistent published counts"
                    )
                groups.append({
                    "origin": origin,
                    "payload": payload,
                    "arm": arm,
                    "round_count": len(arm_rows),
                    "published_messages": published.pop(),
                    "delivered_messages": sum(int(row["result"]["delivered"]) for row in arm_rows),
                    "completion_fraction": min(
                        int(row["result"]["delivered"]) / (row["rate"] * row["duration_s"])
                        for row in arm_rows),
                    "quantiles_ns": {
                        metric: statistics.median(
                            int(row["result"][f"latency_{metric}_ns"]) for row in arm_rows
                        )
                        for metric in METRICS
                    },
                    "quantile_range_ns": {
                        metric: [min(int(row["result"][f"latency_{metric}_ns"])
                                     for row in arm_rows),
                                 max(int(row["result"][f"latency_{metric}_ns"])
                                     for row in arm_rows)]
                        for metric in METRICS
                    },
                    "throughput_median_mmsg_s": statistics.median(
                        float(row["result"]["publish_msg_per_sec"]) / 1e6
                        for row in arm_rows
                    ),
                })
    return groups


def assess_g4(groups):
    """Apply the registered p50 and completion rules to planned-origin groups."""
    results = []
    for payload in sorted({group["payload"] for group in groups
                           if group["origin"] == "planned"}):
        by_arm = {group["arm"]: group for group in groups
                  if group["origin"] == "planned" and group["payload"] == payload}
        seq = by_arm["S"]
        asym = by_arm["A"]
        baseline = by_arm["B"]
        delta = asym["quantiles_ns"]["p50"] - seq["quantiles_ns"]["p50"]
        completion = {
            arm: by_arm[arm]["published_messages"] for arm in "BSA"
        }
        completion_ok = (len(set(completion.values())) == 1 and
                         all(by_arm[arm]["completion_fraction"] == 1 for arm in "BSA"))
        results.append({
            "origin": "planned",
            "payload": payload,
            "p50_ns": {arm: by_arm[arm]["quantiles_ns"]["p50"] for arm in "BSA"},
            "p99_ns": {arm: by_arm[arm]["quantiles_ns"]["p99"] for arm in "BSA"},
            "p999_ns": {arm: by_arm[arm]["quantiles_ns"]["p999"] for arm in "BSA"},
            "p50_asym_minus_seq_ns": delta,
            "p50_allowance_ns": G4_P50_ALLOWANCE_NS,
            "p50_passed": delta <= G4_P50_ALLOWANCE_NS,
            "published_messages": completion,
            "completion_passed": completion_ok,
            "baseline_reference": baseline["quantiles_ns"],
            "short_latency_passed": delta <= G4_P50_ALLOWANCE_NS and completion_ok,
            "hft_tail_status": "REQUIRED_SEPARATELY",
            "passed": False if delta > G4_P50_ALLOWANCE_NS or not completion_ok else None,
        })
    return results


def compare(directory, baseline_sha256=BASELINE_SHA256):
    """Verify one latency output directory and apply G4."""
    directory = Path(directory).resolve()
    arms = load_arms(directory, baseline_sha256)
    rows = load_rows(directory, arms)
    groups = summarize(rows)
    gates = assess_g4(groups)
    return {
        "directory": str(directory),
        "arms": {arm: {"client_sha256": entry["client_sha256"],
                       "environment": entry["environment"]}
                 for arm, entry in arms.items()},
        "aggregation": "Median of independent run quantiles, not pooled quantiles",
        "groups": groups,
        "g4": gates,
        "short_latency_all_passed": bool(gates) and all(item["short_latency_passed"] for item in gates),
        "g4_all_passed": False if any(item["passed"] is False for item in gates) else None,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", type=Path, nargs="+")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    results = [compare(path) for path in args.inputs]
    if any(result["arms"] != results[0]["arms"] for result in results):
        raise SystemExit("latency inputs use different client identities")
    groups = [group for result in results for group in result["groups"]]
    keys = [(group["origin"], group["payload"], group["arm"]) for group in groups]
    if len(set(keys)) != len(keys) or set(keys) != {
            (origin, size, arm) for origin in ("attempt", "planned")
            for size in (64, 256) for arm in "BSA"}:
        raise SystemExit("require exactly both origins, both payloads and all three arms")
    payload = {
        "inputs": [result["directory"] for result in results],
        "arms": results[0]["arms"],
        "aggregation": results[0]["aggregation"],
        "groups": groups,
        "g4": [gate for result in results for gate in result["g4"]],
        "short_latency_all_passed": all(result["short_latency_all_passed"] for result in results),
        "g4_all_passed": False if any(result["g4_all_passed"] is False for result in results) else None,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2) + "\n")
    print("| Origin | B | Arm | p50 ns | p99 ns | p99.9 ns |")
    print("|---|---:|---|---:|---:|---:|")
    for group in payload["groups"]:
        q = group["quantiles_ns"]
        print(f"| {group['origin']} | {group['payload']} | {group['arm']} | "
              f"{q['p50']} | {q['p99']} | {q['p999']} |")
    for gate in payload["g4"]:
        print(f"G4 planned/{gate['payload']}B: p50 A-S={gate['p50_asym_minus_seq_ns']:+g} ns "
              f"(allowance {gate['p50_allowance_ns']}), completion="
              f"{'ok' if gate['completion_passed'] else 'mismatch'} -> "
              f"short={'PASS' if gate['short_latency_passed'] else 'FAIL'}; HFT tail required separately")


if __name__ == "__main__":
    main()
