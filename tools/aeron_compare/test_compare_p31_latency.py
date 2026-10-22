#!/usr/bin/env python3
"""Tests for the P3-1 latency audit (G4)."""

import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest


sys.path.insert(0, str(Path(__file__).resolve().parent))
from compare_p31_latency import (  # noqa: E402
    ARM_ENVIRONMENT,
    LatencyAnalysisError,
    assess_g4,
    compare,
)


class P31LatencyTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def build(self, p50, origins=("planned",), payloads=(64, 256), published=2_000_000):
        baseline = self.root / "baseline"
        candidate = self.root / "candidate"
        baseline.write_text("frozen")
        candidate.write_text("candidate")
        baseline_sha = hashlib.sha256(baseline.read_bytes()).hexdigest()
        candidate_sha = hashlib.sha256(candidate.read_bytes()).hexdigest()
        arms = [
            {"arm": "B", "client": str(baseline), "client_sha256": baseline_sha,
             "environment": {}},
            {"arm": "S", "client": str(candidate), "client_sha256": candidate_sha,
             "environment": dict(ARM_ENVIRONMENT["S"])},
            {"arm": "A", "client": str(candidate), "client_sha256": candidate_sha,
             "environment": {}},
        ]
        (self.root / "arms.json").write_text(json.dumps(arms))
        rows = []
        for origin in origins:
            for payload in payloads:
                for arm in "BSA":
                    for round_number in (0, *range(1, 11)):
                        latency = dict(p50[arm], p999=p50[arm]["p50"] * 3)
                        latency["p99"] = p50[arm]["p50"] * 2
                        rows.append({
                            "origin": origin, "payload": payload, "arm": arm,
                            "rate": 1_000_000, "producers": 1, "duration_s": 2,
                            "engine": "fifo", "phase": "warmup" if round_number == 0 else "measured",
                            "round": round_number,
                            "client_sha256": candidate_sha if arm != "B" else baseline_sha,
                            "arm_environment": dict(ARM_ENVIRONMENT[arm]),
                            "result": {
                                "library": "salias-ipc", "scenario": "fifo",
                                "producers": "1", "consumers": "1", "payload": str(payload),
                                "capacity": "1048576", "batch_size": "1", "poll_limit": "64",
                                "published": str(published),
                                "delivered": str(published), "latency_samples": "125000",
                                "latency_sample_rate": "16", "rate_per_producer": "1000000",
                                "latency_origin": origin,
                                "latency_p50_ns": str(latency["p50"]),
                                "latency_p99_ns": str(latency["p99"]),
                                "latency_p999_ns": str(latency["p999"]),
                                "publish_msg_per_sec": "1000000",
                            },
                            "resources": {"memory.events":
                                          "low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\n"},
                        })
                        row = rows[-1]
                        row["consumers"] = [{"samples": "125000", **{
                            f"{metric}_ns": row["result"][f"latency_{metric}_ns"]
                            for metric in ("p50", "p99", "p999")}}]
                        prefix = f"{origin}-{payload}-{arm}-{round_number}"
                        stdout = self.root / f"{prefix}.stdout"
                        stderr = self.root / f"{prefix}.stderr"
                        stdout.write_text("LATENCY " + " ".join(
                            f"{key}={value}" for key, value in row["consumers"][0].items()) +
                            "\nRESULT " + " ".join(
                                f"{key}={value}" for key, value in row["result"].items()) + "\n")
                        stderr.write_text("")
                        for kind, path in (("stdout", stdout), ("stderr", stderr)):
                            row[kind] = path.name
                            row[f"{kind}_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
        (self.root / "results.jsonl").write_text(
            "".join(json.dumps(row) + "\n" for row in rows))
        return baseline_sha

    def quantiles(self, value):
        return {"p50": value}

    def test_g4_passes_within_allowance(self):
        sha = self.build({"B": self.quantiles(1000), "S": self.quantiles(1000),
                          "A": self.quantiles(1005)})
        result = compare(self.root, sha)
        gate = result["g4"][0]
        self.assertEqual(gate["p50_asym_minus_seq_ns"], 5)
        self.assertTrue(gate["completion_passed"])
        self.assertTrue(gate["short_latency_passed"])
        self.assertIsNone(gate["passed"])
        self.assertIsNone(result["g4_all_passed"])

    def test_g4_fails_above_allowance(self):
        sha = self.build({"B": self.quantiles(1000), "S": self.quantiles(1000),
                          "A": self.quantiles(1011)})
        result = compare(self.root, sha)
        self.assertEqual(result["g4"][0]["p50_asym_minus_seq_ns"], 11)
        self.assertFalse(result["g4"][0]["passed"])

    def test_warmup_is_excluded_and_origins_are_separate(self):
        sha = self.build({"B": self.quantiles(1000), "S": self.quantiles(1000),
                          "A": self.quantiles(1000)},
                         origins=("planned", "attempt"))
        result = compare(self.root, sha)
        self.assertEqual({group["origin"] for group in result["groups"]},
                         {"planned", "attempt"})
        self.assertEqual(len(result["g4"]), 2)
        planned = [group for group in result["groups"] if group["origin"] == "planned"]
        self.assertTrue(all(group["round_count"] == 10 for group in planned))

    def test_rejects_missing_round_and_bad_arm_environment(self):
        sha = self.build({"B": self.quantiles(1000), "S": self.quantiles(1000),
                          "A": self.quantiles(1000)})
        rows = [json.loads(line) for line in
                (self.root / "results.jsonl").read_text().splitlines()]
        (self.root / "results.jsonl").write_text(
            "".join(json.dumps(row) + "\n" for row in rows[:-1]))
        with self.assertRaises(LatencyAnalysisError):
            compare(self.root, sha)

        sha = self.build({"B": self.quantiles(1000), "S": self.quantiles(1000),
                          "A": self.quantiles(1000)})
        rows = [json.loads(line) for line in
                (self.root / "results.jsonl").read_text().splitlines()]
        for row in rows:
            if row["arm"] == "A":
                row["arm_environment"] = {"SALIAS_DISABLE_ASYMMETRIC_FENCE": "1"}
        (self.root / "results.jsonl").write_text(
            "".join(json.dumps(row) + "\n" for row in rows))
        with self.assertRaises(LatencyAnalysisError):
            compare(self.root, sha)

    def test_g4_requires_equal_completion(self):
        groups = [
            {"origin": "planned", "payload": 64, "arm": arm,
             "published_messages": 1_000 if arm != "A" else 999,
             "completion_fraction": 1.0 if arm != "A" else 0.999,
             "quantiles_ns": {"p50": 1000, "p99": 2000, "p999": 3000}}
            for arm in "BSA"
        ]
        gate = assess_g4(groups)[0]
        self.assertFalse(gate["completion_passed"])
        self.assertFalse(gate["passed"])

    def test_rejects_empty_or_incomplete_matrix(self):
        sha = self.build({arm: self.quantiles(1000) for arm in "BSA"}, payloads=(64,))
        with self.assertRaises(LatencyAnalysisError):
            compare(self.root, sha)
        (self.root / "results.jsonl").write_text("")
        with self.assertRaises(LatencyAnalysisError):
            compare(self.root, sha)

    def test_rejects_tampered_raw_output(self):
        sha = self.build({arm: self.quantiles(1000) for arm in "BSA"})
        next(self.root.glob("*.stdout")).write_text("changed\n")
        with self.assertRaises(LatencyAnalysisError):
            compare(self.root, sha)

    def test_rejects_wrong_counts_even_when_arms_match(self):
        sha = self.build({arm: self.quantiles(1000) for arm in "BSA"}, published=0)
        with self.assertRaises(LatencyAnalysisError):
            compare(self.root, sha)


if __name__ == "__main__":
    unittest.main()
