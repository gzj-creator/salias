#!/usr/bin/env python3
"""Tests for the P3-1 three-arm throughput gates."""

import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest


sys.path.insert(0, str(Path(__file__).resolve().parent))
from compare_p31_throughput import (  # noqa: E402
    ARM_DIRECTORIES,
    ARM_SETENV,
    AnalysisError,
    GROUPS,
    compare,
    expected_sequence,
    gate_g1,
    gate_g3,
)


ROUNDS = 20


class P31ThroughputTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.baseline = self.root / "baseline-client"
        self.candidate = self.root / "candidate-client"
        self.baseline.write_bytes(b"baseline")
        self.candidate.write_bytes(b"candidate")
        self.baseline_sha = hashlib.sha256(self.baseline.read_bytes()).hexdigest()
        self.candidate_sha = hashlib.sha256(self.candidate.read_bytes()).hexdigest()

    def compare(self, payload=64):
        return compare(self.root, payload, baseline_sha256=self.baseline_sha)

    @staticmethod
    def write_checksums(directory):
        entries = sorted(path for path in directory.rglob("*")
                         if path.is_file() and path.name != "SHA256SUMS")
        (directory / "SHA256SUMS").write_text("".join(
            f"{hashlib.sha256(path.read_bytes()).hexdigest()}  "
            f"{path.relative_to(directory)}\n" for path in entries))

    def build_arm(self, directory_name, values, client_sha, payload, setenv):
        directory = self.root / directory_name
        directory.mkdir(parents=True)
        environment = {
            "groups": GROUPS,
            "rounds_per_group": ROUNDS,
            "messages_per_round": 50_000_000,
            "payload_bytes": payload,
            "capacity_bytes": 1 << 20,
            "rate_per_producer": 0,
            "latency_sample_rate": 0,
            "environment_variables": list(setenv),
            "client_sha256": client_sha,
            "client": str(self.baseline if directory_name == "baseline" else self.candidate),
        }
        (directory / "environment.json").write_text(json.dumps(environment))
        entries = []
        for group_idx in range(GROUPS):
            group_dir = directory / f"group-{group_idx:02d}"
            group_dir.mkdir()
            records = []
            for round_idx in range(ROUNDS):
                rate = values[group_idx][round_idx]
                stdout_path = group_dir / f"round-{round_idx:02d}.stdout"
                stdout_path.write_text(
                    "RESULT library=salias-ipc scenario=fifo producers=1 consumers=2 "
                    f"payload={payload} capacity={1 << 20} batch_size=1 poll_limit=64 "
                    f"published={50_000_000} delivered={100_000_000} "
                    f"publish_msg_per_sec={rate * 1_000_000}\n"
                )
                records.append({"group": group_idx, "round": round_idx,
                                "publish_mmsg_s": rate, "stdout": stdout_path.name})
                entries.append(stdout_path)
            results = group_dir / "results.jsonl"
            results.write_text("".join(json.dumps(record) + "\n" for record in records))
            entries.append(results)
        self.write_checksums(directory)
        return directory

    def rates(self, base):
        return [[base + group * 0.01 + round_idx * 0.001 for round_idx in range(ROUNDS)]
                for group in range(GROUPS)]

    def build_experiment(self, payload=64, b=4.0, s=4.0, a=5.2,
                         s_setenv=("SALIAS_DISABLE_ASYMMETRIC_FENCE=1",),
                         s_sha=None, a_sha=None, sequence=None):
        s_sha = s_sha or self.candidate_sha
        a_sha = a_sha or self.candidate_sha
        self.build_arm(ARM_DIRECTORIES["B"], self.rates(b), self.baseline_sha,
                       payload, ())
        self.build_arm(ARM_DIRECTORIES["S"], self.rates(s), s_sha, payload, s_setenv)
        self.build_arm(ARM_DIRECTORIES["A"], self.rates(a), a_sha, payload,
                       ARM_SETENV["A"])
        blocks = []
        index = 0
        for group in range(GROUPS):
            for arm in ("B", "S", "A") if group % 2 == 0 else ("A", "S", "B"):
                blocks.append({"block": index, "arm": arm, "group": group,
                               "returncode": 0})
                index += 1
        manifest = {
            "payload_bytes": payload,
            "messages_per_round": 50_000_000,
            "capacity_bytes": 1 << 20,
            "groups": GROUPS,
            "rounds_per_group": ROUNDS,
            "sequence": sequence or expected_sequence(),
            "arms": {
                arm: {"output": ARM_DIRECTORIES[arm],
                      "client": str(self.baseline if arm == "B" else self.candidate),
                      "sha256": (self.baseline_sha if arm == "B"
                                 else (s_sha if arm == "S" else a_sha)),
                      "setenv": list(ARM_SETENV[arm] if arm != "S" else s_setenv)}
                for arm in ("B", "S", "A")
            },
            "blocks": blocks,
        }
        (self.root / "experiment.json").write_text(json.dumps(manifest))

    def test_g1_and_g2_pass(self):
        self.build_experiment(b=4.0, s=4.0, a=5.3)
        result = self.compare()
        self.assertTrue(result["g1_barrier_effect"]["passed"])
        self.assertEqual(result["g1_barrier_effect"]["directions_a_above_s"], 6)
        self.assertGreater(result["g1_barrier_effect"]["ratio_a_over_s"], 1.25)
        self.assertTrue(result["g2_product_ratio"]["passed"])
        self.assertNotIn("g3_192b_non_inferior", result)

    def test_g1_needs_go_ratio(self):
        self.build_experiment(b=4.0, s=4.0, a=4.3)
        result = self.compare()
        self.assertFalse(result["g1_barrier_effect"]["passed"])
        self.assertEqual(result["g1_barrier_effect"]["decision"], "FAIL")

    def test_g1_needs_five_directions(self):
        seq = {"median": 4.0, "groups": [{"median": 4.0}] * 6}
        asym = {"median": 5.3, "groups": [{"median": x} for x in (3, 3, 6, 6, 6, 6)]}
        result = gate_g1(seq, asym)
        self.assertEqual(result["directions_a_above_s"], 4)
        self.assertFalse(result["passed"])

    def test_g2_fails_without_blocking_g1(self):
        self.build_experiment(b=5.0, s=4.0, a=5.3)
        result = self.compare()
        self.assertTrue(result["g1_barrier_effect"]["passed"])
        self.assertFalse(result["g2_product_ratio"]["passed"])

    def test_g3_requires_ratio_and_high_mode(self):
        self.build_experiment(payload=192, b=16.0, s=16.0, a=16.0)
        result = self.compare(192)
        self.assertTrue(result["g3_192b_non_inferior"]["passed"])
        self.assertAlmostEqual(result["g3_192b_non_inferior"]["high_mode_rate_seq"], 1.0)

        self.assertFalse(result["g1_barrier_effect"]["eligible"])
        self.assertIsNone(result["g1_barrier_effect"]["passed"])
        self.assertNotIn("g2_product_ratio", result)

    def test_g3_fails_on_ratio(self):
        self.build_experiment(payload=192, b=16.0, s=16.0, a=15.0)
        result = self.compare(192)
        self.assertLess(result["g3_192b_non_inferior"]["ratio_a_over_s"], 0.97)
        self.assertFalse(result["g3_192b_non_inferior"]["passed"])

    def test_g3_fails_on_high_mode_drop(self):
        seq = {"median": 16.0, "rates": [16.0] * 120}
        asym = {"median": 16.0, "rates": [16.0] * 100 + [14.9] * 20}
        result = gate_g3(seq, asym)
        self.assertEqual(result["ratio_a_over_s"], 1.0)
        self.assertGreater(result["high_mode_drop_pp"], 10)
        self.assertFalse(result["passed"])

    def test_rejects_same_environment_for_both_candidate_arms(self):
        self.build_experiment(s_setenv=())
        with self.assertRaises(AnalysisError):
            self.compare()

    def test_rejects_different_candidate_binaries(self):
        self.build_experiment(a_sha="d" * 64)
        with self.assertRaises(AnalysisError):
            self.compare()

    def test_rejects_changed_block_rotation(self):
        sequence = expected_sequence()
        sequence[0] = ["B0", "A0", "S0"]
        self.build_experiment(sequence=sequence)
        with self.assertRaises(AnalysisError):
            self.compare()

    def test_rejects_changed_executed_order(self):
        self.build_experiment()
        path = self.root / "experiment.json"
        manifest = json.loads(path.read_text())
        manifest["blocks"][0]["arm"] = "A"
        path.write_text(json.dumps(manifest))
        with self.assertRaises(AnalysisError):
            self.compare()

    def test_rejects_environment_identity_disagreeing_with_manifest(self):
        self.build_experiment()
        directory = self.root / ARM_DIRECTORIES["B"]
        path = directory / "environment.json"
        environment = json.loads(path.read_text())
        environment["client_sha256"] = self.candidate_sha
        path.write_text(json.dumps(environment))
        self.write_checksums(directory)
        with self.assertRaises(AnalysisError):
            self.compare()

    def test_rejects_client_changed_after_execution(self):
        self.build_experiment()
        self.candidate.write_bytes(b"changed")
        with self.assertRaises(AnalysisError):
            self.compare()


if __name__ == "__main__":
    unittest.main()
