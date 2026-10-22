#!/usr/bin/env python3
"""Tests for the E1 zero-layout throughput comparison."""

import json
from pathlib import Path
import sys
import tempfile
import unittest


sys.path.insert(0, str(Path(__file__).resolve().parent))
from compare_throughput_ab import (  # noqa: E402
    AnalysisError,
    GROUPS,
    ROUNDS,
    compare,
    decide,
    load_arm,
    permutation_test,
    verify_checksums,
)


class ThroughputABTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def build_arm(self, name, values, client_sha, payload=64, groups=GROUPS):
        directory = self.root / name
        directory.mkdir()
        environment = {
            "groups": groups,
            "rounds_per_group": ROUNDS,
            "messages_per_round": 50_000_000,
            "payload_bytes": payload,
            "capacity_bytes": 1 << 20,
            "rate_per_producer": 0,
            "latency_sample_rate": 0,
            "client_sha256": client_sha,
        }
        (directory / "environment.json").write_text(json.dumps(environment))
        checksums = []
        for group_idx in range(groups):
            group_dir = directory / f"group-{group_idx:02d}"
            group_dir.mkdir()
            records = []
            for round_idx in range(ROUNDS):
                rate = values[group_idx][round_idx]
                name_prefix = f"round-{round_idx:02d}"
                stdout = (
                    f"RESULT library=salias-ipc scenario=fifo producers=1 consumers=2 "
                    f"payload={payload} capacity={1 << 20} batch_size=1 poll_limit=64 "
                    f"published={50_000_000} delivered={100_000_000} "
                    f"publish_msg_per_sec={rate * 1_000_000}\n"
                )
                stdout_path = group_dir / f"{name_prefix}.stdout"
                stdout_path.write_text(stdout)
                record = {
                    "group": group_idx,
                    "round": round_idx,
                    "publish_mmsg_s": rate,
                    "stdout": stdout_path.name,
                }
                records.append(record)
                checksums.append((stdout_path, stdout))
            results = group_dir / "results.jsonl"
            results.write_text("".join(json.dumps(record) + "\n" for record in records))
            checksums.append((results, results.read_text()))
        import hashlib
        lines = []
        for path in sorted(checksums, key=lambda item: item[0]):
            lines.append(f"{hashlib.sha256(path[0].read_bytes()).hexdigest()}  "
                         f"{path[0].relative_to(directory)}\n")
        (directory / "SHA256SUMS").write_text("".join(lines))
        return directory

    def rates(self, base, groups=GROUPS):
        return [[base + group * 0.01 + round_idx * 0.001 for round_idx in range(ROUNDS)]
                for group in range(groups)]

    def test_go_requires_all_three_conditions(self):
        self.assertEqual(decide(1.26, 0.001, 5), "GO")
        self.assertEqual(decide(1.26, 0.01, 6), "INCONCLUSIVE")
        self.assertEqual(decide(1.26, 0.001, 4), "INCONCLUSIVE")
        self.assertEqual(decide(1.09, 0.001, 6), "NO-GO")
        self.assertEqual(decide(1.30, 0.05, 6), "NO-GO")
        self.assertEqual(decide(1.15, 0.03, 6), "INCONCLUSIVE")
        self.assertEqual(decide(1.15, 0.001, 6), "INCONCLUSIVE")

    def test_exact_permutation_counts_all_924_splits(self):
        baseline = [1.0, 1.1, 1.2, 1.3, 1.4, 1.5]
        candidate = [1.6, 1.7, 1.8, 1.9, 2.0, 2.1]
        result = permutation_test(baseline, candidate)
        self.assertEqual(result["partitions"], 924)
        self.assertEqual(result["extreme_partitions"], 2)
        self.assertAlmostEqual(result["p_value"], 2 / 924)

    def test_complete_go_comparison(self):
        baseline = self.build_arm("b", self.rates(4.0), "a" * 64)
        candidate = self.build_arm("c", self.rates(5.2), "b" * 64)
        result = compare(baseline, candidate, 64)
        self.assertEqual(result["decision"], "GO")
        self.assertEqual(result["abba_directions_c_above_b"], 6)
        self.assertEqual(result["baseline"]["verified_files"], 126)
        self.assertEqual(result["candidate"]["verified_files"], 126)
        self.assertGreater(result["bootstrap"]["ci95_low_mmsg_s"], 0)

    def test_192b_result_is_descriptive_not_gated(self):
        baseline = self.build_arm("b", self.rates(4.0), "a" * 64, payload=192)
        candidate = self.build_arm("c", self.rates(5.2), "b" * 64, payload=192)
        result = compare(baseline, candidate, 192)
        self.assertFalse(result["decision_eligible"])
        self.assertEqual(result["decision"], "DESCRIPTIVE")

    def test_incomplete_group_is_rejected(self):
        baseline = self.build_arm("b", self.rates(4.0), "a" * 64)
        candidate = self.build_arm("c", self.rates(5.2), "b" * 64)
        results = candidate / "group-05/results.jsonl"
        records = results.read_text().splitlines()[:-1]
        results.write_text("".join(line + "\n" for line in records))
        (candidate / "SHA256SUMS").write_text(
            "".join(
                f"{__import__('hashlib').sha256(item.read_bytes()).hexdigest()}  "
                f"{item.relative_to(candidate)}\n"
                for item in sorted(candidate.rglob("*"))
                if item.is_file() and item.name != "SHA256SUMS"
            )
        )
        with self.assertRaisesRegex(AnalysisError, "19 rounds"):
            load_arm(candidate, 64)

    def test_checksum_mismatch_is_rejected(self):
        baseline = self.build_arm("b", self.rates(4.0), "a" * 64)
        (baseline / "group-00/round-00.stdout").write_text("tampered\n")
        with self.assertRaisesRegex(AnalysisError, "SHA256 mismatch"):
            load_arm(baseline, 64)

    def test_result_mismatch_is_rejected(self):
        baseline = self.build_arm("b", self.rates(4.0), "a" * 64)
        path = baseline / "group-00/round-00.stdout"
        path.write_text(path.read_text().replace("consumers=2", "consumers=1"))
        (baseline / "SHA256SUMS").write_text(
            "".join(
                f"{__import__('hashlib').sha256(item.read_bytes()).hexdigest()}  "
                f"{item.relative_to(baseline)}\n"
                for item in sorted(baseline.rglob("*"))
                if item.is_file() and item.name != "SHA256SUMS"
            )
        )
        with self.assertRaisesRegex(AnalysisError, "RESULT mismatch"):
            load_arm(baseline, 64)

    def test_identical_client_identity_is_rejected(self):
        baseline = self.build_arm("b", self.rates(4.0), "a" * 64)
        candidate = self.build_arm("c", self.rates(5.2), "a" * 64)
        with self.assertRaisesRegex(AnalysisError, "identical"):
            compare(baseline, candidate, 64)


if __name__ == "__main__":
    unittest.main()
