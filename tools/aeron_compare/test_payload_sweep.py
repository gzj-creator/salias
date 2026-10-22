#!/usr/bin/env python3
"""Regression tests for payload sweep matrix validation and analysis."""

import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location(
    "payload_runner", ROOT / "doc/plans/runners/payload-sweep-runner.py"
)
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)
from analyze_payload_sweep import summarize


class PayloadSweepTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.output = Path(self.temporary.name)

    def build(self, payloads=(48, 56, 64), rounds=1, rates=None):
        (self.output / "environment.json").write_text(json.dumps({
            "payloads": list(payloads), "rounds": rounds, "messages": 100,
            "capacity_bytes": 1048576,
        }))
        records = []
        number = 0
        for round_number in range(rounds + 1):
            for payload in payloads:
                number += 1
                rate = (rates or {}).get(payload, 10_000_000)
                result = {
                    "published": "100", "delivered": "200", "producers": "1",
                    "consumers": "2", "payload": str(payload), "latency_samples": "0",
                    "latency_sample_rate": "0", "rate_per_producer": "0",
                    "scenario": "fifo", "publish_msg_per_sec": str(rate),
                }
                stdout = self.output / f"{number:03d}.stdout"
                stdout.write_text("RESULT " + " ".join(
                    f"{key}={value}" for key, value in result.items()) + "\n")
                records.append({
                    "number": number, "engine": "salias", "kind": "throughput",
                    "topology": "1P2S", "payload": payload, "round": round_number,
                    "phase": "warmup" if round_number == 0 else "measured",
                    "result": result, "stdout": stdout.name,
                    "stdout_sha256": hashlib.sha256(stdout.read_bytes()).hexdigest(),
                })
        (self.output / "results.jsonl").write_text(
            "".join(json.dumps(record) + "\n" for record in records))
        files = sorted(path for path in self.output.iterdir() if path.is_file())
        (self.output / "SHA256SUMS").write_text("".join(
            f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n"
            for path in files))
        return records

    def test_runner_has_expected_payloads_and_topology(self):
        self.assertEqual(runner.PAYLOADS, (48, 56, 64, 72, 80, 96, 128, 192, 256))
        self.assertEqual(runner.TOPOLOGY["consumers"], 2)

    def test_analysis_excludes_warmup_and_marks_stride(self):
        self.build(rates={48: 10_000_000, 56: 14_000_000, 64: 10_000_000})
        result = summarize(self.output)
        self.assertEqual(result["measured_runs"], 3)
        self.assertEqual(result["warmup_runs"], 3)
        self.assertEqual(result["groups"]["56"]["frame_stride_bytes"], 64)
        self.assertTrue(result["groups"]["56"]["frame_stride_cacheline_aligned"])
        self.assertEqual(result["groups"]["64"]["frame_stride_bytes"], 72)
        self.assertAlmostEqual(result["decision"]["difference_56_vs_64_percent"], 40)
        self.assertTrue(result["decision"]["h2_supported"])

    def test_breakpoint_is_based_on_adjacent_median_change(self):
        self.build(rates={48: 10_000_000, 56: 10_000_000, 64: 20_000_000})
        result = summarize(self.output)
        breakpoints = result["decision"]["breakpoints"]
        self.assertEqual([(row["from_payload"], row["to_payload"]) for row in breakpoints],
                         [(56, 64)])

    def test_tamper_duplicate_and_missing_round_are_rejected(self):
        for mutation in ("tamper", "duplicate", "missing"):
            with self.subTest(mutation=mutation):
                with tempfile.TemporaryDirectory() as directory:
                    original = self.output
                    self.output = Path(directory)
                    records = self.build()
                    if mutation == "tamper":
                        (self.output / records[0]["stdout"]).write_text("changed\n")
                    elif mutation == "duplicate":
                        records[-1] = dict(records[-2])
                    else:
                        records.pop()
                    (self.output / "results.jsonl").write_text(
                        "".join(json.dumps(record) + "\n" for record in records))
                    with self.assertRaises(ValueError):
                        summarize(self.output)
                    self.output = original


if __name__ == "__main__":
    unittest.main()
