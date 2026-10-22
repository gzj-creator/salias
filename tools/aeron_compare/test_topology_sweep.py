#!/usr/bin/env python3
"""Regression tests for topology sweep matrix validation and analysis."""

import hashlib
import importlib.util
import json
import math
from pathlib import Path
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location(
    "topology_runner", ROOT / "doc/plans/runners/topology-sweep-runner.py"
)
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)
from analyze_topology_sweep import summarize
from analyze_topology_sweep import refresh_checksums


class TopologySweepTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.output = Path(self.temporary.name)

    def build(self, topologies=("1P1S", "1P2S"), payloads=(64, 256),
              rounds=1, rates=None):
        environment = {
            "tested_topologies": list(topologies),
            "payloads": list(payloads),
            "rounds": rounds,
            "messages": 100,
        }
        (self.output / "environment.json").write_text(json.dumps(environment))
        records = []
        number = 0
        for round_number in range(rounds + 1):
            for topology in topologies:
                for payload in payloads:
                    number += 1
                    consumers = int(topology[2])
                    key = (topology, payload)
                    rate = (rates or {}).get(key, 10_000_000)
                    result = {
                        "published": "100", "delivered": str(100 * consumers),
                        "producers": "1", "consumers": str(consumers),
                        "payload": str(payload), "latency_samples": "0",
                        "latency_sample_rate": "0", "rate_per_producer": "0",
                        "scenario": "fifo", "publish_msg_per_sec": str(rate),
                    }
                    stdout = self.output / f"{number:03d}.stdout"
                    stdout.write_text("RESULT " + " ".join(
                        f"{key}={value}" for key, value in result.items()) + "\n")
                    records.append({
                        "number": number, "engine": "salias", "kind": "throughput",
                        "topology": topology, "payload": payload, "consumers": consumers,
                        "round": round_number,
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

    def test_topology_selection_skips_1p3s_on_four_cpus(self):
        self.assertEqual([t["consumers"] for t in runner.selected_topologies(4)], [1, 2])
        self.assertEqual([t["consumers"] for t in runner.selected_topologies(8)], [1, 2, 3])

    def test_worker_mapping_places_publisher_after_consumers(self):
        self.assertEqual(runner.worker_cpu(runner.TOPOLOGIES[0]), 2)
        self.assertEqual(runner.worker_cpu(runner.TOPOLOGIES[1]), 3)
        self.assertEqual(runner.worker_cpu(runner.TOPOLOGIES[2]), 4)

    def test_analysis_excludes_warmup_and_calculates_transitions(self):
        self.build(rates={("1P1S", 64): 12_000_000,
                          ("1P2S", 64): 6_000_000,
                          ("1P1S", 256): 20_000_000,
                          ("1P2S", 256): 18_000_000})
        result = summarize(self.output)
        self.assertEqual(result["measured_runs"], 4)
        self.assertEqual(result["warmup_runs"], 4)
        self.assertEqual(result["groups"]["1P1S:64"]["median_mmsg_s"], 12)
        transition = next(row for row in result["transitions"]
                          if row["payload"] == 64)
        self.assertAlmostEqual(transition["relative_change"], -0.5)
        self.assertTrue(transition["exceeds_30_percent_degradation"])
        self.assertEqual(result["decision"]["conclusion"],
                         "subscriber-scanner bottleneck candidate")

    def test_borderline_fast_1p1s_is_not_marked_publisher_side(self):
        self.build(rates={("1P1S", 64): 9_500_000, ("1P2S", 64): 6_500_000,
                          ("1P1S", 256): 15_000_000, ("1P2S", 256): 15_000_000})
        conclusion = summarize(self.output)["decision"]["conclusion"]
        self.assertIn("unclear", conclusion)
        self.assertIn("below 10", conclusion)

    def test_slow_1p1s_selects_publisher_side(self):
        self.build(rates={("1P1S", 64): 6_000_000, ("1P2S", 64): 5_000_000,
                          ("1P1S", 256): 15_000_000, ("1P2S", 256): 14_000_000})
        self.assertIn("publisher-side", summarize(self.output)["decision"]["conclusion"])

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

    def test_statistics_use_sample_standard_deviation(self):
        values = [1.0, 2.0, 3.0]
        result = random_stats(values)
        self.assertAlmostEqual(result["median_mmsg_s"], 2)
        self.assertAlmostEqual(result["q1_mmsg_s"], 1.5)
        self.assertAlmostEqual(result["q3_mmsg_s"], 2.5)
        self.assertAlmostEqual(result["cv"], 1 / 2)

    def test_checksum_refresh_does_not_include_manifest_itself(self):
        self.build()
        analysis = self.output / "topology-analysis.json"
        analysis.write_text("{}\n")
        refresh_checksums(self.output)
        names = [line.partition("  ")[2]
                 for line in (self.output / "SHA256SUMS").read_text().splitlines()]
        self.assertIn("topology-analysis.json", names)
        self.assertNotIn("SHA256SUMS", names)


def random_stats(values):
    from analyze_topology_sweep import stats
    return stats(values)


if __name__ == "__main__":
    unittest.main()
