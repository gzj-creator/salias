#!/usr/bin/env python3
"""Regression tests for diagnostic parsing and source-derived frame analysis."""

import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

from analyze_frame_layout import analyze, source_constants
from parse_diagnostic import analyze_record, parse_stdout, summarize


class Phase1DiagnosticsTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)

    def record(self, mode="timed", round_number=1, payload=64):
        path = self.directory / f"{payload}b-{mode}-r{round_number}.stdout"
        lines = []
        if mode != "off":
            lines = [f"diagnostic_mode={mode}", "diagnostic_claim_success=1000",
                     "diagnostic_claim_backpressure=100", "diagnostic_claim_yield_cycles=100",
                     f"diagnostic_claim_total_ns={100000 if mode == 'timed' else 0}"]
            if mode == "timed":
                lines.append("diagnostic_claim_avg_ns=1e2")
            for i in range(2):
                lines += [f"diagnostic_consumer{i}_poll_empty=100",
                          f"diagnostic_consumer{i}_poll_success=50",
                          f"diagnostic_consumer{i}_poll_total_messages=1000",
                          f"diagnostic_consumer{i}_avg_batch=20"]
        path.write_text("\n".join(lines) + "\n")
        return dict(number=1, payload=payload, round=round_number,
                    phase="warmup" if round_number == 0 else "measured", mode=mode,
                    stdout=path.name, stdout_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                    messages=1000, result=dict(published="1000", delivered="2000", producers="1",
                                               consumers="2", publish_msg_per_sec="1e7", seconds="0.0001"))

    def test_rates_and_timing_use_exact_totals(self):
        row = analyze_record(self.record(), self.directory)
        self.assertAlmostEqual(row["backpressure_rate"], 100 / 1100)
        self.assertEqual(row["claim_avg_ns"], 100)
        self.assertAlmostEqual(row["consumers"][0]["empty_rate"], 2 / 3)
        self.assertEqual(row["consumers"][1]["avg_batch"], 20)

    def test_counters_do_not_invent_zero_time(self):
        row = analyze_record(self.record(mode="counters"), self.directory)
        self.assertIsNone(row["claim_avg_ns"])

    def test_changed_stdout_fails_checksum(self):
        record = self.record()
        (self.directory / record["stdout"]).write_text("changed")
        with self.assertRaisesRegex(ValueError, "checksum"):
            analyze_record(record, self.directory)

    def test_mismatched_counts_fail(self):
        record = self.record()
        record["messages"] = 999
        with self.assertRaisesRegex(ValueError, "Delivery"):
            analyze_record(record, self.directory)

    def test_duplicate_and_nonfinite_metrics_fail(self):
        path = self.directory / "bad.stdout"
        for text in ("diagnostic_x=1\ndiagnostic_x=2", "diagnostic_x=1e999",
                     "diagnostic_x=-1"):
            with self.subTest(text=text):
                path.write_text(text)
                with self.assertRaises(ValueError):
                    parse_stdout(path)

    def test_warmups_are_excluded_and_missing_rounds_rejected(self):
        environment = dict(modes=["off", "counters", "timed"], rounds=2)
        (self.directory / "environment.json").write_text(json.dumps(environment))
        records = [self.record(mode, r, payload) for payload in (64, 256)
                   for mode in environment["modes"] for r in range(3)]
        path = self.directory / "results.jsonl"
        path.write_text("\n".join(json.dumps(r) for r in records))
        result = summarize(self.directory)
        self.assertEqual(result["measured_runs"], 12)
        self.assertEqual(result["warmup_runs"], 6)
        self.assertTrue(all(g["rounds"] == 2 for g in result["groups"]))
        path.write_text("\n".join(json.dumps(r) for r in records[:-1]))
        with self.assertRaisesRegex(ValueError, "matrix"):
            summarize(self.directory)

    def test_layout_matches_source_and_double_mapped_wraps(self):
        constants = source_constants()
        self.assertEqual(constants["kHeaderSize"], 8)
        self.assertEqual(constants["kFrameAlign"], 8)
        small = analyze(64, 100_000_000, [12, 16], 8, 8)
        large = analyze(256, 100_000_000, [6, 7], 8, 8)
        self.assertEqual(small["frame_stride_bytes"], 72)
        self.assertEqual(large["frame_stride_bytes"], 264)
        self.assertLess(small["ring_wraps_nominal"], large["ring_wraps_nominal"])
        self.assertFalse(small["header_can_straddle_cacheline"])
        self.assertEqual(analyze(64, 14563, [1, 1], 8, 8)["ring_padding_bytes"], 0)
        crossed = analyze(64, 14564, [1, 1], 8, 8)
        self.assertEqual(crossed["ring_padding_bytes"], 0)
        self.assertEqual(crossed["ring_wraps_completed"], 1)


@unittest.skipUnless(os.environ.get("SALIAS_PHASE1_CLIENT"), "Set SALIAS_PHASE1_CLIENT for IPC smoke tests")
class ComparatorSmokeTest(unittest.TestCase):
    def run_client(self, *args):
        return subprocess.run([os.environ["SALIAS_PHASE1_CLIENT"], *args],
                              capture_output=True, text=True, timeout=10)

    def test_modes_deliver_and_emit_consistent_counters(self):
        for mode in ("off", "counters", "timed"):
            with self.subTest(mode=mode):
                result = self.run_client("--scenario", "fifo", "--messages", "1000",
                                         "--producers", "1", "--consumers", "2",
                                         "--payload", "64", "--diagnostic", mode)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn(" published=1000 delivered=2000 ", result.stdout)
                if mode == "off":
                    self.assertNotIn("diagnostic_", result.stdout)
                    continue
                self.assertIn("diagnostic_claim_success=1000\n", result.stdout)
                for i in range(2):
                    self.assertIn(f"diagnostic_consumer{i}_poll_total_messages=1000\n", result.stdout)
                self.assertEqual("diagnostic_claim_avg_ns=" in result.stdout, mode == "timed")

    def test_unsupported_diagnostic_topologies_fail_closed(self):
        for args in (("--diagnostic", "invalid"),
                     ("--diagnostic", "timed", "--producers", "2"),
                     ("--diagnostic", "counters", "--producers", "1", "--batch-size", "2"),
                     ("--diagnostic", "timed", "--producers", "1", "--rate", "1000")):
            with self.subTest(args=args):
                result = self.run_client(*args)
                self.assertEqual(result.returncode, 2)
                self.assertIn("diagnostic requires", result.stderr)


if __name__ == "__main__":
    unittest.main()
