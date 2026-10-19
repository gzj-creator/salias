#!/usr/bin/env python3
"""Regression tests for source-derived layout and retained baseline evidence."""

import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from analyze_frame_layout import analyze, source_constants, summarize_baseline


class FrameLayoutTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)

    def baseline(self):
        path = self.directory / "summary.json"
        path.write_text(json.dumps(dict(environment=dict(commit="retained-baseline"))))
        records = []
        for payload in (64, 256):
            for round_number in range(1, 6):
                result = dict(published="100000000", delivered="200000000", producers="1",
                              consumers="2", payload=str(payload), capacity="1048576",
                              scenario="fifo", batch_size="1", latency_samples="0",
                              latency_sample_rate="0", rate_per_producer="0",
                              seconds=str(10 + round_number))
                stdout = self.directory / f"{payload}-r{round_number}.stdout"
                stdout.write_text("RESULT " + " ".join(f"{k}={v}" for k, v in result.items()) + "\n")
                records.append(dict(engine="salias", kind="throughput", phase="measured",
                                    round=round_number, payload=payload, result=result,
                                    stdout=stdout.name,
                                    stdout_sha256=hashlib.sha256(stdout.read_bytes()).hexdigest()))
        records += [dict(engine="salias", kind="throughput", phase="warmup", payload=64),
                    dict(engine="salias", kind="latency", phase="measured", payload=64),
                    dict(engine="aeron", kind="throughput", phase="measured", payload=64)]
        self.save(records)
        return path, records

    def save(self, records):
        (self.directory / "results.jsonl").write_text("\n".join(json.dumps(r) for r in records))

    def refresh_stdout(self, record):
        path = self.directory / record["stdout"]
        path.write_text("RESULT " + " ".join(f"{k}={v}" for k, v in record["result"].items()) + "\n")
        record["stdout_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()

    def test_actual_layout_and_source_evidence(self):
        constants = source_constants()
        self.assertEqual((constants["kHeaderSize"], constants["kFrameAlign"]), (8, 8))
        self.assertEqual(len(constants["source_sha256"]), 6)
        self.assertIn("src/core/platform/mapping.cpp", constants["source_sha256"])
        for payload, expected in ((63, 72), (64, 72), (65, 80), (255, 264), (256, 264), (257, 272)):
            with self.subTest(payload=payload):
                self.assertEqual(analyze(payload, 100, [1, 2], 8, 8)["frame_stride_bytes"], expected)

    def test_density_is_not_whole_frames_and_headers_do_not_straddle(self):
        for payload in (64, 256):
            row = analyze(payload, 100_000_000, [12, 16], 8, 8)
            self.assertEqual(row["header_offsets_mod64"], list(range(0, 64, 8)))
            self.assertFalse(row["header_can_straddle_cacheline"])
            self.assertFalse(row["frame_stride_cacheline_aligned"])
            self.assertEqual(row["frame_stride_mod64"], 8)
            self.assertEqual(row["complete_frames_per_cacheline"], 0)
            self.assertAlmostEqual(row["frames_per_cacheline_density"], 64 / (payload + 8))

    def test_crossing_physical_boundary_adds_no_padding(self):
        for payload, last in ((64, 14563), (256, 3971)):
            with self.subTest(payload=payload):
                before = analyze(payload, last, [1, 1], 8, 8)
                after = analyze(payload, last + 1, [1, 1], 8, 8)
                self.assertEqual(before["ring_wraps_completed"], 0)
                self.assertEqual(after["ring_wraps_completed"], 1)
                self.assertEqual(after["ring_padding_bytes"], 0)
                self.assertEqual(after["total_frame_bytes"], (last + 1) * (payload + 8))

    def test_invalid_layout_parameters_and_durations_fail(self):
        valid = dict(payload=64, messages=100, durations=[1, 2], header=8, alignment=8, capacity=1048576)
        invalid = [dict(payload=-1), dict(messages=-1), dict(header=0), dict(alignment=0),
                   dict(alignment=3), dict(capacity=0), dict(capacity=1000), dict(capacity=32),
                   dict(messages=True), dict(payload=1.5), dict(durations=[]),
                   dict(durations=[0]), dict(durations=[-1]), dict(durations=[float("nan")]),
                   dict(durations=[float("inf")])]
        for override in invalid:
            with self.subTest(override=override), self.assertRaises(ValueError):
                analyze(**(valid | override))

    def test_retained_baseline_excludes_warmup_other_engines_and_latency(self):
        baseline, records = self.baseline()
        result = summarize_baseline(baseline)
        self.assertEqual(result["baseline_commit"], "retained-baseline")
        self.assertEqual(result["baseline_sha256"], hashlib.sha256(baseline.read_bytes()).hexdigest())
        self.assertAlmostEqual(result["comparison"]["wrap_ratio_64_to_256"], 3 / 11)
        self.assertFalse(result["comparison"]["wraps_64b_more_than_twice_256b"])
        self.assertFalse(result["comparison"]["frame_alignment_root_cause_confirmed"])
        for layout in result["layouts"]:
            self.assertEqual(len(layout["measured_rounds"]), 5)
            self.assertEqual(layout["duration_s_range"], [11, 15])
        self.assertEqual(len(records), 13)

    def test_duplicate_or_missing_rounds_fail(self):
        baseline, records = self.baseline()
        records[1]["round"] = 1
        self.save(records)
        with self.assertRaisesRegex(ValueError, "five consistent"):
            summarize_baseline(baseline)
        records.pop(1)
        self.save(records)
        with self.assertRaisesRegex(ValueError, "five consistent"):
            summarize_baseline(baseline)

    def test_changed_stdout_or_result_metadata_fail(self):
        baseline, records = self.baseline()
        stdout = self.directory / records[0]["stdout"]
        original = stdout.read_bytes()
        stdout.write_text("changed")
        with self.assertRaisesRegex(ValueError, "checksum"):
            summarize_baseline(baseline)
        stdout.write_bytes(original)
        records[0]["result"]["seconds"] = "99"
        self.save(records)
        with self.assertRaisesRegex(ValueError, "RESULT metadata"):
            summarize_baseline(baseline)

    def test_uses_actual_capacity_and_rejects_mixed_comparisons(self):
        baseline, records = self.baseline()
        for record in records[:10]:
            record["result"]["capacity"] = "2097152"
            self.refresh_stdout(record)
        self.save(records)
        result = summarize_baseline(baseline)
        self.assertEqual(result["layouts"][0]["capacity_bytes"], 2097152)
        self.assertEqual(result["layouts"][0]["max_inflight_frames"], 2097152 // 72)
        for record in records[5:10]:
            record["result"]["capacity"] = "1048576"
            self.refresh_stdout(record)
        self.save(records)
        with self.assertRaisesRegex(ValueError, "equal ring capacities"):
            summarize_baseline(baseline)

    def test_inconsistent_message_counts_fail_comparison(self):
        baseline, records = self.baseline()
        for record in records[5:10]:
            record["result"].update(published="1000", delivered="2000")
            self.refresh_stdout(record)
        self.save(records)
        with self.assertRaisesRegex(ValueError, "equal message counts"):
            summarize_baseline(baseline)

    def test_incomplete_delivery_is_rejected(self):
        baseline, records = self.baseline()
        records[0]["result"]["delivered"] = "1"
        self.refresh_stdout(records[0])
        self.save(records)
        with self.assertRaisesRegex(ValueError, "Delivery/topology"):
            summarize_baseline(baseline)

    def test_invalid_duration_in_middle_round_is_rejected(self):
        baseline, records = self.baseline()
        for value in ("nan", "inf", "0", "-1"):
            with self.subTest(value=value):
                records[2]["result"]["seconds"] = value
                self.refresh_stdout(records[2])
                self.save(records)
                with self.assertRaisesRegex(ValueError, "Invalid measured duration"):
                    summarize_baseline(baseline)

    def test_changed_ring_or_double_mapping_fails_closed(self):
        read_text = Path.read_text
        for filename, old, new in (
                ("magic_ring.cpp", "return {base_ + (pos & mask()), len};", "return {};"),
                ("mapping.cpp", "::mmap(base + size, size,", "::mmap(base + size, size / 2,")):
            def changed(path, *args, **kwargs):
                text = read_text(path, *args, **kwargs)
                return text.replace(old, new) if path.name == filename else text
            with self.subTest(filename=filename), patch.object(Path, "read_text", changed):
                with self.assertRaisesRegex(ValueError, "changed"):
                    source_constants()


if __name__ == "__main__":
    unittest.main()
