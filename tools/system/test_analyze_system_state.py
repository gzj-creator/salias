#!/usr/bin/env python3
"""Regression tests for runtime-state correlation."""

import json
from pathlib import Path
import tempfile
import unittest

from analyze_system_state import summarize


class AnalyzeSystemStateTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.output = Path(self.temporary.name)
        self.monitor = self.output / "system-monitor"
        self.monitor.mkdir()

    def build(self, high_cpu=80.0, low_cpu=40.0, high_irq=750.0, low_irq=1000.0):
        records = []
        for number, mode in enumerate(("high", "low"), start=1):
            start = number * 10_000_000_000
            finish = start + 2_000_000_000
            rate = 12_000_000 if mode == "high" else 6_000_000
            records.append({
                "number": number,
                "group": "group1-direct-64b",
                "phase": "measured",
                "payload": 64,
                "started_monotonic_ns": start,
                "finished_monotonic_ns": finish,
                "started_at_ns": start + 1,
                "finished_at_ns": finish + 1,
                "result": {"publish_msg_per_sec": str(rate)},
            })
        (self.output / "results.jsonl").write_text(
            "".join(json.dumps(item) + "\n" for item in records)
        )
        timeline = ["timestamp_ns monotonic_ns load1 load5 load15 running_tasks process_count"]
        cpu_lines = [
            "Linux 6.8.0 test host 10/19/26 _x86_64_ (4 CPU)",
            "",
        ]
        for second, cpu, irq in (
            (10_000_000_000, high_cpu, high_irq),
            (11_000_000_000, high_cpu, high_irq),
            (20_000_000_000, low_cpu, low_irq),
            (21_000_000_000, low_cpu, low_irq),
        ):
            offset = int(__import__("time").localtime(second // 1_000_000_000).tm_gmtoff)
            local_second = second // 1_000_000_000 + offset
            timestamp = (
                f"{local_second // 3600:02d}:"
                f"{local_second % 3600 // 60:02d}:{local_second % 60:02d}"
            )
            load = 1.0 if second < 20_000_000_000 else 2.0
            timeline.append(
                f"{second} {second} "
                f"{load} 0 0 1 100"
            )
            cpu_lines.append(
                f"{timestamp} CPU %usr %nice %sys %iowait %irq %soft %steal %guest %gnice %idle"
            )
            for sample_cpu in ("1", "2", "3"):
                idle = 100.0 - cpu
                cpu_lines.append(
                    f"{timestamp} {sample_cpu} {cpu:.2f} 0 0 0 0 0 0 0 0 {idle:.2f}"
                )
            cpu_lines.extend(["", f"{timestamp} CPU intr/s"])
            for sample_cpu in ("1", "2", "3"):
                cpu_lines.append(f"{timestamp} {sample_cpu} {irq:.2f}")
            cpu_lines.append("")
        (self.monitor / "timeline.log").write_text("\n".join(timeline) + "\n")
        (self.monitor / "mpstat.log").write_text("\n".join(cpu_lines) + "\n")
        (self.monitor / "interrupts.log").write_text(
            "           CPU0 CPU1 CPU2 CPU3\n"
            " 29: 1 2 3 4 IO-APIC 29-edge virtio\n"
        )

    def test_high_mode_cpu_and_irq_are_correlated(self):
        self.build()
        result = summarize(self.output)
        comparison = result["comparison"]
        self.assertEqual(comparison["high_mode_rounds"], 1)
        self.assertEqual(comparison["low_mode_rounds"], 1)
        self.assertAlmostEqual(comparison["cpu"]["1"]["high_percent"], 80.0)
        self.assertAlmostEqual(comparison["cpu"]["1"]["low_percent"], 40.0)
        self.assertAlmostEqual(comparison["irq"]["1"]["high_per_second"], 750.0)
        self.assertIn("high mode may use more CPU",
                      result["decision"]["all_rounds"]["conclusion"])
        self.assertEqual(result["decision"]["all_rounds"]["significant_cpu_cpus"],
                         ["1", "2", "3"])

    def test_preload_rounds_participate_in_mode_comparison(self):
        self.build()
        records = [json.loads(line) for line in
                   (self.output / "results.jsonl").read_text().splitlines()]
        records[1]["phase"] = "preload"
        (self.output / "results.jsonl").write_text(
            "".join(json.dumps(item) + "\n" for item in records)
        )
        result = summarize(self.output)
        self.assertEqual(result["measured_rounds"], 1)
        self.assertEqual(result["all_rounds"], 2)
        self.assertEqual(result["comparison"]["high_mode_rounds"], 1)
        self.assertEqual(result["comparison"]["low_mode_rounds"], 1)

    def test_irq_only_difference_is_reported(self):
        self.build(high_cpu=50.0, low_cpu=50.0, high_irq=500.0, low_irq=1000.0)
        result = summarize(self.output)
        self.assertIn("lower IRQ rate",
                      result["decision"]["all_rounds"]["conclusion"])
        self.assertEqual(result["decision"]["all_rounds"]["significant_irq_cpus"],
                         ["1", "2", "3"])

    def test_same_payload_comparison_is_reported(self):
        self.build()
        records = [json.loads(line) for line in
                   (self.output / "results.jsonl").read_text().splitlines()]
        records[1]["payload"] = 192
        (self.output / "results.jsonl").write_text(
            "".join(json.dumps(item) + "\n" for item in records)
        )
        result = summarize(self.output)
        self.assertEqual(result["mode_breakdown"]["same_payload"], [])

    def test_missing_time_boundary_is_rejected(self):
        self.build()
        records = [json.loads(line) for line in
                   (self.output / "results.jsonl").read_text().splitlines()]
        del records[0]["started_monotonic_ns"]
        (self.output / "results.jsonl").write_text(
            "".join(json.dumps(item) + "\n" for item in records)
        )
        with self.assertRaisesRegex(ValueError, "lacks time boundaries"):
            summarize(self.output)


if __name__ == "__main__":
    unittest.main()
