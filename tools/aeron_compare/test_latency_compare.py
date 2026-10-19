#!/usr/bin/env python3
"""Small synthetic tests; no benchmark processes or real host load."""

import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import run_latency_compare as runner
import summarize_latency as summary


def fixture(round_number=1):
    return {"origin": "planned", "producers": 1, "payload": 64, "rate": 2000,
            "engine": "fifo", "duration_s": 1, "phase": "measured", "round": round_number,
            "min_mem_available_kib": 1200000,
            "result": {"published": "2000", "delivered": "2000", "latency_samples": "125",
                       "consumers": "1", "latency_origin": "planned",
                       "latency_p50_ns": "200", "latency_p99_ns": "300",
                       "latency_p999_ns": "400", "latency_max_ns": "399"},
            "consumers": [{"samples": "125"}],
            "resources": {"memory.events": "low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\n",
                          "memory.peak": "1000000"}}


class SummaryTests(unittest.TestCase):
    def summarize_rows(self, rows):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "results.jsonl"
            path.write_text("".join(json.dumps(row) + "\n" for row in rows))
            return summary.summarize([path])

    def test_excludes_warmup_and_reports_round_medians_and_ranges(self):
        rows = [fixture(i) for i in (0, 1, 2, 3)]
        rows[0]["phase"] = "warmup"
        for row, value in zip(rows, (9999, 100, 500, 300)):
            row["result"]["latency_p999_ns"] = str(value)
        result = self.summarize_rows(rows)
        group = result["groups"][0]
        self.assertEqual(result["measured_run_count"], 3)
        self.assertEqual(group["round_median_ns"]["p999"], 300)
        self.assertEqual(group["round_min_ns"]["p999"], 100)
        self.assertEqual(group["round_max_ns"]["p999"], 500)
        self.assertEqual(group["sampled_max_ns"], 399)

    def test_does_not_pool_different_origins(self):
        planned = fixture()
        attempt = copy.deepcopy(planned)
        attempt["origin"] = attempt["result"]["latency_origin"] = "attempt"
        self.assertEqual(len(self.summarize_rows([planned, attempt])["groups"]), 2)

    def test_rejects_bad_counts_samples_pressure_and_rounds(self):
        for field, value in (("delivered", "1999"), ("latency_samples", "124")):
            with self.subTest(field=field):
                row = fixture()
                row["result"][field] = value
                with self.assertRaises(ValueError):
                    self.summarize_rows([row])
        row = fixture()
        row["resources"]["memory.events"] = "high 1\noom_kill 0\n"
        with self.assertRaises(ValueError):
            self.summarize_rows([row])
        for rows in ([fixture(), fixture()], [fixture(2)]):
            with self.assertRaises(ValueError):
                self.summarize_rows(rows)


class ResourceTests(unittest.TestCase):
    def test_rejects_missing_limits_and_cpu_zero(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            (path / "memory.max").write_text(str(704 * 1024 * 1024))
            (path / "memory.swap.max").write_text("0")
            with patch.object(runner, "cgroup_directory", return_value=path), \
                    patch.object(runner.os, "sched_getaffinity", return_value={1, 2, 3}), \
                    patch.dict(runner.os.environ, {"SALIAS_RESOURCE_CAPPED": "1"}):
                runner.check_resource_limits()
                for limit in ("max", str(705 * 1024 * 1024)):
                    (path / "memory.max").write_text(limit)
                    with self.assertRaises(RuntimeError):
                        runner.check_resource_limits()
                (path / "memory.max").write_text(str(704 * 1024 * 1024))
                with patch.object(runner.os, "sched_getaffinity", return_value={0, 1, 2, 3}):
                    with self.assertRaises(RuntimeError):
                        runner.check_resource_limits()

    def test_low_memory_never_launches_workers(self):
        with patch.object(runner, "available_kib", return_value=600 * 1024), \
                patch.object(runner.subprocess, "Popen") as popen:
            with self.assertRaises(RuntimeError):
                runner.run_case(None, None, 1)
            popen.assert_not_called()

    def test_stops_entire_process_group(self):
        process = Mock(pid=1234)
        with patch.object(runner.os, "killpg") as killpg:
            runner.stop(process)
            killpg.assert_called_once_with(1234, runner.signal.SIGTERM)
            process.wait.assert_called_once_with(timeout=3)


if __name__ == "__main__":
    unittest.main()
