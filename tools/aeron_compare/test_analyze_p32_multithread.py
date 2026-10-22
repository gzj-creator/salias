#!/usr/bin/env python3
"""Synthetic tests for the independent P3-2 matrix analyzer."""

from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import analyze_p32_multithread as analyzer


VALID_CGROUP = """CGROUP_EVIDENCE_BEGIN version=1 cgroup_path=/test
[CGROUP_FILE cpu.stat]
usage_usec 1
[CGROUP_FILE memory.events]
low 0
high 0
max 0
oom 0
oom_kill 0
oom_group_kill 0
[CGROUP_FILE memory.peak]
4096
[CGROUP_FILE memory.high]
671088640
[CGROUP_FILE memory.max]
738197504
[CGROUP_FILE memory.swap.max]
0
[CGROUP_FILE memory.swap.current]
0
CGROUP_EVIDENCE_END
"""

VALID_PROBE = """\
PROBE identity=p3-p32-publisher-thread-probe-v1 source_sha256=6a23a77598c6e980f7b6625eb2471cc42a26917469dc77fc51ab9d5b767bab1e barrier=A publisher_mode=st
THREAD_STATE role=publisher mode=st expected_threads=1 actual_threads=1 libc_single_threaded_available=1 libc_single_threaded=1 before_publish=1
RESULT published=1000 delivered=1000 seconds=1 publish_msg_per_sec=1000 delivery_msg_per_sec=1000 latency_samples=0 latency_p50_ns=0 latency_p99_ns=0 latency_p999_ns=0 latency_max_ns=0
"""


class AnalyzerTests(unittest.TestCase):
    def test_summary_is_unpooled_statistics(self):
        result = analyzer.summarize([1.0, 2.0, 3.0])
        self.assertEqual(result["n"], 3)
        self.assertEqual(result["median"], 2.0)
        self.assertEqual(result["mean"], 2.0)
        self.assertGreater(result["stddev"], 0)

    def test_valid_cgroup_stays_within_frozen_limits(self):
        values = analyzer.validate_cgroup(VALID_CGROUP)
        self.assertEqual(values["memory.high"], "671088640")
        self.assertEqual(values["memory.swap.current"], "0")

    def test_memory_event_is_not_silently_accepted(self):
        damaged = VALID_CGROUP.replace("oom 0", "oom 1")
        with self.assertRaisesRegex(analyzer.AuditError, "memory event oom"):
            analyzer.validate_cgroup(damaged)

    def test_thread_mode_mismatch_is_rejected(self):
        damaged = VALID_PROBE.replace("actual_threads=1", "actual_threads=2")
        with self.assertRaisesRegex(analyzer.AuditError, "thread count differs"):
            analyzer.parse_probe(damaged, "throughput-64b", "ST")

    def test_libc_state_mismatch_is_rejected(self):
        damaged = VALID_PROBE.replace("libc_single_threaded=1", "libc_single_threaded=0")
        with self.assertRaisesRegex(analyzer.AuditError, "libc single-threaded"):
            analyzer.parse_probe(damaged, "throughput-64b", "ST")

    def test_duplicate_or_missing_control_line_is_rejected(self):
        with self.assertRaisesRegex(analyzer.AuditError, "expected one PROBE"):
            analyzer.parse_probe(VALID_PROBE + VALID_PROBE.splitlines()[0] + "\n",
                                 "throughput-64b", "ST")
        with self.assertRaisesRegex(analyzer.AuditError, "expected one RESULT"):
            analyzer.parse_probe("\n".join(VALID_PROBE.splitlines()[:2]) + "\n",
                                 "throughput-64b", "ST")


if __name__ == "__main__":
    unittest.main()
