#!/usr/bin/env python3
"""Regression tests for experiment isolation, aggregation and scheduler evidence."""

import copy
from pathlib import Path
import subprocess
import unittest
from unittest.mock import Mock, patch

from analyze_switches import correlate, parse_switches
from build_design_variants import lazy_poll, replace_once, split_cursor
from build_stage_probe import instrument as instrument_stages
from build_worker_scheduler import instrument as instrument_scheduler
from diagnose_design import scheduled_command, stop_worker, summarize
from frame_acquire_prototype import transform as frame_protocol


class DesignDiagnosticsTests(unittest.TestCase):
    def test_summary_does_not_pool_rounds_or_profiles(self):
        record = dict(variant="baseline", origin="attempt", rate=1000000, payload=64,
                      phase="measured", profiled=False,
                      result={f"latency_{m}_ns": "100" for m in ("p50", "p99", "p999", "max")})
        second = copy.deepcopy(record)
        second["result"]["latency_p999_ns"] = "1000"
        profiled = {**record, "profiled": True}
        warmup = {**record, "phase": "warmup"}
        rows = summarize([record, second, profiled, warmup])
        self.assertEqual(rows[0]["rounds"], 2)
        self.assertEqual(rows[0]["p999"], dict(median_ns=550, min_ns=100, max_ns=1000))

    def test_summary_separates_scheduler_and_priority(self):
        record = dict(variant="baseline", origin="planned", rate=1000000, payload=64,
                      phase="measured", profiled=False,
                      result={f"latency_{m}_ns": "100" for m in ("p50", "p99", "p999", "max")})
        rows = summarize([record, {**record, "scheduler": "fifo-workers", "fifo_priority": 1},
                          {**record, "scheduler": "fifo-workers", "fifo_priority": 2}])
        self.assertEqual(len(rows), 3)

    def test_worker_scheduler_does_not_elevate_controller_or_aeron_conductors(self):
        root = Path(__file__).resolve().parents[2]
        for library in ("salias", "aeron"):
            source = (root / f"tools/aeron_compare/{library}_ipc_compare.cpp").read_text()
            generated = instrument_scheduler(source, library == "aeron")
            self.assertEqual(generated.count("    activate_fifo_worker();"), 2)
            self.assertIn("::sched_setscheduler(0, SCHED_OTHER, &parameter)", generated)
            self.assertIn("SCHED_FIFO | SCHED_RESET_ON_FORK", generated)
            self.assertLess(generated.index("int experimental_fifo_priority"), generated.index("Options parse_options"))
        self.assertEqual(scheduled_command(["probe"], 0), ["probe"])
        self.assertEqual(scheduled_command(["probe"], 1),
                         ["sudo", "-n", "chrt", "--reset-on-fork", "--fifo", "1", "probe"])

    def test_frame_protocol_keeps_holes_and_registration_guard(self):
        root = Path(__file__).resolve().parents[2]
        source = (root / "src/core/channel/shared_hybrid_mpsc.hpp").read_text()
        generated = frame_protocol(source)
        self.assertIn("meta & ~frame::FLAG_COMMITTED", generated)
        self.assertIn("for (auto position = published_pos_; position < end;)", generated)
        self.assertEqual(generated.count("ClaimReservationGuard reservation_guard"),
                         source.count("ClaimReservationGuard reservation_guard"))
        self.assertIn("if constexpr (!(Ordering == Order::Fifo && !DropOldest))", generated)
        self.assertNotIn("hybrid_detail::store_header_plain(", generated)
        self.assertNotIn("hybrid_detail::load_header_plain(", generated)
        checked = frame_protocol(source, checked=True)
        self.assertNotIn("visible_bound = position + producer.ring.capacity()", checked)
        self.assertIn(".load(std::memory_order_acquire);", checked)
        self.assertIn("if (position >= visible_bound)", checked)

    def test_switch_correlation_uses_integer_monotonic_time(self):
        text = """ trace  123 [001] 84743.150012: PERF_RECORD_SWITCH OUT preempt
 trace  123 [001] 84743.150216: PERF_RECORD_SWITCH IN
 trace  456 [003] 84743.150001: PERF_RECORD_SWITCH OUT
 trace  456 [003] 84748.150001: PERF_RECORD_SWITCH IN
"""
        spans, cpus = parse_switches(text)
        self.assertEqual(spans[0]["off_cpu_ns"], 204000)
        self.assertEqual(cpus["123"], {1})
        event = dict(send_ns=84743150010983, receive_ns=84743150221080, latency_ns=210097)
        rows = correlate([event], spans)
        self.assertEqual(len(rows[0]["overlaps"]), 1)
        self.assertEqual(rows[0]["overlaps"][0]["overlap_ns"], 204000)

    def test_variant_anchors_fail_closed(self):
        with self.assertRaises(ValueError):
            replace_once("nothing", "missing", "x")
        with self.assertRaises(ValueError):
            replace_once("xx", "x", "y")

    def test_stage_probes_reject_unsafe_payloads_and_origins(self):
        root = Path(__file__).resolve().parents[2]
        for library in ("salias", "aeron"):
            with self.subTest(library=library):
                source = (root / f"tools/aeron_compare/{library}_ipc_compare.cpp").read_text()
                generated = instrument_stages(source, library == "aeron")
                self.assertIn('options.payload < 32 || options.latency_origin != "attempt"', generated)
                self.assertIn('stage probe requires payload >= 32', generated)
                self.assertLess(generated.index('stage probe requires payload >= 32'),
                                generated.index("  return options;"))

    @patch("diagnose_design.subprocess.run")
    def test_privileged_cleanup_targets_only_owned_process_group(self, run):
        run.return_value = Mock(returncode=0)
        process = Mock(pid=123456)
        process.wait.side_effect = [subprocess.TimeoutExpired("perf", 3), 0]
        stop_worker(process, privileged=True)
        self.assertEqual([call.args[0] for call in run.call_args_list], [
            ["sudo", "-n", "kill", "-TERM", "--", "-123456"],
            ["sudo", "-n", "kill", "-KILL", "--", "-123456"],
        ])

    @patch("diagnose_design.harness.stop")
    def test_plain_cleanup_does_not_request_privilege(self, stop):
        process = Mock()
        stop_worker(process)
        stop.assert_called_once_with(process)

    def test_experimental_layout_keeps_total_padding_constant(self):
        root = Path(__file__).resolve().parents[2]
        channel = (root / "src/salias/src/channel.cpp").read_text()
        changed = split_cursor(channel)
        self.assertIn("alignas(64) std::uint64_t local_sequence", changed)
        self.assertIn("kCacheLineGuard - 64 - 7 * sizeof", changed)
        header = (root / "src/salias/include/salias/subscriber.hpp").read_text()
        self.assertLess(lazy_poll(header).index("handler(first)"), lazy_poll(header).index("Message buffer[kChunk]"))


if __name__ == "__main__":
    unittest.main()
