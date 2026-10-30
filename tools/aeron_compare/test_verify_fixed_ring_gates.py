#!/usr/bin/env python3
"""Synthetic tests for fixed-ring performance gate evaluation."""

import unittest

import verify_fixed_ring_gates as gates


def measured_group(topology, kind, payload, engine, throughput, p50, p99, rounds=5):
    group = {
        "topology": topology,
        "kind": kind,
        "payload": payload,
        "origin": "claim" if kind == "throughput" else "attempt",
        "engine": engine,
        "rounds": rounds,
    }
    if kind == "throughput":
        group["publish_mmsg_s"] = {"median": throughput, "min": throughput, "max": throughput}
    else:
        group["latency_ns"] = {
            name: {"median": value, "min": value, "max": value}
            for name, value in (("p50", p50), ("p99", p99), ("p999", p99))
        }
    return group


def passing_summaries():
    fast = []
    baseline = []
    for topology in gates.TOPOLOGIES:
        for payload in gates.PAYLOADS:
            fast.extend((
                measured_group(topology, "throughput", payload, "salias", 130, 0, 0),
                measured_group(topology, "throughput", payload, "aeron", 100, 0, 0),
                measured_group(topology, "latency", payload, "salias", 0, 100, 110),
                measured_group(topology, "latency", payload, "aeron", 0, 200, 100),
            ))
            baseline.append(
                measured_group(topology, "throughput", payload, "salias", 100, 0, 0))
    return {"groups": fast}, {"groups": baseline}


class FixedRingGateTests(unittest.TestCase):
    def test_all_cells_pass_at_inclusive_thresholds(self):
        fast, baseline = passing_summaries()
        report = gates.verify_gates(fast, baseline)
        self.assertTrue(report["passed"], report["failures"])
        self.assertEqual(len(report["cells"]), 8)

    def test_attempt_p99_failure_is_reported(self):
        fast, baseline = passing_summaries()
        group = next(group for group in fast["groups"]
                     if group["topology"] == "mpmc" and group["payload"] == 256 and
                     group["kind"] == "latency" and group["engine"] == "salias")
        group["latency_ns"]["p99"]["median"] = 111
        report = gates.verify_gates(fast, baseline)
        self.assertFalse(report["passed"])
        self.assertIn("mpmc/256B: attempt p99 exceeds 1.10x Aeron", report["failures"])

    def test_attempt_p50_failure_is_reported(self):
        fast, baseline = passing_summaries()
        group = next(group for group in fast["groups"]
                     if group["topology"] == "spsc" and group["payload"] == 64 and
                     group["kind"] == "latency" and group["engine"] == "salias")
        group["latency_ns"]["p50"]["median"] = 201
        report = gates.verify_gates(fast, baseline)
        self.assertFalse(report["passed"])
        self.assertIn("spsc/64B: attempt p50 exceeds Aeron", report["failures"])

    def test_fifo_baseline_regression_is_reported(self):
        fast, baseline = passing_summaries()
        group = next(group for group in fast["groups"]
                     if group["topology"] == "mpsc" and group["payload"] == 256 and
                     group["kind"] == "throughput" and group["engine"] == "salias")
        group["publish_mmsg_s"]["median"] = 120
        report = gates.verify_gates(fast, baseline)
        self.assertFalse(report["passed"])
        self.assertIn("mpsc/256B: FIFO throughput gain below 1.25x", report["failures"])

    def test_short_runs_cannot_be_signed_off(self):
        fast, baseline = passing_summaries()
        fast["environment"] = {"workload": {"measured_rounds": 5}}
        for group in fast["groups"]:
            group["rounds"] = 4
        report = gates.verify_gates(fast, baseline)
        self.assertFalse(report["passed"])
        self.assertTrue(all("only 4 measured rounds across 1 groups" in failure
                            for failure in report["failures"]))

    def test_dedicated_driver_requires_matching_suite_environment(self):
        fast, baseline = passing_summaries()
        report = gates.verify_gates(fast, baseline, require_dedicated_driver=True)
        self.assertFalse(report["passed"])
        self.assertIn("suite was not run with --require-dedicated-driver",
                      report["failures"])

        fast["environment"] = {
            "cpu_capacity": {
                "require_dedicated_driver": True,
                "dedicated_driver_available": True,
            },
        }
        report = gates.verify_gates(fast, baseline, require_dedicated_driver=True)
        self.assertTrue(report["passed"], report["failures"])

    def test_final_protocol_requires_six_abba_groups_of_twenty_full_verified_rounds(self):
        fast, baseline = passing_summaries()
        workload = {
            "groups": 6,
            "measured_rounds_per_engine_per_group": 20,
            "engine_order": "ABBA paired rounds",
            "verification": "full",
        }
        for summary in (fast, baseline):
            summary["environment"] = {
                "workload": dict(workload),
                "cpu_capacity": {
                    "require_dedicated_driver": True,
                    "dedicated_driver_available": True,
                },
            }
            for group in summary["groups"]:
                group["rounds"] = 120
                group["groups"] = 6

        report = gates.verify_gates(
            fast, baseline, minimum_rounds=20, minimum_groups=6,
            require_dedicated_driver=True, require_full_verification=True,
            require_abba=True, require_matched_protocol=True,
        )
        self.assertTrue(report["passed"], report["failures"])

        fast["environment"]["workload"]["engine_order"] = "alternating"
        report = gates.verify_gates(
            fast, baseline, minimum_rounds=20, minimum_groups=6,
            require_dedicated_driver=True, require_full_verification=True,
            require_abba=True, require_matched_protocol=True,
        )
        self.assertFalse(report["passed"])
        self.assertIn("suite was not run with ABBA paired-round ordering", report["failures"])
        self.assertTrue(any("baseline protocol engine_order" in failure
                            for failure in report["failures"]))

    def test_full_verification_must_cover_fast_and_baseline(self):
        fast, baseline = passing_summaries()
        fast["environment"] = {"workload": {"verification": "full"}}
        baseline["environment"] = {"workload": {"verification": "none"}}
        report = gates.verify_gates(fast, baseline, require_full_verification=True)
        self.assertFalse(report["passed"])
        self.assertIn("baseline suite was not run with --verify full", report["failures"])


if __name__ == "__main__":
    unittest.main()
