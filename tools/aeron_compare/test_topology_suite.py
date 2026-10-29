#!/usr/bin/env python3
"""Synthetic tests for the four-topology comparison runner."""

import unittest

import run_topology_suite as runner


def latency_row(engine, value, round_number=1):
    published = "2000000"
    delivered = "2000000"
    return {
        "phase": "measured",
        "round": round_number,
        "engine": engine,
        "topology": "spsc",
        "kind": "latency",
        "payload": 64,
        "origin": "attempt",
        "min_mem_available_kib": 1200000,
        "result": {
            "publish_msg_per_sec": "1000000",
            "delivery_msg_per_sec": "1000000",
            "seconds": "2",
            "published": published,
            "delivered": delivered,
            "latency_samples": "125000",
        },
        "worst_subscriber_ns": {
            "p50": value,
            "p99": value * 2,
            "p999": value * 3,
            "max": value * 4,
        },
        "consumers_detail": [{
            "p50_ns": str(value),
            "p99_ns": str(value * 2),
            "p999_ns": str(value * 3),
            "max_ns": str(value * 4),
        }],
    }


class TopologySuiteTests(unittest.TestCase):
    def test_all_four_topologies_are_explicit(self):
        self.assertEqual(runner.TOPOLOGIES, {
            "spsc": {"producers": 1, "consumers": 1},
            "mpsc": {"producers": 2, "consumers": 1},
            "spmc": {"producers": 1, "consumers": 2},
            "mpmc": {"producers": 2, "consumers": 2},
        })

    def test_cpu_layout_reserves_cpu_zero_only_when_possible(self):
        self.assertEqual(runner.cpu_layout(1, 1)["worker_cpu_base"], 1)
        self.assertEqual(runner.cpu_layout(2, 1)["worker_cpu_base"], 1)
        self.assertEqual(runner.cpu_layout(1, 2)["worker_cpu_base"], 1)
        mpmc = runner.cpu_layout(2, 2)
        self.assertEqual(mpmc["worker_cpu_base"], 0)
        self.assertEqual(mpmc["driver_cpu"], 0)

    def test_worker_roles_place_consumers_before_producers(self):
        roles = runner.worker_roles(2, 2, 0)
        self.assertEqual(
            [(role["role"], role["index"], role["cpu"]) for role in roles],
            [("subscriber", 0, 0), ("subscriber", 1, 1),
             ("publisher", 0, 2), ("publisher", 1, 3)],
        )

    def test_expected_counts_cover_fanout_and_sampling(self):
        self.assertEqual(runner.expected_counts("latency", 2, 2, 1000000, 16),
                         (2000000, 4000000, 250000))
        self.assertEqual(runner.expected_counts("throughput", 1, 2, 10000000, 0),
                         (10000000, 20000000, 0))

    def test_summary_separates_engines_and_uses_worst_subscriber(self):
        rows = [latency_row("salias", 300), latency_row("aeron", 200)]
        summary = runner.summarize(rows, {"test": True})
        self.assertEqual(summary["measured_runs"], 2)
        self.assertEqual(len(summary["groups"]), 2)
        comparison = summary["paired"][0]
        self.assertEqual(comparison["latency_ns"]["p50"]["salias_minus_aeron_median"], 100)
        self.assertEqual(comparison["publish_mmsg_s"]["salias_over_aeron"], 1)

    def test_summary_excludes_warmup(self):
        rows = [latency_row("salias", 300), latency_row("aeron", 200)]
        rows[0]["phase"] = "warmup"
        summary = runner.summarize(rows, {})
        self.assertEqual(summary["warmup_runs"], 1)
        self.assertEqual(len(summary["paired"]), 0)


if __name__ == "__main__":
    unittest.main()
