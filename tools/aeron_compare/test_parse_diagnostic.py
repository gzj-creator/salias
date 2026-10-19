#!/usr/bin/env python3
"""Check pooled consumer metrics, threshold boundaries, and warmup exclusion."""

import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from parse_diagnostic import analyze_record, assess_hypotheses, summarize


class DiagnosticSummaryTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)

    def record(self, payload=64, mode="timed", round_number=1):
        path = self.directory / f"{payload}-{mode}-{round_number}.stdout"
        metrics = {}
        if mode != "off":
            metrics = dict(diagnostic_mode=mode, diagnostic_claim_success=1000,
                           diagnostic_claim_backpressure=100,
                           diagnostic_claim_yield_cycles=100,
                           diagnostic_claim_total_ns=200000 if mode == "timed" else 0)
            if mode == "timed":
                metrics["diagnostic_claim_avg_ns"] = 200
            for consumer, empty, polls in ((0, 10, 100), (1, 90, 50)):
                prefix = f"diagnostic_consumer{consumer}_"
                metrics.update({prefix + "poll_empty": empty, prefix + "poll_success": polls,
                                prefix + "poll_total_messages": 1000,
                                prefix + "avg_batch": 1000 / polls})
        path.write_text("\n".join(f"{key}={value}" for key, value in metrics.items()) + "\n")
        return dict(number=round_number, payload=payload, round=round_number, mode=mode,
                    phase="warmup" if round_number == 0 else "measured", messages=1000,
                    stdout=path.name, stdout_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                    result=dict(published="1000", delivered="2000", producers="1", consumers="2",
                                publish_msg_per_sec="10000000", seconds="0.0001"))

    def summary(self, modes=("off", "counters", "timed")):
        (self.directory / "environment.json").write_text(json.dumps(dict(modes=modes, rounds=5)))
        records = [self.record(payload, mode, r) for payload in (64, 256)
                   for mode in modes for r in range(6)]
        # Extreme warmup throughput must not affect measured summaries or pairing.
        for record in records:
            if record["round"] == 0:
                record["result"]["publish_msg_per_sec"] = "1"
        (self.directory / "results.jsonl").write_text("\n".join(json.dumps(r) for r in records))
        return summarize(self.directory)

    def test_consumers_are_pooled_by_observations_not_mean_ratios(self):
        row = analyze_record(self.record(), self.directory)
        self.assertAlmostEqual(row["consumer_empty_rate"], 100 / 250)
        self.assertAlmostEqual(row["avg_batch"], 2000 / 150)
        self.assertNotAlmostEqual(row["consumer_empty_rate"], (10 / 110 + 90 / 140) / 2)
        self.assertEqual(row["metrics"]["diagnostic_consumer1_avg_batch"], 20)

    def test_summary_contains_all_metrics_and_five_measured_rounds(self):
        result = self.summary()
        self.assertEqual(result["measured_runs"], 30)
        self.assertEqual(result["warmup_runs"], 6)
        self.assertEqual(result["schema_version"], 2)
        for payload in result["payload_summary"]:
            self.assertEqual(payload["rounds"], 5)
            self.assertEqual(payload["counter_mode"], "counters")
            self.assertEqual(payload["claim_timing_mode"], "timed")
            self.assertAlmostEqual(payload["backpressure_rate"], 100 / 1100)
            self.assertAlmostEqual(payload["consumer_empty_rate"], 0.4)
            self.assertAlmostEqual(payload["avg_batch"], 2000 / 150)
            self.assertEqual(payload["claim_avg_ns"], 200)
        self.assertFalse(result["instrumentation_exceeds_5_percent"])
        self.assertTrue(all(group["publish_mmsg_s"]["median"] == 10 for group in result["groups"]))

    def test_thresholds_are_strictly_greater(self):
        groups = self.summary()["groups"]
        for group in groups:
            if group["mode"] == "off":
                continue
            group["backpressure_rate"]["median"] = 0.1
            group["consumer_empty_rate"]["median"] = 0.5
            if group["mode"] == "timed":
                group["claim_avg_ns"]["median"] = 200
        boundary = assess_hypotheses(groups)
        self.assertTrue(all(not h["threshold_exceeded"] for h in boundary["hypotheses"].values()))
        for group in groups:
            if group["mode"] == "off":
                continue
            group["backpressure_rate"]["median"] += 0.001
            group["consumer_empty_rate"]["median"] += 0.001
            if group["mode"] == "timed":
                group["claim_avg_ns"]["median"] += 0.001
        exceeded = assess_hypotheses(groups)
        self.assertTrue(all(h["threshold_exceeded"] for h in exceeded["hypotheses"].values()))
        self.assertTrue(any("Phase 2A" in c for c in exceeded["phase2_candidates"]))
        self.assertTrue(any("Phase 2B" in c for c in exceeded["phase2_candidates"]))
        self.assertIn("not Guard-exclusive", exceeded["hypotheses"]["H3"]["meaning"])

    def test_missing_timing_is_not_reported_as_zero(self):
        result = self.summary(modes=("counters",))
        self.assertTrue(all(p["claim_avg_ns"] is None for p in result["payload_summary"]))
        self.assertIsNone(result["hypotheses"]["H3"]["threshold_exceeded"])
        self.assertIsNone(result["instrumentation_exceeds_5_percent"])

    def test_timed_only_can_supply_counter_metrics(self):
        result = self.summary(modes=("timed",))
        self.assertTrue(all(p["counter_mode"] == "timed" for p in result["payload_summary"]))
        self.assertEqual(result["measured_runs"], 10)

    def test_off_only_does_not_invent_diagnostic_observations(self):
        result = self.summary(modes=("off",))
        self.assertTrue(all(h["value"] is None for h in result["hypotheses"].values()))
        self.assertTrue(all(p["backpressure_rate"] is None for p in result["payload_summary"]))

    def test_instrumentation_perturbation_is_reported(self):
        groups = self.summary()["groups"]
        counters = next(g for g in groups if g["payload"] == 64 and g["mode"] == "counters")
        counters["paired_throughput_change_fraction"]["median"] = -0.2
        self.assertTrue(assess_hypotheses(groups)["instrumentation_exceeds_5_percent"])


if __name__ == "__main__":
    unittest.main()
