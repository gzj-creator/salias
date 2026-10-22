#!/usr/bin/env python3
"""P3-1 tail gates must not inherit the historical 20% improvement target."""

import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

from compare_p31_tail import ARMS, CONFIGURATIONS, ORDER, PROTOCOL, assess_pair, audit, digest
from summary_fixture import histogram, make_run
import summarize


class P31TailTests(unittest.TestCase):
    @staticmethod
    def version(p99=1000, p999=2000, valid=True, budget=False):
        return {"worst_planned_p99_ns": p99, "worst_planned_p999_ns": p999,
                "valid_and_complete": valid, "planned_p999_budget_pass": budget}

    def test_both_exact_boundaries_pass(self):
        result = assess_pair(self.version(), self.version(1050, 2100))
        self.assertTrue(result["passed"])
        self.assertFalse(result["candidate_50us_pass"])

    def test_each_quantile_can_fail_independently(self):
        for p99, p999 in ((1051, 2000), (1000, 2101)):
            with self.subTest(p99=p99, p999=p999):
                self.assertFalse(assess_pair(self.version(), self.version(p99, p999))["passed"])

    def test_equal_latency_is_noninferior_not_required_to_improve(self):
        self.assertTrue(assess_pair(self.version(), self.version())["passed"])

    def test_bad_completion_or_empty_baseline_cannot_pass(self):
        for before, after in ((self.version(valid=False), self.version()),
                              (self.version(), self.version(valid=False)),
                              (self.version(p99=0), self.version())):
            self.assertFalse(assess_pair(before, after)["passed"])

    def test_protocol_is_fixed_before_collection(self):
        self.assertEqual(PROTOCOL["groups"], 3)
        self.assertEqual(PROTOCOL["seconds_per_run"], 30)
        self.assertEqual(ORDER, ("S", "A", "A", "S"))
        self.assertEqual(ARMS, {"S": {"SALIAS_DISABLE_ASYMMETRIC_FENCE": "1"}, "A": {}})
        self.assertEqual(len(PROTOCOL["configurations"]), 8)

    def test_runner_uses_unchanged_hft_traffic(self):
        path = Path(__file__).resolve().parents[2] / "doc/plans/runners/p3-p31-tail.py"
        spec = importlib.util.spec_from_file_location("p31_tail_runner", path)
        runner = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(runner)
        command = runner.command(Path("/tmp/acceptance"), "2p2s-256b-burst", 30)
        self.assertEqual(command[:5], ["timeout", "--signal=TERM", "--kill-after=10s",
                                      "70s", "/tmp/acceptance"])
        pairs = dict(zip(command[5::2], command[6::2]))
        self.assertEqual(pairs, {"--subscribers": "2", "--payload": "256", "--seconds": "30",
                                 "--pattern": "burst", "--rate": "1000000",
                                 "--peak-rate": "4000000", "--burst-ms": "100", "--cpu-base": "0"})


class P31TailAuditTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        client = self.root / "client"
        client.write_bytes(b"synthetic frozen HFT binary")
        self.manifest = {"protocol": copy.deepcopy(PROTOCOL), "client": str(client),
                         "client_sha256": digest(client), "inputs": {}, "runs": []}
        for group in range(1, 4):
            for name in CONFIGURATIONS:
                sub, payload, pattern = name.split("-")
                count = int(sub[2])
                planned = 30 * (1_300_000 if pattern == "burst" else 1_000_000)
                rows = make_run()
                start, result = rows
                start.update(duration_s=30, subscribers=count, payload_bytes=int(payload[:-1]),
                             pattern=pattern, burst_ms_per_second=100 if pattern == "burst" else 0,
                             planned_published=planned, cpus=list(range(count + 2)))
                result.update(measurement_seconds=30, wall_seconds_including_drain_and_reap=30.01,
                              planned_published=planned, published=planned, delivered=planned * count,
                              subscribers=result["subscribers"][:count])
                for publisher in result["publishers"]:
                    publisher["sent"] = planned // 2
                for subscriber in result["subscribers"]:
                    subscriber["received"] = planned
                    for sequence in subscriber["sequence"]:
                        sequence.update(received=planned // 2, unique=planned // 2)
                for aggregate, key in (("aggregate_from_first_attempt", "from_first_attempt"),
                                       ("aggregate_from_planned_arrival", "from_planned_arrival")):
                    result[aggregate] = summarize.merge([s[key] for s in result["subscribers"]])
                for round_number, arm in enumerate(ORDER):
                    relative = f"group-{group}/round-{round_number}/{arm}/{name}"
                    raw = self.root / f"{relative}.jsonl"
                    stderr = self.root / f"{relative}.stderr.txt"
                    raw.parent.mkdir(parents=True, exist_ok=True)
                    raw.write_text("".join(json.dumps(row) + "\n" for row in rows))
                    stderr.write_text("")
                    cmd = ["timeout", "--signal=TERM", "--kill-after=10s", "70s", str(client),
                           "--subscribers", str(count), "--payload", payload[:-1], "--seconds", "30",
                           "--pattern", pattern, "--rate", "1000000", "--peak-rate", "4000000",
                           "--burst-ms", "100", "--cpu-base", "0"]
                    self.manifest["runs"].append({
                        "group": group, "configuration": name, "round": round_number,
                        "arm": arm, "command": cmd, "environment": ARMS[arm], "returncode": 0,
                        "client_sha256": digest(client), "raw": str(raw.relative_to(self.root)),
                        "raw_sha256": digest(raw), "stderr": str(stderr.relative_to(self.root)),
                        "stderr_sha256": digest(stderr)})
        self.save()

    def save(self):
        (self.root / "experiment.json").write_text(json.dumps(self.manifest))

    def test_full_audit_recomputes_all_groups(self):
        result = audit(self.root)
        self.assertTrue(result["passed"])
        self.assertTrue(result["all_valid_and_complete"])
        self.assertEqual(result["formal_runs"], 96)
        self.assertEqual(result["measurement_seconds"], 2880)

    def test_missing_run_and_wrong_environment_are_rejected(self):
        self.manifest["runs"][0]["environment"] = {}
        self.save()
        with self.assertRaisesRegex(ValueError, "environment"):
            audit(self.root)
        self.manifest["runs"].pop()
        self.save()
        with self.assertRaisesRegex(ValueError, "formal HFT runs"):
            audit(self.root)

    def test_tampered_raw_and_failed_exit_are_rejected(self):
        record = self.manifest["runs"][0]
        raw = self.root / record["raw"]
        raw.write_text("tampered\n")
        with self.assertRaisesRegex(ValueError, "hash/path"):
            audit(self.root)
        record["raw_sha256"] = digest(raw)
        record["returncode"] = 1
        self.save()
        with self.assertRaisesRegex(ValueError, "exit=1"):
            audit(self.root)

    def test_changed_client_and_frozen_input_are_rejected(self):
        client = Path(self.manifest["client"])
        original = client.read_bytes()
        client.write_bytes(b"changed synthetic client")
        with self.assertRaisesRegex(ValueError, "client hash"):
            audit(self.root)
        client.write_bytes(original)
        frozen = self.root / "frozen.cpp"
        frozen.write_text("original synthetic input\n")
        self.manifest["inputs"][str(frozen)] = digest(frozen)
        self.save()
        frozen.write_text("changed synthetic input\n")
        with self.assertRaisesRegex(ValueError, "frozen input changed"):
            audit(self.root)

    def test_protocol_and_command_changes_are_rejected(self):
        self.manifest["protocol"]["maximum_candidate_percent_of_seq"] = 106
        self.save()
        with self.assertRaisesRegex(ValueError, "protocol differs"):
            audit(self.root)
        self.manifest["protocol"] = copy.deepcopy(PROTOCOL)
        command = self.manifest["runs"][0]["command"]
        command[command.index("--seconds") + 1] = "31"
        self.save()
        with self.assertRaisesRegex(ValueError, "command differs"):
            audit(self.root)

    def test_extra_raw_file_and_changed_order_are_rejected(self):
        extra = self.root / "group-1/round-0/S/extra.jsonl"
        extra.write_text("unexpected synthetic run\n")
        with self.assertRaisesRegex(ValueError, "unexpected HFT raw files"):
            audit(self.root)
        extra.unlink()
        runs = self.manifest["runs"]
        runs[0], runs[1] = runs[1], runs[0]
        self.save()
        with self.assertRaisesRegex(ValueError, "order/identity"):
            audit(self.root)

    def test_one_incomplete_or_corrupt_run_fails_the_gate(self):
        record = self.manifest["runs"][1]
        raw = self.root / record["raw"]
        original = raw.read_text()
        for failure in ("incomplete", "corrupt"):
            with self.subTest(failure=failure):
                rows = [json.loads(line) for line in original.splitlines()]
                result = rows[1]
                if failure == "incomplete":
                    result["offered_load_completed"] = False
                else:
                    result["integrity_pass"] = False
                    result["subscribers"][0]["integrity_pass"] = False
                    result["subscribers"][0]["sequence"][0]["corrupt"] = 1
                raw.write_text("".join(json.dumps(row) + "\n" for row in rows))
                record["raw_sha256"] = digest(raw)
                self.save()
                assessment = audit(self.root)
                self.assertFalse(assessment["all_valid_and_complete"])
                self.assertFalse(assessment["passed"])
                self.assertFalse(assessment["groups"][0]["passed"])
                self.assertTrue(all(g["passed"] for g in assessment["groups"][1:]))

    def test_one_bad_tail_group_cannot_be_rescued_by_other_groups_or_budget(self):
        record = self.manifest["runs"][1]
        raw = self.root / record["raw"]
        rows = [json.loads(line) for line in raw.read_text().splitlines()]
        result = rows[1]
        subscriber = result["subscribers"][0]
        subscriber["from_planned_arrival"] = histogram(
            3000, subscriber["from_planned_arrival"]["samples"])
        result["aggregate_from_planned_arrival"] = summarize.merge(
            [s["from_planned_arrival"] for s in result["subscribers"]])
        raw.write_text("".join(json.dumps(row) + "\n" for row in rows))
        record["raw_sha256"] = digest(raw)
        self.save()
        assessment = audit(self.root)
        self.assertTrue(assessment["all_valid_and_complete"])
        self.assertTrue(assessment["candidate_50us_pass"])
        self.assertFalse(assessment["passed"])
        self.assertFalse(assessment["groups"][0]["passed"])
        self.assertTrue(all(g["passed"] for g in assessment["groups"][1:]))


if __name__ == "__main__":
    unittest.main()
