#!/usr/bin/env python3
"""Synthetic regression tests for the P3-1 historical evidence analyzer."""

from __future__ import annotations

import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import analyze_p31_followup as analyzer


SHORTFALLS = {
    (1, "2p2s-256b-steady", 0, "S"): 3540,
    (2, "2p1s-256b-burst", 0, "S"): 516,
    (2, "2p2s-64b-steady", 3, "S"): 9,
    (3, "2p2s-256b-steady", 0, "S"): 597,
}
INCOMPLETE = set(SHORTFALLS)


def histogram(value=1000, samples=1000):
    return {
        "samples": samples,
        "p50_ns": value,
        "p99_ns": value,
        "p999_ns": value,
        "max_sampled_ns": value,
        "above_50us": 0,
        "p99_pass_50us": True,
        "p999_pass_50us": True,
        "buckets": [],
    }


def resources(index=0):
    return {
        "user_us": 1_000 + index,
        "system_us": 10 + index,
        "voluntary_context_switches": index,
        "involuntary_context_switches": 20 + index,
        "minor_faults": 2,
        "major_faults": 0,
    }


class SyntheticEvidence:
    def __init__(self, root: Path):
        self.root = root
        client = root / "client"
        client.write_bytes(b"synthetic HFT client")
        self.manifest = {
            "protocol": copy.deepcopy(analyzer.PROTOCOL),
            "client": str(client.resolve()),
            "client_sha256": analyzer.digest(client),
            "inputs": {},
            "runs": [],
            "warmup": [],
        }
        for index in range(analyzer.EXPECTED_INPUTS):
            path = root / f"input-{index:02d}"
            path.write_bytes(f"frozen input {index}\n".encode())
            self.manifest["inputs"][str(path.resolve())] = analyzer.digest(path)
        for group in range(1, 4):
            for configuration in analyzer.CONFIGURATIONS:
                for round_number, arm in enumerate(analyzer.ORDER):
                    identity = (group, configuration, round_number, arm)
                    self.manifest["runs"].append(
                        self._formal_record(identity, incomplete=identity in INCOMPLETE)
                    )
        for configuration in analyzer.CONFIGURATIONS:
            for round_number, arm in enumerate(("S", "A")):
                raw = f"warmup/{arm}/{configuration}.jsonl"
                stderr = f"warmup/{arm}/{configuration}.stderr.txt"
                self._write(raw, "{}\n")
                self._write(stderr, "")
                self.manifest["warmup"].append({
                    "group": 0,
                    "configuration": configuration,
                    "round": round_number,
                    "arm": arm,
                    "command": analyzer._expected_command(self.manifest["client"], configuration, 2),
                    "environment": copy.deepcopy(analyzer.ARMS[arm]),
                    "client_sha256": self.manifest["client_sha256"],
                    "returncode": 0,
                    "raw": raw,
                    "raw_sha256": analyzer.digest(root / raw),
                    "stderr": stderr,
                    "stderr_sha256": analyzer.digest(root / stderr),
                })
        self.save_manifest()

    def _write(self, relative: str, content: str) -> None:
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)

    def _formal_record(self, identity, incomplete: bool):
        group, configuration, round_number, arm = identity
        subscribers, payload, pattern = analyzer._configuration_identity(configuration)
        planned = 30 * (1_300_000 if pattern == "burst" else 1_000_000)
        unsent = SHORTFALLS.get(identity, 0) if incomplete else 0
        published = planned - unsent
        first_sent = published // 2
        second_sent = published - first_sent
        first_unsent = planned // 2 - first_sent
        second_unsent = planned // 2 - second_sent
        start = {
            "type": "start",
            "start_monotonic_ns": 1_000_000_000,
            "duration_s": 30,
            "subscribers": subscribers,
            "payload_bytes": payload,
            "pattern": pattern,
            "steady_total_rate": 1_000_000,
            "peak_total_rate": 4_000_000,
            "burst_ms_per_second": 100 if pattern == "burst" else 0,
            "planned_published": planned,
            "capacity_per_publisher_bytes": 4_194_304,
            "publication_window_bytes": 131_072,
            "batch": 1,
            "poll_limit": 64,
            "sample_hash_mask": 63,
            "cpus": list(range(subscribers + 2)),
            "diagnostics": False,
            "stall_subscriber_ms": 0,
        }
        publishers = []
        for publisher, sent, publisher_unsent in (
            (0, first_sent, first_unsent), (1, second_sent, second_unsent)
        ):
            publishers.append({
                "publisher": publisher,
                "sent": sent,
                "unsent": publisher_unsent,
                "backpressure_retries": 100 if incomplete else 0,
                "backpressured_messages": 2 if incomplete else 0,
                "backpressure_episode_ns": 10_000 if incomplete else 0,
                "max_backpressure_episode_ns": 5_000 if incomplete else 0,
                "unsent_backlog_highwater_observed": 200 if incomplete else 0,
                "first_attempt_to_commit": histogram(),
                "planned_to_first_attempt": histogram(2_000),
                "resources": resources(publisher),
            })
        sequence = []
        for publisher in range(2):
            sequence.append({
                "publisher": publisher,
                "received": publishers[publisher]["sent"],
                "unique": publishers[publisher]["sent"],
                "missing_published": 0,
                "duplicates": 0,
                "out_of_order": 0,
                "gap_events": 0,
                "gap_positions_observed": 0,
                "corrupt": 0,
                "invalid": 0,
                "channel_producer_id": 1 - publisher,
            })
        subscriber_rows = []
        for subscriber in range(subscribers):
            subscriber_rows.append({
                "subscriber": subscriber,
                "received": published,
                "unreceived_at_measurement_cutoff_observed": unsent,
                "unreceived_after_drain": unsent,
                "invalid_publisher": 0,
                "clock_reversals": 0,
                "sequence": copy.deepcopy(sequence),
                "integrity_pass": True,
                "from_first_attempt": histogram(),
                "from_planned_arrival": histogram(),
                "resources": resources(subscriber + 2),
            })
        result = {
            "type": "result",
            "measurement_seconds": 30,
            "wall_seconds_including_drain_and_reap": 30.01,
            "planned_published": planned,
            "published": published,
            "unsent_at_cutoff": unsent,
            "actual_published_msg_s": published / 30,
            "publishers": publishers,
            "subscribers": subscriber_rows,
            "aggregate_from_first_attempt": histogram(),
            "aggregate_from_planned_arrival": histogram(),
            "delivered": published * subscribers,
            "integrity_pass": True,
            "offered_load_completed": unsent == 0,
            "worker_failure": False,
        }
        progress = {
            "type": "progress",
            "elapsed_s": 30.0,
            "due": planned,
            "sent": [first_sent, second_sent],
            "received": [published] * subscribers,
        }
        relative = f"group-{group}/round-{round_number}/{arm}/{configuration}.jsonl"
        stderr = relative.replace(".jsonl", ".stderr.txt")
        rows = [start, progress, result]
        self._write(relative, "".join(json.dumps(row) + "\n" for row in rows))
        self._write(stderr, "")
        return {
            "group": group,
            "configuration": configuration,
            "round": round_number,
            "arm": arm,
            "command": analyzer._expected_command(self.manifest["client"], configuration, 30),
            "environment": copy.deepcopy(analyzer.ARMS[arm]),
            "client_sha256": self.manifest["client_sha256"],
            "returncode": 0,
            "raw": relative,
            "raw_sha256": analyzer.digest(self.root / relative),
            "stderr": stderr,
            "stderr_sha256": analyzer.digest(self.root / stderr),
        }

    def save_manifest(self) -> None:
        (self.root / "experiment.json").write_text(json.dumps(self.manifest))

    def run(self, identity):
        group, configuration, round_number, arm = identity
        return next(
            run for run in self.manifest["runs"]
            if (run["group"], run["configuration"], run["round"], run["arm"]) == identity
        )

    def rows(self, identity):
        path = self.root / self.run(identity)["raw"]
        return [json.loads(line) for line in path.read_text().splitlines()]

    def rewrite(self, identity, rows) -> None:
        run = self.run(identity)
        path = self.root / run["raw"]
        path.write_text("".join(json.dumps(row) + "\n" for row in rows))
        run["raw_sha256"] = analyzer.digest(path)
        self.save_manifest()


class FollowupAnalyzerTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.evidence = SyntheticEvidence(self.root)

    def test_verifies_all_manifest_roles_and_preserves_four_incomplete_runs(self):
        document = analyzer.analyze(self.root)
        self.assertEqual(document["evidence"]["frozen_inputs"], 50)
        self.assertEqual(document["evidence"]["formal_runs"], 96)
        self.assertEqual(document["evidence"]["formal_raw_sha256"], 96)
        self.assertEqual(document["evidence"]["formal_stderr_sha256"], 96)
        self.assertEqual(document["run_inventory"]["complete"], 92)
        self.assertEqual({run["arm"] for run in document["run_inventory"]["incomplete"]}, {"S"})
        self.assertEqual(len(document["runs"]), 96)
        self.assertEqual(document["p3_1_g4"]["conclusion"], "FAIL")
        self.assertTrue(all(run["role_structure"]["status"] == "verified" for run in document["runs"]))

    def test_client_hash_damage_is_rejected(self):
        Path(self.evidence.manifest["client"]).write_bytes(b"damaged client")
        with self.assertRaisesRegex(analyzer.AuditError, "client hash changed"):
            analyzer.analyze(self.root)

    def test_raw_hash_damage_is_rejected(self):
        run = self.evidence.manifest["runs"][0]
        (self.root / run["raw"]).write_text("{}\n")
        with self.assertRaisesRegex(analyzer.AuditError, "raw hash mismatch"):
            analyzer.analyze(self.root)

    def test_frozen_input_damage_is_rejected(self):
        path = Path(next(iter(self.evidence.manifest["inputs"])))
        path.write_text("changed input\n")
        with self.assertRaisesRegex(analyzer.AuditError, "frozen input changed"):
            analyzer.analyze(self.root)

    def test_truncated_jsonl_is_rejected(self):
        identity = self.evidence.manifest["runs"][0]
        run = self.evidence.run((identity["group"], identity["configuration"],
                                 identity["round"], identity["arm"]))
        path = self.root / run["raw"]
        path.write_text('{"type":"start"}\n{"type":"progress"')
        run["raw_sha256"] = analyzer.digest(path)
        self.evidence.save_manifest()
        with self.assertRaisesRegex(analyzer.AuditError, "invalid JSONL"):
            analyzer.analyze(self.root)

    def test_missing_result_is_rejected_but_offered_load_shortfall_is_retained(self):
        complete_identity = (1, "2p1s-64b-steady", 0, "S")
        rows = self.evidence.rows(complete_identity)
        self.evidence.rewrite(complete_identity, rows[:-1])
        with self.assertRaisesRegex(analyzer.AuditError, "trailing result"):
            analyzer.analyze(self.root)

        self.evidence = SyntheticEvidence(self.root)
        document = analyzer.analyze(self.root)
        incomplete = {
            (run["group"], run["configuration"], run["round"], run["arm"])
            for run in document["run_inventory"]["incomplete"]
        }
        self.assertEqual(incomplete, INCOMPLETE)

    def test_missing_publisher_role_is_rejected(self):
        identity = (1, "2p1s-64b-steady", 0, "S")
        rows = self.evidence.rows(identity)
        rows[-1]["publishers"].pop()
        self.evidence.rewrite(identity, rows)
        with self.assertRaisesRegex(analyzer.AuditError, "exactly two publishers"):
            analyzer.analyze(self.root)

    def test_subscriber_count_must_match_configuration(self):
        identity = (1, "2p1s-64b-steady", 0, "S")
        rows = self.evidence.rows(identity)
        rows[-1]["subscribers"].append(copy.deepcopy(rows[-1]["subscribers"][0]))
        rows[-1]["subscribers"][-1]["subscriber"] = 1
        self.evidence.rewrite(identity, rows)
        with self.assertRaisesRegex(analyzer.AuditError, "subscriber count differs"):
            analyzer.analyze(self.root)

    def test_publisher_sequence_ids_cannot_be_swapped(self):
        identity = (1, "2p1s-64b-steady", 0, "S")
        rows = self.evidence.rows(identity)
        rows[-1]["subscribers"][0]["sequence"].reverse()
        self.evidence.rewrite(identity, rows)
        with self.assertRaisesRegex(analyzer.AuditError, "sequence ids must be"):
            analyzer.analyze(self.root)

    def test_stale_progress_snapshot_cannot_replace_measurement_cutoff(self):
        identity = (2, "2p2s-64b-steady", 3, "S")
        rows = self.evidence.rows(identity)
        rows[1]["sent"] = [0, 0]
        rows[1]["due"] = 29_000_000
        self.evidence.rewrite(identity, rows)
        document = analyzer.analyze(self.root)
        run = next(
            run for run in document["runs"]
            if (run["group"], run["configuration"], run["round"], run["arm"]) == identity
        )
        self.assertEqual(run["progress_observation"]["last"]["due_minus_sent_total_lower_bound"],
                         29_000_000)
        self.assertFalse(run["progress_observation"]["is_measurement_cutoff"])
        self.assertEqual(run["measurement_cutoff"]["unsent"], 9)
        self.assertFalse(run["complete"])

    def test_candidate_ranking_never_claims_unmeasured_root_cause(self):
        document = analyzer.analyze(self.root)
        assessment = document["root_cause_assessment"]
        self.assertTrue(assessment["ranking"])
        self.assertTrue(all(item["proven"] is False for item in assessment["ranking"]))
        self.assertIn("S-arm-specific", assessment["ranking"][0]["candidate"])
        self.assertIn("host scheduler as root cause", assessment["explicitly_not_proven"])
        self.assertIn("CPU cache or cache-line contention as root cause",
                      assessment["explicitly_not_proven"])

    def test_protocol_change_is_rejected(self):
        self.evidence.manifest["protocol"]["maximum_candidate_percent_of_seq"] = 106
        self.evidence.save_manifest()
        with self.assertRaisesRegex(analyzer.AuditError, "protocol differs"):
            analyzer.analyze(self.root)


if __name__ == "__main__":
    unittest.main()
