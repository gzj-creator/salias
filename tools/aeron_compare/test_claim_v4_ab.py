#!/usr/bin/env python3
"""Tests for the claim API v4 three-arm runner and analyzer."""

import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools/aeron_compare"))
import analyze_claim_v4_ab as analyzer  # noqa: E402


def load_runner():
    spec = importlib.util.spec_from_file_location(
        "p3_claim_api_v4_ab", ROOT / "doc/plans/runners/p3-claim-api-v4-ab.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


runner = load_runner()


class ClaimV4RunnerTest(unittest.TestCase):
    def test_three_arm_rotation_reverses_each_group(self):
        sequence = runner.block_sequence(6)
        self.assertEqual(sequence[0], [("B", 0), ("S", 0), ("A", 0)])
        self.assertEqual(sequence[1], [("A", 1), ("S", 1), ("B", 1)])
        self.assertEqual(sum(len(block) for block in sequence), 18)
        for group in range(6):
            arms = [arm for block in sequence for arm, value in block if value == group]
            self.assertEqual(sorted(arms), ["A", "B", "S"])

    def test_sequential_arm_is_the_only_fence_override(self):
        self.assertEqual(runner.ARM_ENVIRONMENT["S"],
                         ["SALIAS_DISABLE_ASYMMETRIC_FENCE=1"])
        self.assertEqual(runner.ARM_ENVIRONMENT["B"], [])
        self.assertEqual(runner.ARM_ENVIRONMENT["A"], [])

    def test_manifest_identity_is_enforced(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            baseline = root / "baseline"
            candidate = root / "candidate"
            baseline.write_bytes(b"v3")
            candidate.write_bytes(b"v4")
            manifest = {
                "protocol": runner.PROTOCOL,
                "client_template_identity": runner.CLIENT_PATH,
                "client_template_sha256": "c" * 64,
                "baseline_client_source_identity": f"git:v3.0.0:{runner.CLIENT_PATH}",
                "baseline_client_source_sha256": "a" * 64,
                "variants": {
                    "v3": {"client": str(baseline),
                           "client_sha256": hashlib.sha256(b"v3").hexdigest(),
                           "client_source_sha256": "a" * 64,
                           "submission_sites": {"claim_commit": 2, "publisher_commit": 0}},
                    "v4": {"client": str(candidate),
                           "client_sha256": hashlib.sha256(b"v4").hexdigest(),
                           "client_source_sha256": "b" * 64,
                           "submission_sites": {"claim_commit": 0, "publisher_commit": 2}},
                },
            }
            path = root / "manifest.json"
            path.write_text(json.dumps(manifest))
            loaded = runner.load_manifest(path)
            self.assertEqual(loaded["clients"]["B"], str(baseline.resolve()))
            self.assertEqual(loaded["clients"]["S"], str(candidate.resolve()))
            self.assertEqual(loaded["client_template_sha256"], "c" * 64)
            self.assertEqual(loaded["baseline_client_source_sha256"], "a" * 64)
            candidate.write_bytes(b"tampered")
            with self.assertRaisesRegex(ValueError, "SHA-256"):
                runner.load_manifest(path)

    def test_legacy_manifest_pinned_source_field_remains_readable(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            baseline = root / "baseline"
            candidate = root / "candidate"
            baseline.write_bytes(b"v3")
            candidate.write_bytes(b"v4")
            manifest = {
                "protocol": runner.PROTOCOL,
                "client_source_identity": f"git:v3.0.0:{runner.CLIENT_PATH}",
                "client_source_sha256": "a" * 64,
                "variants": {
                    "v3": {"client": str(baseline),
                           "client_sha256": hashlib.sha256(b"v3").hexdigest(),
                           "client_source_sha256": "a" * 64,
                           "submission_sites": {"claim_commit": 2, "publisher_commit": 0}},
                    "v4": {"client": str(candidate),
                           "client_sha256": hashlib.sha256(b"v4").hexdigest(),
                           "client_source_sha256": "b" * 64,
                           "submission_sites": {"claim_commit": 0, "publisher_commit": 2}},
                },
            }
            path = root / "manifest.json"
            path.write_text(json.dumps(manifest))
            loaded = runner.load_manifest(path)
            self.assertEqual(loaded["baseline_client_source_sha256"], "a" * 64)
            self.assertEqual(loaded["client_template_sha256"], "a" * 64)

    def test_latency_command_keeps_fence_override_on_s_only(self):
        class Args:
            latency_seconds = 2
            latency_rounds = 10

        command = runner.latency_command(
            {"B": "/tmp/b", "S": "/tmp/s", "A": "/tmp/a"}, Path("/tmp/out"),
            64, "attempt", Args(),
        )
        self.assertIn("--salias-arm", command)
        self.assertIn("B=/tmp/b", command)
        self.assertIn("S=/tmp/s", command)
        self.assertIn("A=/tmp/a", command)
        self.assertEqual(command.count("--salias-arm-env"), 1)
        self.assertIn("S=SALIAS_DISABLE_ASYMMETRIC_FENCE=1", command)
        self.assertIn("--setenv=SALIAS_RESOURCE_CAPPED=1", command)


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


class ClaimV4AnalyzerTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def build_experiment(self, ratios=(1.0, 1.3, 1.5)):
        experiment = self.root / "experiment"
        clients = {
            "B": {"path": "/tmp/v3", "sha256": "a" * 64, "environment": []},
            "S": {"path": "/tmp/v4", "sha256": "b" * 64,
                  "environment": ["SALIAS_DISABLE_ASYMMETRIC_FENCE=1"]},
            "A": {"path": "/tmp/v4", "sha256": "b" * 64, "environment": []},
        }
        write_json(experiment / "experiment.json", {
            "protocol": "p3-claim-api-v4-formal-ab-v1",
            "mode": "formal",
            "finished": "2026-10-22T20:00:00+0800",
            "clients": clients,
            "throughput": {
                "payloads": [64, 256], "groups": 6, "rounds_per_group": 20,
                "messages_per_round": 50_000_000, "capacity_bytes": 1 << 20,
                "sequence": [
                    [f"{arm}{group}" for arm in (("B", "S", "A") if group % 2 == 0
                                                 else ("A", "S", "B"))]
                    for group in range(6)
                ],
            },
            "latency": {
                "payloads": [64, 256], "origins": ["attempt", "planned"],
                "rounds": 10, "rate_per_producer": 1_000_000,
            },
            "blocks": [
                {"kind": "throughput", "payload": payload, "arm": arm, "group": group,
                 "returncode": 0}
                for payload in (64, 256) for group in range(6) for arm in ("B", "S", "A")
            ] + [
                {"kind": "latency", "payload": payload, "origin": origin, "returncode": 0}
                for payload in (64, 256) for origin in ("attempt", "planned")
            ],
        })
        for payload in (64, 256):
            for arm, base in (("B", 4.0), ("S", 4.0 * ratios[1]), ("A", 4.0 * ratios[2])):
                directory = experiment / f"throughput-{payload}b" / analyzer.ARM_DIRECTORIES[arm]
                write_json(directory / "environment.json", {
                    "payload_bytes": payload,
                    "environment_variables": (["SALIAS_DISABLE_ASYMMETRIC_FENCE=1"]
                                              if arm == "S" else []),
                    "capacity_bytes": 1 << 20, "groups": 6, "rounds_per_group": 20,
                    "messages_per_round": 50_000_000, "rate_per_producer": 0,
                    "latency_sample_rate": 0,
                    "client_sha256": clients[arm]["sha256"],
                })
                for group in range(6):
                    group_dir = directory / f"group-{group:02d}"
                    group_dir.mkdir(parents=True)
                    records = []
                    for round_number in range(20):
                        rate = base + group * 0.01
                        stdout = group_dir / f"round-{round_number:02d}.stdout"
                        stdout.write_text(
                            "RESULT library=salias-ipc scenario=fifo producers=1 consumers=2 "
                            f"payload={payload} capacity={1 << 20} batch_size=1 "
                            f"poll_limit=64 published={analyzer.MESSAGES} "
                            f"delivered={2 * analyzer.MESSAGES} latency_sample_rate=0 "
                            f"rate_per_producer=0 publish_msg_per_sec={rate * 1e6}\n"
                        )
                        records.append({"group": group, "round": round_number,
                                        "publish_mmsg_s": rate,
                                        "stdout": stdout.name})
                    (group_dir / "results.jsonl").write_text(
                        "".join(json.dumps(row) + "\n" for row in records)
                    )
                files = [path for path in directory.rglob("*")
                         if path.is_file() and path.name != "SHA256SUMS"]
                (directory / "SHA256SUMS").write_text("".join(
                    f"{hashlib.sha256(path.read_bytes()).hexdigest()}  "
                    f"{path.relative_to(directory)}\n" for path in sorted(files)
                ))
            for origin in analyzer.LATENCY_ORIGINS:
                case_directory = experiment / "latency" / f"{payload}b-{origin}"
                rows = []
                for arm, scale in (("B", 1.0), ("S", 0.9), ("A", 0.8)):
                    for round_number in range(1, 11):
                        stdout = case_directory / f"{arm}-round-{round_number:02d}.stdout"
                        stdout.parent.mkdir(parents=True, exist_ok=True)
                        result = {
                            "latency_p50_ns": str(int(100 * scale)),
                            "latency_p99_ns": str(int(1000 * scale)),
                            "latency_p999_ns": str(int(10000 * scale)),
                            "published": "2000000",
                            "delivered": "2000000",
                            "latency_samples": "125000",
                            "publish_msg_per_sec": "1000000",
                        }
                        stdout.write_text(
                            "RESULT " + " ".join(f"{key}={value}"
                                                for key, value in result.items()) + "\n"
                        )
                        rows.append({
                            "arm": arm,
                            "origin": origin, "phase": "measured", "round": round_number,
                            "engine": "fifo", "payload": payload,
                            "producers": 1, "rate": 1_000_000, "duration_s": 2,
                            "client_sha256": clients[arm]["sha256"],
                            "arm_environment": ({"SALIAS_DISABLE_ASYMMETRIC_FENCE": "1"}
                                                if arm == "S" else {}),
                            "result": result,
                            "stdout": stdout.name,
                            "stdout_sha256": hashlib.sha256(stdout.read_bytes()).hexdigest(),
                            "resources": {"memory.events": "low 0\nhigh 0\nmax 0\n"
                                                          "oom 0\noom_kill 0\n"},
                        })
                case_directory.mkdir(parents=True, exist_ok=True)
                (case_directory / "results.jsonl").write_text(
                    "".join(json.dumps(row) + "\n" for row in rows)
                )
        return experiment

    def test_analyzer_separates_claim_and_fence_effects(self):
        experiment = self.build_experiment()
        report = analyzer.analyze(experiment, self.root / "output")
        self.assertEqual(len(report["payloads"]), 2)
        throughput = report["payloads"][0]["throughput"]
        # The fixture adds a group offset, so compare against its raw medians.
        baseline = report["payloads"][0]["throughput"]["claim_api_S_vs_B"][
            "baseline_median_mmsg_s"]
        self.assertAlmostEqual(
            throughput["claim_api_S_vs_B"]["candidate_median_mmsg_s"] / baseline,
            throughput["claim_api_S_vs_B"]["ratio"],
        )
        self.assertGreater(throughput["claim_api_S_vs_B"]["ratio"], 1.1)
        self.assertGreater(throughput["fence_A_vs_S"]["ratio"], 1.1)
        self.assertGreater(throughput["combined_A_vs_B"]["ratio"], 1.4)
        self.assertEqual(throughput["claim_api_S_vs_B"]
                         ["group_directions_candidate_above_baseline"], 6)
        self.assertEqual(report["payloads"][0]["claim_api_throughput_decision"], "GO")
        self.assertEqual(report["payloads"][1]["claim_api_throughput_decision"], "DESCRIPTIVE")
        latency = report["payloads"][0]["latency"]
        self.assertAlmostEqual(latency["claim_api_S_vs_B"]["attempt"]["p50"], 0.9)
        detail = report["payloads"][0]["latency_detail"]["claim_api_S_vs_B"]["attempt"]
        self.assertEqual(detail["p50"]["candidate_range_ns"], [90, 90])
        self.assertTrue((self.root / "output" / "claim-api-v4-64b.md").is_file())

    def test_swapped_fence_environment_is_rejected(self):
        experiment = self.build_experiment()
        path = (experiment / "throughput-64b"
                / analyzer.ARM_DIRECTORIES["S"] / "environment.json")
        environment = json.loads(path.read_text())
        environment["environment_variables"] = []
        path.write_text(json.dumps(environment))
        with self.assertRaisesRegex(analyzer.AnalysisError, "environment"):
            analyzer.analyze(experiment)

    def test_incomplete_group_is_rejected(self):
        experiment = self.build_experiment()
        path = (experiment / "throughput-64b"
                / analyzer.ARM_DIRECTORIES["A"] / "group-05" / "results.jsonl")
        path.write_text("".join(path.read_text().splitlines(True)[:-1]))
        directory = path.parent.parent
        (directory / "SHA256SUMS").write_text("".join(
            f"{hashlib.sha256(item.read_bytes()).hexdigest()}  "
            f"{item.relative_to(directory)}\n" for item in sorted(directory.rglob("*"))
            if item.is_file() and item.name != "SHA256SUMS"
        ))
        with self.assertRaisesRegex(analyzer.AnalysisError, "incomplete"):
            analyzer.analyze(experiment)

    def test_tampered_throughput_stdout_is_rejected(self):
        experiment = self.build_experiment()
        directory = (experiment / "throughput-64b" / analyzer.ARM_DIRECTORIES["B"])
        path = directory / "group-00" / "round-00.stdout"
        path.write_text(path.read_text().replace("library=salias-ipc", "library=other"))
        with self.assertRaisesRegex(analyzer.AnalysisError, "SHA256SUMS"):
            analyzer.analyze(experiment)

    def test_tampered_latency_stdout_is_rejected(self):
        experiment = self.build_experiment()
        case = experiment / "latency" / "64b-attempt"
        path = case / "B-round-01.stdout"
        path.write_text(path.read_text().replace("latency_p50_ns=100", "latency_p50_ns=99"))
        with self.assertRaisesRegex(analyzer.AnalysisError, "stdout identity"):
            analyzer.analyze(experiment)

    def test_failed_block_is_rejected(self):
        experiment = self.build_experiment()
        path = experiment / "experiment.json"
        data = json.loads(path.read_text())
        data["blocks"][0]["returncode"] = 1
        path.write_text(json.dumps(data))
        with self.assertRaisesRegex(analyzer.AnalysisError, "block failed"):
            analyzer.analyze(experiment)


if __name__ == "__main__":
    unittest.main()
