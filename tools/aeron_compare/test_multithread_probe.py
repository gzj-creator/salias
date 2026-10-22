#!/usr/bin/env python3
"""Tests for the P3-2 probe generator, runner contract, and optional smoke."""

import importlib.util
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest


ROOT = Path(__file__).resolve().parents[2]


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


builder = load_module("p32_builder", ROOT / "tools/aeron_compare/build_multithread_probe.py")
runner = load_module("p32_runner", ROOT / "doc/plans/runners/p3-p32-multithread.py")


def fake_stdout(configuration, arm):
    expected_threads = 1 if arm == "ST" else 2
    libc = 1 if arm == "ST" else 0
    published = configuration["producers"] * configuration["messages"]
    delivered = published * configuration["consumers"]
    return f'''\
PROBE identity={builder.PROBE_IDENTITY} source_sha256={builder.V3_SOURCE_SHA256} barrier=A publisher_mode={arm.lower()}
THREAD_STATE role=publisher mode={arm.lower()} expected_threads={expected_threads} actual_threads={expected_threads} libc_single_threaded_available=1 libc_single_threaded={libc} before_publish=1
RESULT published={published} delivered={delivered} seconds=0.1 publish_msg_per_sec=1000000 delivery_msg_per_sec=1000000 latency_samples=0 latency_p50_ns=0 latency_p99_ns=0 latency_p999_ns=0 latency_max_ns=0
CGROUP_EVIDENCE_BEGIN version=1 cgroup_path=/test
[CGROUP_FILE cpu.stat]
usage_usec 1
[CGROUP_FILE cpu.pressure]
some avg10=0.00 avg60=0.00 avg300=0.00 total=0
[CGROUP_FILE memory.events]
low 0
high 0
max 0
oom 0
oom_kill 0
oom_group_kill 0
[CGROUP_FILE memory.high]
671088640
[CGROUP_FILE memory.max]
738197504
[CGROUP_FILE memory.peak]
4096
[CGROUP_FILE memory.swap.max]
0
[CGROUP_FILE memory.swap.current]
0
[CGROUP_FILE memory.pressure]
some avg10=0.00 avg60=0.00 avg300=0.00 total=0
[CGROUP_FILE cpuset.cpus.effective]
0-3
CGROUP_EVIDENCE_END
'''


class GeneratorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        source, cls.source_identity, cls.source_sha = builder.load_v3_source()
        cls.generated = builder.instrument(source)

    def test_source_is_pinned_v3(self):
        self.assertEqual(self.source_identity, "git:v3.0.0:tools/aeron_compare/salias_ipc_compare.cpp")
        self.assertEqual(
            self.source_sha,
            "6a23a77598c6e980f7b6625eb2471cc42a26917469dc77fc51ab9d5b767bab1e",
        )
        self.assertNotIn("pthread_atfork", self.generated)

    def test_identity_and_environment_switch_are_embedded(self):
        self.assertIn("SALIAS_P32_PUBLISHER_MODE", self.generated)
        self.assertIn("P3-2 requires product barrier mode A", self.generated)
        self.assertIn("--self-test", self.generated)
        self.assertNotIn("--publisher-mode", self.generated)

    def test_helper_runs_only_in_publisher_post_fork_path(self):
        producer = self.generated.index("run_producer_child")
        publisher = self.generated.index("auto publisher = channel.publisher();", producer)
        helper = self.generated.index("helper.start_if(", producer)
        ready = self.generated.index("ready_and_wait_for_start(state);", publisher)
        publish_loop = self.generated.index("if (options.batch_size == 1)", helper)
        self.assertGreater(helper, ready)
        self.assertLess(helper, publish_loop)
        consumer = self.generated.index("run_consumer_child")
        self.assertLess(consumer, producer)
        self.assertEqual(self.generated.count("helper.start_if("), 1)
        self.assertIn("stop_condition_.wait(lock, [this] { return stop_; });", self.generated)
        self.assertNotIn("while (!stop_)", self.generated)

    def test_thread_state_is_checked_before_publish(self):
        helper = self.generated.index("validate_and_print_publisher_thread_state(")
        publish_loop = self.generated.index("if (options.batch_size == 1)", helper)
        self.assertLess(helper, publish_loop)
        self.assertIn("actual_threads=", self.generated)
        self.assertIn("__libc_single_threaded", self.generated)
        self.assertIn("libc.value != (expected == 1)", self.generated)


class RunnerTests(unittest.TestCase):
    def test_matrix_is_fixed(self):
        runner.validate_matrix()
        schedule = runner.formal_schedule()
        self.assertEqual(len(runner.CONFIGURATIONS), 6)
        self.assertEqual(len(schedule), 144)
        for configuration in runner.CONFIGURATIONS:
            rows = [row for row in schedule
                    if row["configuration"]["id"] == configuration["id"]]
            self.assertEqual(len(rows), 24)
            for group in range(6):
                arms = [row["arm"] for row in rows if row["group"] == group]
                self.assertEqual(arms, ["ST", "MT", "MT", "ST"])
        throughput = [item for item in runner.CONFIGURATIONS if item["kind"] == "throughput"]
        latency = [item for item in runner.CONFIGURATIONS if item["kind"] == "latency"]
        self.assertEqual([item["payload"] for item in throughput], [64, 256])
        self.assertEqual([(item["payload"], item["latency_origin"]) for item in latency], [
            (64, "attempt"), (64, "planned"), (256, "attempt"), (256, "planned")
        ])

    def test_commands_use_fixed_cgroup_and_explicit_environment(self):
        configuration = runner.CONFIGURATIONS[0]
        command = runner.systemd_round_command(
            configuration, "MT", Path("/tmp/probe"), "p32-name", "p32-unit"
        )
        self.assertEqual(command[0], "systemd-run")
        self.assertIn("--property=AllowedCPUs=0-3", command)
        self.assertIn("--property=Nice=5", command)
        self.assertIn("--property=MemoryHigh=640M", command)
        self.assertIn("--property=MemoryMax=704M", command)
        self.assertIn("--property=MemorySwapMax=0", command)
        self.assertIn("--setenv=SALIAS_P32_PUBLISHER_MODE=mt", command)
        self.assertIn("--setenv=SALIAS_DISABLE_ASYMMETRIC_FENCE=0", command)
        client = runner.client_command(configuration, "MT", Path("/tmp/probe"), "p32-name")
        self.assertEqual(client[:3], ["taskset", "-c", "0"])
        self.assertEqual(client[client.index("--cpu-base") + 1], "1")
        self.assertEqual(client[client.index("--consumers") + 1], "2")

    def test_thread_state_identity_mismatch_is_rejected(self):
        configuration = runner.CONFIGURATIONS[0]
        stdout = fake_stdout(configuration, "MT").replace(
            "actual_threads=2", "actual_threads=1", 1
        )
        _, reasons = runner.parse_probe_output(stdout, configuration, "MT")
        self.assertIn("publisher actual thread count does not match fixed mode", reasons)

    def test_failure_is_retained_without_retry(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            client = root / "probe"
            client.write_text("#!/bin/sh\nexit 0\n")
            client.chmod(0o755)
            calls = []

            def invoke(command, **kwargs):
                calls.append(command)
                kwargs["stdout"].write("")
                kwargs["stderr"].write("synthetic failure\n")
                return SimpleNamespace(returncode=7)

            record = runner.execute_round(
                root / "output",
                "measured",
                runner.CONFIGURATIONS[0],
                "ST",
                0,
                client,
                group=0,
                position=0,
                invoke=invoke,
            )
            self.assertEqual(len(calls), 1)
            self.assertEqual(record["status"], "failed")
            self.assertEqual(record["returncode"], 7)
            self.assertTrue((root / "output" / record["stdout"]).is_file())
            self.assertTrue((root / "output" / record["stderr"]).is_file())
            self.assertTrue((root / "output" / record["record"]).is_file())
            self.assertIsNone(record["cgroup_evidence"])
            self.assertIn("non-zero return code: 7", record["failure_reasons"])

    def test_valid_round_retains_all_evidence(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            client = root / "probe"
            client.write_text("#!/bin/sh\nexit 0\n")
            client.chmod(0o755)
            configuration = runner.CONFIGURATIONS[0]

            def invoke(command, **kwargs):
                kwargs["stdout"].write(fake_stdout(configuration, "MT"))
                kwargs["stderr"].write("")
                return SimpleNamespace(returncode=0)

            record = runner.execute_round(
                root / "output", "measured", configuration, "MT", 4, client,
                group=1, position=0, invoke=invoke,
            )
            self.assertEqual(record["status"], "ok")
            self.assertEqual(record["group"], 1)
            self.assertEqual(record["position"], 0)
            self.assertIsNotNone(record["cgroup_sha256"])
            self.assertEqual(record["environment"]["SALIAS_P32_PUBLISHER_MODE"], "mt")
            saved = json.loads((root / "output" / record["record"]).read_text())
            self.assertEqual(saved["stdout_sha256"], record["stdout_sha256"])
            self.assertEqual(saved["command"], record["command"])

    def test_run_records_stops_and_writes_failure(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            client = root / "probe"
            client.write_text("#!/bin/sh\nexit 0\n")
            client.chmod(0o755)
            output = root / "output"
            output.mkdir()
            manifest = {
                "client": str(client),
                "records": [],
                "progress": {"measured_rounds": 0},
                "status": "running",
            }
            calls = []

            def invoke(command, **kwargs):
                calls.append(command)
                kwargs["stderr"].write("failure\n")
                return SimpleNamespace(returncode=9)

            schedule = [{
                "configuration": runner.CONFIGURATIONS[0],
                "arm": "ST",
                "ordinal": 0,
                "group": 0,
                "position": 0,
            }]
            with self.assertRaisesRegex(RuntimeError, "measured round failed"):
                runner.run_records(output, manifest, "measured", schedule, invoke)
            self.assertEqual(len(calls), 1)
            failure = json.loads((output / "failure.json").read_text())
            self.assertEqual(failure["retry_policy"], "none")
            self.assertEqual(json.loads((output / "experiment.json").read_text())["status"], "failed")


PROBE = os.environ.get("SALIAS_P32_PROBE")


@unittest.skipUnless(PROBE, "set SALIAS_P32_PROBE to the compiled P3-2 probe")
class CompiledProbeTests(unittest.TestCase):
    def environment(self, mode):
        environment = dict(os.environ)
        environment.pop("SALIAS_P32_PUBLISHER_MODE", None)
        environment["SALIAS_DISABLE_ASYMMETRIC_FENCE"] = "0"
        if mode is not None:
            environment["SALIAS_P32_PUBLISHER_MODE"] = mode
        return environment

    def test_self_test_requires_explicit_mode(self):
        missing = self.run_probe(["--self-test"], self.environment(None))
        self.assertEqual(missing.returncode, 1)
        self.assertIn("must be explicitly set to st or mt", missing.stderr)
        for mode, expected in (("st", "1"), ("mt", "2")):
            completed = self.run_probe(["--self-test"], self.environment(mode))
            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertIn(f"publisher_mode={mode}", completed.stdout)
            self.assertIn(f"expected_publisher_threads={expected}", completed.stdout)

    def test_mode_b_rejects_non_a_barrier(self):
        environment = self.environment("st")
        environment["SALIAS_DISABLE_ASYMMETRIC_FENCE"] = "1"
        completed = self.run_probe(["--self-test"], environment)
        self.assertEqual(completed.returncode, 1)
        self.assertIn("requires product barrier mode A", completed.stderr)

    def test_publisher_thread_state_matches_mode(self):
        for mode, expected in (("st", 1), ("mt", 2)):
            name = f"p32-test-{os.getpid()}-{mode}"
            completed = self.run_probe([
                "--scenario", "fifo", "--producers", "1", "--consumers", "1",
                "--messages", "10000", "--payload", "64", "--page", "normal",
                "--capacity", str(1 << 20), "--batch-size", "1", "--poll-limit", "64",
                "--cpu-base", "1", "--cpu-stride", "1", "--name", name,
                "--rate", "0", "--latency-sample-rate", "0", "--latency-origin", "claim",
            ], self.environment(mode))
            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertIn(
                f"THREAD_STATE role=publisher mode={mode} expected_threads={expected} "
                f"actual_threads={expected} libc_single_threaded_available=1 "
                f"libc_single_threaded={1 if expected == 1 else 0} before_publish=1",
                completed.stdout,
            )

    @staticmethod
    def run_probe(arguments, environment):
        import subprocess
        return subprocess.run(
            [PROBE, *arguments], cwd=ROOT, env=environment, text=True,
            capture_output=True, check=False, timeout=30,
        )


if __name__ == "__main__":
    unittest.main()
