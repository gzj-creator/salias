#!/usr/bin/env python3
"""Tests for the claim API v4 same-client A/B builder."""

import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location(
    "build_claim_v4_ab", ROOT / "tools/aeron_compare/build_claim_v4_ab.py"
)
builder = importlib.util.module_from_spec(spec)
spec.loader.exec_module(builder)


class ClaimV4BuildTest(unittest.TestCase):
    def setUp(self):
        self.source = (ROOT / builder.CLIENT_PATH).read_text()

    def test_extracted_arms_have_exactly_the_expected_submission_calls(self):
        v3 = builder.extract_client(self.source, "v3")
        v4 = builder.extract_client(self.source, "v4")
        self.assertEqual(builder.client_api_counts(v3),
                         {"claim_commit": 2, "publisher_commit": 0})
        self.assertEqual(builder.client_api_counts(v4),
                         {"claim_commit": 0, "publisher_commit": 2})

    def test_anchor_changes_fail_closed(self):
        with self.assertRaisesRegex(ValueError, "submission sites"):
            builder.extract_client(
                self.source.replace("publisher.commit(std::move(claim));",
                                    "publisher.commit(std::move(claim));\n"
                                    "      publisher.commit(std::move(claim));", 1),
                "v3",
            )

    def test_missing_v3_anchor_fails_closed(self):
        changed = self.source.replace(
            "      publisher.commit(std::move(claim));\n",
            "      publisher.commit(claim);\n",
            1,
        )
        with self.assertRaisesRegex(ValueError, "anchor"):
            builder.extract_client(changed, "v3")

    def test_manifest_records_client_and_implementation_hashes(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            with mock.patch.object(builder, "build_variant", side_effect=[
                        {"api": "v3", "client": "v3/client",
                         "client_source_sha256": "a" * 64,
                         "implementation_sha256": {"channel.cpp": "b" * 64}},
                        {"api": "v4", "client": "v4/client",
                         "client_source_sha256": "c" * 64,
                         "implementation_sha256": {"channel.cpp": "d" * 64}},
                    ]):
                builder.build(type("Args", (), {"output": output})())
            manifest = json.loads((output / "manifest.json").read_text())
            self.assertEqual(manifest["protocol"], builder.PROTOCOL)
            self.assertEqual(manifest["client_template_identity"],
                             builder.CLIENT_PATH)
            self.assertEqual(
                manifest["client_template_sha256"],
                builder.digest((ROOT / builder.CLIENT_PATH).read_bytes()),
            )
            self.assertEqual(manifest["baseline_client_source_identity"],
                             f"git:{builder.BASELINE_TAG}:{builder.CLIENT_PATH}")
            self.assertEqual(
                manifest["baseline_client_source_sha256"],
                builder.BASELINE_CLIENT_SHA256,
            )
            self.assertEqual(set(manifest["variants"]), {"v3", "v4"})
            self.assertEqual(set(manifest["arms"]),
                             {"v3", "v4-sequential-fence", "v4-default-fence"})
            self.assertEqual(manifest["variants"]["v3"]["client_source_sha256"],
                             "a" * 64)
            self.assertEqual(manifest["variants"]["v4"]["implementation_sha256"],
                             {"channel.cpp": "d" * 64})


if __name__ == "__main__":
    unittest.main()
