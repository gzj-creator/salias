#!/usr/bin/env python3
"""Tests for the zero-layout E1 client patcher."""

import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock


sys.path.insert(0, str(Path(__file__).resolve().parent))
from make_fence_patch_client import (  # noqa: E402
    PATCH_OFFSETS,
    PATCHED_SHA256,
    PatchError,
    build,
    patch_bytes,
    verify_source,
)


FROZEN_CLIENT = (Path(__file__).resolve().parents[2] / "build/benchmarks/"
                 "baseline-extended-20261019-183822/salias_ipc_compare.uninstrumented")


class FencePatchTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.output = Path(self.temporary.name) / "e1"

    def test_real_frozen_client_builds_registered_artifact(self):
        if not FROZEN_CLIENT.is_file():
            self.skipTest("frozen v3 client is not available in this checkout")
        target, manifest = build(FROZEN_CLIENT, self.output)
        self.assertEqual(hashlib.sha256(target.read_bytes()).hexdigest(), PATCHED_SHA256)
        data = json.loads(manifest.read_text())
        self.assertTrue(data["diagnostic_only"])
        self.assertEqual(data["patch_count"], 4)
        self.assertEqual(data["verification"]["cmp_differences"], 4)
        self.assertEqual(data["verification"]["disassembly_changes"], 4)
        self.assertEqual(
            [item["file_offset"] for item in data["patches"]],
            [f"0x{offset:x}" for offset in PATCH_OFFSETS],
        )
        for patch in data["patches"]:
            self.assertTrue(patch["before_instruction"].startswith("xchg "))
            self.assertTrue(patch["after_instruction"].startswith("mov "))
            self.assertEqual(len(patch["before_bytes"].split()),
                             len(patch["after_bytes"].split()))

    def test_source_identity_is_enforced(self):
        if not FROZEN_CLIENT.is_file():
            self.skipTest("frozen v3 client is not available in this checkout")
        data = FROZEN_CLIENT.read_bytes()
        with self.assertRaisesRegex(PatchError, "SHA-256"):
            verify_source(data, "0" * 64)

    def test_patch_changes_only_four_opcodes(self):
        if not FROZEN_CLIENT.is_file():
            self.skipTest("frozen v3 client is not available in this checkout")
        data = FROZEN_CLIENT.read_bytes()
        patched = patch_bytes(data)
        self.assertEqual(len(patched), len(data))
        self.assertEqual(
            [index for index in PATCH_OFFSETS if data[index] != patched[index]],
            list(PATCH_OFFSETS),
        )

    def test_non_empty_output_is_refused(self):
        self.output.mkdir(parents=True)
        (self.output / "existing").write_text("keep")
        with self.assertRaisesRegex(PatchError, "not empty"):
            build(FROZEN_CLIENT, self.output)

    def test_digest_inconsistency_removes_partial_artifact(self):
        if not FROZEN_CLIENT.is_file():
            self.skipTest("frozen v3 client is not available in this checkout")
        with mock.patch(
            "make_fence_patch_client.PATCHED_SHA256", "0" * 64
        ):
            with self.assertRaisesRegex(PatchError, "patched SHA-256"):
                build(FROZEN_CLIENT, self.output)
        self.assertFalse((self.output / "salias_ipc_compare.nofence-diag").exists())
        self.assertFalse((self.output / "patch-manifest.json").exists())


if __name__ == "__main__":
    unittest.main()
