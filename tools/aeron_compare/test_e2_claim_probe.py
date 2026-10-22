#!/usr/bin/env python3
"""Test the P3 E2 claim-isolation probe build and self-test."""

import json
import subprocess
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
PROBE_DIR = ROOT / "build/p3-e2-claim-probe"


class TestE2ClaimProbe(unittest.TestCase):
    """Test E2 claim probe build and runtime identity."""

    def test_owning_binary_exists(self):
        """Verify owning variant binary exists."""
        binary = PROBE_DIR / "owning" / "probe"
        self.assertTrue(binary.is_file(), f"owning binary missing: {binary}")
        self.assertTrue(binary.stat().st_mode & 0o111, "owning binary not executable")

    def test_borrowed_binary_exists(self):
        """Verify borrowed variant binary exists."""
        binary = PROBE_DIR / "borrowed" / "probe"
        self.assertTrue(binary.is_file(), f"borrowed binary missing: {binary}")
        self.assertTrue(binary.stat().st_mode & 0o111, "borrowed binary not executable")

    def test_owning_self_test_st(self):
        """Run owning variant self-test in ST mode."""
        binary = PROBE_DIR / "owning" / "probe"
        env = {
            "SALIAS_E2_PUBLISHER_MODE": "st",
            "SALIAS_E2_CLAIM_VARIANT": "owning",
            "SALIAS_DISABLE_ASYMMETRIC_FENCE": "0",
        }
        result = subprocess.run(
            [str(binary), "--self-test"],
            env=env,
            capture_output=True,
            text=True,
            timeout=5,
        )
        self.assertEqual(result.returncode, 0, f"self-test failed:\n{result.stderr}")
        self.assertIn("SELF_TEST", result.stdout)
        self.assertIn("publisher_mode=st", result.stdout)
        self.assertIn("claim_variant=owning", result.stdout)

    def test_owning_self_test_mt_o(self):
        """Run owning variant self-test in MT-O mode."""
        binary = PROBE_DIR / "owning" / "probe"
        env = {
            "SALIAS_E2_PUBLISHER_MODE": "mt-o",
            "SALIAS_E2_CLAIM_VARIANT": "owning",
            "SALIAS_DISABLE_ASYMMETRIC_FENCE": "0",
        }
        result = subprocess.run(
            [str(binary), "--self-test"],
            env=env,
            capture_output=True,
            text=True,
            timeout=5,
        )
        self.assertEqual(result.returncode, 0, f"self-test failed:\n{result.stderr}")
        self.assertIn("publisher_mode=mt-o", result.stdout)
        self.assertIn("claim_variant=owning", result.stdout)

    def test_owning_self_test_mt_x(self):
        """Run owning variant self-test in MT-X mode."""
        binary = PROBE_DIR / "owning" / "probe"
        env = {
            "SALIAS_E2_PUBLISHER_MODE": "mt-x",
            "SALIAS_E2_CLAIM_VARIANT": "owning",
            "SALIAS_DISABLE_ASYMMETRIC_FENCE": "0",
        }
        result = subprocess.run(
            [str(binary), "--self-test"],
            env=env,
            capture_output=True,
            text=True,
            timeout=5,
        )
        self.assertEqual(result.returncode, 0, f"self-test failed:\n{result.stderr}")
        self.assertIn("publisher_mode=mt-x", result.stdout)
        self.assertIn("claim_variant=owning", result.stdout)

    def test_borrowed_self_test_mt_b(self):
        """Run borrowed variant self-test in MT-B mode."""
        binary = PROBE_DIR / "borrowed" / "probe"
        env = {
            "SALIAS_E2_PUBLISHER_MODE": "mt-b",
            "SALIAS_E2_CLAIM_VARIANT": "borrowed",
            "SALIAS_DISABLE_ASYMMETRIC_FENCE": "0",
        }
        result = subprocess.run(
            [str(binary), "--self-test"],
            env=env,
            capture_output=True,
            text=True,
            timeout=5,
        )
        self.assertEqual(result.returncode, 0, f"self-test failed:\n{result.stderr}")
        self.assertIn("publisher_mode=mt-b", result.stdout)
        self.assertIn("claim_variant=borrowed", result.stdout)

    def test_variant_consistency_check(self):
        """Verify MT-B with owning claim is rejected."""
        binary = PROBE_DIR / "owning" / "probe"
        env = {
            "SALIAS_E2_PUBLISHER_MODE": "mt-b",
            "SALIAS_E2_CLAIM_VARIANT": "owning",  # Mismatch
            "SALIAS_DISABLE_ASYMMETRIC_FENCE": "0",
        }
        result = subprocess.run(
            [str(binary), "--self-test"],
            env=env,
            capture_output=True,
            text=True,
            timeout=5,
        )
        self.assertNotEqual(result.returncode, 0, "should reject MT-B + owning")
        self.assertIn("MT-B requires borrowed claim", result.stderr)

    def test_combined_manifest(self):
        """Verify combined manifest has both variants."""
        manifest_path = PROBE_DIR / "combined-manifest.json"
        self.assertTrue(manifest_path.is_file(), "combined manifest missing")

        manifest = json.loads(manifest_path.read_text())
        self.assertEqual(manifest["protocol"], "p3-e2-claim-isolation-probe-v1")
        self.assertIn("owning", manifest["variants"])
        self.assertIn("borrowed", manifest["variants"])

        owning = manifest["variants"]["owning"]
        borrowed = manifest["variants"]["borrowed"]

        self.assertEqual(owning["variant"], "owning")
        self.assertEqual(borrowed["variant"], "borrowed")
        self.assertNotEqual(owning["client_sha256"], borrowed["client_sha256"])


if __name__ == "__main__":
    if not PROBE_DIR.is_dir():
        print(f"Probe directory not found: {PROBE_DIR}", file=sys.stderr)
        print("Run: python3 tools/aeron_compare/build_e2_claim_probe.py", file=sys.stderr)
        sys.exit(1)

    unittest.main()
