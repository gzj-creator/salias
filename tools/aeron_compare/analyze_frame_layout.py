#!/usr/bin/env python3
"""Analyze the actual no-TTL frame layout against measured long-run results."""

import argparse
import hashlib
import json
import math
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[2]


def source_constants():
    header = ROOT / "src/core/frame/header.hpp"
    codec = ROOT / "src/core/frame/codec.hpp"
    channel = ROOT / "src/core/channel/shared_hybrid_mpsc.hpp"
    ring = ROOT / "src/core/ring/magic_ring.hpp"
    constants = {}
    for name in ("kHeaderSize", "kFrameAlign"):
        match = re.search(rf"\b{name}\s*=\s*(\d+)\s*;", header.read_text())
        if not match:
            raise ValueError(f"Cannot extract {name} from {header}")
        constants[name] = int(match.group(1))
    if "return kHeaderSize + (has_expiry ? sizeof(std::uint64_t) : 0) + align_up(payload_len);" not in codec.read_text():
        raise ValueError("frame_len changed; update the layout analysis before using it")
    if "producer_pos_ = tail + need;" not in channel.read_text():
        raise ValueError("Claim position stepping changed; recheck wrap semantics")
    constants["source_sha256"] = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
                                  for p in (header, codec, channel, ring)}
    return constants


def analyze(payload, messages, durations, header, alignment, capacity=1048576):
    stride = header + (payload + alignment - 1) // alignment * alignment
    frames = capacity // stride
    # SharedHybridMpsc uses MagicRing's double mapping: frames cross the physical
    # boundary without padding, and the absolute position advances by frame_len.
    offsets = sorted({(i * stride) % 64 for i in range(64 // math.gcd(stride, 64))})
    return dict(payload=payload, messages=messages, header_bytes=header, alignment_bytes=alignment,
                frame_stride_bytes=stride, capacity_bytes=capacity, max_inflight_frames=frames,
                frame_stride_cacheline_aligned=stride % 64 == 0, header_offsets_mod64=offsets,
                header_can_straddle_cacheline=any(o + header > 64 for o in offsets),
                ring_wraps_nominal=messages * stride / capacity,
                ring_wraps_completed=messages * stride // capacity,
                ring_padding_bytes=0, frames_per_cacheline_density=64 / stride,
                duration_s_range=durations,
                average_frame_interval_ns_range=[s * 1e9 / messages for s in durations])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    baseline = args.baseline
    if baseline is None:
        candidates = sorted((ROOT / "build/benchmarks").glob("baseline-extended-*/summary.json"))
        if not candidates:
            parser.error("No completed extended baseline; supply --baseline")
        baseline = candidates[-1]
    summary = json.loads(baseline.read_text())
    rows = [json.loads(line) for line in baseline.with_name("results.jsonl").read_text().splitlines()]
    constants = source_constants()
    analyses = []
    for payload in (64, 256):
        cell = [r for r in rows if r["engine"] == "salias" and r["kind"] == "throughput"
                and r["payload"] == payload and r["phase"] == "measured"]
        if len(cell) != 5 or len({r["result"]["published"] for r in cell}) != 1:
            raise ValueError(f"Require five consistent measured Salias rounds for {payload}B")
        messages = int(cell[0]["result"]["published"])
        durations = [float(r["result"]["seconds"]) for r in cell]
        analyses.append(analyze(payload, messages, [min(durations), max(durations)],
                                constants["kHeaderSize"], constants["kFrameAlign"]))
    result = dict(baseline=str(baseline.resolve()), baseline_commit=summary["environment"]["commit"],
                  ttl_enabled=False, constants=constants, layouts=analyses,
                  interpretation="Both strides are 8 mod 64; nonalignment alone does not explain a 64B-specific gap. "
                  "At equal message counts 64B wraps LESS often than 256B, not more. "
                  "Frames/cacheline is a byte-density ratio, not a count of whole frames fitting a cacheline.")
    if args.output:
        args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
