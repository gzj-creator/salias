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
    ring_impl = ROOT / "src/core/ring/magic_ring.cpp"
    mapping = ROOT / "src/core/platform/mapping.cpp"
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
    if "return {base_ + (pos & mask()), len};" not in ring_impl.read_text():
        raise ValueError("Ring slicing changed; recheck contiguous boundary access")
    mapping_text = mapping.read_text()
    if ("const std::size_t mapped_len = size * 2;" not in mapping_text or
            not re.search(r"::mmap\(base,\s*size,[^;]*?MAP_SHARED\s*\|\s*MAP_FIXED[^;]*?fd,\s*0\)",
                          mapping_text) or
            not re.search(r"::mmap\(base\s*\+\s*size,\s*size,[^;]*?MAP_SHARED\s*\|\s*MAP_FIXED[^;]*?fd,\s*0\)",
                          mapping_text)):
        raise ValueError("Double mapping changed; recheck padding assumptions")
    constants["source_sha256"] = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
                                  for p in (header, codec, channel, ring, ring_impl, mapping)}
    return constants


def analyze(payload, messages, durations, header, alignment, capacity=1048576):
    if (any(not isinstance(value, int) or isinstance(value, bool) for value in
            (payload, messages, header, alignment, capacity)) or
            payload < 0 or messages < 0 or header <= 0 or alignment <= 0 or
            alignment & (alignment - 1) or capacity <= 0 or capacity & (capacity - 1)):
        raise ValueError("Require nonnegative payload/messages and positive power-of-two alignment/capacity")
    if not durations or any(not math.isfinite(s) or s <= 0 for s in durations):
        raise ValueError("Require finite positive baseline durations")
    stride = header + (payload + alignment - 1) // alignment * alignment
    if stride > capacity:
        raise ValueError("Frame exceeds ring capacity")
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
                complete_frames_per_cacheline=64 // stride,
                frame_stride_mod64=stride % 64, total_frame_bytes=messages * stride,
                duration_s_range=durations,
                average_frame_interval_ns_range=[s * 1e9 / messages for s in durations])


def summarize_baseline(baseline):
    """Use five distinct measured rounds and verify their retained stdout evidence."""
    summary = json.loads(baseline.read_text())
    records_path = baseline.with_name("results.jsonl")
    rows = [json.loads(line) for line in records_path.read_text().splitlines()]
    constants = source_constants()
    analyses = []
    for payload in (64, 256):
        cell = [r for r in rows if r["engine"] == "salias" and r["kind"] == "throughput"
                and r["payload"] == payload and r["phase"] == "measured"]
        if (len(cell) != 5 or {r["round"] for r in cell} != set(range(1, 6)) or
                len({r["result"]["published"] for r in cell}) != 1 or
                len({r["result"]["capacity"] for r in cell}) != 1):
            raise ValueError(f"Require five consistent measured Salias rounds for {payload}B")
        measured = []
        for record in sorted(cell, key=lambda row: row["round"]):
            stdout = baseline.parent / Path(record["stdout"]).name
            digest = hashlib.sha256(stdout.read_bytes()).hexdigest()
            if digest != record["stdout_sha256"]:
                raise ValueError(f"stdout checksum mismatch: {stdout}")
            result_lines = [line for line in stdout.read_text().splitlines() if line.startswith("RESULT ")]
            if (len(result_lines) != 1 or
                    dict(item.split("=", 1) for item in result_lines[0].split()[1:]) != record["result"]):
                raise ValueError(f"RESULT metadata mismatch: {stdout}")
            result = record["result"]
            published = int(result["published"])
            if (published <= 0 or int(result["delivered"]) != 2 * published or
                    int(result["producers"]) != 1 or int(result["consumers"]) != 2 or
                    int(result["payload"]) != payload or result["scenario"] != "fifo" or
                    int(result["batch_size"]) != 1 or int(result["latency_samples"]) != 0 or
                    int(result["latency_sample_rate"]) != 0 or int(result["rate_per_producer"]) != 0):
                raise ValueError(f"Delivery/topology/throughput mismatch: {stdout}")
            duration = float(result["seconds"])
            if not math.isfinite(duration) or duration <= 0:
                raise ValueError(f"Invalid measured duration: {stdout}")
            measured.append(dict(round=record["round"], duration_s=duration,
                                 stdout=stdout.name, stdout_sha256=digest))
        messages = int(cell[0]["result"]["published"])
        durations = [r["duration_s"] for r in measured]
        layout = analyze(payload, messages, [min(durations), max(durations)],
                         constants["kHeaderSize"], constants["kFrameAlign"],
                         capacity=int(cell[0]["result"]["capacity"]))
        layout["measured_rounds"] = measured
        analyses.append(layout)
    if len({layout["messages"] for layout in analyses}) != 1:
        raise ValueError("Require equal message counts for the payload wrap comparison")
    small, large = analyses
    if len({layout["capacity_bytes"] for layout in analyses}) != 1:
        raise ValueError("Require equal ring capacities for the payload wrap comparison")
    return dict(
        schema_version=2, baseline=str(baseline.resolve()),
        baseline_commit=summary["environment"]["commit"],
        baseline_sha256=hashlib.sha256(baseline.read_bytes()).hexdigest(),
        results_sha256=hashlib.sha256(records_path.read_bytes()).hexdigest(),
        ttl_enabled=False, constants=constants, layouts=analyses,
        comparison=dict(
            wrap_ratio_64_to_256=small["ring_wraps_nominal"] / large["ring_wraps_nominal"],
            wraps_64b_more_than_twice_256b=small["ring_wraps_nominal"] > 2 * large["ring_wraps_nominal"],
            both_strides_nonaligned=all(not r["frame_stride_cacheline_aligned"] for r in analyses),
            frame_alignment_root_cause_confirmed=False,
            phase2c_supported_by_layout_alone=False),
        interpretation="Both strides are 8 mod 64; nonalignment alone does not explain a 64B-specific gap. "
        "At equal message counts 64B wraps LESS often than 256B, not more. "
        "Frames/cacheline is a byte-density ratio, not a count of whole frames fitting a cacheline.")


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
    result = summarize_baseline(baseline)
    if args.output:
        args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
