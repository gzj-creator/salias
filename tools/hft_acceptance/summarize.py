#!/usr/bin/env python3
"""Validate raw acceptance JSONL and generate audited summary artifacts."""

import argparse
import hashlib
import json
from pathlib import Path


def bucket_upper(index):
    if index < 64:
        return index
    if index >= 3775:
        return (1 << 64) - 1
    exponent, sub = divmod(index - 64, 64)
    base = 1 << (exponent + 6)
    return base + (sub + 1) * (base // 64) - 1


def percentile(histogram, numerator, denominator):
    samples = histogram["samples"]
    rank = (samples * numerator + denominator - 1) // denominator
    if not samples:
        return 0
    total = 0
    for index, count in histogram["buckets"]:
        total += count
        if total >= rank:
            return bucket_upper(index)
    raise AssertionError("histogram count mismatch")


def check_histogram(h):
    assert sum(count for _, count in h["buckets"]) == h["samples"]
    assert h["buckets"] == sorted(h["buckets"])
    assert len({index for index, _ in h["buckets"]}) == len(h["buckets"])
    assert all(0 <= index < 3776 and count > 0 for index, count in h["buckets"])
    for key, n, d in [("p50_ns", 50, 100), ("p99_ns", 99, 100), ("p999_ns", 999, 1000)]:
        assert h[key] == percentile(h, n, d), key
        rank = (h["samples"] * n + d - 1) // d
        if key != "p50_ns":
            passed = bool(h["samples"] and h["samples"] - h["above_50us"] >= rank)
            assert h[key.replace("_ns", "_pass_50us")] == passed
    if h["samples"]:
        assert h["max_sampled_ns"] <= bucket_upper(h["buckets"][-1][0])
    else:
        assert h["max_sampled_ns"] == 0


def merge(histograms):
    buckets = {}
    for h in histograms:
        check_histogram(h)
        for index, count in h["buckets"]:
            buckets[index] = buckets.get(index, 0) + count
    result = {
        "samples": sum(h["samples"] for h in histograms),
        "max_sampled_ns": max((h["max_sampled_ns"] for h in histograms), default=0),
        "above_50us": sum(h["above_50us"] for h in histograms),
        "buckets": sorted(map(list, buckets.items())),
    }
    for key, n, d in [("p50_ns", 50, 100), ("p99_ns", 99, 100), ("p999_ns", 999, 1000)]:
        result[key] = percentile(result, n, d)
        if key != "p50_ns":
            rank = (result["samples"] * n + d - 1) // d
            result[key.replace("_ns", "_pass_50us")] = bool(
                result["samples"] and result["samples"] - result["above_50us"] >= rank
            )
    check_histogram(result)
    return result


def load_phase(path):
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    starts = [row for row in rows if row["type"] == "start"]
    results = [row for row in rows if row["type"] == "result"]
    assert len(starts) == len(results) == 1, path
    start, result = starts[0], results[0]
    assert len(result["publishers"]) == 2
    assert len(result["subscribers"]) == start["subscribers"]
    assert [p["publisher"] for p in result["publishers"]] == [0, 1]
    assert [s["subscriber"] for s in result["subscribers"]] == list(range(start["subscribers"]))
    assert result["measurement_seconds"] == start["duration_s"]
    assert result["planned_published"] == start["planned_published"]
    assert result["wall_seconds_including_drain_and_reap"] >= start["duration_s"]
    assert result["published"] == sum(p["sent"] for p in result["publishers"])
    assert result["planned_published"] - result["published"] == result["unsent_at_cutoff"]
    assert result["delivered"] == sum(s["received"] for s in result["subscribers"])
    assert 0 <= result["published"] <= result["planned_published"]
    if result["offered_load_completed"]:
        assert result["unsent_at_cutoff"] == 0
        assert result["delivered"] == result["published"] * start["subscribers"]
    total_publisher_samples = sum(p["first_attempt_to_commit"]["samples"] for p in result["publishers"])
    for publisher in result["publishers"]:
        check_histogram(publisher["first_attempt_to_commit"])
        check_histogram(publisher["planned_to_first_attempt"])
        assert publisher["planned_to_first_attempt"]["samples"] == publisher["first_attempt_to_commit"]["samples"]
        for key in ("successful_claim", "failed_claim", "commit_call"):
            if key in publisher:
                check_histogram(publisher[key])
                if not start.get("diagnostics", False):
                    assert publisher[key]["samples"] == 0, "formal run contains diagnostic samples"
        if start.get("diagnostics", False):
            assert publisher["successful_claim"]["samples"] == publisher["commit_call"]["samples"] == publisher["first_attempt_to_commit"]["samples"]
    for sub in result["subscribers"]:
        assert len(sub["sequence"]) == 2
        assert [q["publisher"] for q in sub["sequence"]] == [0, 1]
        check_histogram(sub["from_first_attempt"])
        check_histogram(sub["from_planned_arrival"])
        if sub["integrity_pass"]:
            assert sub["from_first_attempt"]["samples"] == total_publisher_samples
            assert sub["from_planned_arrival"]["samples"] == total_publisher_samples
            assert sub["received"] == result["published"]
            for sequence, publisher in zip(sub["sequence"], result["publishers"]):
                assert sequence["unique"] == publisher["sent"]
                assert not any(sequence[key] for key in ["missing_published", "duplicates", "out_of_order", "gap_events", "corrupt", "invalid"])
    if result["integrity_pass"]:
        assert not result["worker_failure"]
        assert all(sub["integrity_pass"] for sub in result["subscribers"])
    for key, subkey in [("aggregate_from_first_attempt", "from_first_attempt"), ("aggregate_from_planned_arrival", "from_planned_arrival")]:
        assert merge([sub[subkey] for sub in result["subscribers"]]) == result[key]
    progress = [row for row in rows if row["type"] == "progress"]
    max_due_minus_sent = max((max(0, row["due"] - sum(row["sent"])) for row in progress), default=0)
    max_due_minus_received = [
        max((max(0, row["due"] - row["received"][i]) for row in progress), default=0)
        for i in range(start["subscribers"])
    ]
    max_sent_minus_received = [
        max((max(0, sum(row["sent"]) - row["received"][i]) for row in progress), default=0)
        for i in range(start["subscribers"])
    ]
    # Acceptance uses the original arrival schedule and requires EVERY subscriber
    # to pass. Pooling broadcasts must not hide a slower critical subscriber.
    valid_and_complete = bool(
        not result["worker_failure"] and result["integrity_pass"] and
        result["offered_load_completed"] and
        result["unsent_at_cutoff"] == 0 and
        all(sub["integrity_pass"] and sub["from_planned_arrival"]["samples"] > 0
            for sub in result["subscribers"])
    )
    assessment = {"valid_and_complete": valid_and_complete}
    for key, label in [("p99_ns", "p99"), ("p999_ns", "p999")]:
        worst = max(result["subscribers"], key=lambda sub: sub["from_planned_arrival"][key])
        assessment[f"worst_subscriber_{label}"] = worst["subscriber"]
        assessment[f"worst_planned_{label}_ns"] = worst["from_planned_arrival"][key]
        assessment[f"{label}_acceptance_pass"] = bool(valid_and_complete and all(
            sub["from_planned_arrival"][f"{label}_pass_50us"] for sub in result["subscribers"]
        ))
    return {
        "phase": path.stem,
        "raw_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "start": start,
        "result": result,
        "progress_observations": len(progress),
        "max_due_minus_sent_1s_observed": max_due_minus_sent,
        "max_due_minus_received_1s_observed": max_due_minus_received,
        "max_sent_minus_received_1s_observed": max_sent_minus_received,
        "acceptance": assessment,
    }


def check_formal_start(start):
    """Formal matrices must keep offered traffic and omit intrusive/fault diagnostics."""
    assert not start.get("diagnostics", False), "diagnostic run is not acceptance data"
    assert start.get("stall_subscriber_ms", 0) == 0, "fault-injection run is not acceptance data"
    assert (start["steady_total_rate"], start["peak_total_rate"]) == (1000000, 4000000), "offered rate changed"
    assert start["burst_ms_per_second"] == (100 if start["pattern"] == "burst" else 0)


def cells(h):
    values = [h[key] / 1000 for key in ["p50_ns", "p99_ns", "p999_ns", "max_sampled_ns"]]
    return " | ".join(f"{value:.3f}" for value in values)


def pass_pair(h):
    return "/".join("PASS" if h[key] else "FAIL" for key in ["p99_pass_50us", "p999_pass_50us"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--expected-seconds", type=int, default=225)
    args = parser.parse_args()
    phases = [load_phase(path) for path in sorted(args.directory.glob("2p*s-*b-*.jsonl"))]
    expected = {f"2p{s}s-{payload}b-{pattern}" for s in [1, 2] for payload in [64, 256] for pattern in ["steady", "burst"]}
    assert {phase["phase"] for phase in phases} == expected
    assert all(p["start"]["duration_s"] == args.expected_seconds for p in phases)
    for phase in phases:
        start = phase["start"]
        check_formal_start(start)
        assert phase["phase"] == f"2p{start['subscribers']}s-{start['payload_bytes']}b-{start['pattern']}"
        assert (start["capacity_per_publisher_bytes"], start["publication_window_bytes"], start["batch"], start["poll_limit"], start["sample_hash_mask"]) == (4194304, 131072, 1, 64, 63)
        if start["pattern"] == "steady":
            planned = start["duration_s"] * start["steady_total_rate"]
        else:
            planned = start["duration_s"] * (
                start["peak_total_rate"] * start["burst_ms_per_second"] +
                start["steady_total_rate"] * (1000 - start["burst_ms_per_second"])
            ) // 1000
        assert start["planned_published"] == phase["result"]["planned_published"] == planned
    measured = sum(p["result"]["measurement_seconds"] for p in phases)
    published = sum(p["result"]["published"] for p in phases)
    delivered = sum(p["result"]["delivered"] for p in phases)
    global_attempt = merge([p["result"]["aggregate_from_first_attempt"] for p in phases])
    global_planned = merge([p["result"]["aggregate_from_planned_arrival"] for p in phases])
    document = {
        "schema": 1,
        "measurement_seconds_sum": measured,
        "measurement_seconds_per_phase": args.expected_seconds,
        "planned_published": sum(p["result"]["planned_published"] for p in phases),
        "published": published,
        "delivered": delivered,
        "published_payload_bytes": sum(p["result"]["published"] * p["start"]["payload_bytes"] for p in phases),
        "delivered_payload_bytes": sum(p["result"]["delivered"] * p["start"]["payload_bytes"] for p in phases),
        "sampled_deliveries": global_attempt["samples"],
        "all_integrity_pass": all(p["result"]["integrity_pass"] for p in phases),
        "all_offered_load_completed": all(p["result"]["offered_load_completed"] for p in phases),
        "all_stages_p99_acceptance_pass": all(p["acceptance"]["p99_acceptance_pass"] for p in phases),
        "all_stages_p999_acceptance_pass": all(p["acceptance"]["p999_acceptance_pass"] for p in phases),
        "global_heterogeneous_mix_from_first_attempt": global_attempt,
        "global_heterogeneous_mix_from_planned_arrival": global_planned,
        "phases": phases,
    }
    state = args.directory / "run-state.txt"
    if state.exists():
        document["run_state"] = state.read_text()
        timing = dict(line.split("=", 1) for line in state.read_text().splitlines() if line.startswith(("start_epoch=", "end_epoch=")))
        if len(timing) == 2:
            document["matrix_elapsed_wall_seconds"] = float(timing["end_epoch"]) - float(timing["start_epoch"])
    (args.directory / "summary.json").write_text(json.dumps(document, indent=2) + "\n")
    lines = [
        "# Synthetic FIFO IPC Acceptance Results", "",
        f"Measurement: **{measured} seconds total**, {args.expected_seconds} seconds per phase; 8 phases.",
        f"Published **{published:,}**, delivered **{delivered:,}**, sampled deliveries **{global_attempt['samples']:,}**.",
        f"Integrity: **{document['all_integrity_pass']}**; offered-load completed: **{document['all_offered_load_completed']}**.",
        "", "All latency columns are microseconds. Percentiles are computed from merged histogram buckets, not median-of-percentiles.",
        "Maximum is a **sampled** maximum. 50 us is a provisional IPC-hop budget, not a production SLA.",
        "", "## Primary Acceptance: Worst Subscriber, Planned Arrival", "",
        "A stage needs healthy workers, correct delivery, completion of offered input, and every subscriber meeting the percentile budget.",
        "First-attempt latency is supplementary; pooled subscriber histograms are not the acceptance criterion.",
        "", "| Phase | Valid and complete | Worst planned p99 us (subscriber) | Worst planned p99.9 us (subscriber) | p99/p99.9 acceptance |",
        "|---|---|---:|---:|---|",
    ]
    for p in phases:
        a = p["acceptance"]
        verdict = "/".join("PASS" if a[f"{key}_acceptance_pass"] else "FAIL" for key in ["p99", "p999"])
        lines.append(f"| {p['phase']} | {a['valid_and_complete']} | {a['worst_planned_p99_ns']/1000:.3f} (S{a['worst_subscriber_p99']}) | "
                     f"{a['worst_planned_p999_ns']/1000:.3f} (S{a['worst_subscriber_p999']}) | {verdict} |")
    lines += [
        "", "## First Attempt to Subscriber", "",
        "| Phase | p50 | p99 | p99.9 | Sampled max | p99/p99.9 <=50us |",
        "|---|---:|---:|---:|---:|---|",
    ]
    for p in phases:
        h = p["result"]["aggregate_from_first_attempt"]
        lines.append(f"| {p['phase']} | {cells(h)} | {pass_pair(h)} |")
    lines += ["", "## Planned Arrival to Subscriber", "",
              "| Phase | p50 | p99 | p99.9 | Sampled max | p99/p99.9 <=50us |",
              "|---|---:|---:|---:|---:|---|"]
    for p in phases:
        h = p["result"]["aggregate_from_planned_arrival"]
        lines.append(f"| {p['phase']} | {cells(h)} | {pass_pair(h)} |")
    lines += ["", "## Per Subscriber", "",
              "| Phase | Subscriber | Attempt p99/p99.9 | Planned p99/p99.9 | Planned sampled max | Samples | Integrity |",
              "|---|---:|---:|---:|---:|---:|---|"]
    for p in phases:
        for s in p["result"]["subscribers"]:
            a, h = s["from_first_attempt"], s["from_planned_arrival"]
            lines.append(f"| {p['phase']} | {s['subscriber']} | {a['p99_ns']/1000:.3f}/{a['p999_ns']/1000:.3f} | "
                         f"{h['p99_ns']/1000:.3f}/{h['p999_ns']/1000:.3f} | {h['max_sampled_ns']/1000:.3f} | {h['samples']:,} | {s['integrity_pass']} |")
    lines += ["", "## Load and Backpressure", "",
              "| Phase | Published | Delivered | Actual Mmsg/s | Unsent | BP retries | BP episode ms sum (2P) | Observed unsent highwater (P0/P1) |",
              "|---|---:|---:|---:|---:|---:|---:|---|" ]
    for p in phases:
        r = p["result"]
        lines.append(f"| {p['phase']} | {r['published']:,} | {r['delivered']:,} | {r['actual_published_msg_s']/1e6:.6f} | {r['unsent_at_cutoff']:,} | "
                     f"{sum(x['backpressure_retries'] for x in r['publishers']):,} | {sum(x['backpressure_episode_ns'] for x in r['publishers'])/1e6:.3f} | "
                     + "/".join(str(x["unsent_backlog_highwater_observed"]) for x in r["publishers"]) + " |")
    lines += ["", "## Heterogeneous Global Sample Merge", "",
              "This is descriptive only: different topologies, payloads and traffic patterns must not be pooled for configuration acceptance.",
              f"Attempt p50/p99/p99.9/max us: {cells(global_attempt)}.",
              f"Planned p50/p99/p99.9/max us: {cells(global_planned)}.", "",
              "Raw JSONL retains per-publisher/per-subscriber integrity checks, sparse histograms, per-second progress and planned/uncompleted arrivals.",
              "Progress-counter backlog is approximate: publisher counters publish every 256 messages and independent snapshots are not atomic.",
              "Backpressure time sums across publishers; it is not additional wall-clock duration.",
              "Passing synthetic sequence and payload checks does not validate exchange sequencing, recovery or order-book correctness.", ""]
    lines += ["Neither p99.99 nor the worst latency over ALL messages was measured; FIFO does not imply cross-publisher total order.",
              "Matrix exit code records execution/integrity, not a latency acceptance pass.", ""]
    (args.directory / "summary.md").write_text("\n".join(lines))
    print(json.dumps({key: document[key] for key in ["measurement_seconds_sum", "published", "delivered", "sampled_deliveries", "all_integrity_pass", "all_offered_load_completed"]}))


if __name__ == "__main__":
    main()
