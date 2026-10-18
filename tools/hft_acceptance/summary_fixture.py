"""Small synthetic regression input; never read measured benchmark artifacts."""

import copy

import summarize


def histogram(value, samples):
    """Build a one-bucket synthetic distribution with exact budget counters."""
    index = next(i for i in range(3776) if summarize.bucket_upper(i) >= value)
    result = {
        "samples": samples,
        "max_sampled_ns": value,
        "above_50us": samples if value > 50_000 else 0,
        "buckets": [[index, samples]],
    }
    for key in ("p50_ns", "p99_ns", "p999_ns"):
        result[key] = summarize.bucket_upper(index)
    for key in ("p99_pass_50us", "p999_pass_50us"):
        result[key] = value <= 50_000
    return result


def make_run():
    """Return independent, explicitly synthetic 2P2S burst records per test."""
    start = {
        "type": "start", "start_monotonic_ns": 1_000_000_000,
        "duration_s": 1, "subscribers": 2, "payload_bytes": 256,
        "pattern": "burst", "steady_total_rate": 1_000_000,
        "peak_total_rate": 4_000_000, "burst_ms_per_second": 100,
        "planned_published": 1_300_000, "capacity_per_publisher_bytes": 4_194_304,
        "publication_window_bytes": 131_072, "batch": 1, "poll_limit": 64,
        "sample_hash_mask": 63, "cpus": [0, 1, 2, 3],
    }
    publishers = [
        {"publisher": i, "sent": 650_000, "unsent": 0,
         "backpressure_retries": 0, "first_attempt_to_commit": histogram(1000, 1000),
         "planned_to_first_attempt": histogram(1000, 1000)}
        for i in range(2)
    ]
    sequence = [
        {"publisher": i, "received": 650_000, "unique": 650_000,
         "missing_published": 0, "duplicates": 0, "out_of_order": 0,
         "gap_events": 0, "gap_positions_observed": 0, "corrupt": 0, "invalid": 0}
        for i in range(2)
    ]
    subscribers = [
        {"subscriber": i, "received": 1_300_000, "integrity_pass": True,
         "sequence": copy.deepcopy(sequence),
         "from_first_attempt": histogram(1000 + i * 1000, 2000),
         "from_planned_arrival": histogram(1000 + i * 1000, 2000)}
        for i in range(2)
    ]
    result = {
        "type": "result", "measurement_seconds": 1,
        "wall_seconds_including_drain_and_reap": 1.001,
        "planned_published": 1_300_000, "published": 1_300_000,
        "unsent_at_cutoff": 0, "delivered": 2_600_000,
        "publishers": publishers, "subscribers": subscribers,
        "integrity_pass": True, "offered_load_completed": True,
        "worker_failure": False,
    }
    for aggregate, key in (("aggregate_from_first_attempt", "from_first_attempt"),
                           ("aggregate_from_planned_arrival", "from_planned_arrival")):
        result[aggregate] = summarize.merge([sub[key] for sub in subscribers])
    return [start, result]
