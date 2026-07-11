#!/usr/bin/env python3
import argparse
import math
import re
import statistics
from collections import defaultdict


def percentile(values, fraction):
    ordered = sorted(values)
    if not ordered:
        return 0.0
    index = (len(ordered) - 1) * fraction
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] * (upper - index) + ordered[upper] * (index - lower)


parser = argparse.ArgumentParser()
parser.add_argument("log")
parser.add_argument("--markdown", action="store_true")
args = parser.parse_args()

groups = defaultdict(list)
for line in open(args.log, encoding="utf-8"):
    if not line.startswith("RUN "):
        continue
    fields = dict(re.findall(r"([a-zA-Z0-9_]+)=([^ ]+)", line))
    key = (
        fields["library"], fields["mode"], fields["topology"],
        int(fields["capacity"]), int(fields["batch"]),
    )
    groups[key].append(fields)

if args.markdown:
    print("| Library | Mode | Topology | Capacity | Batch | N | Publish median | p10-p90 | Delivery median | Backpressure median |")
    print("|---|---|---|---:|---:|---:|---:|---:|---:|---:|")
for key, rows in sorted(groups.items()):
    library, mode, topology, capacity, batch = key
    publish = [float(row["publish_msg_per_sec"]) / 1e6 for row in rows]
    delivery = [float(row["delivery_msg_per_sec"]) / 1e6 for row in rows]
    backpressure = [float(row["backpressure_rate"]) * 100 for row in rows]
    if args.markdown:
        print(
            f"| {library} | {mode} | {topology} | {capacity // 1048576} MiB | {batch} | {len(rows)} "
            f"| {statistics.median(publish):.3f} M/s "
            f"| {percentile(publish, .1):.3f}-{percentile(publish, .9):.3f} "
            f"| {statistics.median(delivery):.3f} M/s "
            f"| {statistics.median(backpressure):.3f}% |"
        )
    else:
        print(
            f"SUMMARY library={library} mode={mode} topology={topology} capacity={capacity} "
            f"batch={batch} samples={len(rows)} publish_median_mmsg_s={statistics.median(publish):.6f} "
            f"publish_p10_mmsg_s={percentile(publish,.1):.6f} publish_p90_mmsg_s={percentile(publish,.9):.6f} "
            f"delivery_median_mmsg_s={statistics.median(delivery):.6f} "
            f"backpressure_median_pct={statistics.median(backpressure):.6f}"
        )
