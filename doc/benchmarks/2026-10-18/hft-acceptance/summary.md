# Synthetic FIFO IPC Acceptance Results

Measurement: **1800 seconds total**, 225 seconds per phase; 8 phases.
Published **2,070,000,000**, delivered **3,105,000,000**, sampled deliveries **48,543,540**.
Integrity: **True**; offered-load completed: **True**.

All latency columns are microseconds. Percentiles are computed from merged histogram buckets, not median-of-percentiles.
Maximum is a **sampled** maximum. 50 us is a provisional IPC-hop budget, not a production SLA.

## Primary Acceptance: Worst Subscriber, Planned Arrival

A stage needs healthy workers, correct delivery, completion of offered input, and every subscriber meeting the percentile budget.
First-attempt latency is supplementary; pooled subscriber histograms are not the acceptance criterion.

| Phase | Valid and complete | Worst planned p99 us (subscriber) | Worst planned p99.9 us (subscriber) | p99/p99.9 acceptance |
|---|---|---:|---:|---|
| 2p1s-256b-burst | True | 2555.903 (S0) | 37748.735 (S0) | FAIL/FAIL |
| 2p1s-256b-steady | True | 11.391 (S0) | 1392.639 (S0) | PASS/FAIL |
| 2p1s-64b-burst | True | 9.343 (S0) | 503.807 (S0) | PASS/FAIL |
| 2p1s-64b-steady | True | 7.423 (S0) | 190.463 (S0) | PASS/FAIL |
| 2p2s-256b-burst | True | 1605.631 (S0) | 6422.527 (S0) | FAIL/FAIL |
| 2p2s-256b-steady | True | 1196.031 (S0) | 4587.519 (S0) | FAIL/FAIL |
| 2p2s-64b-burst | True | 1294.335 (S0) | 31719.423 (S0) | FAIL/FAIL |
| 2p2s-64b-steady | True | 1097.727 (S1) | 3571.711 (S1) | FAIL/FAIL |

## First Attempt to Subscriber

| Phase | p50 | p99 | p99.9 | Sampled max | p99/p99.9 <=50us |
|---|---:|---:|---:|---:|---|
| 2p1s-256b-burst | 0.615 | 106.495 | 2621.439 | 16105.657 | FAIL/FAIL |
| 2p1s-256b-steady | 0.583 | 2.527 | 215.039 | 8915.821 | PASS/FAIL |
| 2p1s-64b-burst | 0.519 | 0.903 | 106.495 | 3916.705 | PASS/FAIL |
| 2p1s-64b-steady | 0.551 | 1.007 | 41.983 | 5009.844 | PASS/PASS |
| 2p2s-256b-burst | 0.599 | 39.935 | 1753.087 | 9985.133 | PASS/FAIL |
| 2p2s-256b-steady | 0.599 | 49.151 | 1949.695 | 8978.185 | PASS/FAIL |
| 2p2s-64b-burst | 0.519 | 82.943 | 2588.671 | 15017.096 | FAIL/FAIL |
| 2p2s-64b-steady | 0.535 | 397.311 | 2555.903 | 12636.620 | FAIL/FAIL |

## Planned Arrival to Subscriber

| Phase | p50 | p99 | p99.9 | Sampled max | p99/p99.9 <=50us |
|---|---:|---:|---:|---:|---|
| 2p1s-256b-burst | 0.671 | 2555.903 | 37748.735 | 95371.495 | FAIL/FAIL |
| 2p1s-256b-steady | 0.647 | 11.391 | 1392.639 | 23978.981 | PASS/FAIL |
| 2p1s-64b-burst | 0.575 | 9.343 | 503.807 | 6061.054 | PASS/FAIL |
| 2p1s-64b-steady | 0.607 | 7.423 | 190.463 | 5009.920 | PASS/FAIL |
| 2p2s-256b-burst | 0.671 | 1507.327 | 6422.527 | 20732.433 | FAIL/FAIL |
| 2p2s-256b-steady | 0.671 | 1179.647 | 4521.983 | 53663.911 | FAIL/FAIL |
| 2p2s-64b-burst | 0.583 | 1277.951 | 31195.135 | 71233.605 | FAIL/FAIL |
| 2p2s-64b-steady | 0.599 | 1081.343 | 3538.943 | 22774.305 | FAIL/FAIL |

## Per Subscriber

| Phase | Subscriber | Attempt p99/p99.9 | Planned p99/p99.9 | Planned sampled max | Samples | Integrity |
|---|---:|---:|---:|---:|---:|---|
| 2p1s-256b-burst | 0 | 106.495/2621.439 | 2555.903/37748.735 | 95371.495 | 4,572,840 | True |
| 2p1s-256b-steady | 0 | 2.527/215.039 | 11.391/1392.639 | 23978.981 | 3,517,750 | True |
| 2p1s-64b-burst | 0 | 0.903/106.495 | 9.343/503.807 | 6061.054 | 4,572,840 | True |
| 2p1s-64b-steady | 0 | 1.007/41.983 | 7.423/190.463 | 5009.920 | 3,517,750 | True |
| 2p2s-256b-burst | 0 | 41.983/2293.759 | 1605.631/6422.527 | 20732.433 | 4,572,840 | True |
| 2p2s-256b-burst | 1 | 38.911/1277.951 | 1425.407/6356.991 | 19732.774 | 4,572,840 | True |
| 2p2s-256b-steady | 0 | 46.591/2195.455 | 1196.031/4587.519 | 53663.884 | 3,517,750 | True |
| 2p2s-256b-steady | 1 | 51.711/1622.015 | 1163.263/4456.447 | 53663.911 | 3,517,750 | True |
| 2p2s-64b-burst | 0 | 87.039/2719.743 | 1294.335/31719.423 | 71233.605 | 4,572,840 | True |
| 2p2s-64b-burst | 1 | 78.847/2359.295 | 1245.183/30408.703 | 70339.328 | 4,572,840 | True |
| 2p2s-64b-steady | 0 | 372.735/2457.599 | 1048.575/3538.943 | 19873.281 | 3,517,750 | True |
| 2p2s-64b-steady | 1 | 425.983/2588.671 | 1097.727/3571.711 | 22774.305 | 3,517,750 | True |

## Load and Backpressure

| Phase | Published | Delivered | Actual Mmsg/s | Unsent | BP retries | BP episode ms sum (2P) | Observed unsent highwater (P0/P1) |
|---|---:|---:|---:|---:|---:|---:|---|
| 2p1s-256b-burst | 292,500,000 | 292,500,000 | 1.300000 | 0 | 442,577 | 4202.412 | 114835/76847 |
| 2p1s-256b-steady | 225,000,000 | 225,000,000 | 1.000000 | 0 | 33,447 | 317.230 | 6956/11667 |
| 2p1s-64b-burst | 292,500,000 | 292,500,000 | 1.300000 | 0 | 254 | 2.248 | 1598/3025 |
| 2p1s-64b-steady | 225,000,000 | 225,000,000 | 1.000000 | 0 | 556 | 3.860 | 1444/1520 |
| 2p2s-256b-burst | 292,500,000 | 585,000,000 | 1.300000 | 0 | 283,645 | 4366.936 | 22366/18762 |
| 2p2s-256b-steady | 225,000,000 | 450,000,000 | 1.000000 | 0 | 236,382 | 3761.001 | 26851/16678 |
| 2p2s-64b-burst | 292,500,000 | 585,000,000 | 1.300000 | 0 | 102,227 | 1469.896 | 89579/88791 |
| 2p2s-64b-steady | 225,000,000 | 450,000,000 | 1.000000 | 0 | 18,805 | 343.429 | 5001/9795 |

## Heterogeneous Global Sample Merge

This is descriptive only: different topologies, payloads and traffic patterns must not be pooled for configuration acceptance.
Attempt p50/p99/p99.9/max us: 0.551 | 45.055 | 2015.231 | 16105.657.
Planned p50/p99/p99.9/max us: 0.615 | 1048.575 | 7208.959 | 95371.495.

Historical raw JSONL and histogram buckets were removed after verification at user request; scalar results remain in ../performance-results.json.
Progress-counter backlog is approximate: publisher counters publish every 256 messages and independent snapshots are not atomic.
Backpressure time sums across publishers; it is not additional wall-clock duration.
Passing synthetic sequence and payload checks does not validate exchange sequencing, recovery or order-book correctness.

Neither p99.99 nor the worst latency over ALL messages was measured; FIFO does not imply cross-publisher total order.
Matrix exit code records execution/integrity, not a latency acceptance pass.
