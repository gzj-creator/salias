# Synthetic FIFO IPC Acceptance Results

Measurement: **1800 seconds total**, 225 seconds per phase; 8 phases.
Published **2,069,999,999**, delivered **3,104,999,998**, sampled deliveries **48,543,540**.
Integrity: **True**; offered-load completed: **False**.

All latency columns are microseconds. Percentiles are computed from merged histogram buckets, not median-of-percentiles.
Maximum is a **sampled** maximum. 50 us is a provisional IPC-hop budget, not a production SLA.

## Primary Acceptance: Worst Subscriber, Planned Arrival

A stage needs healthy workers, correct delivery, completion of offered input, and every subscriber meeting the percentile budget.
First-attempt latency is supplementary; pooled subscriber histograms are not the acceptance criterion.

| Phase | Valid and complete | Worst planned p99 us (subscriber) | Worst planned p99.9 us (subscriber) | p99/p99.9 acceptance |
|---|---|---:|---:|---|
| 2p1s-256b-burst | True | 10.495 (S0) | 1228.799 (S0) | PASS/FAIL |
| 2p1s-256b-steady | True | 7.039 (S0) | 1097.727 (S0) | PASS/FAIL |
| 2p1s-64b-burst | True | 32.255 (S0) | 2015.231 (S0) | PASS/FAIL |
| 2p1s-64b-steady | True | 7.359 (S0) | 802.815 (S0) | PASS/FAIL |
| 2p2s-256b-burst | True | 2981.887 (S0) | 7208.959 (S0) | FAIL/FAIL |
| 2p2s-256b-steady | True | 1359.871 (S1) | 3637.247 (S1) | FAIL/FAIL |
| 2p2s-64b-burst | True | 1310.719 (S1) | 3276.799 (S1) | FAIL/FAIL |
| 2p2s-64b-steady | False | 1261.567 (S1) | 3244.031 (S1) | FAIL/FAIL |

## First Attempt to Subscriber

| Phase | p50 | p99 | p99.9 | Sampled max | p99/p99.9 <=50us |
|---|---:|---:|---:|---:|---|
| 2p1s-256b-burst | 0.583 | 3.743 | 169.983 | 3579.347 | PASS/FAIL |
| 2p1s-256b-steady | 0.591 | 1.071 | 182.271 | 5269.330 | PASS/FAIL |
| 2p1s-64b-burst | 0.519 | 3.135 | 761.855 | 6013.743 | PASS/FAIL |
| 2p1s-64b-steady | 0.535 | 1.647 | 327.679 | 5088.916 | PASS/FAIL |
| 2p2s-256b-burst | 0.607 | 385.023 | 2162.687 | 7843.754 | FAIL/FAIL |
| 2p2s-256b-steady | 0.599 | 589.823 | 2260.991 | 11304.133 | FAIL/FAIL |
| 2p2s-64b-burst | 0.535 | 729.087 | 2457.599 | 8785.360 | FAIL/FAIL |
| 2p2s-64b-steady | 0.559 | 606.207 | 2195.455 | 9389.755 | FAIL/FAIL |

## Planned Arrival to Subscriber

| Phase | p50 | p99 | p99.9 | Sampled max | p99/p99.9 <=50us |
|---|---:|---:|---:|---:|---|
| 2p1s-256b-burst | 0.639 | 10.495 | 1228.799 | 4234.673 | PASS/FAIL |
| 2p1s-256b-steady | 0.647 | 7.039 | 1097.727 | 6990.858 | PASS/FAIL |
| 2p1s-64b-burst | 0.567 | 32.255 | 2015.231 | 9987.440 | PASS/FAIL |
| 2p1s-64b-steady | 0.583 | 7.359 | 802.815 | 8886.708 | PASS/FAIL |
| 2p2s-256b-burst | 0.671 | 2949.119 | 7208.959 | 17628.101 | FAIL/FAIL |
| 2p2s-256b-steady | 0.663 | 1343.487 | 3604.479 | 22934.207 | FAIL/FAIL |
| 2p2s-64b-burst | 0.599 | 1277.951 | 3178.495 | 12051.120 | FAIL/FAIL |
| 2p2s-64b-steady | 0.623 | 1228.799 | 3112.959 | 20106.121 | FAIL/FAIL |

## Per Subscriber

| Phase | Subscriber | Attempt p99/p99.9 | Planned p99/p99.9 | Planned sampled max | Samples | Integrity |
|---|---:|---:|---:|---:|---:|---|
| 2p1s-256b-burst | 0 | 3.743/169.983 | 10.495/1228.799 | 4234.673 | 4,572,840 | True |
| 2p1s-256b-steady | 0 | 1.071/182.271 | 7.039/1097.727 | 6990.858 | 3,517,750 | True |
| 2p1s-64b-burst | 0 | 3.135/761.855 | 32.255/2015.231 | 9987.440 | 4,572,840 | True |
| 2p1s-64b-steady | 0 | 1.647/327.679 | 7.359/802.815 | 8886.708 | 3,517,750 | True |
| 2p2s-256b-burst | 0 | 458.751/2424.831 | 2981.887/7208.959 | 17628.101 | 4,572,840 | True |
| 2p2s-256b-burst | 1 | 331.775/1720.319 | 2916.351/7208.959 | 17384.806 | 4,572,840 | True |
| 2p2s-256b-steady | 0 | 614.399/1998.847 | 1327.103/3571.711 | 21789.731 | 3,517,750 | True |
| 2p2s-256b-steady | 1 | 565.247/2424.831 | 1359.871/3637.247 | 22934.207 | 3,517,750 | True |
| 2p2s-64b-burst | 0 | 557.055/2162.687 | 1228.799/3080.191 | 12051.120 | 4,572,840 | True |
| 2p2s-64b-burst | 1 | 851.967/2621.439 | 1310.719/3276.799 | 12051.043 | 4,572,840 | True |
| 2p2s-64b-steady | 0 | 540.671/2047.999 | 1196.031/3014.655 | 20106.121 | 3,517,750 | True |
| 2p2s-64b-steady | 1 | 663.551/2326.527 | 1261.567/3244.031 | 16525.323 | 3,517,750 | True |

## Load and Backpressure

| Phase | Published | Delivered | Actual Mmsg/s | Unsent | BP retries | BP episode ms sum (2P) | Observed unsent highwater (P0/P1) |
|---|---:|---:|---:|---:|---:|---:|---|
| 2p1s-256b-burst | 292,500,000 | 292,500,000 | 1.300000 | 0 | 30,907 | 225.753 | 7953/7818 |
| 2p1s-256b-steady | 225,000,000 | 225,000,000 | 1.000000 | 0 | 32,206 | 260.853 | 2872/3351 |
| 2p1s-64b-burst | 292,500,000 | 292,500,000 | 1.300000 | 0 | 17,493 | 143.719 | 6744/19863 |
| 2p1s-64b-steady | 225,000,000 | 225,000,000 | 1.000000 | 0 | 1,177 | 8.710 | 3289/1999 |
| 2p2s-256b-burst | 292,500,000 | 585,000,000 | 1.300000 | 0 | 1,111,128 | 8952.484 | 22167/20841 |
| 2p2s-256b-steady | 225,000,000 | 450,000,000 | 1.000000 | 0 | 835,382 | 6655.122 | 5552/10894 |
| 2p2s-64b-burst | 292,500,000 | 585,000,000 | 1.300000 | 0 | 100,892 | 811.550 | 9016/10672 |
| 2p2s-64b-steady | 224,999,999 | 449,999,998 | 1.000000 | 1 | 34,930 | 282.320 | 8135/7088 |

## Heterogeneous Global Sample Merge

This is descriptive only: different topologies, payloads and traffic patterns must not be pooled for configuration acceptance.
Attempt p50/p99/p99.9/max us: 0.559 | 247.807 | 1916.927 | 11304.133.
Planned p50/p99/p99.9/max us: 0.615 | 1327.103 | 4587.519 | 22934.207.

Historical raw JSONL and histogram buckets were removed after verification at user request; scalar results remain in ../../performance-results.json.
Progress-counter backlog is approximate: publisher counters publish every 256 messages and independent snapshots are not atomic.
Backpressure time sums across publishers; it is not additional wall-clock duration.
Passing synthetic sequence and payload checks does not validate exchange sequencing, recovery or order-book correctness.

Neither p99.99 nor the worst latency over ALL messages was measured; FIFO does not imply cross-publisher total order.
Matrix exit code records execution/integrity, not a latency acceptance pass.
