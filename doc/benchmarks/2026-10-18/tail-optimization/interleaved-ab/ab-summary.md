# Interleaved Tail Latency A/B

Order per configuration: baseline, candidate, candidate, baseline; two runs per binary.
Primary latency starts at the unchanged planned arrival. Each subscriber is merged separately across runs.
These short runs screen effects; they do not replace the 30-minute candidate acceptance.

| Configuration | Baseline p99 us | Candidate p99 us | Baseline p99.9 us | Candidate p99.9 us | p99.9 change | Valid/complete |
|---|---:|---:|---:|---:|---:|---|
| 2p1s-256b-burst | 28.159 | 8.191 | 1671.167 | 614.399 | -63.2% | False/True |
| 2p1s-256b-steady | 9.471 | 7.487 | 802.815 | 778.239 | -3.1% | True/True |
| 2p1s-64b-burst | 12.543 | 7.871 | 1228.799 | 178.175 | -85.5% | True/True |
| 2p1s-64b-steady | 10.239 | 6.463 | 1261.567 | 712.703 | -43.5% | True/True |
| 2p2s-256b-burst | 2621.439 | 2686.975 | 9437.183 | 6946.815 | -26.4% | True/True |
| 2p2s-256b-steady | 1392.639 | 1474.559 | 3538.943 | 3735.551 | +5.6% | False/False |
| 2p2s-64b-burst | 1638.399 | 3047.423 | 3604.479 | 7077.887 | +96.4% | True/True |
| 2p2s-64b-steady | 1490.943 | 1474.559 | 3440.639 | 3440.639 | +0.0% | False/True |

## Individual Runs

| Configuration | Version | Round | Worst p99 us | Worst p99.9 us | Valid/complete |
|---|---|---|---:|---:|---|
| 2p1s-256b-burst | baseline | round-0 | 401.407 | 2162.687 | True |
| 2p1s-256b-burst | baseline | round-3 | 11.391 | 589.823 | False |
| 2p1s-256b-burst | candidate | round-1 | 7.295 | 356.351 | True |
| 2p1s-256b-burst | candidate | round-2 | 9.471 | 778.239 | True |
| 2p1s-256b-steady | baseline | round-0 | 10.879 | 933.887 | True |
| 2p1s-256b-steady | baseline | round-3 | 8.319 | 598.015 | True |
| 2p1s-256b-steady | candidate | round-1 | 5.503 | 483.327 | True |
| 2p1s-256b-steady | candidate | round-2 | 9.727 | 991.231 | True |
| 2p1s-64b-burst | baseline | round-0 | 19.199 | 1605.631 | True |
| 2p1s-64b-burst | baseline | round-3 | 8.319 | 307.199 | True |
| 2p1s-64b-burst | candidate | round-1 | 9.983 | 96.255 | True |
| 2p1s-64b-burst | candidate | round-2 | 6.079 | 290.815 | True |
| 2p1s-64b-steady | baseline | round-0 | 11.775 | 1540.095 | True |
| 2p1s-64b-steady | baseline | round-3 | 8.831 | 925.695 | True |
| 2p1s-64b-steady | candidate | round-1 | 6.207 | 786.431 | True |
| 2p1s-64b-steady | candidate | round-2 | 6.655 | 630.783 | True |
| 2p2s-256b-burst | baseline | round-0 | 2949.119 | 10223.615 | True |
| 2p2s-256b-burst | baseline | round-3 | 1867.775 | 5308.415 | True |
| 2p2s-256b-burst | candidate | round-1 | 2785.279 | 6815.743 | True |
| 2p2s-256b-burst | candidate | round-2 | 2621.439 | 7667.711 | True |
| 2p2s-256b-steady | baseline | round-0 | 1605.631 | 3702.783 | True |
| 2p2s-256b-steady | baseline | round-3 | 1310.719 | 3342.335 | False |
| 2p2s-256b-steady | candidate | round-1 | 1589.247 | 3702.783 | True |
| 2p2s-256b-steady | candidate | round-2 | 1409.023 | 3768.319 | False |
| 2p2s-64b-burst | baseline | round-0 | 1605.631 | 3538.943 | True |
| 2p2s-64b-burst | baseline | round-3 | 1687.551 | 3932.159 | True |
| 2p2s-64b-burst | candidate | round-1 | 1966.079 | 3997.695 | True |
| 2p2s-64b-burst | candidate | round-2 | 5242.879 | 7536.639 | True |
| 2p2s-64b-steady | baseline | round-0 | 1556.479 | 3473.407 | True |
| 2p2s-64b-steady | baseline | round-3 | 1409.023 | 4030.463 | False |
| 2p2s-64b-steady | candidate | round-1 | 1474.559 | 3112.959 | True |
| 2p2s-64b-steady | candidate | round-2 | 1589.247 | 3702.783 | True |

Negative change means lower p99.9. No causal claim about guest scheduling or host contention is made.
Do not subtract independent quantiles to attribute component time. No cross-configuration percentiles are reported.
