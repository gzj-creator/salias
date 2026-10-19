#!/usr/bin/env python3
"""Generate a diagnostic-only top-32 tail timestamp recorder for perf correlation."""

import argparse
from pathlib import Path
import subprocess

from build_design_variants import replace_once

ROOT = Path(__file__).resolve().parents[2]


def instrument(text):
    text = replace_once(text, "  std::uint64_t start_ns = 0;", """  std::uint64_t start_ns = 0;
  struct TailEvent { std::uint64_t send_ns = 0; std::uint64_t receive_ns = 0; };
  std::array<std::array<TailEvent, 32>, kMaxWorkers> tail_events{};
""")
    text = replace_once(text, '    } else if (arg == "--help") {',
        '    } else if (arg == "--tail-threshold-ns") {\n      static_cast<void>(require_value("--tail-threshold-ns"));\n    } else if (arg == "--help") {')
    text = replace_once(text, "      max_latency_ns = std::max(max_latency_ns, elapsed_ns);", """      max_latency_ns = std::max(max_latency_ns, elapsed_ns);
      if (elapsed_ns >= 10000) {
        auto& events = state.tail_events[consumer_index];
        auto smallest = std::min_element(events.begin(), events.end(), [](const auto& a, const auto& b) {
          return a.receive_ns - a.send_ns < b.receive_ns - b.send_ns;
        });
        if (elapsed_ns > smallest->receive_ns - smallest->send_ns) {
          *smallest = {.send_ns = send_time_ns, .receive_ns = receive_time_ns};
        }
      }""")
    return replace_once(text, "      aggregate_max_ns = std::max(aggregate_max_ns, max_ns);", """      aggregate_max_ns = std::max(aggregate_max_ns, max_ns);
      for (const auto& event : state.tail_events[consumer]) {
        if (event.receive_ns != 0) {
          std::cout << "TAIL consumer=" << consumer << " send_ns=" << event.send_ns
                    << " receive_ns=" << event.receive_ns
                    << " latency_ns=" << event.receive_ns - event.send_ns << '\\n';
        }
      }""")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    for header in ("latency_histogram.hpp", "latency_pacing.hpp"):
        (args.output / header).write_text((ROOT / "tools/aeron_compare" / header).read_text())
    path = args.output / "tail_probe.cpp"
    path.write_text(instrument((ROOT / "tools/aeron_compare/salias_ipc_compare.cpp").read_text()))
    subprocess.run(["c++", "-std=c++23", "-O3", "-g", "-DNDEBUG", "-march=native", "-flto=1",
                    "-I", str(ROOT / "src/salias/include"), "-I", str(ROOT / "src"), str(path),
                    str(ROOT / "build/release/src/salias/libsalias.a"),
                    str(ROOT / "build/release/src/core/libsalias_core.a"), "-pthread", "-lrt",
                    "-o", str(args.output / "trace")], check=True)


if __name__ == "__main__":
    main()
