#!/usr/bin/env python3
"""Build separately instrumented, matched-message timing probes, not release binaries."""

import argparse
from pathlib import Path
import subprocess

from build_design_variants import replace_once

ROOT = Path(__file__).resolve().parents[2]


def instrument(text, aeron):
    text = replace_once(text, "  return options;", """  if (options.payload < 32 || options.latency_origin != "attempt" ||
      options.latency_sample_rate == 0) {
    throw std::invalid_argument("stage probe requires payload >= 32, attempt origin and sampling");
  }
  return options;""")
    text = replace_once(text, "  std::uint64_t start_ns = 0;", """  std::uint64_t start_ns = 0;
  std::array<std::array<std::array<std::uint64_t, latency::kBucketCount>, 3>, kMaxWorkers> stage_buckets{};
  std::array<std::array<std::uint64_t, 3>, kMaxWorkers> stage_sum{};
""")
    emit = """
void emit_stages(const SharedRunState& state, std::uint32_t consumers) {
  const char* names[] = {"claim", "precommit", "handoff"};
  for (std::uint32_t c = 0; c < consumers; ++c) {
    const auto samples = state.latency_samples[c];
    for (int stage = 0; stage < 3; ++stage) {
      const auto* buckets = state.stage_buckets[c][stage].data();
      std::cout << "STAGE consumer=" << c << " stage=" << names[stage] << " samples=" << samples
                << " p50_ns=" << latency::percentile(buckets, samples, 50, 100)
                << " p99_ns=" << latency::percentile(buckets, samples, 99, 100)
                << " p999_ns=" << latency::percentile(buckets, samples, 999, 1000)
                << " sum_ns=" << state.stage_sum[c][stage] << '\\n';
    }
  }
}

"""
    anchor = "Result run_case(const Options& options) {" if aeron else "template <salias::Mode M>\nResult run_case_typed(const Options& options) {"
    text = replace_once(text, anchor, emit + anchor)
    text = replace_once(text, "    destroy_shared_state(state);\n    return Result",
                        "    emit_stages(state, options.consumers);\n    destroy_shared_state(state);\n    return Result")
    if aeron:
        text = replace_once(text, "        if (position > 0) {", "        if (position > 0) {\n          const auto claim_end_ns = sampled ? latency::monotonic_now_ns() : 0;")
        text = replace_once(text, "          claim.commit();", """          if (sampled) {
            claim.buffer().putInt64(claim.offset() + 16, claim_end_ns);
            claim.buffer().putInt64(claim.offset() + 24, latency::monotonic_now_ns());
          }
          claim.commit();""")
        load = """      const auto claim_end_ns = static_cast<std::uint64_t>(buffer.getInt64(offset + 16));
      const auto precommit_ns = static_cast<std::uint64_t>(buffer.getInt64(offset + 24));
"""
    else:
        text = replace_once(text, "    if (claim_result) {", "    if (claim_result) {\n      const auto claim_end_ns = sampled ? latency::monotonic_now_ns() : 0;")
        text = replace_once(text, "        publisher.commit(std::move(claim));", """        if (sampled) {
        std::memcpy(claim.payload().data() + 16, &claim_end_ns, sizeof(claim_end_ns));
        const auto precommit_ns = latency::monotonic_now_ns();
        std::memcpy(claim.payload().data() + 24, &precommit_ns, sizeof(precommit_ns));
      }
        publisher.commit(std::move(claim));""")
        load = """      std::uint64_t claim_end_ns, precommit_ns;
      std::memcpy(&claim_end_ns, message.payload.data() + 16, sizeof(claim_end_ns));
      std::memcpy(&precommit_ns, message.payload.data() + 24, sizeof(precommit_ns));
"""
    text = replace_once(text, "      const std::uint64_t elapsed_ns = receive_time_ns - send_time_ns;",
        "      const std::uint64_t elapsed_ns = receive_time_ns - send_time_ns;\n" + load + """      const std::uint64_t stages[] = {claim_end_ns - send_time_ns,
                                      precommit_ns - claim_end_ns, receive_time_ns - precommit_ns};
      for (int stage = 0; stage < 3; ++stage) {
        latency::record(state.stage_buckets[consumer_index][stage].data(), stages[stage]);
        state.stage_sum[consumer_index][stage] += stages[stage];
      }
""")
    return text


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--fixed-source", type=Path, required=True)
    parser.add_argument("--aeron-source", type=Path, required=True)
    parser.add_argument("--aeron-build", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    for header in ("latency_histogram.hpp", "latency_pacing.hpp"):
        (args.output / header).write_text((ROOT / "tools/aeron_compare" / header).read_text())
    for library in ("salias", "aeron"):
        aeron = library == "aeron"
        source = ROOT / "tools/aeron_compare/aeron_ipc_compare.cpp" if aeron else args.fixed_source
        path = args.output / f"{library}_stages.cpp"
        path.write_text(instrument(source.read_text(), aeron))
        command = ["c++", "-std=c++23", "-O3", "-g", "-DNDEBUG", "-march=native", "-flto=1"]
        if aeron:
            command += ["-DDISABLE_BOUNDS_CHECKS", "-I", str(args.aeron_source / "aeron-client/src/main/cpp_wrapper"),
                        "-I", str(args.aeron_source / "aeron-client/src/main/c")]
            libraries = [str(args.aeron_build / "lib/libaeron_static.a"), "-ldl", "-lm"]
        else:
            command += ["-I", str(ROOT / "src/salias/include"), "-I", str(ROOT / "src")]
            libraries = [str(ROOT / "build/release/src/salias/libsalias.a"), str(ROOT / "build/release/src/core/libsalias_core.a")]
        subprocess.run([*command, str(path), *libraries, "-pthread", "-lrt", "-o", str(args.output / library)], check=True)


if __name__ == "__main__":
    main()
