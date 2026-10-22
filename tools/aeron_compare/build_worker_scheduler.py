#!/usr/bin/env python3
"""Build worker-only FIFO benchmarks, leaving controllers/conductor threads normal."""

import argparse
from pathlib import Path
import subprocess

from build_design_variants import replace_once

ROOT = Path(__file__).resolve().parents[2]


def instrument(text, aeron):
    text = replace_once(text, "#include <unistd.h>", "#include <unistd.h>\n#include <sched.h>\n#include <cstdio>")
    text = replace_once(text, "void pin_worker_if_requested(const Options& options, std::uint32_t worker_index) {", """
// Parent has SCHED_RESET_ON_FORK; only the actual pinned hot worker opts into FIFO.
int experimental_fifo_priority = 0;
void activate_fifo_worker() {
  sched_param parameter{};
  parameter.sched_priority = experimental_fifo_priority;
  if (parameter.sched_priority != 0 &&
      ::sched_setscheduler(0, SCHED_FIFO | SCHED_RESET_ON_FORK, &parameter) != 0) {
    throw std::runtime_error("cannot activate bounded FIFO worker");
  }
  if (parameter.sched_priority != 0) {
    char status[160];
    const int size = std::snprintf(status, sizeof(status),
        "SALIAS_WORKER_FIFO_V1 pid=%d cpu=%d policy=%d priority=%d\\n",
        static_cast<int>(::getpid()), ::sched_getcpu(), ::sched_getscheduler(0),
        parameter.sched_priority);
    if (size > 0 && static_cast<std::size_t>(size) < sizeof(status)) {
      const auto written = ::write(STDERR_FILENO, status, static_cast<std::size_t>(size));
      static_cast<void>(written);
    }
  }
}
void pin_worker_if_requested(const Options& options, std::uint32_t worker_index) {""")
    anchor = "  return options;"
    text = replace_once(text, anchor, """  const int policy = ::sched_getscheduler(0);
  if (policy == SCHED_OTHER) { return options; }
  if (policy != (SCHED_FIFO | SCHED_RESET_ON_FORK)) {
    throw std::runtime_error("worker scheduler requires normal or reset-on-fork FIFO");
  }
  sched_param parameter{};
  if (::sched_getparam(0, &parameter) != 0 || parameter.sched_priority < 1 ||
      parameter.sched_priority > 10) {
    throw std::runtime_error("worker FIFO priority must be 1-10");
  }
  experimental_fifo_priority = parameter.sched_priority;
  parameter.sched_priority = 0;
  if (::sched_setscheduler(0, SCHED_OTHER, &parameter) != 0) {
    throw std::runtime_error("cannot restore normal controller scheduler");
  }
  return options;""")
    # The helper/global must precede parse_options.
    start = text.index("// Parent has SCHED_RESET_ON_FORK;")
    end = text.index("void pin_worker_if_requested", start)
    helper = text[start:end]
    text = text[:start] + text[end:]
    text = replace_once(text, "Options parse_options(int argc, char** argv) {",
                        helper + "\nOptions parse_options(int argc, char** argv) {")
    if aeron:
        anchor = "    aeron::concurrent::BusySpinIdleStrategy idle;"
        if text.count(anchor) != 2:
            raise ValueError("Aeron worker activation anchors changed")
        # Aeron conductors are already running; sched_setscheduler(0) changes only this thread.
        text = text.replace(anchor, "    activate_fifo_worker();\n" + anchor)
    else:
        text = text.replace("    if (has_failed(state)) {\n      _exit(21);",
                            "    activate_fifo_worker();\n    if (has_failed(state)) {\n      _exit(21);")
        text = replace_once(text, "    if (options.batch_size == 1) {",
                            "    activate_fifo_worker();\n    if (options.batch_size == 1) {")
        if text.count("    activate_fifo_worker();") != 2:
            raise ValueError("Salias worker activation anchors changed")
    return text


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--baseline-source", type=Path, required=True)
    parser.add_argument("--baseline-object", type=Path, required=True)
    parser.add_argument("--baseline-include", type=Path, required=True)
    parser.add_argument("--aeron-source", type=Path, required=True)
    parser.add_argument("--aeron-build", type=Path, required=True)
    parser.add_argument("--tail", action="store_true", help="add Salias top-tail timestamps for switch tracing")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    for header in ("latency_histogram.hpp", "latency_pacing.hpp"):
        (args.output / header).write_text((ROOT / "tools/aeron_compare" / header).read_text())
    for variant in (("baseline", "stack") if args.tail else ("baseline", "stack", "aeron")):
        aeron = variant == "aeron"
        source = ROOT / "tools/aeron_compare" / ("aeron_ipc_compare.cpp" if aeron else "salias_ipc_compare.cpp")
        if variant == "baseline":
            source = args.baseline_source
        output = args.output / f"{variant}.cpp"
        text = source.read_text()
        if args.tail:
            from build_tail_probe import instrument as tail_instrument
            text = tail_instrument(text)
        output.write_text(instrument(text, aeron))
        flags = ["c++", "-std=c++23", "-O3", "-g", "-DNDEBUG", "-march=native", "-flto=1"]
        if aeron:
            flags += ["-DDISABLE_BOUNDS_CHECKS", "-I", str(args.aeron_source / "aeron-client/src/main/cpp_wrapper"),
                      "-I", str(args.aeron_source / "aeron-client/src/main/c")]
            libs = [str(args.aeron_build / "lib/libaeron_static.a"), "-ldl", "-lm"]
        else:
            if variant == "baseline":
                flags += ["-I", str(args.baseline_include)]
            flags += ["-I", str(ROOT / "src/salias/include"), "-I", str(ROOT / "src")]
            libs = [str(args.baseline_object) if variant == "baseline" else str(ROOT / "build/release/src/salias/libsalias.a"),
                    str(ROOT / "build/release/src/core/libsalias_core.a")]
        subprocess.run([*flags, str(output), *libs, "-pthread", "-lrt", "-o", str(args.output / variant)], check=True)


if __name__ == "__main__":
    main()
