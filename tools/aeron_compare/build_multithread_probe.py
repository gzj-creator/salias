#!/usr/bin/env python3
"""Render and build the P3-2 publisher-thread probe from the frozen v3 benchmark.

The v3 benchmark source is deliberately not copied into the repository.  This
generator reads ``v3.0.0:tools/aeron_compare/salias_ipc_compare.cpp`` (or an
explicit byte-identical copy), injects the P3-2 diagnostics, and compiles the
generated source against the current product libraries.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess


ROOT = Path(__file__).resolve().parents[2]
V3_TAG = "v3.0.0"
V3_SOURCE_PATH = "tools/aeron_compare/salias_ipc_compare.cpp"
V3_SOURCE_SHA256 = "6a23a77598c6e980f7b6625eb2471cc42a26917469dc77fc51ab9d5b767bab1e"
PROBE_IDENTITY = "p3-p32-publisher-thread-probe-v1"


def digest(data):
    """Return the lowercase SHA-256 digest of bytes."""
    return hashlib.sha256(data).hexdigest()


def replace_once(text, needle, replacement, label):
    """Replace one exact anchor and fail loudly when the pinned source changes."""
    count = text.count(needle)
    if count != 1:
        raise ValueError(f"expected one {label} anchor, found {count}")
    return text.replace(needle, replacement, 1)


def load_v3_source(path=None):
    """Load and verify the unmodified v3.0.0 benchmark source."""
    if path is None:
        result = subprocess.run(
            ["git", "show", f"{V3_TAG}:{V3_SOURCE_PATH}"],
            cwd=ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        if result.returncode:
            raise RuntimeError(result.stderr.decode("utf-8", "replace").strip())
        data = result.stdout
        identity = f"git:{V3_TAG}:{V3_SOURCE_PATH}"
    else:
        source = Path(path).resolve()
        data = source.read_bytes()
        identity = str(source)
    actual = digest(data)
    if actual != V3_SOURCE_SHA256:
        raise ValueError(
            f"v3 benchmark source SHA-256 is {actual}, expected {V3_SOURCE_SHA256}"
        )
    return data.decode("utf-8"), identity, actual


def instrument(source):
    """Inject identity, mode, thread-state, helper, and cgroup guards into v3."""
    if "pthread_atfork" in source:
        raise ValueError("the pinned v3 source unexpectedly contains pthread_atfork")

    source = replace_once(
        source,
        "#include <thread>\n#include <vector>\n",
        "#include <thread>\n#include <vector>\n\n"
        "#include <condition_variable>\n"
        "#include <dirent.h>\n"
        "#include <dlfcn.h>\n"
        "#include <fstream>\n"
        "#include <mutex>\n"
        "#include <string_view>\n",
        "includes",
    )

    identity_code = f'''\
/// P3-2 probe identity.  The source digest binds this output to the frozen v3 client.
inline constexpr std::string_view kProbeIdentity = "{PROBE_IDENTITY}";
inline constexpr std::string_view kProbeSourceSha256 = "{V3_SOURCE_SHA256}";
inline constexpr std::string_view kPublisherModeEnvironment =
    "SALIAS_P32_PUBLISHER_MODE";

/// Publisher thread modes are selected only through the explicit process environment.
enum class PublisherThreadMode {{ Single, Multi }};

PublisherThreadMode publisher_thread_mode_from_environment() {{
  const char* value = std::getenv(kPublisherModeEnvironment.data());
  if (value == nullptr) {{
    throw std::runtime_error(
        "SALIAS_P32_PUBLISHER_MODE must be explicitly set to st or mt");
  }}
  if (std::strcmp(value, "st") == 0) {{
    return PublisherThreadMode::Single;
  }}
  if (std::strcmp(value, "mt") == 0) {{
    return PublisherThreadMode::Multi;
  }}
  throw std::runtime_error("SALIAS_P32_PUBLISHER_MODE must be st or mt");
}}

const char* publisher_thread_mode_name(PublisherThreadMode mode) noexcept {{
  return mode == PublisherThreadMode::Single ? "st" : "mt";
}}

std::uint32_t expected_publisher_threads(PublisherThreadMode mode) noexcept {{
  return mode == PublisherThreadMode::Single ? 1U : 2U;
}}

/// Reject a non-A fence override; explicit "0" is accepted and still means A.
void validate_fixed_barrier_mode() {{
  const char* value = std::getenv("SALIAS_DISABLE_ASYMMETRIC_FENCE");
  if (value != nullptr && value[0] != '\\0' && std::strcmp(value, "0") != 0) {{
    throw std::runtime_error("P3-2 requires product barrier mode A");
  }}
}}

void print_probe_identity(PublisherThreadMode mode) {{
  std::cout << "PROBE identity=" << kProbeIdentity
            << " source_sha256=" << kProbeSourceSha256
            << " barrier=A publisher_mode=" << publisher_thread_mode_name(mode)
            << std::endl;
}}

std::uint32_t process_thread_count() noexcept {{
  DIR* directory = ::opendir("/proc/self/task");
  if (directory == nullptr) {{
    return 0;
  }}
  std::uint32_t count = 0;
  while (dirent* entry = ::readdir(directory)) {{
    if (entry->d_name[0] != '.') {{
      ++count;
    }}
  }}
  static_cast<void>(::closedir(directory));
  return count;
}}

struct LibcSingleThreadedState {{
  bool available = false;
  bool value = false;
}};

LibcSingleThreadedState read_libc_single_threaded_state() noexcept {{
  const void* symbol = ::dlsym(RTLD_DEFAULT, "__libc_single_threaded");
  if (symbol == nullptr) {{
    return {{}};
  }}
  return {{.available = true,
          .value = *static_cast<const char*>(symbol) != 0}};
}}

void validate_and_print_publisher_thread_state(PublisherThreadMode mode) {{
  const std::uint32_t actual = process_thread_count();
  const std::uint32_t expected = expected_publisher_threads(mode);
  const LibcSingleThreadedState libc = read_libc_single_threaded_state();
  std::cout << "THREAD_STATE role=publisher mode="
            << publisher_thread_mode_name(mode) << " expected_threads=" << expected
            << " actual_threads=" << actual
            << " libc_single_threaded_available=" << (libc.available ? 1 : 0)
            << " libc_single_threaded="
            << (libc.available ? (libc.value ? 1 : 0) : -1)
            << " before_publish=1" << std::endl;
  if (actual != expected) {{
    throw std::runtime_error("publisher thread count does not match the explicit mode");
  }}
  if (libc.available && libc.value != (expected == 1)) {{
    throw std::runtime_error("libc single-threaded state does not match the explicit mode");
  }}
}}

/// A condition-variable sleeper.  It never publishes, polls, or spins.
class SleepingHelper {{
 public:
  ~SleepingHelper() {{ stop(); }}

  void start_if(PublisherThreadMode mode) {{
    if (mode != PublisherThreadMode::Multi || worker_.joinable()) {{
      return;
    }}
    worker_ = std::thread([this] {{
      std::unique_lock lock(mutex_);
      ready_ = true;
      ready_condition_.notify_all();
      stop_condition_.wait(lock, [this] {{ return stop_; }});
    }});
    std::unique_lock lock(mutex_);
    ready_condition_.wait(lock, [this] {{ return ready_; }});
  }}

  void stop() noexcept {{
    try {{
      {{
        std::lock_guard lock(mutex_);
        stop_ = true;
      }}
      stop_condition_.notify_all();
      if (worker_.joinable()) {{
        worker_.join();
      }}
    }} catch (...) {{
    }}
  }}

 private:
  std::mutex mutex_;
  std::condition_variable ready_condition_;
  std::condition_variable stop_condition_;
  bool ready_ = false;
  bool stop_ = false;
  std::thread worker_;
}};

/// Emit raw cgroup-v2 counters after the run.  The coordinator remains in the
/// same transient unit, so this covers successful and failed worker paths.
class CgroupEvidenceWriter {{
 public:
  ~CgroupEvidenceWriter() {{ emit(); }}

  void emit() noexcept {{
    if (emitted_) {{
      return;
    }}
    emitted_ = true;
    try {{
      std::string cgroup_path = "unavailable";
      std::ifstream membership("/proc/self/cgroup");
      std::string line;
      while (std::getline(membership, line)) {{
        if (line.rfind("0::", 0) == 0) {{
          cgroup_path = line.substr(3);
          break;
        }}
      }}
      const std::string root =
          cgroup_path == "unavailable" ? std::string{{}} : "/sys/fs/cgroup" + cgroup_path;
      std::cout << "\\nCGROUP_EVIDENCE_BEGIN version=1 cgroup_path=" << cgroup_path
                << '\\n';
      constexpr const char* files[] = {{
          "cpu.stat", "cpu.pressure", "memory.current", "memory.peak",
          "memory.events", "memory.high", "memory.max", "memory.swap.max",
          "memory.swap.current", "memory.pressure", "io.pressure",
          "cpuset.cpus", "cpuset.cpus.effective", "pids.current", "pids.max",
      }};
      for (const char* name : files) {{
        std::cout << "[CGROUP_FILE " << name << "]\\n";
        std::ifstream input(root + "/" + name);
        if (!input) {{
          std::cout << "<unavailable>\\n";
          continue;
        }}
        char character = '\\n';
        bool wrote_any = false;
        while (input.get(character)) {{
          std::cout.put(character);
          wrote_any = true;
        }}
        if (!wrote_any || character != '\\n') {{
          std::cout.put('\\n');
        }}
      }}
      std::cout << "CGROUP_EVIDENCE_END" << std::endl;
    }} catch (...) {{
    }}
  }}

 private:
  bool emitted_ = false;
}};

'''
    source = replace_once(
        source,
        "namespace latency = salias::tools::latency;\n",
        "namespace latency = salias::tools::latency;\n\n" + identity_code,
        "identity insertion",
    )

    source = replace_once(
        source,
        "  bool consumers_set = false;             ///< 标记 consumers 是否由用户显式指定\n",
        "  bool consumers_set = false;             ///< 标记 consumers 是否由用户显式指定\n"
        "  bool self_test = false;                 ///< 仅检查构建身份与环境开关\n",
        "self-test option member",
    )
    source = replace_once(
        source,
        "    } else if (arg == \"--help\") {\n",
        "    } else if (arg == \"--self-test\") {\n"
        "      options.self_test = true;\n"
        "    } else if (arg == \"--help\") {\n",
        "self-test parser",
    )
    source = replace_once(
        source,
        "                   \"[--latency-origin claim|attempt|planned]\\n\";\n",
        "                   \"[--latency-origin claim|attempt|planned] [--self-test]\\n\";\n",
        "help text",
    )

    source = replace_once(
        source,
        "    auto publisher = channel.publisher();\n    ready_and_wait_for_start(state);\n",
        "    auto publisher = channel.publisher();\n"
        "    ready_and_wait_for_start(state);\n"
        "    if (has_failed(state)) {\n"
        "      _exit(31);\n"
        "    }\n\n"
        "    // This is the publisher child's normal post-fork path.  No atfork\n"
        "    // callback creates threads; the helper only sleeps after MT is selected.\n"
        "    helper.start_if(publisher_thread_mode_from_environment());\n"
        "    validate_and_print_publisher_thread_state(\n"
        "        publisher_thread_mode_from_environment());\n",
        "publisher helper insertion",
    )
    # Remove the original duplicate failure check after the inserted validation.
    source = replace_once(
        source,
        "    validate_and_print_publisher_thread_state(\n"
        "        publisher_thread_mode_from_environment());\n"
        "    if (has_failed(state)) {\n"
        "      _exit(31);\n"
        "    }\n\n",
        "    validate_and_print_publisher_thread_state(\n"
        "        publisher_thread_mode_from_environment());\n\n",
        "publisher duplicate failure check",
    )
    source = replace_once(
        source,
        "[[noreturn]] void run_producer_child(const Options& options, std::uint32_t producer_index,\n"
        "                                     SharedRunState& state) noexcept {\n  try {\n",
        "[[noreturn]] void run_producer_child(const Options& options, std::uint32_t producer_index,\n"
        "                                     SharedRunState& state) noexcept {\n"
        "  SleepingHelper helper;\n  try {\n",
        "producer helper lifetime",
    )
    source = replace_once(
        source,
        "        if (!claim_until_committed<M>(publisher, options.payload, marker, i, options, state,\n"
        "                                      planned_ns)) {\n"
        "          _exit(32);\n"
        "        }\n",
        "        if (!claim_until_committed<M>(publisher, options.payload, marker, i, options, state,\n"
        "                                      planned_ns)) {\n"
        "          helper.stop();\n"
        "          _exit(32);\n"
        "        }\n",
        "producer claim failure",
    )
    source = replace_once(
        source,
        "      if (!offer_batch_until_committed<M>(publisher, options, producer_index, state)) {\n"
        "        _exit(32);\n"
        "      }\n",
        "      if (!offer_batch_until_committed<M>(publisher, options, producer_index, state)) {\n"
        "        helper.stop();\n"
        "        _exit(32);\n"
        "      }\n",
        "producer offer failure",
    )
    source = replace_once(
        source,
        "    }\n    _exit(0);\n  } catch (...) {\n    mark_failed(state);\n    _exit(33);\n  }\n}\n",
        "    }\n    helper.stop();\n    _exit(0);\n  } catch (...) {\n"
        "    helper.stop();\n    mark_failed(state);\n    _exit(33);\n  }\n}\n",
        "producer helper teardown",
    )

    source = replace_once(
        source,
        "int main(int argc, char** argv) {\n  try {\n"
        "    const Options options = parse_options(argc, argv);\n"
        "    print_result(options, run_case(options));\n",
        "int main(int argc, char** argv) {\n"
        "  CgroupEvidenceWriter cgroup_evidence;\n"
        "  try {\n"
        "    const Options options = parse_options(argc, argv);\n"
        "    validate_fixed_barrier_mode();\n"
        "    const PublisherThreadMode publisher_mode = publisher_thread_mode_from_environment();\n"
        "    print_probe_identity(publisher_mode);\n"
        "    if (options.self_test) {\n"
        "      std::cout << \"SELF_TEST identity=\" << kProbeIdentity\n"
        "                << \" source_sha256=\" << kProbeSourceSha256\n"
        "                << \" barrier=A publisher_mode=\"\n"
        "                << publisher_thread_mode_name(publisher_mode)\n"
        "                << \" expected_publisher_threads=\"\n"
        "                << expected_publisher_threads(publisher_mode) << std::endl;\n"
        "      return 0;\n"
        "    }\n"
        "    print_result(options, run_case(options));\n",
        "main identity and self-test",
    )
    return source


def default_paths():
    """Return the default generated-source, binary, and manifest locations."""
    directory = ROOT / "build/p3-p32-multithread-probe"
    return directory / "source/salias_ipc_compare.p32.cpp", directory / "probe"


def build(args):
    """Render the pinned source, compile the probe, and write a build manifest."""
    source_text, source_identity, source_sha = load_v3_source(args.source)
    generated_text = instrument(source_text)
    _, default_binary = default_paths()
    output = args.output.resolve() if args.output else default_binary
    generated = args.generated_source.resolve() if args.generated_source else (
        output.parent / "source" / "salias_ipc_compare.p32.cpp"
    )
    generated.parent.mkdir(parents=True, exist_ok=True)
    generated.write_text(generated_text)
    output.parent.mkdir(parents=True, exist_ok=True)

    command = [
        os.environ.get("CXX", "c++"),
        "-std=c++23",
        "-O3",
        "-g",
        "-DNDEBUG",
        "-march=native",
        "-flto=1",
        "-I",
        str(ROOT / "src/salias/include"),
        "-I",
        str(ROOT / "src"),
        "-I",
        str(ROOT / "tools/aeron_compare"),
        str(generated),
        str(args.salias_library),
        str(args.core_library),
        "-pthread",
        "-lrt",
        "-ldl",
        "-o",
        str(output),
    ]
    subprocess.run(command, cwd=ROOT, check=True)
    manifest = {
        "protocol": PROBE_IDENTITY,
        "source_identity": source_identity,
        "source_sha256": source_sha,
        "generated_source": str(generated),
        "generated_source_sha256": digest(generated.read_bytes()),
        "client": str(output),
        "client_sha256": digest(output.read_bytes()),
        "compile_command": command,
        "compile_command_shell": shlex.join(command),
        "product_libraries": {
            "salias": {
                "path": str(args.salias_library),
                "sha256": digest(args.salias_library.read_bytes()),
            },
            "core": {
                "path": str(args.core_library),
                "sha256": digest(args.core_library.read_bytes()),
            },
        },
    }
    (output.parent / "build-manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n"
    )
    print(json.dumps(manifest, indent=2))
    return manifest


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=None,
                        help="optional byte-identical v3 source copy; defaults to git tag")
    parser.add_argument("--output", type=Path, default=None, help="probe binary path")
    parser.add_argument("--generated-source", type=Path, default=None,
                        help="generated C++ path; defaults beside the binary")
    parser.add_argument("--salias-library", type=Path,
                        default=ROOT / "build/release/src/salias/libsalias.a")
    parser.add_argument("--core-library", type=Path,
                        default=ROOT / "build/release/src/core/libsalias_core.a")
    return parser.parse_args()


def main():
    args = parse_args()
    for path in (args.salias_library, args.core_library):
        if not Path(path).is_file():
            raise SystemExit(f"missing product library: {path}")
    build(args)


if __name__ == "__main__":
    main()
