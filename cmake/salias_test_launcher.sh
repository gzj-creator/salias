#!/usr/bin/env bash

# A single CTest launcher for native tests, examples, and the installed
# consumer.  It performs cheap, deterministic capability checks before the
# one and only execution of the test process.  In particular, a sanitizer
# runtime failure is reported as an environment failure by the test output;
# the launcher never retries or silently marks the test passed.
set -u
tsan_runner=()

if [[ $# -eq 0 ]]; then
  echo "salias test launcher: no command supplied" >&2
  exit 125
fi

if [[ "${1:-}" == "--tsan-probe" ]]; then
  if [[ $# -lt 4 || "${3:-}" != "--" ]]; then
    echo "salias test launcher: expected --tsan-probe PATH -- COMMAND [ARG ...]" >&2
    exit 125
  fi
  probe_path="$2"
  shift 3
  if [[ ! -x "$probe_path" ]]; then
    echo "salias test environment failure: ThreadSanitizer probe is unavailable: $probe_path" >&2
    exit 125
  fi
  if [[ "$(uname -s 2>/dev/null || true)" == "Linux" ]]; then
    machine="$(uname -m 2>/dev/null || true)"
    setarch_path="$(command -v setarch 2>/dev/null || true)"
    if [[ -n "$setarch_path" ]]; then
      "$setarch_path" "$machine" -R "$probe_path" >/dev/null 2>&1
      probe_status=$?
      if [[ $probe_status -eq 0 ]]; then
        tsan_runner=("$setarch_path" "$machine" -R)
      fi
    fi
    if [[ ${#tsan_runner[@]} -eq 0 ]]; then
      "$probe_path" >/dev/null 2>&1
      probe_status=$?
      if [[ $probe_status -ne 0 ]]; then
        echo "salias test environment failure: ThreadSanitizer runtime cannot start in this environment (probe exit $probe_status)" >&2
        exit 125
      fi
    fi
  else
    "$probe_path" >/dev/null 2>&1
    probe_status=$?
    if [[ $probe_status -ne 0 ]]; then
      echo "salias test environment failure: ThreadSanitizer runtime cannot start in this environment (probe exit $probe_status)" >&2
      exit 125
    fi
  fi
fi

command_path="$1"
if [[ "$command_path" != */* ]]; then
  command_path="$(command -v -- "$command_path" 2>/dev/null || true)"
fi
if [[ -z "$command_path" || ! -e "$command_path" ]]; then
  echo "salias test environment failure: executable '$1' is unavailable" >&2
  exit 125
fi
if [[ ! -x "$command_path" ]]; then
  echo "salias test environment failure: executable '$command_path' is not executable" >&2
  exit 125
fi

host_system="$(uname -s 2>/dev/null || true)"
if [[ "$host_system" == "Linux" ]]; then
  # Ensure the process can be inspected before entering sanitizer code.  This
  # catches restricted containers deterministically and avoids setarch (which
  # is unavailable on many Linux images and is not a macOS primitive).
  if [[ ! -r /proc/self/maps ]]; then
    echo "salias test environment failure: /proc/self/maps is unreadable" >&2
    exit 125
  fi
elif [[ "$host_system" == "Darwin" ]]; then
  : # Native execution is intentional on macOS.
else
  echo "salias test environment failure: unsupported host '$host_system'" >&2
  exit 125
fi

if [[ ${#tsan_runner[@]} -ne 0 ]]; then
  exec "${tsan_runner[@]}" "$@"
fi
exec "$@"
