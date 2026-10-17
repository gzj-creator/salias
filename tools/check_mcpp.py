#!/usr/bin/env python3
"""Build independent module/header consumers against the shared source package."""
import pathlib
import shutil
import subprocess
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[1]
SHARED = pathlib.Path("/opt/mcpp-deps/salias")
MANIFEST = '''[package]
name = "salias-mcpp-consumer"
version = "0.1.0"
standard = "c++23"

[toolchain]
default = "llvm@22.1.8"

[dependencies]
"gzj-creator.salias" = { path = "/opt/mcpp-deps/salias" }

[build]
sources = ["header_probe.cpp"]
include_dirs = ["/opt/mcpp-deps/salias/src", "/opt/mcpp-deps/salias/src/salias/include"]
cxxflags = ["-Wall", "-Wextra", "-Wpedantic", "-Werror"]

[targets.consumer]
kind = "bin"
main = "import.cpp"
'''


def main():
    if not (SHARED / "mcpp.toml").is_file() or SHARED.resolve() != ROOT:
        raise SystemExit(f"Prepare {SHARED} as this source package before checking consumers")
    with tempfile.TemporaryDirectory(prefix="salias-mcpp-check-") as directory:
        for name in ("first", "nested/second"):
            consumer = pathlib.Path(directory) / name
            consumer.mkdir(parents=True)
            (consumer / "mcpp.toml").write_text(MANIFEST)
            for source in ("import.cpp", "header_probe.cpp", "typed_schema.hpp"):
                shutil.copyfile(ROOT / "test/mcpp" / source, consumer / source)
            command = ["mcpp", "build", "--offline", "--release", "--jobs", "4"]
            subprocess.run(command, cwd=consumer, check=True)
            subprocess.run(command + ["--locked"], cwd=consumer, check=True)
            binaries = list((consumer / "target").glob("**/bin/consumer"))
            if len(binaries) != 1:
                raise RuntimeError(f"Expected one consumer executable, found {binaries}")
            subprocess.run([str(binaries[0])], cwd=consumer, check=True, timeout=15)
    print("Two independent shared-path consumers passed with offline/locked builds")


if __name__ == "__main__":
    main()
