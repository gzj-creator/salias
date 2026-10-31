#!/usr/bin/env python3
"""Validate the Channel API manifest, removed-entry contracts, and docs."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import re
import subprocess
import sys
from datetime import date


ROOT = pathlib.Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "doc/api/channel-api-manifest.json"
README = ROOT / "README.md"
API_DOC = ROOT / "doc/11-L7-api.md"
MIGRATION = ROOT / "doc/migration/channel-api-convergence.md"
UMBRELLA = ROOT / "src/salias/include/salias/salias.hpp"
MODULE = ROOT / "src/salias.cppm"
EXTRAS_MODULE = ROOT / "src/salias_extras.cppm"
INSTALL_TEST = ROOT / "cmake/install_consumer_test.cmake"
TEST_CMAKE = ROOT / "test/CMakeLists.txt"
REMOVED_TEST = ROOT / "test/api/removed_api_contract_test.cpp"
CORE_IMPORT_TEST = ROOT / "test/mcpp/core_import.cpp"
MCPP_IMPORT_TEST = ROOT / "test/mcpp/import.cpp"
FREEZE = ROOT / "doc/api/channel-api-freeze.json"
FREEZE_STATE = "v6_extras_removed_candidate"
FREEZE_RELEASE_TAG = "v6.0.0"

ALIASES = (
    "FifoMpscChannel",
    "FifoMpmcChannel",
    "OrderedMpscChannel",
    "OrderedMpmcChannel",
)
EXTRAS = (
    "LatestByKeyChannel",
    "LatestByKeyPublisher",
    "LatestByKeySubscriber",
    "LatestByKeyMessage",
    "PriorityPublisher",
    "PrioritySubscriber",
    "PriorityMessage",
    "PriorityWaitPolicy",
    "Ringbuffer",
)
REMOVED_PUBLIC_HEADERS = (
    "extras/extras.hpp",
    "extras/latest_by_key_channel.hpp",
    "extras/priority_channel.hpp",
    "extras/ringbuffer.hpp",
    "latest_by_key_channel.hpp",
    "priority_channel.hpp",
    "ringbuffer.hpp",
)
REMOVED_INTERNAL_HEADERS = ("detail/latest_by_key_storage.hpp",)
REMOVED_HEADERS = (*REMOVED_PUBLIC_HEADERS, *REMOVED_INTERNAL_HEADERS)
CANONICAL_EXTRAS_NAMES = tuple(f"salias::extras::{name}" for name in EXTRAS)
TOP_LEVEL_EXTRAS_NAMES = tuple(f"salias::{name}" for name in EXTRAS)
REMOVED_ALIAS_NAMES = tuple(f"salias::{name}" for name in ALIASES) + (
    *CANONICAL_EXTRAS_NAMES,
    *TOP_LEVEL_EXTRAS_NAMES,
)
REMOVED_TYPE_EXPRESSIONS = {
    "salias::FifoMpscChannel": "salias::FifoMpscChannel",
    "salias::FifoMpmcChannel": "salias::FifoMpmcChannel",
    "salias::OrderedMpscChannel": "salias::OrderedMpscChannel",
    "salias::OrderedMpmcChannel": "salias::OrderedMpmcChannel",
    "salias::extras::LatestByKeyChannel": "salias::extras::LatestByKeyChannel<std::uint64_t, std::uint64_t, 4>",
    "salias::extras::LatestByKeyPublisher": "salias::extras::LatestByKeyPublisher<std::uint64_t, std::uint64_t>",
    "salias::extras::LatestByKeySubscriber": "salias::extras::LatestByKeySubscriber<std::uint64_t, std::uint64_t, 4>",
    "salias::extras::LatestByKeyMessage": "salias::extras::LatestByKeyMessage<std::uint64_t, std::uint64_t>",
    "salias::extras::PriorityPublisher": "salias::extras::PriorityPublisher<salias::Mode::FifoMpsc, salias::Mode::FifoMpsc>",
    "salias::extras::PrioritySubscriber": "salias::extras::PrioritySubscriber<salias::Mode::FifoMpsc, salias::Mode::FifoMpsc>",
    "salias::extras::PriorityMessage": "salias::extras::PriorityMessage",
    "salias::extras::PriorityWaitPolicy": "salias::extras::PriorityWaitPolicy",
    "salias::extras::Ringbuffer": "salias::extras::Ringbuffer<int, 4>",
    "salias::LatestByKeyChannel": "salias::LatestByKeyChannel<std::uint64_t, std::uint64_t, 4>",
    "salias::LatestByKeyPublisher": "salias::LatestByKeyPublisher<std::uint64_t, std::uint64_t>",
    "salias::LatestByKeySubscriber": "salias::LatestByKeySubscriber<std::uint64_t, std::uint64_t, 4>",
    "salias::LatestByKeyMessage": "salias::LatestByKeyMessage<std::uint64_t, std::uint64_t>",
    "salias::PriorityPublisher": "salias::PriorityPublisher<salias::Mode::FifoMpsc, salias::Mode::FifoMpsc>",
    "salias::PrioritySubscriber": "salias::PrioritySubscriber<salias::Mode::FifoMpsc, salias::Mode::FifoMpsc>",
    "salias::PriorityMessage": "salias::PriorityMessage",
    "salias::PriorityWaitPolicy": "salias::PriorityWaitPolicy",
    "salias::Ringbuffer": "salias::Ringbuffer<int, 4>",
}
NEGATIVE_CONTRACT_TESTS = {REMOVED_TEST}
CODE_ROOTS = ("test", "bench", "example")
EXPECTED_COUNTS = {"core": 30, "compat": 0, "extras": 0, "removed": 23}
EXPECTED_MODULE_COUNTS = {"core": 29, "extras": 0}
FREEZE_PACKAGING_FILES = (
    "CMakeLists.txt",
    "cmake/saliasConfig.cmake.in",
    "mcpp.toml",
    "src/core/CMakeLists.txt",
    "src/salias/CMakeLists.txt",
)
README_REMOVED_HEADING = "## \u5df2\u79fb\u9664\u5feb\u6377\u5165\u53e3"
DIRECT_LATEST_RE = re.compile(
    r"(?:salias::)?Channel<(?:salias::)?Mode::LatestMpmc(?=[,>])"
)


def fail(message: str) -> None:
    print(f"channel API check failed: {message}", file=sys.stderr)
    raise SystemExit(1)


def read(path: pathlib.Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError as error:
        fail(f"cannot read {path.relative_to(ROOT)}: {error}")


def load_manifest() -> dict:
    try:
        data = json.loads(read(MANIFEST))
    except json.JSONDecodeError as error:
        fail(f"invalid manifest JSON: {error}")
    if data.get("schema_version") != 1:
        fail("manifest schema_version must be 1")
    return data


def validate_location(location: dict, label: str, require_file: bool) -> None:
    for field in ("file", "line", "declaration"):
        if field not in location:
            fail(f"{label} is missing {field}")
    if not isinstance(location["line"], int) or location["line"] < 1:
        fail(f"{label} has an invalid line number")
    if not isinstance(location["declaration"], str) or not location["declaration"]:
        fail(f"{label} has an invalid declaration")
    if not require_file:
        return
    path = ROOT / location["file"]
    if not path.is_file():
        fail(f"{label} points to missing file {location['file']}")
    lines = read(path).splitlines()
    line_number = location["line"]
    if line_number > len(lines):
        fail(f"{label} line {line_number} is outside {location['file']}")
    if location["declaration"] not in lines[line_number - 1]:
        fail(
            f"{label} declaration mismatch at "
            f"{location['file']}:{line_number}: {lines[line_number - 1]!r}"
        )


def validate_locations(symbols: list[dict]) -> None:
    for symbol in symbols:
        name = symbol["name"]
        location = symbol.get("location")
        previous_location = symbol.get("previous_location")
        if location is not None:
            validate_location(location, f"{name} location", require_file=True)
        if previous_location is not None:
            validate_location(previous_location, f"{name} previous_location", require_file=False)
        if symbol["status"] != "removed" and location is None:
            fail(f"{name} must have a current location")
        if name in REMOVED_ALIAS_NAMES and location is not None:
            fail(f"{name} is removed but still declares a current location")
        if name in REMOVED_ALIAS_NAMES and previous_location is None:
            fail(f"{name} is removed but has no previous_location")


def validate_symbols(data: dict) -> tuple[list[dict], dict[str, dict]]:
    symbols = data.get("symbols", [])
    statuses = set(data.get("statuses", {}))
    if statuses != {"core", "compat", "extras", "removed"}:
        fail("status definitions must contain exactly core, compat, extras, removed")

    by_name: dict[str, dict] = {}
    for symbol in symbols:
        name = symbol.get("name")
        if not name or name in by_name:
            fail(f"missing or duplicate symbol name: {name!r}")
        if symbol.get("status") not in statuses:
            fail(f"{name} has invalid status {symbol.get('status')!r}")
        if not symbol.get("replacement"):
            fail(f"{name} is missing a replacement/canonical entry")
        exports = symbol.get("exports", {})
        for field in ("header", "umbrella", "module", "cmake", "mcpp"):
            if not isinstance(exports.get(field), bool):
                fail(f"{name} has invalid exports.{field}")
        if symbol["status"] == "removed" and any(exports.values()):
            fail(f"{name} is removed but still declares an exported surface")
        for member in symbol.get("members", []):
            if member.get("status") not in statuses:
                fail(f"{name}::{member.get('name')} has invalid status")
        by_name[name] = symbol

    counts = {status: sum(s["status"] == status for s in symbols) for status in statuses}
    if counts != EXPECTED_COUNTS:
        fail(f"manifest counts {counts} != expected {EXPECTED_COUNTS}")
    if by_name.get("salias::Channel<Mode::LatestMpmc>", {}).get("status") != "removed":
        fail("direct Channel<Mode::LatestMpmc> creation must remain removed")
    if data.get("current_api_state") != FREEZE_STATE:
        fail(f"manifest current_api_state must be {FREEZE_STATE}")
    removal_metadata = data.get("removal_metadata", {})
    if removal_metadata.get("state") != "extras_removed_candidate":
        fail("removal metadata must record the v6 removal candidate")
    if removal_metadata.get("release_tag") != FREEZE_RELEASE_TAG:
        fail(f"removal metadata release tag must be {FREEZE_RELEASE_TAG}")
    if removal_metadata.get("wire_protocol_changed") is not False:
        fail("removal metadata must state that the wire protocol did not change")
    if removal_metadata.get("compatibility_surface_retained") is not False:
        fail("removal metadata must state that no compatibility surface is retained")

    for name in (*CANONICAL_EXTRAS_NAMES, *TOP_LEVEL_EXTRAS_NAMES):
        symbol = by_name.get(name)
        if symbol is None or symbol["status"] != "removed":
            fail(f"extras symbol {name} must be listed as removed")
        if not symbol["replacement"].startswith("None; removed permanently in v6.0.0"):
            fail(f"extras symbol {name} must not advertise a replacement")

    removed_names = {symbol["name"] for symbol in symbols if symbol["status"] == "removed"}
    if set(data.get("removed_symbols", [])) != removed_names:
        fail("removed_symbols must match all symbols with removed status")

    validate_locations(symbols)
    return symbols, by_name


def validate_module_and_umbrella(data: dict, symbols: list[dict]) -> None:
    module_text = read(MODULE)
    module_exports = set(re.findall(r"using ::salias::([A-Za-z_][A-Za-z0-9_]*);", module_text))
    declared_exports = set(data.get("module_exports", []))
    if module_exports != declared_exports:
        fail(
            "module_exports do not match src/salias.cppm: "
            f"manifest-only={sorted(declared_exports - module_exports)}, "
            f"module-only={sorted(module_exports - declared_exports)}"
        )
    if len(module_exports) != EXPECTED_MODULE_COUNTS["core"]:
        fail(f"core module exports {len(module_exports)} symbols, expected 29")
    if "import salias.extras;" in module_text or "export import salias.extras;" in module_text:
        fail("core module still imports salias.extras")
    if EXTRAS_MODULE.exists():
        fail("removed extras module interface src/salias_extras.cppm still exists")
    if data.get("extras_module_exports") != []:
        fail("extras_module_exports must be empty")
    if len(data.get("extras_module_exports", [])) != EXPECTED_MODULE_COUNTS["extras"]:
        fail("extras module export count must be zero")

    for symbol in symbols:
        qualified_name = symbol["name"].removeprefix("salias::")
        base_name = qualified_name.rsplit("::", 1)[-1].split("<", 1)[0]
        if not symbol["exports"]["module"]:
            continue
        if base_name not in module_exports:
            fail(f"salias module export is missing {base_name}")

    umbrella_text = read(UMBRELLA)
    included = set(re.findall(r'(?m)^#include "salias/([^"]+\.hpp)"', umbrella_text))
    required = {
        "channel.hpp",
        "config.hpp",
        "error.hpp",
        "fixed_channel.hpp",
        "latest_channel.hpp",
        "message.hpp",
        "publisher.hpp",
        "subscriber.hpp",
    }
    if included != required:
        fail(f"unexpected umbrella header set: {sorted(included ^ required)}")


def require_absent(text: str, pattern: str, label: str) -> None:
    if re.search(pattern, text):
        fail(label)


def validate_extras_boundaries(data: dict, by_name: dict[str, dict]) -> None:
    for header in REMOVED_HEADERS:
        if (ROOT / "src/salias/include/salias" / header).exists():
            fail(f"removed extras header still exists: salias/{header}")

    if data.get("install_surfaces", {}).get("install_components") != ["core"]:
        fail("install components must contain only core")
    if data.get("install_surfaces", {}).get("cmake_targets") != [
        "salias::salias",
        "salias::salias_core",
    ]:
        fail("CMake targets must contain only the two core targets")

    boundary_files = (
        "CMakeLists.txt",
        "cmake/saliasConfig.cmake.in",
        "mcpp.toml",
        "src/core/CMakeLists.txt",
        "src/salias/CMakeLists.txt",
        "src/salias.cppm",
        "test/CMakeLists.txt",
        "bench/CMakeLists.txt",
        "example/CMakeLists.txt",
    )
    forbidden_boundaries = (
        r"\bsalias_extras\b",
        r"\bsaliasExtras\b",
        r"\bCOMPONENT\s+extras\b",
        r"\bimport\s+salias\.extras\b",
        r"salias/extras/",
    )
    for relative in boundary_files:
        text = read(ROOT / relative)
        for pattern in forbidden_boundaries:
            require_absent(text, pattern, f"{relative} still references removed extras boundary")

    install_test = read(INSTALL_TEST)
    required_install_checks = (
        "include/salias/extras",
        "include/salias/detail/latest_by_key_storage.hpp",
        "libsalias_extras.a",
        "saliasExtrasTargets.cmake",
        "removed extras artifacts were unexpectedly installed",
        "LatestByKeyStorage",
        "salias::extras",
        'salias_install_component(core "core-only")',
    )
    for snippet in required_install_checks:
        if snippet not in install_test:
            fail(f"install consumer test is missing removed extras assertion {snippet!r}")

    for name in CANONICAL_EXTRAS_NAMES:
        symbol = by_name[name]
        if any(symbol["exports"].values()):
            fail(f"removed canonical extras symbol {name} still declares an export")


def validate_removed_code(by_name: dict[str, dict]) -> None:
    channel_header = read(ROOT / "src/salias/include/salias/channel.hpp")
    module_text = read(MODULE)
    for alias in ALIASES:
        symbol = by_name.get(f"salias::{alias}")
        if symbol is None or symbol["status"] != "removed":
            fail(f"{alias} must be listed as removed")
        require_absent(
            channel_header,
            rf"\busing\s+{re.escape(alias)}\b",
            f"{alias} still has a channel.hpp declaration",
        )
        require_absent(module_text, rf"\b{re.escape(alias)}\b", f"salias module still exports {alias}")

    removed_test = read(REMOVED_TEST)
    for alias in ALIASES:
        symbol = by_name.get(f"salias::{alias}")
        if symbol is None or symbol["status"] != "removed":
            fail(f"top-level legacy name {alias} must be listed as removed")
    for name in (*CANONICAL_EXTRAS_NAMES, *TOP_LEVEL_EXTRAS_NAMES):
        symbol = by_name.get(name)
        if symbol is None or symbol["status"] != "removed":
            fail(f"extras legacy name {name} must be listed as removed")
    for header in REMOVED_HEADERS:
        include = f"#if __has_include(<salias/{header}>)"
        if include not in removed_test:
            fail(f"removed contract test does not lock salias/{header}")

    test_cmake = read(TEST_CMAKE)
    if "api/removed_api_contract_test.cpp" not in test_cmake:
        fail("test/CMakeLists.txt does not build the removed API contract test")
    for stale_test in ("channel_alias_compat_test.cpp", "extras_compat_test.cpp"):
        if stale_test in test_cmake:
            fail(f"test/CMakeLists.txt still builds removed compat test {stale_test}")

    core_import = read(CORE_IMPORT_TEST)
    mcpp_import = read(MCPP_IMPORT_TEST)
    if "import salias;" not in core_import:
        fail("core module import test does not import salias")
    if "import salias;" not in mcpp_import:
        fail("mcpp module/header test does not import salias")
    for path in (CORE_IMPORT_TEST, MCPP_IMPORT_TEST):
        require_absent(read(path), r"\bimport\s+salias\.extras\b", f"{path.name} imports removed salias.extras")

    alias_pattern = re.compile(r"\bsalias::(" + "|".join((*ALIASES, *EXTRAS)) + r")\b")
    extras_namespace_pattern = re.compile(r"\bsalias::extras\b")
    header_pattern = re.compile(
        r"<salias/(" + "|".join(re.escape(header) for header in REMOVED_HEADERS) + r")>"
    )
    extras_module_pattern = re.compile(r"\bimport\s+salias\.extras\b")
    for root_name in CODE_ROOTS:
        for path in (ROOT / root_name).rglob("*"):
            if not path.is_file() or path.suffix not in {".hpp", ".cpp", ".cc", ".cxx"}:
                continue
            if path in NEGATIVE_CONTRACT_TESTS:
                continue
            text = read(path)
            match = alias_pattern.search(text)
            if match:
                fail(f"non-contract code still uses removed API {match.group(1)}: {path.relative_to(ROOT)}")
            if extras_namespace_pattern.search(text):
                fail(f"non-contract code still uses salias::extras: {path.relative_to(ROOT)}")
            header_match = header_pattern.search(text)
            if header_match:
                fail(f"non-contract code still includes removed header {header_match.group(1)}: {path.relative_to(ROOT)}")
            if extras_module_pattern.search(text):
                fail(f"non-contract code still imports salias.extras: {path.relative_to(ROOT)}")


def compile_must_fail(compiler: str, source: str, expected_token: str, label: str) -> None:
    command = [
        compiler,
        "-std=c++23",
        "-fsyntax-only",
        f"-I{ROOT / 'src/salias/include'}",
        f"-I{ROOT / 'src'}",
        "-x",
        "c++",
        "-",
    ]
    try:
        result = subprocess.run(
            command,
            input=source,
            text=True,
            capture_output=True,
            cwd=ROOT,
            check=False,
        )
    except OSError as error:
        fail(f"cannot run compiler {compiler!r}: {error}")
    if result.returncode == 0:
        fail(f"removed API unexpectedly compiles: {label}")
    diagnostic = result.stdout + result.stderr
    if expected_token not in diagnostic:
        fail(f"negative compile for {label} failed for an unexpected reason: {diagnostic.strip()}")


def validate_removed_compile_contract(compiler: str) -> None:
    preamble = "#include <cstdint>\n#include <salias/salias.hpp>\n"
    for label, expression in REMOVED_TYPE_EXPRESSIONS.items():
        source = preamble + f"using Removed = {expression};\nint main() {{ return 0; }}\n"
        expected_token = (
            "extras" if label.startswith("salias::extras::") else label.rsplit("::", 1)[-1].split("<", 1)[0]
        )
        compile_must_fail(compiler, source, expected_token, label)
    for header in REMOVED_HEADERS:
        source = f"#include <salias/{header}>\nint main() {{ return 0; }}\n"
        compile_must_fail(compiler, source, header, f"salias/{header}")


def validate_docs(by_name: dict[str, dict]) -> None:
    readme = read(README)
    start = readme.find("## \u5feb\u901f\u5f00\u59cb")
    end = readme.find("\n## ", start + 1)
    if start < 0 or end < 0:
        fail("README is missing a bounded quick-start section")
    quick_start = readme[start:end]
    required_snippets = (
        "salias::Channel<salias::Mode::FifoMpsc, QuoteSchema>",
        "salias::FixedChannel<salias::FixedTopology::Spsc, QuoteSchema, 1024>",
        "salias::LatestMpmcChannel<1u << 20, 90, QuoteSchema>",
    )
    for snippet in required_snippets:
        if snippet not in quick_start:
            fail(f"README quick start is missing {snippet}")
    for forbidden in (*ALIASES, *EXTRAS, "Channel<salias::Mode::LatestMpmc>"):
        if forbidden in quick_start:
            fail(f"README quick start promotes removed or non-core entry {forbidden}")

    removed_start = readme.find(README_REMOVED_HEADING)
    removed_end = readme.find("\n## ", removed_start + 1) if removed_start >= 0 else -1
    if removed_start < 0 or removed_end < 0:
        fail("README is missing a bounded removed-entry section")
    removed_section = readme[removed_start:removed_end]
    for alias in ALIASES:
        if alias not in removed_section:
            fail(f"README removed-entry section does not map {alias}")
    if "2026-10-30" not in removed_section or "5.0.0" not in removed_section:
        fail("README removed-entry section must record owner approval date and the 5.0.0 release")
    if "2026-10-31" not in removed_section or "6.0.0" not in removed_section:
        fail("README removed-entry section must record the v6 removal")
    for name in (*CANONICAL_EXTRAS_NAMES, *TOP_LEVEL_EXTRAS_NAMES):
        if name not in removed_section:
            fail(f"README removed-entry section does not mark {name} as removed")
    for header in REMOVED_PUBLIC_HEADERS:
        if f"salias/{header}" not in removed_section:
            fail(f"README removed-entry section does not mark salias/{header} as removed")
    if "import salias.extras;" not in removed_section:
        fail("README removed-entry section does not mark import salias.extras as removed")
    if "compat" in removed_section.lower() or "still retained" in removed_section.lower():
        fail("README removed-entry section still describes compatibility exports")

    api_doc = read(API_DOC)
    migration = read(MIGRATION)
    if "removed" not in api_doc.lower() or "removed" not in migration.lower():
        fail("API and migration docs must explicitly mark removed entries")
    for name in (
        *(f"salias::{alias}" for alias in ALIASES),
        *CANONICAL_EXTRAS_NAMES,
        *TOP_LEVEL_EXTRAS_NAMES,
    ):
        if name not in api_doc:
            fail(f"API doc does not mark removed symbol {name}")
        if name not in migration:
            fail(f"migration doc does not map removed symbol {name}")
    for header in REMOVED_PUBLIC_HEADERS:
        if f"salias/{header}" not in api_doc or f"salias/{header}" not in migration:
            fail(f"docs do not map removed include salias/{header}")
    if not DIRECT_LATEST_RE.search(api_doc) or not DIRECT_LATEST_RE.search(migration):
        fail("docs do not mark direct Channel<Mode::LatestMpmc> as removed")
    for required in ("v6.0.0", "salias::extras", "import salias.extras;", "没有替代入口"):
        if required not in api_doc:
            fail(f"API doc is missing v6 removal detail: {required}")
    for required in ("2026-10-31", "v6.0.0", "wire protocol", "没有替代 API"):
        if required not in migration:
            fail(f"migration doc is missing required removal/release detail: {required}")
    if "仍作为 `4.x` 兼容入口保留" in readme or "本版本不删除旧入口" in readme:
        fail("README still claims old entries remain source-compatible")

    for symbol in by_name.values():
        if symbol["status"] == "compat":
            fail(f"manifest still contains compat symbol {symbol['name']}")


def validate_versions(data: dict) -> None:
    cmake_text = read(ROOT / "CMakeLists.txt")
    match = re.search(r"project\(salias VERSION ([0-9]+\.[0-9]+\.[0-9]+)", cmake_text)
    if not match:
        fail("cannot read CMake project version")
    if match.group(1) != data["baseline_version"]:
        fail(f"manifest version {data['baseline_version']} != CMake {match.group(1)}")


def sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as error:
        fail(f"cannot hash {path.relative_to(ROOT)}: {error}")
    return digest.hexdigest()


def freeze_paths() -> list[pathlib.Path]:
    headers = sorted((ROOT / "src/salias/include/salias").rglob("*.hpp"))
    headers += sorted((ROOT / "src/core").rglob("*.hpp"))
    files = [
        MANIFEST,
        MODULE,
        *(ROOT / relative for relative in FREEZE_PACKAGING_FILES),
        *headers,
    ]
    unique = {path.resolve() for path in files}
    return sorted(unique, key=lambda path: path.relative_to(ROOT).as_posix())


def freeze_document(data: dict, symbols: list[dict]) -> dict:
    files = [
        {
            "path": path.relative_to(ROOT).as_posix(),
            "sha256": sha256_file(path),
        }
        for path in freeze_paths()
    ]
    aggregate_input = "".join(f"{entry['sha256']}  {entry['path']}\n" for entry in files)
    counts = {status: sum(s["status"] == status for s in symbols) for status in data["statuses"]}
    return {
        "schema_version": 1,
        "api": "salias-channel",
        "state": FREEZE_STATE,
        "release_tag": FREEZE_RELEASE_TAG,
        "basis_commit": current_revision(),
        "freeze_date": date.today().isoformat(),
        "api_counts": counts,
        "module_export_counts": {
            "core": len(data["module_exports"]),
            "extras": len(data["extras_module_exports"]),
        },
        "wire_protocol_version": current_wire_version(),
        "files": files,
        "aggregate_sha256": hashlib.sha256(aggregate_input.encode("ascii")).hexdigest(),
    }


def current_wire_version() -> int:
    channel_source = read(ROOT / "src/salias/src/channel.cpp")
    match = re.search(r"kNamedVersion\s*=\s*([0-9]+)", channel_source)
    if not match:
        fail("cannot read kNamedVersion from channel.cpp")
    return int(match.group(1))


def current_revision() -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        fail(f"cannot read git revision: {result.stderr.strip()}")
    return result.stdout.strip()


def validate_freeze(data: dict, symbols: list[dict], write: bool) -> dict:
    expected_paths = [path.relative_to(ROOT).as_posix() for path in freeze_paths()]
    if write:
        document = freeze_document(data, symbols)
        FREEZE.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    else:
        try:
            document = json.loads(read(FREEZE))
        except json.JSONDecodeError as error:
            fail(f"invalid API freeze JSON: {error}")

    if document.get("schema_version") != 1 or document.get("api") != "salias-channel":
        fail("API freeze metadata is invalid")
    if document.get("state") != FREEZE_STATE:
        fail(f"API freeze state must be {FREEZE_STATE}")
    if document.get("release_tag") != FREEZE_RELEASE_TAG:
        fail(f"API freeze release tag must be {FREEZE_RELEASE_TAG}")
    if document.get("wire_protocol_version") != current_wire_version():
        fail("API freeze wire protocol version does not match channel.cpp")

    counts = {status: sum(s["status"] == status for s in symbols) for status in data["statuses"]}
    if document.get("api_counts") != counts:
        fail(f"API freeze counts {document.get('api_counts')} != manifest {counts}")
    module_counts = {
        "core": len(data["module_exports"]),
        "extras": len(data["extras_module_exports"]),
    }
    if document.get("module_export_counts") != module_counts:
        fail("API freeze module export counts do not match the manifest")

    entries = document.get("files", [])
    paths = [entry.get("path") for entry in entries]
    if paths != expected_paths:
        missing = sorted(set(expected_paths) - set(paths))
        extra = sorted(set(paths) - set(expected_paths))
        fail(f"API freeze file set differs: missing={missing}, extra={extra}, order_or_duplicate={paths != expected_paths}")
    for entry in entries:
        path = ROOT / entry["path"]
        if not path.is_file():
            fail(f"API freeze points to missing file {entry['path']}")
        if entry.get("sha256") != sha256_file(path):
            fail(f"API freeze hash mismatch for {entry['path']}")
    aggregate_input = "".join(f"{entry['sha256']}  {entry['path']}\n" for entry in entries)
    aggregate = hashlib.sha256(aggregate_input.encode("ascii")).hexdigest()
    if document.get("aggregate_sha256") != aggregate:
        fail("API freeze aggregate SHA-256 is invalid")
    return document


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--compiler",
        default=os.environ.get("CXX", "c++"),
        help="C++ compiler used for removed-header and removed-symbol compile checks",
    )
    parser.add_argument(
        "--write-freeze",
        action="store_true",
        help="Regenerate the reviewed API freeze candidate from the current tree",
    )
    args = parser.parse_args()

    data = load_manifest()
    symbols, by_name = validate_symbols(data)
    validate_module_and_umbrella(data, symbols)
    validate_extras_boundaries(data, by_name)
    validate_removed_code(by_name)
    validate_removed_compile_contract(args.compiler)
    validate_docs(by_name)
    validate_versions(data)
    freeze = validate_freeze(data, symbols, args.write_freeze)
    counts = {status: sum(s["status"] == status for s in symbols) for status in data["statuses"]}
    print(
        "Channel API manifest OK: "
        f"{counts['core']} core, {counts['compat']} compat, "
        f"{counts['extras']} extras, {counts['removed']} removed; "
        f"core module {len(data['module_exports'])}, "
        f"extras module {len(data['extras_module_exports'])}; "
        f"negative compile checks {len(REMOVED_TYPE_EXPRESSIONS) + len(REMOVED_HEADERS)}; "
        f"freeze {len(freeze['files'])} files, aggregate {freeze['aggregate_sha256']}"
    )


if __name__ == "__main__":
    main()
