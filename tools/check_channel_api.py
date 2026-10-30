#!/usr/bin/env python3
"""Validate the frozen Channel API manifest, compatibility docs, and quick start."""

from __future__ import annotations

import json
import pathlib
import re
import sys


ROOT = pathlib.Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "doc/api/channel-api-manifest.json"
README = ROOT / "README.md"
API_DOC = ROOT / "doc/11-L7-api.md"
MIGRATION = ROOT / "doc/migration/channel-api-convergence.md"
UMBRELLA = ROOT / "src/salias/include/salias/salias.hpp"
MODULE = ROOT / "src/salias.cppm"
COMPAT_TEST = ROOT / "test/api/channel_alias_compat_test.cpp"

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
CODE_ROOTS = ("test", "bench", "example")


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


def validate_locations(symbols: list[dict]) -> None:
    for symbol in symbols:
        locations = [symbol["location"]]
        locations.extend(symbol.get("secondary_locations", []))
        for location in locations:
            path = ROOT / location["file"]
            if not path.is_file():
                fail(f"{symbol['name']} points to missing file {location['file']}")
            lines = read(path).splitlines()
            line_number = location["line"]
            if line_number < 1 or line_number > len(lines):
                fail(f"{symbol['name']} line {line_number} is outside {location['file']}")
            if location["declaration"] not in lines[line_number - 1]:
                fail(
                    f"{symbol['name']} declaration mismatch at "
                    f"{location['file']}:{line_number}: {lines[line_number - 1]!r}"
                )


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
        for member in symbol.get("members", []):
            if member.get("status") not in statuses:
                fail(f"{name}::{member.get('name')} has invalid status")
        by_name[name] = symbol

    if not any(symbol["status"] == "core" for symbol in symbols):
        fail("manifest has no core symbols")
    if not any(symbol["status"] == "compat" for symbol in symbols):
        fail("manifest has no compat symbols")
    if not any(symbol["status"] == "extras" for symbol in symbols):
        fail("manifest has no extras symbols")
    for symbol in symbols:
        if symbol["status"] == "compat" and symbol["replacement"].endswith("(self)"):
            fail(f"{symbol['name']} is compat but points only to itself")

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

    for symbol in symbols:
        base_name = symbol["name"].removeprefix("salias::").split("<", 1)[0]
        if symbol["exports"]["module"] and base_name not in module_exports:
            fail(f"module export is missing {base_name}")

    umbrella_text = read(UMBRELLA)
    included = set(re.findall(r'(?m)^#include "salias/([^"]+\.hpp)"', umbrella_text))
    required = {
        "channel.hpp",
        "config.hpp",
        "error.hpp",
        "fixed_channel.hpp",
        "latest_by_key_channel.hpp",
        "latest_channel.hpp",
        "message.hpp",
        "priority_channel.hpp",
        "publisher.hpp",
        "ringbuffer.hpp",
        "subscriber.hpp",
    }
    if included != required:
        fail(f"unexpected umbrella header set: {sorted(included ^ required)}")


def validate_compat_code(by_name: dict[str, dict]) -> None:
    channel_header = read(ROOT / "src/salias/include/salias/channel.hpp")
    for alias in ALIASES:
        symbol = by_name.get(f"salias::{alias}")
        if symbol is None or symbol["status"] != "compat":
            fail(f"{alias} must be listed as compat")
        if alias not in channel_header or "[[deprecated(" not in channel_header:
            fail(f"{alias} is not source-compatible and deprecated")

    compat_test = read(COMPAT_TEST)
    for alias in ALIASES:
        if alias not in compat_test:
            fail(f"compatibility test does not cover {alias}")

    alias_pattern = re.compile(r"\b(?:salias::)?(" + "|".join(ALIASES) + r")\b")
    for root_name in CODE_ROOTS:
        for path in (ROOT / root_name).rglob("*"):
            if not path.is_file() or path.suffix not in {".hpp", ".cpp", ".cc", ".cxx"}:
                continue
            if path == COMPAT_TEST:
                continue
            match = alias_pattern.search(read(path))
            if match:
                fail(f"non-compat code still uses {match.group(1)}: {path.relative_to(ROOT)}")


def validate_docs(by_name: dict[str, dict]) -> None:
    readme = read(README)
    start = readme.find("## 快速开始")
    end = readme.find("\n## ", start + 1)
    if start < 0 or end < 0:
        fail("README is missing a bounded 快速开始 section")
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
            fail(f"README quick start promotes non-core entry {forbidden}")

    api_doc = read(API_DOC)
    migration = read(MIGRATION)
    for alias in ALIASES:
        if alias not in api_doc or alias not in migration:
            fail(f"compatibility docs do not mention {alias}")
    direct_latest = re.compile(r"(?:salias::)?Channel<(?:salias::)?Mode::LatestMpmc(?=[,>])")
    if not direct_latest.search(api_doc) or not direct_latest.search(migration):
        fail("compatibility docs do not mark direct Channel<Mode::LatestMpmc>")
    if "5.0.0" not in migration or "deprecated" not in read(ROOT / "src/salias/include/salias/channel.hpp"):
        fail("migration policy does not state the major-version removal window")

    for symbol in by_name.values():
        if symbol["status"] != "compat":
            continue
        short_name = symbol["name"].removeprefix("salias::").split("<", 1)[0]
        if symbol["name"] == "salias::Channel<Mode::LatestMpmc>":
            continue
        if short_name not in api_doc:
            fail(f"API doc does not mark compat symbol {symbol['name']}")


def validate_versions(data: dict) -> None:
    cmake_text = read(ROOT / "CMakeLists.txt")
    match = re.search(r"project\(salias VERSION ([0-9]+\.[0-9]+\.[0-9]+)", cmake_text)
    if not match:
        fail("cannot read CMake project version")
    if match.group(1) != data["baseline_version"]:
        fail(f"manifest version {data['baseline_version']} != CMake {match.group(1)}")


def main() -> None:
    data = load_manifest()
    symbols, by_name = validate_symbols(data)
    validate_module_and_umbrella(data, symbols)
    validate_compat_code(by_name)
    validate_docs(by_name)
    validate_versions(data)
    counts = {status: sum(s["status"] == status for s in symbols) for status in data["statuses"]}
    print(
        "Channel API manifest OK: "
        f"{counts['core']} core, {counts['compat']} compat, "
        f"{counts['extras']} extras, {counts['removed']} removed"
    )


if __name__ == "__main__":
    main()
