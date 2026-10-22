#!/usr/bin/env python3
"""Build the zero-layout E1 diagnostic client from the frozen v3 benchmark.

The four seq_cst stores in the Fifo claim path are encoded as ``xchg``. E1
replaces only the opcode byte of each instruction with ``mov`` so every
instruction keeps its original address and length. The resulting binary is a
diagnostic artifact and must never be reported as a product build.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import struct
import subprocess


SOURCE_SHA256 = "3f8fc6957ce56483d7051dd6625f44fd8f755d57a66d04e1e5efa48e65b9c7da"
PATCHED_SHA256 = "5efae0b41ed42f9a38d69997ed8a1fafc69d78ae69eea948c79abbdec0f433b6"
PATCH_OFFSETS = (0x23259, 0x23264, 0x23283, 0x232C5)
OLD_OPCODE = 0x87
NEW_OPCODE = 0x89
REX_PREFIX = 0x41


class PatchError(RuntimeError):
    """Raised when the frozen client does not match the registered patch."""


def digest(path):
    """Return the SHA-256 digest of one file."""
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def executable_load_segments(data):
    """Return ``(file_offset, virtual_address, file_size)`` for executable LOADs."""
    if len(data) < 64 or data[:4] != b"\x7fELF":
        raise PatchError("source is not an ELF file")
    if data[4] != 2 or data[5] != 1:
        raise PatchError("source must be a little-endian ELF64 file")
    header = struct.unpack_from("<16sHHIQQQIHHHHHH", data, 0)
    program_offset = header[5]
    entry_size = header[9]
    entry_count = header[10]
    if entry_size < 56:
        raise PatchError("invalid ELF program-header size")
    segments = []
    for index in range(entry_count):
        offset = program_offset + index * entry_size
        if offset + 56 > len(data):
            raise PatchError("truncated ELF program header")
        p_type, p_flags, p_offset, p_vaddr, _, p_filesz, _, _ = struct.unpack_from(
            "<IIQQQQQQ", data, offset
        )
        if p_type == 1 and p_flags & 0x1:
            segments.append((p_offset, p_vaddr, p_filesz))
    if not segments:
        raise PatchError("source has no executable LOAD segment")
    return segments


def verify_source(data, expected_sha256=SOURCE_SHA256):
    """Verify the source identity, executable mapping and every patch site."""
    actual_sha256 = hashlib.sha256(data).hexdigest()
    if actual_sha256 != expected_sha256:
        raise PatchError(
            f"source SHA-256 is {actual_sha256}, expected {expected_sha256}"
        )
    segments = executable_load_segments(data)
    for patch_offset in PATCH_OFFSETS:
        owners = [
            segment for segment in segments
            if segment[0] <= patch_offset < segment[0] + segment[2]
        ]
        if len(owners) != 1:
            raise PatchError(f"patch offset {patch_offset:#x} is not in one executable LOAD")
        file_offset, virtual_address, _ = owners[0]
        if file_offset != virtual_address:
            raise PatchError(
                f"executable LOAD at {file_offset:#x} does not map file offset to vaddr"
            )
        if data[patch_offset] != OLD_OPCODE or data[patch_offset - 1] != REX_PREFIX:
            raise PatchError(
                f"unexpected bytes at {patch_offset:#x}: "
                f"{data[patch_offset - 1]:02x} {data[patch_offset]:02x}"
            )


def patch_bytes(data):
    """Return a copy of ``data`` with exactly four opcode bytes changed."""
    patched = bytearray(data)
    for patch_offset in PATCH_OFFSETS:
        patched[patch_offset] = NEW_OPCODE
    changed = [index for index, (before, after) in enumerate(zip(data, patched))
               if before != after]
    if changed != list(PATCH_OFFSETS):
        raise PatchError(f"patch changed unexpected byte offsets: {changed}")
    return bytes(patched)


def parse_disassembly(path):
    """Map instruction addresses to raw bytes and decoded text with objdump."""
    result = subprocess.run(
        ["objdump", "-d", str(path)], capture_output=True, text=True, check=False
    )
    if result.returncode:
        raise PatchError(f"objdump failed: {result.stderr.strip()}")
    instructions = {}
    pattern = re.compile(
        r"^\s*([0-9a-fA-F]+):\s+((?:[0-9a-fA-F]{2}\s)+)\s*(\S+)(?:\s+(.*))?$"
    )
    for line in result.stdout.splitlines():
        match = pattern.match(line)
        if not match:
            continue
        address = int(match.group(1), 16)
        raw = match.group(2).split()
        mnemonic = match.group(3)
        operands = (match.group(4) or "").strip()
        instructions[address] = (raw, mnemonic, operands)
    return instructions


def verify_disassembly(source, patched):
    """Require exactly four xchg-to-mov changes and no other instruction change."""
    before = parse_disassembly(source)
    after = parse_disassembly(patched)
    if before.keys() != after.keys():
        raise PatchError("disassembly instruction addresses differ")
    changed = [address for address in before if before[address] != after[address]]
    # PATCH_OFFSETS point at the opcode byte; objdump addresses the REX prefix.
    instruction_addresses = [offset - 1 for offset in PATCH_OFFSETS]
    if changed != instruction_addresses:
        raise PatchError(f"unexpected disassembly changes at {[hex(x) for x in changed]}")
    details = []
    for patch_offset, address in zip(PATCH_OFFSETS, instruction_addresses):
        old_raw, old_mnemonic, old_operands = before[address]
        new_raw, new_mnemonic, new_operands = after[address]
        if old_mnemonic != "xchg" or new_mnemonic != "mov":
            raise PatchError(
                f"instruction at {address:#x} is {old_mnemonic} -> {new_mnemonic}"
            )
        if old_operands != new_operands:
            raise PatchError(f"operands changed at {address:#x}")
        if len(old_raw) != len(new_raw):
            raise PatchError(f"instruction length changed at {address:#x}")
        details.append({
            "file_offset": f"0x{patch_offset:x}",
            "instruction_address": f"0x{address:x}",
            "before_bytes": " ".join(old_raw),
            "after_bytes": " ".join(new_raw),
            "before_instruction": f"{old_mnemonic} {old_operands}",
            "after_instruction": f"{new_mnemonic} {new_operands}",
        })
    return details


def verify_cmp(source, patched):
    """Use cmp to prove that exactly the registered four bytes differ."""
    result = subprocess.run(
        ["cmp", "-l", str(source), str(patched)], capture_output=True, text=True,
        check=False,
    )
    if result.returncode != 1:
        raise PatchError(f"cmp expected four differences, exited {result.returncode}")
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    offsets = []
    for line in lines:
        fields = line.split()
        if len(fields) != 3:
            raise PatchError(f"unexpected cmp output: {line}")
        offsets.append(int(fields[0], 10) - 1)
    if offsets != list(PATCH_OFFSETS):
        raise PatchError(f"cmp differences are at {[hex(x) for x in offsets]}")


def build(source, output_dir):
    """Create and fully verify the diagnostic client in ``output_dir``."""
    source = Path(source).resolve()
    output_dir = Path(output_dir).resolve()
    if not source.is_file():
        raise PatchError(f"source does not exist: {source}")
    if output_dir.exists() and any(output_dir.iterdir()):
        raise PatchError(f"output directory is not empty: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / "salias_ipc_compare.nofence-diag"
    manifest_path = output_dir / "patch-manifest.json"
    data = source.read_bytes()
    verify_source(data)
    patched = patch_bytes(data)
    target.write_bytes(patched)
    target.chmod(source.stat().st_mode & 0o777)
    try:
        patched_sha256 = digest(target)
        if patched_sha256 != PATCHED_SHA256:
            raise PatchError(
                f"patched SHA-256 is {patched_sha256}, expected {PATCHED_SHA256}"
            )
        verify_cmp(source, target)
        instructions = verify_disassembly(source, target)
        manifest = {
            "diagnostic_only": True,
            "source": str(source),
            "source_sha256": digest(source),
            "source_size": source.stat().st_size,
            "patched": str(target),
            "patched_sha256": patched_sha256,
            "patched_size": target.stat().st_size,
            "offset_basis": "ELF executable LOAD has p_offset == p_vaddr",
            "patch_count": len(PATCH_OFFSETS),
            "patches": instructions,
            "verification": {
                "source_sha256": True,
                "patched_sha256": True,
                "cmp_differences": len(PATCH_OFFSETS),
                "disassembly_changes": len(instructions),
            },
        }
        manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    except Exception:
        target.unlink(missing_ok=True)
        manifest_path.unlink(missing_ok=True)
        raise
    return target, manifest_path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True,
                        help="frozen v3 salias_ipc_compare client")
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    try:
        target, manifest = build(args.source, args.output_dir)
    except PatchError as error:
        raise SystemExit(f"patch failed: {error}")
    print(f"patched client: {target}")
    print(f"manifest: {manifest}")
    print(f"sha256: {digest(target)}")


if __name__ == "__main__":
    main()
