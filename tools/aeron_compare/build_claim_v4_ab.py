#!/usr/bin/env python3
"""Build a same-client v3.0.0 versus working-tree v4 claim A/B pair.

Both executables compile the current benchmark client. The baseline executable
uses v3.0.0 product headers and the pinned v3.0.0 implementation, while the
candidate uses the working-tree product. Only the extract call differs, so a
throughput or latency difference is not caused by a different benchmark loop.
"""

import argparse
import hashlib
import json
from pathlib import Path
import shlex
import subprocess


ROOT = Path(__file__).resolve().parents[2]
BASELINE_TAG = "v3.0.0"
CLIENT_PATH = "tools/aeron_compare/salias_ipc_compare.cpp"
BASELINE_CLIENT_SHA256 = (
    "6a23a77598c6e980f7b6625eb2471cc42a26917469dc77fc51ab9d5b767bab1e"
)
PROTOCOL = "p3-claim-api-v4-product-ab-v1"
CLIENT_ANCHORS = {
    "v3": {"claim_commit": 2, "publisher_commit": 0},
    "v4": {"claim_commit": 0, "publisher_commit": 2},
}


def digest(data):
    """Return the lowercase SHA-256 digest of bytes."""
    return hashlib.sha256(data).hexdigest()


def git_bytes(ref, path):
    """Read one exact Git object without depending on the current worktree."""
    result = subprocess.run(
        ["git", "show", f"{ref}:{path}"],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode:
        raise RuntimeError(result.stderr.decode("utf-8", "replace").strip())
    return result.stdout


def replace_once(text, needle, replacement, label):
    """Replace an exact anchor and fail when the pinned source shape changes."""
    count = text.count(needle)
    if count != 1:
        raise ValueError(f"expected one {label} anchor, found {count}")
    return text.replace(needle, replacement, 1)


def client_api_counts(text):
    """Count the two claim submission forms in one benchmark client."""
    return {
        "claim_commit": text.count("claim.commit(") + text.count("claim->commit("),
        "publisher_commit": text.count("publisher.commit("),
    }


def verify_client_api(text, api):
    """Reject a client that does not contain exactly the expected submission sites."""
    if api not in CLIENT_ANCHORS:
        raise ValueError(f"unknown API shape: {api}")
    actual = client_api_counts(text)
    expected = CLIENT_ANCHORS[api]
    if actual != expected:
        raise ValueError(
            f"{api} client submission sites are {actual}, expected {expected}"
        )
    return actual


def write_extracted_product(directory, header, implementation):
    """Materialize v3.0.0 public headers and implementation for a baseline build."""
    include = directory / "include"
    for path, data in header.items():
        # Keep v3 public headers under the same names the sources include.
        target = include / path.split("src/salias/include/", 1)[1]
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
    source_dir = directory / "src"
    source_dir.mkdir(parents=True, exist_ok=True)
    for path, data in implementation.items():
        target = source_dir / path.split("src/", 1)[1]
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)


def baseline_sources():
    """Return v3.0.0 public headers and non-claim implementation members."""
    header_paths = subprocess.run(
        ["git", "ls-tree", "-r", "--name-only", BASELINE_TAG, "src/salias/include"],
        cwd=ROOT, stdout=subprocess.PIPE, check=True, text=True,
    ).stdout.splitlines()
    implementation_paths = [
        "src/salias/src/channel.cpp",
        "src/core/flow/consumer.cpp",
        "src/core/flow/producer.cpp",
        "src/core/ring/magic_ring.cpp",
        "src/core/platform/mapping.cpp",
    ] + subprocess.run(
        ["git", "ls-tree", "-r", "--name-only", BASELINE_TAG, "src/core"],
        cwd=ROOT, stdout=subprocess.PIPE, check=True, text=True,
    ).stdout.splitlines()
    return (
        {path: git_bytes(BASELINE_TAG, path) for path in header_paths},
        {path: git_bytes(BASELINE_TAG, path) for path in implementation_paths},
    )


def extract_client(text, api):
    """Return one client with exactly one claim API shape."""
    if api == "v3":
        verify_client_api(text, "v4")
        text = replace_once(
            text,
            "write_payload(claim.payload(), marker, send_time_ns, options.latency_sample_rate != 0);\n"
            "      publisher.commit(std::move(claim));\n",
            "write_payload(claim.payload(), marker, send_time_ns, options.latency_sample_rate != 0);\n"
            "      claim.commit();\n",
            "v3 first commit",
        )
        text = replace_once(
            text,
            "write_payload(claim.payload(), marker, send_time_ns, options.latency_sample_rate != 0);\n"
            "        publisher.commit(std::move(claim));\n",
            "write_payload(claim.payload(), marker, send_time_ns, options.latency_sample_rate != 0);\n"
            "        claim.commit();\n",
            "v3 diagnostic commit",
        )
    verify_client_api(text, api)
    return text


def build_variant(output, api):
    """Compile one API arm and return its manifest."""
    directory = output / api
    directory.mkdir(parents=True, exist_ok=True)
    client_source = extract_client(
        (ROOT / CLIENT_PATH).read_text(), api
    )
    client = directory / "client.cpp"
    client.write_text(client_source)
    binary = directory / "client"

    include_args = []
    product_sources = [ROOT / "src/salias/src/channel.cpp",
                       ROOT / "src/core/flow/consumer.cpp",
                       ROOT / "src/core/flow/producer.cpp",
                       ROOT / "src/core/ring/magic_ring.cpp",
                       ROOT / "src/core/platform/mapping.cpp"]
    if api == "v3":
        headers, sources = baseline_sources()
        product_dir = directory / "product"
        write_extracted_product(product_dir, headers, sources)
        # v3 core headers come from the pinned baseline, not the current worktree.
        include_args = [
            "-I", str(product_dir / "include"),
            "-I", str(product_dir / "src"),
        ]
        product_sources = [
            product_dir / "src" / path.split("src/", 1)[1]
            for path in sources if path.endswith(".cpp")
        ]
        implementation_identity = {
            path: digest(data) for path, data in sources.items()
        }
    else:
        include_args = ["-I", str(ROOT / "src/salias/include"), "-I", str(ROOT / "src")]
        implementation_identity = {
            str(path.relative_to(ROOT)): digest(path.read_bytes())
            for path in product_sources
        }

    command = [
        "c++", "-std=c++23", "-O3", "-g", "-DNDEBUG", "-march=native", "-flto=1",
        *include_args,
        "-I", str(ROOT / "tools/aeron_compare"),
        str(client), *map(str, product_sources),
        "-pthread", "-lrt", "-ldl", "-o", str(binary),
    ]
    subprocess.run(command, cwd=ROOT, check=True)
    manifest = {
        "api": api,
        "client": str(binary),
        "client_sha256": digest(binary.read_bytes()),
        "client_source_sha256": digest(client.read_bytes()),
        # Both variants are generated from the same working-tree client template;
        # only the claim submission calls differ.
        "client_source_identity": f"{CLIENT_PATH}#{api}",
        "submission_sites": client_api_counts(client_source),
        "implementation_sha256": implementation_identity,
        "compile_command": shlex.join(command),
    }
    return manifest


def build(args):
    """Build both arms and write a single manifest."""
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    source = git_bytes(BASELINE_TAG, CLIENT_PATH)
    source_sha = digest(source)
    if source_sha != BASELINE_CLIENT_SHA256:
        raise ValueError(
            f"v3.0.0 benchmark source is {source_sha}, expected {BASELINE_CLIENT_SHA256}"
        )
    # The v3 variant is generated from the current client template by replacing
    # the two submission calls. It intentionally need not equal the historical
    # v3.0.0 client, which lacks later diagnostic options. The baseline product
    # sources are still pinned to BASELINE_TAG.
    template_sha = digest((ROOT / CLIENT_PATH).read_bytes())
    manifest = {
        "protocol": PROTOCOL,
        "baseline": f"git:{BASELINE_TAG}",
        "client_template_identity": CLIENT_PATH,
        "client_template_sha256": template_sha,
        "baseline_client_source_identity": f"git:{BASELINE_TAG}:{CLIENT_PATH}",
        "baseline_client_source_sha256": source_sha,
        "client_submission_sites": CLIENT_ANCHORS,
        "arms": {
            "v3": {"client": "v3/client", "environment": []},
            "v4-sequential-fence": {
                "client": "v4/client",
                "environment": ["SALIAS_DISABLE_ASYMMETRIC_FENCE=1"],
            },
            "v4-default-fence": {"client": "v4/client", "environment": []},
        },
        "variants": {
            "v3": build_variant(output, "v3"),
            "v4": build_variant(output, "v4"),
        },
    }
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps(manifest, indent=2))


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


if __name__ == "__main__":
    build(parse_args())
