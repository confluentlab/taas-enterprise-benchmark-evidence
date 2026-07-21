#!/usr/bin/env python3
from __future__ import annotations

import hashlib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def regenerate(bundle: Path, manifest_name: str) -> None:
    manifest = bundle / manifest_name
    files = sorted(
        (path for path in bundle.rglob("*") if path.is_file() and path != manifest),
        key=lambda path: path.relative_to(bundle).as_posix(),
    )
    lines = [f"{digest(path)}  {path.relative_to(bundle).as_posix()}" for path in files]
    manifest.write_bytes(("\n".join(lines) + "\n").encode("utf-8"))


integration_bundles = list((ROOT / "evidence/integration-proofs").glob("*/runs/*"))
trigger_bundles = list((ROOT / "evidence/trigger-proofs").glob("*/runs/*"))
for bundle in integration_bundles:
    if bundle.is_dir():
        regenerate(bundle, "hashes.sha256")
for bundle in trigger_bundles:
    if bundle.is_dir():
        regenerate(bundle, "SHA256SUMS.txt")
print(f"Regenerated {len(integration_bundles)} integration and {len(trigger_bundles)} trigger checksum manifests.")
