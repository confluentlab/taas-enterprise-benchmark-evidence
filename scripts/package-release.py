#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import shutil
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
manifest = json.loads((ROOT / "evidence/manifest.json").read_text(encoding="utf-8"))
release_root = ROOT / "release" / manifest["releaseTag"]
release_root.mkdir(parents=True, exist_ok=True)


def write_zip(destination: Path, entries: list[tuple[Path, str]]) -> None:
    with zipfile.ZipFile(destination, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for source, name in sorted(entries, key=lambda item: item[1]):
            archive.write(source, name)


raw_entries = [
    (path, path.relative_to(ROOT / "evidence").as_posix())
    for evidence_root in (ROOT / "evidence/integration-proofs", ROOT / "evidence/trigger-proofs")
    for path in evidence_root.rglob("*") if path.is_file()
]
write_zip(release_root / "raw-evidence.zip", raw_entries)

audit_root = ROOT / "evidence/live-local-supplement/audit"
environment_entries = [(path, path.relative_to(audit_root.parent).as_posix()) for path in audit_root.rglob("*") if path.is_file()]
environment_entries.append((ROOT / "reproduction/live-local-verification/README.md", "README.md"))
write_zip(release_root / "environment-bundle.zip", environment_entries)

screenshots_root = ROOT / "evidence/live-local-supplement/screenshots"
graphics_entries = [(path, path.relative_to(screenshots_root.parent).as_posix()) for path in screenshots_root.rglob("*") if path.is_file()]
for relative in ("assets/benchmark-summary.svg", "assets/architecture.svg"):
    graphics_entries.append((ROOT / relative, relative))
write_zip(release_root / "benchmark-graphics.zip", graphics_entries)

shutil.copyfile(ROOT / "evidence/manifest.json", release_root / "evidence-manifest.json")
shutil.copyfile(ROOT / "assets/architecture.svg", release_root / "architecture.svg")

checksums = release_root / "checksums.sha256"
assets = sorted(path for path in release_root.iterdir() if path.is_file() and path != checksums)
lines = [f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}" for path in assets]
checksums.write_bytes(("\n".join(lines) + "\n").encode("utf-8"))
print(f"Packaged {manifest['releaseTag']} with {len(assets)} checksummed assets.")
