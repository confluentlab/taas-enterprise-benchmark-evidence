#!/usr/bin/env python3
from __future__ import annotations

import hashlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "evidence/checksums.sha256"
EXCLUDED = {
    "evidence/checksums.sha256",
    "release/evidence-2026.07.1/checksums.sha256",
}


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


files = sorted(
    path
    for path in ROOT.rglob("*")
    if path.is_file()
    and ".git" not in path.parts
    and path.relative_to(ROOT).as_posix() not in EXCLUDED
)
lines = [f"{digest(path)}  {path.relative_to(ROOT).as_posix()}" for path in files]
MANIFEST.write_bytes(("\n".join(lines) + "\n").encode("utf-8"))
print(f"Regenerated {MANIFEST.relative_to(ROOT).as_posix()} with {len(lines)} entries.")
