#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "evidence/checksums.sha256"
EXCLUDED = {"evidence/checksums.sha256"}


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


inventory = subprocess.run(
    ["git", "ls-files", "--cached", "--others", "--exclude-standard"],
    cwd=ROOT, check=True, capture_output=True, text=True,
).stdout.splitlines()
files = sorted(
    (ROOT / relative for relative in inventory
     if relative.replace("\\", "/") not in EXCLUDED
     and (ROOT / relative).is_file()
     and not (relative.replace("\\", "/").startswith("release/") and relative.replace("\\", "/").endswith("/checksums.sha256"))),
    key=lambda path: path.relative_to(ROOT).as_posix(),
)
lines = [f"{digest(path)}  {path.relative_to(ROOT).as_posix()}" for path in files]
MANIFEST.write_bytes(("\n".join(lines) + "\n").encode("utf-8"))
print(f"Regenerated {MANIFEST.relative_to(ROOT).as_posix()} with {len(lines)} entries.")
