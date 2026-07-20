#!/usr/bin/env python3
from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def check_hashes() -> None:
    manifest = ROOT / "evidence" / "checksums.sha256"
    for line in manifest.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        expected, relative = line.split("  ", 1)
        path = ROOT / relative
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        assert actual == expected, f"checksum mismatch: {relative}"


def check_soak() -> None:
    producer = json.loads((ROOT / "evidence/kafka-soak/producer-results.json").read_text())
    consumer = json.loads((ROOT / "evidence/kafka-soak/consumer-results.json").read_text())
    produced = producer["recordsProducedToRaw"]
    consumed = consumer["recordsConsumedFromRaw"]
    output = consumer["recordsProducedToOutput"]
    failed = consumer["recordsFailed"]
    assert produced == 1_080_001, f"unexpected produced count: {produced}"
    assert consumed == produced, f"consumed {consumed} != produced {produced}"
    assert output + failed == produced, "soak accounting mismatch"
    assert failed == 10_800, f"unexpected intentional failure count: {failed}"


def check_parity() -> None:
    with (ROOT / "evidence/runtime-parity/output-hashes.csv").open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 5
    hashes = {row["valid_output_sha256"] for row in rows}
    assert hashes == {"6afb9b453bb3f66f696621dec4de4bf23b57b4de1e16490691062d450c1d6a58"}


def check_scaling() -> None:
    with (ROOT / "evidence/payload-scaling/results.csv").open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert [int(row["size_mib"]) for row in rows] == [1, 2, 4, 8, 16, 32, 64]
    assert all(float(row["mean_ms_per_op"]) > 0 for row in rows)


if __name__ == "__main__":
    check_hashes()
    check_soak()
    check_parity()
    check_scaling()
    print("Evidence validation passed.")
