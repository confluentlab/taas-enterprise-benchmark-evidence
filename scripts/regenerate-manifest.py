#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def digest(relative: str) -> str:
    return hashlib.sha256((ROOT / relative).read_bytes()).hexdigest()


RUNS = [
    {
        "id": "core-1m-20260717",
        "status": "MEASURED_NOT_QUALIFIED",
        "workload": "core-1m-976-fields",
        "summary": "evidence/core-engine/summary.md",
        "rawResults": "evidence/core-engine/results.csv",
        "environment": "evidence/core-engine/environment.txt",
        "methodology": "evidence/core-engine/methodology.md",
        "charts": ["evidence/core-engine/charts/latency.svg"],
    },
    {
        "id": "payload-scaling-20260718",
        "status": "MEASURED",
        "workload": "core-1m-to-64m-976-fields-fixed-output",
        "summary": "evidence/payload-scaling/summary.md",
        "rawResults": "evidence/payload-scaling/results.csv",
        "environment": "evidence/core-engine/environment.txt",
        "methodology": "docs/benchmark-methodology.md",
        "charts": [
            "evidence/payload-scaling/charts/mean-latency.svg",
            "evidence/payload-scaling/charts/tail-latency.svg",
            "evidence/payload-scaling/charts/allocation.svg",
        ],
    },
    {
        "id": "kafka-soak-20260711",
        "status": "LIVE_LOCAL_VERIFIED",
        "workload": "kafka-flink-100kb-600rps-30m",
        "summary": "evidence/kafka-soak/summary.md",
        "rawResults": "evidence/kafka-soak/consumer-results.json",
        "environment": "evidence/kafka-soak/summary.md",
        "methodology": "docs/benchmark-methodology.md",
        "charts": ["evidence/kafka-soak/charts/accounting-and-lag.svg"],
    },
    {
        "id": "flink-1m-20260718",
        "status": "LIVE_LOCAL_VERIFIED",
        "workload": "flink-kafka-1m-four-partitions-40rps",
        "summary": "evidence/live-flink-runtime/summary.md",
        "rawResults": "evidence/live-flink-runtime/latency-results.json",
        "environment": "evidence/live-flink-runtime/summary.md",
        "methodology": "docs/benchmark-methodology.md",
        "charts": [],
    },
    {
        "id": "runtime-parity-20260712",
        "status": "CONTRACT_VERIFIED",
        "workload": "runtime-parity-canonical-v1",
        "summary": "evidence/runtime-parity/summary.md",
        "rawResults": "evidence/runtime-parity/parity-output-report.json",
        "environment": "evidence/runtime-parity/summary.md",
        "methodology": "evidence/runtime-parity/summary.md",
        "charts": [],
    },
    {
        "id": "streaming-tool-matrix-20260720",
        "status": "MEASURED_NOT_QUALIFIED",
        "workload": "22-local-streaming-and-ecosystem-integrations",
        "summary": "evidence/integration-proofs/EVIDENCE-OVERVIEW.md",
        "rawResults": "evidence/live-local-supplement/audit/execution-matrix.csv",
        "environment": "evidence/live-local-supplement/audit/execution-matrix.json",
        "methodology": "reproduction/live-local-verification/README.md",
        "charts": [],
    },
    {
        "id": "runtime-surface-matrix-20260721",
        "status": "LIVE_LOCAL_VERIFIED",
        "workload": "jvm-http-grpc-serverless-baselines-and-five-minute-soaks",
        "summary": "evidence/integration-proofs/EVIDENCE-OVERVIEW.md",
        "rawResults": "evidence/integration-proofs/grpc-batch/runs/20260721T054138Z/actual/bridge-result.json",
        "environment": "evidence/integration-proofs/grpc-batch/runs/20260721T054138Z/environment.json",
        "methodology": "reproduction/live-local-verification/README.md",
        "charts": [],
    },
    {
        "id": "provider-triggers-20260721",
        "status": "LIVE_LOCAL_VERIFIED",
        "workload": "azure-queue-eventhub-and-gcp-pubsub-local-emulators",
        "summary": "evidence/integration-proofs/EVIDENCE-OVERVIEW.md",
        "rawResults": "evidence/trigger-proofs/gcp-pubsub/runs/20260721t060215z/actual/trigger-result.json",
        "environment": "evidence/trigger-proofs/gcp-pubsub/runs/20260721t060215z/proof-manifest.json",
        "methodology": "reproduction/live-local-verification/README.md",
        "charts": [],
    },
]

for run in RUNS:
    run["checksumAlgorithm"] = "sha256"
    run["checksum"] = digest(run["rawResults"])

manifest = {
    "schemaVersion": 1,
    "evidenceVersion": "2026.07.2",
    "generatedAt": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
    "flowplaneBuild": "10a26df4d7ed6a41f8076a5d7280d73db543c13a",
    "repositoryCommit": "resolved-by-release-tag:evidence-2026.07.2",
    "releaseTag": "evidence-2026.07.2",
    "statusVocabulary": "docs/evidence-classification.md",
    "claimsMatrix": "evidence/claims-matrix.csv",
    "checksums": "evidence/checksums.sha256",
    "releaseAssets": [
        "release/evidence-2026.07.2/evidence-manifest.json",
        "release/evidence-2026.07.2/checksums.sha256",
        "release/evidence-2026.07.2/benchmark-summary.md",
        "release/evidence-2026.07.2/raw-evidence.zip",
        "release/evidence-2026.07.2/environment-bundle.zip",
        "release/evidence-2026.07.2/benchmark-graphics.zip",
        "release/evidence-2026.07.2/architecture.svg"
    ],
    "runs": RUNS,
}

(ROOT / "evidence/manifest.json").write_bytes((json.dumps(manifest, indent=2) + "\n").encode("utf-8"))
print("Evidence manifest regenerated.")
