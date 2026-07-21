#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

INTEGRATION_RUNS = {
    "pulsar": ["20260720T135944Z"],
    "redpanda-connect": ["20260720T160552Z"],
    "logstash": ["20260720T161054Z"],
    "camel": ["20260720T161756Z"],
    "spring-cloud-stream": ["20260720T163032Z"],
    "nifi": ["20260720T163618Z"],
    "spark-structured-streaming": ["20260720T164322Z"],
    "beam-directrunner": ["20260720T170558Z"],
    "kafka-connect": ["20260720T171900Z"],
    "kafka-streams": ["20260720T172502Z"],
    "flink": ["20260720T174759Z"],
    "bento-warpstream": ["20260720T175209Z"],
    "activemq-classic": ["20260720T180530Z"],
    "nats-jetstream": ["20260720T181423Z"],
    "redis-streams": ["20260720T182351Z"],
    "rabbitmq-streams": ["20260720T183755Z"],
    "emqx-mqtt": ["20260720T185514Z"],
    "rocketmq": ["20260720T192342Z"],
    "activemq-artemis": ["20260720T194319Z"],
    "vector": ["20260720T200000Z"],
    "opentelemetry": ["20260720T200343Z"],
    "debezium": ["20260720T202552Z"],
    "embedded-spring": ["20260721T050302Z", "20260721T054132Z"],
    "http-single": ["20260721T045008Z", "20260721T054134Z"],
    "http-batch": ["20260721T044908Z", "20260721T054136Z"],
    "grpc-batch": ["20260721T045105Z", "20260721T054138Z"],
    "grpc-streaming": ["20260721T045213Z", "20260721T054141Z"],
    "serverless-aws": ["20260721T050358Z"],
    "serverless-azure": ["20260721T045510Z"],
    "serverless-gcp": ["20260721T045957Z"],
}

TRIGGER_RUNS = {
    "azure-queue": "20260721t054012z",
    "azure-eventhub": "20260721t055132z",
    "gcp-pubsub": "20260721t060215z",
}

SOAK_RUNS = {
    ("embedded-spring", "20260721T054132Z"),
    ("http-single", "20260721T054134Z"),
    ("http-batch", "20260721T054136Z"),
    ("grpc-batch", "20260721T054138Z"),
    ("grpc-streaming", "20260721T054141Z"),
}


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def verify_checksum_file(bundle: Path, name: str) -> None:
    checksum_file = bundle / name
    assert checksum_file.is_file(), f"missing checksum manifest: {checksum_file}"
    for line in checksum_file.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        expected, relative = line.split("  ", 1)
        path = bundle / relative
        assert path.is_file(), f"manifest-listed file missing: {path}"
        assert digest(path) == expected.lower(), f"checksum mismatch: {path}"


def verify_write_boundary(bundle: Path) -> None:
    audit = load(bundle / "actual/write-boundary-audit.json")
    assert audit["passed"] is True, f"write-boundary audit failed: {bundle}"
    downstream_keys = (
        "verifierDownstreamProducerTargets",
        "verifierDownstreamProducerCalls",
    )
    present = [key for key in downstream_keys if key in audit]
    assert present, f"no verifier downstream-write assertion: {bundle}"
    assert all(audit[key] == 0 for key in present), f"verifier wrote downstream: {bundle}"


def verify_integration_runs() -> tuple[int, int, int, int]:
    attempted = transformed = errors = screenshots = 0
    for integration, run_ids in INTEGRATION_RUNS.items():
        for run_id in run_ids:
            bundle = ROOT / "evidence/integration-proofs" / integration / "runs" / run_id
            verify_checksum_file(bundle, "hashes.sha256")
            manifest = load(bundle / "run-manifest.json")
            result = load(bundle / "verification-result.json")
            assert manifest["runId"] == run_id
            assert manifest["status"] == result["status"] == "LIVE_LOCAL_VERIFIED"
            assert manifest["successfulOutputs"] == manifest["validRecords"]
            assert manifest["errorOutputs"] == manifest["invalidRecords"]
            assert manifest["duplicates"] == 0
            assert manifest["unexplainedMissing"] == 0
            verify_write_boundary(bundle)
            if (integration, run_id) in SOAK_RUNS:
                assert manifest["validRecords"] == 9900
                assert manifest["invalidRecords"] == 100
                assert manifest["durationSeconds"] >= 300
                stability = load(bundle / "metrics/stability-observations.json")
                assert stability["minimumDurationSeconds"] == 300
                assert max(item["elapsedSeconds"] for item in stability["observations"]) >= 300
                assert all(item["containerRunning"] for item in stability["observations"])
                assert all(item["controlPlaneHealth"] == "HEALTHY" for item in stability["observations"])
            else:
                assert manifest["validRecords"] == 100
                assert manifest["invalidRecords"] == 10
            attempted += manifest["validRecords"] + manifest["invalidRecords"]
            transformed += manifest["successfulOutputs"]
            errors += manifest["errorOutputs"]
            screenshots += len(list(bundle.rglob("*.png")))
    return attempted, transformed, errors, screenshots


def verify_trigger_runs() -> tuple[int, int, int]:
    attempted = transformed = errors = 0
    for trigger, run_id in TRIGGER_RUNS.items():
        bundle = ROOT / "evidence/trigger-proofs" / trigger / "runs" / run_id
        verify_checksum_file(bundle, "SHA256SUMS.txt")
        manifest = load(bundle / "proof-manifest.json")
        result = load(bundle / "actual/trigger-result.json")
        validation = load(bundle / "actual/content-validation.json")
        assert manifest["status"] == "PASS"
        assert manifest["validRecords"] == 100 and manifest["invalidRecords"] == 10
        assert result["attemptedInput"] == 110
        assert result["successfulOutput"] == 100 and result["errorOutput"] == 10
        assert len(result["verifierWriteTargets"]) == 1
        assert validation == {
            "expectedOutputs": 100,
            "actualOutputs": 100,
            "expectedHashMatches": 100,
            "intentionalErrors": 10,
            "errorContractMatches": 10,
            "duplicates": 0,
            "passed": True,
        }
        attempted += result["attemptedInput"]
        transformed += result["successfulOutput"]
        errors += result["errorOutput"]
    return attempted, transformed, errors


if __name__ == "__main__":
    integration = verify_integration_runs()
    triggers = verify_trigger_runs()
    totals = (
        integration[0] + triggers[0],
        integration[1] + triggers[1],
        integration[2] + triggers[2],
    )
    assert totals == (53_630, 52_800, 830)
    assert integration[3] == 62
    print(
        "Live-local evidence validation passed: "
        "38 bundles, 53,630 inputs, 52,800 transformed, 830 intentional DLQ, "
        "62 bundle screenshots, checksum-valid, and no verifier downstream writes."
    )
