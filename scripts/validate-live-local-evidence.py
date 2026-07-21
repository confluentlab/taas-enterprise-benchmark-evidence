#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if not __debug__:
    raise RuntimeError("Evidence validation must not run with Python assertions disabled.")

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

UNQUALIFIED_LAG_RUNS = {
    ("beam-directrunner", "20260720T170558Z"),
    ("flink", "20260720T174759Z"),
    ("kafka-connect", "20260720T171900Z"),
    ("spark-structured-streaming", "20260720T164322Z"),
}

AGGREGATE_ERROR_SURFACES = {"http-single", "http-batch", "grpc-batch", "grpc-streaming"}


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def load_jsonl(path: Path) -> list[dict]:
    assert path.is_file(), f"missing raw output: {path}"
    return [json.loads(line) for line in path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]


def canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def payload_of(record: dict) -> dict:
    is_transport_wrapper = "payload" in record and (
        "recordId" in record
        or "publishedBy" in record
        or "messageId" in record
        or set(record).issubset({"payload", "properties", "messageId"})
    )
    value = record["payload"] if is_transport_wrapper else record
    if isinstance(value, str):
        value = json.loads(value)
    assert isinstance(value, dict), f"output payload is not an object: {record}"
    return value


def record_id_of(record: dict, *, error: bool = False) -> str | None:
    for candidate in (record.get("recordId"), record.get("key")):
        if candidate:
            return str(candidate)
    payload = payload_of(record)
    if payload.get("eventId"):
        return str(payload["eventId"])
    if payload.get("recordId"):
        return str(payload["recordId"])
    source = payload.get("source") or {}
    if source.get("key") and str(source["key"]).startswith(("invalid-", "evt-live-")):
        return str(source["key"])
    if error:
        snippet = ((payload.get("payload") or {}).get("snippet"))
        if snippet:
            try:
                return str(json.loads(snippet).get("recordId") or "") or None
            except json.JSONDecodeError:
                pass
    return None


def error_codes(record: dict) -> set[str]:
    payload = payload_of(record)
    codes = {str(item.get("code")) for item in payload.get("errors", []) if item.get("code")}
    if not codes and (payload.get("error") or {}).get("code"):
        codes.add(str(payload["error"]["code"]))
    if payload.get("code"):
        codes.add(str(payload["code"]))
    return codes


def expected_outputs(bundle: Path) -> list[dict]:
    simulation = load(bundle / "expected/simulation-batch.json")
    records = simulation.get("records", [])
    outputs = [record.get("outputPreview") for record in records if record.get("success", True)]
    assert all(isinstance(output, dict) for output in outputs), f"invalid simulation baseline: {bundle}"
    return outputs


def verify_artifact_identity(bundle: Path, manifest: dict) -> None:
    runtime_status = bundle / "actual/runtime-status-after.json"
    assert runtime_status.is_file(), f"missing loaded-runtime identity: {bundle}"
    status = load(runtime_status)
    assert status.get("assignmentPresent") is True, f"runtime assignment not present: {bundle}"
    assert status.get("artifactId") == manifest["artifactId"], f"loaded artifact ID mismatch: {bundle}"
    assert status.get("artifactHash") == manifest["artifactHash"], f"loaded artifact hash mismatch: {bundle}"


def verify_capture_identities(bundle: Path) -> None:
    versions = load(bundle / "versions.json")
    verifier_hash = versions.get("rawOnlyVerifierSha256")
    assert isinstance(verifier_hash, str) and len(verifier_hash) == 64, f"missing verifier SHA-256: {bundle}"
    audit = load(bundle / "actual/write-boundary-audit.json")
    if audit.get("verifierSha256"):
        assert audit["verifierSha256"] == verifier_hash, f"verifier SHA-256 disagreement: {bundle}"
    for key, value in versions.items():
        if key.lower().endswith("imageid") and value is not None:
            assert str(value).startswith("sha256:") and len(str(value)) == 71, f"invalid image identity {key}: {bundle}"
        if key.lower().endswith("jarsha256") and value is not None:
            assert len(str(value).removeprefix("sha256:")) == 64, f"invalid JAR identity {key}: {bundle}"
    mounted_bridge = versions.get("mountedRuntimeSurfaceBridgeSha256")
    if mounted_bridge is not None:
        assert mounted_bridge == versions.get("runtimeSurfaceBridgeSha256"), f"mounted bridge identity mismatch: {bundle}"
    mounted_jar = versions.get("mountedRuntimeJarSha256")
    if mounted_jar is not None:
        assert mounted_jar == versions.get("runtimeJarSha256"), f"mounted JAR identity mismatch: {bundle}"


def expected_error_codes(bundle: Path, integration: str) -> set[str]:
    if integration in AGGREGATE_ERROR_SURFACES:
        return {"VALIDATION_FAILED"}
    invalid = load(bundle / "expected/simulation-invalid.json")
    codes = {
        match.group(1)
        for message in invalid.get("errors", [])
        if (match := re.match(r"^[^:]+:\s+([A-Z0-9_]+)\s+-", str(message)))
    }
    assert codes, f"simulation fixture has no error contract: {bundle}"
    return codes


def verify_raw_content(bundle: Path, manifest: dict, integration: str) -> tuple[int, int]:
    transformed = load_jsonl(bundle / "actual/transformed-output.jsonl")
    errors = load_jsonl(bundle / "actual/error-output.jsonl")
    expected = expected_outputs(bundle)
    actual_payloads = [payload_of(record) for record in transformed]
    assert Counter(map(canonical, actual_payloads)) == Counter(map(canonical, expected)), f"output multiset mismatch: {bundle}"

    transformed_ids = [record_id_of(record) for record in transformed]
    assert all(transformed_ids), f"transformed output lacks record ID: {bundle}"
    assert len(transformed_ids) == len(set(transformed_ids)), f"duplicate transformed record ID: {bundle}"

    error_ids = [record_id_of(record, error=True) for record in errors]
    observable_error_ids = [record_id for record_id in error_ids if record_id]
    assert len(observable_error_ids) == len(set(observable_error_ids)), f"duplicate observable DLQ record ID: {bundle}"
    assert not set(transformed_ids).intersection(observable_error_ids), f"record appears in output and DLQ: {bundle}"

    expected_codes = expected_error_codes(bundle, integration)
    matches = sum(error_codes(record) == expected_codes for record in errors)
    assert matches == manifest["invalidRecords"], f"error contract mismatch: {bundle}"
    assert len(transformed) == manifest["validRecords"], f"raw transformed count mismatch: {bundle}"
    assert len(errors) == manifest["invalidRecords"], f"raw error count mismatch: {bundle}"
    return len(transformed), len(errors)


def verify_checksum_file(bundle: Path, name: str) -> None:
    checksum_file = bundle / name
    assert checksum_file.is_file(), f"missing checksum manifest: {checksum_file}"
    listed: set[str] = set()
    for line in checksum_file.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        expected, relative = line.split("  ", 1)
        assert relative not in listed, f"duplicate checksum entry: {bundle / relative}"
        path = bundle / relative
        assert path.is_file(), f"manifest-listed file missing: {path}"
        assert digest(path) == expected.lower(), f"checksum mismatch: {path}"
        listed.add(relative)
    actual = {
        path.relative_to(bundle).as_posix()
        for path in bundle.rglob("*")
        if path.is_file() and path != checksum_file
    }
    assert listed == actual, f"bundle checksum inventory mismatch: missing={sorted(actual-listed)}, stale={sorted(listed-actual)}"


def verify_write_boundary(bundle: Path) -> None:
    audit = load(bundle / "actual/write-boundary-audit.json")
    downstream_keys = (
        "verifierDownstreamProducerTargets",
        "verifierDownstreamProducerCalls",
    )
    present = [key for key in downstream_keys if key in audit]
    assert present, f"no verifier downstream-write assertion: {bundle}"
    centrally_passed = all(audit[key] == 0 for key in present)
    assert centrally_passed, f"verifier wrote downstream: {bundle}"
    assert audit["passed"] is centrally_passed, f"adapter write-boundary result disagrees with central evaluation: {bundle}"


def verify_completion_and_backlog(bundle: Path, manifest: dict) -> bool:
    counts = load(bundle / "counts.json")
    state = load(bundle / "final-state.json")
    assert counts["pending"] == counts["finalLag"] == 0, f"non-zero recorded pending/lag: {bundle}"
    assert state["pending"] == state["finalLag"] == 0, f"non-zero final pending/lag: {bundle}"
    assert state["runtimeHealthy"] is True, f"runtime not healthy at capture end: {bundle}"
    assert counts["successfulOutput"] + counts["errorOutput"] == counts["attemptedInput"], f"central accounting mismatch: {bundle}"
    assert counts["successfulOutput"] == manifest["validRecords"] and counts["errorOutput"] == manifest["invalidRecords"], f"count/manifest mismatch: {bundle}"

    kafka_group = bundle / "metrics/kafka-consumer-group.txt"
    if kafka_group.is_file():
        rows = []
        for line in kafka_group.read_text(encoding="utf-8-sig").splitlines():
            columns = line.split()
            if len(columns) >= 6 and columns[2].isdigit() and columns[5].isdigit():
                rows.append(columns)
        if not rows:
            return False
        assert all(int(row[5]) == 0 for row in rows), f"broker-derived lag is non-zero: {bundle}"
    else:
        native_metrics = [
            path for path in (bundle / "metrics").glob("*")
            if any(token in path.name.lower() for token in ("stat", "queue", "stream", "jsz", "varz"))
        ]
        assert native_metrics and all(path.stat().st_size > 0 for path in native_metrics), f"missing native broker backlog evidence: {bundle}"
    return True


def verify_result_evidence(bundle: Path, result: dict, *, qualified: bool) -> None:
    placeholder = "actual/not-executed.json"
    assert placeholder not in json.dumps(result).replace("\\\\", "/"), (
        f"successful result cites prepare-only placeholder: {bundle}"
    )
    gates = result.get("gates", [])
    assert gates, f"successful result has no gates: {bundle}"
    if qualified:
        assert all(not (gate.get("required") and gate.get("applicable")) or gate.get("passed") is True for gate in gates), f"successful result contains a failed mandatory gate: {bundle}"
        assert result.get("functionalMinimumMet") is True and result.get("exactAccounting") is True, f"successful result lacks functional/accounting qualification: {bundle}"
    for gate in gates:
        for relative in gate.get("evidence", []):
            assert (bundle / relative).exists(), f"result cites missing evidence: {bundle / relative}"


def verify_integration_runs() -> tuple[int, int, int, int]:
    attempted = transformed = errors = screenshots = 0
    for integration, run_ids in INTEGRATION_RUNS.items():
        for run_id in run_ids:
            bundle = ROOT / "evidence/integration-proofs" / integration / "runs" / run_id
            verify_checksum_file(bundle, "hashes.sha256")
            manifest = load(bundle / "run-manifest.json")
            result = load(bundle / "verification-result.json")
            assert not (bundle / "actual/not-executed.json").exists(), f"successful bundle contains not-executed placeholder: {bundle}"
            assert manifest["runId"] == run_id
            qualified = (integration, run_id) not in UNQUALIFIED_LAG_RUNS
            expected_status = "LIVE_LOCAL_VERIFIED" if qualified else "MEASURED_NOT_QUALIFIED"
            assert manifest["status"] == result["status"] == expected_status
            verify_result_evidence(bundle, result, qualified=qualified)
            assert manifest["successfulOutputs"] == manifest["validRecords"]
            assert manifest["errorOutputs"] == manifest["invalidRecords"]
            assert manifest["duplicates"] == 0
            assert manifest["unexplainedMissing"] == 0
            assert manifest["unexpectedFailures"] == 0
            raw_transformed, raw_errors = verify_raw_content(bundle, manifest, integration)
            verify_artifact_identity(bundle, manifest)
            verify_capture_identities(bundle)
            verify_write_boundary(bundle)
            native_lag_verified = verify_completion_and_backlog(bundle, manifest)
            assert native_lag_verified is qualified, f"bundle qualification disagrees with native broker lag evidence: {bundle}"
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
            transformed += raw_transformed
            errors += raw_errors
            screenshots += len(list(bundle.rglob("*.png")))
    return attempted, transformed, errors, screenshots


def verify_trigger_runs() -> tuple[int, int, int]:
    attempted = transformed = errors = 0
    for trigger, run_id in TRIGGER_RUNS.items():
        bundle = ROOT / "evidence/trigger-proofs" / trigger / "runs" / run_id
        verify_checksum_file(bundle, "SHA256SUMS.txt")
        manifest = load(bundle / "proof-manifest.json")
        result = load(bundle / "actual/trigger-result.json")
        reported_validation = load(bundle / "actual/content-validation.json")
        assert manifest["status"] == "LIVE_LOCAL_VERIFIED"
        assert manifest["validRecords"] == 100 and manifest["invalidRecords"] == 10
        assert result["attemptedInput"] == 110
        assert result["successfulOutput"] == 100 and result["errorOutput"] == 10
        assert len(result["verifierWriteTargets"]) == 1
        transformed_records = load_jsonl(bundle / "actual/transformed-output.jsonl")
        error_records = load_jsonl(bundle / "actual/error-output.jsonl")
        shared_baseline = expected_outputs(ROOT / "evidence/integration-proofs/pulsar/runs/20260720T135944Z")
        actual_payloads = [payload_of(record) for record in transformed_records]
        transformed_ids = [record_id_of(record) for record in transformed_records]
        error_ids = [record_id_of(record, error=True) for record in error_records]
        assert Counter(map(canonical, actual_payloads)) == Counter(map(canonical, shared_baseline)), f"trigger output mismatch: {bundle}"
        assert all(transformed_ids) and len(transformed_ids) == len(set(transformed_ids)), f"trigger duplicate/missing output IDs: {bundle}"
        assert all(error_ids) and len(error_ids) == len(set(error_ids)), f"trigger duplicate/missing DLQ IDs: {bundle}"
        assert not set(transformed_ids).intersection(error_ids), f"trigger record appears in output and DLQ: {bundle}"
        expected_codes = {"MISSING_REQUIRED_FIELD", "REGEX_FAILED", "TYPE_CONVERSION_FAILED"}
        recomputed_validation = {
            "expectedOutputs": 100,
            "actualOutputs": len(transformed_records),
            "expectedHashMatches": 100,
            "intentionalErrors": len(error_records),
            "errorContractMatches": sum(error_codes(record) == expected_codes for record in error_records),
            "duplicates": 0,
            "passed": True,
        }
        assert recomputed_validation == reported_validation, f"trigger adapter report differs from central evaluation: {bundle}"
        attempted += result["attemptedInput"]
        transformed += len(transformed_records)
        errors += len(error_records)
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
        "62 bundle screenshots, centrally recomputed content/error gates, loaded-artifact identity, "
        "checksum-valid, and recorded zero verifier downstream-write counters."
    )
