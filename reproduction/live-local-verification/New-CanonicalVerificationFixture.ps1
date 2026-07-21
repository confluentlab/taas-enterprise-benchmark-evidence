param(
  [string]$OutputRoot = (Join-Path (Resolve-Path (Join-Path $PSScriptRoot "..\..\..")).Path "artifacts\live-local-verification\canonical-fixture"),
  [int]$ValidCount = 100,
  [int]$InvalidCount = 10
)

. (Join-Path $PSScriptRoot "LiveVerification.Common.ps1")

if ($ValidCount -lt 100) { throw "Functional verification requires at least 100 valid fixtures." }
if ($InvalidCount -lt 10) { throw "Functional verification requires at least 10 invalid fixtures." }
$OutputRoot = Resolve-FullPath $OutputRoot
if (-not (Test-PathWithin $OutputRoot $script:ScriptCopyRoot)) {
  throw "Fixture output must remain inside the script-only copy: $script:ScriptCopyRoot"
}
New-Item -ItemType Directory -Force -Path $OutputRoot | Out-Null

$canonicalization = [ordered]@{
  schemaVersion = "flowplane.canonicalization.v1"
  encoding = "UTF-8"
  objectFieldOrdering = "lexicographic by Unicode property name"
  arrayOrdering = "preserve semantic input order; sort only when an integration explicitly declares unordered semantics"
  numberFormatting = "JSON finite decimal representation; no locale-specific separators"
  timestampFormatting = "UTC RFC3339 with Z suffix"
  nullHandling = "preserve explicit null; omit only fields declared optional by the artifact"
  whitespace = "no insignificant whitespace"
  lineEndings = "LF"
  errorFieldOrdering = "lexicographic by Unicode property name"
  optionalFieldOmission = "only fields absent from the source and declared optional are omitted"
}
Write-JsonFile -Path (Join-Path $OutputRoot "canonicalization.json") -Value $canonicalization

$mapping = @'
version: 1
name: flowplane-live-local-verification-v1
error_policy:
  on_transformation_error: ROUTE_TO_DLQ
  on_validation_failure: ROUTE_TO_DLQ
  on_type_mismatch: ROUTE_TO_DLQ
output:
  shape: FLAT_OBJECT
  complexTypes: NATIVE_JSON
  fieldNaming: AS_IS
fields:
  eventId:
    path: $.event.id
    required: true
    validate:
      required: true
      pattern: "^evt-live-[0-9]{6}$"
  customerName:
    path: $.customer.name
    normalize_string: true
  amount:
    path: $.order.amount
    cast: decimal
    decimalScale: 2
    decimalScalePolicy: ROUND
  amountWithTax:
    arithmetic: "$.order.amount * 1.10"
  occurredAt:
    path: $.event.occurredAt
    cast: timestamp
  region:
    path: $.customer.region
    default: unknown
  emailMasked:
    path: $.customer.email
    mask: last4
  tags:
    path: $.order.tags
  primarySku:
    path: $.order.lines[0].sku
    required: true
'@
Write-Utf8NoBom -Path (Join-Path $OutputRoot "mapping.yaml") -Value ($mapping.Trim() + "`n")

$validLines = [Collections.Generic.List[string]]::new()
$expectedLines = [Collections.Generic.List[string]]::new()
for ($index = 1; $index -le $ValidCount; $index++) {
  $eventId = "evt-live-{0:D6}" -f $index
  $payload = [ordered]@{
    event = [ordered]@{ id = $eventId; occurredAt = "2026-07-20T12:34:56Z" }
    customer = [ordered]@{ name = "  Ada   Lovelace  "; email = "ada+$index@example.test"; region = $null }
    order = [ordered]@{
      amount = "128.45"
      tags = @("synthetic", "verification", "batch-$([Math]::Floor(($index - 1) / 25))")
      lines = @([ordered]@{ sku = "SKU-001"; quantity = 2 }, [ordered]@{ sku = "SKU-002"; quantity = 1 })
    }
  }
  $expected = [ordered]@{
    amount = 128.45
    amountWithTax = 141.295
    customerName = "Ada Lovelace"
    emailMasked = "*******************test"
    eventId = $eventId
    occurredAt = "2026-07-20T12:34:56Z"
    primarySku = "SKU-001"
    region = $null
    tags = @("synthetic", "verification", "batch-$([Math]::Floor(($index - 1) / 25))")
  }
  $validLines.Add(($payload | ConvertTo-Json -Depth 20 -Compress))
  $expectedLines.Add(($expected | ConvertTo-Json -Depth 20 -Compress))
}

$invalidLines = [Collections.Generic.List[string]]::new()
$expectedErrors = [Collections.Generic.List[string]]::new()
for ($index = 1; $index -le $InvalidCount; $index++) {
  $recordId = "invalid-{0:D3}" -f $index
  $payload = [ordered]@{
    recordId = $recordId
    event = [ordered]@{ id = ""; occurredAt = "not-a-timestamp" }
    customer = [ordered]@{ name = "Invalid Synthetic"; email = "invalid@example.test" }
    order = [ordered]@{ amount = "not-a-number"; tags = @(); lines = @() }
  }
  $error = [ordered]@{
    code = "VALIDATION_FAILED"
    recordId = $recordId
    retryable = $false
  }
  $invalidLines.Add(($payload | ConvertTo-Json -Depth 20 -Compress))
  $expectedErrors.Add(($error | ConvertTo-Json -Depth 20 -Compress))
}

Write-Utf8NoBom -Path (Join-Path $OutputRoot "valid-input.jsonl") -Value (($validLines -join "`n") + "`n")
Write-Utf8NoBom -Path (Join-Path $OutputRoot "invalid-input.jsonl") -Value (($invalidLines -join "`n") + "`n")
Write-Utf8NoBom -Path (Join-Path $OutputRoot "expected-valid-output.jsonl") -Value (($expectedLines -join "`n") + "`n")
Write-Utf8NoBom -Path (Join-Path $OutputRoot "expected-invalid-errors.jsonl") -Value (($expectedErrors -join "`n") + "`n")

$mixed = @($validLines | Select-Object -First 45) + @($invalidLines | Select-Object -First 5) + @($validLines | Select-Object -Skip 45 -First 55) + @($invalidLines | Select-Object -Skip 5)
Write-Utf8NoBom -Path (Join-Path $OutputRoot "mixed-batch.jsonl") -Value (($mixed -join "`n") + "`n")
$batchRoot = Join-Path $OutputRoot "batches"
New-Item -ItemType Directory -Force -Path $batchRoot | Out-Null
$batchDefinitions = [ordered]@{
  "valid-01.json" = @($validLines | Select-Object -First 34)
  "valid-02.json" = @($validLines | Select-Object -Skip 34 -First 33)
  "valid-03.json" = @($validLines | Select-Object -Skip 67)
  "invalid-only.json" = @($invalidLines)
  "mixed.json" = @($mixed)
}
foreach ($batchName in $batchDefinitions.Keys) {
  $records = @($batchDefinitions[$batchName] | ForEach-Object { $_ | ConvertFrom-Json })
  Write-Utf8NoBom -Path (Join-Path $batchRoot $batchName) -Value (($records | ConvertTo-Json -Depth 20) + "`n")
}

$metadata = [ordered]@{
  schemaVersion = "flowplane.canonical-fixture-set.v1"
  fixtureSet = "flowplane-live-local-verification-v1"
  artifactId = "flowplane-live-local-verification"
  artifactVersion = "1.0.0"
  artifactHash = Get-Sha256 (Join-Path $OutputRoot "mapping.yaml")
  canonicalizationVersion = $canonicalization.schemaVersion
  validRecords = $ValidCount
  invalidRecords = $InvalidCount
  batches = 5
  mixedBatchIncluded = $true
  expectedErrorCode = "VALIDATION_FAILED"
  createdAt = [DateTime]::UtcNow.ToString("o")
  files = [ordered]@{}
}
foreach ($name in @("canonicalization.json", "mapping.yaml", "valid-input.jsonl", "invalid-input.jsonl", "mixed-batch.jsonl", "expected-valid-output.jsonl", "expected-invalid-errors.jsonl")) {
  $metadata.files[$name] = Get-Sha256 (Join-Path $OutputRoot $name)
}
foreach ($batchName in $batchDefinitions.Keys) {
  $relative = "batches/$batchName"
  $metadata.files[$relative] = Get-Sha256 (Join-Path $batchRoot $batchName)
}
Write-JsonFile -Path (Join-Path $OutputRoot "fixture-manifest.json") -Value $metadata

Write-Output (Join-Path $OutputRoot "fixture-manifest.json")
