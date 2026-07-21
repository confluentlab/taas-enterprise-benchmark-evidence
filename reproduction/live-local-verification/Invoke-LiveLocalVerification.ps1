param(
  [string]$FlowplaneRoot = "C:\FlowPlaneNew\repositories\flowplane-controlplane",
  [string]$PublicEvidenceRoot = "C:\FlowPlaneNew\flowplane-enterprise-benchmark-evidence",
  [string]$OutputRoot = (Join-Path (Resolve-Path (Join-Path $PSScriptRoot "..\..\..")).Path "artifacts\live-local-verification"),
  [string[]]$Integration = @(),
  [ValidateRange(1,100000)][int]$ValidRecordCount = 100,
  [ValidateRange(1,10000)][int]$InvalidRecordCount = 10,
  [ValidateRange(0,3600)][int]$MinimumDurationSeconds = 0,
  [switch]$PrepareRunBundles,
  [switch]$Execute
)

. (Join-Path $PSScriptRoot "LiveVerification.Common.ps1")

$FlowplaneRoot = Resolve-FullPath $FlowplaneRoot -RequireExisting
$PublicEvidenceRoot = Resolve-FullPath $PublicEvidenceRoot -RequireExisting
$OutputRoot = Resolve-FullPath $OutputRoot
Assert-IsolatedOutputRoot -OutputRoot $OutputRoot -FlowplaneRoot $FlowplaneRoot -PublicEvidenceRoot $PublicEvidenceRoot
New-Item -ItemType Directory -Force -Path $OutputRoot | Out-Null

$inventory = @(Get-Inventory)
$knownIds = @($inventory | ForEach-Object id)
foreach ($id in $Integration) {
  if ($knownIds -notcontains $id) { throw "Unknown integration '$id'. Known integrations: $($knownIds -join ', ')" }
}
if ($Execute -and $Integration.Count -eq 0) {
  throw "-Execute requires one or more explicit -Integration values. The runner never starts every runtime implicitly."
}
if ($Execute) { $PrepareRunBundles = $true }

$flowplaneGit = Get-GitState $FlowplaneRoot
$evidenceGit = Get-GitState $PublicEvidenceRoot
$flowplaneCommit = $flowplaneGit.commit
$evidenceCommit = $evidenceGit.commit
$auditRoot = Join-Path $OutputRoot "audit"
New-Item -ItemType Directory -Force -Path $auditRoot | Out-Null

$matrix = @()
foreach ($item in $inventory) {
  $implementationChecks = @($item.implementationPaths | ForEach-Object {
    $relative = [string]$_
    [ordered]@{
      path = $relative
      exists = (Test-Path -LiteralPath (Join-Path $FlowplaneRoot $relative)) -or
        (Test-Path -LiteralPath (Join-Path $script:ScriptCopyRoot $relative))
    }
  })
  $harnessExists = -not [string]::IsNullOrWhiteSpace([string]$item.harness) -and (
    (Test-Path -LiteralPath (Join-Path $FlowplaneRoot ([string]$item.harness))) -or
    (Test-Path -LiteralPath (Join-Path $script:ScriptCopyRoot ([string]$item.harness)))
  )
  $implementationPresent = ($implementationChecks.Count -gt 0) -and (@($implementationChecks | Where-Object { -not $_.exists }).Count -eq 0)
  $missing = @()
  if (-not $implementationPresent) { $missing += "no complete executable implementation/configuration found" }
  if (-not $harnessExists) { $missing += "no runnable harness found" }
  if ($item.currentPublicStatus -ne "LIVE_LOCAL_VERIFIED") {
    $missing += "strict 100-valid/10-invalid bundle with all 26 gate assertions"
  }
  if ($item.coreStabilityRequired -and $item.coreStabilityStatus -ne "LIVE_LOCAL_VERIFIED") { $missing += "10,000-record, 1%-invalid, five-minute stability proof" }
  $matrix += [pscustomobject][ordered]@{
    Integration = $item.id
    Type = $item.type
    ImplementationPath = ($item.implementationPaths -join "; ")
    ImplementationPresent = $implementationPresent
    RealRuntimeRequired = $item.runtime
    ExistingEvidence = $item.currentPublicStatus
    CurrentStatus = $item.currentPublicStatus
    StabilityStatus = $item.coreStabilityStatus
    PreferredStabilityRun = $item.preferredStabilityRun
    Harness = $item.harness
    HarnessExists = $harnessExists
    MissingProof = ($missing -join "; ")
    PlannedCommand = $item.plannedCommand
  }
}
$matrix | Export-Csv -LiteralPath (Join-Path $auditRoot "execution-matrix.csv") -NoTypeInformation -Encoding UTF8
Write-JsonFile -Path (Join-Path $auditRoot "execution-matrix.json") -Value ([ordered]@{
  schemaVersion = "flowplane.execution-matrix.v1"
  generatedAt = [DateTime]::UtcNow.ToString("o")
  flowplaneCommit = $flowplaneCommit
  publicEvidenceCommit = $evidenceCommit
  flowplaneWorktree = $flowplaneGit
  publicEvidenceWorktree = $evidenceGit
  entries = $matrix
})

$claimFiles = @(
  "README.md",
  "docs/runtime-portability.md",
  "docs/evidence-classification.md",
  "docs/limitations.md",
  "evidence/claims-matrix.csv",
  "evidence/manifest.json",
  "evidence/integration-proofs/README.md"
)
$claimAudit = foreach ($relative in $claimFiles) {
  $path = Join-Path $PublicEvidenceRoot $relative
  [ordered]@{ path = $relative; exists = (Test-Path -LiteralPath $path -PathType Leaf); sha256 = if (Test-Path -LiteralPath $path -PathType Leaf) { Get-Sha256 $path } else { $null } }
}
Write-JsonFile -Path (Join-Path $auditRoot "public-claims-snapshot.json") -Value ([ordered]@{
  schemaVersion = "flowplane.public-claims-snapshot.v1"
  capturedAt = [DateTime]::UtcNow.ToString("o")
  publicEvidenceCommit = $evidenceCommit
  files = $claimAudit
  note = "Read-only snapshot. This script does not update or publish public claims."
})

$fixtureRoot = Join-Path $OutputRoot "canonical-fixture"
& (Join-Path $PSScriptRoot "New-CanonicalVerificationFixture.ps1") -OutputRoot $fixtureRoot | Out-Null

if (-not $PrepareRunBundles) {
  Write-Output "Audit complete: $auditRoot"
  Write-Output "Canonical fixture complete: $fixtureRoot"
  exit 0
}

$selected = if ($Integration.Count -gt 0) { @($inventory | Where-Object { $Integration -contains $_.id }) } else { $inventory }
foreach ($item in $selected) {
  $runId = [DateTime]::UtcNow.ToString("yyyyMMddTHHmmssZ")
  $bundleRoot = Join-Path $OutputRoot ("evidence\integration-proofs\{0}\{1}" -f $item.id, $runId)
  while (Test-Path -LiteralPath $bundleRoot) {
    Start-Sleep -Milliseconds 25
    $runId = [DateTime]::UtcNow.ToString("yyyyMMddTHHmmssfffZ")
    $bundleRoot = Join-Path $OutputRoot ("evidence\integration-proofs\{0}\{1}" -f $item.id, $runId)
  }
  New-Item -ItemType Directory -Force -Path $bundleRoot | Out-Null
  New-EvidenceDirectories -BundleRoot $bundleRoot
  Copy-Item -LiteralPath (Join-Path $PSScriptRoot "config\gates.json") -Destination (Join-Path $bundleRoot "configuration\gates.json")
  Write-JsonFile -Path (Join-Path $bundleRoot "configuration\integration.json") -Value $item
  foreach ($fixture in Get-ChildItem -LiteralPath $fixtureRoot -File -Recurse) {
    $fixtureRelative = $fixture.FullName.Substring($fixtureRoot.Length).TrimStart('\', '/')
    $fixtureDestination = Join-Path (Join-Path $bundleRoot "fixtures") $fixtureRelative
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $fixtureDestination) | Out-Null
    Copy-Item -LiteralPath $fixture.FullName -Destination $fixtureDestination
  }
  Copy-Item -LiteralPath (Join-Path $fixtureRoot "expected-valid-output.jsonl") -Destination (Join-Path $bundleRoot "expected\expected-valid-output.jsonl")
  Copy-Item -LiteralPath (Join-Path $fixtureRoot "expected-invalid-errors.jsonl") -Destination (Join-Path $bundleRoot "expected\expected-invalid-errors.jsonl")

  $fixtureManifest = Read-JsonFile (Join-Path $fixtureRoot "fixture-manifest.json")
  $startedAt = [DateTime]::UtcNow.ToString("o")
  $runManifest = [ordered]@{
    runId = $runId
    integration = $item.id
    integrationType = $item.type
    status = "NOT_TESTED"
    startedAt = $startedAt
    completedAt = $null
    durationSeconds = 0
    flowplaneCommit = $flowplaneCommit
    evidenceRepoCommit = $evidenceCommit
    flowplaneWorktree = $flowplaneGit
    evidenceRepoWorktree = $evidenceGit
    artifactId = $fixtureManifest.artifactId
    artifactVersion = $fixtureManifest.artifactVersion
    artifactHash = $fixtureManifest.artifactHash
    fixtureSet = $fixtureManifest.fixtureSet
    canonicalizationVersion = $fixtureManifest.canonicalizationVersion
    runtime = [ordered]@{ name = $item.runtime; version = "not recorded"; executionMode = "not executed"; containerImages = @() }
    host = [ordered]@{ os = "see environment.json"; cpu = "see environment.json"; logicalCores = 0; memoryBytes = 0; javaVersion = "see environment.json"; dockerVersion = "see environment.json" }
    protocolBoundary = $item.protocolBoundary
    sourceBoundary = "defined by integration adapter"
    sinkBoundary = "defined by integration adapter"
    validRecords = 0
    invalidRecords = 0
    successfulOutputs = 0
    errorOutputs = 0
    duplicates = 0
    unexplainedMissing = 0
    finalLag = 0
    unexpectedFailures = 0
  }
  Write-JsonFile -Path (Join-Path $bundleRoot "run-manifest.json") -Value $runManifest
  Write-JsonFile -Path (Join-Path $bundleRoot "environment.json") -Value (Get-HostEvidence)
  Write-JsonFile -Path (Join-Path $bundleRoot "versions.json") -Value ([ordered]@{ flowplane = $flowplaneGit; publicEvidence = $evidenceGit; runtimeVersion = "not recorded"; containerImages = @() })
  Write-Utf8NoBom -Path (Join-Path $bundleRoot "commands.txt") -Value ("PLANNED: " + $item.plannedCommand + "`n")
  Write-JsonFile -Path (Join-Path $bundleRoot "counts.json") -Value ([ordered]@{ attemptedInput = 0; acceptedInput = 0; successfulOutput = 0; intentionalInvalid = 0; errorOutput = 0; filtered = 0; duplicates = 0; unexpectedFailures = 0; pending = 0; finalLag = 0; retries = 0; timeouts = 0 })
  Write-JsonFile -Path (Join-Path $bundleRoot "final-state.json") -Value ([ordered]@{ captured = $false; runtimeHealthy = $false; pending = $null; finalLag = $null; reason = "Run has not executed." })
  Write-JsonFile -Path (Join-Path $bundleRoot "actual\not-executed.json") -Value ([ordered]@{ integration = $item.id; executionAttempted = $false; reason = "Prepared bundle only." })
  Write-Utf8NoBom -Path (Join-Path $bundleRoot "sanitized-logs\runner.log") -Value "Execution has not started.`n"
  Write-Utf8NoBom -Path (Join-Path $bundleRoot "summary.md") -Value "# $($item.id) verification`n`nStatus pending automated evaluation.`n"
  $reproduce = @(
    "param([string]`$FlowplaneRoot = 'C:\FlowPlaneNew\repositories\flowplane-controlplane')",
    "throw 'No isolated adapter is registered for this prepared bundle. Implement and review the adapter before execution.'"
  ) -join "`n"
  Write-Utf8NoBom -Path (Join-Path $bundleRoot "reproduce.ps1") -Value ($reproduce + "`n")

  $gateDocument = [ordered]@{
    schemaVersion = "flowplane.gate-assertions.v1"
    executionAttempted = $false
    commandExitCode = $null
    boundaryClass = "live"
    gates = New-BlankGateAssertions
    warnings = @("Prepared only; no live runtime was started.")
  }

  if ($Execute) {
    $adapter = Join-Path $PSScriptRoot ("adapters\{0}.ps1" -f $item.id)
    if (-not (Test-Path -LiteralPath $adapter -PathType Leaf)) {
      $gateDocument.warnings = @("No reviewed isolated adapter exists for $($item.id); execution was not attempted.")
    } else {
      $gateDocument.executionAttempted = $true
      $previousErrorAction = $ErrorActionPreference
      try {
        $ErrorActionPreference = "Continue"
        $transcript = & powershell -NoProfile -ExecutionPolicy Bypass -File $adapter -FlowplaneRoot $FlowplaneRoot -BundleRoot $bundleRoot -FixtureRoot $fixtureRoot -ValidRecordCount $ValidRecordCount -InvalidRecordCount $InvalidRecordCount -MinimumDurationSeconds $MinimumDurationSeconds 2>&1
        $exitCode = $LASTEXITCODE
      } finally {
        $ErrorActionPreference = $previousErrorAction
      }
      Write-Utf8NoBom -Path (Join-Path $bundleRoot "sanitized-logs\adapter.log") -Value ((ConvertTo-SafeLogText ($transcript -join "`n")) + "`n")
      $gateDocument.commandExitCode = $exitCode
      $adapterAssertions = Join-Path $bundleRoot "actual\adapter-gate-assertions.json"
      if (Test-Path -LiteralPath $adapterAssertions -PathType Leaf) {
        $provided = Read-JsonFile $adapterAssertions
        $gateDocument.gates = $provided.gates
        $gateDocument.boundaryClass = $provided.boundaryClass
        $gateDocument.warnings = @($provided.warnings)
      } else {
        $gateDocument.warnings = @("Adapter did not produce actual/adapter-gate-assertions.json; no gates were promoted.")
      }
      # Once an adapter has actually run, the prepare-only marker is no longer
      # truthful evidence, regardless of whether the run ultimately passes.
      $notExecutedMarker = Join-Path $bundleRoot "actual\not-executed.json"
      if (Test-Path -LiteralPath $notExecutedMarker -PathType Leaf) {
        Remove-Item -LiteralPath $notExecutedMarker -Force
      }
    }
  }
  Write-JsonFile -Path (Join-Path $bundleRoot "actual\gate-assertions.json") -Value $gateDocument
  $result = & (Join-Path $PSScriptRoot "Test-LiveVerificationBundle.ps1") -BundleRoot $bundleRoot -PassThru
  Write-Output "$($item.id): $($result.status) -> $bundleRoot"
}

Write-Output "No product or public evidence files were modified."
