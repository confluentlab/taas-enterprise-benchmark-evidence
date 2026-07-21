param(
  [Parameter(Mandatory)][string]$BundleRoot,
  [switch]$PassThru
)

. (Join-Path $PSScriptRoot "LiveVerification.Common.ps1")

$BundleRoot = Resolve-FullPath $BundleRoot -RequireExisting
$manifestPath = Join-Path $BundleRoot "run-manifest.json"
$assertionPath = Join-Path $BundleRoot "actual\gate-assertions.json"
$countsPath = Join-Path $BundleRoot "counts.json"
foreach ($requiredPath in @($manifestPath, $assertionPath, $countsPath)) {
  if (-not (Test-Path -LiteralPath $requiredPath -PathType Leaf)) { throw "Missing required evidence file: $requiredPath" }
}

$manifest = Read-JsonFile $manifestPath
$assertionsDocument = Read-JsonFile $assertionPath
$counts = Read-JsonFile $countsPath
$knownGateIds = Get-GateIds
$assertionsById = @{}
foreach ($assertion in @($assertionsDocument.gates)) {
  if ($knownGateIds -notcontains [string]$assertion.id) { throw "Unknown gate assertion: $($assertion.id)" }
  if ($assertionsById.ContainsKey([string]$assertion.id)) { throw "Duplicate gate assertion: $($assertion.id)" }
  $assertionsById[[string]$assertion.id] = $assertion
}

function Test-NonEmptyFile([string]$RelativePath) {
  $path = Join-Path $BundleRoot $RelativePath
  return (Test-Path -LiteralPath $path -PathType Leaf) -and ((Get-Item -LiteralPath $path).Length -gt 0)
}

function Set-AutomaticGate([string]$Id, [bool]$Passed, [string[]]$Evidence, [string]$Reason) {
  $assertionsById[$Id] = [pscustomobject]@{
    id = $Id
    applicable = $true
    required = $true
    passed = $Passed
    evidence = $Evidence
    reason = $Reason
  }
}

Set-AutomaticGate "evidence.environmentRecorded" (Test-NonEmptyFile "environment.json") @("environment.json") "Environment evidence is mandatory."
Set-AutomaticGate "evidence.commandsRecorded" (Test-NonEmptyFile "commands.txt") @("commands.txt") "Executed commands are mandatory."
$logFiles = @(Get-ChildItem -LiteralPath (Join-Path $BundleRoot "sanitized-logs") -File -ErrorAction SilentlyContinue)
Set-AutomaticGate "evidence.logsPreserved" ($logFiles.Count -gt 0) @($logFiles | ForEach-Object { Get-RelativeEvidencePath $BundleRoot $_.FullName }) "At least one sanitized runtime log is mandatory."
$rawFiles = @(Get-ChildItem -LiteralPath (Join-Path $BundleRoot "actual") -File -ErrorAction SilentlyContinue | Where-Object Name -ne "gate-assertions.json")
Set-AutomaticGate "evidence.rawOutputsPreserved" ($rawFiles.Count -gt 0) @($rawFiles | ForEach-Object { Get-RelativeEvidencePath $BundleRoot $_.FullName }) "At least one raw result is mandatory."
Set-AutomaticGate "evidence.reproductionScriptAvailable" (Test-NonEmptyFile "reproduce.ps1") @("reproduce.ps1") "A bundle-local reproduction command is mandatory."
Write-Checksums -BundleRoot $BundleRoot
Set-AutomaticGate "evidence.checksumsVerified" (Test-Checksums -BundleRoot $BundleRoot) @("hashes.sha256") "The evaluator recomputed all listed SHA-256 values."

$inputTotal = [int64]$counts.attemptedInput
$accounted = [int64]$counts.successfulOutput + [int64]$counts.errorOutput + [int64]$counts.filtered
$reconciled = ($inputTotal -eq $accounted)
if ($assertionsById.ContainsKey("accounting.inputReconciled")) {
  $assertionsById["accounting.inputReconciled"].passed = [bool]$assertionsById["accounting.inputReconciled"].passed -and $reconciled
  if (-not $reconciled) { $assertionsById["accounting.inputReconciled"].reason = "attemptedInput ($inputTotal) does not equal successfulOutput + errorOutput + filtered ($accounted)." }
}

$gates = @()
foreach ($id in $knownGateIds) {
  $gate = if ($assertionsById.ContainsKey($id)) { $assertionsById[$id] } else {
    [pscustomobject]@{ id = $id; applicable = $true; required = $true; passed = $false; evidence = @(); reason = "No assertion was recorded." }
  }
  $applicable = if ($null -eq $gate.applicable) { $true } else { [bool]$gate.applicable }
  $required = if ($null -eq $gate.required) { $true } else { [bool]$gate.required }
  $passed = [bool]$gate.passed
  $evidence = @($gate.evidence)
  $reason = [string]$gate.reason
  if (-not $applicable) {
    if ([string]::IsNullOrWhiteSpace($reason)) {
      $passed = $false
      $applicable = $true
      $reason = "A non-applicable gate requires an explicit technical reason."
    }
  } elseif ($passed) {
    if ($evidence.Count -eq 0) {
      $passed = $false
      $reason = "A passed gate must cite bundle-local evidence."
    } else {
      foreach ($relative in $evidence) {
        $candidate = Join-Path $BundleRoot ([string]$relative).Replace('/', [IO.Path]::DirectorySeparatorChar)
        if (-not (Test-Path -LiteralPath $candidate)) {
          $passed = $false
          $reason = "Cited evidence does not exist: $relative"
          break
        }
      }
    }
  }
  $gates += [ordered]@{ id = $id; required = $required; applicable = $applicable; passed = $passed; evidence = $evidence; reason = $reason }
}

$functionalMinimumMet = ([int64]$counts.successfulOutput -ge 100) -and ([int64]$counts.intentionalInvalid -ge 10) -and ([int64]$counts.errorOutput -ge 10)
$exactState = ([int64]$counts.duplicates -eq 0) -and ([int64]$counts.unexpectedFailures -eq 0) -and ([int64]$counts.pending -eq 0)
$lagApplicableGate = @($gates | Where-Object id -eq "state.finalLagZero")[0]
$lagAcceptable = (-not $lagApplicableGate.applicable) -or ([int64]$counts.finalLag -eq 0)
$failed = @($gates | Where-Object { $_.required -and $_.applicable -and -not $_.passed } | ForEach-Object id)
$executionAttempted = [bool]$assertionsDocument.executionAttempted
$commandExitCode = if ($null -eq $assertionsDocument.commandExitCode) { -1 } else { [int]$assertionsDocument.commandExitCode }
$boundaryClass = [string]$assertionsDocument.boundaryClass

if (-not $executionAttempted) {
  $status = "NOT_TESTED"
} elseif ($commandExitCode -ne 0) {
  $status = "PRESERVED_FAILURE"
} elseif ($boundaryClass -eq "contract") {
  $status = if ($failed.Count -eq 0) { "CONTRACT_VERIFIED" } else { "INCOMPLETE" }
} elseif ($boundaryClass -eq "measured") {
  $status = if ($failed.Count -eq 0) { "MEASURED" } else { "MEASURED_NOT_QUALIFIED" }
} elseif ($failed.Count -eq 0 -and $functionalMinimumMet -and $reconciled -and $exactState -and $lagAcceptable) {
  $status = "LIVE_LOCAL_VERIFIED"
} else {
  $status = "INCOMPLETE"
}

$manifest.status = $status
$manifest.completedAt = [DateTime]::UtcNow.ToString("o")
if ($manifest.startedAt) {
  $manifest.durationSeconds = [Math]::Round(([DateTime]::Parse($manifest.completedAt).ToUniversalTime() - [DateTime]::Parse($manifest.startedAt).ToUniversalTime()).TotalSeconds, 3)
}
Write-JsonFile -Path $manifestPath -Value $manifest

$summaryLines = @(
  "# $($manifest.integration) live local verification",
  "",
  "- Status: ``$status``",
  "- Run ID: ``$($manifest.runId)``",
  "- Boundary: $($manifest.protocolBoundary)",
  "- Successful outputs: $($counts.successfulOutput)",
  "- Intentional invalid records: $($counts.intentionalInvalid)",
  "- Error outputs: $($counts.errorOutput)",
  "- Unexplained missing: $([Math]::Max(0, $inputTotal - $accounted))",
  "- Duplicates: $($counts.duplicates)",
  "- Final lag: $($counts.finalLag)",
  "- Failed required gates: $($failed.Count)",
  ""
)
if ($failed.Count -gt 0) {
  $summaryLines += "## Failed required gates"
  $summaryLines += ""
  $summaryLines += @($failed | ForEach-Object { "- ``$_``" })
  $summaryLines += ""
}
$summaryLines += "This status was generated by the evaluator. A process exit code alone cannot produce ``LIVE_LOCAL_VERIFIED``."
Write-Utf8NoBom -Path (Join-Path $BundleRoot "summary.md") -Value (($summaryLines -join "`n") + "`n")

Write-Checksums -BundleRoot $BundleRoot
$checksumsPassed = Test-Checksums -BundleRoot $BundleRoot
foreach ($gate in $gates) {
  if ($gate.id -eq "evidence.checksumsVerified") {
    $gate.passed = $checksumsPassed
    $gate.evidence = @("hashes.sha256")
    $gate.reason = "The evaluator recomputed all listed SHA-256 values."
  }
}
$failed = @($gates | Where-Object { $_.required -and $_.applicable -and -not $_.passed } | ForEach-Object id)
if ($status -eq "LIVE_LOCAL_VERIFIED" -and $failed.Count -gt 0) {
  $status = "INCOMPLETE"
  $manifest.status = $status
  Write-JsonFile -Path $manifestPath -Value $manifest
  Write-Checksums -BundleRoot $BundleRoot
  $checksumsPassed = Test-Checksums -BundleRoot $BundleRoot
}

$result = [ordered]@{
  schemaVersion = "flowplane.verification-result.v1"
  status = $status
  functionalMinimumMet = $functionalMinimumMet
  exactAccounting = $reconciled
  checksumsVerified = $checksumsPassed
  gates = $gates
  failedRequiredGates = $failed
  warnings = @($assertionsDocument.warnings)
}
Write-JsonFile -Path (Join-Path $BundleRoot "verification-result.json") -Value $result

if ($PassThru) { return $result }
Write-Output "$status $BundleRoot"
if ($status -notin @("LIVE_LOCAL_VERIFIED", "CONTRACT_VERIFIED", "MEASURED")) { exit 2 }
