$ErrorActionPreference = "Stop"

$script:LiveVerificationRoot = (Resolve-Path $PSScriptRoot).Path
$script:ScriptCopyRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..\..")).Path

function Resolve-FullPath {
  param([Parameter(Mandatory)][string]$Path, [switch]$RequireExisting)
  $full = [IO.Path]::GetFullPath($Path)
  if ($RequireExisting -and -not (Test-Path -LiteralPath $full)) {
    throw "Required path does not exist: $full"
  }
  return $full.TrimEnd([IO.Path]::DirectorySeparatorChar, [IO.Path]::AltDirectorySeparatorChar)
}

function Test-PathWithin {
  param([Parameter(Mandatory)][string]$Child, [Parameter(Mandatory)][string]$Parent)
  $childFull = (Resolve-FullPath $Child) + [IO.Path]::DirectorySeparatorChar
  $parentFull = (Resolve-FullPath $Parent) + [IO.Path]::DirectorySeparatorChar
  return $childFull.StartsWith($parentFull, [StringComparison]::OrdinalIgnoreCase)
}

function Assert-IsolatedOutputRoot {
  param(
    [Parameter(Mandatory)][string]$OutputRoot,
    [Parameter(Mandatory)][string]$FlowplaneRoot,
    [Parameter(Mandatory)][string]$PublicEvidenceRoot
  )
  if ((Test-PathWithin $OutputRoot $FlowplaneRoot) -or (Test-PathWithin $OutputRoot $PublicEvidenceRoot)) {
    throw "OutputRoot must not be inside the product or public evidence repository: $OutputRoot"
  }
  if (-not (Test-PathWithin $OutputRoot $script:ScriptCopyRoot)) {
    throw "OutputRoot must remain inside the script-only copy: $script:ScriptCopyRoot"
  }
}

function Get-GitRevision {
  param([Parameter(Mandatory)][string]$Root)
  try {
    $value = & git -C $Root rev-parse HEAD 2>$null
    if ($LASTEXITCODE -eq 0) { return ($value | Select-Object -First 1).Trim() }
  } catch {}
  return "unknown"
}

function Get-TextSha256 {
  param([AllowEmptyString()][string]$Value)
  $bytes = [Text.Encoding]::UTF8.GetBytes($Value)
  $hasher = [Security.Cryptography.SHA256]::Create()
  try {
    return ([BitConverter]::ToString($hasher.ComputeHash($bytes))).Replace("-", "").ToLowerInvariant()
  } finally {
    $hasher.Dispose()
  }
}

function Get-GitState {
  param([Parameter(Mandatory)][string]$Root)
  $revision = Get-GitRevision $Root
  try {
    $entries = @(& git -C $Root status --porcelain=v1 --untracked-files=all 2>$null)
    if ($LASTEXITCODE -ne 0) { throw "git status failed" }
    $normalized = ($entries -join "`n")
    return [ordered]@{
      commit = $revision
      dirty = ($entries.Count -gt 0)
      statusEntryCount = $entries.Count
      statusDigestSha256 = Get-TextSha256 $normalized
    }
  } catch {
    return [ordered]@{ commit = $revision; dirty = $null; statusEntryCount = $null; statusDigestSha256 = $null }
  }
}

function Get-Sha256 {
  param([Parameter(Mandatory)][string]$Path)
  return (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
}

function Write-Utf8NoBom {
  param([Parameter(Mandatory)][string]$Path, [Parameter(Mandatory)][string]$Value)
  $parent = Split-Path -Parent $Path
  if (-not [string]::IsNullOrWhiteSpace($parent)) {
    New-Item -ItemType Directory -Force -Path $parent | Out-Null
  }
  [IO.File]::WriteAllText($Path, $Value, [Text.UTF8Encoding]::new($false))
}

function Write-JsonFile {
  param([Parameter(Mandatory)][string]$Path, [Parameter(Mandatory)]$Value)
  Write-Utf8NoBom -Path $Path -Value (($Value | ConvertTo-Json -Depth 100) + "`n")
}

function Read-JsonFile {
  param([Parameter(Mandatory)][string]$Path)
  return Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json
}

function New-EvidenceDirectories {
  param([Parameter(Mandatory)][string]$BundleRoot)
  foreach ($name in @("configuration", "fixtures", "expected", "actual", "sanitized-logs", "metrics", "screenshots")) {
    New-Item -ItemType Directory -Force -Path (Join-Path $BundleRoot $name) | Out-Null
  }
}

function Get-HostEvidence {
  $computer = Get-CimInstance Win32_ComputerSystem
  $os = Get-CimInstance Win32_OperatingSystem
  $java = try { (& java -version 2>&1) -join "`n" } catch { "unavailable" }
  $docker = try { (& docker version --format '{{.Server.Version}}' 2>$null) -join "`n" } catch { "unavailable" }
  return [ordered]@{
    capturedAt = [DateTime]::UtcNow.ToString("o")
    os = $os.Caption
    osVersion = $os.Version
    cpu = (Get-CimInstance Win32_Processor | ForEach-Object Name) -join "; "
    logicalCores = [int]$computer.NumberOfLogicalProcessors
    memoryBytes = [int64]$computer.TotalPhysicalMemory
    javaVersion = $java
    dockerVersion = $docker
  }
}

function Get-Inventory {
  return (Read-JsonFile (Join-Path $script:LiveVerificationRoot "config\integrations.json")).integrations
}

function Get-GateIds {
  return @((Read-JsonFile (Join-Path $script:LiveVerificationRoot "config\gates.json")).gates)
}

function New-BlankGateAssertions {
  $items = @()
  foreach ($id in Get-GateIds) {
    $items += [ordered]@{
      id = $id
      applicable = $true
      required = $true
      passed = $false
      evidence = @()
      reason = "No adapter assertion was recorded."
    }
  }
  return $items
}

function Write-Checksums {
  param([Parameter(Mandatory)][string]$BundleRoot)
  $excluded = @("hashes.sha256", "verification-result.json")
  $lines = foreach ($file in Get-ChildItem -LiteralPath $BundleRoot -File -Recurse | Sort-Object FullName) {
    $relative = $file.FullName.Substring($BundleRoot.Length).TrimStart('\', '/').Replace('\', '/')
    if ($excluded -contains $relative) { continue }
    "$(Get-Sha256 $file.FullName)  $relative"
  }
  Write-Utf8NoBom -Path (Join-Path $BundleRoot "hashes.sha256") -Value (($lines -join "`n") + "`n")
}

function Test-Checksums {
  param([Parameter(Mandatory)][string]$BundleRoot)
  $manifest = Join-Path $BundleRoot "hashes.sha256"
  if (-not (Test-Path -LiteralPath $manifest -PathType Leaf)) { return $false }
  foreach ($line in Get-Content -LiteralPath $manifest) {
    if ([string]::IsNullOrWhiteSpace($line)) { continue }
    if ($line -notmatch '^([a-fA-F0-9]{64})  (.+)$') { return $false }
    $path = Join-Path $BundleRoot ($Matches[2].Replace('/', [IO.Path]::DirectorySeparatorChar))
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) { return $false }
    if ((Get-Sha256 $path) -ne $Matches[1].ToLowerInvariant()) { return $false }
  }
  return $true
}

function ConvertTo-SafeLogText {
  param([AllowEmptyString()][string]$Text)
  if ($null -eq $Text) { return "" }
  $safe = $Text
  $safe = [regex]::Replace($safe, '(?i)(authorization\s*[:=]\s*bearer\s+)[^\s\"'']+', '$1[REDACTED]')
  $safe = [regex]::Replace($safe, '(?i)(client[_-]?secret|password|token|api[_-]?key)(\s*[:=]\s*)[^\s,;]+', '$1$2[REDACTED]')
  $safe = [regex]::Replace($safe, '(?i)[A-Za-z]:\\Users\\[^\\\s]+', '[USER_HOME]')
  return $safe
}

function Get-RelativeEvidencePath {
  param([Parameter(Mandatory)][string]$BundleRoot, [Parameter(Mandatory)][string]$Path)
  $full = Resolve-FullPath $Path
  if (-not (Test-PathWithin $full $BundleRoot)) { throw "Evidence path is outside the run bundle: $full" }
  return $full.Substring((Resolve-FullPath $BundleRoot).Length).TrimStart('\', '/').Replace('\', '/')
}
