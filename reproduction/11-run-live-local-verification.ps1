param(
  [string]$FlowplaneRoot = "C:\FlowPlaneNew\repositories\flowplane-controlplane",
  [string]$PublicEvidenceRoot = "C:\FlowPlaneNew\flowplane-enterprise-benchmark-evidence",
  [string[]]$Integration = @(),
  [ValidateRange(1,100000)][int]$ValidRecordCount = 100,
  [ValidateRange(1,10000)][int]$InvalidRecordCount = 10,
  [ValidateRange(0,3600)][int]$MinimumDurationSeconds = 0,
  [switch]$PrepareRunBundles,
  [switch]$Execute
)

$runner = Join-Path $PSScriptRoot "live-local-verification\Invoke-LiveLocalVerification.ps1"
$arguments = @{
  FlowplaneRoot = $FlowplaneRoot
  PublicEvidenceRoot = $PublicEvidenceRoot
  PrepareRunBundles = $PrepareRunBundles
  Execute = $Execute
  ValidRecordCount = $ValidRecordCount
  InvalidRecordCount = $InvalidRecordCount
  MinimumDurationSeconds = $MinimumDurationSeconds
}
if ($Integration.Count -gt 0) { $arguments.Integration = $Integration }
& $runner @arguments
