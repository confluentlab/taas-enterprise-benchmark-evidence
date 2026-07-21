param(
  [Parameter(Mandatory)][string]$FlowplaneRoot,
  [Parameter(Mandatory)][string]$BundleRoot,
  [Parameter(Mandatory)][string]$FixtureRoot
)

$ErrorActionPreference = "Stop"
& powershell -NoProfile -ExecutionPolicy Bypass -File (Join-Path $PSScriptRoot "redpanda-connect.ps1") `
  -FlowplaneRoot $FlowplaneRoot `
  -BundleRoot $BundleRoot `
  -FixtureRoot $FixtureRoot `
  -IntegrationId spring-cloud-stream
exit $LASTEXITCODE
