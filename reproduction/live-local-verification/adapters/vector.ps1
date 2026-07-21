param(
  [Parameter(Mandatory)][string]$FlowplaneRoot,
  [Parameter(Mandatory)][string]$BundleRoot,
  [Parameter(Mandatory)][string]$FixtureRoot
)

& (Join-Path $PSScriptRoot "redpanda-connect.ps1") -FlowplaneRoot $FlowplaneRoot -BundleRoot $BundleRoot -FixtureRoot $FixtureRoot -IntegrationId "vector"
exit $LASTEXITCODE
