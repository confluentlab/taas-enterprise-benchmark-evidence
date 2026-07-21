param(
  [Parameter(Mandatory)][string]$FlowplaneRoot,
  [Parameter(Mandatory)][string]$BundleRoot,
  [Parameter(Mandatory)][string]$FixtureRoot
)

& (Join-Path $PSScriptRoot "pulsar.ps1") -FlowplaneRoot $FlowplaneRoot -BundleRoot $BundleRoot -FixtureRoot $FixtureRoot -IntegrationId "activemq-classic"
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
