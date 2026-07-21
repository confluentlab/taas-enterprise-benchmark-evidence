param([Parameter(Mandatory)][string]$FlowplaneRoot,[Parameter(Mandatory)][string]$BundleRoot,[Parameter(Mandatory)][string]$FixtureRoot)
& (Join-Path $PSScriptRoot "runtime-surface.ps1") -FlowplaneRoot $FlowplaneRoot -BundleRoot $BundleRoot -FixtureRoot $FixtureRoot -IntegrationId "serverless-aws"
exit $LASTEXITCODE
