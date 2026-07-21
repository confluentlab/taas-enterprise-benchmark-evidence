param([Parameter(Mandatory)][string]$FlowplaneRoot,[Parameter(Mandatory)][string]$BundleRoot,[Parameter(Mandatory)][string]$FixtureRoot,[int]$ValidRecordCount=100,[int]$InvalidRecordCount=10,[int]$MinimumDurationSeconds=0)
& (Join-Path $PSScriptRoot "runtime-surface.ps1") -FlowplaneRoot $FlowplaneRoot -BundleRoot $BundleRoot -FixtureRoot $FixtureRoot -IntegrationId "http-batch" -ValidRecordCount $ValidRecordCount -InvalidRecordCount $InvalidRecordCount -MinimumDurationSeconds $MinimumDurationSeconds
exit $LASTEXITCODE
