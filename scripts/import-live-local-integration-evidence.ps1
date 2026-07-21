[CmdletBinding()]
param(
    [string]$SourceRoot = 'C:\FlowPlaneNew\video-generation-scripts-copy',
    [string]$DestinationRoot
)

$ErrorActionPreference = 'Stop'
if ([string]::IsNullOrWhiteSpace($DestinationRoot)) {
    $DestinationRoot = Split-Path -Parent $PSScriptRoot
}

$artifactRoot = Join-Path $SourceRoot 'artifacts\live-local-verification'
$sourceProofRoot = Join-Path $artifactRoot 'evidence'
$destinationProofRoot = Join-Path $DestinationRoot 'evidence'

$integrationRuns = [ordered]@{
    'pulsar'                    = @('20260720T135944Z')
    'redpanda-connect'          = @('20260720T160552Z')
    'logstash'                  = @('20260720T161054Z')
    'camel'                     = @('20260720T161756Z')
    'spring-cloud-stream'       = @('20260720T163032Z')
    'nifi'                      = @('20260720T163618Z')
    'spark-structured-streaming'= @('20260720T164322Z')
    'beam-directrunner'         = @('20260720T170558Z')
    'kafka-connect'             = @('20260720T171900Z')
    'kafka-streams'             = @('20260720T172502Z')
    'flink'                     = @('20260720T174759Z')
    'bento-warpstream'          = @('20260720T175209Z')
    'activemq-classic'          = @('20260720T180530Z')
    'nats-jetstream'            = @('20260720T181423Z')
    'redis-streams'             = @('20260720T182351Z')
    'rabbitmq-streams'          = @('20260720T183755Z')
    'emqx-mqtt'                 = @('20260720T185514Z')
    'rocketmq'                  = @('20260720T192342Z')
    'activemq-artemis'          = @('20260720T194319Z')
    'vector'                    = @('20260720T200000Z')
    'opentelemetry'             = @('20260720T200343Z')
    'debezium'                  = @('20260720T202552Z')
    'embedded-spring'           = @('20260721T050302Z', '20260721T054132Z')
    'http-single'               = @('20260721T045008Z', '20260721T054134Z')
    'http-batch'                = @('20260721T044908Z', '20260721T054136Z')
    'grpc-batch'                = @('20260721T045105Z', '20260721T054138Z')
    'grpc-streaming'            = @('20260721T045213Z', '20260721T054141Z')
    'serverless-aws'            = @('20260721T050358Z')
    'serverless-azure'          = @('20260721T045510Z')
    'serverless-gcp'            = @('20260721T045957Z')
}

$triggerRuns = [ordered]@{
    'azure-queue'    = @('20260721t054012z')
    'azure-eventhub' = @('20260721t055132z')
    'gcp-pubsub'     = @('20260721t060215z')
}

function Copy-ChecksummedBundle {
    param(
        [Parameter(Mandatory)] [string]$Source,
        [Parameter(Mandatory)] [string]$Destination,
        [Parameter(Mandatory)] [string]$ChecksumName
    )

    $checksumPath = Join-Path $Source $ChecksumName
    if (-not (Test-Path -LiteralPath $checksumPath -PathType Leaf)) {
        throw "Checksum manifest not found: $checksumPath"
    }

    New-Item -ItemType Directory -Path $Destination -Force | Out-Null
    $entries = foreach ($line in Get-Content -LiteralPath $checksumPath) {
        if ($line -notmatch '^([0-9a-fA-F]{64})\s{2}(.+)$') {
            throw "Invalid checksum line in ${checksumPath}: $line"
        }
        [pscustomobject]@{ Hash = $Matches[1].ToLowerInvariant(); RelativePath = $Matches[2] }
    }

    foreach ($entry in $entries) {
        $sourceFile = Join-Path $Source ($entry.RelativePath -replace '/', '\')
        if (-not (Test-Path -LiteralPath $sourceFile -PathType Leaf)) {
            throw "Manifest-listed source file not found: $sourceFile"
        }
        $destinationFile = Join-Path $Destination ($entry.RelativePath -replace '/', '\')
        New-Item -ItemType Directory -Path (Split-Path -Parent $destinationFile) -Force | Out-Null
        Copy-Item -LiteralPath $sourceFile -Destination $destinationFile -Force
        $actualHash = (Get-FileHash -LiteralPath $destinationFile -Algorithm SHA256).Hash.ToLowerInvariant()
        if ($actualHash -ne $entry.Hash) {
            throw "Checksum mismatch after copy: $destinationFile"
        }
    }

    Copy-Item -LiteralPath $checksumPath -Destination (Join-Path $Destination $ChecksumName) -Force
    $verificationResult = Join-Path $Source 'verification-result.json'
    if (Test-Path -LiteralPath $verificationResult -PathType Leaf) {
        Copy-Item -LiteralPath $verificationResult -Destination (Join-Path $Destination 'verification-result.json') -Force
    }
    [pscustomobject]@{ Destination = $Destination; FileCount = $entries.Count }
}

$copied = @()
foreach ($integration in $integrationRuns.Keys) {
    foreach ($runId in $integrationRuns[$integration]) {
        $source = Join-Path $sourceProofRoot "integration-proofs\$integration\$runId"
        $destination = Join-Path $destinationProofRoot "integration-proofs\$integration\runs\$runId"
        $copied += Copy-ChecksummedBundle -Source $source -Destination $destination -ChecksumName 'hashes.sha256'
    }
}

foreach ($trigger in $triggerRuns.Keys) {
    foreach ($runId in $triggerRuns[$trigger]) {
        $source = Join-Path $sourceProofRoot "trigger-proofs\$trigger\$runId"
        $destination = Join-Path $destinationProofRoot "trigger-proofs\$trigger\runs\$runId"
        $copied += Copy-ChecksummedBundle -Source $source -Destination $destination -ChecksumName 'SHA256SUMS.txt'
    }
}

$harnessSource = Join-Path $SourceRoot 'scripts\demo\live-local-verification'
$harnessDestination = Join-Path $DestinationRoot 'reproduction\live-local-verification'
$sourceFiles = Get-ChildItem -LiteralPath $harnessSource -Recurse -File | Where-Object {
    $_.FullName -notmatch '[\\/]node_modules[\\/]' -and
    $_.FullName -notmatch '[\\/]__pycache__[\\/]'
}
foreach ($sourceFile in $sourceFiles) {
    $relative = $sourceFile.FullName.Substring($harnessSource.Length).TrimStart('\')
    $destinationFile = Join-Path $harnessDestination $relative
    New-Item -ItemType Directory -Path (Split-Path -Parent $destinationFile) -Force | Out-Null
    Copy-Item -LiteralPath $sourceFile.FullName -Destination $destinationFile -Force
}

Copy-Item -LiteralPath (Join-Path $SourceRoot 'scripts\demo\11-run-live-local-verification.ps1') `
    -Destination (Join-Path $DestinationRoot 'reproduction\11-run-live-local-verification.ps1') -Force

Copy-Item -LiteralPath (Join-Path $artifactRoot 'canonical-fixture') `
    -Destination (Join-Path $DestinationRoot 'reproduction') -Recurse -Force

$supplementDestination = Join-Path $destinationProofRoot 'live-local-supplement'
New-Item -ItemType Directory -Path $supplementDestination -Force | Out-Null
Copy-Item -LiteralPath (Join-Path $artifactRoot 'audit') `
    -Destination $supplementDestination -Recurse -Force
Copy-Item -LiteralPath (Join-Path $sourceProofRoot 'remaining-scope\screenshots') `
    -Destination $supplementDestination -Recurse -Force

Write-Output ("Copied {0} checksum-verified run bundles and {1} harness source files." -f $copied.Count, $sourceFiles.Count)
