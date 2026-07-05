param(
  [string]$SuiteName = "flowplane-1mb-ramp-suite",
  [string]$CorpusPath = "C:\FlowPlanenew\FLOWPLANE_Controlplane\bench-runs\multi-runtime-1mb\flowplane-1mb-connect-flink-50unique-1000probe-20260612105548\source-payloads-50unique-1mb.jsonl",
  [int[]]$Rates = @(25, 50, 75, 100),
  [int]$DurationSecondsPerRate = 300,
  [string]$TenantId = "acme-corp"
)

$ErrorActionPreference = "Stop"
$root = Resolve-Path "."
$suiteDir = Join-Path $root "bench-runs\multi-runtime-1mb\$SuiteName"
New-Item -ItemType Directory -Force -Path $suiteDir | Out-Null

function Invoke-Json([string]$Method, [string]$Uri, $Body = $null) {
  $params = @{
    Method = $Method
    Uri = $Uri
    ContentType = "application/json"
  }
  if ($null -ne $Body) {
    $params.Body = $Body | ConvertTo-Json -Depth 40 -Compress
  }
  Invoke-RestMethod @params
}

function Get-AuthToken {
  $login = Invoke-Json Post "http://127.0.0.1:8081/api/v1/auth/login" @{ username = "admin@flowplane.local"; password = "admin123" }
  [string]$login.accessToken
}

function Retarget-MongoConnector([string]$RawTopic, [string]$DlqTopic, [string]$Collection, [string]$RunDir) {
  $config = Invoke-RestMethod "http://localhost:8084/connectors/flowplane-1mb-mongo-sink-runtime/config"
  $config.topics = $RawTopic
  $config.collection = $Collection
  $config.'errors.deadletterqueue.topic.name' = $DlqTopic
  $config.'transforms.flowplane.flowplane.error.topic' = $DlqTopic
  $config | ConvertTo-Json -Depth 30 | Set-Content -LiteralPath (Join-Path $RunDir "mongo-config.json")
  Invoke-RestMethod `
    -Method Put `
    -Uri "http://localhost:8084/connectors/flowplane-1mb-mongo-sink-runtime/config" `
    -ContentType "application/json" `
    -Body ($config | ConvertTo-Json -Depth 30 -Compress) | Out-Null
}

function Ensure-Topic([string]$Topic) {
  docker exec flowplane-kafka kafka-topics `
    --bootstrap-server kafka:9092 `
    --create `
    --if-not-exists `
    --topic $Topic `
    --partitions 24 `
    --replication-factor 1 `
    --config max.message.bytes=2097152 `
    --config retention.ms=14400000 | Out-Null
}

function Retarget-Flink([string]$RawTopic, [string]$OutputTopic, [string]$DlqTopic, [string]$Token) {
  $jobs = (Invoke-RestMethod "http://localhost:8089/jobs").jobs | Where-Object { $_.status -eq "RUNNING" }
  foreach ($job in $jobs) {
    docker exec flowplane-flink-jobmanager /opt/flink/bin/flink cancel $job.id | Out-Null
  }
  Start-Sleep -Seconds 3
  powershell -NoProfile -ExecutionPolicy Bypass -File ".\scripts\dev\deploy-flink-runtime.ps1" `
    -SkipBuild `
    -RuntimeId "flink-runtime-1mb-breakpoint" `
    -RuntimeName "Flink Runtime 1MB Breakpoint" `
    -OwnerTeam "Quality Engineering" `
    -ProjectId "1mb-breakpoint" `
    -TenantId $TenantId `
    -InputTopic $RawTopic `
    -OutputTopic $OutputTopic `
    -ErrorTopic $DlqTopic `
    -AuthToken $Token `
    -RuntimeClientSecret "flowplane-runtime-dev-secret" `
    -OutputShape "JSON_STRING" `
    -FlinkOutputMode "JSON_STRING" `
    -Parallelism 2 | Out-Null
}

$suite = [ordered]@{
  suiteName = $SuiteName
  startedAt = (Get-Date).ToUniversalTime().ToString("o")
  corpusPath = $CorpusPath
  rates = $Rates
  durationSecondsPerRate = $DurationSecondsPerRate
  runs = @()
}

$token = Get-AuthToken

foreach ($rate in $Rates) {
  $ts = Get-Date -Format "yyyyMMdd-HHmmss"
  $runName = "$SuiteName-${rate}rps-$ts"
  $runDir = Join-Path $suiteDir $runName
  New-Item -ItemType Directory -Force -Path $runDir | Out-Null
  $records = $rate * $DurationSecondsPerRate
  $raw = "raw.flowplane.1mb.ramp-${rate}rps-$ts"
  $flinkOut = "transformed.flowplane.1mb.flink-ramp-${rate}rps-$ts"
  $connectDlq = "dlq.flowplane.1mb.connect-ramp-${rate}rps-$ts"
  $flinkDlq = "dlq.flowplane.1mb.flink-ramp-${rate}rps-$ts"
  $mongoCollection = "orders_1mb_ramp_${rate}rps_$ts"

  [ordered]@{
    runName = $runName
    rate = $rate
    records = $records
    rawTopic = $raw
    flinkOutputTopic = $flinkOut
    connectDlqTopic = $connectDlq
    flinkDlqTopic = $flinkDlq
    mongoDatabase = "flowplane_sink"
    mongoCollection = $mongoCollection
    startedAt = (Get-Date).ToUniversalTime().ToString("o")
  } | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath (Join-Path $runDir "level-start.json")

  foreach ($topic in @($raw, $flinkOut, $connectDlq, $flinkDlq)) {
    Ensure-Topic $topic
  }
  Retarget-MongoConnector $raw $connectDlq $mongoCollection $runDir
  Invoke-RestMethod `
    -Method Post `
    -Uri "http://localhost:8084/connectors/flowplane-1mb-mongo-sink-runtime/restart?includeTasks=true&onlyFailed=false" `
    -ContentType "application/json" `
    -Body "" | Out-Null
  Retarget-Flink $raw $flinkOut $flinkDlq $token

  $samples = [Math]::Ceiling($DurationSecondsPerRate / 10) + 18
  $monitor = Start-Process powershell.exe `
    -WindowStyle Hidden `
    -ArgumentList @(
      "-NoProfile", "-ExecutionPolicy", "Bypass",
      "-File", (Resolve-Path ".\scripts\qa\monitor-1mb-platform-run.ps1"),
      "-RunDir", $runDir,
      "-RawTopic", $raw,
      "-FlinkOutputTopic", $flinkOut,
      "-ConnectDlqTopic", $connectDlq,
      "-FlinkDlqTopic", $flinkDlq,
      "-IntervalSeconds", "10",
      "-Samples", "$samples"
    ) `
    -RedirectStandardOutput (Join-Path $runDir "monitor-process.out.log") `
    -RedirectStandardError (Join-Path $runDir "monitor-process.err.log") `
    -PassThru

  & ".\scripts\qa\run-1mb-platform-run.ps1" `
    -RunName $runName `
    -RunDir $runDir `
    -RawTopic $raw `
    -FlinkOutputTopic $flinkOut `
    -ConnectDlqTopic $connectDlq `
    -FlinkDlqTopic $flinkDlq `
    -CorpusPath $CorpusPath `
    -RecordCount $records `
    -TargetRate $rate `
    -Partitions 24 `
    -Compression "lz4"

  Stop-Process -Id $monitor.Id -Force -ErrorAction SilentlyContinue
  Invoke-RestMethod "http://localhost:8084/connectors/flowplane-1mb-mongo-sink-runtime/status" |
    ConvertTo-Json -Depth 20 |
    Set-Content -LiteralPath (Join-Path $runDir "connector-status-end.json")
  Invoke-RestMethod "http://localhost:8089/jobs" |
    ConvertTo-Json -Depth 20 |
    Set-Content -LiteralPath (Join-Path $runDir "flink-jobs-end.json")

  $suite.runs += [ordered]@{
    runName = $runName
    rate = $rate
    runDir = $runDir
    completedAt = (Get-Date).ToUniversalTime().ToString("o")
  }
  $suite | ConvertTo-Json -Depth 20 | Set-Content -LiteralPath (Join-Path $suiteDir "suite-progress.json")
}

$suite.completedAt = (Get-Date).ToUniversalTime().ToString("o")
$suite | ConvertTo-Json -Depth 20 | Set-Content -LiteralPath (Join-Path $suiteDir "suite-summary.json")
$suite | ConvertTo-Json -Depth 20
