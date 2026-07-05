param(
  [Parameter(Mandatory = $true)]
  [string]$RunName,

  [Parameter(Mandatory = $true)]
  [string]$RawTopic,

  [Parameter(Mandatory = $true)]
  [string]$FlinkOutputTopic,

  [Parameter(Mandatory = $true)]
  [string]$ConnectDlqTopic,

  [Parameter(Mandatory = $true)]
  [string]$FlinkDlqTopic,

  [Parameter(Mandatory = $true)]
  [string]$CorpusPath,

  [int]$RecordCount = 15000,
  [int]$TargetRate = 50,
  [int]$Partitions = 24,
  [int]$MaxMessageBytes = 2097152,
  [string]$Compression = "lz4",
  [int]$ProducerMaxInFlight = 5,
  [string]$RunDir = ""
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($RunDir)) {
  $RunDir = Join-Path (Resolve-Path ".") "bench-runs\multi-runtime-1mb\$RunName"
}
New-Item -ItemType Directory -Force -Path $RunDir | Out-Null

function Add-Topic([string]$Topic) {
  docker exec flowplane-kafka kafka-topics `
    --bootstrap-server kafka:9092 `
    --create `
    --if-not-exists `
    --topic $Topic `
    --partitions $Partitions `
    --replication-factor 1 `
    --config "max.message.bytes=$MaxMessageBytes" `
    --config retention.ms=14400000 | Out-Null
}

function Get-OffsetTotal([string]$Topic) {
  $lines = docker exec flowplane-kafka kafka-get-offsets --bootstrap-server kafka:9092 --topic $Topic 2>$null
  $sum = 0L
  foreach ($line in $lines) {
    $parts = $line -split ":"
    if ($parts.Count -ge 3 -and $parts[2] -match "^\d+$") {
      $sum += [int64]$parts[2]
    }
  }
  $sum
}

foreach ($topic in @($RawTopic, $FlinkOutputTopic, $ConnectDlqTopic, $FlinkDlqTopic)) {
  Add-Topic $topic
}

$containerCorpus = "/tmp/$RunName-payloads.jsonl"
docker cp $CorpusPath "flowplane-kafka:$containerCorpus"

$start = [ordered]@{
  runName = $RunName
  startedAt = (Get-Date).ToUniversalTime().ToString("o")
  rawTopic = $RawTopic
  flinkOutputTopic = $FlinkOutputTopic
  connectDlqTopic = $ConnectDlqTopic
  flinkDlqTopic = $FlinkDlqTopic
  corpusPath = $CorpusPath
  recordCount = $RecordCount
  targetRate = $TargetRate
  partitions = $Partitions
  compression = $Compression
  offsetsBefore = [ordered]@{
    raw = Get-OffsetTotal $RawTopic
    flinkOutput = Get-OffsetTotal $FlinkOutputTopic
    connectDlq = Get-OffsetTotal $ConnectDlqTopic
    flinkDlq = Get-OffsetTotal $FlinkDlqTopic
  }
}
$start | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $RunDir "run-start.json")

$producerLog = Join-Path $RunDir "producer.log"
docker exec flowplane-kafka kafka-producer-perf-test `
  --topic $RawTopic `
  --num-records $RecordCount `
  --throughput $TargetRate `
  --payload-file $containerCorpus `
  --producer-props `
    bootstrap.servers=kafka:9092 `
    "compression.type=$Compression" `
    "max.request.size=$MaxMessageBytes" `
    "buffer.memory=1073741824" `
    "batch.size=1048576" `
    "linger.ms=5" `
    "acks=1" `
    "max.in.flight.requests.per.connection=$ProducerMaxInFlight" `
    "client.id=$RunName-producer" `
  *> $producerLog

$end = [ordered]@{
  runName = $RunName
  endedAt = (Get-Date).ToUniversalTime().ToString("o")
  offsetsAfter = [ordered]@{
    raw = Get-OffsetTotal $RawTopic
    flinkOutput = Get-OffsetTotal $FlinkOutputTopic
    connectDlq = Get-OffsetTotal $ConnectDlqTopic
    flinkDlq = Get-OffsetTotal $FlinkDlqTopic
  }
}
$end | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $RunDir "run-end.json")
