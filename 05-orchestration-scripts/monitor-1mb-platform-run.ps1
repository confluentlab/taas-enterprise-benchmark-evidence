param(
  [Parameter(Mandatory = $true)]
  [string]$RunDir,

  [Parameter(Mandatory = $true)]
  [string]$RawTopic,

  [Parameter(Mandatory = $true)]
  [string]$FlinkOutputTopic,

  [Parameter(Mandatory = $true)]
  [string]$ConnectDlqTopic,

  [Parameter(Mandatory = $true)]
  [string]$FlinkDlqTopic,

  [string]$ConnectGroup = "connect-flowplane-1mb-mongo-sink-runtime",
  [int]$IntervalSeconds = 10,
  [int]$Samples = 210
)

$ErrorActionPreference = "Continue"
New-Item -ItemType Directory -Force -Path $RunDir | Out-Null
$out = Join-Path $RunDir "monitor-10s.csv"

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

function Get-ConnectLag {
  $lines = docker exec flowplane-kafka kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group $ConnectGroup 2>$null
  $sum = 0L
  foreach ($line in $lines) {
    $escapedTopic = [regex]::Escape($RawTopic)
    if ($line -match "^\s*$ConnectGroup\s+$escapedTopic\s+\d+\s+\d+\s+\d+\s+(\d+)") {
      $sum += [int64]$Matches[1]
    }
  }
  $sum
}

function Get-Stats {
  $names = @("flowplane-connect", "flowplane-flink-taskmanager", "flowplane-flink-jobmanager", "flowplane-kafka", "flowplane-mongo", "flowplane-backend")
  $stats = @{}
  $lines = docker stats --no-stream --format "{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}}" 2>$null
  foreach ($line in $lines) {
    $parts = $line -split ",", 4
    if ($parts.Count -eq 4 -and $names -contains $parts[0]) {
      $stats[$parts[0]] = [ordered]@{
        cpu = $parts[1]
        memUsage = $parts[2]
        memPercent = $parts[3]
      }
    }
  }
  $stats
}

"ts,rawTotal,flinkOutputTotal,connectDlqTotal,flinkDlqTotal,connectLag,fieldFailures,runtimeMetrics,connectCpu,connectMem,flinkCpu,flinkMem,kafkaCpu,kafkaMem,mongoCpu,mongoMem,backendCpu,backendMem" |
  Set-Content -LiteralPath $out

for ($i = 0; $i -lt $Samples; $i++) {
  $stats = Get-Stats
  $controlCounts = docker exec flowplane-mongo mongosh flowplane_control_plane --quiet --eval "print([db.field_failures.countDocuments(), db.runtime_metrics.countDocuments()].join(','));" 2>$null
  $parts = (($controlCounts | Select-Object -Last 1) -split ",")
  $row = [ordered]@{
    ts = (Get-Date).ToUniversalTime().ToString("o")
    rawTotal = Get-OffsetTotal $RawTopic
    flinkOutputTotal = Get-OffsetTotal $FlinkOutputTopic
    connectDlqTotal = Get-OffsetTotal $ConnectDlqTopic
    flinkDlqTotal = Get-OffsetTotal $FlinkDlqTopic
    connectLag = Get-ConnectLag
    fieldFailures = if ($parts.Count -gt 0 -and $parts[0] -match "^\d+$") { [int64]$parts[0] } else { 0 }
    runtimeMetrics = if ($parts.Count -gt 1 -and $parts[1] -match "^\d+$") { [int64]$parts[1] } else { 0 }
    connectCpu = $stats["flowplane-connect"].cpu
    connectMem = $stats["flowplane-connect"].memUsage
    flinkCpu = $stats["flowplane-flink-taskmanager"].cpu
    flinkMem = $stats["flowplane-flink-taskmanager"].memUsage
    kafkaCpu = $stats["flowplane-kafka"].cpu
    kafkaMem = $stats["flowplane-kafka"].memUsage
    mongoCpu = $stats["flowplane-mongo"].cpu
    mongoMem = $stats["flowplane-mongo"].memUsage
    backendCpu = $stats["flowplane-backend"].cpu
    backendMem = $stats["flowplane-backend"].memUsage
  }
  ($row.Values -join ",") | Add-Content -LiteralPath $out
  Start-Sleep -Seconds $IntervalSeconds
}
