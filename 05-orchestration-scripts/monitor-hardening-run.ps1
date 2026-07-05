param(
  [Parameter(Mandatory = $true)]
  [string]$RunDir,

  [int]$IntervalSeconds = 30,

  [int]$Samples = 130
)

$ErrorActionPreference = "Continue"

$topics = @(
  "raw.flowplane.500kb.prod-hardening",
  "transformed.flowplane.500kb.prod-hardening",
  "errors.flowplane.500kb.prod-hardening",
  "_confluent-monitoring"
)

New-Item -ItemType Directory -Force -Path $RunDir | Out-Null
$out = Join-Path $RunDir "monitor-samples.jsonl"

function Get-TopicOffsetTotal {
  param([string]$Topic)

  $lines = docker exec flowplane-kafka kafka-get-offsets --bootstrap-server kafka:9092 --topic $Topic 2>$null
  $sum = 0L
  foreach ($line in $lines) {
    $parts = $line -split ":"
    if ($parts.Count -ge 3 -and $parts[2] -match "^\d+$") {
      $sum += [int64]$parts[2]
    }
  }
  return $sum
}

for ($i = 0; $i -lt $Samples; $i++) {
  $offsets = [ordered]@{}
  foreach ($topic in $topics) {
    $offsets[$topic] = Get-TopicOffsetTotal -Topic $topic
  }

  $controlCounts = docker exec flowplane-mongo mongosh flowplane_control_plane --quiet --eval "print([db.field_failures.countDocuments(), db.runtime_metrics.countDocuments()].join(','));"
  $sinkCount = docker exec flowplane-mongo mongosh flowplane_sink --quiet --eval "print(db.orders_500kb_hardening.countDocuments());"
  $controlParts = ($controlCounts | Select-Object -Last 1) -split ","

  $sample = [ordered]@{
    ts = (Get-Date).ToUniversalTime().ToString("o")
    offsets = $offsets
    mongo = [ordered]@{
      field_failures = [int64]$controlParts[0]
      runtime_metrics = [int64]$controlParts[1]
      sink_docs = [int64]($sinkCount | Select-Object -Last 1)
    }
  }

  ($sample | ConvertTo-Json -Compress -Depth 8) | Add-Content -Path $out
  Start-Sleep -Seconds $IntervalSeconds
}
