param(
  [string]$SuiteName = ("flowplane-connect-sink-ramp-" + (Get-Date -Format "yyyyMMdd-HHmmss")),
  [string]$CorpusPath = "C:\FlowPlanenew\FLOWPLANE_Controlplane\bench-runs\multi-runtime-1mb\flowplane-1mb-connect-flink-50unique-1000probe-20260612105548\source-payloads-50unique-1mb.jsonl",
  [string[]]$Connectors = @("mongo", "postgres", "s3"),
  [string[]]$Rates = @("25"),
  [int]$DurationSecondsPerRate = 120,
  [ValidateSet("isolated", "concurrent")]
  [string]$Mode = "isolated",
  [string]$ValueConverter = "org.apache.kafka.connect.converters.ByteArrayConverter",
  [int]$CatchupSeconds = 45,
  [int]$MonitorIntervalSeconds = 10,
  [int]$Partitions = 24,
  [int]$MaxMessageBytes = 2097152,
  [string]$Compression = "lz4",
  [string]$ConnectRestUrl = "http://localhost:8084",
  [string]$RunRoot = "",
  [switch]$SkipProbeCapture,
  [switch]$DryRun
)

$ErrorActionPreference = "Stop"

$Connectors = @(
  $Connectors |
    ForEach-Object { "$_" -split "," } |
    ForEach-Object { $_.Trim().ToLowerInvariant() } |
    Where-Object { $_ -ne "" }
)
$invalidConnectors = @($Connectors | Where-Object { $_ -notin @("mongo", "postgres", "s3") })
if ($invalidConnectors.Count -gt 0) {
  throw "Invalid connector(s): $($invalidConnectors -join ', '). Valid values: mongo, postgres, s3."
}

$Rates = @(
  $Rates |
    ForEach-Object { "$_" -split "," } |
    ForEach-Object { $_.Trim() } |
    Where-Object { $_ -ne "" } |
    ForEach-Object { [int]$_ }
)

if ([string]::IsNullOrWhiteSpace($RunRoot)) {
  $RunRoot = Join-Path (Resolve-Path ".") "bench-runs\multi-runtime-1mb"
}

$suiteDir = Join-Path $RunRoot $SuiteName
$recordsPerRun = {
  param([int]$Rate)
  $Rate * $DurationSecondsPerRate
}

function Write-JsonFile($Value, [string]$Path, [int]$Depth = 10) {
  $Value | ConvertTo-Json -Depth $Depth | Set-Content -LiteralPath $Path -Encoding UTF8
}

function Invoke-Connect([string]$Method, [string]$Path, $Body = $null) {
  $params = @{
    Method = $Method
    Uri = "$ConnectRestUrl$Path"
    ContentType = "application/json"
  }
  if ($null -ne $Body) {
    $params.Body = $Body | ConvertTo-Json -Depth 40 -Compress
  }
  Invoke-RestMethod @params
}

function Get-ConnectorSpec([string]$Kind) {
  switch ($Kind) {
    "mongo" {
      [pscustomobject]@{
        kind = "mongo"
        connector = "flowplane-1mb-mongo-sink-runtime"
        runtimeId = "connect-smt-1mb-mongo-breakpoint"
        targetKey = "collection"
        sinkContainer = "flowplane-mongo"
      }
    }
    "postgres" {
      [pscustomobject]@{
        kind = "postgres"
        connector = "flowplane-1mb-postgres-sink-runtime"
        runtimeId = "connect-smt-1mb-postgres-breakpoint"
        targetKey = "table.name.format"
        sinkContainer = "flowplane-postgres"
      }
    }
    "s3" {
      [pscustomobject]@{
        kind = "s3"
        connector = "flowplane-1mb-s3-sink-runtime"
        runtimeId = "connect-smt-1mb-s3-breakpoint"
        targetKey = "topics.dir"
        sinkContainer = "flowplane-minio"
      }
    }
  }
}

function Get-TopicOffsetTotal([string]$Topic) {
  $lines = docker exec flowplane-kafka kafka-get-offsets --bootstrap-server localhost:29092 --topic $Topic 2>$null
  $sum = 0L
  foreach ($line in $lines) {
    $parts = $line -split ":"
    if ($parts.Count -ge 3 -and $parts[2] -match "^\d+$") {
      $sum += [int64]$parts[2]
    }
  }
  $sum
}

function Get-ConnectLag([string]$Group, [string]$Topic) {
  $lines = docker exec flowplane-kafka kafka-consumer-groups --bootstrap-server localhost:29092 --describe --group $Group 2>$null
  $sum = 0L
  foreach ($line in $lines) {
    $parts = ($line -split "\s+") | Where-Object { $_ -ne "" }
    if ($parts.Count -ge 6 -and $parts[0] -eq $Group -and $parts[1] -eq $Topic -and $parts[5] -match "^\d+$") {
      $sum += [int64]$parts[5]
    }
  }
  $sum
}

function Ensure-Topic([string]$Topic) {
  docker exec flowplane-kafka kafka-topics `
    --bootstrap-server localhost:29092 `
    --create `
    --if-not-exists `
    --topic $Topic `
    --partitions $Partitions `
    --replication-factor 1 `
    --config "max.message.bytes=$MaxMessageBytes" `
    --config retention.ms=14400000 | Out-Null
}

function Get-StatsMap {
  $stats = @{}
  $lines = docker stats --no-stream --format "{{.Name}},{{.CPUPerc}},{{.MemUsage}}" 2>$null
  foreach ($line in $lines) {
    $parts = $line -split ",", 3
    if ($parts.Count -eq 3) {
      $stats[$parts[0]] = [pscustomobject]@{ cpu = $parts[1]; mem = $parts[2] }
    }
  }
  $stats
}

function Get-TargetName($Spec, [int]$Rate, [string]$Stamp) {
  $suffix = ($Stamp -replace "[^0-9]", "")
  switch ($Spec.kind) {
    "mongo" { "orders_1mb_ramp_${Rate}rps_mongo_$suffix" }
    "postgres" { "orders_1mb_ramp_${Rate}rps_pg_$suffix" }
    "s3" { "topics/$SuiteName/${Rate}rps/s3" }
  }
}

function Retarget-Connector($Spec, [string]$RawTopic, [string]$DlqTopic, [string]$Target, [string]$RunDir) {
  $config = Invoke-Connect Get "/connectors/$($Spec.connector)/config"
  $config.topics = $RawTopic
  $config."value.converter" = $ValueConverter
  if ($config.PSObject.Properties.Name -contains "value.converter.schemas.enable") {
    $config.PSObject.Properties.Remove("value.converter.schemas.enable")
  }
  $config."errors.deadletterqueue.topic.name" = $DlqTopic
  $config."transforms.flowplane.flowplane.error.topic" = $DlqTopic

  if ($Spec.kind -eq "mongo") { $config.collection = $Target }
  if ($Spec.kind -eq "postgres") { $config."table.name.format" = $Target }
  if ($Spec.kind -eq "s3") { $config."topics.dir" = $Target }

  Write-JsonFile $config (Join-Path $RunDir "connector-config.json") 40
  Invoke-Connect Put "/connectors/$($Spec.connector)/config" $config | Out-Null
}

function Get-SinkCount($Spec, [string]$Target, [string]$RawTopic = "") {
  if ($Spec.kind -eq "mongo") {
    $out = docker exec flowplane-mongo mongosh flowplane_sink --quiet --eval "print(db.getCollection('$Target').countDocuments())" 2>$null
    return [int64]($out | Select-Object -Last 1)
  }
  if ($Spec.kind -eq "postgres") {
    $out = docker exec flowplane-postgres psql -U flowplane -d flowplane_sink -tAc "select count(*) from $Target;" 2>$null
    $last = $out | Select-Object -Last 1
    if ($last -match "^\d+$") { return [int64]$last }
    return 0
  }
  if ($Spec.kind -eq "s3") {
    $network = docker inspect flowplane-minio --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}'
    $prefix = "$Target/$RawTopic"
    $du = docker run --rm --network $network --entrypoint /bin/sh minio/mc:latest -lc "mc alias set local http://minio:9000 flowplaneminio flowplaneminio123 >/dev/null && mc du --recursive local/flowplane-hardening/$prefix" 2>$null
    $last = $du | Select-Object -Last 1
    if ($last -match '^\s*(\S+)\s+(\d+)\s+objects') {
      return [ordered]@{ objects = [int64]$Matches[2]; size = $Matches[1]; summary = $last }
    }
    return [ordered]@{ objects = 0; size = ""; summary = "" }
  }
}

function Parse-ProducerSummary([string]$ProducerLog) {
  $line = [string](Get-Content -LiteralPath $ProducerLog -ErrorAction SilentlyContinue | Select-Object -Last 1)
  $summary = [ordered]@{ line = $line }
  if ($line -match '(\d+) records sent, ([0-9.]+) records/sec \(([0-9.]+) MB/sec\), ([0-9.]+) ms avg latency, ([0-9.]+) ms max latency, ([0-9]+) ms 50th, ([0-9]+) ms 95th, ([0-9]+) ms 99th, ([0-9]+) ms 99.9th') {
    $summary.records = [int64]$Matches[1]
    $summary.recordsPerSec = [double]$Matches[2]
    $summary.mbPerSec = [double]$Matches[3]
    $summary.avgMs = [double]$Matches[4]
    $summary.maxMs = [double]$Matches[5]
    $summary.p50Ms = [double]$Matches[6]
    $summary.p95Ms = [double]$Matches[7]
    $summary.p99Ms = [double]$Matches[8]
    $summary.p999Ms = [double]$Matches[9]
  }
  $summary
}

function Convert-ProbeItem($Item) {
  if ($null -eq $Item) { return $null }
  $out = [ordered]@{}
  foreach ($name in @("samples", "success", "failure", "p50Ms", "p95Ms", "p99Ms", "maxMs", "windowSamples", "windowMaxMs", "allocP50Bytes", "allocP95Bytes", "allocP99Bytes", "allocMaxBytes", "allocRatioP99", "allocWindowMaxBytes")) {
    if ($Item.PSObject.Properties.Name -contains $name) {
      $out[$name] = [string]$Item.$name
    }
  }
  $out
}

function Parse-ProbeLines([string[]]$Lines, [string]$RuntimeId) {
  $items = @()
  foreach ($line in $Lines) {
    if ($line -notmatch "FLOWPLANE_TRANSFORM_PROBE" -or $line -notmatch [regex]::Escape($RuntimeId)) { continue }
    $item = [ordered]@{}
    foreach ($name in @("samples", "success", "failure", "p50Ms", "p95Ms", "p99Ms", "maxMs", "windowSamples", "windowMaxMs", "allocP50Bytes", "allocP95Bytes", "allocP99Bytes", "allocMaxBytes", "allocRatioP99", "allocWindowMaxBytes")) {
      if ($line -match "$name=([^\s]+)") { $item[$name] = $Matches[1] }
    }
    if ($item.Count -gt 0) { $items += [pscustomobject]$item }
  }
  $items
}

function Capture-ProbeSummary($Spec, [datetime]$StartUtc, [datetime]$EndUtc, [string]$RunDir) {
  if ($SkipProbeCapture) {
    return [ordered]@{ skipped = $true }
  }

  $since = $StartUtc.AddSeconds(-5).ToString("o")
  $until = $EndUtc.AddSeconds(5).ToString("o")
  $warmSince = $StartUtc.AddSeconds(60).ToString("o")

  $coldLines = docker logs --since $since --until $until flowplane-connect 2>&1
  $warmLines = docker logs --since $warmSince --until $until flowplane-connect 2>&1
  $cold = Parse-ProbeLines $coldLines $Spec.runtimeId
  $warm = Parse-ProbeLines $warmLines $Spec.runtimeId
  $warmMax = @($warm | ForEach-Object { if ($_.windowMaxMs) { [double]$_.windowMaxMs } })

  ($coldLines | Select-String -Pattern "FLOWPLANE_TRANSFORM_PROBE.*$($Spec.runtimeId)") |
    Set-Content -LiteralPath (Join-Path $RunDir "probe-cold-inclusive.log")
  ($warmLines | Select-String -Pattern "FLOWPLANE_TRANSFORM_PROBE.*$($Spec.runtimeId)") |
    Set-Content -LiteralPath (Join-Path $RunDir "probe-warm-post60s.log")

  [ordered]@{
    runtimeId = $Spec.runtimeId
    coldInclusiveLast = if ($cold.Count -gt 0) { Convert-ProbeItem $cold[-1] } else { $null }
    warmPost60sLast = if ($warm.Count -gt 0) { Convert-ProbeItem $warm[-1] } else { $null }
    warmPost60sWindowMax = if ($warmMax.Count -gt 0) {
      [ordered]@{
        count = $warmMax.Count
        min = ($warmMax | Measure-Object -Minimum).Minimum
        avg = ($warmMax | Measure-Object -Average).Average
        max = ($warmMax | Measure-Object -Maximum).Maximum
      }
    } else { $null }
    note = "Probe emits cumulative p99 plus rolling windowMaxMs; warm windowMax is not a true rolling p99."
  }
}

function Write-MonitorRow($Spec, [string]$RawTopic, [string]$DlqTopic, [string]$MonitorPath) {
  $stats = Get-StatsMap
  $connect = $stats["flowplane-connect"]
  $kafka = $stats["flowplane-kafka"]
  $sink = $stats[$Spec.sinkContainer]
  $row = @(
    (Get-Date).ToUniversalTime().ToString("o"),
    (Get-TopicOffsetTotal $RawTopic),
    (Get-TopicOffsetTotal $DlqTopic),
    (Get-ConnectLag "connect-$($Spec.connector)" $RawTopic),
    $connect.cpu,
    ('"' + $connect.mem + '"'),
    $kafka.cpu,
    ('"' + $kafka.mem + '"'),
    $sink.cpu,
    ('"' + $sink.mem + '"')
  )
  ($row -join ",") | Add-Content -LiteralPath $MonitorPath
}

function Run-OneConnector($Spec, [int]$Rate, [string]$Stamp) {
  $records = & $recordsPerRun $Rate
  $runName = "$($Spec.kind)-${Rate}rps-$Stamp"
  $runDir = Join-Path $suiteDir $runName
  New-Item -ItemType Directory -Force -Path $runDir | Out-Null

  $rawTopic = "raw.flowplane.1mb.$($Spec.kind).${Rate}rps.$Stamp"
  $dlqTopic = "dlq.flowplane.1mb.$($Spec.kind).${Rate}rps.$Stamp"
  $target = Get-TargetName $Spec $Rate $Stamp

  if ($DryRun) {
    return [ordered]@{ runName = $runName; dryRun = $true; rawTopic = $rawTopic; dlqTopic = $dlqTopic; target = $target }
  }

  Ensure-Topic $rawTopic
  Ensure-Topic $dlqTopic

  if ($Mode -eq "isolated") {
    foreach ($kind in @("mongo", "postgres", "s3")) {
      $other = Get-ConnectorSpec $kind
      Invoke-Connect Put "/connectors/$($other.connector)/pause" | Out-Null
    }
    Start-Sleep -Seconds 3
  }

  Retarget-Connector $Spec $rawTopic $dlqTopic $target $runDir
  Invoke-Connect Post "/connectors/$($Spec.connector)/restart?includeTasks=true&onlyFailed=false" @{} | Out-Null
  Start-Sleep -Seconds 12
  Invoke-Connect Put "/connectors/$($Spec.connector)/resume" | Out-Null
  Start-Sleep -Seconds 8

  $startUtc = (Get-Date).ToUniversalTime()
  $runStart = [ordered]@{
    runName = $runName
    kind = $Spec.kind
    connector = $Spec.connector
    runtimeId = $Spec.runtimeId
    startedAt = $startUtc.ToString("o")
    rate = $Rate
    durationSeconds = $DurationSecondsPerRate
    records = $records
    rawTopic = $rawTopic
    dlqTopic = $dlqTopic
    target = $target
  }
  Write-JsonFile $runStart (Join-Path $runDir "run-start.json")

  $monitorPath = Join-Path $runDir "monitor-10s.csv"
  "ts,rawTotal,dlqTotal,connectLag,connectCpu,connectMem,kafkaCpu,kafkaMem,sinkCpu,sinkMem" |
    Set-Content -LiteralPath $monitorPath

  $producerOut = Join-Path $runDir "producer.out.log"
  $producerErr = Join-Path $runDir "producer.err.log"
  $producerLog = Join-Path $runDir "producer.log"
  $producerArgs = @(
    "exec", "flowplane-kafka", "kafka-producer-perf-test",
    "--topic", $rawTopic,
    "--num-records", "$records",
    "--throughput", "$Rate",
    "--payload-file", $script:ContainerCorpus,
    "--producer-props",
    "bootstrap.servers=kafka:9092",
    "compression.type=$Compression",
    "max.request.size=$MaxMessageBytes",
    "buffer.memory=1073741824",
    "batch.size=1048576",
    "linger.ms=5",
    "acks=1",
    "max.in.flight.requests.per.connection=5",
    "client.id=$SuiteName-$($Spec.kind)-$Rate-producer"
  )

  $producer = Start-Process docker.exe `
    -ArgumentList $producerArgs `
    -RedirectStandardOutput $producerOut `
    -RedirectStandardError $producerErr `
    -PassThru `
    -WindowStyle Hidden

  while (-not $producer.HasExited) {
    Write-MonitorRow $Spec $rawTopic $dlqTopic $monitorPath
    Start-Sleep -Seconds $MonitorIntervalSeconds
  }

  for ($i = 0; $i -lt [Math]::Ceiling($CatchupSeconds / $MonitorIntervalSeconds); $i++) {
    Write-MonitorRow $Spec $rawTopic $dlqTopic $monitorPath
    Start-Sleep -Seconds $MonitorIntervalSeconds
  }

  (Get-Content -LiteralPath $producerOut -ErrorAction SilentlyContinue),
    (Get-Content -LiteralPath $producerErr -ErrorAction SilentlyContinue) |
    Set-Content -LiteralPath $producerLog

  $endUtc = (Get-Date).ToUniversalTime()
  $status = Invoke-Connect Get "/connectors/$($Spec.connector)/status"
  Write-JsonFile $status (Join-Path $runDir "connector-status-end.json") 20

  $rawTotal = Get-TopicOffsetTotal $rawTopic
  $dlqTotal = Get-TopicOffsetTotal $dlqTopic
  $lag = Get-ConnectLag "connect-$($Spec.connector)" $rawTopic
  $sinkCount = Get-SinkCount $Spec $target $rawTopic
  $producerSummary = Parse-ProducerSummary $producerLog
  $probe = Capture-ProbeSummary $Spec $startUtc $endUtc $runDir

  $runEnd = [ordered]@{
    runName = $runName
    kind = $Spec.kind
    connector = $Spec.connector
    startedAt = $startUtc.ToString("o")
    endedAt = $endUtc.ToString("o")
    rawTopic = $rawTopic
    dlqTopic = $dlqTopic
    target = $target
    rawTotal = $rawTotal
    dlqTotal = $dlqTotal
    residualLag = $lag
    sink = $sinkCount
    producer = $producerSummary
    transformProbe = $probe
    runDir = $runDir
  }
  Write-JsonFile $runEnd (Join-Path $runDir "run-end.json") 12
  $runEnd
}

New-Item -ItemType Directory -Force -Path $suiteDir | Out-Null
if (-not (Test-Path -LiteralPath $CorpusPath)) {
  throw "CorpusPath does not exist: $CorpusPath"
}

$script:ContainerCorpus = "/tmp/$SuiteName-payloads.jsonl"
if (-not $DryRun) {
  docker cp $CorpusPath "flowplane-kafka:$script:ContainerCorpus"
}

$suiteStart = [ordered]@{
  suiteName = $SuiteName
  startedAt = (Get-Date).ToUniversalTime().ToString("o")
  corpusPath = $CorpusPath
  connectors = $Connectors
  rates = $Rates
  durationSecondsPerRate = $DurationSecondsPerRate
  mode = $Mode
  valueConverter = $ValueConverter
  catchupSeconds = $CatchupSeconds
  monitorIntervalSeconds = $MonitorIntervalSeconds
  skipProbeCapture = [bool]$SkipProbeCapture
}
Write-JsonFile $suiteStart (Join-Path $suiteDir "suite-start.json")

$results = @()
foreach ($rate in $Rates) {
  $stamp = "$(Get-Date -Format "yyyyMMdd-HHmmss")-${rate}rps"
  if ($Mode -eq "concurrent") {
    throw "Concurrent mode is reserved for the next harness version. Use isolated mode for reliable per-connector evidence."
  }
  foreach ($kind in $Connectors) {
    $spec = Get-ConnectorSpec $kind
    $result = Run-OneConnector $spec $rate $stamp
    $results += $result
    Write-JsonFile ([ordered]@{ suite = $suiteStart; completedRuns = $results }) (Join-Path $suiteDir "suite-progress.json") 14
  }
}

foreach ($kind in @("mongo", "postgres", "s3")) {
  $spec = Get-ConnectorSpec $kind
  if (-not $DryRun) {
    Invoke-Connect Put "/connectors/$($spec.connector)/resume" | Out-Null
  }
}

$suiteEnd = [ordered]@{
  suiteName = $SuiteName
  completedAt = (Get-Date).ToUniversalTime().ToString("o")
  runDir = $suiteDir
  results = $results
  notes = @(
    "Use rawTotal/dlqTotal/residualLag for Kafka evidence.",
    "Use producer.p99Ms for producer/client path latency.",
    "Use transformProbe.coldInclusiveLast.p99Ms for cold-inclusive FLOWPLANE transform p99.",
    "Use transformProbe.warmPost60sWindowMax as warm max signal only; current runtime probe does not emit warm rolling p99.",
    "S3 can retain residual lag with flush.size-only configs; use rotate.interval.ms for deterministic tail flush."
  )
}
Write-JsonFile $suiteEnd (Join-Path $suiteDir "suite-summary.json") 14
$suiteEnd | ConvertTo-Json -Depth 14
