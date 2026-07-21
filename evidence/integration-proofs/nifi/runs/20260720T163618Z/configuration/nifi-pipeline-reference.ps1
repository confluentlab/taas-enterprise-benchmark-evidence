param(
  [string]$RunId = (Get-Date -Format "yyyyMMddHHmmss"),
  [string]$KafkaBootstrap = "kafka:9092",
  [string]$RuntimeUrl = "http://flowplane-bento-runtime-live-validation:8080/transform",
  [string]$BenchmarkPayloadJsonl = "",
  [string]$InvalidPayloadPath = "",
  [int]$ValidRecordCount = 1,
  [int]$TargetRps = 0,
  [int]$DurationSeconds = 0,
  [int]$OutputObservationSeconds = 0,
  [string]$NifiImage = "apache/nifi:1.27.0",
  [int]$NifiHostPort = 18078,
  [switch]$KeepRunning
)

$ErrorActionPreference = "Stop"

$RawTopic = "flowplane.nifi.http.raw.$RunId"
$TransformedTopic = "flowplane.nifi.http.transformed.$RunId"
$DlqTopic = "flowplane.nifi.http.dlq.$RunId"
$GroupId = "flowplane-nifi-http-streaming-$RunId"
$NifiContainer = "flowplane-nifi-kafka-http-streaming-$RunId"
$NifiApi = "http://127.0.0.1:$NifiHostPort/nifi-api"
$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$EvidenceRoot = Split-Path $RepoRoot -Parent
if ([string]::IsNullOrWhiteSpace($BenchmarkPayloadJsonl)) {
  $ValidPayloads = @(Get-Content -Raw (Join-Path $EvidenceRoot "evidence\e2e\sample-inputs\valid-order-1.json"))
} else {
  $ValidPayloads = @(Get-Content -LiteralPath $BenchmarkPayloadJsonl | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
}
if ([string]::IsNullOrWhiteSpace($InvalidPayloadPath)) {
  $InvalidPayload = Get-Content -Raw (Join-Path $EvidenceRoot "evidence\e2e\sample-inputs\invalid-order-missing-required-field.json")
} else {
  $InvalidPayload = Get-Content -Raw -LiteralPath $InvalidPayloadPath
}
if ($ValidPayloads.Count -eq 0) {
  throw "No valid payloads available"
}
$FullPressureRun = ($ValidRecordCount -gt 1000) -or ($DurationSeconds -ge 60)
if ($OutputObservationSeconds -le 0) {
  $OutputObservationSeconds = if ($FullPressureRun) { 60 } else { 300 }
}
$CorpusEventIds = @($ValidPayloads | ForEach-Object { ($_ | ConvertFrom-Json).event.id })
$ExpectedEventIds = if ($FullPressureRun) {
  @($CorpusEventIds)
} else {
  @(for ($index = 0; $index -lt $ValidRecordCount; $index++) {
    $CorpusEventIds[$index % $CorpusEventIds.Count]
  })
}

function Invoke-Native {
  param(
    [Parameter(Mandatory = $true)]
    [string]$FilePath,
    [string[]]$Arguments
  )
  & $FilePath @Arguments
  if ($LASTEXITCODE -ne 0) {
    throw "Command failed with exit code $LASTEXITCODE`: $FilePath $($Arguments -join ' ')"
  }
}

function Wait-Http {
  param(
    [string]$Uri,
    [int]$TimeoutSeconds = 240
  )
  $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
  while ((Get-Date) -lt $deadline) {
    try {
      Invoke-WebRequest -Uri $Uri -UseBasicParsing -TimeoutSec 5 | Out-Null
      return
    } catch {
      Start-Sleep -Seconds 3
    }
  }
  throw "Timed out waiting for $Uri"
}

function Invoke-JsonRequest {
  param(
    [string]$Method,
    [string]$Uri,
    [object]$Body = $null
  )
  $params = @{
    Method = $Method
    Uri = $Uri
    UseBasicParsing = $true
  }
  if ($null -ne $Body) {
    $params.ContentType = "application/json"
    $params.Body = ($Body | ConvertTo-Json -Depth 30)
  }
  Invoke-RestMethod @params
}

function Get-ProcessorType {
  param([string]$Type)
  $types = Invoke-JsonRequest -Method GET -Uri "$NifiApi/flow/processor-types"
  $match = $types.processorTypes | Where-Object { $_.type -eq $Type } | Select-Object -First 1
  if ($null -eq $match) {
    throw "NiFi processor type not found: $Type"
  }
  $match
}

function New-Processor {
  param(
    [string]$RootId,
    [string]$Type,
    [int]$X,
    [int]$Y
  )
  $processorType = Get-ProcessorType -Type $Type
  $body = @{
    revision = @{ version = 0 }
    component = @{
      type = $Type
      bundle = $processorType.bundle
      position = @{ x = $X; y = $Y }
    }
  }
  Invoke-JsonRequest -Method POST -Uri "$NifiApi/process-groups/$RootId/processors" -Body $body
}

function Update-Processor {
  param(
    [object]$Processor,
    [hashtable]$Properties,
    [string[]]$AutoTerminate = @()
  )
  $body = @{
    revision = @{ version = $Processor.revision.version }
    component = @{
      id = $Processor.id
      config = @{
        properties = $Properties
        autoTerminatedRelationships = $AutoTerminate
      }
    }
  }
  Invoke-JsonRequest -Method PUT -Uri "$NifiApi/processors/$($Processor.id)" -Body $body
}

function New-Connection {
  param(
    [string]$RootId,
    [object]$Source,
    [object]$Destination,
    [string[]]$Relationships
  )
  $body = @{
    revision = @{ version = 0 }
    component = @{
      source = @{ id = $Source.id; groupId = $RootId; type = "PROCESSOR" }
      destination = @{ id = $Destination.id; groupId = $RootId; type = "PROCESSOR" }
      selectedRelationships = $Relationships
    }
  }
  Invoke-JsonRequest -Method POST -Uri "$NifiApi/process-groups/$RootId/connections" -Body $body | Out-Null
}

function Start-Processor {
  param([object]$Processor)
  $current = Invoke-JsonRequest -Method GET -Uri "$NifiApi/processors/$($Processor.id)"
  $body = @{
    revision = @{ version = $current.revision.version }
    state = "RUNNING"
  }
  Invoke-JsonRequest -Method PUT -Uri "$NifiApi/processors/$($Processor.id)/run-status" -Body $body | Out-Null
}

function Stop-Processor {
  param([object]$Processor)
  try {
    $current = Invoke-JsonRequest -Method GET -Uri "$NifiApi/processors/$($Processor.id)"
    $body = @{
      revision = @{ version = $current.revision.version }
      state = "STOPPED"
    }
    Invoke-JsonRequest -Method PUT -Uri "$NifiApi/processors/$($Processor.id)/run-status" -Body $body | Out-Null
  } catch {
    Write-Warning "Could not stop processor $($Processor.id): $($_.Exception.Message)"
  }
}

function New-KafkaTopic {
  param([string]$Topic)
  Invoke-Native -FilePath "docker" -Arguments @(
    "exec", "flowplane-kafka", "kafka-topics",
    "--bootstrap-server", "kafka:9092",
    "--create", "--if-not-exists",
    "--topic", $Topic,
    "--partitions", "1",
    "--replication-factor", "1"
  )
}

function Publish-KafkaLines {
  param(
    [string]$Topic,
    [string[]]$Payloads
  )
  $singleLinePayloads = @($Payloads | ForEach-Object {
    $payload = [string]$_
    if ($payload.Contains("`n") -or $payload.Contains("`r")) {
      $payload | ConvertFrom-Json | ConvertTo-Json -Compress -Depth 100
    } else {
      $payload
    }
  })
  $tempHostFile = Join-Path ([IO.Path]::GetTempPath()) ("flowplane-nifi-kafka-payload-{0}.json" -f ([Guid]::NewGuid().ToString("N")))
  $tempContainerFile = "/tmp/$(Split-Path $tempHostFile -Leaf)"
  try {
    [IO.File]::WriteAllLines($tempHostFile, $singleLinePayloads, [Text.UTF8Encoding]::new($false))
    Invoke-Native -FilePath "docker" -Arguments @("cp", $tempHostFile, "flowplane-kafka:$tempContainerFile")
    Invoke-Native -FilePath "docker" -Arguments @(
      "exec", "flowplane-kafka", "bash", "-lc",
      "cat '$tempContainerFile' | kafka-console-producer --bootstrap-server kafka:9092 --topic '$Topic'"
    )
  } finally {
    if (Test-Path -LiteralPath $tempHostFile) {
      Remove-Item -LiteralPath $tempHostFile -Force
    }
    docker exec --user root flowplane-kafka bash -lc "rm -f '$tempContainerFile'" *> $null
  }
}

function Publish-KafkaRoundRobin {
  param(
    [string]$Topic,
    [string[]]$Payloads,
    [int]$RecordCount,
    [int]$RatePerSecond
  )
  $sent = 0
  $started = Get-Date
  $chunkSize = if ($RatePerSecond -gt 0) { $RatePerSecond } else { $RecordCount }
  while ($sent -lt $RecordCount) {
    $secondStart = Get-Date
    $count = [Math]::Min($chunkSize, $RecordCount - $sent)
    $chunk = @(for ($offset = 0; $offset -lt $count; $offset++) {
      $Payloads[($sent + $offset) % $Payloads.Count]
    })
    Publish-KafkaLines -Topic $Topic -Payloads $chunk
    $sent += $count
    if ($RatePerSecond -gt 0 -and $sent -lt $RecordCount) {
      $elapsed = ((Get-Date) - $secondStart).TotalMilliseconds
      if ($elapsed -lt 1000) {
        Start-Sleep -Milliseconds ([int](1000 - $elapsed))
      }
    }
  }
  $elapsedSeconds = [Math]::Max(0.001, ((Get-Date) - $started).TotalSeconds)
  [pscustomobject]@{
    sentValidRecords = $sent
    elapsedSeconds = $elapsedSeconds
    observedPublishRps = [Math]::Round($sent / $elapsedSeconds, 2)
  }
}

function ConvertTo-KafkaLine {
  param([string]$Payload)
  if ($Payload.Contains("`n") -or $Payload.Contains("`r")) {
    return ($Payload | ConvertFrom-Json | ConvertTo-Json -Compress -Depth 100)
  }
  $Payload
}

function Start-KafkaProducer {
  param([string]$Topic)
  $startInfo = [System.Diagnostics.ProcessStartInfo]::new()
  $startInfo.FileName = "docker"
  $startInfo.Arguments = "exec -i flowplane-kafka bash -lc ""kafka-console-producer --bootstrap-server kafka:9092 --topic '$Topic'"""
  $startInfo.UseShellExecute = $false
  $startInfo.RedirectStandardInput = $true
  $startInfo.RedirectStandardError = $true
  $process = [System.Diagnostics.Process]::Start($startInfo)
  $process.StandardInput.NewLine = "`n"
  $process
}

function Complete-KafkaProducer {
  param(
    [System.Diagnostics.Process]$Process,
    [string]$Topic
  )
  $Process.StandardInput.Close()
  if (-not $Process.WaitForExit(120000)) {
    $Process.Kill()
    throw "Timed out waiting for kafka-console-producer to finish for $Topic"
  }
  if ($Process.ExitCode -ne 0) {
    $stderr = $Process.StandardError.ReadToEnd()
    throw "kafka-console-producer failed for $Topic with exit code $($Process.ExitCode): $stderr"
  }
}

function Publish-KafkaLines {
  param(
    [string]$Topic,
    [string[]]$Payloads
  )
  $producer = Start-KafkaProducer -Topic $Topic
  try {
    foreach ($payload in $Payloads) {
      $producer.StandardInput.WriteLine((ConvertTo-KafkaLine -Payload $payload))
    }
  } finally {
    Complete-KafkaProducer -Process $producer -Topic $Topic
  }
}

function Publish-KafkaRoundRobin {
  param(
    [string]$Topic,
    [string[]]$Payloads,
    [int]$RecordCount,
    [int]$RatePerSecond
  )
  $sent = 0
  $producer = $null
  $started = Get-Date
  $chunkSize = if ($RatePerSecond -gt 0) { $RatePerSecond } else { $RecordCount }
  try {
    $producer = Start-KafkaProducer -Topic $Topic
    while ($sent -lt $RecordCount) {
      $secondStart = Get-Date
      $count = [Math]::Min($chunkSize, $RecordCount - $sent)
      for ($offset = 0; $offset -lt $count; $offset++) {
        $producer.StandardInput.WriteLine((ConvertTo-KafkaLine -Payload $Payloads[($sent + $offset) % $Payloads.Count]))
      }
      $producer.StandardInput.Flush()
      $sent += $count
      if ($RatePerSecond -gt 0 -and $sent -lt $RecordCount) {
        $elapsed = ((Get-Date) - $secondStart).TotalMilliseconds
        if ($elapsed -lt 1000) {
          Start-Sleep -Milliseconds ([int](1000 - $elapsed))
        }
      }
    }
  } finally {
    if ($null -ne $producer) {
      Complete-KafkaProducer -Process $producer -Topic $Topic
    }
  }
  $elapsedSeconds = [Math]::Max(0.001, ((Get-Date) - $started).TotalSeconds)
  [pscustomobject]@{
    sentValidRecords = $sent
    elapsedSeconds = $elapsedSeconds
    observedPublishRps = [Math]::Round($sent / $elapsedSeconds, 2)
  }
}

function Publish-KafkaRoundRobin {
  param(
    [string]$Topic,
    [string[]]$Payloads,
    [int]$RecordCount,
    [int]$RatePerSecond,
    [int]$DurationSeconds = 0
  )
  $tempPayloadHostFile = Join-Path ([IO.Path]::GetTempPath()) ("flowplane-round-robin-payloads-{0}.jsonl" -f ([Guid]::NewGuid().ToString("N")))
  $tempScriptHostFile = Join-Path ([IO.Path]::GetTempPath()) ("flowplane-round-robin-producer-{0}.py" -f ([Guid]::NewGuid().ToString("N")))
  $tempPayloadContainerFile = "/tmp/$(Split-Path $tempPayloadHostFile -Leaf)"
  $tempScriptContainerFile = "/tmp/$(Split-Path $tempScriptHostFile -Leaf)"
  $producerScript = @'
import subprocess
import sys
import time

payload_path, topic, count_arg, rps_arg, duration_arg = sys.argv[1:6]
record_count = int(count_arg)
rate_per_second = int(rps_arg)
duration_seconds = int(duration_arg)
with open(payload_path, "r", encoding="utf-8") as handle:
    payloads = [line.rstrip("\r\n") for line in handle if line.strip()]
if not payloads:
    raise SystemExit("no payloads available")
producer = subprocess.Popen(
    ["kafka-console-producer", "--bootstrap-server", "kafka:9092", "--topic", topic],
    stdin=subprocess.PIPE,
    text=True,
    encoding="utf-8",
)
sent = 0
chunk_size = rate_per_second if rate_per_second > 0 else record_count
started = time.time()
deadline = started + duration_seconds if duration_seconds > 0 else None
try:
    while sent < record_count and (deadline is None or time.time() < deadline):
        second_started = time.time()
        count = min(chunk_size, record_count - sent)
        for offset in range(count):
            producer.stdin.write(payloads[(sent + offset) % len(payloads)] + "\n")
        producer.stdin.flush()
        sent += count
        if rate_per_second > 0 and sent < record_count:
            elapsed = time.time() - second_started
            if elapsed < 1:
                time.sleep(1 - elapsed)
finally:
    if producer.stdin:
        producer.stdin.close()
exit_code = producer.wait()
if exit_code != 0:
    raise SystemExit(exit_code)
elapsed_seconds = max(0.001, time.time() - started)
print(f"sentValidRecords={sent}")
print(f"elapsedSeconds={elapsed_seconds:.3f}")
print(f"observedPublishRps={sent / elapsed_seconds:.2f}")
'@
  try {
    [IO.File]::WriteAllLines($tempPayloadHostFile, @($Payloads | ForEach-Object { ConvertTo-KafkaLine -Payload $_ }), [Text.UTF8Encoding]::new($false))
    [IO.File]::WriteAllText($tempScriptHostFile, $producerScript, [Text.UTF8Encoding]::new($false))
    Invoke-Native -FilePath "docker" -Arguments @("cp", $tempPayloadHostFile, "flowplane-kafka:$tempPayloadContainerFile")
    Invoke-Native -FilePath "docker" -Arguments @("cp", $tempScriptHostFile, "flowplane-kafka:$tempScriptContainerFile")
    $output = & docker exec flowplane-kafka python3 $tempScriptContainerFile $tempPayloadContainerFile $Topic $RecordCount $RatePerSecond $DurationSeconds
    if ($LASTEXITCODE -ne 0) {
      throw "In-container Kafka producer failed with exit code $LASTEXITCODE"
    }
    $facts = @{}
    foreach ($line in @($output)) {
      if ($line -match '^([^=]+)=(.+)$') {
        $facts[$Matches[1]] = $Matches[2]
      }
    }
    [pscustomobject]@{
      sentValidRecords = [int]$facts.sentValidRecords
      elapsedSeconds = [double]$facts.elapsedSeconds
      observedPublishRps = [double]$facts.observedPublishRps
    }
  } finally {
    if (Test-Path -LiteralPath $tempPayloadHostFile) {
      Remove-Item -LiteralPath $tempPayloadHostFile -Force
    }
    if (Test-Path -LiteralPath $tempScriptHostFile) {
      Remove-Item -LiteralPath $tempScriptHostFile -Force
    }
    docker exec --user root flowplane-kafka bash -lc "rm -f '$tempPayloadContainerFile' '$tempScriptContainerFile'" *> $null
  }
}

function Read-KafkaMany {
  param(
    [string]$Topic,
    [int]$Count
  )
  $output = docker exec flowplane-kafka bash -lc "kafka-console-consumer --bootstrap-server kafka:9092 --topic '$Topic' --from-beginning --max-messages $Count --timeout-ms 90000 2>/dev/null"
  if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($output)) {
    throw "No record consumed from $Topic"
  }
  $records = @($output -split "`r?`n" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
  if ($records.Count -lt $Count) {
    throw "Expected $Count records from $Topic but consumed $($records.Count)"
  }
  $records
}

function Get-KafkaTopicLatestCount {
  param([string]$Topic)
  $output = docker exec flowplane-kafka bash -lc "kafka-get-offsets --bootstrap-server kafka:9092 --topic '$Topic' --time -1 2>/dev/null"
  if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($output)) {
    return 0
  }
  $sum = 0
  foreach ($line in @($output -split "`r?`n" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })) {
    $parts = $line -split ":"
    $sum += [int64]($parts[$parts.Length - 1])
  }
  $sum
}

function Wait-KafkaTopicCount {
  param(
    [string]$Topic,
    [int]$ExpectedCount,
    [int]$TimeoutSeconds,
    [bool]$RequireComplete
  )
  $started = Get-Date
  $deadline = $started.AddSeconds($TimeoutSeconds)
  $polls = @()
  $count = Get-KafkaTopicLatestCount -Topic $Topic
  $polls += [pscustomobject]@{ atSeconds = 0; count = $count }
  while ($count -lt $ExpectedCount -and (Get-Date) -lt $deadline) {
    Start-Sleep -Seconds 5
    $count = Get-KafkaTopicLatestCount -Topic $Topic
    $polls += [pscustomobject]@{ atSeconds = [Math]::Round(((Get-Date) - $started).TotalSeconds, 2); count = $count }
  }
  $status = if ($count -ge $ExpectedCount) { "PASS" } elseif ($RequireComplete) { "PARTIAL" } else { "MEASURED_WITH_BACKLOG" }
  [pscustomobject]@{
    topic = $Topic
    status = $status
    count = $count
    expectedCount = $ExpectedCount
    backlogRecords = [Math]::Max(0, $ExpectedCount - $count)
    waitedSeconds = [Math]::Round(((Get-Date) - $started).TotalSeconds, 2)
    outputRateIsProducerRateGate = $false
    polls = $polls
  }
}

function Assert-EventIdMultiset {
  param(
    [string[]]$Expected,
    [string[]]$Actual
  )
  $expectedCounts = @{}
  foreach ($id in $Expected) {
    $expectedCounts[$id] = 1 + [int]($expectedCounts[$id])
  }
  $actualCounts = @{}
  foreach ($id in $Actual) {
    $actualCounts[$id] = 1 + [int]($actualCounts[$id])
  }
  foreach ($id in $expectedCounts.Keys) {
    if ([int]($actualCounts[$id]) -ne [int]($expectedCounts[$id])) {
      throw "Unexpected transformed event count for $id`: expected $($expectedCounts[$id]), got $([int]($actualCounts[$id]))"
    }
  }
  foreach ($id in $actualCounts.Keys) {
    if (-not $expectedCounts.ContainsKey($id)) {
      throw "Unexpected transformed event id $id"
    }
  }
}

try {
  foreach ($topic in @($RawTopic, $TransformedTopic, $DlqTopic)) {
    New-KafkaTopic -Topic $topic
  }

  $existing = docker ps -aq --filter "name=^$NifiContainer$"
  if (-not [string]::IsNullOrWhiteSpace($existing)) {
    docker rm -f $NifiContainer | Out-Null
  }
  Invoke-Native -FilePath "docker" -Arguments @(
    "run", "-d",
    "--name", $NifiContainer,
    "--network", "flowplane-quality-stack_default",
    "--add-host=host.docker.internal:host-gateway",
    "-p", "$($NifiHostPort):8080",
    "-e", "NIFI_WEB_HTTP_HOST=0.0.0.0",
    "-e", "NIFI_WEB_HTTP_PORT=8080",
    "-e", "NIFI_SENSITIVE_PROPS_KEY=flowplane-local-streaming-key",
    "-e", "SINGLE_USER_CREDENTIALS_USERNAME=",
    "-e", "SINGLE_USER_CREDENTIALS_PASSWORD=",
    $NifiImage
  )
  Wait-Http -Uri "$NifiApi/flow/status"

  $root = (Invoke-JsonRequest -Method GET -Uri "$NifiApi/flow/process-groups/root").processGroupFlow.id
  $consume = New-Processor -RootId $root -Type "org.apache.nifi.processors.kafka.pubsub.ConsumeKafka_2_6" -X 0 -Y 0
  $invoke = New-Processor -RootId $root -Type "org.apache.nifi.processors.standard.InvokeHTTP" -X 380 -Y 0
  $routeResult = New-Processor -RootId $root -Type "org.apache.nifi.processors.standard.RouteOnAttribute" -X 760 -Y 0
  $publishSuccess = New-Processor -RootId $root -Type "org.apache.nifi.processors.kafka.pubsub.PublishKafka_2_6" -X 1140 -Y 0
  $publishDlq = New-Processor -RootId $root -Type "org.apache.nifi.processors.kafka.pubsub.PublishKafka_2_6" -X 1140 -Y 220

  $consume = Update-Processor -Processor $consume -Properties @{
    "bootstrap.servers" = $KafkaBootstrap
    "topic" = $RawTopic
    "group.id" = $GroupId
    "auto.offset.reset" = "earliest"
    "max.poll.records" = "10"
  }
  $invoke = Update-Processor -Processor $invoke -Properties @{
    "HTTP Method" = "POST"
    "Remote URL" = $RuntimeUrl
    "Content-Type" = "application/json"
    "Always Output Response" = "true"
    "X-FlowPlane-Source-Topic" = $RawTopic
    "X-FlowPlane-Source-Key" = "nifi-kafka-streaming"
  } -AutoTerminate @("Original", "No Retry", "Retry", "Failure")
  $routeResult = Update-Processor -Processor $routeResult -Properties @{
    "transformed" = '${invokehttp.status.code:equals("200")}'
    "dlq" = '${invokehttp.status.code:equals("422")}'
  } -AutoTerminate @("unmatched")
  $publishSuccess = Update-Processor -Processor $publishSuccess -Properties @{
    "bootstrap.servers" = $KafkaBootstrap
    "topic" = $TransformedTopic
    "use-transactions" = "false"
  } -AutoTerminate @("success", "failure")
  $publishDlq = Update-Processor -Processor $publishDlq -Properties @{
    "bootstrap.servers" = $KafkaBootstrap
    "topic" = $DlqTopic
    "use-transactions" = "false"
  } -AutoTerminate @("success", "failure")

  New-Connection -RootId $root -Source $consume -Destination $invoke -Relationships @("success")
  New-Connection -RootId $root -Source $invoke -Destination $routeResult -Relationships @("Response")
  New-Connection -RootId $root -Source $routeResult -Destination $publishSuccess -Relationships @("transformed")
  New-Connection -RootId $root -Source $routeResult -Destination $publishDlq -Relationships @("dlq")

  foreach ($processor in @($publishSuccess, $publishDlq, $routeResult, $invoke, $consume)) {
    Start-Processor -Processor $processor
  }

  Start-Sleep -Seconds 4
  if ($FullPressureRun) {
    Publish-KafkaLines -Topic $RawTopic -Payloads @($InvalidPayload)
  }
  $publishSummary = Publish-KafkaRoundRobin -Topic $RawTopic -Payloads $ValidPayloads -RecordCount $ValidRecordCount -RatePerSecond $TargetRps -DurationSeconds $(if ($FullPressureRun) { $DurationSeconds } else { 0 })
  if (-not $FullPressureRun) {
    Publish-KafkaLines -Topic $RawTopic -Payloads @($InvalidPayload)
  }

  $expectedTransformedCount = if ($FullPressureRun) { $publishSummary.sentValidRecords } else { $ValidRecordCount }
  $wait = Wait-KafkaTopicCount -Topic $TransformedTopic -ExpectedCount $expectedTransformedCount -TimeoutSeconds $OutputObservationSeconds -RequireComplete (-not $FullPressureRun)
  $sampleCount = if ($FullPressureRun) { [Math]::Min(10, [int]$wait.count) } else { $ValidRecordCount }
  $transformedRecords = @()
  if ($sampleCount -gt 0) {
    $transformedRecords = @(Read-KafkaMany -Topic $TransformedTopic -Count $sampleCount)
  }
  $dlqWait = Wait-KafkaTopicCount -Topic $DlqTopic -ExpectedCount 1 -TimeoutSeconds 60 -RequireComplete $true
  $dlq = if ($dlqWait.count -gt 0) { @(Read-KafkaMany -Topic $DlqTopic -Count 1)[0] } else { "" }
  $TransformedEventIds = @($transformedRecords | ForEach-Object { (($_ | ConvertFrom-Json).event_id) })
  if (-not $FullPressureRun) {
    Assert-EventIdMultiset -Expected $ExpectedEventIds -Actual $TransformedEventIds
  } elseif ($TransformedEventIds | Where-Object { $ExpectedEventIds -notcontains $_ }) {
    throw "Observed transformed event id outside the expected round-robin corpus"
  }
  $sampleTransformed = if ($transformedRecords.Count -gt 0) {
    @($transformedRecords | Where-Object { (($_ | ConvertFrom-Json).event_id) -eq $ExpectedEventIds[0] } | Select-Object -First 1)[0]
  } else {
    ""
  }

  if ($dlq -notmatch 'VALIDATION_FAILED') {
    throw "Unexpected DLQ payload from $DlqTopic`: $dlq"
  }

  $observedSeconds = [Math]::Max(0.001, $publishSummary.elapsedSeconds + $wait.waitedSeconds)
  $observedTransformedRps = [Math]::Round($wait.count / $observedSeconds, 2)
  $finalStatus = if ($FullPressureRun) { "MEASURED" } elseif ($wait.status -eq "PASS" -and $dlqWait.status -eq "PASS") { "PASS" } else { "FAIL" }
  Write-Host "NiFi Kafka streaming HTTP run completed."
  Write-Host "RawTopic=$RawTopic"
  Write-Host "TransformedTopic=$TransformedTopic"
  Write-Host "DlqTopic=$DlqTopic"
  Write-Host "ValidRecordCount=$ValidRecordCount"
  Write-Host "SentValidRecords=$($publishSummary.sentValidRecords)"
  Write-Host "PublishElapsedSeconds=$($publishSummary.elapsedSeconds)"
  Write-Host "ObservedPublishRps=$($publishSummary.observedPublishRps)"
  Write-Host "RoundRobinPayloadCount=$($ValidPayloads.Count)"
  Write-Host "InputPattern=round-robin"
  Write-Host "PublisherMode=kafka-console-producer-in-container-python-round-robin-paced-stream"
  Write-Host "TargetRps=$TargetRps"
  Write-Host "DurationSeconds=$DurationSeconds"
  Write-Host "OutputObservationSeconds=$OutputObservationSeconds"
  Write-Host "ObservedTransformedRps=$observedTransformedRps"
  Write-Host "AchievedRps=$observedTransformedRps"
  Write-Host "OutputRateIsProducerRateGate=false"
  if ($ExpectedEventIds.Count -le 1000) {
    Write-Host "ExpectedEventIds=$($ExpectedEventIds -join ',')"
  } else {
    Write-Host "ExpectedEventIdCount=$($publishSummary.sentValidRecords)"
    Write-Host "ExpectedEventIdsPreview=$(@($ExpectedEventIds | Select-Object -First 50) -join ',')"
  }
  Write-Host "TransformedCount=$($wait.count)"
  if (-not $FullPressureRun -and $TransformedEventIds.Count -le 1000) {
    Write-Host "TransformedEventIds=$($TransformedEventIds -join ',')"
  } else {
    Write-Host "TransformedEventIdsPreview=$(@($TransformedEventIds | Select-Object -First 50) -join ',')"
  }
  if ($FullPressureRun) {
    $samplePreview = if ($sampleTransformed.Length -gt 1000) { $sampleTransformed.Substring(0, 1000) } else { $sampleTransformed }
    Write-Host "TransformedSamplePreview=$samplePreview"
  } else {
    Write-Host "Transformed=$sampleTransformed"
  }
  Write-Host "Dlq=$dlq"
  Write-Host "FinalStatus=$finalStatus"
  if ($finalStatus -eq "FAIL") {
    exit 2
  }
} finally {
  if (-not $KeepRunning) {
    try {
      docker rm -f $NifiContainer | Out-Null
    } catch {
      Write-Warning "Could not remove $NifiContainer`: $($_.Exception.Message)"
    }
  }
}



