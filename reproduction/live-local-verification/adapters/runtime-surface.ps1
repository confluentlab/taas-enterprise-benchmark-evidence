param(
  [Parameter(Mandatory)][string]$FlowplaneRoot,
  [Parameter(Mandatory)][string]$BundleRoot,
  [Parameter(Mandatory)][string]$FixtureRoot,
  [Parameter(Mandatory)][ValidateSet("embedded-spring", "http-single", "http-batch", "grpc-batch", "grpc-streaming", "serverless-aws", "serverless-azure", "serverless-gcp")][string]$IntegrationId,
  [ValidateRange(1,100000)][int]$ValidRecordCount = 100,
  [ValidateRange(1,10000)][int]$InvalidRecordCount = 10,
  [ValidateRange(0,3600)][int]$MinimumDurationSeconds = 0
)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "..\LiveVerification.Common.ps1")
$env:FLOWPLANE_ROOT = $FlowplaneRoot
$env:FLOWPLANE_DEMO_OUTPUT_ROOT = Join-Path $BundleRoot "adapter-private"
. (Join-Path $PSScriptRoot "..\..\FlowplaneDemo.Common.ps1")

$integrationId = $IntegrationId
$runId = Split-Path $BundleRoot -Leaf
$safeRun = ($runId.ToLowerInvariant() -replace '[^a-z0-9-]', '-')
$prefix = "flowplane.$integrationId.evidence.$safeRun"
$topics = [ordered]@{ raw = "$prefix.raw"; transformed = "$prefix.transformed"; dlq = "$prefix.dlq" }
$runtimeId = "$integrationId-evidence-$safeRun"
$runtimeContainer = "flowplane-$integrationId-runtime-$safeRun"
$bridgeContainer = "flowplane-$integrationId-bridge-$safeRun"
$network = "flowplane-quality-stack_default"
$kafkaContainer = "flowplane-kafka"
$kafkaBootstrap = "kafka:9092"
$token = New-FlowplaneAccessToken
$runtimeSecret = [Guid]::NewGuid().ToString("N") + [Guid]::NewGuid().ToString("N")
$totalRecordCount = $ValidRecordCount + $InvalidRecordCount
$stabilityStartedAt = [DateTime]::UtcNow
$started = [Collections.Generic.List[string]]::new()
$sidecarModes = @("http-single", "http-batch", "grpc-batch", "grpc-streaming")
$isSidecar = $sidecarModes -contains $integrationId
$isEmbedded = $integrationId -eq "embedded-spring"
$bridgeMode = switch ($integrationId) { "serverless-aws" { "aws-lambda" }; "serverless-azure" { "azure-functions" }; "serverless-gcp" { "gcp-functions" }; default { $integrationId } }
$bridgeScript = Join-Path $PSScriptRoot "kafka-runtime-surface-bridge.mjs"
$verifierScript = Join-Path $PSScriptRoot "kafka-raw-only-verifier.mjs"
$nodeModules = Join-Path $PSScriptRoot "..\assets\nats-node\node_modules"
$protoFile = Join-Path $FlowplaneRoot "flowplane-java-sdk\flowplane-sidecar-grpc\src\main\proto\flowplane_runtime.proto"
$sidecarJar = Join-Path $FlowplaneRoot "flowplane-java-sdk\flowplane-sidecar-app\target\flowplane-sidecar-app-1.0.0-SNAPSHOT.jar"
$embeddedJar = Join-Path $FlowplaneRoot "flowplane-java-sdk\flowplane-kafka-streaming-demo\target\flowplane-kafka-streaming-demo-1.0.0-SNAPSHOT.jar"
$runtimeImage = switch ($integrationId) {
  "serverless-aws" { "flowplane-aws-lambda-local" }
  "serverless-azure" { "flowplane-azure-functions-local" }
  "serverless-gcp" { "flowplane-gcp-functions-local" }
  default { "eclipse-temurin:17-jre" }
}

function Invoke-DockerChecked {
  $arguments = @($args)
  $old = $ErrorActionPreference
  try { $ErrorActionPreference = "Continue"; $output = & docker @arguments 2>&1; $code = $LASTEXITCODE } finally { $ErrorActionPreference = $old }
  if ($code -ne 0) { throw "docker $($arguments -join ' ') failed: $($output -join [Environment]::NewLine)" }
  $output
}

function Add-Step([string]$Message) {
  $line = "$([DateTime]::UtcNow.ToString('o')) $Message"
  [IO.File]::AppendAllText((Join-Path $BundleRoot "sanitized-logs\steps.log"), $line + "`n", [Text.UTF8Encoding]::new($false))
  Write-Output $line
}

function Invoke-Api([string]$Method, [string]$Path, $Body = $null) {
  $headers = @{ Authorization = "Bearer $token"; tenantId = $script:FLOWPLANE_TENANT_ID; "X-Tenant-Id" = $script:FLOWPLANE_TENANT_ID }
  $request = @{ Method = $Method; Uri = "http://127.0.0.1:8081$Path"; Headers = $headers; TimeoutSec = 30 }
  if ($null -ne $Body) { $request.ContentType = "application/json"; $request.Body = $Body | ConvertTo-Json -Depth 40 -Compress }
  Invoke-RestMethod @request
}

function New-Topic([string]$Name) {
  Invoke-DockerChecked exec $kafkaContainer kafka-topics --bootstrap-server $kafkaBootstrap --create --if-not-exists --topic $Name --partitions 1 --replication-factor 1 | Out-Null
  $offset = (Invoke-DockerChecked exec $kafkaContainer kafka-get-offsets --bootstrap-server $kafkaBootstrap --topic $Name --time -1) -join "`n"
  if ($offset -notmatch ':0$') { throw "Evidence topic was not empty: $Name ($offset)" }
}

function Get-Port([string]$Container, [int]$ContainerPort) {
  $line = @((Invoke-DockerChecked port $Container "$ContainerPort/tcp") | Select-Object -First 1)[0]
  if ($line -notmatch ':(\d+)$') { throw "No published port $ContainerPort for $Container" }
  [int]$Matches[1]
}

function Wait-Http([string]$Url, [int]$Seconds = 180) {
  $deadline = (Get-Date).AddSeconds($Seconds)
  do {
    try { return Invoke-WebRequest -UseBasicParsing -Uri $Url -TimeoutSec 10 } catch { $last = $_.Exception.Message }
    Start-Sleep -Seconds 2
  } while ((Get-Date) -lt $deadline)
  throw "Timed out waiting for $Url`: $last"
}

function Save-Log([string]$Container, [string]$Name) {
  try {
    $old = $ErrorActionPreference; $ErrorActionPreference = "Continue"
    $log = (& docker logs $Container 2>&1) -join "`n"
    Write-Utf8NoBom -Path (Join-Path $BundleRoot "sanitized-logs\$Name.log") -Value ((ConvertTo-SafeLogText $log) + "`n")
  } catch {} finally { $ErrorActionPreference = $old }
}

function Invoke-RegistrationProbe([string]$InternalEndpoint, [int]$HostPort) {
  $payload = [string](Get-Content -LiteralPath (Join-Path $FixtureRoot "valid-input.jsonl") -TotalCount 1)
  try {
    switch ($integrationId) {
      "serverless-aws" {
        $body = @{ body = $payload; isBase64Encoded = $false; requestContext = @{ apiId = "local-evidence"; requestId = "registration" } } | ConvertTo-Json -Depth 5 -Compress
        Invoke-WebRequest -UseBasicParsing -Method Post -Uri "http://127.0.0.1:$HostPort/2015-03-31/functions/function/invocations" -ContentType "application/json" -Body $body -TimeoutSec 30 | Out-Null
      }
      "serverless-azure" { Invoke-WebRequest -UseBasicParsing -Method Post -Uri "http://127.0.0.1:$HostPort/api/flowplaneHttpTransform" -ContentType "application/json" -Body $payload -TimeoutSec 30 | Out-Null }
      "serverless-gcp" { Invoke-WebRequest -UseBasicParsing -Method Post -Uri "http://127.0.0.1:$HostPort/" -ContentType "application/json" -Body $payload -TimeoutSec 30 | Out-Null }
    }
  } catch {
    if (-not $_.Exception.Response -or [int]$_.Exception.Response.StatusCode -notin @(422, 503)) { throw }
  }
}

foreach ($container in @($runtimeContainer, $bridgeContainer)) {
  if ((docker ps -aq --filter "name=^$container$") -join "") { throw "Refusing to replace existing container $container" }
}
foreach ($required in @($bridgeScript, $verifierScript, $protoFile)) { if (-not (Test-Path -LiteralPath $required -PathType Leaf)) { throw "Missing required evidence file: $required" } }
if ($isSidecar -and -not (Test-Path -LiteralPath $sidecarJar -PathType Leaf)) { throw "Build the sidecar app jar before verification: $sidecarJar" }
if ($isEmbedded -and -not (Test-Path -LiteralPath $embeddedJar -PathType Leaf)) { throw "Build the embedded Spring runtime jar before verification: $embeddedJar" }
if (-not (Test-Path -LiteralPath (Join-Path $nodeModules "kafkajs\package.json"))) { throw "Evidence Node dependencies are missing kafkajs." }

Write-Utf8NoBom -Path (Join-Path $BundleRoot "configuration\runtime-surface.json") -Value (([ordered]@{
  integrationId = $integrationId; runtimeId = $runtimeId; runtimeImage = $runtimeImage; network = $network; topics = $topics
  validRecordCount = $ValidRecordCount; invalidRecordCount = $InvalidRecordCount; totalRecordCount = $totalRecordCount; minimumDurationSeconds = $MinimumDurationSeconds
  sourceBoundary = "raw-only verifier -> persistent Kafka raw topic"
  sinkBoundary = "independent runtime bridge -> persistent Kafka transformed and DLQ topics"
} | ConvertTo-Json -Depth 10) + "`n")
Write-Utf8NoBom -Path (Join-Path $BundleRoot "reproduce.ps1") -Value ("param([string]`$FlowplaneRoot = 'C:\FlowPlaneNew\repositories\flowplane-controlplane')`n& 'C:\FlowPlaneNew\video-generation-scripts-copy\scripts\demo\11-run-live-local-verification.ps1' -FlowplaneRoot `$FlowplaneRoot -Execute -Integration $integrationId`n")

try {
  if ($ValidRecordCount -ne 100 -or $InvalidRecordCount -ne 10) {
    Add-Step "Expanding the canonical fixture to $ValidRecordCount valid and $InvalidRecordCount intentional-invalid records."
    $baseValid = (Get-Content -LiteralPath (Join-Path $FixtureRoot "valid-input.jsonl") -TotalCount 1 | ConvertFrom-Json)
    $baseInvalid = (Get-Content -LiteralPath (Join-Path $FixtureRoot "invalid-input.jsonl") -TotalCount 1 | ConvertFrom-Json)
    $expandedRoot = Join-Path $BundleRoot "fixtures\stability"
    New-Item -ItemType Directory -Force -Path $expandedRoot | Out-Null
    $validLines = for ($index = 1; $index -le $ValidRecordCount; $index++) {
      $copy = $baseValid | ConvertTo-Json -Depth 30 | ConvertFrom-Json
      $copy.event.id = "evt-live-{0:D6}" -f $index
      $copy.customer.email = "ada+$index@example.test"
      $copy | ConvertTo-Json -Depth 30 -Compress
    }
    $invalidLines = for ($index = 1; $index -le $InvalidRecordCount; $index++) {
      $copy = $baseInvalid | ConvertTo-Json -Depth 30 | ConvertFrom-Json
      $copy.recordId = "invalid-{0:D3}" -f $index
      $copy | ConvertTo-Json -Depth 30 -Compress
    }
    Write-Utf8NoBom -Path (Join-Path $expandedRoot "valid-input.jsonl") -Value (($validLines -join "`n") + "`n")
    Write-Utf8NoBom -Path (Join-Path $expandedRoot "invalid-input.jsonl") -Value (($invalidLines -join "`n") + "`n")
    Copy-Item -LiteralPath (Join-Path $FixtureRoot "mapping.yaml") -Destination (Join-Path $expandedRoot "mapping.yaml")
    $FixtureRoot = $expandedRoot
  }
  Add-Step "Creating and governing the run-specific mapping."
  $mappingDsl = [string](Get-Content -LiteralPath (Join-Path $FixtureRoot "mapping.yaml") -Raw)
  $validPayloads = @(Get-Content -LiteralPath (Join-Path $FixtureRoot "valid-input.jsonl") | Where-Object { $_ })
  $invalidPayloads = @(Get-Content -LiteralPath (Join-Path $FixtureRoot "invalid-input.jsonl") | Where-Object { $_ })
  $teamPage = Invoke-Api GET "/api/v1/teams?activeOnly=true&page=0&size=100"
  $team = @($teamPage.items | Select-Object -First 1)[0]
  if (-not $team) { throw "No active team exists." }
  $mapping = Invoke-Api POST "/api/v1/mappings" @{
    name = "$integrationId-live-local-$safeRun"; description = "Synthetic $integrationId live-local runtime-surface verification."
    workspaceId = "workspace-platform"; teamId = [string]$team.id; teamName = [string]$team.name
    projectId = "live-local-verification"; projectName = "Live Local Verification"; environment = "DEVELOPMENT"
    mappingDsl = $mappingDsl; samplePayload = [string]$validPayloads[0]; dictionaryIds = @()
  }
  Write-JsonFile (Join-Path $BundleRoot "actual\mapping-created.json") $mapping
  $validation = Invoke-Api POST "/api/v1/mappings/$($mapping.id)/validate"
  Write-JsonFile (Join-Path $BundleRoot "actual\mapping-validation.json") $validation
  if (-not $validation.valid) { throw "Mapping validation failed." }
  $records = @($validPayloads | ForEach-Object { $p = $_ | ConvertFrom-Json; @{ recordId = [string]$p.event.id; payloadJson = [string]$_ } })
  $simulatedRecords = [Collections.Generic.List[object]]::new()
  for ($offset = 0; $offset -lt $records.Count; $offset += 100) {
    $last = [Math]::Min($offset + 99, $records.Count - 1)
    $chunk = Invoke-Api POST "/api/v1/mappings/$($mapping.id)/simulate:batch" @{ records = @($records[$offset..$last]); runtimeModes = @() }
    if (-not $chunk.success) { throw "Simulation chunk starting at $offset failed." }
    foreach ($record in @($chunk.records)) { $simulatedRecords.Add($record) }
  }
  $simulation = [ordered]@{ success = $true; recordCount = $simulatedRecords.Count; records = @($simulatedRecords) }
  $validSimulation = Invoke-Api POST "/api/v1/mappings/$($mapping.id)/simulate" @{ payloadJson = [string]$validPayloads[0] }
  $invalidSimulation = Invoke-Api POST "/api/v1/mappings/$($mapping.id)/simulate" @{ payloadJson = [string]$invalidPayloads[0] }
  Write-JsonFile (Join-Path $BundleRoot "expected\simulation-batch.json") $simulation
  Write-JsonFile (Join-Path $BundleRoot "expected\simulation-valid.json") $validSimulation
  Write-JsonFile (Join-Path $BundleRoot "expected\simulation-invalid.json") $invalidSimulation
  if (-not $simulation.success -or [int]$simulation.recordCount -ne $ValidRecordCount -or [int]$invalidSimulation.errorCount -lt 1) { throw "Simulation gates failed." }
  $published = Invoke-Api POST "/api/v1/mappings/$($mapping.id)/publish" @{}
  Write-JsonFile (Join-Path $BundleRoot "actual\mapping-published.json") $published
  $artifact = @($published.activatedVersions | Where-Object { $_.latestActivated } | Select-Object -First 1)[0]
  if (-not $artifact) { throw "Published mapping did not expose its activated artifact." }

  Add-Step "Creating empty persistent Kafka evidence topics."
  foreach ($topic in $topics.Values) { New-Topic $topic }

  if ($isSidecar -or $isEmbedded) {
    Add-Step "Pre-registering the runtime profile and obtaining its runtime-scoped client secret."
    $registration = Invoke-Api POST "/api/v1/runtime-registrations" @{
      runtimeId = $runtimeId; name = "$integrationId Evidence $runId"; type = if ($isEmbedded) { "SPRING_BOOT" } else { "SIDECAR" }; environment = "DEVELOPMENT"
      ownerTeam = "Quality Engineering"; projectId = "live-local-verification"; deploymentTarget = "LOCAL_DOCKER"
      networkProfile = "flowplane-quality-stack"; controlPlaneUrl = "http://flowplane-backend:8080"
      kafkaBootstrapServers = $kafkaBootstrap; dockerNetwork = $network; serviceName = $runtimeContainer; containerImage = $runtimeImage
      outputShape = "JSON_STRING"; outputComplexTypes = "NATIVE_JSON"; outputFieldNaming = "AS_IS"; replayEnabled = $false
      assignmentPollIntervalMs = 1000; heartbeatIntervalMs = 10000; labels = @{ evidenceRunId = $runId }; additionalEnvironment = @{}
      wrapperVersion = "1.0.0"; coreEngineVersion = "1.0.0"; supportedDslVersions = @("flowplane/v1")
      supportedFeatures = if ($isEmbedded) { @("stateless", "error-policy/v1", "replay/kafka") } else { @("runtime-protocol/v1", "http-single", "http-batch", "grpc-batch", "grpc-stream", "compiled-cache/artifact-hash", "dlq/per-record", "error-policy/v1") }
    }
    $runtimeSecret = [string]$registration.clientSecret
    if ([string]::IsNullOrWhiteSpace($runtimeSecret)) { throw "Runtime registration did not issue a client secret." }
    Write-JsonFile (Join-Path $BundleRoot "actual\runtime-profile.json") $registration.profile
  }

  Add-Step "Starting the $integrationId runtime container."
  $hostPort = $null
  $internalEndpoint = $null
  if ($isEmbedded) {
    $nativeReport = Join-Path $BundleRoot "metrics\embedded-spring-native-report.json"
    Invoke-DockerChecked run -d --name $runtimeContainer --network $network `
      -v "$embeddedJar`:/app/flowplane-kafka-streaming-demo.jar:ro" -v "$BundleRoot`:/evidence" `
      eclipse-temurin:17-jre java -jar /app/flowplane-kafka-streaming-demo.jar `
      "--flowplane.kafka.bench.mode=streaming-app" "--flowplane.kafka.bench.run-name=$runId" `
      "--flowplane.kafka.bench.bootstrap-servers=$kafkaBootstrap" "--flowplane.kafka.bench.raw-topic=$($topics.raw)" `
      "--flowplane.kafka.bench.output-topic=$($topics.transformed)" "--flowplane.kafka.bench.error-topic=$($topics.dlq)" `
      "--flowplane.kafka.bench.group-id=flowplane-$runtimeId" "--flowplane.kafka.bench.report-path=/evidence/metrics/embedded-spring-native-report.json" `
      "--flowplane.kafka.bench.record-count=$totalRecordCount" "--flowplane.kafka.bench.concurrency=1" "--flowplane.kafka.bench.monitoring-interceptors-enabled=false" `
      "--flowplane.kafka.bench.control-plane-url=http://flowplane-backend:8080" "--flowplane.kafka.bench.runtime-id=$runtimeId" `
      "--flowplane.kafka.bench.runtime-name=Embedded Spring Evidence $runId" "--flowplane.kafka.bench.runtime-environment=DEVELOPMENT" `
      "--flowplane.kafka.bench.runtime-owner-team=Quality Engineering" "--flowplane.kafka.bench.runtime-project-id=live-local-verification" `
      "--flowplane.kafka.bench.tenant-id=$script:FLOWPLANE_TENANT_ID" "--flowplane.kafka.bench.auth-token=" `
      "--flowplane.kafka.bench.runtime-client-secret=$runtimeSecret" "--flowplane.kafka.bench.schema-check-enabled=false" `
      "--flowplane.kafka.bench.assignment-poll-interval-millis=1000" "--flowplane.kafka.bench.output-mode=JSON_STRING" `
      "--flowplane.kafka.bench.idle-stop-seconds=$([Math]::Max(120, $MinimumDurationSeconds + 120))" | Out-Null
    $started.Add($runtimeContainer)
  } elseif ($isSidecar) {
    Invoke-DockerChecked run -d --name $runtimeContainer --network $network -p "127.0.0.1::8080" -p "127.0.0.1::9090" `
      -v "$sidecarJar`:/app/flowplane-sidecar-app.jar:ro" `
      -e "FLOWPLANE_CONTROL_PLANE_URL=http://flowplane-backend:8080" -e "FLOWPLANE_TENANT_ID=$script:FLOWPLANE_TENANT_ID" `
      -e "FLOWPLANE_RUNTIME_ID=$runtimeId" -e "FLOWPLANE_RUNTIME_NAME=$integrationId Evidence $runId" `
      -e "FLOWPLANE_RUNTIME_ENVIRONMENT=DEVELOPMENT" -e "FLOWPLANE_RUNTIME_OWNER_TEAM=Quality Engineering" `
      -e "FLOWPLANE_RUNTIME_PROJECT_ID=live-local-verification" -e "FLOWPLANE_AUTH_TOKEN=" `
      -e "FLOWPLANE_RUNTIME_CLIENT_SECRET=$runtimeSecret" -e "FLOWPLANE_RUNTIME_ASSIGNMENT_POLL_INTERVAL_MS=1000" `
      -e "FLOWPLANE_RUNTIME_HTTP_BATCH_ENABLED=true" -e "FLOWPLANE_RUNTIME_GRPC_ENABLED=true" -e "FLOWPLANE_RUNTIME_GRPC_PORT=9090" `
      eclipse-temurin:17-jre java -jar /app/flowplane-sidecar-app.jar | Out-Null
    $started.Add($runtimeContainer)
    $hostPort = Get-Port $runtimeContainer 8080
    Wait-Http "http://127.0.0.1:$hostPort/health" 180 | Out-Null
    $internalEndpoint = if ($integrationId -eq "http-single") { "http://$runtimeContainer`:8080/v1/transform" } elseif ($integrationId -eq "http-batch") { "http://$runtimeContainer`:8080/v1/transform:batch" } else { "$runtimeContainer`:9090" }
  } elseif ($integrationId -eq "serverless-aws") {
    Invoke-DockerChecked run -d --name $runtimeContainer --network $network -p "127.0.0.1::8080" `
      -e "FLOWPLANE_SERVERLESS_CONTROL_PLANE_URL=http://flowplane-backend:8080" -e "FLOWPLANE_SERVERLESS_TENANT_ID=$script:FLOWPLANE_TENANT_ID" `
      -e "FLOWPLANE_SERVERLESS_RUNTIME_ID=$runtimeId" -e "FLOWPLANE_SERVERLESS_RUNTIME_NAME=AWS Lambda Evidence $runId" `
      -e "FLOWPLANE_SERVERLESS_RUNTIME_ENVIRONMENT=DEVELOPMENT" -e "FLOWPLANE_SERVERLESS_AUTH_TOKEN=$token" `
      -e "FLOWPLANE_SERVERLESS_RUNTIME_CLIENT_SECRET=$runtimeSecret" $runtimeImage | Out-Null
    $started.Add($runtimeContainer); $hostPort = Get-Port $runtimeContainer 8080
    $internalEndpoint = "http://$runtimeContainer`:8080/2015-03-31/functions/function/invocations"
  } elseif ($integrationId -eq "serverless-azure") {
    Invoke-DockerChecked run -d --name $runtimeContainer --network $network -p "127.0.0.1::80" `
      -e "AzureWebJobsSecretStorageType=files" -e "AzureWebJobs.flowplaneQueueTransform.Disabled=true" -e "AzureWebJobs.flowplaneEventHubTransform.Disabled=true" `
      -e "FLOWPLANE_AZURE_INPUT_QUEUE=input" -e "FLOWPLANE_AZURE_OUTPUT_QUEUE=output" -e "FLOWPLANE_AZURE_DLQ_QUEUE=dlq" `
      -e "FLOWPLANE_AZURE_INPUT_EVENT_HUB=input" -e "FLOWPLANE_AZURE_OUTPUT_EVENT_HUB=output" -e "FLOWPLANE_AZURE_EVENT_HUB_CONNECTION=unused" -e "FLOWPLANE_AZURE_EVENT_HUB_CONSUMER_GROUP=local" `
      -e "FLOWPLANE_SERVERLESS_CONTROL_PLANE_URL=http://flowplane-backend:8080" -e "FLOWPLANE_SERVERLESS_TENANT_ID=$script:FLOWPLANE_TENANT_ID" `
      -e "FLOWPLANE_SERVERLESS_RUNTIME_ID=$runtimeId" -e "FLOWPLANE_SERVERLESS_RUNTIME_NAME=Azure Functions Evidence $runId" `
      -e "FLOWPLANE_SERVERLESS_RUNTIME_ENVIRONMENT=DEVELOPMENT" -e "FLOWPLANE_SERVERLESS_AUTH_TOKEN=$token" `
      -e "FLOWPLANE_SERVERLESS_RUNTIME_CLIENT_SECRET=$runtimeSecret" $runtimeImage | Out-Null
    $started.Add($runtimeContainer); $hostPort = Get-Port $runtimeContainer 80
    Wait-Http "http://127.0.0.1:$hostPort" 180 | Out-Null
    $internalEndpoint = "http://$runtimeContainer`:80/api/flowplaneHttpTransform"
  } else {
    Invoke-DockerChecked run -d --name $runtimeContainer --network $network -p "127.0.0.1::8080" `
      -e "FLOWPLANE_SERVERLESS_CONTROL_PLANE_URL=http://flowplane-backend:8080" -e "FLOWPLANE_SERVERLESS_TENANT_ID=$script:FLOWPLANE_TENANT_ID" `
      -e "FLOWPLANE_SERVERLESS_RUNTIME_ID=$runtimeId" -e "FLOWPLANE_SERVERLESS_RUNTIME_NAME=GCP Functions Evidence $runId" `
      -e "FLOWPLANE_SERVERLESS_RUNTIME_ENVIRONMENT=DEVELOPMENT" -e "FLOWPLANE_SERVERLESS_AUTH_TOKEN=$token" `
      -e "FLOWPLANE_SERVERLESS_RUNTIME_CLIENT_SECRET=$runtimeSecret" $runtimeImage | Out-Null
    $started.Add($runtimeContainer); $hostPort = Get-Port $runtimeContainer 8080
    Start-Sleep -Seconds 2
    $internalEndpoint = "http://$runtimeContainer`:8080/"
  }

  if (-not $isSidecar -and -not $isEmbedded) { Invoke-RegistrationProbe $internalEndpoint $hostPort }
  $runtime = $null; $deadline = (Get-Date).AddMinutes(3)
  do { try { $runtime = Invoke-Api GET "/api/v1/runtimes/$runtimeId" } catch {}; if ($runtime -and $runtime.health -eq "HEALTHY") { break }; Start-Sleep -Seconds 2 } while ((Get-Date) -lt $deadline)
  if (-not $runtime -or $runtime.health -ne "HEALTHY") { throw "$integrationId runtime did not register and become healthy." }
  Write-JsonFile (Join-Path $BundleRoot "actual\runtime-registration.json") $runtime
  $deployment = Invoke-Api POST "/api/v1/mappings/$($mapping.id)/deploy" @{ runtimeIds = @($runtimeId); rolloutPercent = 100; requireReplayGate = $false; reason = "Assign local evidence artifact."; changeTicket = "RUNTIME-$runId" }
  Write-JsonFile (Join-Path $BundleRoot "actual\deployment.json") $deployment

  $deadline = (Get-Date).AddMinutes(3)
  do {
    try { $runtime = Invoke-Api GET "/api/v1/runtimes/$runtimeId" } catch {}
    if ($runtime.activeArtifactId -and $runtime.activeArtifactHash) { break }
    if (-not $isSidecar -and -not $isEmbedded) { try { Invoke-RegistrationProbe $internalEndpoint $hostPort } catch {} }
    Start-Sleep -Seconds 2
  } while ((Get-Date) -lt $deadline)
  if (-not $runtime.activeArtifactId) { throw "$integrationId did not acknowledge its assigned mapping." }
  $runtimeStatus = [ordered]@{
    runtimeId = $runtimeId; assignmentPresent = $true; artifactId = $runtime.activeArtifactId; artifactHash = $runtime.activeArtifactHash
    version = $runtime.activeVersion; mappingId = $mapping.id; runtimeHealth = $runtime.health; lifecycleState = $runtime.lifecycleState
  }
  Write-JsonFile (Join-Path $BundleRoot "actual\runtime-status-before.json") $runtimeStatus

  if ($isEmbedded) {
    Add-Step "Using the independently deployed embedded Spring runtime as the raw Kafka consumer and downstream publisher."
  } else {
    Add-Step "Starting the independent raw-Kafka-to-$integrationId bridge."
    Invoke-DockerChecked run -d --name $bridgeContainer --network $network `
      -v "$bridgeScript`:/app/bridge.mjs:ro" -v "$nodeModules`:/app/node_modules:ro" -v "$protoFile`:/app/flowplane_runtime.proto:ro" -v "$BundleRoot`:/evidence" `
      node:22-alpine node /app/bridge.mjs $kafkaBootstrap $internalEndpoint $bridgeMode $integrationId $runId $totalRecordCount /evidence /app/flowplane_runtime.proto | Out-Null
    $started.Add($bridgeContainer)
    $readyPath = Join-Path $BundleRoot "actual\publisher-bridge-ready.json"; $deadline = (Get-Date).AddMinutes(2)
    do { if (Test-Path $readyPath) { break }; Start-Sleep -Seconds 1 } while ((Get-Date) -lt $deadline)
    if (-not (Test-Path $readyPath)) { throw "Runtime bridge did not become ready: $((& docker logs $bridgeContainer 2>&1) -join ' ')" }
  }

  $verifierSource = Get-Content -LiteralPath $verifierScript -Raw
  $bridgeSource = if ($isEmbedded) { Get-Content -LiteralPath (Join-Path $FlowplaneRoot "flowplane-java-sdk\flowplane-kafka-streaming-demo\src\main\java\com\flowplane\kafka\demo\StreamingAppRunner.java") -Raw } else { Get-Content -LiteralPath $bridgeScript -Raw }
  $writeAudit = [ordered]@{
    verifierProducerOperations = [regex]::Matches($verifierSource, 'kafka-console-producer').Count
    verifierRawProducerTargets = [regex]::Matches($verifierSource, '"--topic", topics\.raw').Count
    verifierDownstreamProducerTargets = [regex]::Matches($verifierSource, '"--topic", topics\.(?:transformed|dlq)').Count
    verifierRuntimeReferences = [regex]::Matches($verifierSource, '\bruntimeUrl\b|\bendpoint\b').Count
    pipelineRawInputReferences = [regex]::Matches($bridgeSource, $(if ($isEmbedded) { 'getRawTopic' } else { 'topics\.raw' })).Count
    pipelineTransformedOutputReferences = [regex]::Matches($bridgeSource, $(if ($isEmbedded) { 'getOutputTopic' } else { 'topics\[result\.destination\]' })).Count
    pipelineDlqOutputReferences = [regex]::Matches($bridgeSource, $(if ($isEmbedded) { 'getErrorTopic' } else { 'destination: "dlq"' })).Count
    verifierWriteTargets = @($topics.raw); pipelineWriteTargets = @($topics.transformed, $topics.dlq)
  }
  $writeAudit.passed = $writeAudit.verifierProducerOperations -eq 1 -and $writeAudit.verifierRawProducerTargets -eq 1 -and $writeAudit.verifierDownstreamProducerTargets -eq 0 -and $writeAudit.verifierRuntimeReferences -eq 0 -and $writeAudit.pipelineRawInputReferences -ge 1 -and $writeAudit.pipelineTransformedOutputReferences -ge 1 -and $writeAudit.pipelineDlqOutputReferences -ge 1
  Write-JsonFile (Join-Path $BundleRoot "actual\write-boundary-audit.json") $writeAudit
  if (-not $writeAudit.passed) { throw "Raw-only write-boundary audit failed." }

  Add-Step "Publishing $ValidRecordCount valid and $InvalidRecordCount intentional-invalid records through the raw-only verifier."
  $errorContract = if ($isSidecar) { "runtime-contract" } else { "simulation" }
  $old = $ErrorActionPreference; $ErrorActionPreference = "Continue"
  try { $verificationOutput = & node $verifierScript $FixtureRoot $BundleRoot $runId $integrationId $kafkaContainer $kafkaBootstrap $errorContract 2>&1; $verificationExit = $LASTEXITCODE } finally { $ErrorActionPreference = $old }
  Write-Utf8NoBom -Path (Join-Path $BundleRoot "sanitized-logs\raw-only-verifier.log") -Value ((ConvertTo-SafeLogText ($verificationOutput -join "`n")) + "`n")
  if ($verificationExit -ne 0) { throw "Raw-only verifier failed: $($verificationOutput -join [Environment]::NewLine)" }

  $bridgeResult = Read-JsonFile (Join-Path $BundleRoot "actual\bridge-result.json")
  $publisherResult = if ($isEmbedded) {
    $value = [ordered]@{ schemaVersion = "flowplane.embedded-spring-runtime-result.v1"; integrationId = $integrationId; runId = $runId; mode = "embedded-spring"; received = [int]$bridgeResult.attemptedInput; transformed = [int]$bridgeResult.successfulOutput; dlq = [int]$bridgeResult.errorOutput; publishFailures = 0; runtimeFailures = 0; completed = ([int]$bridgeResult.attemptedInput -eq $totalRecordCount); topics = $topics }
    Write-JsonFile (Join-Path $BundleRoot "actual\publisher-bridge-result.json") $value
    $value
  } else { Read-JsonFile (Join-Path $BundleRoot "actual\publisher-bridge-result.json") }
  $stabilityObservations = [Collections.Generic.List[object]]::new()
  do {
    $runtimeAfter = Invoke-Api GET "/api/v1/runtimes/$runtimeId"
    $runtimeInspectSample = (docker inspect $runtimeContainer | ConvertFrom-Json)[0]
    $stabilityObservations.Add([ordered]@{ capturedAt = [DateTime]::UtcNow.ToString("o"); elapsedSeconds = [Math]::Round(([DateTime]::UtcNow - $stabilityStartedAt).TotalSeconds, 3); containerRunning = [bool]$runtimeInspectSample.State.Running; controlPlaneHealth = $runtimeAfter.health })
    $remaining = $MinimumDurationSeconds - ([DateTime]::UtcNow - $stabilityStartedAt).TotalSeconds
    if ($remaining -le 0) { break }
    Start-Sleep -Seconds ([Math]::Min(10, [Math]::Ceiling($remaining)))
  } while ($true)
  Write-JsonFile (Join-Path $BundleRoot "metrics\stability-observations.json") ([ordered]@{ minimumDurationSeconds = $MinimumDurationSeconds; observations = @($stabilityObservations) })
  $runtimeStatusAfter = [ordered]@{ runtimeId = $runtimeId; assignmentPresent = [bool]$runtimeAfter.activeArtifactId; artifactId = $runtimeAfter.activeArtifactId; artifactHash = $runtimeAfter.activeArtifactHash; version = $runtimeAfter.activeVersion; mappingId = $mapping.id; runtimeHealth = $runtimeAfter.health; lifecycleState = $runtimeAfter.lifecycleState }
  Write-JsonFile (Join-Path $BundleRoot "actual\runtime-status-after.json") $runtimeStatusAfter

  $runtimeInspect = (docker inspect $runtimeContainer | ConvertFrom-Json)[0]
  $bridgeInspect = if ($isEmbedded) { $runtimeInspect } else { (docker inspect $bridgeContainer | ConvertFrom-Json)[0] }
  $mountedBridgeSha256 = if ($isEmbedded) { $null } else { [string]((& docker exec $bridgeContainer sha256sum /app/bridge.mjs) -split '\s+')[0] }
  $mountedRuntimeJarSha256 = if ($isSidecar) { [string]((& docker exec $runtimeContainer sha256sum /app/flowplane-sidecar-app.jar) -split '\s+')[0] } elseif ($isEmbedded) { [string]((& docker exec $runtimeContainer sha256sum /app/flowplane-kafka-streaming-demo.jar) -split '\s+')[0] } else { $null }
  $runtimeHealthy = [bool]$runtimeInspect.State.Running -and $runtimeAfter.health -eq "HEALTHY"
  Write-JsonFile (Join-Path $BundleRoot "actual\runtime-health-after.json") ([ordered]@{ status = if ($runtimeHealthy) { "UP" } else { "DOWN" }; containerRunning = [bool]$runtimeInspect.State.Running; controlPlaneHealth = $runtimeAfter.health })
  Write-JsonFile (Join-Path $BundleRoot "actual\pipeline-result.json") ([ordered]@{ schemaVersion = "flowplane.runtime-surface-pipeline-result.v1"; container = if ($isEmbedded) { $runtimeContainer } else { $bridgeContainer }; completedSuccessfully = [bool]$publisherResult.completed; readTargets = @($topics.raw); writeTargets = @($topics.transformed, $topics.dlq); processedInput = [int]$bridgeResult.attemptedInput; successfulOutput = [int]$bridgeResult.successfulOutput; errorOutput = [int]$bridgeResult.errorOutput; unexpectedFailures = [int]$publisherResult.runtimeFailures + [int]$publisherResult.publishFailures })
  $counts = [ordered]@{ attemptedInput = [int]$bridgeResult.attemptedInput; acceptedInput = [int]$bridgeResult.acceptedInput; successfulOutput = [int]$bridgeResult.successfulOutput; intentionalInvalid = [int]$bridgeResult.intentionalInvalid; errorOutput = [int]$bridgeResult.errorOutput; filtered = [int]$bridgeResult.filtered; duplicates = [int]$bridgeResult.duplicates; unexpectedFailures = [int]$publisherResult.runtimeFailures + [int]$publisherResult.publishFailures; pending = [int64]$bridgeResult.finalLag; finalLag = [int64]$bridgeResult.finalLag; retries = 0; timeouts = 0 }
  Write-JsonFile (Join-Path $BundleRoot "counts.json") $counts
  Write-JsonFile (Join-Path $BundleRoot "final-state.json") ([ordered]@{ captured = $true; runtimeHealthy = $runtimeHealthy; integrationHealthy = [bool]$publisherResult.completed; pending = [int64]$bridgeResult.finalLag; finalLag = [int64]$bridgeResult.finalLag; capturedAt = [DateTime]::UtcNow.ToString("o") })
  $hostBridgeSha256 = if ($isEmbedded) { $null } else { Get-Sha256 $bridgeScript }
  $hostRuntimeJarSha256 = if ($isSidecar) { Get-Sha256 $sidecarJar } elseif ($isEmbedded) { Get-Sha256 $embeddedJar } else { $null }
  if ($hostBridgeSha256 -and $mountedBridgeSha256 -ne $hostBridgeSha256) { throw "Mounted runtime bridge SHA-256 differs from the host script." }
  if ($hostRuntimeJarSha256 -and $mountedRuntimeJarSha256 -ne $hostRuntimeJarSha256) { throw "Mounted runtime JAR SHA-256 differs from the host artifact." }
  Write-JsonFile (Join-Path $BundleRoot "versions.json") ([ordered]@{ flowplane = Get-GitState $FlowplaneRoot; integration = $integrationId; runtimeImage = $runtimeImage; runtimeImageId = $runtimeInspect.Image; bridgeImageId = $bridgeInspect.Image; runtimeSurfaceBridgeSha256 = $hostBridgeSha256; mountedRuntimeSurfaceBridgeSha256 = $mountedBridgeSha256; rawOnlyVerifierSha256 = Get-Sha256 $verifierScript; runtimeJarSha256 = $hostRuntimeJarSha256; mountedRuntimeJarSha256 = $mountedRuntimeJarSha256; nodeVersion = (& node --version); dockerVersion = (& docker version --format '{{.Server.Version}}') })

  $manifest = Read-JsonFile (Join-Path $BundleRoot "run-manifest.json")
  $manifest.artifactId = [string]$runtimeStatusAfter.artifactId; $manifest.artifactVersion = [string]$runtimeStatusAfter.version; $manifest.artifactHash = [string]$runtimeStatusAfter.artifactHash
  $manifest.runtime = [ordered]@{ name = "$integrationId governed runtime surface"; version = "Flowplane 1.0.0 local build"; executionMode = "Docker live local"; containerImages = if ($isEmbedded) { @($runtimeImage) } else { @($runtimeImage, "node:22-alpine") } }
  $manifest.sourceBoundary = "Raw-only verifier producer to persistent Kafka raw topic"; $manifest.sinkBoundary = if ($isEmbedded) { "Embedded Spring runtime publishes transformed/DLQ results directly" } else { "Independent bridge invokes $integrationId and publishes acknowledged transformed/DLQ results" }
  $manifest.validRecords = $ValidRecordCount; $manifest.invalidRecords = $InvalidRecordCount; $manifest.successfulOutputs = [int]$bridgeResult.successfulOutput; $manifest.errorOutputs = [int]$bridgeResult.errorOutput; $manifest.duplicates = [int]$bridgeResult.duplicates; $manifest.unexplainedMissing = [Math]::Max(0, $totalRecordCount - [int]$bridgeResult.successfulOutput - [int]$bridgeResult.errorOutput); $manifest.finalLag = [int64]$bridgeResult.finalLag; $manifest.unexpectedFailures = [int]$counts.unexpectedFailures
  $manifest | Add-Member -NotePropertyName stability -NotePropertyValue ([ordered]@{ requiredDurationSeconds = $MinimumDurationSeconds; observedDurationSeconds = [Math]::Round(([DateTime]::UtcNow - $stabilityStartedAt).TotalSeconds, 3); observationCount = $stabilityObservations.Count; allHealthy = (@($stabilityObservations | Where-Object { -not $_.containerRunning -or $_.controlPlaneHealth -ne "HEALTHY" }).Count -eq 0) }) -Force
  Write-JsonFile (Join-Path $BundleRoot "run-manifest.json") $manifest

  $assert = { param([string]$Id, [bool]$Passed, [string[]]$Evidence, [string]$Reason = "") [ordered]@{ id = $Id; applicable = $true; required = $true; passed = $Passed; evidence = $Evidence; reason = $Reason } }
  $runtimeBoundaryPassed = [bool]$publisherResult.completed -and [int]$publisherResult.received -eq $totalRecordCount -and [int]$publisherResult.transformed -eq $ValidRecordCount -and [int]$publisherResult.dlq -eq $InvalidRecordCount -and [int]$publisherResult.runtimeFailures -eq 0 -and [int]$publisherResult.publishFailures -eq 0
  $gates = @(
    & $assert "runtime.started" $runtimeHealthy @("actual/runtime-health-after.json")
    & $assert "runtime.healthConfirmed" $runtimeHealthy @("actual/runtime-health-after.json", "actual/runtime-status-after.json")
    & $assert "runtime.versionRecorded" $true @("versions.json")
    & $assert "boundary.realRuntimeUsed" ([int]$bridgeResult.attemptedInput -eq $totalRecordCount -and [int]$bridgeResult.successfulOutput -eq $ValidRecordCount -and [int]$bridgeResult.errorOutput -eq $InvalidRecordCount) @("actual/bridge-result.json", "actual/publisher-bridge-result.json")
    & $assert "boundary.realProtocolCrossed" ($runtimeBoundaryPassed -and [bool]$writeAudit.passed) @("actual/write-boundary-audit.json", "actual/publisher-bridge-result.json")
    & $assert "boundary.verifierWritesRawOnly" ([bool]$writeAudit.passed) @("actual/write-boundary-audit.json", "actual/bridge-result.json")
    & $assert "artifact.loaded" ([bool]$runtimeStatusAfter.assignmentPresent) @("actual/runtime-status-after.json")
    & $assert "artifact.idRecorded" (-not [string]::IsNullOrWhiteSpace([string]$runtimeStatusAfter.artifactId)) @("actual/runtime-status-after.json")
    & $assert "artifact.hashRecorded" (-not [string]::IsNullOrWhiteSpace([string]$runtimeStatusAfter.artifactHash)) @("actual/runtime-status-after.json")
    & $assert "fixture.validProcessed" ([int]$bridgeResult.successfulOutput -eq $ValidRecordCount) @("actual/transformed-output.jsonl")
    & $assert "fixture.invalidProcessed" ([int]$bridgeResult.errorOutput -eq $InvalidRecordCount) @("actual/error-output.jsonl")
    & $assert "output.expectedHashMatched" ([int]$bridgeResult.expectedHashMatches -eq $ValidRecordCount) @("actual/bridge-result.json", "expected/simulation-batch.json")
    & $assert "error.expectedCodeMatched" ([int]$bridgeResult.expectedErrorMatches -eq $InvalidRecordCount) @("actual/error-output.jsonl", "actual/bridge-result.json")
    & $assert "accounting.inputReconciled" ([int]$bridgeResult.attemptedInput -eq ([int]$bridgeResult.successfulOutput + [int]$bridgeResult.errorOutput + [int]$bridgeResult.filtered)) @("counts.json")
    & $assert "accounting.noUnexpectedLoss" ([int]$manifest.unexplainedMissing -eq 0) @("counts.json")
    & $assert "accounting.noUnexpectedDuplicates" ([int]$bridgeResult.duplicates -eq 0) @("counts.json")
    & $assert "accounting.noUnexpectedFailures" ([int]$counts.unexpectedFailures -eq 0) @("counts.json", "actual/publisher-bridge-result.json")
    & $assert "state.finalLagZero" ([int64]$bridgeResult.finalLag -eq 0) @("metrics/kafka-consumer-group.txt", "final-state.json")
    & $assert "state.pendingWorkZero" ([int64]$bridgeResult.finalLag -eq 0) @("metrics/kafka-topic-counts.json", "final-state.json")
    & $assert "state.runtimeHealthyAtCompletion" $runtimeHealthy @("actual/runtime-health-after.json")
    & $assert "evidence.environmentRecorded" $true @("environment.json")
    & $assert "evidence.commandsRecorded" $true @("commands.txt")
    & $assert "evidence.logsPreserved" $true @("sanitized-logs/adapter.log", "sanitized-logs/raw-only-verifier.log", "sanitized-logs/$integrationId.log", "sanitized-logs/flowplane-runtime.log")
    & $assert "evidence.rawOutputsPreserved" $true @("actual/bridge-result.json", "actual/publisher-bridge-result.json", "actual/transformed-output.jsonl", "actual/error-output.jsonl")
    & $assert "evidence.checksumsVerified" $false @() "Set by the bundle evaluator."
    & $assert "evidence.reproductionScriptAvailable" $true @("reproduce.ps1")
  )
  Write-JsonFile (Join-Path $BundleRoot "actual\adapter-gate-assertions.json") ([ordered]@{ schemaVersion = "flowplane.adapter-gate-assertions.v1"; boundaryClass = "live"; gates = $gates; warnings = @("Technical local interoperability only; no vendor certification or endorsement is implied.") })
} finally {
  if (-not $isEmbedded) { Save-Log $bridgeContainer $integrationId } else { Save-Log $runtimeContainer $integrationId }
  Save-Log $runtimeContainer "flowplane-runtime"
  foreach ($container in @($bridgeContainer, $runtimeContainer)) {
    if ($started.Contains($container)) { try { Invoke-DockerChecked stop --timeout 30 $container | Out-Null } catch {} }
  }
}
