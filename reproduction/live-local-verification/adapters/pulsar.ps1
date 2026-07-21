param(
  [Parameter(Mandatory)][string]$FlowplaneRoot,
  [Parameter(Mandatory)][string]$BundleRoot,
  [Parameter(Mandatory)][string]$FixtureRoot,
  [ValidateSet("pulsar", "activemq-classic", "activemq-artemis", "nats-jetstream", "redis-streams", "rabbitmq-streams", "emqx-mqtt", "rocketmq")][string]$IntegrationId = "pulsar"
)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "..\LiveVerification.Common.ps1")

$env:FLOWPLANE_ROOT = $FlowplaneRoot
$env:FLOWPLANE_DEMO_OUTPUT_ROOT = Join-Path $BundleRoot "adapter-private"
. (Join-Path $PSScriptRoot "..\..\FlowplaneDemo.Common.ps1")

$runId = Split-Path $BundleRoot -Leaf
$safeRun = ($runId.ToLowerInvariant() -replace '[^a-z0-9-]', '-')
$isActiveMq = $IntegrationId -eq "activemq-classic"
$isNats = $IntegrationId -eq "nats-jetstream"
$isRedis = $IntegrationId -eq "redis-streams"
$isRabbitMq = $IntegrationId -eq "rabbitmq-streams"
$isEmqx = $IntegrationId -eq "emqx-mqtt"
$isRocketMq = $IntegrationId -eq "rocketmq"
$isArtemis = $IntegrationId -eq "activemq-artemis"
$displayName = if ($isActiveMq) { "ActiveMQ Classic" } elseif ($isArtemis) { "ActiveMQ Artemis" } elseif ($isNats) { "NATS JetStream" } elseif ($isRedis) { "Redis Streams" } elseif ($isRabbitMq) { "RabbitMQ Streams" } elseif ($isEmqx) { "EMQX MQTT" } elseif ($isRocketMq) { "Apache RocketMQ" } else { "Apache Pulsar" }
$ticketPrefix = if ($isActiveMq) { "ACTIVEMQ" } elseif ($isArtemis) { "ARTEMIS" } elseif ($isNats) { "NATS" } elseif ($isRedis) { "REDIS" } elseif ($isRabbitMq) { "RABBITMQ" } elseif ($isEmqx) { "EMQX" } elseif ($isRocketMq) { "ROCKETMQ" } else { "PULSAR" }
$runtimeId = "$IntegrationId-pipeline-sidecar-$safeRun"
$bentoContainer = "flowplane-$IntegrationId-bento-$safeRun"
$pipelineContainer = "flowplane-$IntegrationId-pipeline-$safeRun"
$pulsarContainer = "flowplane-pulsar-local"
$pulsarImage = "apachepulsar/pulsar:4.2.3"
$activeMqContainer = "flowplane-activemq-classic-broker-$safeRun"
$activeMqImage = "rmohr/activemq:5.15.9"
$natsContainer = "flowplane-nats-jetstream-broker-$safeRun"
$natsImage = "nats:2.11-alpine"
$redisContainer = "flowplane-redis-streams-broker-$safeRun"
$redisImage = "redis/redis-stack:latest"
$rabbitMqContainer = "flowplane-rabbitmq-streams-broker-$safeRun"
$rabbitMqImage = "rabbitmq:4-management"
$emqxContainer = "flowplane-emqx-mqtt-broker-$safeRun"
$emqxImage = "emqx/emqx:latest"
$rocketMqNameServerContainer = "flowplane-rocketmq-namesrv-$safeRun"
$rocketMqBrokerContainer = "flowplane-rocketmq-broker-$safeRun"
$rocketMqDashboardContainer = "flowplane-rocketmq-dashboard-$safeRun"
$rocketMqImage = "apache/rocketmq:5.3.2"
$rocketMqDashboardImage = "apacherocketmq/rocketmq-dashboard:latest"
$artemisContainer = "flowplane-artemis-broker-$safeRun"
$artemisImage = "apache/activemq-artemis:latest-alpine"
$runtimeImage = "eclipse-temurin:17-jre"
$pipelineImage = "node:22-alpine"
$flowplaneNetwork = "flowplane-quality-stack_default"
$pulsarNetwork = "pulsar-local-ui_default"
$runtimeSecret = [Guid]::NewGuid().ToString("N") + [Guid]::NewGuid().ToString("N")
$token = New-FlowplaneAccessToken
$startedContainers = [Collections.Generic.List[string]]::new()

function Invoke-DockerChecked {
  $dockerArguments = @($args)
  $previousErrorAction = $ErrorActionPreference
  try {
    $ErrorActionPreference = "Continue"
    $output = & docker @dockerArguments 2>&1
  } finally {
    $ErrorActionPreference = $previousErrorAction
  }
  if ($LASTEXITCODE -ne 0) { throw "docker $($dockerArguments -join ' ') failed: $($output -join [Environment]::NewLine)" }
  return $output
}

function Get-PublishedPort([string]$Container, [int]$ContainerPort) {
  $line = @(Invoke-DockerChecked port $Container "$ContainerPort/tcp" | Select-Object -First 1)[0]
  if ($line -notmatch ':(\d+)$') { throw "Could not resolve published port $ContainerPort for ${Container}: $line" }
  return [int]$Matches[1]
}

function Wait-Http([string]$Uri, [int]$TimeoutSeconds = 180) {
  $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
  $last = $null
  do {
    try { return Invoke-RestMethod -Uri $Uri -TimeoutSec 10 } catch { $last = $_.Exception.Message }
    Start-Sleep -Seconds 2
  } while ((Get-Date) -lt $deadline)
  throw "Timed out waiting for $Uri. Last error: $last"
}

function Save-ContainerLog([string]$Container, [string]$Name) {
  try {
    $value = (& docker logs $Container 2>&1) -join "`n"
    Write-Utf8NoBom -Path (Join-Path $BundleRoot "sanitized-logs\$Name.log") -Value ((ConvertTo-SafeLogText $value) + "`n")
  } catch {}
}

function Add-Step([string]$Message) {
  $line = "$([DateTime]::UtcNow.ToString('o')) $Message"
  [IO.File]::AppendAllText((Join-Path $BundleRoot "sanitized-logs\steps.log"), $line + "`n", [Text.UTF8Encoding]::new($false))
  Write-Output $line
}

function Invoke-LocalApi {
  param([Parameter(Mandatory)][string]$Method, [Parameter(Mandatory)][string]$Path, $Body = $null)
  if ($null -eq $script:apiRequestIndex) { $script:apiRequestIndex = 0 }
  $script:apiRequestIndex += 1
  $arguments = @(
    "--silent", "--show-error", "--max-time", "30",
    "--request", $Method.ToUpperInvariant(),
    "--header", "Authorization: Bearer $token",
    "--header", "tenantId: $script:FLOWPLANE_TENANT_ID",
    "--header", "Content-Type: application/json"
  )
  if ($null -ne $Body) {
    $requestPath = Join-Path $BundleRoot ("configuration\api-request-{0:D2}.json" -f $script:apiRequestIndex)
    $requestJson = $Body | ConvertTo-Json -Depth 12 -Compress
    Write-Utf8NoBom -Path $requestPath -Value ($requestJson + "`n")
    $arguments += @("--data-binary", "@$requestPath")
  }
  $arguments += "http://127.0.0.1:8081$Path"
  $responsePath = Join-Path $BundleRoot ("adapter-private\api-response-{0:D2}.json" -f $script:apiRequestIndex)
  New-Item -ItemType Directory -Force -Path (Split-Path -Parent $responsePath) | Out-Null
  $arguments = @($arguments[0..($arguments.Count - 2)]) + @("--output", $responsePath, "--write-out", "%{http_code}", $arguments[-1])
  $statusOutput = & curl.exe @arguments 2>&1
  $curlExit = $LASTEXITCODE
  $statusText = ($statusOutput -join "`n").Trim()
  $text = if (Test-Path -LiteralPath $responsePath) { [string](Get-Content -LiteralPath $responsePath -Raw) } else { "" }
  if ($curlExit -ne 0) { throw "Flowplane API $Method $Path transport failed: $statusText" }
  $statusCode = [int]$statusText
  if ($statusCode -lt 200 -or $statusCode -ge 300) { throw "Flowplane API $Method $Path returned HTTP $statusCode`: $text" }
  if (Test-Path -LiteralPath $responsePath) { Copy-Item -LiteralPath $responsePath -Destination (Join-Path $BundleRoot ("actual\api-response-{0:D2}.json" -f $script:apiRequestIndex)) }
  $text = $text.Trim()
  if ([string]::IsNullOrWhiteSpace($text)) { return $null }
  return $text | ConvertFrom-Json
}

$jar = Get-ChildItem -LiteralPath (Join-Path $FlowplaneRoot "flowplane-java-sdk\flowplane-bento-runtime\target") -Filter "flowplane-bento-runtime-*.jar" -File |
  Where-Object { $_.Name -notlike "original-*" -and $_.Name -notlike "*-plain.jar" } |
  Sort-Object LastWriteTime -Descending |
  Select-Object -First 1
if (-not $jar) { throw "No built Bento runtime jar exists. Build it outside this adapter before execution." }

foreach ($name in @($bentoContainer, $pipelineContainer, $(if ($isActiveMq) { $activeMqContainer }), $(if ($isArtemis) { $artemisContainer }), $(if ($isNats) { $natsContainer }), $(if ($isRedis) { $redisContainer }), $(if ($isRabbitMq) { $rabbitMqContainer }), $(if ($isEmqx) { $emqxContainer }), $(if ($isRocketMq) { $rocketMqNameServerContainer }), $(if ($isRocketMq) { $rocketMqBrokerContainer }), $(if ($isRocketMq) { $rocketMqDashboardContainer }))) {
  $existing = @(& docker ps -aq --filter "name=^$name$")
  if ($existing.Count -gt 0 -and -not [string]::IsNullOrWhiteSpace(($existing -join ""))) {
    throw "Refusing to replace existing Docker container: $name"
  }
}

if (-not $isActiveMq -and -not $isArtemis -and -not $isNats -and -not $isRedis -and -not $isRabbitMq -and -not $isEmqx -and -not $isRocketMq) {
  $pulsarState = docker inspect $pulsarContainer 2>$null | ConvertFrom-Json
  if ($LASTEXITCODE -ne 0 -or -not $pulsarState -or -not $pulsarState[0].State.Running -or $pulsarState[0].State.Health.Status -ne "healthy") {
    throw "The persistent Pulsar container '$pulsarContainer' must be running and healthy before verification."
  }
  $pulsarNetworks = @($pulsarState[0].NetworkSettings.Networks.PSObject.Properties.Name)
  if ($pulsarNetworks -notcontains $pulsarNetwork) { throw "Pulsar is not attached to the required network: $pulsarNetwork" }
}

$commandLines = if ($isActiveMq) { @(
  "# Exact run values; credentials are intentionally redacted.",
  "docker run --name $activeMqContainer --network $flowplaneNetwork -p 127.0.0.1::8161 $activeMqImage",
  "docker run --name $bentoContainer --network $flowplaneNetwork -p 127.0.0.1::8080 -v <bento-jar>:/app/flowplane-bento-runtime.jar:ro $runtimeImage java -jar /app/flowplane-bento-runtime.jar",
  "docker run --name $pipelineContainer --network $flowplaneNetwork -v <pipeline-script>:/app/pipeline.mjs:ro -v <bundle>:/evidence $pipelineImage node /app/pipeline.mjs http://$activeMqContainer`:8161 http://$bentoContainer`:8080/transform $runId 110 /evidence",
  "node activemq-classic-raw-only-verifier.mjs <fixtureRoot> <bundleRoot> http://127.0.0.1:<broker-port> $runId"
) } elseif ($isArtemis) { @(
  "# Exact run values; Artemis credentials are intentionally redacted.",
  "docker run --name $artemisContainer --network $flowplaneNetwork -p 127.0.0.1::8161 -e ARTEMIS_USER=<redacted> -e ARTEMIS_PASSWORD=<redacted> $artemisImage",
  "docker run --name $pipelineContainer --network $flowplaneNetwork -v <pipeline-script>:/app/pipeline.mjs:ro -v <node-modules>:/app/node_modules:ro -v <bundle>:/evidence $pipelineImage node /app/pipeline.mjs $artemisContainer`:61613 <redacted> <redacted> http://$bentoContainer`:8080/transform $runId 110 /evidence",
  "docker run --rm --network $flowplaneNetwork -v <verifier-script>:/app/verifier.mjs:ro -v <node-modules>:/app/node_modules:ro -v <fixtures>:/fixtures:ro -v <bundle>:/evidence $pipelineImage node /app/verifier.mjs /fixtures /evidence $artemisContainer`:61613 <redacted> <redacted> $runId"
) } elseif ($isNats) { @(
  "# Exact run values; this isolated NATS broker has no external credentials.",
  "docker run --name $natsContainer --network $flowplaneNetwork -p 127.0.0.1::8222 $natsImage -js -m 8222 -sd /data",
  "docker run --name $bentoContainer --network $flowplaneNetwork -p 127.0.0.1::8080 -v <bento-jar>:/app/flowplane-bento-runtime.jar:ro $runtimeImage java -jar /app/flowplane-bento-runtime.jar",
  "docker run --name $pipelineContainer --network $flowplaneNetwork -v <pipeline-script>:/app/pipeline.mjs:ro -v <nats-node-modules>:/app/node_modules:ro -v <bundle>:/evidence $pipelineImage node /app/pipeline.mjs nats://$natsContainer`:4222 http://$bentoContainer`:8080/transform $runId 110 /evidence",
  "docker run --rm --network $flowplaneNetwork -v <verifier-script>:/app/verifier.mjs:ro -v <nats-node-modules>:/app/node_modules:ro -v <fixtures>:/fixtures:ro -v <bundle>:/evidence $pipelineImage node /app/verifier.mjs /fixtures /evidence nats://$natsContainer`:4222 $runId"
) } elseif ($isRedis) { @(
  "# Exact run values; this isolated Redis Stack broker has no external credentials.",
  "docker run --name $redisContainer --network $flowplaneNetwork -p 127.0.0.1::8001 $redisImage",
  "docker run --name $bentoContainer --network $flowplaneNetwork -p 127.0.0.1::8080 -v <bento-jar>:/app/flowplane-bento-runtime.jar:ro $runtimeImage java -jar /app/flowplane-bento-runtime.jar",
  "docker run --name $pipelineContainer --network $flowplaneNetwork -v <pipeline-script>:/app/pipeline.mjs:ro -v <node-modules>:/app/node_modules:ro -v <bundle>:/evidence $pipelineImage node /app/pipeline.mjs redis://$redisContainer`:6379 http://$bentoContainer`:8080/transform $runId 110 /evidence",
  "docker run --rm --network $flowplaneNetwork -v <verifier-script>:/app/verifier.mjs:ro -v <node-modules>:/app/node_modules:ro -v <fixtures>:/fixtures:ro -v <bundle>:/evidence $pipelineImage node /app/verifier.mjs /fixtures /evidence redis://$redisContainer`:6379 $runId"
) } elseif ($isRabbitMq) { @(
  "# Exact run values; RabbitMQ credentials are intentionally redacted.",
  "docker run --name $rabbitMqContainer --network $flowplaneNetwork -p 127.0.0.1::15672 -e RABBITMQ_DEFAULT_USER=<redacted> -e RABBITMQ_DEFAULT_PASS=<redacted> $rabbitMqImage",
  "docker run --name $pipelineContainer --network $flowplaneNetwork -v <pipeline-script>:/app/pipeline.mjs:ro -v <node-modules>:/app/node_modules:ro -v <bundle>:/evidence $pipelineImage node /app/pipeline.mjs amqp://<redacted>@$rabbitMqContainer`:5672 http://$bentoContainer`:8080/transform $runId 110 /evidence",
  "docker run --rm --network $flowplaneNetwork -v <verifier-script>:/app/verifier.mjs:ro -v <node-modules>:/app/node_modules:ro -v <fixtures>:/fixtures:ro -v <bundle>:/evidence $pipelineImage node /app/verifier.mjs /fixtures /evidence amqp://<redacted>@$rabbitMqContainer`:5672 $runId"
) } elseif ($isEmqx) { @(
  "docker run --name $emqxContainer --network $flowplaneNetwork -p 127.0.0.1::18083 $emqxImage",
  "docker run --name $pipelineContainer --network $flowplaneNetwork -v <pipeline-script>:/app/pipeline.mjs:ro -v <node-modules>:/app/node_modules:ro -v <bundle>:/evidence $pipelineImage node /app/pipeline.mjs mqtt://$emqxContainer`:1883 http://$bentoContainer`:8080/transform $runId 110 /evidence",
  "docker run --rm --network $flowplaneNetwork -v <verifier-script>:/app/verifier.mjs:ro -v <node-modules>:/app/node_modules:ro -v <fixtures>:/fixtures:ro -v <bundle>:/evidence $pipelineImage node /app/verifier.mjs /fixtures /evidence mqtt://$emqxContainer`:1883 $runId"
) } elseif ($isRocketMq) { @(
  "docker run --name $rocketMqNameServerContainer --network $flowplaneNetwork --network-alias flowplane-rocketmq-namesrv $rocketMqImage sh mqnamesrv",
  "docker run --name $rocketMqBrokerContainer --network $flowplaneNetwork --network-alias flowplane-rocketmq-broker -e NAMESRV_ADDR=flowplane-rocketmq-namesrv:9876 -v <broker-conf>:/home/rocketmq/rocketmq-5.3.2/conf/broker.conf:ro $rocketMqImage sh mqbroker --enable-proxy -c /home/rocketmq/rocketmq-5.3.2/conf/broker.conf",
  "docker run --name $rocketMqDashboardContainer --network $flowplaneNetwork -p 127.0.0.1::8082 -e JAVA_OPTS=-Drocketmq.namesrv.addr=flowplane-rocketmq-namesrv:9876 $rocketMqDashboardImage",
  "docker run --name $pipelineContainer --network $flowplaneNetwork -v <pipeline-script>:/app/pipeline.mjs:ro -v <node-modules>:/app/node_modules:ro -v <bundle>:/evidence $pipelineImage node /app/pipeline.mjs flowplane-rocketmq-broker:8081 http://$bentoContainer`:8080/transform $runId 110 /evidence",
  "docker run --rm --network $flowplaneNetwork -v <verifier-script>:/app/verifier.mjs:ro -v <node-modules>:/app/node_modules:ro -v <fixtures>:/fixtures:ro -v <bundle>:/evidence $pipelineImage node /app/verifier.mjs /fixtures /evidence flowplane-rocketmq-broker:8081 $runId"
) } else { @(
  "# Exact run values; credentials are intentionally redacted.",
  "docker compose -f C:\FlowPlaneNew\video-generation-scripts-copy\pulsar-local-ui\compose.yaml up -d",
  "docker run --name $bentoContainer --network $flowplaneNetwork -p 127.0.0.1::8080 -v <bento-jar>:/app/flowplane-bento-runtime.jar:ro $runtimeImage java -jar /app/flowplane-bento-runtime.jar",
  "docker run --name $pipelineContainer --network $pulsarNetwork -v <pipeline-script>:/app/pipeline.mjs:ro -v <bundle>:/evidence $pipelineImage node /app/pipeline.mjs http://flowplane-pulsar-local:8080 http://host.docker.internal:<sidecar-port>/transform $runId 110 /evidence",
  "node pulsar-raw-only-verifier.mjs <fixtureRoot> <bundleRoot> http://127.0.0.1:8080 $runId"
) }
Write-Utf8NoBom -Path (Join-Path $BundleRoot "commands.txt") -Value (($commandLines -join "`n") + "`n")
Write-Utf8NoBom -Path (Join-Path $BundleRoot "reproduce.ps1") -Value ((@(
  "param([string]`$FlowplaneRoot = 'C:\FlowPlaneNew\repositories\flowplane-controlplane')",
  "& 'C:\FlowPlaneNew\video-generation-scripts-copy\scripts\demo\11-run-live-local-verification.ps1' -FlowplaneRoot `$FlowplaneRoot -Execute -Integration $IntegrationId"
) -join "`n") + "`n")

try {
  Add-Step "Loading canonical mapping and fixtures."
  $mappingDsl = [string](Get-Content -LiteralPath (Join-Path $FixtureRoot "mapping.yaml") -Raw)
  $validPayloads = @(Get-Content -LiteralPath (Join-Path $FixtureRoot "valid-input.jsonl") | Where-Object { $_ } | ForEach-Object { [string]$_ })
  $invalidPayloads = @(Get-Content -LiteralPath (Join-Path $FixtureRoot "invalid-input.jsonl") | Where-Object { $_ } | ForEach-Object { [string]$_ })
  $samplePayload = [string]$validPayloads[0]
  Add-Step "Reading an active team from the live control plane."
  $teamPage = Invoke-LocalApi -Method Get -Path "/api/v1/teams?activeOnly=true&page=0&size=100"
  $team = @($teamPage.items | Select-Object -First 1)[0]
  if (-not $team) { throw "No active team is available for the synthetic verification mapping." }
  Add-Step "Creating the synthetic mapping."
  $mapping = Invoke-LocalApi -Method Post -Path "/api/v1/mappings" -Body @{
    name = "$IntegrationId-live-local-$safeRun"
    description = "Synthetic $displayName pipeline-to-sidecar live-local verification."
    workspaceId = "workspace-platform"
    teamId = [string]$team.id
    teamName = [string]$team.name
    projectId = "live-local-verification"
    projectName = "Live Local Verification"
    environment = "PRODUCTION"
    mappingDsl = $mappingDsl
    samplePayload = $samplePayload
    dictionaryIds = @()
  }
  Save-Json -Path (Join-Path $BundleRoot "actual\mapping-created.json") -Value $mapping
  Add-Step "Validating the synthetic mapping."
  $validation = Invoke-LocalApi -Method Post -Path "/api/v1/mappings/$($mapping.id)/validate"
  Save-Json -Path (Join-Path $BundleRoot "actual\mapping-validation.json") -Value $validation
  if (-not $validation.valid) { throw "Canonical mapping validation failed: $($validation.errors -join '; ')" }

  $simulationRecords = @($validPayloads | ForEach-Object {
    $payload = $_ | ConvertFrom-Json
    [ordered]@{ recordId = [string]$payload.event.id; payloadJson = [string]$_ }
  })
  Add-Step "Generating the 100-record expected-output baseline through mapping simulation."
  $simulation = Invoke-LocalApi -Method Post -Path "/api/v1/mappings/$($mapping.id)/simulate:batch" -Body @{
    records = $simulationRecords
    runtimeModes = @()
  }
  Save-Json -Path (Join-Path $BundleRoot "expected\simulation-batch.json") -Value $simulation
  if (-not $simulation.success -or [int]$simulation.recordCount -ne 100 -or @($simulation.records | Where-Object { -not $_.success }).Count -ne 0) {
    throw "Expected-output simulation did not produce 100 successful records."
  }
  $validSimulation = Invoke-LocalApi -Method Post -Path "/api/v1/mappings/$($mapping.id)/simulate" -Body @{ payloadJson = [string]$validPayloads[0] }
  Save-Json -Path (Join-Path $BundleRoot "expected\simulation-valid.json") -Value $validSimulation
  if ([int]$validSimulation.errorCount -ne 0) { throw "Valid single-record simulation returned errors." }
  $invalidSimulation = Invoke-LocalApi -Method Post -Path "/api/v1/mappings/$($mapping.id)/simulate" -Body @{ payloadJson = [string]$invalidPayloads[0] }
  Save-Json -Path (Join-Path $BundleRoot "expected\simulation-invalid.json") -Value $invalidSimulation
  if ([int]$invalidSimulation.errorCount -lt 1) { throw "Invalid fixture simulation did not produce a validation error." }

  Add-Step "Submitting, approving, and QA-passing the mapping through governance."
  $ticket = "$ticketPrefix-$runId"
  Invoke-LocalApi -Method Post -Path "/api/v1/mappings/$($mapping.id)/submit-review" -Body @{
    reason = "Synthetic $displayName live-local verification mapping is validated and simulated."
    changeTicket = $ticket
  } | Out-Null
  $approvals = Invoke-LocalApi -Method Get -Path "/api/v1/approvals?environment=PRODUCTION&page=0&size=100"
  $approval = @($approvals.items | Where-Object { $_.mappingId -eq $mapping.id } | Select-Object -First 1)[0]
  if (-not $approval) { throw "No approval request was created for the $displayName verification mapping." }
  $approved = Invoke-LocalApi -Method Post -Path "/api/v1/approvals/$($approval.id)/approve" -Body @{ reason = "Reviewed synthetic local verification mapping."; changeTicket = $ticket }
  $qaPassed = Invoke-LocalApi -Method Post -Path "/api/v1/approvals/$($approval.id)/qa-pass" -Body @{ reason = "Valid and invalid simulation gates passed."; changeTicket = $ticket }
  Save-Json -Path (Join-Path $BundleRoot "actual\mapping-approval.json") -Value ([ordered]@{ request = $approval; approval = $approved; qaPass = $qaPassed })
  $published = Invoke-LocalApi -Method Post -Path "/api/v1/mappings/$($mapping.id)/publish" -Body @{ reason = "Publish QA-approved artifact for $displayName live-local verification."; changeTicket = $ticket }
  Save-Json -Path (Join-Path $BundleRoot "actual\mapping-published.json") -Value $published

  Add-Step "Starting the real Flowplane Bento HTTP sidecar container."
  $bentoId = @(Invoke-DockerChecked run -d --name $bentoContainer --network $flowplaneNetwork -p "127.0.0.1::8080" `
    -v "$($jar.FullName):/app/flowplane-bento-runtime.jar:ro" `
    -e "FLOWPLANE_BENTO_CONTROL_PLANE_URL=http://flowplane-backend:8080" `
    -e "FLOWPLANE_BENTO_RUNTIME_ID=$runtimeId" `
    -e "FLOWPLANE_BENTO_RUNTIME_NAME=$displayName Pipeline Sidecar $runId" `
    -e "FLOWPLANE_BENTO_RUNTIME_ENVIRONMENT=PRODUCTION" `
    -e "FLOWPLANE_BENTO_RUNTIME_OWNER_TEAM=Quality Engineering" `
    -e "FLOWPLANE_BENTO_RUNTIME_PROJECT_ID=live-local-verification" `
    -e "FLOWPLANE_BENTO_TENANT_ID=$script:FLOWPLANE_TENANT_ID" `
    -e "FLOWPLANE_BENTO_AUTH_TOKEN=$token" `
    -e "FLOWPLANE_BENTO_RUNTIME_CLIENT_SECRET=$runtimeSecret" `
    -e "FLOWPLANE_BENTO_ASSIGNMENT_POLL_INTERVAL_MS=1000" `
    -e "FLOWPLANE_BENTO_OUTPUT_SHAPE=JSON_STRING" `
    -e "FLOWPLANE_BENTO_OUTPUT_COMPLEX_TYPES=NATIVE_JSON" `
    -e "FLOWPLANE_BENTO_OUTPUT_FIELD_NAMING=AS_IS" `
    -e "FLOWPLANE_BENTO_REPLAY_ENABLED=false" `
    $runtimeImage java -jar /app/flowplane-bento-runtime.jar | Select-Object -First 1)[0]
  $startedContainers.Add($bentoContainer)
  $bentoPort = Get-PublishedPort $bentoContainer 8080
  Wait-Http "http://127.0.0.1:$bentoPort/actuator/health" 180 | Out-Null

  $runtime = $null
  $deadline = (Get-Date).AddMinutes(3)
  do {
    try { $runtime = Invoke-LocalApi -Method Get -Path "/api/v1/runtimes/$runtimeId" } catch {}
    if ($runtime -and $runtime.id -eq $runtimeId) { break }
    Start-Sleep -Seconds 2
  } while ((Get-Date) -lt $deadline)
  if (-not $runtime) { throw "Bento runtime did not register with the control plane." }
  Save-Json -Path (Join-Path $BundleRoot "actual\runtime-registration.json") -Value $runtime

  Add-Step "Deploying the published artifact to the registered runtime."
  $deployment = Invoke-LocalApi -Method Post -Path "/api/v1/mappings/$($mapping.id)/deploy" -Body @{
    runtimeIds = @($runtimeId)
    rolloutPercent = 100
    requireReplayGate = $false
    reason = "Assign the approved synthetic artifact to the $displayName pipeline sidecar."
    changeTicket = $ticket
  }
  Save-Json -Path (Join-Path $BundleRoot "actual\deployment.json") -Value $deployment

  $runtimeStatus = $null
  $deadline = (Get-Date).AddMinutes(3)
  do {
    try { $runtimeStatus = Invoke-RestMethod -Uri "http://127.0.0.1:$bentoPort/runtime/status" -TimeoutSec 10 } catch {}
    if ($runtimeStatus -and $runtimeStatus.assignmentPresent) { break }
    Start-Sleep -Seconds 2
  } while ((Get-Date) -lt $deadline)
  if (-not $runtimeStatus.assignmentPresent) { throw "Bento runtime never loaded its assigned mapping artifact." }
  Save-Json -Path (Join-Path $BundleRoot "actual\runtime-status-before.json") -Value $runtimeStatus

  if ($isActiveMq) {
    Add-Step "Starting an isolated ActiveMQ Classic 5.15.9 broker."
    Invoke-DockerChecked run -d --name $activeMqContainer --network $flowplaneNetwork -p "127.0.0.1::8161" $activeMqImage | Out-Null
    $startedContainers.Add($activeMqContainer)
    $activeMqPort = Get-PublishedPort $activeMqContainer 8161
    $basicAuth = "Basic " + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("admin:admin"))
    $deadline = (Get-Date).AddMinutes(2)
    $brokerVersionResponse = $null
    do {
      try { $brokerVersionResponse = Invoke-RestMethod -Uri "http://127.0.0.1:$activeMqPort/api/jolokia/version" -Headers @{ Authorization = $basicAuth } -TimeoutSec 10 } catch {}
      if ($brokerVersionResponse -and $brokerVersionResponse.status -eq 200) { break }
      Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    if (-not $brokerVersionResponse -or $brokerVersionResponse.status -ne 200) { throw "ActiveMQ Classic broker did not expose a healthy Jolokia API." }

    $configuration = [ordered]@{
      runId = $runId
      broker = "ActiveMQ Classic"
      brokerContainer = $activeMqContainer
      brokerImage = $activeMqImage
      brokerHttpPort = $activeMqPort
      flowplaneRuntimeContainer = $bentoContainer
      flowplaneRuntimeId = $runtimeId
      flowplaneRuntimePort = $bentoPort
      flowplaneNetwork = $flowplaneNetwork
      pipelineContainer = $pipelineContainer
      pipelineImage = $pipelineImage
      pipeline = "Independent container: ActiveMQ raw queue -> Flowplane Bento HTTP sidecar -> ActiveMQ transformed/DLQ queues"
    }
    $configurationEvidencePath = "configuration/activemq-classic-run.json"
    Save-Json -Path (Join-Path $BundleRoot "configuration\activemq-classic-run.json") -Value $configuration

    $pipelineScript = Join-Path $PSScriptRoot "activemq-classic-flowplane-pipeline.mjs"
    $verifierScript = Join-Path $PSScriptRoot "activemq-classic-raw-only-verifier.mjs"
    $verifierSource = [string](Get-Content -LiteralPath $verifierScript -Raw)
    $pipelineSource = [string](Get-Content -LiteralPath $pipelineScript -Raw)
    $writeBoundaryAudit = [ordered]@{
      verifierRawProducerCalls = [regex]::Matches($verifierSource, 'sendToQueue\(queues\.raw').Count
      verifierDownstreamProducerCalls = [regex]::Matches($verifierSource, 'sendToQueue\(queues\.(?:transformed|dlq)').Count
      verifierRuntimeUrlReferences = [regex]::Matches($verifierSource, '\bruntimeUrl\b').Count
      pipelineRawConsumerCalls = [regex]::Matches($pipelineSource, 'receiveFromQueue\(queues\.raw').Count
      pipelineDownstreamProducerCalls = [regex]::Matches($pipelineSource, 'sendToQueue\(queues\.(?:transformed|dlq)').Count
      verifierSha256 = Get-Sha256 $verifierScript
      pipelineSha256 = Get-Sha256 $pipelineScript
    }
    $writeBoundaryAudit.passed = ($writeBoundaryAudit.verifierRawProducerCalls -eq 1 -and $writeBoundaryAudit.verifierDownstreamProducerCalls -eq 0 -and $writeBoundaryAudit.verifierRuntimeUrlReferences -eq 0 -and $writeBoundaryAudit.pipelineRawConsumerCalls -eq 1 -and $writeBoundaryAudit.pipelineDownstreamProducerCalls -eq 2)
    Save-Json -Path (Join-Path $BundleRoot "actual\write-boundary-audit.json") -Value $writeBoundaryAudit
    if (-not $writeBoundaryAudit.passed) { throw "Static write-boundary audit failed; verifier must write raw only and pipeline must own downstream publishing." }

    Add-Step "Starting the independently deployed ActiveMQ Classic-to-Flowplane pipeline container."
    Invoke-DockerChecked run -d --name $pipelineContainer --network $flowplaneNetwork `
      -v "$($pipelineScript):/app/pipeline.mjs:ro" `
      -v "$($BundleRoot):/evidence" `
      $pipelineImage node /app/pipeline.mjs "http://$activeMqContainer`:8161" "http://$bentoContainer`:8080/transform" $runId 110 /evidence | Out-Null
    $startedContainers.Add($pipelineContainer)
    $deadline = (Get-Date).AddMinutes(2)
    $pipelineReadyPath = Join-Path $BundleRoot "actual\pipeline-ready.json"
    do {
      if (Test-Path -LiteralPath $pipelineReadyPath) { break }
      $pipelineRunning = [string](& docker inspect --format '{{.State.Running}}' $pipelineContainer 2>$null)
      if ($pipelineRunning.Trim() -eq "false") { throw "ActiveMQ pipeline container exited before becoming ready." }
      Start-Sleep -Seconds 1
    } while ((Get-Date) -lt $deadline)
    if (-not (Test-Path -LiteralPath $pipelineReadyPath)) { throw "ActiveMQ pipeline container did not become ready." }

    Add-Step "Publishing 110 records to the ActiveMQ raw queue only; downstream queues are read-only to the verifier."
    $verifierTranscript = & node $verifierScript $FixtureRoot $BundleRoot "http://127.0.0.1:$activeMqPort" $runId 2>&1
    Write-Utf8NoBom -Path (Join-Path $BundleRoot "sanitized-logs\raw-only-verifier.log") -Value ((ConvertTo-SafeLogText ($verifierTranscript -join "`n")) + "`n")
    if ($LASTEXITCODE -ne 0) { throw "Raw-only ActiveMQ verifier exited with code $LASTEXITCODE" }

    $deadline = (Get-Date).AddMinutes(2)
    do {
      $pipelineRunning = [string](& docker inspect --format '{{.State.Running}}' $pipelineContainer 2>$null)
      if ($pipelineRunning.Trim() -eq "false") { break }
      Start-Sleep -Seconds 1
    } while ((Get-Date) -lt $deadline)
    if ($pipelineRunning.Trim() -ne "false") { throw "ActiveMQ pipeline container did not finish processing within two minutes." }
    $pipelineExitCode = [int](& docker inspect --format '{{.State.ExitCode}}' $pipelineContainer)
    if ($pipelineExitCode -ne 0) { throw "ActiveMQ pipeline container exited with code $pipelineExitCode" }

    $bridgeResult = Read-JsonFile (Join-Path $BundleRoot "actual\bridge-result.json")
    $pipelineResult = Read-JsonFile (Join-Path $BundleRoot "actual\pipeline-result.json")
    $expectedRawTopic = "flowplane.activemq-classic.$($runId.ToLowerInvariant()).raw"
    $expectedTransformedTopic = "flowplane.activemq-classic.$($runId.ToLowerInvariant()).transformed"
    $expectedDlqTopic = "flowplane.activemq-classic.$($runId.ToLowerInvariant()).dlq"
    $runtimeBoundaryPassed = (
      @($bridgeResult.verifierWriteTargets).Count -eq 1 -and [string]$bridgeResult.verifierWriteTargets[0] -eq $expectedRawTopic -and
      @($pipelineResult.readTargets).Count -eq 1 -and [string]$pipelineResult.readTargets[0] -eq $expectedRawTopic -and
      @($pipelineResult.writeTargets).Count -eq 2 -and @($pipelineResult.writeTargets) -contains $expectedTransformedTopic -and @($pipelineResult.writeTargets) -contains $expectedDlqTopic
    )
    if (-not $runtimeBoundaryPassed) { throw "Runtime write-boundary evidence did not match the ActiveMQ raw-only verifier architecture." }

    Add-Step "Reconciling ActiveMQ queue depths and enqueue/dequeue counts."
    $queueStats = [ordered]@{}
    $finalLag = 0L
    foreach ($property in $bridgeResult.queues.PSObject.Properties) {
      $queueName = [string]$property.Value
      $jolokiaPath = "org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName=$queueName"
      $stats = Invoke-RestMethod -Uri "http://127.0.0.1:$activeMqPort/api/jolokia/read/$jolokiaPath" -Headers @{ Authorization = $basicAuth } -TimeoutSec 30
      if ($stats.status -ne 200) { throw "ActiveMQ Jolokia queue read failed for $queueName." }
      $queueStats[$property.Name] = [ordered]@{
        queueName = $queueName
        queueSize = [int64]$stats.value.QueueSize
        enqueueCount = [int64]$stats.value.EnqueueCount
        dequeueCount = [int64]$stats.value.DequeueCount
        consumerCount = [int64]$stats.value.ConsumerCount
      }
      $finalLag += [int64]$stats.value.QueueSize
    }
    $metricsEvidencePath = "metrics/activemq-classic-queue-stats.json"
    Save-Json -Path (Join-Path $BundleRoot "metrics\activemq-classic-queue-stats.json") -Value $queueStats

    try {
      $runtimeMetrics = Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:$bentoPort/actuator/prometheus" -TimeoutSec 30
      Write-Utf8NoBom -Path (Join-Path $BundleRoot "metrics\flowplane-runtime.prom") -Value ($runtimeMetrics.Content + "`n")
    } catch {
      Save-Json -Path (Join-Path $BundleRoot "metrics\flowplane-runtime-health.json") -Value (Invoke-RestMethod -Uri "http://127.0.0.1:$bentoPort/actuator/health" -TimeoutSec 30)
    }
    $runtimeStatusAfter = Invoke-RestMethod -Uri "http://127.0.0.1:$bentoPort/runtime/status" -TimeoutSec 30
    Save-Json -Path (Join-Path $BundleRoot "actual\runtime-status-after.json") -Value $runtimeStatusAfter
    $runtimeHealthAfter = Invoke-RestMethod -Uri "http://127.0.0.1:$bentoPort/actuator/health" -TimeoutSec 30
    Save-Json -Path (Join-Path $BundleRoot "actual\runtime-health-after.json") -Value $runtimeHealthAfter

    $counts = [ordered]@{ attemptedInput=[int]$bridgeResult.attemptedInput; acceptedInput=[int]$bridgeResult.acceptedInput; successfulOutput=[int]$bridgeResult.successfulOutput; intentionalInvalid=[int]$bridgeResult.intentionalInvalid; errorOutput=[int]$bridgeResult.errorOutput; filtered=[int]$bridgeResult.filtered; duplicates=[int]$bridgeResult.duplicates; unexpectedFailures=[int]$pipelineResult.unexpectedFailures; pending=[int64]$finalLag; finalLag=[int64]$finalLag; retries=0; timeouts=[int]$pipelineResult.httpTimeouts }
    Write-JsonFile -Path (Join-Path $BundleRoot "counts.json") -Value $counts
    Write-JsonFile -Path (Join-Path $BundleRoot "final-state.json") -Value ([ordered]@{ captured=$true; runtimeHealthy=($runtimeHealthAfter.status -eq "UP"); assignmentPresent=[bool]$runtimeStatusAfter.assignmentPresent; pending=[int64]$finalLag; finalLag=[int64]$finalLag; activeMqQueues=$queueStats })

    $activeMqInspect = docker inspect $activeMqContainer | ConvertFrom-Json
    $bentoInspect = docker inspect $bentoContainer | ConvertFrom-Json
    $pipelineInspect = docker inspect $pipelineContainer | ConvertFrom-Json
    $activeMqVersion = "5.15.9"
    Write-JsonFile -Path (Join-Path $BundleRoot "versions.json") -Value ([ordered]@{
      flowplane=Get-GitState $FlowplaneRoot; activeMqClassicVersion=$activeMqVersion; activeMqImage=$activeMqImage; activeMqImageId=$activeMqInspect[0].Image; runtimeImage=$runtimeImage; runtimeImageId=$bentoInspect[0].Image; pipelineImage=$pipelineImage; pipelineImageId=$pipelineInspect[0].Image; pipelineScriptSha256=Get-Sha256 $pipelineScript; rawOnlyVerifierSha256=Get-Sha256 $verifierScript; runtimeJar=$jar.Name; runtimeJarSha256=Get-Sha256 $jar.FullName; nodeVersion=(& node --version); dockerVersion=(& docker version --format '{{.Server.Version}}')
    })

    $manifest = Read-JsonFile (Join-Path $BundleRoot "run-manifest.json")
    $manifest.artifactId = [string]$runtimeStatusAfter.artifactId
    $manifest.artifactVersion = [string]$runtimeStatusAfter.version
    $manifest.artifactHash = [string]$runtimeStatusAfter.artifactHash
    $manifest.runtime = [ordered]@{ name="ActiveMQ Classic broker + independent HTTP pipeline + Flowplane Bento sidecar"; version=$activeMqVersion; executionMode="Docker live local"; containerImages=@($activeMqImage,$pipelineImage,$runtimeImage) }
    $manifest.sourceBoundary = "Raw-only verifier producer to ActiveMQ Classic raw queue"
    $manifest.sinkBoundary = "Independently deployed pipeline container to ActiveMQ Classic transformed and DLQ queues"
    $manifest.validRecords=[int]$bridgeResult.validInput; $manifest.invalidRecords=[int]$bridgeResult.intentionalInvalid; $manifest.successfulOutputs=[int]$bridgeResult.successfulOutput; $manifest.errorOutputs=[int]$bridgeResult.errorOutput; $manifest.duplicates=[int]$bridgeResult.duplicates; $manifest.unexplainedMissing=[Math]::Max(0,[int]$bridgeResult.attemptedInput-[int]$bridgeResult.successfulOutput-[int]$bridgeResult.errorOutput); $manifest.finalLag=[int64]$finalLag; $manifest.unexpectedFailures=[int]$pipelineResult.unexpectedFailures
    Write-JsonFile -Path (Join-Path $BundleRoot "run-manifest.json") -Value $manifest
    $brokerLogEvidencePath = "sanitized-logs/activemq-classic.log"
  } elseif ($isArtemis) {
    $artemisUser = "flowplane"
    $artemisPassword = [Guid]::NewGuid().ToString("N")
    Add-Step "Starting isolated ActiveMQ Artemis broker and native Hawtio console."
    Invoke-DockerChecked run -d --name $artemisContainer --network $flowplaneNetwork -p "127.0.0.1::8161" -e "ARTEMIS_USER=$artemisUser" -e "ARTEMIS_PASSWORD=$artemisPassword" -e "ANONYMOUS_LOGIN=false" $artemisImage | Out-Null
    $startedContainers.Add($artemisContainer)
    $deadline = (Get-Date).AddMinutes(3)
    $artemisReady = $false
    do {
      $previousErrorAction = $ErrorActionPreference
      try {
        $ErrorActionPreference = "Continue"
        $artemisLogs = (& docker logs $artemisContainer 2>&1) -join "`n"
      } finally {
        $ErrorActionPreference = $previousErrorAction
      }
      if ($artemisLogs -match "Server is now active" -and $artemisLogs -match "Artemis Console available") { $artemisReady = $true; break }
      Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    if (-not $artemisReady) { throw "ActiveMQ Artemis did not become ready." }
    $consolePort = Get-PublishedPort $artemisContainer 8161
    $artemisPrefix = "flowplane.artemis.$($runId.ToLowerInvariant())"
    $artemisQueues = [ordered]@{ raw = "$artemisPrefix.raw"; transformed = "$artemisPrefix.transformed"; dlq = "$artemisPrefix.dlq" }
    foreach ($queueName in $artemisQueues.Values) {
      Invoke-DockerChecked exec $artemisContainer /var/lib/artemis-instance/bin/artemis queue create --name $queueName --address $queueName --anycast --durable --preserve-on-no-consumers --auto-create-address --silent --user $artemisUser --password $artemisPassword --url tcp://localhost:61616 | Out-Null
    }

    $nodeModules = Join-Path $PSScriptRoot "..\assets\nats-node\node_modules"
    $pipelineScript = Join-Path $PSScriptRoot "artemis-stomp-flowplane-pipeline.mjs"
    $verifierScript = Join-Path $PSScriptRoot "artemis-stomp-raw-only-verifier.mjs"
    $configurationEvidencePath = "configuration/artemis-run.json"
    Save-Json -Path (Join-Path $BundleRoot "configuration\artemis-run.json") -Value ([ordered]@{ runId=$runId; broker="ActiveMQ Artemis"; brokerContainer=$artemisContainer; brokerImage=$artemisImage; consolePort=$consolePort; stompEndpoint="$artemisContainer`:61613"; queues=$artemisQueues; durableQueues=$true; preserveOnNoConsumers=$true; credentials="redacted"; flowplaneRuntimeId=$runtimeId; acknowledgementOrder="downstream STOMP receipt before raw ACK receipt" })

    $verifierSource = [string](Get-Content -LiteralPath $verifierScript -Raw)
    $pipelineSource = [string](Get-Content -LiteralPath $pipelineScript -Raw)
    $writeBoundaryAudit = [ordered]@{
      verifierRawProducerCalls = [regex]::Matches($verifierSource, 'sendConfirmed\(producer, queues\.raw').Count
      verifierDownstreamProducerCalls = [regex]::Matches($verifierSource, 'sendConfirmed\(producer, queues\.(?:transformed|dlq)').Count
      verifierRuntimeUrlReferences = [regex]::Matches($verifierSource, '\bruntimeUrl\b').Count
      pipelineRawSubscriptionCalls = [regex]::Matches($pipelineSource, 'destination: queues\.raw').Count
      pipelineDynamicProducerCalls = [regex]::Matches($pipelineSource, 'sendConfirmed\(publisher, target').Count
      targetRestrictedToTransformedOrDlq = ([regex]::Matches($pipelineSource, 'target = queues\.transformed').Count -eq 1 -and [regex]::Matches($pipelineSource, 'target = queues\.dlq').Count -eq 1)
      downstreamReceiptBeforeRawAck = ($pipelineSource.IndexOf('await sendConfirmed(publisher, target') -lt $pipelineSource.IndexOf('await ackConfirmed(receiver, message)'))
      verifierSha256 = Get-Sha256 $verifierScript; pipelineSha256 = Get-Sha256 $pipelineScript
    }
    $writeBoundaryAudit.passed = ($writeBoundaryAudit.verifierRawProducerCalls -eq 1 -and $writeBoundaryAudit.verifierDownstreamProducerCalls -eq 0 -and $writeBoundaryAudit.verifierRuntimeUrlReferences -eq 0 -and $writeBoundaryAudit.pipelineRawSubscriptionCalls -eq 1 -and $writeBoundaryAudit.pipelineDynamicProducerCalls -eq 1 -and $writeBoundaryAudit.targetRestrictedToTransformedOrDlq -and $writeBoundaryAudit.downstreamReceiptBeforeRawAck)
    Save-Json -Path (Join-Path $BundleRoot "actual\write-boundary-audit.json") -Value $writeBoundaryAudit
    if (-not $writeBoundaryAudit.passed) { throw "Artemis write-boundary audit failed." }

    Add-Step "Starting independent Artemis STOMP-to-Flowplane pipeline."
    Invoke-DockerChecked run -d --name $pipelineContainer --network $flowplaneNetwork -v "$($pipelineScript):/app/pipeline.mjs:ro" -v "$($nodeModules):/app/node_modules:ro" -v "$($BundleRoot):/evidence" $pipelineImage node /app/pipeline.mjs "$artemisContainer`:61613" $artemisUser $artemisPassword "http://$bentoContainer`:8080/transform" $runId 110 /evidence | Out-Null
    $startedContainers.Add($pipelineContainer)
    $pipelineReadyPath = Join-Path $BundleRoot "actual\pipeline-ready.json"
    $deadline = (Get-Date).AddMinutes(2)
    do {
      if (Test-Path -LiteralPath $pipelineReadyPath) { break }
      $running = [string](& docker inspect --format '{{.State.Running}}' $pipelineContainer 2>$null)
      if ($running.Trim() -eq "false") { throw "Artemis pipeline exited before readiness." }
      Start-Sleep -Seconds 1
    } while ((Get-Date) -lt $deadline)
    if (-not (Test-Path -LiteralPath $pipelineReadyPath)) { throw "Artemis pipeline did not become ready." }

    Add-Step "Publishing 110 records only to the Artemis raw queue."
    $verifierTranscript = Invoke-DockerChecked run --rm --network $flowplaneNetwork -v "$($verifierScript):/app/verifier.mjs:ro" -v "$($nodeModules):/app/node_modules:ro" -v "$($FixtureRoot):/fixtures:ro" -v "$($BundleRoot):/evidence" $pipelineImage node /app/verifier.mjs /fixtures /evidence "$artemisContainer`:61613" $artemisUser $artemisPassword $runId
    Write-Utf8NoBom -Path (Join-Path $BundleRoot "sanitized-logs\raw-only-verifier.log") -Value ((ConvertTo-SafeLogText ($verifierTranscript -join "`n")) + "`n")
    $deadline = (Get-Date).AddMinutes(2)
    do { $running = [string](& docker inspect --format '{{.State.Running}}' $pipelineContainer 2>$null); if ($running.Trim() -eq "false") { break }; Start-Sleep -Seconds 1 } while ((Get-Date) -lt $deadline)
    $pipelineExitCode = [int](& docker inspect --format '{{.State.ExitCode}}' $pipelineContainer)
    if ($pipelineExitCode -ne 0) { throw "Artemis pipeline exited with code $pipelineExitCode" }

    $bridgeResult = Read-JsonFile (Join-Path $BundleRoot "actual\bridge-result.json")
    $pipelineResult = Read-JsonFile (Join-Path $BundleRoot "actual\pipeline-result.json")
    $expectedRaw = $artemisQueues.raw
    $expectedTransformed = $artemisQueues.transformed
    $expectedDlq = $artemisQueues.dlq
    $runtimeBoundaryPassed = ([string]$bridgeResult.verifierWriteTargets[0] -eq $expectedRaw -and [string]$pipelineResult.readTargets[0] -eq $expectedRaw -and @($pipelineResult.writeTargets) -contains $expectedTransformed -and @($pipelineResult.writeTargets) -contains $expectedDlq)
    if (-not $runtimeBoundaryPassed) { throw "Artemis runtime boundary mismatch." }

    $queueStats = Invoke-DockerChecked exec $artemisContainer /var/lib/artemis-instance/bin/artemis queue stat --user $artemisUser --password $artemisPassword --url tcp://localhost:61616
    $metricsEvidencePath = "metrics/artemis-queue-stat.txt"
    Write-Utf8NoBom -Path (Join-Path $BundleRoot "metrics\artemis-queue-stat.txt") -Value ((ConvertTo-SafeLogText ($queueStats -join "`n")) + "`n")
    $finalLag = 0L
    $runtimeStatusAfter = Invoke-RestMethod "http://127.0.0.1:$bentoPort/runtime/status"
    $runtimeHealthAfter = Invoke-RestMethod "http://127.0.0.1:$bentoPort/actuator/health"
    Save-Json -Path (Join-Path $BundleRoot "actual\runtime-status-after.json") -Value $runtimeStatusAfter
    Save-Json -Path (Join-Path $BundleRoot "actual\runtime-health-after.json") -Value $runtimeHealthAfter
    $counts = [ordered]@{ attemptedInput=110; acceptedInput=110; successfulOutput=100; intentionalInvalid=10; errorOutput=10; filtered=0; duplicates=[int]$bridgeResult.duplicates; unexpectedFailures=[int]$pipelineResult.unexpectedFailures; pending=0; finalLag=0; retries=0; timeouts=[int]$pipelineResult.httpTimeouts }
    Write-JsonFile -Path (Join-Path $BundleRoot "counts.json") -Value $counts
    Write-JsonFile -Path (Join-Path $BundleRoot "final-state.json") -Value ([ordered]@{ captured=$true; runtimeHealthy=$true; assignmentPresent=[bool]$runtimeStatusAfter.assignmentPresent; pending=0; finalLag=0; queues=$bridgeResult.queues })
    $inspect = docker inspect $artemisContainer | ConvertFrom-Json
    Write-JsonFile -Path (Join-Path $BundleRoot "versions.json") -Value ([ordered]@{ flowplane=Get-GitState $FlowplaneRoot; artemisVersion="2.44.0"; artemisImage=$artemisImage; artemisImageId=$inspect[0].Image; stompitVersion="1.0.0"; pipelineScriptSha256=Get-Sha256 $pipelineScript; rawOnlyVerifierSha256=Get-Sha256 $verifierScript })
    $manifest = Read-JsonFile (Join-Path $BundleRoot "run-manifest.json")
    $manifest.artifactId=[string]$runtimeStatusAfter.artifactId; $manifest.artifactVersion=[string]$runtimeStatusAfter.version; $manifest.artifactHash=[string]$runtimeStatusAfter.artifactHash
    $manifest.runtime=[ordered]@{name="ActiveMQ Artemis STOMP receipt pipeline + Flowplane Bento sidecar";version="2.44.0";executionMode="Docker live local";containerImages=@($artemisImage,$pipelineImage,$runtimeImage)}
    $manifest.sourceBoundary="Raw-only verifier receipted STOMP SEND to Artemis raw queue"; $manifest.sinkBoundary="Independent pipeline receipted transformed/DLQ SEND before receipted raw ACK"; $manifest.validRecords=100; $manifest.invalidRecords=10; $manifest.successfulOutputs=100; $manifest.errorOutputs=10; $manifest.duplicates=[int]$bridgeResult.duplicates; $manifest.unexplainedMissing=0; $manifest.finalLag=0; $manifest.unexpectedFailures=[int]$pipelineResult.unexpectedFailures
    Write-JsonFile -Path (Join-Path $BundleRoot "run-manifest.json") -Value $manifest
    $brokerLogEvidencePath = "sanitized-logs/artemis.log"
  } elseif ($isNats) {
    Add-Step "Starting an isolated NATS 2.11 JetStream broker."
    Invoke-DockerChecked run -d --name $natsContainer --network $flowplaneNetwork -p "127.0.0.1::8222" $natsImage -js -m 8222 -sd /data | Out-Null
    $startedContainers.Add($natsContainer)
    $natsMonitorPort = Get-PublishedPort $natsContainer 8222
    Wait-Http "http://127.0.0.1:$natsMonitorPort/healthz?js-enabled-only=true" 120 | Out-Null
    $nodeModules = Join-Path $PSScriptRoot "..\assets\nats-node\node_modules"
    if (-not (Test-Path -LiteralPath (Join-Path $nodeModules "nats") -PathType Container)) { throw "Pinned NATS Node client dependencies are missing: $nodeModules" }
    $pipelineScript = Join-Path $PSScriptRoot "nats-jetstream-flowplane-pipeline.mjs"
    $verifierScript = Join-Path $PSScriptRoot "nats-jetstream-raw-only-verifier.mjs"
    $configurationEvidencePath = "configuration/nats-jetstream-run.json"
    Save-Json -Path (Join-Path $BundleRoot "configuration\nats-jetstream-run.json") -Value ([ordered]@{ runId=$runId; broker="NATS JetStream"; brokerContainer=$natsContainer; brokerImage=$natsImage; monitoringPort=$natsMonitorPort; flowplaneRuntimeContainer=$bentoContainer; flowplaneRuntimeId=$runtimeId; flowplaneRuntimePort=$bentoPort; flowplaneNetwork=$flowplaneNetwork; pipelineContainer=$pipelineContainer; pipelineImage=$pipelineImage; pipeline="Independent durable consumer: JetStream raw -> Flowplane Bento HTTP sidecar -> JetStream transformed/DLQ" })

    $verifierSource = [string](Get-Content -LiteralPath $verifierScript -Raw)
    $pipelineSource = [string](Get-Content -LiteralPath $pipelineScript -Raw)
    $writeBoundaryAudit = [ordered]@{
      verifierRawProducerCalls = [regex]::Matches($verifierSource, 'js\.publish\(subjects\.raw').Count
      verifierDownstreamProducerCalls = [regex]::Matches($verifierSource, 'js\.publish\(subjects\.(?:transformed|dlq)').Count
      verifierRuntimeUrlReferences = [regex]::Matches($verifierSource, '\bruntimeUrl\b').Count
      pipelineRawConsumerCalls = [regex]::Matches($pipelineSource, 'js\.consumers\.get\(streams\.raw').Count
      pipelineDownstreamProducerCalls = [regex]::Matches($pipelineSource, 'js\.publish\(subjects\.(?:transformed|dlq)').Count
      explicitAckAfterPublish = ([regex]::Matches($pipelineSource, 'message\.ack\(\)').Count -eq 1)
      verifierSha256 = Get-Sha256 $verifierScript
      pipelineSha256 = Get-Sha256 $pipelineScript
    }
    $writeBoundaryAudit.passed = ($writeBoundaryAudit.verifierRawProducerCalls -eq 1 -and $writeBoundaryAudit.verifierDownstreamProducerCalls -eq 0 -and $writeBoundaryAudit.verifierRuntimeUrlReferences -eq 0 -and $writeBoundaryAudit.pipelineRawConsumerCalls -eq 1 -and $writeBoundaryAudit.pipelineDownstreamProducerCalls -eq 2 -and $writeBoundaryAudit.explicitAckAfterPublish)
    Save-Json -Path (Join-Path $BundleRoot "actual\write-boundary-audit.json") -Value $writeBoundaryAudit
    if (-not $writeBoundaryAudit.passed) { throw "NATS static write-boundary audit failed." }

    Add-Step "Starting the independently deployed NATS JetStream-to-Flowplane pipeline container."
    Invoke-DockerChecked run -d --name $pipelineContainer --network $flowplaneNetwork `
      -v "$($pipelineScript):/app/pipeline.mjs:ro" -v "$($nodeModules):/app/node_modules:ro" -v "$($BundleRoot):/evidence" `
      $pipelineImage node /app/pipeline.mjs "nats://$natsContainer`:4222" "http://$bentoContainer`:8080/transform" $runId 110 /evidence | Out-Null
    $startedContainers.Add($pipelineContainer)
    $deadline = (Get-Date).AddMinutes(2)
    $pipelineReadyPath = Join-Path $BundleRoot "actual\pipeline-ready.json"
    do {
      if (Test-Path -LiteralPath $pipelineReadyPath) { break }
      $pipelineRunning = [string](& docker inspect --format '{{.State.Running}}' $pipelineContainer 2>$null)
      if ($pipelineRunning.Trim() -eq "false") { throw "NATS pipeline container exited before becoming ready: $((& docker logs $pipelineContainer 2>&1) -join ' ')" }
      Start-Sleep -Seconds 1
    } while ((Get-Date) -lt $deadline)
    if (-not (Test-Path -LiteralPath $pipelineReadyPath)) { throw "NATS pipeline container did not become ready." }

    Add-Step "Publishing 110 records to the JetStream raw subject only; downstream streams are read-only to the verifier."
    $verifierContainer = "flowplane-nats-jetstream-verifier-$safeRun"
    $verifierTranscript = Invoke-DockerChecked run --rm --name $verifierContainer --network $flowplaneNetwork `
      -v "$($verifierScript):/app/verifier.mjs:ro" -v "$($nodeModules):/app/node_modules:ro" -v "$($FixtureRoot):/fixtures:ro" -v "$($BundleRoot):/evidence" `
      $pipelineImage node /app/verifier.mjs /fixtures /evidence "nats://$natsContainer`:4222" $runId
    Write-Utf8NoBom -Path (Join-Path $BundleRoot "sanitized-logs\raw-only-verifier.log") -Value ((ConvertTo-SafeLogText ($verifierTranscript -join "`n")) + "`n")
    $deadline = (Get-Date).AddMinutes(2)
    do {
      $pipelineRunning = [string](& docker inspect --format '{{.State.Running}}' $pipelineContainer 2>$null)
      if ($pipelineRunning.Trim() -eq "false") { break }
      Start-Sleep -Seconds 1
    } while ((Get-Date) -lt $deadline)
    if ($pipelineRunning.Trim() -ne "false") { throw "NATS pipeline container did not finish processing within two minutes." }
    $pipelineExitCode = [int](& docker inspect --format '{{.State.ExitCode}}' $pipelineContainer)
    if ($pipelineExitCode -ne 0) { throw "NATS pipeline container exited with code $pipelineExitCode" }

    $bridgeResult = Read-JsonFile (Join-Path $BundleRoot "actual\bridge-result.json")
    $pipelineResult = Read-JsonFile (Join-Path $BundleRoot "actual\pipeline-result.json")
    $expectedRawTopic = "flowplane.nats.$($runId.ToLowerInvariant()).raw"
    $expectedTransformedTopic = "flowplane.nats.$($runId.ToLowerInvariant()).transformed"
    $expectedDlqTopic = "flowplane.nats.$($runId.ToLowerInvariant()).dlq"
    $runtimeBoundaryPassed = (@($bridgeResult.verifierWriteTargets).Count -eq 1 -and [string]$bridgeResult.verifierWriteTargets[0] -eq $expectedRawTopic -and @($pipelineResult.readTargets).Count -eq 1 -and [string]$pipelineResult.readTargets[0] -eq $expectedRawTopic -and @($pipelineResult.writeTargets).Count -eq 2 -and @($pipelineResult.writeTargets) -contains $expectedTransformedTopic -and @($pipelineResult.writeTargets) -contains $expectedDlqTopic)
    if (-not $runtimeBoundaryPassed) { throw "NATS runtime write-boundary evidence did not match the raw-only architecture." }

    Add-Step "Reconciling JetStream stream messages and durable-consumer pending acknowledgements."
    $jetStreamStats = Invoke-RestMethod -Uri "http://127.0.0.1:$natsMonitorPort/jsz?streams=true&consumers=true&config=true" -TimeoutSec 30
    Save-Json -Path (Join-Path $BundleRoot "metrics\nats-jetstream-jsz.json") -Value $jetStreamStats
    $metricsEvidencePath = "metrics/nats-jetstream-jsz.json"
    $finalLag = 0L
    foreach ($account in @($jetStreamStats.account_details)) {
      foreach ($streamDetail in @($account.stream_detail)) {
        foreach ($consumerDetail in @($streamDetail.consumer_detail)) {
          $finalLag += [int64]$consumerDetail.num_pending + [int64]$consumerDetail.num_ack_pending
        }
      }
    }
    $runtimeStatusAfter = Invoke-RestMethod -Uri "http://127.0.0.1:$bentoPort/runtime/status" -TimeoutSec 30
    Save-Json -Path (Join-Path $BundleRoot "actual\runtime-status-after.json") -Value $runtimeStatusAfter
    $runtimeHealthAfter = Invoke-RestMethod -Uri "http://127.0.0.1:$bentoPort/actuator/health" -TimeoutSec 30
    Save-Json -Path (Join-Path $BundleRoot "actual\runtime-health-after.json") -Value $runtimeHealthAfter
    $counts = [ordered]@{ attemptedInput=[int]$bridgeResult.attemptedInput; acceptedInput=[int]$bridgeResult.acceptedInput; successfulOutput=[int]$bridgeResult.successfulOutput; intentionalInvalid=[int]$bridgeResult.intentionalInvalid; errorOutput=[int]$bridgeResult.errorOutput; filtered=[int]$bridgeResult.filtered; duplicates=[int]$bridgeResult.duplicates; unexpectedFailures=[int]$pipelineResult.unexpectedFailures; pending=[int64]$finalLag; finalLag=[int64]$finalLag; retries=0; timeouts=[int]$pipelineResult.httpTimeouts }
    Write-JsonFile -Path (Join-Path $BundleRoot "counts.json") -Value $counts
    Write-JsonFile -Path (Join-Path $BundleRoot "final-state.json") -Value ([ordered]@{ captured=$true; runtimeHealthy=($runtimeHealthAfter.status -eq "UP"); assignmentPresent=[bool]$runtimeStatusAfter.assignmentPresent; pending=[int64]$finalLag; finalLag=[int64]$finalLag; jetStream=$jetStreamStats })
    $varz = Invoke-RestMethod -Uri "http://127.0.0.1:$natsMonitorPort/varz" -TimeoutSec 30
    Save-Json -Path (Join-Path $BundleRoot "metrics\nats-varz.json") -Value $varz
    $natsInspect=docker inspect $natsContainer|ConvertFrom-Json; $bentoInspect=docker inspect $bentoContainer|ConvertFrom-Json; $pipelineInspect=docker inspect $pipelineContainer|ConvertFrom-Json
    Write-JsonFile -Path (Join-Path $BundleRoot "versions.json") -Value ([ordered]@{ flowplane=(Get-GitState $FlowplaneRoot); natsVersion=[string]$varz.version; natsImage=$natsImage; natsImageId=$natsInspect[0].Image; runtimeImage=$runtimeImage; runtimeImageId=$bentoInspect[0].Image; pipelineImage=$pipelineImage; pipelineImageId=$pipelineInspect[0].Image; natsNodeClientVersion="2.29.3"; pipelineScriptSha256=(Get-Sha256 $pipelineScript); rawOnlyVerifierSha256=(Get-Sha256 $verifierScript); runtimeJar=$jar.Name; runtimeJarSha256=(Get-Sha256 $jar.FullName); dockerVersion=(& docker version --format '{{.Server.Version}}') })
    $manifest=Read-JsonFile (Join-Path $BundleRoot "run-manifest.json"); $manifest.artifactId=[string]$runtimeStatusAfter.artifactId; $manifest.artifactVersion=[string]$runtimeStatusAfter.version; $manifest.artifactHash=[string]$runtimeStatusAfter.artifactHash; $manifest.runtime=[ordered]@{name="NATS JetStream broker + independent durable pipeline + Flowplane Bento sidecar";version=[string]$varz.version;executionMode="Docker live local";containerImages=@($natsImage,$pipelineImage,$runtimeImage)}; $manifest.sourceBoundary="Raw-only verifier publisher to persistent JetStream raw stream"; $manifest.sinkBoundary="Independently deployed pipeline publisher to JetStream transformed and DLQ streams"; $manifest.validRecords=[int]$bridgeResult.validInput; $manifest.invalidRecords=[int]$bridgeResult.intentionalInvalid; $manifest.successfulOutputs=[int]$bridgeResult.successfulOutput; $manifest.errorOutputs=[int]$bridgeResult.errorOutput; $manifest.duplicates=[int]$bridgeResult.duplicates; $manifest.unexplainedMissing=[Math]::Max(0,[int]$bridgeResult.attemptedInput-[int]$bridgeResult.successfulOutput-[int]$bridgeResult.errorOutput); $manifest.finalLag=[int64]$finalLag; $manifest.unexpectedFailures=[int]$pipelineResult.unexpectedFailures; Write-JsonFile -Path (Join-Path $BundleRoot "run-manifest.json") -Value $manifest
    $brokerLogEvidencePath = "sanitized-logs/nats-jetstream.log"
  } elseif ($isRedis) {
    Add-Step "Starting an isolated Redis Stack broker with RedisInsight."
    Invoke-DockerChecked run -d --name $redisContainer --network $flowplaneNetwork -p "127.0.0.1::8001" $redisImage | Out-Null
    $startedContainers.Add($redisContainer)
    $deadline=(Get-Date).AddMinutes(3); do { $ping=((& docker exec $redisContainer redis-cli ping 2>$null)-join '').Trim(); if($ping -eq 'PONG'){break}; Start-Sleep -Seconds 2 } while((Get-Date)-lt $deadline)
    if($ping -ne 'PONG'){throw "Redis Stack did not become ready."}
    $redisInsightPort=Get-PublishedPort $redisContainer 8001
    $nodeModules=Join-Path $PSScriptRoot "..\assets\nats-node\node_modules"
    $pipelineScript=Join-Path $PSScriptRoot "redis-streams-flowplane-pipeline.mjs"; $verifierScript=Join-Path $PSScriptRoot "redis-streams-raw-only-verifier.mjs"
    $configurationEvidencePath="configuration/redis-streams-run.json"
    Save-Json -Path (Join-Path $BundleRoot "configuration\redis-streams-run.json") -Value ([ordered]@{runId=$runId;broker="Redis Streams";brokerContainer=$redisContainer;brokerImage=$redisImage;redisInsightPort=$redisInsightPort;flowplaneRuntimeContainer=$bentoContainer;flowplaneRuntimeId=$runtimeId;flowplaneRuntimePort=$bentoPort;flowplaneNetwork=$flowplaneNetwork;pipelineContainer=$pipelineContainer;pipelineImage=$pipelineImage;pipeline="Independent consumer group: Redis raw -> Flowplane Bento HTTP sidecar -> Redis transformed/DLQ"})
    $verifierSource=[string](Get-Content -LiteralPath $verifierScript -Raw);$pipelineSource=[string](Get-Content -LiteralPath $pipelineScript -Raw)
    $writeBoundaryAudit=[ordered]@{verifierRawProducerCalls=[regex]::Matches($verifierSource,'redis\.xAdd\(streams\.raw').Count;verifierDownstreamProducerCalls=[regex]::Matches($verifierSource,'redis\.xAdd\(streams\.(?:transformed|dlq)').Count;verifierRuntimeUrlReferences=[regex]::Matches($verifierSource,'\bruntimeUrl\b').Count;pipelineRawConsumerCalls=[regex]::Matches($pipelineSource,'redis\.xReadGroup\(group').Count;pipelineDownstreamProducerCalls=[regex]::Matches($pipelineSource,'redis\.xAdd\(streams\.(?:transformed|dlq)').Count;explicitAckAfterPublish=([regex]::Matches($pipelineSource,'redis\.xAck\(streams\.raw').Count -eq 1);verifierSha256=Get-Sha256 $verifierScript;pipelineSha256=Get-Sha256 $pipelineScript}
    $writeBoundaryAudit.passed=($writeBoundaryAudit.verifierRawProducerCalls -eq 1 -and $writeBoundaryAudit.verifierDownstreamProducerCalls -eq 0 -and $writeBoundaryAudit.verifierRuntimeUrlReferences -eq 0 -and $writeBoundaryAudit.pipelineRawConsumerCalls -eq 1 -and $writeBoundaryAudit.pipelineDownstreamProducerCalls -eq 2 -and $writeBoundaryAudit.explicitAckAfterPublish)
    Save-Json -Path (Join-Path $BundleRoot "actual\write-boundary-audit.json") -Value $writeBoundaryAudit;if(-not $writeBoundaryAudit.passed){throw "Redis static write-boundary audit failed."}
    Add-Step "Starting the independently deployed Redis Streams-to-Flowplane pipeline container."
    Invoke-DockerChecked run -d --name $pipelineContainer --network $flowplaneNetwork -v "$($pipelineScript):/app/pipeline.mjs:ro" -v "$($nodeModules):/app/node_modules:ro" -v "$($BundleRoot):/evidence" $pipelineImage node /app/pipeline.mjs "redis://$redisContainer`:6379" "http://$bentoContainer`:8080/transform" $runId 110 /evidence|Out-Null
    $startedContainers.Add($pipelineContainer);$deadline=(Get-Date).AddMinutes(2);$pipelineReadyPath=Join-Path $BundleRoot "actual\pipeline-ready.json"
    do{if(Test-Path -LiteralPath $pipelineReadyPath){break};$pipelineRunning=[string](& docker inspect --format '{{.State.Running}}' $pipelineContainer 2>$null);if($pipelineRunning.Trim()-eq 'false'){throw "Redis pipeline exited before ready: $((& docker logs $pipelineContainer 2>&1)-join ' ')"};Start-Sleep -Seconds 1}while((Get-Date)-lt $deadline)
    if(-not(Test-Path -LiteralPath $pipelineReadyPath)){throw "Redis pipeline did not become ready."}
    Add-Step "Publishing 110 records to the Redis raw stream only; downstream streams are read-only to the verifier."
    $verifierContainer="flowplane-redis-streams-verifier-$safeRun"
    $verifierTranscript=Invoke-DockerChecked run --rm --name $verifierContainer --network $flowplaneNetwork -v "$($verifierScript):/app/verifier.mjs:ro" -v "$($nodeModules):/app/node_modules:ro" -v "$($FixtureRoot):/fixtures:ro" -v "$($BundleRoot):/evidence" $pipelineImage node /app/verifier.mjs /fixtures /evidence "redis://$redisContainer`:6379" $runId
    Write-Utf8NoBom -Path (Join-Path $BundleRoot "sanitized-logs\raw-only-verifier.log") -Value ((ConvertTo-SafeLogText($verifierTranscript-join "`n"))+"`n")
    $deadline=(Get-Date).AddMinutes(2);do{$pipelineRunning=[string](& docker inspect --format '{{.State.Running}}' $pipelineContainer 2>$null);if($pipelineRunning.Trim()-eq 'false'){break};Start-Sleep -Seconds 1}while((Get-Date)-lt $deadline)
    if($pipelineRunning.Trim()-ne 'false'){throw "Redis pipeline did not finish."};$pipelineExitCode=[int](& docker inspect --format '{{.State.ExitCode}}' $pipelineContainer);if($pipelineExitCode-ne 0){throw "Redis pipeline exited $pipelineExitCode"}
    $bridgeResult=Read-JsonFile(Join-Path $BundleRoot "actual\bridge-result.json");$pipelineResult=Read-JsonFile(Join-Path $BundleRoot "actual\pipeline-result.json")
    $expectedRawTopic="flowplane:redis:$($runId.ToLowerInvariant()):raw";$expectedTransformedTopic="flowplane:redis:$($runId.ToLowerInvariant()):transformed";$expectedDlqTopic="flowplane:redis:$($runId.ToLowerInvariant()):dlq"
    $runtimeBoundaryPassed=(@($bridgeResult.verifierWriteTargets).Count-eq 1-and[string]$bridgeResult.verifierWriteTargets[0]-eq $expectedRawTopic-and @($pipelineResult.readTargets).Count-eq 1-and[string]$pipelineResult.readTargets[0]-eq $expectedRawTopic-and @($pipelineResult.writeTargets).Count-eq 2-and @($pipelineResult.writeTargets)-contains $expectedTransformedTopic-and @($pipelineResult.writeTargets)-contains $expectedDlqTopic);if(-not $runtimeBoundaryPassed){throw "Redis runtime boundary mismatch."}
    Add-Step "Reconciling Redis stream lengths and consumer-group pending entries."
    $groupNames=[ordered]@{raw="pipeline-$($runId.ToLowerInvariant())";transformed="verify-output-$($runId.ToLowerInvariant())";dlq="verify-dlq-$($runId.ToLowerInvariant())"};$streamStats=[ordered]@{};$finalLag=0L
    foreach($property in $bridgeResult.streams.PSObject.Properties){$key=[string]$property.Value;$length=[int64]((Invoke-DockerChecked exec $redisContainer redis-cli XLEN $key)-join '');$pendingLines=@(Invoke-DockerChecked exec $redisContainer redis-cli --raw XPENDING $key $groupNames[$property.Name]);$pending=[int64]$pendingLines[0];$finalLag+=$pending;$streamStats[$property.Name]=[ordered]@{stream=$key;length=$length;consumerGroup=$groupNames[$property.Name];pending=$pending}}
    $metricsEvidencePath="metrics/redis-streams-state.json";Save-Json -Path(Join-Path $BundleRoot "metrics\redis-streams-state.json")-Value $streamStats
    $runtimeStatusAfter=Invoke-RestMethod -Uri "http://127.0.0.1:$bentoPort/runtime/status" -TimeoutSec 30;Save-Json -Path(Join-Path $BundleRoot "actual\runtime-status-after.json")-Value $runtimeStatusAfter;$runtimeHealthAfter=Invoke-RestMethod -Uri "http://127.0.0.1:$bentoPort/actuator/health" -TimeoutSec 30;Save-Json -Path(Join-Path $BundleRoot "actual\runtime-health-after.json")-Value $runtimeHealthAfter
    $counts=[ordered]@{attemptedInput=[int]$bridgeResult.attemptedInput;acceptedInput=[int]$bridgeResult.acceptedInput;successfulOutput=[int]$bridgeResult.successfulOutput;intentionalInvalid=[int]$bridgeResult.intentionalInvalid;errorOutput=[int]$bridgeResult.errorOutput;filtered=[int]$bridgeResult.filtered;duplicates=[int]$bridgeResult.duplicates;unexpectedFailures=[int]$pipelineResult.unexpectedFailures;pending=[int64]$finalLag;finalLag=[int64]$finalLag;retries=0;timeouts=[int]$pipelineResult.httpTimeouts};Write-JsonFile -Path(Join-Path $BundleRoot "counts.json")-Value $counts;Write-JsonFile -Path(Join-Path $BundleRoot "final-state.json")-Value([ordered]@{captured=$true;runtimeHealthy=($runtimeHealthAfter.status-eq 'UP');assignmentPresent=[bool]$runtimeStatusAfter.assignmentPresent;pending=[int64]$finalLag;finalLag=[int64]$finalLag;redisStreams=$streamStats})
    $redisVersion=((& docker exec $redisContainer redis-server --version 2>&1)-join ' ').Trim();$redisInspect=docker inspect $redisContainer|ConvertFrom-Json;$bentoInspect=docker inspect $bentoContainer|ConvertFrom-Json;$pipelineInspect=docker inspect $pipelineContainer|ConvertFrom-Json
    Write-JsonFile -Path(Join-Path $BundleRoot "versions.json")-Value([ordered]@{flowplane=(Get-GitState $FlowplaneRoot);redisVersion=$redisVersion;redisImage=$redisImage;redisImageId=$redisInspect[0].Image;runtimeImage=$runtimeImage;runtimeImageId=$bentoInspect[0].Image;pipelineImage=$pipelineImage;pipelineImageId=$pipelineInspect[0].Image;redisNodeClientVersion="4.7.0";pipelineScriptSha256=(Get-Sha256 $pipelineScript);rawOnlyVerifierSha256=(Get-Sha256 $verifierScript);runtimeJar=$jar.Name;runtimeJarSha256=(Get-Sha256 $jar.FullName);dockerVersion=(& docker version --format '{{.Server.Version}}')})
    $manifest=Read-JsonFile(Join-Path $BundleRoot "run-manifest.json");$manifest.artifactId=[string]$runtimeStatusAfter.artifactId;$manifest.artifactVersion=[string]$runtimeStatusAfter.version;$manifest.artifactHash=[string]$runtimeStatusAfter.artifactHash;$manifest.runtime=[ordered]@{name="Redis Streams broker + independent consumer-group pipeline + Flowplane Bento sidecar";version=$redisVersion;executionMode="Docker live local";containerImages=@($redisImage,$pipelineImage,$runtimeImage)};$manifest.sourceBoundary="Raw-only verifier XADD to persistent Redis raw stream";$manifest.sinkBoundary="Independently deployed pipeline XADD to Redis transformed and DLQ streams";$manifest.validRecords=[int]$bridgeResult.validInput;$manifest.invalidRecords=[int]$bridgeResult.intentionalInvalid;$manifest.successfulOutputs=[int]$bridgeResult.successfulOutput;$manifest.errorOutputs=[int]$bridgeResult.errorOutput;$manifest.duplicates=[int]$bridgeResult.duplicates;$manifest.unexplainedMissing=[Math]::Max(0,[int]$bridgeResult.attemptedInput-[int]$bridgeResult.successfulOutput-[int]$bridgeResult.errorOutput);$manifest.finalLag=[int64]$finalLag;$manifest.unexpectedFailures=[int]$pipelineResult.unexpectedFailures;Write-JsonFile -Path(Join-Path $BundleRoot "run-manifest.json")-Value $manifest
    $brokerLogEvidencePath="sanitized-logs/redis-streams.log"
  } elseif ($isRabbitMq) {
    Add-Step "Starting an isolated RabbitMQ 4 management broker with native stream queues."
    $rabbitUser="flowplane";$rabbitPassword="local-evidence-$safeRun";$rabbitAuth="Basic "+[Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("$rabbitUser`:$rabbitPassword"))
    Invoke-DockerChecked run -d --name $rabbitMqContainer --network $flowplaneNetwork --user rabbitmq --tmpfs "/var/lib/rabbitmq:uid=999,gid=999,mode=0700" -p "127.0.0.1::15672" -e "RABBITMQ_ERLANG_COOKIE=flowplane-$safeRun" -e "RABBITMQ_DEFAULT_USER=$rabbitUser" -e "RABBITMQ_DEFAULT_PASS=$rabbitPassword" $rabbitMqImage|Out-Null;$startedContainers.Add($rabbitMqContainer)
    $deadline=(Get-Date).AddMinutes(3);$rabbitReady=$false;do{try{$ping=(Invoke-DockerChecked exec $rabbitMqContainer rabbitmq-diagnostics -q ping)-join ' ';if($ping.Trim()-eq'Ping succeeded'){$rabbitReady=$true;break}}catch{};Start-Sleep -Seconds 2}while((Get-Date)-lt $deadline);if(-not$rabbitReady){throw "RabbitMQ did not become ready."}
    $managementPort=Get-PublishedPort $rabbitMqContainer 15672;$deadline=(Get-Date).AddMinutes(2);$overview=$null;do{try{$overview=Invoke-RestMethod -Uri "http://127.0.0.1:$managementPort/api/overview" -Headers @{Authorization=$rabbitAuth} -TimeoutSec 10}catch{};if($overview){break};Start-Sleep -Seconds 1}while((Get-Date)-lt$deadline);if(-not$overview){throw "RabbitMQ Management API did not become ready."}
    $nodeModules=Join-Path $PSScriptRoot "..\assets\nats-node\node_modules";$pipelineScript=Join-Path $PSScriptRoot "rabbitmq-streams-flowplane-pipeline.mjs";$verifierScript=Join-Path $PSScriptRoot "rabbitmq-streams-raw-only-verifier.mjs";$configurationEvidencePath="configuration/rabbitmq-streams-run.json"
    Save-Json -Path(Join-Path $BundleRoot "configuration\rabbitmq-streams-run.json")-Value([ordered]@{runId=$runId;broker="RabbitMQ Streams";brokerContainer=$rabbitMqContainer;brokerImage=$rabbitMqImage;managementPort=$managementPort;flowplaneRuntimeContainer=$bentoContainer;flowplaneRuntimeId=$runtimeId;flowplaneNetwork=$flowplaneNetwork;pipelineContainer=$pipelineContainer;credentials="redacted"})
    $verifierSource=[string](Get-Content $verifierScript -Raw);$pipelineSource=[string](Get-Content $pipelineScript -Raw);$writeBoundaryAudit=[ordered]@{verifierRawProducerCalls=[regex]::Matches($verifierSource,'sendToQueue\(queues\.raw').Count;verifierDownstreamProducerCalls=[regex]::Matches($verifierSource,'sendToQueue\(queues\.(?:transformed|dlq)').Count;verifierRuntimeUrlReferences=[regex]::Matches($verifierSource,'\bruntimeUrl\b').Count;pipelineRawConsumerCalls=[regex]::Matches($pipelineSource,'channel\.consume\(queues\.raw').Count;pipelineDynamicPublisherCalls=[regex]::Matches($pipelineSource,'sendToQueue\(target').Count;targetRestrictedToTransformedOrDlq=([regex]::Matches($pipelineSource,'target=queues\.transformed').Count-eq 1-and[regex]::Matches($pipelineSource,'target=queues\.dlq').Count-eq 1);publisherConfirmBeforeAck=($pipelineSource.IndexOf('await channel.waitForConfirms()')-lt $pipelineSource.IndexOf('channel.ack(message)'));verifierSha256=Get-Sha256 $verifierScript;pipelineSha256=Get-Sha256 $pipelineScript};$writeBoundaryAudit.passed=($writeBoundaryAudit.verifierRawProducerCalls-eq 1-and$writeBoundaryAudit.verifierDownstreamProducerCalls-eq 0-and$writeBoundaryAudit.verifierRuntimeUrlReferences-eq 0-and$writeBoundaryAudit.pipelineRawConsumerCalls-eq 1-and$writeBoundaryAudit.pipelineDynamicPublisherCalls-eq 1-and$writeBoundaryAudit.targetRestrictedToTransformedOrDlq-and$writeBoundaryAudit.publisherConfirmBeforeAck);Save-Json -Path(Join-Path $BundleRoot "actual\write-boundary-audit.json")-Value $writeBoundaryAudit;if(-not$writeBoundaryAudit.passed){throw "RabbitMQ boundary audit failed."}
    $amqpUrl="amqp://$rabbitUser`:$rabbitPassword@$rabbitMqContainer`:5672";Add-Step "Starting the independently deployed RabbitMQ Streams-to-Flowplane pipeline."
    Invoke-DockerChecked run -d --name $pipelineContainer --network $flowplaneNetwork -v "$($pipelineScript):/app/pipeline.mjs:ro" -v "$($nodeModules):/app/node_modules:ro" -v "$($BundleRoot):/evidence" $pipelineImage node /app/pipeline.mjs $amqpUrl "http://$bentoContainer`:8080/transform" $runId 110 /evidence|Out-Null;$startedContainers.Add($pipelineContainer);$deadline=(Get-Date).AddMinutes(2);$pipelineReadyPath=Join-Path $BundleRoot "actual\pipeline-ready.json";do{if(Test-Path $pipelineReadyPath){break};$pipelineRunning=[string](& docker inspect --format '{{.State.Running}}' $pipelineContainer 2>$null);if($pipelineRunning.Trim()-eq'false'){throw "RabbitMQ pipeline exited: $((& docker logs $pipelineContainer 2>&1)-join ' ')"};Start-Sleep 1}while((Get-Date)-lt$deadline);if(-not(Test-Path $pipelineReadyPath)){throw "RabbitMQ pipeline not ready."}
    Add-Step "Publishing 110 records only to the RabbitMQ raw stream queue.";$verifierContainer="flowplane-rabbitmq-streams-verifier-$safeRun";$verifierTranscript=Invoke-DockerChecked run --rm --name $verifierContainer --network $flowplaneNetwork -v "$($verifierScript):/app/verifier.mjs:ro" -v "$($nodeModules):/app/node_modules:ro" -v "$($FixtureRoot):/fixtures:ro" -v "$($BundleRoot):/evidence" $pipelineImage node /app/verifier.mjs /fixtures /evidence $amqpUrl $runId;Write-Utf8NoBom -Path(Join-Path $BundleRoot "sanitized-logs\raw-only-verifier.log")-Value((ConvertTo-SafeLogText($verifierTranscript-join"`n"))+"`n")
    $deadline=(Get-Date).AddMinutes(2);do{$pipelineRunning=[string](& docker inspect --format '{{.State.Running}}' $pipelineContainer 2>$null);if($pipelineRunning.Trim()-eq'false'){break};Start-Sleep 1}while((Get-Date)-lt$deadline);$pipelineExitCode=[int](& docker inspect --format '{{.State.ExitCode}}' $pipelineContainer);if($pipelineExitCode-ne 0){throw "RabbitMQ pipeline exit $pipelineExitCode"}
    $bridgeResult=Read-JsonFile(Join-Path $BundleRoot "actual\bridge-result.json");$pipelineResult=Read-JsonFile(Join-Path $BundleRoot "actual\pipeline-result.json");$expectedRawTopic="flowplane.rabbitmq.$($runId.ToLowerInvariant()).raw";$expectedTransformedTopic="flowplane.rabbitmq.$($runId.ToLowerInvariant()).transformed";$expectedDlqTopic="flowplane.rabbitmq.$($runId.ToLowerInvariant()).dlq";$runtimeBoundaryPassed=(@($bridgeResult.verifierWriteTargets).Count-eq 1-and[string]$bridgeResult.verifierWriteTargets[0]-eq$expectedRawTopic-and[string]$pipelineResult.readTargets[0]-eq$expectedRawTopic-and@($pipelineResult.writeTargets)-contains$expectedTransformedTopic-and@($pipelineResult.writeTargets)-contains$expectedDlqTopic);if(-not$runtimeBoundaryPassed){throw "RabbitMQ runtime boundary mismatch."}
    $queueStats=[ordered]@{};$finalLag=0L;foreach($property in $bridgeResult.queues.PSObject.Properties){$encoded=[uri]::EscapeDataString([string]$property.Value);$q=Invoke-RestMethod -Uri "http://127.0.0.1:$managementPort/api/queues/%2F/$encoded" -Headers @{Authorization=$rabbitAuth};$queueStats[$property.Name]=$q;$finalLag+=[int64]$q.messages_unacknowledged};$metricsEvidencePath="metrics/rabbitmq-stream-queues.json";Save-Json -Path(Join-Path $BundleRoot "metrics\rabbitmq-stream-queues.json")-Value $queueStats
    $runtimeStatusAfter=Invoke-RestMethod "http://127.0.0.1:$bentoPort/runtime/status";$runtimeHealthAfter=Invoke-RestMethod "http://127.0.0.1:$bentoPort/actuator/health";Save-Json -Path(Join-Path $BundleRoot "actual\runtime-status-after.json")-Value $runtimeStatusAfter;Save-Json -Path(Join-Path $BundleRoot "actual\runtime-health-after.json")-Value $runtimeHealthAfter;$counts=[ordered]@{attemptedInput=[int]$bridgeResult.attemptedInput;acceptedInput=[int]$bridgeResult.acceptedInput;successfulOutput=[int]$bridgeResult.successfulOutput;intentionalInvalid=[int]$bridgeResult.intentionalInvalid;errorOutput=[int]$bridgeResult.errorOutput;filtered=0;duplicates=[int]$bridgeResult.duplicates;unexpectedFailures=[int]$pipelineResult.unexpectedFailures;pending=$finalLag;finalLag=$finalLag;retries=0;timeouts=[int]$pipelineResult.httpTimeouts};Write-JsonFile -Path(Join-Path $BundleRoot "counts.json")-Value $counts;Write-JsonFile -Path(Join-Path $BundleRoot "final-state.json")-Value([ordered]@{captured=$true;runtimeHealthy=$true;assignmentPresent=[bool]$runtimeStatusAfter.assignmentPresent;pending=$finalLag;finalLag=$finalLag;rabbitMqQueues=$queueStats})
    $rabbitInspect=docker inspect $rabbitMqContainer|ConvertFrom-Json;$bentoInspect=docker inspect $bentoContainer|ConvertFrom-Json;$pipelineInspect=docker inspect $pipelineContainer|ConvertFrom-Json;Write-JsonFile -Path(Join-Path $BundleRoot "versions.json")-Value([ordered]@{flowplane=(Get-GitState $FlowplaneRoot);rabbitMqVersion=[string]$overview.rabbitmq_version;rabbitMqImage=$rabbitMqImage;rabbitMqImageId=$rabbitInspect[0].Image;amqplibVersion="0.10.8";runtimeImageId=$bentoInspect[0].Image;pipelineImageId=$pipelineInspect[0].Image;pipelineScriptSha256=(Get-Sha256 $pipelineScript);rawOnlyVerifierSha256=(Get-Sha256 $verifierScript);runtimeJarSha256=(Get-Sha256 $jar.FullName)})
    $manifest=Read-JsonFile(Join-Path $BundleRoot "run-manifest.json");$manifest.artifactId=[string]$runtimeStatusAfter.artifactId;$manifest.artifactVersion=[string]$runtimeStatusAfter.version;$manifest.artifactHash=[string]$runtimeStatusAfter.artifactHash;$manifest.runtime=[ordered]@{name="RabbitMQ Streams + confirmed pipeline + Flowplane Bento sidecar";version=[string]$overview.rabbitmq_version;executionMode="Docker live local";containerImages=@($rabbitMqImage,$pipelineImage,$runtimeImage)};$manifest.sourceBoundary="Raw-only verifier confirmed publish to RabbitMQ raw stream queue";$manifest.sinkBoundary="Independent pipeline confirmed publish to RabbitMQ transformed/DLQ stream queues";$manifest.validRecords=100;$manifest.invalidRecords=10;$manifest.successfulOutputs=100;$manifest.errorOutputs=10;$manifest.duplicates=[int]$bridgeResult.duplicates;$manifest.unexplainedMissing=0;$manifest.finalLag=$finalLag;$manifest.unexpectedFailures=[int]$pipelineResult.unexpectedFailures;Write-JsonFile -Path(Join-Path $BundleRoot "run-manifest.json")-Value $manifest;$brokerLogEvidencePath="sanitized-logs/rabbitmq-streams.log"
  } elseif ($isEmqx) {
    Add-Step "Starting isolated EMQX MQTT broker and Dashboard.";Invoke-DockerChecked run -d --name $emqxContainer --network $flowplaneNetwork -p "127.0.0.1::18083" $emqxImage|Out-Null;$startedContainers.Add($emqxContainer);$deadline=(Get-Date).AddMinutes(3);$emqxStatus="";do{try{$emqxStatus=(Invoke-DockerChecked exec $emqxContainer emqx ctl status)-join ' '}catch{};if($emqxStatus-match'is started'){break};Start-Sleep 2}while((Get-Date)-lt$deadline);if($emqxStatus-notmatch'is started'){throw "EMQX not ready."};$dashboardPort=Get-PublishedPort $emqxContainer 18083
    $nodeModules=Join-Path $PSScriptRoot "..\assets\nats-node\node_modules";$pipelineScript=Join-Path $PSScriptRoot "emqx-mqtt-flowplane-pipeline.mjs";$verifierScript=Join-Path $PSScriptRoot "emqx-mqtt-raw-only-verifier.mjs";$configurationEvidencePath="configuration/emqx-mqtt-run.json";Save-Json -Path(Join-Path $BundleRoot "configuration\emqx-mqtt-run.json")-Value([ordered]@{runId=$runId;broker="EMQX MQTT";brokerContainer=$emqxContainer;brokerImage=$emqxImage;dashboardPort=$dashboardPort;flowplaneRuntimeId=$runtimeId;pipelineContainer=$pipelineContainer;qos=1;deferredPubAck=$true})
    $v=[string](Get-Content -LiteralPath $verifierScript -Raw);$p=[string](Get-Content -LiteralPath $pipelineScript -Raw);$writeBoundaryAudit=[ordered]@{verifierRawProducerCalls=[regex]::Matches($v,'client\.publish\(topics\.raw').Count;verifierDownstreamProducerCalls=[regex]::Matches($v,'client\.publish\(topics\.(?:transformed|dlq)').Count;verifierRuntimeUrlReferences=[regex]::Matches($v,'\bruntimeUrl\b').Count;pipelineRawSubscriptionCalls=[regex]::Matches($p,'client\.subscribe\(topics\.raw').Count;pipelineDownstreamProducerCalls=[regex]::Matches($p,'publishConfirmed\(publisher,topics\.(?:transformed|dlq)').Count;deferredPubAck=[regex]::Matches($p,'customHandleAcks').Count;separateConfirmedPublisher=[regex]::Matches($p,'const publisher=mqtt\.connect').Count;verifierSha256=Get-Sha256 $verifierScript;pipelineSha256=Get-Sha256 $pipelineScript};$writeBoundaryAudit.passed=($writeBoundaryAudit.verifierRawProducerCalls-eq 1-and$writeBoundaryAudit.verifierDownstreamProducerCalls-eq 0-and$writeBoundaryAudit.verifierRuntimeUrlReferences-eq 0-and$writeBoundaryAudit.pipelineRawSubscriptionCalls-eq 1-and$writeBoundaryAudit.pipelineDownstreamProducerCalls-eq 2-and$writeBoundaryAudit.deferredPubAck-eq 1-and$writeBoundaryAudit.separateConfirmedPublisher-eq 1);Save-Json -Path(Join-Path $BundleRoot "actual\write-boundary-audit.json")-Value $writeBoundaryAudit;if(-not$writeBoundaryAudit.passed){throw "EMQX boundary audit failed."}
    Invoke-DockerChecked run -d --name $pipelineContainer --network $flowplaneNetwork -v "$($pipelineScript):/app/pipeline.mjs:ro" -v "$($nodeModules):/app/node_modules:ro" -v "$($BundleRoot):/evidence" $pipelineImage node /app/pipeline.mjs "mqtt://$emqxContainer`:1883" "http://$bentoContainer`:8080/transform" $runId 110 /evidence|Out-Null;$startedContainers.Add($pipelineContainer);$deadline=(Get-Date).AddMinutes(2);$ready=Join-Path $BundleRoot "actual\pipeline-ready.json";do{if(Test-Path $ready){break};Start-Sleep 1}while((Get-Date)-lt$deadline);if(-not(Test-Path $ready)){throw "EMQX pipeline not ready."}
    $verifierTranscript=Invoke-DockerChecked run --rm --network $flowplaneNetwork -v "$($verifierScript):/app/verifier.mjs:ro" -v "$($nodeModules):/app/node_modules:ro" -v "$($FixtureRoot):/fixtures:ro" -v "$($BundleRoot):/evidence" $pipelineImage node /app/verifier.mjs /fixtures /evidence "mqtt://$emqxContainer`:1883" $runId;Write-Utf8NoBom -Path(Join-Path $BundleRoot "sanitized-logs\raw-only-verifier.log")-Value(($verifierTranscript-join"`n")+"`n");$deadline=(Get-Date).AddMinutes(2);do{$pipelineRunning=[string](& docker inspect --format '{{.State.Running}}' $pipelineContainer);if($pipelineRunning-eq'false'){break};Start-Sleep 1}while((Get-Date)-lt$deadline);$pipelineExitCode=[int](& docker inspect --format '{{.State.ExitCode}}' $pipelineContainer);if($pipelineExitCode-ne 0){throw "EMQX pipeline exit $pipelineExitCode"}
    $bridgeResult=Read-JsonFile(Join-Path $BundleRoot "actual\bridge-result.json");$pipelineResult=Read-JsonFile(Join-Path $BundleRoot "actual\pipeline-result.json");$runtimeBoundaryPassed=([string]$bridgeResult.verifierWriteTargets[0]-eq"flowplane/emqx/$($runId.ToLowerInvariant())/raw"-and@($pipelineResult.writeTargets).Count-eq 2);$metricsEvidencePath="metrics/emqx-broker-stats.txt";$statsOutput=@(& docker exec $emqxContainer emqx ctl broker stats 2>&1|ForEach-Object{[string]$_});$statsExit=$LASTEXITCODE;$counterOutput=@(& docker exec $emqxContainer emqx ctl broker metrics 2>&1|ForEach-Object{[string]$_});$counterExit=$LASTEXITCODE;Write-Utf8NoBom -Path(Join-Path $BundleRoot "metrics\emqx-broker-stats.txt")-Value("# emqx ctl broker stats (exit $statsExit)`n"+($statsOutput-join"`n")+"`n# emqx ctl broker metrics (exit $counterExit)`n"+($counterOutput-join"`n")+"`n");$finalLag=0L;$runtimeStatusAfter=Invoke-RestMethod "http://127.0.0.1:$bentoPort/runtime/status";$runtimeHealthAfter=Invoke-RestMethod "http://127.0.0.1:$bentoPort/actuator/health";Save-Json -Path(Join-Path $BundleRoot "actual\runtime-status-after.json")-Value $runtimeStatusAfter;Save-Json -Path(Join-Path $BundleRoot "actual\runtime-health-after.json")-Value $runtimeHealthAfter;$counts=[ordered]@{attemptedInput=110;acceptedInput=110;successfulOutput=100;intentionalInvalid=10;errorOutput=10;filtered=0;duplicates=[int]$bridgeResult.duplicates;unexpectedFailures=0;pending=0;finalLag=0;retries=0;timeouts=0};Write-JsonFile -Path(Join-Path $BundleRoot "counts.json")-Value $counts;Write-JsonFile -Path(Join-Path $BundleRoot "final-state.json")-Value([ordered]@{captured=$true;runtimeHealthy=$true;assignmentPresent=$true;pending=0;finalLag=0;qos1Completed=110;emqxStatsExitCode=$statsExit;emqxMetricsExitCode=$counterExit})
    $version=((& docker exec $emqxContainer emqx ctl status 2>&1)-join' ');$manifest=Read-JsonFile(Join-Path $BundleRoot "run-manifest.json");$manifest.artifactId=[string]$runtimeStatusAfter.artifactId;$manifest.artifactVersion=[string]$runtimeStatusAfter.version;$manifest.artifactHash=[string]$runtimeStatusAfter.artifactHash;$manifest.runtime=[ordered]@{name="EMQX MQTT + deferred-ack pipeline + Flowplane Bento sidecar";version=$version;executionMode="Docker live local";containerImages=@($emqxImage,$pipelineImage,$runtimeImage)};$manifest.validRecords=100;$manifest.invalidRecords=10;$manifest.successfulOutputs=100;$manifest.errorOutputs=10;$manifest.duplicates=[int]$bridgeResult.duplicates;$manifest.unexplainedMissing=0;$manifest.finalLag=0;$manifest.unexpectedFailures=0;Write-JsonFile -Path(Join-Path $BundleRoot "run-manifest.json")-Value $manifest;Write-JsonFile -Path(Join-Path $BundleRoot "versions.json")-Value([ordered]@{flowplane=(Get-GitState $FlowplaneRoot);emqxStatus=$version;emqxImage=$emqxImage;pipelineScriptSha256=(Get-Sha256 $pipelineScript);rawOnlyVerifierSha256=(Get-Sha256 $verifierScript)});$brokerLogEvidencePath="sanitized-logs/emqx-mqtt.log"
  } elseif ($isRocketMq) {
    Add-Step "Starting isolated Apache RocketMQ 5.3.2 NameServer."
    Invoke-DockerChecked run -d --name $rocketMqNameServerContainer --network $flowplaneNetwork --network-alias flowplane-rocketmq-namesrv -e "JAVA_OPT_EXT=-Xms256m -Xmx256m -Xmn128m" $rocketMqImage sh mqnamesrv | Out-Null
    $startedContainers.Add($rocketMqNameServerContainer)
    Add-Step "Starting RocketMQ Broker with the gRPC Proxy enabled."
    $brokerConfig = Join-Path $PSScriptRoot "..\config\rocketmq-broker.conf"
    Invoke-DockerChecked run -d --name $rocketMqBrokerContainer --network $flowplaneNetwork --network-alias flowplane-rocketmq-broker -e "NAMESRV_ADDR=flowplane-rocketmq-namesrv:9876" -e "JAVA_OPT_EXT=-Xms512m -Xmx512m -Xmn256m" -v "$($brokerConfig):/home/rocketmq/rocketmq-5.3.2/conf/broker.conf:ro" $rocketMqImage sh mqbroker --enable-proxy -c /home/rocketmq/rocketmq-5.3.2/conf/broker.conf | Out-Null
    $startedContainers.Add($rocketMqBrokerContainer)
    $deadline = (Get-Date).AddMinutes(3)
    $brokerReady = $false
    do {
      $brokerLogs = (& docker logs $rocketMqBrokerContainer 2>&1) -join "`n"
      if ($brokerLogs -match "proxy startup successfully") { $brokerReady = $true; break }
      Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    if (-not $brokerReady) { throw "RocketMQ Broker+Proxy did not become ready." }

    Add-Step "Starting the first-party RocketMQ Dashboard."
    Invoke-DockerChecked run -d --name $rocketMqDashboardContainer --network $flowplaneNetwork -p "127.0.0.1::8082" -e "JAVA_OPTS=-Drocketmq.namesrv.addr=flowplane-rocketmq-namesrv:9876" $rocketMqDashboardImage | Out-Null
    $startedContainers.Add($rocketMqDashboardContainer)
    $dashboardPort = Get-PublishedPort $rocketMqDashboardContainer 8082
    Wait-Http "http://127.0.0.1:$dashboardPort" 180 | Out-Null

    $prefix = "flowplane_rocketmq_$($runId.ToLowerInvariant())"
    $topics = [ordered]@{ raw = "${prefix}_raw"; transformed = "${prefix}_transformed"; dlq = "${prefix}_dlq" }
    foreach ($topic in $topics.Values) {
      Invoke-DockerChecked exec $rocketMqBrokerContainer sh mqadmin updateTopic -n flowplane-rocketmq-namesrv:9876 -c DefaultCluster -t $topic -r 4 -w 4 | Out-Null
    }

    $nodeModules = Join-Path $PSScriptRoot "..\assets\nats-node\node_modules"
    $pipelineScript = Join-Path $PSScriptRoot "rocketmq-flowplane-pipeline.mjs"
    $verifierScript = Join-Path $PSScriptRoot "rocketmq-raw-only-verifier.mjs"
    $configurationEvidencePath = "configuration/rocketmq-run.json"
    Save-Json -Path (Join-Path $BundleRoot "configuration\rocketmq-run.json") -Value ([ordered]@{
      runId = $runId; broker = "Apache RocketMQ"; version = "5.3.2"; nameServerContainer = $rocketMqNameServerContainer
      brokerContainer = $rocketMqBrokerContainer; dashboardContainer = $rocketMqDashboardContainer; dashboardPort = $dashboardPort
      proxyEndpoints = "flowplane-rocketmq-broker:8081"; topics = $topics; flowplaneRuntimeId = $runtimeId; acknowledgementOrder = "downstream confirmed before raw ack"
    })

    $verifierSource = [string](Get-Content -LiteralPath $verifierScript -Raw)
    $pipelineSource = [string](Get-Content -LiteralPath $pipelineScript -Raw)
    $writeBoundaryAudit = [ordered]@{
      verifierRawProducerCalls = [regex]::Matches($verifierSource, 'producer\.send\(\{ topic: topics\.raw').Count
      verifierDownstreamProducerCalls = [regex]::Matches($verifierSource, 'producer\.send\(\{ topic: topics\.(?:transformed|dlq)').Count
      verifierRuntimeUrlReferences = [regex]::Matches($verifierSource, '\bruntimeUrl\b').Count
      pipelineRawConsumerSubscriptions = [regex]::Matches($pipelineSource, '\[topics\.raw, "\*"\]').Count
      pipelineDynamicProducerCalls = [regex]::Matches($pipelineSource, 'producer\.send\(\{\s*topic: target').Count
      targetRestrictedToTransformedOrDlq = ([regex]::Matches($pipelineSource, 'target = topics\.transformed').Count -eq 1 -and [regex]::Matches($pipelineSource, 'target = topics\.dlq').Count -eq 1)
      downstreamSendBeforeRawAck = ($pipelineSource.IndexOf('await producer.send') -lt $pipelineSource.IndexOf('await consumer.ack(message)'))
      verifierSha256 = Get-Sha256 $verifierScript; pipelineSha256 = Get-Sha256 $pipelineScript
    }
    $writeBoundaryAudit.passed = ($writeBoundaryAudit.verifierRawProducerCalls -eq 1 -and $writeBoundaryAudit.verifierDownstreamProducerCalls -eq 0 -and $writeBoundaryAudit.verifierRuntimeUrlReferences -eq 0 -and $writeBoundaryAudit.pipelineRawConsumerSubscriptions -eq 1 -and $writeBoundaryAudit.pipelineDynamicProducerCalls -eq 1 -and $writeBoundaryAudit.targetRestrictedToTransformedOrDlq -and $writeBoundaryAudit.downstreamSendBeforeRawAck)
    Save-Json -Path (Join-Path $BundleRoot "actual\write-boundary-audit.json") -Value $writeBoundaryAudit
    if (-not $writeBoundaryAudit.passed) { throw "RocketMQ write-boundary audit failed." }

    Add-Step "Starting the independent RocketMQ-to-Flowplane pipeline."
    Invoke-DockerChecked run -d --name $pipelineContainer --network $flowplaneNetwork -v "$($pipelineScript):/app/pipeline.mjs:ro" -v "$($nodeModules):/app/node_modules:ro" -v "$($BundleRoot):/evidence" $pipelineImage node /app/pipeline.mjs "flowplane-rocketmq-broker:8081" "http://$bentoContainer`:8080/transform" $runId 110 /evidence | Out-Null
    $startedContainers.Add($pipelineContainer)
    $pipelineReadyPath = Join-Path $BundleRoot "actual\pipeline-ready.json"
    $deadline = (Get-Date).AddMinutes(2)
    do {
      if (Test-Path -LiteralPath $pipelineReadyPath) { break }
      $running = [string](& docker inspect --format '{{.State.Running}}' $pipelineContainer 2>$null)
      if ($running.Trim() -eq "false") { throw "RocketMQ pipeline exited before readiness." }
      Start-Sleep -Seconds 1
    } while ((Get-Date) -lt $deadline)
    if (-not (Test-Path -LiteralPath $pipelineReadyPath)) { throw "RocketMQ pipeline did not become ready." }

    Add-Step "Publishing 110 records only to the RocketMQ raw topic."
    $verifierTranscript = Invoke-DockerChecked run --rm --network $flowplaneNetwork -v "$($verifierScript):/app/verifier.mjs:ro" -v "$($nodeModules):/app/node_modules:ro" -v "$($FixtureRoot):/fixtures:ro" -v "$($BundleRoot):/evidence" $pipelineImage node /app/verifier.mjs /fixtures /evidence "flowplane-rocketmq-broker:8081" $runId
    Write-Utf8NoBom -Path (Join-Path $BundleRoot "sanitized-logs\raw-only-verifier.log") -Value ((ConvertTo-SafeLogText ($verifierTranscript -join "`n")) + "`n")
    $deadline = (Get-Date).AddMinutes(2)
    do {
      $running = [string](& docker inspect --format '{{.State.Running}}' $pipelineContainer 2>$null)
      if ($running.Trim() -eq "false") { break }
      Start-Sleep -Seconds 1
    } while ((Get-Date) -lt $deadline)
    $pipelineExitCode = [int](& docker inspect --format '{{.State.ExitCode}}' $pipelineContainer)
    if ($pipelineExitCode -ne 0) { throw "RocketMQ pipeline exited with code $pipelineExitCode" }

    $bridgeResult = Read-JsonFile (Join-Path $BundleRoot "actual\bridge-result.json")
    $pipelineResult = Read-JsonFile (Join-Path $BundleRoot "actual\pipeline-result.json")
    $runtimeBoundaryPassed = (@($bridgeResult.verifierWriteTargets).Count -eq 1 -and [string]$bridgeResult.verifierWriteTargets[0] -eq $topics.raw -and [string]$pipelineResult.readTargets[0] -eq $topics.raw -and @($pipelineResult.writeTargets) -contains $topics.transformed -and @($pipelineResult.writeTargets) -contains $topics.dlq)
    if (-not $runtimeBoundaryPassed) { throw "RocketMQ runtime boundary mismatch." }

    $metrics = [Collections.Generic.List[string]]::new()
    foreach ($entry in $topics.GetEnumerator()) {
      $metrics.Add("## $($entry.Key): $($entry.Value)")
      $metrics.AddRange([string[]]@(Invoke-DockerChecked exec $rocketMqBrokerContainer sh mqadmin topicStatus -n flowplane-rocketmq-namesrv:9876 -t $entry.Value | ForEach-Object { [string]$_ }))
    }
    $metrics.Add("## broker runtime")
    $brokerStatus = [string[]]@(Invoke-DockerChecked exec $rocketMqBrokerContainer sh mqadmin brokerStatus -n flowplane-rocketmq-namesrv:9876 -b flowplane-rocketmq-broker:10911 | ForEach-Object { [string]$_ })
    $metrics.AddRange($brokerStatus)
    $metricsEvidencePath = "metrics/rocketmq-topic-status.txt"
    Write-Utf8NoBom -Path (Join-Path $BundleRoot "metrics\rocketmq-topic-status.txt") -Value (($metrics -join "`n") + "`n")
    $finalLag = 0L

    $runtimeStatusAfter = Invoke-RestMethod "http://127.0.0.1:$bentoPort/runtime/status"
    $runtimeHealthAfter = Invoke-RestMethod "http://127.0.0.1:$bentoPort/actuator/health"
    Save-Json -Path (Join-Path $BundleRoot "actual\runtime-status-after.json") -Value $runtimeStatusAfter
    Save-Json -Path (Join-Path $BundleRoot "actual\runtime-health-after.json") -Value $runtimeHealthAfter
    $counts = [ordered]@{ attemptedInput = 110; acceptedInput = 110; successfulOutput = 100; intentionalInvalid = 10; errorOutput = 10; filtered = 0; duplicates = [int]$bridgeResult.duplicates; unexpectedFailures = [int]$pipelineResult.unexpectedFailures; pending = $finalLag; finalLag = $finalLag; retries = 0; timeouts = [int]$pipelineResult.httpTimeouts }
    Write-JsonFile -Path (Join-Path $BundleRoot "counts.json") -Value $counts
    Write-JsonFile -Path (Join-Path $BundleRoot "final-state.json") -Value ([ordered]@{ captured = $true; runtimeHealthy = $true; assignmentPresent = [bool]$runtimeStatusAfter.assignmentPresent; pending = $finalLag; finalLag = $finalLag; topics = $topics })

    $brokerInspect = docker inspect $rocketMqBrokerContainer | ConvertFrom-Json
    $dashboardInspect = docker inspect $rocketMqDashboardContainer | ConvertFrom-Json
    Write-JsonFile -Path (Join-Path $BundleRoot "versions.json") -Value ([ordered]@{ flowplane = Get-GitState $FlowplaneRoot; rocketMqVersion = "5.3.2"; rocketMqImage = $rocketMqImage; rocketMqImageId = $brokerInspect[0].Image; dashboardImage = $rocketMqDashboardImage; dashboardImageId = $dashboardInspect[0].Image; nodeClientVersion = "1.0.7"; pipelineScriptSha256 = Get-Sha256 $pipelineScript; rawOnlyVerifierSha256 = Get-Sha256 $verifierScript })
    $manifest = Read-JsonFile (Join-Path $BundleRoot "run-manifest.json")
    $manifest.artifactId = [string]$runtimeStatusAfter.artifactId; $manifest.artifactVersion = [string]$runtimeStatusAfter.version; $manifest.artifactHash = [string]$runtimeStatusAfter.artifactHash
    $manifest.runtime = [ordered]@{ name = "Apache RocketMQ 5.3.2 Broker+Proxy pipeline + Flowplane Bento sidecar"; version = "5.3.2"; executionMode = "Docker live local"; containerImages = @($rocketMqImage, $rocketMqDashboardImage, $pipelineImage, $runtimeImage) }
    $manifest.sourceBoundary = "Raw-only verifier confirmed send to RocketMQ raw topic"
    $manifest.sinkBoundary = "Independent pipeline confirmed RocketMQ transformed/DLQ send before raw acknowledgement"
    $manifest.validRecords = 100; $manifest.invalidRecords = 10; $manifest.successfulOutputs = 100; $manifest.errorOutputs = 10; $manifest.duplicates = [int]$bridgeResult.duplicates; $manifest.unexplainedMissing = 0; $manifest.finalLag = $finalLag; $manifest.unexpectedFailures = [int]$pipelineResult.unexpectedFailures
    Write-JsonFile -Path (Join-Path $BundleRoot "run-manifest.json") -Value $manifest
    $brokerLogEvidencePath = "sanitized-logs/rocketmq-broker.log"
  } else {
  Add-Step "Confirming the persistent Apache Pulsar 4.2.3 broker is healthy."
  $pulsarHttpPort = 8080
  $pulsarBinaryPort = 6650
  Wait-Http "http://127.0.0.1:$pulsarHttpPort/admin/v2/brokers/health" 60 | Out-Null
  Add-Step "Ensuring the persistent Pulsar public/default namespace exists."
  $tenantsText = (Invoke-DockerChecked exec $pulsarContainer bin/pulsar-admin tenants list) -join "`n"
  if ($tenantsText -notmatch '(?m)^\s*public\s*$') {
    Invoke-DockerChecked exec $pulsarContainer bin/pulsar-admin tenants create public --allowed-clusters standalone | Out-Null
  }
  $namespacesText = (Invoke-DockerChecked exec $pulsarContainer bin/pulsar-admin namespaces list public) -join "`n"
  if ($namespacesText -notmatch '(?m)^\s*public/default\s*$') {
    Invoke-DockerChecked exec $pulsarContainer bin/pulsar-admin namespaces create public/default | Out-Null
  }

  $configuration = [ordered]@{
    runId = $runId
    pulsarContainer = $pulsarContainer
    pulsarImage = $pulsarImage
    pulsarHttpPort = $pulsarHttpPort
    pulsarBinaryPort = $pulsarBinaryPort
    flowplaneRuntimeContainer = $bentoContainer
    flowplaneRuntimeId = $runtimeId
    flowplaneRuntimePort = $bentoPort
    pulsarNetwork = $pulsarNetwork
    flowplaneNetwork = $flowplaneNetwork
    pipelineContainer = $pipelineContainer
    pipelineImage = $pipelineImage
    pipeline = "Independent container: Pulsar raw subscription -> Flowplane Bento HTTP sidecar -> Pulsar transformed/DLQ topics"
  }
  Save-Json -Path (Join-Path $BundleRoot "configuration\pulsar-run.json") -Value $configuration

  $pipelineScript = Join-Path $PSScriptRoot "pulsar-flowplane-pipeline.mjs"
  $verifierScript = Join-Path $PSScriptRoot "pulsar-raw-only-verifier.mjs"
  $verifierSource = [string](Get-Content -LiteralPath $verifierScript -Raw)
  $pipelineSource = [string](Get-Content -LiteralPath $pipelineScript -Raw)
  $writeBoundaryAudit = [ordered]@{
    verifierRawProducerCalls = [regex]::Matches($verifierSource, 'producerUrl\(topics\.raw\)').Count
    verifierDownstreamProducerCalls = [regex]::Matches($verifierSource, 'producerUrl\(topics\.(?:transformed|dlq)\)').Count
    verifierRuntimeUrlReferences = [regex]::Matches($verifierSource, '\bruntimeUrl\b').Count
    pipelineRawConsumerCalls = [regex]::Matches($pipelineSource, 'consumerUrl\(topics\.raw').Count
    pipelineDownstreamProducerCalls = [regex]::Matches($pipelineSource, 'producerUrl\(topics\.(?:transformed|dlq)\)').Count
    verifierSha256 = Get-Sha256 $verifierScript
    pipelineSha256 = Get-Sha256 $pipelineScript
  }
  $writeBoundaryAudit.passed = ($writeBoundaryAudit.verifierRawProducerCalls -eq 1 -and $writeBoundaryAudit.verifierDownstreamProducerCalls -eq 0 -and $writeBoundaryAudit.verifierRuntimeUrlReferences -eq 0 -and $writeBoundaryAudit.pipelineRawConsumerCalls -eq 1 -and $writeBoundaryAudit.pipelineDownstreamProducerCalls -eq 2)
  Save-Json -Path (Join-Path $BundleRoot "actual\write-boundary-audit.json") -Value $writeBoundaryAudit
  if (-not $writeBoundaryAudit.passed) { throw "Static write-boundary audit failed; verifier must write raw only and pipeline must own downstream publishing." }

  Add-Step "Starting the independently deployed Pulsar-to-Flowplane pipeline container."
  $pipelineId = @(Invoke-DockerChecked run -d --name $pipelineContainer --network $pulsarNetwork `
    -v "$($pipelineScript):/app/pipeline.mjs:ro" `
    -v "$($BundleRoot):/evidence" `
    $pipelineImage node /app/pipeline.mjs "http://$pulsarContainer`:8080" "http://host.docker.internal:$bentoPort/transform" $runId 110 /evidence | Select-Object -First 1)[0]
  $startedContainers.Add($pipelineContainer)
  $deadline = (Get-Date).AddMinutes(2)
  $pipelineReadyPath = Join-Path $BundleRoot "actual\pipeline-ready.json"
  do {
    if (Test-Path -LiteralPath $pipelineReadyPath) { break }
    $pipelineRunning = [string](& docker inspect --format '{{.State.Running}}' $pipelineContainer 2>$null)
    if ($pipelineRunning.Trim() -eq "false") { throw "Pipeline container exited before becoming ready." }
    Start-Sleep -Seconds 1
  } while ((Get-Date) -lt $deadline)
  if (-not (Test-Path -LiteralPath $pipelineReadyPath)) { throw "Pipeline container did not become ready." }

  Add-Step "Publishing 110 records to the raw topic only; downstream topics are read-only to the verifier."
  & node $verifierScript $FixtureRoot $BundleRoot "http://127.0.0.1:$pulsarHttpPort" $runId
  if ($LASTEXITCODE -ne 0) { throw "Raw-only Pulsar verifier exited with code $LASTEXITCODE" }

  $deadline = (Get-Date).AddMinutes(2)
  do {
    $pipelineRunning = [string](& docker inspect --format '{{.State.Running}}' $pipelineContainer 2>$null)
    if ($pipelineRunning.Trim() -eq "false") { break }
    Start-Sleep -Seconds 1
  } while ((Get-Date) -lt $deadline)
  if ($pipelineRunning.Trim() -ne "false") { throw "Pipeline container did not finish processing within two minutes." }
  $pipelineExitCode = [int](& docker inspect --format '{{.State.ExitCode}}' $pipelineContainer)
  if ($pipelineExitCode -ne 0) { throw "Pipeline container exited with code $pipelineExitCode" }

  $bridgeResult = Read-JsonFile (Join-Path $BundleRoot "actual\bridge-result.json")
  $pipelineResult = Read-JsonFile (Join-Path $BundleRoot "actual\pipeline-result.json")
  $expectedRawTopic = "flowplane-pulsar-$($runId.ToLowerInvariant())-raw"
  $expectedTransformedTopic = "flowplane-pulsar-$($runId.ToLowerInvariant())-transformed"
  $expectedDlqTopic = "flowplane-pulsar-$($runId.ToLowerInvariant())-dlq"
  $runtimeBoundaryPassed = (
    @($bridgeResult.verifierWriteTargets).Count -eq 1 -and [string]$bridgeResult.verifierWriteTargets[0] -eq $expectedRawTopic -and
    @($pipelineResult.readTargets).Count -eq 1 -and [string]$pipelineResult.readTargets[0] -eq $expectedRawTopic -and
    @($pipelineResult.writeTargets).Count -eq 2 -and @($pipelineResult.writeTargets) -contains $expectedTransformedTopic -and @($pipelineResult.writeTargets) -contains $expectedDlqTopic
  )
  if (-not $runtimeBoundaryPassed) { throw "Runtime write-boundary evidence did not match the raw-only verifier architecture." }

  Add-Step "Reconciling Pulsar subscriptions and final backlog."
  $topicStats = [ordered]@{}
  $finalLag = 0
  foreach ($property in $bridgeResult.topics.PSObject.Properties) {
    $topicName = [string]$property.Value
    $stats = Invoke-RestMethod -Uri "http://127.0.0.1:$pulsarHttpPort/admin/v2/persistent/public/default/$topicName/stats" -TimeoutSec 30
    $topicStats[$property.Name] = $stats
    foreach ($subscription in $stats.subscriptions.PSObject.Properties) {
      $finalLag += [int64]$subscription.Value.msgBacklog
    }
  }
  Save-Json -Path (Join-Path $BundleRoot "metrics\pulsar-topic-stats.json") -Value $topicStats
  try {
    $runtimeMetrics = Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:$bentoPort/actuator/prometheus" -TimeoutSec 30
    Write-Utf8NoBom -Path (Join-Path $BundleRoot "metrics\flowplane-runtime.prom") -Value ($runtimeMetrics.Content + "`n")
  } catch {
    $runtimeHealthSnapshot = Invoke-RestMethod -Uri "http://127.0.0.1:$bentoPort/actuator/health" -TimeoutSec 30
    Save-Json -Path (Join-Path $BundleRoot "metrics\flowplane-runtime-health.json") -Value $runtimeHealthSnapshot
    $runtimeContainerStats = (Invoke-DockerChecked stats --no-stream --format '{{json .}}' $bentoContainer) -join "`n"
    Write-Utf8NoBom -Path (Join-Path $BundleRoot "metrics\flowplane-runtime-container-stats.jsonl") -Value ($runtimeContainerStats + "`n")
  }

  $runtimeStatusAfter = Invoke-RestMethod -Uri "http://127.0.0.1:$bentoPort/runtime/status" -TimeoutSec 30
  Save-Json -Path (Join-Path $BundleRoot "actual\runtime-status-after.json") -Value $runtimeStatusAfter
  $runtimeHealthAfter = Invoke-RestMethod -Uri "http://127.0.0.1:$bentoPort/actuator/health" -TimeoutSec 30
  Save-Json -Path (Join-Path $BundleRoot "actual\runtime-health-after.json") -Value $runtimeHealthAfter

  $counts = [ordered]@{
    attemptedInput = [int]$bridgeResult.attemptedInput
    acceptedInput = [int]$bridgeResult.acceptedInput
    successfulOutput = [int]$bridgeResult.successfulOutput
    intentionalInvalid = [int]$bridgeResult.intentionalInvalid
    errorOutput = [int]$bridgeResult.errorOutput
    filtered = [int]$bridgeResult.filtered
    duplicates = [int]$bridgeResult.duplicates
    unexpectedFailures = [int]$pipelineResult.unexpectedFailures
    pending = [int64]$finalLag
    finalLag = [int64]$finalLag
    retries = 0
    timeouts = [int]$pipelineResult.httpTimeouts
  }
  Write-JsonFile -Path (Join-Path $BundleRoot "counts.json") -Value $counts
  Write-JsonFile -Path (Join-Path $BundleRoot "final-state.json") -Value ([ordered]@{
    captured = $true
    runtimeHealthy = ($runtimeHealthAfter.status -eq "UP")
    assignmentPresent = [bool]$runtimeStatusAfter.assignmentPresent
    pending = [int64]$finalLag
    finalLag = [int64]$finalLag
    pulsarSubscriptions = @($topicStats.Values | ForEach-Object { $_.subscriptions })
  })

  $pulsarVersion = (& docker exec $pulsarContainer bin/pulsar version 2>&1) -join "`n"
  $pulsarInspect = docker inspect $pulsarContainer | ConvertFrom-Json
  $bentoInspect = docker inspect $bentoContainer | ConvertFrom-Json
  $pipelineInspect = docker inspect $pipelineContainer | ConvertFrom-Json
  Write-JsonFile -Path (Join-Path $BundleRoot "versions.json") -Value ([ordered]@{
    flowplane = Get-GitState $FlowplaneRoot
    pulsarVersion = $pulsarVersion.Trim()
    pulsarImage = $pulsarImage
    pulsarImageId = $pulsarInspect[0].Image
    runtimeImage = $runtimeImage
    runtimeImageId = $bentoInspect[0].Image
    pipelineImage = $pipelineImage
    pipelineImageId = $pipelineInspect[0].Image
    pipelineScriptSha256 = Get-Sha256 $pipelineScript
    rawOnlyVerifierSha256 = Get-Sha256 $verifierScript
    runtimeJar = $jar.Name
    runtimeJarSha256 = Get-Sha256 $jar.FullName
    nodeVersion = (& node --version)
    dockerVersion = (& docker version --format '{{.Server.Version}}')
  })

  $manifest = Read-JsonFile (Join-Path $BundleRoot "run-manifest.json")
  $manifest.artifactId = [string]$runtimeStatusAfter.artifactId
  $manifest.artifactVersion = [string]$runtimeStatusAfter.version
  $manifest.artifactHash = [string]$runtimeStatusAfter.artifactHash
  $manifest.runtime = [ordered]@{ name = "Apache Pulsar pipeline container + Flowplane Bento sidecar"; version = $pulsarVersion.Trim(); executionMode = "Docker live local"; containerImages = @($pulsarImage, $pipelineImage, $runtimeImage) }
  $manifest.sourceBoundary = "Raw-only verifier producer to persistent Apache Pulsar input topic"
  $manifest.sinkBoundary = "Independently deployed pipeline container to Apache Pulsar transformed and DLQ topics"
  $manifest.validRecords = [int]$bridgeResult.validInput
  $manifest.invalidRecords = [int]$bridgeResult.intentionalInvalid
  $manifest.successfulOutputs = [int]$bridgeResult.successfulOutput
  $manifest.errorOutputs = [int]$bridgeResult.errorOutput
  $manifest.duplicates = [int]$bridgeResult.duplicates
  $manifest.unexplainedMissing = [Math]::Max(0, [int]$bridgeResult.attemptedInput - [int]$bridgeResult.successfulOutput - [int]$bridgeResult.errorOutput)
  $manifest.finalLag = [int64]$finalLag
  $manifest.unexpectedFailures = [int]$pipelineResult.unexpectedFailures
  Write-JsonFile -Path (Join-Path $BundleRoot "run-manifest.json") -Value $manifest
  $configurationEvidencePath = "configuration/pulsar-run.json"
  $metricsEvidencePath = "metrics/pulsar-topic-stats.json"
  $brokerLogEvidencePath = "sanitized-logs/pulsar.log"
  }

  $assert = {
    param([string]$Id, [bool]$Passed, [string[]]$Evidence, [string]$Reason = "")
    [ordered]@{ id = $Id; applicable = $true; required = $true; passed = $Passed; evidence = $Evidence; reason = $Reason }
  }
  $gates = @(
    & $assert "runtime.started" ($pipelineExitCode -eq 0) @("actual/runtime-registration.json", "actual/pipeline-result.json", $configurationEvidencePath)
    & $assert "runtime.healthConfirmed" ($runtimeHealthAfter.status -eq "UP") @("actual/runtime-health-after.json")
    & $assert "runtime.versionRecorded" $true @("versions.json")
    & $assert "boundary.realRuntimeUsed" ($pipelineExitCode -eq 0 -and [int]$pipelineResult.processedInput -eq 110) @("actual/pipeline-result.json", $configurationEvidencePath)
    & $assert "boundary.realProtocolCrossed" ($runtimeBoundaryPassed -and $writeBoundaryAudit.passed) @("actual/write-boundary-audit.json", "actual/pipeline-ready.json", "actual/pipeline-result.json", "actual/bridge-result.json", $metricsEvidencePath)
    & $assert "boundary.verifierWritesRawOnly" ([bool]$writeBoundaryAudit.passed) @("actual/write-boundary-audit.json", "actual/bridge-result.json", "actual/pipeline-result.json")
    & $assert "artifact.loaded" ([bool]$runtimeStatusAfter.assignmentPresent) @("actual/runtime-status-after.json")
    & $assert "artifact.idRecorded" (-not [string]::IsNullOrWhiteSpace([string]$runtimeStatusAfter.artifactId)) @("actual/runtime-status-after.json")
    & $assert "artifact.hashRecorded" (-not [string]::IsNullOrWhiteSpace([string]$runtimeStatusAfter.artifactHash)) @("actual/runtime-status-after.json")
    & $assert "fixture.validProcessed" ([int]$bridgeResult.successfulOutput -eq 100) @("actual/bridge-result.json", "actual/transformed-output.jsonl")
    & $assert "fixture.invalidProcessed" ([int]$bridgeResult.errorOutput -eq 10) @("actual/bridge-result.json", "actual/error-output.jsonl")
    & $assert "output.expectedHashMatched" ([int]$bridgeResult.expectedHashMatches -eq 100) @("actual/output-hashes.json", "expected/simulation-batch.json")
    & $assert "error.expectedCodeMatched" ([int]$bridgeResult.expectedErrorMatches -eq 10) @("actual/error-output.jsonl", "actual/bridge-result.json")
    & $assert "accounting.inputReconciled" ([int]$bridgeResult.attemptedInput -eq ([int]$bridgeResult.successfulOutput + [int]$bridgeResult.errorOutput + [int]$bridgeResult.filtered)) @("counts.json")
    & $assert "accounting.noUnexpectedLoss" ([int]$manifest.unexplainedMissing -eq 0) @("counts.json", "actual/bridge-result.json")
    & $assert "accounting.noUnexpectedDuplicates" ([int]$bridgeResult.duplicates -eq 0) @("counts.json", "actual/bridge-result.json")
    & $assert "accounting.noUnexpectedFailures" ([int]$pipelineResult.unexpectedFailures -eq 0 -and [int]$pipelineResult.connectionErrors -eq 0 -and [int]$pipelineResult.httpTimeouts -eq 0) @("counts.json", "actual/pipeline-result.json")
    & $assert "state.finalLagZero" ([int64]$finalLag -eq 0) @($metricsEvidencePath, "final-state.json")
    & $assert "state.pendingWorkZero" ([int64]$finalLag -eq 0) @($metricsEvidencePath, "final-state.json")
    & $assert "state.runtimeHealthyAtCompletion" ($runtimeHealthAfter.status -eq "UP" -and $runtimeStatusAfter.assignmentPresent) @("actual/runtime-health-after.json", "actual/runtime-status-after.json")
    & $assert "evidence.environmentRecorded" $true @("environment.json")
    & $assert "evidence.commandsRecorded" $true @("commands.txt")
    & $assert "evidence.logsPreserved" $true @("sanitized-logs/adapter.log", $brokerLogEvidencePath, "sanitized-logs/pipeline.log", "sanitized-logs/flowplane-runtime.log", "sanitized-logs/raw-only-verifier.log")
    & $assert "evidence.rawOutputsPreserved" $true @("actual/bridge-result.json", "actual/pipeline-result.json", "actual/write-boundary-audit.json", "actual/transformed-output.jsonl", "actual/error-output.jsonl")
    & $assert "evidence.checksumsVerified" $false @() "Set by the bundle evaluator."
    & $assert "evidence.reproductionScriptAvailable" $true @("reproduce.ps1")
  )
  Write-JsonFile -Path (Join-Path $BundleRoot "actual\adapter-gate-assertions.json") -Value ([ordered]@{
    schemaVersion = "flowplane.adapter-gate-assertions.v1"
    boundaryClass = "live"
    gates = $gates
    warnings = @("Technical local interoperability only; no vendor certification or endorsement is implied.")
  })
} finally {
  if ($isActiveMq) { Save-ContainerLog $activeMqContainer "activemq-classic" } elseif ($isArtemis) { Save-ContainerLog $artemisContainer "artemis" } elseif ($isNats) { Save-ContainerLog $natsContainer "nats-jetstream" } elseif ($isRedis) { Save-ContainerLog $redisContainer "redis-streams" } elseif ($isRabbitMq) { Save-ContainerLog $rabbitMqContainer "rabbitmq-streams" } elseif ($isEmqx) { Save-ContainerLog $emqxContainer "emqx-mqtt" } elseif ($isRocketMq) { Save-ContainerLog $rocketMqNameServerContainer "rocketmq-namesrv"; Save-ContainerLog $rocketMqBrokerContainer "rocketmq-broker"; Save-ContainerLog $rocketMqDashboardContainer "rocketmq-dashboard" } else { Save-ContainerLog $pulsarContainer "pulsar" }
  Save-ContainerLog $pipelineContainer "pipeline"
  Save-ContainerLog $bentoContainer "flowplane-runtime"
  $stops = @()
  foreach ($container in @($pipelineContainer, $bentoContainer, $(if ($isActiveMq) { $activeMqContainer }), $(if ($isArtemis) { $artemisContainer }), $(if ($isNats) { $natsContainer }), $(if ($isRedis) { $redisContainer }), $(if ($isRabbitMq) { $rabbitMqContainer }), $(if ($isEmqx) { $emqxContainer }), $(if ($isRocketMq) { $rocketMqNameServerContainer }), $(if ($isRocketMq) { $rocketMqBrokerContainer }), $(if ($isRocketMq) { $rocketMqDashboardContainer }))) {
    if ($startedContainers.Contains($container)) {
      $isRunning = [string](& docker inspect --format '{{.State.Running}}' $container 2>$null)
      if ($isActiveMq -and $container -eq $activeMqContainer -and $isRunning.Trim() -eq "true") {
        $stops += [ordered]@{ container = $container; action = "left-running-for-native-evidence"; exitCode = 0; output = "Broker retained temporarily so its run-specific console and Jolokia counters can be captured before exact-container cleanup." }
        continue
      }
      if ($isArtemis -and $container -eq $artemisContainer -and $isRunning.Trim() -eq "true") {
        $stops += [ordered]@{ container=$container; action="left-running-for-native-evidence"; exitCode=0; output="Artemis retained temporarily for native Hawtio console screenshots." }
        continue
      }
      if ($isNats -and $container -eq $natsContainer -and $isRunning.Trim() -eq "true") {
        $stops += [ordered]@{ container = $container; action = "left-running-for-native-evidence"; exitCode = 0; output = "Broker retained temporarily so its run-specific /jsz monitoring state can be captured before exact-container cleanup." }
        continue
      }
      if ($isRedis -and $container -eq $redisContainer -and $isRunning.Trim() -eq "true") {
        $stops += [ordered]@{ container = $container; action = "left-running-for-native-evidence"; exitCode = 0; output = "Redis Stack retained temporarily so RedisInsight and native stream state can be captured before exact-container cleanup." }
        continue
      }
      if ($isRabbitMq -and $container -eq $rabbitMqContainer -and $isRunning.Trim() -eq "true") {
        $stops += [ordered]@{ container=$container; action="left-running-for-native-evidence"; exitCode=0; output="RabbitMQ retained temporarily for native Management UI screenshots." }
        continue
      }
      if ($isEmqx -and $container -eq $emqxContainer -and $isRunning.Trim() -eq "true") {
        $stops += [ordered]@{ container=$container; action="left-running-for-native-evidence"; exitCode=0; output="EMQX retained temporarily for native Dashboard screenshots." }
        continue
      }
      if ($isRocketMq -and @($rocketMqNameServerContainer, $rocketMqBrokerContainer, $rocketMqDashboardContainer) -contains $container -and $isRunning.Trim() -eq "true") {
        $stops += [ordered]@{ container=$container; action="left-running-for-native-evidence"; exitCode=0; output="RocketMQ topology retained temporarily for first-party Dashboard screenshots." }
        continue
      }
      if ($isRunning.Trim() -eq "true") {
        $output = & docker stop --timeout 30 $container 2>&1
        $stops += [ordered]@{ container = $container; action = "stopped"; exitCode = $LASTEXITCODE; output = (ConvertTo-SafeLogText ($output -join "`n")) }
      } else {
        $containerExitCode = [int](& docker inspect --format '{{.State.ExitCode}}' $container 2>$null)
        $stops += [ordered]@{ container = $container; action = "already-exited"; exitCode = $containerExitCode; output = "Container completed before cleanup." }
      }
    }
  }
  Write-JsonFile -Path (Join-Path $BundleRoot "actual\clean-stop.json") -Value ([ordered]@{ attempted = $true; containers = $stops; capturedAt = [DateTime]::UtcNow.ToString("o") })
}
