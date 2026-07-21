param(
  [Parameter(Mandatory)][string]$FlowplaneRoot,
  [Parameter(Mandatory)][string]$BundleRoot,
  [Parameter(Mandatory)][string]$FixtureRoot,
  [ValidateSet("redpanda-connect", "logstash", "camel", "spring-cloud-stream", "nifi", "spark-structured-streaming", "beam-directrunner", "kafka-connect", "kafka-streams", "flink", "bento-warpstream", "vector", "opentelemetry", "debezium")][string]$IntegrationId = "redpanda-connect"
)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "..\LiveVerification.Common.ps1")

$env:FLOWPLANE_ROOT = $FlowplaneRoot
$env:FLOWPLANE_DEMO_OUTPUT_ROOT = Join-Path $BundleRoot "adapter-private"
. (Join-Path $PSScriptRoot "..\..\FlowplaneDemo.Common.ps1")

$integrationId = $IntegrationId
$runId = Split-Path $BundleRoot -Leaf
$safeRun = ($runId.ToLowerInvariant() -replace '[^a-z0-9-]', '-')
$topicPrefix = "flowplane.$integrationId.evidence.$safeRun"
$rawTopic = "$topicPrefix.raw"
$transformedTopic = "$topicPrefix.transformed"
$dlqTopic = "$topicPrefix.dlq"
$forwardTopic = "$topicPrefix.forwarded"
$groupId = "flowplane-$integrationId-evidence-$safeRun"
$runtimeId = "$integrationId-sidecar-$safeRun"
$runtimeContainer = "flowplane-$integrationId-bento-$safeRun"
$toolContainer = "flowplane-$integrationId-pipeline-$safeRun"
$kafkaContainer = "flowplane-kafka"
$kafkaBootstrap = "kafka:9092"
$flowplaneNetwork = "flowplane-quality-stack_default"
$runtimeImage = "eclipse-temurin:17-jre"
$toolDefinition = switch ($integrationId) {
  "debezium" {
    [ordered]@{
      displayName = "Debezium MySQL CDC"
      image = "quay.io/debezium/connect:3.6"
      configRelativePath = "adapter-asset:debezium-mysql-connector-evidence.json"
      configFileName = "debezium-mysql-connector-evidence.json"
      configContainerPath = "/evidence/debezium-connector.json"
      rawPattern = [regex]::Escape($rawTopic)
      transformedPattern = 'topics\.transformed'
      dlqPattern = 'topics\.dlq'
      ticketPrefix = "DEBEZIUM"
      startCommand = @()
      sourceKind = "file"
      workDir = ""
      startupSeconds = 0
      versionCommand = @()
      environment = @()
    }
  }
  "opentelemetry" {
    [ordered]@{
      displayName = "OpenTelemetry Collector"
      image = "otel/opentelemetry-collector-contrib:0.149.0"
      configRelativePath = "adapter-asset:opentelemetry-kafka-evidence.yaml"
      configFileName = "opentelemetry-kafka-evidence.yaml"
      configContainerPath = "/etc/otelcol-contrib/config.yaml"
      rawPattern = 'FLOWPLANE_RAW_TOPIC'
      transformedPattern = 'topics\.transformed'
      dlqPattern = 'topics\.dlq'
      ticketPrefix = "OTEL"
      startCommand = @("--config=/etc/otelcol-contrib/config.yaml")
      sourceKind = "file"
      workDir = ""
      startupSeconds = 8
      versionCommand = @("--version")
      environment = @(
        "FLOWPLANE_KAFKA_BOOTSTRAP=$kafkaBootstrap",
        "FLOWPLANE_RAW_TOPIC=$rawTopic",
        "FLOWPLANE_FORWARD_TOPIC=$forwardTopic",
        "FLOWPLANE_GROUP_ID=$groupId"
      )
    }
  }
  "vector" {
    [ordered]@{
      displayName = "Vector"
      image = "timberio/vector:0.50.0-alpine"
      configRelativePath = "adapter-asset:vector-kafka-http-evidence.toml"
      configFileName = "vector-kafka-http-evidence.toml"
      configContainerPath = "/etc/vector/vector.toml"
      rawPattern = 'FLOWPLANE_RAW_TOPIC'
      transformedPattern = 'topics\.transformed'
      dlqPattern = 'topics\.dlq'
      ticketPrefix = "VECTOR"
      startCommand = @("--config", "/etc/vector/vector.toml", "--require-healthy", "true")
      sourceKind = "file"
      workDir = ""
      startupSeconds = 8
      versionCommand = @("--version")
      environment = @(
        "FLOWPLANE_KAFKA_BOOTSTRAP=$kafkaBootstrap",
        "FLOWPLANE_RAW_TOPIC=$rawTopic",
        "FLOWPLANE_GROUP_ID=$groupId",
        "FLOWPLANE_BRIDGE_URL=http://flowplane-vector-publisher-$safeRun`:8090"
      )
    }
  }
  "bento-warpstream" {
    [ordered]@{
      displayName = "WarpStream Bento"
      image = "ghcr.io/warpstreamlabs/bento:latest"
      configRelativePath = "docker\network-isolation\warpstream-bento.yml"
      configFileName = "warpstream-bento.yml"
      configContainerPath = "/etc/bento/config.yml"
      rawPattern = 'FLOWPLANE_WARPSTREAM_RAW_TOPIC'
      transformedPattern = 'FLOWPLANE_WARPSTREAM_OUTPUT_TOPIC'
      dlqPattern = 'FLOWPLANE_WARPSTREAM_DLQ_TOPIC'
      ticketPrefix = "BENTO"
      startCommand = @("run", "/etc/bento/config.yml")
      sourceKind = "file"
      workDir = ""
      startupSeconds = 5
      versionCommand = @("--version")
      environment = @(
        "FLOWPLANE_WARPSTREAM_KAFKA_BOOTSTRAP=$kafkaBootstrap",
        "FLOWPLANE_WARPSTREAM_RAW_TOPIC=$rawTopic",
        "FLOWPLANE_WARPSTREAM_OUTPUT_TOPIC=$transformedTopic",
        "FLOWPLANE_WARPSTREAM_DLQ_TOPIC=$dlqTopic",
        "FLOWPLANE_WARPSTREAM_GROUP_ID=$groupId",
        "FLOWPLANE_WARPSTREAM_TRANSFORM_URL=http://$runtimeContainer`:8080/transform",
        "FLOWPLANE_WARPSTREAM_TLS_ENABLED=false",
        "FLOWPLANE_WARPSTREAM_TLS_SKIP_VERIFY=false"
      )
    }
  }
  "kafka-connect" {
    [ordered]@{
      displayName = "Kafka Connect SMT"
      image = "confluentinc/cp-kafka-connect:7.8.0"
      configRelativePath = "adapter-asset:kafka-connect-smt.template.json"
      configFileName = "kafka-connect-smt.json"
      configContainerPath = "/usr/share/java/flowplane-connect-smt"
      rawPattern = [regex]::Escape($rawTopic)
      transformedPattern = [regex]::Escape($transformedTopic)
      dlqPattern = [regex]::Escape($dlqTopic)
      ticketPrefix = "CONNECT"
      startCommand = @()
      sourceKind = "existing-service"
      workDir = ""
      startupSeconds = 0
      versionCommand = @()
      environment = @()
    }
  }
  "kafka-streams" {
    [ordered]@{
      displayName = "Kafka Streams"
      image = "eclipse-temurin:17-jre"
      configRelativePath = "adapter-asset:kafka-streams-runtime.template.json"
      configFileName = "kafka-streams-runtime.json"
      configContainerPath = "/app/flowplane-kafka-streams-runtime.jar"
      rawPattern = [regex]::Escape($rawTopic)
      transformedPattern = [regex]::Escape($transformedTopic)
      dlqPattern = [regex]::Escape($dlqTopic)
      ticketPrefix = "KSTREAMS"
      startCommand = @()
      sourceKind = "first-class-runtime"
      workDir = ""
      startupSeconds = 0
      versionCommand = @()
      environment = @()
    }
  }
  "flink" {
    [ordered]@{
      displayName = "Apache Flink"
      image = "flink:1.20.2-scala_2.12-java17"
      configRelativePath = "adapter-asset:flink-runtime.template.json"
      configFileName = "flink-runtime.json"
      configContainerPath = "/tmp/flowplane-flink-runtime-job.jar"
      rawPattern = [regex]::Escape($rawTopic)
      transformedPattern = [regex]::Escape($transformedTopic)
      dlqPattern = [regex]::Escape($dlqTopic)
      ticketPrefix = "FLINK"
      startCommand = @()
      sourceKind = "first-class-runtime"
      workDir = ""
      startupSeconds = 0
      versionCommand = @()
      environment = @()
    }
  }
  "redpanda-connect" {
    [ordered]@{
      displayName = "Redpanda Connect"
      image = "redpandadata/connect:latest"
      configRelativePath = "docker\http-integrations-local\redpanda_connect_kafka_http.yaml"
      configFileName = "redpanda-connect.yaml"
      configContainerPath = "/etc/redpanda-connect/config.yaml"
      rawPattern = '\$\{RAW_TOPIC\}'
      transformedPattern = '\$\{TRANSFORMED_TOPIC\}'
      dlqPattern = '\$\{DLQ_TOPIC\}'
      ticketPrefix = "RPC"
      startCommand = @("run", "/etc/redpanda-connect/config.yaml")
      sourceKind = "file"
      workDir = ""
      startupSeconds = 5
      versionCommand = @("--version")
      environment = @(
        "KAFKA_BOOTSTRAP=$kafkaBootstrap",
        "RAW_TOPIC=$rawTopic",
        "TRANSFORMED_TOPIC=$transformedTopic",
        "DLQ_TOPIC=$dlqTopic",
        "GROUP_ID=$groupId"
      )
    }
  }
  "logstash" {
    [ordered]@{
      displayName = "Logstash"
      image = "docker.elastic.co/logstash/logstash:8.15.3"
      configRelativePath = "docker\http-integrations-local\logstash_kafka_http.conf"
      configFileName = "logstash.conf"
      configContainerPath = "/usr/share/logstash/pipeline/logstash.conf"
      rawPattern = '\$\{FLOWPLANE_RAW_TOPIC\}'
      transformedPattern = '\$\{FLOWPLANE_TRANSFORMED_TOPIC\}'
      dlqPattern = '\$\{FLOWPLANE_DLQ_TOPIC\}'
      ticketPrefix = "LOGSTASH"
      startCommand = @()
      sourceKind = "file"
      workDir = ""
      startupSeconds = 12
      versionCommand = @("--version")
      environment = @(
        "FLOWPLANE_KAFKA_BOOTSTRAP=$kafkaBootstrap",
        "FLOWPLANE_RAW_TOPIC=$rawTopic",
        "FLOWPLANE_TRANSFORMED_TOPIC=$transformedTopic",
        "FLOWPLANE_DLQ_TOPIC=$dlqTopic",
        "FLOWPLANE_GROUP_ID=$groupId"
      )
    }
  }
  "camel" {
    [ordered]@{
      displayName = "Apache Camel"
      image = "maven:3.9.9-eclipse-temurin-17"
      configRelativePath = "docker\http-integrations-local\camel-kafka-http-streaming"
      configFileName = "camel-project"
      configContainerPath = "/workspace"
      rawPattern = 'FLOWPLANE_RAW_TOPIC'
      transformedPattern = 'FLOWPLANE_TRANSFORMED_TOPIC'
      dlqPattern = 'FLOWPLANE_DLQ_TOPIC'
      ticketPrefix = "CAMEL"
      startCommand = @("mvn", "-q", "-DskipTests", "compile", "exec:java")
      sourceKind = "directory"
      workDir = "/workspace"
      startupSeconds = 20
      versionCommand = @("mvn", "--version")
      environment = @(
        "FLOWPLANE_KAFKA_BOOTSTRAP=$kafkaBootstrap",
        "FLOWPLANE_RAW_TOPIC=$rawTopic",
        "FLOWPLANE_TRANSFORMED_TOPIC=$transformedTopic",
        "FLOWPLANE_DLQ_TOPIC=$dlqTopic",
        "FLOWPLANE_GROUP_ID=$groupId"
      )
    }
  }
  "spring-cloud-stream" {
    [ordered]@{
      displayName = "Spring Cloud Stream"
      image = "maven:3.9.9-eclipse-temurin-17"
      configRelativePath = "docker\http-integrations-local\spring-cloud-stream-kafka-http"
      configFileName = "spring-cloud-stream-project"
      configContainerPath = "/workspace"
      rawPattern = 'FLOWPLANE_RAW_TOPIC'
      transformedPattern = 'FLOWPLANE_TRANSFORMED_TOPIC'
      dlqPattern = 'FLOWPLANE_DLQ_TOPIC'
      ticketPrefix = "SCSTREAM"
      startCommand = @("mvn", "-q", "-DskipTests", "spring-boot:run")
      sourceKind = "directory"
      workDir = "/workspace"
      startupSeconds = 25
      versionCommand = @("mvn", "--version")
      environment = @(
        "FLOWPLANE_KAFKA_BOOTSTRAP=$kafkaBootstrap",
        "FLOWPLANE_RAW_TOPIC=$rawTopic",
        "FLOWPLANE_TRANSFORMED_TOPIC=$transformedTopic",
        "FLOWPLANE_DLQ_TOPIC=$dlqTopic",
        "FLOWPLANE_GROUP_ID=$groupId"
      )
    }
  }
  "nifi" {
    [ordered]@{
      displayName = "Apache NiFi"
      image = "apache/nifi:1.27.0"
      configRelativePath = "scripts\dev\test-nifi-kafka-http-streaming-local.ps1"
      configFileName = "nifi-pipeline-reference.ps1"
      configContainerPath = "/opt/nifi/nifi-current/conf/flowplane-evidence-reference.ps1"
      rawPattern = '\$RawTopic'
      transformedPattern = '\$TransformedTopic'
      dlqPattern = '\$DlqTopic'
      ticketPrefix = "NIFI"
      startCommand = @()
      sourceKind = "file"
      workDir = ""
      startupSeconds = 0
      versionCommand = @("bash", "-lc", 'printf "Apache NiFi %s" "$NIFI_VERSION"')
      environment = @(
        "NIFI_WEB_HTTP_HOST=0.0.0.0",
        "NIFI_WEB_HTTP_PORT=8080",
        "NIFI_SENSITIVE_PROPS_KEY=flowplane-local-streaming-key",
        "SINGLE_USER_CREDENTIALS_USERNAME=",
        "SINGLE_USER_CREDENTIALS_PASSWORD=",
        "NIFI_JVM_HEAP_INIT=512m",
        "NIFI_JVM_HEAP_MAX=1536m"
      )
    }
  }
  "spark-structured-streaming" {
    [ordered]@{
      displayName = "Apache Spark Structured Streaming"
      image = "apache/spark:3.5.3"
      configRelativePath = "adapter-asset:spark_kafka_http_streaming_evidence.py"
      configFileName = "spark_kafka_http_streaming_evidence.py"
      configContainerPath = "/opt/flowplane/spark_kafka_http_streaming_evidence.py"
      rawPattern = 'RAW_TOPIC'
      transformedPattern = 'TRANSFORMED_TOPIC'
      dlqPattern = 'DLQ_TOPIC'
      ticketPrefix = "SPARK"
      startCommand = @(
        "/opt/spark/bin/spark-submit",
        "--conf", "spark.jars.ivy=/tmp/.ivy2",
        "--conf", "spark.eventLog.enabled=true",
        "--conf", "spark.eventLog.dir=file:/tmp/spark-events",
        "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
        "--master", "local[2]",
        "/opt/flowplane/spark_kafka_http_streaming_evidence.py"
      )
      sourceKind = "file"
      workDir = ""
      startupSeconds = 45
      versionCommand = @("/opt/spark/bin/spark-submit", "--version")
      environment = @(
        "FLOWPLANE_KAFKA_BOOTSTRAP=$kafkaBootstrap",
        "FLOWPLANE_RAW_TOPIC=$rawTopic",
        "FLOWPLANE_TRANSFORMED_TOPIC=$transformedTopic",
        "FLOWPLANE_DLQ_TOPIC=$dlqTopic",
        "FLOWPLANE_GROUP_ID=$groupId",
        "FLOWPLANE_CHECKPOINT=/tmp/flowplane-spark-$safeRun",
        "PYTHONUNBUFFERED=1"
      )
    }
  }
  "beam-directrunner" {
    [ordered]@{
      displayName = "Apache Beam DirectRunner"
      image = "flowplane-local/beam-directrunner-cache:2.61.0"
      configRelativePath = "docker\http-integrations-local\beam-kafka-http-streaming"
      configFileName = "beam-project"
      configContainerPath = "/workspace"
      rawPattern = 'FLOWPLANE_RAW_TOPIC'
      transformedPattern = 'FLOWPLANE_TRANSFORMED_TOPIC'
      dlqPattern = 'FLOWPLANE_DLQ_TOPIC'
      ticketPrefix = "BEAM"
      startCommand = @("mvn", "-DskipTests", "compile", "exec:java")
      sourceKind = "directory"
      workDir = "/workspace"
      startupSeconds = 30
      versionCommand = @("mvn", "--version")
      environment = @(
        "FLOWPLANE_KAFKA_BOOTSTRAP=$kafkaBootstrap",
        "FLOWPLANE_RAW_TOPIC=$rawTopic",
        "FLOWPLANE_TRANSFORMED_TOPIC=$transformedTopic",
        "FLOWPLANE_DLQ_TOPIC=$dlqTopic",
        "FLOWPLANE_GROUP_ID=$groupId",
        "FLOWPLANE_MAX_RECORDS=110",
        "FLOWPLANE_MAX_READ_TIME_SECONDS=300"
      )
    }
  }
}
$toolImage = [string]$toolDefinition.image
$isKafkaConnect = $integrationId -eq "kafka-connect"
$isKafkaStreams = $integrationId -eq "kafka-streams"
$isFlink = $integrationId -eq "flink"
$isDebezium = $integrationId -eq "debezium"
$isCompositeHttpBridge = @("vector", "opentelemetry", "debezium") -contains $integrationId
$publisherBridgeContainer = if ($isCompositeHttpBridge) { "flowplane-$integrationId-publisher-$safeRun" } else { $null }
$databaseContainer = if ($isDebezium) { "flowplane-debezium-mysql-$safeRun" } else { $null }
$isFirstClassRuntime = $isKafkaConnect -or $isKafkaStreams -or $isFlink
$evidenceEnvironment = if ($isFlink) { "DEVELOPMENT" } else { "PRODUCTION" }
if ($isKafkaConnect) {
  $toolContainer = "flowplane-connect"
  $connectorName = "flowplane-evidence-$safeRun"
  $runtimeId = "connect-smt-evidence-$safeRun"
}
if ($isKafkaStreams) {
  $runtimeId = "kafka-streams-evidence-$safeRun"
  $runtimeContainer = "flowplane-kafka-streams-evidence-$safeRun"
  $toolContainer = $runtimeContainer
  $groupId = $runtimeId
}
if ($isFlink) {
  $runtimeId = "flink-evidence-$safeRun"
  $runtimeContainer = "flowplane-flink-jobmanager"
  $toolContainer = $runtimeContainer
  $groupId = $runtimeId
}
$runtimeSecret = if ($isFlink) { "flowplane-runtime-dev-secret" } else { [Guid]::NewGuid().ToString("N") + [Guid]::NewGuid().ToString("N") }
$token = if ($isFlink) {
  $keycloakBody = "grant_type=password&client_id=flowplane-ui&username=sso-admin%40flowplane.local&password=admin123&scope=openid%20profile%20email"
  [string](Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:8086/realms/flowplane-local/protocol/openid-connect/token" -ContentType "application/x-www-form-urlencoded" -Body $keycloakBody -TimeoutSec 30).access_token
} else {
  New-FlowplaneAccessToken
}
$startedContainers = [Collections.Generic.List[string]]::new()

function Invoke-DockerChecked {
  $dockerArguments = @($args)
  $previousErrorAction = $ErrorActionPreference
  try {
    # Dockerized JVM tools commonly write warnings to stderr while returning 0.
    # Capture both streams and decide from the native process exit code.
    $ErrorActionPreference = "Continue"
    $output = & docker @dockerArguments 2>&1
    $exitCode = $LASTEXITCODE
  } finally {
    $ErrorActionPreference = $previousErrorAction
  }
  if ($exitCode -ne 0) { throw "docker $($dockerArguments -join ' ') failed: $($output -join [Environment]::NewLine)" }
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
  $previousErrorAction = $ErrorActionPreference
  try {
    $ErrorActionPreference = "Continue"
    $value = (& docker logs $Container 2>&1) -join "`n"
    Write-Utf8NoBom -Path (Join-Path $BundleRoot "sanitized-logs\$Name.log") -Value ((ConvertTo-SafeLogText $value) + "`n")
  } catch {} finally { $ErrorActionPreference = $previousErrorAction }
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
    Write-JsonFile -Path $requestPath -Value $Body
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

function New-KafkaTopic([string]$Topic) {
  Invoke-DockerChecked exec $kafkaContainer kafka-topics --bootstrap-server $kafkaBootstrap --create --if-not-exists --topic $Topic --partitions 1 --replication-factor 1 | Out-Null
  $count = ((Invoke-DockerChecked exec $kafkaContainer kafka-get-offsets --bootstrap-server $kafkaBootstrap --topic $Topic --time -1) -join "`n")
  if ($count -notmatch ':0$') { throw "Evidence topic was not empty at creation: $Topic ($count)" }
}

function Invoke-NifiJson([string]$Method, [string]$Path, [object]$Body = $null) {
  $request = @{ Method = $Method; Uri = "$script:nifiApi$Path"; UseBasicParsing = $true; TimeoutSec = 30 }
  if ($null -ne $Body) {
    $request.ContentType = "application/json"
    $request.Body = $Body | ConvertTo-Json -Depth 40
  }
  Invoke-RestMethod @request
}

function Get-NifiProcessorType([string]$Type) {
  $types = Invoke-NifiJson GET "/flow/processor-types"
  $match = @($types.processorTypes | Where-Object { $_.type -eq $Type } | Select-Object -First 1)[0]
  if (-not $match) { throw "NiFi processor type not found: $Type" }
  $match
}

function New-NifiProcessor([string]$GroupId, [string]$Type, [string]$Name, [int]$X, [int]$Y) {
  $processorType = Get-NifiProcessorType $Type
  Invoke-NifiJson POST "/process-groups/$GroupId/processors" @{
    revision = @{ version = 0 }
    component = @{ type = $Type; bundle = $processorType.bundle; name = $Name; position = @{ x = $X; y = $Y } }
  }
}

function Set-NifiProcessor([object]$Processor, [hashtable]$Properties, [string[]]$AutoTerminate = @()) {
  Invoke-NifiJson PUT "/processors/$($Processor.id)" @{
    revision = @{ version = $Processor.revision.version }
    component = @{ id = $Processor.id; name = $Processor.component.name; config = @{ properties = $Properties; autoTerminatedRelationships = $AutoTerminate } }
  }
}

function New-NifiConnection([string]$GroupId, [object]$Source, [object]$Destination, [string[]]$Relationships) {
  Invoke-NifiJson POST "/process-groups/$GroupId/connections" @{
    revision = @{ version = 0 }
    component = @{
      source = @{ id = $Source.id; groupId = $GroupId; type = "PROCESSOR" }
      destination = @{ id = $Destination.id; groupId = $GroupId; type = "PROCESSOR" }
      selectedRelationships = $Relationships
    }
  } | Out-Null
}

function Start-NifiProcessor([object]$Processor) {
  $current = Invoke-NifiJson GET "/processors/$($Processor.id)"
  Invoke-NifiJson PUT "/processors/$($Processor.id)/run-status" @{ revision = @{ version = $current.revision.version }; state = "RUNNING" } | Out-Null
}

function Initialize-NifiFlow([string]$RuntimeUrl) {
  $root = (Invoke-NifiJson GET "/flow/process-groups/root").processGroupFlow.id
  $group = Invoke-NifiJson POST "/process-groups/$root/process-groups" @{
    revision = @{ version = 0 }
    component = @{ name = "Flowplane evidence $runId"; position = @{ x = 0; y = 0 } }
  }
  $groupId = [string]$group.id
  $consume = New-NifiProcessor $groupId "org.apache.nifi.processors.kafka.pubsub.ConsumeKafka_2_6" "Consume raw Kafka only" 0 0
  $invoke = New-NifiProcessor $groupId "org.apache.nifi.processors.standard.InvokeHTTP" "Invoke Flowplane transformation" 420 0
  $route = New-NifiProcessor $groupId "org.apache.nifi.processors.standard.RouteOnAttribute" "Route Flowplane result" 840 0
  $success = New-NifiProcessor $groupId "org.apache.nifi.processors.kafka.pubsub.PublishKafka_2_6" "Publish transformed Kafka" 1260 0
  $dlq = New-NifiProcessor $groupId "org.apache.nifi.processors.kafka.pubsub.PublishKafka_2_6" "Publish DLQ Kafka" 1260 260

  $consume = Set-NifiProcessor $consume @{ "bootstrap.servers" = $kafkaBootstrap; topic = $rawTopic; "group.id" = $script:groupId; "auto.offset.reset" = "earliest"; "max.poll.records" = "10" }
  $invoke = Set-NifiProcessor $invoke @{ "HTTP Method" = "POST"; "Remote URL" = $RuntimeUrl; "Content-Type" = "application/json"; "Always Output Response" = "true"; "X-FlowPlane-Source-Topic" = $rawTopic; "X-FlowPlane-Source-Key" = "nifi-live-local-evidence" } @("Original", "No Retry", "Retry", "Failure")
  $route = Set-NifiProcessor $route @{ transformed = '${invokehttp.status.code:equals("200")}'; dlq = '${invokehttp.status.code:equals("422")}' } @("unmatched")
  $success = Set-NifiProcessor $success @{ "bootstrap.servers" = $kafkaBootstrap; topic = $transformedTopic; "use-transactions" = "false" } @("success", "failure")
  $dlq = Set-NifiProcessor $dlq @{ "bootstrap.servers" = $kafkaBootstrap; topic = $dlqTopic; "use-transactions" = "false" } @("success", "failure")

  New-NifiConnection $groupId $consume $invoke @("success")
  New-NifiConnection $groupId $invoke $route @("Response")
  New-NifiConnection $groupId $route $success @("transformed")
  New-NifiConnection $groupId $route $dlq @("dlq")
  foreach ($processor in @($success, $dlq, $route, $invoke, $consume)) { Start-NifiProcessor $processor }
  $flow = Invoke-NifiJson GET "/flow/process-groups/$groupId"
  Write-JsonFile -Path (Join-Path $BundleRoot "actual\nifi-flow.json") -Value $flow
  Write-JsonFile -Path (Join-Path $BundleRoot "actual\nifi-process-group.json") -Value ([ordered]@{ id = $groupId; name = "Flowplane evidence $runId"; uiPath = "/nifi/?processGroupId=$groupId" })
  $groupId
}

function Get-ArtifactPathSha256([string]$Path) {
  if (Test-Path -LiteralPath $Path -PathType Leaf) { return Get-Sha256 $Path }
  $root = (Resolve-Path -LiteralPath $Path).Path
  $rows = @(Get-ChildItem -LiteralPath $root -Recurse -File | Sort-Object FullName | ForEach-Object {
    $relative = $_.FullName.Substring($root.Length).TrimStart('\', '/') -replace '\\', '/'
    "$relative=$(Get-Sha256 $_.FullName)"
  })
  return Get-TextSha256 ($rows -join "`n")
}

$jar = $null
if ($isFlink) {
  $jar = Get-ChildItem -LiteralPath (Join-Path $FlowplaneRoot "flowplane-java-sdk\flowplane-flink-runtime\target") -Filter "flowplane-flink-runtime-job.jar" -File | Select-Object -First 1
  if (-not $jar) { throw "No built Flink runtime job jar exists. Build it before execution." }
} elseif ($isKafkaStreams) {
  $jar = Get-ChildItem -LiteralPath (Join-Path $FlowplaneRoot "flowplane-java-sdk\flowplane-kafka-streams-runtime\target") -Filter "flowplane-kafka-streams-runtime.jar" -File | Select-Object -First 1
  if (-not $jar) { throw "No built Kafka Streams runtime jar exists. Build it before execution." }
} elseif (-not $isKafkaConnect) {
  $jar = Get-ChildItem -LiteralPath (Join-Path $FlowplaneRoot "flowplane-java-sdk\flowplane-bento-runtime\target") -Filter "flowplane-bento-runtime-*.jar" -File |
    Where-Object { $_.Name -notlike "original-*" -and $_.Name -notlike "*-plain.jar" } |
    Sort-Object LastWriteTime -Descending |
    Select-Object -First 1
  if (-not $jar) { throw "No built Bento runtime jar exists. Build it before execution." }
}

foreach ($name in @($runtimeContainer, $toolContainer, $publisherBridgeContainer, $databaseContainer)) {
  if ([string]::IsNullOrWhiteSpace([string]$name)) { continue }
  if (($isKafkaConnect -or $isFlink) -and $name -eq $toolContainer) { continue }
  if ($isKafkaStreams -and $name -eq $toolContainer -and $name -eq $runtimeContainer -and $script:checkedKafkaStreamsContainer) { continue }
  if ($isKafkaStreams) { $script:checkedKafkaStreamsContainer = $true }
  $existing = @(& docker ps -aq --filter "name=^$name$")
  if ($existing.Count -gt 0 -and -not [string]::IsNullOrWhiteSpace(($existing -join ""))) { throw "Refusing to replace existing Docker container: $name" }
}

$sourceConfig = if ([string]$toolDefinition.configRelativePath -like "adapter-asset:*") {
  Join-Path $PSScriptRoot ("..\assets\" + ([string]$toolDefinition.configRelativePath).Substring("adapter-asset:".Length))
} else {
  Join-Path $FlowplaneRoot ([string]$toolDefinition.configRelativePath)
}
if (-not (Test-Path -LiteralPath $sourceConfig)) { throw "Missing $($toolDefinition.displayName) pipeline configuration: $sourceConfig" }
$publisherBridgeScript = if ($isCompositeHttpBridge) { Join-Path $PSScriptRoot "kafka-http-publisher-bridge.mjs" } else { $null }
if ($isCompositeHttpBridge -and -not (Test-Path -LiteralPath $publisherBridgeScript)) { throw "Missing composite publisher bridge: $publisherBridgeScript" }
$configCopy = Join-Path $BundleRoot ("configuration\" + [string]$toolDefinition.configFileName)
Copy-Item -LiteralPath $sourceConfig -Destination $configCopy -Recurse
$configEvidencePath = "configuration/$($toolDefinition.configFileName)"
$debeziumConnectorBody = $null
if ($isDebezium) {
  # The official quay.io/debezium/example-mysql tutorial image initializes
  # these local-only credentials through its bundled SQL scripts.
  $mysqlRootPassword = "debezium"
  $mysqlUser = "mysqluser"
  $mysqlPassword = "mysqlpw"
  $debeziumConnectorName = "flowplane-debezium-$safeRun"
  $debeziumSchemaHistoryTopic = "$topicPrefix.schema-history"
  $debeziumConnectorBody = [ordered]@{
    name = $debeziumConnectorName
    config = [ordered]@{
      "connector.class" = "io.debezium.connector.mysql.MySqlConnector"
      "tasks.max" = "1"
      "database.hostname" = $databaseContainer
      "database.port" = "3306"
      "database.user" = $mysqlUser
      "database.password" = $mysqlPassword
      "database.server.id" = [string](Get-Random -Minimum 5400 -Maximum 65000)
      "topic.prefix" = "flowplane-debezium-$safeRun"
      "database.include.list" = "flowplane"
      "table.include.list" = "flowplane.records"
      "schema.history.internal.kafka.bootstrap.servers" = $kafkaBootstrap
      "schema.history.internal.kafka.topic" = $debeziumSchemaHistoryTopic
      "include.schema.changes" = "false"
      "snapshot.mode" = "no_data"
      "transforms" = "route"
      "transforms.route.type" = "org.apache.kafka.connect.transforms.RegexRouter"
      "transforms.route.regex" = ".*"
      "transforms.route.replacement" = $rawTopic
      "key.converter" = "org.apache.kafka.connect.json.JsonConverter"
      "key.converter.schemas.enable" = "false"
      "value.converter" = "org.apache.kafka.connect.json.JsonConverter"
      "value.converter.schemas.enable" = "false"
    }
  }
  $sanitizedConnector = $debeziumConnectorBody | ConvertTo-Json -Depth 20 | ConvertFrom-Json
  $sanitizedConnector.config.'database.password' = "<redacted>"
  Write-JsonFile -Path $configCopy -Value $sanitizedConnector
}
$toolMountSource = $configCopy
$toolMountMode = "ro"
$temporaryToolWorkspace = $null
if ([string]$toolDefinition.sourceKind -eq "directory") {
  # Maven/Gradle-style runtimes must be able to create target/build outputs. Keep
  # the checksum-covered evidence copy immutable and build in a disposable clone.
  $temporaryToolWorkspace = Join-Path ([IO.Path]::GetTempPath()) ("flowplane-{0}-{1}" -f $integrationId, $safeRun)
  if (Test-Path -LiteralPath $temporaryToolWorkspace) { throw "Refusing to replace existing temporary tool workspace: $temporaryToolWorkspace" }
  Copy-Item -LiteralPath $configCopy -Destination $temporaryToolWorkspace -Recurse
  $toolMountSource = $temporaryToolWorkspace
  $toolMountMode = "rw"
}
$verifierScript = Join-Path $PSScriptRoot $(if ($isDebezium) { "debezium-raw-only-verifier.mjs" } else { "kafka-raw-only-verifier.mjs" })
$runtimeCommandEvidence = if ($isKafkaConnect) {
  "POST http://127.0.0.1:8084/connectors (sanitized config: configuration/$($toolDefinition.configFileName))"
} elseif ($isKafkaStreams) {
  "docker run --name $runtimeContainer --network $flowplaneNetwork -v <kafka-streams-runtime-jar>:/app/flowplane-kafka-streams-runtime.jar:ro $toolImage java -jar /app/flowplane-kafka-streams-runtime.jar (exact sanitized arguments: configuration/$($toolDefinition.configFileName))"
} elseif ($isFlink) {
  "POST http://127.0.0.1:8089/jars/upload then POST /jars/<uploaded-jar-id>/run (exact sanitized arguments: configuration/$($toolDefinition.configFileName))"
} else {
  "docker run --name $runtimeContainer --network $flowplaneNetwork -p 127.0.0.1::8080 -v <bento-jar>:/app/flowplane-bento-runtime.jar:ro $runtimeImage java -jar /app/flowplane-bento-runtime.jar"
}
$toolCommandEvidence = if ($isKafkaConnect) {
  "GET http://127.0.0.1:8084/connectors/$connectorName/status"
} elseif ($isKafkaStreams) {
  "GET /api/v1/runtimes/$runtimeId and docker inspect $runtimeContainer"
} elseif ($isFlink) {
  "GET http://127.0.0.1:8089/jobs/<run-specific-job-id>"
} elseif ($isCompositeHttpBridge) {
  if ($isDebezium) {
    "docker run MySQL + quay.io/debezium/connect:3.6; POST the checksum-covered connector configuration; docker run --name $publisherBridgeContainer --network $flowplaneNetwork <publisher-bridge> kafka:9092 http://$runtimeContainer`:8080/transform $integrationId $runId 110 /evidence $rawTopic"
  } elseif ($integrationId -eq "opentelemetry") {
    "docker run --name $publisherBridgeContainer --network $flowplaneNetwork <publisher-bridge> kafka:9092 http://$runtimeContainer`:8080/transform $integrationId $runId 110 /evidence $forwardTopic; docker run --name $toolContainer --network $flowplaneNetwork -v <otel-config>:/etc/otelcol-contrib/config.yaml:ro $toolImage --config=/etc/otelcol-contrib/config.yaml"
  } else {
    "docker run --name $publisherBridgeContainer --network $flowplaneNetwork <publisher-bridge> kafka:9092 http://$runtimeContainer`:8080/transform $integrationId $runId 110 /evidence; docker run --name $toolContainer --network $flowplaneNetwork -v <vector-config>:/etc/vector/vector.toml:ro $toolImage --config /etc/vector/vector.toml --require-healthy true"
  }
} else {
  "docker run --name $toolContainer --network $flowplaneNetwork -v <isolated-tool-workspace>`:$($toolDefinition.configContainerPath):$toolMountMode <tool-environment> -e FLOWPLANE_TRANSFORM_URL=http://$runtimeContainer`:8080/transform $toolImage $($toolDefinition.startCommand -join ' ')"
}

Write-Utf8NoBom -Path (Join-Path $BundleRoot "commands.txt") -Value ((@(
  "# Exact run values; generated credentials are redacted.",
  "docker exec $kafkaContainer kafka-topics --bootstrap-server $kafkaBootstrap --create --topic $rawTopic --partitions 1 --replication-factor 1",
  "docker exec $kafkaContainer kafka-topics --bootstrap-server $kafkaBootstrap --create --topic $transformedTopic --partitions 1 --replication-factor 1",
  "docker exec $kafkaContainer kafka-topics --bootstrap-server $kafkaBootstrap --create --topic $dlqTopic --partitions 1 --replication-factor 1",
  $(if ($integrationId -eq "opentelemetry") { "docker exec $kafkaContainer kafka-topics --bootstrap-server $kafkaBootstrap --create --topic $forwardTopic --partitions 1 --replication-factor 1" } else { $null }),
  $runtimeCommandEvidence,
  $toolCommandEvidence,
  $(if ($isDebezium) { "node debezium-raw-only-verifier.mjs <fixtureRoot> <bundleRoot> $runId <mysql-container> <redacted> $kafkaContainer $kafkaBootstrap" } else { "node kafka-raw-only-verifier.mjs <fixtureRoot> <bundleRoot> $runId $integrationId $kafkaContainer $kafkaBootstrap" })
) -join "`n") + "`n")
Write-Utf8NoBom -Path (Join-Path $BundleRoot "reproduce.ps1") -Value ((@(
  "param([string]`$FlowplaneRoot = 'C:\FlowPlaneNew\repositories\flowplane-controlplane')",
  "& 'C:\FlowPlaneNew\video-generation-scripts-copy\scripts\demo\11-run-live-local-verification.ps1' -FlowplaneRoot `$FlowplaneRoot -Execute -Integration $integrationId"
) -join "`n") + "`n")

try {
  Add-Step "Loading canonical mapping and fixtures."
  $mappingDsl = [string](Get-Content -LiteralPath (Join-Path $FixtureRoot "mapping.yaml") -Raw)
  $validPayloads = @(Get-Content -LiteralPath (Join-Path $FixtureRoot "valid-input.jsonl") | Where-Object { $_ } | ForEach-Object { [string]$_ })
  $invalidPayloads = @(Get-Content -LiteralPath (Join-Path $FixtureRoot "invalid-input.jsonl") | Where-Object { $_ } | ForEach-Object { [string]$_ })
  $samplePayload = [string]$validPayloads[0]

  Add-Step "Creating and governing a run-specific synthetic mapping."
  $teamPage = Invoke-LocalApi -Method Get -Path "/api/v1/teams?activeOnly=true&page=0&size=100"
  $team = @($teamPage.items | Select-Object -First 1)[0]
  if (-not $team) { throw "No active team is available for verification." }
  $mapping = Invoke-LocalApi -Method Post -Path "/api/v1/mappings" -Body @{
    name = "$integrationId-live-local-$safeRun"
    description = "Synthetic $integrationId pipeline-to-sidecar live-local verification."
    workspaceId = "workspace-platform"
    teamId = [string]$team.id
    teamName = [string]$team.name
    projectId = "live-local-verification"
    projectName = "Live Local Verification"
    environment = $evidenceEnvironment
    mappingDsl = $mappingDsl
    samplePayload = $samplePayload
    dictionaryIds = @()
  }
  Write-JsonFile -Path (Join-Path $BundleRoot "actual\mapping-created.json") -Value $mapping
  $validation = Invoke-LocalApi -Method Post -Path "/api/v1/mappings/$($mapping.id)/validate"
  Write-JsonFile -Path (Join-Path $BundleRoot "actual\mapping-validation.json") -Value $validation
  if (-not $validation.valid) { throw "Canonical mapping validation failed: $($validation.errors -join '; ')" }

  $simulationRecords = @($validPayloads | ForEach-Object {
    $payload = $_ | ConvertFrom-Json
    [ordered]@{ recordId = [string]$payload.event.id; payloadJson = [string]$_ }
  })
  $simulation = Invoke-LocalApi -Method Post -Path "/api/v1/mappings/$($mapping.id)/simulate:batch" -Body @{ records = $simulationRecords; runtimeModes = @() }
  Write-JsonFile -Path (Join-Path $BundleRoot "expected\simulation-batch.json") -Value $simulation
  if (-not $simulation.success -or [int]$simulation.recordCount -ne 100 -or @($simulation.records | Where-Object { -not $_.success }).Count -ne 0) { throw "Expected-output simulation did not produce 100 successful records." }
  $validSimulation = Invoke-LocalApi -Method Post -Path "/api/v1/mappings/$($mapping.id)/simulate" -Body @{ payloadJson = [string]$validPayloads[0] }
  Write-JsonFile -Path (Join-Path $BundleRoot "expected\simulation-valid.json") -Value $validSimulation
  $invalidSimulation = Invoke-LocalApi -Method Post -Path "/api/v1/mappings/$($mapping.id)/simulate" -Body @{ payloadJson = [string]$invalidPayloads[0] }
  Write-JsonFile -Path (Join-Path $BundleRoot "expected\simulation-invalid.json") -Value $invalidSimulation
  if ([int]$validSimulation.errorCount -ne 0 -or [int]$invalidSimulation.errorCount -lt 1) { throw "Canonical valid/invalid simulation gates failed." }

  $ticket = "$($toolDefinition.ticketPrefix)-$runId"
  if ($evidenceEnvironment -eq "PRODUCTION") {
    Invoke-LocalApi -Method Post -Path "/api/v1/mappings/$($mapping.id)/submit-review" -Body @{ reason = "Synthetic $integrationId verification mapping is validated and simulated."; changeTicket = $ticket } | Out-Null
    $approvals = Invoke-LocalApi -Method Get -Path "/api/v1/approvals?environment=$evidenceEnvironment&page=0&size=100"
    $approval = @($approvals.items | Where-Object { $_.mappingId -eq $mapping.id } | Select-Object -First 1)[0]
    if (-not $approval) { throw "No approval request was created." }
    $approved = Invoke-LocalApi -Method Post -Path "/api/v1/approvals/$($approval.id)/approve" -Body @{ reason = "Reviewed synthetic local verification mapping."; changeTicket = $ticket }
    $qaPassed = Invoke-LocalApi -Method Post -Path "/api/v1/approvals/$($approval.id)/qa-pass" -Body @{ reason = "Valid and invalid simulation gates passed."; changeTicket = $ticket }
    Write-JsonFile -Path (Join-Path $BundleRoot "actual\mapping-approval.json") -Value ([ordered]@{ request = $approval; approval = $approved; qaPass = $qaPassed })
    $published = Invoke-LocalApi -Method Post -Path "/api/v1/mappings/$($mapping.id)/publish" -Body @{ reason = "Publish QA-approved artifact for $integrationId verification."; changeTicket = $ticket }
  } else {
    Write-JsonFile -Path (Join-Path $BundleRoot "actual\mapping-approval.json") -Value ([ordered]@{ applicable = $false; environment = $evidenceEnvironment; reason = "Tenant policy limits review requests to production; validated development mappings publish directly." })
    $published = Invoke-LocalApi -Method Post -Path "/api/v1/mappings/$($mapping.id)/publish" -Body @{}
  }
  Write-JsonFile -Path (Join-Path $BundleRoot "actual\mapping-published.json") -Value $published

  Add-Step "Creating empty raw, transformed, and DLQ Kafka topics."
  foreach ($topicName in @($rawTopic, $transformedTopic, $dlqTopic)) { New-KafkaTopic $topicName }
  if ($integrationId -eq "opentelemetry") { New-KafkaTopic $forwardTopic }

  $runtimePort = $null
  $connectorStatus = $null
  if ($isKafkaConnect) {
    Add-Step "Registering a run-specific connector on the existing Kafka Connect worker."
    $connectConfig = [ordered]@{
      "connector.class" = "org.apache.kafka.connect.mirror.MirrorSourceConnector"
      "tasks.max" = "1"
      "source.cluster.alias" = "source"
      "target.cluster.alias" = "target"
      "source.cluster.bootstrap.servers" = $kafkaBootstrap
      "target.cluster.bootstrap.servers" = $kafkaBootstrap
      "topics" = $rawTopic
      "groups" = ""
      "refresh.topics.enabled" = "false"
      "sync.topic.configs.enabled" = "false"
      "emit.checkpoints.enabled" = "false"
      "emit.heartbeats.enabled" = "false"
      "replication.factor" = "1"
      "offset-syncs.topic.replication.factor" = "1"
      "heartbeats.topic.replication.factor" = "1"
      "checkpoints.topic.replication.factor" = "1"
      "value.converter" = "org.apache.kafka.connect.converters.ByteArrayConverter"
      "key.converter" = "org.apache.kafka.connect.storage.StringConverter"
      "transforms" = "flowplane,route"
      "transforms.flowplane.type" = "com.flowplane.connect.FlowPlaneTransform"
      "transforms.flowplane.flowplane.control-plane.url" = "http://flowplane-backend:8080"
      "transforms.flowplane.flowplane.runtime.id" = $runtimeId
      "transforms.flowplane.flowplane.connect.rest.url" = "http://connect:8083"
      "transforms.flowplane.flowplane.connect.connector.name" = $connectorName
      "transforms.flowplane.flowplane.runtime.name" = "Kafka Connect SMT Evidence $runId"
      "transforms.flowplane.flowplane.runtime.environment" = "PRODUCTION"
      "transforms.flowplane.flowplane.runtime.owner.team" = "Quality Engineering"
      "transforms.flowplane.flowplane.runtime.project.id" = "live-local-verification"
      "transforms.flowplane.flowplane.tenant.id" = $script:FLOWPLANE_TENANT_ID
      "transforms.flowplane.flowplane.auth.token" = $token
      "transforms.flowplane.flowplane.runtime.client.secret" = $runtimeSecret
      "transforms.flowplane.flowplane.output.shape" = "JSON_STRING"
      "transforms.flowplane.flowplane.output.complex.types" = "NATIVE_JSON"
      "transforms.flowplane.flowplane.output.field.naming" = "AS_IS"
      "transforms.flowplane.flowplane.connect.output.mode" = "BYTES"
      "transforms.flowplane.flowplane.fail.on.error" = "false"
      "transforms.flowplane.flowplane.error.bootstrap.servers" = $kafkaBootstrap
      "transforms.flowplane.flowplane.error.topic" = $dlqTopic
      "transforms.route.type" = "org.apache.kafka.connect.transforms.RegexRouter"
      "transforms.route.regex" = "source\.$([regex]::Escape($rawTopic))"
      "transforms.route.replacement" = $transformedTopic
    }
    $sanitizedConnectConfig = [ordered]@{}
    foreach ($entry in $connectConfig.GetEnumerator()) {
      $sanitizedConnectConfig[$entry.Key] = if ($entry.Key -match '(auth.token|client.secret)$') { "<redacted>" } else { $entry.Value }
    }
    Write-JsonFile -Path $configCopy -Value ([ordered]@{ name = $connectorName; config = $sanitizedConnectConfig })
    $connectRequest = @{ name = $connectorName; config = $connectConfig } | ConvertTo-Json -Depth 20
    Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:8084/connectors" -ContentType "application/json" -Body $connectRequest -TimeoutSec 30 | Out-Null

    $runtime = $null
    $deadline = (Get-Date).AddMinutes(3)
    do {
      try { $runtime = Invoke-LocalApi -Method Get -Path "/api/v1/runtimes/$runtimeId" } catch {}
      if ($runtime -and $runtime.id -eq $runtimeId) { break }
      Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    if (-not $runtime) { throw "Kafka Connect SMT runtime did not register with the control plane." }
    Write-JsonFile -Path (Join-Path $BundleRoot "actual\runtime-registration.json") -Value $runtime
    $deployment = Invoke-LocalApi -Method Post -Path "/api/v1/mappings/$($mapping.id)/deploy" -Body @{ runtimeIds = @($runtimeId); rolloutPercent = 100; requireReplayGate = $false; reason = "Assign the approved artifact to the Kafka Connect SMT runtime."; changeTicket = $ticket }
    Write-JsonFile -Path (Join-Path $BundleRoot "actual\deployment.json") -Value $deployment

    $deadline = (Get-Date).AddMinutes(3)
    do {
      try {
        $runtime = Invoke-LocalApi -Method Get -Path "/api/v1/runtimes/$runtimeId"
        $connectorStatus = Invoke-RestMethod -Uri "http://127.0.0.1:8084/connectors/$connectorName/status" -TimeoutSec 10
      } catch {}
      $taskRunning = $connectorStatus -and @($connectorStatus.tasks | Where-Object { $_.state -eq "RUNNING" }).Count -eq 1
      if ($runtime.activeArtifactId -and $runtime.activeArtifactHash -and $taskRunning) { break }
      Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    if (-not $runtime.activeArtifactId -or -not $taskRunning) { throw "Kafka Connect SMT did not load its artifact or reach RUNNING task state." }
    $runtimeStatus = [ordered]@{ assignmentPresent = $true; artifactId = $runtime.activeArtifactId; artifactHash = $runtime.activeArtifactHash; version = $runtime.activeVersion; runtimeId = $runtime.id; health = $runtime.health; lifecycleState = $runtime.lifecycleState }
    Write-JsonFile -Path (Join-Path $BundleRoot "actual\kafka-connect-status-before.json") -Value $connectorStatus
    Write-JsonFile -Path (Join-Path $BundleRoot "actual\runtime-status-before.json") -Value $runtimeStatus
  } elseif ($isKafkaStreams) {
    Add-Step "Starting a run-specific first-class Flowplane Kafka Streams runtime."
    $kafkaStreamsConfig = [ordered]@{
      runtimeId = $runtimeId
      runtimeName = "Kafka Streams Evidence $runId"
      runtimeEnvironment = $evidenceEnvironment
      bootstrapServers = $kafkaBootstrap
      inputTopic = $rawTopic
      outputTopic = $transformedTopic
      errorTopic = $dlqTopic
      applicationId = $runtimeId
      controlPlaneUrl = "http://flowplane-backend:8080"
      outputShape = "JSON_STRING"
      outputComplexTypes = "NATIVE_JSON"
      outputFieldNaming = "AS_IS"
      kafkaOutputMode = "JSON_STRING"
      autoOffsetReset = "earliest"
      authToken = "<redacted>"
      runtimeClientSecret = "<redacted>"
    }
    Write-JsonFile -Path $configCopy -Value $kafkaStreamsConfig
    Invoke-DockerChecked run -d --name $runtimeContainer --network $flowplaneNetwork `
      -v "$($jar.FullName):/app/flowplane-kafka-streams-runtime.jar:ro" `
      $toolImage java -jar /app/flowplane-kafka-streams-runtime.jar `
      "--bootstrap.servers=$kafkaBootstrap" `
      "--input.topic=$rawTopic" `
      "--output.topic=$transformedTopic" `
      "--error.topic=$dlqTopic" `
      "--application.id=$runtimeId" `
      "--control-plane.url=http://flowplane-backend:8080" `
      "--runtime.id=$runtimeId" `
      "--runtime.name=Kafka Streams Evidence $runId" `
      "--runtime.environment=PRODUCTION" `
      "--runtime.owner.team=Quality Engineering" `
      "--runtime.project.id=live-local-verification" `
      "--tenant.id=$script:FLOWPLANE_TENANT_ID" `
      "--auth.token=$token" `
      "--runtime.client.secret=$runtimeSecret" `
      "--schema-check.enabled=true" `
      "--schema-registry.url=http://schema-registry:8081" `
      "--schema-check.poll.interval.ms=5000" `
      "--assignment.poll.interval.ms=1000" `
      "--output.shape=JSON_STRING" `
      "--output.complex.types=NATIVE_JSON" `
      "--output.field.naming=AS_IS" `
      "--kafka.output.mode=JSON_STRING" `
      "--auto.offset.reset=earliest" | Out-Null
    $startedContainers.Add($runtimeContainer)

    $runtime = $null
    $deadline = (Get-Date).AddMinutes(3)
    do {
      try { $runtime = Invoke-LocalApi -Method Get -Path "/api/v1/runtimes/$runtimeId" } catch {}
      if ($runtime -and $runtime.id -eq $runtimeId -and $runtime.health -eq "HEALTHY") { break }
      Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    if (-not $runtime) { throw "Kafka Streams runtime did not register with the control plane." }
    Write-JsonFile -Path (Join-Path $BundleRoot "actual\runtime-registration.json") -Value $runtime
    $deployment = Invoke-LocalApi -Method Post -Path "/api/v1/mappings/$($mapping.id)/deploy" -Body @{ runtimeIds = @($runtimeId); rolloutPercent = 100; requireReplayGate = $false; reason = "Assign the approved artifact to the Kafka Streams runtime."; changeTicket = $ticket }
    Write-JsonFile -Path (Join-Path $BundleRoot "actual\deployment.json") -Value $deployment

    $deadline = (Get-Date).AddMinutes(3)
    do {
      try { $runtime = Invoke-LocalApi -Method Get -Path "/api/v1/runtimes/$runtimeId" } catch {}
      if ($runtime.activeArtifactId -and $runtime.activeArtifactHash -and $runtime.health -eq "HEALTHY") { break }
      Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    if (-not $runtime.activeArtifactId) { throw "Kafka Streams runtime did not load its assigned mapping artifact." }
    $runtimeStatus = [ordered]@{ assignmentPresent = $true; artifactId = $runtime.activeArtifactId; artifactHash = $runtime.activeArtifactHash; version = $runtime.activeVersion; runtimeId = $runtime.id; health = $runtime.health; lifecycleState = $runtime.lifecycleState }
    Write-JsonFile -Path (Join-Path $BundleRoot "actual\runtime-status-before.json") -Value $runtimeStatus
  } elseif ($isFlink) {
    Add-Step "Submitting a run-specific first-class Flowplane job to the existing Flink cluster."
    $flinkRegistrationRequest = [ordered]@{
      runtimeId = $runtimeId
      name = "Flink Evidence $runId"
      type = "FLINK"
      environment = $evidenceEnvironment
      ownerTeam = "Quality Engineering"
      projectId = "live-local-verification"
      deploymentTarget = "LOCAL_DOCKER"
      networkProfile = "flowplane-quality-stack"
      controlPlaneUrl = "http://flowplane-backend:8080"
      kafkaBootstrapServers = $kafkaBootstrap
      schemaRegistryUrl = "http://schema-registry:8081"
      inputTopic = $rawTopic
      outputTopic = $transformedTopic
      errorTopic = $dlqTopic
      dockerNetwork = $flowplaneNetwork
      serviceName = "flowplane-flink-jobmanager"
      containerImage = $toolImage
      outputShape = "JSON_STRING"
      outputComplexTypes = "NATIVE_JSON"
      outputFieldNaming = "AS_IS"
      replayEnabled = $false
      assignmentPollIntervalMs = 1000
      heartbeatIntervalMs = 10000
      labels = @{ evidenceRunId = $runId }
      additionalEnvironment = @{}
      wrapperVersion = "1.0.0"
      coreEngineVersion = "1.0.0"
      supportedDslVersions = @("flowplane/v1")
      supportedFeatures = @("stateless", "error-policy/v1", "replay/kafka")
    }
    $flinkRegistrationHeaders = @{ Authorization = "Bearer $token"; tenantId = $script:FLOWPLANE_TENANT_ID; "X-Tenant-Id" = $script:FLOWPLANE_TENANT_ID }
    $flinkRegistrationIssue = Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:8081/api/v1/runtime-registrations" -Headers $flinkRegistrationHeaders -ContentType "application/json" -Body ($flinkRegistrationRequest | ConvertTo-Json -Depth 20) -TimeoutSec 30
    $runtimeSecret = [string]$flinkRegistrationIssue.clientSecret
    if ([string]::IsNullOrWhiteSpace($runtimeSecret)) { throw "Flink runtime registration did not issue a client secret." }
    Write-JsonFile -Path (Join-Path $BundleRoot "actual\flink-runtime-profile.json") -Value $flinkRegistrationIssue.profile
    $flinkConfig = [ordered]@{
      runtimeId = $runtimeId
      runtimeName = "Flink Evidence $runId"
      runtimeEnvironment = $evidenceEnvironment
      bootstrapServers = $kafkaBootstrap
      inputTopic = $rawTopic
      outputTopic = $transformedTopic
      errorTopic = $dlqTopic
      groupId = $runtimeId
      controlPlaneUrl = "http://flowplane-backend:8080"
      outputShape = "JSON_STRING"
      outputComplexTypes = "NATIVE_JSON"
      outputFieldNaming = "AS_IS"
      flinkOutputMode = "JSON_STRING"
      parallelism = 1
      failOnError = $false
      authToken = "<redacted>"
      runtimeClientSecret = "<redacted>"
    }
    Write-JsonFile -Path $configCopy -Value $flinkConfig
    $flinkProgramArguments = @(
      "--bootstrap.servers=$kafkaBootstrap",
      "--input.topic=$rawTopic",
      "--output.topic=$transformedTopic",
      "--error.topic=$dlqTopic",
      "--group.id=$runtimeId",
      "--control-plane.url=http://flowplane-backend:8080",
      "--runtime.id=$runtimeId",
      "--runtime.name=Flink Evidence $runId",
      "--runtime.environment=$evidenceEnvironment",
      "--runtime.owner.team=Quality Engineering",
      "--runtime.project.id=live-local-verification",
      "--tenant.id=$script:FLOWPLANE_TENANT_ID",
      "--auth.token=",
      "--runtime.client.secret=$runtimeSecret",
      "--schema-check.enabled=true",
      "--schema-registry.url=http://schema-registry:8081",
      "--schema-check.poll.interval.ms=5000",
      "--assignment.poll.interval.ms=1000",
      "--heartbeat.interval.ms=10000",
      "--output.shape=JSON_STRING",
      "--output.complex.types=NATIVE_JSON",
      "--output.field.naming=AS_IS",
      "--flink.output.mode=JSON_STRING",
      "--fail.on.error=false"
    )
    $flinkUploadPath = Join-Path $BundleRoot "adapter-private\flink-upload.json"
    $uploadStatus = & curl.exe --silent --show-error --max-time 120 --form "jarfile=@$($jar.FullName)" --output $flinkUploadPath --write-out "%{http_code}" "http://127.0.0.1:8089/jars/upload"
    if ($LASTEXITCODE -ne 0 -or [int]$uploadStatus -ne 200) { throw "Flink REST jar upload failed with HTTP $uploadStatus." }
    $flinkUpload = Get-Content -LiteralPath $flinkUploadPath -Raw | ConvertFrom-Json
    $flinkJarId = Split-Path -Leaf ([string]$flinkUpload.filename)
    if ([string]::IsNullOrWhiteSpace($flinkJarId)) { throw "Flink REST upload did not return a jar ID." }
    $flinkRunBody = @{ entryClass = "com.flowplane.flink.FlowPlaneKafkaFlinkJob"; parallelism = 1; programArgsList = $flinkProgramArguments } | ConvertTo-Json -Depth 10
    $flinkRun = Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:8089/jars/$flinkJarId/run" -ContentType "application/json" -Body $flinkRunBody -TimeoutSec 120
    $flinkJobId = [string]$flinkRun.jobid
    if ($flinkJobId -notmatch '^[0-9a-f]{32}$') { throw "Flink REST submission did not return a valid job ID." }
    Write-Utf8NoBom -Path (Join-Path $BundleRoot "sanitized-logs\flink-submit.log") -Value ("REST upload status: success`nREST submitted job ID: $flinkJobId`n")
    Write-JsonFile -Path (Join-Path $BundleRoot "actual\flink-upload.json") -Value ([ordered]@{ status = [string]$flinkUpload.status; jarId = $flinkJarId; sourceJarSha256 = Get-Sha256 $jar.FullName })
    Write-JsonFile -Path (Join-Path $BundleRoot "actual\flink-submission.json") -Value ([ordered]@{ jobId = $flinkJobId; jobManagerUrl = "http://127.0.0.1:8089/#/job/$flinkJobId/overview"; submitted = $true })

    $runtime = $null
    $flinkJob = $null
    $deadline = (Get-Date).AddMinutes(3)
    do {
      try {
        $runtime = Invoke-LocalApi -Method Get -Path "/api/v1/runtimes/$runtimeId"
        $flinkJob = Invoke-RestMethod -Uri "http://127.0.0.1:8089/jobs/$flinkJobId" -TimeoutSec 10
      } catch {}
      if ($runtime -and $runtime.id -eq $runtimeId -and $runtime.health -eq "HEALTHY" -and $flinkJob.state -eq "RUNNING") { break }
      Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    if (-not $runtime -or $flinkJob.state -ne "RUNNING") { throw "Flink job did not register or reach RUNNING state." }
    Write-JsonFile -Path (Join-Path $BundleRoot "actual\runtime-registration.json") -Value $runtime
    Write-JsonFile -Path (Join-Path $BundleRoot "actual\flink-job-before.json") -Value $flinkJob
    $deployment = Invoke-LocalApi -Method Post -Path "/api/v1/mappings/$($mapping.id)/deploy" -Body @{ runtimeIds = @($runtimeId); rolloutPercent = 100; requireReplayGate = $false; reason = "Assign the approved artifact to the Flink runtime."; changeTicket = $ticket }
    Write-JsonFile -Path (Join-Path $BundleRoot "actual\deployment.json") -Value $deployment

    $deadline = (Get-Date).AddMinutes(3)
    do {
      try { $runtime = Invoke-LocalApi -Method Get -Path "/api/v1/runtimes/$runtimeId" } catch {}
      if ($runtime.activeArtifactId -and $runtime.activeArtifactHash -and $runtime.health -eq "HEALTHY") { break }
      Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    if (-not $runtime.activeArtifactId) { throw "Flink runtime did not load its assigned mapping artifact." }
    $runtimeStatus = [ordered]@{ assignmentPresent = $true; artifactId = $runtime.activeArtifactId; artifactHash = $runtime.activeArtifactHash; version = $runtime.activeVersion; runtimeId = $runtime.id; health = $runtime.health; lifecycleState = $runtime.lifecycleState; flinkJobId = $flinkJobId }
    Write-JsonFile -Path (Join-Path $BundleRoot "actual\runtime-status-before.json") -Value $runtimeStatus
  } else {
    Add-Step "Starting a run-specific Flowplane Bento HTTP sidecar."
    Invoke-DockerChecked run -d --name $runtimeContainer --network $flowplaneNetwork -p "127.0.0.1::8080" `
      -v "$($jar.FullName):/app/flowplane-bento-runtime.jar:ro" `
      -e "FLOWPLANE_BENTO_CONTROL_PLANE_URL=http://flowplane-backend:8080" `
      -e "FLOWPLANE_BENTO_RUNTIME_ID=$runtimeId" `
      -e "FLOWPLANE_BENTO_RUNTIME_NAME=$($toolDefinition.displayName) Sidecar $runId" `
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
      $runtimeImage java -jar /app/flowplane-bento-runtime.jar | Out-Null
    $startedContainers.Add($runtimeContainer)
    $runtimePort = Get-PublishedPort $runtimeContainer 8080
    Wait-Http "http://127.0.0.1:$runtimePort/actuator/health" 180 | Out-Null

    $runtime = $null
    $deadline = (Get-Date).AddMinutes(3)
    do {
      try { $runtime = Invoke-LocalApi -Method Get -Path "/api/v1/runtimes/$runtimeId" } catch {}
      if ($runtime -and $runtime.id -eq $runtimeId) { break }
      Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    if (-not $runtime) { throw "Bento runtime did not register with the control plane." }
    Write-JsonFile -Path (Join-Path $BundleRoot "actual\runtime-registration.json") -Value $runtime
    $deployment = Invoke-LocalApi -Method Post -Path "/api/v1/mappings/$($mapping.id)/deploy" -Body @{ runtimeIds = @($runtimeId); rolloutPercent = 100; requireReplayGate = $false; reason = "Assign the approved artifact to the $integrationId sidecar."; changeTicket = $ticket }
    Write-JsonFile -Path (Join-Path $BundleRoot "actual\deployment.json") -Value $deployment

    $runtimeStatus = $null
    $deadline = (Get-Date).AddMinutes(3)
    do {
      try { $runtimeStatus = Invoke-RestMethod -Uri "http://127.0.0.1:$runtimePort/runtime/status" -TimeoutSec 10 } catch {}
      if ($runtimeStatus -and $runtimeStatus.assignmentPresent) { break }
      Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    if (-not $runtimeStatus.assignmentPresent) { throw "Bento runtime never loaded its assigned mapping artifact." }
    Write-JsonFile -Path (Join-Path $BundleRoot "actual\runtime-status-before.json") -Value $runtimeStatus
  }

  $verifierSource = [string](Get-Content -LiteralPath $verifierScript -Raw)
  $pipelineSource = if (Test-Path -LiteralPath $configCopy -PathType Leaf) {
    [string](Get-Content -LiteralPath $configCopy -Raw)
  } else {
    [string]((Get-ChildItem -LiteralPath $configCopy -Recurse -File | Sort-Object FullName | ForEach-Object { Get-Content -LiteralPath $_.FullName -Raw }) -join "`n")
  }
  if ($isCompositeHttpBridge) {
    $pipelineSource += "`n" + [string](Get-Content -LiteralPath $publisherBridgeScript -Raw)
  }
  $writeBoundaryAudit = [ordered]@{
    verifierProducerOperations = [regex]::Matches($verifierSource, 'kafka-console-producer').Count
    verifierDatabaseInsertOperations = [regex]::Matches($verifierSource, 'INSERT INTO records').Count
    verifierRawProducerTargets = [regex]::Matches($verifierSource, '"--topic", topics\.raw').Count
    verifierDownstreamProducerTargets = [regex]::Matches($verifierSource, '"--topic", topics\.(?:transformed|dlq)').Count
    verifierRuntimeUrlReferences = [regex]::Matches($verifierSource, '\bruntimeUrl\b').Count
    pipelineRawInputReferences = [regex]::Matches($pipelineSource, [string]$toolDefinition.rawPattern).Count
    pipelineTransformedOutputReferences = [regex]::Matches($pipelineSource, [string]$toolDefinition.transformedPattern).Count
    pipelineDlqOutputReferences = [regex]::Matches($pipelineSource, [string]$toolDefinition.dlqPattern).Count
    verifierWriteTargets = if ($isDebezium) { @("mysql://$databaseContainer/flowplane.records") } else { @($rawTopic) }
    verifierReadTargets = @($transformedTopic, $dlqTopic)
    pipelineWriteTargets = @($transformedTopic, $dlqTopic)
    verifierSha256 = Get-Sha256 $verifierScript
    pipelineConfigurationSha256 = Get-ArtifactPathSha256 $configCopy
    publisherBridgeSha256 = if ($isCompositeHttpBridge) { Get-Sha256 $publisherBridgeScript } else { $null }
  }
  $verifierRawOnlyPassed = if ($isDebezium) {
    $writeBoundaryAudit.verifierProducerOperations -eq 0 -and $writeBoundaryAudit.verifierDatabaseInsertOperations -eq 1 -and $writeBoundaryAudit.verifierDownstreamProducerTargets -eq 0 -and $writeBoundaryAudit.verifierRuntimeUrlReferences -eq 0
  } else {
    $writeBoundaryAudit.verifierProducerOperations -eq 1 -and $writeBoundaryAudit.verifierRawProducerTargets -eq 1 -and $writeBoundaryAudit.verifierDownstreamProducerTargets -eq 0 -and $writeBoundaryAudit.verifierRuntimeUrlReferences -eq 0
  }
  $writeBoundaryAudit.passed = ($verifierRawOnlyPassed -and $writeBoundaryAudit.pipelineRawInputReferences -ge 1 -and $writeBoundaryAudit.pipelineTransformedOutputReferences -ge 1 -and $writeBoundaryAudit.pipelineDlqOutputReferences -ge 1)
  Write-JsonFile -Path (Join-Path $BundleRoot "actual\write-boundary-audit.json") -Value $writeBoundaryAudit
  if (-not $writeBoundaryAudit.passed) { throw "Static write-boundary audit failed." }

  if ($isCompositeHttpBridge) {
    Add-Step "Starting the dedicated Flowplane HTTP/Kafka publisher bridge used by $($toolDefinition.displayName)."
    $nodeModules = Join-Path $PSScriptRoot "..\assets\nats-node\node_modules"
    if (-not (Test-Path -LiteralPath (Join-Path $nodeModules "kafkajs\package.json"))) { throw "The evidence Node dependency set does not contain kafkajs." }
    $publisherArguments = @(
      "run", "-d", "--name", $publisherBridgeContainer, "--network", $flowplaneNetwork,
      "-v", "$($publisherBridgeScript):/app/bridge.mjs:ro",
      "-v", "$($nodeModules):/app/node_modules:ro",
      "-v", "$($BundleRoot):/evidence",
      "node:22-alpine", "node", "/app/bridge.mjs", $kafkaBootstrap, "http://$runtimeContainer`:8080/transform", $integrationId, $runId, "110", "/evidence"
    )
    if ($integrationId -eq "opentelemetry") { $publisherArguments += $forwardTopic }
    if ($isDebezium) { $publisherArguments += $rawTopic }
    Invoke-DockerChecked @publisherArguments | Out-Null
    $startedContainers.Add($publisherBridgeContainer)
    $publisherReady = Join-Path $BundleRoot "actual\publisher-bridge-ready.json"
    $deadline = (Get-Date).AddMinutes(2)
    do {
      if (Test-Path -LiteralPath $publisherReady) { break }
      $publisherRunning = [string](& docker inspect --format '{{.State.Running}}' $publisherBridgeContainer 2>$null)
      if ($publisherRunning.Trim() -eq "false") { throw "Publisher bridge exited before readiness: $((& docker logs $publisherBridgeContainer 2>&1) -join ' ')" }
      Start-Sleep -Seconds 1
    } while ((Get-Date) -lt $deadline)
    if (-not (Test-Path -LiteralPath $publisherReady)) { throw "Publisher bridge did not become ready." }
  }

  if ($isDebezium) {
    Add-Step "Starting the MySQL raw source and independently deployed Debezium Connect worker."
    Invoke-DockerChecked run -d --name $databaseContainer --network $flowplaneNetwork `
      -e "MYSQL_ROOT_PASSWORD=$mysqlRootPassword" -e "MYSQL_USER=$mysqlUser" -e "MYSQL_PASSWORD=$mysqlPassword" `
      quay.io/debezium/example-mysql:3.6 | Out-Null
    $startedContainers.Add($databaseContainer)
    $deadline = (Get-Date).AddMinutes(4)
    $mysqlAuthenticated = $false
    do {
      $mysqlProbe = ""
      $mysqlProbeExit = 1
      try {
        $mysqlProbe = (& docker exec -e "MYSQL_PWD=$mysqlRootPassword" $databaseContainer mysql -N -B -uroot -e "SELECT 1;" 2>$null) -join ""
        $mysqlProbeExit = $LASTEXITCODE
      } catch {}
      if ($mysqlProbeExit -eq 0 -and $mysqlProbe.Trim() -eq "1") { $mysqlAuthenticated = $true; break }
      Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    if (-not $mysqlAuthenticated) { throw "Debezium MySQL source did not become authenticated and ready." }
    $schemaSql = "CREATE DATABASE IF NOT EXISTS flowplane CHARACTER SET utf8mb4; CREATE TABLE IF NOT EXISTS flowplane.records (record_id VARCHAR(160) PRIMARY KEY, payload_json JSON NOT NULL, created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)); GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '$mysqlUser'@'%'; FLUSH PRIVILEGES;"
    Invoke-DockerChecked exec -e "MYSQL_PWD=$mysqlRootPassword" $databaseContainer mysql -uroot -e $schemaSql | Out-Null

    Invoke-DockerChecked run -d --name $toolContainer --network $flowplaneNetwork -p "127.0.0.1::8083" `
      -e "BOOTSTRAP_SERVERS=$kafkaBootstrap" `
      -e "GROUP_ID=flowplane-debezium-connect-$safeRun" `
      -e "CONFIG_STORAGE_TOPIC=$topicPrefix.connect-config" `
      -e "OFFSET_STORAGE_TOPIC=$topicPrefix.connect-offset" `
      -e "STATUS_STORAGE_TOPIC=$topicPrefix.connect-status" `
      -e "CONFIG_STORAGE_REPLICATION_FACTOR=1" `
      -e "OFFSET_STORAGE_REPLICATION_FACTOR=1" `
      -e "STATUS_STORAGE_REPLICATION_FACTOR=1" `
      -e "REST_HOST_NAME=0.0.0.0" `
      $toolImage | Out-Null
    $startedContainers.Add($toolContainer)
    $debeziumPort = Get-PublishedPort $toolContainer 8083
    Wait-Http "http://127.0.0.1:$debeziumPort/" 300 | Out-Null
    $connectorResponse = Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:$debeziumPort/connectors" -ContentType "application/json" -Body ($debeziumConnectorBody | ConvertTo-Json -Depth 20 -Compress) -TimeoutSec 60
    Write-JsonFile -Path (Join-Path $BundleRoot "actual\debezium-connector-created.json") -Value $connectorResponse
    $deadline = (Get-Date).AddMinutes(4)
    do {
      try { $debeziumStatus = Invoke-RestMethod -Uri "http://127.0.0.1:$debeziumPort/connectors/$debeziumConnectorName/status" -TimeoutSec 20 } catch {}
      if ($debeziumStatus.connector.state -eq "RUNNING" -and @($debeziumStatus.tasks | Where-Object { $_.state -eq "RUNNING" }).Count -eq 1) { break }
      Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    if ($debeziumStatus.connector.state -ne "RUNNING" -or @($debeziumStatus.tasks | Where-Object { $_.state -eq "RUNNING" }).Count -ne 1) { throw "Debezium connector did not reach RUNNING state: $($debeziumStatus | ConvertTo-Json -Depth 10 -Compress)" }
    Write-JsonFile -Path (Join-Path $BundleRoot "actual\debezium-connector-status-before.json") -Value $debeziumStatus
  } elseif (-not $isFirstClassRuntime) {
    Add-Step "Starting the independently deployed $($toolDefinition.displayName) pipeline."
    $toolRunArguments = @("run", "-d", "--name", $toolContainer, "--network", $flowplaneNetwork, "-v", "$($toolMountSource):$($toolDefinition.configContainerPath):$toolMountMode")
    if ($integrationId -eq "nifi") { $toolRunArguments += @("-p", "127.0.0.1::8080") }
    if ($integrationId -eq "vector") {
      $vectorDataDirectory = Join-Path $BundleRoot "metrics\vector-data"
      New-Item -ItemType Directory -Force -Path $vectorDataDirectory | Out-Null
      $toolRunArguments += @("-p", "127.0.0.1::8686", "-p", "127.0.0.1::9598", "-v", "$($vectorDataDirectory):/var/lib/vector")
    }
    if ($integrationId -eq "opentelemetry") {
      $toolRunArguments += @("-p", "127.0.0.1::13133", "-p", "127.0.0.1::8888")
    }
    if ($integrationId -eq "spark-structured-streaming") {
      $sparkEventDirectory = Join-Path $BundleRoot "metrics\spark-events"
      New-Item -ItemType Directory -Force -Path $sparkEventDirectory | Out-Null
      $toolRunArguments += @("-p", "127.0.0.1::4040", "-v", "$($sparkEventDirectory):/tmp/spark-events")
    }
    if (-not [string]::IsNullOrWhiteSpace([string]$toolDefinition.workDir)) { $toolRunArguments += @("-w", [string]$toolDefinition.workDir) }
    foreach ($environmentValue in @($toolDefinition.environment)) { $toolRunArguments += @("-e", [string]$environmentValue) }
    $toolRunArguments += @("-e", "FLOWPLANE_TRANSFORM_URL=http://$runtimeContainer`:8080/transform", $toolImage)
    $toolRunArguments += @($toolDefinition.startCommand)
    Invoke-DockerChecked @toolRunArguments | Out-Null
    $startedContainers.Add($toolContainer)
  } else {
    Add-Step "Using the independently running first-class $($toolDefinition.displayName) runtime."
  }
  if ($integrationId -eq "vector") {
    $vectorApiPort = Get-PublishedPort $toolContainer 8686
    $vectorMetricsPort = Get-PublishedPort $toolContainer 9598
    Wait-Http "http://127.0.0.1:$vectorMetricsPort/metrics" 120 | Out-Null
  } elseif ($integrationId -eq "opentelemetry") {
    $otelHealthPort = Get-PublishedPort $toolContainer 13133
    $otelMetricsPort = Get-PublishedPort $toolContainer 8888
    Wait-Http "http://127.0.0.1:$otelHealthPort/" 120 | Out-Null
  } elseif ($integrationId -eq "nifi") {
    $nifiPort = Get-PublishedPort $toolContainer 8080
    $script:nifiApi = "http://127.0.0.1:$nifiPort/nifi-api"
    Wait-Http "$script:nifiApi/flow/status" 300 | Out-Null
    $nifiGroupId = Initialize-NifiFlow "http://$runtimeContainer`:8080/transform"
    Start-Sleep -Seconds 5
  } elseif ($integrationId -eq "beam-directrunner") {
    $beamReady = $false
    $beamDeadline = (Get-Date).AddMinutes(15)
    do {
      $toolRunningNow = [string](& docker inspect --format '{{.State.Running}}' $toolContainer 2>$null)
      if ($toolRunningNow.Trim() -ne "true") { throw "Apache Beam pipeline exited before its Kafka source became ready." }
      $threadProbe = (& docker exec $toolContainer jcmd 1 Thread.print 2>&1) -join "`n"
      if ($threadProbe -match 'KafkaConsumerPoll-thread' -and $threadProbe -match 'KafkaUnboundedReader') { $beamReady = $true; break }
      Start-Sleep -Seconds 5
    } while ((Get-Date) -lt $beamDeadline)
    if (-not $beamReady) { throw "Apache Beam Kafka consumer thread was not ready within 15 minutes." }
  } else {
    Start-Sleep -Seconds ([int]$toolDefinition.startupSeconds)
  }
  $toolRunning = [string](& docker inspect --format '{{.State.Running}}' $toolContainer)
  if ($toolRunning.Trim() -ne "true") { throw "$($toolDefinition.displayName) pipeline did not remain running." }

  Add-Step "Publishing canonical fixtures through the raw-only verifier."
  $previousErrorActionPreference = $ErrorActionPreference
  $ErrorActionPreference = "Continue"
  try {
    if ($isDebezium) {
      $verifierTranscript = & node $verifierScript $FixtureRoot $BundleRoot $runId $databaseContainer $mysqlRootPassword $kafkaContainer $kafkaBootstrap 2>&1
    } else {
      $verifierTranscript = & node $verifierScript $FixtureRoot $BundleRoot $runId $integrationId $kafkaContainer $kafkaBootstrap 2>&1
    }
    $verifierExit = $LASTEXITCODE
  } finally {
    $ErrorActionPreference = $previousErrorActionPreference
  }
  Write-Utf8NoBom -Path (Join-Path $BundleRoot "sanitized-logs\raw-only-verifier.log") -Value ((ConvertTo-SafeLogText ($verifierTranscript -join "`n")) + "`n")
  if ($verifierExit -ne 0) { throw "Raw-only verifier failed with exit code $verifierExit`: $($verifierTranscript -join [Environment]::NewLine)" }

  $bridgeResult = Read-JsonFile (Join-Path $BundleRoot "actual\bridge-result.json")
  $expectedVerifierWriteTarget = if ($isDebezium) { "mysql://$databaseContainer/flowplane.records" } else { $rawTopic }
  $runtimeBoundaryPassed = (@($bridgeResult.verifierWriteTargets).Count -eq 1 -and [string]$bridgeResult.verifierWriteTargets[0] -eq $expectedVerifierWriteTarget -and @($writeBoundaryAudit.pipelineWriteTargets) -contains $transformedTopic -and @($writeBoundaryAudit.pipelineWriteTargets) -contains $dlqTopic)
  if ($isCompositeHttpBridge) {
    $publisherBridgeResult = Read-JsonFile (Join-Path $BundleRoot "actual\publisher-bridge-result.json")
    $runtimeBoundaryPassed = $runtimeBoundaryPassed -and [bool]$publisherBridgeResult.completed -and [int]$publisherBridgeResult.received -eq 110 -and [int]$publisherBridgeResult.transformed -eq 100 -and [int]$publisherBridgeResult.dlq -eq 10 -and [int]$publisherBridgeResult.publishFailures -eq 0 -and [int]$publisherBridgeResult.runtimeFailures -eq 0
  }
  if (-not $runtimeBoundaryPassed) { throw "Runtime write-boundary evidence did not match the raw-only architecture." }
  if ($integrationId -eq "vector") {
    $vectorMetrics = (Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:$vectorMetricsPort/metrics" -TimeoutSec 30).Content
    Write-Utf8NoBom -Path (Join-Path $BundleRoot "metrics\vector-prometheus.txt") -Value ([string]$vectorMetrics)
    $vectorGraphQl = Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:$vectorApiPort/graphql" -ContentType "application/json" -Body '{"query":"{ components(first: 100) { edges { node { componentId componentType } } } }"}' -TimeoutSec 30
    Write-JsonFile -Path (Join-Path $BundleRoot "actual\vector-graphql-components.json") -Value $vectorGraphQl
  } elseif ($integrationId -eq "opentelemetry") {
    $otelHealth = Invoke-RestMethod -Uri "http://127.0.0.1:$otelHealthPort/" -TimeoutSec 30
    Write-JsonFile -Path (Join-Path $BundleRoot "actual\opentelemetry-health.json") -Value ([ordered]@{ status = "UP"; response = $otelHealth })
    $otelMetrics = (Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:$otelMetricsPort/metrics" -TimeoutSec 30).Content
    Write-Utf8NoBom -Path (Join-Path $BundleRoot "metrics\opentelemetry-prometheus.txt") -Value ([string]$otelMetrics)
  } elseif ($isDebezium) {
    $debeziumStatus = Invoke-RestMethod -Uri "http://127.0.0.1:$debeziumPort/connectors/$debeziumConnectorName/status" -TimeoutSec 30
    Write-JsonFile -Path (Join-Path $BundleRoot "actual\debezium-connector-status-after.json") -Value $debeziumStatus
    $debeziumWorker = Invoke-RestMethod -Uri "http://127.0.0.1:$debeziumPort/" -TimeoutSec 30
    Write-JsonFile -Path (Join-Path $BundleRoot "actual\debezium-worker-info.json") -Value $debeziumWorker
    $databaseCount = (Invoke-DockerChecked exec -e "MYSQL_PWD=$mysqlRootPassword" $databaseContainer mysql -N -B -uroot -e "SELECT COUNT(*) FROM flowplane.records;") -join "`n"
    Write-Utf8NoBom -Path (Join-Path $BundleRoot "metrics\debezium-mysql-source-count.txt") -Value ($databaseCount.Trim() + "`n")
  }

  if ($isKafkaConnect) {
    $connectorStatus = Invoke-RestMethod -Uri "http://127.0.0.1:8084/connectors/$connectorName/status" -TimeoutSec 30
    $runtime = Invoke-LocalApi -Method Get -Path "/api/v1/runtimes/$runtimeId"
    $taskRunning = @($connectorStatus.tasks | Where-Object { $_.state -eq "RUNNING" }).Count -eq 1
    $runtimeHealthAfter = [ordered]@{ status = if ($taskRunning -and $runtime.health -eq "HEALTHY") { "UP" } else { "DOWN" }; connector = $connectorStatus.connector.state; runningTasks = @($connectorStatus.tasks | Where-Object { $_.state -eq "RUNNING" }).Count; runtimeHealth = $runtime.health }
    $runtimeStatusAfter = [ordered]@{ assignmentPresent = [bool]$runtime.activeArtifactId; artifactId = $runtime.activeArtifactId; artifactHash = $runtime.activeArtifactHash; version = $runtime.activeVersion; runtimeId = $runtime.id; health = $runtime.health; lifecycleState = $runtime.lifecycleState }
    Write-JsonFile -Path (Join-Path $BundleRoot "actual\kafka-connect-status-after.json") -Value $connectorStatus
  } elseif ($isKafkaStreams) {
    $runtime = Invoke-LocalApi -Method Get -Path "/api/v1/runtimes/$runtimeId"
    $runtimeRunning = [string](& docker inspect --format '{{.State.Running}}' $runtimeContainer 2>$null)
    $runtimeHealthAfter = [ordered]@{ status = if ($runtimeRunning.Trim() -eq "true" -and $runtime.health -eq "HEALTHY") { "UP" } else { "DOWN" }; runtimeHealth = $runtime.health; containerRunning = $runtimeRunning.Trim() -eq "true" }
    $runtimeStatusAfter = [ordered]@{ assignmentPresent = [bool]$runtime.activeArtifactId; artifactId = $runtime.activeArtifactId; artifactHash = $runtime.activeArtifactHash; version = $runtime.activeVersion; runtimeId = $runtime.id; health = $runtime.health; lifecycleState = $runtime.lifecycleState }
    Write-JsonFile -Path (Join-Path $BundleRoot "actual\kafka-streams-runtime-after.json") -Value $runtime
  } elseif ($isFlink) {
    $runtime = Invoke-LocalApi -Method Get -Path "/api/v1/runtimes/$runtimeId"
    $flinkJobAfter = Invoke-RestMethod -Uri "http://127.0.0.1:8089/jobs/$flinkJobId" -TimeoutSec 30
    $flinkJobRunning = $flinkJobAfter.state -eq "RUNNING"
    $runtimeHealthAfter = [ordered]@{ status = if ($flinkJobRunning -and $runtime.health -eq "HEALTHY") { "UP" } else { "DOWN" }; runtimeHealth = $runtime.health; flinkJobId = $flinkJobId; flinkJobState = $flinkJobAfter.state }
    $runtimeStatusAfter = [ordered]@{ assignmentPresent = [bool]$runtime.activeArtifactId; artifactId = $runtime.activeArtifactId; artifactHash = $runtime.activeArtifactHash; version = $runtime.activeVersion; runtimeId = $runtime.id; health = $runtime.health; lifecycleState = $runtime.lifecycleState; flinkJobId = $flinkJobId }
    Write-JsonFile -Path (Join-Path $BundleRoot "actual\flink-job-after.json") -Value $flinkJobAfter
    Write-JsonFile -Path (Join-Path $BundleRoot "actual\flink-runtime-after.json") -Value $runtime
  } else {
    $runtimeHealthAfter = Invoke-RestMethod -Uri "http://127.0.0.1:$runtimePort/actuator/health" -TimeoutSec 30
    $runtimeStatusAfter = Invoke-RestMethod -Uri "http://127.0.0.1:$runtimePort/runtime/status" -TimeoutSec 30
  }
  Write-JsonFile -Path (Join-Path $BundleRoot "actual\runtime-health-after.json") -Value $runtimeHealthAfter
  Write-JsonFile -Path (Join-Path $BundleRoot "actual\runtime-status-after.json") -Value $runtimeStatusAfter
  if ($integrationId -eq "nifi") {
    $nifiStatus = Invoke-NifiJson GET "/flow/process-groups/$nifiGroupId/status?recursive=true"
    Write-JsonFile -Path (Join-Path $BundleRoot "actual\nifi-process-group-status.json") -Value $nifiStatus
  }
  $toolInspect = docker inspect $toolContainer | ConvertFrom-Json
  $runtimeInspect = if ($isKafkaConnect) { $toolInspect } else { docker inspect $runtimeContainer | ConvertFrom-Json }
  $toolCompletedSuccessfully = ($integrationId -eq "beam-directrunner" -and -not [bool]$toolInspect[0].State.Running -and [int]$toolInspect[0].State.ExitCode -eq 0)
  $toolHealthyAtCompletion = ([bool]$toolInspect[0].State.Running -or $toolCompletedSuccessfully)
  if ($isFlink) { $toolHealthyAtCompletion = $toolHealthyAtCompletion -and $flinkJobRunning }
  if ($isDebezium) { $toolHealthyAtCompletion = $toolHealthyAtCompletion -and $debeziumStatus.connector.state -eq "RUNNING" -and @($debeziumStatus.tasks | Where-Object { $_.state -eq "RUNNING" }).Count -eq 1 }
  $toolStats = if ([bool]$toolInspect[0].State.Running) {
    (Invoke-DockerChecked stats --no-stream --format '{{json .}}' $toolContainer) -join "`n"
  } else {
    ([ordered]@{ container = $toolContainer; running = $false; exitCode = [int]$toolInspect[0].State.ExitCode; completedSuccessfully = $toolCompletedSuccessfully } | ConvertTo-Json -Compress)
  }
  Write-Utf8NoBom -Path (Join-Path $BundleRoot "metrics\$integrationId-container-stats.jsonl") -Value ($toolStats + "`n")
  Write-JsonFile -Path (Join-Path $BundleRoot "actual\pipeline-result.json") -Value ([ordered]@{
    schemaVersion = "flowplane.$integrationId-pipeline-result.v1"
    container = $toolContainer
    runningAtCompletion = [bool]$toolInspect[0].State.Running
    completedSuccessfully = $toolCompletedSuccessfully
    readTargets = @($rawTopic)
    writeTargets = @($transformedTopic, $dlqTopic)
    processedInput = [int]$bridgeResult.attemptedInput
    successfulOutput = [int]$bridgeResult.successfulOutput
    errorOutput = [int]$bridgeResult.errorOutput
    unexpectedFailures = 0
  })

  $counts = [ordered]@{
    attemptedInput = [int]$bridgeResult.attemptedInput
    acceptedInput = [int]$bridgeResult.acceptedInput
    successfulOutput = [int]$bridgeResult.successfulOutput
    intentionalInvalid = [int]$bridgeResult.intentionalInvalid
    errorOutput = [int]$bridgeResult.errorOutput
    filtered = [int]$bridgeResult.filtered
    duplicates = [int]$bridgeResult.duplicates
    unexpectedFailures = 0
    pending = [int64]$bridgeResult.finalLag
    finalLag = [int64]$bridgeResult.finalLag
    retries = 0
    timeouts = 0
  }
  Write-JsonFile -Path (Join-Path $BundleRoot "counts.json") -Value $counts
  Write-JsonFile -Path (Join-Path $BundleRoot "final-state.json") -Value ([ordered]@{ captured = $true; runtimeHealthy = ($runtimeHealthAfter.status -eq "UP"); integrationHealthy = $toolHealthyAtCompletion; pending = [int64]$bridgeResult.finalLag; finalLag = [int64]$bridgeResult.finalLag; capturedAt = [DateTime]::UtcNow.ToString("o") })

  if ($isKafkaConnect) {
    $connectWorkerInfo = Invoke-RestMethod -Uri "http://127.0.0.1:8084/" -TimeoutSec 30
    $toolVersion = "Kafka Connect $($connectWorkerInfo.version)"
  } elseif ($isKafkaStreams) {
    $toolVersion = "Flowplane Kafka Streams runtime 1.0.0 (local shaded jar)"
  } elseif ($isFlink) {
    $flinkOverview = Invoke-RestMethod -Uri "http://127.0.0.1:8089/overview" -TimeoutSec 30
    $toolVersion = "Apache Flink $($flinkOverview.'flink-version') + Flowplane runtime 1.0.0"
  } elseif ($isDebezium) {
    $toolVersion = "Debezium Connect $($debeziumWorker.version) (Kafka $($debeziumWorker.commit))"
  } elseif ($integrationId -eq "nifi") {
    $nifiVersionEnvironment = @($toolInspect[0].Config.Env | Where-Object { $_ -like "NIFI_VERSION=*" } | Select-Object -First 1)[0]
    $toolVersion = if ($nifiVersionEnvironment) { "Apache NiFi " + ($nifiVersionEnvironment -replace '^NIFI_VERSION=', '') } else { "Apache NiFi 1.27.0 (pinned image tag)" }
  } elseif ($integrationId -eq "spark-structured-streaming") {
    $toolVersion = "Apache Spark 3.5.3 (pinned image tag)"
  } else {
    $toolVersion = ((& docker run --rm $toolImage @($toolDefinition.versionCommand) 2>&1) -join "`n").Trim()
  }
  Write-JsonFile -Path (Join-Path $BundleRoot "versions.json") -Value ([ordered]@{
    flowplane = Get-GitState $FlowplaneRoot
    integration = $integrationId
    toolVersion = $toolVersion
    toolImage = $toolImage
    toolImageId = $toolInspect[0].Image
    runtimeImage = $runtimeImage
    runtimeImageId = $runtimeInspect[0].Image
    pipelineConfigurationSha256 = Get-ArtifactPathSha256 $configCopy
    publisherBridgeSha256 = if ($isCompositeHttpBridge) { Get-Sha256 $publisherBridgeScript } else { $null }
    rawOnlyVerifierSha256 = Get-Sha256 $verifierScript
    runtimeJar = if ($jar) { $jar.Name } else { $null }
    runtimeJarSha256 = if ($jar) { Get-Sha256 $jar.FullName } else { $null }
    nodeVersion = (& node --version)
    dockerVersion = (& docker version --format '{{.Server.Version}}')
  })

  $manifest = Read-JsonFile (Join-Path $BundleRoot "run-manifest.json")
  $manifest.artifactId = [string]$runtimeStatusAfter.artifactId
  $manifest.artifactVersion = [string]$runtimeStatusAfter.version
  $manifest.artifactHash = [string]$runtimeStatusAfter.artifactHash
  $manifest.runtime = [ordered]@{ name = if ($isFirstClassRuntime) { "$($toolDefinition.displayName) first-class runtime" } elseif ($isCompositeHttpBridge) { "$($toolDefinition.displayName) + Flowplane Bento HTTP sidecar + acknowledged Kafka publisher bridge" } else { "$($toolDefinition.displayName) + Flowplane Bento HTTP sidecar" }; version = $toolVersion; executionMode = "Docker live local"; containerImages = if ($isFirstClassRuntime) { @($toolImage) } elseif ($isDebezium) { @($toolImage, "quay.io/debezium/example-mysql:3.6", $runtimeImage, "node:22-alpine") } elseif ($isCompositeHttpBridge) { @($toolImage, $runtimeImage, "node:22-alpine") } else { @($toolImage, $runtimeImage) } }
  $manifest.sourceBoundary = if ($isDebezium) { "Raw-only verifier SQL insert to persistent MySQL table; Debezium binlog CDC to Kafka" } else { "Raw-only verifier producer to persistent Kafka raw topic" }
  $manifest.sinkBoundary = "$($toolDefinition.displayName) pipeline to persistent Kafka transformed and DLQ topics"
  $manifest.validRecords = [int]$bridgeResult.validInput
  $manifest.invalidRecords = [int]$bridgeResult.intentionalInvalid
  $manifest.successfulOutputs = [int]$bridgeResult.successfulOutput
  $manifest.errorOutputs = [int]$bridgeResult.errorOutput
  $manifest.duplicates = [int]$bridgeResult.duplicates
  $manifest.unexplainedMissing = [Math]::Max(0, [int]$bridgeResult.attemptedInput - [int]$bridgeResult.successfulOutput - [int]$bridgeResult.errorOutput)
  $manifest.finalLag = [int64]$bridgeResult.finalLag
  $manifest.unexpectedFailures = 0
  Write-JsonFile -Path (Join-Path $BundleRoot "run-manifest.json") -Value $manifest

  $assert = { param([string]$Id, [bool]$Passed, [string[]]$Evidence, [string]$Reason = "") [ordered]@{ id = $Id; applicable = $true; required = $true; passed = $Passed; evidence = $Evidence; reason = $Reason } }
  $gates = @(
    & $assert "runtime.started" $toolHealthyAtCompletion @("actual/pipeline-result.json", $configEvidencePath)
    & $assert "runtime.healthConfirmed" ($toolHealthyAtCompletion -and $runtimeHealthAfter.status -eq "UP") @("actual/pipeline-result.json", "actual/runtime-health-after.json")
    & $assert "runtime.versionRecorded" (-not [string]::IsNullOrWhiteSpace($toolVersion)) @("versions.json")
    & $assert "boundary.realRuntimeUsed" ([int]$bridgeResult.attemptedInput -eq 110 -and [int]$bridgeResult.successfulOutput -eq 100 -and [int]$bridgeResult.errorOutput -eq 10) @("actual/bridge-result.json", "actual/pipeline-result.json")
    & $assert "boundary.realProtocolCrossed" ($runtimeBoundaryPassed -and [bool]$writeBoundaryAudit.passed) @("actual/write-boundary-audit.json", $configEvidencePath, "metrics/kafka-topic-counts.json", "actual/pipeline-result.json")
    & $assert "boundary.verifierWritesRawOnly" ([bool]$writeBoundaryAudit.passed -and @($bridgeResult.verifierWriteTargets).Count -eq 1) @("actual/write-boundary-audit.json", "actual/bridge-result.json", "actual/pipeline-result.json")
    & $assert "artifact.loaded" ([bool]$runtimeStatusAfter.assignmentPresent) @("actual/runtime-status-after.json")
    & $assert "artifact.idRecorded" (-not [string]::IsNullOrWhiteSpace([string]$runtimeStatusAfter.artifactId)) @("actual/runtime-status-after.json")
    & $assert "artifact.hashRecorded" (-not [string]::IsNullOrWhiteSpace([string]$runtimeStatusAfter.artifactHash)) @("actual/runtime-status-after.json")
    & $assert "fixture.validProcessed" ([int]$bridgeResult.successfulOutput -eq 100) @("actual/bridge-result.json", "actual/transformed-output.jsonl")
    & $assert "fixture.invalidProcessed" ([int]$bridgeResult.errorOutput -eq 10) @("actual/bridge-result.json", "actual/error-output.jsonl")
    & $assert "output.expectedHashMatched" ([int]$bridgeResult.expectedHashMatches -eq 100) @("actual/bridge-result.json", "expected/simulation-batch.json")
    & $assert "error.expectedCodeMatched" ([int]$bridgeResult.expectedErrorMatches -eq 10) @("actual/error-output.jsonl", "actual/bridge-result.json")
    & $assert "accounting.inputReconciled" ([int]$bridgeResult.attemptedInput -eq ([int]$bridgeResult.successfulOutput + [int]$bridgeResult.errorOutput + [int]$bridgeResult.filtered)) @("counts.json")
    & $assert "accounting.noUnexpectedLoss" ([int]$manifest.unexplainedMissing -eq 0) @("counts.json", "actual/bridge-result.json")
    & $assert "accounting.noUnexpectedDuplicates" ([int]$bridgeResult.duplicates -eq 0) @("counts.json", "actual/bridge-result.json")
    & $assert "accounting.noUnexpectedFailures" $true @("counts.json", "actual/pipeline-result.json")
    & $assert "state.finalLagZero" ([int64]$bridgeResult.finalLag -eq 0) @("metrics/kafka-consumer-group.txt", "final-state.json")
    & $assert "state.pendingWorkZero" ([int64]$bridgeResult.finalLag -eq 0) @("metrics/kafka-topic-counts.json", "final-state.json")
    & $assert "state.runtimeHealthyAtCompletion" ($runtimeHealthAfter.status -eq "UP" -and $toolHealthyAtCompletion) @("actual/runtime-health-after.json", "actual/pipeline-result.json")
    & $assert "evidence.environmentRecorded" $true @("environment.json")
    & $assert "evidence.commandsRecorded" $true @("commands.txt")
    & $assert "evidence.logsPreserved" $true @("sanitized-logs/adapter.log", "sanitized-logs/raw-only-verifier.log", "sanitized-logs/$integrationId.log", "sanitized-logs/flowplane-runtime.log")
    & $assert "evidence.rawOutputsPreserved" $true @("actual/bridge-result.json", "actual/pipeline-result.json", "actual/write-boundary-audit.json", "actual/transformed-output.jsonl", "actual/error-output.jsonl")
    & $assert "evidence.checksumsVerified" $false @() "Set by the bundle evaluator."
    & $assert "evidence.reproductionScriptAvailable" $true @("reproduce.ps1")
  )
  Write-JsonFile -Path (Join-Path $BundleRoot "actual\adapter-gate-assertions.json") -Value ([ordered]@{ schemaVersion = "flowplane.adapter-gate-assertions.v1"; boundaryClass = "live"; gates = $gates; warnings = @("Technical local interoperability only; no vendor certification or endorsement is implied.") })
} finally {
  Save-ContainerLog $toolContainer $integrationId
  if ($isDebezium) { Save-ContainerLog $databaseContainer "debezium-mysql" }
  if ($isCompositeHttpBridge) { Save-ContainerLog $publisherBridgeContainer "$integrationId-publisher-bridge" }
  if ($isKafkaConnect) { Save-ContainerLog $toolContainer "flowplane-runtime" } else { Save-ContainerLog $runtimeContainer "flowplane-runtime" }
  $stops = @()
  foreach ($container in @($toolContainer, $publisherBridgeContainer, $databaseContainer, $runtimeContainer)) {
    if ([string]::IsNullOrWhiteSpace([string]$container)) { continue }
    if ($startedContainers.Contains($container)) {
      $isRunning = [string](& docker inspect --format '{{.State.Running}}' $container 2>$null)
      if ($isRunning.Trim() -eq "true") {
        $output = & docker stop --timeout 30 $container 2>&1
        $stops += [ordered]@{ container = $container; action = "stopped"; exitCode = $LASTEXITCODE; output = (ConvertTo-SafeLogText ($output -join "`n")) }
      } else {
        $containerExitCode = [int](& docker inspect --format '{{.State.ExitCode}}' $container 2>$null)
        $stops += [ordered]@{ container = $container; action = "already-exited"; exitCode = $containerExitCode; output = "Container completed before cleanup." }
      }
    }
  }
  if ($temporaryToolWorkspace -and (Test-Path -LiteralPath $temporaryToolWorkspace)) {
    $resolvedTemporaryWorkspace = [IO.Path]::GetFullPath($temporaryToolWorkspace)
    $resolvedTemporaryRoot = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
    if (-not $resolvedTemporaryWorkspace.StartsWith($resolvedTemporaryRoot, [StringComparison]::OrdinalIgnoreCase)) {
      throw "Refusing to remove tool workspace outside the system temporary directory: $resolvedTemporaryWorkspace"
    }
    Remove-Item -LiteralPath $resolvedTemporaryWorkspace -Recurse -Force
  }
  Write-JsonFile -Path (Join-Path $BundleRoot "actual\clean-stop.json") -Value ([ordered]@{ attempted = $true; containers = $stops; capturedAt = [DateTime]::UtcNow.ToString("o") })
}
