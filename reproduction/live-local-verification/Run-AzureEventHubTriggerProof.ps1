param(
  [string]$FlowplaneRoot = "C:\FlowPlaneNew\repositories\flowplane-controlplane",
  [string]$FixtureRoot = "C:\FlowPlaneNew\video-generation-scripts-copy\artifacts\live-local-verification\canonical-fixture",
  [string]$OutputRoot = "C:\FlowPlaneNew\video-generation-scripts-copy\artifacts\live-local-verification\evidence\trigger-proofs\azure-eventhub"
)
$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "LiveVerification.Common.ps1")
$env:FLOWPLANE_ROOT = $FlowplaneRoot; $env:FLOWPLANE_DEMO_OUTPUT_ROOT = Join-Path $OutputRoot "adapter-private"
. (Join-Path $PSScriptRoot "..\FlowplaneDemo.Common.ps1")
$run = [DateTime]::UtcNow.ToString("yyyyMMddTHHmmssZ").ToLowerInvariant(); $bundle = Join-Path $OutputRoot $run
New-Item -ItemType Directory -Force -Path (Join-Path $bundle "actual"), (Join-Path $bundle "logs") | Out-Null
$container = "flowplane-azure-eventhub-trigger-$run"; $dlqQueue = "fp-eh-dlq-$run"
$runtimeId = "azure-eventhub-evidence-$run"
$azuriteKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
$storageInternal = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=$azuriteKey;BlobEndpoint=http://flowplane-serverless-azurite:10000/devstoreaccount1;QueueEndpoint=http://flowplane-serverless-azurite:10001/devstoreaccount1;TableEndpoint=http://flowplane-serverless-azurite:10002/devstoreaccount1;"
$storageHost = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=$azuriteKey;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;"
$eventHubInternal = "Endpoint=sb://flowplane-serverless-eventhubs;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"
$eventHubHost = "Endpoint=sb://127.0.0.1;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"
$token = New-FlowplaneAccessToken
function Invoke-Api([string]$Method, [string]$Path, $Body = $null) {
  $headers = @{ Authorization = "Bearer $token"; tenantId = "acme-corp"; "X-Tenant-Id" = "acme-corp" }; $request = @{ Method=$Method; Uri="http://127.0.0.1:8081$Path"; Headers=$headers; TimeoutSec=30 }
  if ($null -ne $Body) { $request.ContentType="application/json"; $request.Body=$Body | ConvertTo-Json -Depth 20 -Compress }; Invoke-RestMethod @request
}
try {
  docker run -d --name $container --network flowplane-quality-stack_default -p "127.0.0.1::80" `
    -e "AzureWebJobsStorage=$storageInternal" -e "AzureWebJobsSecretStorageType=files" -e "AzureWebJobs.flowplaneQueueTransform.Disabled=true" `
    -e "FLOWPLANE_AZURE_INPUT_EVENT_HUB=flowplane-serverless-input" -e "FLOWPLANE_AZURE_OUTPUT_EVENT_HUB=flowplane-serverless-output" `
    -e "FLOWPLANE_AZURE_EVENT_HUB_CONNECTION=$eventHubInternal" -e "FLOWPLANE_AZURE_EVENT_HUB_CONSUMER_GROUP=flowplane-local" -e "FLOWPLANE_AZURE_DLQ_QUEUE=$dlqQueue" `
    -e "FLOWPLANE_SERVERLESS_CONTROL_PLANE_URL=http://flowplane-backend:8080" -e "FLOWPLANE_SERVERLESS_TENANT_ID=acme-corp" `
    -e "FLOWPLANE_SERVERLESS_RUNTIME_ID=$runtimeId" -e "FLOWPLANE_SERVERLESS_RUNTIME_NAME=Azure Event Hub Trigger Evidence" `
    -e "FLOWPLANE_SERVERLESS_AUTH_TOKEN=$token" flowplane-azure-functions-local | Out-Null
  Start-Sleep -Seconds 15
  $hostPort = [int]((docker port $container "80/tcp" | Select-Object -First 1) -replace '^.*:',''); $probe=Get-Content -LiteralPath (Join-Path $FixtureRoot "valid-input.jsonl") -TotalCount 1
  try { Invoke-WebRequest -UseBasicParsing -Method Post -Uri "http://127.0.0.1:$hostPort/api/flowplaneHttpTransform" -ContentType "application/json" -Body $probe -TimeoutSec 30 | Out-Null } catch { if (-not $_.Exception.Response -or [int]$_.Exception.Response.StatusCode -notin @(422,503)) { throw } }
  $deadline=(Get-Date).AddMinutes(2); do { try{$runtime=Invoke-Api GET "/api/v1/runtimes/$runtimeId"}catch{};if($runtime){break};Start-Sleep 2}while((Get-Date)-lt $deadline);if(-not $runtime){throw "Fresh Event Hub runtime did not register."}
  Invoke-Api POST "/api/v1/mappings/6a5efbb08dd158f5c963161b/deploy" @{runtimeIds=@($runtimeId);rolloutPercent=100;requireReplayGate=$false;reason="Azure Event Hub trigger proof";changeTicket="AZEH-$run"}|Out-Null
  $deadline=(Get-Date).AddMinutes(2);do{try{Invoke-WebRequest -UseBasicParsing -Method Post -Uri "http://127.0.0.1:$hostPort/api/flowplaneHttpTransform" -ContentType "application/json" -Body $probe -TimeoutSec 30|Out-Null}catch{};$runtime=Invoke-Api GET "/api/v1/runtimes/$runtimeId";if($runtime.activeArtifactId){break};Start-Sleep 2}while((Get-Date)-lt $deadline);if(-not $runtime.activeArtifactId){throw "Event Hub runtime did not acknowledge its mapping."}
  $client = Join-Path $PSScriptRoot "assets\serverless-event-node\azure-eventhub-proof.mjs"
  $oldPreference=$ErrorActionPreference;$ErrorActionPreference="Continue";try{$output = & node $client $eventHubHost "flowplane-serverless-input" "flowplane-serverless-output" '$Default' $storageHost $dlqQueue $FixtureRoot $bundle 2>&1;$clientExit=$LASTEXITCODE}finally{$ErrorActionPreference=$oldPreference}
  if ($clientExit -ne 0) { throw ($output -join "`n") }
  & node (Join-Path $PSScriptRoot "assets\serverless-event-node\validate-trigger-output.mjs") $FixtureRoot $bundle (Join-Path $PSScriptRoot "..\..\..\artifacts\live-local-verification\evidence\integration-proofs\serverless-azure\20260721T045510Z\expected\simulation-batch.json") | Out-Null
  if ($LASTEXITCODE -ne 0) { throw "Azure Event Hub content validation failed." }
  docker logs $container 2>&1 | Set-Content -Encoding utf8 (Join-Path $bundle "logs\azure-functions.log")
  Write-JsonFile (Join-Path $bundle "proof-manifest.json") ([ordered]@{ status="PASS"; runId=$run; trigger="Azure EventHubTrigger"; runtimeId=$runtimeId; artifactId=$runtime.activeArtifactId; artifactHash=$runtime.activeArtifactHash; validRecords=100; invalidRecords=10; sourceBoundary="Verifier writes only the emulator input Event Hub"; sinkBoundary="Azure Functions EventHubTrigger writes output Event Hub and Azurite DLQ"; emulatorImage=(docker inspect flowplane-serverless-eventhubs --format '{{.Image}}'); functionImage=(docker inspect $container --format '{{.Image}}'); completedAt=[DateTime]::UtcNow.ToString("o") })
  Write-Output "PASS $bundle"
} finally {
  if (docker ps -aq --filter "name=^$container$") { docker logs $container 2>&1 | Set-Content -Encoding utf8 (Join-Path $bundle "logs\azure-functions-final.log"); docker rm -f $container | Out-Null }
}
