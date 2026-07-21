$env:FLOWPLANE_DEMO_RUN_ID = "flowplane-live-demo-$((Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssZ'))-$([Guid]::NewGuid().ToString('N').Substring(0,8))"
. "$PSScriptRoot\FlowplaneDemo.Common.ps1"

$ErrorActionPreference = "Stop"

$ffmpeg = (Get-Command ffmpeg -ErrorAction Stop).Source
$node = (Get-Command node -ErrorAction Stop).Source
$recorder = Join-Path $PSScriptRoot "record-connect-flink-live-demo.mjs"
$webmPath = Join-Path $script:FLOWPLANE_DEMO_ROOT "flowplane-live-screen-demo.webm"
$mp4Path = Join-Path $script:FLOWPLANE_DEMO_ROOT "flowplane-live-screen-demo.mp4"
$motionPath = Join-Path $script:FLOWPLANE_DEMO_ROOT "flowplane-live-screen-demo-motion.mp4"
$manifestPath = Join-Path $script:FLOWPLANE_DEMO_ROOT "flowplane-live-screen-video-manifest.json"

function Invoke-DemoScript([string]$Name) {
  & powershell -NoProfile -ExecutionPolicy Bypass -File (Join-Path $PSScriptRoot $Name)
  if ($LASTEXITCODE -ne 0) { throw "$Name failed with exit code $LASTEXITCODE" }
}

$staleRunArtifacts = @(
  $webmPath,
  $mp4Path,
  $motionPath,
  $manifestPath,
  (Join-Path $script:FLOWPLANE_DEMO_ROOT "live-demo-report.json"),
  (Join-Path $script:FLOWPLANE_DEMO_ROOT "ui-verification-report.json"),
  (Join-Path $script:FLOWPLANE_DEMO_ROOT "flowplane-live-screen-video.md")
)
foreach ($file in $staleRunArtifacts) {
  if (Test-Path -LiteralPath $file) { Remove-Item -LiteralPath $file -Force }
}

Invoke-DemoScript "01-reset-demo-state.ps1"
Invoke-DemoScript "00-prepare-connect-flink-demo.ps1"
Invoke-DemoScript "assert-runtime-write-boundary.ps1"

& $node $recorder
if ($LASTEXITCODE -ne 0) { throw "Connect + Flink live recorder failed with exit code $LASTEXITCODE" }
if (-not (Test-Path -LiteralPath $webmPath)) { throw "Recorder did not produce $webmPath" }

& powershell -NoProfile -ExecutionPolicy Bypass -File (Join-Path $PSScriptRoot "assert-runtime-write-boundary.ps1") -PostRun
if ($LASTEXITCODE -ne 0) { throw "Post-run runtime write-boundary verification failed." }

& $ffmpeg -y -hide_banner -loglevel error -i $webmPath -vf "scale=1920:1200:flags=lanczos,pad=1920:1200:0:0,format=yuv420p" -c:v libx264 -profile:v high -level 4.2 -movflags +faststart $mp4Path
if ($LASTEXITCODE -ne 0) { throw "FFmpeg conversion failed with exit code $LASTEXITCODE" }

$manifest = Read-Json $manifestPath
$report = Read-Json (Join-Path $script:FLOWPLANE_DEMO_ROOT "live-demo-report.json")
if ($manifest.status -ne "PASS" -or $report.status -ne "PASS") { throw "The live run is not eligible for final rendering." }
if ($manifest.runId -ne $report.metadata.runId -or $manifest.gitCommit -ne $report.metadata.gitCommit) {
  throw "Video manifest and evidence report do not describe the same run and commit."
}
$manifest | Add-Member -NotePropertyName mp4 -NotePropertyValue $mp4Path -Force
$manifest | Add-Member -NotePropertyName mp4Bytes -NotePropertyValue (Get-Item $mp4Path).Length -Force
$manifest | Add-Member -NotePropertyName webmBytes -NotePropertyValue (Get-Item $webmPath).Length -Force
Save-Json -Path $manifestPath -Value $manifest

& powershell -NoProfile -ExecutionPolicy Bypass -File (Join-Path $PSScriptRoot "remotion-motion\render-motion-video.ps1") -RefreshAssets
if ($LASTEXITCODE -ne 0) { throw "Evidence-gated motion assembly failed." }
if (-not (Test-Path -LiteralPath $motionPath)) { throw "Final motion video was not generated." }

Write-MarkdownSummary -Path (Join-Path $script:FLOWPLANE_DEMO_ROOT "flowplane-live-screen-video.md") -Title "Flowplane Connect + Flink Live Demo Video" -Lines @(
  "- Status: PASS",
  "- Run ID: $($manifest.runId)",
  "- Runtime meaning: one Kafka Connect connector and one Flink job",
  "- Connect registration: live Flowplane UI",
  "- Flink registration: visible PowerShell command submitting the live job",
  "- Control Center: zero-connector baseline, live connector creation, and Kafka topic before/after evidence",
  "- Source capture: flowplane-live-screen-demo.mp4",
  "- Final video: flowplane-live-screen-demo-motion.mp4",
  "- Evidence: live-demo-report.json and flowplane-live-screen-video-manifest.json"
)
Write-Pass "Generated evidence-bound Connect + Flink live demo video: $motionPath"
exit 0
