param([switch]$RefreshAssets)

$ErrorActionPreference = 'Stop'
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..\..')).Path
$evidenceRoot = Join-Path $projectRoot 'evidence\demo\live-demo'
$assetsRoot = Join-Path $evidenceRoot 'motion-assets'
$sourceVideo = Join-Path $evidenceRoot 'flowplane-live-screen-demo.mp4'
$outputVideo = Join-Path $evidenceRoot 'flowplane-live-screen-demo-motion.mp4'
$manifestPath = Join-Path $evidenceRoot 'flowplane-live-screen-video-manifest.json'
$reportPath = Join-Path $evidenceRoot 'live-demo-report.json'
$narrationCuesPath = Join-Path $PSScriptRoot 'narration-cues.json'
$narrationCaptionsPath = Join-Path $assetsRoot 'detailed-narration-captions.json'
$narrationAssPath = Join-Path $assetsRoot 'detailed-narration.ass'
$narrationManifestPath = Join-Path $evidenceRoot 'motion-narration-manifest.json'

foreach ($path in @($sourceVideo, $manifestPath, $reportPath, $narrationCuesPath)) {
    if (-not (Test-Path -LiteralPath $path)) { throw "Required current-run input is missing: $path" }
}
$manifest = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json
$report = Get-Content -LiteralPath $reportPath -Raw | ConvertFrom-Json
if ($manifest.status -ne 'PASS' -or $report.status -ne 'PASS') { throw 'Motion rendering requires PASS from both UI recording and evidence report.' }
if ($manifest.runId -ne $report.metadata.runId -or $manifest.gitCommit -ne $report.metadata.gitCommit) { throw 'Manifest/report run or commit mismatch.' }
$chapters = @($manifest.chapterStarts)
if ($chapters.Count -ne 9) { throw "Expected 9 recorded chapters, found $($chapters.Count)." }

$sourceDurationText = & ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 $sourceVideo
if ($LASTEXITCODE -ne 0) { throw 'ffprobe could not read the source video.' }
$sourceDuration = [double]::Parse(($sourceDurationText | Select-Object -Last 1), [Globalization.CultureInfo]::InvariantCulture)
if ($sourceDuration -le 10) { throw "Source video duration is not credible: $sourceDuration seconds." }

New-Item -ItemType Directory -Force -Path $assetsRoot | Out-Null

function Format-AssTimestamp([double]$seconds) {
    $bounded = [Math]::Max(0, $seconds)
    $span = [TimeSpan]::FromSeconds($bounded)
    return '{0}:{1:00}:{2:00}.{3:00}' -f [Math]::Floor($span.TotalHours), $span.Minutes, $span.Seconds, [Math]::Floor($span.Milliseconds / 10)
}

function Escape-AssText([string]$value) {
    return $value.Replace('\', '\\').Replace('{', '\{').Replace('}', '\}')
}

$narrationCues = Get-Content -LiteralPath $narrationCuesPath -Raw | ConvertFrom-Json
if ($narrationCues.Count -lt 20) { throw "Detailed narration is incomplete: only $($narrationCues.Count) cues were found." }
$resolvedNarration = [Collections.Generic.List[object]]::new()
for ($i = 0; $i -lt $narrationCues.Count; $i++) {
    $cue = $narrationCues[$i]
    if ($cue.PSObject.Properties.Name -contains 'sourceSeconds') {
        $start = [double]$cue.sourceSeconds
    } else {
        $chapterIndex = [int]$cue.chapterIndex
        if ($chapterIndex -lt 0 -or $chapterIndex -ge $chapters.Count) { throw "Narration cue $i references invalid chapter index $chapterIndex." }
        $start = [double]$chapters[$chapterIndex].atSeconds + [double]$cue.offsetSeconds
    }
    if ($i -lt $narrationCues.Count - 1) {
        $nextCue = $narrationCues[$i + 1]
        if ($nextCue.PSObject.Properties.Name -contains 'sourceSeconds') {
            $nextStart = [double]$nextCue.sourceSeconds
        } else {
            $nextStart = [double]$chapters[[int]$nextCue.chapterIndex].atSeconds + [double]$nextCue.offsetSeconds
        }
    } else {
        $nextStart = $sourceDuration
    }
    $end = [Math]::Min($sourceDuration, $nextStart)
    if ($start -lt 0 -or $start -ge $sourceDuration -or $end -le $start) { throw "Narration cue $i has invalid resolved timing $start-$end." }
    $resolvedNarration.Add([ordered]@{
        text = "$($cue.title)`n$($cue.detail)"
        startMs = [int][Math]::Round($start * 1000)
        endMs = [int][Math]::Round($end * 1000)
        timestampMs = $null
        confidence = $null
    })
}
$resolvedNarration | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $narrationCaptionsPath -Encoding utf8

$assLines = [Collections.Generic.List[string]]::new()
$assLines.Add('[Script Info]')
$assLines.Add('ScriptType: v4.00+')
$assLines.Add('PlayResX: 1920')
$assLines.Add('PlayResY: 1200')
$assLines.Add('WrapStyle: 0')
$assLines.Add('ScaledBorderAndShadow: yes')
$assLines.Add('')
$assLines.Add('[V4+ Styles]')
$assLines.Add('Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding')
$assLines.Add('Style: Narration,Segoe UI,31,&H00F8FAFC,&H00F8FAFC,&H00000000,&H00000000,0,0,0,0,100,100,0,0,1,1.4,0,2,92,92,24,1')
$assLines.Add('')
$assLines.Add('[Events]')
$assLines.Add('Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text')
for ($i = 0; $i -lt $resolvedNarration.Count; $i++) {
    $caption = $resolvedNarration[$i]
    $lines = @($caption.text -split "`n", 2)
    $title = Escape-AssText $lines[0]
    $detail = Escape-AssText $lines[1]
    $startText = Format-AssTimestamp ($caption.startMs / 1000.0)
    $endText = Format-AssTimestamp ($caption.endMs / 1000.0)
    $eventText = "{\b1\fs25\c&H00E8D27D&\q2}$title{\r\q2}\N$detail"
    $assLines.Add("Dialogue: 0,$startText,$endText,Narration,,0,0,0,,$eventText")
}
$assLines | Set-Content -LiteralPath $narrationAssPath -Encoding utf8

$assets = @(
    @{ Composition = 'FlowplaneIntro'; File = 'intro.mp4'; Arguments = @('--codec=h264', '--crf=18') },
    @{ Composition = 'FlowplaneOutro'; File = 'outro.mp4'; Arguments = @('--codec=h264', '--crf=18') }
)
foreach ($index in 1..9) {
    $id = $index.ToString('00')
    $assets += @{ Composition = "FlowplaneChapter$id"; File = "chapter-$id.mov"; Arguments = @('--codec=prores', '--prores-profile=4444', '--pixel-format=yuva444p10le', '--image-format=png') }
}

Push-Location $PSScriptRoot
try {
    foreach ($asset in $assets) {
        $assetPath = Join-Path $assetsRoot $asset.File
        if ($RefreshAssets -or -not (Test-Path -LiteralPath $assetPath)) {
            & npx.cmd remotion render $asset.Composition $assetPath @($asset.Arguments)
            if ($LASTEXITCODE -ne 0) { throw "Remotion failed while rendering $($asset.Composition)." }
        }
    }
}
finally { Pop-Location }

$filterParts = [Collections.Generic.List[string]]::new()
$filterParts.Add('[0:v]fps=25,scale=in_range=pc:out_range=tv,format=yuv420p,settb=AVTB,setpts=PTS-STARTPTS[intro]')
$filterParts.Add('[1:v]fps=25,format=rgba,settb=AVTB,setpts=PTS-STARTPTS[base]')
$filterParts.Add('[2:v]fps=25,scale=in_range=pc:out_range=tv,format=yuv420p,settb=AVTB,setpts=PTS-STARTPTS[outro]')
$previous = 'base'
for ($i = 0; $i -lt $chapters.Count; $i++) {
    $inputIndex = $i + 3
    $overlay = "overlay$($i + 1)"
    $next = "base$($i + 1)"
    $at = [double]$chapters[$i].atSeconds
    if ($at -lt 0 -or $at -ge $sourceDuration) { throw "Chapter $($i + 1) timestamp $at is outside the source video." }
    $atText = $at.ToString('0.###', [Globalization.CultureInfo]::InvariantCulture)
    $filterParts.Add("[$inputIndex`:v]fps=25,format=rgba,settb=AVTB,setpts=PTS-STARTPTS+$atText/TB[$overlay]")
    $filterParts.Add("[$previous][$overlay]overlay=0:0:eof_action=pass:format=auto[$next]")
    $previous = $next
}
$outroOffset = ($sourceDuration + 2.8).ToString('0.###', [Globalization.CultureInfo]::InvariantCulture)
$firstNarrationSeconds = ($resolvedNarration[0].startMs / 1000.0).ToString('0.###', [Globalization.CultureInfo]::InvariantCulture)
$lastNarrationSeconds = ($resolvedNarration[$resolvedNarration.Count - 1].endMs / 1000.0).ToString('0.###', [Globalization.CultureInfo]::InvariantCulture)
$assFilterPath = $narrationAssPath.Replace('\', '/').Replace(':', '\:').Replace("'", "\'")
$filterParts.Add("[$previous]drawbox=x=0:y=ih-172:w=iw:h=172:color=0x050d16@0.94:t=fill:enable='between(t,$firstNarrationSeconds,$lastNarrationSeconds)',drawbox=x=0:y=ih-172:w=iw:h=3:color=0x67e8d2@0.75:t=fill:enable='between(t,$firstNarrationSeconds,$lastNarrationSeconds)',subtitles=filename='$assFilterPath':alpha=1,format=yuv420p,settb=AVTB[source]")
$filterParts.Add('[intro][source]xfade=transition=fade:duration=0.6:offset=3.4[first]')
$filterParts.Add("[first][outro]xfade=transition=fade:duration=0.6:offset=$outroOffset,format=yuv420p,setparams=range=tv[final]")

$ffmpegArguments = @('-y', '-i', (Join-Path $assetsRoot 'intro.mp4'), '-i', $sourceVideo, '-i', (Join-Path $assetsRoot 'outro.mp4'))
foreach ($index in 1..9) { $ffmpegArguments += @('-i', (Join-Path $assetsRoot "chapter-$($index.ToString('00')).mov")) }
$ffmpegArguments += @('-filter_complex', ($filterParts -join ';'), '-map', '[final]', '-an', '-c:v', 'libx264', '-preset', 'fast', '-crf', '18', '-pix_fmt', 'yuv420p', '-movflags', '+faststart', $outputVideo)
& ffmpeg @ffmpegArguments
if ($LASTEXITCODE -ne 0) { throw 'FFmpeg failed while assembling the evidence-gated final video.' }

$finalDurationText = & ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 $outputVideo
if ($LASTEXITCODE -ne 0 -or -not $finalDurationText) { throw 'Final video verification failed.' }
$narrationManifest = [ordered]@{
    status = 'PASS'
    generatedAt = (Get-Date).ToUniversalTime().ToString('o')
    runId = $manifest.runId
    gitCommit = $manifest.gitCommit
    captionCount = $resolvedNarration.Count
    captionFile = 'motion-assets/detailed-narration-captions.json'
    sourceVideo = 'flowplane-live-screen-demo.mp4'
    outputVideo = 'flowplane-live-screen-demo-motion.mp4'
    policy = 'Detailed lower-third narration only; recorded runtime evidence is unchanged.'
    persistencePolicy = 'Each narration remains visible until the next narration starts.'
}
$narrationManifest | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $narrationManifestPath -Encoding utf8
Write-Host "Flowplane evidence-gated motion video written to $outputVideo"
