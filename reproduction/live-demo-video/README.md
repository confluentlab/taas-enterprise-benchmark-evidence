# How the live walkthrough video was produced

This directory preserves the publication copy of the scripts, Remotion compositions, and narration data used to turn the verified Flowplane screen recording into the final captioned walkthrough.

The video treatment is deliberately separate from the evidence itself. Remotion creates the intro, outro, and chapter cards. FFmpeg places those graphics and the explanatory captions over the original screen capture. Neither tool changes the application state, runtime results, Kafka records, DLQ records, Mongo documents, telemetry, or audit events visible in the recording.

## Start with these files

| File | Purpose |
|---|---|
| [`10-generate-demo-video.ps1`](orchestration/10-generate-demo-video.ps1) | Orchestrates the live recording, checks the runtime write boundary, converts the raw capture, and starts the evidence-gated final render |
| [`render-motion-video.ps1`](remotion-motion/render-motion-video.ps1) | Verifies the run identity, resolves narration timing, renders the motion assets, and assembles the final H.264 MP4 |
| [`Composition.tsx`](remotion-motion/src/Composition.tsx) | Defines the intro, outro, nine chapter overlays, dimensions, frame rate, and motion treatment |
| [`narration-cues.json`](remotion-motion/narration-cues.json) | Human-authored titles and explanations, anchored either to a chapter or an exact source-video time |
| [`detailed-narration-captions.json`](detailed-narration-captions.json) | The 58 resolved captions with their final start and end times |
| [`package.json`](remotion-motion/package.json) and [`package-lock.json`](remotion-motion/package-lock.json) | Locked Node.js and Remotion dependencies used by the rendering project |
| [`source-snapshot.json`](source-snapshot.json) | Provenance and limitations of this public script snapshot |

The finished media and run-level identity are recorded in the [video provenance manifest](../../evidence/live-demo/video-manifest.json).

## Generation flow

1. **Record the live workflow.** The orchestration script resets the demo state, prepares Kafka Connect and Flink, checks that the producer cannot write to downstream destinations, and launches the browser recorder.
2. **Accept only a passing run.** Before rendering, the script requires `PASS` from both the screen-recording manifest and the live evidence report. Their run ID and Git revision must agree.
3. **Normalize the source capture.** FFmpeg converts the browser recording to a 1920 × 1200 H.264 source video with a browser-friendly pixel format.
4. **Resolve the narration timeline.** The render script combines the nine recorded chapter timestamps with the human-authored narration cues. Each explanation remains visible until the next cue begins.
5. **Render motion assets.** Remotion renders the intro, outro, and nine transparent chapter overlays at 25 frames per second.
6. **Assemble the final video.** FFmpeg overlays the chapter cards, adds the persistent lower-third caption area, burns in the narration, adds short fades, removes audio, and writes the final H.264 MP4.
7. **Preserve identity.** The evidence repository records the final file size, duration, format, and SHA-256 digest and validates them centrally.

## Requirements

- Windows PowerShell 5.1 or PowerShell 7
- Node.js and npm
- FFmpeg and FFprobe on `PATH`
- a complete Flowplane control-plane checkout for the live capture and private run report
- the original uncaptioned screen recording
- Segoe UI regular and bold font files available from a licensed Windows installation

The font binaries, generated motion clips, `node_modules`, build cache, uncaptioned source video, and private live-run report are intentionally not duplicated here.

## Inspect the rendering project

From [`remotion-motion/`](remotion-motion/):

```powershell
npm ci
npm run lint
npm run dev
```

Remotion Studio can preview the compositions after the uncaptioned recording is placed at `public/flowplane-live-screen-demo.mp4` and the required fonts are placed under `public/fonts/`.

The published project was checked with `npm ci` and `npm run lint`. The preserved lockfile currently reports two low-severity advisories in ESLint development tooling and no moderate, high, or critical findings. The dependency versions remain pinned to the inspected rendering project instead of being silently upgraded in an evidence snapshot.

## Run the original full render

In a complete Flowplane control-plane checkout, the original command was:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass `
  -File .\scripts\demo\10-generate-demo-video.ps1
```

The final assembly step can be rerun from the Remotion project with:

```powershell
npm ci
npm run render:full
```

These copied scripts retain the original source-tree-relative paths so reviewers can inspect the exact control flow. They are not a standalone replacement for the omitted Flowplane source tree, browser recorder, live runtime environment, or private evidence inputs.

## Provenance boundary

The run records source revision `10a26df4d7ed6a41f8076a5d7280d73db543c13a`, but the video-generation files were present in a dirty development worktree. Three rendering files were modified and the narration cue file was untracked at the time this public snapshot was collected. The historical run did not preserve execution-time SHA-256 values for those scripts.

For that reason, this directory is classified as `SOURCE_INSPECTED`. It is the preserved post-run source snapshot that explains the generation path; it is not claimed as a byte-for-byte, execution-time build attestation. The publication copy normalizes text files to LF and changes the root package license metadata from `UNLICENSED` to `Apache-2.0`; the rendering logic and narration content are otherwise copied from the inspected worktree.

All files in this directory are covered by the repository-wide [checksum inventory](../../evidence/checksums.sha256). The central evidence validator also confirms that the documented pipeline files and provenance record remain present.
