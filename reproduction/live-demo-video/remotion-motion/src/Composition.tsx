import {loadFont} from '@remotion/fonts';
import {Video} from '@remotion/media';
import {TransitionSeries, linearTiming} from '@remotion/transitions';
import {fade} from '@remotion/transitions/fade';
import {AbsoluteFill, Composition, Easing, interpolate, Sequence, staticFile, useCurrentFrame} from 'remotion';
import {NativeProofFocusSample, RuntimeTopologySample, VersionShiftSample} from './SampleMotions';

const FPS = 25;
const WIDTH = 1920;
const HEIGHT = 1200;
const INTRO_FRAMES = 100;
const OUTRO_FRAMES = 100;
const TRANSITION_FRAMES = 15;
const SOURCE_FRAMES = 25197;
const PREVIEW_SOURCE_FRAMES = 300;
const fullDuration = INTRO_FRAMES + SOURCE_FRAMES + OUTRO_FRAMES - TRANSITION_FRAMES * 2;
const previewDuration = INTRO_FRAMES + PREVIEW_SOURCE_FRAMES + OUTRO_FRAMES - TRANSITION_FRAMES * 2;

void Promise.all([
  loadFont({family: 'Flowplane UI', url: staticFile('fonts/segoeui.ttf'), weight: '400'}),
  loadFont({family: 'Flowplane UI', url: staticFile('fonts/segoeuib.ttf'), weight: '700'}),
]);

const clamp = {extrapolateLeft: 'clamp', extrapolateRight: 'clamp'} as const;
const ease = Easing.bezier(0.16, 1, 0.3, 1);

const GridBackground: React.FC = () => {
  const frame = useCurrentFrame();
  const drift = interpolate(frame, [0, 100], [0, 22], clamp);
  return (
    <AbsoluteFill
      style={{
        backgroundColor: '#071018',
        backgroundImage: 'radial-gradient(circle at 22% 24%, rgba(45,212,191,.14), transparent 30%), radial-gradient(circle at 82% 74%, rgba(96,165,250,.12), transparent 34%), linear-gradient(rgba(148,163,184,.055) 1px, transparent 1px), linear-gradient(90deg, rgba(148,163,184,.055) 1px, transparent 1px)',
        backgroundSize: 'auto, auto, 56px 56px, 56px 56px',
        backgroundPosition: `0 0, 0 0, ${drift}px ${drift}px, ${drift}px ${drift}px`,
      }}
    />
  );
};

const RuntimeRail: React.FC = () => {
  const frame = useCurrentFrame();
  const line = interpolate(frame, [18, 72], [0, 1], {...clamp, easing: ease});
  const nodes = ['GOVERN', 'VERSION', 'ASSIGN', 'EXECUTE', 'OBSERVE'];
  return (
    <div style={{display: 'flex', alignItems: 'center', width: 1180, marginTop: 74}}>
      {nodes.map((node, index) => {
        const reveal = interpolate(frame, [24 + index * 7, 42 + index * 7], [0, 1], clamp);
        return (
          <div key={node} style={{display: 'contents'}}>
            <div style={{display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 14, opacity: reveal}}>
              <div style={{width: 18, height: 18, borderRadius: 999, background: index === nodes.length - 1 ? '#67e8d2' : '#93c5fd', boxShadow: '0 0 26px rgba(103,232,210,.42)', scale: interpolate(reveal, [0, 1], [0.4, 1])}} />
              <div style={{fontSize: 25, letterSpacing: 4, color: '#a9bdd0', fontWeight: 700}}>{node}</div>
            </div>
            {index < nodes.length - 1 ? (
              <div style={{height: 2, flex: 1, margin: '0 18px 47px', background: '#20354a', overflow: 'hidden'}}>
                <div style={{height: '100%', width: `${line * 100}%`, background: 'linear-gradient(90deg,#60a5fa,#67e8d2)'}} />
              </div>
            ) : null}
          </div>
        );
      })}
    </div>
  );
};

const Intro: React.FC = () => {
  const frame = useCurrentFrame();
  const titleOpacity = interpolate(frame, [8, 30], [0, 1], {...clamp, easing: ease});
  const titleY = interpolate(frame, [8, 34], [58, 0], {...clamp, easing: ease});
  const subtitleOpacity = interpolate(frame, [25, 48], [0, 1], clamp);
  const accentHeight = interpolate(frame, [4, 42], [0, 310], {...clamp, easing: ease});
  return (
    <AbsoluteFill style={{fontFamily: 'Flowplane UI', color: '#f8fafc', overflow: 'hidden'}}>
      <GridBackground />
      <div style={{position: 'absolute', left: 146, top: 336, width: 8, height: accentHeight, background: 'linear-gradient(#67e8d2,#60a5fa)', borderRadius: 10}} />
      <div style={{position: 'relative', height: '100%', display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center'}}>
        <div style={{fontSize: 30, letterSpacing: 10, color: '#67e8d2', fontWeight: 700, opacity: subtitleOpacity}}>GOVERNED STREAMING TRANSFORMATION</div>
        <div style={{fontSize: 154, lineHeight: 1, letterSpacing: -6, fontWeight: 700, marginTop: 34, opacity: titleOpacity, translate: `0 ${titleY}px`}}>FLOWPLANE</div>
        <div style={{fontSize: 49, color: '#b9c8d8', marginTop: 30, opacity: subtitleOpacity}}>One mapping. One connector. One Flink job.</div>
        <RuntimeRail />
      </div>
      <div style={{position: 'absolute', right: 110, bottom: 76, fontSize: 24, letterSpacing: 4, color: '#6f879d'}}>LIVE DEMO / VERIFIED EVIDENCE</div>
    </AbsoluteFill>
  );
};

type Chapter = {atSeconds: number; index: string; title: string; detail: string};
const chapters: Chapter[] = [
  {atSeconds: 1, index: '01', title: 'Clean operational state', detail: 'Teams and login preserved; runtime work starts empty'},
  {atSeconds: 1, index: '02', title: 'Register the connector', detail: 'Kafka Connect profile filled and issued in the live UI'},
  {atSeconds: 1, index: '03', title: 'Register the Flink job', detail: 'Scoped credential renews short-lived runtime access tokens'},
  {atSeconds: 1, index: '04', title: 'Govern and deploy v1', detail: 'One artifact assigned to one connector and one job'},
  {atSeconds: 1, index: '05', title: 'Publish v1.1.0', detail: 'Versioned change on the same mapping'},
  {atSeconds: 1, index: '06', title: 'Connect candidate replay', detail: 'Historical Kafka records processed inside the connector runtime'},
  {atSeconds: 1, index: '07', title: 'Flink schema check', detail: 'Downstream topic schema validated by the Flink job'},
  {atSeconds: 1, index: '08', title: 'Deploy and verify v2', detail: 'Runtime-written Kafka, Mongo, and DLQ evidence'},
  {atSeconds: 1, index: '09', title: 'Lifecycle proof', detail: 'Two immutable versions on one governed mapping'},
];

const ChapterCard: React.FC<Chapter> = ({index, title, detail}) => {
  const frame = useCurrentFrame();
  const enter = interpolate(frame, [0, 20], [0, 1], {...clamp, easing: ease});
  const leave = interpolate(frame, [82, 108], [1, 0], clamp);
  const opacity = Math.min(enter, leave);
  const x = interpolate(frame, [0, 22], [-70, 0], {...clamp, easing: ease});
  const sweepX = interpolate(frame, [0, 90], [-500, 2100], clamp);
  return (
    <AbsoluteFill style={{pointerEvents: 'none', fontFamily: 'Flowplane UI'}}>
      <div style={{position: 'absolute', inset: 0, overflow: 'hidden', opacity: opacity * 0.15}}>
        <div style={{position: 'absolute', top: 0, bottom: 0, width: 180, translate: `${sweepX}px 0`, background: 'linear-gradient(90deg,transparent,#67e8d2,transparent)', filter: 'blur(28px)'}} />
      </div>
      <div style={{position: 'absolute', left: 58, bottom: 190, width: 760, display: 'flex', alignItems: 'stretch', opacity, translate: `${x}px 0`, border: '1px solid rgba(148,163,184,.28)', borderRadius: 12, overflow: 'hidden', boxShadow: '0 22px 70px rgba(2,6,23,.42)', background: 'rgba(5,13,22,.91)'}}>
        <div style={{width: 9, background: 'linear-gradient(#67e8d2,#60a5fa)'}} />
        <div style={{padding: '22px 28px 24px', display: 'flex', flexDirection: 'column', gap: 7}}>
          <div style={{fontSize: 24, color: '#67e8d2', fontWeight: 700, letterSpacing: 5}}>CHAPTER {index}</div>
          <div style={{fontSize: 45, color: '#f8fafc', fontWeight: 700, lineHeight: 1.12}}>{title}</div>
          <div style={{fontSize: 29, color: '#a9bdd0'}}>{detail}</div>
        </div>
      </div>
    </AbsoluteFill>
  );
};

const ScreenRecording: React.FC<{durationInFrames: number; preview: boolean}> = ({durationInFrames, preview}) => {
  const frame = useCurrentFrame();
  const progress = interpolate(frame, [0, Math.max(1, durationInFrames - 1)], [0, 100], clamp);
  const visibleChapters = preview ? chapters.slice(0, 1) : chapters;
  return (
    <AbsoluteFill style={{backgroundColor: '#020617'}}>
      <Video src={staticFile('flowplane-live-screen-demo.mp4')} muted objectFit="cover" style={{width: WIDTH, height: HEIGHT}} />
      <AbsoluteFill style={{background: 'linear-gradient(180deg,rgba(2,6,23,.08),transparent 10%,transparent 90%,rgba(2,6,23,.18))', pointerEvents: 'none'}} />
      {visibleChapters.map((chapter) => (
        <Sequence key={chapter.index} from={Math.round(chapter.atSeconds * FPS)} durationInFrames={110}>
          <ChapterCard {...chapter} />
        </Sequence>
      ))}
      <div style={{position: 'absolute', left: 0, right: 0, bottom: 0, height: 8, background: 'rgba(15,23,42,.7)'}}>
        <div style={{height: '100%', width: `${progress}%`, background: 'linear-gradient(90deg,#60a5fa,#67e8d2)', boxShadow: '0 0 18px rgba(103,232,210,.7)'}} />
      </div>
    </AbsoluteFill>
  );
};

const Outro: React.FC = () => {
  const frame = useCurrentFrame();
  const reveal = interpolate(frame, [5, 34], [0, 1], {...clamp, easing: ease});
  const ring = interpolate(frame, [8, 50], [565, 0], {...clamp, easing: ease});
  const check = interpolate(frame, [42, 70], [90, 0], {...clamp, easing: ease});
  return (
    <AbsoluteFill style={{fontFamily: 'Flowplane UI', color: '#f8fafc', alignItems: 'center', justifyContent: 'center', overflow: 'hidden'}}>
      <GridBackground />
      <div style={{position: 'relative', display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 27, opacity: reveal, scale: interpolate(reveal, [0, 1], [0.94, 1])}}>
        <svg width="230" height="230" viewBox="0 0 230 230">
          <circle cx="115" cy="115" r="90" fill="rgba(16,185,129,.08)" stroke="#67e8d2" strokeWidth="8" strokeDasharray="565" strokeDashoffset={ring} transform="rotate(-90 115 115)" />
          <path d="M72 116 L103 147 L162 87" fill="none" stroke="#67e8d2" strokeWidth="12" strokeLinecap="round" strokeLinejoin="round" strokeDasharray="90" strokeDashoffset={check} />
        </svg>
        <div style={{fontSize: 28, color: '#67e8d2', fontWeight: 700, letterSpacing: 8}}>VERIFIED LIVE RUN / PASS</div>
        <div style={{fontSize: 96, fontWeight: 700, letterSpacing: -3}}>CONNECT + FLINK PROOF COMPLETE</div>
        <div style={{fontSize: 39, color: '#b9c8d8'}}>one connector · one Flink job · replay + schema gates</div>
      </div>
      <div style={{position: 'absolute', bottom: 64, fontSize: 25, color: '#70879c', letterSpacing: 4}}>FLOWPLANE / GOVERNED STREAMING TRANSFORMATION</div>
    </AbsoluteFill>
  );
};

const MotionDemo: React.FC<{sourceFrames: number; preview: boolean}> = ({sourceFrames, preview}) => (
  <TransitionSeries>
    <TransitionSeries.Sequence durationInFrames={INTRO_FRAMES}><Intro /></TransitionSeries.Sequence>
    <TransitionSeries.Transition presentation={fade()} timing={linearTiming({durationInFrames: TRANSITION_FRAMES})} />
    <TransitionSeries.Sequence durationInFrames={sourceFrames}><ScreenRecording durationInFrames={sourceFrames} preview={preview} /></TransitionSeries.Sequence>
    <TransitionSeries.Transition presentation={fade()} timing={linearTiming({durationInFrames: TRANSITION_FRAMES})} />
    <TransitionSeries.Sequence durationInFrames={OUTRO_FRAMES}><Outro /></TransitionSeries.Sequence>
  </TransitionSeries>
);

export const FlowplaneCompositions: React.FC = () => (
  <>
    <Composition id="FlowplaneMotion" component={MotionDemo} defaultProps={{sourceFrames: SOURCE_FRAMES, preview: false}} durationInFrames={fullDuration} fps={FPS} width={WIDTH} height={HEIGHT} />
    <Composition id="FlowplaneMotionPreview" component={MotionDemo} defaultProps={{sourceFrames: PREVIEW_SOURCE_FRAMES, preview: true}} durationInFrames={previewDuration} fps={FPS} width={WIDTH} height={HEIGHT} />
    <Composition id="FlowplaneIntro" component={Intro} durationInFrames={INTRO_FRAMES} fps={FPS} width={WIDTH} height={HEIGHT} />
    <Composition id="FlowplaneOutro" component={Outro} durationInFrames={OUTRO_FRAMES} fps={FPS} width={WIDTH} height={HEIGHT} />
    {chapters.map((chapter) => (
      <Composition
        key={chapter.index}
        id={`FlowplaneChapter${chapter.index}`}
        component={ChapterCard}
        defaultProps={chapter}
        durationInFrames={110}
        fps={FPS}
        width={WIDTH}
        height={HEIGHT}
      />
    ))}
    <Composition id="SampleRuntimeTopology" component={RuntimeTopologySample} durationInFrames={150} fps={FPS} width={WIDTH} height={HEIGHT} />
    <Composition id="SampleVersionShift" component={VersionShiftSample} durationInFrames={150} fps={FPS} width={WIDTH} height={HEIGHT} />
    <Composition id="SampleNativeProofFocus" component={NativeProofFocusSample} durationInFrames={150} fps={FPS} width={WIDTH} height={HEIGHT} />
  </>
);
