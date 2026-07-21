import {Video} from '@remotion/media';
import {AbsoluteFill, Easing, interpolate, staticFile, useCurrentFrame} from 'remotion';

const FPS = 25;
const clamp = {extrapolateLeft: 'clamp', extrapolateRight: 'clamp'} as const;
const ease = Easing.bezier(0.16, 1, 0.3, 1);

const RecordedUi: React.FC<{trimSeconds: number}> = ({trimSeconds}) => (
  <Video
    src={staticFile('flowplane-live-screen-demo.mp4')}
    trimBefore={trimSeconds * FPS}
    muted
    objectFit="cover"
    style={{width: '100%', height: '100%'}}
  />
);

const SampleLabel: React.FC<{number: string; title: string}> = ({number, title}) => (
  <div style={{position: 'absolute', left: 54, top: 46, padding: '13px 20px', borderRadius: 8, background: 'rgba(4,12,20,.9)', border: '1px solid rgba(103,232,210,.35)', color: '#e6f1fb', fontFamily: 'Flowplane UI', fontSize: 23, letterSpacing: 2.5}}>
    <span style={{color: '#67e8d2', fontWeight: 700}}>SAMPLE {number}</span>
    <span style={{color: '#7890a6'}}> / </span>
    {title}
  </div>
);

export const RuntimeTopologySample: React.FC = () => {
  const frame = useCurrentFrame();
  const enter = interpolate(frame, [10, 34], [0, 1], {...clamp, easing: ease});
  const progress = interpolate(frame, [35, 118], [0, 1], {...clamp, easing: ease});
  const nodes = ['SOURCE', 'FLOWPLANE', 'TRANSFORMED', 'DLQ'];

  return (
    <AbsoluteFill style={{fontFamily: 'Flowplane UI', overflow: 'hidden'}}>
      <RecordedUi trimSeconds={83} />
      <AbsoluteFill style={{background: 'linear-gradient(180deg,rgba(2,6,23,.24),transparent 34%)'}} />
      <SampleLabel number="01" title="LIVE RUNTIME PATH" />
      <div style={{position: 'absolute', left: 250, right: 250, top: 205, height: 210, borderRadius: 18, background: 'rgba(4,13,22,.92)', border: '1px solid rgba(148,163,184,.28)', boxShadow: '0 28px 80px rgba(2,6,23,.48)', opacity: enter, translate: `0 ${interpolate(enter, [0, 1], [-38, 0])}px`}}>
        <div style={{position: 'absolute', left: 110, right: 110, top: 93, height: 3, background: '#21384b'}}>
          <div style={{height: '100%', width: `${progress * 100}%`, background: 'linear-gradient(90deg,#60a5fa,#67e8d2)', boxShadow: '0 0 18px rgba(103,232,210,.75)'}} />
        </div>
        <div style={{height: '100%', padding: '48px 80px 28px', display: 'flex', justifyContent: 'space-between'}}>
          {nodes.map((node, index) => {
            const active = interpolate(progress, [index / 3 - 0.08, index / 3 + 0.06], [0, 1], clamp);
            return (
              <div key={node} style={{display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 25, zIndex: 1}}>
                <div style={{width: 38, height: 38, borderRadius: 999, background: active > 0.5 ? '#67e8d2' : '#172b3c', border: '5px solid #07121e', boxShadow: active > 0.5 ? '0 0 0 8px rgba(103,232,210,.13), 0 0 30px rgba(103,232,210,.55)' : 'none', scale: interpolate(active, [0, 1], [0.82, 1])}} />
                <div style={{fontSize: 24, fontWeight: 700, letterSpacing: 3, color: active > 0.5 ? '#effdfb' : '#70869a'}}>{node}</div>
              </div>
            );
          })}
        </div>
      </div>
      <div style={{position: 'absolute', right: 84, bottom: 62, color: '#67e8d2', fontSize: 25, letterSpacing: 3, fontWeight: 700, opacity: interpolate(frame, [92, 118], [0, 1], clamp)}}>RUNTIME-WRITTEN RECORDS ✓</div>
    </AbsoluteFill>
  );
};

export const VersionShiftSample: React.FC = () => {
  const frame = useCurrentFrame();
  const reveal = interpolate(frame, [6, 28], [0, 1], {...clamp, easing: ease});
  const transfer = interpolate(frame, [38, 108], [0, 1], {...clamp, easing: ease});
  const completed = interpolate(frame, [102, 130], [0, 1], clamp);

  return (
    <AbsoluteFill style={{fontFamily: 'Flowplane UI', color: '#f8fafc', backgroundColor: '#071018', backgroundImage: 'radial-gradient(circle at 25% 40%,rgba(96,165,250,.13),transparent 28%),radial-gradient(circle at 76% 58%,rgba(103,232,210,.14),transparent 30%),linear-gradient(rgba(148,163,184,.055) 1px,transparent 1px),linear-gradient(90deg,rgba(148,163,184,.055) 1px,transparent 1px)', backgroundSize: 'auto,auto,56px 56px,56px 56px'}}>
      <SampleLabel number="02" title="VERSION CHANGE MOMENT" />
      <div style={{position: 'relative', height: '100%', display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', gap: 62}}>
        <div style={{fontSize: 30, color: '#67e8d2', letterSpacing: 8, fontWeight: 700, opacity: reveal}}>SAME MAPPING / NEW IMMUTABLE VERSION</div>
        <div style={{width: 1420, display: 'grid', gridTemplateColumns: '1fr 270px 1fr', alignItems: 'center'}}>
          <div style={{padding: '50px 58px', borderRadius: 18, background: 'rgba(9,21,34,.92)', border: '1px solid #294159', opacity: reveal, translate: `${interpolate(reveal, [0, 1], [-60, 0])}px 0`}}>
            <div style={{fontSize: 24, color: '#7e94aa', letterSpacing: 4}}>CURRENT</div>
            <div style={{fontSize: 91, fontWeight: 700, marginTop: 14}}>v1.0.0</div>
            <div style={{fontSize: 29, color: '#9cb0c3', marginTop: 18}}>Published · deployed · verified</div>
          </div>
          <div style={{position: 'relative', height: 12, margin: '0 30px', background: '#20384c', borderRadius: 99, overflow: 'visible'}}>
            <div style={{height: '100%', width: `${transfer * 100}%`, borderRadius: 99, background: 'linear-gradient(90deg,#60a5fa,#67e8d2)'}} />
            <div style={{position: 'absolute', top: -12, left: `calc(${transfer * 100}% - 18px)`, width: 36, height: 36, borderRadius: 99, background: '#67e8d2', boxShadow: '0 0 28px rgba(103,232,210,.72)', opacity: interpolate(frame, [34, 42], [0, 1], clamp)}} />
          </div>
          <div style={{padding: '50px 58px', borderRadius: 18, background: 'rgba(8,27,33,.94)', border: '1px solid rgba(103,232,210,.55)', boxShadow: `0 26px 90px rgba(45,212,191,${completed * 0.16})`, opacity: interpolate(frame, [55, 82], [0.25, 1], clamp), scale: interpolate(completed, [0, 1], [0.98, 1.02])}}>
            <div style={{fontSize: 24, color: '#67e8d2', letterSpacing: 4}}>PROMOTED</div>
            <div style={{fontSize: 91, fontWeight: 700, marginTop: 14}}>v1.1.0</div>
            <div style={{fontSize: 29, color: '#b9c8d8', marginTop: 18}}>Approved · published · runtime-ready</div>
          </div>
        </div>
        <div style={{fontSize: 29, letterSpacing: 3, color: '#91a7bb', opacity: completed}}>ARTIFACT HASH ALIGNED <span style={{color: '#67e8d2'}}>✓</span></div>
      </div>
    </AbsoluteFill>
  );
};

export const NativeProofFocusSample: React.FC = () => {
  const frame = useCurrentFrame();
  const reveal = interpolate(frame, [8, 30], [0, 1], {...clamp, easing: ease});
  const scan = interpolate(frame, [34, 124], [210, 930], clamp);
  const pulse = interpolate(frame % 40, [0, 20, 39], [0.55, 1, 0.55], clamp);

  return (
    <AbsoluteFill style={{fontFamily: 'Flowplane UI', overflow: 'hidden'}}>
      <RecordedUi trimSeconds={904} />
      <SampleLabel number="03" title="GUIDED PROOF FOCUS" />
      <div style={{position: 'absolute', left: 300, top: 185, width: 1320, height: 760, border: '3px solid rgba(103,232,210,.82)', borderRadius: 16, boxShadow: `0 0 0 9999px rgba(2,6,23,${0.34 * reveal}), 0 0 42px rgba(103,232,210,.22)`, opacity: reveal, scale: interpolate(reveal, [0, 1], [1.025, 1])}}>
        <div style={{position: 'absolute', left: 0, right: 0, top: scan - 185, height: 3, background: 'linear-gradient(90deg,transparent,#67e8d2,transparent)', boxShadow: '0 0 22px rgba(103,232,210,.65)'}} />
        <div style={{position: 'absolute', right: 24, top: 22, padding: '12px 18px', borderRadius: 7, background: 'rgba(4,15,22,.9)', color: '#67e8d2', fontSize: 23, fontWeight: 700, letterSpacing: 3, opacity: pulse}}>NATIVE UI / LIVE</div>
      </div>
      <div style={{position: 'absolute', left: 300, bottom: 88, color: '#effdfb', fontSize: 38, fontWeight: 700, opacity: interpolate(frame, [60, 88], [0, 1], clamp)}}>Execution proof where the runtime actually runs</div>
    </AbsoluteFill>
  );
};
