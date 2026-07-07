import type { ReactNode } from 'react';

// ── I3 — min-sample guard for rate/percentage displays ──────────────────────
// A percentage derived from a tiny denominator (e.g. "e1=100%" from a single
// evaluation) is noise, not signal. Anywhere the UI shows a rate built from a
// denominator, it must refuse to present a confident coloured % when the
// sample is too small. The backend now ships `low_sample` + a denominator on
// telemetry / spread / analytics payloads; this module reads them tolerantly
// and falls back to the raw count when the flags are absent.

export const MIN_SAMPLE = 20;

/** Read a boolean `low_sample` (or common aliases) from a payload object. */
export function readLowSample(obj: unknown): boolean | undefined {
  if (!obj || typeof obj !== 'object') return undefined;
  const o = obj as Record<string, unknown>;
  const v = o.low_sample ?? o.lowSample ?? o.insufficient_sample ?? o.is_low_sample;
  return typeof v === 'boolean' ? v : undefined;
}

// Field names the backend might use for the denominator behind a rate.
const DENOM_KEYS = [
  'denominator', 'denom', 'sample', 'sample_size', 'sample_count', 'n',
  'count', 'evaluations', 'evaluated', 'evaluations_count', 'trades',
  'total_trades', 'total', 'observations',
];

/** Read a denominator/sample size from a payload, trying common field names. */
export function readDenominator(obj: unknown, extraKeys: string[] = []): number | undefined {
  if (!obj || typeof obj !== 'object') return undefined;
  const o = obj as Record<string, unknown>;
  for (const k of [...extraKeys, ...DENOM_KEYS]) {
    const v = o[k];
    if (typeof v === 'number' && isFinite(v)) return v;
  }
  return undefined;
}

/**
 * Decide whether a rate should be suppressed as low-sample.
 *  - explicit low_sample flag always wins
 *  - otherwise a known denominator < MIN_SAMPLE is low-sample
 *  - unknown denominator + no flag → not guarded (caller may fall back)
 */
export function isLowSample(count?: number | null, lowSample?: boolean): boolean {
  if (lowSample === true) return true;
  if (lowSample === false) return false;
  return typeof count === 'number' && count < MIN_SAMPLE;
}

/** The muted "N/A (n=k)" replacement shown in place of a confident %. */
export function LowSampleNA({ count, className }: { count?: number | null; className?: string }) {
  return (
    <span
      className={className ?? 'text-muted-foreground/50 font-mono'}
      title={`insufficient sample (<${MIN_SAMPLE})`}
    >
      N/A{typeof count === 'number' ? ` (n=${count})` : ''}
    </span>
  );
}

/**
 * Wrap a confident rate rendering: when the sample is too small it renders the
 * greyed "N/A (n=k)" instead of `children`.
 */
export function RateGuard({
  count, lowSample, children, naClassName,
}: {
  count?: number | null;
  lowSample?: boolean;
  children: ReactNode;
  naClassName?: string;
}) {
  if (isLowSample(count, lowSample)) return <LowSampleNA count={count} className={naClassName} />;
  return <>{children}</>;
}
