// I4 — single source of truth for a selected coin's lifecycle status.
//
// The operator selected ~100 coins; every one MUST render somewhere with a
// truthful badge. A coin that appears nowhere is indistinguishable from a bug.
// This module classifies a symbol from the backend universe-health signals so
// both the CoinSelectorPanel (left rail) and the Market Signals table agree on
// what to show.

import type { InvalidInfo } from '@/hooks/useUniverseHealth';

export type CoinLifecycle =
  | 'valid'     // in universe + backfill_ready → normal row
  | 'warming'   // in universe but not backfill_ready yet (freshly added, backfilling)
  | 'halted'    // status BREAK/AUCTION — trading paused; bot auto-restores
  | 'delisted'; // pending/auto-removed — gone for good; replace with successor

export interface CoinStatus {
  lifecycle: CoinLifecycle;
  /** successor pair when the backend proposes a rename (delisted → successor). */
  successor?: string;
  /** 0–100 backfill completion when warming and known. */
  backfillPct?: number;
}

/**
 * BREAK/AUCTION → halted (auto-restores, informational).
 * everything else invalid (delisted/renamed/invalid) → delisted (prune/replace).
 */
export function classifyInvalid(info: InvalidInfo): 'halted' | 'delisted' {
  // Prefer the backend's authoritative excluded_kind; fall back to sniffing the
  // raw exchange status only when it's absent.
  if (info.excludedKind === 'break') return 'halted';
  if (info.excludedKind === 'delisted') return 'delisted';
  const s = (info.status ?? '').toUpperCase();
  if (s === 'BREAK' || s === 'AUCTION' || s.includes('HALT')) return 'halted';
  return 'delisted';
}

export interface UniverseStatusInputs {
  /** upper-cased symbol → invalid info from /api/universe/validate */
  invalid: Record<string, InvalidInfo>;
  /** upper-cased symbols still backfilling (in universe, not backfill_ready) */
  warming: Set<string>;
  /** upper-cased symbols known backfill_ready */
  ready: Set<string>;
  /** upper-cased symbol → backfill % (0–100) when known */
  backfillPct?: Record<string, number>;
}

/**
 * Resolve one symbol's lifecycle. Precedence: delisted/halted (validate) beats
 * warming beats valid — a delisted coin that is also "not ready" is delisted.
 *
 * Unknown-but-selected coins default to 'valid' so we never invent a scary
 * badge when the backend hasn't shipped readiness yet (graceful degradation).
 */
export function resolveCoinStatus(symbol: string, u: UniverseStatusInputs): CoinStatus {
  const sym = symbol.toUpperCase();
  const info = u.invalid[sym];
  if (info) {
    const kind = classifyInvalid(info);
    if (kind === 'halted') return { lifecycle: 'halted' };
    return { lifecycle: 'delisted', successor: info.suggestedRename };
  }
  const pct = u.backfillPct?.[sym];
  // Warming when explicitly listed as backfilling/uncovered, OR when an explicit
  // ready set is known and this coin isn't in it. Each branch is guarded by its
  // own availability so an empty ready set never marks every coin warming.
  if (u.warming.has(sym) || (u.ready.size > 0 && !u.ready.has(sym))) {
    return { lifecycle: 'warming', backfillPct: pct };
  }
  return { lifecycle: 'valid', backfillPct: pct };
}
