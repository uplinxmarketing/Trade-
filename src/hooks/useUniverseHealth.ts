import { useState, useEffect } from 'react';

function normSym(s: unknown): string {
  return typeof s === 'string' ? s.toUpperCase().trim() : '';
}

export interface InvalidInfo {
  status?: string;          // raw exchange status, e.g. "BREAK", "TRADING"
  suggestedRename?: string; // present when the backend proposes a replacement
  // Authoritative kind from /api/universe/validate: 'break' (halted, auto-
  // restores) vs 'delisted' (gone). Preferred over sniffing `status`.
  excludedKind?: 'break' | 'delisted';
}

export interface UniverseHealth {
  /** symbol (upper-case, e.g. "LUNAUSDT") -> reason it's no longer valid */
  invalid: Record<string, InvalidInfo>;
  invalidCount: number;
  /** symbols chronically skipped for lot-waste / min-notional ("ticket too small") */
  lotWaste: Set<string>;
  // I4 — backfill readiness surfaced via get_data_health so freshly-added coins
  // render a "warming up — backfilling" badge instead of silently missing.
  /** upper-cased symbols still backfilling (in universe, not backfill_ready yet) */
  warming: Set<string>;
  /** upper-cased symbols known backfill_ready */
  ready: Set<string>;
  /** upper-cased symbol -> backfill completion % (0–100) when known */
  backfillPct: Record<string, number>;
  /** overall backfill % across the universe when the backend reports it */
  backfillOverallPct: number | null;
  loaded: boolean;
}

// Tolerantly extract per-coin backfill readiness from get_data_health. The
// backend may expose it several ways; we accept any it ships and degrade to
// empty sets (no warming badges) when none are present.
function extractReadiness(raw: unknown): {
  warming: Set<string>; ready: Set<string>;
  backfillPct: Record<string, number>; overall: number | null;
} | null {
  if (!raw || typeof raw !== 'object') return null;
  const top = raw as Record<string, unknown>;
  if (top.error) return null;
  // readiness fields may live at the top level or under the `data` section.
  const dataSec = (top.data && typeof top.data === 'object') ? top.data as Record<string, unknown> : {};
  const pick = (k: string): unknown => top[k] ?? dataSec[k];

  const warming = new Set<string>();
  const ready = new Set<string>();
  const backfillPct: Record<string, number> = {};

  const addTo = (set: Set<string>, v: unknown) => {
    if (Array.isArray(v)) v.forEach(s => { const u = normSym(typeof s === 'string' ? s : (s as Record<string, unknown>)?.symbol); if (u) set.add(u); });
  };
  // Explicit ready / warming symbol lists (future-proof: used if the backend
  // ships them). Today get_data_health exposes `uncovered_symbols` — valid
  // watchlist coins not yet covered/backfilled — which is exactly "warming".
  addTo(ready, pick('backfill_ready') ?? pick('ready_coins') ?? pick('ready'));
  addTo(warming, pick('backfilling') ?? pick('warming') ?? pick('not_ready')
    ?? pick('excluded_warming') ?? pick('uncovered_symbols'));

  // Per-coin readiness map: { SYM: { ready: bool, backfill_pct: n } | bool | number }
  const cr = pick('coin_readiness') ?? pick('readiness') ?? pick('per_coin_readiness');
  if (cr && typeof cr === 'object' && !Array.isArray(cr)) {
    for (const [k, val] of Object.entries(cr as Record<string, unknown>)) {
      const sym = normSym(k);
      if (!sym) continue;
      let isReady: boolean | undefined;
      let pct: number | undefined;
      if (typeof val === 'boolean') isReady = val;
      else if (typeof val === 'number') { pct = val; isReady = val >= 100; }
      else if (val && typeof val === 'object') {
        const o = val as Record<string, unknown>;
        if (typeof o.ready === 'boolean') isReady = o.ready;
        const p = o.backfill_pct ?? o.pct ?? o.percent;
        if (typeof p === 'number') { pct = p; if (isReady === undefined) isReady = p >= 100; }
      }
      if (pct != null) backfillPct[sym] = pct;
      if (isReady === true) ready.add(sym);
      else if (isReady === false) warming.add(sym);
    }
  }

  const overallRaw = pick('backfill_pct') ?? pick('backfill_overall_pct');
  const overall = typeof overallRaw === 'number' ? overallRaw : null;

  if (warming.size === 0 && ready.size === 0 && Object.keys(backfillPct).length === 0 && overall == null) {
    return null; // nothing readiness-related shipped
  }
  return { warming, ready, backfillPct, overall };
}

// Tolerantly pull a set of symbols out of whatever the diagnostics endpoint
// exposes for lot-waste. Handles arrays of strings, arrays of {symbol},
// objects keyed by symbol, or a nested field under a few likely names.
function extractLotWaste(data: unknown): Set<string> | null {
  if (!data || typeof data !== 'object') return null;
  const o = data as Record<string, unknown>;
  if (o.error) return null;

  const out = new Set<string>();
  const add = (v: unknown) => {
    if (typeof v === 'string') { const s = normSym(v); if (s) out.add(s); }
    else if (v && typeof v === 'object') {
      const s = normSym((v as Record<string, unknown>).symbol);
      if (s) out.add(s);
    }
  };
  const consume = (c: unknown) => {
    if (!c) return;
    if (Array.isArray(c)) c.forEach(add);
    else if (typeof c === 'object') Object.keys(c as object).forEach(k => { const s = normSym(k); if (s) out.add(s); });
  };

  // Try the likely container fields, then the top-level object itself.
  consume(o.lot_waste ?? o.lot_waste_skipped ?? o.lot_waste_symbols
    ?? o.ticket_too_small ?? o.min_notional_skipped ?? o.symbols);
  if (out.size === 0 && Array.isArray(data)) consume(data);
  return out;
}

/**
 * Fetches watchlist-coin health from the backend, tolerantly:
 *   - GET /api/universe/validate -> {valid, invalid:[{symbol,status,suggested_rename}], ...}
 *   - lot-waste flags from /api/diagnostics/lot-waste (best-effort; skipped if absent)
 *
 * Any non-ok / {error} / unreachable response yields empty data (no badges),
 * so the UI degrades gracefully when the backend hasn't shipped these yet.
 */
export function useUniverseHealth(pollIntervalMs = 120_000): UniverseHealth {
  const [invalid, setInvalid] = useState<Record<string, InvalidInfo>>({});
  const [lotWaste, setLotWaste] = useState<Set<string>>(() => new Set());
  const [warming, setWarming] = useState<Set<string>>(() => new Set());
  const [ready, setReady] = useState<Set<string>>(() => new Set());
  const [backfillPct, setBackfillPct] = useState<Record<string, number>>({});
  const [backfillOverallPct, setBackfillOverallPct] = useState<number | null>(null);
  const [loaded, setLoaded] = useState(false);

  useEffect(() => {
    let cancelled = false;

    const fetchValidate = async () => {
      try {
        const res = await fetch(`/api/universe/validate?t=${Date.now()}`, { cache: 'no-store' });
        if (!res.ok) return;
        const data = await res.json().catch(() => null);
        if (!data || typeof data !== 'object' || (data as Record<string, unknown>).error) return;
        const arr = Array.isArray((data as Record<string, unknown>).invalid)
          ? (data as { invalid: unknown[] }).invalid
          : [];
        const map: Record<string, InvalidInfo> = {};
        for (const item of arr) {
          const sym = normSym(typeof item === 'string' ? item : (item as Record<string, unknown>)?.symbol);
          if (!sym) continue;
          const obj = (item && typeof item === 'object') ? item as Record<string, unknown> : {};
          const rename = obj.suggested_rename;
          const kind = obj.excluded_kind;
          map[sym] = {
            status: typeof obj.status === 'string' ? obj.status : undefined,
            suggestedRename: typeof rename === 'string' && rename.trim() ? rename.trim().toUpperCase() : undefined,
            excludedKind: kind === 'break' || kind === 'delisted' ? kind : undefined,
          };
        }
        if (!cancelled) setInvalid(map);
      } catch { /* graceful — no badges */ }
    };

    const fetchLotWaste = async () => {
      try {
        const res = await fetch(`/api/diagnostics/lot-waste?t=${Date.now()}`, { cache: 'no-store' });
        if (!res.ok) return;
        const data = await res.json().catch(() => null);
        const set = extractLotWaste(data);
        if (set && !cancelled) setLotWaste(set);
      } catch { /* best-effort — silently skip */ }
    };

    const fetchReadiness = async () => {
      try {
        const res = await fetch(`/api/health/data?t=${Date.now()}`, { cache: 'no-store' });
        if (!res.ok) return;
        const data = await res.json().catch(() => null);
        const r = extractReadiness(data);
        if (r && !cancelled) {
          setWarming(r.warming);
          setReady(r.ready);
          setBackfillPct(r.backfillPct);
          setBackfillOverallPct(r.overall);
        }
      } catch { /* best-effort — no warming badges */ }
    };

    const run = async () => {
      await Promise.allSettled([fetchValidate(), fetchLotWaste(), fetchReadiness()]);
      if (!cancelled) setLoaded(true);
    };

    run();
    const id = setInterval(run, pollIntervalMs);
    return () => { cancelled = true; clearInterval(id); };
  }, [pollIntervalMs]);

  return {
    invalid, invalidCount: Object.keys(invalid).length, lotWaste,
    warming, ready, backfillPct, backfillOverallPct, loaded,
  };
}
