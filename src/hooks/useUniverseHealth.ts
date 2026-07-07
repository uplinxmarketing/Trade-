import { useState, useEffect } from 'react';

function normSym(s: unknown): string {
  return typeof s === 'string' ? s.toUpperCase().trim() : '';
}

export interface InvalidInfo {
  status?: string;          // e.g. "delisted", "renamed", "invalid"
  suggestedRename?: string; // present when the backend proposes a replacement
}

export interface UniverseHealth {
  /** symbol (upper-case, e.g. "LUNAUSDT") -> reason it's no longer valid */
  invalid: Record<string, InvalidInfo>;
  invalidCount: number;
  /** symbols chronically skipped for lot-waste / min-notional ("ticket too small") */
  lotWaste: Set<string>;
  loaded: boolean;
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
          map[sym] = {
            status: typeof obj.status === 'string' ? obj.status : undefined,
            suggestedRename: typeof rename === 'string' && rename.trim() ? rename.trim().toUpperCase() : undefined,
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

    const run = async () => {
      await Promise.allSettled([fetchValidate(), fetchLotWaste()]);
      if (!cancelled) setLoaded(true);
    };

    run();
    const id = setInterval(run, pollIntervalMs);
    return () => { cancelled = true; clearInterval(id); };
  }, [pollIntervalMs]);

  return { invalid, invalidCount: Object.keys(invalid).length, lotWaste, loaded };
}
