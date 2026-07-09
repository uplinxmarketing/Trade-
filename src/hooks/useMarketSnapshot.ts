import { useState, useEffect } from 'react';

// N1 — one cheap local market-data poll replaces the per-symbol Binance REST
// storm. Every panel that showed last price / 24h change % / spread reads from
// this map instead of firing its own /api/proxy/binance/ticker/* call.
//
// Endpoint: GET /api/market/snapshot
//   -> { [symbol]: { price, pct_24h, spread_pct, quote_vol_24h, ts, age_sec } }
// The backend serves this from its own cache, so polling it every few seconds
// costs one local request regardless of how many coins are on screen.

export interface MarketSnapshotEntry {
  price: number;
  pct_24h: number;
  spread_pct: number;
  quote_vol_24h: number;
  ts: number;
  age_sec: number;
  /** false when the backend reports the symbol as unavailable/missing. */
  available: boolean;
}

export type MarketSnapshot = Record<string, MarketSnapshotEntry>;

export interface UseMarketSnapshot {
  snapshot: MarketSnapshot;
  /** true once at least one successful poll has returned. */
  loaded: boolean;
  /** true when the last poll reached the backend. */
  available: boolean;
}

function num(v: unknown): number {
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  if (typeof v === 'string') { const n = parseFloat(v); return Number.isFinite(n) ? n : 0; }
  return 0;
}

// Tolerantly pull a per-symbol entry out of whatever field names the backend
// ships. Accepts both snake_case (price/pct_24h/...) and a few common aliases.
function parseEntry(raw: unknown): MarketSnapshotEntry | null {
  if (!raw || typeof raw !== 'object') return null;
  const o = raw as Record<string, unknown>;
  if (o.available === false) {
    return { price: 0, pct_24h: 0, spread_pct: 0, quote_vol_24h: 0, ts: 0, age_sec: 0, available: false };
  }
  const price = num(o.price ?? o.last ?? o.lastPrice);
  const pct = num(o.pct_24h ?? o.priceChangePercent ?? o.change_pct ?? o.pct);
  const spread = num(o.spread_pct ?? o.spread ?? o.spreadPct);
  const qv = num(o.quote_vol_24h ?? o.quoteVolume ?? o.quote_volume ?? o.qv);
  const ts = num(o.ts ?? o.timestamp ?? o.time);
  const age = num(o.age_sec ?? o.age ?? o.ageSec);
  return { price, pct_24h: pct, spread_pct: spread, quote_vol_24h: qv, ts, age_sec: age, available: true };
}

// The payload may be keyed directly by symbol, or nested under `symbols`/`data`.
function parseSnapshot(raw: unknown): MarketSnapshot {
  if (!raw || typeof raw !== 'object') return {};
  const top = raw as Record<string, unknown>;
  if (top.error) return {};
  const container =
    (top.symbols && typeof top.symbols === 'object') ? top.symbols as Record<string, unknown>
    : (top.data && typeof top.data === 'object') ? top.data as Record<string, unknown>
    : top;

  const out: MarketSnapshot = {};
  for (const [k, v] of Object.entries(container)) {
    if (k === 'ts' || k === 'error' || k === 'available' || k === 'age_sec') continue;
    const sym = k.toUpperCase().trim();
    if (!sym) continue;
    const entry = parseEntry(v);
    if (entry) out[sym] = entry;
  }
  return out;
}

/**
 * Poll the local market snapshot. Any non-ok / unreachable response leaves the
 * previous map intact and flips `available` to false so callers can render "—".
 */
export function useMarketSnapshot(pollIntervalMs = 4000): UseMarketSnapshot {
  const [snapshot, setSnapshot] = useState<MarketSnapshot>({});
  const [loaded, setLoaded] = useState(false);
  const [available, setAvailable] = useState(false);

  useEffect(() => {
    let cancelled = false;

    const poll = async () => {
      try {
        const res = await fetch(`/api/market/snapshot?t=${Date.now()}`, { cache: 'no-store' });
        if (!res.ok) { if (!cancelled) setAvailable(false); return; }
        const data = await res.json().catch(() => null);
        const map = parseSnapshot(data);
        if (cancelled) return;
        setAvailable(true);
        setLoaded(true);
        // Only replace when we actually parsed symbols — never wipe a good map
        // with an empty/transient response.
        if (Object.keys(map).length > 0) setSnapshot(map);
      } catch {
        if (!cancelled) setAvailable(false);
      }
    };

    poll();
    const id = setInterval(poll, pollIntervalMs);
    return () => { cancelled = true; clearInterval(id); };
  }, [pollIntervalMs]);

  return { snapshot, loaded, available };
}
