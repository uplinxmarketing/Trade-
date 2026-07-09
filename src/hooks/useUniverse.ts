import { useState, useEffect } from 'react';

// N3.2 — drive the coin list from the engine universe instead of a hardcoded
// frontend array (which still listed delisted MKRUSDT / broken TONUSDT).
//
// Endpoint: GET /api/universe
//   -> valid + suspended symbols, each with a per-symbol status and, when the
//      pair was replaced, a successor ticker.
//
// The payload shape is parsed tolerantly: it may ship a flat `symbols` array of
// objects, or split lists ({ valid, suspended, delisted }), or a symbol->status
// map. Any unreachable / malformed response yields `available:false` so the
// caller can fall back to its offline dev list.

export type UniverseSymbolStatus = 'trading' | 'suspended' | 'delisted';

export interface UniverseSymbol {
  symbol: string;               // upper-cased, e.g. "BTCUSDT"
  status: UniverseSymbolStatus;
  /** successor pair when a delisted symbol was renamed/replaced. */
  successor?: string;
}

export interface Universe {
  /** ordered symbol list as the backend returned it. */
  symbols: UniverseSymbol[];
  /** upper-cased symbol -> record for O(1) lookup. */
  bySymbol: Record<string, UniverseSymbol>;
  /** true when the last poll reached the backend and parsed at least one symbol. */
  available: boolean;
  loaded: boolean;
}

function normSym(s: unknown): string {
  return typeof s === 'string' ? s.toUpperCase().trim() : '';
}

function normStatus(s: unknown): UniverseSymbolStatus {
  const v = typeof s === 'string' ? s.toLowerCase() : '';
  if (v === 'suspended' || v === 'break' || v === 'halt' || v === 'halted' || v === 'auction') return 'suspended';
  if (v === 'delisted' || v === 'removed' || v === 'renamed' || v === 'gone') return 'delisted';
  return 'trading';
}

function pushSym(
  out: UniverseSymbol[],
  seen: Set<string>,
  raw: unknown,
  fallbackStatus: UniverseSymbolStatus,
) {
  let sym = '';
  let status = fallbackStatus;
  let successor: string | undefined;
  if (typeof raw === 'string') {
    sym = normSym(raw);
  } else if (raw && typeof raw === 'object') {
    const o = raw as Record<string, unknown>;
    sym = normSym(o.symbol ?? o.pair ?? o.ticker);
    if (o.status !== undefined) status = normStatus(o.status);
    const succ = o.successor ?? o.suggested_rename ?? o.rename ?? o.replacement;
    if (typeof succ === 'string' && succ.trim()) successor = succ.trim().toUpperCase();
  }
  if (!sym || seen.has(sym)) return;
  seen.add(sym);
  out.push({ symbol: sym, status, successor });
}

function parseUniverse(raw: unknown): UniverseSymbol[] {
  if (!raw || typeof raw !== 'object') return [];
  const top = raw as Record<string, unknown>;
  if (top.error) return [];

  const out: UniverseSymbol[] = [];
  const seen = new Set<string>();

  // 1) Flat array of symbols (strings or {symbol,status,successor}).
  const flat = top.symbols ?? top.universe ?? top.coins;
  if (Array.isArray(flat)) {
    for (const item of flat) pushSym(out, seen, item, 'trading');
  }

  // 2) Split status lists.
  if (Array.isArray(top.valid)) for (const s of top.valid) pushSym(out, seen, s, 'trading');
  if (Array.isArray(top.trading)) for (const s of top.trading) pushSym(out, seen, s, 'trading');
  if (Array.isArray(top.suspended)) for (const s of top.suspended) pushSym(out, seen, s, 'suspended');
  if (Array.isArray(top.delisted)) for (const s of top.delisted) pushSym(out, seen, s, 'delisted');

  // 3) symbol -> status map (only if nothing else matched, and not itself an array).
  if (out.length === 0 && flat && typeof flat === 'object' && !Array.isArray(flat)) {
    for (const [k, v] of Object.entries(flat as Record<string, unknown>)) {
      pushSym(out, seen, { symbol: k, ...(typeof v === 'object' && v ? v : { status: v }) }, 'trading');
    }
  }

  return out;
}

export function useUniverse(pollIntervalMs = 120_000): Universe {
  const [symbols, setSymbols] = useState<UniverseSymbol[]>([]);
  const [available, setAvailable] = useState(false);
  const [loaded, setLoaded] = useState(false);

  useEffect(() => {
    let cancelled = false;

    const poll = async () => {
      try {
        const res = await fetch(`/api/universe?t=${Date.now()}`, { cache: 'no-store' });
        if (!res.ok) { if (!cancelled) { setAvailable(false); setLoaded(true); } return; }
        const data = await res.json().catch(() => null);
        const parsed = parseUniverse(data);
        if (cancelled) return;
        setLoaded(true);
        if (parsed.length > 0) {
          setSymbols(parsed);
          setAvailable(true);
        } else {
          setAvailable(false);
        }
      } catch {
        if (!cancelled) { setAvailable(false); setLoaded(true); }
      }
    };

    poll();
    const id = setInterval(poll, pollIntervalMs);
    return () => { cancelled = true; clearInterval(id); };
  }, [pollIntervalMs]);

  const bySymbol: Record<string, UniverseSymbol> = {};
  for (const s of symbols) bySymbol[s.symbol] = s;

  return { symbols, bySymbol, available, loaded };
}
