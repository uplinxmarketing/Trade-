import { useCallback, useEffect, useRef, useState } from 'react';

// I4 — auto-remove notices. The bot may remove/suspend/restore coins on its own
// (delistings, trading breaks). The operator explicitly asked to be told when
// their universe changed under them, so we poll GET /api/universe/notices and
// surface recent changes in an unobtrusive-but-visible banner.

export type UniverseNoticeAction = 'removed' | 'suspended' | 'restored' | 'deferred';

export interface UniverseNotice {
  ts: number;
  action: UniverseNoticeAction;
  symbol: string;
  reason?: string;
  successor?: string;
}

function normSym(s: unknown): string {
  return typeof s === 'string' ? s.toUpperCase().trim() : '';
}

// Stable identity for a notice so dismissals persist across polls.
export function noticeKey(n: UniverseNotice): string {
  return `${n.action}:${n.symbol}:${n.ts}`;
}

function parseNotices(raw: unknown): UniverseNotice[] {
  if (!raw || typeof raw !== 'object') return [];
  const arr = (raw as Record<string, unknown>).notices;
  if (!Array.isArray(arr)) return [];
  const out: UniverseNotice[] = [];
  for (const item of arr) {
    if (!item || typeof item !== 'object') continue;
    const o = item as Record<string, unknown>;
    const symbol = normSym(o.symbol);
    const action = o.action;
    if (!symbol || (action !== 'removed' && action !== 'suspended' && action !== 'restored' && action !== 'deferred')) continue;
    out.push({
      ts: typeof o.ts === 'number' ? o.ts : 0,
      action,
      symbol,
      reason: typeof o.reason === 'string' ? o.reason : undefined,
      successor: normSym(o.successor) || undefined,
    });
  }
  return out;
}

export interface UseUniverseNotices {
  notices: UniverseNotice[];
  dismissed: Set<string>;
  dismiss: (key: string) => void;
  dismissAll: () => void;
}

const DISMISS_KEY = 'universe_notices_dismissed';

/**
 * Polls GET /api/universe/notices. Degrades to an empty list on any error so
 * the UI never breaks when the endpoint isn't shipped. Dismissals are kept in
 * localStorage so a dismissed banner doesn't reappear on the next poll/reload.
 */
export function useUniverseNotices(pollIntervalMs = 45_000): UseUniverseNotices {
  const [notices, setNotices] = useState<UniverseNotice[]>([]);
  const [dismissed, setDismissed] = useState<Set<string>>(() => {
    try { return new Set(JSON.parse(localStorage.getItem(DISMISS_KEY) ?? '[]')); }
    catch { return new Set(); }
  });
  const dismissedRef = useRef(dismissed);
  dismissedRef.current = dismissed;

  useEffect(() => {
    let cancelled = false;
    const poll = async () => {
      try {
        const res = await fetch(`/api/universe/notices?t=${Date.now()}`, { cache: 'no-store' });
        if (!res.ok) return;
        const data = await res.json().catch(() => null);
        const parsed = parseNotices(data);
        if (!cancelled) {
          setNotices(parsed);
          // Garbage-collect dismissals for notices the backend no longer reports.
          const live = new Set(parsed.map(noticeKey));
          const cur = dismissedRef.current;
          if (cur.size > 0) {
            const pruned = new Set([...cur].filter(k => live.has(k)));
            if (pruned.size !== cur.size) {
              setDismissed(pruned);
              try { localStorage.setItem(DISMISS_KEY, JSON.stringify([...pruned])); } catch { /* ignore */ }
            }
          }
        }
      } catch { /* graceful — no banner */ }
    };
    poll();
    const id = setInterval(poll, pollIntervalMs);
    return () => { cancelled = true; clearInterval(id); };
  }, [pollIntervalMs]);

  const dismiss = useCallback((key: string) => {
    setDismissed(prev => {
      const next = new Set(prev); next.add(key);
      try { localStorage.setItem(DISMISS_KEY, JSON.stringify([...next])); } catch { /* ignore */ }
      return next;
    });
  }, []);

  const dismissAll = useCallback(() => {
    setDismissed(() => {
      const next = new Set(notices.map(noticeKey));
      try { localStorage.setItem(DISMISS_KEY, JSON.stringify([...next])); } catch { /* ignore */ }
      return next;
    });
  }, [notices]);

  return { notices, dismissed, dismiss, dismissAll };
}
