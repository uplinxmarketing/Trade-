import { useState, useEffect, useRef, useCallback } from 'react';
import type { KlineData } from '@/lib/binance-types';

const INTERVAL_MAP: Record<string, { binance: string; limit: number }> = {
  '1m':  { binance: '1m',  limit: 60 },
  '3m':  { binance: '3m',  limit: 60 },
  '5m':  { binance: '5m',  limit: 60 },
  '15m': { binance: '15m', limit: 60 },
  '1h':  { binance: '1h',  limit: 48 },
  '4h':  { binance: '4h',  limit: 48 },
  '1d':  { binance: '1d',  limit: 30 },
  '1w':  { binance: '1w',  limit: 20 },
};

const BASES = ['/api/proxy/binance'];

export function useBinanceKlines(symbol: string, interval: string) {
  const [klines, setKlines]   = useState<KlineData[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError]     = useState<string | null>(null);
  const wsRef = useRef<WebSocket | null>(null);

  // Clear stale data immediately when the active coin changes.
  // Without this, the previous coin's candles show until the fetch resolves,
  // and incoming WebSocket messages from the new coin can corrupt the old chart.
  useEffect(() => {
    setKlines([]);
    setError(null);
  }, [symbol]);

  const fetchKlines = useCallback(async () => {
    const cfg = INTERVAL_MAP[interval];
    if (!cfg || !symbol) return;
    setLoading(true);
    setError(null);

    let lastErr = 'All endpoints failed';
    for (const base of BASES) {
      try {
        const resp = await fetch(
          `${base}/klines?symbol=${symbol}&interval=${cfg.binance}&limit=${cfg.limit}`,
          { signal: AbortSignal.timeout(8000) }
        );
        if (!resp.ok) {
          const body = await resp.json().catch(() => ({}));
          lastErr = body.msg ?? `HTTP ${resp.status}`;
          if (resp.status === 400) break; // invalid symbol — don't try other bases
          continue;
        }
        const data = await resp.json();
        const mapped: KlineData[] = (data as any[]).map(k => ({
          time:   formatTime(k[0], interval),
          open:   parseFloat(k[1]),
          high:   parseFloat(k[2]),
          low:    parseFloat(k[3]),
          close:  parseFloat(k[4]),
          volume: parseFloat(k[5]),
        }));
        setKlines(mapped);
        setLoading(false);
        return; // success
      } catch (e: any) {
        lastErr = e?.message ?? 'Network error';
      }
    }

    setKlines([]);
    setError(lastErr);
    setLoading(false);
  }, [symbol, interval]);

  useEffect(() => {
    fetchKlines();
  }, [fetchKlines]);

  // Live WebSocket — reconnects automatically when symbol or interval changes
  useEffect(() => {
    const cfg = INTERVAL_MAP[interval];
    if (!cfg || !symbol) return;

    const ws = new WebSocket(
      `wss://stream.binance.com:9443/ws/${symbol.toLowerCase()}@kline_${cfg.binance}`
    );
    wsRef.current = ws;

    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        const k = data.k;
        if (!k) return;
        const candle: KlineData = {
          time:   formatTime(k.t, interval),
          open:   parseFloat(k.o),
          high:   parseFloat(k.h),
          low:    parseFloat(k.l),
          close:  parseFloat(k.c),
          volume: parseFloat(k.v),
        };
        setKlines(prev => {
          if (prev.length === 0) return [candle];
          const last = prev[prev.length - 1];
          if (last.time === candle.time) return [...prev.slice(0, -1), candle];
          return [...prev.slice(1), candle];
        });
        setError(null);
      } catch {}
    };

    ws.onerror = () => ws.close();
    return () => { ws.close(); };
  }, [symbol, interval]);

  return { klines, loading, error };
}

function formatTime(ts: number, interval: string): string {
  const d = new Date(ts);
  if (interval === '1d' || interval === '1w')
    return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
  if (interval === '4h' || interval === '1h')
    return d.toLocaleString('en-US', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', hour12: false });
  return d.toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit', hour12: false });
}
