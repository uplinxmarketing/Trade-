import { useState, useEffect, useRef, useCallback } from 'react';
import type { KlineData } from '@/lib/binance-types';

const INTERVAL_MAP: Record<string, { binance: string; limit: number }> = {
  '1m': { binance: '1m', limit: 60 },
  '5m': { binance: '5m', limit: 60 },
  '15m': { binance: '15m', limit: 60 },
  '1h': { binance: '1h', limit: 48 },
  '4h': { binance: '4h', limit: 48 },
  '1d': { binance: '1d', limit: 30 },
  '1w': { binance: '1w', limit: 20 },
};

export function useBinanceKlines(symbol: string, interval: string) {
  const [klines, setKlines] = useState<KlineData[]>([]);
  const [loading, setLoading] = useState(false);
  const wsRef = useRef<WebSocket | null>(null);

  const fetchKlines = useCallback(async () => {
    const cfg = INTERVAL_MAP[interval];
    if (!cfg || !symbol) return;
    setLoading(true);
    try {
      const resp = await fetch(
        `https://api.binance.com/api/v3/klines?symbol=${symbol}&interval=${cfg.binance}&limit=${cfg.limit}`
      );
      const data = await resp.json();
      const mapped: KlineData[] = data.map((k: any[]) => ({
        time: formatTime(k[0], interval),
        open: parseFloat(k[1]),
        high: parseFloat(k[2]),
        low: parseFloat(k[3]),
        close: parseFloat(k[4]),
        volume: parseFloat(k[5]),
      }));
      setKlines(mapped);
    } catch {
      // If fetching fails, we don't want to clear existing data, just log the error
      console.error("Failed to fetch klines");
    } finally {
      setLoading(false);
    }
  }, [symbol, interval]);

  // Initial fetch
  useEffect(() => {
    fetchKlines();
  }, [fetchKlines]);

  // Live WebSocket stream for real-time candle updates
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
          time: formatTime(k.t, interval),
          open: parseFloat(k.o),
          high: parseFloat(k.h),
          low: parseFloat(k.l),
          close: parseFloat(k.c),
          volume: parseFloat(k.v),
        };
        setKlines(prev => {
          if (prev.length === 0) return [candle];
          const last = prev[prev.length - 1];
          if (last.time === candle.time) {
            return [...prev.slice(0, -1), candle];
          }
          // If the new candle is for a new time period, add it and remove the oldest
          // This maintains the limit set by the initial fetch
          return [...prev.slice(1), candle];
        });
      } catch (error) {
        console.error("WebSocket message error:", error);
      }
    };

    ws.onerror = (error) => {
      console.error("WebSocket error:", error);
      ws.close();
    };

    return () => { ws.close(); };
  }, [symbol, interval]);

  return { klines, loading };
}

function formatTime(ts: number, interval: string): string {
  const d = new Date(ts);
  if (interval === '1d' || interval === '1w') {
    return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
  }
  if (interval === '4h' || interval === '1h') {
    return d.toLocaleString('en-US', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', hour12: false });
  }
  return d.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: false });
}
