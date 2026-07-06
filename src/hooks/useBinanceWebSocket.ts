import { useState, useEffect, useRef, useCallback } from 'react';

export interface WebSocketTicker {
  symbol: string;
  price: string;
  priceChangePercent: string;
  volume: string;
  high: string;
  low: string;
}

export function useBinanceWebSocket(symbols: string[]) {
  const [prices, setPrices] = useState<Record<string, WebSocketTicker>>({});
  const [connected, setConnected] = useState(false);
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const connect = useCallback(() => {
    if (symbols.length === 0) return;

    // Clear any pending reconnect so overlapping schedules can't stack sockets.
    if (reconnectRef.current) {
      clearTimeout(reconnectRef.current);
      reconnectRef.current = null;
    }

    const streams = symbols.map(s => `${s.toLowerCase()}@ticker`).join('/');
    const url = `wss://stream.binance.com:9443/ws/${streams}`;

    const ws = new WebSocket(url);
    wsRef.current = ws;

    ws.onopen = () => setConnected(true);

    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        if (data.s) {
          setPrices(prev => ({
            ...prev,
            [data.s]: {
              symbol: data.s,
              price: data.c,
              priceChangePercent: data.P,
              volume: data.v,
              high: data.h,
              low: data.l,
            },
          }));
        }
      } catch {}
    };

    ws.onclose = () => {
      setConnected(false);
      // Only reconnect if this socket is still the active one — an intentionally
      // replaced/closed socket must not respawn with a stale symbol list.
      if (wsRef.current !== ws) return;
      reconnectRef.current = setTimeout(connect, 3000);
    };

    ws.onerror = () => ws.close();
  }, [symbols]);

  useEffect(() => {
    connect();
    return () => {
      // Intentional close: detach handlers first so onclose can't schedule a
      // reconnect from the old closure, and clear any pending reconnect timer.
      if (reconnectRef.current) {
        clearTimeout(reconnectRef.current);
        reconnectRef.current = null;
      }
      const ws = wsRef.current;
      if (ws) {
        wsRef.current = null;
        ws.onclose = null;
        ws.onerror = null;
        ws.close();
      }
    };
  }, [connect]);

  return { prices, connected };
}
