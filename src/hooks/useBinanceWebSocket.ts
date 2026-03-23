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
      reconnectRef.current = setTimeout(connect, 3000);
    };

    ws.onerror = () => ws.close();
  }, [symbols]);

  useEffect(() => {
    connect();
    return () => {
      wsRef.current?.close();
      if (reconnectRef.current) clearTimeout(reconnectRef.current);
    };
  }, [connect]);

  return { prices, connected };
}
