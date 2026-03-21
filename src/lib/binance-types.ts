export interface TickerData {
  symbol: string;
  price: string;
  priceChangePercent: string;
  volume: string;
  high: string;
  low: string;
}

export interface BalanceItem {
  asset: string;
  free: string;
  locked: string;
  usdValue?: number;
}

export interface Position {
  symbol: string;
  side: 'LONG' | 'SHORT';
  entryPrice: string;
  markPrice: string;
  unrealizedPnl: string;
  quantity: string;
  leverage: number;
}

export interface TradeOrder {
  symbol: string;
  side: 'BUY' | 'SELL';
  type: 'MARKET' | 'LIMIT';
  quantity: string;
  price?: string;
}

export interface KlineData {
  time: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export interface ChatMessage {
  role: 'user' | 'assistant';
  content: string;
}

export interface BotStrategy {
  id: string;
  name: string;
  status: 'active' | 'paused' | 'stopped';
  pair: string;
  pnl: number;
  trades: number;
  winRate: number;
}
