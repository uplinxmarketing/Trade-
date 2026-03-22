-- Paper trading tables for test mode
CREATE TABLE public.paper_trades (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_session text NOT NULL DEFAULT 'default',
  symbol text NOT NULL,
  side text NOT NULL CHECK (side IN ('BUY', 'SELL')),
  price numeric NOT NULL,
  quantity numeric NOT NULL,
  total numeric GENERATED ALWAYS AS (price * quantity) STORED,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE public.paper_portfolio (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_session text NOT NULL DEFAULT 'default',
  symbol text NOT NULL,
  quantity numeric NOT NULL DEFAULT 0,
  avg_entry_price numeric NOT NULL DEFAULT 0,
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(user_session, symbol)
);

CREATE TABLE public.bot_config (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_session text NOT NULL DEFAULT 'default' UNIQUE,
  selected_coins text[] NOT NULL DEFAULT ARRAY['BTCUSDT','ETHUSDT','SOLUSDT','BNBUSDT','DOGEUSDT'],
  mode text NOT NULL DEFAULT 'test' CHECK (mode IN ('test', 'live')),
  is_running boolean NOT NULL DEFAULT false,
  initial_balance numeric NOT NULL DEFAULT 10000,
  current_balance numeric NOT NULL DEFAULT 10000,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE public.bot_trade_history (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_session text NOT NULL DEFAULT 'default',
  symbol text NOT NULL,
  side text NOT NULL CHECK (side IN ('BUY', 'SELL')),
  price numeric NOT NULL,
  quantity numeric NOT NULL,
  pnl numeric,
  reason text,
  created_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE public.paper_trades ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.paper_portfolio ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.bot_config ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.bot_trade_history ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Allow all paper_trades" ON public.paper_trades FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all paper_portfolio" ON public.paper_portfolio FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all bot_config" ON public.bot_config FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all bot_trade_history" ON public.bot_trade_history FOR ALL USING (true) WITH CHECK (true);

ALTER PUBLICATION supabase_realtime ADD TABLE public.bot_trade_history;