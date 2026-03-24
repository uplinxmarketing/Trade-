
-- Multi-bot system table
CREATE TABLE public.trading_bots (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_session text NOT NULL DEFAULT 'default',
  bot_name text NOT NULL DEFAULT 'Bot 1',
  status text NOT NULL DEFAULT 'stopped' CHECK (status IN ('active', 'paused', 'stopped')),
  assigned_coins text[] NOT NULL DEFAULT ARRAY['BTCUSDT']::text[],
  allocated_budget numeric NOT NULL DEFAULT 100,
  available_balance numeric NOT NULL DEFAULT 100,
  used_balance numeric NOT NULL DEFAULT 0,
  max_budget_cap numeric NOT NULL DEFAULT 1000,
  trade_mode text NOT NULL DEFAULT 'USDT_TO_COIN' CHECK (trade_mode IN ('USDT_TO_COIN', 'COIN_TO_USDT')),
  reinvest_profits boolean NOT NULL DEFAULT false,
  fixed_balance_mode boolean NOT NULL DEFAULT false,
  max_trades_per_hour integer NOT NULL DEFAULT 10,
  cooldown_seconds integer NOT NULL DEFAULT 15,
  allow_multiple_open boolean NOT NULL DEFAULT false,
  min_profit_percent numeric NOT NULL DEFAULT 0.5,
  stop_loss_percent numeric NOT NULL DEFAULT 0.8,
  max_daily_loss numeric NOT NULL DEFAULT 50,
  max_drawdown_percent numeric NOT NULL DEFAULT 10,
  stop_after_consecutive_losses integer NOT NULL DEFAULT 5,
  consecutive_losses integer NOT NULL DEFAULT 0,
  daily_loss numeric NOT NULL DEFAULT 0,
  daily_loss_reset_at timestamp with time zone NOT NULL DEFAULT now(),
  last_trade_at timestamp with time zone,
  total_trades integer NOT NULL DEFAULT 0,
  winning_trades integer NOT NULL DEFAULT 0,
  total_pnl numeric NOT NULL DEFAULT 0,
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  updated_at timestamp with time zone NOT NULL DEFAULT now()
);

ALTER TABLE public.trading_bots ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow all trading_bots" ON public.trading_bots FOR ALL TO public USING (true) WITH CHECK (true);

-- Bot-specific trade history
ALTER TABLE public.bot_trade_history ADD COLUMN IF NOT EXISTS bot_id uuid REFERENCES public.trading_bots(id) ON DELETE SET NULL;

-- Bot-specific portfolio
ALTER TABLE public.paper_portfolio ADD COLUMN IF NOT EXISTS bot_id uuid REFERENCES public.trading_bots(id) ON DELETE SET NULL;

-- Enable realtime for trading_bots
ALTER PUBLICATION supabase_realtime ADD TABLE public.trading_bots;
