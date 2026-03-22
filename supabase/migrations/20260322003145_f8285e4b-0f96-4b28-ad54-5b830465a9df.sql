-- AI analysis and learning engine tables
CREATE TABLE public.ai_analyses (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_session text NOT NULL DEFAULT 'default',
  symbol text NOT NULL,
  analysis_type text NOT NULL DEFAULT 'chart',
  signal text NOT NULL CHECK (signal IN ('strong_buy', 'buy', 'hold', 'sell', 'strong_sell')),
  confidence numeric NOT NULL DEFAULT 0,
  reasoning text,
  price_at_analysis numeric NOT NULL,
  predicted_direction text CHECK (predicted_direction IN ('up', 'down', 'sideways')),
  predicted_change_percent numeric,
  actual_direction text CHECK (actual_direction IN ('up', 'down', 'sideways')),
  actual_change_percent numeric,
  was_correct boolean,
  reviewed_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE public.learning_metrics (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_session text NOT NULL DEFAULT 'default',
  symbol text NOT NULL,
  total_predictions integer NOT NULL DEFAULT 0,
  correct_predictions integer NOT NULL DEFAULT 0,
  accuracy_percent numeric NOT NULL DEFAULT 0,
  avg_confidence numeric NOT NULL DEFAULT 0,
  best_pattern text,
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(user_session, symbol)
);

ALTER TABLE public.ai_analyses ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.learning_metrics ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Allow all ai_analyses" ON public.ai_analyses FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all learning_metrics" ON public.learning_metrics FOR ALL USING (true) WITH CHECK (true);

ALTER PUBLICATION supabase_realtime ADD TABLE public.ai_analyses;