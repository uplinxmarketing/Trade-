import { useState, useEffect, useCallback } from 'react';
import { Brain, RefreshCw, TrendingUp, TrendingDown, Minus, Target, Shield, Loader2 } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { supabase } from '@/integrations/supabase/client';
import { toast } from 'sonner';

interface Analysis {
  symbol: string;
  signal: string;
  confidence: number;
  reasoning: string;
  predicted_direction: string;
  predicted_change_percent: number;
  price_at_analysis: number;
  key_levels?: {
    support: number;
    resistance: number;
    stop_loss?: number;
    take_profit?: number;
  };
  currentPrice?: number;
  priceChange24h?: number;
  rsi?: number;
  sma7?: number;
  sma20?: number;
  volatility?: number;
  created_at?: string;
}

interface LearningMetric {
  symbol: string;
  total_predictions: number;
  correct_predictions: number;
  accuracy_percent: number;
  best_pattern: string | null;
}

interface AiAnalysisPanelProps {
  selectedCoins: string[];
}

const signalConfig: Record<string, { color: string; icon: typeof TrendingUp; label: string }> = {
  strong_buy: { color: 'text-gain', icon: TrendingUp, label: 'STRONG BUY' },
  buy: { color: 'text-gain/80', icon: TrendingUp, label: 'BUY' },
  hold: { color: 'text-warn', icon: Minus, label: 'HOLD' },
  sell: { color: 'text-loss/80', icon: TrendingDown, label: 'SELL' },
  strong_sell: { color: 'text-loss', icon: TrendingDown, label: 'STRONG SELL' },
};

const AiAnalysisPanel = ({ selectedCoins }: AiAnalysisPanelProps) => {
  const [analyses, setAnalyses] = useState<Analysis[]>([]);
  const [metrics, setMetrics] = useState<LearningMetric[]>([]);
  const [loading, setLoading] = useState(false);
  const [expandedCoin, setExpandedCoin] = useState<string | null>(null);

  const loadLatest = useCallback(async () => {
    if (selectedCoins.length === 0) return;
    const { data, error } = await supabase.functions.invoke('ai-analysis', {
      body: { action: 'get_latest', symbols: selectedCoins },
    });
    if (data?.analyses) setAnalyses(data.analyses);
    if (data?.metrics) setMetrics(data.metrics);
  }, [selectedCoins]);

  useEffect(() => {
    loadLatest();
  }, [loadLatest]);

  const runAnalysis = async () => {
    setLoading(true);
    try {
      const { data, error } = await supabase.functions.invoke('ai-analysis', {
        body: { action: 'analyze', symbols: selectedCoins },
      });
      if (error) throw error;
      if (data?.error) throw new Error(data.error);

      if (data?.analyses) {
        setAnalyses(data.analyses);
        toast.success('AI analysis complete', {
          description: `Analyzed ${data.analyses.length} coin${data.analyses.length > 1 ? 's' : ''}`,
        });
      }
      // Reload to get latest metrics too
      await loadLatest();
    } catch (err: any) {
      toast.error('Analysis failed', { description: err.message });
    } finally {
      setLoading(false);
    }
  };

  const overallAccuracy = metrics.length > 0
    ? (metrics.reduce((sum, m) => sum + Number(m.accuracy_percent), 0) / metrics.length).toFixed(1)
    : null;

  const totalPredictions = metrics.reduce((sum, m) => sum + Number(m.total_predictions), 0);

  return (
    <div className="bg-card border border-border rounded-lg p-4 space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Brain className="w-4 h-4 text-accent" />
          <h3 className="text-sm font-medium">AI Analysis Engine</h3>
        </div>
        <Button
          variant="ghost"
          size="sm"
          className="h-7 text-xs gap-1.5"
          onClick={runAnalysis}
          disabled={loading || selectedCoins.length === 0}
        >
          {loading ? <Loader2 className="w-3 h-3 animate-spin" /> : <RefreshCw className="w-3 h-3" />}
          Analyze
        </Button>
      </div>

      {/* Learning progress */}
      {totalPredictions > 0 && (
        <div className="flex items-center gap-3 bg-muted/20 rounded-md p-2.5">
          <Target className="w-4 h-4 text-accent shrink-0" />
          <div className="flex-1 min-w-0">
            <div className="flex items-center justify-between">
              <span className="text-[10px] uppercase tracking-widest text-muted-foreground">Learning Progress</span>
              <span className="text-xs font-mono font-semibold text-accent">{overallAccuracy}% accuracy</span>
            </div>
            <div className="w-full bg-muted/40 rounded-full h-1.5 mt-1">
              <div
                className="bg-accent rounded-full h-1.5 transition-all duration-500"
                style={{ width: `${Math.min(100, Number(overallAccuracy || 0))}%` }}
              />
            </div>
            <span className="text-[10px] text-muted-foreground mt-0.5 block">
              {totalPredictions} predictions reviewed
            </span>
          </div>
        </div>
      )}

      {/* Analysis results */}
      {analyses.length > 0 ? (
        <div className="space-y-2">
          {analyses.map(a => {
            const cfg = signalConfig[a.signal] || signalConfig.hold;
            const Icon = cfg.icon;
            const isExpanded = expandedCoin === a.symbol;
            const metric = metrics.find(m => m.symbol === a.symbol);

            return (
              <div key={a.symbol} className="border border-border/50 rounded-md overflow-hidden">
                <button
                  onClick={() => setExpandedCoin(isExpanded ? null : a.symbol)}
                  className="w-full flex items-center justify-between p-2.5 hover:bg-muted/20 transition-colors"
                >
                  <div className="flex items-center gap-2">
                    <Icon className={`w-3.5 h-3.5 ${cfg.color}`} />
                    <span className="text-sm font-mono font-medium">{a.symbol.replace('USDT', '')}</span>
                    <span className={`text-[10px] font-semibold tracking-wide px-1.5 py-0.5 rounded ${cfg.color} bg-muted/30`}>
                      {cfg.label}
                    </span>
                  </div>
                  <div className="flex items-center gap-2">
                    <span className="text-xs font-mono text-muted-foreground">{a.confidence}%</span>
                    {metric && (
                      <Shield className={`w-3 h-3 ${Number(metric.accuracy_percent) > 60 ? 'text-gain' : 'text-muted-foreground'}`} />
                    )}
                  </div>
                </button>

                {isExpanded && (
                  <div className="px-2.5 pb-2.5 space-y-2 border-t border-border/30">
                    {/* Reasoning */}
                    <p className="text-xs text-muted-foreground mt-2 leading-relaxed">
                      {a.reasoning}
                    </p>

                    {/* Prediction */}
                    <div className="grid grid-cols-2 gap-2 text-[10px]">
                      <div className="bg-muted/20 rounded p-1.5">
                        <span className="text-muted-foreground block">Direction</span>
                        <span className={`font-semibold ${
                          a.predicted_direction === 'up' ? 'text-gain' :
                          a.predicted_direction === 'down' ? 'text-loss' : 'text-warn'
                        }`}>
                          {a.predicted_direction?.toUpperCase()} {a.predicted_change_percent?.toFixed(1)}%
                        </span>
                      </div>
                      <div className="bg-muted/20 rounded p-1.5">
                        <span className="text-muted-foreground block">Price</span>
                        <span className="font-mono font-semibold">
                          ${(a.currentPrice || a.price_at_analysis)?.toLocaleString()}
                        </span>
                      </div>
                    </div>

                    {/* Key levels */}
                    {a.key_levels && (
                      <div className="grid grid-cols-2 gap-2 text-[10px]">
                        <div className="bg-muted/20 rounded p-1.5">
                          <span className="text-muted-foreground block">Support</span>
                          <span className="font-mono text-gain">${a.key_levels.support?.toLocaleString()}</span>
                        </div>
                        <div className="bg-muted/20 rounded p-1.5">
                          <span className="text-muted-foreground block">Resistance</span>
                          <span className="font-mono text-loss">${a.key_levels.resistance?.toLocaleString()}</span>
                        </div>
                      </div>
                    )}

                    {/* Technical indicators */}
                    {a.rsi !== undefined && (
                      <div className="grid grid-cols-3 gap-1.5 text-[10px]">
                        <div className="bg-muted/20 rounded p-1.5 text-center">
                          <span className="text-muted-foreground block">RSI</span>
                          <span className={`font-mono font-semibold ${
                            a.rsi > 70 ? 'text-loss' : a.rsi < 30 ? 'text-gain' : 'text-foreground'
                          }`}>{a.rsi?.toFixed(0)}</span>
                        </div>
                        <div className="bg-muted/20 rounded p-1.5 text-center">
                          <span className="text-muted-foreground block">Volatility</span>
                          <span className="font-mono font-semibold">{a.volatility?.toFixed(1)}%</span>
                        </div>
                        <div className="bg-muted/20 rounded p-1.5 text-center">
                          <span className="text-muted-foreground block">24h</span>
                          <span className={`font-mono font-semibold ${
                            (a.priceChange24h || 0) >= 0 ? 'text-gain' : 'text-loss'
                          }`}>{a.priceChange24h?.toFixed(1)}%</span>
                        </div>
                      </div>
                    )}

                    {/* Learning metric for this coin */}
                    {metric && Number(metric.total_predictions) > 0 && (
                      <div className="flex items-center gap-2 text-[10px] text-muted-foreground pt-1 border-t border-border/30">
                        <Shield className="w-3 h-3" />
                        <span>
                          Learned from {metric.total_predictions} predictions ·{' '}
                          <span className={Number(metric.accuracy_percent) > 60 ? 'text-gain' : 'text-loss'}>
                            {Number(metric.accuracy_percent).toFixed(0)}% accurate
                          </span>
                        </span>
                      </div>
                    )}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      ) : (
        <div className="text-center py-6">
          <Brain className="w-8 h-8 text-muted-foreground/30 mx-auto mb-2" />
          <p className="text-xs text-muted-foreground">
            Click "Analyze" to run AI chart analysis on your selected coins
          </p>
        </div>
      )}
    </div>
  );
};

export default AiAnalysisPanel;
