import { Play, Pause, Square } from 'lucide-react';
import { Button } from '@/components/ui/button';
import type { BotStrategy } from '@/lib/binance-types';

interface BotStrategiesProps {
  strategies: BotStrategy[];
  onToggle: (id: string) => void;
}

const BotStrategies = ({ strategies, onToggle }: BotStrategiesProps) => {
  return (
    <div className="trading-card p-4 animate-fade-in-up" style={{ animationDelay: '480ms' }}>
      <h3 className="text-sm font-medium text-muted-foreground mb-3">Bot Strategies</h3>
      <div className="space-y-3">
        {strategies.map((bot) => (
          <div key={bot.id} className="flex items-center justify-between py-2 border-b border-border last:border-0">
            <div className="flex items-center gap-3">
              <div className={`w-2 h-2 rounded-full ${
                bot.status === 'active' ? 'bg-gain' :
                bot.status === 'paused' ? 'bg-warn' : 'bg-muted-foreground'
              }`} />
              <div>
                <div className="text-sm font-medium">{bot.name}</div>
                <div className="text-xs text-muted-foreground font-mono">{bot.pair}</div>
              </div>
            </div>
            <div className="flex items-center gap-4">
              <div className="text-right">
                <div className={`text-sm font-mono tabular-nums ${bot.pnl >= 0 ? 'text-gain' : 'text-loss'}`}>
                  {bot.pnl >= 0 ? '+' : ''}${bot.pnl.toFixed(2)}
                </div>
                <div className="text-xs text-muted-foreground">
                  {bot.trades} trades · {bot.winRate}% win
                </div>
              </div>
              <Button
                variant="ghost"
                size="icon"
                className="h-8 w-8"
                onClick={() => onToggle(bot.id)}
              >
                {bot.status === 'active' ? <Pause className="w-3.5 h-3.5" /> :
                 bot.status === 'paused' ? <Play className="w-3.5 h-3.5" /> :
                 <Square className="w-3.5 h-3.5" />}
              </Button>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
};

export default BotStrategies;
