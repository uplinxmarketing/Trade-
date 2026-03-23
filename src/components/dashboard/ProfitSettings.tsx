import { useState, useEffect } from 'react';
import { Settings2, Save, Percent } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { supabase } from '@/integrations/supabase/client';
import { toast } from 'sonner';

interface ProfitSettingsProps {
  onSaved?: () => void;
}

const ProfitSettings = ({ onSaved }: ProfitSettingsProps) => {
  const [minProfit, setMinProfit] = useState(0.5);
  const [stopLoss, setStopLoss] = useState(3);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    (async () => {
      const { data } = await supabase
        .from('bot_config')
        .select('min_profit_percent, stop_loss_percent')
        .eq('user_session', 'default')
        .maybeSingle();
      if (data) {
        setMinProfit(Number((data as any).min_profit_percent ?? 0.5));
        setStopLoss(Number((data as any).stop_loss_percent ?? 3));
      }
    })();
  }, []);

  const save = async () => {
    setSaving(true);
    try {
      await supabase.from('bot_config').upsert({
        user_session: 'default',
        min_profit_percent: minProfit,
        stop_loss_percent: stopLoss,
        updated_at: new Date().toISOString(),
      } as any, { onConflict: 'user_session' });
      toast.success('Profit targets saved', {
        description: `Take profit: ${minProfit}% · Stop loss: ${stopLoss}%`,
      });
      onSaved?.();
    } catch (err: any) {
      toast.error('Failed to save', { description: err.message });
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="bg-card border border-border rounded-lg p-4 space-y-3">
      <div className="flex items-center gap-2">
        <Settings2 className="w-4 h-4 text-accent" />
        <h3 className="text-sm font-medium">Profit Targets</h3>
      </div>

      <div className="grid grid-cols-2 gap-3">
        <div className="space-y-1.5">
          <label className="text-[10px] uppercase tracking-widest text-muted-foreground flex items-center gap-1">
            <Percent className="w-3 h-3" /> Min Profit
          </label>
          <div className="relative">
            <input
              type="number"
              min={0.1}
              max={50}
              step={0.1}
              value={minProfit}
              onChange={e => setMinProfit(parseFloat(e.target.value) || 0.5)}
              className="w-full bg-muted/30 border border-border rounded-md px-3 py-2 text-sm font-mono tabular-nums text-gain focus:outline-none focus:ring-1 focus:ring-gain/50"
            />
            <span className="absolute right-3 top-1/2 -translate-y-1/2 text-xs text-muted-foreground">%</span>
          </div>
          <p className="text-[10px] text-muted-foreground">Sell when profit reaches this %</p>
        </div>

        <div className="space-y-1.5">
          <label className="text-[10px] uppercase tracking-widest text-muted-foreground flex items-center gap-1">
            <Percent className="w-3 h-3" /> Stop Loss
          </label>
          <div className="relative">
            <input
              type="number"
              min={0.5}
              max={50}
              step={0.5}
              value={stopLoss}
              onChange={e => setStopLoss(parseFloat(e.target.value) || 3)}
              className="w-full bg-muted/30 border border-border rounded-md px-3 py-2 text-sm font-mono tabular-nums text-loss focus:outline-none focus:ring-1 focus:ring-loss/50"
            />
            <span className="absolute right-3 top-1/2 -translate-y-1/2 text-xs text-muted-foreground">%</span>
          </div>
          <p className="text-[10px] text-muted-foreground">Cut losses at this %</p>
        </div>
      </div>

      {/* Visual slider indicators */}
      <div className="flex items-center gap-2 text-[10px]">
        <span className="text-loss font-mono">-{stopLoss}%</span>
        <div className="flex-1 h-1.5 rounded-full bg-muted/40 relative overflow-hidden">
          <div
            className="absolute left-0 h-full bg-loss/40 rounded-l-full"
            style={{ width: `${(stopLoss / (stopLoss + minProfit)) * 100}%` }}
          />
          <div
            className="absolute right-0 h-full bg-gain/40 rounded-r-full"
            style={{ width: `${(minProfit / (stopLoss + minProfit)) * 100}%` }}
          />
        </div>
        <span className="text-gain font-mono">+{minProfit}%</span>
      </div>

      <Button
        onClick={save}
        disabled={saving}
        size="sm"
        className="w-full bg-accent hover:bg-accent/90 text-accent-foreground font-semibold gap-1.5"
      >
        <Save className="w-3.5 h-3.5" />
        {saving ? 'Saving...' : 'Save Targets'}
      </Button>
    </div>
  );
};

export default ProfitSettings;
