import { useState, useEffect, useCallback } from 'react';
import { Settings2, Zap, Percent, Lock, Layers } from 'lucide-react';
import { toast } from 'sonner';
import { API_BASE } from '@/config';

const BOT_API = API_BASE;

type BudgetMode = 'fixed' | 'percent' | 'capped' | 'per_coin';

const DEFAULT_PER_COIN: Record<string, number> = {
  BTCUSDT: 200, ETHUSDT: 150, SOLUSDT: 100, BNBUSDT: 100, DOGEUSDT: 50,
};

interface Config {
  budget_mode: BudgetMode;
  budget_fixed_usdt: number;
  budget_pct_of_free: number;
  budget_total_cap_usdt: number;
  budget_per_coin: Record<string, number>;
}

const DEFAULT_CONFIG: Config = {
  budget_mode: 'fixed',
  budget_fixed_usdt: 100,
  budget_pct_of_free: 10,
  budget_total_cap_usdt: 500,
  budget_per_coin: DEFAULT_PER_COIN,
};

const MODES: { key: BudgetMode; label: string; icon: React.ReactNode; desc: string }[] = [
  { key: 'fixed',   label: 'Fixed',        icon: <Lock className="w-3.5 h-3.5" />,    desc: 'Same USDT per trade' },
  { key: 'percent', label: '% of Balance', icon: <Percent className="w-3.5 h-3.5" />, desc: 'Scales with free balance' },
  { key: 'capped',  label: 'Total Cap',    icon: <Zap className="w-3.5 h-3.5" />,     desc: 'Max total deployed USDT' },
  { key: 'per_coin',label: 'Per Coin',     icon: <Layers className="w-3.5 h-3.5" />,  desc: 'Individual per-coin limit' },
];

const BudgetPanel = ({ watchedCoins = Object.keys(DEFAULT_PER_COIN) }: { watchedCoins?: string[] }) => {
  const [cfg, setCfg]         = useState<Config>(DEFAULT_CONFIG);
  const [saving, setSaving]   = useState(false);
  const [botOnline, setBotOnline] = useState(false);

  const loadConfig = useCallback(async () => {
    try {
      const res = await fetch(`${BOT_API}/config`, { signal: AbortSignal.timeout(3000) });
      if (res.ok) {
        const data = await res.json();
        setCfg(c => ({ ...c, ...data }));
        setBotOnline(true);
      }
    } catch {
      setBotOnline(false);
    }
  }, []);

  useEffect(() => { loadConfig(); }, [loadConfig]);

  const patch = useCallback(async (updates: Partial<Config>) => {
    setCfg(c => ({ ...c, ...updates }));
    setSaving(true);
    try {
      const res = await fetch(`${BOT_API}/config`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(updates),
        signal: AbortSignal.timeout(4000),
      });
      if (res.ok) {
        setBotOnline(true);
        toast.success('Budget config saved to bot');
      } else {
        toast.error('Bot rejected config update');
      }
    } catch {
      setBotOnline(false);
      toast.warning('Bot offline — config saved locally only');
    } finally {
      setSaving(false);
    }
  }, []);

  const maxPositions = 5;
  const perTradeFromCap = cfg.budget_total_cap_usdt / maxPositions;

  return (
    <div className="bg-card border border-border rounded-lg p-4 space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Settings2 className="w-4 h-4 text-accent" />
          <h3 className="text-sm font-semibold">Budget Settings</h3>
          <span className="text-[9px] font-mono px-1.5 py-0.5 rounded bg-accent/20 text-accent">Python bot</span>
        </div>
        <div className={`text-[9px] font-mono flex items-center gap-1 ${botOnline ? 'text-gain' : 'text-muted-foreground'}`}>
          <span className={`w-1.5 h-1.5 rounded-full ${botOnline ? 'bg-gain animate-pulse' : 'bg-muted-foreground'}`} />
          {botOnline ? 'Bot connected' : 'Bot offline'}
        </div>
      </div>

      {/* Mode selector */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-1.5">
        {MODES.map(m => (
          <button
            key={m.key}
            onClick={() => patch({ budget_mode: m.key })}
            disabled={saving}
            className={`flex flex-col items-center gap-1 py-2.5 px-2 rounded-md text-xs font-semibold transition-colors border
              ${cfg.budget_mode === m.key
                ? 'bg-accent text-accent-foreground border-accent'
                : 'bg-muted/20 text-muted-foreground border-border hover:border-accent/50 hover:text-foreground'
              } disabled:opacity-50`}
          >
            {m.icon}
            <span>{m.label}</span>
            <span className="text-[8px] font-normal opacity-70">{m.desc}</span>
          </button>
        ))}
      </div>

      {/* Mode-specific inputs */}
      <div className="bg-muted/20 border border-border rounded-md p-3 space-y-3">

        {cfg.budget_mode === 'fixed' && (
          <div>
            <label className="text-[10px] uppercase tracking-widest text-muted-foreground block mb-1.5">
              Trade size (USDT)
            </label>
            <div className="flex items-center gap-2">
              <input
                type="number" min={10} step={10}
                value={cfg.budget_fixed_usdt}
                onChange={e => setCfg(c => ({ ...c, budget_fixed_usdt: Number(e.target.value) }))}
                onBlur={e => patch({ budget_fixed_usdt: Math.max(10, Number(e.target.value)) })}
                className="w-32 bg-muted/40 border border-border rounded px-2 py-1.5 text-xs font-mono focus:outline-none focus:border-accent"
              />
              <span className="text-[10px] text-muted-foreground">USDT per trade</span>
            </div>
            <p className="text-[9px] text-muted-foreground mt-1.5">
              Each trade will allocate exactly <span className="text-foreground font-mono">{cfg.budget_fixed_usdt} USDT</span> regardless of balance.
            </p>
          </div>
        )}

        {cfg.budget_mode === 'percent' && (
          <div>
            <label className="text-[10px] uppercase tracking-widest text-muted-foreground block mb-1.5">
              % of free balance per trade
            </label>
            <div className="flex items-center gap-3">
              <input
                type="range" min={1} max={25} step={1}
                value={cfg.budget_pct_of_free}
                onChange={e => setCfg(c => ({ ...c, budget_pct_of_free: Number(e.target.value) }))}
                onMouseUp={e => patch({ budget_pct_of_free: Number((e.target as HTMLInputElement).value) })}
                onTouchEnd={e => patch({ budget_pct_of_free: Number((e.target as HTMLInputElement).value) })}
                className="flex-1 accent-accent"
              />
              <span className="text-sm font-mono font-bold w-10 text-right">{cfg.budget_pct_of_free}%</span>
            </div>
            <div className="flex items-center justify-between mt-1.5 text-[9px] text-muted-foreground">
              <span>1%</span>
              <span className="text-foreground">At 1,000 USDT free: <span className="font-mono text-accent">{(1000 * cfg.budget_pct_of_free / 100).toFixed(2)} USDT</span> per trade</span>
              <span>25%</span>
            </div>
          </div>
        )}

        {cfg.budget_mode === 'capped' && (
          <div>
            <label className="text-[10px] uppercase tracking-widest text-muted-foreground block mb-1.5">
              Total bot allocation (USDT)
            </label>
            <div className="flex items-center gap-2">
              <input
                type="number" min={50} step={50}
                value={cfg.budget_total_cap_usdt}
                onChange={e => setCfg(c => ({ ...c, budget_total_cap_usdt: Number(e.target.value) }))}
                onBlur={e => patch({ budget_total_cap_usdt: Math.max(50, Number(e.target.value)) })}
                className="w-32 bg-muted/40 border border-border rounded px-2 py-1.5 text-xs font-mono focus:outline-none focus:border-accent"
              />
              <span className="text-[10px] text-muted-foreground">USDT maximum deployed</span>
            </div>
            <p className="text-[9px] text-muted-foreground mt-1.5">
              Cap: <span className="text-foreground font-mono">{cfg.budget_total_cap_usdt} USDT</span>
              {' · '}Per-trade size (÷{maxPositions} max): <span className="text-accent font-mono">{perTradeFromCap.toFixed(2)} USDT</span>
            </p>
          </div>
        )}

        {cfg.budget_mode === 'per_coin' && (
          <div>
            <label className="text-[10px] uppercase tracking-widest text-muted-foreground block mb-2">
              Per-coin trade size (USDT)
            </label>
            <div className="space-y-2">
              {watchedCoins.map(sym => {
                const ticker = sym.replace('USDT', '');
                const val = cfg.budget_per_coin[sym] ?? DEFAULT_PER_COIN[sym] ?? 100;
                return (
                  <div key={sym} className="flex items-center gap-3">
                    <span className="text-xs font-mono font-bold w-14 shrink-0">{ticker}</span>
                    <input
                      type="number" min={10} step={10}
                      value={val}
                      onChange={e => setCfg(c => ({
                        ...c,
                        budget_per_coin: { ...c.budget_per_coin, [sym]: Number(e.target.value) },
                      }))}
                      onBlur={e => patch({
                        budget_per_coin: { ...cfg.budget_per_coin, [sym]: Math.max(10, Number(e.target.value)) },
                      })}
                      className="w-28 bg-muted/40 border border-border rounded px-2 py-1 text-xs font-mono focus:outline-none focus:border-accent"
                    />
                    <span className="text-[10px] text-muted-foreground">USDT</span>
                  </div>
                );
              })}
            </div>
          </div>
        )}
      </div>

      {!botOnline && (
        <p className="text-[10px] text-muted-foreground bg-muted/20 rounded px-3 py-2">
          Python bot is not running on localhost:8000. Settings will be applied when the bot starts.
        </p>
      )}
    </div>
  );
};

export default BudgetPanel;
