import { useState, useEffect, useCallback } from 'react';
import { Plus, Trash2, Play, Pause, Square, Settings2, DollarSign, Coins, Shield, AlertTriangle, Bot, Search } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { supabase } from '@/integrations/supabase/client';
import { toast } from 'sonner';
import { BINANCE_COINS } from '@/lib/binance-coins';

interface TradingBot {
  id: string;
  bot_name: string;
  status: string;
  assigned_coins: string[];
  allocated_budget: number;
  available_balance: number;
  used_balance: number;
  max_budget_cap: number;
  reinvest_profits: boolean;
  fixed_balance_mode: boolean;
  max_trades_per_hour: number;
  cooldown_seconds: number;
  allow_multiple_open: boolean;
  min_profit_percent: number;
  stop_loss_percent: number;
  max_daily_loss: number;
  max_drawdown_percent: number;
  stop_after_consecutive_losses: number;
  consecutive_losses: number;
  daily_loss: number;
  total_trades: number;
  winning_trades: number;
  total_pnl: number;
  last_trade_at: string | null;
}

interface BotManagerProps {
  onKillSwitch: () => void;
  killSwitchActive: boolean;
}

const BotManager = ({ onKillSwitch, killSwitchActive }: BotManagerProps) => {
  const [bots, setBots] = useState<TradingBot[]>([]);
  const [expandedBot, setExpandedBot] = useState<string | null>(null);
  const [newBotName, setNewBotName] = useState('');
  const [showCreate, setShowCreate] = useState(false);
  const [addFundsBot, setAddFundsBot] = useState<string | null>(null);
  const [fundsAmount, setFundsAmount] = useState('');

  const loadBots = useCallback(async () => {
    const { data } = await supabase
      .from('trading_bots')
      .select('*')
      .eq('user_session', 'default')
      .order('created_at', { ascending: true });
    if (data) setBots(data as unknown as TradingBot[]);
  }, []);

  useEffect(() => {
    loadBots();
    const ch = supabase
      .channel('bots-realtime')
      .on('postgres_changes', { event: '*', schema: 'public', table: 'trading_bots' }, loadBots)
      .subscribe();
    return () => { supabase.removeChannel(ch); };
  }, [loadBots]);

  const createBot = async () => {
    const name = newBotName.trim() || `Bot ${bots.length + 1}`;
    await supabase.from('trading_bots').insert({
      user_session: 'default',
      bot_name: name,
      assigned_coins: ['BTCUSDT'],
      allocated_budget: 100,
      available_balance: 100,
    } as any);
    setNewBotName('');
    setShowCreate(false);
    toast.success(`${name} created`);
    loadBots();
  };

  const deleteBot = async (id: string, name: string) => {
    await supabase.from('trading_bots').delete().eq('id', id);
    toast.info(`${name} deleted`);
    loadBots();
  };

  const updateBotStatus = async (id: string, status: string) => {
    await supabase.from('trading_bots').update({
      status,
      updated_at: new Date().toISOString(),
    } as any).eq('id', id);
    const statusLabels: Record<string, string> = { active: 'started', paused: 'paused', stopped: 'stopped' };
    toast.success(`Bot ${statusLabels[status]}`);
    loadBots();
  };

  const updateBotField = async (id: string, field: string, value: any) => {
    await supabase.from('trading_bots').update({
      [field]: value,
      updated_at: new Date().toISOString(),
    } as any).eq('id', id);
    loadBots();
  };

  const addFunds = async (botId: string) => {
    const amount = parseFloat(fundsAmount);
    if (!amount || amount <= 0) return;
    const bot = bots.find(b => b.id === botId);
    if (!bot) return;
    const newBudget = bot.allocated_budget + amount;
    const newAvail = bot.available_balance + amount;
    if (newBudget > bot.max_budget_cap) {
      toast.error(`Exceeds max cap of $${bot.max_budget_cap}`);
      return;
    }
    await supabase.from('trading_bots').update({
      allocated_budget: newBudget,
      available_balance: newAvail,
      updated_at: new Date().toISOString(),
    } as any).eq('id', botId);
    setFundsAmount('');
    setAddFundsBot(null);
    toast.success(`Added $${amount} to ${bot.bot_name}`);
    loadBots();
  };

  const removeFunds = async (botId: string) => {
    const amount = parseFloat(fundsAmount);
    if (!amount || amount <= 0) return;
    const bot = bots.find(b => b.id === botId);
    if (!bot) return;
    if (amount > bot.available_balance) {
      toast.error('Cannot remove more than available balance');
      return;
    }
    await supabase.from('trading_bots').update({
      allocated_budget: bot.allocated_budget - amount,
      available_balance: bot.available_balance - amount,
      updated_at: new Date().toISOString(),
    } as any).eq('id', botId);
    setFundsAmount('');
    setAddFundsBot(null);
    toast.success(`Removed $${amount} from ${bot.bot_name}`);
    loadBots();
  };

  const toggleCoin = async (botId: string, coin: string, currentCoins: string[]) => {
    const newCoins = currentCoins.includes(coin)
      ? currentCoins.filter(c => c !== coin)
      : [...currentCoins, coin];
    await updateBotField(botId, 'assigned_coins', newCoins);
  };

  const activeBots = bots.filter(b => b.status === 'active').length;

  return (
    <div className="bg-card border border-border rounded-lg p-4 space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Bot className="w-4 h-4 text-accent" />
          <h3 className="text-sm font-medium">Bot Manager</h3>
          <span className="text-[10px] font-mono text-muted-foreground bg-muted/40 px-1.5 py-0.5 rounded">
            {activeBots}/{bots.length} active
          </span>
        </div>
        <div className="flex items-center gap-1.5">
          {/* Kill Switch */}
          <Button
            variant="ghost"
            size="icon"
            className={`h-7 w-7 ${killSwitchActive ? 'text-loss' : 'text-muted-foreground'}`}
            onClick={onKillSwitch}
            title="Kill Switch — stop all bots"
          >
            <AlertTriangle className="w-3.5 h-3.5" />
          </Button>
          <Button
            variant="ghost"
            size="sm"
            className="h-7 text-xs gap-1"
            onClick={() => setShowCreate(!showCreate)}
          >
            <Plus className="w-3 h-3" /> New Bot
          </Button>
        </div>
      </div>

      {/* Kill switch warning */}
      {killSwitchActive && (
        <div className="flex items-center gap-2 bg-loss/10 border border-loss/20 rounded-md px-3 py-2">
          <AlertTriangle className="w-3.5 h-3.5 text-loss" />
          <span className="text-xs text-loss font-medium">Kill switch active — all bots stopped</span>
        </div>
      )}

      {/* Create new bot */}
      {showCreate && (
        <div className="flex items-center gap-2 p-2 bg-muted/20 rounded-md">
          <Input
            placeholder="Bot name..."
            value={newBotName}
            onChange={e => setNewBotName(e.target.value)}
            className="h-8 text-xs bg-muted/40"
            onKeyDown={e => e.key === 'Enter' && createBot()}
          />
          <Button size="sm" className="h-8 text-xs" onClick={createBot}>Create</Button>
        </div>
      )}

      {/* Bot list */}
      <div className="space-y-2 max-h-[500px] overflow-y-auto scrollbar-thin">
        {bots.map(bot => {
          const isExpanded = expandedBot === bot.id;
          const winRate = bot.total_trades > 0 ? ((bot.winning_trades / bot.total_trades) * 100).toFixed(1) : '0';
          return (
            <div key={bot.id} className="border border-border/50 rounded-md overflow-hidden">
              {/* Bot header row */}
              <div className="flex items-center gap-2 p-2.5 hover:bg-muted/20 transition-colors">
                <div className={`w-2 h-2 rounded-full flex-shrink-0 ${
                  bot.status === 'active' ? 'bg-gain animate-pulse' :
                  bot.status === 'paused' ? 'bg-warn' : 'bg-muted-foreground'
                }`} />
                <button className="flex-1 text-left" onClick={() => setExpandedBot(isExpanded ? null : bot.id)}>
                  <span className="text-xs font-medium">{bot.bot_name}</span>
                  <div className="flex items-center gap-3 mt-0.5">
                    <span className="text-[10px] font-mono text-muted-foreground">
                      ${bot.available_balance.toFixed(2)}
                    </span>
                    <span className={`text-[10px] font-mono ${bot.total_pnl >= 0 ? 'text-gain' : 'text-loss'}`}>
                      {bot.total_pnl >= 0 ? '+' : ''}${bot.total_pnl.toFixed(2)}
                    </span>
                    <span className="text-[10px] font-mono text-muted-foreground">
                      {bot.assigned_coins.length} coins · {winRate}% win
                    </span>
                  </div>
                </button>
                <div className="flex items-center gap-0.5">
                  {bot.status !== 'active' && (
                    <Button variant="ghost" size="icon" className="h-6 w-6" onClick={() => updateBotStatus(bot.id, 'active')} disabled={killSwitchActive}>
                      <Play className="w-3 h-3 text-gain" />
                    </Button>
                  )}
                  {bot.status === 'active' && (
                    <Button variant="ghost" size="icon" className="h-6 w-6" onClick={() => updateBotStatus(bot.id, 'paused')}>
                      <Pause className="w-3 h-3 text-warn" />
                    </Button>
                  )}
                  {bot.status !== 'stopped' && (
                    <Button variant="ghost" size="icon" className="h-6 w-6" onClick={() => updateBotStatus(bot.id, 'stopped')}>
                      <Square className="w-3 h-3 text-loss" />
                    </Button>
                  )}
                  <Button variant="ghost" size="icon" className="h-6 w-6" onClick={() => deleteBot(bot.id, bot.bot_name)}>
                    <Trash2 className="w-3 h-3 text-muted-foreground hover:text-loss" />
                  </Button>
                </div>
              </div>

              {/* Expanded settings */}
              {isExpanded && (
                <div className="px-3 pb-3 space-y-3 border-t border-border/30 pt-3">
                  {/* Budget management */}
                  <div>
                    <label className="text-[10px] uppercase tracking-widest text-muted-foreground flex items-center gap-1 mb-1.5">
                      <DollarSign className="w-3 h-3" /> Budget
                    </label>
                    <div className="grid grid-cols-3 gap-2 text-[10px]">
                      <div className="bg-muted/20 rounded p-1.5">
                        <span className="text-muted-foreground block">Allocated</span>
                        <span className="font-mono font-semibold">${bot.allocated_budget.toFixed(2)}</span>
                      </div>
                      <div className="bg-muted/20 rounded p-1.5">
                        <span className="text-muted-foreground block">Available</span>
                        <span className="font-mono font-semibold">${bot.available_balance.toFixed(2)}</span>
                      </div>
                      <div className="bg-muted/20 rounded p-1.5">
                        <span className="text-muted-foreground block">In Use</span>
                        <span className="font-mono font-semibold">${bot.used_balance.toFixed(2)}</span>
                      </div>
                    </div>
                    {addFundsBot === bot.id ? (
                      <div className="flex items-center gap-1.5 mt-2">
                        <Input
                          type="number" min="1" step="1" placeholder="Amount"
                          value={fundsAmount} onChange={e => setFundsAmount(e.target.value)}
                          className="h-7 text-xs bg-muted/40 font-mono"
                        />
                        <Button size="sm" className="h-7 text-[10px] bg-gain/90 hover:bg-gain text-background" onClick={() => addFunds(bot.id)}>Add</Button>
                        <Button size="sm" variant="outline" className="h-7 text-[10px]" onClick={() => removeFunds(bot.id)}>Remove</Button>
                        <Button size="sm" variant="ghost" className="h-7 text-[10px]" onClick={() => { setAddFundsBot(null); setFundsAmount(''); }}>Cancel</Button>
                      </div>
                    ) : (
                      <Button variant="outline" size="sm" className="h-6 text-[10px] mt-1.5 w-full" onClick={() => setAddFundsBot(bot.id)}>
                        Manage Funds
                      </Button>
                    )}
                    <div className="mt-1.5">
                      <label className="text-[10px] text-muted-foreground">Max Budget Cap</label>
                      <Input
                        type="number" min="10" step="10"
                        value={bot.max_budget_cap}
                        onChange={e => updateBotField(bot.id, 'max_budget_cap', parseFloat(e.target.value) || 1000)}
                        className="h-7 text-xs bg-muted/40 font-mono mt-0.5"
                      />
                    </div>
                  </div>

                  {/* Coin assignment */}
                  <div>
                    <label className="text-[10px] uppercase tracking-widest text-muted-foreground flex items-center gap-1 mb-1.5">
                      <Coins className="w-3 h-3" /> Assigned Coins
                    </label>
                    <div className="flex flex-wrap gap-1">
                      {ALL_COINS.slice(0, 10).map(coin => {
                        const isSelected = bot.assigned_coins.includes(coin);
                        return (
                          <button
                            key={coin}
                            onClick={() => toggleCoin(bot.id, coin, bot.assigned_coins)}
                            className={`px-1.5 py-0.5 rounded text-[10px] font-mono transition-colors ${
                              isSelected ? 'bg-accent/20 text-accent border border-accent/30' : 'text-muted-foreground hover:text-foreground bg-muted/20'
                            }`}
                          >
                            {coin.replace('USDT', '')}
                          </button>
                        );
                      })}
                    </div>
                  </div>

                  {/* Trade settings */}
                  <div>
                    <label className="text-[10px] uppercase tracking-widest text-muted-foreground flex items-center gap-1 mb-1.5">
                      <Settings2 className="w-3 h-3" /> Trade Settings
                    </label>
                    <div className="grid grid-cols-2 gap-2">
                      <div>
                        <label className="text-[10px] text-muted-foreground">Min Profit %</label>
                        <Input type="number" min="0.1" max="10" step="0.1"
                          value={bot.min_profit_percent}
                          onChange={e => updateBotField(bot.id, 'min_profit_percent', parseFloat(e.target.value) || 0.5)}
                          className="h-7 text-xs bg-muted/40 font-mono mt-0.5"
                        />
                      </div>
                      <div>
                        <label className="text-[10px] text-muted-foreground">Stop Loss %</label>
                        <Input type="number" min="0.1" max="10" step="0.1"
                          value={bot.stop_loss_percent}
                          onChange={e => updateBotField(bot.id, 'stop_loss_percent', parseFloat(e.target.value) || 0.8)}
                          className="h-7 text-xs bg-muted/40 font-mono mt-0.5"
                        />
                      </div>
                      <div>
                        <label className="text-[10px] text-muted-foreground">Max Trades/Hr</label>
                        <Input type="number" min="1" max="100" step="1"
                          value={bot.max_trades_per_hour}
                          onChange={e => updateBotField(bot.id, 'max_trades_per_hour', parseInt(e.target.value) || 10)}
                          className="h-7 text-xs bg-muted/40 font-mono mt-0.5"
                        />
                      </div>
                      <div>
                        <label className="text-[10px] text-muted-foreground">Cooldown (s)</label>
                        <Input type="number" min="5" max="300" step="5"
                          value={bot.cooldown_seconds}
                          onChange={e => updateBotField(bot.id, 'cooldown_seconds', parseInt(e.target.value) || 15)}
                          className="h-7 text-xs bg-muted/40 font-mono mt-0.5"
                        />
                      </div>
                    </div>

                    {/* Toggles */}
                    <div className="flex flex-wrap gap-2 mt-2">
                      {[
                        { field: 'reinvest_profits', label: 'Reinvest', value: bot.reinvest_profits },
                        { field: 'fixed_balance_mode', label: 'Fixed Balance', value: bot.fixed_balance_mode },
                        { field: 'allow_multiple_open', label: 'Multi Open', value: bot.allow_multiple_open },
                      ].map(toggle => (
                        <button
                          key={toggle.field}
                          onClick={() => updateBotField(bot.id, toggle.field, !toggle.value)}
                          className={`px-2 py-1 rounded text-[10px] font-medium transition-colors ${
                            toggle.value ? 'bg-accent/20 text-accent' : 'bg-muted/20 text-muted-foreground'
                          }`}
                        >
                          {toggle.label}: {toggle.value ? 'ON' : 'OFF'}
                        </button>
                      ))}
                    </div>
                  </div>

                  {/* Risk controls */}
                  <div>
                    <label className="text-[10px] uppercase tracking-widest text-muted-foreground flex items-center gap-1 mb-1.5">
                      <Shield className="w-3 h-3" /> Risk Controls
                    </label>
                    <div className="grid grid-cols-3 gap-2">
                      <div>
                        <label className="text-[10px] text-muted-foreground">Max Daily Loss</label>
                        <Input type="number" min="1" step="5"
                          value={bot.max_daily_loss}
                          onChange={e => updateBotField(bot.id, 'max_daily_loss', parseFloat(e.target.value) || 50)}
                          className="h-7 text-xs bg-muted/40 font-mono mt-0.5"
                        />
                      </div>
                      <div>
                        <label className="text-[10px] text-muted-foreground">Max DD %</label>
                        <Input type="number" min="1" max="50" step="1"
                          value={bot.max_drawdown_percent}
                          onChange={e => updateBotField(bot.id, 'max_drawdown_percent', parseFloat(e.target.value) || 10)}
                          className="h-7 text-xs bg-muted/40 font-mono mt-0.5"
                        />
                      </div>
                      <div>
                        <label className="text-[10px] text-muted-foreground">Stop After Losses</label>
                        <Input type="number" min="1" max="20" step="1"
                          value={bot.stop_after_consecutive_losses}
                          onChange={e => updateBotField(bot.id, 'stop_after_consecutive_losses', parseInt(e.target.value) || 5)}
                          className="h-7 text-xs bg-muted/40 font-mono mt-0.5"
                        />
                      </div>
                    </div>
                    {bot.consecutive_losses > 0 && (
                      <div className="mt-1.5 text-[10px] text-warn font-mono">
                        ⚠️ {bot.consecutive_losses} consecutive losses ({bot.stop_after_consecutive_losses} max)
                      </div>
                    )}
                  </div>
                </div>
              )}
            </div>
          );
        })}
      </div>

      {bots.length === 0 && (
        <p className="text-xs text-muted-foreground text-center py-4">
          Create your first bot to start trading
        </p>
      )}
    </div>
  );
};

export default BotManager;
