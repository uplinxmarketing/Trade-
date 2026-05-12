import { useState, useEffect, useCallback } from 'react';
import { Key, Eye, EyeOff, CheckCircle2, XCircle, Loader2, X, Wifi, FlaskConical, AlertTriangle, RefreshCw, Edit2 } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { toast } from 'sonner';
import { API_BASE } from '@/config';

interface BinanceConnectProps {
  isOpen: boolean;
  onClose: () => void;
  onConnectionChange: (connected: boolean) => void;
}

type BotMode = 'paper' | 'live' | 'testnet' | 'unknown';

interface BotStatus {
  mode: BotMode;
  running: boolean;
  balance_usdt: number;
  live_error?: string;
}

const MIN_TRADE_USDT = 10; // Binance minimum notional

const BinanceConnect = ({ isOpen, onClose, onConnectionChange }: BinanceConnectProps) => {
  const [apiKey, setApiKey]       = useState('');
  const [apiSecret, setApiSecret] = useState('');
  const [showSecret, setShowSecret] = useState(false);
  const [saving, setSaving]       = useState(false);
  const [checking, setChecking]   = useState(false);
  const [botStatus, setBotStatus] = useState<BotStatus | null>(null);
  const [restartPoll, setRestartPoll] = useState<ReturnType<typeof setInterval> | null>(null);
  const [showUpdateKeys, setShowUpdateKeys] = useState(false);

  const checkStatus = useCallback(async () => {
    setChecking(true);
    try {
      const res = await fetch(`${API_BASE}/api/status`, { cache: 'no-store' });
      if (!res.ok) { setBotStatus(null); onConnectionChange(false); return; }
      const d = await res.json();
      const status: BotStatus = {
        mode:         d.mode ?? 'unknown',
        running:      Boolean(d.running),
        balance_usdt: Number(d.balance_usdt ?? 0),
        live_error:   d.live_error ?? undefined,
      };
      setBotStatus(status);
      onConnectionChange(status.mode === 'live' && !status.live_error);
    } catch {
      setBotStatus(null);
      onConnectionChange(false);
    } finally {
      setChecking(false);
    }
  }, [onConnectionChange]);

  useEffect(() => {
    if (isOpen) checkStatus();
  }, [isOpen, checkStatus]);

  // Poll after switch until the new mode appears (process restarts take ~3–5 s)
  const pollUntilMode = useCallback((targetMode: BotMode) => {
    let attempts = 0;
    const id = setInterval(async () => {
      attempts++;
      try {
        const res = await fetch(`${API_BASE}/api/status`, { cache: 'no-store' });
        if (res.ok) {
          const d = await res.json();
          const status: BotStatus = {
            mode:         d.mode ?? 'unknown',
            running:      Boolean(d.running),
            balance_usdt: Number(d.balance_usdt ?? 0),
            live_error:   d.live_error ?? undefined,
          };
          // Live connection failed — bot fell back to paper
          if (targetMode === 'live' && d.live_error) {
            clearInterval(id);
            setRestartPoll(null);
            setBotStatus(status);
            onConnectionChange(false);
            toast.error('Binance connection failed', { description: d.live_error });
            return;
          }
          if (d.mode === targetMode) {
            clearInterval(id);
            setRestartPoll(null);
            setBotStatus(status);
            onConnectionChange(targetMode === 'live');
            setShowUpdateKeys(false);
            if (targetMode === 'live') {
              const bal = Number(d.balance_usdt ?? 0);
              if (bal < MIN_TRADE_USDT) {
                toast.warning(`Connected — but only $${bal.toFixed(2)} USDT available. Binance requires ≥$${MIN_TRADE_USDT} per trade.`);
              } else {
                toast.success(`Connected to Binance Live — $${bal.toFixed(2)} USDT`);
              }
            } else {
              toast.success('Switched to Paper mode');
            }
            return;
          }
        }
      } catch { /* still restarting */ }
      if (attempts >= 30) {
        clearInterval(id);
        setRestartPoll(null);
        toast.error('Timeout waiting for bot restart — check bot logs');
        checkStatus();
      }
    }, 2000);
    setRestartPoll(id);
  }, [onConnectionChange, checkStatus]);

  useEffect(() => {
    return () => { if (restartPoll) clearInterval(restartPoll); };
  }, [restartPoll]);

  const connectLive = async () => {
    if (!apiKey.trim() || !apiSecret.trim()) {
      toast.error('Both API Key and Secret are required');
      return;
    }
    setSaving(true);
    try {
      const res = await fetch(`${API_BASE}/api/mode`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ mode: 'live', api_key: apiKey.trim(), api_secret: apiSecret.trim() }),
      });
      const d = await res.json();
      if (!d.ok) throw new Error(d.error ?? 'Unknown error');
      toast.info('Credentials saved — bot restarting…');
      setApiKey('');
      setApiSecret('');
      pollUntilMode('live');
    } catch (err: any) {
      toast.error('Failed to connect', { description: err.message });
    } finally {
      setSaving(false);
    }
  };

  const switchToPaper = async () => {
    setSaving(true);
    try {
      const res = await fetch(`${API_BASE}/api/mode`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ mode: 'paper' }),
      });
      const d = await res.json();
      if (!d.ok) throw new Error(d.error ?? 'Unknown error');
      toast.info('Switching to Paper mode — bot restarting…');
      pollUntilMode('paper');
    } catch (err: any) {
      toast.error('Switch failed', { description: err.message });
    } finally {
      setSaving(false);
    }
  };

  const isRestarting = restartPoll !== null;
  const isLive       = botStatus?.mode === 'live';
  const lowBalance   = isLive && (botStatus?.balance_usdt ?? 0) < MIN_TRADE_USDT && (botStatus?.balance_usdt ?? 0) > 0;

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-background/80 backdrop-blur-sm">
      <div className="bg-card border border-border rounded-lg p-6 w-full max-w-md mx-4 shadow-2xl">

        {/* Header */}
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center gap-2">
            <Key className="w-5 h-5 text-accent" />
            <h2 className="text-base font-semibold">Binance Connection</h2>
          </div>
          <button onClick={onClose} className="text-muted-foreground hover:text-foreground transition-colors">
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Current status */}
        <div className={`flex items-center gap-2 p-3 rounded-md mb-4 ${
          isRestarting                  ? 'bg-warn/10 border border-warn/20'  :
          botStatus?.mode === 'live'    ? 'bg-gain/10 border border-gain/20'  :
          botStatus?.mode === 'paper'   ? 'bg-accent/10 border border-accent/20' :
          botStatus === null            ? 'bg-loss/10 border border-loss/20'  :
          'bg-muted/20 border border-border'
        }`}>
          {isRestarting || checking ? (
            <Loader2 className="w-4 h-4 animate-spin text-warn" />
          ) : botStatus?.mode === 'live' ? (
            <Wifi className="w-4 h-4 text-gain" />
          ) : botStatus?.mode === 'paper' ? (
            <FlaskConical className="w-4 h-4 text-accent" />
          ) : (
            <XCircle className="w-4 h-4 text-loss" />
          )}
          <span className={`text-sm font-medium ${
            isRestarting                ? 'text-warn'   :
            botStatus?.mode === 'live'  ? 'text-gain'   :
            botStatus?.mode === 'paper' ? 'text-accent'  :
            'text-loss'
          }`}>
            {isRestarting ? 'Restarting bot…' :
             checking     ? 'Checking…' :
             botStatus?.mode === 'live'    ? 'Live — Binance connected' :
             botStatus?.mode === 'paper'   ? 'Paper trading mode' :
             botStatus?.mode === 'testnet' ? 'Testnet mode' :
             'Bot unreachable'}
          </span>
          {!isRestarting && botStatus?.mode === 'live' && (
            <span className="text-xs text-muted-foreground ml-auto font-mono">
              ${botStatus.balance_usdt.toFixed(2)} USDT
            </span>
          )}
          {!isRestarting && botStatus && (
            <button onClick={checkStatus} className="ml-auto text-[10px] text-muted-foreground hover:text-foreground flex items-center gap-1">
              {checking ? '' : <><RefreshCw className="w-2.5 h-2.5" />Refresh</>}
            </button>
          )}
        </div>

        {isRestarting && (
          <p className="text-xs text-muted-foreground text-center mb-4">
            Waiting for the bot to restart with new credentials…
          </p>
        )}

        {/* Low balance warning */}
        {!isRestarting && lowBalance && (
          <div className="flex items-start gap-2 p-3 rounded bg-warn/10 border border-warn/30 text-xs mb-3">
            <AlertTriangle className="w-4 h-4 text-warn mt-0.5 shrink-0" />
            <div>
              <div className="font-semibold text-warn">Insufficient balance for trading</div>
              <div className="text-muted-foreground mt-0.5">
                Your Binance account has ${botStatus!.balance_usdt.toFixed(2)} USDT.
                Binance requires a minimum of ${MIN_TRADE_USDT} USDT per trade (minimum notional value).
                Deposit more USDT to start trading.
              </div>
            </div>
          </div>
        )}

        {/* Live connection error */}
        {!isRestarting && botStatus?.live_error && (
          <div className="flex items-start gap-2 p-3 rounded bg-loss/10 border border-loss/30 text-xs mb-3">
            <XCircle className="w-4 h-4 text-loss mt-0.5 shrink-0" />
            <div>
              <div className="font-semibold text-loss">Binance connection failed</div>
              <div className="text-muted-foreground mt-0.5">{botStatus.live_error}</div>
              <div className="text-muted-foreground mt-1">Check your API key, secret, and IP whitelist, then try again.</div>
            </div>
          </div>
        )}

        {/* Live mode: connected state */}
        {!isRestarting && isLive && !showUpdateKeys && (
          <div className="space-y-3">
            <div className="flex items-start gap-2 p-3 rounded bg-gain/5 border border-gain/20 text-xs">
              <CheckCircle2 className="w-4 h-4 text-gain mt-0.5 shrink-0" />
              <div>
                <div className="font-semibold text-foreground">Real Binance account connected</div>
                <div className="text-muted-foreground mt-0.5">
                  The bot is placing real orders on your Binance spot account. Positions and balances
                  shown in the wallet panel are your actual Binance holdings.
                </div>
              </div>
            </div>
            <div className="flex gap-2">
              <Button
                variant="outline"
                className="flex-1 text-xs"
                onClick={() => setShowUpdateKeys(true)}
              >
                <Edit2 className="w-3.5 h-3.5 mr-1.5" />
                Update API Keys
              </Button>
              <Button
                variant="outline"
                className="flex-1 text-loss border-loss/30 hover:bg-loss/10 text-xs"
                onClick={switchToPaper}
                disabled={saving}
              >
                {saving ? <Loader2 className="w-3.5 h-3.5 animate-spin mr-1.5" /> : null}
                Switch to Paper
              </Button>
            </div>
          </div>
        )}

        {/* API key form — shown in paper mode, or when updating keys in live mode */}
        {!isRestarting && (!isLive || showUpdateKeys) && (
          <div className="space-y-3">
            {showUpdateKeys && (
              <div className="flex items-center justify-between">
                <span className="text-xs font-semibold text-foreground">Update API Keys</span>
                <button onClick={() => setShowUpdateKeys(false)} className="text-[10px] text-muted-foreground hover:text-foreground">Cancel</button>
              </div>
            )}

            {!showUpdateKeys && (
              <div className="flex items-start gap-2 p-3 rounded bg-warn/5 border border-warn/20 text-xs mb-1">
                <AlertTriangle className="w-4 h-4 text-warn mt-0.5 shrink-0" />
                <div>
                  <div className="font-semibold text-foreground">Live trading uses real money</div>
                  <div className="text-muted-foreground mt-0.5">
                    Enable Spot Trading on your Binance API key. Do NOT enable Withdrawals or Margin.
                    Minimum ${MIN_TRADE_USDT} USDT balance required to trade.
                  </div>
                </div>
              </div>
            )}

            <div>
              <label className="text-[10px] uppercase tracking-widest text-muted-foreground mb-1 block">API Key</label>
              <Input
                type="text"
                placeholder="Enter your Binance API key"
                value={apiKey}
                onChange={e => setApiKey(e.target.value)}
                className="bg-muted/40 border-border text-sm font-mono"
                autoComplete="off"
              />
            </div>
            <div>
              <label className="text-[10px] uppercase tracking-widest text-muted-foreground mb-1 block">API Secret</label>
              <div className="relative">
                <Input
                  type={showSecret ? 'text' : 'password'}
                  placeholder="Enter your Binance API secret"
                  value={apiSecret}
                  onChange={e => setApiSecret(e.target.value)}
                  className="bg-muted/40 border-border text-sm font-mono pr-10"
                  autoComplete="off"
                />
                <button
                  type="button"
                  onClick={() => setShowSecret(!showSecret)}
                  className="absolute right-3 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
                >
                  {showSecret ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
                </button>
              </div>
            </div>

            <p className="text-[10px] text-muted-foreground leading-relaxed">
              Keys are saved securely to the server environment and never sent to Supabase or any third party.
              The bot restarts automatically after saving — this takes ~5 seconds.
            </p>

            <div className="flex gap-2">
              <Button
                onClick={connectLive}
                disabled={saving || !apiKey.trim() || !apiSecret.trim()}
                className="flex-1 font-semibold"
              >
                {saving ? <Loader2 className="w-4 h-4 animate-spin mr-1.5" /> : <Wifi className="w-4 h-4 mr-1.5" />}
                {showUpdateKeys ? 'Save New Keys' : 'Connect to Binance Live'}
              </Button>
              <Button variant="outline" onClick={checkStatus} disabled={checking}>
                {checking ? <Loader2 className="w-4 h-4 animate-spin" /> : 'Test'}
              </Button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default BinanceConnect;
