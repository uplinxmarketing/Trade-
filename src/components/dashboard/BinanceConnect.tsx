import { useState, useEffect } from 'react';
import { Key, Eye, EyeOff, CheckCircle2, XCircle, Loader2, X } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { supabase } from '@/integrations/supabase/client';
import { toast } from 'sonner';

interface BinanceConnectProps {
  isOpen: boolean;
  onClose: () => void;
  onConnectionChange: (connected: boolean) => void;
}

const BinanceConnect = ({ isOpen, onClose, onConnectionChange }: BinanceConnectProps) => {
  const [apiKey, setApiKey] = useState('');
  const [apiSecret, setApiSecret] = useState('');
  const [showSecret, setShowSecret] = useState(false);
  const [testing, setTesting] = useState(false);
  const [connectionStatus, setConnectionStatus] = useState<'unknown' | 'connected' | 'failed'>('unknown');
  const [accountInfo, setAccountInfo] = useState<{ balances: number; canTrade: boolean } | null>(null);

  useEffect(() => {
    if (isOpen) testExistingConnection();
  }, [isOpen]);

  const testExistingConnection = async () => {
    setTesting(true);
    try {
      const { data, error } = await supabase.functions.invoke('binance-proxy', {
        body: { action: 'account', params: {} },
      });
      if (error || data?.error) {
        setConnectionStatus('failed');
        onConnectionChange(false);
      } else {
        setConnectionStatus('connected');
        onConnectionChange(true);
        const nonZeroBalances = data.balances?.filter((b: any) => parseFloat(b.free) > 0 || parseFloat(b.locked) > 0) || [];
        setAccountInfo({ balances: nonZeroBalances.length, canTrade: data.canTrade });
      }
    } catch {
      setConnectionStatus('failed');
      onConnectionChange(false);
    } finally {
      setTesting(false);
    }
  };

  const saveAndTest = async () => {
    if (!apiKey.trim() || !apiSecret.trim()) {
      toast.error('Both API Key and Secret are required');
      return;
    }
    setTesting(true);
    try {
      // Save keys via edge function that stores them
      const { data, error } = await supabase.functions.invoke('binance-proxy', {
        body: { action: 'save_keys', params: { apiKey: apiKey.trim(), apiSecret: apiSecret.trim() } },
      });
      if (error) throw error;
      if (data?.error) throw new Error(data.error);

      // Now test connection
      await testExistingConnection();
      toast.success('Binance API connected successfully');
    } catch (err: any) {
      toast.error('Connection failed', { description: err.message });
      setConnectionStatus('failed');
      onConnectionChange(false);
    } finally {
      setTesting(false);
    }
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-background/80 backdrop-blur-sm">
      <div className="bg-card border border-border rounded-lg p-6 w-full max-w-md mx-4 shadow-2xl">
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center gap-2">
            <Key className="w-5 h-5 text-accent" />
            <h2 className="text-base font-semibold">Binance API Connection</h2>
          </div>
          <button onClick={onClose} className="text-muted-foreground hover:text-foreground transition-colors">
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Connection status */}
        <div className={`flex items-center gap-2 p-3 rounded-md mb-4 ${
          connectionStatus === 'connected' ? 'bg-gain/10 border border-gain/20' :
          connectionStatus === 'failed' ? 'bg-loss/10 border border-loss/20' :
          'bg-muted/20 border border-border'
        }`}>
          {testing ? (
            <Loader2 className="w-4 h-4 animate-spin text-muted-foreground" />
          ) : connectionStatus === 'connected' ? (
            <CheckCircle2 className="w-4 h-4 text-gain" />
          ) : connectionStatus === 'failed' ? (
            <XCircle className="w-4 h-4 text-loss" />
          ) : (
            <div className="w-4 h-4 rounded-full bg-muted-foreground/30" />
          )}
          <span className={`text-sm font-medium ${
            connectionStatus === 'connected' ? 'text-gain' :
            connectionStatus === 'failed' ? 'text-loss' : 'text-muted-foreground'
          }`}>
            {testing ? 'Testing connection...' :
             connectionStatus === 'connected' ? 'Connected' :
             connectionStatus === 'failed' ? 'Not connected' : 'Unknown'}
          </span>
          {accountInfo && connectionStatus === 'connected' && (
            <span className="text-xs text-muted-foreground ml-auto">
              {accountInfo.balances} assets · {accountInfo.canTrade ? 'Trading enabled' : 'Read-only'}
            </span>
          )}
        </div>

        {/* API Key inputs */}
        <div className="space-y-3">
          <div>
            <label className="text-[10px] uppercase tracking-widest text-muted-foreground mb-1 block">API Key</label>
            <Input
              type="text"
              placeholder="Enter your Binance API key"
              value={apiKey}
              onChange={e => setApiKey(e.target.value)}
              className="bg-muted/40 border-border text-sm font-mono"
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
            Your keys are stored securely as encrypted secrets and never exposed to the frontend. 
            Enable "Spot Trading" permission on Binance. Do NOT enable withdrawal permissions.
          </p>

          <div className="flex gap-2">
            <Button
              onClick={saveAndTest}
              disabled={testing || !apiKey.trim() || !apiSecret.trim()}
              className="flex-1 font-semibold"
            >
              {testing ? <Loader2 className="w-4 h-4 animate-spin mr-1.5" /> : null}
              Save & Connect
            </Button>
            <Button
              variant="outline"
              onClick={testExistingConnection}
              disabled={testing}
              className="font-medium"
            >
              Test
            </Button>
          </div>
        </div>
      </div>
    </div>
  );
};

export default BinanceConnect;
