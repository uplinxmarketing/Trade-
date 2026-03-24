import { useEffect, useRef, useCallback } from 'react';
import { supabase } from '@/integrations/supabase/client';
import { toast } from 'sonner';

const NotificationCenter = () => {
  const audioRef = useRef<HTMLAudioElement | null>(null);

  const playSound = useCallback(() => {
    try {
      // Simple beep using Web Audio API
      const ctx = new AudioContext();
      const osc = ctx.createOscillator();
      const gain = ctx.createGain();
      osc.connect(gain);
      gain.connect(ctx.destination);
      osc.frequency.value = 800;
      gain.gain.value = 0.1;
      osc.start();
      osc.stop(ctx.currentTime + 0.15);
    } catch {}
  }, []);

  useEffect(() => {
    const channel = supabase
      .channel('trade-notifications')
      .on('postgres_changes', {
        event: 'INSERT',
        schema: 'public',
        table: 'bot_trade_history',
      }, (payload: any) => {
        const trade = payload.new;
        if (!trade) return;

        playSound();

        const coinName = trade.symbol?.replace('USDT', '') || 'Unknown';
        const pnl = trade.pnl != null ? Number(trade.pnl) : null;
        const isStopLoss = trade.reason?.includes('Stop loss');
        const isTakeProfit = trade.reason?.includes('Take profit') || trade.reason?.includes('Early exit');

        if (trade.side === 'BUY') {
          toast.success(`📈 Bought ${coinName}`, {
            description: `${trade.quantity} @ $${Number(trade.price).toFixed(2)} — ${trade.reason || ''}`,
            duration: 6000,
          });
        } else if (isStopLoss) {
          toast.error(`🛑 Stop Loss: ${coinName}`, {
            description: `Sold @ $${Number(trade.price).toFixed(2)} — Loss: $${pnl?.toFixed(2)}`,
            duration: 8000,
          });
        } else if (isTakeProfit && pnl !== null) {
          toast.success(`💰 Profit: ${coinName}`, {
            description: `+$${pnl.toFixed(2)} — ${trade.reason || ''}`,
            duration: 6000,
          });
        } else {
          toast.info(`${trade.side} ${coinName}`, {
            description: `@ $${Number(trade.price).toFixed(2)}${pnl != null ? ` | P&L: $${pnl.toFixed(2)}` : ''}`,
            duration: 5000,
          });
        }
      })
      .subscribe();

    // Also listen for bot status changes
    const botChannel = supabase
      .channel('bot-status-notifications')
      .on('postgres_changes', {
        event: 'UPDATE',
        schema: 'public',
        table: 'trading_bots',
      }, (payload: any) => {
        const bot = payload.new;
        const old = payload.old;
        if (!bot || !old) return;

        if (old.status !== bot.status) {
          if (bot.status === 'stopped' && old.status === 'active') {
            toast.warning(`⚠️ ${bot.bot_name} stopped`, {
              description: bot.consecutive_losses >= bot.stop_after_consecutive_losses
                ? 'Too many consecutive losses'
                : 'Bot has been stopped',
              duration: 8000,
            });
            playSound();
          }
        }
      })
      .subscribe();

    return () => {
      supabase.removeChannel(channel);
      supabase.removeChannel(botChannel);
    };
  }, [playSound]);

  return null; // Invisible component — just handles notifications
};

export default NotificationCenter;
