import { useState, useEffect } from 'react';
import { AlertTriangle } from 'lucide-react';
import { Button } from '@/components/ui/button';

// ── Phase 5 §5.2.7 — LIVE-mode confirmation modal ─────────────────────────────
// Requires the user to type the word LIVE before the confirm button enables.
// Used by strategy settings save, signals editor save and config rollback when
// the bot is running in live mode. onConfirm receives the confirm string that
// must be included in the request body (confirm: "LIVE").

interface LiveConfirmModalProps {
  open: boolean;
  title?: string;
  description?: string;
  onConfirm: (confirmWord: string) => void;
  onCancel: () => void;
}

const CONFIRM_WORD = 'LIVE';

export function LiveConfirmModal({
  open,
  title = 'Confirm live change',
  description = 'The bot is RUNNING in LIVE mode. This change takes effect immediately and affects real-money trading.',
  onConfirm,
  onCancel,
}: LiveConfirmModalProps) {
  const [text, setText] = useState('');

  // Reset the input every time the modal opens
  useEffect(() => { if (open) setText(''); }, [open]);

  if (!open) return null;

  const ok = text.trim() === CONFIRM_WORD;

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4"
      onClick={onCancel}
    >
      <div
        className="w-full max-w-sm bg-background border border-loss/40 rounded-lg shadow-xl p-4 space-y-3"
        onClick={e => e.stopPropagation()}
      >
        <div className="flex items-center gap-2">
          <div className="p-1.5 rounded bg-loss/20">
            <AlertTriangle className="w-4 h-4 text-loss" />
          </div>
          <h3 className="text-sm font-bold text-loss">{title}</h3>
        </div>
        <p className="text-[11px] text-muted-foreground leading-relaxed">{description}</p>
        <div className="space-y-1">
          <label className="text-[10px] uppercase tracking-wider text-muted-foreground">
            Type <span className="font-bold text-loss">{CONFIRM_WORD}</span> to confirm
          </label>
          <input
            autoFocus
            value={text}
            onChange={e => setText(e.target.value)}
            onKeyDown={e => { if (e.key === 'Enter' && ok) onConfirm(CONFIRM_WORD); if (e.key === 'Escape') onCancel(); }}
            placeholder={CONFIRM_WORD}
            className={`w-full bg-muted/40 border rounded px-2 py-1.5 text-sm font-mono focus:outline-none ${
              ok ? 'border-loss/60 text-loss' : 'border-border focus:border-loss/40'
            }`}
          />
        </div>
        <div className="flex gap-2 pt-1">
          <Button variant="outline" size="sm" className="flex-1" onClick={onCancel}>
            Cancel
          </Button>
          <Button
            size="sm"
            variant="destructive"
            className="flex-1 font-bold"
            disabled={!ok}
            onClick={() => onConfirm(CONFIRM_WORD)}
          >
            Confirm live change
          </Button>
        </div>
      </div>
    </div>
  );
}

export default LiveConfirmModal;
