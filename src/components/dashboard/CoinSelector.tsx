import { useState } from 'react';
import { Check, X } from 'lucide-react';
import { Button } from '@/components/ui/button';

const ALL_COINS = [
  'BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'DOGEUSDT',
  'XRPUSDT', 'ADAUSDT', 'AVAXUSDT', 'DOTUSDT', 'MATICUSDT',
  'LINKUSDT', 'LTCUSDT', 'UNIUSDT', 'ATOMUSDT', 'NEARUSDT',
];

interface CoinSelectorProps {
  selected: string[];
  onChange: (coins: string[]) => void;
  maxCoins?: number;
}

const CoinSelector = ({ selected, onChange, maxCoins = 5 }: CoinSelectorProps) => {
  const [isOpen, setIsOpen] = useState(false);

  const toggle = (coin: string) => {
    if (selected.includes(coin)) {
      onChange(selected.filter(c => c !== coin));
    } else if (selected.length < maxCoins) {
      onChange([...selected, coin]);
    }
  };

  return (
    <div className="bg-card border border-border rounded-lg p-4">
      <div className="flex items-center justify-between mb-3">
        <h3 className="text-sm font-medium text-muted-foreground">
          Active Coins ({selected.length}/{maxCoins})
        </h3>
        <Button
          variant="ghost"
          size="sm"
          className="text-xs h-7"
          onClick={() => setIsOpen(!isOpen)}
        >
          {isOpen ? 'Done' : 'Edit'}
        </Button>
      </div>

      {/* Selected coins display */}
      <div className="flex flex-wrap gap-1.5 mb-2">
        {selected.map(coin => (
          <span
            key={coin}
            className="inline-flex items-center gap-1 px-2 py-1 rounded bg-accent/20 text-accent text-xs font-mono font-medium"
          >
            {coin.replace('USDT', '')}
            {isOpen && (
              <button
                onClick={() => toggle(coin)}
                className="hover:text-loss transition-colors"
              >
                <X className="w-3 h-3" />
              </button>
            )}
          </span>
        ))}
      </div>

      {/* Coin picker */}
      {isOpen && (
        <div className="mt-3 pt-3 border-t border-border">
          <div className="grid grid-cols-3 gap-1.5">
            {ALL_COINS.map(coin => {
              const isSelected = selected.includes(coin);
              const isDisabled = !isSelected && selected.length >= maxCoins;
              return (
                <button
                  key={coin}
                  onClick={() => !isDisabled && toggle(coin)}
                  disabled={isDisabled}
                  className={`flex items-center justify-between px-2 py-1.5 rounded text-xs font-mono transition-colors ${
                    isSelected
                      ? 'bg-accent/20 text-accent border border-accent/30'
                      : isDisabled
                      ? 'text-muted-foreground/40 cursor-not-allowed'
                      : 'text-muted-foreground hover:text-foreground hover:bg-muted/50 border border-transparent'
                  }`}
                >
                  {coin.replace('USDT', '')}
                  {isSelected && <Check className="w-3 h-3" />}
                </button>
              );
            })}
          </div>
          {selected.length >= maxCoins && (
            <p className="text-[10px] text-muted-foreground mt-2">
              Max {maxCoins} coins. Remove one to add another.
            </p>
          )}
        </div>
      )}
    </div>
  );
};

export default CoinSelector;
