import { useState, memo } from 'react';
import { Check, X, Search } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { BINANCE_COINS, COIN_CATEGORIES } from '@/lib/binance-coins';
import { useUniverseAvailable } from '@/hooks/useUniverse';
import { CoinIcon } from '@/components/CoinIcon';

interface CoinSelectorProps {
  selected: string[];
  onChange: (coins: string[]) => void;
  maxCoins?: number;
}

const CoinSelector = ({ selected, onChange, maxCoins = 10 }: CoinSelectorProps) => {
  const [isOpen, setIsOpen] = useState(false);
  const [search, setSearch] = useState('');
  const [activeCategory, setActiveCategory] = useState<string>('All');
  // One source of truth: the live TRADING-USDT universe from /api/universe/available.
  // Fall back to the bundled list ONLY when the backend is unreachable.
  const { available, exchangeInfoAvailable } = useUniverseAvailable();
  const universeReady = exchangeInfoAvailable && available.length > 0;
  const allCoins = universeReady ? available : BINANCE_COINS;
  const allSet = new Set(allCoins);

  const toggle = (coin: string) => {
    if (selected.includes(coin)) {
      onChange(selected.filter(c => c !== coin));
    } else if (selected.length < maxCoins) {
      onChange([...selected, coin]);
    }
  };

  const categories = ['All', ...Object.keys(COIN_CATEGORIES)];

  const filteredCoins = (() => {
    // "All" → the full live universe. A category → its curated members that are
    // ALSO live (so delisted coins never show even in a category). When the
    // universe is unavailable we degrade to the bundled category list.
    let coins = activeCategory === 'All'
      ? allCoins
      : (universeReady
          ? (COIN_CATEGORIES[activeCategory] || []).filter(c => allSet.has(c))
          : (COIN_CATEGORIES[activeCategory] || []));
    if (search.trim()) {
      const q = search.toUpperCase().trim();
      coins = coins.filter(c => c.includes(q));
    }
    return coins;
  })();

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
            <CoinIcon symbol={coin} size={14} />
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
        <div className="mt-3 pt-3 border-t border-border space-y-2">
          {/* Search */}
          <div className="relative">
            <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-muted-foreground" />
            <input
              type="text"
              placeholder="Search coins..."
              value={search}
              onChange={e => setSearch(e.target.value)}
              className="w-full bg-muted/30 border border-border rounded-md pl-8 pr-3 py-1.5 text-xs font-mono text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-accent/50"
            />
          </div>

          {/* Category tabs */}
          <div className="flex gap-1 overflow-x-auto scrollbar-thin pb-1">
            {categories.map(cat => (
              <button
                key={cat}
                onClick={() => setActiveCategory(cat)}
                className={`px-2 py-1 rounded text-[10px] font-medium whitespace-nowrap transition-colors ${
                  activeCategory === cat
                    ? 'bg-accent/20 text-accent'
                    : 'text-muted-foreground hover:text-foreground bg-muted/20'
                }`}
              >
                {cat}
              </button>
            ))}
          </div>

          {/* Coin grid */}
          <div className="grid grid-cols-3 gap-1 max-h-48 overflow-y-auto scrollbar-thin">
            {filteredCoins.map(coin => {
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
                  <span className="inline-flex items-center gap-1.5 min-w-0">
                    <CoinIcon symbol={coin} size={16} />
                    <span className="truncate">{coin.replace('USDT', '')}</span>
                  </span>
                  {isSelected && <Check className="w-3 h-3 shrink-0" />}
                </button>
              );
            })}
          </div>
          {filteredCoins.length === 0 && (
            <p className="text-[10px] text-muted-foreground text-center py-2">No coins match "{search}"</p>
          )}
          {selected.length >= maxCoins && (
            <p className="text-[10px] text-muted-foreground">
              Max {maxCoins} coins. Remove one to add another.
            </p>
          )}
        </div>
      )}
    </div>
  );
};

export default memo(CoinSelector, (prev, next) => {
  // Only re-render when selected coins or maxCoins actually change.
  // Reference-identity change of onChange must NOT trigger re-render
  // (parent passes a new function each render, which previously caused
  // the dropdown to close and inputs to lose focus).
  if (prev.maxCoins !== next.maxCoins) return false;
  if (prev.selected.length !== next.selected.length) return false;
  for (let i = 0; i < prev.selected.length; i++) {
    if (prev.selected[i] !== next.selected[i]) return false;
  }
  return true;
});
