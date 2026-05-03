import { useState, useEffect, useMemo } from 'react';
import { Star, Search } from 'lucide-react';
import type { LivePrices } from '@/lib/trading-engine';

const COIN_COLORS: Record<string, string> = {
  BTC: '#F7931A', ETH: '#627EEA', SOL: '#9945FF', BNB: '#F3BA2F',
  DOGE: '#C3A634', XRP: '#346AA9', ADA: '#0033AD', AVAX: '#E84142',
  DOT: '#E6007A', LINK: '#2A5ADA', POL: '#8247E5',   UNI: '#FF007A',
  LTC: '#BFBBBB', ATOM: '#2E3148', SHIB: '#FFA409', ARB: '#28A0F0',
  OP: '#FF0420',  INJ: '#00B3B4', FET: '#1A1F36',  NEAR: '#00C08B',
  TRX: '#EF0027', TON: '#0098EA', APT: '#2DD8A3',  SUI: '#6FBCF0',
  SEI: '#9D5CFF', STX: '#5546FF', LDO: '#F08848',  MKR: '#1AAB9B',
  PEPE:'#00A86B', WIF: '#7B5EA7', BONK:'#F7931A',  JUP: '#C7A84F',
  FLOKI:'#F0A500',MEME:'#FF6B35', AAVE:'#B6509E',  CRV: '#40649F',
  GRT: '#6747ED', SNX: '#00D1FF', ENJ: '#7866D5',  SAND:'#04ADEF',
  MANA:'#FF2D55', AXS: '#0055D5', GALA:'#FCCD05',  IMX: '#17B5CB',
  ALGO:'#000000', VET: '#15BDFF', FIL: '#0090FF',  ICP: '#29ABE2',
  HBAR:'#222222', RENDER:'#5A5AFF', DOGS:'#F7931A', NEIRO:'#FF69B4',
  BRETT:'#627EEA', '1000SATS':'#F7931A',
};

const ALL_USDT_COINS = [
  // Large-caps
  'BTCUSDT','ETHUSDT','SOLUSDT','BNBUSDT','XRPUSDT',
  'ADAUSDT','AVAXUSDT','DOGEUSDT','DOTUSDT','LINKUSDT',
  'POLUSDT', 'UNIUSDT', 'LTCUSDT', 'ATOMUSDT','TRXUSDT',
  // Mid-caps / L2
  'ARBUSDT','OPUSDT','INJUSDT','FETUSDT','NEARUSDT',
  'TONUSDT','APTUSDT','SUIUSDT','SHIBUSDT','RENDERUSDT',
  'TIAUSDT','SEIUSDT','STXUSDT','LDOUSDT','MKRUSDT',
  // Meme / high-vol
  'PEPEUSDT','WIFUSDT','BONKUSDT','JUPUSDT','FLOKIUSDT',
  'MEMEUSDT','DOGSUSDT','NEIROUSDT','BRETTUSDT','1000SATSUSDT',
  // DeFi / AI / Gaming
  'AAVEUSDT','CRVUSDT','GRTUSDT','SNXUSDT','ENJUSDT',
  'SANDUSDT','MANAUSDT','AXSUSDT','GALAUSDT','IMXUSDT',
  'ALGOUSDT','VETUSDT','FILUSDT','ICPUSDT','HBARUSDT',
];

interface Props {
  selectedCoins: string[];
  activeCoin: string;
  onActiveCoin: (coin: string) => void;
  prices: LivePrices;
}

const FAV_KEY = 'coin_favorites';

function CoinRow({
  symbol, price, changePct, isActive, isFav, onSelect, onFav,
}: {
  symbol: string; price: number; changePct: number;
  isActive: boolean; isFav: boolean;
  onSelect: () => void; onFav: () => void;
}) {
  const ticker = symbol.replace('USDT', '');
  const color = COIN_COLORS[ticker] ?? '#6b7280';
  const up = changePct >= 0;
  const priceStr = price > 0
    ? price >= 1000 ? price.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
    : price >= 1   ? price.toFixed(4)
    : price.toFixed(6)
    : '—';

  return (
    <button
      onClick={onSelect}
      className={`w-full flex items-center gap-2 px-3 py-2 text-left transition-colors hover:bg-muted/20 ${
        isActive ? 'bg-gain/10 border-l-2 border-gain' : 'border-l-2 border-transparent'
      }`}
    >
      <button
        onClick={e => { e.stopPropagation(); onFav(); }}
        className={`text-muted-foreground hover:text-warn flex-shrink-0 ${isFav ? 'text-warn' : ''}`}
      >
        <Star className="w-3 h-3" fill={isFav ? 'currentColor' : 'none'} />
      </button>
      <div style={{ background: color }} className="w-5 h-5 rounded-full flex items-center justify-center text-white text-[8px] font-bold flex-shrink-0">
        {ticker.slice(0, 3)}
      </div>
      <div className="flex-1 min-w-0">
        <div className={`text-xs font-semibold leading-none ${isActive ? 'text-gain' : 'text-foreground'}`}>{ticker}</div>
        <div className="text-[9px] text-muted-foreground">USDT</div>
      </div>
      <div className="text-right flex-shrink-0">
        <div className="text-xs font-mono tabular-nums text-foreground">{priceStr}</div>
        <div className={`text-[9px] font-mono tabular-nums ${up ? 'text-gain' : 'text-loss'}`}>
          {up ? '+' : ''}{changePct.toFixed(2)}%
        </div>
      </div>
    </button>
  );
}

export default function CoinSelectorPanel({ selectedCoins, activeCoin, onActiveCoin, prices }: Props) {
  const [search, setSearch]       = useState('');
  const [tab, setTab]             = useState<'USDT' | 'FAV'>('USDT');
  const [favorites, setFavorites] = useState<Set<string>>(() => {
    try { return new Set(JSON.parse(localStorage.getItem(FAV_KEY) ?? '[]')); } catch { return new Set(); }
  });
  const [extraPrices, setExtraPrices] = useState<Record<string, { price: number; changePct: number }>>({});

  // Fetch 24hr ticker for coins not in websocket
  useEffect(() => {
    const missing = ALL_USDT_COINS.filter(c => !prices[c]);
    if (missing.length === 0) return;
    const symbols = JSON.stringify(missing);
    fetch(`https://api.binance.com/api/v3/ticker/24hr?symbols=${encodeURIComponent(symbols)}`)
      .then(r => r.json())
      .then((data: { symbol: string; lastPrice: string; priceChangePercent: string }[]) => {
        const map: Record<string, { price: number; changePct: number }> = {};
        data.forEach(d => { map[d.symbol] = { price: parseFloat(d.lastPrice), changePct: parseFloat(d.priceChangePercent) }; });
        setExtraPrices(map);
      })
      .catch(() => {});
  }, [prices]);

  const toggleFav = (coin: string) => {
    setFavorites(prev => {
      const next = new Set(prev);
      next.has(coin) ? next.delete(coin) : next.add(coin);
      localStorage.setItem(FAV_KEY, JSON.stringify([...next]));
      return next;
    });
  };

  const allCoins = useMemo(() => {
    const set = new Set([...ALL_USDT_COINS, ...selectedCoins]);
    return [...set];
  }, [selectedCoins]);

  const getPrice = (sym: string) => {
    const lp = prices[sym];
    if (lp) return { price: parseFloat(lp.price), changePct: parseFloat(lp.priceChangePercent) };
    return extraPrices[sym] ?? { price: 0, changePct: 0 };
  };

  const filtered = useMemo(() => {
    let list = tab === 'FAV' ? allCoins.filter(c => favorites.has(c)) : allCoins;
    if (search.trim()) {
      const q = search.trim().toUpperCase();
      list = list.filter(c => c.includes(q));
    }
    return list;
  }, [allCoins, tab, favorites, search]);

  return (
    <div className="flex flex-col h-full bg-card border-r border-border">
      {/* Search */}
      <div className="p-2 border-b border-border">
        <div className="relative">
          <Search className="w-3 h-3 absolute left-2.5 top-1/2 -translate-y-1/2 text-muted-foreground" />
          <input
            type="text" value={search} onChange={e => setSearch(e.target.value)}
            placeholder="Search pair — BTC, ETH, SOL..."
            className="w-full bg-secondary border border-border rounded px-2 py-1.5 pl-7 text-xs text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring"
          />
        </div>
      </div>

      {/* Tabs */}
      <div className="flex border-b border-border">
        {(['USDT', 'FAV'] as const).map(t => (
          <button key={t} onClick={() => setTab(t)}
            className={`flex-1 py-1.5 text-[10px] font-medium transition-colors ${
              tab === t ? 'text-accent border-b border-accent' : 'text-muted-foreground hover:text-foreground'
            }`}>
            {t === 'FAV' ? '★ Fav' : t}
          </button>
        ))}
      </div>

      {/* Column headers */}
      <div className="flex px-3 py-1 border-b border-border/50 text-[9px] text-muted-foreground uppercase tracking-widest">
        <span className="w-16">PAIR</span>
        <span className="flex-1 text-right">PRICE</span>
        <span className="w-12 text-right">24H %</span>
      </div>

      {/* Coin list */}
      <div className="flex-1 overflow-y-auto scrollbar-thin">
        {filtered.length === 0 ? (
          <div className="p-4 text-center text-xs text-muted-foreground">No pairs found</div>
        ) : (
          filtered.map(coin => {
            const { price, changePct } = getPrice(coin);
            return (
              <CoinRow
                key={coin} symbol={coin} price={price} changePct={changePct}
                isActive={activeCoin === coin} isFav={favorites.has(coin)}
                onSelect={() => onActiveCoin(coin)}
                onFav={() => toggleFav(coin)}
              />
            );
          })
        )}
      </div>

      {/* Footer hint */}
      <div className="px-3 py-1.5 border-t border-border/50 text-[9px] text-muted-foreground leading-relaxed">
        Green pill = up 24h · Red = down · Highlighted = active pair · Prices live via WebSocket
      </div>
    </div>
  );
}
