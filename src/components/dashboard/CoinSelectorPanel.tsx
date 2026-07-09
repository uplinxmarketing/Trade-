import { useState, useMemo } from 'react';
import { Star, Search, AlertCircle, Ban, PauseCircle, Loader2 } from 'lucide-react';
import type { LivePrices } from '@/lib/trading-engine';
import { useUniverseHealth, type InvalidInfo } from '@/hooks/useUniverseHealth';
import { useMarketSnapshot } from '@/hooks/useMarketSnapshot';
import { useUniverse } from '@/hooks/useUniverse';

// I4 — distinguish the two dead-ticker statuses from /api/universe/validate:
//  - BREAK   → symbol trading is halted; the backend auto-restores it, so this
//              is NOT a remove prompt (e.g. TON during a maintenance break).
//  - DELISTED→ pair is gone for good; user should remove/replace it.
// 'renamed' remains a delisted-family case (pair replaced by suggested_rename).
type DeadKind = 'break' | 'delisted';
function classifyInvalid(info: InvalidInfo): DeadKind {
  // Prefer the backend's authoritative excluded_kind; sniff status only if absent.
  if (info.excludedKind === 'break') return 'break';
  if (info.excludedKind === 'delisted') return 'delisted';
  const s = (info.status ?? '').toUpperCase();
  if (s === 'BREAK' || s.includes('HALT')) return 'break';
  return 'delisted';
}

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
  BRETT:'#627EEA', '1000SATS':'#F7931A', TIA: '#9D5CFF',
};

// N3.2 — offline-dev fallback ONLY. The live coin list is driven by the engine
// universe (GET /api/universe via useUniverse); this array is used solely when
// the backend is unavailable, so a delisted ticker here is never surfaced as
// tradeable while the backend is reachable.
const FALLBACK_USDT_COINS = [
  // Large-caps
  'BTCUSDT','ETHUSDT','SOLUSDT','BNBUSDT','XRPUSDT',
  'ADAUSDT','AVAXUSDT','DOGEUSDT','DOTUSDT','LINKUSDT',
  'POLUSDT','UNIUSDT','LTCUSDT','ATOMUSDT','TRXUSDT',
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
  symbol, price, changePct, isActive, isFav, onSelect, onFav, invalidInfo, ticketTooSmall,
  warming, backfillPct,
}: {
  symbol: string; price: number; changePct: number;
  isActive: boolean; isFav: boolean;
  onSelect: () => void; onFav: () => void;
  invalidInfo?: InvalidInfo; ticketTooSmall?: boolean;
  warming?: boolean; backfillPct?: number;
}) {
  const ticker = symbol.replace('USDT', '');
  const color  = COIN_COLORS[ticker] ?? '#6b7280';
  const up     = changePct >= 0;
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
        {invalidInfo ? (
          classifyInvalid(invalidInfo) === 'break' ? (
            // BREAK — trading temporarily halted; auto-restores. Not a remove prompt.
            <div className="flex items-center gap-1 mt-0.5 flex-wrap">
              <span
                className="inline-flex items-center gap-0.5 px-1 py-px rounded bg-warn/15 text-warn text-[8px] font-semibold uppercase tracking-wide"
                title="Trading on this pair is halted (BREAK). The bot auto-restores it when it resumes — no action needed."
              >
                <PauseCircle className="w-2 h-2" />
                halted (BREAK) — auto-restores
              </span>
            </div>
          ) : (
            // DELISTED (incl. renamed) — pair is gone; prompt to remove/replace.
            <div className="flex items-center gap-1 mt-0.5 flex-wrap">
              <span
                className="inline-flex items-center gap-0.5 px-1 py-px rounded bg-loss/15 text-loss text-[8px] font-semibold uppercase tracking-wide"
                title="This pair is no longer tradable on Binance. Remove or replace it in your watchlist."
              >
                <Ban className="w-2 h-2" />
                {invalidInfo.suggestedRename
                  ? `delisted → ${invalidInfo.suggestedRename}`
                  : 'delisted'}
              </span>
            </div>
          )
        ) : warming ? (
          // WARMING — in the universe but not backfill_ready yet (freshly added).
          // Amber + muted: it will start trading automatically once warmed.
          <span
            className="inline-flex items-center gap-0.5 px-1 py-px mt-0.5 rounded bg-warn/10 text-warn/80 text-[8px] font-semibold"
            title="This coin is in your universe but still backfilling history. It will start trading automatically once warmed up."
          >
            <Loader2 className="w-2 h-2 animate-spin" />
            warming up — backfilling{typeof backfillPct === 'number' ? ` ${Math.round(backfillPct)}%` : ''}
          </span>
        ) : ticketTooSmall ? (
          <span
            className="inline-flex items-center px-1 py-px mt-0.5 rounded bg-warn/15 text-warn text-[8px] font-semibold"
            title="Trades on this coin are chronically skipped because the min lot/notional is larger than your trade size. Raise trade size for this coin."
          >
            ticket too small
          </span>
        ) : (
          <div className="text-[9px] text-muted-foreground">USDT</div>
        )}
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
  // N1 — one local snapshot poll seeds prices/24h% (WebSocket still overrides in
  // real time). This replaces the old per-list /ticker/24hr?symbols=… storm.
  const { snapshot } = useMarketSnapshot();
  // N3.2 — the coin list itself comes from the engine universe.
  const { symbols: universeSymbols, available: universeAvailable } = useUniverse();
  // Backend-driven watchlist health: delisted/renamed pairs + lot-waste flags,
  // plus backfill readiness so freshly-added coins show a "warming up" badge.
  const { invalid: healthInvalid, lotWaste, warming, ready, backfillPct } = useUniverseHealth();

  // Merge universe status into the health-derived invalid map so suspended/
  // delisted universe symbols render the same badges. /api/universe/validate
  // stays authoritative when it also flags a symbol.
  const invalid = useMemo<Record<string, InvalidInfo>>(() => {
    const merged: Record<string, InvalidInfo> = {};
    for (const s of universeSymbols) {
      if (s.status === 'suspended') merged[s.symbol] = { excludedKind: 'break', status: 'BREAK' };
      else if (s.status === 'delisted') merged[s.symbol] = { excludedKind: 'delisted', suggestedRename: s.successor };
    }
    return { ...merged, ...healthInvalid };
  }, [universeSymbols, healthInvalid]);

  // A selected coin is "warming" when it's not delisted/halted, and either the
  // backend explicitly lists it as backfilling, or readiness is known and this
  // coin isn't ready yet. Never treat a coin as warming when readiness is
  // unknown (both sets empty) — graceful degradation, no phantom badges.
  const isWarming = (sym: string): boolean => {
    const u = sym.toUpperCase();
    if (invalid[u]) return false;
    if (warming.has(u)) return true;
    return ready.size > 0 && !ready.has(u);
  };

  // N3.2 — coin list driven by the engine universe (GET /api/universe). The
  // static list is offline-dev fallback only, gated behind universe availability
  // so a delisted ticker (e.g. MKRUSDT/TONUSDT) is never surfaced as tradeable
  // while the backend is reachable. Watchlist coins and any backend-flagged
  // delisted/suspended symbol are always included so their badge stays visible.
  const allCoins = useMemo(() => {
    const base = universeAvailable ? universeSymbols.map(s => s.symbol) : FALLBACK_USDT_COINS;
    const set = new Set<string>([...base, ...selectedCoins]);
    for (const k of Object.keys(invalid)) set.add(k);
    return [...set];
  }, [universeAvailable, universeSymbols, selectedCoins, invalid]);

  const toggleFav = (coin: string) => {
    setFavorites(prev => {
      const next = new Set(prev);
      next.has(coin) ? next.delete(coin) : next.add(coin);
      localStorage.setItem(FAV_KEY, JSON.stringify([...next]));
      return next;
    });
  };

  const getPrice = (sym: string) => {
    // WebSocket ticks win; the local snapshot seeds coins not yet streamed.
    const lp = prices[sym];
    if (lp) return { price: parseFloat(lp.price), changePct: parseFloat(lp.priceChangePercent) };
    const snap = snapshot[sym.toUpperCase()];
    if (snap && snap.available) return { price: snap.price, changePct: snap.pct_24h };
    return { price: 0, changePct: 0 };
  };

  const filtered = useMemo(() => {
    let list = tab === 'FAV' ? allCoins.filter(c => favorites.has(c)) : allCoins;
    if (search.trim()) {
      const q = search.trim().toUpperCase();
      list = list.filter(c => c.includes(q));
    }
    return list;
  }, [allCoins, tab, favorites, search]);

  // Count watchlist coins by dead-ticker kind: delisted (remove/replace) vs
  // halted BREAK (auto-restores, informational only).
  const { delistedCount, haltedCount } = useMemo(() => {
    let delisted = 0, halted = 0;
    for (const c of allCoins) {
      const info = invalid[c.toUpperCase()];
      if (!info) continue;
      if (classifyInvalid(info) === 'break') halted++;
      else delisted++;
    }
    return { delistedCount: delisted, haltedCount: halted };
  }, [allCoins, invalid]);

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

      {/* Universe loading indicator — before the engine universe is available,
          the list falls back to the offline dev set. */}
      {!universeAvailable && (
        <div className="px-3 py-1 text-[9px] text-muted-foreground flex items-center gap-1 animate-pulse border-b border-border/30">
          <AlertCircle className="w-2.5 h-2.5" />Loading universe…
        </div>
      )}

      {/* Dead-ticker summary — count delisted (prune) vs halted (auto-restores) separately */}
      {(delistedCount > 0 || haltedCount > 0) && (
        <div className={`px-3 py-1 text-[9px] flex items-center gap-1 border-b ${
          delistedCount > 0 ? 'text-loss border-loss/20 bg-loss/5' : 'text-warn border-warn/20 bg-warn/5'
        }`}>
          {delistedCount > 0
            ? <Ban className="w-2.5 h-2.5 flex-shrink-0" />
            : <PauseCircle className="w-2.5 h-2.5 flex-shrink-0" />}
          <span>
            {delistedCount} delisted, {haltedCount} halted
            {delistedCount > 0 ? ' — prune delisted' : ' — auto-restores'}
          </span>
        </div>
      )}

      {/* Coin list */}
      <div className="flex-1 overflow-y-auto scrollbar-thin">
        {filtered.length === 0 ? (
          <div className="p-4 text-center text-xs text-muted-foreground">No valid pairs found</div>
        ) : (
          filtered.map(coin => {
            const { price, changePct } = getPrice(coin);
            return (
              <CoinRow
                key={coin} symbol={coin} price={price} changePct={changePct}
                isActive={activeCoin === coin} isFav={favorites.has(coin)}
                onSelect={() => onActiveCoin(coin)}
                onFav={() => toggleFav(coin)}
                invalidInfo={invalid[coin.toUpperCase()]}
                ticketTooSmall={lotWaste.has(coin.toUpperCase())}
                warming={isWarming(coin)}
                backfillPct={backfillPct[coin.toUpperCase()]}
              />
            );
          })
        )}
      </div>

      {/* Footer */}
      <div className="px-3 py-1.5 border-t border-border/50 text-[9px] text-muted-foreground leading-relaxed">
        {universeAvailable
          ? `${filtered.length} pairs · universe-driven · prices live via WebSocket`
          : `${filtered.length} pairs · offline fallback · prices live via WebSocket`}
      </div>
    </div>
  );
}
