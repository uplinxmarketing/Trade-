import { useState } from 'react';

// Shared coin icon: a real logo image with a guaranteed fallback so NOTHING ever
// renders blank/broken. Chain: CDN image (keyed on base asset) → on 404/error,
// a deterministic colored circle with the ticker initials.

const QUOTE_RE = /(USDT|USDC|FDUSD|BUSD|TUSD|DAI|BTC|ETH|BNB)$/;

/** Base asset from a trading symbol: strip the quote suffix.
 *  BTCUSDT→BTC, 1INCHUSDT→1INCH, 1000SATSUSDT→1000SATS (digit prefixes kept). */
export function baseAsset(symbol: string): string {
  const s = (symbol || '').toUpperCase().trim();
  return s.replace(QUOTE_RE, '') || s;
}

/** Clean label for the fallback circle: drop a leading numeric multiplier so
 *  1000SATS→SATS, 1000PEPE→PEPE, 1INCH→INCH read cleanly (not "100"/"1IN"). */
function iconLabel(base: string): string {
  const stripped = base.replace(/^\d+/, '') || base;
  return stripped.slice(0, 4);
}

/** Stable, distinct color per base asset for the fallback circle. */
function colorFor(base: string): string {
  let h = 0;
  for (let i = 0; i < base.length; i++) h = (h * 31 + base.charCodeAt(i)) >>> 0;
  return `hsl(${h % 360} 55% 45%)`;
}

// Module-level cache of base assets whose CDN icon 404'd. Once a base fails we
// render the initials fallback directly and never re-request the image — so the
// fallback "sticks" and icon loading never adds repeated network churn.
const failedIcons = new Set<string>();

export function CoinIcon({ symbol, size = 20, className = '' }: {
  symbol: string; size?: number; className?: string;
}) {
  const base = baseAsset(symbol);
  const [failed, setFailed] = useState(() => failedIcons.has(base));
  if (!base) return null;
  const px = `${size}px`;

  if (!failed) {
    // cryptocurrency-icons CDN, keyed on lowercase base asset. Misses (newer
    // coins) hit onError → the initials fallback below.
    const src = `https://cdn.jsdelivr.net/npm/cryptocurrency-icons@0.18.1/128/color/${base.toLowerCase()}.png`;
    return (
      <img
        src={src}
        alt={base}
        width={size}
        height={size}
        loading="lazy"
        className={`rounded-full ${className}`}
        style={{ width: px, height: px, objectFit: 'contain' }}
        onError={() => { failedIcons.add(base); setFailed(true); }}
      />
    );
  }

  return (
    <div
      className={`rounded-full flex items-center justify-center font-bold text-white shrink-0 ${className}`}
      style={{ width: px, height: px, background: colorFor(base),
               fontSize: `${Math.max(7, Math.round(size * 0.36))}px`, lineHeight: 1 }}
      title={base}
    >
      {iconLabel(base)}
    </div>
  );
}

export default CoinIcon;
