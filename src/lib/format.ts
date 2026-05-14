// Signed number with proper minus sign (U+2212)
export function formatSignedNumber(
  value: number | null | undefined,
  decimals: number = 4
): string {
  if (value == null || isNaN(value)) return '—';
  if (value === 0) return '0.' + '0'.repeat(decimals);
  const fixed = Math.abs(value).toFixed(decimals);
  return value > 0 ? `+${fixed}` : `−${fixed}`;
}

// Percent with sign
export function formatPercent(
  value: number | null | undefined,
  decimals: number = 2
): string {
  if (value == null || isNaN(value)) return '—';
  const sign = value > 0 ? '+' : value < 0 ? '−' : '';
  return `${sign}${Math.abs(value).toFixed(decimals)}%`;
}

// Dollar-prefixed value
export function formatUSDT(
  value: number | null | undefined,
  decimals: number = 2
): string {
  if (value == null || isNaN(value)) return '—';
  return `$${value.toFixed(decimals)}`;
}

// PnL with sign + dollar prefix
export function formatPnL(
  value: number | null | undefined,
  decimals: number = 4
): string {
  if (value == null || isNaN(value)) return '—';
  if (value === 0) return '$0.00';
  const fixed = Math.abs(value).toFixed(decimals);
  return value > 0 ? `+$${fixed}` : `−$${fixed}`;
}

// Adaptive price precision for micro-caps
export function formatPrice(value: number | null | undefined): string {
  if (value == null || isNaN(value)) return '—';
  if (value === 0) return '$0.00';
  const abs = Math.abs(value);
  let decimals: number;
  if (abs >= 1000) decimals = 2;
  else if (abs >= 1) decimals = 4;
  else if (abs >= 0.01) decimals = 5;
  else if (abs >= 0.0001) decimals = 7;
  else decimals = 9;
  return `$${value.toFixed(decimals)}`;
}

// 24-hour time
export function formatTime(ts: number | string | Date): string {
  const d = new Date(ts as any);
  return d.toLocaleTimeString('en-GB', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  });
}

// 24-hour datetime
export function formatDateTime(ts: number | string | Date): string {
  const d = new Date(ts as any);
  const date = d.toLocaleDateString('en-GB', { month: 'short', day: '2-digit' });
  return `${date} ${formatTime(d)}`;
}
