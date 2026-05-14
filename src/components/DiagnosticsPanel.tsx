import { useEffect, useState } from 'react';

interface DiagIssue {
  ts: number;
  iso: string;
  source: string;
  severity: 'error' | 'warn' | 'info';
  message: string;
  detail: string;
}

interface Diagnostics {
  server_time: number;
  binance: {
    rest_ok: boolean;
    last_rest_age_sec: number | null;
    last_latency_ms: number;
    used_weight_1m: number;
    used_weight_pct: number;
    weight_limit: number;
    rest_error_count: number;
    last_error_age_sec: number | null;
    last_error_msg: string;
  };
  websocket: {
    connected: boolean;
    last_message_age_sec: number | null;
    messages_received: number;
    connect_count: number;
    disconnect_count: number;
    subscribed_symbols: number;
  };
  signal_scanner: {
    last_refresh_age_sec: number | null;
    next_refresh_in_sec: number;
    scan_progress_pct: number;
    interval_sec: number;
    last_duration_ms: number;
    scans_completed: number;
    cached_signals_count: number;
  };
  sell_monitor: {
    alive: boolean;
    heartbeat_age_sec: number | null;
    open_positions: number;
    in_progress_sells: number;
  };
  price_refresher: {
    alive: boolean;
    thread_name: string | null;
  };
  system: {
    deploy_id: string;
    active_threads_count: number;
    active_threads: string[];
  };
  issues: {
    recent: DiagIssue[];
    error_count: number;
    warn_count: number;
    total_buffered: number;
  };
  claude_api?: {
    error_count: number;
    last_error_age_sec: number | null;
    last_error_msg: string;
    disabled: boolean;
    disabled_until: number | null;
  };
}

function StatusDot({ ok, pulse = true }: { ok: boolean; pulse?: boolean }) {
  return (
    <span
      className={`inline-block w-2 h-2 rounded-full flex-shrink-0 ${
        ok ? 'bg-green-500' : 'bg-red-500'
      } ${ok && pulse ? 'animate-pulse' : ''}`}
    />
  );
}

function Bar({ pct, color = 'bg-blue-500' }: { pct: number; color?: string }) {
  return (
    <div className="w-full h-1.5 bg-gray-700 rounded-full overflow-hidden">
      <div
        className={`h-full ${color} transition-all duration-500`}
        style={{ width: `${Math.min(100, Math.max(0, pct))}%` }}
      />
    </div>
  );
}

function IssueRow({ issue, expanded, onToggle }: {
  issue: DiagIssue;
  expanded: boolean;
  onToggle: () => void;
}) {
  const time = new Date(issue.ts * 1000).toLocaleTimeString();
  const colorClass =
    issue.severity === 'error' ? 'border-red-500 text-red-300' :
    issue.severity === 'warn'  ? 'border-yellow-500 text-yellow-200' :
                                  'border-blue-500 text-blue-200';

  return (
    <div
      className={`border-l-2 ${colorClass} pl-2 text-[10px] cursor-pointer`}
      onClick={onToggle}
    >
      <div className="flex justify-between text-gray-400">
        <span>[{issue.source}]</span>
        <span>{time}</span>
      </div>
      <div className="font-mono break-words">{issue.message}</div>
      {!expanded && issue.detail && (
        <div className="text-gray-500 text-[9px] mt-0.5 truncate">↳ {issue.detail}</div>
      )}
      {expanded && issue.detail && (
        <div className="text-gray-400 text-[9px] mt-1 p-1 bg-black/30 rounded font-mono whitespace-pre-wrap break-words">
          {issue.detail}
        </div>
      )}
    </div>
  );
}

const POLL_MS = 2000;

export function DiagnosticsPanel() {
  const [diag, setDiag]           = useState<Diagnostics | null>(null);
  const [collapsed, setCollapsed] = useState(false);
  const [error, setError]         = useState<string | null>(null);
  const [expandedTs, setExpandedTs] = useState<Set<number>>(new Set());
  const [copyMsg, setCopyMsg]     = useState('');

  useEffect(() => {
    let cancelled = false;
    async function poll() {
      try {
        const r = await fetch('/api/diagnostics');
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        const data = await r.json();
        if (!cancelled) { setDiag(data); setError(null); }
      } catch (e: unknown) {
        if (!cancelled) setError((e as Error).message ?? 'fetch failed');
      }
    }
    poll();
    const id = setInterval(poll, POLL_MS);
    return () => { cancelled = true; clearInterval(id); };
  }, []);

  const overallOk =
    (diag?.binance.rest_ok ?? false) &&
    (diag?.websocket.connected ?? false) &&
    (diag?.sell_monitor.alive ?? false) &&
    (diag?.price_refresher.alive ?? false);

  if (collapsed) {
    return (
      <div className="fixed bottom-4 right-4 z-50">
        <button
          onClick={() => setCollapsed(false)}
          className="flex items-center gap-2 bg-gray-900 border border-gray-700 rounded-lg px-3 py-2 text-xs text-gray-300 shadow-lg hover:bg-gray-800"
        >
          <StatusDot ok={overallOk} />
          <span>Diagnostics</span>
          {(diag?.issues?.error_count ?? 0) > 0 && (
            <span className="bg-red-600 text-white text-[9px] px-1 rounded">
              {diag!.issues.error_count}
            </span>
          )}
        </button>
      </div>
    );
  }

  if (!diag) {
    return (
      <div className="fixed bottom-4 right-4 z-50 bg-gray-900 border border-gray-700 rounded-lg p-3 text-xs text-gray-400 w-72 shadow-xl">
        {error ? `Error: ${error}` : 'Loading diagnostics…'}
        <button onClick={() => setCollapsed(true)} className="ml-2 text-gray-500 hover:text-gray-300">×</button>
      </div>
    );
  }

  const weightColor =
    diag.binance.used_weight_pct < 50 ? 'bg-green-500' :
    diag.binance.used_weight_pct < 80 ? 'bg-yellow-500' : 'bg-red-500';

  async function copyLog() {
    try {
      const r = await fetch('/api/diagnostics/log/text');
      const text = await r.text();
      await navigator.clipboard.writeText(text);
      setCopyMsg('Copied!');
      setTimeout(() => setCopyMsg(''), 2000);
    } catch (e) {
      setCopyMsg('Failed');
      setTimeout(() => setCopyMsg(''), 2000);
    }
  }

  async function clearLog() {
    if (!confirm('Clear diagnostic log?')) return;
    await fetch('/api/diagnostics/log/clear', { method: 'POST' });
  }

  function toggleExpanded(ts: number) {
    setExpandedTs(prev => {
      const next = new Set(prev);
      next.has(ts) ? next.delete(ts) : next.add(ts);
      return next;
    });
  }

  return (
    <div className="fixed bottom-4 right-4 z-50 bg-gray-900 border border-gray-700 rounded-lg shadow-xl w-80 text-xs text-gray-200 font-mono select-none">
      {/* Header */}
      <div className="flex items-center justify-between px-3 py-2 border-b border-gray-700">
        <div className="flex items-center gap-2 font-semibold text-gray-100">
          <StatusDot ok={overallOk} />
          Diagnostics
        </div>
        <button
          onClick={() => setCollapsed(true)}
          className="text-gray-500 hover:text-gray-200 text-base leading-none px-1"
          title="Collapse"
        >×</button>
      </div>

      <div className="p-3 space-y-3 max-h-[80vh] overflow-y-auto">
        {/* Binance REST */}
        <section>
          <div className="flex items-center gap-2 mb-1">
            <StatusDot ok={diag.binance.rest_ok} />
            <span className="font-semibold">Binance REST</span>
            <span className="ml-auto text-gray-500 text-[10px]">
              {diag.binance.last_latency_ms}ms · {diag.binance.last_rest_age_sec ?? '—'}s ago
            </span>
          </div>
          <div className="flex justify-between text-[10px] text-gray-400 mb-0.5">
            <span>Weight {diag.binance.used_weight_1m}/{diag.binance.weight_limit}</span>
            <span>{diag.binance.used_weight_pct.toFixed(1)}%</span>
          </div>
          <Bar pct={diag.binance.used_weight_pct} color={weightColor} />
          {diag.binance.rest_error_count > 0 && (
            <div className="text-red-400 mt-1 text-[10px]">
              {diag.binance.rest_error_count} error{diag.binance.rest_error_count !== 1 ? 's' : ''}
              {diag.binance.last_error_msg && (
                <div className="truncate text-red-300/70" title={diag.binance.last_error_msg}>
                  ↳ {diag.binance.last_error_msg}
                </div>
              )}
            </div>
          )}
        </section>

        {/* WebSocket */}
        <section>
          <div className="flex items-center gap-2 mb-1">
            <StatusDot ok={diag.websocket.connected} />
            <span className="font-semibold">WebSocket</span>
            <span className="ml-auto text-gray-500 text-[10px]">
              {diag.websocket.last_message_age_sec != null
                ? `${diag.websocket.last_message_age_sec}s ago`
                : 'no msgs yet'}
            </span>
          </div>
          <div className="flex justify-between text-[10px] text-gray-400">
            <span>{diag.websocket.subscribed_symbols} symbols · {diag.websocket.messages_received.toLocaleString()} msgs</span>
            <span>↺ {diag.websocket.disconnect_count}</span>
          </div>
        </section>

        {/* Signal scanner */}
        <section>
          <div className="flex items-center gap-2 mb-1">
            <StatusDot
              ok={(diag.signal_scanner.last_refresh_age_sec ?? 999) < diag.signal_scanner.interval_sec * 2}
            />
            <span className="font-semibold">Signal Scan</span>
            <span className="ml-auto text-gray-500 text-[10px]">
              next in {diag.signal_scanner.next_refresh_in_sec.toFixed(0)}s
            </span>
          </div>
          <div className="flex justify-between text-[10px] text-gray-400 mb-0.5">
            <span>{diag.signal_scanner.cached_signals_count} cached · {diag.signal_scanner.last_duration_ms}ms</span>
            <span>{diag.signal_scanner.scans_completed} runs</span>
          </div>
          <Bar pct={diag.signal_scanner.scan_progress_pct} color="bg-blue-500" />
        </section>

        {/* Sell monitor */}
        <section className="flex items-center gap-2">
          <StatusDot ok={diag.sell_monitor.alive} />
          <span className="font-semibold">Sell Monitor</span>
          <span className="ml-auto text-[10px] text-gray-500">
            hb {diag.sell_monitor.heartbeat_age_sec ?? '—'}s · {diag.sell_monitor.open_positions} pos
            {diag.sell_monitor.in_progress_sells > 0 && (
              <span className="text-yellow-400"> · {diag.sell_monitor.in_progress_sells} selling</span>
            )}
          </span>
        </section>

        {/* Price refresher */}
        <section className="flex items-center gap-2">
          <StatusDot ok={diag.price_refresher.alive} />
          <span className="font-semibold">Price Refresher</span>
          <span className="ml-auto text-[10px] text-gray-500">
            {diag.price_refresher.alive ? 'running' : <span className="text-red-400">stopped</span>}
          </span>
        </section>

        {/* Claude API — only show when errors present */}
        {diag.claude_api && (diag.claude_api.error_count > 0 || diag.claude_api.disabled) && (
          <section>
            <div className="flex items-center gap-2 mb-1">
              <StatusDot ok={!diag.claude_api.disabled} />
              <span className="font-semibold">Claude API</span>
              <span className="ml-auto text-[10px] text-gray-500">
                {diag.claude_api.last_error_age_sec != null
                  ? `${diag.claude_api.last_error_age_sec}s ago`
                  : ''}
              </span>
            </div>
            <div className="text-[10px]">
              {diag.claude_api.disabled ? (
                <span className="text-red-400">
                  Disabled (auth failures) — restart bot after fixing ANTHROPIC_API_KEY
                </span>
              ) : (
                <span className="text-yellow-400">
                  {diag.claude_api.error_count} error{diag.claude_api.error_count !== 1 ? 's' : ''} since start
                </span>
              )}
              {diag.claude_api.last_error_msg && (
                <div className="truncate text-red-300/70 mt-0.5" title={diag.claude_api.last_error_msg}>
                  ↳ {diag.claude_api.last_error_msg}
                </div>
              )}
            </div>
          </section>
        )}

        {/* System */}
        <section className="border-t border-gray-700 pt-2 space-y-0.5 text-[10px] text-gray-500">
          <div className="flex justify-between">
            <span>Deploy</span>
            <span className="font-mono">{diag.system.deploy_id.slice(0, 8)}</span>
          </div>
          <div className="flex justify-between">
            <span>Threads</span>
            <span>{diag.system.active_threads_count}</span>
          </div>
        </section>

        {/* Issues feed */}
        <section className="border-t border-gray-700 pt-2">
          <div className="flex justify-between items-center mb-2">
            <span className="font-semibold flex items-center gap-1.5">
              Issues
              {(diag.issues?.error_count ?? 0) > 0 && (
                <span className="bg-red-600 text-white text-[9px] px-1.5 py-0.5 rounded">
                  {diag.issues.error_count} err
                </span>
              )}
              {(diag.issues?.warn_count ?? 0) > 0 && (
                <span className="bg-yellow-600 text-white text-[9px] px-1.5 py-0.5 rounded">
                  {diag.issues.warn_count} warn
                </span>
              )}
            </span>
            <div className="flex gap-1 items-center">
              {copyMsg && <span className="text-green-400 text-[9px]">{copyMsg}</span>}
              <button
                onClick={copyLog}
                className="text-[10px] bg-gray-700 hover:bg-gray-600 px-2 py-0.5 rounded"
                title="Copy plain-text report to clipboard"
              >
                Copy
              </button>
              <button
                onClick={clearLog}
                className="text-[10px] bg-gray-700 hover:bg-gray-600 px-2 py-0.5 rounded"
                title="Clear issue log"
              >
                Clear
              </button>
            </div>
          </div>

          {(!diag.issues?.recent?.length) ? (
            <div className="text-gray-500 text-[10px] italic">No recent issues</div>
          ) : (
            <div className="space-y-1.5 max-h-48 overflow-y-auto pr-0.5">
              {diag.issues.recent.slice(0, 10).map((issue, i) => (
                <IssueRow
                  key={`${issue.ts}-${i}`}
                  issue={issue}
                  expanded={expandedTs.has(issue.ts)}
                  onToggle={() => toggleExpanded(issue.ts)}
                />
              ))}
            </div>
          )}
        </section>
      </div>
    </div>
  );
}
