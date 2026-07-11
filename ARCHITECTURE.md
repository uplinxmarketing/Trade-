# WolfBot — System Architecture & Scoring Reference

> Live spot crypto trading bot (Binance). Scores a watchlist with the **WolfScore v3**
> model, auto-buys coins above a score threshold, and manages exits with a separate
> (deliberately frozen) stop/take-profit engine. Single Python process + React SPA.

---

## 1. High-level design

- **Backend:** one Python process — FastAPI + uvicorn.
- **Frontend:** React/TypeScript SPA, nginx-served, polls the REST API.
- **Market data:** Binance **WebSocket-first** (klines + miniTicker); signed REST only for account/orders.
- **Scoring:** WolfScore v3 (`ev_model.py`) — 8 sub-metrics → regime-gated families → logistic → 0–100.
- **Buys:** WolfScore ≥ threshold is the **sole gate** (legacy signal engine removed from the buy path).
- **Exits:** separate engine (ATR stops, R:R TP, trailing, ratchet) — **not modified**.
- **Persistence:** SQLite (WAL) with a single global read/write lock; Supabase mirrors coins + positions.
- **Deploy:** VPS `/opt/tradebot`; `deploy.sh` = `git reset --hard origin/main` + committed `dist/` + `systemctl restart tradebot`.

---

## 2. Backend modules

| Module | Responsibility |
|---|---|
| `control_api.py` | REST API, boot sequence, config migrations, diagnostics, response caching |
| `trade_engine.py` | Buy path, WolfScore orchestration, **exit engine**, positions, risk latches |
| `data_collector.py` | Binance WebSocket, in-memory candle buffers, backfill, gap repair |
| `database.py` | SQLite (WAL), global `_RWLock`, all tables, prunes/vacuum |
| `ev_model.py` | **WolfScore** math (sub-metrics, weights, logistic fit, adaptive floor) |
| `signal_registry.py` | Legacy 6-signal engine — **disabled & removed from buy path** |
| `paper_shadow.py` | Paper-trade data generator — **disabled** |
| `connection.py` / `binance_direct.py` | Binance auth/orders (urllib + HMAC, geo-block bypass) |
| `exchange_info.py` | Symbol status, renames/delistings (`RENAMED_SYMBOLS`, `KNOWN_DELISTED`) |
| `indicators.py` | EMA / RSI / MACD / ATR / Bollinger / OBV / VWAP |
| `backfill.py` | Startup historical kline download |
| `supabase_sync.py` | Mirrors coin list + positions to Supabase |
| `lever_matrix.py`, `attribution.py`, `learning.py`, `exit_orders.py` | Backtest levers, edge report, trade learning, managed exit orders |
| `futures_*` | Separate futures agent (independent of spot) |

**Runtime threads/loops:** websocket, signal_scanner, entry-heartbeat, fast-recheck, sell-monitor / held-refresher / managed-exit-poller (the **exit engine**), position-guardian, anomaly-checker, daily-maintenance.

---

## 3. Frontend components

| Component | Purpose | Endpoint(s) |
|---|---|---|
| `AITradingAgent` | Main live agent (status, positions, coins, wallet, trades) | `/api/all` |
| `WalletPanelV2` | Balance / equity | `/api/wallet`, `/api/all` |
| `EvScorePanel` | Live per-coin WolfScore feed (0–100, colour-coded, sortable) | `/api/ev/scores` |
| `EntryGatePanel` | Live gate state + **editable buy threshold** | `/api/diagnostics/entry-report`, `/api/entries/buy-threshold` |
| `EvModelPanel` | Model status (trained/untrained) + **Retrain** | `/api/ev/model`, `/api/ev/train`, `/api/ev/activate` |
| `EvExpectancyCard` | Live vs paper-shadow expectancy | `/api/ev/expectancy` |
| `RegimePanel` | Market regime (up/side/down) + adaptive floor | `/api/market-health`, ev meta |
| `FunnelPanel` | Entry funnel (ready→fresh-recheck→budget→order→filled) | `/api/funnel` |
| `RiskPanel` | Risk latches (daily stop, consec-loss, breaker) | `/api/risk/status` |
| `CoinSelector` / `CoinSelectorPanel` | Pick watchlist coins | `POST /api/coins`, `/api/universe` |
| `UniverseNoticesBanner` | Delist/successor notices | `/api/universe/notices` |
| `StrategySettingsPanel` / `StrategyPreviewDrawer` | Edit strategy config | `/api/settings`, `/api/strategy`, `/api/strategy/preview` |
| `SignalsEditorPanel` / `SignalEnginePanel` | Legacy signal roles/engine (**deprecated/display-only**) | `/api/signals/registry`, `/api/signal-engine/config` |
| `ConfigHistoryPanel` | Config version history + rollback | `/api/strategy/history`, `/api/strategy/rollback` |
| `DataHealthPanel` | WS/data health, kline coverage | `/api/health/data`, `/api/klines/coverage` |
| `DiagnosticsTab` / `DiagnosticsPanel` | Diagnostics bundle, logs, errors | `/api/diagnostics/*` |
| `BinanceConnect` | Binance connection indicator | `/api/status`, `/api/diagnostics` |
| `ChartPanelV2` | Price chart | proxy / market data |
| `OrderBookPanel` | Live order book | `/api/market/snapshot` |
| `OrderFormPanel` | Manual buy/sell | `POST /api/force-buy|force-sell` |
| `MarketStatsBar` | Market-wide stats | `/api/market-health`, `/api/stats` |
| `ReportDashboard` | Daily/summary P&L report | `/api/stats/daily`, `/api/stats/summary` |
| `SessionStatsPanel` / `AnalyticsPanel` | Session analytics | `/api/analytics/sessions` |
| `BacktestPanel` / `LeverMatrixPanel` | Backtester + lever matrix | `/api/backtest*` |
| `ShadowLabPanel` | Paper-shadow lab (**disabled**) | `/api/ev/shadow` |
| `AiChatPanel` | AI chat assistant | `POST /api/chat` |
| `NotificationCenter` | Alerts / phantoms | `/api/alerts`, `/api/diagnostics/phantoms` |
| `FuturesAgent` | Futures trading (separate) | `/api/futures/*` |
| `LiveConfirmModal` | Confirm switching to live mode | `POST /api/mode` |
| `TopBar` / `VersionFooter` / `NavLink` | Nav, version, update check | `/api/version`, `/api/update/check` |

---

## 4. API endpoints (by group)

- **Aggregate/status:** `/api/all`, `/api/status`, `/api/ping`, `/api/version`, `/api/version/served`, `/api/update/check`, `/api/health/data`
- **Scoring/model:** `/api/ev/scores`, `/api/ev/model`, `/api/ev/expectancy`, `/api/ev/train` (GET+POST), `/api/ev/activate`, `/api/ev/shadow`, `/api/entries/buy-threshold` (GET+POST)
- **Positions/trades/stats:** `/api/positions`, `/api/trades`, `/api/wallet`, `/api/stats*`, `/api/funnel`, `/api/risk/status`, `POST /api/force-buy|force-sell|reset`
- **Coins/universe:** `POST /api/coins`, `/api/universe`, `/api/universe/notices|validate`
- **Config:** `/api/settings` (GET+POST), `/api/strategy` (GET/PUT), `/api/strategy/schema|history|preview|rollback`, `/api/config`, `/api/gate/preset`
- **Diagnostics:** `/api/diagnostics`, `/api/diagnostics/entry-report`, `/…/bundle`, `/…/log`, `/…/errors/summary`, `/…/thread_health`, `/…/coin-trace/{sym}`, `/…/exit-r`, `/…/sell-timing`, `/…/veto-stats`, `/…/signal-win-rates`, `/api/buy-rejections`
- **Control:** `POST /api/agent/start|stop`, `/api/mode`, `/api/pause`, `/api/resume`, `/api/risk/resume|rebaseline`, `/api/db/backup`, `/api/backup/export|import`
- **Futures (separate):** `/api/futures/*`
- **Proxy:** `/api/proxy/binance/{path}` (browser → Binance via server)

---

## 5. WolfScore v3 — exact scoring math (`ev_model.py`)

`compute_submetrics(inp, cohort)` → 8 bounded sub-metrics, then `wolfscore(sub, tilt)` →
regime-gated families → sigmoid → **0–100 `pct`**. All inputs are in-memory (streamed
trades + 1m/5m klines + miniTicker); a missing feed degrades that metric to 0, never raises.

### 5.1 Sub-metrics (each clipped)
| Sym | Name | Definition |
|---|---|---|
| **T** | Trend alignment | `((EMA9−EMA21)/EMA21·100) / ATR%`, clip [−1,1] |
| **M** | Momentum | `MACD_hist / rolling_max_|hist|_20`, clip [−1,1] |
| **R** | Decoupling (cohort-relative) | `tanh(8·(ROC_15m − basket_median_ROC_15m))` |
| **C** | **CVD / order-flow pressure** | `(taker_buy_vol_5m − taker_sell_vol_5m) / total_vol_5m`, clip [−1,1] |
| **W** | VWAP room (anti-chasing) | `1 − 4·|mid − VWAP_15m|/VWAP_15m`, clip [−1,1] |
| **V** | Volume confirmation | `clip(vol_5m/avg_vol_20 − 1, 0, 2) / 2` → [0,1] |
| **X** | Volatility fitness | tent: `1 − |ATR% − atr_target|/atr_halfwidth`, clip [0,1] |
| **F** | **Friction** (dominant, negative) | `(half_spread% + avg_slippage%) / planned_stop%`, clip [0,1] |

### 5.2 Regime gating
From BTC 1h ROC → `tilt`. Then `up = max(0,tilt)`, `dn = max(0,−tilt)`, `neutral = 1−|tilt|`.
Regime label: **up** if `tilt>0.15`, **down** if `tilt<−0.15`, else **side**.

### 5.3 Families → z → probability
```
mom_core         = wm_T·T + wm_M·M                    # 1.6, 1.4
def_core         = wd_R·R + wd_W·W                    # 1.8, 1.4
momentum_family  = (up + 0.5·neutral) · mom_core      # counts in uptrends
defensive_family = (dn + 0.5·neutral) · def_core      # counts in downtrends
up_room          = wu_W·W·up                          # 1.5  (VWAP anti-chase in uptrends)
base             = w_C·C + w_V·V + w_X·X − w_F·F       # 1.1, 0.7, 0.5, 3.5  (always counts)
residual         = 0.3·def_core·up − 0.3·mom_core·dn   # cross-regime coupling
z                = b0 + momentum_family + defensive_family + up_room + base + residual   # b0 = −1.0
pct              = sigmoid(z) · 100
```

### 5.4 Hard gates (return score 0 before the sigmoid)
- **friction:** `F > 0.5` (round-trip cost > half the 1R stop) → `hard_gate="friction"`.
- **extended_uptrend:** `up_extension_veto` AND `tilt>0.15` AND `W ≤ up_extension_w_thr` → `hard_gate="extended_uptrend"` (anti pump-chase).

### 5.5 Weights & training
- **Interim v3 defaults:** `wm_T=1.6, wm_M=1.4, wd_R=1.8, wd_W=1.4, wu_W=1.5, w_C=1.1, w_V=0.7, w_X=0.5, w_F=3.5, b0=−1.0`.
- **Trained:** 9-weight logistic fit (`_fit_logistic`), **realized-R-weighted**, friction-gated (rows with `F>0.5` excluded), on stored `(submetrics, regime_tilt, label, realized_r, ts)` samples. `trained=false` until enough clean trades accumulate; `/api/ev/train` fits, `/api/ev/activate` promotes a version.
- **Adaptive floor:** effective gate = `max(absolute_floor, distribution_rule)`; modes `absolute` (current) / p75 / meanstd / off.

---

## 6. Buy path (current — WolfScore-only) — `_check_buys_from_cache`

1. **Entries armed?** (backfill complete or ~2 min grace) — else return.
2. **Publish scores** for the UI (runs even if buys are blocked below).
3. **Global latches** (any → no buys): `trading_active` off, correlated-dump pause, daily-loss stop, consecutive-loss pause.
4. **Capacity:** at `max_positions` → wait for an exit.
5. **Tiered WolfScore** per coin (HOT/WARM/COLD cadence — see §7).
6. **Hard gates:** friction, extended_uptrend.
7. **Up-regime restriction:** `live_up_regime_mode` (currently `allow` = off).
8. **THE gate:** `pct ≥ buy_score_threshold` (**currently 61**); below → `ev_prob_floor` block.
9. **Legacy engine + mandatory layer:** *not evaluated at all* under sole-gate.
10. **Real-time safety only:** slippage veto, correlation guard. (bb_upper / 5m_downtrend / falling_knife / trend_health / volume are disabled under sole-gate — WolfScore's T/W/X/V/C already price them in.)
11. **Structural:** budget / min-notional (~$10) / lot size / cooldown / already-held / in-flight claim / price-drift.
12. **Fire:** top-ranked ready coin **instant-fires a market (taker) order**; `taker_fallback` crosses spreads up to `taker_fallback_max_spread_pct` (0.30%).

---

## 7. ⏱️ Timing & refresh (the whole flow, fastest → slowest)

### Market data (data_collector, real-time)
| Signal | Cadence |
|---|---|
| WS `@trade` / `miniTicker` ticks | sub-second / ~1s rollup |
| WS 1m kline close → indicators + signal callback | on close (~60s/coin) |
| WS 5m / 15m kline close | on close |
| 5m trend veto | cached 180s/coin |
| Gap repair after WS reconnect | throttled 30s |
| `backfill_ready` per coin | when **1m ≥ 16 AND 5m ≥ 21** candles |

### Scoring & buy loop (trade_engine)
| Mechanism | Constant | Cadence |
|---|---|---|
| Buy dispatch (entry-heartbeat) | `eval_heartbeat_sec` | **5s** |
| Fast re-check (pending candidate) | `_FAST_RECHECK_SEC` | **2.5s** |
| Signal scanner (REST-backup refresh) | `SCAN_INTERVAL_SEC` | **30s** base, adaptive **30–120s** |
| Tiered re-score per coin | `_TIER_HOT/WARM/COLD_SEC` | HOT **2.5s** / WARM **12s** / COLD **45s** |
| Decoupled UI score publish | `_REFRESH_PUBLISH_MIN_SEC` | **12s** (throttled; CPU-heavy) |
| Confirm-then-fire hold | `confirm_seconds` | **3s** (top coin instant-fires) |
| Buy stagger (min between buys) | `_BUY_STAGGER_SEC` | **15s** |
| ATR (planned-stop) memo | `_WOLF_STOP_CACHE_TTL_SEC` | 45s |
| Slippage cache | `_SLIPPAGE_CACHE_TTL_SEC` | 300s |
| 24h quote-volume cache | `_QV24H_CACHE_TTL` | 300s |
| Bulk DB prefetch (candles+slippage) | `_BUY_PREFETCH_TTL_SEC` | 30s |
| Reversal memo | `_REVERSAL_TTL_SEC` | 30s |
| Stale-signal threshold | `_STALE_SIGNAL_SEC` | 180s |
| EV score memo / pub freshness | `_EV_SCORES_TTL_SEC` / `_EV_PUB_MAX_AGE_SEC` | 5s / 20s |

### Binance API cadence (ban-safe)
| Call | Cadence |
|---|---|
| Signed `/account` (balance) | ≤ **1 / 45s**, cached, non-blocking to UI |
| `exchangeInfo` | 24h |
| Orders | only when a buy fires (market/taker) |
| Public WS | continuous (no REST cost) |

### Backend response caches (single-flight TTL memos)
| Endpoint | TTL |
|---|---|
| `/api/all` | 2s (positions) / 3s (idle); prices re-stamped live each call |
| `/api/ev/scores` | served from published scores (no recompute) |
| `/api/diagnostics` | 2s |
| `/api/diagnostics/entry-report` | 4s |
| `/api/ev/model` | 6s |
| `/api/ev/expectancy` | 10s |
| trade-stats memo / recent-trades / health-snapshot | 8s / 5s / 2s |
| shadow summary / stats | 12s / 8s |

### Frontend poll cadence
| UI | Poll |
|---|---|
| Live price/status panels | ~0.5–1s |
| Stats / diagnostics panels | 5–10s |
| EntryGatePanel report / buy-threshold | 6s / 8s |

### Maintenance
| Job | Cadence |
|---|---|
| Daily maintenance (prune candles/klines/evals, vacuum, backup) | 900s after boot, then 24h |
| `activity_log` / `buy_rejections` cap-prune | once per ~300 / ~500 writes (not per write) |

---

## 8. Exit engine (SEPARATE — frozen, not modified)

ATR-scaled stop-loss, R:R take-profit, breakeven move + trailing stop, maker take-profit,
optional OCO, ATR profit-ratchet, capital recycler, delist/ghost handling. Runs on its own
monitor threads (sell-monitor / managed-exit-poller / held-refresher), independent of the
buy path. **Intentionally untouched during buy-path/perf work.**

---

## 9. Data & database

- **In-memory buffers:** `ws_candles` (1m→120, 5m→60, 15m→60), `price_ticks`, `price_samples`.
- **Tables:** candles, klines, trades, positions, activity_log, buy_rejections, entry_snapshots,
  training_samples, paper_trades, config_history, patterns, decisions, futures_*.
- **Concurrency:** one global `_RWLock` — shared reads / exclusive writes (writer-preferring).
- **Retention:** candles 5d, klines 7d, training/paper 3000 rows each, activity ~5000, rejections ~50000.
- **Perf note:** the single write lock is the shared bottleneck. Cap-prune `DELETE`s were moved
  off the per-write path; scoring reads only in-memory data; heavy endpoints are TTL-cached.

---

## 10. Config & boot migrations

`strategy.json` ↔ Pydantic schema (`strategy_config.py`); V2 blocks: `entries`, `exits`,
`sizing`, `risk`, `regime`, `data`. Rev-guarded boot migrations force the operator model:

- `buy_score_threshold = 61`, `wolfscore_sole_gate = true`
- `signal_engine.enabled = false`, `mandatory_signals_enabled = false`
- `maker_first = false`, `taker_fallback = true`, `taker_fallback_max_spread_pct = 0.30`
- `paper_shadow_enabled = false`
- `kline_retention_days = 7`, `ev_floor_mode = absolute`, `live_up_regime_mode = allow`

---

## 11. Deploy

`deploy.sh`: `git fetch + reset --hard origin/main`, use committed `dist/`, conditional pip,
`systemctl restart tradebot`, wait healthy. Version in `public/version.json` (vite-stamped).
Each restart re-backfills all watchlist coins (the few-minute score warmup) — so avoid
unnecessary restarts once on a stable version.
