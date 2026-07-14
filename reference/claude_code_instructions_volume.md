# CLAUDE CODE INSTRUCTIONS - WolfScore-R VOLUME MODE (live experiment, user-ordered)
# (sandbox = C:\Users\Admin\Downloads\New folder (3); companion doc: claude_code_instructions_r.md)

## 0. What this is
Ship the HIGH-VOLUME buy engine live, per user order, with a weekly refine loop. Three changes:
  (1) Scoring: use the SAME artifact wolf_r_model.json (dual heads pW/pZ, 3-member ensemble,
      pW = mean, pZ = MAX of members) but at the VOLUME gate: pw_min=50, pz_max=6.0.
      Model format, 26-feature spec, and inference math: EXACTLY as claude_code_instructions_r.md
      sections 2-3. Do not re-derive anything; port verbatim from sandbox rlib.py.
  (2) REMOVE the ATR stop loss (loss-side) from the sell engine. NOTHING else in the sell
      engine changes (TP 1.6R, ratchet, trailing, breakeven 1.2R stay byte-identical).
  (3) NO loss exits of ANY kind in this mode: no stop loss, no valve, no time exit.
      Positions are held to recovery. The ONLY negative-PnL path is nothing - assert it.

## 1. Baseline the live test must beat (r11.py, both years, honest fills, this exact artifact)
                          tr/day   win%    year P&L      $/day
  VOLUME  (50,6)+guard:   3.5-4.7  81-83%  -$49 / -$83   -0.13 / -0.23   (y1 in-sample / y2 holdout)
  BALANCED(55,4)+guard:   2.0-3.0  82-87%  -$13 / -$39   -0.03 / -0.11
  MAX     (50,6) no guard 6.1-6.3  80-83%  -$78 / -$91   -0.21 / -0.25
The user has ordered the live test with these baselines on record. The scorecard (section 4)
exists so live reality is compared to this table weekly - that comparison IS the refine loop.
Freezes (positions stuck underwater >3d) are the entire loss channel: 18-52/yr in backtest.

## 2. Configuration (all knobs live-editable, persisted)
  scoring_engine: 'wolf-r-volume'            # new mode value
  pw_min: 50            # score threshold, wired to the existing UI slider
  pz_max: 6.0           # freeze-veto ceiling (worst ensemble member)
  preset: 'volume'      # 'volume' = (50, 6.0) | 'balanced' = (55, 4.0) - just sets the two above
  pacing: max_new_per_30min: 4, coin_cooldown_bars: 12, slots: 8, ticket_usdt: 11
  cluster_guard: true   # PAUSE new buys while >=3 open positions are >288 bars old AND underwater
  hour_window: off      # optional knob: only enter 12:00-17:00 UTC (loss-reducing, volume-reducing)
  Existing universe / friction / macro-bear gates: KEEP as configured (they only remove trades).
  Ranking: pW descending among eligible coins, one candidate per coin per 5m scan.

## 3. Loss-exit removal and the no-loss-exit assertion
Locate and disable the loss-side ATR protective stop (any exit selling below entry from an ATR
distance). Profit-side ATR roles stay: R = clip(1.2*ATR%, 0.5%, 2.5%), TP = 1.6R, ratchet trail
1xATR from peak, breakeven arm 1.2R. In 'wolf-r-volume' mode there is NO valve and NO time exit.
Add a startup + runtime assertion: in this mode, no code path may submit a sell order whose
expected PnL is negative. Any violation = log CRITICAL and block the order.

## 4. Live scorecard (the refine instrument - build this with the same care as the scoring)
Persist a daily rollup (SQLite/json, plus a frontend panel):
  date | trades | wins | breakevens | net_usd | fees_usd | trades_per_slot |
  open_positions | open_gt_24h | open_gt_72h | worst_drawdown_pct | deployed_usd |
  new_freezes (coin, entry_time, current_drawdown%)
Plus a 7-day summary row computed nightly: avg tr/day, win%, net $/day - printed next to the
section-1 baseline for the active preset, with a divergence column (live minus baseline).
This panel is the deliverable that makes "test live and keep refining" real.

## 5. Verification before flipping the flag (deterministic, no shadow phase)
1) Equivalence test exactly as claude_code_instructions_r.md section 8 items 1-2:
   live pipeline vs sandbox rlib.py reference on >=1,000 (coin,bar) samples,
   PASS = max |delta pW| and |delta pZ| < 1e-6. Timing: full-universe scan fits the 5m loop.
2) ATR-stop-removed assertion test: position deep underwater in dry run -> NO exit fires.
3) Cluster-guard unit test: 3 synthetic positions aged >288 bars underwater -> new buys paused;
   one recovers/closes -> buys resume.
4) Scorecard writes a correct rollup for a synthetic day of trades.
Then deploy fully live (no phases, per standing instruction).

## 6. Rollback
scoring_engine: 'wolf-r-volume' | 'wolf-r' | 'wolf-p5' (P5 path stays dormant in the repo).
Switching modes must not disturb open positions - they keep running on the untouched sell engine.

## 7. Weekly refine protocol
Change ONE knob per week, in this order of expected impact: preset volume->balanced;
cluster_guard off->on (if off); hour_window on; pz_max sweep 4.0-8.0; pw_min sweep 45-60.
Each week: paste the 7-day scorecard summary back to the strategy chat; the sandbox
(rlib.py + r11.py) re-simulates the exact knob change on both years before the next flip.
Quarterly: refresh the year file and rerun r10.py to retrain wolf_r_model.json.
