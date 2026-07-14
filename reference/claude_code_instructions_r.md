# CLAUDE CODE INSTRUCTIONS - Deploy WolfScore-R + Valve3 to WolfBot
# (complete handoff; sandbox = C:\Users\Admin\Downloads\New folder (3))
# Supersedes the P5 scoring path. PROFIT-SIDE SELL ENGINE IS UNTOUCHABLE.

## 0. What this is
Three changes, nothing else:
  (1) REPLACE the WolfScore-P5 entry scoring with WolfScore-R: a dual-head model that outputs
      per coin per 5m close: pW = % chance the sell engine closes this buy in profit within 4h,
      and pZ = % chance the position is still underwater 3 days later. Buy gate: pW >= 55 AND
      pZ <= 2.4. Rank eligible coins by pW, highest first. Score UI keeps 0-100 semantics (show pW).
  (2) REMOVE the ATR stop loss (the loss-side protective stop) from the sell engine. Nothing else
      in the sell engine changes: take-profit 1.6R, ATR profit-ratchet, trailing, breakeven at 1.2R
      all stay byte-identical. ATR keeps its PROFIT-side roles (R geometry, ratchet trail width).
  (3) ADD the Valve: an independent position check, NOT a modification of the profit engine.
      From position age >= 864 bars (3 days), on every 5m CLOSE: if close < average entry price,
      market-sell the position and put that coin on a 288-bar (1 day) cooldown. Judged on CLOSE
      only, never on wicks. Max underwater hold becomes exactly 3 days; frozen slots become zero.
Reference implementation: sandbox rlib.py (feats_coin + mlp_prob + platt_apply) and r10.py.
Trained model artifact: wolf_r_model.json (sandbox). Validation log: r10out.txt.

## 1. Validation summary (why this ships)
- Artifact-exact validation (r10.py): the very weights in wolf_r_model.json, harsh sim
  (entry at next-bar open +5bp slip, exact-bar candidates, RT 0.20%, 8x$11, live pacing):
    Year 1 2025-07 -> 2026-07 (bear, IN-SAMPLE sanity): +$11.29, 0.43 tr/d, 91.7% win,
      1 valve fire, 0 freezes, max underwater hold 3.0d.
    Year 2 2024-07 -> 2025-07 (SEALED HOLDOUT):        +$2.25,  0.42 tr/d, 87.5% win,
      8 valve fires, 0 freezes, max underwater hold 3.0d.
  Valve value on identical entries (holdout): +$2.25 WITH valve vs -$11.91 without.
  Gate sensitivity (holdout): pZ<=2.2 +$3.07 | 2.4 +$2.25 | 2.6 +$3.97 - not knife-edge.
  Ensemble necessity: single-seed same gate = -$1.17 - the 3-member worst-case veto is load-bearing.
- Lineage: config family validated across BOTH years in the r-series/Path-B sweeps
  (champion pZ<=2.4+valve3: +$5.86/+$7.80 under the earlier sim conventions).
- Attack suite passed: fee stress (RT 0.25%), entry-fill honesty, stale-candidate check,
  coin dropout (78 coins, no concentration). Known limits: quarterly calibration drift in the
  extreme tail -> retrain quarterly (section 9); NO $/day promise - expect ~0.3-0.6 trades/day
  and small positive expectancy; the edge is a percentage, dollars scale with ticket size.

## 2. Model artifact format (wolf_r_model.json)
{ version: 'wolf-r-v1',
  features: [26 names in exact order - section 3],
  hidden: 16, activation: 'tanh',
  members: [ 3 x { head_win4h:  {mu[26], sd[26], W1[26x16], b1[16], W2[16], b2, platt:[a,b]},
                   head_freeze3d: {same shape} } ],
  aggregation: { pW: mean of members, pZ: MAX of members (worst-case veto) },
  gate: {pw_min: 55.0, pz_max: 2.4}, valve: {age >= 864 bars, judge close, cooldown 288},
  pacing: {max_new_per_30min: 2, coin_cooldown_bars: 36, slots: 8, ticket_usdt: 11.0},
  warmup_bars: 620 }
INFERENCE (per head, per member) - must match training exactly:
  x = features(26) -> clip to [-1e6, 1e6] -> replace any non-finite with 0.0
  z = clip((x - mu) / sd, -6, 6)
  h = tanh(z @ W1 + b1)            # 16 units
  s = clip(h @ W2 + b2, -30, 30)
  p_raw = sigmoid(s)
  p = sigmoid(a * logit(p_raw) + b)     # platt; logit(q)=ln(q/(1-q)) with q clipped to [1e-6, 1-1e-6]
  head% = 100 * p
pW = mean(head_win4h% over 3 members);  pZ = max(head_freeze3d% over 3 members).
Deterministic, no training at runtime. Pure numpy or pure python.

## 3. Feature spec - port VERBATIM from sandbox rlib.py feats_coin() (v2=False branch, 26 features)
All windows are in 5m bars and INCLUDE the current bar t. c/h/l/v = close/high/low/volume,
tb = taker-buy base volume (already in the 5m cache since P5). pc = previous close.
ATR14: TR = max(h-l, |h-pc|, |l-pc|); TR at the first bar := h-l; atr = simple mean of last 14 TR.
 0 r5      = c[t]/c[t-1] - 1
 1 r15     = c[t]/c[t-3] - 1
 2 r1h     = c[t]/c[t-12] - 1
 3 r4h     = c[t]/c[t-48] - 1
 4 r24h    = c[t]/c[t-288] - 1
 5 accel15 = r15 - (c[t-3]/c[t-6] - 1)
 6 pullbk  = (c - max(h,12)) / max(max(h,12) - min(l,12), 1e-9)
 7 lowsl1h = (min(l,6) - min(l,6 ending at t-6)) / max(atr14, 1e-9)
 8 volr    = v / max(mean(v,288), 1e-9)
 9 vburst  = sum(v,3) / max(3*mean(v,288), 1e-9)
10 tb5     = tb/v if v>0 else NaN (->0 after clean)
11 tb1h    = sum(tb,12) / max(sum(v,12), 1e-12)
12 tbshift = tb1h - mean( tb5_with_NaN_replaced_by_0.5, 288 )
13 dollv   = log10( max(mean(v,288)*c, 1e-9) )
14 atrp    = atr14 / max(c, 1e-9)
15 coil    = atr14 / max(atr14 at t-288, 1e-9)
16 rngexp  = (h - l) / max(atr14, 1e-9)
17 dhigh24 = c / max(max(h,288), 1e-9) - 1
18 pos24   = (c - min(l,288)) / max(max(h,288) - min(l,288), 1e-9)
19 rs1h    = r1h - median over universe coins of r1h        (cross-sectional, this scan)
20 rs24h   = r24h - median over universe coins of r24h      (cross-sectional, this scan)
21 breadth = share of universe coins with r1h > 0           (cross-sectional, this scan)
22 btc15   = BTC c[t]/c[t-3] - 1
23 btc1h   = BTC c[t]/c[t-12] - 1
24 hsin    = sin(2*pi * UTC_hour_of_bar_open / 24)
25 hcos    = cos(2*pi * UTC_hour_of_bar_open / 24)
Cross-sectional medians/breadth: computed once per 5m scan over all universe coins that have
passed the 620-bar warmup (coins in warmup are excluded from the median/breadth, matching training).
Coins gate as 'warmup' until 620 bars of history exist (P5's 50d backfill covers this).

## 4. Buy path (replaces the P5 scoring call, keeps everything around it)
Every 5m scan: for each eligible coin (universe pass, warmup done, not held, not cooling):
compute the 26 features -> pW, pZ. Eligible to buy if pW >= score_threshold (default 55,
stays wired to the existing UI slider) AND pZ <= pz_max (new config, default 2.4).
Rank eligibles by pW descending; buy respecting EXISTING pacing exactly as-is
(max 2 new per 30-min scan window, 36-bar coin cooldown, 8 slots, $11 tickets).
Existing protective gates (universe filter, friction, macro-bear) STAY - they only remove
trades; expected live trade count is therefore AT OR BELOW the validated ~0.4/day.
UI: score column = pW (0-100). Add pZ and a gate-reason field (ok / below_thr / pz_veto /
cooldown / warmup) to the signals panel for diagnosability.

## 5. Valve implementation (new, isolated; DO NOT touch profit-engine code paths)
A single new function checked once per 5m close for every open position, BEFORE nothing and
AFTER nothing in the profit engine - it is an independent monitor:
  age_bars = bars since entry fill
  if age_bars >= 864 and close < position_avg_entry_price:
      market-sell entire position now; set coin cooldown = 288 bars; log reason='valve3'
Judged on CLOSE only (never intrabar/wick). If the position is above water at any close,
the valve does nothing and the profit engine exits it normally whenever it can.

## 6. ATR stop-loss removal (loss side only)
Locate the loss-side ATR protective stop (any exit that sells BELOW entry based on an ATR
distance from entry or from price). Disable/delete that trigger completely - no residual code
path may sell a position at a loss except the Valve in section 5.
KEEP every profit-side ATR use untouched: R = clip(1.2*ATR%, 0.5%, 2.5%), TP = entry*(1+1.6R),
ratchet arm at 0.8R or +1.5%, trail = max(peak*(1-ATR%), entry*(1+0.5*peak_gain)),
breakeven arm at 1.2R with exit at entry*(1+fees). After the change, assert in a code search:
zero references to the loss-stop trigger remain; the only negative-PnL exit path is 'valve3'.

## 7. Replacement map
Route every P5 scoring touchpoint to R: the scorer call in the scan loop, the signals-summary
fast path, the frontend score field, diagnostics bundle, and any threshold checks. P5 code
stays in the repo behind a config flag scoring_engine: 'wolf-r' | 'wolf-p5' (default 'wolf-r')
for instant rollback. Copy wolf_r_model.json to the same location wolf_p5_model.json lives;
load once at startup; fail loudly (gate everything as 'model_missing') if absent or malformed.

## 8. Verification protocol (deterministic - no shadow period required)
1) Port feats_coin (v2=False), the inference math, and platt_apply from sandbox rlib.py VERBATIM.
2) Equivalence test (must pass before switching the flag): script that loads wolf_r_model.json
   plus a slice of the VPS 5m cache, computes (pW, pZ) through the LIVE pipeline and through
   the sandbox reference on >= 1,000 random (coin, bar) samples across >= 20 coins.
   PASS = max |delta pW| < 1e-6 and max |delta pZ| < 1e-6. (Same standard as the P5 deploy:
   FEAT diff 0.0.) Include one full-universe scan timing check: must fit the 5m loop budget.
3) Valve unit test: synthetic position aged 863 bars underwater -> no action; 864 underwater
   -> sell + 288 cooldown; 864+ above water -> no action; wick below entry but close above -> no action.
4) ATR-stop test: force a position deep underwater pre-day-3 in a dry run -> assert NO exit fires.
5) Deploy fully live after 1-4 pass (no phases, per standing instruction). First 48h: watch the
   gate-reason panel; pZ_veto should dominate; buys should be rare (~0-1/day) and high-quality.

## 9. Ops and retraining
- Quarterly retrain (calibration drift is documented): on the sandbox PC, refresh the year file
  (fetch_year.py pattern into wolfbot_bt), run: python r10.py  -> regenerates wolf_r_model.json
  and re-validates on the holdout year; redeploy the json only if the DEPLOY row stays positive.
- Config knobs exposed: score_threshold (default 55; warn the user that 65 => near-zero trades),
  pz_max (default 2.4; validated range 2.2-2.6), valve age_bars (864) and cooldown (288).
- Expectations to surface in the UI/README: ~0.3-0.6 trades/day, 86-92% win rate, zero frozen
  slots, max underwater hold 3 days, ~1-15 valve exits/year at roughly -$0.3 to -$0.5 each.
  No $/day promise; expectancy is small and positive; dollars scale with ticket size only.
