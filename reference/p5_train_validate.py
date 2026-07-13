# p5.py - WolfScore-P5 year validation: waits for klines_5m_1y.npz, then runs
# quarter-by-quarter walk-forward with macro-regime analysis, MR + oracle baselines,
# and the no-loss-exit vs 5-day-valve comparison.
import os, math, time, datetime, traceback, json
import numpy as np
from collections import Counter, deque
HERE = os.path.dirname(os.path.abspath(__file__))
BT = os.path.join(os.path.expanduser('~'), 'wolfbot_bt')
LOG = open(os.path.join(HERE, 'p5out.txt'), 'w', encoding='utf-8')
def P(*a):
    s = ' '.join(str(x) for x in a)
    print(s); LOG.write(s + '\n'); LOG.flush()
NPZ = os.path.join(BT, 'klines_5m_1y.npz')
FLOG = os.path.join(BT, 'fetch_year.log')
T0 = time.time()
waited = 0
while True:
    done = os.path.exists(NPZ) and os.path.exists(os.path.join(BT, 'meta_1y.json'))
    if done:
        try:
            done = 'DONE' in open(FLOG, encoding='utf-8').read()[-200:]
        except Exception:
            pass
    if done: break
    if waited == 0: P('waiting for year fetch to finish...')
    time.sleep(30); waited += 30
    if waited > 7200:
        P('gave up waiting after 2h'); LOG.close(); raise SystemExit
P('year data ready after %ds wait; loading...' % waited)
Z = np.load(NPZ)
META = json.load(open(os.path.join(BT, 'meta_1y.json')))
FV = {k: int(v) for k, v in META['first_valid'].items()}
times = Z['times']
coins = sorted([k for k in Z.files if k != 'times'])
N = len(times)
TSMS = times.tolist()
S = {}
for c in coins:
    a = Z[c].astype(np.float64)
    S[c] = {'op': a[:, 0].tolist(), 'h': a[:, 1].tolist(), 'l': a[:, 2].tolist(),
            'c': a[:, 3].tolist(), 'v': a[:, 4].tolist(), 'tb': a[:, 5].tolist()}
del Z
BTC = S['BTC']['c']
FEE = 0.001; SPREAD = 0.0005; RT = 2 * FEE + SPREAD
def clip(x, a, b): return max(a, min(b, x))
def mean(x):
    x = list(x); return sum(x) / len(x) if x else 0.0
P('coins %d bars %d = %.0f days | RT cost %.3f%%' % (len(coins), N, N * 5 / 1440, RT * 100))
P('precomputing prefix sums / rolling 30d highs...')
CS = {}
for c in coins:
    cl = S[c]['c']; cs = [0.0] * (N + 1)
    for i in range(N): cs[i + 1] = cs[i] + cl[i]
    CS[c] = cs
def sma(c, i, n):
    if i < n: n = i
    if n <= 0: return S[c]['c'][i]
    cs = CS[c]; return (cs[i] - cs[i - n]) / n
HI30 = {}
for c in coins:
    hi = S[c]['h']; H = [max(hi[j:j + 12]) for j in range(0, N, 12)]
    out = [0.0] * len(H); dq = deque()
    for idx, v in enumerate(H):
        while dq and H[dq[-1]] <= v: dq.pop()
        dq.append(idx)
        while dq[0] < idx - 719: dq.popleft()
        out[idx] = H[dq[0]]
    HI30[c] = out
def hi30(c, i): return HI30[c][min(i // 12, len(HI30[c]) - 1)] or S[c]['c'][i]
def rsi(cl, i, n=14):
    up = dn = 0.0
    for j in range(i - n, i):
        d = cl[j] - cl[j - 1]
        if d > 0: up += d
        else: dn -= d
    if up + dn == 0: return 0.0
    return (up / (up + dn)) * 2 - 1
def htf(cl, i, mult, n):
    out = []; j = i
    for _ in range(n):
        if j - mult < 0: break
        out.append(cl[j - 1]); j -= mult
    return out[::-1]
BR = {}
def breadth(i):
    if i in BR: return BR[i]
    n = 0
    for c in coins:
        if S[c]['c'][i] > sma(c, i, 240): n += 1
    b = n / len(coins); BR[i] = b; return b
def btilt_f(i):
    return math.tanh(60 * ((BTC[i - 1] / BTC[i - 13] - 1) if i >= 13 and BTC[i - 13] else 0))
MAC = {}
def macro(i):
    if i in MAC: return MAC[i]
    b50 = (BTC[i] / (sma('BTC', i, 14400) or BTC[i]) - 1)
    n = 0
    for c in coins:
        if S[c]['c'][i] > sma(c, i, 14400): n += 1
    br50 = n / len(coins)
    if b50 < -0.02 and br50 < 0.35: st = 'BEAR'
    elif b50 > 0.01 and br50 > 0.55: st = 'BULL'
    else: st = 'MIX'
    out = (st, b50, br50); MAC[i] = out; return out
CTX = {}
def ctx3(i):
    if i in CTX: return CTX[i]
    rocs = []; nd = 0; nt = 0
    for c in coins:
        cl = S[c]['c']
        if cl[i - 4]:
            r = cl[i - 1] / cl[i - 4] - 1
            rocs.append(r); nt += 1
            if r < -0.001: nd += 1
    rocs.sort(); med = rocs[len(rocs) // 2] if rocs else 0.0
    br = breadth(i); bs = br - breadth(max(240, i - 288))
    codip = nd / nt if nt else 0.0
    out = (med, btilt_f(i), br, bs, codip); CTX[i] = out
    return out
FN3 = ['dip','rsi5','rsi15','rsi1h','rsi4h','pos_rng','bb','tr15','tr1','tr4',
       'sl15','sl1','sl4','align','stack','volsurge','cvd','cvd_div','atrpn','atr_exp',
       'btc','rs','wick','redrun','vwapd','fr','ret24','ret3d','dhigh','bre','bslope','btcma',
       'ret15','ret1h','low1h','hl2','coil','codip','btc15','hsin','hcos']
IX3 = {k: j for j, k in enumerate(FN3)}
def FEAT(c, i, cx):
    med, btilt, bre_, bslope, codip = cx
    cl = S[c]['c']; hi = S[c]['h']; lo = S[c]['l']; vo = S[c]['v']; tb = S[c]['tb']; op = S[c]['op']
    if i < FV.get(c, 0) + 320 or cl[i] == 0: return None
    px = cl[i]
    m20 = mean(cl[i - 20:i]); sd20 = (mean([(x - m20) ** 2 for x in cl[i - 20:i]])) ** 0.5 or 1e-9
    h15 = htf(cl, i, 3, 30); h1 = htf(cl, i, 12, 26); h4 = htf(cl, i, 48, 13)
    if len(h15) < 25 or len(h1) < 21 or len(h4) < 9: return None
    ma15 = mean(h15[-20:]); ma1 = mean(h1[-20:]); ma4 = mean(h4[-8:])
    trs = [max(hi[j] - lo[j], abs(hi[j] - cl[j - 1]), abs(lo[j] - cl[j - 1])) for j in range(i - 14, i)]
    atr = mean(trs); atrp = atr / px if px else 0
    trs_o = [max(hi[j] - lo[j], abs(hi[j] - cl[j - 1]), abs(lo[j] - cl[j - 1])) for j in range(i - 40, i - 14)]
    atr_o = mean(trs_o) or 1e-9
    vs = mean(vo[i - 20:i]) or 1e-9; v5 = mean(vo[i - 5:i])
    cvd5 = sum(2 * tb[j] - vo[j] for j in range(i - 5, i)); tv5 = sum(vo[i - 5:i]) or 1e-9
    cvdp_ = sum(2 * tb[j] - vo[j] for j in range(i - 10, i - 5)); tvp = sum(vo[i - 10:i - 5]) or 1e-9
    ret5 = px / (cl[i - 5] or px) - 1
    cvdn = cvd5 / tv5; cvdp = cvdp_ / tvp
    hh = max(hi[i - 48:i]); ll = min(lo[i - 48:i]); rng = (hh - ll) or 1e-9
    vwv = sum(vo[i - 20:i]) or 1e-9
    vwap = sum(cl[j] * vo[j] for j in range(i - 20, i)) / vwv
    red = 0
    for j in range(i - 1, i - 7, -1):
        if cl[j] < op[j]: red += 1
        else: break
    r1 = (hi[i - 1] - lo[i - 1]) or 1e-9
    f = {}
    f['dip'] = clip((m20 - px) / (2 * sd20), -1, 1); f['rsi5'] = rsi(cl, i, 14)
    f['rsi15'] = rsi(h15, len(h15), 14) if len(h15) > 15 else 0
    f['rsi1h'] = rsi(h1, len(h1), 14) if len(h1) > 15 else 0
    f['rsi4h'] = rsi(h4, len(h4), 8) if len(h4) > 9 else 0
    f['pos_rng'] = clip((px - ll) / rng * 2 - 1, -1, 1); f['bb'] = clip((px - m20) / (2 * sd20), -1, 1)
    f['tr15'] = clip((px / ma15 - 1) * 40, -1, 1); f['tr1'] = clip((px / ma1 - 1) * 25, -1, 1)
    f['tr4'] = clip((px / ma4 - 1) * 15, -1, 1)
    f['sl15'] = clip((h15[-1] / h15[-5] - 1) * 80, -1, 1) if h15[-5] else 0
    f['sl1'] = clip((h1[-1] / h1[-6] - 1) * 60, -1, 1) if h1[-6] else 0
    f['sl4'] = clip((h4[-1] / h4[-4] - 1) * 20, -1, 1) if h4[-4] else 0
    f['align'] = clip((f['tr15'] + f['tr1'] + f['tr4']) / 3, -1, 1)
    f['stack'] = clip((1 if f['tr1'] > 0 else -1) * 0.4 + (1 if f['tr4'] > 0 else -1) * 0.4 + (1 if f['sl4'] > 0 else -1) * 0.2, -1, 1)
    f['volsurge'] = clip(v5 / vs - 1, -1, 2) / 2; f['cvd'] = clip(cvdn, -1, 1)
    f['cvd_div'] = clip((cvdn - cvdp) * 2 - ret5 * 80, -1, 1)
    f['atrpn'] = clip(atrp * 150, 0, 1); f['atr_exp'] = clip(atr / atr_o - 1, -1, 1)
    f['btc'] = btilt
    f['rs'] = math.tanh(200 * ((cl[i - 1] / cl[i - 4] - 1) - med)) if cl[i - 4] else 0
    f['wick'] = clip(((min(cl[i - 1], op[i - 1]) - lo[i - 1]) / r1) * 2 - 1, -1, 1)
    f['redrun'] = clip(red / 3.0 - 1, -1, 1)
    f['vwapd'] = clip((px - vwap) / vwap * 100, -1, 1)
    f['fr'] = clip(RT / max(atrp, 1e-6), 0, 1); f['_atrp'] = atrp
    f['ret24'] = clip((px / cl[i - 288] - 1) * 8, -1, 1) if i >= 288 and cl[i - 288] else 0.0
    f['ret3d'] = clip((px / cl[i - 864] - 1) * 5, -1, 1) if i >= 864 and cl[i - 864] else 0.0
    f['dhigh'] = clip((px / hi30(c, i) - 1) * 4, -1, 0)
    f['bre'] = bre_ * 2 - 1
    f['bslope'] = clip(bslope * 4, -1, 1)
    f['btcma'] = clip((BTC[i] / (sma('BTC', i, 576) or px) - 1) * 10, -1, 1)
    f['ret15'] = clip((px / cl[i - 3] - 1) * 100, -1, 1) if cl[i - 3] else 0.0
    f['ret1h'] = clip((px / cl[i - 12] - 1) * 60, -1, 1) if cl[i - 12] else 0.0
    l1 = min(lo[i - 12:i]) or 1e-9
    f['low1h'] = clip((px / l1 - 1) * 50, 0, 1)
    hl = sum(1 for j in range(i - 5, i) if lo[j] > lo[j - 1])
    f['hl2'] = hl / 5.0 * 2 - 1
    tr5 = mean(trs[-5:]); tr20 = mean(trs[-14:] + trs_o[-6:]) or 1e-9
    f['coil'] = clip(tr5 / tr20 - 1, -1, 1)
    f['codip'] = codip * 2 - 1
    f['btc15'] = clip((BTC[i] / BTC[i - 3] - 1) * 150, -1, 1) if BTC[i - 3] else 0.0
    hr = (TSMS[i] // 3600000) % 24
    f['hsin'] = math.sin(2 * math.pi * hr / 24.0)
    f['hcos'] = math.cos(2 * math.pi * hr / 24.0)
    return f
KEY = ['tr4', 'tr1', 'dip', 'align', 'stack', 'rsi5', 'cvd_div', 'bre', 'ret3d', 'dhigh']
PAIRS = [('tr4','dip'),('tr1','rsi5'),('stack','dip'),('align','volsurge'),('btc','dip'),
         ('atrpn','fr'),('bre','dip'),('btcma','dip'),('bre','atrpn'),('ret3d','dip'),
         ('codip','dip'),('low1h','volsurge')]
def expand(x):
    e = list(x)
    for k in KEY: e.append(x[IX3[k]] * x[IX3[k]])
    for a, b in PAIRS: e.append(x[IX3[a]] * x[IX3[b]])
    return e

def sim_ns(c, i, atrp, maxbars=8640):
    cl = S[c]['c']; hi = S[c]['h']; lo = S[c]['l']
    entry = cl[i] * (1 + SPREAD)
    R = clip(1.2 * atrp, 0.005, 0.025)
    tp = entry * (1 + 1.6 * R); be_price = entry * (1 + RT)
    peak = entry; armed = False; be = False
    end = min(i + maxbars, N - 1)
    for j in range(i + 1, end):
        h = hi[j]; l = lo[j]; px = cl[j]
        if px > peak: peak = px
        gain = peak / entry - 1
        if not armed and (gain >= 0.8 * R or gain >= 0.015): armed = True
        if not be and (h / entry - 1) >= 1.2 * R: be = True
        if h >= tp: return tp / entry - 1 - RT, j - i, 'tp'
        if armed:
            ta = peak * (1 - 1.0 * atrp); pp = peak / entry - 1
            tg = entry * (1 + pp * 0.5); tr_ = max(ta, tg)
            if l <= tr_:
                net = tr_ / entry - 1 - RT
                if net >= 0.0009: return net, j - i, 'ratchet'
        if be and l <= be_price: return 0.0, j - i, 'breakeven'
    return cl[end] / entry - 1 - RT, end - i, 'still_open'

def sim_valve(c, i, atrp, bail=1440, maxbars=8640):
    cl = S[c]['c']; hi = S[c]['h']; lo = S[c]['l']
    entry = cl[i] * (1 + SPREAD)
    R = clip(1.2 * atrp, 0.005, 0.025)
    tp = entry * (1 + 1.6 * R); be_price = entry * (1 + RT)
    peak = entry; armed = False; be = False
    end = min(i + maxbars, N - 1)
    for j in range(i + 1, end):
        h = hi[j]; l = lo[j]; px = cl[j]
        if px > peak: peak = px
        gain = peak / entry - 1
        if not armed and (gain >= 0.8 * R or gain >= 0.015): armed = True
        if not be and (h / entry - 1) >= 1.2 * R: be = True
        if h >= tp: return tp / entry - 1 - RT, j - i, 'tp'
        if armed:
            ta = peak * (1 - 1.0 * atrp); pp = peak / entry - 1
            tg = entry * (1 + pp * 0.5); tr_ = max(ta, tg)
            if l <= tr_:
                net = tr_ / entry - 1 - RT
                if net >= 0.0009: return net, j - i, 'ratchet'
        if be and l <= be_price: return 0.0, j - i, 'breakeven'
        if j - i >= bail and px < entry:
            return px / entry - 1 - RT, j - i, 'valve'
    return cl[end] / entry - 1 - RT, end - i, 'still_open'
SIMC = {}; SIMV = {}
def simc(c, i, atrp):
    k = (c, i)
    if k not in SIMC: SIMC[k] = sim_ns(c, i, atrp)
    return SIMC[k]
def simv(c, i, atrp):
    k = (c, i)
    if k not in SIMV: SIMV[k] = sim_valve(c, i, atrp)
    return SIMV[k]

def mlp_train(Xm, y, hid=24, epochs=250, lr=0.05, l2=1e-4, seed=7):
    rng = np.random.RandomState(seed)
    n, d = Xm.shape
    W1 = rng.randn(d, hid) * math.sqrt(2.0 / d); b1 = np.zeros(hid)
    W2 = rng.randn(hid) * math.sqrt(1.0 / hid); b2 = 0.0
    mW1 = np.zeros_like(W1); mb1 = np.zeros_like(b1); mW2 = np.zeros_like(W2); mb2 = 0.0
    for ep in range(epochs):
        H = np.maximum(Xm @ W1 + b1, 0.0)
        z = np.clip(H @ W2 + b2, -30, 30)
        p = 1.0 / (1.0 + np.exp(-z))
        e = (p - y) / n
        gW2 = H.T @ e + l2 * W2; gb2 = e.sum()
        dH = np.outer(e, W2); dH[H <= 0] = 0.0
        gW1 = Xm.T @ dH + l2 * W1; gb1 = dH.sum(0)
        mW2 = 0.9 * mW2 - lr * gW2; W2 = W2 + mW2
        mb2 = 0.9 * mb2 - lr * gb2; b2 = b2 + mb2
        mW1 = 0.9 * mW1 - lr * gW1; W1 = W1 + mW1
        mb1 = 0.9 * mb1 - lr * gb1; b1 = b1 + mb1
    return (W1, b1, W2, b2)
def mlp_prob(m, Xm):
    W1, b1, W2, b2 = m
    H = np.maximum(Xm @ W1 + b1, 0.0)
    z = np.clip(H @ W2 + b2, -30, 30)
    return 1.0 / (1.0 + np.exp(-z))
def _logit(p):
    p = np.clip(p, 1e-6, 1 - 1e-6)
    return np.log(p / (1 - p))
def platt_fit(p_raw, y):
    z = _logit(p_raw); a = 1.0; b = 0.0
    for _ in range(600):
        q = 1.0 / (1.0 + np.exp(-(a * z + b)))
        e = q - y
        a -= 0.5 * (e * z).mean(); b -= 0.5 * e.mean()
    return (a, b)
def platt_apply(ab, p_raw):
    z = _logit(p_raw)
    return 1.0 / (1.0 + np.exp(-(ab[0] * z + ab[1])))

P('building setup rows over the full year (stride 60)...')
_t = time.time()
ROWS = []
step_ct = 0
for i in range(320, N - 1, 60):
    cx = ctx3(i)
    for c in coins:
        f = FEAT(c, i, cx)
        if not f: continue
        r, bars, how = sim_ns(c, i, f['_atrp'])
        ROWS.append((i, c, expand([f[k] for k in FN3]), f['_atrp'], r, bars, how))
    step_ct += 1
    if step_ct % 250 == 0:
        P('  ... %d/%d steps, %d rows (%.0fs)' % (step_ct, (N - 321) // 60 + 1, len(ROWS), time.time() - _t))
P('rows %d  (%.0fs)' % (len(ROWS), time.time() - _t))
X = np.array([r[2] for r in ROWS], dtype=np.float64)
I = np.array([r[0] for r in ROWS]); RET = np.array([r[4] for r in ROWS])
BARS = np.array([r[5] for r in ROWS]); HOW = [r[6] for r in ROWS]
WIN24 = ((RET >= 0.004) & (BARS <= 288)).astype(np.float64)
TRAP = np.array([1.0 if (HOW[k] == 'still_open' or BARS[k] > 864) else 0.0 for k in range(len(ROWS))])
P('labels: WIN24 base %.1f%%  TRAP base %.1f%%  avg ret %+.3f%%' % (100 * WIN24.mean(), 100 * TRAP.mean(), 100 * RET.mean()))
SIMC.clear()

def fitq(qs, qe, tag):
    emb = 864
    tr = I < (qs - emb); te = (I >= qs) & (I < qe)
    vcut = int((qs - emb) * 0.85)
    core = I < vcut; val = (I >= vcut) & (I < (qs - emb))
    mW = mlp_train(X[core], WIN24[core]); mT = mlp_train(X[core], TRAP[core], seed=11)
    abW = platt_fit(mlp_prob(mW, X[val]), WIN24[val])
    abT = platt_fit(mlp_prob(mT, X[val]), TRAP[val])
    Rtr = RET[tr]
    Wm = float(Rtr[WIN24[tr] > 0.5].mean()); Tm = float(Rtr[TRAP[tr] > 0.5].mean())
    mid = (WIN24[tr] < 0.5) & (TRAP[tr] < 0.5); Mm = float(Rtr[mid].mean())
    P('')
    P('=== %s | train %d (val %d) test %d ===' % (tag, int(tr.sum()), int(val.sum()), int(te.sum())))
    P('class means: WIN %+.3f%%  MID %+.3f%%  TRAP %+.3f%%' % (100 * Wm, 100 * Mm, 100 * Tm))
    pw = platt_apply(abW, mlp_prob(mW, X[te])) * 100
    P('unconditional test: WIN24 %.1f%%  TRAP %.1f%%  avg ret %+.3f%%'
      % (100 * WIN24[te].mean(), 100 * TRAP[te].mean(), 100 * RET[te].mean()))
    P('CALIBRATION (unseen):')
    for lo, hi_ in [(0,20),(20,30),(30,40),(40,50),(50,55),(55,60),(60,65),(65,101)]:
        m = (pw >= lo) & (pw < hi_)
        if m.sum() >= 30:
            P('  score %3d-%3d: n=%5d  WIN24 %5.1f%%  TRAP %5.1f%%  avg ret %+.3f%%'
              % (lo, hi_, int(m.sum()), 100 * WIN24[te][m].mean(), 100 * TRAP[te][m].mean(), 100 * RET[te][m].mean()))
    return dict(mW=mW, mT=mT, abW=abW, abT=abT, qs=qs, qe=qe, Wm=Wm, Mm=Mm, Tm=Tm, tag=tag)

SC = {}
def scores(i, M_, dcut=0.70):
    key = (i, M_['qs'])
    if key in SC: return SC[key]
    cx = ctx3(i)
    rows = []; feats = []
    for c in coins:
        if dcut > 0:
            h = hi30(c, i)
            if h > 0 and S[c]['c'][i] / h < dcut: continue
        f = FEAT(c, i, cx)
        if not f or f['fr'] > 0.6: continue
        rows.append((c, f['_atrp'])); feats.append(expand([f[k] for k in FN3]))
    out = []
    if feats:
        Xm = np.array(feats)
        pw = platt_apply(M_['abW'], mlp_prob(M_['mW'], Xm))
        pt = platt_apply(M_['abT'], mlp_prob(M_['mT'], Xm))
        pm = np.clip(1.0 - pw - pt, 0.0, 1.0)
        ev = pw * M_['Wm'] + pt * M_['Tm'] + pm * M_['Mm']
        out = sorted(((float(ev[k]), float(pw[k]) * 100, float(pt[k]), rows[k][0], rows[k][1])
                      for k in range(len(rows))), reverse=True)
    SC[key] = out; return out

def report(trades, qs, qe, label, extra=''):
    days = (qe - qs) * 5 / 1440.0; n = len(trades)
    if n == 0:
        P('  %-26s NO TRADES %s' % (label, extra)); return
    tot = sum(t['usd'] for t in trades)
    win = 100.0 * sum(1 for t in trades if t['r'] > 0) / n
    wins = [t['usd'] for t in trades if t['r'] > 0]
    loss = [t['usd'] for t in trades if t['r'] <= 0]
    aw = sum(wins) / len(wins) if wins else 0.0
    al = sum(loss) / len(loss) if loss else 0.0
    hold = mean([t['bars'] for t in trades]) * 5 / 60
    op = [t for t in trades if t['how'] == 'still_open']
    vl = sum(1 for t in trades if t['how'] == 'valve')
    jail = sum(t['bars'] for t in trades if t['bars'] > 288) * 5 / 1440.0
    P('  %-26s n=%4d (%4.1f/d) win %3.0f%%  net $%+7.2f (%+.3f $/d)  W/L $%+.3f/$%+.3f  hold %5.1fh  open %d ($%+.2f)  valve %d  jail %.0fsd %s'
      % (label, n, n / days, win, tot, tot / days, aw, al, hold, len(op),
         sum(t['usd'] for t in op), vl, jail, extra))

def port(M_, thr, cap, evmin, gmacro, valve=False, slots=8, ticket=11.0,
         step=6, max_new=2, cool=36, label=''):
    qs = M_['qs']; qe = M_['qe']
    simfn = simv if valve else simc
    pos = {}; trades = []; lastx = {}
    blocked = 0; cash = 0; steps = 0
    for i in range(qs, qe, step):
        for c in list(pos):
            if i >= pos[c]['xi']:
                trades.append(pos.pop(c)); lastx[c] = i
        steps += 1; cash += (slots - len(pos))
        if len(pos) >= slots: continue
        if gmacro and macro(i)[0] == 'BEAR':
            blocked += 1; continue
        newn = 0
        for ev, pw, pt, c, atrp in scores(i, M_):
            if len(pos) >= slots or newn >= max_new: break
            if c in pos or pw < thr or pt > cap or ev < evmin: continue
            if i - lastx.get(c, -10**9) < cool: continue
            r, bars, how = simfn(c, i, atrp)
            pos[c] = dict(r=r, bars=bars, how=how, xi=i + bars, usd=r * ticket, ei=i)
            newn += 1
    trades += list(pos.values())
    cashp = 100.0 * cash / (steps * slots) if steps else 0.0
    report(trades, qs, qe, label, extra='cash %2.0f%% blk %d' % (cashp, blocked))

def oracle(qs, qe, dcut=0.70, slots=8, ticket=11.0, step=12, max_new=2, cool=36):
    pos = {}; trades = []; lastx = {}
    for i in range(qs, qe, step):
        for c in list(pos):
            if i >= pos[c]['xi']:
                trades.append(pos.pop(c)); lastx[c] = i
        if len(pos) >= slots: continue
        cands = []
        for c in coins:
            if c in pos or i - lastx.get(c, -10**9) < cool: continue
            cl = S[c]['c']
            if i < FV.get(c, 0) + 320 or cl[i] == 0: continue
            if dcut > 0:
                h = hi30(c, i)
                if h > 0 and cl[i] / h < dcut: continue
            hi = S[c]['h']; lo = S[c]['l']
            trs = [max(hi[j] - lo[j], abs(hi[j] - cl[j - 1]), abs(lo[j] - cl[j - 1])) for j in range(i - 14, i)]
            atrp = mean(trs) / cl[i]
            if RT / max(atrp, 1e-9) > 0.6: continue
            r, bars, how = simc(c, i, atrp)
            if r >= 0.004 and bars <= 288:
                cands.append((r / max(bars, 1), r, bars, how, c))
        cands.sort(reverse=True)
        newn = 0
        for sp, r, bars, how, c in cands:
            if len(pos) >= slots or newn >= max_new: break
            pos[c] = dict(r=r, bars=bars, how=how, xi=i + bars, usd=r * ticket, ei=i)
            newn += 1
    trades += list(pos.values())
    report(trades, qs, qe, 'ORACLE ceiling (1h step)', extra='(perfect foresight)')

def mr_score(c, i, med, btilt):
    cl = S[c]['c']; hi = S[c]['h']; lo = S[c]['l']; vo = S[c]['v']; tb = S[c]['tb']; op = S[c]['op']
    px = cl[i]
    if px == 0 or not cl[i - 7]: return 0.0, 0.0
    m20 = mean(cl[i - 20:i]); sd = (mean([(x - m20) ** 2 for x in cl[i - 20:i]])) ** 0.5
    O = clip((m20 - px) / (2 * sd), -2, 2) if sd > 0 else 0.0
    cvd = [2 * tb[j] - vo[j] for j in range(i - 6, i)]
    ct = 1 if sum(cvd[3:]) > sum(cvd[:3]) else -1
    vf = 1 if mean(vo[i - 3:i]) < mean(vo[i - 20:i]) else -0.5
    rng = (hi[i - 1] - lo[i - 1]) or 1e-9
    lw = (min(cl[i - 1], op[i - 1]) - lo[i - 1]) / rng
    E = clip(0.5 * ct + 0.3 * vf + 0.6 * lw, -1, 1)
    roc3 = cl[i - 1] / cl[i - 4] - 1; rocp = cl[i - 4] / cl[i - 7] - 1
    K = clip(-(roc3 - rocp) * 200, -1, 1)
    b1 = (BTC[i] / BTC[i - 12] - 1) if BTC[i - 12] else 0
    Mk = clip(b1 * 150, -1, 1)
    trs = [max(hi[j] - lo[j], abs(hi[j] - cl[j - 1]), abs(lo[j] - cl[j - 1])) for j in range(i - 14, i)]
    atrp = (mean(trs) / px) if px else 0
    Fr = min(1.0, RT / max(atrp, 1e-9))
    Q = clip((px / (sma(c, i, 200) or px) - 1) * 20, -1, 1)
    RS = math.tanh(200 * (roc3 - med))
    t = btilt
    if t > 0.15: dm, wE, wK, wMk, wQ, wRS, wFr, b0 = 0.49, 2.4, 1.5, 1.2, 0.8, 0.0, 3.0, -0.5
    elif t < -0.15: dm, wE, wK, wMk, wQ, wRS, wFr, b0 = 1.12, 2.0, 2.5, 2.0, 0.8, 1.5, 3.0, -1.4
    else: dm, wE, wK, wMk, wQ, wRS, wFr, b0 = 0.70, 2.0, 1.5, 1.2, 0.8, 0.0, 3.0, -0.8
    if O < dm or K < -0.5 or Mk < -0.6 or Fr > 0.5 or Q < -0.5: return 0.0, atrp
    th = wE * E + wK * K + wMk * Mk + wQ * Q + wRS * RS - wFr * Fr
    z = b0 + min(1.0, max(0.0, O)) * th
    return 100.0 / (1.0 + math.exp(-clip(z, -30, 30))), atrp
MRC = {}
def mr_port(qs, qe, thr=55, slots=8, ticket=11.0, step=6):
    pos = {}; trades = []
    for i in range(qs, qe, step):
        for c in list(pos):
            if i >= pos[c]['xi']: trades.append(pos.pop(c))
        if len(pos) >= slots: continue
        if i not in MRC:
            med, bt, br, bs, codip = ctx3(i)
            out = []
            for c in coins:
                if i < FV.get(c, 0) + 320: continue
                s, atrp = mr_score(c, i, med, bt)
                if s > 0: out.append((s, c, atrp))
            out.sort(reverse=True); MRC[i] = out
        for s, c, atrp in MRC[i]:
            if len(pos) >= slots: break
            if c in pos or s < thr: continue
            r, bars, how = simc(c, i, atrp)
            pos[c] = dict(r=r, bars=bars, how=how, xi=i + bars, usd=r * ticket, ei=i)
    trades += list(pos.values())
    report(trades, qs, qe, 'MR-live replica thr55')

try:
    def _dt(ms): return datetime.datetime.fromtimestamp(ms / 1000, datetime.timezone.utc).strftime('%Y-%m-%d')
    P('')
    P('=' * 110)
    P('DATASET %s -> %s UTC' % (_dt(TSMS[0]), _dt(TSMS[-1])))
    QL = N // 4
    for q in range(4):
        s, e = q * QL, min((q + 1) * QL, N - 1)
        ew = mean([S[c]['c'][e] / S[c]['c'][max(s, FV.get(c, 0))] - 1 for c in coins if S[c]['c'][max(s, FV.get(c, 0))]])
        P('  Q%d %s -> %s : EW %+7.2f%%   BTC %+7.2f%%' % (q + 1, _dt(TSMS[s]), _dt(TSMS[e]),
          100 * ew, 100 * (BTC[e] / BTC[s] - 1)))
    P('')
    P('MACRO-STATE outcome table (slow regime: BTC vs 50d SMA + 50d breadth), all rows:')
    STROW = np.array([macro(int(i))[0] for i in I])
    P('%-6s %7s %7s %8s %8s %10s %12s' % ('state', 'n', 'WIN24%', 'TRAP%', 'avg ret', 'trap sev', 'time share'))
    uniqI = sorted(set(int(x) for x in I))
    tsh = Counter(macro(i)[0] for i in uniqI)
    for st in ['BULL', 'MIX', 'BEAR']:
        m = STROW == st
        if m.sum() < 50: continue
        tm = (TRAP > 0.5) & m
        sev = 100 * RET[tm].mean() if tm.sum() else 0.0
        P('%-6s %7d %6.1f%% %7.1f%% %+8.3f%% %+9.2f%% %10.1f%%' % (st, int(m.sum()),
          100 * WIN24[m].mean(), 100 * TRAP[m].mean(), 100 * RET[m].mean(), sev,
          100.0 * tsh[st] / len(uniqI)))
    QN = ['Q2', 'Q3', 'Q4']
    for qi, q in enumerate((1, 2, 3)):
        qs, qe = q * QL, min((q + 1) * QL, N - 1)
        M_ = fitq(qs, qe, QN[qi])
        P('')
        P('--- PORTFOLIO unseen %s (8 x $11, MTM incl. open) ---' % QN[qi])
        mr_port(qs, qe)
        oracle(qs, qe)
        port(M_, 55, 0.12, -0.0005, False, label='P5 thr55')
        port(M_, 55, 0.12, -0.0005, True, label='P5 thr55 +macrogate')
        port(M_, 60, 0.12, -0.0005, True, label='P5 thr60 +macrogate')
        port(M_, 50, 0.12, 0.0005, True, label='P5 thr50 EV>=.05 +mg')
        port(M_, 0, 0.12, 0.0010, True, label='P5 EVonly>=.10 +mg')
        port(M_, 55, 0.12, -0.0005, True, valve=True, label='P5v thr55 +mg VALVE')
        port(M_, 50, 0.12, 0.0005, True, valve=True, label='P5v thr50 EV.05 VALVE')
        port(M_, 0, 0.12, 0.0010, True, valve=True, label='P5v EVonly.10 VALVE')
        SC.clear(); MRC.clear()
        P('  [t=%.0fs]' % (time.time() - T0))
    P('')
    P('TOTAL RUNTIME %.0fs' % (time.time() - T0))
except Exception:
    P(traceback.format_exc())
LOG.close()
