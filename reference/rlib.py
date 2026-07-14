# rlib.py - shared pipeline for the R-series attack/fix cycle. Single source of truth.
import os, json, time
import numpy as np
from numpy.lib.stride_tricks import sliding_window_view as swv
BT = os.path.join(os.path.expanduser('~'), 'wolfbot_bt')
HERE = os.path.dirname(os.path.abspath(__file__))
LOGF = None
T0 = time.time()
def init(logname):
    global LOGF, T0
    LOGF = open(os.path.join(HERE, logname), 'w', encoding='utf-8'); T0 = time.time()
def P(*a):
    s = ' '.join(str(x) for x in a)
    print(s, flush=True)
    if LOGF: LOGF.write(s + '\n'); LOGF.flush()
def sh(x, k):
    o = np.full(len(x), np.nan, np.float32); o[k:] = x[:-k]; return o
def rmean(x, w):
    cs = np.cumsum(np.insert(x.astype(np.float64), 0, 0.0))
    o = np.full(len(x), np.nan, np.float32); o[w - 1:] = ((cs[w:] - cs[:-w]) / w).astype(np.float32); return o
def rsum(x, w):
    cs = np.cumsum(np.insert(x.astype(np.float64), 0, 0.0))
    o = np.full(len(x), np.nan, np.float32); o[w - 1:] = (cs[w:] - cs[:-w]).astype(np.float32); return o
def rmax(x, w):
    o = np.full(len(x), np.nan, np.float32); o[w - 1:] = swv(np.ascontiguousarray(x), w).max(axis=1); return o
def rmin(x, w):
    o = np.full(len(x), np.nan, np.float32); o[w - 1:] = swv(np.ascontiguousarray(x), w).min(axis=1); return o
RT_DEF = 0.0020
def build(tag, step=3, cap=864, v2=False):
    Z = np.load(os.path.join(BT, 'klines_5m_%s.npz' % tag))
    META = json.load(open(os.path.join(BT, 'meta_%s.json' % tag)))
    FV = {k: int(v) for k, v in META.get('first_valid', {}).items()}
    times = Z['times']; N = len(times)
    coins = sorted([k for k in Z.files if k != 'times'])
    hrs = (((times.astype(np.int64) // 1000) // 3600) % 24).astype(np.float32)
    HS = np.sin(2 * np.pi * hrs / 24.0).astype(np.float32); HC = np.cos(2 * np.pi * hrs / 24.0).astype(np.float32)
    R1H = np.full((len(coins), N), np.nan, np.float32)
    R24 = np.full((len(coins), N), np.nan, np.float32)
    for ci, c in enumerate(coins):
        cl = Z[c][:, 3].astype(np.float32)
        s = FV.get(c, 0); cl2 = cl.copy(); cl2[:s] = np.nan
        R1H[ci] = cl2 / sh(cl2, 12) - 1
        R24[ci] = cl2 / sh(cl2, 288) - 1
    MED1H = np.nanmedian(R1H, axis=0); MED24 = np.nanmedian(R24, axis=0)
    B1 = np.where(np.isnan(R1H), np.nan, (R1H > 0).astype(np.float32))
    BREADTH1H = np.nanmean(B1, axis=0)
    B2 = np.where(np.isnan(R24), np.nan, (R24 > 0).astype(np.float32))
    BREADTH24 = np.nanmean(B2, axis=0)
    del B1, B2, R1H, R24
    btc = Z['BTC'][:, 3].astype(np.float32)
    BTC15 = btc / sh(btc, 3) - 1; BTC1H = btc / sh(btc, 12) - 1
    BTC7D = btc / sh(btc, 2016) - 1
    NF = 29 if v2 else 26
    ARR = {}; Xs = []; Ws = []; Fz = []; Is = []; Cs = []; As = []
    warm = 2100 if v2 else 620
    return _build2(Z, FV, N, coins, HS, HC, MED1H, MED24, BREADTH1H, BREADTH24,
                   BTC15, BTC1H, BTC7D, NF, ARR, Xs, Ws, Fz, Is, Cs, As, warm, step, cap, v2, tag)
def feats_coin(a, s, N, NF, MED1H, MED24, BREADTH1H, BREADTH24, BTC15, BTC1H, BTC7D, HS, HC, v2):
    op, hi, lo, cl, vo, tb = a[:, 0], a[:, 1], a[:, 2], a[:, 3], a[:, 4], a[:, 5]
    pc = sh(cl, 1)
    tr = np.maximum(hi - lo, np.maximum(np.abs(hi - pc), np.abs(lo - pc)))
    tr = np.where(np.isfinite(tr), tr, hi - lo)
    atr = rmean(tr, 14)
    v288 = rmean(vo, 288)
    tbsh = np.where(vo > 0, tb / np.maximum(vo, 1e-12), np.nan)
    tb1h = rsum(tb, 12) / np.maximum(rsum(vo, 12), 1e-12)
    tbsh288 = rmean(np.nan_to_num(tbsh, nan=0.5), 288)
    mx12 = rmax(hi, 12); mn12 = rmin(lo, 12)
    mx288 = rmax(hi, 288); mn288 = rmin(lo, 288)
    lo6 = rmin(lo, 6)
    F = np.empty((N, NF), np.float32)
    F[:, 0] = cl / sh(cl, 1) - 1
    F[:, 1] = cl / sh(cl, 3) - 1
    F[:, 2] = cl / sh(cl, 12) - 1
    F[:, 3] = cl / sh(cl, 48) - 1
    F[:, 4] = cl / sh(cl, 288) - 1
    F[:, 5] = F[:, 1] - (sh(cl, 3) / sh(cl, 6) - 1)
    F[:, 6] = (cl - mx12) / np.maximum(mx12 - mn12, 1e-9)
    F[:, 7] = (lo6 - sh(lo6, 6)) / np.maximum(atr, 1e-9)
    F[:, 8] = vo / np.maximum(v288, 1e-9)
    F[:, 9] = rsum(vo, 3) / np.maximum(3 * v288, 1e-9)
    F[:, 10] = tbsh
    F[:, 11] = tb1h
    F[:, 12] = tb1h - tbsh288
    F[:, 13] = np.log10(np.maximum(v288 * cl, 1e-9))
    F[:, 14] = atr / np.maximum(cl, 1e-9)
    F[:, 15] = atr / np.maximum(sh(atr, 288), 1e-9)
    F[:, 16] = (hi - lo) / np.maximum(atr, 1e-9)
    F[:, 17] = cl / np.maximum(mx288, 1e-9) - 1
    F[:, 18] = (cl - mn288) / np.maximum(mx288 - mn288, 1e-9)
    F[:, 19] = F[:, 2] - MED1H
    F[:, 20] = F[:, 4] - MED24
    F[:, 21] = BREADTH1H
    F[:, 22] = BTC15
    F[:, 23] = BTC1H
    F[:, 24] = HS
    F[:, 25] = HC
    if v2:
        F[:, 26] = cl / sh(cl, 2016) - 1
        F[:, 27] = BTC7D
        F[:, 28] = BREADTH24
    return F, atr
def _build2(Z, FV, N, coins, HS, HC, MED1H, MED24, BREADTH1H, BREADTH24, BTC15, BTC1H, BTC7D,
            NF, ARR, Xs, Ws, Fz, Is, Cs, As, warm, step, cap, v2, tag):
    for ci, c in enumerate(coins):
        a = Z[c].astype(np.float32)
        op, hi, lo, cl = a[:, 0], a[:, 1], a[:, 2], a[:, 3]
        ARR[ci] = (op, hi, lo, cl)
        s = FV.get(c, 0); t0 = s + warm
        F, atr = feats_coin(a, s, N, NF, MED1H, MED24, BREADTH1H, BREADTH24, BTC15, BTC1H, BTC7D, HS, HC, v2)
        idx = np.arange(t0, N - cap - 2, step)
        if len(idx) < 300: continue
        entry = cl[idx]
        ok = np.isfinite(entry) & (entry > 0) & np.isfinite(atr[idx])
        idx = idx[ok]; entry = entry[ok]
        if len(idx) < 300: continue
        ne = len(idx)
        atrp_e = np.clip(atr[idx] / entry, 1e-4, 1.0)
        Rr = np.clip(1.2 * atrp_e, 0.005, 0.025)
        tp = entry * (1 + 1.6 * Rr); tpret = tp / entry - 1 - RT_DEF
        bearm = 1.2 * Rr; bepx = entry * (1 + RT_DEF)
        peak = entry.copy()
        armed = np.zeros(ne, bool); befl = np.zeros(ne, bool); done = np.zeros(ne, bool)
        ret = np.zeros(ne, np.float32); bars = np.full(ne, cap, np.int32)
        for k in range(1, cap + 1):
            j = idx + k
            h = hi[j]; l = lo[j]; px = cl[j]
            act = ~done
            peak = np.where(act & (px > peak), px, peak)
            gain = peak / entry - 1
            armed |= act & ((gain >= 0.8 * Rr) | (gain >= 0.015))
            befl |= act & ((h / entry - 1) >= bearm)
            tr_ = np.maximum(peak * (1 - atrp_e), entry * (1 + gain * 0.5))
            net_rt = tr_ / entry - 1 - RT_DEF
            d1 = act & (h >= tp)
            ret[d1] = tpret[d1]; bars[d1] = k; done |= d1
            d2 = ~done & act & armed & (l <= tr_) & (net_rt >= 0.0009)
            ret[d2] = net_rt[d2]; bars[d2] = k; done |= d2
            d3 = ~done & act & befl & (l <= bepx)
            ret[d3] = 0.0; bars[d3] = k; done |= d3
            if (k % 96) == 0 and done.all(): break
        opn = ~done
        WIN4 = done & (bars <= 48) & (ret >= 0.0009)
        FRZ = opn & (cl[idx + cap] < entry)
        Xs.append(F[idx]); Ws.append(WIN4); Fz.append(FRZ)
        Is.append(idx.astype(np.int32)); Cs.append(np.full(ne, ci, np.int16)); As.append(atrp_e)
    X = np.nan_to_num(np.clip(np.concatenate(Xs), -1e6, 1e6), nan=0.0)
    out = dict(X=X, W4=np.concatenate(Ws), FZ=np.concatenate(Fz), IDX=np.concatenate(Is),
               CIN=np.concatenate(Cs), ATRP=np.concatenate(As), ARR=ARR, N=N, coins=coins,
               MED1H=MED1H, MED24=MED24, B1H=BREADTH1H, B24=BREADTH24, BTC15=BTC15, BTC1H=BTC1H,
               BTC7D=BTC7D, HS=HS, HC=HC, FV=FV, Z=Z, NF=NF, v2=v2)
    P('[%s%s] built: events=%d WIN4H=%.1f%% FRZ=%.2f%% %.0fs' % (tag, ' v2' if v2 else '',
      len(out['W4']), 100 * out['W4'].mean(), 100 * out['FZ'].mean(), time.time() - T0))
    return out
def mlp_train(Xt, y, seed=7, H=16, ep=3, bs=8192, lr=0.01):
    rs = np.random.RandomState(seed)
    mu = Xt.mean(0); sd = Xt.std(0) + 1e-9
    Xn = np.clip((Xt - mu) / sd, -6, 6).astype(np.float32)
    n, f = Xn.shape
    W1 = (rs.randn(f, H) * 0.15).astype(np.float32); b1 = np.zeros(H, np.float32)
    W2 = (rs.randn(H) * 0.15).astype(np.float32); b2 = 0.0
    mW1 = np.zeros_like(W1); vW1 = np.zeros_like(W1); mb1 = np.zeros_like(b1); vb1 = np.zeros_like(b1)
    mW2 = np.zeros_like(W2); vW2 = np.zeros_like(W2); mb2 = 0.0; vb2 = 0.0
    t = 0; yf = y.astype(np.float32)
    for e in range(ep):
        pi = rs.permutation(n)
        for s0 in range(0, n, bs):
            bi = pi[s0:s0 + bs]
            xb = Xn[bi]; yb = yf[bi]
            h1 = np.tanh(xb @ W1 + b1)
            z = np.clip(h1 @ W2 + b2, -30, 30)
            p = 1.0 / (1.0 + np.exp(-z))
            g = (p - yb) / len(bi)
            gW2 = h1.T @ g; gb2 = float(g.sum())
            gh = np.outer(g, W2) * (1 - h1 * h1)
            gW1 = xb.T @ gh; gb1 = gh.sum(0)
            t += 1; c1 = 1 - 0.9 ** t; c2 = 1 - 0.999 ** t
            for Pp, Gg, Mm, Vv in ((W1, gW1, mW1, vW1), (b1, gb1, mb1, vb1), (W2, gW2, mW2, vW2)):
                Mm *= 0.9; Mm += 0.1 * Gg; Vv *= 0.999; Vv += 0.001 * Gg * Gg
                Pp -= lr * (Mm / c1) / (np.sqrt(Vv / c2) + 1e-8)
            mb2 = 0.9 * mb2 + 0.1 * gb2; vb2 = 0.999 * vb2 + 0.001 * gb2 * gb2
            b2 -= lr * (mb2 / c1) / (np.sqrt(vb2 / c2) + 1e-8)
    return dict(mu=mu, sd=sd, W1=W1, b1=b1, W2=W2, b2=b2)
def mlp_prob(M, Xt):
    Xn = np.clip((Xt - M['mu']) / M['sd'], -6, 6)
    h1 = np.tanh(Xn @ M['W1'] + M['b1'])
    z = np.clip(h1 @ M['W2'] + M['b2'], -30, 30)
    return 1.0 / (1.0 + np.exp(-z))
def platt_fit(p, y, iters=400, lr=0.5):
    pc = np.clip(p.astype(np.float64), 1e-6, 1 - 1e-6)
    z = np.log(pc / (1 - pc)); yf = y.astype(np.float64)
    a, b = 1.0, 0.0
    for _ in range(iters):
        q = 1.0 / (1.0 + np.exp(-(a * z + b))); g = q - yf
        a -= lr * float((g * z).mean()); b -= lr * float(g.mean())
    return (a, b)
def platt_apply(ab, p):
    pc = np.clip(p.astype(np.float64), 1e-6, 1 - 1e-6)
    z = np.log(pc / (1 - pc))
    return 1.0 / (1.0 + np.exp(-(ab[0] * z + ab[1])))
def loqo_scores(Y, seeds=(7, 11), timeblock_val=False):
    n = len(Y['W4']); QL = Y['N'] // 4
    PW = np.zeros(n); PZ = np.zeros(n)
    rs = np.random.RandomState(99); vp_rand = rs.rand(n) < 0.15
    for q in range(4):
        qs = q * QL; qe = (q + 1) * QL if q < 3 else Y['N']
        trm = (Y['IDX'] < (qs - 864)) | (Y['IDX'] >= (qe + 864))
        if timeblock_val:
            tri = Y['IDX'][trm]
            cut = np.percentile(tri, 85)
            vp = trm & (Y['IDX'] >= cut) & (Y['IDX'] < qs - 864) if qs > 0 else trm & (Y['IDX'] >= cut)
            if vp.sum() < 5000: vp = trm & vp_rand
        else:
            vp = trm & vp_rand
        core = trm & ~vp
        te = (Y['IDX'] >= qs) & (Y['IDX'] < qe)
        mW = mlp_train(Y['X'][core], Y['W4'][core], seed=seeds[0])
        mT = mlp_train(Y['X'][core], Y['FZ'][core], seed=seeds[1])
        abW = platt_fit(mlp_prob(mW, Y['X'][vp]), Y['W4'][vp])
        abT = platt_fit(mlp_prob(mT, Y['X'][vp]), Y['FZ'][vp])
        PW[te] = platt_apply(abW, mlp_prob(mW, Y['X'][te])) * 100
        PZ[te] = platt_apply(abT, mlp_prob(mT, Y['X'][te])) * 100
    return PW, PZ
CACHE = {}
def simdeep(Y, ci, i, atrp, entry_mode='close', slip=0.0, rt=RT_DEF):
    key = (id(Y), ci, i, entry_mode, slip, rt)
    if key in CACHE: return CACHE[key]
    op, hi, lo, cl = Y['ARR'][ci]; Nn = Y['N']
    if entry_mode == 'nextopen':
        entry = op[i + 1] * (1.0 + slip)
        if not np.isfinite(entry) or entry <= 0: entry = cl[i] * (1.0 + slip)
    else:
        entry = cl[i] * (1.0 + slip)
    R = min(max(1.2 * atrp, 0.005), 0.025)
    tp = entry * (1 + 1.6 * R); bearm = 1.2 * R; bepx = entry * (1 + rt)
    peak = entry; armed = False; be = False
    end = min(i + 8640, Nn - 1); out = None
    for j in range(i + 1, end):
        h = hi[j]; l = lo[j]; px = cl[j]
        if px > peak: peak = px
        gain = peak / entry - 1
        if not armed and (gain >= 0.8 * R or gain >= 0.015): armed = True
        if not be and (h / entry - 1) >= bearm: be = True
        if h >= tp: out = (tp / entry - 1 - rt, j - i); break
        if armed:
            tr_ = max(peak * (1 - atrp), entry * (1 + gain * 0.5))
            if l <= tr_:
                net = tr_ / entry - 1 - rt
                if net >= 0.0009: out = (net, j - i); break
        if be and l <= bepx: out = (0.0, j - i); break
    if out is None: out = (cl[end] / entry - 1 - rt, end - i)
    CACHE[key] = out
    return out
def port(Y, PW, PZ, thr, zc, q, entry_mode='close', slip=0.0, rt=RT_DEF, step=6, exactbar=False,
         rank='pw', lam=3.0, excl=None, slots=8, ticket=11.0, maxnew=2, cooldown=36, collect=False,
         oldcap=None):
    Nn = Y['N']; QL = Nn // 4
    qs = q * QL; qe = (q + 1) * QL if q < 3 else Nn
    m = (PW >= thr) & (PZ <= zc) & (Y['IDX'] >= qs) & (Y['IDX'] < qe)
    ei = Y['IDX'][m]; ec = Y['CIN'][m]; ew = PW[m]; ez = PZ[m]; ea = Y['ATRP'][m]
    o = np.argsort(ei, kind='stable')
    ei, ec, ew, ez, ea = ei[o], ec[o], ew[o], ez[o], ea[o]
    if rank == 'ev': score = ew - lam * ez
    else: score = ew
    ptr = 0; busy = {}; cool = {}
    pnl = 0.0; trades = 0; wins = 0; frz = 0; hold = 0
    recent = []; tl = []
    excl = excl or set()
    for t in range(qs, qe, step):
        for cn in [cn for cn, fb in busy.items() if fb[0] <= t]: del busy[cn]
        cands = {}
        while ptr < len(ei) and ei[ptr] <= t:
            fresh = (ei[ptr] == t) if exactbar else (ei[ptr] > t - step)
            if fresh:
                cn = int(ec[ptr])
                if cn not in excl and cn not in busy and cool.get(cn, -1) < t:
                    if cn not in cands or score[ptr] > score[cands[cn]]: cands[cn] = ptr
            ptr += 1
        if not cands: continue
        recent = [b for b in recent if b > t - 6]
        if oldcap is not None and sum(1 for fb in busy.values() if t - fb[1] > 288) >= oldcap: continue
        room = min(maxnew - len(recent) if exactbar else maxnew, slots - len(busy))
        if room <= 0: continue
        take = sorted(cands.values(), key=lambda p: -score[p])[:room]
        for p in take:
            cn = int(ec[p]); i = int(ei[p])
            net, b = simdeep(Y, cn, i, float(ea[p]), entry_mode, slip, rt)
            pnl += net * ticket; trades += 1; hold += b
            if net >= 0.0009: wins += 1
            if (b >= 8639 or (i + b) >= Nn - 2) and net < 0: frz += 1
            busy[cn] = (i + b, t); cool[cn] = i + b + cooldown
            recent.append(t)
            if collect: tl.append((i, cn, net, b))
    r = dict(pnl=pnl, tr=trades, w=wins, frz=frz, hold=hold / max(trades, 1))
    if collect: r['tl'] = tl
    return r
