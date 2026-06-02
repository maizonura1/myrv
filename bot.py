"""
Bot Scalping v20 — ASYMMETRIC RR MODE (THE TRUE FLIP)
====================================================
- STOP SPAMMING: Kurangi frekuensi trade, hindari bakar duit di Fee Binance.
- LOGIKA RR DITUKAR: Dulu SL jauh TP dekat (sering menang tapi minus). 
  Sekarang SL ketat (0.3%), TP Jauh (Minimal 0.9%). 1x Win = 3x Loss.
- NO PANIC CUT: Hapus aturan "5 detik nggak profit tebas". Beri ruang market bergerak.
- FEE-AWARE: Trailing baru aktif kalau profit sudah tembus 0.3% (aman dari fee taker).
"""

import os, time, math, threading, queue
import requests
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from binance.client import Client
import ta
import pandas as pd
import numpy as np

load_dotenv()
client = Client(os.getenv("API_KEY"), os.getenv("API_SECRET"))
client.FUTURES_URL = "https://testnet.binancefuture.com/fapi"

# ═══════════════════════════════════════════════════════
#  CONFIG v20 - ASYMMETRIC RR MODE
# ═══════════════════════════════════════════════════════
LEVERAGE       = 20
ORDER_USDT     = 2.0    
MAX_POSITIONS  = 3      

# ── LOGIKA RR YANG DITUKAR (PROFIT KENCENG, LOSS DITAHAN) ──
# Stop loss dibuat masuk akal (0.7x ATR), Take profit dibuat jauh (2.0x ATR)
ATR_IC_MULT    = 0.7    
ATR_TP1_MULT   = 2.0    
ATR_TP2_MULT   = 4.0    
ATR_TRAIL_MULT = 0.5   

# Hard cap batas mutlak (Dalam Persen)
MAX_IC_PCT     = 0.003  # Maksimal Loss 0.3% (Cukup untuk cover noise)
MAX_TP1_PCT    = 0.009  # Minimal Profit 0.9% (1x menang nutup 3x loss)
MAX_TP2_PCT    = 0.020  

# Trail activation: JANGAN aktif sebelum fee tertutup!
TRAIL_ACTIVATE_MULT = 0.30 

TP1_RATIO      = 0.60
BTC_FILTER     = True
ADX_MIN        = 25     # Wajib ada tren jelas

# ── FILTER ENTRY KETAT ────────────────────────────────
MIN_BASE_VOL   = 30_000_000 
MIN_VR         = 1.5    # Momentum harus gede
BR_LONG_MIN    = 0.55   # Buyer harus beneran dominan
BR_SHORT_MAX   = 0.45   # Seller harus beneran dominan

SCAN_INTERVAL  = 1.5
MONITOR_INT    = 0.5
SCAN_DELAY     = 0.02
BATCH_SIZE     = 15
MAX_WORKERS    = 8
MAX_HOLD_SEC   = 420    # Beri waktu 7 menit untuk market bergerak

MIN_SCORE      = 60     # Skor super ketat, biar nggak spam
MIN_GAP        = 20     
COOLDOWN_SEC   = 10     

DAILY_LOSS     = -10.0
CONSEC_MAX     = 8
CONSEC_PAUSE   = 60

# ═══════════════════════════════════════════════════════
#  SYMBOLS
# ═══════════════════════════════════════════════════════
SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "TRXUSDT", "DOTUSDT",
    "LINKUSDT", "MATICUSDT", "LTCUSDT", "ATOMUSDT", "UNIUSDT",
    "NEARUSDT", "APTUSDT", "ARBUSDT", "OPUSDT", "INJUSDT",
    "SUIUSDT", "SEIUSDT", "FETUSDT", "WLDUSDT", "AAVEUSDT",
    "ORDIUSDT", "TONUSDT", "1000PEPEUSDT", "WIFUSDT", "JUPUSDT",
]
SYMBOLS = list(dict.fromkeys(SYMBOLS))

# ═══════════════════════════════════════════════════════
#  STATE
# ═══════════════════════════════════════════════════════
paper_positions = {}
trade_log       = []
_ohlcv_cache    = {}
_sym_cooldown   = {}
_ticker_cache   = {}
_ticker_ts      = 0
_lock           = threading.Lock()
_executor       = ThreadPoolExecutor(max_workers=MAX_WORKERS)
_rescan_q       = queue.Queue()
_hot_syms       = deque(maxlen=20)

_macro = {"fng": 50, "btc": "UNKNOWN", "last_fng": 0, "last_btc": 0}
_ks    = {"active": False, "reason": "", "resume": 0, "consec": 0, "daily": 0.0, "day_reset": 0}
_stats = {"trades": 0, "wins": 0, "losses": 0, "pnl": 0.0, "best": 0.0, "worst": 0.0, "tp1": 0, "tp2": 0, "sl": 0, "cut": 0, "guard": 0, "force": 0, "hist": deque(maxlen=200), "start": time.time()}

# ═══════════════════════════════════════════════════════
#  UTILS
# ═══════════════════════════════════════════════════════
def qty(price): return (ORDER_USDT * LEVERAGE) / price

def price_live(symbol):
    try: return float(client.futures_symbol_ticker(symbol=symbol)["price"])
    except: return 0.0

def tickers_all():
    global _ticker_cache, _ticker_ts
    now = time.time()
    if now - _ticker_ts < 5 and _ticker_cache: return _ticker_cache
    try:
        raw = client.futures_ticker()
        new = {t["symbol"]: {"pct": float(t["priceChangePercent"]), "vol": float(t["quoteVolume"]), "last": float(t["lastPrice"])} for t in raw}
        _ticker_cache = new; _ticker_ts = now; return new
    except: return _ticker_cache

def ok_cooldown(sym): return (time.time() - _sym_cooldown.get(sym, 0)) >= COOLDOWN_SEC
def set_cd(sym): _sym_cooldown[sym] = time.time()

# ═══════════════════════════════════════════════════════
#  OHLCV & TA
# ═══════════════════════════════════════════════════════
def ohlcv(symbol, interval, limit=100):
    key = (symbol, interval); now = time.time()
    if key in _ohlcv_cache and now - _ohlcv_cache[key][0] < 5: return _ohlcv_cache[key][1]
    try:
        kl = client.futures_klines(symbol=symbol, interval=interval, limit=limit)
        df = pd.DataFrame(kl, columns=["time","open","high","low","close","volume","ct","qv","trades","tbbase","tbquote","ignore"])
        for c in ["open","high","low","close","volume","tbbase","tbquote"]: df[c] = df[c].astype(float)
        df["time"] = pd.to_numeric(df["time"])
        _ohlcv_cache[key] = (now, df)
        return df
    except: return _ohlcv_cache.get(key, (None, None))[1]

def run_ta(df):
    c, h, l, v = df["close"], df["high"], df["low"], df["volume"]
    df["rsi"]  = ta.momentum.RSIIndicator(c, 14).rsi()
    df["mh"]   = ta.trend.MACD(c, 12, 26, 9).macd_diff()
    df["e5"]   = ta.trend.EMAIndicator(c, 5).ema_indicator()
    df["e9"]   = ta.trend.EMAIndicator(c, 9).ema_indicator()
    df["e21"]  = ta.trend.EMAIndicator(c, 21).ema_indicator()
    df["e50"]  = ta.trend.EMAIndicator(c, 50).ema_indicator()
    df["atr"]  = ta.volatility.AverageTrueRange(h, l, c, 14).average_true_range()
    df["adx"]  = ta.trend.ADXIndicator(h, l, c, 14).adx()
    df["vm"]   = v.rolling(20).mean()
    df["vr"]   = v / df["vm"].replace(0, 1)
    df["br"]   = df["tbbase"] / df["volume"].replace(0, 1)
    df["body"] = abs(c - df["open"])
    df["rng"]  = h - l
    df["br2"]  = df["body"] / df["rng"].replace(0, 1)
    df["m5"]   = (c - c.shift(5)) / c.shift(5)
    return df

def btc_trend():
    try:
        df = run_ta(ohlcv("BTCUSDT", Client.KLINE_INTERVAL_5MINUTE, 80).copy())
        if df is None or len(df) < 30: return "UNKNOWN"
        row = df.iloc[-2]; p, e5, e9, e21, m5 = row["close"], row["e5"], row["e9"], row["e21"], row["m5"]
        if p > e5 > e9 > e21 and m5 > 0.001: return "BULL"
        if p < e5 < e9 < e21 and m5 < -0.001: return "BEAR"
        if p > e9 > e21: return "MILD_BULL"
        if p < e9 < e21: return "MILD_BEAR"
        return "SIDEWAYS"
    except: return "UNKNOWN"

def ks_check():
    k, now = _ks, time.time()
    if k["active"] and now >= k["resume"]: k["active"] = False; k["consec"] = 0
    if k["active"]: return True, k["reason"]
    day = now - (now % 86400)
    if day > k["day_reset"]: k["daily"] = 0.0; k["day_reset"] = day
    if k["daily"] <= DAILY_LOSS: k["active"] = True; k["reason"] = f"daily({k['daily']:.2f})"; k["resume"] = day + 86400; return True, k["reason"]
    if k["consec"] >= CONSEC_MAX: k["active"] = True; k["reason"] = f"consec({k['consec']})"; k["resume"] = now + CONSEC_PAUSE; return True, k["reason"]
    return False, ""

def ks_upd(pnl):
    _ks["daily"] += pnl; _ks["consec"] = 0 if pnl >= 0 else _ks["consec"] + 1

def calc_levels(direction, entry, atr):
    if direction == "LONG":
        ic      = entry - min(atr * ATR_IC_MULT,   entry * MAX_IC_PCT)
        tp1     = entry + min(atr * ATR_TP1_MULT,  entry * MAX_TP1_PCT)
        tp2     = entry + min(atr * ATR_TP2_MULT,  entry * MAX_TP2_PCT)
        hard_sl = entry - min(atr * ATR_IC_MULT * 1.2, entry * MAX_IC_PCT * 1.2)
        trail0  = entry - min(atr * ATR_TRAIL_MULT, entry * MAX_IC_PCT)
        trail_a = entry + atr * TRAIL_ACTIVATE_MULT  
    else:
        ic      = entry + min(atr * ATR_IC_MULT,   entry * MAX_IC_PCT)
        tp1     = entry - min(atr * ATR_TP1_MULT,  entry * MAX_TP1_PCT)
        tp2     = entry - min(atr * ATR_TP2_MULT,  entry * MAX_TP2_PCT)
        hard_sl = entry + min(atr * ATR_IC_MULT * 1.2, entry * MAX_IC_PCT * 1.2)
        trail0  = entry + min(atr * ATR_TRAIL_MULT, entry * MAX_IC_PCT)
        trail_a = entry - atr * TRAIL_ACTIVATE_MULT
    ic_pct  = abs(entry - ic) / entry * 100
    tp1_pct = abs(tp1 - entry) / entry * 100
    return {"ic": ic, "tp1": tp1, "tp2": tp2, "hard_sl": hard_sl, "trail0": trail0, "trail_act": trail_a, "atr": atr, "ic_pct": ic_pct, "tp1_pct": tp1_pct, "rr": tp1_pct / ic_pct if ic_pct > 0 else 0}

# ═══════════════════════════════════════════════════════
#  SIGNAL ENGINE 
# ═══════════════════════════════════════════════════════
def signal(df):
    if df is None or len(df) < 55: return None, 0, [], 0.0

    row, prev, prev2 = df.iloc[-2], df.iloc[-3], df.iloc[-4]
    p, e5, e9, e21, e50 = row["close"], row["e5"], row["e9"], row["e21"], row["e50"]
    rsi, mh, mh_p, mh_p2 = row["rsi"], row["mh"], prev["mh"], prev2["mh"]
    vr, br, m5, body, atr, adx, btc = row["vr"], row["br"], row["m5"], row["br2"], row["atr"], row["adx"], _macro["btc"]

    if btc in ["SIDEWAYS", "UNKNOWN"]: return None, 0, [], atr
    if adx < ADX_MIN or vr < MIN_VR: return None, 0, [], atr

    lp = sp = 0
    sl, ss = [], []

    if p > e5 > e9 > e21 > e50:   lp += 30; sl.append("EMA_stack↑")
    elif p > e5 > e9 > e21:        lp += 22; sl.append("EMA↑↑")
    if p < e5 < e9 < e21 < e50:   sp += 30; ss.append("EMA_stack↓")
    elif p < e5 < e9 < e21:        sp += 22; ss.append("EMA↓↓")

    if m5 > 0.005:    lp += 25; sl.append(f"Mom+{m5*100:.1f}%")
    if m5 < -0.005:   sp += 25; ss.append(f"Mom{m5*100:.1f}%")

    if mh_p <= 0 and mh > 0:           lp += 22; sl.append("MACD_X↑")
    if mh_p >= 0 and mh < 0:           sp += 22; ss.append("MACD_X↓")

    if vr >= 3.0: lp += 15; sp += 15; sl.append(f"Vol{vr:.1f}x"); ss.append(f"Vol{vr:.1f}x")

    if br > 0.65:    lp += 18; sl.append(f"Buy{br:.0%}")
    if br < 0.35:    sp += 18; ss.append(f"Sell{1-br:.0%}")

    if btc == "BULL": lp += 20; sp = 0
    elif btc == "BEAR": sp += 20; lp = 0

    gap = abs(lp - sp)

    if lp >= MIN_SCORE and gap >= MIN_GAP and br >= BR_LONG_MIN: 
        return "LONG", lp, sl[:3], atr
    
    if sp >= MIN_SCORE and gap >= MIN_GAP and br <= BR_SHORT_MAX: 
        return "SHORT", sp, ss[:3], atr
        
    return None, 0, [], atr 

# ═══════════════════════════════════════════════════════
#  PAPER OPEN / CLOSE
# ═══════════════════════════════════════════════════════
def paper_open(sym, direction, score, sigs, price, atr):
    with _lock:
        if sym in paper_positions or len(paper_positions) >= MAX_POSITIONS: return
        paper_positions[sym] = {"_r": True}
    q = qty(price); lvl = calc_levels(direction, price, atr)
    pos = {"side": direction, "entry": price, "qty": q, "qty_rem": q, "ic": lvl["ic"], "hard_sl": lvl["hard_sl"], "tp1": lvl["tp1"], "tp2": lvl["tp2"], "trail_sl": lvl["trail0"], "trail_act": lvl["trail_act"], "peak": price, "trail_on": False, "tp1_hit": False, "be_on": False, "open_time": time.time(), "score": score, "sigs": sigs, "atr": atr}
    with _lock: paper_positions[sym] = pos
    print(f"\n  {'🟢' if direction == 'LONG' else '🔴'} [PAPER] {sym} {direction} @{price:.6g}")
    print(f"     SL:{lvl['ic']:.5g} | TP1:{lvl['tp1']:.5g} | R:R 1:{lvl['rr']:.1f} | Sigs:{' | '.join(sigs)}")
    _stats["trades"] += 1

def paper_close(sym, reason, price=None):
    with _lock: pos = paper_positions.pop(sym, None)
    if pos is None or pos.get("_r"): return
    if price is None: price = price_live(sym)
    side, entry = pos["side"], pos["entry"]
    qr = pos.get("qty_rem", pos["qty"])
    pnl = (price - entry) * qr if side == "LONG" else (entry - price) * qr
    pct = (price - entry) / entry * 100 if side == "LONG" else (entry - price) / entry * 100
    hold = time.time() - pos["open_time"]
    
    print(f"  {'🟢' if pnl >= 0 else '🔴'} [PAPER] {sym} {side} CLOSE — {reason}")
    print(f"     {entry:.6g}→{price:.6g} ({pct:+.3f}%) hold:{hold:.0f}s | PnL:{pnl:+.5f}U")
    
    _stats["pnl"] += pnl; _stats["hist"].append(pnl); ks_upd(pnl)
    if pnl >= 0: _stats["wins"] += 1; _stats["best"] = max(_stats["best"], pnl)
    else: _stats["losses"] += 1; _stats["worst"] = min(_stats["worst"], pnl)
    
    if "TP" in reason: _stats["tp1"] += 1
    if "SL_Cut" in reason: _stats["sl"] += 1
    
    trade_log.append({"sym": sym, "side": side, "entry": round(entry, 7), "exit": round(price, 7), "pnl": round(pnl, 5), "reason": reason, "hold": int(hold)})
    set_cd(sym); _hot_syms.appendleft(sym); _rescan_q.put(1); print_inline()

def paper_tp1(sym, price):
    pos = paper_positions.get(sym)
    if pos is None or pos.get("tp1_hit") or pos.get("_r"): return
    side, entry, atr = pos["side"], pos["entry"], pos["atr"]
    cq = pos["qty"] * TP1_RATIO
    pnl = (price - entry) * cq if side == "LONG" else (entry - price) * cq
    hold = time.time() - pos["open_time"]
    print(f"  🎯 [PAPER] {sym} TP1 @{price:.6g} hold:{hold:.0f}s | PnL:{pnl:+.5f}U")
    
    pos["tp1_hit"] = True; pos["qty_rem"] = pos["qty"] * (1 - TP1_RATIO); pos["be_on"] = True
    if side == "LONG": pos["hard_sl"] = entry * 1.0010; pos["trail_sl"] = price - atr * ATR_TRAIL_MULT * 0.7
    else: pos["hard_sl"] = entry * 0.9990; pos["trail_sl"] = price + atr * ATR_TRAIL_MULT * 0.7
    pos["peak"] = price; pos["trail_on"] = True
    
    _stats["pnl"] += pnl; _stats["hist"].append(pnl); _stats["wins"] += 1; _stats["tp1"] += 1; ks_upd(pnl)
    _stats["best"] = max(_stats["best"], pnl)
    trade_log.append({"sym": sym, "side": side, "entry": round(entry, 7), "exit": round(price, 7), "pnl": round(pnl, 5), "reason": "TP1", "hold": int(hold)})
    print_inline()

# ═══════════════════════════════════════════════════════
#  MONITOR - SABAR & HOLD LOGIC
# ═══════════════════════════════════════════════════════
def monitor_positions():
    for sym in list(paper_positions.keys()):
        pos = paper_positions.get(sym)
        if pos is None or pos.get("_r"): continue
        px = price_live(sym)
        if px == 0: continue

        side, entry, atr, hold = pos["side"], pos["entry"], pos["atr"], time.time() - pos["open_time"]

        if hold >= MAX_HOLD_SEC: paper_close(sym, "Timeout_Close", px); continue
        
        # 🚨 HAPUS IMPATIENT CUT 5 DETIK! Biarkan harga bernapas melawan spread.

        if side == "LONG":
            prof_pct = (px - entry) / entry

            if px <= pos["ic"]: paper_close(sym, "SL_Cut", px); continue
            if not pos["tp1_hit"] and px <= pos["hard_sl"]: paper_close(sym, "HardGuard", px); continue
            if not pos["tp1_hit"] and px >= pos["tp1"]: paper_tp1(sym, px); continue

            # Trail Act: Harus nembus margin yang aman dari fee dulu
            if px >= pos["trail_act"] and not pos["trail_on"]:
                pos["trail_on"] = True; pos["peak"] = px; pos["trail_sl"] = entry * 1.0015; pos["hard_sl"] = entry * 1.0010

            if pos["trail_on"] and px > pos["peak"]:
                pos["peak"] = px; new_t = px - atr * ATR_TRAIL_MULT; pos["trail_sl"] = max(pos["trail_sl"], new_t, entry * 1.0015)

            if pos["trail_on"] and px <= pos["trail_sl"]: paper_close(sym, "TrailStop", px); continue
            if pos["tp1_hit"] and px >= pos["tp2"]: paper_close(sym, "TP2", px); continue

            pnl_now = (px - entry) * pos.get("qty_rem", pos["qty"])
            print(f"  📌 {sym} L@{entry:.5g}→{px:.5g}({prof_pct*100:+.2f}%) {pnl_now:+.4f}U {hold:.0f}s")

        else:  # SHORT
            prof_pct = (entry - px) / entry

            if px >= pos["ic"]: paper_close(sym, "SL_Cut", px); continue
            if not pos["tp1_hit"] and px >= pos["hard_sl"]: paper_close(sym, "HardGuard", px); continue
            if not pos["tp1_hit"] and px <= pos["tp1"]: paper_tp1(sym, px); continue

            if px <= pos["trail_act"] and not pos["trail_on"]:
                pos["trail_on"] = True; pos["peak"] = px; pos["trail_sl"] = entry * 0.9985; pos["hard_sl"] = entry * 0.9990

            if pos["trail_on"] and px < pos["peak"]:
                pos["peak"] = px; new_t = px + atr * ATR_TRAIL_MULT; pos["trail_sl"] = min(pos["trail_sl"], new_t, entry * 0.9985)

            if pos["trail_on"] and px >= pos["trail_sl"]: paper_close(sym, "TrailStop", px); continue
            if pos["tp1_hit"] and px <= pos["tp2"]: paper_close(sym, "TP2", px); continue

            pnl_now = (entry - px) * pos.get("qty_rem", pos["qty"])
            print(f"  📌 {sym} S@{entry:.5g}→{px:.5g}({prof_pct*100:+.2f}%) {pnl_now:+.4f}U {hold:.0f}s")

# ═══════════════════════════════════════════════════════
#  SCANNER & RUNNER
# ═══════════════════════════════════════════════════════
def scan_one(sym):
    try:
        time.sleep(SCAN_DELAY)
        if not ok_cooldown(sym): return None
        if _ticker_cache.get(sym, {}).get("vol", 0) < MIN_BASE_VOL: return None

        df = run_ta(ohlcv(sym, Client.KLINE_INTERVAL_5MINUTE, 100).copy())
        px, atr = df["close"].iloc[-2], df["atr"].iloc[-2]
        if px == 0 or atr / px > 0.03: return None

        dir_, sc, sigs, atr_val = signal(df)
        if dir_:
            px_live = price_live(sym)
            if px_live > 0: return (sym, dir_, sc, sigs, px_live, atr_val)
    except: pass
    return None

def scan_batch(syms):
    res = []
    fut = {_executor.submit(scan_one, s): s for s in syms[:BATCH_SIZE]}
    for f in as_completed(fut, timeout=10):
        try: 
            if r := f.result(timeout=2): res.append(r)
        except: pass
    return res

def top_movers(syms, n=20):
    tk, ss = tickers_all(), set(syms)
    mv = [(s, abs(d["pct"])) for s, d in tk.items() if s in ss and d["vol"] >= MIN_BASE_VOL]
    mv.sort(key=lambda x: x[1], reverse=True)
    return [s for s, _ in mv[:n]]

def print_inline():
    n = _stats["wins"] + _stats["losses"]; wr = _stats["wins"] / n * 100 if n else 0
    print(f"     └ [v20 ASYMMETRIC] {n}T WR:{wr:.0f}% W:{_stats['wins']} L:{_stats['losses']} {'💚' if _stats['pnl']>=0 else '🔴'}PnL:{_stats['pnl']:+.4f}U")

def print_full():
    n = _stats["wins"] + _stats["losses"]; wr = _stats["wins"] / n * 100 if n else 0
    sess = (time.time() - _stats["start"]) / 3600; tph = n / sess if sess > 0 else 0
    print(f"\n  {'─'*62}\n  🧪 PAPER v20 [ASYMMETRIC RR] — {sess*60:.0f}m | {tph:.1f}T/jam")
    print(f"  🎯 {n}T WR:{wr:.0f}% W:{_stats['wins']} L:{_stats['losses']} | PnL:{_stats['pnl']:+.5f}U\n  {'─'*62}")

def t_monitor():
    while True:
        try:
            if paper_positions: monitor_positions()
        except: pass
        time.sleep(MONITOR_INT)

def t_rescan(syms):
    while True:
        try:
            _rescan_q.get(timeout=30); time.sleep(0.3)
            slots = MAX_POSITIONS - len(paper_positions)
            if slots <= 0 or ks_check()[0]: continue
            hot = [s for s in _hot_syms if s not in paper_positions]
            rest = [s for s in syms if s not in paper_positions and s not in hot]
            res = scan_batch((hot + rest)[:25])
            if res:
                res.sort(key=lambda x: x[2], reverse=True)
                for r in res[:slots]:
                    if len(paper_positions) >= MAX_POSITIONS: break
                    paper_open(*r)
        except: pass

def t_macro():
    while True:
        try: _macro["btc"] = btc_trend()
        except: pass
        time.sleep(5)

def run_bot():
    print("╔═══════════════════════════════════════════════════════╗")
    print("║  🧪 PAPER TRADE v20 — ASYMMETRIC RR MODE            ║")
    print("╠═══════════════════════════════════════════════════════╣")
    print("║  Menukar posisi RR: SL Ketat (0.3%), TP Jauh (0.9%).  ║")
    print("║  Menang 1x akan menutupi 3x loss. Market dibiarkan    ║")
    print("║  bernapas, STOP SPAMMING TRANSAKSI!                   ║")
    print("╚═══════════════════════════════════════════════════════╝")
    try:
        valid = {s["symbol"] for s in client.futures_exchange_info()["symbols"] if s["status"] == "TRADING"}
        syms = list(dict.fromkeys([s for s in SYMBOLS if s in valid]))
    except: syms = list(dict.fromkeys(SYMBOLS))

    threading.Thread(target=t_monitor, daemon=True).start()
    threading.Thread(target=t_rescan, args=(syms,), daemon=True).start()
    threading.Thread(target=t_macro, daemon=True).start()

    time.sleep(4); tickers_all()
    cycle = scan_idx = 0; n_bat = math.ceil(len(syms) / BATCH_SIZE)

    while True:
        cycle += 1; slots = MAX_POSITIONS - len(paper_positions)
        print(f"\n{'═'*57}\n  #{cycle} {time.strftime('%H:%M:%S')} BTC:{_macro['btc']} ({len(paper_positions)}/{MAX_POSITIONS}) PnL:{_stats['pnl']:+.4f}U")

        if (k := ks_check())[0]: print(f"  🚨 KS:{k[1]}"); time.sleep(SCAN_INTERVAL); continue

        if slots > 0:
            mv = [s for s in top_movers(syms, 20) if s not in paper_positions]
            bs = scan_idx * BATCH_SIZE
            reg = [s for s in syms[bs:bs+BATCH_SIZE] if s not in paper_positions and s not in mv]
            scan_idx = (scan_idx + 1) % n_bat
            scan_list = mv[:15] + reg[:10]

            print(f"  🔍 {len(scan_list)} syms | {slots} slot kosong")
            try: res = scan_batch(scan_list)
            except: res = []

            if res:
                res.sort(key=lambda x: x[2], reverse=True)
                for r in res[:slots]:
                    if len(paper_positions) >= MAX_POSITIONS: break
                    paper_open(*r)
            elif len(paper_positions) == 0:
                print("  ⚠️  Wide scan...")
                try: r2 = scan_batch([s for s in syms if s not in paper_positions])
                except: r2 = []
                if r2:
                    r2.sort(key=lambda x: x[2], reverse=True)
                    paper_open(*r2[0])
            else: print(f"  ⏳ {len(paper_positions)} pos aktif")
        else: print(f"  ✅ Full ({MAX_POSITIONS}/{MAX_POSITIONS})")

        if cycle % 20 == 0: print_full()
        time.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    run_bot()
