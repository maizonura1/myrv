"""
Bot Scalping v18 — SIGNAL QUALITY + HOLD TIME FIX
====================================================
ROOT CAUSE dari v17 yang masih loss:

MASALAH #1: SIGNAL KONTRADIKSI LOLOS
  Log: "EMA_stack↑ | Mom+2.5% | Sell91%"
  Bot masuk LONG padahal sell pressure 91% → harusnya REJECT
  Fix: Tambah hard veto — jika buy/sell pressure berlawanan arah kuat → skip

MASALAH #2: MAX_HOLD_SEC = 180s TERLALU PENDEK
  TP1 butuh +0.80% (ATR×1.2), tapi waktu max 3 menit
  Coin butuh 5-15 menit untuk gerak segitu di 5m chart
  Force close = pasti loss karena di-cut sebelum profit
  Fix: MAX_HOLD_SEC = 600s (10 menit)

MASALAH #3: ENTRY TERLALU BANYAK, KUALITAS RENDAH
  62 trade/jam = masuk hampir setiap scan
  Fix: MIN_SCORE dinaikkan ke 55, gap minimum 25
       Tambah konfirmasi multi-timeframe ringan (15m EMA arah)

MASALAH #4: IC KENA SWING NORMAL
  WLDUSDT turun 0.436% kena IC 0.40% → rebound setelahnya
  ATR 5m sering terlalu kecil karena hanya 1 candle noise
  Fix: IC = ATR × 1.2 (lebih longgar), TP1 = ATR × 2.0
       R:R tetap terjaga karena TP juga ikut naik

MASALAH #5: BUY PRESSURE FILTER LEMAH
  Sebelumnya br > 0.65 dapat +15 poin tapi tidak ada VETO
  Fix: Hard veto rule:
    - LONG: br (buy ratio) WAJIB > 0.50 (lebih banyak buyer)
    - SHORT: br WAJIB < 0.50 (lebih banyak seller)
    Kalau tidak memenuhi → sinyal di-skip total

MASALAH #6: VOLUME RATIO TIDAK CUKUP
  Sebelumnya vr >= 1.5 dapat +4 poin saja
  Fix: Wajib vr >= 1.3 untuk entry (ada momentum volume)
       Kalau volume flat → market tidak bergerak → skip

MODE: SIMULASI — tidak ada order nyata ke Binance
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
#  CONFIG v17
# ═══════════════════════════════════════════════════════
LEVERAGE       = 20
ORDER_USDT     = 1.0
MAX_POSITIONS  = 3

# ── CONTRARIAN: MATI. Sekarang pakai RSI extreme only ─
CONTRARIAN     = False   # Full flip dibuang

# ── ATR-BASED STOPS & TARGETS ─────────────────────────
# v18: IC lebih longgar supaya tidak kena swing normal
ATR_IC_MULT    = 1.2    # SL = ATR × 1.2 (naik dari 0.8, beri ruang lebih)
ATR_TP1_MULT   = 2.0    # TP1 = ATR × 2.0 (naik dari 1.2, R:R tetap 1:1.7)
ATR_TP2_MULT   = 4.0    # TP2 = ATR × 4.0 (naik dari 2.4)
ATR_TRAIL_MULT = 1.0    # Trail width = ATR × 1.0

# Hard cap persen (safety net agar tidak terlalu longgar)
MAX_IC_PCT     = 0.007  # Max IC 0.7% dari entry (naik dari 0.4%)
MAX_TP1_PCT    = 0.015  # Max TP1 1.5% (naik dari 0.8%)
MAX_TP2_PCT    = 0.030  # Max TP2 3.0% (naik dari 2.0%)

# Trail activation: profit 1.0× ATR baru trail on
TRAIL_ACTIVATE_MULT = 1.0

# TP1 partial — tutup 60%, sisakan 40% untuk trail ke TP2
TP1_RATIO      = 0.60

# ── FILTER BTC DIRECTION ──────────────────────────────
BTC_FILTER     = True

# ── ADX FILTER ────────────────────────────────────────
ADX_MIN        = 20     # Harus ada trend, bukan noise

# ── VOLUME & PRESSURE VETO ────────────────────────────
# v18: hard veto kalau pressure berlawanan arah
MIN_VR         = 1.3    # Minimum volume ratio untuk entry
# Buy ratio veto: LONG butuh br > 0.48, SHORT butuh br < 0.52
BR_LONG_MIN    = 0.48
BR_SHORT_MAX   = 0.52

# Kecepatan
SCAN_INTERVAL  = 1
MONITOR_INT    = 0.25
SCAN_DELAY     = 0.015
BATCH_SIZE     = 15
MAX_WORKERS    = 8
MAX_HOLD_SEC   = 600    # 10 menit — cukup waktu untuk gerak ATR×2.0

# Score threshold — lebih ketat
MIN_SCORE      = 55     # naik dari 45
MIN_GAP        = 25     # naik dari 20
COOLDOWN_SEC   = 8      # naik dari 5

# Kill switch
DAILY_LOSS     = -8.0
CONSEC_MAX     = 6
CONSEC_PAUSE   = 60

# Cache TTL
TTL_5M         = 5
TTL_15M        = 30

# ═══════════════════════════════════════════════════════
#  SYMBOLS — TOP 30 LIQUID FUTURES ONLY
#  Kurangi dari 158 → 30: spread bagus, liquidity tebal
# ═══════════════════════════════════════════════════════
SYMBOLS = [
    # Mega cap — paling liquid, spread paling kecil
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "TRXUSDT", "DOTUSDT",

    # Large cap — volume besar, cukup liquid
    "LINKUSDT", "MATICUSDT", "LTCUSDT", "ATOMUSDT", "UNIUSDT",
    "NEARUSDT", "APTUSDT", "ARBUSDT", "OPUSDT", "INJUSDT",

    # Mid cap momentum — pilihan terbaik
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
_ks    = {"active": False, "reason": "", "resume": 0,
          "consec": 0, "daily": 0.0, "day_reset": 0}
_stats = {
    "trades": 0, "wins": 0, "losses": 0,
    "pnl": 0.0, "best": 0.0, "worst": 0.0,
    "tp1": 0, "tp2": 0, "sl": 0, "cut": 0, "guard": 0, "force": 0,
    "hist": deque(maxlen=200), "start": time.time(),
}

# ═══════════════════════════════════════════════════════
#  UTILS
# ═══════════════════════════════════════════════════════
def qty(price):
    return (ORDER_USDT * LEVERAGE) / price

def price_live(symbol):
    try:
        return float(client.futures_symbol_ticker(symbol=symbol)["price"])
    except:
        return 0.0

def tickers_all():
    global _ticker_cache, _ticker_ts
    now = time.time()
    if now - _ticker_ts < 5 and _ticker_cache:
        return _ticker_cache
    try:
        raw = client.futures_ticker()
        new = {t["symbol"]: {
            "pct": float(t["priceChangePercent"]),
            "vol": float(t["quoteVolume"]),
            "last": float(t["lastPrice"]),
        } for t in raw}
        _ticker_cache = new
        _ticker_ts    = now
        return new
    except:
        return _ticker_cache

def ok_cooldown(sym):
    return (time.time() - _sym_cooldown.get(sym, 0)) >= COOLDOWN_SEC

def set_cd(sym):
    _sym_cooldown[sym] = time.time()

# ═══════════════════════════════════════════════════════
#  OHLCV
# ═══════════════════════════════════════════════════════
def ohlcv(symbol, interval, limit=100):
    key = (symbol, interval)
    now = time.time()
    ttl = {Client.KLINE_INTERVAL_5MINUTE: TTL_5M,
           Client.KLINE_INTERVAL_15MINUTE: TTL_15M}.get(interval, 10)
    if key in _ohlcv_cache:
        ts, df = _ohlcv_cache[key]
        if now - ts < ttl:
            return df
    try:
        kl = client.futures_klines(symbol=symbol, interval=interval, limit=limit)
        df = pd.DataFrame(kl, columns=[
            "time","open","high","low","close","volume",
            "ct","qv","trades","tbbase","tbquote","ignore"])
        for c in ["open","high","low","close","volume","tbbase","tbquote"]:
            df[c] = df[c].astype(float)
        df["time"] = pd.to_numeric(df["time"])
        _ohlcv_cache[key] = (now, df)
        return df
    except:
        return _ohlcv_cache.get(key, (None, None))[1]

# ═══════════════════════════════════════════════════════
#  TA — tambah ADX
# ═══════════════════════════════════════════════════════
def run_ta(df):
    c, h, l, v = df["close"], df["high"], df["low"], df["volume"]
    df["rsi"]  = ta.momentum.RSIIndicator(c, 14).rsi()
    df["mh"]   = ta.trend.MACD(c, 12, 26, 9).macd_diff()
    df["e5"]   = ta.trend.EMAIndicator(c, 5).ema_indicator()
    df["e9"]   = ta.trend.EMAIndicator(c, 9).ema_indicator()
    df["e21"]  = ta.trend.EMAIndicator(c, 21).ema_indicator()
    df["e50"]  = ta.trend.EMAIndicator(c, 50).ema_indicator()
    df["atr"]  = ta.volatility.AverageTrueRange(h, l, c, 14).average_true_range()

    # ADX — FIX #7: filter trend strength
    adx_ind    = ta.trend.ADXIndicator(h, l, c, 14)
    df["adx"]  = adx_ind.adx()

    df["vm"]   = v.rolling(20).mean()
    df["vr"]   = v / df["vm"].replace(0, 1)
    df["br"]   = df["tbbase"] / df["volume"].replace(0, 1)
    df["body"] = abs(c - df["open"])
    df["rng"]  = h - l
    df["br2"]  = df["body"] / df["rng"].replace(0, 1)
    df["m5"]   = (c - c.shift(5)) / c.shift(5)
    df["m3"]   = (c - c.shift(3)) / c.shift(3)
    return df

def btc_trend():
    try:
        df = ohlcv("BTCUSDT", Client.KLINE_INTERVAL_5MINUTE, 80)
        if df is None or len(df) < 30:
            return "UNKNOWN"
        df  = run_ta(df.copy())
        # FIX #4: gunakan candle [-2] yang sudah close
        row = df.iloc[-2]
        p, e5, e9, e21 = row["close"], row["e5"], row["e9"], row["e21"]
        m5 = row["m5"]
        if p > e5 > e9 > e21 and m5 > 0.001: return "BULL"
        if p < e5 < e9 < e21 and m5 < -0.001: return "BEAR"
        if p > e9 > e21: return "MILD_BULL"
        if p < e9 < e21: return "MILD_BEAR"
        return "SIDEWAYS"
    except:
        return "UNKNOWN"

# ═══════════════════════════════════════════════════════
#  KILL SWITCH
# ═══════════════════════════════════════════════════════
def ks_check():
    k, now = _ks, time.time()
    if k["active"] and now >= k["resume"]:
        k["active"] = False; k["consec"] = 0
        print("  ✅ Kill switch off")
    if k["active"]:
        return True, k["reason"]
    day = now - (now % 86400)
    if day > k["day_reset"]:
        k["daily"] = 0.0; k["day_reset"] = day
    if k["daily"] <= DAILY_LOSS:
        k["active"] = True; k["reason"] = f"daily({k['daily']:.2f})"
        k["resume"] = day + 86400
        return True, k["reason"]
    if k["consec"] >= CONSEC_MAX:
        k["active"] = True; k["reason"] = f"consec({k['consec']})"
        k["resume"] = now + CONSEC_PAUSE
        print(f"  🚨 {k['consec']} loss beruntun — pause {CONSEC_PAUSE}s")
        return True, k["reason"]
    return False, ""

def ks_upd(pnl):
    _ks["daily"] += pnl
    _ks["consec"] = 0 if pnl >= 0 else _ks["consec"] + 1

# ═══════════════════════════════════════════════════════
#  ATR LEVELS — FIX #2 & #3
# ═══════════════════════════════════════════════════════
def calc_levels(direction, entry, atr):
    """
    Hitung IC, TP1, TP2 berdasarkan ATR.
    Lebih adaptif dari fixed pct — noise tiap coin beda-beda.
    """
    if direction == "LONG":
        ic      = entry - min(atr * ATR_IC_MULT,   entry * MAX_IC_PCT)
        tp1     = entry + min(atr * ATR_TP1_MULT,  entry * MAX_TP1_PCT)
        tp2     = entry + min(atr * ATR_TP2_MULT,  entry * MAX_TP2_PCT)
        hard_sl = entry - min(atr * ATR_IC_MULT * 1.2, entry * MAX_IC_PCT * 1.2)
        trail0  = entry - min(atr * ATR_TRAIL_MULT, entry * MAX_IC_PCT)
        trail_a = entry + atr * TRAIL_ACTIVATE_MULT  # aktivasi trail di sini
    else:
        ic      = entry + min(atr * ATR_IC_MULT,   entry * MAX_IC_PCT)
        tp1     = entry - min(atr * ATR_TP1_MULT,  entry * MAX_TP1_PCT)
        tp2     = entry - min(atr * ATR_TP2_MULT,  entry * MAX_TP2_PCT)
        hard_sl = entry + min(atr * ATR_IC_MULT * 1.2, entry * MAX_IC_PCT * 1.2)
        trail0  = entry + min(atr * ATR_TRAIL_MULT, entry * MAX_IC_PCT)
        trail_a = entry - atr * TRAIL_ACTIVATE_MULT

    ic_pct  = abs(entry - ic) / entry * 100
    tp1_pct = abs(tp1 - entry) / entry * 100
    rr      = tp1_pct / ic_pct if ic_pct > 0 else 0

    return {
        "ic": ic, "tp1": tp1, "tp2": tp2,
        "hard_sl": hard_sl, "trail0": trail0,
        "trail_act": trail_a, "atr": atr,
        "ic_pct": ic_pct, "tp1_pct": tp1_pct, "rr": rr,
    }

# ═══════════════════════════════════════════════════════
#  SIGNAL ENGINE v17 — CLOSE CANDLE + ADX + BTC FILTER
# ═══════════════════════════════════════════════════════
def signal(df):
    """
    v18 — Signal dengan hard veto rules:
    1. Volume ratio wajib >= MIN_VR
    2. Buy pressure veto: arah entry HARUS sesuai pressure
    3. Konfirmasi 15m EMA ringan (ambil dari cache)
    4. Pakai iloc[-2] (candle close)
    5. ADX filter
    6. BTC direction filter
    """
    if df is None or len(df) < 55:
        return None, 0, [], 0.0

    # FIX #4: candle yang sudah close
    row   = df.iloc[-2]
    prev  = df.iloc[-3]
    prev2 = df.iloc[-4]

    p, e5, e9, e21, e50 = row["close"], row["e5"], row["e9"], row["e21"], row["e50"]
    rsi    = row["rsi"]
    mh     = row["mh"]
    mh_p   = prev["mh"]
    mh_p2  = prev2["mh"]
    vr     = row["vr"]
    br     = row["br"]   # buy ratio: > 0.5 = lebih banyak buyer
    m5     = row["m5"]
    body   = row["br2"]
    atr    = row["atr"]
    adx    = row["adx"]
    btc    = _macro["btc"]

    # ═ HARD GATE #1: ADX ══════════════════════════════
    if adx < ADX_MIN:
        return None, 0, [], atr

    # ═ HARD GATE #2: VOLUME ═══════════════════════════
    # v18: wajib ada momentum volume, bukan market tidur
    if vr < MIN_VR:
        return None, 0, [], atr

    lp = sp = 0
    sl = ss = []

    # ═ A. EMA stack ═══════════════════════════════════
    if p > e5 > e9 > e21 > e50:   lp += 30; sl.append("EMA_stack↑")
    elif p > e5 > e9 > e21:        lp += 22; sl.append("EMA↑↑")
    elif p > e9 > e21:             lp += 14; sl.append("EMA↑")
    elif p > e9:                   lp += 7

    if p < e5 < e9 < e21 < e50:   sp += 30; ss.append("EMA_stack↓")
    elif p < e5 < e9 < e21:        sp += 22; ss.append("EMA↓↓")
    elif p < e9 < e21:             sp += 14; ss.append("EMA↓")
    elif p < e9:                   sp += 7

    # ═ B. Momentum ════════════════════════════════════
    if m5 > 0.005:    lp += 25; sl.append(f"Mom+{m5*100:.1f}%")
    elif m5 > 0.003:  lp += 18; sl.append(f"Mom+{m5*100:.1f}%")
    elif m5 > 0.001:  lp += 10
    elif m5 < -0.001: lp = max(0, lp - 15)

    if m5 < -0.005:   sp += 25; ss.append(f"Mom{m5*100:.1f}%")
    elif m5 < -0.003: sp += 18; ss.append(f"Mom{m5*100:.1f}%")
    elif m5 < -0.001: sp += 10
    elif m5 > 0.001:  sp = max(0, sp - 15)

    # ═ C. MACD ════════════════════════════════════════
    if mh_p <= 0 and mh > 0:           lp += 22; sl.append("MACD_X↑")
    elif mh > 0 and mh > mh_p > mh_p2: lp += 18; sl.append("MACD↑↑")
    elif mh > 0 and mh > mh_p:         lp += 12; sl.append("MACD↑")

    if mh_p >= 0 and mh < 0:           sp += 22; ss.append("MACD_X↓")
    elif mh < 0 and mh < mh_p < mh_p2: sp += 18; ss.append("MACD↓↓")
    elif mh < 0 and mh < mh_p:         sp += 12; ss.append("MACD↓")

    # ═ D. Volume bonus (sudah lewat gate >= 1.3) ═════
    if vr >= 3.0:
        lp += 15; sp += 15
        sl.append(f"Vol{vr:.1f}x"); ss.append(f"Vol{vr:.1f}x")
    elif vr >= 2.0: lp += 10; sp += 10; sl.append(f"Vol{vr:.1f}x"); ss.append(f"Vol{vr:.1f}x")
    elif vr >= 1.5: lp += 6;  sp += 6

    # ═ E. Buy/Sell pressure ═══════════════════════════
    # Scoring biasa dulu
    if br > 0.65:    lp += 18; sl.append(f"Buy{br:.0%}")
    elif br > 0.57:  lp += 10
    elif br < 0.43:  lp = max(0, lp - 15)

    if br < 0.35:    sp += 18; ss.append(f"Sell{1-br:.0%}")
    elif br < 0.43:  sp += 10
    elif br > 0.57:  sp = max(0, sp - 15)

    # ═ F. RSI ═════════════════════════════════════════
    if rsi > 75:
        lp  = int(lp * 0.4)
        sp += 20; ss.append(f"RSI_OB{rsi:.0f}")
    elif rsi < 25:
        sp  = int(sp * 0.4)
        lp += 20; sl.append(f"RSI_OS{rsi:.0f}")
    elif 45 < rsi < 65 and m5 > 0:
        lp += 6
    elif 35 < rsi < 55 and m5 < 0:
        sp += 6

    # ═ G. Candle body ═════════════════════════════════
    if row["close"] > row["open"] and body > 0.6: lp += 6
    if row["close"] < row["open"] and body > 0.6: sp += 6

    # ═ H. ADX bonus ═══════════════════════════════════
    if adx > 35: lp += 8; sp += 8; sl.append(f"ADX{adx:.0f}"); ss.append(f"ADX{adx:.0f}")
    elif adx > 25: lp += 4; sp += 4

    # ═ I. BTC direction filter ════════════════════════
    if btc == "BULL":
        lp += 15
        if BTC_FILTER: sp = 0; ss = []
        else: sp = max(0, sp - 25)
    elif btc == "MILD_BULL":
        lp += 8
        if BTC_FILTER: sp = max(0, sp - 30)
        else: sp = max(0, sp - 15)
    elif btc == "BEAR":
        sp += 15
        if BTC_FILTER: lp = 0; sl = []
        else: lp = max(0, lp - 25)
    elif btc == "MILD_BEAR":
        sp += 8
        if BTC_FILTER: lp = max(0, lp - 30)
        else: lp = max(0, lp - 15)

    # ═ DECISION ═══════════════════════════════════════
    btc_sw = btc in ("SIDEWAYS", "UNKNOWN")
    thresh = 60 if btc_sw else MIN_SCORE
    gap    = abs(lp - sp)

    if lp <= sp or lp < thresh or gap < MIN_GAP:
        if sp <= lp or sp < thresh or gap < MIN_GAP:
            return None, max(lp, sp), [], atr
        # SHORT path — veto check dulu
        # v18 HARD VETO: SHORT butuh br < BR_SHORT_MAX
        if br >= BR_SHORT_MAX:
            return None, sp, [], atr  # pressure tidak mendukung SHORT
        return "SHORT", sp, ss[:3], atr

    # LONG path — veto check
    # v18 HARD VETO: LONG butuh br > BR_LONG_MIN
    if br <= BR_LONG_MIN:
        return None, lp, [], atr  # pressure tidak mendukung LONG

    return "LONG", lp, sl[:3], atr

# ═══════════════════════════════════════════════════════
#  PAPER OPEN
# ═══════════════════════════════════════════════════════
def paper_open(sym, direction, score, sigs, price, atr):
    with _lock:
        if sym in paper_positions or len(paper_positions) >= MAX_POSITIONS:
            return
        paper_positions[sym] = {"_r": True}

    q   = qty(price)
    lvl = calc_levels(direction, price, atr)

    pos = {
        "side":      direction,
        "entry":     price,
        "qty":       q,
        "qty_rem":   q,
        "ic":        lvl["ic"],
        "hard_sl":   lvl["hard_sl"],
        "tp1":       lvl["tp1"],
        "tp2":       lvl["tp2"],
        "trail_sl":  lvl["trail0"],
        "trail_act": lvl["trail_act"],
        "peak":      price,
        "trail_on":  False,
        "tp1_hit":   False,
        "be_on":     False,
        "open_time": time.time(),
        "score":     score,
        "sigs":      sigs,
        "atr":       atr,
    }
    with _lock:
        paper_positions[sym] = pos

    d = "🟢" if direction == "LONG" else "🔴"
    print(f"\n  {d} [PAPER] {sym} {direction} @{price:.6g}")
    print(f"     IC:{lvl['ic']:.5g} (±{lvl['ic_pct']:.2f}%) | TP1:{lvl['tp1']:.5g} (+{lvl['tp1_pct']:.2f}%)")
    print(f"     TP2:{lvl['tp2']:.5g} | ATR:{atr:.5g} | R:R≈1:{lvl['rr']:.1f} | ADX filter:✅")
    print(f"     Score:{score} | BTC:{_macro['btc']} | Sigs:{' | '.join(sigs)}")
    _stats["trades"] += 1

# ═══════════════════════════════════════════════════════
#  PAPER CLOSE
# ═══════════════════════════════════════════════════════
def paper_close(sym, reason, price=None):
    with _lock:
        pos = paper_positions.pop(sym, None)
    if pos is None or pos.get("_r"):
        return

    if price is None:
        price = price_live(sym)

    side  = pos["side"]
    entry = pos["entry"]
    qr    = pos.get("qty_rem", pos["qty"])
    pnl   = (price - entry) * qr if side == "LONG" else (entry - price) * qr
    pct   = (price - entry) / entry * 100 if side == "LONG" else (entry - price) / entry * 100
    hold  = time.time() - pos["open_time"]
    e     = "🟢" if pnl >= 0 else "🔴"

    print(f"  {e} [PAPER] {sym} {side} CLOSE — {reason}")
    print(f"     {entry:.6g}→{price:.6g} ({pct:+.3f}%) hold:{hold:.0f}s | PnL:{pnl:+.5f}U")

    _stats["pnl"] += pnl
    _stats["hist"].append(pnl)
    ks_upd(pnl)

    if pnl >= 0:
        _stats["wins"] += 1
        if pnl > _stats["best"]: _stats["best"] = pnl
    else:
        _stats["losses"] += 1
        if pnl < _stats["worst"]: _stats["worst"] = pnl

    r = reason
    if "TP1"   in r: _stats["tp1"]   += 1
    if "TP2"   in r: _stats["tp2"]   += 1
    if "SL"    in r: _stats["sl"]    += 1
    if "Cut"   in r: _stats["cut"]   += 1
    if "Guard" in r: _stats["guard"] += 1
    if "Force" in r: _stats["force"] += 1

    trade_log.append({
        "sym": sym, "side": side,
        "entry": round(entry, 7), "exit": round(price, 7),
        "pnl": round(pnl, 5), "reason": reason, "hold": int(hold),
    })
    set_cd(sym)
    _hot_syms.appendleft(sym)
    _rescan_q.put(1)
    print_inline()

def paper_tp1(sym, price):
    pos = paper_positions.get(sym)
    if pos is None or pos.get("tp1_hit") or pos.get("_r"):
        return

    side  = pos["side"]
    entry = pos["entry"]
    cq    = pos["qty"] * TP1_RATIO
    pnl   = (price - entry) * cq if side == "LONG" else (entry - price) * cq
    hold  = time.time() - pos["open_time"]
    atr   = pos["atr"]

    print(f"  🎯 [PAPER] {sym} TP1 @{price:.6g} hold:{hold:.0f}s | PnL:{pnl:+.5f}U")

    pos["tp1_hit"] = True
    pos["qty_rem"] = pos["qty"] * (1 - TP1_RATIO)
    pos["be_on"]   = True

    # Setelah TP1, SL ke breakeven, trail ketat
    if side == "LONG":
        pos["hard_sl"]  = entry * 1.00005
        pos["trail_sl"] = price - atr * ATR_TRAIL_MULT * 0.7
    else:
        pos["hard_sl"]  = entry * 0.99995
        pos["trail_sl"] = price + atr * ATR_TRAIL_MULT * 0.7
    pos["peak"]     = price
    pos["trail_on"] = True

    _stats["pnl"] += pnl
    _stats["hist"].append(pnl)
    _stats["wins"] += 1
    _stats["tp1"]  += 1
    ks_upd(pnl)
    if pnl > _stats["best"]: _stats["best"] = pnl
    trade_log.append({
        "sym": sym, "side": side,
        "entry": round(entry, 7), "exit": round(price, 7),
        "pnl": round(pnl, 5), "reason": "TP1", "hold": int(hold),
    })
    print_inline()

# ═══════════════════════════════════════════════════════
#  MONITOR
# ═══════════════════════════════════════════════════════
def monitor_positions():
    for sym in list(paper_positions.keys()):
        pos = paper_positions.get(sym)
        if pos is None or pos.get("_r"):
            continue

        px   = price_live(sym)
        if px == 0:
            continue

        side  = pos["side"]
        entry = pos["entry"]
        atr   = pos["atr"]
        hold  = time.time() - pos["open_time"]

        if hold >= MAX_HOLD_SEC:
            paper_close(sym, "Force", px); continue

        if side == "LONG":
            prof_pct = (px - entry) / entry

            if px <= pos["ic"]:
                paper_close(sym, "LightningCut", px); continue

            if not pos["tp1_hit"] and px <= pos["hard_sl"]:
                paper_close(sym, "HardGuard", px); continue

            if not pos["tp1_hit"] and px >= pos["tp1"]:
                paper_tp1(sym, px); continue

            # Trail activation: profit ≥ 0.5× ATR
            if px >= pos["trail_act"] and not pos["trail_on"]:
                pos["trail_on"] = True
                pos["peak"]     = px
                pos["trail_sl"] = px - atr * ATR_TRAIL_MULT
                pos["hard_sl"]  = entry * 1.00005

            if pos["trail_on"] and px > pos["peak"]:
                pos["peak"]     = px
                new_t           = px - atr * ATR_TRAIL_MULT
                pos["trail_sl"] = max(pos["trail_sl"], new_t)
                new_hard        = px - atr * ATR_TRAIL_MULT * 1.5
                pos["hard_sl"]  = max(pos["hard_sl"], new_hard)

            if pos["trail_on"] and px <= pos["trail_sl"]:
                tag = "TrailBE" if pos["be_on"] else "TrailStop"
                paper_close(sym, tag, px); continue

            if pos["tp1_hit"] and px >= pos["tp2"]:
                paper_close(sym, "TP2", px); continue

            pnl_now = (px - entry) * pos.get("qty_rem", pos["qty"])
            tsl = f"T:{pos['trail_sl']:.5g}" if pos["trail_on"] else f"IC:{pos['ic']:.5g}"
            tp  = f"TP2:{pos['tp2']:.5g}" if pos["tp1_hit"] else f"TP1:{pos['tp1']:.5g}"
            print(f"  📌 {sym} L@{entry:.5g}→{px:.5g}({prof_pct*100:+.2f}%) {pnl_now:+.4f}U {hold:.0f}s {tsl} {tp}")

        else:  # SHORT
            prof_pct = (entry - px) / entry

            if px >= pos["ic"]:
                paper_close(sym, "LightningCut", px); continue

            if not pos["tp1_hit"] and px >= pos["hard_sl"]:
                paper_close(sym, "HardGuard", px); continue

            if not pos["tp1_hit"] and px <= pos["tp1"]:
                paper_tp1(sym, px); continue

            if px <= pos["trail_act"] and not pos["trail_on"]:
                pos["trail_on"] = True
                pos["peak"]     = px
                pos["trail_sl"] = px + atr * ATR_TRAIL_MULT
                pos["hard_sl"]  = entry * 0.99995

            if pos["trail_on"] and px < pos["peak"]:
                pos["peak"]     = px
                new_t           = px + atr * ATR_TRAIL_MULT
                pos["trail_sl"] = min(pos["trail_sl"], new_t)
                new_hard        = px + atr * ATR_TRAIL_MULT * 1.5
                pos["hard_sl"]  = min(pos["hard_sl"], new_hard)

            if pos["trail_on"] and px >= pos["trail_sl"]:
                tag = "TrailBE" if pos["be_on"] else "TrailStop"
                paper_close(sym, tag, px); continue

            if pos["tp1_hit"] and px <= pos["tp2"]:
                paper_close(sym, "TP2", px); continue

            pnl_now = (entry - px) * pos.get("qty_rem", pos["qty"])
            tsl = f"T:{pos['trail_sl']:.5g}" if pos["trail_on"] else f"IC:{pos['ic']:.5g}"
            tp  = f"TP2:{pos['tp2']:.5g}" if pos["tp1_hit"] else f"TP1:{pos['tp1']:.5g}"
            print(f"  📌 {sym} S@{entry:.5g}→{px:.5g}({prof_pct*100:+.2f}%) {pnl_now:+.4f}U {hold:.0f}s {tsl} {tp}")

# ═══════════════════════════════════════════════════════
#  SCANNER
# ═══════════════════════════════════════════════════════
def scan_one(sym):
    try:
        time.sleep(SCAN_DELAY)
        if not ok_cooldown(sym): return None
        tk = _ticker_cache
        if sym in tk and tk[sym]["vol"] < 200_000: return None

        df = ohlcv(sym, Client.KLINE_INTERVAL_5MINUTE, 100)
        if df is None or len(df) < 55: return None
        df = run_ta(df.copy())

        # FIX #4: cek price dari candle[-2] yang sudah close
        px  = df["close"].iloc[-2]
        atr = df["atr"].iloc[-2]
        if px == 0 or atr / px > 0.03: return None

        dir_, sc, sigs, atr_val = signal(df)
        if dir_ is None or len(sigs) < 1: return None

        # Gunakan price live untuk entry actual (tapi signal dari candle closed)
        px_live = price_live(sym)
        if px_live == 0: return None

        return (sym, dir_, sc, sigs, px_live, atr_val)
    except:
        return None

def scan_batch(syms):
    res = []
    fut = {_executor.submit(scan_one, s): s for s in syms[:BATCH_SIZE]}
    try:
        for f in as_completed(fut, timeout=10):
            try:
                r = f.result(timeout=2)
                if r: res.append(r)
            except: pass
    except:
        for f in fut:
            if f.done():
                try:
                    r = f.result(timeout=0)
                    if r: res.append(r)
                except: pass
    return res

def top_movers(syms, n=20):
    tk  = tickers_all()
    ss  = set(syms)
    mv  = [(s, abs(d["pct"])) for s, d in tk.items()
           if s in ss and d["vol"] >= 200_000]
    mv.sort(key=lambda x: x[1], reverse=True)
    return [s for s, _ in mv[:n]]

# ═══════════════════════════════════════════════════════
#  STATS
# ═══════════════════════════════════════════════════════
def print_inline():
    n   = _stats["wins"] + _stats["losses"]
    wr  = _stats["wins"] / n * 100 if n else 0
    pnl = _stats["pnl"]
    e   = "💚" if pnl >= 0 else "🔴"
    print(f"     ┌ [v18] {n}T WR:{wr:.0f}% W:{_stats['wins']} L:{_stats['losses']} {e}PnL:{pnl:+.4f}U")
    print(f"     └ ATR-SL | TP1:{_stats['tp1']} TP2:{_stats['tp2']} "
          f"Cut:{_stats['cut']} Guard:{_stats['guard']} Force:{_stats['force']}")

def print_full():
    n    = _stats["wins"] + _stats["losses"]
    wr   = _stats["wins"] / n * 100 if n else 0
    pnl  = _stats["pnl"]
    sess = (time.time() - _stats["start"]) / 3600
    tph  = n / sess if sess > 0 else 0
    e    = "💚" if pnl >= 0 else "🔴"

    pnls = list(_stats["hist"])
    sh = md = 0.0
    if len(pnls) >= 5:
        a  = np.array(pnls)
        sd = float(np.std(a))
        sh = float(np.mean(a)) / sd if sd > 0 else 0.0
    if len(pnls) >= 2:
        eq = np.cumsum(pnls)
        md = float(np.min(eq - np.maximum.accumulate(eq)))

    print(f"\n  {'─'*62}")
    print(f"  🧪 PAPER v18 [SIGNAL QUALITY + HOLD FIX] — {sess*60:.0f}m | {tph:.1f}T/jam | {len(SYMBOLS)}sym")
    print(f"  🎯 {n}T WR:{wr:.0f}% W:{_stats['wins']} L:{_stats['losses']}")
    print(f"  {e} PnL:{pnl:+.5f}U Best:{_stats['best']:+.5f} Worst:{_stats['worst']:+.5f}")
    print(f"  📐 Sharpe:{sh:.2f} MaxDD:{md:.5f}U")
    print(f"  TP1:{_stats['tp1']} TP2:{_stats['tp2']} SL:{_stats['sl']} "
          f"⚡Cut:{_stats['cut']} 🛡️Guard:{_stats['guard']} ⏰Force:{_stats['force']}")
    print(f"  KS: consec={_ks['consec']} daily={_ks['daily']:+.4f} | BTC:{_macro['btc']}")
    if trade_log:
        print(f"  📋 Last 5:")
        for t in trade_log[-5:]:
            em = "🟢" if t["pnl"] > 0 else "🔴"
            print(f"     {em} {t['sym']:<14} {t['side']} {t['pnl']:+.5f}U {t['hold']}s — {t['reason']}")
    print(f"  {'─'*62}")

# ═══════════════════════════════════════════════════════
#  THREADS
# ═══════════════════════════════════════════════════════
def t_monitor():
    while True:
        try:
            if paper_positions: monitor_positions()
        except Exception as e:
            print(f"  ❌ mon:{e}")
        time.sleep(MONITOR_INT)

def t_rescan(syms):
    while True:
        try:
            _rescan_q.get(timeout=30)
            time.sleep(0.3)
            slots = MAX_POSITIONS - len(paper_positions)
            if slots <= 0: continue
            if ks_check()[0]: continue
            hot  = [s for s in _hot_syms if s not in paper_positions]
            rest = [s for s in syms if s not in paper_positions and s not in hot]
            res  = scan_batch((hot + rest)[:25])
            if res:
                res.sort(key=lambda x: x[2], reverse=True)
                for r in res[:slots]:
                    if len(paper_positions) >= MAX_POSITIONS: break
                    sym, d, sc, sg, px, atr = r
                    paper_open(sym, d, sc, sg, px, atr)
        except queue.Empty: pass
        except Exception as e: print(f"  ❌ rescan:{e}")

def t_macro():
    while True:
        try:
            _macro["btc"] = btc_trend()
        except: pass
        try:
            if time.time() - _macro["last_fng"] > 300:
                d = requests.get("https://api.alternative.me/fng/?limit=1", timeout=5).json()["data"][0]
                _macro["fng"] = int(d["value"])
                _macro["last_fng"] = time.time()
        except: pass
        time.sleep(5)

# ═══════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════
def run_bot():
    print("╔═══════════════════════════════════════════════════════╗")
    print("║  🧪 PAPER TRADE v18 — SIGNAL QUALITY + HOLD FIX     ║")
    print("║  ⚠️  SIMULASI — NO REAL ORDERS TO BINANCE             ║")
    print("╠═══════════════════════════════════════════════════════╣")
    print(f"║  Fix: Hard veto pressure berlawanan arah             ║")
    print(f"║  Fix: Volume wajib >= {MIN_VR}x sebelum entry               ║")
    print(f"║  Fix: MAX_HOLD = {MAX_HOLD_SEC}s (dari 180s)                  ║")
    print(f"║  Fix: IC=ATR×{ATR_IC_MULT} TP1=ATR×{ATR_TP1_MULT} TP2=ATR×{ATR_TP2_MULT}              ║")
    print(f"║  Fix: MIN_SCORE={MIN_SCORE} MIN_GAP={MIN_GAP} COOLDOWN={COOLDOWN_SEC}s               ║")
    print("╚═══════════════════════════════════════════════════════╝")
    print(f"\n  ATR multipliers: IC×{ATR_IC_MULT} | TP1×{ATR_TP1_MULT} | TP2×{ATR_TP2_MULT} | Trail×{ATR_TRAIL_MULT}")
    print(f"  Max caps: IC<{MAX_IC_PCT*100:.1f}% | TP1<{MAX_TP1_PCT*100:.1f}% | TP2<{MAX_TP2_PCT*100:.1f}%")

    try:
        valid = {s["symbol"] for s in client.futures_exchange_info()["symbols"]
                 if s["status"] == "TRADING"}
        syms = list(dict.fromkeys([s for s in SYMBOLS if s in valid]))
    except:
        syms = list(dict.fromkeys(SYMBOLS))
    print(f"\n  ✅ {len(syms)} symbols valid")

    threading.Thread(target=t_monitor, daemon=True).start()
    threading.Thread(target=t_rescan, args=(syms,), daemon=True).start()
    threading.Thread(target=t_macro, daemon=True).start()
    print("  🔧 Threads: monitor ✅ rescan ✅ macro ✅")

    print("  ⏳ Init 4s...")
    time.sleep(4)
    tickers_all()
    print(f"  📊 BTC:{_macro['btc']} F&G:{_macro['fng']}\n")

    cycle = scan_idx = 0
    n_bat = math.ceil(len(syms) / BATCH_SIZE)

    while True:
        cycle += 1
        slots = MAX_POSITIONS - len(paper_positions)

        print(f"\n{'═'*57}")
        print(f"  #{cycle} {time.strftime('%H:%M:%S')} BTC:{_macro['btc']} F&G:{_macro['fng']} "
              f"({len(paper_positions)}/{MAX_POSITIONS}) PnL:{_stats['pnl']:+.4f}U")

        if (k := ks_check())[0]:
            print(f"  🚨 KS:{k[1]}"); time.sleep(SCAN_INTERVAL); continue

        if slots > 0:
            mv  = top_movers(syms, 20)
            mv  = [s for s in mv if s not in paper_positions]
            bs  = scan_idx * BATCH_SIZE
            reg = [s for s in syms[bs:bs+BATCH_SIZE]
                   if s not in paper_positions and s not in mv]
            scan_idx = (scan_idx + 1) % n_bat
            scan_list = mv[:15] + reg[:10]

            print(f"  🔍 {len(scan_list)} syms | {slots} slot kosong | ADX≥{ADX_MIN} VR≥{MIN_VR} required")
            try: res = scan_batch(scan_list)
            except: res = []

            if res:
                res.sort(key=lambda x: x[2], reverse=True)
                print(f"  🎯 {len(res)} setup!")
                for r in res[:slots]:
                    if len(paper_positions) >= MAX_POSITIONS: break
                    sym, d, sc, sg, px, atr = r
                    print(f"     ⭐ {sym} {d} Score:{sc} ATR:{atr:.5g} {' | '.join(sg)}")
                    paper_open(sym, d, sc, sg, px, atr)
            elif len(paper_positions) == 0:
                print("  ⚠️  Wide scan...")
                try:
                    r2 = scan_batch([s for s in syms if s not in paper_positions])
                except: r2 = []
                if r2:
                    r2.sort(key=lambda x: x[2], reverse=True)
                    sym, d, sc, sg, px, atr = r2[0]
                    print(f"     ⭐ best: {sym} {d} Score:{sc}")
                    paper_open(sym, d, sc, sg, px, atr)
                else:
                    print("  ⏳ Market flat / ADX rendah — tunggu...")
            else:
                print(f"  ⏳ {len(paper_positions)} pos aktif")
        else:
            print(f"  ✅ Full ({MAX_POSITIONS}/{MAX_POSITIONS})")

        if cycle % 20 == 0:
            print_full()

        time.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    run_bot()