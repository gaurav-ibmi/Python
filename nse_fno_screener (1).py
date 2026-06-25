"""
=============================================================================
NSE F&O Breakout Screener  —  Free, No API Key Required
=============================================================================
Version : 2.0.0  (NSE-only, colourful dashboard output)

DATA SOURCES  (100% free, no registration)
------------------------------------------
• yfinance   — historical daily OHLCV (400 days) for all indicators
               NSE tickers: symbol + ".NS"  e.g. RELIANCE.NS
• nsetools   — real-time live quotes directly from NSE website
               Used for live LTP polling every 60 seconds

WHAT IT DOES
------------
1. Fetches NSE F&O stock symbols + company names from NSE website live,
   falls back to a built-in list if the website is unreachable.
2. Downloads 400 days of daily OHLCV via yfinance for every stock.
3. Computes: SMA-20/50/200, RSI-14, ATR-14, vol-SMA-20/50,
             40-day rolling high, 100-day rolling high, 52-week high.
4. Fires Strategy A (swing) and Strategy B (positional) breakout signals.
5. Prints a colourful terminal dashboard with RSI gauges and volume bars.
6. Saves signals_YYYYMMDD.csv.
7. Live-polls LTP every 60 s during market hours (09:15-15:30 IST).

STRATEGIES
----------
A — Swing Breakout (7-20 days, target 10-20%)
    close > SMA20 & SMA50  |  high & close ≥ 40-day high
    volume ≥ 1.5x avg      |  RSI-14 ≥ 55
    SL = entry - ATR14  (max 8%)  |  T1 = +2R  |  T2 = +3R

B — Positional Breakout (1-6 weeks, target 15-40%)
    close > SMA50 & SMA200  |  close ≥ 100-day OR 52-week high
    volume ≥ 1.5x avg       |  RSI-14 ≥ 60
    SL = entry - ATR14  (max 8%)  |  T1 = +2R  |  T2 = +3R

HOW TO RUN
----------
pip install yfinance nsetools pandas pandas-ta numpy requests colorama
python nse_fno_screener.py              # full scan + live polling
python nse_fno_screener.py --scan-only  # scan + dashboard, no live poll
python nse_fno_screener.py --live-only  # reload last scan + live poll
=============================================================================
"""

import os, sys, time, json, logging, argparse, warnings, threading
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
warnings.filterwarnings("ignore")

try:
    import yfinance as yf
except ImportError:
    print("ERROR: pip install yfinance"); sys.exit(1)

try:
    from nsetools import Nse as _Nse
    _nse = _Nse()
    NSE_OK = True
except ImportError:
    _nse = None; NSE_OK = False

try:
    import pandas_ta as ta; TA_OK = True
except ImportError:
    TA_OK = False

try:
    from colorama import init as _cinit, Fore, Back, Style
    _cinit(autoreset=True); COLOR_OK = True
except ImportError:
    COLOR_OK = False
    class _Stub:
        def __getattr__(self, _): return ""
    Fore = Back = Style = _Stub()

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)
logging.getLogger("yfinance").setLevel(logging.CRITICAL)
IST = ZoneInfo("Asia/Kolkata")


# ===========================================================================
# 1. CONFIG
# ===========================================================================
class Config:
    CLOSE_MIN_A   = 100;   CLOSE_MIN_B   = 150
    SMA_SHORT     = 20;    SMA_MED       = 50;   SMA_LONG     = 200
    VOL_SMA_SHORT = 20;    VOL_SMA_LONG  = 50
    RSI_PERIOD    = 14;    ATR_PERIOD    = 14
    HHV_SWING     = 40;    HHV_POS       = 100;  HHV_52W      = 252
    VOL_MULT_A    = 1.5;   VOL_MULT_B    = 1.5
    RSI_MIN_A     = 55;    RSI_MIN_B     = 60
    T1_R          = 2.0;   T2_R          = 3.0
    HIST_DAYS     = 420
    POLL_INTERVAL = 60
    MARKET_OPEN   = (9, 15);  MARKET_CLOSE = (15, 30)
    OUTPUT_CSV    = f"signals_{date.today():%Y%m%d}.csv"
    LAST_CSV      = "last_scan_signals.csv"


# ===========================================================================
# 2. NSE SYMBOL MASTER  — fetched live from NSE, with built-in fallback
# ===========================================================================

# Built-in F&O symbol → company name map (153 stocks, Jun 2025)
_BUILTIN = {
    "ADANIENT":"Adani Enterprises","ADANIPORTS":"Adani Ports",
    "APOLLOHOSP":"Apollo Hospitals","ASIANPAINT":"Asian Paints",
    "AXISBANK":"Axis Bank","BAJAJ-AUTO":"Bajaj Auto",
    "BAJFINANCE":"Bajaj Finance","BAJAJFINSV":"Bajaj Finserv",
    "BPCL":"BPCL","BHARTIARTL":"Bharti Airtel",
    "BRITANNIA":"Britannia Industries","CIPLA":"Cipla",
    "COALINDIA":"Coal India","DIVISLAB":"Divi's Labs",
    "DRREDDY":"Dr. Reddy's","EICHERMOT":"Eicher Motors",
    "GRASIM":"Grasim Industries","HCLTECH":"HCL Technologies",
    "HDFCBANK":"HDFC Bank","HDFCLIFE":"HDFC Life",
    "HEROMOTOCO":"Hero MotoCorp","HINDALCO":"Hindalco",
    "HINDUNILVR":"Hindustan Unilever","ICICIBANK":"ICICI Bank",
    "ITC":"ITC","INDUSINDBK":"IndusInd Bank",
    "INFY":"Infosys","JSWSTEEL":"JSW Steel",
    "KOTAKBANK":"Kotak Mahindra Bank","LT":"Larsen & Toubro",
    "M&M":"Mahindra & Mahindra","MARUTI":"Maruti Suzuki",
    "NESTLEIND":"Nestle India","NTPC":"NTPC",
    "ONGC":"ONGC","POWERGRID":"Power Grid Corp",
    "RELIANCE":"Reliance Industries","SBILIFE":"SBI Life",
    "SHRIRAMFIN":"Shriram Finance","SBIN":"State Bank of India",
    "SUNPHARMA":"Sun Pharma","TCS":"Tata Consultancy Services",
    "TATACONSUM":"Tata Consumer","TATAMOTORS":"Tata Motors",
    "TATASTEEL":"Tata Steel","TECHM":"Tech Mahindra",
    "TITAN":"Titan Company","TRENT":"Trent",
    "ULTRACEMCO":"UltraTech Cement","WIPRO":"Wipro",
    "ABB":"ABB India","ABCAPITAL":"Aditya Birla Capital",
    "ABFRL":"Aditya Birla Fashion","ADANIGREEN":"Adani Green Energy",
    "ADANIPOWER":"Adani Power","ALKEM":"Alkem Laboratories",
    "AMBUJACEM":"Ambuja Cements","APLAPOLLO":"APL Apollo Tubes",
    "ASTRAL":"Astral","AUROPHARMA":"Aurobindo Pharma",
    "AUBANK":"AU Small Finance Bank","BALKRISIND":"Balkrishna Industries",
    "BANDHANBNK":"Bandhan Bank","BANKBARODA":"Bank of Baroda",
    "BEL":"Bharat Electronics","BERGEPAINT":"Berger Paints",
    "BHEL":"BHEL","BIOCON":"Biocon",
    "CANBK":"Canara Bank","CANFINHOME":"Can Fin Homes",
    "CHOLAFIN":"Cholamandalam Finance","COFORGE":"Coforge",
    "CONCOR":"Container Corp of India","CROMPTON":"Crompton Greaves Consumer",
    "CUMMINSIND":"Cummins India","DABUR":"Dabur India",
    "DEEPAKNTR":"Deepak Nitrite","DELHIVERY":"Delhivery",
    "DIXON":"Dixon Technologies","DLF":"DLF",
    "EXIDEIND":"Exide Industries","FEDERALBNK":"Federal Bank",
    "GAIL":"GAIL India","GLENMARK":"Glenmark Pharma",
    "GMRINFRA":"GMR Airports Infrastructure","GODREJCP":"Godrej Consumer Products",
    "GODREJPROP":"Godrej Properties","GRANULES":"Granules India",
    "HAL":"Hindustan Aeronautics","HAVELLS":"Havells India",
    "HFCL":"HFCL","IDFCFIRSTB":"IDFC First Bank",
    "IEX":"Indian Energy Exchange","IGL":"Indraprastha Gas",
    "INDHOTEL":"Indian Hotels","INDIAMART":"IndiaMART InterMESH",
    "INDUSTOWER":"Indus Towers","IRCTC":"IRCTC",
    "IRFC":"Indian Railway Finance Corp","JKCEMENT":"JK Cement",
    "JUBLFOOD":"Jubilant FoodWorks","KPITTECH":"KPIT Technologies",
    "KANSAINER":"Kansai Nerolac Paints","LICI":"LIC of India",
    "LALPATHLAB":"Dr Lal PathLabs","LAURUSLABS":"Laurus Labs",
    "LICHSGFIN":"LIC Housing Finance","LTTS":"L&T Technology Services",
    "LUPIN":"Lupin","MANAPPURAM":"Manappuram Finance",
    "MARICO":"Marico","METROPOLIS":"Metropolis Healthcare",
    "MFSL":"Max Financial Services","MGL":"Mahanagar Gas",
    "MOTHERSON":"Samvardhana Motherson","MPHASIS":"Mphasis",
    "MRF":"MRF","NAUKRI":"Info Edge (Naukri)",
    "NAVINFLUOR":"Navin Fluorine International","OBEROIRLTY":"Oberoi Realty",
    "OFSS":"Oracle Financial Services","PAGEIND":"Page Industries",
    "PEL":"Piramal Enterprises","PERSISTENT":"Persistent Systems",
    "PETRONET":"Petronet LNG","PFC":"Power Finance Corp",
    "PIDILITIND":"Pidilite Industries","PIIND":"PI Industries",
    "POLYCAB":"Polycab India","POONAWALLA":"Poonawalla Fincorp",
    "PVR":"PVR INOX","RAMCOCEM":"Ramco Cements",
    "RBLBANK":"RBL Bank","RECLTD":"REC",
    "SAIL":"SAIL","SCHAEFFLER":"Schaeffler India",
    "SIEMENS":"Siemens India","SRF":"SRF",
    "SUPREMEIND":"Supreme Industries","TATACOMM":"Tata Communications",
    "TATAELXSI":"Tata Elxsi","TATAPOWER":"Tata Power",
    "TATACHEM":"Tata Chemicals","TORNTPHARM":"Torrent Pharma",
    "TORNTPOWER":"Torrent Power","UBL":"United Breweries",
    "UNITDSPR":"United Spirits","UPL":"UPL",
    "VEDL":"Vedanta","VOLTAS":"Voltas",
    "WHIRLPOOL":"Whirlpool India","ZOMATO":"Zomato",
    "ZYDUSLIFE":"Zydus Lifesciences",
}


def fetch_nse_fno_symbols() -> dict[str, str]:
    """
    Fetch live NSE F&O symbol list from NSE website.
    Returns {symbol: company_name} dict.
    Falls back to _BUILTIN if fetch fails.

    NSE publishes the F&O security list at:
    https://www.nseindia.com/api/equity-stockIndices?index=SECURITIES%20IN%20F%26O
    """
    import requests
    url = ("https://www.nseindia.com/api/equity-stockIndices"
           "?index=SECURITIES%20IN%20F%26O")
    headers = {
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 Chrome/124 Safari/537.36"),
        "Accept": "application/json",
        "Referer": "https://www.nseindia.com/",
    }
    try:
        session = requests.Session()
        # Warm up the session cookie (NSE requires this)
        session.get("https://www.nseindia.com", headers=headers, timeout=8)
        r = session.get(url, headers=headers, timeout=10)
        if r.status_code == 200:
            data = r.json().get("data", [])
            result = {}
            for item in data:
                sym  = item.get("symbol", "").strip()
                name = item.get("meta", {}).get("companyName", "") or \
                       item.get("companyName", "") or sym
                if sym:
                    result[sym] = name
            if result:
                log.info("Fetched %d F&O symbols live from NSE website.", len(result))
                return result
    except Exception as exc:
        log.warning("Live NSE fetch failed (%s) — using built-in list.", exc)
    log.info("Using built-in F&O symbol list (%d stocks).", len(_BUILTIN))
    return dict(_BUILTIN)


# ===========================================================================
# 3. INDICATORS
# ===========================================================================

def _rsi(close, p=14):
    d = close.diff()
    g = d.clip(lower=0).ewm(com=p-1, min_periods=p).mean()
    l = (-d).clip(lower=0).ewm(com=p-1, min_periods=p).mean()
    return 100 - 100/(1 + g/l.replace(0, np.nan))

def _atr(h, l, c, p=14):
    tr = pd.concat([h-l, (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(1)
    return tr.ewm(com=p-1, min_periods=p).mean()

def add_indicators(df, cfg=Config()):
    c, h, l, v = df.close, df.high, df.low, df.volume
    df["sma_20"]       = c.rolling(cfg.SMA_SHORT).mean()
    df["sma_50"]       = c.rolling(cfg.SMA_MED).mean()
    df["sma_200"]      = c.rolling(cfg.SMA_LONG).mean()
    df["vol_sma_20"]   = v.rolling(cfg.VOL_SMA_SHORT).mean()
    df["vol_sma_50"]   = v.rolling(cfg.VOL_SMA_LONG).mean()
    df["rsi_14"]       = ta.rsi(c, length=14) if TA_OK else _rsi(c)
    df["atr_14"]       = ta.atr(h, l, c, length=14) if TA_OK else _atr(h, l, c)
    df["hhv_high_40"]  = h.rolling(cfg.HHV_SWING).max()
    df["hhv_high_100"] = h.rolling(cfg.HHV_POS).max()
    df["hhv_close_252"]= c.rolling(cfg.HHV_52W).max()
    return df


# ===========================================================================
# 4. DATA FETCH
# ===========================================================================

def fetch_ohlcv(yf_ticker, cfg=Config()):
    end   = datetime.today()
    start = end - timedelta(days=cfg.HIST_DAYS)
    try:
        raw = yf.download(yf_ticker, start=start.strftime("%Y-%m-%d"),
                          end=end.strftime("%Y-%m-%d"), interval="1d",
                          progress=False, auto_adjust=True)
    except Exception:
        return None
    if raw is None or raw.empty: return None
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = raw.columns.droplevel(1)
    raw = (raw.rename(columns=str.lower).reset_index()
             .rename(columns={"index":"date","Date":"date"}))
    raw["date"] = pd.to_datetime(raw["date"]).dt.date
    raw = raw.dropna(subset=["close"]).sort_values("date").reset_index(drop=True)
    return raw if len(raw) >= 60 else None


def get_live_ltp(symbol, yf_ticker):
    """Real-time LTP: nsetools first, yfinance 1m fallback."""
    if NSE_OK:
        try:
            q = _nse.get_quote(symbol.upper())
            if q:
                ltp = q.get("lastPrice") or q.get("lastTradedPrice")
                if ltp:
                    return float(str(ltp).replace(",","")), "NSE live"
        except Exception:
            pass
    try:
        df = yf.download(yf_ticker, period="1d", interval="1m",
                         progress=False, auto_adjust=True)
        if df is not None and not df.empty:
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.droplevel(1)
            return float(df["Close"].iloc[-1]), "yfinance 1m"
    except Exception:
        pass
    return None, "unavailable"


# ===========================================================================
# 5. SIGNAL LOGIC
# ===========================================================================

def _exits(close, atr, low_n, cfg):
    sl   = min(max(close - atr, low_n), close * 0.92)
    risk = max(close - sl, close * 0.03)
    return dict(sl=round(sl,2),
                target1=round(close + cfg.T1_R*risk, 2),
                target2=round(close + cfg.T2_R*risk, 2),
                risk_pct=round(risk/close*100, 2),
                t1_pct=round(cfg.T1_R*risk/close*100, 2),
                t2_pct=round(cfg.T2_R*risk/close*100, 2))

def _nan(*cols, row):
    return any(pd.isna(row.get(c)) for c in cols)

def check_a(row, hist, cfg=Config()):
    if _nan("sma_20","sma_50","rsi_14","atr_14","hhv_high_40","vol_sma_20", row=row):
        return None
    c, h = float(row.close), float(row.high)
    if c < cfg.CLOSE_MIN_A:                                  return None
    if c <= float(row.sma_20) or c <= float(row.sma_50):    return None
    hhv = float(row.hhv_high_40)
    if h < hhv or c < hhv:                                  return None
    if float(row.volume) < cfg.VOL_MULT_A*float(row.vol_sma_20): return None
    if float(row.rsi_14) < cfg.RSI_MIN_A:                   return None
    e = _exits(c, float(row.atr_14), float(hist.low.tail(10).min()), cfg)
    return dict(strategy="A — Swing Breakout",
                breakout="40-Day High",
                entry_price=round(c,2), hhv_40d=round(hhv,2),
                sma_20=round(float(row.sma_20),2),
                sma_50=round(float(row.sma_50),2),
                rsi_14=round(float(row.rsi_14),1),
                vol_ratio=round(float(row.volume)/float(row.vol_sma_20),2),
                volume=int(row.volume), vol_avg=round(float(row.vol_sma_20),0),
                **e)

def check_b(row, hist, cfg=Config()):
    if _nan("sma_50","sma_200","rsi_14","atr_14","hhv_high_100",
            "hhv_close_252","vol_sma_50", row=row):
        return None
    c = float(row.close)
    if c < cfg.CLOSE_MIN_B:                                  return None
    if c <= float(row.sma_50) or c <= float(row.sma_200):   return None
    h100 = float(row.hhv_high_100); h52 = float(row.hhv_close_252)
    at100 = c >= h100; at52 = c >= h52
    if not (at100 or at52):                                  return None
    if float(row.volume) < cfg.VOL_MULT_B*float(row.vol_sma_50): return None
    if float(row.rsi_14) < cfg.RSI_MIN_B:                   return None
    lbls = (["52-Week High"] if at52 else []) + (["100-Day High"] if at100 else [])
    e = _exits(c, float(row.atr_14), float(hist.low.tail(20).min()), cfg)
    return dict(strategy="B — Positional Breakout",
                breakout=" & ".join(lbls),
                entry_price=round(c,2),
                hhv_100d=round(h100,2), hhv_52w=round(h52,2),
                sma_50=round(float(row.sma_50),2),
                sma_200=round(float(row.sma_200),2),
                rsi_14=round(float(row.rsi_14),1),
                vol_ratio=round(float(row.volume)/float(row.vol_sma_50),2),
                volume=int(row.volume), vol_avg=round(float(row.vol_sma_50),0),
                **e)


# ===========================================================================
# 6. COLOURFUL TERMINAL DASHBOARD
# ===========================================================================

W  = 100   # total dashboard width

def _clr_rsi(rsi):
    """Return colour code for RSI value."""
    if   rsi >= 70: return Fore.RED
    elif rsi >= 60: return Fore.YELLOW
    elif rsi >= 55: return Fore.GREEN
    else:           return Fore.CYAN

def _rsi_bar(rsi, width=20):
    """ASCII bar representing RSI 0-100."""
    filled = int(round(rsi / 100 * width))
    bar    = "█" * filled + "░" * (width - filled)
    return f"{_clr_rsi(rsi)}{bar}{Style.RESET_ALL}"

def _vol_bar(ratio, width=15):
    """ASCII bar for volume ratio (0 – 4x)."""
    capped = min(ratio, 4.0)
    filled = int(round(capped / 4.0 * width))
    bar    = "▓" * filled + "░" * (width - filled)
    col    = Fore.GREEN if ratio >= 2.0 else Fore.YELLOW if ratio >= 1.5 else Fore.WHITE
    return f"{col}{bar}{Style.RESET_ALL} {ratio:.1f}x"

def _strategy_badge(s):
    if s.startswith("A"):
        return f"{Back.BLUE}{Fore.WHITE} SWING {Style.RESET_ALL}"
    return f"{Back.MAGENTA}{Fore.WHITE} POSITIONAL {Style.RESET_ALL}"

def _price_col(price):
    return f"{Fore.CYAN}{Style.BRIGHT}₹{price:,.2f}{Style.RESET_ALL}"

def _sl_col(sl):
    return f"{Fore.RED}₹{sl:,.2f}{Style.RESET_ALL}"

def _t1_col(t1):
    return f"{Fore.YELLOW}₹{t1:,.2f}{Style.RESET_ALL}"

def _t2_col(t2):
    return f"{Fore.GREEN}{Style.BRIGHT}₹{t2:,.2f}{Style.RESET_ALL}"

def _score_stars(score):
    stars = int(round(score * 5))
    return f"{Fore.YELLOW}{'★'*stars}{'☆'*(5-stars)}{Style.RESET_ALL}"

def print_dashboard(signals: pd.DataFrame, scan_time: str):
    SEP  = "─" * W
    DSEP = "═" * W

    # ── Header
    print(f"\n{Fore.CYAN}{Style.BRIGHT}{DSEP}{Style.RESET_ALL}")
    title = f"  NSE F&O BREAKOUT SCREENER  ·  {date.today():%d %b %Y}  ·  {scan_time}"
    print(f"{Fore.CYAN}{Style.BRIGHT}{title}{Style.RESET_ALL}")
    sub = "  Data: Yahoo Finance (historical)  +  nsetools (live NSE quotes)  ·  FREE"
    print(f"{Fore.WHITE}{sub}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{Style.BRIGHT}{DSEP}{Style.RESET_ALL}")

    if signals.empty:
        print(f"\n  {Fore.YELLOW}No breakout signals today.{Style.RESET_ALL}")
        print(f"  Tip: relax RSI_MIN_A to 50 or VOL_MULT_A to 1.2 in Config.\n")
        return

    # ── Summary bar
    a_cnt = signals.strategy.str.startswith("A").sum()
    b_cnt = signals.strategy.str.startswith("B").sum()
    print(f"\n  {Fore.WHITE}Total signals: {Style.BRIGHT}{len(signals)}{Style.RESET_ALL}"
          f"   {Back.BLUE}{Fore.WHITE} A-Swing: {a_cnt} {Style.RESET_ALL}"
          f"   {Back.MAGENTA}{Fore.WHITE} B-Positional: {b_cnt} {Style.RESET_ALL}\n")

    # ── Per-signal cards
    for i, (_, sig) in enumerate(signals.iterrows(), 1):
        sym      = sig.get("symbol", "")
        name     = sig.get("company", sym)
        strategy = sig.get("strategy", "")
        breakout = sig.get("breakout", "")
        entry    = sig.get("entry_price", 0)
        sl       = sig.get("sl", 0)
        t1       = sig.get("target1", 0)
        t2       = sig.get("target2", 0)
        rsi      = sig.get("rsi_14", 0)
        vratio   = sig.get("vol_ratio", 0)
        risk_pct = sig.get("risk_pct", 0)
        t1_pct   = sig.get("t1_pct", 0)
        t2_pct   = sig.get("t2_pct", 0)
        score    = sig.get("score", 0)
        vol      = sig.get("volume", 0)
        vol_avg  = sig.get("vol_avg", 0)
        scan_dt  = sig.get("scan_date", "")

        print(f"  {SEP}")

        # Row 1: symbol + name + badge + score
        badge = _strategy_badge(strategy)
        stars = _score_stars(score)
        sym_str = f"{Fore.WHITE}{Style.BRIGHT}{sym:<14}{Style.RESET_ALL}"
        name_str= f"{Fore.WHITE}{name:<35}{Style.RESET_ALL}"
        print(f"  {sym_str} {name_str} {badge}  {stars}  score: {score:.2f}")

        # Row 2: breakout level + scan date
        print(f"  {Fore.MAGENTA}Breakout: {breakout:<30}{Style.RESET_ALL}"
              f"  Scan date: {scan_dt}")

        # Row 3: prices
        print(f"  Entry {_price_col(entry)}   "
              f"SL {_sl_col(sl)} ({Fore.RED}-{risk_pct:.1f}%{Style.RESET_ALL})   "
              f"T1 {_t1_col(t1)} ({Fore.YELLOW}+{t1_pct:.1f}%{Style.RESET_ALL})   "
              f"T2 {_t2_col(t2)} ({Fore.GREEN}+{t2_pct:.1f}%{Style.RESET_ALL})")

        # Row 4: RSI gauge
        rsi_bar = _rsi_bar(rsi)
        rsi_val = f"{_clr_rsi(rsi)}{rsi:.1f}{Style.RESET_ALL}"
        zone = ("Overbought" if rsi >= 70 else
                "Strong"     if rsi >= 60 else
                "Bullish"    if rsi >= 55 else "Neutral")
        print(f"  RSI-14 [{rsi_bar}] {rsi_val:>5}  {Fore.WHITE}{zone}{Style.RESET_ALL}")

        # Row 5: Volume bar
        vol_bar = _vol_bar(vratio)
        vol_fmt = f"{vol/1e6:.2f}M" if vol >= 1e6 else f"{vol/1e3:.0f}K"
        avg_fmt = f"{vol_avg/1e6:.2f}M" if vol_avg >= 1e6 else f"{vol_avg/1e3:.0f}K"
        print(f"  Volume [{vol_bar}]  Today: {Fore.WHITE}{vol_fmt}{Style.RESET_ALL}"
              f"  Avg-20d: {Fore.WHITE}{avg_fmt}{Style.RESET_ALL}")

        # Row 6: SMAs
        sma20  = sig.get("sma_20",  sig.get("sma_50", 0))
        sma50  = sig.get("sma_50",  0)
        sma200 = sig.get("sma_200", 0)
        if sma200:
            print(f"  SMA-20: {Fore.CYAN}₹{sma20:,.2f}{Style.RESET_ALL}  "
                  f"SMA-50: {Fore.CYAN}₹{sma50:,.2f}{Style.RESET_ALL}  "
                  f"SMA-200: {Fore.CYAN}₹{sma200:,.2f}{Style.RESET_ALL}")
        else:
            print(f"  SMA-20: {Fore.CYAN}₹{sma20:,.2f}{Style.RESET_ALL}  "
                  f"SMA-50: {Fore.CYAN}₹{sma50:,.2f}{Style.RESET_ALL}")

        print()

    print(f"  {DSEP}")
    print(f"\n  {Fore.WHITE}RSI colour guide: "
          f"{Fore.GREEN}55-59 Bullish  "
          f"{Fore.YELLOW}60-69 Strong  "
          f"{Fore.RED}70+ Overbought{Style.RESET_ALL}")
    print(f"  {Fore.WHITE}Volume guide: "
          f"{Fore.YELLOW}1.5x avg = min threshold  "
          f"{Fore.GREEN}2x+ = strong confirmation{Style.RESET_ALL}\n")


# ===========================================================================
# 7. MAIN SCAN
# ===========================================================================

def _rank(df):
    df = df.copy()
    w = {"rsi_14":0.30, "vol_ratio":0.30, "t2_pct":0.40}
    for col, wt in w.items():
        if col not in df.columns: continue
        lo, hi = df[col].min(), df[col].max()
        df[f"_n_{col}"] = (df[col]-lo)/(hi-lo) if hi>lo else 0.5
    sc = [f"_n_{c}" for c in w if f"_n_{c}" in df.columns]
    wts= [w[c]      for c in w if f"_n_{c}" in df.columns]
    df["score"] = sum(df[s]*wt for s,wt in zip(sc,wts))
    df["score"] = df.score.round(3)
    df.drop(columns=[c for c in df.columns if c.startswith("_n_")], inplace=True)
    return df.sort_values(["strategy","score"], ascending=[True,False]).reset_index(drop=True)


def run_scan(symbol_map: dict[str, str], cfg=Config()):
    total    = len(symbol_map)
    signals  = []
    cache    = {}
    skipped  = 0

    print(f"\n{Fore.CYAN}{'='*68}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}  Scanning {total} NSE F&O stocks …{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{'='*68}{Style.RESET_ALL}\n")

    for idx, (sym, name) in enumerate(symbol_map.items(), 1):
        yf_tick = f"{sym}.NS"
        df = fetch_ohlcv(yf_tick, cfg)
        if df is None:
            log.debug("[%d/%d]  %-16s  SKIP", idx, total, yf_tick)
            skipped += 1
            continue

        close_last = df.close.iloc[-1]
        bar_pct = idx / total
        bar_w   = 40
        filled  = int(bar_pct * bar_w)
        bar     = f"{'█'*filled}{'░'*(bar_w-filled)}"
        print(f"\r  {Fore.CYAN}{bar}{Style.RESET_ALL} {idx}/{total}  "
              f"{sym:<14}  ₹{close_last:>9,.2f}", end="", flush=True)

        df   = add_indicators(df, cfg)
        cache[yf_tick] = df
        row  = df.iloc[-1]

        for checker in (check_a, check_b):
            sig = checker(row, df, cfg)
            if sig:
                signals.append({
                    "symbol":    sym,
                    "company":   name,
                    "yf_ticker": yf_tick,
                    "scan_date": row.date,
                    **sig,
                })

    print()   # newline after progress bar
    log.info("Scan complete. Signals: %d  |  Skipped: %d / %d",
             len(signals), skipped, total)

    if not signals:
        return pd.DataFrame(), cache

    return _rank(pd.DataFrame(signals)), cache


# ===========================================================================
# 8. LIVE POLLER
# ===========================================================================

class LivePoller:
    def __init__(self, signals: pd.DataFrame, cache: dict, cfg=Config()):
        self.cfg     = cfg
        self.cache   = cache
        self._stop   = threading.Event()
        self._alerted= set()
        self.sig_map = {}
        if not signals.empty:
            for _, r in signals.iterrows():
                self.sig_map[r.yf_ticker] = r.to_dict()

    @staticmethod
    def _market_open(cfg):
        now = datetime.now(IST)
        if now.weekday() >= 5: return False
        o = now.replace(hour=cfg.MARKET_OPEN[0],  minute=cfg.MARKET_OPEN[1],  second=0, microsecond=0)
        c = now.replace(hour=cfg.MARKET_CLOSE[0], minute=cfg.MARKET_CLOSE[1], second=0, microsecond=0)
        return o <= now <= c

    def _poll(self):
        for yf_tick, sig in list(self.sig_map.items()):
            sym   = sig["symbol"]
            entry = sig["entry_price"]
            sl    = sig["sl"]
            t1    = sig["target1"]
            t2    = sig["target2"]
            ltp, src = get_live_ltp(sym, yf_tick)
            if not ltp: continue
            chg = (ltp - entry) / entry * 100
            ts  = datetime.now(IST).strftime("%H:%M:%S IST")

            if ltp <= sl and (yf_tick,"SL") not in self._alerted:
                print(f"\n  {Back.RED}{Fore.WHITE} 🔴 SL HIT {Style.RESET_ALL}  "
                      f"[{ts}]  {Fore.WHITE}{Style.BRIGHT}{sym}{Style.RESET_ALL}  "
                      f"LTP={Fore.RED}₹{ltp:,.2f}{Style.RESET_ALL}  "
                      f"SL=₹{sl:,.2f}  ({Fore.RED}{chg:+.1f}%{Style.RESET_ALL})  [{src}]")
                self._alerted.add((yf_tick,"SL"))

            elif ltp >= t2 and (yf_tick,"T2") not in self._alerted:
                print(f"\n  {Back.GREEN}{Fore.WHITE} 🟢 T2 HIT {Style.RESET_ALL}  "
                      f"[{ts}]  {Fore.WHITE}{Style.BRIGHT}{sym}{Style.RESET_ALL}  "
                      f"LTP={Fore.GREEN}₹{ltp:,.2f}{Style.RESET_ALL}  "
                      f"T2=₹{t2:,.2f}  ({Fore.GREEN}{chg:+.1f}%{Style.RESET_ALL})  [{src}]")
                self._alerted.add((yf_tick,"T2"))

            elif ltp >= t1 and (yf_tick,"T1") not in self._alerted:
                print(f"\n  {Back.YELLOW}{Fore.BLACK} 🟡 T1 HIT {Style.RESET_ALL}  "
                      f"[{ts}]  {Fore.WHITE}{Style.BRIGHT}{sym}{Style.RESET_ALL}  "
                      f"LTP={Fore.YELLOW}₹{ltp:,.2f}{Style.RESET_ALL}  "
                      f"T1=₹{t1:,.2f}  ({Fore.YELLOW}{chg:+.1f}%{Style.RESET_ALL})  [{src}]")
                self._alerted.add((yf_tick,"T1"))

            # Fresh breakout check
            hist = self.cache.get(yf_tick)
            if hist is not None and yf_tick not in self.sig_map:
                last = hist.iloc[-1].copy()
                last["close"] = ltp
                last["high"]  = max(float(hist.high.iloc[-1]), ltp)
                for checker in (check_a, check_b):
                    s = checker(last, hist, self.cfg)
                    if s:
                        print(f"\n  {Back.CYAN}{Fore.BLACK} ⚡ LIVE SIGNAL {Style.RESET_ALL}  "
                              f"[{ts}]  {sym}  entry=₹{s['entry_price']:,.2f}  "
                              f"sl=₹{s['sl']:,.2f}  T1=₹{s['target1']:,.2f}  "
                              f"T2=₹{s['target2']:,.2f}  RSI={s['rsi_14']}")
                        self.sig_map[yf_tick] = {**s,"symbol":sym,"yf_ticker":yf_tick}

    def _loop(self):
        while not self._stop.is_set():
            if self._market_open(self.cfg):
                ts = datetime.now(IST).strftime("%H:%M:%S")
                log.info("[%s IST] Polling %d stocks ...", ts, len(self.sig_map))
                try: self._poll()
                except Exception as e: log.error("Poll error: %s", e)
            else:
                log.info("Market closed. Next check in %ds.", self.cfg.POLL_INTERVAL)
            self._stop.wait(self.cfg.POLL_INTERVAL)

    def start(self):
        if not self.sig_map:
            log.info("No signal stocks to poll."); return
        t = threading.Thread(target=self._loop, daemon=True, name="LivePoller")
        t.start()
        log.info("Live poller started — polling every %ds. Press Ctrl+C to stop.",
                 self.cfg.POLL_INTERVAL)

    def stop(self): self._stop.set()


# ===========================================================================
# 9. ENTRY POINT
# ===========================================================================

def main():
    ap = argparse.ArgumentParser(description="NSE F&O Breakout Screener (free)")
    ap.add_argument("--scan-only",  action="store_true",
                    help="Scan + dashboard only, no live polling.")
    ap.add_argument("--live-only",  action="store_true",
                    help="Skip scan, reload last signals CSV and start live poller.")
    args = ap.parse_args()
    cfg  = Config()

    if args.live_only:
        p = Path(cfg.LAST_CSV)
        if not p.exists():
            log.error("No saved scan (%s). Run without --live-only first.", cfg.LAST_CSV)
            sys.exit(1)
        signals = pd.read_csv(p)
        log.info("Loaded %d signals from %s", len(signals), cfg.LAST_CSV)
        cache = {}
    else:
        # Fetch symbol master
        symbol_map = fetch_nse_fno_symbols()
        # Run scan
        scan_start = datetime.now(IST)
        signals, cache = run_scan(symbol_map, cfg)
        scan_time = scan_start.strftime("%H:%M IST")
        # Dashboard
        print_dashboard(signals, scan_time)
        # Save
        if not signals.empty:
            signals.to_csv(cfg.OUTPUT_CSV, index=False)
            signals.to_csv(cfg.LAST_CSV,   index=False)
            log.info("Signals saved → %s", cfg.OUTPUT_CSV)

    if args.scan_only:
        log.info("--scan-only: done."); return

    # Live poll
    poller = LivePoller(signals, cache, cfg)
    poller.start()
    try:
        while True: time.sleep(1)
    except KeyboardInterrupt:
        log.info("Stopping..."); poller.stop()

if __name__ == "__main__":
    main()
